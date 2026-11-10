#!/usr/bin/env python3
"""Small, explicit publisher. No runtime Git pulls and no shell-evaluated config."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
import errno
import fcntl
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("onionflash.deploy")
SAFE_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
PUBLIC_FILES = {"index.html", "feed.xml", "feed.json", "style.css", "app.js", "mark.svg", "_headers"}
TOPICS = {"security", "release", "dos", "directory-authority", "relay", "protocol", "feature", "network"}


def public_path_allowed(relative):
    return (relative in PUBLIC_FILES or
            bool(re.fullmatch(r"news/[A-Za-z0-9_-]{1,64}/index\.html", relative)) or
            relative in {f"tags/{topic}.xml" for topic in TOPICS})


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def sync_directory(path):
    """Persist directory entries where the platform supports directory fsync."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.EBADF}:
            raise


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


def load_env(path):
    """Literal dotenv syntax, with existing process environment taking priority."""
    path = Path(path)
    if not path.exists():
        return
    if path.is_symlink() or path.stat().st_mode & 0o077:
        raise ValueError("Credential file must be a regular file with mode 600")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        key, separator, raw = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
            raise ValueError("Invalid credential-file assignment")
        tokens = shlex.split(raw, comments=True)
        if len(tokens) > 1:
            raise ValueError("Quote values containing spaces")
        os.environ.setdefault(key, tokens[0] if tokens else "")


class BusyLock(BlockingIOError):
    """Only lock acquisition failures, not I/O failures inside a locked job."""


@contextmanager
def lock(path, shared=False, nonblocking=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a+") as stream:
        flags = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        if nonblocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(stream, flags)
        except BlockingIOError:
            raise BusyLock("Another Onion Flash deployment is running") from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


@contextmanager
def job_log(path):
    """Only use inside the job lock, including handler setup and teardown."""
    handler = RotatingFileHandler(path, maxBytes=1048576, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(handler)
    try:
        yield
    finally:
        LOG.removeHandler(handler)
        handler.close()


class Publisher:
    def __init__(self, config_path):
        self.config_path = Path(config_path).resolve()
        self.cfg = tomllib.loads(self.config_path.read_text())
        self.directory = self.config_path.parent
        load_env(self.path(self.cfg.get("env_file", "secrets.env")))
        self.state = self.path(self.cfg.get("state_dir", "state"))
        self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.state, 0o700)
        self.app_config = self.path(self.cfg["app_config"])
        self.app = tomllib.loads(self.app_config.read_text())
        self.public = (self.app_config.parent / self.app["storage"]["output_dir"]).resolve()
        self.database = (self.app_config.parent / self.app["storage"]["database"]).resolve()
        self.base_url = self.app["site"]["base_url"].rstrip("/") + "/"
        parsed = urlsplit(self.base_url)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Public base_url must be an HTTPS URL without credentials, query, or fragment")
        if not re.fullmatch(r"(?:/[a-zA-Z0-9_-]+)*/", parsed.path):
            raise ValueError("Use a plain path for the public site")
        self.base_path = parsed.path.rstrip("/")
        self.host = parsed.netloc
        self.cf = self.cfg["cloudflare"]
        if not SAFE_ID.fullmatch(self.cf["worker_name"]):
            raise ValueError("Invalid Worker name")
        self.net = self.cfg["network"]
        for key in ["attempts", "timeout_seconds", "backoff_seconds", "backoff_cap_seconds", "jitter_seconds", "max_response_bytes"]:
            if (isinstance(self.net[key], bool) or not isinstance(self.net[key], (int, float)) or
                    not math.isfinite(self.net[key]) or self.net[key] < 0):
                raise ValueError("Invalid network parameter: " + key)
        if (type(self.net["attempts"]) is not int or not 1 <= self.net["attempts"] <= 10 or
                self.net["timeout_seconds"] <= 0 or type(self.net["max_response_bytes"]) is not int or
                self.net["max_response_bytes"] < 1):
            raise ValueError("Invalid attempt/timeout limits")
        self.account = self.secret(self.cf["account_id_env"])
        if not re.fullmatch(r"[0-9a-f]{32}", self.account):
            raise ValueError("Invalid Cloudflare account ID")
        self.token = self.secret(self.cf["api_token_env"])
        self.api_base = self.cf["api_base_url"].rstrip("/")
        if urlsplit(self.api_base).scheme != "https":
            raise ValueError("Cloudflare API endpoint must use HTTPS")
        self._http_client = None
        self.node = ROOT / "node_modules/node/bin/node"
        self.wrangler = ROOT / "node_modules/wrangler/bin/wrangler.js"

    def path(self, value):
        # Preserve the final executable symlink: resolving .venv/bin/python to
        # /usr/bin/python makes Python forget its virtual environment.
        return Path(os.path.abspath(self.directory / value))

    @staticmethod
    def secret(name):
        value = os.environ.get(name, "")
        if not value:
            raise ValueError("Missing environment setting: " + name)
        return value

    def clean_error(self, value):
        result = str(value)
        for secret in self.secret_values():
            result = result.replace(secret, "[redacted]")
        return result[-2500:]

    def secret_values(self):
        names = {k for k in os.environ if any(x in k.upper() for x in ("TOKEN", "SECRET", "API_KEY", "SPACES_KEY"))}
        for section in [self.cf, self.cfg.get("spaces", {}), self.app.get("llm", {})]:
            names.update(v for k, v in section.items() if k in {"api_token_env", "zone_token_env", "access_key_env", "secret_key_env", "api_key_env"})
        return [os.environ[name] for name in names if len(os.environ.get(name, "")) >= 8]

    def http_settings(self):
        return {"timeout_seconds": self.net["timeout_seconds"],
                "connect_timeout_seconds": self.net["timeout_seconds"],
                "total_timeout_seconds": self.net["timeout_seconds"],
                "retry_attempts": self.net["attempts"], "requests_per_minute": 0,
                "backoff_base_seconds": self.net["backoff_seconds"],
                "backoff_cap_seconds": self.net["backoff_cap_seconds"],
                "backoff_jitter_seconds": self.net["jitter_seconds"],
                "max_retry_after_seconds": self.net["backoff_cap_seconds"],
                "max_response_bytes": self.net["max_response_bytes"], "max_redirects": 10,
                "user_agent": "OnionFlashPublisher/0.1"}

    def delay(self, attempt, retry_after=""):
        from onionflash.transport import retry_delay
        time.sleep(retry_delay(attempt, self.http_settings(), retry_after))

    def request(self, url, method="GET", data=None, token=None):
        # Lazy import keeps install.py usable before the app is installed.
        from onionflash.transport import FetchError, HttpClient
        if self._http_client is None:
            self._http_client = HttpClient(self.http_settings(), same_origin_redirects=True)
        headers = {"Accept-Encoding": "identity"}
        if token:
            headers["Authorization"] = "Bearer " + token
        try:
            return self._http_client.request(method, url, headers=headers, json_body=data).body
        except FetchError as exc:
            if exc.status:
                raise HTTPError(url, exc.status, str(exc), {}, None) from None
            if exc.retryable:
                raise URLError(str(exc)) from None
            raise ValueError(str(exc)) from None

    def close(self):
        if self._http_client is not None:
            self._http_client.close()

    def api(self, suffix, method="GET", data=None, zone=False):
        token = self.secret(self.cf.get("zone_token_env", "CLOUDFLARE_ZONE_TOKEN")) if zone else self.token
        response = json.loads(self.request(self.api_base + suffix, method, data, token))
        if not response.get("success"):
            raise RuntimeError("Cloudflare API rejected request: " + self.clean_error(response.get("errors")))
        return response.get("result")

    def current_version(self):
        try:
            result = self.api(f"/accounts/{self.account}/workers/scripts/{self.cf['worker_name']}/deployments")
        except HTTPError as exc:
            if exc.code == 404:
                return None
            raise
        deployments = result.get("deployments", []) if isinstance(result, dict) else (result or [])
        if not deployments:
            return None
        versions = deployments[0].get("versions", [])
        if len(versions) != 1 or versions[0].get("percentage") != 100:
            raise ValueError("Refusing to replace a split deployment")
        return versions[0]["version_id"]

    def command(self, arguments, metadata=None):
        env = os.environ.copy()
        env.update(CLOUDFLARE_ACCOUNT_ID=self.account, CLOUDFLARE_API_TOKEN=self.token,
                   WRANGLER_SEND_METRICS="false", WRANGLER_LOG_SANITIZE="true",
                   WRANGLER_LOG_PATH=str(self.state / "wrangler-logs"), CI="true")
        if metadata:
            env["WRANGLER_OUTPUT_FILE_PATH"] = str(metadata)
        for attempt in range(self.net["attempts"]):
            try:
                result = subprocess.run([str(self.node), str(self.wrangler), *arguments], cwd=ROOT, env=env,
                                        capture_output=True, text=True, timeout=self.cf["command_timeout_seconds"])
            except subprocess.TimeoutExpired:
                if attempt + 1 == self.net["attempts"]:
                    raise
                self.delay(attempt)
                continue
            if result.returncode == 0:
                return result.stdout
            message = self.clean_error(result.stdout + result.stderr)
            if attempt + 1 == self.net["attempts"] or not re.search(r"ECONNRESET|ETIMEDOUT|EAI_AGAIN|fetch failed|network error|\b(?:429|500|502|503|504)\b", message, re.I):
                raise RuntimeError(message)
            self.delay(attempt)
        raise RuntimeError("Wrangler exhausted its bounded retry budget")

    def make_config(self, directory, bootstrap=False):
        config = {"name": self.cf["worker_name"], "account_id": self.account,
                  "compatibility_date": self.cf["compatibility_date"], "workers_dev": True,
                  "preview_urls": True, "main": str(ROOT / ("bootstrap.mjs" if bootstrap else "worker.mjs"))}
        if not bootstrap:
            config["assets"] = {"directory": str(directory), "binding": "ASSETS",
                                "html_handling": "auto-trailing-slash", "not_found_handling": "none"}
            config["vars"] = {"BASE_PATH": self.base_path}
        target = self.state / ("wrangler-bootstrap.json" if bootstrap else "wrangler.json")
        atomic_json(target, config)
        return target

    def prepare_assets(self, public):
        """Mount files by their asset keys; no account-gated base_path feature."""
        if not self.base_path:
            return public
        assets = public.parent / "cloudflare-assets"
        if assets.exists():
            raise ValueError("Asset staging directory must be new")
        target = assets / self.base_path.lstrip("/")
        target.parent.mkdir(parents=True)
        shutil.copytree(public, target)
        # Cloudflare reads _headers only from the asset root, never serve it as
        # an ordinary file under the mounted application directory.
        (target / "_headers").replace(assets / "_headers")
        return assets

    def validate(self, directory):
        files, total = {}, 0
        secrets = [value.encode() for value in self.secret_values()]
        for path in sorted(directory.rglob("*")):
            if path.is_symlink():
                raise ValueError("Symlinks are forbidden in public output")
            if not path.is_file():
                continue
            relative = path.relative_to(directory).as_posix()
            if not public_path_allowed(relative):
                raise ValueError("Unexpected public file: " + relative)
            size = path.stat().st_size
            total += size
            if size > self.cf["max_file_bytes"] or total > self.cf["max_total_bytes"]:
                raise ValueError("Public output exceeds size budget")
            body = path.read_bytes()
            if any(secret in body for secret in secrets):
                raise ValueError("Credential detected in public output")
            files[relative] = hashlib.sha256(body).hexdigest()
        if len(files) > self.cf["max_files"]:
            raise ValueError("Public output exceeds file count budget")
        for relative in {*PUBLIC_FILES, *self.cf["verify_paths"]}:
            if relative not in files:
                raise ValueError("Missing required public file: " + relative)
        import xml.etree.ElementTree as ET
        root = ET.parse(directory / "feed.xml").getroot()
        if root.tag != "rss":
            raise ValueError("Expected RSS feed")
        json.loads((directory / "feed.json").read_text())
        return files

    def verify(self, base_url, manifest):
        for relative in self.cf["verify_paths"]:
            # Newly activated assets may take a short time to reach the edge.
            for attempt in range(self.net["attempts"]):
                try:
                    body = self.request(base_url.rstrip("/") + "/" + relative)
                    if hashlib.sha256(body).hexdigest() != manifest[relative]:
                        raise ValueError("Published hash mismatch: " + relative)
                    break
                except (ValueError, HTTPError) as exc:
                    if isinstance(exc, HTTPError) and exc.code != 404:
                        raise
                    if attempt + 1 == self.net["attempts"]:
                        raise
                    self.delay(attempt)

    def activate(self, version, config):
        if not re.fullmatch(r"[0-9a-f-]{36}", version):
            raise ValueError("Invalid version identity")
        try:
            self.command(["versions", "deploy", version + "@100%", "--yes", "--config", str(config)])
        except (RuntimeError, subprocess.TimeoutExpired):
            if self.current_version() != version:
                raise

    def control_config(self):
        target = self.state / "wrangler-control.json"
        atomic_json(target, {"name": self.cf["worker_name"], "account_id": self.account,
                             "compatibility_date": self.cf["compatibility_date"]})
        return target

    def verify_receipt(self, receipt):
        self.verify(receipt["origin_url"], receipt["manifest"])
        if (self.state / "routes.json").exists():
            self.verify(self.base_url, receipt["manifest"])

    def commit_receipt(self, receipt):
        version = receipt["version_id"]
        if not re.fullmatch(r"[0-9a-f-]{36}", version):
            raise ValueError("Invalid receipt version identity")
        # A current pointer must never reference an unarchived publication.
        atomic_json(self.state / "receipts" / (version + ".json"), receipt)
        atomic_json(self.state / "publication.json", receipt)

    def finish_transition(self, journal, outcome):
        journal = {**journal, "outcome": outcome, "finished_at": datetime.now(timezone.utc).isoformat()}
        atomic_json(self.state / "last-transition.json", journal)
        (self.state / "transition.json").unlink(missing_ok=True)
        sync_directory(self.state)

    def complete_transition(self, journal):
        receipt = dict(journal["target"])
        if journal["kind"] == "publish":
            archived = read_json(self.state / "receipts" / (receipt["version_id"] + ".json"), {})
            receipt["published_at"] = archived.get("published_at", datetime.now(timezone.utc).isoformat())
        self.commit_receipt(receipt)
        if journal["kind"] == "rollback":
            atomic_json(self.state / "rollback.json", {"version_id": receipt["version_id"], "timestamp": time.time()})
            atomic_json(self.state / "publication-hold.json", {"reason": "rollback completed", "to": receipt["version_id"], "timestamp": time.time()})
        self.finish_transition({**journal, "target": receipt}, "committed")
        return receipt

    def restore_transition(self, journal):
        actual = self.current_version()
        before = journal["before_version"]
        if actual not in {before, journal["target"]["version_id"]}:
            raise ValueError("Unknown external version; transition retained for investigation")
        if actual != before:
            self.activate(before, self.control_config())
        receipt = journal.get("before_receipt")
        if receipt:
            self.verify_receipt(receipt)
            self.commit_receipt(receipt)
        else:
            (self.state / "publication.json").unlink(missing_ok=True)
        self.finish_transition(journal, "restored-before")
        return receipt

    def transition(self, target, before_version, before_receipt, kind="publish"):
        if read_json(self.state / "transition.json"):
            raise ValueError("An unfinished transition exists; run reconcile first")
        if self.current_version() != before_version:
            raise ValueError("Active Worker version changed before activation")
        journal = {"kind": kind, "before_version": before_version,
                   "before_receipt": before_receipt, "target": target,
                   "started_at": datetime.now(timezone.utc).isoformat()}
        atomic_json(self.state / "transition.json", journal)
        # Interruptions, including process termination, leave this journal for
        # reconciliation. All metadata is gathered before the external mutation.
        try:
            self.activate(target["version_id"], self.control_config())
            self.verify_receipt(target)
            return self.complete_transition(journal)
        except Exception:
            # Restore and fail this run; the next run retries. An automatic hold
            # would freeze publication while every later run reported success.
            try:
                self.restore_transition(journal)
            except Exception as recovery_error:
                raise RuntimeError("Transition uncertain; run reconcile: " + self.clean_error(recovery_error)) from recovery_error
            raise

    def reconcile(self):
        journal = read_json(self.state / "transition.json")
        if not journal:
            return None
        actual = self.current_version()
        if actual == journal["target"]["version_id"]:
            try:
                self.verify_receipt(journal["target"])
            except Exception:
                # Restore the previous version, then report the failed check.
                self.restore_transition(journal)
                raise
            return self.complete_transition(journal)
        if actual == journal["before_version"]:
            return self.restore_transition(journal)
        raise ValueError("Unknown external version; transition retained for investigation")

    def rollback(self):
        if read_json(self.state / "transition.json"):
            raise ValueError("Run reconcile before requesting another rollback")
        receipt = read_json(self.state / "publication.json", {})
        previous = receipt.get("previous_version_id")
        restored = read_json(self.state / "receipts" / (str(previous) + ".json"))
        if not previous or not restored:
            raise ValueError("No verified previous publication exists; use hold to pause publication")
        atomic_json(self.state / "publication-hold.json", {"reason": "rollback requested", "from": receipt["version_id"], "to": previous, "timestamp": time.time()})
        return self.transition(restored, receipt["version_id"], receipt, "rollback")

    def publish(self):
        if not (self.state / "publication-hold.json").exists():
            self.reconcile()
        if (self.state / "publication-hold.json").exists():
            raise ValueError("Publication is on hold; inspect status and use the explicit resume command")
        if not self.public.is_dir():
            raise ValueError("Build the public output first")
        with tempfile.TemporaryDirectory(prefix="release-", dir=self.state) as temporary:
            stage = Path(temporary) / "public"
            # Core and publisher use the same lock for a consistent snapshot.
            with lock(str(self.database) + ".lock", shared=True):
                shutil.copytree(self.public, stage, symlinks=True)
            manifest = self.validate(stage)
            identity = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
            previous_path = self.state / "publication.json"
            previous = read_json(previous_path, {})
            if previous.get("content_hash") == identity:
                if self.current_version() != previous.get("version_id"):
                    raise ValueError("Active Worker version differs from the publication receipt; reconcile before publishing")
                LOG.info("Public output unchanged; skipping Cloudflare upload")
                return previous
            old_version = self.current_version()
            if previous and old_version != previous.get("version_id"):
                raise ValueError("Active Worker version changed outside this publisher; reconcile before uploading")
            if old_version is None:
                # Initialize only this new Worker, with a 503 placeholder and no routes.
                bootstrap = self.make_config(stage, bootstrap=True)
                self.command(["deploy", "--config", str(bootstrap)])
                old_version = self.current_version()
            config = self.make_config(self.prepare_assets(stage))
            metadata = Path(temporary) / "upload.ndjson"
            self.command(["versions", "upload", "--config", str(config), "--preview-alias", "candidate"], metadata)
            records = [json.loads(line) for line in metadata.read_text().splitlines() if line.strip()]
            uploaded = next(x for x in records if x.get("type") == "version-upload")
            version, preview = uploaded["version_id"], uploaded["preview_url"]
            parsed = urlsplit(preview)
            if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".workers.dev") or parsed.username:
                raise ValueError("Invalid Cloudflare preview URL")
            self.verify(preview.rstrip("/") + self.base_path, manifest)
            # Fetch fallible metadata before activation, never after changing production.
            account_subdomain = self.api(f"/accounts/{self.account}/workers/subdomain")["subdomain"]
            origin = f"https://{self.cf['worker_name']}.{account_subdomain}.workers.dev" + self.base_path
            receipt = {"version_id": version, "previous_version_id": old_version,
                       "content_hash": identity, "manifest": manifest,
                       "preview_url": preview, "origin_url": origin,
                       "app_version": self.app_version(),
                       "app_commit": self.git_commit(self.path(self.cfg.get("app_source", "../onionflash"))),
                       "deployment_commit": self.git_commit(ROOT)}
            receipt = self.transition(receipt, old_version, previous or None)
            LOG.info("Published version %s (%s files)", version, len(manifest))
            return receipt

    def app_version(self):
        result = subprocess.run([str(self.path(self.cfg["app_python"])), "-c", "import importlib.metadata; print(importlib.metadata.version('onionflash'))"], capture_output=True, text=True, timeout=10)
        return result.stdout.strip() if result.returncode == 0 else "unknown"

    @staticmethod
    def git_commit(directory):
        result = subprocess.run(["git", "-C", str(directory), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10)
        value = result.stdout.strip()
        return value if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", value) else None

    def routes(self):
        receipt = json.loads((self.state / "publication.json").read_text())
        self.verify(receipt["origin_url"], receipt["manifest"])
        zone = self.secret(self.cf["zone_id_env"])
        if not re.fullmatch(r"[0-9a-f]{32}", zone):
            raise ValueError("Invalid zone ID")
        existing = self.api(f"/zones/{zone}/workers/routes", zone=True)
        # CF matches query strings too: a terminal wildcard is needed for the
        # slashless mount with ?query. worker.mjs explicitly passes sibling
        # paths (e.g. /onionflash-other) through to their unchanged origin.
        patterns = [self.host + self.base_path + "*"] if self.base_path else [self.host + "/*"]
        pending = []
        for pattern in patterns:
            found = [x for x in existing if x["pattern"] == pattern]
            if found and any(x.get("script") != self.cf["worker_name"] for x in found):
                raise ValueError("Route is already owned by another Worker: " + pattern)
            if not found:
                pending.append(pattern)
        created = []
        try:
            for pattern in pending:
                route = self.api(f"/zones/{zone}/workers/routes", "POST", {"pattern": pattern, "script": self.cf["worker_name"]}, zone=True)
                created.append(route["id"])
                LOG.info("Added scoped route %s", pattern)
            self.verify(self.base_url, receipt["manifest"])
        except Exception:
            for route_id in reversed(created):
                self.api(f"/zones/{zone}/workers/routes/{route_id}", "DELETE", zone=True)
            raise
        atomic_json(self.state / "routes.json", {"patterns": patterns, "verified_at": datetime.now(timezone.utc).isoformat()})
        LOG.info("Verified public website and RSS at %s", self.base_url)

    def backup(self, force=False):
        cfg = self.cfg.get("spaces", {})
        if not cfg.get("enabled"):
            return
        for key in ["retention_days", "backup_interval_seconds"]:
            if type(cfg.get(key)) is not int or cfg[key] < 1:
                raise ValueError("Spaces " + key + " must be a positive integer")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", cfg["bucket"]):
            raise ValueError("Invalid Spaces bucket")
        prefix = cfg["prefix"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]+/backups/", prefix):
            raise ValueError("Backup prefix must be a project-specific name/backups/")
        if urlsplit(cfg["endpoint_url"]).scheme != "https":
            raise ValueError("Spaces endpoint must use HTTPS")
        destination = {key: cfg[key] for key in ("endpoint_url", "bucket", "prefix")}
        marker = self.state / "backup.json"
        previous = read_json(marker, {})
        age = time.time() - previous.get("timestamp", 0)
        if not force and previous.get("destination") == destination and 0 <= age < cfg["backup_interval_seconds"]:
            return
        if not self.database.is_file():
            raise ValueError("No application database to back up")
        import boto3
        from botocore.config import Config
        from onionflash.store import backup_database
        client = boto3.client("s3", endpoint_url=cfg["endpoint_url"], region_name=cfg["region"],
                              aws_access_key_id=self.secret(cfg["access_key_env"]),
                              aws_secret_access_key=self.secret(cfg["secret_key_env"]),
                              config=Config(connect_timeout=self.net["timeout_seconds"], read_timeout=self.net["timeout_seconds"],
                                            retries={"mode": "standard", "max_attempts": int(self.net["attempts"])},
                                            request_checksum_calculation="when_required", response_checksum_validation="when_required"))
        if cfg.get("create_bucket"):
            try:
                client.head_bucket(Bucket=cfg["bucket"])
            except client.exceptions.ClientError as exc:
                if str(exc.response.get("Error", {}).get("Code")) not in {"404", "NoSuchBucket"}:
                    raise
                client.create_bucket(Bucket=cfg["bucket"], ACL="private")
        with tempfile.TemporaryDirectory(prefix="backup-", dir=self.state) as temporary:
            temporary = Path(temporary)
            archive = temporary / "snapshot.tar.gz"
            with lock(str(self.database) + ".lock", shared=True):
                # One lock keeps the consistent DB copy and generated files at
                # the same application state; the shared helper closes handles.
                backup_database(self.database, temporary / "onionflash.sqlite3")
                public_error = None
                try:
                    manifest = self.validate(self.public) if self.public.is_dir() else {}
                except Exception as exc:
                    public_error = self.clean_error(exc)
                    manifest = {}
                    LOG.warning("Backing up SQLite without invalid generated files: %s", public_error)
                publication = read_json(self.state / "publication.json", {})
                atomic_json(temporary / "snapshot-manifest.json", {
                    "captured_at": datetime.now(timezone.utc).isoformat(), "public_manifest": manifest, "public_error": public_error,
                    "matches_publication": bool(publication) and manifest == publication.get("manifest")})
                with tarfile.open(archive, "w:gz") as tar:
                    tar.add(temporary / "onionflash.sqlite3", arcname="onionflash.sqlite3")
                    tar.add(temporary / "snapshot-manifest.json", arcname="snapshot-manifest.json")
                    if publication:
                        atomic_json(temporary / "publication.json", publication)
                        tar.add(temporary / "publication.json", arcname="publication.json")
                    if manifest:
                        tar.add(self.public, arcname="public", recursive=True)
            with archive.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            # Unique names ensure failure cleanup cannot remove an older backup.
            key = prefix + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + digest[:12] + "-" + uuid.uuid4().hex + ".tar.gz"
            try:
                client.upload_file(str(archive), cfg["bucket"], key, ExtraArgs={"ACL": "private", "ContentType": "application/gzip", "Metadata": {"sha256": digest}})
                head = client.head_object(Bucket=cfg["bucket"], Key=key)
                if head.get("Metadata", {}).get("sha256") != digest or head["ContentLength"] != archive.stat().st_size:
                    raise ValueError("Backup upload verification failed")
                acl = client.get_object_acl(Bucket=cfg["bucket"], Key=key)
                if any(grant.get("Grantee", {}).get("Type") == "Group" for grant in acl.get("Grants", [])):
                    raise ValueError("Backup object unexpectedly has a public/group ACL")
            except Exception as failure:
                try:
                    client.delete_object(Bucket=cfg["bucket"], Key=key)
                except Exception as cleanup:
                    raise RuntimeError("Backup verification/upload failed and cleanup failed for " + key + ": " + self.clean_error(failure) + "; " + self.clean_error(cleanup)) from cleanup
                raise
            cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])
            pattern = re.escape(prefix) + r"\d{8}T\d{6}Z-[0-9a-f]{12}(?:-[0-9a-f]{32})?\.tar\.gz"
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=cfg["bucket"], Prefix=prefix):
                for item in page.get("Contents", []):
                    if item["Key"] != key and item["LastModified"] < cutoff and re.fullmatch(pattern, item["Key"]):
                        client.delete_object(Bucket=cfg["bucket"], Key=item["Key"])
            # Retention failures remain retryable; a verified newest object is
            # retained even if cleanup of older objects fails.
            atomic_json(marker, {"timestamp": time.time(), "key": key, "sha256": digest, "destination": destination})
            LOG.info("Verified private Spaces backup %s", key)

    def schedule(self):
        interval = self.cfg.get("interval_minutes", 5)
        if not isinstance(interval, int) or not 1 <= interval <= 59:
            raise ValueError("interval_minutes must be 1..59")
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode and "no crontab" not in result.stderr.lower():
            raise RuntimeError("Cannot read existing crontab")
        begin, end = "# BEGIN ONIONFLASH", "# END ONIONFLASH"
        current = result.stdout
        if current.count(begin) != current.count(end) or current.count(begin) > 1:
            raise ValueError("Ambiguous existing Onion Flash schedule")
        current = re.sub(re.escape(begin) + r".*?" + re.escape(end) + r"\n?", "", current, flags=re.S)
        command = " ".join(shlex.quote(str(x)) for x in [self.path(self.cfg["app_python"]), ROOT / "publish.py", "--config", self.config_path, "run"])
        # The application logs bounded summaries, never credential-bearing command lines.
        block = f"{begin}\n*/{interval} * * * * {command} 2>&1 | logger -t onionflash\n{end}\n"
        subprocess.run(["crontab", "-"], input=current.rstrip() + "\n" + block, text=True, check=True)
        LOG.info("Installed a %s-minute cron job", interval)


    def application(self, command):
        """Run one app command and return its output; failures carry the app's error."""
        budget = self.app.get("collection", {}).get("run_budget_seconds", 240)
        result = subprocess.run([str(self.path(self.cfg["app_python"])), "-m", "onionflash", command,
                                 "--config", str(self.app_config)], capture_output=True, text=True,
                                timeout=budget + 300)
        if result.returncode:
            detail = result.stderr.strip() or f"exited with status {result.returncode}"
            raise RuntimeError(self.clean_error(detail[-1000:]))
        return result.stdout.strip()

    def collect(self, result):
        report = self.application("run")
        LOG.info("run: %s", self.clean_error(report.splitlines()[-1] if report else "no report"))
        try:
            result["report"] = json.loads(report.splitlines()[-1])
        except (ValueError, IndexError):
            result["report"] = None

    def run_cycle(self):
        result = {"phases": {}, "errors": [], "warnings": []}
        def attempt(phase, action, required=True):
            try:
                action()
                result["phases"][phase] = "ok"
                return True
            except Exception as exc:
                message = phase + ": " + self.clean_error(exc)
                result["phases"][phase] = "failed"
                result["errors" if required else "warnings"].append(message)
                LOG.error("%s", message)
                return False
        if attempt("collection", lambda: self.collect(result)):
            if (self.state / "publication-hold.json").exists():
                if read_json(self.state / "transition.json"):
                    message = "publication: unfinished transition; run reconcile before resuming"
                    result["phases"]["publication"] = "unreconciled"
                    result["errors"].append(message)
                    LOG.error("%s", message)
                else:
                    result["phases"]["publication"] = "held"
            else:
                attempt("publication", self.publish)
        else:
            result["phases"]["publication"] = "skipped"
        # Collection may have changed the DB even when publication cannot run.
        attempt("backup", self.backup, required=self.cfg.get("spaces", {}).get("required", False))
        result.update(ok=not result["errors"], degraded=bool(result["warnings"]))
        return result

    def status(self):
        try:
            with lock(self.state / "run.lock", shared=True, nonblocking=True):
                running = False
        except BusyLock:
            running = True
        print(json.dumps({"running": running}))
        for name in ["publication.json", "publication-hold.json", "transition.json", "last-transition.json", "backup.json", "last-run.json"]:
            data = read_json(self.state / name)
            if data is not None:
                data.pop("manifest", None)
                if name in {"transition.json", "last-transition.json"}:
                    data = {key: data.get(key) for key in ("kind", "outcome", "before_version", "started_at", "finished_at")} | {"target_version": data.get("target", {}).get("version_id")}
                print(json.dumps({name: data}, indent=2))

    def execute(self, command, force=False):
        if command == "run":
            return self.run_cycle()
        if command == "check":
            print(self.application(command))
            if not self.node.is_file() or not self.wrangler.is_file():
                raise ValueError("Run install.sh to install the publishing toolchain")
            print("Application and deployment configuration valid")
        elif command == "backup":
            self.backup(force=force)
        elif command == "hold":
            atomic_json(self.state / "publication-hold.json", {"reason": "manual hold", "timestamp": time.time()})
        elif command == "resume":
            if read_json(self.state / "transition.json"):
                raise ValueError("Run reconcile before releasing a publication hold")
            (self.state / "publication-hold.json").unlink(missing_ok=True)
            sync_directory(self.state)
        else:
            {"publish": self.publish, "routes": self.routes, "schedule": self.schedule,
             "rollback": self.rollback, "reconcile": self.reconcile}[command]()
        return {"ok": True}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "deployment.toml"))
    parser.add_argument("command", choices=["check", "run", "publish", "backup", "routes", "schedule", "status", "rollback", "hold", "resume", "reconcile"])
    parser.add_argument("--force", action="store_true", help="Create a backup even when its interval has not elapsed")
    args = parser.parse_args()
    if args.force and args.command != "backup":
        parser.error("--force is supported only for backup")
    publisher = None
    console = logging.StreamHandler()
    LOG.setLevel(logging.INFO)
    LOG.addHandler(console)
    try:
        publisher = Publisher(args.config)
        if args.command == "status":
            publisher.status()
            return 0
        with lock(publisher.state / "run.lock", nonblocking=True), job_log(publisher.state / "run.log"):
            # Only the job-lock owner opens or rotates the shared log file.
            try:
                result = publisher.execute(args.command, force=args.force)
            except Exception as exc:
                message = publisher.clean_error(exc)
                LOG.error("%s", message)
                result = {"ok": False, "error": message}
            atomic_json(publisher.state / "last-run.json", {**result, "command": args.command, "at": datetime.now(timezone.utc).isoformat()})
            return 0 if result["ok"] else 1
    except BusyLock:
        if args.command == "run":
            LOG.info("Another Onion Flash deployment is running; skipped")
            return 0
        LOG.error("Another Onion Flash deployment is running; command not performed; retry after completion")
        return 75
    except Exception as exc:
        LOG.error("%s", publisher.clean_error(exc) if publisher else type(exc).__name__ + ": " + str(exc))
        return 1
    finally:
        if publisher is not None:
            publisher.close()
        LOG.removeHandler(console)
        console.close()


if __name__ == "__main__":
    sys.exit(main())
