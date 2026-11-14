#!/usr/bin/env python3
"""Small, explicit publisher. No runtime Git pulls and no shell-evaluated config."""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import errno
import fcntl
import gzip
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random
import re
import secrets
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from onionflash.publication import public_files, public_path_allowed
from onionflash.store import backup_database, get_meta

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("onionflash.deploy")
SAFE_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]*\Z")
API_BASE = "https://api.cloudflare.com/client/v4"
MAX_FILES, MAX_FILE_BYTES, MAX_TOTAL_BYTES = 10000, 10 * 1024 * 1024, 100 * 1024 * 1024
COMMAND_TIMEOUT_SECONDS = 240
VERIFY_PATHS = ("index.html", "feed.xml", "feed.json")
VERIFY_ATTEMPTS = 8
ARTICLE = re.compile(r"news/[A-Za-z0-9_-]{1,64}/index\.html")
NETWORK = {"timeout_seconds": 30, "attempts": 4, "backoff_seconds": 2, "backoff_cap_seconds": 30,
           "jitter_seconds": 1, "max_response_bytes": MAX_FILE_BYTES}
ACCOUNT_ID, API_TOKEN, ZONE_TOKEN, ZONE_ID = ("CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN",
                                              "CLOUDFLARE_ZONE_TOKEN", "CLOUDFLARE_ZONE_ID")
SPACES_KEY, SPACES_SECRET = "DO_SPACES_KEY", "DO_SPACES_SECRET"
ARCHIVE_KEYS = {"enabled", "endpoint_url", "region", "bucket", "public_prefix", "private_prefix",
                "public_origin", "access_key_env", "secret_key_env"}


def clean_https_url(value, label):
    parsed = urlsplit(value) if isinstance(value, str) else None
    if not parsed or parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"{label} must be a clean HTTPS URL")
    return parsed


def validate_s3_location(cfg, label):
    clean_https_url(cfg.get("endpoint_url"), f"{label} endpoint")
    bucket = cfg.get("bucket")
    if not isinstance(bucket, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError(f"Invalid {label} bucket")
    if not isinstance(cfg.get("region"), str) or not cfg["region"].strip():
        raise ValueError(f"{label} region must be a non-empty string")


def require_uuid(value, message="Invalid version identity"):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f-]{36}\Z", value):
        raise ValueError(message)
    return value


def require_cf_id(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}\Z", value):
        raise ValueError(f"Invalid {label}")
    return value


def require_env_name(value, message="Invalid credential-file assignment"):
    if not isinstance(value, str) or not ENV_NAME.fullmatch(value):
        raise ValueError(message)
    return value


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


CREDENTIAL_NAMES = set()


def credential_values(names=()):
    """Values of credential-like environment variables, long enough to redact safely."""
    found = {k for k in os.environ if any(x in k.upper() for x in ("TOKEN", "SECRET", "API_KEY", "SPACES_KEY"))}
    found |= CREDENTIAL_NAMES | set(names)
    return [os.environ[name] for name in found if len(os.environ.get(name, "")) >= 8]


def redact(value, names=()):
    result = str(value)
    for secret in credential_values(names):
        result = result.replace(secret, "[redacted]")
    return result[-2500:]


def _clean_error(publisher, error):
    clean = getattr(publisher, "clean_error", None)
    return clean(error) if clean else redact(f"{type(error).__name__}: {error}")


def load_env(path):
    """KEY=value lines with shell-style quoting and no expansion; the existing environment wins."""
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
        if not separator:
            raise ValueError("Invalid credential-file assignment")
        require_env_name(key)
        tokens = shlex.split(raw)
        if len(tokens) > 1:
            raise ValueError("Quote values containing spaces; put comments on their own line")
        CREDENTIAL_NAMES.add(key)
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
def open_db(path, *, shared=False):
    target = Path(path).resolve(strict=True)
    uri = target.as_uri() + ("?mode=ro" if shared else "?mode=rw")
    with lock(f"{Path(path)}.lock", shared=shared), closing(sqlite3.connect(uri, uri=True)) as db:
        db.row_factory = sqlite3.Row
        yield db


def s3_client(publisher, cfg, access_key_env, secret_key_env, *, max_attempts=1, checksums=False):
    import boto3
    from botocore.config import Config
    extra = {"request_checksum_calculation": "when_required", "response_checksum_validation": "when_required"} if checksums else {}
    return boto3.client(
        "s3", endpoint_url=cfg["endpoint_url"], region_name=cfg["region"],
        aws_access_key_id=publisher.secret(access_key_env),
        aws_secret_access_key=publisher.secret(secret_key_env),
        config=Config(connect_timeout=publisher.net["timeout_seconds"], read_timeout=publisher.net["timeout_seconds"],
                      retries={"mode": "standard", "total_max_attempts": int(max_attempts)}, **extra),
    )


def verify_private_object(publisher, endpoint_url, bucket, key, label, delete=None):
    problem = publisher.unsigned_read_problem(f"{endpoint_url.rstrip('/')}/{bucket}/{quote(key)}")
    if not problem:
        return
    if delete is not None:
        try:
            delete()
        except Exception as cleanup:
            raise ValueError(f"{label} {key} {problem}, and deleting it failed: {_clean_error(publisher, cleanup)}") from cleanup
    raise ValueError(f"{label} {key} {problem}")


def upload_private_stream(publisher, client, endpoint_url, bucket, key, stream, label):
    delete = lambda: client.delete_object(Bucket=bucket, Key=key)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=stream, ACL="private", ContentType="application/gzip")
    except Exception as failure:
        try:
            delete()
        except Exception as cleanup:
            raise RuntimeError(f"{label} upload failed and {key} could not be removed: "
                               f"{_clean_error(publisher, failure)}; {_clean_error(publisher, cleanup)}") from failure
        raise
    verify_private_object(publisher, endpoint_url, bucket, key, f"The new {label.lower()}", delete=delete)


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
    def __init__(self, config_path, *, offline=False):
        self.config_path = Path(config_path).resolve()
        self.cfg = tomllib.loads(self.config_path.read_text())
        self.directory = self.config_path.parent
        load_env(self.path(self.cfg.get("env_file", "secrets.env")))
        for section in ("archive", "retention", "spaces"):
            if type(self.cfg.get(section, {}).get("enabled", False)) is not bool:
                raise ValueError(section + ".enabled must be a boolean")
        if type(self.cfg.get("spaces", {}).get("required", False)) is not bool:
            raise ValueError("spaces.required must be a boolean")
        if unknown := set(self.cfg.get("archive", {})) - ARCHIVE_KEYS:
            raise ValueError("Unknown archive settings: " + ", ".join(sorted(unknown)))
        if unknown := set(self.cfg.get("retention", {})) - {"enabled"}:
            raise ValueError("Unknown retention settings: " + ", ".join(sorted(unknown)))
        if self.cfg.get("retention", {}).get("enabled") and not self.cfg.get("archive", {}).get("enabled"):
            raise ValueError("Private retention requires configured permanent storage")
        self.state = self.path(self.cfg.get("state_dir", "state"))
        self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.state, 0o700)
        self.app_config = self.path(self.cfg["app_config"])
        from onionflash.config import load_config
        self.app = load_config(self.app_config)
        self.public = Path(self.app["storage"]["output_dir"])
        self.database = Path(self.app["storage"]["database"])
        self.base_url = self.app["site"]["base_url"]
        parsed = clean_https_url(self.base_url, "Public base_url")
        if not re.fullmatch(r"(?:/[a-zA-Z0-9_-]+)*/", parsed.path):
            raise ValueError("Use a plain path for the public site")
        self.base_path = parsed.path.rstrip("/")
        self.host = parsed.netloc
        self.cf = self.cfg["cloudflare"]
        if not SAFE_ID.fullmatch(self.cf["worker_name"]):
            raise ValueError("Invalid Worker name")
        self.net = dict(NETWORK)
        self.account = self.token = ""
        if not offline:
            self._load_cloudflare()
        self.api_base = API_BASE
        self._archive = None
        self.node = ROOT / "node_modules/node/bin/node"
        self.wrangler = ROOT / "node_modules/wrangler/bin/wrangler.js"

    def _load_cloudflare(self):
        if not self.account or not self.token:
            self.account = require_cf_id(self.secret(ACCOUNT_ID), "Cloudflare account ID")
            self.token = self.secret(API_TOKEN)

    def path(self, value):
        # Preserve the final executable symlink so .venv/bin/python keeps its venv.
        return Path(os.path.abspath(self.directory / value))

    @staticmethod
    def secret(name):
        value = os.environ.get(name, "")
        if not value:
            raise ValueError("Missing environment setting: " + name)
        return value

    def clean_error(self, value):
        return redact(value, self.private_names())

    def private_names(self):
        """Environment variables holding credentials for Cloudflare, Spaces or the LLM."""
        archive = self.cfg.get("archive", {})
        names = {API_TOKEN, ZONE_TOKEN, SPACES_KEY, SPACES_SECRET, self.app["llm"]["api_key_env"],
                 archive.get("access_key_env", ""), archive.get("secret_key_env", "")}
        return names - {""}

    def secret_values(self):
        return credential_values(self.private_names())

    def delay(self, attempt):
        """Storage retries have their own bounded backoff, independent of collection."""
        delay = min(self.net["backoff_cap_seconds"], self.net["backoff_seconds"] * 2 ** min(attempt, 10)
                    + random.uniform(0, self.net["jitter_seconds"]))
        time.sleep(delay)

    def request(self, url, method="GET", data=None, token=None):
        """One bounded urllib request; HEAD reads no body."""
        headers = {"User-Agent": "OnionFlashPublisher", "Accept-Encoding": "identity"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(url, None if data is None else json.dumps(data).encode(), headers, method=method)
        if token:
            request.add_unredirected_header("Authorization", "Bearer " + token)
        with urlopen(request, timeout=self.net["timeout_seconds"]) as response:
            body = b"" if method.upper() == "HEAD" else response.read(self.net["max_response_bytes"] + 1)
        if len(body) > self.net["max_response_bytes"]:
            raise ValueError("Response exceeds the size limit: " + url)
        return body

    def unsigned_read_problem(self, url, public=False):
        """An anonymous request is the real access path: only a 403 proves an object private,
        and only a success proves it public, whatever ACLs or bucket policies say."""
        for attempt in range(self.net["attempts"]):
            if attempt:
                self.delay(attempt - 1)
            try:
                self.request(url, "HEAD")
                return None if public else "was publicly readable"
            except HTTPError as exc:
                if exc.code == 403:
                    return "is not publicly readable" if public else None
                problem = f"got the unexpected anonymous response {exc.code}"
                if exc.code not in (408, 429) and exc.code < 500:
                    return problem
            except OSError as exc:
                problem = f"could not be checked {'for public access' if public else 'for privacy'} ({type(exc).__name__})"
        return problem

    @property
    def archive(self):
        if not self.cfg.get("archive", {}).get("enabled", False):
            return None
        if self._archive is None:
            from archive_store import ArchiveStore
            self._archive = ArchiveStore(self)
        return self._archive

    def close(self):
        if self._archive is not None and (close := getattr(self._archive.client, "close", None)):
            close()

    def api(self, suffix, method="GET", data=None, zone=False):
        if not zone:
            self._load_cloudflare()
        token = self.secret(ZONE_TOKEN) if zone else self.token
        response = json.loads(self.request(self.api_base + suffix, method, data, token))
        if not response.get("success"):
            raise RuntimeError("Cloudflare API rejected request: " + self.clean_error(response.get("errors")))
        return response.get("result")

    def record_first_version(self):
        version = self.current_version()
        if version:
            atomic_json(self.state / "publication.json", {"version_id": version})
        return version

    def current_version(self):
        self._load_cloudflare()
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
        return require_uuid(versions[0].get("version_id"), "Cloudflare returned an invalid version identity")

    def command(self, arguments, metadata=None):
        self._load_cloudflare()
        private = self.private_names()
        env = {k: v for k, v in os.environ.items() if k not in private}
        env.update(CLOUDFLARE_ACCOUNT_ID=self.account, CLOUDFLARE_API_TOKEN=self.token,
                   WRANGLER_SEND_METRICS="false", WRANGLER_LOG_SANITIZE="true",
                   WRANGLER_LOG_PATH=str(self.state / "wrangler-logs"), CI="true")
        if metadata:
            env["WRANGLER_OUTPUT_FILE_PATH"] = str(metadata)
        result = subprocess.run([str(self.node), str(self.wrangler), *arguments], cwd=ROOT, env=env,
                                capture_output=True, text=True, timeout=COMMAND_TIMEOUT_SECONDS)
        if result.returncode:
            raise RuntimeError(self.clean_error(result.stdout + result.stderr))
        return result.stdout

    def worker_vars(self, archive=None):
        return {"BASE_PATH": self.base_path, **(self.archive.worker_vars(archive["manifest_sha256"]) if archive else {})}

    def publication_identity(self, manifest, archive=None):
        value = {"manifest": manifest, "worker": hashlib.sha256((ROOT / "worker.mjs").read_bytes()).hexdigest(),
                 "vars": self.worker_vars(archive), "compatibility_date": self.cf["compatibility_date"]}
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    def _write_wrangler_config(self, filename, **extra):
        self._load_cloudflare()
        target = self.state / filename
        atomic_json(target, {"name": self.cf["worker_name"], "account_id": self.account,
                             "compatibility_date": self.cf["compatibility_date"], **extra})
        return target

    def make_config(self, directory, archive=None):
        return self._write_wrangler_config(
            "wrangler.json", workers_dev=True, preview_urls=True, main=str(ROOT / "worker.mjs"),
            assets={"directory": str(directory), "binding": "ASSETS",
                    "html_handling": "auto-trailing-slash", "not_found_handling": "none"},
            vars=self.worker_vars(archive),
        )

    def control_config(self):
        return self._write_wrangler_config("wrangler-control.json")

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
        (target / "_headers").replace(assets / "_headers")
        return assets

    def validate(self, directory, *, preverified=False):
        files, total = {}, 0
        secrets_bytes = [] if preverified else [value.encode() for value in self.secret_values()]
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
            if size > MAX_FILE_BYTES or total > MAX_TOTAL_BYTES:
                raise ValueError("Public output exceeds size budget")
            if preverified:
                files[relative] = ""
            else:
                body = path.read_bytes()
                if any(secret in body for secret in secrets_bytes):
                    raise ValueError("Credential detected in public output")
                files[relative] = hashlib.sha256(body).hexdigest()
        if len(files) > MAX_FILES:
            raise ValueError("Public output exceeds file count budget")
        for relative in public_files():
            if relative not in files:
                raise ValueError("Missing required public file: " + relative)
        import xml.etree.ElementTree as ET
        if ET.parse(directory / "feed.xml").getroot().tag != "rss":
            raise ValueError("Expected RSS feed")
        json.loads((directory / "feed.json").read_text())
        return files

    def verify(self, base_url, manifest, *, archive=None):
        paths = list(VERIFY_PATHS)
        if archive:  # One article also proves the Worker's route to permanent storage.
            paths += sorted(path for path in manifest if ARTICLE.fullmatch(path))[:1]
        for relative in paths:
            for attempt in range(VERIFY_ATTEMPTS):
                try:
                    actual = hashlib.sha256(self.request(base_url.rstrip("/") + "/" + relative)).hexdigest()
                    if actual != manifest[relative]:
                        raise ValueError("Published hash mismatch: " + relative)
                    break
                except (ValueError, OSError) as exc:
                    if isinstance(exc, HTTPError) and exc.code != 404 and exc.code < 500:
                        raise
                    if attempt + 1 == VERIFY_ATTEMPTS:
                        raise
                    self.delay(attempt)

    def activate(self, version, config):
        require_uuid(version)
        try:
            self.command(["versions", "deploy", version + "@100%", "--yes", "--config", str(config)])
        except (RuntimeError, subprocess.TimeoutExpired):
            if self.current_version() != version:
                raise

    def origin_url(self):
        self._load_cloudflare()
        subdomain = self.api(f"/accounts/{self.account}/workers/subdomain")["subdomain"]
        return f"https://{self.cf['worker_name']}.{subdomain}.workers.dev" + self.base_path

    def forget_content(self, receipt):
        """Clear recorded content hash before changing the live version."""
        receipt.pop("content_hash", None)
        atomic_json(self.state / "publication.json", receipt)

    def rollback(self):
        """Hold publication and reactivate the previous version."""
        receipt = read_json(self.state / "publication.json", {})
        previous = receipt.get("previous_version_id")
        if not previous:
            raise ValueError("No previous publication is recorded; use hold to pause publication")
        require_uuid(previous, "Invalid previous version identity")
        expected = receipt.get("version_id")
        if self.current_version() != expected:
            raise ValueError("Active Worker version differs from the publication receipt; rollback not performed")
        restored = read_json(self.state / "receipts" / (previous + ".json"))
        if restored is not None and restored.get("version_id") != previous:
            raise ValueError("Previous publication receipt has the wrong version")
        config = self.control_config()
        atomic_json(self.state / "publication-hold.json", {"reason": "rollback requested", "from": expected, "to": previous, "timestamp": time.time()})
        self.forget_content(receipt)
        self.activate(previous, config)
        restored = dict(restored or {"version_id": previous})
        restored.pop("content_hash", None)
        restored.update(rolled_back_from=receipt["version_id"], rolled_back_at=datetime.now(timezone.utc).isoformat())
        atomic_json(self.state / "publication.json", restored)

    def publication_readiness(self, manifest=None, *, initial_archive=False):
        """Keep incomplete builds, first migrations and local restores off the live site."""
        if not self.database.is_file():
            raise ValueError("Run the core build before publishing")
        with open_db(self.database, shared=True) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='public_artifacts'").fetchone():
                raise ValueError("Run the upgraded core build before publishing this database")
            if get_meta(db, "publication_build_pending") == "1":
                raise ValueError("Publication build is incomplete; existing site retained")
            if initial_archive and db.execute("SELECT 1 FROM public_render_queue WHERE last_error IS NULL LIMIT 1").fetchone():
                raise ValueError("Initial permanent-storage migration still has articles pending")
            ledger = dict(db.execute("SELECT path,sha256 FROM public_artifacts"))
        if manifest is not None:
            for path, sha in ledger.items():
                if manifest.get(path) != sha:
                    raise ValueError("Local publication snapshot is incomplete: " + path)
            if extra := sorted(set(manifest) - set(ledger)):
                raise ValueError("Local publication snapshot has untracked file: " + extra[0])
        return ledger

    @staticmethod
    def _prune_untracked(stage, ledger):
        if not stage.is_dir():
            return
        for path in sorted(stage.rglob("*"), reverse=True):
            if path.is_symlink():
                continue
            if path.is_file():
                relative = path.relative_to(stage).as_posix()
                if relative not in ledger and public_path_allowed(relative):
                    path.unlink()
            elif path.is_dir():
                try:
                    path.rmdir()
                except OSError:
                    pass

    def publish(self):
        """Prepare durable objects, check preview, activate, then verify or restore."""
        if (self.state / "publication-hold.json").exists():
            raise ValueError("Publication is on hold; inspect status and use the explicit resume command")
        if not self.public.is_dir():
            raise ValueError("Build the public output first")
        ledger = self.publication_readiness()
        previous = read_json(self.state / "publication.json", {})
        archive_info = None
        with tempfile.TemporaryDirectory(prefix="release-", dir=self.state) as temporary:
            stage = Path(temporary) / "public"
            if self.archive:
                synchronized = self.archive.sync()
                atomic_json(self.state / "archive-sync.json", synchronized)
                if not synchronized["ready"]:
                    raise ValueError("Permanent storage is catching up; existing publication retained")
                generation = synchronized["generation"]
                if not previous.get("archive"):
                    self.publication_readiness(initial_archive=True)
                manifest = self.archive.snapshot(stage, generation)
                self.validate(stage, preverified=True)
                articles = {path for path in manifest if ARTICLE.fullmatch(path)}
                archive_info = {"manifest_sha256": synchronized["manifest_sha256"],
                                "generation": generation, "origin": self.archive.cfg["public_origin"]}
            else:
                manifest = ledger
                if previous.get("content_hash") != self.publication_identity(ledger):
                    with lock(str(self.database) + ".lock", shared=True):
                        ledger = self.publication_readiness()
                        shutil.copytree(self.public, stage, symlinks=True)
                    self._prune_untracked(stage, ledger)
                    manifest = self.validate(stage)
                    ledger = self.publication_readiness(manifest)
                articles = {path for path in ledger if ARTICLE.fullmatch(path)}
            permalinks = self.state / "permalinks.json"
            try:
                recorded = read_json(permalinks, [])
            except ValueError:
                recorded = None
            if not isinstance(recorded, list) or not all(isinstance(path, str) and ARTICLE.fullmatch(path) for path in recorded):
                raise ValueError(f"{permalinks} is damaged; restore it from a copy")
            published = set(recorded) | {path for path in [*previous.get("manifest", {}), *previous.get("articles", [])]
                                         if ARTICLE.fullmatch(path)}
            if published - articles:
                raise ValueError("A previously published article is missing from this publication: restore a "
                                 "database that contains it; to withdraw a story, retract it")
            if not permalinks.exists() or set(recorded) != published:
                atomic_json(permalinks, sorted(published))
            identity = self.publication_identity(manifest, archive_info)
            before = self.current_version()
            if before != previous.get("version_id"):
                raise ValueError("Active Worker version differs from the publication receipt; inspect before "
                                 f"publishing (live {before}, recorded {previous.get('version_id')})")
            if previous.get("content_hash") == identity:
                LOG.info("Public output unchanged; skipping Cloudflare upload")
                return previous
            config = self.make_config(self.prepare_assets(stage), archive=archive_info)
            live = self.base_url if (self.state / "routes.json").exists() else self.origin_url()
            provenance = {"app_version": self.app_version(),
                          "app_commit": self.git_commit(self.path(self.cfg.get("app_source", "../onionflash"))),
                          "deployment_commit": self.git_commit(ROOT)}
            if previous.get("version_id") == before and previous.get("manifest"):
                prior_path = self.state / "receipts" / (before + ".json")
                if not prior_path.exists():
                    atomic_json(prior_path, previous)
            self.forget_content(previous)
            if before is None:
                try:
                    self.command(["deploy", "--config", str(config)])
                except BaseException:
                    try:
                        self.record_first_version()
                    except Exception as lookup:
                        LOG.error("Could not read the Worker version after a failed deploy: %s", self.clean_error(lookup))
                    raise
                version = self.record_first_version()
                if not version:
                    raise RuntimeError("Cloudflare did not report the deployed version")
            else:
                metadata = Path(temporary) / "upload.ndjson"
                self.command(["versions", "upload", "--config", str(config), "--preview-alias", "candidate"], metadata)
                lines = metadata.read_text().splitlines() if metadata.exists() else []
                uploaded = next((x for x in (json.loads(line) for line in lines if line.strip()) if x.get("type") == "version-upload"), {})
                version, preview = uploaded.get("version_id"), uploaded.get("preview_url")
                if not version or not preview:
                    raise RuntimeError("Wrangler did not report the uploaded version and its preview URL")
                if not clean_https_url(preview, "Cloudflare preview URL").hostname.endswith(".workers.dev"):
                    raise ValueError("Invalid Cloudflare preview URL")
                self.verify(preview.rstrip("/") + self.base_path, manifest, archive=archive_info)
                if self.current_version() != before:
                    raise ValueError("Active Worker version changed during preview; external version retained")
                self.activate(version, self.control_config())
            try:
                self.verify(live, manifest, archive=archive_info)
            except Exception as failure:
                if before is not None:
                    if self.current_version() != version:
                        raise RuntimeError("Live check failed and the Worker changed outside this publication; external version retained") from failure
                    try:
                        self.activate(before, self.control_config())
                    except Exception as restore:
                        raise RuntimeError(f"Live check failed ({failure}) and restoring {before} failed: {restore}") from failure
                raise
            atomic_json(permalinks, sorted(articles))
            receipt = {"version_id": version, "previous_version_id": before,
                       "content_hash": identity, "manifest": manifest,
                       "published_at": datetime.now(timezone.utc).isoformat(), **provenance}
            if archive_info:
                receipt["archive"] = archive_info
            atomic_json(self.state / "receipts" / (version + ".json"), receipt)
            atomic_json(self.state / "publication.json", receipt)
            if not archive_info:
                for old in (self.state / "receipts").glob("*.json"):
                    if old.stem not in {version, before}:
                        try:
                            saved = read_json(old, {})
                        except ValueError:
                            saved = {}
                        if not isinstance(saved, dict) or "archive" not in saved:
                            old.unlink()
            LOG.info("Published version %s (%s files) at %s", version, len(manifest), live)
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
        if "manifest" not in receipt:
            raise ValueError("The publication receipt lists no files to check; publish once, then attach routes")
        self.verify(self.origin_url(), receipt["manifest"], archive=receipt.get("archive"))
        zone = require_cf_id(self.secret(ZONE_ID), "zone ID")
        existing = self.api(f"/zones/{zone}/workers/routes", zone=True)
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
            self.verify(self.base_url, receipt["manifest"], archive=receipt.get("archive"))
        except Exception:
            for route_id in reversed(created):
                self.api(f"/zones/{zone}/workers/routes/{route_id}", "DELETE", zone=True)
            raise
        atomic_json(self.state / "routes.json", {"patterns": patterns, "verified_at": datetime.now(timezone.utc).isoformat()})
        LOG.info("Verified public website and RSS at %s", self.base_url)

    def validate_spaces(self):
        cfg = self.cfg.get("spaces", {})
        for key in ("retention_days", "backup_interval_seconds"):
            if type(cfg.get(key)) is not int or cfg[key] < 1:
                raise ValueError("Spaces " + key + " must be a positive integer")
        validate_s3_location(cfg, "Spaces")
        prefix = cfg.get("prefix")
        if not isinstance(prefix, str) or not re.fullmatch(r"[a-zA-Z0-9_-]+/backups/", prefix):
            raise ValueError("Backup prefix must be a project-specific name/backups/")
        self.secret(SPACES_KEY)
        self.secret(SPACES_SECRET)
        return cfg

    def backup(self, force=False):
        cfg = self.cfg.get("spaces", {})
        if not cfg.get("enabled"):
            return
        self.validate_spaces()
        prefix = cfg["prefix"]
        destination = {key: cfg[key] for key in ("endpoint_url", "bucket", "prefix")}
        marker = self.state / "backup.json"
        previous = read_json(marker, {})
        age = time.time() - previous.get("timestamp", 0)
        if not force and previous.get("destination") == destination and 0 <= age < cfg["backup_interval_seconds"]:
            return
        if not self.database.is_file():
            raise ValueError("No application database to back up")
        client = s3_client(self, cfg, SPACES_KEY, SPACES_SECRET, max_attempts=self.net["attempts"])
        key = prefix + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4) + ".sqlite3.gz"
        with tempfile.TemporaryDirectory(prefix="backup-", dir=self.state) as temporary:
            snapshot, compressed = Path(temporary) / "onionflash.sqlite3", Path(temporary) / "onionflash.sqlite3.gz"
            with lock(str(self.database) + ".lock", shared=True):
                backup_database(self.database, snapshot)
            with snapshot.open("rb") as source, gzip.open(compressed, "wb") as target:
                shutil.copyfileobj(source, target)
            with compressed.open("rb") as body:
                upload_private_stream(self, client, cfg["endpoint_url"], cfg["bucket"], key, body, "Backup")
        cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])
        pattern = re.escape(prefix) + r"\d{8}T\d{6}Z(?:(?:-[0-9a-f]{8})?\.sqlite3\.gz|-[0-9a-f]{12}(?:-[0-9a-f]{32})?\.tar\.gz)"
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=cfg["bucket"], Prefix=prefix):
            for item in page.get("Contents", []):
                if item["Key"] != key and item["LastModified"] < cutoff and re.fullmatch(pattern, item["Key"]):
                    client.delete_object(Bucket=cfg["bucket"], Key=item["Key"])
        atomic_json(marker, {"timestamp": time.time(), "key": key, "destination": destination})
        LOG.info("Private Spaces backup %s", key)

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
        if "\n" in command or "\r" in command:
            raise ValueError("Scheduled command paths must not contain newlines")
        block = f"{begin}\n*/{interval} * * * * {command.replace('%', r'\%')} 2>&1 | logger -t onionflash\n{end}\n"
        subprocess.run(["crontab", "-"], input=current.rstrip() + "\n" + block, text=True, check=True)
        LOG.info("Installed a %s-minute cron job", interval)

    def application(self, *arguments):
        """Run one app command and return its output; failures carry the app's error."""
        budget = self.app["collection"]["run_budget_seconds"]
        private = self.private_names() - {self.app["llm"]["api_key_env"]}
        env = {k: v for k, v in os.environ.items() if k not in private}
        result = subprocess.run([str(self.path(self.cfg["app_python"])), "-m", "onionflash", *arguments,
                                 "--config", str(self.app_config)], capture_output=True, text=True,
                                timeout=budget + 300, env=env)
        if result.returncode:
            detail = result.stderr.strip() or f"exited with status {result.returncode}"
            raise RuntimeError(self.clean_error(detail)[-1000:])
        return result.stdout.strip()

    def collect(self, result):
        report = self.application("run")
        LOG.info("run: %s", self.clean_error(report.splitlines()[-1] if report else "no report"))
        try:
            result["report"] = json.loads(report.splitlines()[-1])
        except (ValueError, IndexError):
            result["report"] = None

    def retain_private_history(self):
        import retention
        previous = read_json(self.state / "retention.json", {})
        age = time.time() - previous.get("timestamp", 0)
        if (previous.get("batch_complete") and previous.get("destination") == retention.destination(self.archive)
                and 0 <= age < retention.INTERVAL_SECONDS):
            return
        with self.archive.budget(retention.RUN_BUDGET_SECONDS):
            result = retention.archive_evidence(self.archive)
        atomic_json(self.state / "retention.json", {**result, "timestamp": time.time()})

    def weekly_summary(self):
        year, week, _ = datetime.now(timezone.utc).isocalendar()
        target = self.state / "weekly" / f"{year}-W{week:02d}.md"
        if not target.exists():
            summary = json.loads(self.application("decisions", "summary", "--since", "7d"))
            atomic_json(target.with_name("latest.md"), summary)
            atomic_json(target, summary)

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
                (LOG.error if required else LOG.warning)("%s", message)
                return False
        if attempt("collection", lambda: self.collect(result)):
            if (self.state / "publication-hold.json").exists():
                result["phases"]["publication"] = "held"
            else:
                attempt("publication", self.publish)
        else:
            result["phases"]["publication"] = "skipped"
        attempt("backup", self.backup, required=self.cfg.get("spaces", {}).get("required", False))
        if self.cfg.get("retention", {}).get("enabled"):
            attempt("retention", self.retain_private_history, required=False)
        attempt("weekly_summary", self.weekly_summary, required=False)
        result.update(ok=not result["errors"], degraded=bool(result["warnings"]))
        return result

    def status(self):
        try:
            with lock(self.state / "run.lock", shared=True, nonblocking=True):
                running = False
        except BusyLock:
            running = True
        report = {"running": running}
        for name in ("publication.json", "publication-hold.json", "backup.json", "archive-sync.json", "retention.json", "last-run.json"):
            data = read_json(self.state / name)
            if data is not None:
                data.pop("manifest", None)
                data.pop("articles", None)
                report[name] = data
        print(json.dumps(report, indent=2))

    def execute(self, command, force=False):
        if command == "run":
            return self.run_cycle()
        if command == "check":
            print(self.application(command))
            if not self.node.is_file() or not self.wrangler.is_file():
                raise ValueError("Run install.sh to install the publishing toolchain")
            if self.cfg.get("spaces", {}).get("enabled"):
                self.validate_spaces()
            if self.cfg.get("archive", {}).get("enabled"):
                self.archive
            print("Application and deployment configuration valid")
        elif command == "backup":
            if not self.cfg.get("spaces", {}).get("enabled"):
                raise ValueError("Spaces backups are disabled; set spaces.enabled = true or copy the database yourself")
            self.backup(force=force)
        elif command == "hold":
            atomic_json(self.state / "publication-hold.json", {"reason": "manual hold", "timestamp": time.time()})
        elif command == "resume":
            (self.state / "publication-hold.json").unlink(missing_ok=True)
            sync_directory(self.state)
        else:
            {"publish": self.publish, "routes": self.routes, "schedule": self.schedule,
             "rollback": self.rollback}[command]()
        return {"ok": True}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "deployment.toml"))
    parser.add_argument("command", choices=["check", "run", "publish", "backup", "routes", "schedule", "status", "rollback", "hold", "resume"])
    parser.add_argument("--force", action="store_true", help="Create a backup even when its interval has not elapsed")
    args = parser.parse_args()
    if args.force and args.command != "backup":
        parser.error("--force is supported only for backup")
    publisher = None
    console = logging.StreamHandler()
    LOG.setLevel(logging.INFO)
    LOG.addHandler(console)
    try:
        publisher = Publisher(args.config, offline=True) if args.command in {"status", "hold", "resume"} else Publisher(args.config)
        if args.command == "status":
            publisher.status()
            return 0
        with lock(publisher.state / "run.lock", nonblocking=True), job_log(publisher.state / "run.log"):
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
        LOG.error("%s", _clean_error(publisher, exc))
        return 1
    finally:
        if publisher is not None:
            publisher.close()
        LOG.removeHandler(console)
        console.close()


if __name__ == "__main__":
    sys.exit(main())
