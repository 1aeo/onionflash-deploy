#!/usr/bin/env python3
"""Small, explicit publisher. No runtime Git pulls and no shell-evaluated config."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
import fcntl
from email.utils import parsedate_to_datetime
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urljoin
from urllib.request import Request, build_opener, HTTPRedirectHandler

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
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


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


@contextmanager
def lock(path, shared=False, nonblocking=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a+") as stream:
        flags = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        if nonblocking:
            flags |= fcntl.LOCK_NB
        fcntl.flock(stream, flags)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


class SameOriginRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        if urlsplit(newurl).netloc != urlsplit(request.full_url).netloc or urlsplit(newurl).scheme != "https":
            raise ValueError("Cross-origin or insecure redirect refused")
        return super().redirect_request(request, fp, code, msg, headers, newurl)


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
            if not isinstance(self.net[key], (int, float)) or self.net[key] < 0:
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
        self.opener = build_opener(SameOriginRedirect())
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

    def delay(self, attempt, retry_after=""):
        delay = self.net["backoff_seconds"] * 2 ** attempt
        if retry_after:
            try:
                advised = float(retry_after) if retry_after.isdigit() else (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
                delay = max(delay, advised)
            except (TypeError, ValueError, OverflowError):
                pass
        delay += random.uniform(0, self.net["jitter_seconds"])
        time.sleep(min(self.net["backoff_cap_seconds"], max(0, delay)))

    def retry(self, function):
        for attempt in range(int(self.net["attempts"])):
            try:
                return function()
            except HTTPError as exc:
                if exc.code not in {408, 425, 429, 500, 502, 503, 504}:
                    raise
                retry_after = exc.headers.get("Retry-After", "")
            except (URLError, TimeoutError, ConnectionError):
                retry_after = ""
            if attempt + 1 == self.net["attempts"]:
                raise RuntimeError("Dependency unavailable after bounded retries")
            self.delay(attempt, retry_after)

    def request(self, url, method="GET", data=None, token=None):
        headers = {"User-Agent": "OnionFlashPublisher/0.1", "Accept-Encoding": "identity"}
        if token:
            headers["Authorization"] = "Bearer " + token
        if data is not None:
            data = json.dumps(data).encode()
            headers["Content-Type"] = "application/json"
        def perform():
            with self.opener.open(Request(url, headers=headers, data=data, method=method), timeout=self.net["timeout_seconds"]) as response:
                body = response.read(int(self.net["max_response_bytes"]) + 1)
                if len(body) > self.net["max_response_bytes"]:
                    raise ValueError("Response exceeds byte limit")
                return body
        return self.retry(perform)

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
                   WRANGLER_LOG_PATH=str(self.state / "wrangler.log"), CI="true")
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
            config["assets"] = {"directory": str(directory), "binding": "ASSETS", "base_path": self.base_path,
                                "html_handling": "auto-trailing-slash", "not_found_handling": "none"}
            config["vars"] = {"BASE_PATH": self.base_path}
        target = self.state / ("wrangler-bootstrap.json" if bootstrap else "wrangler.json")
        atomic_json(target, config)
        return target

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

    def publish(self):
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
            previous = json.loads(previous_path.read_text()) if previous_path.exists() else {}
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
            config = self.make_config(stage)
            metadata = Path(temporary) / "upload.ndjson"
            self.command(["versions", "upload", "--config", str(config), "--preview-alias", "candidate"], metadata)
            records = [json.loads(line) for line in metadata.read_text().splitlines() if line.strip()]
            uploaded = next(x for x in records if x.get("type") == "version-upload")
            version, preview = uploaded["version_id"], uploaded["preview_url"]
            parsed = urlsplit(preview)
            if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".workers.dev") or parsed.username:
                raise ValueError("Invalid Cloudflare preview URL")
            self.verify(preview.rstrip("/") + self.base_path, manifest)
            self.activate(version, config)
            try:
                # A workers.dev origin remains available before the hostname route is installed.
                account_subdomain = self.api(f"/accounts/{self.account}/workers/subdomain")["subdomain"]
                origin = f"https://{self.cf['worker_name']}.{account_subdomain}.workers.dev" + self.base_path
                self.verify(origin, manifest)
                if (self.state / "routes.json").exists():
                    self.verify(self.base_url, manifest)
            except Exception:
                if old_version:
                    self.activate(old_version, config)
                raise
            receipt = {"version_id": version, "previous_version_id": old_version,
                       "content_hash": identity, "published_at": datetime.now(timezone.utc).isoformat(),
                       "manifest": manifest, "preview_url": preview, "origin_url": origin,
                       "app_version": self.app_version(),
                       "app_commit": self.git_commit(self.path(self.cfg.get("app_source", "../onionflash"))),
                       "deployment_commit": self.git_commit(ROOT)}
            atomic_json(previous_path, receipt)
            atomic_json(self.state / "receipts" / (version + ".json"), receipt)
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

    def backup(self):
        cfg = self.cfg.get("spaces", {})
        if not cfg.get("enabled"):
            return
        for key in ["retention_days", "backup_interval_seconds"]:
            if type(cfg.get(key)) is not int or cfg[key] < 1:
                raise ValueError("Spaces " + key + " must be a positive integer")
        marker = self.state / "backup.json"
        previous = json.loads(marker.read_text()) if marker.exists() else {}
        if time.time() - previous.get("timestamp", 0) < cfg["backup_interval_seconds"]:
            return
        if not self.database.is_file():
            raise ValueError("No application database to back up")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", cfg["bucket"]):
            raise ValueError("Invalid Spaces bucket")
        prefix = cfg["prefix"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]+/backups/", prefix):
            raise ValueError("Backup prefix must be a project-specific name/backups/")
        if urlsplit(cfg["endpoint_url"]).scheme != "https":
            raise ValueError("Spaces endpoint must use HTTPS")
        import boto3
        from botocore.config import Config
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
            # SQLite's backup API gives a consistent WAL-aware copy; never copy the live DB file.
            with sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True) as source:
                with sqlite3.connect(temporary / "onionflash.sqlite3") as target:
                    source.backup(target, pages=128)
                    if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        raise ValueError("Database backup validation failed")
            archive = temporary / "snapshot.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(temporary / "onionflash.sqlite3", arcname="onionflash.sqlite3")
                for name in ["publication.json"]:
                    path = self.state / name
                    if path.is_file():
                        tar.add(path, arcname=name)
                with lock(str(self.database) + ".lock", shared=True):
                    self.validate(self.public)
                    tar.add(self.public, arcname="public", recursive=True)
            with archive.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            key = prefix + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + digest[:12] + ".tar.gz"
            client.upload_file(str(archive), cfg["bucket"], key, ExtraArgs={"ACL": "private", "ContentType": "application/gzip", "Metadata": {"sha256": digest}})
            head = client.head_object(Bucket=cfg["bucket"], Key=key)
            if head.get("Metadata", {}).get("sha256") != digest or head["ContentLength"] != archive.stat().st_size:
                raise ValueError("Backup upload verification failed")
            acl = client.get_object_acl(Bucket=cfg["bucket"], Key=key)
            if any(grant.get("Grantee", {}).get("Type") == "Group" for grant in acl.get("Grants", [])):
                raise ValueError("Backup object unexpectedly has a public/group ACL")
            atomic_json(marker, {"timestamp": time.time(), "key": key, "sha256": digest})
            # Delete only old objects in this application's dedicated backup namespace.
            cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=cfg["bucket"], Prefix=prefix):
                for item in page.get("Contents", []):
                    if item["Key"] != key and item["LastModified"] < cutoff and re.fullmatch(re.escape(prefix) + r"\d{8}T\d{6}Z-[0-9a-f]{12}\.tar\.gz", item["Key"]):
                        client.delete_object(Bucket=cfg["bucket"], Key=item["Key"])
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
        block = f"{begin}\n*/{interval} * * * * {command} >/dev/null 2>&1\n{end}\n"
        subprocess.run(["crontab", "-"], input=current.rstrip() + "\n" + block, text=True, check=True)
        LOG.info("Installed a %s-minute cron job", interval)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "deployment.toml"))
    parser.add_argument("command", choices=["check", "run", "publish", "backup", "routes", "schedule", "status", "rollback", "hold", "resume"])
    args = parser.parse_args()
    publisher = None
    try:
        publisher = Publisher(args.config)
        LOG.setLevel(logging.INFO)
        LOG.addHandler(logging.StreamHandler())
        handler = RotatingFileHandler(publisher.state / "run.log", maxBytes=1048576, backupCount=3)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        LOG.addHandler(handler)
        with lock(publisher.state / "run.lock", nonblocking=True):
            if args.command == "status":
                for filename in ["publication.json", "publication-hold.json", "backup.json", "last-run.json"]:
                    path = publisher.state / filename
                    if path.exists():
                        data = json.loads(path.read_text())
                        data.pop("manifest", None)
                        print(json.dumps({filename: data}, indent=2))
                return 0
            if args.command in {"check", "run"}:
                command = "check" if args.command == "check" else "run"
                subprocess.run([str(publisher.path(publisher.cfg["app_python"])), "-m", "onionflash", command, "--config", str(publisher.app_config)], check=True)
                if args.command == "check":
                    if not publisher.node.is_file() or not publisher.wrangler.is_file():
                        raise ValueError("Run install.sh to install the publishing toolchain")
                    print("Application and deployment configuration valid")
                    return 0
            if args.command in {"run", "publish"}:
                publisher.publish()
            if args.command in {"run", "backup"}:
                try:
                    publisher.backup()
                except Exception as exc:
                    if publisher.cfg.get("spaces", {}).get("required") or args.command == "backup":
                        raise
                    LOG.error("Backup deferred: %s", publisher.clean_error(exc))
            if args.command == "routes":
                publisher.routes()
            if args.command == "schedule":
                publisher.schedule()
            if args.command == "rollback":
                path = publisher.state / "publication.json"
                receipt = json.loads(path.read_text())
                previous = receipt.get("previous_version_id")
                if not previous:
                    raise ValueError("No previous version recorded")
                old_receipt = publisher.state / "receipts" / (previous + ".json")
                if not old_receipt.is_file():
                    raise ValueError("No verified previous publication exists; use hold to pause publication")
                restored = json.loads(old_receipt.read_text())
                # Persist a hold before changing production; cron must not undo a rollback.
                atomic_json(publisher.state / "publication-hold.json", {"reason": "manual rollback", "from": receipt["version_id"], "to": previous, "timestamp": time.time()})
                config = publisher.make_config(publisher.public)
                publisher.activate(previous, config)
                publisher.verify(restored["origin_url"], restored["manifest"])
                if (publisher.state / "routes.json").exists():
                    publisher.verify(publisher.base_url, restored["manifest"])
                atomic_json(publisher.state / "rollback.json", {"version_id": previous, "timestamp": time.time()})
                atomic_json(path, restored)
            if args.command == "hold":
                atomic_json(publisher.state / "publication-hold.json", {"reason": "manual hold", "timestamp": time.time()})
            if args.command == "resume":
                (publisher.state / "publication-hold.json").unlink(missing_ok=True)
            atomic_json(publisher.state / "last-run.json", {"ok": True, "command": args.command, "at": datetime.now(timezone.utc).isoformat()})
        return 0
    except BlockingIOError:
        LOG.info("Another Onion Flash deployment is running; skipped")
        return 0
    except Exception as exc:
        message = publisher.clean_error(exc) if publisher else type(exc).__name__ + ": " + str(exc)
        LOG.error("%s", message)
        if publisher:
            atomic_json(publisher.state / "last-run.json", {"ok": False, "command": args.command, "at": datetime.now(timezone.utc).isoformat(), "error": message})
        return 1


if __name__ == "__main__":
    sys.exit(main())
