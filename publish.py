#!/usr/bin/env python3
"""Small, explicit publisher. No runtime Git pulls and no shell-evaluated config."""
import argparse
import base64
from contextlib import closing, contextmanager, suppress
from datetime import datetime, timedelta, timezone
import errno
import fcntl
import gzip
import hashlib
from http.client import HTTPException
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
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from onionflash import config as app_config
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
DEPLOY_KEYS = {"app_config", "app_source", "env_file", "state_dir", "app_python",
               "interval_minutes", "cloudflare", "spaces", "archive", "retention"}
CLOUDFLARE_KEYS = {"worker_name", "compatibility_date"}
SPACES_KEYS = {"enabled", "required", "endpoint_url", "region", "bucket", "prefix",
               "backup_interval_seconds", "retention_days"}
ARCHIVE_KEYS = {"enabled", "endpoint_url", "region", "bucket", "public_prefix", "private_prefix",
                "public_origin", "access_key_env", "secret_key_env"}
MISSING_OBJECT_CODES = {"404", "NoSuchKey", "NotFound"}


def file_md5(path):
    """Return a non-security MD5 hash object computed across path's bytes."""
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, lambda: hashlib.md5(usedforsecurity=False))


def clean_https_url(value, label):
    """Parse value and require an HTTPS URL without credentials, query string, or fragment."""
    if (not isinstance(value, str) or "\\" in value or "?" in value or "#" in value
            or any(ord(c) <= 32 or ord(c) == 127 for c in value)):
        raise ValueError(f"{label} must be a clean HTTPS URL")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{label} must be a clean HTTPS URL") from exc
    if (parsed.scheme != "https" or not parsed.hostname or parsed.netloc.endswith(":")
            or parsed.username is not None or parsed.password is not None
            or port == 0 or parsed.query or parsed.fragment):
        raise ValueError(f"{label} must be a clean HTTPS URL")
    return parsed


def validate_s3_location(cfg, label):
    """Validate S3-compatible endpoint URL, bucket name, and region settings."""
    clean_https_url(cfg.get("endpoint_url"), f"{label} endpoint")
    bucket = cfg.get("bucket")
    if not isinstance(bucket, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError(f"Invalid {label} bucket")
    if not isinstance(cfg.get("region"), str) or not cfg["region"].strip():
        raise ValueError(f"{label} region must be a non-empty string")


def require_uuid(value, message="Invalid version identity"):
    """Return value when it matches the 36-character lowercase hex-and-hyphen UUID format."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f-]{36}\Z", value):
        raise ValueError(message)
    return value


def require_cf_id(value, label):
    """Return value when it is a 32-character lowercase hexadecimal Cloudflare identifier."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}\Z", value):
        raise ValueError(f"Invalid {label}")
    return value


def require_env_name(value, message="Invalid credential-file assignment"):
    """Return value when it is a valid uppercase POSIX environment variable name."""
    if not isinstance(value, str) or not ENV_NAME.fullmatch(value):
        raise ValueError(message)
    return value


def reject_unknown_keys(cfg, allowed, section):
    """Raise ValueError when cfg contains keys outside allowed for section."""
    if unknown := set(cfg) - allowed:
        raise ValueError(f"Unknown {section} settings: " + ", ".join(sorted(unknown)))


def require_public_files(files):
    """Raise ValueError naming the first missing required public file when files is incomplete."""
    if missing := public_files() - files.keys():
        raise ValueError("Missing required public file: " + min(missing))


def atomic_json(path, value):
    """Write formatted JSON atomically via fsynced temporary file and parent directory sync."""
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
    """Load JSON from path, returning default when the file does not exist."""
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


CREDENTIAL_NAMES = set()


def credential_values(names=()):
    """Values of credential-like environment variables, long enough to redact safely."""
    found = {k for k in os.environ if any(x in k.upper() for x in ("TOKEN", "SECRET", "API_KEY", "SPACES_KEY"))}
    found |= {n for n in (*CREDENTIAL_NAMES, *names) if isinstance(n, str) and n}
    return sorted({os.environ[name] for name in found if len(os.environ.get(name, "")) >= 8}, key=len, reverse=True)


def redact(value, names=()):
    """Replace known credential values in text with [redacted] and bound output length."""
    result = str(value)
    for secret in credential_values(names):
        result = result.replace(secret, "[redacted]")
    return result[-2500:]


def clean_error(publisher, error):
    """Format and redact an exception using publisher secrets when available."""
    clean = getattr(publisher, "clean_error", None)
    return clean(error) if clean else redact(f"{type(error).__name__}: {error}")


def load_env(path):
    """KEY=value lines with shell-style quoting and no expansion; the existing environment wins."""
    path = Path(path)
    try:
        st = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(st.st_mode) or st.st_mode & 0o077:
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
    """Acquire an advisory flock on path, raising BusyLock on nonblocking contention."""
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
    """Open the SQLite database under its companion lock in read-only or read-write mode."""
    target = Path(path).resolve(strict=True)
    uri = target.as_uri() + ("?mode=ro" if shared else "?mode=rw")
    with lock(f"{Path(path)}.lock", shared=shared), closing(sqlite3.connect(uri, uri=True)) as db:
        db.row_factory = sqlite3.Row
        yield db


def s3_client(publisher, cfg, access_key_env, secret_key_env, *, max_attempts=1):
    """Construct a bounded boto3 S3 client using credentials from the named environment variables."""
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=cfg["endpoint_url"], region_name=cfg["region"],
        aws_access_key_id=publisher.secret(access_key_env),
        aws_secret_access_key=publisher.secret(secret_key_env),
        config=Config(
            connect_timeout=publisher.net["timeout_seconds"], read_timeout=publisher.net["timeout_seconds"],
            retries={"mode": "standard", "total_max_attempts": int(max_attempts)},
            request_checksum_calculation="when_required", response_checksum_validation="when_required",
        ),
    )


def verify_private_object(publisher, endpoint_url, bucket, key, label, delete=None):
    """Ensure an uploaded object rejects unsigned reads with HTTP 403, deleting it on exposure."""
    problem = publisher.unsigned_read_problem(f"{endpoint_url.rstrip('/')}/{bucket}/{quote(key)}")
    if not problem:
        return
    if delete is not None:
        try:
            delete()
        except Exception as cleanup:
            raise ValueError(f"{label} {key} {problem}, and deleting it failed: {publisher.clean_error(cleanup)}") from cleanup
    raise ValueError(f"{label} {key} {problem}")


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
    """Validate deployment configuration and run guarded Worker publications, backups, and schedules."""

    def __init__(self, config_path, *, offline=False):
        self.config_path = Path(config_path).resolve()
        self.cfg = tomllib.loads(self.config_path.read_text())
        self.directory = self.config_path.parent
        env_file = self.cfg.get("env_file", "secrets.env")
        if not isinstance(env_file, str) or not env_file.strip() or "\x00" in env_file:
            raise ValueError("env_file must be a non-empty string")
        load_env(self.path(env_file))
        reject_unknown_keys(self.cfg, DEPLOY_KEYS, "deployment")
        for key in ("app_config", "app_python"):
            if not isinstance(self.cfg.get(key), str) or not self.cfg[key].strip() or "\x00" in self.cfg[key]:
                raise ValueError(f"{key} must be a non-empty string")
        for key in ("state_dir", "app_source"):
            if key in self.cfg and (not isinstance(self.cfg[key], str) or not self.cfg[key].strip() or "\x00" in self.cfg[key]):
                raise ValueError(f"{key} must be a non-empty string")
        interval = self.cfg.get("interval_minutes", 5)
        if type(interval) is not int or not 1 <= interval <= 59:
            raise ValueError("interval_minutes must be 1..59")
        if not isinstance(self.cfg.get("cloudflare"), dict):
            raise ValueError("cloudflare must be a table")
        for section in ("spaces", "archive", "retention"):
            if section in self.cfg and not isinstance(self.cfg[section], dict):
                raise ValueError(f"{section} must be a table")
        for section, key in (("archive", "enabled"), ("retention", "enabled"), ("spaces", "enabled"), ("spaces", "required")):
            if type(self.cfg.get(section, {}).get(key, False)) is not bool:
                raise ValueError(f"{section}.{key} must be a boolean")
        reject_unknown_keys(self.cfg["cloudflare"], CLOUDFLARE_KEYS, "cloudflare")
        reject_unknown_keys(self.cfg.get("spaces", {}), SPACES_KEYS, "spaces")
        reject_unknown_keys(self.cfg.get("archive", {}), ARCHIVE_KEYS, "archive")
        reject_unknown_keys(self.cfg.get("retention", {}), {"enabled"}, "retention")
        for key in ("access_key_env", "secret_key_env"):
            if key in self.cfg.get("archive", {}):
                require_env_name(self.cfg["archive"][key], "Invalid archive credential variable name: " + key)
        if self.cfg.get("spaces", {}).get("required") and not self.cfg.get("spaces", {}).get("enabled"):
            raise ValueError("Required Spaces backups must also set spaces.enabled = true")
        if self.cfg.get("retention", {}).get("enabled") and not self.cfg.get("archive", {}).get("enabled"):
            raise ValueError("Private retention requires configured permanent storage")
        self.state = self.path(self.cfg.get("state_dir", "state"))
        self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.state, 0o700)
        self._pub_path, self._hold_path = self.state / "publication.json", self.state / "publication-hold.json"
        self.app_config = self.path(self.cfg["app_config"])
        self.app = app_config.load_config(self.app_config)
        self.public = Path(self.app["storage"]["output_dir"])
        self.database = Path(self.app["storage"]["database"])
        self.base_url = self.app["site"]["base_url"]
        parsed = clean_https_url(self.base_url, "Public base_url")
        if parsed.port is not None or parsed.hostname.endswith(".") or ":" in parsed.hostname:
            raise ValueError("Public base_url must use the default HTTPS port and a canonical hostname")
        if not re.fullmatch(r"(?:/[a-zA-Z0-9_-]+)*/", parsed.path):
            raise ValueError("Use a plain path for the public site")
        if any(seg.startswith("_") for seg in parsed.path.split("/")[:2] if seg):
            raise ValueError("Public site path may not start with a reserved underscore segment")
        self.base_path = parsed.path.rstrip("/")
        self.host = parsed.hostname
        self.base_url = f"https://{self.host}{parsed.path}"
        self.route_pattern = f"{self.host}{self.base_path or '/'}*"
        self.cf = self.cfg["cloudflare"]
        if not isinstance(self.cf.get("worker_name"), str) or not SAFE_ID.fullmatch(self.cf["worker_name"]):
            raise ValueError("Invalid Worker name")
        compat = self.cf.get("compatibility_date")
        if not isinstance(compat, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}\Z", compat):
            raise ValueError("Invalid Cloudflare compatibility_date")
        try:
            datetime.strptime(compat, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Invalid Cloudflare compatibility_date") from exc
        self.net = dict(NETWORK)
        self.account = self.token = ""
        if not offline:
            self._load_cloudflare()
        self.api_base = API_BASE
        self._archive = None
        self.node, self.wrangler = ROOT / "node_modules/node/bin/node", ROOT / "node_modules/wrangler/bin/wrangler.js"

    def _load_cloudflare(self):
        if not self.account or not self.token:
            self.account = require_cf_id(self.secret(ACCOUNT_ID), "Cloudflare account ID")
            self.token = self.secret(API_TOKEN)

    def path(self, value):
        """Resolve value relative to the config directory while preserving a final executable symlink."""
        return Path(os.path.abspath(self.directory / value))

    def _receipt_path(self, version):
        return self.state / "receipts" / f"{version}.json"

    def _marker_fresh(self, name, destination, interval, *, require_key=None):
        prev = read_json(self.state / f"{name}.json", {})
        return (not require_key or bool(prev.get(require_key))) and prev.get("destination") == destination and 0 <= time.time() - prev.get("timestamp", 0) < interval

    @staticmethod
    def secret(name):
        """Return a required non-empty credential from os.environ."""
        value = os.environ.get(name, "")
        if not value:
            raise ValueError("Missing environment setting: " + name)
        return value

    def clean_error(self, value):
        """Redact deployment and application secrets from an error value."""
        return redact(value, self.private_names())

    def private_names(self):
        """Environment variables holding credentials for Cloudflare, Spaces or the LLM."""
        archive = self.cfg.get("archive") if isinstance(self.cfg.get("archive"), dict) else {}
        candidates = (API_TOKEN, ZONE_TOKEN, SPACES_KEY, SPACES_SECRET, self.app["llm"]["api_key_env"],
                      archive.get("access_key_env"), archive.get("secret_key_env"))
        return {name for name in candidates if isinstance(name, str) and name}

    def secret_values(self):
        """Return active secret strings long enough for safe output and artifact scanning."""
        return credential_values(self.private_names())

    def _isolated_env(self, keep=(), **extra):
        private = self.private_names() - set(keep)
        return {k: v for k, v in os.environ.items() if k not in private} | extra

    def delay(self, attempt):
        """Storage retries have their own bounded backoff, independent of collection."""
        delay = min(self.net["backoff_cap_seconds"], self.net["backoff_seconds"] * 2 ** min(attempt, 10)
                    + random.uniform(0, self.net["jitter_seconds"]))
        time.sleep(delay)

    def request(self, url, method="GET", data=None, token=None, headers=None):
        """One bounded urllib request; HEAD reads no body."""
        defaults = {"User-Agent": "OnionFlashPublisher", "Accept-Encoding": "identity",
                    **({"Content-Type": "application/json"} if data is not None else {})}
        lower = {k.lower() for k in (headers or ())}
        merged = {k: v for k, v in defaults.items() if k.lower() not in lower} | (headers or {})
        request = Request(url, None if data is None else json.dumps(data).encode(), merged, method=method)
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
            except (OSError, HTTPException) as exc:
                problem = f"could not be checked {'for public access' if public else 'for privacy'} ({type(exc).__name__})"
        return problem

    @property
    def archive(self):
        """Lazily initialized ArchiveStore when permanent archive storage is enabled, else None."""
        if not self.cfg.get("archive", {}).get("enabled", False):
            return None
        if self._archive is None:
            from archive_store import ArchiveStore
            self._archive = ArchiveStore(self)
        return self._archive

    def close(self):
        """Close the underlying archive S3 client when initialized."""
        if self._archive is not None and (close := getattr(self._archive.client, "close", None)):
            close()

    def api(self, suffix, method="GET", data=None, zone=False):
        """Call the Cloudflare v4 JSON API with account or zone credentials and return result."""
        if not zone:
            self._load_cloudflare()
        token = self.secret(ZONE_TOKEN) if zone else self.token
        response = json.loads(self.request(self.api_base + suffix, method, data, token))
        if not response.get("success"):
            raise RuntimeError("Cloudflare API rejected request: " + self.clean_error(response.get("errors")))
        return response.get("result")

    def record_first_version(self):
        """Record the initial deployed Worker version in publication.json when present."""
        version = self.current_version()
        if version:
            atomic_json(self._pub_path, {"version_id": version})
        return version

    def current_version(self):
        """Return the single 100%-active Worker version UUID, or None before first deployment."""
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
        """Run the local Wrangler CLI with isolated Cloudflare credentials and sanitized output."""
        self._load_cloudflare()
        env = self._isolated_env(
            CLOUDFLARE_ACCOUNT_ID=self.account, CLOUDFLARE_API_TOKEN=self.token,
            WRANGLER_SEND_METRICS="false", WRANGLER_LOG_SANITIZE="true",
            WRANGLER_LOG_PATH=str(self.state / "wrangler-logs"), CI="true",
            **({"WRANGLER_OUTPUT_FILE_PATH": str(metadata)} if metadata else {}),
        )
        result = subprocess.run([str(self.node), str(self.wrangler), *arguments], cwd=ROOT, env=env,
                                capture_output=True, text=True, timeout=COMMAND_TIMEOUT_SECONDS)
        if result.returncode:
            raise RuntimeError(self.clean_error(result.stdout + result.stderr))
        return result.stdout

    def worker_vars(self, archive=None):
        """Build Worker environment variables for the site mount path and optional archive manifest."""
        return {"BASE_PATH": self.base_path, **(self.archive.worker_vars(archive["manifest_sha256"]) if archive else {})}

    def publication_identity(self, manifest, archive=None):
        """Compute a deterministic SHA-256 digest over manifest hashes, Worker code, vars, and compat date."""
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
        """Write wrangler.json for uploading static assets and Worker bindings."""
        return self._write_wrangler_config(
            "wrangler.json", workers_dev=True, preview_urls=True, main=str(ROOT / "worker.mjs"),
            assets={"directory": str(directory), "binding": "ASSETS",
                    "html_handling": "auto-trailing-slash", "not_found_handling": "none"},
            vars=self.worker_vars(archive),
        )

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
        """Verify public directory paths, size budgets, required feeds, and absence of symlinks or secrets."""
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
        require_public_files(files)
        import xml.etree.ElementTree as ET
        if ET.parse(directory / "feed.xml").getroot().tag != "rss":
            raise ValueError("Expected RSS feed")
        json.loads((directory / "feed.json").read_text())
        return files

    def verify(self, base_url, manifest, *, archive=None):
        """Fetch live or preview endpoints uncached with bounded retries and verify their SHA-256 digests."""
        paths = list(VERIFY_PATHS)
        if archive:  # One article also proves the Worker's route to permanent storage.
            paths += sorted(path for path in manifest if ARTICLE.fullmatch(path))[:1]
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        for relative in paths:
            for attempt in range(VERIFY_ATTEMPTS):
                try:
                    actual = hashlib.sha256(self.request(base_url.rstrip("/") + "/" + relative, headers=headers)).hexdigest()
                    if actual != manifest[relative]:
                        raise ValueError("Published hash mismatch: " + relative)
                    break
                except (ValueError, OSError, HTTPException) as exc:
                    if isinstance(exc, HTTPError) and exc.code != 404 and exc.code < 500:
                        raise
                    if attempt + 1 == VERIFY_ATTEMPTS:
                        raise
                    self.delay(attempt)

    def activate(self, version):
        """Promote a validated Worker version UUID to 100% traffic, tolerating post-commit CLI timeouts."""
        require_uuid(version)
        config = self._write_wrangler_config("wrangler-control.json")
        try:
            self.command(["versions", "deploy", version + "@100%", "--yes", "--config", str(config)])
        except (RuntimeError, subprocess.TimeoutExpired):
            if self.current_version() != version:
                raise

    def origin_url(self):
        """Return the Worker's workers.dev origin URL including any configured base path."""
        self._load_cloudflare()
        subdomain = self.api(f"/accounts/{self.account}/workers/subdomain")["subdomain"]
        return f"https://{self.cf['worker_name']}.{subdomain}.workers.dev" + self.base_path

    def forget_content(self, receipt):
        """Clear recorded content hash and write receipt to publication.json."""
        receipt.pop("content_hash", None)
        atomic_json(self._pub_path, receipt)

    def rollback(self):
        """Hold publication and reactivate the previous version."""
        receipt = read_json(self._pub_path, {})
        previous = receipt.get("previous_version_id")
        if not previous:
            raise ValueError("No previous publication is recorded; use hold to pause publication")
        require_uuid(previous, "Invalid previous version identity")
        expected = receipt.get("version_id")
        if self.current_version() != expected:
            raise ValueError("Active Worker version differs from the publication receipt; rollback not performed")
        restored = read_json(self._receipt_path(previous))
        if restored is not None and restored.get("version_id") != previous:
            raise ValueError("Previous publication receipt has the wrong version")
        atomic_json(self._hold_path, {"reason": "rollback requested", "from": expected, "to": previous, "timestamp": time.time()})
        self.forget_content(receipt)
        self.activate(previous)
        self.forget_content({**(restored or {"version_id": previous}), "rolled_back_from": receipt["version_id"],
                             "rolled_back_at": datetime.now(timezone.utc).isoformat()})

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

    def publish(self):
        """Prepare durable objects, check preview, activate, then verify or restore."""
        if self._hold_path.exists():
            raise ValueError("Publication is on hold; inspect status and use the explicit resume command")
        if not self.public.is_dir():
            raise ValueError("Build the public output first")
        ledger = self.publication_readiness()
        previous = read_json(self._pub_path, {})
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
                archive_info = {"manifest_sha256": synchronized["manifest_sha256"],
                                "generation": generation, "origin": self.archive.cfg["public_origin"]}
            else:
                manifest = ledger
                if previous.get("content_hash") != self.publication_identity(ledger):
                    with lock(str(self.database) + ".lock", shared=True):
                        ledger = self.publication_readiness()
                        shutil.copytree(self.public, stage, symlinks=True)
                    for path in sorted(stage.rglob("*"), reverse=True):
                        if path.is_symlink():
                            continue
                        if path.is_file():
                            relative = path.relative_to(stage).as_posix()
                            if relative not in ledger and public_path_allowed(relative):
                                path.unlink()
                        elif path.is_dir():
                            with suppress(OSError):
                                path.rmdir()
                    manifest = self.validate(stage)
                    self.publication_readiness(manifest)
            articles = {path for path in manifest if ARTICLE.fullmatch(path)}
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
            saved_routes = read_json(self.state / "routes.json")
            routed = (isinstance(saved_routes, dict) and isinstance(saved_routes.get("patterns"), list)
                      and self.route_pattern in saved_routes["patterns"])
            live = self.base_url if routed else self.origin_url()
            provenance = {"app_version": self.app_version(),
                          "app_commit": self.git_commit(self.path(self.cfg.get("app_source", "../onionflash"))),
                          "deployment_commit": self.git_commit(ROOT)}
            if before and previous.get("manifest") and not (prior := self._receipt_path(before)).exists():
                atomic_json(prior, previous)
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
                uploaded = next(
                    (x for x in (json.loads(line) for line in lines if line.strip())
                     if isinstance(x, dict) and x.get("type") == "version-upload"),
                    {},
                )
                version, preview = uploaded.get("version_id"), uploaded.get("preview_url")
                if not version or not preview:
                    raise RuntimeError("Wrangler did not report the uploaded version and its preview URL")
                require_uuid(version, "Cloudflare returned an invalid version identity")
                if not clean_https_url(preview, "Cloudflare preview URL").hostname.endswith(".workers.dev"):
                    raise ValueError("Invalid Cloudflare preview URL")
                self.verify(preview.rstrip("/") + self.base_path, manifest, archive=archive_info)
                if self.current_version() != before:
                    raise ValueError("Active Worker version changed during preview; external version retained")
                self.activate(version)
            try:
                self.verify(live, manifest, archive=archive_info)
            except Exception as failure:
                if before is not None:
                    if self.current_version() != version:
                        raise RuntimeError("Live check failed and the Worker changed outside this publication; external version retained") from failure
                    try:
                        self.activate(before)
                    except Exception as restore:
                        raise RuntimeError(f"Live check failed ({failure}) and restoring {before} failed: {restore}") from failure
                raise
            atomic_json(permalinks, sorted(articles))
            receipt = {"version_id": version, "previous_version_id": before,
                       "content_hash": identity, "manifest": manifest,
                       "published_at": datetime.now(timezone.utc).isoformat(), **provenance}
            if archive_info:
                receipt["archive"] = archive_info
            atomic_json(self._receipt_path(version), receipt)
            atomic_json(self._pub_path, receipt)
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
        """Read the installed onionflash package version from the configured application virtualenv."""
        try:
            result = subprocess.run(
                [str(self.path(self.cfg["app_python"])), "-c",
                 "import importlib.metadata; print(importlib.metadata.version('onionflash'))"],
                capture_output=True, text=True, timeout=10, env=self._isolated_env(),
            )
        except (OSError, subprocess.SubprocessError):
            return "unknown"
        return result.stdout.strip() if result.returncode == 0 else "unknown"

    def git_commit(self, directory):
        """Return the 40-hex HEAD commit SHA for directory, or None when unavailable."""
        env = {k: v for k, v in self._isolated_env().items() if not k.startswith("GIT_")}
        try:
            result = subprocess.run(
                ["git", "--no-optional-locks", "-C", str(directory), "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=10, env=env,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        value = result.stdout.strip()
        return value if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}\Z", value) else None

    def routes(self):
        """Verify origin health, attach missing Cloudflare zone routes, and roll back new routes on failure."""
        receipt = read_json(self._pub_path, {})
        if "manifest" not in receipt:
            raise ValueError("The publication receipt lists no files to check; publish once, then attach routes")
        self.verify(self.origin_url(), receipt["manifest"], archive=receipt.get("archive"))
        zone = require_cf_id(self.secret(ZONE_ID), "zone ID")
        existing = self.api(f"/zones/{zone}/workers/routes", zone=True)
        pattern = self.route_pattern
        found = [
            x for x in existing
            if isinstance(x, dict) and isinstance(x.get("pattern"), str)
            and (lambda h, s, rest: h.lower() + s + rest)(*x["pattern"].partition("/")) == pattern
        ]
        if any(x.get("script") != self.cf["worker_name"] for x in found):
            raise ValueError("Route is already owned by another Worker: " + pattern)
        created_id = None
        try:
            if not found:
                created_id = self.api(f"/zones/{zone}/workers/routes", "POST",
                                      {"pattern": pattern, "script": self.cf["worker_name"]}, zone=True)["id"]
                LOG.info("Added scoped route %s", pattern)
            self.verify(self.base_url, receipt["manifest"], archive=receipt.get("archive"))
        except BaseException as failure:
            if created_id is not None:
                try:
                    self.api(f"/zones/{zone}/workers/routes/{created_id}", "DELETE", zone=True)
                except Exception as cleanup:
                    raise RuntimeError(
                        f"Route verification failed ({self.clean_error(failure)}) and removing route {created_id} failed: "
                        f"{self.clean_error(cleanup)}"
                    ) from failure
            raise
        atomic_json(self.state / "routes.json", {"patterns": [pattern], "verified_at": datetime.now(timezone.utc).isoformat()})
        LOG.info("Verified public website and RSS at %s", self.base_url)

    def validate_spaces(self):
        """Validate Spaces backup retention, interval, bucket location, prefix, and credentials."""
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
        """Upload a gzipped private SQLite snapshot to Spaces when due and prune expired backups."""
        cfg = self.cfg.get("spaces", {})
        if not cfg.get("enabled"):
            if cfg.get("required"):
                raise ValueError("Required Spaces backups must also set spaces.enabled = true")
            return
        self.validate_spaces()
        prefix, bucket = cfg["prefix"], cfg["bucket"]
        destination = {key: cfg[key] for key in ("endpoint_url", "bucket", "prefix")}
        if not force and self._marker_fresh("backup", destination, cfg["backup_interval_seconds"]):
            return
        if not self.database.is_file():
            raise ValueError("No application database to back up")
        client = s3_client(self, cfg, SPACES_KEY, SPACES_SECRET, max_attempts=self.net["attempts"])
        try:
            key = prefix + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4) + ".sqlite3.gz"
            attempted = uploaded = False
            def delete():
                """Remove the newly uploaded backup object if present."""
                nonlocal attempted
                attempted = True
                from botocore.exceptions import ClientError
                try:
                    client.delete_object(Bucket=bucket, Key=key)
                except ClientError as exc:
                    if str(exc.response.get("Error", {}).get("Code")) not in MISSING_OBJECT_CODES:
                        raise
            try:
                with tempfile.TemporaryDirectory(prefix="backup-", dir=self.state) as temporary:
                    snapshot, compressed = Path(temporary) / "onionflash.sqlite3", Path(temporary) / "onionflash.sqlite3.gz"
                    with lock(str(self.database) + ".lock", shared=True):
                        backup_database(self.database, snapshot)
                    with snapshot.open("rb") as source, gzip.open(compressed, "wb") as target:
                        shutil.copyfileobj(source, target)
                    size = compressed.stat().st_size
                    md5 = base64.b64encode(file_md5(compressed).digest()).decode()
                    uploaded = True
                    with compressed.open("rb") as body:
                        client.put_object(
                            Bucket=bucket, Key=key, Body=body, ContentLength=size, ContentMD5=md5,
                            ACL="private", ContentType="application/gzip",
                        )
                verify_private_object(self, cfg["endpoint_url"], bucket, key, "The new backup", delete=delete)
            except BaseException as failure:
                if uploaded and not attempted:
                    try:
                        delete()
                    except Exception as cleanup:
                        raise RuntimeError(
                            f"Backup upload failed and {key} could not be removed: "
                            f"{self.clean_error(failure)}; {self.clean_error(cleanup)}"
                        ) from failure
                raise
            cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])
            pattern = re.escape(prefix) + r"\d{8}T\d{6}Z(?:(?:-[0-9a-f]{8})?\.sqlite3\.gz|-[0-9a-f]{12}(?:-[0-9a-f]{32})?\.tar\.gz)"
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                for item in page.get("Contents", []):
                    if item["Key"] != key and item["LastModified"] < cutoff and re.fullmatch(pattern, item["Key"]):
                        client.delete_object(Bucket=bucket, Key=item["Key"])
            atomic_json(self.state / "backup.json", {"timestamp": time.time(), "key": key, "destination": destination})
            LOG.info("Private Spaces backup %s", key)
        finally:
            if close := getattr(client, "close", None):
                close()

    def schedule(self):
        """Install or update the managed crontab block for periodic publish.py run execution."""
        interval = self.cfg.get("interval_minutes", 5)
        if type(interval) is not int or not 1 <= interval <= 59:
            raise ValueError("interval_minutes must be 1..59")
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=10)
        if result.returncode and "no crontab" not in result.stderr.lower():
            detail = self.clean_error((result.stderr or result.stdout or "").strip())
            raise RuntimeError("Cannot read existing crontab" + (f": {detail}" if detail else ""))
        begin, end = "# BEGIN ONIONFLASH", "# END ONIONFLASH"
        raw_lines = result.stdout.split("\n")
        if raw_lines and raw_lines[-1] == "":
            raw_lines.pop()
        lines = [line.removesuffix("\r") for line in raw_lines]
        if any((begin in line and line != begin) or (end in line and line != end) for line in lines):
            raise ValueError("Ambiguous existing Onion Flash schedule")
        begins = [i for i, line in enumerate(lines) if line == begin]
        ends = [i for i, line in enumerate(lines) if line == end]
        if len(begins) != len(ends) or len(begins) > 1 or (begins and begins[0] > ends[0]):
            raise ValueError("Ambiguous existing Onion Flash schedule")
        if begins:
            del lines[begins[0]:ends[0] + 1]
        kept = "\n".join(lines).strip("\r\n")
        command = " ".join(shlex.quote(str(x)) for x in [self.path(self.cfg["app_python"]), ROOT / "publish.py", "--config", self.config_path, "run"])
        if any(c in command for c in ("\n", "\r", "\\", "\x00")):
            raise ValueError("Scheduled command paths must not contain newlines or backslashes")
        escaped_command = command.replace("%", r"\%")
        block = f"{begin}\n*/{interval} * * * * {escaped_command} 2>&1 | logger -t onionflash\n{end}\n"
        try:
            installed = subprocess.run(
                ["crontab", "-"], input=(f"{kept}\n{block}" if kept.strip(" \t\r\n") else block),
                capture_output=True, text=True, timeout=10, check=True,
            )
        except subprocess.CalledProcessError as exc:
            detail = self.clean_error((exc.stderr or exc.stdout or "").strip() or f"exited with status {exc.returncode}")
            raise RuntimeError(f"Cannot install crontab: {detail}") from exc
        if installed.returncode:
            detail = self.clean_error((installed.stderr or installed.stdout or "").strip() or f"exited with status {installed.returncode}")
            raise RuntimeError(f"Cannot install crontab: {detail}")
        LOG.info("Installed a %s-minute cron job", interval)

    def application(self, *arguments):
        """Run one app command and return its output; failures carry the app's error."""
        budget = (app_config.run_command_budget_seconds(self.app)
                  if arguments[:1] == ("run",) and hasattr(app_config, "run_command_budget_seconds")
                  else self.app["collection"]["run_budget_seconds"])
        env = self._isolated_env((self.app["llm"]["api_key_env"],))
        result = subprocess.run([str(self.path(self.cfg["app_python"])), "-m", "onionflash", *arguments,
                                 "--config", str(self.app_config)], capture_output=True, text=True,
                                timeout=budget + 300, env=env)
        if result.returncode:
            detail = result.stderr.strip() or f"exited with status {result.returncode}"
            raise RuntimeError(self.clean_error(detail)[-1000:])
        return result.stdout.strip()

    def collect(self, result):
        """Execute an application collection and build pass and attach its JSON report to result."""
        report = self.application("run")
        LOG.info("run: %s", self.clean_error(report.splitlines()[-1] if report else "no report"))
        try:
            parsed = json.loads(report.splitlines()[-1])
        except (ValueError, IndexError):
            parsed = None
        result["report"] = parsed
        return not (isinstance(parsed, dict) and parsed.get("status") == "already-running")

    def retain_private_history(self):
        """Archive cold private evidence to Spaces and compact local rows when the retention interval elapses."""
        import retention
        if self._marker_fresh("retention", retention.destination(self.archive), retention.INTERVAL_SECONDS, require_key="batch_complete"):
            return
        with self.archive.budget(retention.RUN_BUDGET_SECONDS):
            result = retention.archive_evidence(self.archive)
        atomic_json(self.state / "retention.json", {**result, "timestamp": time.time()})

    def weekly_summary(self):
        """Write the current ISO week's editorial decisions summary to state/weekly when absent."""
        year, week, _ = datetime.now(timezone.utc).isocalendar()
        target = self.state / "weekly" / f"{year}-W{week:02d}.md"
        if not target.exists():
            summary = json.loads(self.application("decisions", "summary", "--since", "7d"))
            atomic_json(target.with_name("latest.md"), summary)
            atomic_json(target, summary)

    def run_cycle(self):
        """Run collection, guarded publication, backup, retention, and weekly summary phases."""
        result = {"phases": {}, "errors": [], "warnings": []}
        def attempt(phase, action, required=True):
            """Run one cycle phase and record its outcome in result."""
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
            if isinstance(result.get("report"), dict) and result["report"].get("status") == "already-running":
                message = "collection: application database is locked by another process; cycle skipped"
                result["phases"].update(
                    collection="busy", publication="skipped", backup="skipped",
                    **({"retention": "skipped"} if self.cfg.get("retention", {}).get("enabled") else {}),
                    weekly_summary="skipped",
                )
                result["warnings"].append(message)
                LOG.warning("%s", message)
                result.update(ok=True, degraded=True)
                return result
            if self._hold_path.exists():
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
        """Print a JSON summary of lock state and recent deployment receipts without full file manifests."""
        try:
            with lock(self.state / "run.lock", shared=True, nonblocking=True):
                running = False
        except BusyLock:
            running = True
        report = {"running": running}
        for name in ("publication.json", "publication-hold.json", "backup.json", "archive-sync.json", "retention.json", "last-run.json"):
            if (data := read_json(self.state / name)) is not None:
                report[name] = {k: v for k, v in data.items() if k not in ("manifest", "articles")}
        print(json.dumps(report, indent=2))

    def execute(self, command, force=False):
        """Dispatch a locked deployment CLI subcommand and return its run-receipt payload."""
        if command == "run":
            return self.run_cycle()
        if command == "check":
            print(self.application(command))
            if not self.node.is_file() or not self.wrangler.is_file():
                raise ValueError("Run install.sh to install the publishing toolchain")
            if self.cfg.get("spaces", {}).get("enabled"):
                self.validate_spaces()
            self.archive
            print("Application and deployment configuration valid")
        elif command == "backup":
            if not self.cfg.get("spaces", {}).get("enabled"):
                raise ValueError("Spaces backups are disabled; set spaces.enabled = true or copy the database yourself")
            self.backup(force=force)
        elif command == "hold":
            atomic_json(self._hold_path, {"reason": "manual hold", "timestamp": time.time()})
        elif command == "resume":
            self._hold_path.unlink(missing_ok=True)
            sync_directory(self.state)
        else:
            {"publish": self.publish, "routes": self.routes, "schedule": self.schedule,
             "rollback": self.rollback}[command]()
        return {"ok": True}


def main():
    """Parse CLI arguments, coordinate locking and logging, and return a POSIX exit status."""
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
        publisher = Publisher(args.config, offline=args.command in {"status", "hold", "resume"})
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
        LOG.error("%s", clean_error(publisher, exc))
        return 1
    finally:
        if publisher is not None:
            publisher.close()
        LOG.removeHandler(console)
        console.close()


if __name__ == "__main__":
    sys.exit(main())
