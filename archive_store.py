"""Immutable article objects and private evidence in Spaces.

Article permalinks (news/<id>/index.html and story.json) are uploaded once as
content-addressed public objects, routed by one compressed manifest built from
the core's ledger. Every other public file ships in the Worker's own bundle.
"""
import base64
from contextlib import closing, contextmanager
import gzip
import hashlib
from pathlib import Path
import re
import tempfile
import time

from botocore.exceptions import (
    ClientError, ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError,
    IncompleteReadError, ReadTimeoutError, ResponseStreamingError,
)
from onionflash.store import get_meta, set_meta
from onionflash.util import canonical_json
from publish import (
    ARCHIVE_KEYS, ARTICLE, MISSING_OBJECT_CODES, clean_https_url, file_md5, open_db, public_path_allowed,
    reject_unknown_keys, require_env_name, require_public_files, s3_client, validate_s3_location, verify_private_object,
)

SHA = re.compile(r"[a-f0-9]{64}\Z")
CHUNK = 65536
MAX_UPLOADS_PER_RUN, MAX_UPLOAD_BYTES_PER_RUN, SYNC_BUDGET_SECONDS = 500, 512 * 1024**2, 180
MAX_OBJECT_BYTES, MAX_MANIFEST_BYTES, MAX_MANIFEST_RAW_BYTES = 256 * 1024**2, 8 * 1024**2, 16 * 1024**2
MANIFEST_FORMAT = "onionflash-archive-manifest-v1"
PENDING = "path GLOB 'news/*' AND (uploaded_sha256 IS NULL OR uploaded_sha256 != sha256)"
TRANSIENT_ERRORS = (ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError,
                    IncompleteReadError, ReadTimeoutError, ResponseStreamingError)
RETRYABLE_CODES = {"408", "429", "500", "502", "503", "504", "SlowDown",
                   "RequestTimeout", "InternalError", "ServiceUnavailable"}


def _valid_size(size):
    return type(size) is int and 0 <= size <= MAX_OBJECT_BYTES


def _matches_head(head, sha256, size):
    return head is not None and head.get("ContentLength") == size and head.get("Metadata", {}).get("sha256") == sha256


class ArchiveBudgetReached(TimeoutError):
    """Raised when archive synchronization reaches its per-run time budget."""


def validate_config(cfg):
    """Validate archive bucket, origin, project prefixes, and credential variable names."""
    reject_unknown_keys(cfg, ARCHIVE_KEYS, "archive")
    validate_s3_location(cfg, "Archive")
    origin = clean_https_url(cfg.get("public_origin"), "Archive public_origin")
    if not cfg["public_origin"].endswith("/"):
        raise ValueError("Archive public_origin must end with a slash")
    for key in ("access_key_env", "secret_key_env"):
        require_env_name(cfg.get(key), "Invalid archive credential variable name: " + key)
    for key, suffix in (("public_prefix", "public"), ("private_prefix", "private")):
        prefix = cfg.get(key)
        if not isinstance(prefix, str) or not re.fullmatch(r"[A-Za-z0-9_-]+/" + suffix + "/", prefix):
            raise ValueError("Archive prefixes must be project-specific public/private prefixes")
    if origin.path != "/" + cfg["public_prefix"]:
        raise ValueError("Archive public_origin must point to the public prefix")
    if cfg["public_prefix"].split("/")[0] != cfg["private_prefix"].split("/")[0]:
        raise ValueError("Archive public/private prefixes must belong to the same project")
    return cfg


class ArchiveStore:
    """Content-addressed public permalink archive and verified private object store in Spaces."""

    def __init__(self, publisher, client=None):
        self.publisher = publisher
        self.cfg = validate_config(publisher.cfg["archive"])
        self.bucket = self.cfg["bucket"]
        self.private_prefix = self.cfg["private_prefix"]
        self.public = Path(publisher.public)
        self.database = Path(publisher.database)
        self._deadline, self._uploaded = None, 0
        self.client = client or s3_client(
            publisher, self.cfg, self.cfg["access_key_env"], self.cfg["secret_key_env"]
        )

    @property
    def prefix(self) -> str:
        """Configured S3 key prefix for public archive objects and manifests."""
        return self.cfg["public_prefix"]

    def worker_vars(self, manifest_sha):
        """Return Worker environment bindings for the active archive origin and manifest digest."""
        if not SHA.fullmatch(str(manifest_sha)):
            raise ValueError("Invalid archive manifest identity")
        return {"ARCHIVE_ORIGIN": self.cfg["public_origin"], "ARCHIVE_MANIFEST_SHA": manifest_sha}

    def _budget(self):
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise ArchiveBudgetReached("Archive synchronization budget reached")

    @contextmanager
    def budget(self, seconds):
        """Enforce a monotonic deadline across archive storage and copy operations."""
        self._deadline = time.monotonic() + seconds
        try:
            yield
        finally:
            self._deadline = None

    def _database(self, shared=False):
        return open_db(self.database, shared=shared)

    def _retry(self, action, *, budgeted=True):
        for attempt in range(self.publisher.net["attempts"]):
            if budgeted:
                self._budget()
            try:
                return action()
            except (ClientError, *TRANSIENT_ERRORS) as exc:
                if (isinstance(exc, ClientError) and str(exc.response.get("Error", {}).get("Code")) not in RETRYABLE_CODES) or attempt + 1 == self.publisher.net["attempts"]:
                    raise
            self.publisher.delay(attempt)

    def _call(self, method, **kwargs):
        return self._retry(lambda: getattr(self.client, method)(Bucket=self.bucket, **kwargs))

    def _head(self, key):
        try:
            return self._call("head_object", Key=key)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) in MISSING_OBJECT_CODES:
                return None
            raise

    @staticmethod
    def _generation(db):
        if get_meta(db, "publication_build_pending") == "1":
            raise ValueError("Public build has not completed")
        generation = get_meta(db, "publication_generation")
        if generation is None:
            raise ValueError("Build the incremental public catalog before publishing")
        return str(generation)

    def _initialize(self):
        destination = hashlib.sha256(str((self.cfg["endpoint_url"], self.cfg["bucket"], self.prefix)).encode()).hexdigest()
        with self._database() as db:
            self._generation(db)
            db.executescript(
                "DROP TABLE IF EXISTS archive_shards; DROP TABLE IF EXISTS archive_manifest_shards;"
                "CREATE INDEX IF NOT EXISTS public_artifacts_pending ON public_artifacts(path) "
                "WHERE uploaded_sha256 IS NULL OR uploaded_sha256 != sha256;"
            )
            dest_changed = get_meta(db, "archive_destination") != destination
            if dest_changed or get_meta(db, "archive_manifest_format") != MANIFEST_FORMAT:
                with db:
                    if dest_changed:
                        db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL")
                    set_meta(db, archive_destination=destination, archive_manifest_sha256="",
                             archive_manifest_generation="", archive_manifest_format=MANIFEST_FORMAT)

    @staticmethod
    def _pending(db):
        return db.execute("SELECT COUNT(*) FROM public_artifacts WHERE " + PENDING).fetchone()[0]

    def _same_generation(self, db, generation):
        return self._generation(db) == generation and not self._pending(db)

    def _ready(self, db, generation):
        return self._same_generation(db, generation) and get_meta(db, "archive_manifest_generation") == generation

    def _sync_status(self, db, *, generation=None, ready=False, manifest_sha=None, **extra):
        return {"generation": self._generation(db) if generation is None else generation, "ready": bool(ready),
                "uploaded": self._uploaded, "pending": 0 if ready else self._pending(db), "manifest_sha256": manifest_sha, **extra}

    def _file(self, relative):
        if not public_path_allowed(relative):
            raise ValueError("Unexpected public file: " + str(relative))
        if self.public.is_symlink() or not self.public.is_dir():
            raise ValueError("Unsafe public archive directory")
        path = self.public / relative
        current = path
        while current != self.public:
            if current.is_symlink():
                raise ValueError("Public archive paths may not contain symlinks")
            current = current.parent
        if not path.is_file():
            raise ValueError("Missing public archive artifact: " + relative)
        return path

    def _copy(self, row, destination):
        if not SHA.fullmatch(str(row["sha256"])) or not _valid_size(row["size"]):
            raise ValueError("Invalid public artifact metadata")
        path = self._file(row["path"])
        secrets = [value.encode() for value in self.publisher.secret_values()]
        overlap = max((len(value) - 1 for value in secrets), default=0)
        previous, digest, size = b"", hashlib.sha256(), 0
        destination.parent.mkdir(parents=True, exist_ok=True)
        with path.open("rb") as source, destination.open("xb") as target:
            while chunk := source.read(CHUNK):
                self._budget()
                size += len(chunk)
                if size > row["size"]:
                    raise ValueError("Public artifact changed during snapshot")
                window = previous + chunk
                if any(secret in window for secret in secrets):
                    raise ValueError("Credential detected in public archive artifact")
                previous = window[-overlap:] if overlap else b""
                digest.update(chunk)
                target.write(chunk)
        if size != row["size"] or digest.hexdigest() != row["sha256"]:
            raise ValueError("Public artifact hash differs from its catalog")

    def _acl(self, key):
        acl = self._call("get_object_acl", Key=key)
        owner = acl.get("Owner", {}).get("ID")
        if not owner or not acl.get("Grants"):
            raise ValueError("Archive object ACL cannot be verified")
        for grant in acl["Grants"]:
            who = grant.get("Grantee", {})
            if who.get("Type") != "CanonicalUser" or who.get("ID") != owner or grant.get("Permission") != "FULL_CONTROL":
                raise ValueError("Unexpected archive object ACL")

    def verify_object(self, key, sha256, size):
        """Confirm stored object length, SHA-256 metadata, owner ACL, and streamed content hash."""
        if not SHA.fullmatch(str(sha256)) or not _valid_size(size):
            raise ValueError("Invalid archive verification bounds")
        if not _matches_head(self._head(key), sha256, size):
            raise ValueError("Archive object metadata verification failed")
        self._acl(key)
        def download():
            """Stream and verify object bytes against the expected size and SHA-256."""
            result = self.client.get_object(Bucket=self.bucket, Key=key)
            digest, count = hashlib.sha256(), 0
            with closing(result["Body"]) as body:
                while chunk := body.read(CHUNK):
                    self._budget()
                    count += len(chunk)
                    if count > size:
                        raise ValueError("Archive object exceeds verified length")
                    digest.update(chunk)
            if count != size or digest.hexdigest() != sha256:
                raise ValueError("Archive object content verification failed")
        self._retry(download)

    def _put(self, path, key, sha256, size, *, public, mime):
        md5 = file_md5(path)
        def upload():
            """Upload one object with explicit length, Content-MD5, ACL, and cache policy."""
            with path.open("rb") as stream:
                return self.client.put_object(
                    Bucket=self.bucket, Key=key, Body=stream, ContentLength=size,
                    ContentMD5=base64.b64encode(md5.digest()).decode(),
                    ContentType=mime, ACL="public-read" if public else "private",
                    CacheControl="public, max-age=31536000, immutable" if public else "no-store",
                    Metadata={"sha256": sha256},
                )
        self._retry(upload)

    def put_private(self, path, key, sha256):
        """Upload an immutable private gzipped bundle and verify both its contents and anonymous denial."""
        path = Path(path)
        if not key.startswith(self.private_prefix) or ".." in key.split("/") or path.is_symlink() or not path.is_file():
            raise ValueError("Unsafe private archive object")
        size = path.stat().st_size
        if size > MAX_OBJECT_BYTES:
            raise ValueError("Private archive object exceeds size budget")
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != sha256:
                raise ValueError("Private archive source hash changed")
        old = self._head(key)
        if old is not None:
            if not _matches_head(old, sha256, size):
                raise ValueError("Refusing to overwrite immutable private archive object")
            self.verify_object(key, sha256, size)
            verify_private_object(self.publisher, self.cfg["endpoint_url"], self.bucket, key, "Private archive object")
            return {"key": key, "sha256": sha256, "size": size}
        attempted = False
        def delete():
            """Remove a newly uploaded private object without consulting the run time budget."""
            nonlocal attempted
            attempted = True
            try:
                self._retry(lambda: self.client.delete_object(Bucket=self.bucket, Key=key), budgeted=False)
            except ClientError as exc:
                if str(exc.response.get("Error", {}).get("Code")) not in MISSING_OBJECT_CODES:
                    raise
        try:
            self._put(path, key, sha256, size, public=False, mime="application/gzip")
            self.verify_object(key, sha256, size)
            verify_private_object(
                self.publisher, self.cfg["endpoint_url"], self.bucket, key, "Private archive object", delete=delete
            )
        except BaseException as failure:
            if not attempted:
                try:
                    delete()
                except Exception as cleanup:
                    raise RuntimeError(
                        f"Private archive upload failed and {key} could not be removed: "
                        f"{self.publisher.clean_error(failure)}; {self.publisher.clean_error(cleanup)}"
                    ) from failure
            raise
        return {"key": key, "sha256": sha256, "size": size}

    def _public_immutable(self, path, key, sha256, mime="application/octet-stream"):
        size, old = path.stat().st_size, self._head(key)
        if old is None:
            self._put(path, key, sha256, size, public=True, mime=mime)
        elif not _matches_head(old, sha256, size) or old.get("ETag", "").strip('"') != file_md5(path).hexdigest():
            raise ValueError("Immutable public archive object changed outside this publisher")
        problem = self.publisher.unsigned_read_problem(self.cfg["public_origin"] + key.removeprefix(self.prefix), True)
        if problem:
            raise ValueError(f"Public archive object {key} {problem}")

    def _prepare_manifest(self, generation, directory):
        """Upload one gzipped manifest for this generation's articles, or return None if generation changed."""
        with self._database(shared=True) as db:
            if not self._same_generation(db, generation):
                return None
            uploaded_sha = get_meta(db, "archive_manifest_sha256")
            entries = {}
            for path, sha, size, ctype in db.execute(
                "SELECT path,sha256,size,content_type FROM public_artifacts WHERE path GLOB 'news/*' ORDER BY path"
            ):
                self._budget()
                entries[path] = {
                    "key": f"objects/{sha}", "sha256": sha, "size": size,
                    "content_type": ctype or ("text/html; charset=utf-8" if path.endswith(".html") else "application/json; charset=utf-8"),
                }
        raw = canonical_json({"format": MANIFEST_FORMAT, "entries": entries}).encode("utf-8")
        if len(raw) > MAX_MANIFEST_RAW_BYTES:
            raise ValueError("Archive manifest exceeds its decompressed size limit")
        body = gzip.compress(raw, mtime=0)
        if len(body) > MAX_MANIFEST_BYTES:
            raise ValueError("Archive manifest exceeds its size limit")
        manifest_sha = hashlib.sha256(body).hexdigest()
        if uploaded_sha != manifest_sha:
            manifest_path = directory / "manifest.json.gz"
            manifest_path.write_bytes(body)
            self._public_immutable(manifest_path, f"{self.prefix}manifests/{manifest_sha}.json.gz", manifest_sha, "application/gzip")
        with self._database() as db, db:
            if not self._same_generation(db, generation):
                return None
            set_meta(db, archive_manifest_sha256=manifest_sha, archive_manifest_generation=generation)
        return manifest_sha

    def _sync(self):
        self._initialize()
        with tempfile.TemporaryDirectory(prefix="archive-", dir=self.publisher.state) as temporary:
            snapshot = Path(temporary)
            with self._database(shared=True) as db:
                generation = self._generation(db)
                if self._ready(db, generation):
                    return self._sync_status(db, generation=generation, ready=True, manifest_sha=get_meta(db, "archive_manifest_sha256"))
                rows = db.execute("SELECT path,sha256,size FROM public_artifacts WHERE " + PENDING + " ORDER BY path LIMIT ?",
                                  (MAX_UPLOADS_PER_RUN,)).fetchall()
                selected, total = [], 0
                for row in map(dict, rows):
                    if not _valid_size(row["size"]):
                        raise ValueError("Public archive object exceeds its size limit")
                    if total + row["size"] > MAX_UPLOAD_BYTES_PER_RUN:
                        break
                    self._copy(row, snapshot / row["path"])
                    total += row["size"]
                    selected.append(row)
            for row in selected:
                self._public_immutable(snapshot / row["path"], self.prefix + "objects/" + row["sha256"], row["sha256"])
                with self._database() as db, db:
                    db.execute("UPDATE public_artifacts SET uploaded_sha256=? WHERE path=? AND sha256=?",
                               (row["sha256"], row["path"], row["sha256"]))
                    set_meta(db, archive_manifest_generation="")
                self._uploaded += 1
            manifest_sha = self._prepare_manifest(generation, snapshot)
        with self._database(shared=True) as db:
            return self._sync_status(db, generation=generation, ready=bool(manifest_sha) and self._same_generation(db, generation), manifest_sha=manifest_sha)

    def sync(self):
        """Upload pending public article objects and refresh the generation manifest within the time budget."""
        self._uploaded = 0
        try:
            with self.budget(SYNC_BUDGET_SECONDS):
                return self._sync()
        except ArchiveBudgetReached:
            with self._database(shared=True) as db:
                return self._sync_status(db, budget_reached=True)

    def snapshot(self, destination, generation):
        """Copy this generation's hot files into destination and return the full publication manifest."""
        destination.mkdir(parents=True)
        manifest = {}
        with self._database(shared=True) as db:
            if not self._ready(db, str(generation)):
                raise ValueError("Public archive is not ready for this generation")
            for row in map(dict, db.execute("SELECT path,sha256,size FROM public_artifacts")):
                if not row["path"].startswith("news/"):
                    self._copy(row, destination / row["path"])
                elif not ARTICLE.fullmatch(row["path"]):
                    continue
                manifest[row["path"]] = row["sha256"]
        require_public_files(manifest)
        return manifest
