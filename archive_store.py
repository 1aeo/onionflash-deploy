"""Immutable article objects and private evidence in Spaces.

Article permalinks (news/<id>/index.html and story.json) are uploaded once as
content-addressed public objects, routed by a sharded manifest built from the
core's ledger. Every other public file ships in the Worker's own bundle. The
caller holds the publisher's job lock. Database locks protect bounded local
snapshots; network operations never hold that lock. The public prefix has one
writer. Readers (including the Worker) never receive storage credentials.
"""

from __future__ import annotations

import base64
from contextlib import closing, contextmanager
import hashlib
import itertools
import json
import re
import sqlite3
import tempfile
import time
from pathlib import Path
from urllib.parse import quote, urlsplit

from publish import ARTICLE, lock, public_files, public_path_allowed

SHA = re.compile(r"[a-f0-9]{64}\Z")
CHUNK = 65536
# Bounds of one synchronization. worker.mjs reads manifests of up to MAX_MANIFEST_BYTES.
MAX_UPLOADS_PER_RUN, MAX_UPLOAD_BYTES_PER_RUN, SYNC_BUDGET_SECONDS = 500, 512 * 1024**2, 180
MAX_OBJECT_BYTES, MAX_MANIFEST_BYTES, SHARD_CHARS = 256 * 1024**2, 8 * 1024**2, 2
# Article rows whose bytes are not in storage yet.
PENDING = "path GLOB 'news/*' AND (uploaded_sha256 IS NULL OR uploaded_sha256 != sha256)"


class ArchiveBudgetReached(TimeoutError):
    pass


def validate_config(cfg):
    for key in ("endpoint_url", "public_origin"):
        parsed = urlsplit(cfg[key])
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Archive URLs must be clean HTTPS URLs")
    if not cfg["public_origin"].endswith("/"):
        raise ValueError("Archive public_origin must end with a slash")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", cfg["bucket"]):
        raise ValueError("Invalid archive bucket")
    for key, suffix in (("public_prefix", "public"), ("private_prefix", "private")):
        if not re.fullmatch(r"[A-Za-z0-9_-]+/" + suffix + "/", cfg[key]):
            raise ValueError("Archive prefixes must be project-specific public/private prefixes")
    if urlsplit(cfg["public_origin"]).path != "/" + cfg["public_prefix"]:
        raise ValueError("Archive public_origin must point to the public prefix")
    if cfg["public_prefix"].split("/")[0] != cfg["private_prefix"].split("/")[0]:
        raise ValueError("Archive public/private prefixes must belong to the same project")
    return cfg


class ArchiveStore:
    def __init__(self, publisher, client=None):
        self.publisher = publisher
        self.cfg = validate_config(publisher.cfg["archive"])
        self.bucket = self.cfg["bucket"]
        self.private_prefix = self.cfg["private_prefix"]
        self.public = Path(publisher.public)
        self.database = Path(publisher.database)
        self._deadline = None
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client(
                "s3", endpoint_url=self.cfg["endpoint_url"], region_name=self.cfg["region"],
                aws_access_key_id=publisher.secret(self.cfg["access_key_env"]),
                aws_secret_access_key=publisher.secret(self.cfg["secret_key_env"]),
                config=Config(connect_timeout=publisher.net["timeout_seconds"],
                              read_timeout=publisher.net["timeout_seconds"],
                              retries={"mode": "standard", "total_max_attempts": 1},
                              request_checksum_calculation="when_required",
                              response_checksum_validation="when_required"),
            )
        self.client = client

    def worker_vars(self, manifest_sha):
        if not SHA.fullmatch(str(manifest_sha)):
            raise ValueError("Invalid archive manifest identity")
        return {"ARCHIVE_ORIGIN": self.cfg["public_origin"], "ARCHIVE_MANIFEST_SHA": manifest_sha}

    def _budget(self):
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise ArchiveBudgetReached("Archive synchronization budget reached")

    @contextmanager
    def budget(self, seconds):
        """A cooperative deadline for this store's storage work."""
        self._deadline = time.monotonic() + seconds
        try:
            yield
        finally:
            self._deadline = None

    @contextmanager
    def _database(self, shared=False):
        uri = self.database.resolve(strict=True).as_uri() + ("?mode=ro" if shared else "?mode=rw")
        with lock(str(self.database) + ".lock", shared=shared), closing(sqlite3.connect(uri, uri=True)) as db:
            db.row_factory = sqlite3.Row
            yield db

    def _retry(self, action):
        from botocore.exceptions import (ClientError, ConnectTimeoutError, ConnectionClosedError,
                                        EndpointConnectionError, IncompleteReadError,
                                        ReadTimeoutError, ResponseStreamingError)
        for attempt in range(self.publisher.net["attempts"]):
            self._budget()
            try:
                return action()
            except ClientError as exc:
                retryable = str(exc.response.get("Error", {}).get("Code")) in {
                    "408", "429", "500", "502", "503", "504", "SlowDown",
                    "RequestTimeout", "InternalError", "ServiceUnavailable",
                }
                if not retryable or attempt + 1 == self.publisher.net["attempts"]:
                    raise
            except (ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError,
                    IncompleteReadError, ReadTimeoutError, ResponseStreamingError):
                if attempt + 1 == self.publisher.net["attempts"]:
                    raise
            self.publisher.delay(attempt)

    def _call(self, method, **kwargs):
        return self._retry(lambda: getattr(self.client, method)(Bucket=self.bucket, **kwargs))

    def _head(self, key):
        from botocore.exceptions import ClientError
        try:
            return self._call("head_object", Key=key)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    @staticmethod
    def _generation(db):
        pending = db.execute("SELECT value FROM metadata WHERE key='publication_build_pending'").fetchone()
        if pending and pending[0] == "1":
            raise ValueError("Public build has not completed")
        row = db.execute("SELECT value FROM metadata WHERE key='publication_generation'").fetchone()
        if row is None:
            raise ValueError("Build the incremental public catalog before publishing")
        return str(row[0])

    @staticmethod
    def _meta(db, key):
        row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    @staticmethod
    def _set_meta(db, **values):
        db.executemany("INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", values.items())

    def _initialize(self):
        destination = hashlib.sha256(str((self.cfg["endpoint_url"], self.bucket, self.cfg["public_prefix"])).encode()).hexdigest()
        with self._database() as db:
            self._generation(db)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS archive_shards (shard TEXT PRIMARY KEY, sha256 TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS public_artifacts_pending ON public_artifacts(path)
                    WHERE uploaded_sha256 IS NULL OR uploaded_sha256 != sha256;
            """)
            if self._meta(db, "archive_destination") != destination:
                with db:  # A new destination starts empty: every object, shard and root uploads again.
                    db.execute("DELETE FROM archive_shards")
                    db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL")
                    self._set_meta(db, archive_destination=destination, archive_manifest_sha256="", archive_manifest_generation="")

    @staticmethod
    def _pending(db):
        return db.execute("SELECT COUNT(*) FROM public_artifacts WHERE " + PENDING).fetchone()[0]

    def _same_generation(self, db, generation):
        """The core still publishes this generation, and every article of it is uploaded."""
        return self._generation(db) == generation and not self._pending(db)

    def _ready(self, db, generation):
        return self._same_generation(db, generation) and self._meta(db, "archive_manifest_generation") == generation

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
        if not SHA.fullmatch(str(row["sha256"])) or type(row["size"]) is not int or not 0 <= row["size"] <= MAX_OBJECT_BYTES:
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
        if not SHA.fullmatch(str(sha256)) or type(size) is not int or not 0 <= size <= MAX_OBJECT_BYTES:
            raise ValueError("Invalid archive verification bounds")
        head = self._head(key)
        if head is None or head.get("ContentLength") != size or head.get("Metadata", {}).get("sha256") != sha256:
            raise ValueError("Archive object metadata verification failed")
        self._acl(key)
        def download():
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
        return head

    @staticmethod
    def _md5(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, lambda: hashlib.md5(usedforsecurity=False))

    def _put(self, path, key, sha256, size, *, public, mime):
        md5 = self._md5(path)
        def upload():
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
        if old is None:
            self._put(path, key, sha256, size, public=False, mime="application/gzip")
        elif old.get("Metadata", {}).get("sha256") != sha256 or old.get("ContentLength") != size:
            raise ValueError("Refusing to overwrite immutable private archive object")
        self.verify_object(key, sha256, size)
        # A bucket policy can expose an object whose ACL is private.
        problem = self.publisher.unsigned_read_problem(self.cfg["endpoint_url"].rstrip("/") + "/" + self.bucket + "/" + quote(key))
        if problem:
            if old is None:  # An existing object may back earlier archive references.
                try:
                    self._call("delete_object", Key=key)
                except Exception as cleanup:  # the publisher redacts the message when it reports it
                    raise ValueError(f"Private archive object {key} {problem}, and deleting it failed: "
                                     f"{type(cleanup).__name__}: {cleanup}") from cleanup
            raise ValueError(f"Private archive object {key} {problem}")
        return {"key": key, "sha256": sha256, "size": size}

    def _public_immutable(self, path, key, sha256, mime="application/octet-stream"):
        # Storage checks each upload's Content-MD5, so a new object needs no read-back. An
        # existing one must match in size, sha256 metadata and ETag (its MD5). Either way,
        # anonymous readers must reach it: a bucket policy or a later change can deny them.
        size, old = path.stat().st_size, self._head(key)
        if old is None:
            self._put(path, key, sha256, size, public=True, mime=mime)
        # This publisher's single-part uploads have a plain MD5 ETag. An object another tool
        # uploaded in parts has a composite one and fails closed here, as changed.
        elif (old.get("Metadata", {}).get("sha256") != sha256 or old.get("ContentLength") != size
                or old.get("ETag", "").strip('"') != self._md5(path).hexdigest()):
            raise ValueError("Immutable public archive object changed outside this publisher")
        # Probe the URL the Worker reads, which validate_config ties to the public prefix.
        problem = self.publisher.unsigned_read_problem(self.cfg["public_origin"] + key.removeprefix(self.cfg["public_prefix"]), True)
        if problem:
            raise ValueError(f"Public archive object {key} {problem}")

    def _prepare_manifest(self, generation, directory):
        """Upload the changed shards and the root that route this generation's
        articles, built from the core's ledger. None if the generation changed."""
        def document(name, value):  # Its content address, and its file when not uploaded yet.
            body = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
            if len(body) > MAX_MANIFEST_BYTES:
                raise ValueError("Archive manifest exceeds its size limit")
            sha = hashlib.sha256(body).hexdigest()
            if uploaded.get(name) != sha:
                (directory / name).write_bytes(body)
            return sha
        def upload(name, sha):
            self._public_immutable(directory / name, self.cfg["public_prefix"] + "manifests/" + sha + ".json", sha, "application/json")
        shards = {}
        with self._database(shared=True) as db:
            if not self._same_generation(db, generation):
                return None
            uploaded = dict(db.execute("SELECT shard,sha256 FROM archive_shards"))
            uploaded["root"] = self._meta(db, "archive_manifest_sha256")
            db.create_function("shard", 1, lambda path: hashlib.sha256(path.encode()).hexdigest()[:SHARD_CHARS])
            rows = db.execute("SELECT shard(path),path,sha256,size FROM public_artifacts WHERE path GLOB 'news/*' ORDER BY 1")
            for shard, group in itertools.groupby(rows, key=lambda row: row[0]):
                self._budget()
                shards[shard] = document(shard, {"schema": 1, "entries": {
                    path: {"sha256": sha, "size": size} for _, path, sha, size in group}})
        for shard, sha in shards.items():
            if uploaded.get(shard) != sha:
                upload(shard, sha)
                with self._database() as db, db:  # Kept per shard, so a run cut off by its budget keeps its progress.
                    db.execute("INSERT INTO archive_shards(shard,sha256) VALUES(?,?) "
                               "ON CONFLICT(shard) DO UPDATE SET sha256=excluded.sha256", (shard, sha))
        root = document("root", {"schema": 1, "shard_chars": SHARD_CHARS, "shards": shards})
        if uploaded["root"] != root:
            upload("root", root)
        with self._database() as db, db:
            if not self._same_generation(db, generation):
                return None
            self._set_meta(db, archive_manifest_sha256=root, archive_manifest_generation=generation)
        return root

    def _sync(self):
        self._initialize()
        with tempfile.TemporaryDirectory(prefix="archive-", dir=self.publisher.state) as temporary:
            snapshot = Path(temporary)
            with self._database(shared=True) as db:
                generation = self._generation(db)
                if self._ready(db, generation):
                    return {"generation": generation, "uploaded": 0, "pending": 0, "ready": True,
                            "manifest_sha256": self._meta(db, "archive_manifest_sha256")}
                rows = db.execute("SELECT path,sha256,size FROM public_artifacts WHERE " + PENDING + " ORDER BY path LIMIT ?",
                                  (MAX_UPLOADS_PER_RUN,)).fetchall()
                selected, total = [], 0
                for row in map(dict, rows):
                    if type(row["size"]) is not int or not 0 <= row["size"] <= MAX_OBJECT_BYTES:
                        raise ValueError("Public archive object exceeds its size limit")
                    if total + row["size"] > MAX_UPLOAD_BYTES_PER_RUN:
                        break
                    self._copy(row, snapshot / row["path"])
                    total += row["size"]
                    selected.append(row)
            for row in selected:
                self._public_immutable(snapshot / row["path"], self.cfg["public_prefix"] + "objects/" + row["sha256"], row["sha256"])
                with self._database() as db, db:
                    # Record the uploaded bytes: a direct core build may have rendered the article again meanwhile.
                    db.execute("UPDATE public_artifacts SET uploaded_sha256=? WHERE path=? AND sha256=?",
                               (row["sha256"], row["path"], row["sha256"]))
                self._uploaded += 1
            manifest_sha = self._prepare_manifest(generation, snapshot)
        with self._database(shared=True) as db:
            pending = self._pending(db)
            ready = bool(manifest_sha) and pending == 0 and self._generation(db) == generation
        return {"generation": generation, "uploaded": self._uploaded, "pending": pending, "ready": ready,
                "manifest_sha256": manifest_sha}

    def sync(self):
        self._uploaded = 0
        try:
            with self.budget(SYNC_BUDGET_SECONDS):
                return self._sync()
        except ArchiveBudgetReached:
            with self._database(shared=True) as db:
                return {"generation": self._generation(db), "pending": self._pending(db), "uploaded": self._uploaded,
                        "ready": False, "manifest_sha256": None, "budget_reached": True}

    def snapshot(self, destination, generation):
        """Copy this generation's hot files into a new directory, checking each one's
        path, ledger hash and absence of credentials. Return the publication manifest:
        those files and the article permalinks its root routes, with their hashes."""
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
        missing = public_files() - manifest.keys()
        if missing:
            raise ValueError("Missing required public file: " + min(missing))
        return manifest
