"""Incremental immutable public objects and private evidence in Spaces.

The caller holds the publisher's job lock. Database locks protect bounded local
snapshots; network operations never hold that lock. The public prefix has one
writer. Readers (including the Worker) never receive storage credentials.
"""

from __future__ import annotations

import base64
from contextlib import closing, contextmanager
import hashlib
import re
import sqlite3
import tempfile
import time
from pathlib import Path
from urllib.parse import quote, urlsplit

from onionflash.render import ASSET_FILES
from onionflash.schema import CATEGORIES

HOT_FILES = frozenset({"index.html", "_headers", *ASSET_FILES})
SHA = re.compile(r"[a-f0-9]{64}\Z")
CHUNK = 65536


class ArchiveBudgetReached(TimeoutError):
    pass


def archive_path_allowed(path):
    return isinstance(path, str) and bool(
        path in {"feed.xml", "feed.json"}
        or re.fullmatch(r"news/[A-Za-z0-9_-]{1,64}/(?:index\.html|story\.json)", path)
        or re.fullmatch(r"page/[1-9][0-9]{0,8}/index\.html", path)
        or path in {f"tags/{topic}.xml" for topic in CATEGORIES}
        or topic_path_allowed(path)
    )


def topic_path_allowed(path):
    if not isinstance(path, str):
        return False
    match = re.fullmatch(r"topics/([a-z][a-z0-9-]{0,39})/(?:page/[1-9][0-9]{0,8}/)?index\.html", path)
    return bool(match and match[1] in CATEGORIES)


def content_type(path):
    if path.endswith(".html"):
        return "text/html; charset=utf-8"
    if path.endswith(".xml"):
        return "application/xml; charset=utf-8"
    if path == "feed.json":
        return "application/feed+json; charset=utf-8"
    return "application/json; charset=utf-8"


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
    bounds = {
        "max_uploads_per_run": (1, 10000),
        "max_upload_bytes_per_run": (1024, 20 * 1024**3),
        "max_object_bytes": (1024, 5 * 1024**3),
        "cache_ttl_seconds": (0, 86400),
        "immutable_cache_ttl_seconds": (60, 31536000),
        "sync_budget_seconds": (1, 3600),
        "shard_prefix_chars": (1, 4),
        "max_manifest_bytes": (1024, 16777216),
        "worker_timeout_ms": (100, 30000),
        "worker_retry_attempts": (1, 5),
        "worker_backoff_ms": (0, 5000),
        "worker_backoff_cap_ms": (0, 10000),
    }
    for key, (low, high) in bounds.items():
        if type(cfg.get(key)) is not int or not low <= cfg[key] <= high:
            raise ValueError("Invalid archive setting: " + key)
    if cfg["max_upload_bytes_per_run"] < cfg["max_object_bytes"]:
        raise ValueError("Archive per-run byte budget must fit one maximum-size object")
    if cfg["max_manifest_bytes"] > cfg["max_object_bytes"]:
        raise ValueError("Archive manifests must fit the object size limit")
    if cfg["worker_backoff_cap_ms"] < cfg["worker_backoff_ms"]:
        raise ValueError("Archive Worker backoff cap must cover its base")
    return cfg


class ArchiveStore:
    def __init__(self, publisher, client=None, lock_factory=None):
        self.publisher = publisher
        self.cfg = validate_config(publisher.cfg["archive"])
        self.bucket = self.cfg["bucket"]
        self.private_prefix = self.cfg["private_prefix"]
        self.public = Path(publisher.public)
        self.database = Path(publisher.database)
        self._deadline = None
        if lock_factory is None:
            from publish import lock
            lock_factory = lock
        self.lock = lock_factory
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
        return {
            "ARCHIVE_ORIGIN": self.cfg["public_origin"],
            "ARCHIVE_MANIFEST_SHA": manifest_sha,
            "ARCHIVE_TOPICS": ",".join(CATEGORIES),
            "ARCHIVE_CACHE_TTL": str(self.cfg["cache_ttl_seconds"]),
            "ARCHIVE_OBJECT_CACHE_TTL": str(self.cfg["immutable_cache_ttl_seconds"]),
            "ARCHIVE_MAX_MANIFEST_BYTES": str(self.cfg["max_manifest_bytes"]),
            "ARCHIVE_TIMEOUT_MS": str(self.cfg["worker_timeout_ms"]),
            "ARCHIVE_RETRY_ATTEMPTS": str(self.cfg["worker_retry_attempts"]),
            "ARCHIVE_BACKOFF_MS": str(self.cfg["worker_backoff_ms"]),
            "ARCHIVE_BACKOFF_CAP_MS": str(self.cfg["worker_backoff_cap_ms"]),
        }

    def _budget(self):
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise ArchiveBudgetReached("Archive synchronization budget reached")

    @contextmanager
    def budget(self, seconds):
        """Share a cooperative deadline without depending on the app CLI."""
        previous = self._deadline
        deadline = time.monotonic() + seconds
        self._deadline = min(previous, deadline) if previous is not None else deadline
        try:
            yield
        finally:
            self._deadline = previous

    @contextmanager
    def _database(self, shared=False):
        uri = self.database.resolve(strict=True).as_uri() + ("?mode=ro" if shared else "?mode=rw")
        with self.lock(str(self.database) + ".lock", shared=shared), closing(sqlite3.connect(uri, uri=True)) as db:
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
                CREATE TABLE IF NOT EXISTS archive_index (
                    path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, size INTEGER NOT NULL,
                    content_type TEXT NOT NULL, shard TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS archive_index_shard ON archive_index(shard,path);
                CREATE TABLE IF NOT EXISTS archive_shards (
                    shard TEXT PRIMARY KEY, sha256 TEXT, dirty INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS archive_dirty_paths (path TEXT PRIMARY KEY);
                CREATE INDEX IF NOT EXISTS public_artifacts_pending ON public_artifacts(path)
                    WHERE uploaded_sha256 IS NULL OR uploaded_sha256 != sha256;
                CREATE TRIGGER IF NOT EXISTS archive_artifact_insert AFTER INSERT ON public_artifacts
                    WHEN NEW.path NOT IN ('index.html','style.css','app.js','mark.svg','_headers')
                    BEGIN INSERT INTO archive_dirty_paths(path) SELECT NEW.path WHERE NOT EXISTS (SELECT 1 FROM archive_dirty_paths WHERE path=NEW.path); END;
                CREATE TRIGGER IF NOT EXISTS archive_artifact_update AFTER UPDATE OF sha256,size,content_type ON public_artifacts
                    WHEN NEW.path NOT IN ('index.html','style.css','app.js','mark.svg','_headers')
                    AND (OLD.sha256!=NEW.sha256 OR OLD.size!=NEW.size OR OLD.content_type!=NEW.content_type)
                    BEGIN INSERT INTO archive_dirty_paths(path) SELECT NEW.path WHERE NOT EXISTS (SELECT 1 FROM archive_dirty_paths WHERE path=NEW.path); END;
                DROP TRIGGER IF EXISTS archive_page_delete;
                CREATE TRIGGER archive_page_delete AFTER DELETE ON public_artifacts
                    WHEN OLD.path LIKE 'page/%' OR OLD.path LIKE 'topics/%'
                    BEGIN INSERT INTO archive_dirty_paths(path) SELECT OLD.path WHERE NOT EXISTS (SELECT 1 FROM archive_dirty_paths WHERE path=OLD.path); END;
            """)
            with db:
                if self._meta(db, "archive_destination") != destination:
                    db.execute("DELETE FROM archive_index")
                    db.execute("DELETE FROM archive_shards")
                    db.execute("DELETE FROM archive_dirty_paths")
                    db.execute("UPDATE public_artifacts SET uploaded_sha256=NULL")
                    db.execute("INSERT OR IGNORE INTO archive_dirty_paths SELECT path FROM public_artifacts WHERE path NOT IN ('index.html','style.css','app.js','mark.svg','_headers')")
                    self._set_meta(db, archive_destination=destination, archive_manifest_sha256="", archive_manifest_generation="")
                if self._meta(db, "archive_queue_version") != "3":
                    db.execute("INSERT OR IGNORE INTO archive_dirty_paths SELECT path FROM public_artifacts WHERE path NOT IN ('index.html','style.css','app.js','mark.svg','_headers')")
                    db.execute("INSERT OR IGNORE INTO archive_dirty_paths SELECT a.path FROM archive_index a LEFT JOIN public_artifacts p ON p.path=a.path WHERE p.path IS NULL AND (a.path LIKE 'page/%' OR a.path LIKE 'topics/%')")
                    self._set_meta(db, archive_queue_version="3")
                shard_chars = str(self.cfg["shard_prefix_chars"])
                if self._meta(db, "archive_shard_chars") != shard_chars:
                    for (path,) in db.execute("SELECT path FROM archive_index ORDER BY path"):
                        shard = hashlib.sha256(path.encode()).hexdigest()[:int(shard_chars)]
                        db.execute("UPDATE archive_index SET shard=? WHERE path=?", (shard, path))
                    db.execute("DELETE FROM archive_shards")
                    db.execute("INSERT INTO archive_shards(shard,dirty) SELECT DISTINCT shard,1 FROM archive_index")
                    self._set_meta(db, archive_shard_chars=shard_chars, archive_manifest_sha256="", archive_manifest_generation="")

    @staticmethod
    def _pending(db):
        placeholders = ",".join("?" for _ in HOT_FILES)
        return db.execute(
            f"SELECT COUNT(*) FROM public_artifacts WHERE path NOT IN ({placeholders}) "
            "AND (uploaded_sha256 IS NULL OR uploaded_sha256 != sha256)", tuple(HOT_FILES),
        ).fetchone()[0]

    def _file(self, relative):
        if relative not in HOT_FILES and not archive_path_allowed(relative):
            raise ValueError("Unexpected public archive path: " + str(relative))
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
        if not SHA.fullmatch(str(row["sha256"])) or type(row["size"]) is not int or not 0 <= row["size"] <= self.cfg["max_object_bytes"]:
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
        if row["path"].endswith(".html"):
            from public_html import validate_static_html
            validate_static_html(destination)

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
        if not SHA.fullmatch(str(sha256)) or type(size) is not int or not 0 <= size <= self.cfg["max_object_bytes"]:
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
                    CacheControl=f"public, max-age={self.cfg['immutable_cache_ttl_seconds']}, immutable" if public else "no-store",
                    Metadata={"sha256": sha256},
                )
        self._retry(upload)

    def put_private(self, path, key, sha256=None):
        path = Path(path)
        if not key.startswith(self.private_prefix) or ".." in key.split("/") or path.is_symlink() or not path.is_file():
            raise ValueError("Unsafe private archive object")
        size = path.stat().st_size
        if size > self.cfg["max_object_bytes"]:
            raise ValueError("Private archive object exceeds size budget")
        with path.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if sha256 is not None and actual != sha256:
            raise ValueError("Private archive source hash changed")
        sha256 = actual
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

    def _public_immutable(self, path, key, sha256, size, mime="application/octet-stream"):
        # Storage checks each upload's Content-MD5, so a new object needs no read-back. An
        # existing one must match in size, sha256 metadata and ETag (its MD5). Either way,
        # anonymous readers must reach it: a bucket policy or a later change can deny them.
        old = self._head(key)
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

    def _upload_artifact(self, row, path):
        self._public_immutable(path, self.cfg["public_prefix"] + "objects/" + row["sha256"], row["sha256"], row["size"])

    def _same_generation(self, db, generation):
        return self._generation(db) == generation and self._pending(db) == 0

    def _write_manifest(self, path, fragments):
        digest, size = hashlib.sha256(), 0
        with path.open("xb") as stream:
            for fragment in fragments:
                self._budget()
                data = fragment.encode("utf-8")
                size += len(data)
                if size > self.cfg["max_manifest_bytes"]:
                    raise ValueError("Archive manifest exceeds configured size; increase shard_prefix_chars")
                digest.update(data)
                stream.write(data)
        return digest.hexdigest(), size

    def _invalidate_dirty_root(self, db):
        # This runs inside the same transaction that marks a shard dirty. If a
        # later upload fails, clean/new shards must not reuse the previous root.
        if db.execute("SELECT 1 FROM archive_shards WHERE dirty!=0 LIMIT 1").fetchone():
            self._set_meta(db, archive_manifest_sha256="", archive_manifest_generation="")

    def _root_document(self, db):
        import json
        shards = dict(db.execute("SELECT shard,sha256 FROM archive_shards ORDER BY shard"))
        return json.dumps({"schema": 1, "shard_chars": self.cfg["shard_prefix_chars"], "shards": shards}, separators=(",", ":"), sort_keys=True)

    def _prepare_manifest(self, generation, directory):
        import json
        with self._database() as db:
            if not self._same_generation(db, generation):
                return None
            with db:
                for path, shard in db.execute("SELECT a.path,a.shard FROM archive_dirty_paths d JOIN archive_index a ON a.path=d.path LEFT JOIN public_artifacts p ON p.path=d.path WHERE p.path IS NULL ORDER BY a.path"):
                    # Pagination URLs may become obsolete when page size grows;
                    # article permalinks and their immutable history never do.
                    if re.fullmatch(r"page/[1-9][0-9]{0,8}/index\.html", path) or topic_path_allowed(path):
                        db.execute("DELETE FROM archive_index WHERE path=?", (path,))
                        db.execute("INSERT INTO archive_shards(shard,dirty) VALUES(?,1) ON CONFLICT(shard) DO UPDATE SET dirty=1", (shard,))
                self._invalidate_dirty_root(db)
            changed = db.execute(
                "SELECT p.path,p.sha256,p.size,p.content_type FROM archive_dirty_paths d JOIN public_artifacts p ON p.path=d.path "
                "LEFT JOIN archive_index a ON a.path=p.path "
                "WHERE a.path IS NULL OR a.sha256!=p.sha256 OR a.size!=p.size OR a.content_type!=p.content_type ORDER BY p.path",
            )
            with db:
                for row in changed:
                    if not archive_path_allowed(row["path"]):
                        raise ValueError("Unexpected public archive path")
                    shard = hashlib.sha256(row["path"].encode()).hexdigest()[:self.cfg["shard_prefix_chars"]]
                    db.execute("INSERT INTO archive_index VALUES(?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,size=excluded.size,content_type=excluded.content_type,shard=excluded.shard",
                               (row["path"], row["sha256"], row["size"], row["content_type"], shard))
                    db.execute("INSERT INTO archive_shards(shard,dirty) VALUES(?,1) ON CONFLICT(shard) DO UPDATE SET dirty=1", (shard,))
                db.execute("DELETE FROM archive_dirty_paths")
                self._invalidate_dirty_root(db)
            dirty = [row[0] for row in db.execute("SELECT shard FROM archive_shards WHERE dirty=1 ORDER BY shard")]
            previous_root = self._meta(db, "archive_manifest_sha256")
            if not dirty and previous_root:
                # Bind the cached identity to the actual committed shard set,
                # including when recovering state produced by an older build.
                expected_root = hashlib.sha256(self._root_document(db).encode()).hexdigest()
                with db:
                    if previous_root == expected_root:
                        self._set_meta(db, archive_manifest_generation=generation)
                        return previous_root
                    self._set_meta(db, archive_manifest_sha256="", archive_manifest_generation="")
        for shard in dirty:
            path = directory / ("manifest-" + shard + ".json")
            with self._database(shared=True) as db:
                if not self._same_generation(db, generation):
                    return None
                def fragments():
                    yield '{"schema":1,"entries":{'
                    separator = ""
                    for relative, sha, size in db.execute("SELECT path,sha256,size FROM archive_index WHERE shard=? ORDER BY path", (shard,)):
                        yield separator + json.dumps(relative) + ":" + json.dumps({"sha256": sha, "size": size, "content_type": content_type(relative)}, separators=(",", ":"), sort_keys=True)
                        separator = ","
                    yield "}}"
                sha, size = self._write_manifest(path, fragments())
            self._public_immutable(path, self.cfg["public_prefix"] + "manifests/" + sha + ".json", sha, size, "application/json")
            with self._database() as db:
                if not self._same_generation(db, generation):
                    return None
                with db:
                    db.execute("UPDATE archive_shards SET sha256=?,dirty=0 WHERE shard=?", (sha, shard))
        path = directory / "manifest-root.json"
        with self._database(shared=True) as db:
            if not self._same_generation(db, generation) or db.execute("SELECT COUNT(*) FROM archive_shards WHERE dirty!=0").fetchone()[0]:
                return None
            sha, size = self._write_manifest(path, [self._root_document(db)])
        self._public_immutable(path, self.cfg["public_prefix"] + "manifests/" + sha + ".json", sha, size, "application/json")
        with self._database() as db:
            if not self._same_generation(db, generation):
                return None
            with db:
                self._set_meta(db, archive_manifest_sha256=sha, archive_manifest_generation=generation)
        return sha

    def _sync(self):
        self._initialize()
        uploaded = 0
        self._uploaded = 0
        manifest_sha = None
        with tempfile.TemporaryDirectory(prefix="archive-", dir=self.publisher.state) as temporary:
            snapshot = Path(temporary)
            with self._database(shared=True) as db:
                generation = self._generation(db)
                placeholders = ",".join("?" for _ in HOT_FILES)
                rows = db.execute(
                    f"SELECT * FROM public_artifacts WHERE path NOT IN ({placeholders}) "
                    "AND (uploaded_sha256 IS NULL OR uploaded_sha256 != sha256) "
                    "ORDER BY CASE WHEN path LIKE 'news/%' THEN 0 WHEN path LIKE 'page/%' THEN 1 ELSE 2 END,path LIMIT ?",
                    (*HOT_FILES, self.cfg["max_uploads_per_run"]),
                ).fetchall()
                selected, total = [], 0
                for raw in rows:
                    row = dict(raw)
                    if not archive_path_allowed(row["path"]):
                        raise ValueError("Unexpected public archive path")
                    if type(row["size"]) is not int or not 0 <= row["size"] <= self.cfg["max_object_bytes"]:
                        raise ValueError("Public archive object exceeds configured size")
                    if total + row["size"] > self.cfg["max_upload_bytes_per_run"]:
                        break
                    self._copy(row, snapshot / row["path"])
                    total += row["size"]
                    selected.append(row)
            for row in selected:
                self._budget()
                self._upload_artifact(row, snapshot / row["path"])
                with self._database() as db:
                    with db:
                        db.execute("UPDATE public_artifacts SET uploaded_sha256=? WHERE path=? AND sha256=?",
                                   (row["sha256"], row["path"], row["sha256"]))
                uploaded += 1
                self._uploaded = uploaded
            manifest_sha = self._prepare_manifest(generation, snapshot)
        with self._database(shared=True) as db:
            pending = self._pending(db)
            ready = bool(manifest_sha) and pending == 0 and self._generation(db) == generation
        return {"generation": generation, "uploaded": uploaded, "pending": pending, "ready": ready, "manifest_sha256": manifest_sha}

    def sync(self):
        try:
            with self.budget(self.cfg["sync_budget_seconds"]):
                return self._sync()
        except ArchiveBudgetReached:
            with self._database(shared=True) as db:
                return {"generation": self._generation(db), "pending": self._pending(db),
                        "uploaded": getattr(self, "_uploaded", 0), "ready": False,
                        "manifest_sha256": None, "budget_reached": True}

    def manifest(self, paths, generation=None):
        result = {}
        with self._database(shared=True) as db:
            current_generation = self._generation(db)
            if generation is not None and current_generation != str(generation):
                raise ValueError("Publication generation changed")
            if self._meta(db, "archive_manifest_generation") != current_generation or self._pending(db):
                raise ValueError("Archive routing manifest is not ready")
            for path in paths:
                if not archive_path_allowed(path):
                    raise ValueError("Unexpected verification path")
                row = db.execute("SELECT sha256 FROM archive_index WHERE path=?", (path,)).fetchone()
                if row is None or not SHA.fullmatch(str(row[0])):
                    raise ValueError("Public artifact has not been verified in Spaces")
                result[path] = row[0]
        return result

    def articles(self, generation):
        """Article permalinks the routing manifest of this generation maps."""
        with self._database(shared=True) as db:
            if self._generation(db) != str(generation) or self._meta(db, "archive_manifest_generation") != str(generation):
                raise ValueError("Archive routing manifest is not ready")
            return {path for (path,) in db.execute("SELECT path FROM archive_index WHERE path LIKE 'news/%/index.html'")}

    def snapshot_hot(self, destination, generation):
        destination = Path(destination)
        if destination.exists():
            raise ValueError("Hot asset snapshot must be new")
        destination.mkdir(parents=True)
        result = {}
        with self._database(shared=True) as db:
            if self._generation(db) != str(generation) or self._pending(db) or self._meta(db, "archive_manifest_generation") != str(generation):
                raise ValueError("Public archive is not ready for this generation")
            for path in sorted(HOT_FILES):
                row = db.execute("SELECT * FROM public_artifacts WHERE path=?", (path,)).fetchone()
                if row is None:
                    raise ValueError("Missing hot public artifact")
                self._copy(dict(row), destination / path)
                result[path] = row["sha256"]
        return result
