"""Move cold evidence to verified private objects without losing replay state.

The caller owns the deployment job lock. Database locks are held only while
snapshotting or applying guarded compaction, never during object-store I/O.
Published stories, catalog entries and incident state are never removed here.
"""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import uuid


DEFAULTS = {"enabled": False, "hot_days": 90, "receipt_hot_days": 30,
            "batch_size": 100, "max_bundle_bytes": 16777216, "run_budget_seconds": 60}


def _json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _stamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("A retention timestamp must be a string")
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError("A retention timestamp must have a timezone")
    return moment.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _no_symlinks(path: Path) -> None:
    """Reject linked targets and linked parent components of owned state paths."""
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("Retention state paths may not contain symlinks")


def _read_file(path: Path, limit: int) -> bytes:
    _no_symlinks(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise ValueError("Retention input must be a bounded regular file")
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        current = os.stat(path, follow_symlinks=False)
    if len(data) > limit or (before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("Retention input changed or exceeded its byte limit")
    if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("Retention input was replaced while reading")
    return data


class RetentionStore:
    def __init__(self, publisher, s3_client=None):
        self.publisher = publisher
        self.cfg = {**DEFAULTS, **publisher.cfg.get("retention", {})}
        if set(self.cfg) != set(DEFAULTS) or type(self.cfg["enabled"]) is not bool:
            raise ValueError("Invalid retention configuration")
        for key, low, high in (("hot_days", 1, 36500), ("receipt_hot_days", 1, 36500),
                               ("batch_size", 1, 1000), ("max_bundle_bytes", 1024, 67108864),
                               ("run_budget_seconds", 1, 3600)):
            if type(self.cfg[key]) is not int or not low <= self.cfg[key] <= high:
                raise ValueError("Invalid retention " + key)
        self.client = s3_client
        self._archive = None
        self.state = Path(publisher.state)
        self.database = Path(publisher.database)

    @property
    def archive(self):
        if self._archive is None:
            from archive_store import ArchiveStore
            self._archive = ArchiveStore(self.publisher, client=self.client)
        return self._archive

    def _db(self):
        _no_symlinks(self.database)
        if not self.database.is_file():
            raise ValueError("Retention requires an existing application database")
        db = sqlite3.connect(self.database.as_uri() + "?mode=rw", uri=True, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        return db

    @staticmethod
    def _initialize(db):
        db.executescript("""
            CREATE TABLE IF NOT EXISTS private_archive_refs (
                kind TEXT NOT NULL, record_id TEXT NOT NULL, record_hash TEXT NOT NULL,
                object_key TEXT NOT NULL, object_sha256 TEXT NOT NULL,
                archived_at TEXT NOT NULL,
                PRIMARY KEY(kind, record_id, record_hash)
            );
            CREATE INDEX IF NOT EXISTS retention_items_age ON items(last_seen,id);
            CREATE INDEX IF NOT EXISTS retention_analyses_age ON analyses(created_at,cache_key);
        """)

    def _put_bundle(self, family: str, value: dict) -> dict:
        raw = _json(value)
        if len(raw) > self.cfg["max_bundle_bytes"]:
            raise ValueError("Private retention bundle exceeds configured byte limit")
        body = gzip.compress(raw, mtime=0)
        checksum = _sha(body)
        key = self.archive.private_prefix + family + "/" + checksum + ".json.gz"
        _no_symlinks(self.state)
        if not self.state.is_dir():
            raise ValueError("Retention requires an owned state directory")
        with tempfile.TemporaryDirectory(prefix="retention-", dir=self.state) as temporary:
            path = Path(temporary) / "bundle.json.gz"
            path.write_bytes(body)
            path.chmod(0o600)
            result = self.archive.put_private(path, key, sha256=checksum)
        # The shared helper verifies bytes and ACL. Bind its receipt to this
        # exact bundle before any local data can be removed.
        expected = {"key": key, "sha256": checksum, "size": len(body)}
        if not isinstance(result, dict) or any(result.get(key) != value for key, value in expected.items()):
            raise ValueError("Private retention verification receipt does not match")
        return result

    def archive_evidence(self, now: datetime | None = None) -> dict:
        if not self.cfg["enabled"]:
            return {"enabled": False}
        now = now or datetime.now(timezone.utc)
        cutoff = _iso(now - timedelta(days=self.cfg["hot_days"]))
        from publish import lock
        records = []
        over_limit = 0
        size = len(_json({"format": "onionflash-private-evidence-v1", "records": []}))
        with lock(str(self.database) + ".lock"), closing(self._db()) as db:
            self._initialize(db)
            queries = (
                ("item", """SELECT * FROM items WHERE status IN ('published','ignored')
                    AND last_seen<? AND NOT EXISTS (
                        SELECT 1 FROM private_archive_refs a WHERE a.kind='item'
                        AND a.record_id=items.id AND a.record_hash=items.content_hash)
                    ORDER BY last_seen,id LIMIT ?"""),
                ("analysis", "SELECT * FROM analyses WHERE created_at<? ORDER BY created_at,cache_key LIMIT ?"),
            )
            for kind, query in queries:
                # LIMIT bounds scan work as well as the resulting bundle.
                for row in db.execute(query, (cutoff, self.cfg["batch_size"])):
                    if len(records) >= self.cfg["batch_size"]:
                        break
                    row = dict(row)
                    stamp = row["last_seen"] if kind == "item" else row["created_at"]
                    if _stamp(stamp) >= _stamp(cutoff):
                        continue
                    record = {"kind": kind, "row": row}
                    amount = len(_json(record)) + 1
                    if size + amount > self.cfg["max_bundle_bytes"]:
                        over_limit += 1
                        continue
                    size += amount
                    records.append(record)
        result = {"records_archived": 0, "items_compacted": 0, "analyses_evicted": 0,
                  "changed_records_preserved": 0, "over_limit": over_limit}
        if not records:
            return result
        uploaded = self._put_bundle("evidence", {"format": "onionflash-private-evidence-v1", "records": records})
        with lock(str(self.database) + ".lock"), closing(self._db()) as db, db:
            for record in records:
                kind, row = record["kind"], record["row"]
                table, key = ("items", "id") if kind == "item" else ("analyses", "cache_key")
                current = db.execute("SELECT * FROM " + table + " WHERE " + key + "=?", (row[key],)).fetchone()
                if current is None or dict(current) != row:
                    result["changed_records_preserved"] += 1
                    continue
                # Keep compact controls and deduplication state. Only the large
                # source body moves out of a completed item's hot payload.
                record_hash = row["content_hash"] if kind == "item" else _sha(_json(row))
                db.execute("INSERT OR IGNORE INTO private_archive_refs VALUES(?,?,?,?,?,?)",
                           (kind, row[key], record_hash, uploaded["key"], uploaded["sha256"], _iso(now)))
                if kind == "item":
                    try:
                        payload = json.loads(row["payload"])
                    except (ValueError, TypeError, RecursionError):
                        # Preserve malformed hot input verbatim; its archive is
                        # still useful evidence, but its structure is unknown.
                        payload = None
                    if isinstance(payload, dict) and "content" in payload:
                        payload.pop("content")
                        db.execute("UPDATE items SET payload=? WHERE id=?", (_json(payload).decode(), row["id"]))
                        result["items_compacted"] += 1
                else:
                    db.execute("DELETE FROM analyses WHERE cache_key=?", (row["cache_key"],))
                    result["analyses_evicted"] += 1
                result["records_archived"] += 1
        result["object_key"] = uploaded["key"]
        return result

    def _pinned_receipts(self) -> set[str]:
        pinned = set()

        def visit(value):
            if isinstance(value, dict):
                for child in value.values():
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)
            elif isinstance(value, str):
                try:
                    if str(uuid.UUID(value)) == value:
                        pinned.add(value)
                except ValueError:
                    pass

        for name in ("publication.json", "transition.json", "last-transition.json",
                     "publication-hold.json", "rollback.json"):
            path = self.state / name
            _no_symlinks(path)
            if name == "publication.json" and not path.is_file():
                raise ValueError("Receipt retention requires a publication pointer")
            if path.exists():
                value = json.loads(_read_file(path, self.cfg["max_bundle_bytes"]))
                if not isinstance(value, dict):
                    raise ValueError("Invalid publication control record")
                if name == "publication.json":
                    version = value.get("version_id")
                    try:
                        valid = isinstance(version, str) and str(uuid.UUID(version)) == version
                    except ValueError:
                        valid = False
                    if not valid:
                        raise ValueError("Publication pointer has no valid version identity")
                visit(value)
        return pinned

    def archive_receipts(self, now: datetime | None = None) -> dict:
        if not self.cfg["enabled"]:
            return {"enabled": False}
        now = now or datetime.now(timezone.utc)
        cutoff = now - timedelta(days=self.cfg["receipt_hot_days"])
        receipts = self.state / "receipts"
        _no_symlinks(receipts)
        result = {"receipts_archived": 0, "changed_receipts_preserved": 0, "over_limit": 0}
        if not receipts.exists():
            return result
        if not receipts.is_dir() or not (self.state / "publication.json").is_file():
            raise ValueError("Receipt retention requires a valid publication pointer")
        pinned = self._pinned_receipts()
        records = []
        size = len(_json({"format": "onionflash-private-receipts-v1", "records": []}))
        for path in receipts.iterdir():
            _no_symlinks(path)
            if path.suffix != ".json":
                continue
            try:
                version = str(uuid.UUID(path.stem))
            except ValueError:
                continue
            if version != path.stem or version in pinned:
                continue
            if path.stat().st_size > self.cfg["max_bundle_bytes"]:
                result["over_limit"] += 1
                continue
            body = _read_file(path, self.cfg["max_bundle_bytes"])
            value = json.loads(body)
            if not isinstance(value, dict) or value.get("version_id") != version:
                raise ValueError("Receipt identity does not match its filename")
            if _stamp(value.get("published_at")) >= cutoff:
                continue
            record = {"version_id": version, "sha256": _sha(body), "content": body.decode("utf-8")}
            amount = len(_json(record)) + 1
            if size + amount > self.cfg["max_bundle_bytes"]:
                result["over_limit"] += 1
                continue
            size += amount
            records.append(record)
            if len(records) >= self.cfg["batch_size"]:
                break
        if not records:
            return result
        uploaded = self._put_bundle("receipts", {"format": "onionflash-private-receipts-v1", "records": records})
        pinned = self._pinned_receipts()
        # A recoverable private index makes removed local receipts discoverable
        # without relying on bucket-wide object listings.
        from publish import lock
        with lock(str(self.database) + ".lock"), closing(self._db()) as db, db:
            self._initialize(db)
            for record in records:
                version = record["version_id"]
                path = receipts / (version + ".json")
                if version in pinned or not path.exists():
                    result["changed_receipts_preserved"] += 1
                    continue
                if _sha(_read_file(path, self.cfg["max_bundle_bytes"])) != record["sha256"]:
                    result["changed_receipts_preserved"] += 1
                    continue
                db.execute("INSERT OR IGNORE INTO private_archive_refs VALUES(?,?,?,?,?,?)",
                           ("receipt", version, record["sha256"], uploaded["key"], uploaded["sha256"], _iso(now)))
                # Commit the durable locator before removing a hot receipt.
                db.commit()
                path.unlink()
                result["receipts_archived"] += 1
        result["object_key"] = uploaded["key"]
        return result

    def run(self, now: datetime | None = None) -> dict:
        if not self.cfg["enabled"]:
            return {"enabled": False}
        now = now or datetime.now(timezone.utc)
        return {"enabled": True, "evidence": self.archive_evidence(now),
                "receipts": self.archive_receipts(now)}
