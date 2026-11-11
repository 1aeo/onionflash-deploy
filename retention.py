"""Copy cold private evidence to verified private objects, then compact it locally.

Analysis rows older than HOT_DAYS are bundled and deleted. Old ignored items that
no story uses, except GitLab issues (the core keeps refreshing their comments), are
bundled and lose their source body; every other item stays whole. The caller owns
the deployment job lock. The database lock is held while selecting and compacting
rows, never during object-store I/O.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import tempfile


# A sweep bundles at most BATCH_SIZE rows and MAX_BUNDLE_BYTES within RUN_BUDGET_SECONDS
# of storage work. Complete sweeps run once per INTERVAL_SECONDS; a limited one continues.
HOT_DAYS, BATCH_SIZE, MAX_BUNDLE_BYTES = 90, 100, 16 * 1024**2
RUN_BUDGET_SECONDS, INTERVAL_SECONDS = 60, 86400
FORMAT = "onionflash-private-evidence-v1"
SCHEMA = """
    CREATE TABLE IF NOT EXISTS private_archive_refs (
        kind TEXT NOT NULL, record_id TEXT NOT NULL, record_hash TEXT NOT NULL,
        object_key TEXT NOT NULL, object_sha256 TEXT NOT NULL,
        archived_at TEXT NOT NULL, destination_json TEXT NOT NULL,
        PRIMARY KEY(kind, record_id, record_hash)
    );
    CREATE INDEX IF NOT EXISTS retention_items_age ON items(last_seen,id) WHERE status='ignored' AND story_id IS NULL;
    CREATE INDEX IF NOT EXISTS retention_analyses_age ON analyses(created_at,cache_key);
"""
# The partial age index lists only candidate items, oldest first; the hint keeps the planner on it.
QUERIES = (
    ("item", """SELECT * FROM items INDEXED BY retention_items_age
        WHERE status='ignored' AND story_id IS NULL AND last_seen<?
        AND json_extract(payload,'$.kind') IS NOT 'gitlab_issue'
        AND NOT EXISTS (SELECT 1 FROM private_archive_refs a WHERE a.kind='item'
            AND a.record_id=items.id AND a.record_hash=items.content_hash)
        ORDER BY last_seen,id LIMIT ?"""),
    ("analysis", "SELECT * FROM analyses WHERE created_at<? ORDER BY created_at,cache_key LIMIT ?"),
)


def _json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def destination(archive) -> dict:
    """Where new evidence goes; each reference keeps the one it was archived to."""
    return {key: archive.cfg[key] for key in ("endpoint_url", "region", "bucket")}


def archive_evidence(archive, now: datetime | None = None) -> dict:
    """Bundle one batch of cold rows privately, then compact the rows unchanged meanwhile."""
    now = now or datetime.now(timezone.utc)
    cutoff = _iso(now - timedelta(days=HOT_DAYS))
    records, over_limit = [], 0
    size = len(_json({"format": FORMAT, "records": []}))
    with archive._database() as db:
        db.executescript(SCHEMA)
        for kind, query in QUERIES:
            for row in db.execute(query, (cutoff, BATCH_SIZE - len(records))):
                record = {"kind": kind, "row": dict(row)}
                amount = len(_json(record)) + 1
                if size + amount > MAX_BUNDLE_BYTES:
                    over_limit += 1
                    continue
                size += amount
                records.append(record)
    result = {"records_archived": 0, "items_compacted": 0, "analyses_evicted": 0,
              "changed_records_preserved": 0, "over_limit": over_limit, "destination": destination(archive),
              # Only a batch that made progress and hit a limit leaves work for the next run.
              "batch_complete": not records or (len(records) < BATCH_SIZE and not over_limit)}
    if not records:
        return result
    body = gzip.compress(_json({"format": FORMAT, "records": records}), mtime=0)
    checksum = _sha(body)
    with tempfile.TemporaryDirectory(prefix="retention-", dir=archive.publisher.state) as temporary:
        path = Path(temporary) / "bundle.json.gz"
        path.write_bytes(body)
        uploaded = archive.put_private(path, archive.private_prefix + "evidence/" + checksum + ".json.gz", checksum)
    with archive._database() as db, db:
        for record in records:
            kind, row = record["kind"], record["row"]
            table, key = ("items", "id") if kind == "item" else ("analyses", "cache_key")
            current = db.execute("SELECT * FROM " + table + " WHERE " + key + "=?", (row[key],)).fetchone()
            if current is None or dict(current) != row:  # Changed during the upload: keep it.
                result["changed_records_preserved"] += 1
                continue
            # Never replace the locator a record was first archived under.
            db.execute("INSERT INTO private_archive_refs VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                       (kind, row[key], row["content_hash"] if kind == "item" else _sha(_json(row)), uploaded["key"],
                        uploaded["sha256"], _iso(now), _json(result["destination"]).decode("utf-8")))
            if kind == "item":
                # Keep compact controls and deduplication state; only the source body moves out.
                payload = json.loads(row["payload"])
                if payload.pop("content", None) is not None:
                    db.execute("UPDATE items SET payload=? WHERE id=?", (json.dumps(payload, ensure_ascii=False), row["id"]))
                    result["items_compacted"] += 1
            else:
                db.execute("DELETE FROM analyses WHERE cache_key=?", (row["cache_key"],))
                result["analyses_evicted"] += 1
            result["records_archived"] += 1
    result["object_key"] = uploaded["key"]
    return result
