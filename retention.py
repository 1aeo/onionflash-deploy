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
from itertools import islice
import json
import logging
from pathlib import Path
import tempfile

from onionflash.store import retained_analysis_keys


# A sweep bundles at most BATCH_SIZE rows and MAX_BUNDLE_BYTES within RUN_BUDGET_SECONDS
# of storage work. Complete sweeps run once per INTERVAL_SECONDS; a limited one continues.
HOT_DAYS, BATCH_SIZE, MAX_BUNDLE_BYTES = 90, 100, 16 * 1024**2
RUN_BUDGET_SECONDS, INTERVAL_SECONDS = 60, 86400
FORMAT = "onionflash-private-evidence-v1"
LOG = logging.getLogger("onionflash.deploy")
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
# A damaged item, whose payload is not a JSON object in text, is skipped and stays as it is. CASE
# tests that first, since SQLite fixes no order for WHERE terms and json_extract raises on bad JSON.
# _items then skips the damage SQLite cannot see; the query returns the bytes it checks.
QUERIES = (
    ("item", """SELECT rowid,CAST(id AS BLOB),CAST(payload AS BLOB) FROM items INDEXED BY retention_items_age
        WHERE status='ignored' AND story_id IS NULL AND last_seen<?
        AND CASE WHEN typeof(payload)='text' AND json_valid(payload) THEN json_type(payload)='object'
            AND json_extract(payload,'$.kind') IS NOT 'gitlab_issue' END
        AND NOT EXISTS (SELECT 1 FROM private_archive_refs a WHERE a.kind='item'
            AND a.record_id=items.id AND a.record_hash=items.content_hash)
        ORDER BY last_seen,id"""),
    ("analysis", """SELECT rowid,CAST(cache_key AS BLOB) FROM analyses INDEXED BY retention_analyses_age
        WHERE created_at<?
        AND NOT EXISTS (SELECT 1 FROM items i WHERE i.id=analyses.source_item_id
            AND (i.status!='ignored' OR i.story_id IS NOT NULL))
        ORDER BY created_at,cache_key"""),
)


def _json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _decode_row(db, table: str, columns: list[str], where: str, param, *, key_col: str) -> dict | None:
    expr = ",".join(f'typeof("{c}"),CAST("{c}" AS BLOB),CASE WHEN typeof("{c}") IN (\'integer\',\'real\') THEN "{c}" END'
                    for c in columns)
    raw = db.execute(f'SELECT {expr} FROM "{table}" WHERE {where}', (param,)).fetchone()
    if raw is None:
        return None
    decoded = {}
    for index, name in enumerate(columns):
        kind, blob, number = raw[index * 3 : index * 3 + 3]
        if kind == "null":
            decoded[name] = None
        elif kind in ("integer", "real"):
            decoded[name] = number
        elif kind == "text":
            decoded[name] = str(blob, "utf-8")
        else:
            raise ValueError(f"Unexpected {kind} column {name}")
    if not isinstance(decoded.get(key_col), str):
        raise ValueError(f"Missing string {key_col}")
    _json(decoded)
    return decoded


def _analyses(db, query, cutoff, pinned_keys: set[str]):
    columns = [info[1] for info in db.execute("PRAGMA table_info(analyses)")]
    for rowid, raw_key in db.execute(query, (cutoff,)):
        label = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, bytes) else raw_key
        try:
            row = _decode_row(db, "analyses", columns, "rowid=?", rowid, key_col="cache_key")
        except (UnicodeError, ValueError, RecursionError) as error:
            LOG.warning("Skipping damaged retention analysis %r (%s)", label, type(error).__name__)
            continue
        if row is not None and row["cache_key"] not in pinned_keys:
            yield row


def _items(db, query, cutoff):
    """The item query's rows whose payload compaction can read and write back. SQLite accepts bytes
    that are not UTF-8 in a JSON string, which fail any read of the row, and an escaped lone
    surrogate, which cannot be stored once parsed; such an item is skipped like other damage."""
    columns = [info[1] for info in db.execute("PRAGMA table_info(items)")]
    for rowid, raw_id, payload in db.execute(query, (cutoff,)):
        label = raw_id.decode("utf-8", errors="replace") if isinstance(raw_id, bytes) else raw_id
        try:
            json.dumps(json.loads(str(payload, "utf-8")), ensure_ascii=False).encode()
            row = _decode_row(db, "items", columns, "rowid=?", rowid, key_col="id")
        except (UnicodeError, ValueError, RecursionError) as error:
            LOG.warning("Skipping damaged retention item %r (%s)", label, type(error).__name__)
            continue
        if row is not None:
            yield row


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
        pinned_keys = retained_analysis_keys(db)
        for kind, query in QUERIES:
            limit = BATCH_SIZE - len(records)  # a skipped item takes no place in the batch
            source = _items(db, query, cutoff) if kind == "item" else _analyses(db, query, cutoff, pinned_keys)
            for row in islice(source, limit):
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
        pinned_keys = retained_analysis_keys(db)
        item_cols = [info[1] for info in db.execute("PRAGMA table_info(items)")]
        analysis_cols = [info[1] for info in db.execute("PRAGMA table_info(analyses)")]
        for record in records:
            kind, row = record["kind"], record["row"]
            key = "id" if kind == "item" else "cache_key"
            try:
                if kind == "item":
                    current = _decode_row(db, "items", item_cols, "id=?", row["id"], key_col="id")
                    if current is not None:
                        json.dumps(json.loads(current["payload"]), ensure_ascii=False).encode()
                else:
                    current = _decode_row(db, "analyses", analysis_cols, "cache_key=?", row["cache_key"], key_col="cache_key")
            except (UnicodeError, ValueError, RecursionError):
                current = None
            if current is None or current != row:  # Changed during the upload: keep it.
                result["changed_records_preserved"] += 1
                continue
            if kind == "analysis" and (
                row["cache_key"] in pinned_keys
                or (row.get("source_item_id") and db.execute(
                    "SELECT 1 FROM items WHERE id=? AND (status!='ignored' OR story_id IS NOT NULL)",
                    (row["source_item_id"],),
                ).fetchone())
            ):
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
