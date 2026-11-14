"""Copy cold private evidence to verified private objects, then compact it locally."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import logging
from pathlib import Path
import tempfile

from onionflash.store import compact_retained_rows, select_retention_candidates
from onionflash.util import canonical_json

HOT_DAYS, BATCH_SIZE, MAX_BUNDLE_BYTES = 90, 100, 16 * 1024**2
RUN_BUDGET_SECONDS, INTERVAL_SECONDS = 60, 86400
FORMAT = "onionflash-private-evidence-v1"
LOG = logging.getLogger("onionflash.deploy")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def destination(archive) -> dict:
    """Where new evidence goes; each reference keeps the one it was archived to."""
    return {key: archive.cfg[key] for key in ("endpoint_url", "region", "bucket")}


def archive_evidence(archive, now: datetime | None = None) -> dict:
    """Bundle one batch of cold rows privately, then compact the rows unchanged meanwhile."""
    now = now or datetime.now(timezone.utc)
    cutoff = _iso(now - timedelta(days=HOT_DAYS))
    base_overhead = len(canonical_json({"format": FORMAT, "records": []}).encode("utf-8"))
    with archive._database() as db:
        records, over_limit = select_retention_candidates(
            db, cutoff=cutoff, batch_size=BATCH_SIZE, max_bundle_bytes=MAX_BUNDLE_BYTES,
            base_overhead=base_overhead, logger=LOG,
        )
    dest = destination(archive)
    result = {
        "records_archived": 0, "items_compacted": 0, "analyses_evicted": 0,
        "changed_records_preserved": 0, "over_limit": over_limit, "destination": dest,
        "batch_complete": not records or (len(records) < BATCH_SIZE and not over_limit),
    }
    if not records:
        return result
    body = gzip.compress(canonical_json({"format": FORMAT, "records": records}).encode("utf-8"), mtime=0)
    checksum = hashlib.sha256(body).hexdigest()
    with tempfile.TemporaryDirectory(prefix="retention-", dir=archive.publisher.state) as temporary:
        path = Path(temporary) / "bundle.json.gz"
        path.write_bytes(body)
        uploaded = archive.put_private(path, f"{archive.private_prefix}evidence/{checksum}.json.gz", checksum)
    with archive._database() as db:
        compacted = compact_retained_rows(
            db, records, uploaded_key=uploaded["key"], uploaded_sha256=uploaded["sha256"],
            archived_at=_iso(now), destination_json=canonical_json(dest),
        )
    return {**result, **compacted, "object_key": uploaded["key"]}
