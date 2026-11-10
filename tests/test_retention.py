"""Cold evidence stays recoverable before any hot data is compacted."""

from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import tomllib
from types import SimpleNamespace
import unittest

from botocore.exceptions import ClientError

from retention import RetentionStore


NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
OLD = "2026-01-01T12:00:00Z"
RECENT = "2026-11-09T12:00:00Z"
VERSIONS = [f"{n:08x}-1111-4111-8111-111111111111" for n in range(1, 12)]


class FakeSpaces:
    def __init__(self, failure=None, callback=None):
        self.failure, self.callback = failure, callback
        self.objects, self.puts = {}, []

    def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        body, options = self.objects[Bucket, Key]
        return {"ContentLength": len(body), "Metadata": {
            "sha256": "0" * 64 if self.failure == "metadata" else options["Metadata"]["sha256"]},
            "ETag": "verified-etag"}

    def put_object(self, *, Bucket, Key, Body, **options):
        if self.failure == "upload":
            raise RuntimeError("Upload unavailable")
        body = Body.read()
        assert len(body) == options["ContentLength"]
        assert options["ACL"] == "private"
        self.objects[Bucket, Key] = (body, options)
        self.puts.append(Key)
        if self.callback:
            callback, self.callback = self.callback, None
            callback()
        return {}

    def get_object_acl(self, **kwargs):
        grants = [{"Grantee": {"Type": "CanonicalUser", "ID": "owner"}, "Permission": "FULL_CONTROL"}]
        if self.failure == "acl":
            grants.append({"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"}, "Permission": "READ"})
        return {"Owner": {"ID": "owner"}, "Grants": grants}

    def get_object(self, *, Bucket, Key):
        body = self.objects[Bucket, Key][0]
        if self.failure == "content":
            body = b"x" + body[1:]
        return {"Body": io.BytesIO(body)}


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        state = self.root / "state"
        state.mkdir()
        archive = tomllib.loads((Path(__file__).parents[1] / "deployment.example.toml").read_text())["archive"]
        self.publisher = SimpleNamespace(
            state=state, database=state / "news.sqlite3", public=self.root / "public",
            cfg={"retention": {"enabled": True}, "archive": {
                **archive, "enabled": True, "endpoint_url": "https://s3.example.org",
                "public_origin": "https://archive.example.org/onionflash/public/",
            }}, net={"attempts": 2, "timeout_seconds": 10}, delay=lambda attempt: None,
        )
        with self.db() as db:
            db.executescript("""
                CREATE TABLE items (
                    id TEXT PRIMARY KEY, url TEXT UNIQUE, source_id TEXT, payload TEXT,
                    content_hash TEXT, cluster_key TEXT, first_seen TEXT, last_seen TEXT,
                    status TEXT, attempts INTEGER, next_attempt_at TEXT, last_error TEXT,
                    priority_score INTEGER, story_id TEXT
                );
                CREATE TABLE analyses (
                    cache_key TEXT PRIMARY KEY, result TEXT, created_at TEXT,
                    source_payload TEXT, source_item_id TEXT
                );
                CREATE TABLE stories (id TEXT PRIMARY KEY,payload TEXT);
                CREATE TABLE source_state (id TEXT PRIMARY KEY, watermark TEXT);
                CREATE TABLE monitoring_groups (id TEXT PRIMARY KEY, highwater TEXT);
                INSERT INTO stories VALUES('story','public story must remain');
                INSERT INTO source_state VALUES('source','do not reset');
                INSERT INTO monitoring_groups VALUES('incident','do not replay');
            """)

    def db(self):
        return sqlite3.connect(self.publisher.database)

    def item(self, identity="item", *, status="published", date=OLD, content="private body", **fields):
        payload = {"title": "Source title", "content": content, "kind": "metrics_timeline",
                   "published_at": "2025-12-01T00:00:00Z", "updated_at": date,
                   "source_entry_id": "a" * 64, "labels": ["Security"], "product": "network",
                   "url": "https://example.org/" + identity, **fields}
        row = (identity, payload["url"], "source", json.dumps(payload), hashlib.sha256(content.encode()).hexdigest(),
               "cluster:" + identity, OLD, date, status, 2, None, None, 10, "story")
        with self.db() as db:
            db.execute("INSERT INTO items VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)
        return payload

    def analysis(self, key="cache", date=OLD):
        with self.db() as db:
            db.execute("INSERT INTO analyses VALUES(?,?,?,?,?)", (key, '{"evidence":["private quote"]}', date,
                       '{"content":"collected source snapshot; not an exact model request"}', "item"))

    def rows(self, table):
        with self.db() as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT * FROM " + table)]

    def engine(self, client=None, **settings):
        self.publisher.cfg["retention"].update(settings)
        return RetentionStore(self.publisher, client or FakeSpaces())

    def test_published_example_configuration_is_accepted(self):
        example = tomllib.loads((Path(__file__).parents[1] / "deployment.example.toml").read_text())
        self.publisher.cfg["retention"] = example["retention"]
        self.assertEqual(self.engine().cfg["run_budget_seconds"], 60)

    def receipt(self, version, previous=None, date=OLD):
        value = {"version_id": version, "previous_version_id": previous, "published_at": date,
                 "manifest": {"index.html": "a" * 64}}
        directory = self.publisher.state / "receipts"
        directory.mkdir(exist_ok=True)
        path = directory / (version + ".json")
        path.write_text(json.dumps(value))
        return path, value

    def current(self, version=VERSIONS[0], previous=None):
        _, value = self.receipt(version, previous)
        (self.publisher.state / "publication.json").write_text(json.dumps(value))

    def test_private_bundle_precedes_compaction_and_preserves_controls(self):
        payload = self.item()
        self.item("pending", status="pending")
        self.item("recent", date=RECENT)
        self.analysis()
        before = {row["id"]: row for row in self.rows("items")}
        client = FakeSpaces()
        result = self.engine(client).archive_evidence(NOW)
        self.assertEqual(result["items_compacted"], 1)
        self.assertEqual(result["analyses_evicted"], 1)
        current = {row["id"]: row for row in self.rows("items")}
        self.assertEqual(current["pending"], before["pending"])
        self.assertEqual(current["recent"], before["recent"])
        self.assertEqual({k: v for k, v in current["item"].items() if k != "payload"},
                         {k: v for k, v in before["item"].items() if k != "payload"})
        self.assertEqual(json.loads(current["item"]["payload"]), {k: v for k, v in payload.items() if k != "content"})
        bundle = json.loads(gzip.decompress(next(iter(client.objects.values()))[0]))
        archived_item = next(r["row"] for r in bundle["records"] if r["kind"] == "item")
        archived_analysis = next(r["row"] for r in bundle["records"] if r["kind"] == "analysis")
        self.assertEqual(archived_item, before["item"])
        self.assertIn("collected source snapshot", archived_analysis["source_payload"])
        self.assertEqual(len(self.rows("private_archive_refs")), 2)
        self.assertEqual(len(self.rows("stories")), 1)
        self.assertEqual(self.rows("source_state")[0]["watermark"], "do not reset")
        self.assertEqual(self.rows("monitoring_groups")[0]["highwater"], "do not replay")
        repeat = self.engine(client).archive_evidence(NOW)
        self.assertEqual(repeat["records_archived"], 0)
        self.assertEqual(len(client.puts), 1)

    def test_upload_content_metadata_or_acl_failure_never_compacts(self):
        self.item()
        self.analysis()
        before_items, before_analyses = self.rows("items"), self.rows("analyses")
        for failure in ["upload", "content", "metadata", "acl"]:
            with self.subTest(failure=failure):
                with self.assertRaises((ValueError, RuntimeError)):
                    self.engine(FakeSpaces(failure=failure)).archive_evidence(NOW)
                self.assertEqual(self.rows("items"), before_items)
                self.assertEqual(self.rows("analyses"), before_analyses)
                self.assertEqual(self.rows("private_archive_refs"), [])

    def test_changed_rows_during_upload_are_not_overwritten(self):
        self.item()
        self.analysis()

        def concurrent():
            with self.db() as db:
                db.execute("UPDATE items SET status='pending' WHERE id='item'")
                db.execute("UPDATE analyses SET result='new analysis' WHERE cache_key='cache'")

        result = self.engine(FakeSpaces(callback=concurrent)).archive_evidence(NOW)
        self.assertEqual(result["changed_records_preserved"], 2)
        self.assertIn("private body", self.rows("items")[0]["payload"])
        self.assertEqual(self.rows("analyses")[0]["result"], "new analysis")
        self.assertEqual(self.rows("private_archive_refs"), [])

    def test_new_item_revision_can_be_archived_without_losing_earlier_proof(self):
        self.item()
        engine = self.engine()
        engine.archive_evidence(NOW)
        with self.db() as db:
            db.execute("UPDATE items SET content_hash=?,payload=? WHERE id='item'",
                       ("b" * 64, '{"content":"second body","updated_at":"2026-01-02T00:00:00Z"}'))
        engine.archive_evidence(NOW)
        refs = self.rows("private_archive_refs")
        self.assertEqual(len(refs), 2)
        self.assertNotEqual(refs[0]["object_key"], refs[1]["object_key"])

    def test_legacy_analysis_schema_is_exported_without_invented_inputs(self):
        with self.db() as db:
            db.execute("DROP TABLE analyses")
            db.execute("CREATE TABLE analyses(cache_key TEXT PRIMARY KEY,result TEXT,created_at TEXT)")
            db.execute("INSERT INTO analyses VALUES(?,?,?)", ("legacy", '{"evidence":["quote"]}', OLD))
        client = FakeSpaces()
        self.engine(client).archive_evidence(NOW)
        record = json.loads(gzip.decompress(next(iter(client.objects.values()))[0]))["records"][0]["row"]
        self.assertEqual(set(record), {"cache_key", "result", "created_at"})
        self.assertEqual(self.rows("analyses"), [])

    def test_malformed_item_payload_is_archived_but_not_rewritten(self):
        self.item("bad")
        self.item("good")
        with self.db() as db:
            db.execute("UPDATE items SET payload='malformed source snapshot' WHERE id='bad'")
        result = self.engine().archive_evidence(NOW)
        self.assertEqual(result["records_archived"], 2)
        self.assertEqual(result["items_compacted"], 1)
        rows = {row["id"]: row for row in self.rows("items")}
        self.assertEqual(rows["bad"]["payload"], "malformed source snapshot")
        self.assertNotIn("content", json.loads(rows["good"]["payload"]))

    def test_batch_and_byte_limits_keep_excess_hot(self):
        self.item("a")
        self.item("b")
        result = self.engine(batch_size=1).archive_evidence(NOW)
        self.assertEqual(result["records_archived"], 1)
        self.assertEqual(sum("content" in json.loads(r["payload"]) for r in self.rows("items")), 1)
        self.item("large", content="x" * 10000)
        result = self.engine(max_bundle_bytes=1024, batch_size=100).archive_evidence(NOW)
        self.assertGreater(result["over_limit"], 0)
        large = next(row for row in self.rows("items") if row["id"] == "large")
        self.assertEqual(len(json.loads(large["payload"])["content"]), 10000)

    def test_optional_retention_does_nothing_when_disabled(self):
        self.item()
        result = self.engine(enabled=False).run(NOW)
        self.assertEqual(result, {"enabled": False})
        with self.db() as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='private_archive_refs'").fetchone())

    def test_current_previous_transition_hold_and_rollback_receipts_are_pinned(self):
        for version in VERSIONS[:9]:
            self.receipt(version)
        self.current(VERSIONS[0], VERSIONS[1])
        controls = {
            "transition.json": {"before_version": VERSIONS[2], "target": {"version_id": VERSIONS[3]}},
            "last-transition.json": {"before_receipt": {"version_id": VERSIONS[4]}},
            "publication-hold.json": {"from": VERSIONS[5], "to": VERSIONS[6]},
            "rollback.json": {"version_id": VERSIONS[7]},
        }
        for name, value in controls.items():
            (self.publisher.state / name).write_text(json.dumps(value))
        result = self.engine().archive_receipts(NOW)
        self.assertEqual(result["receipts_archived"], 1)
        for version in VERSIONS[:8]:
            self.assertTrue((self.publisher.state / "receipts" / (version + ".json")).exists())
        self.assertFalse((self.publisher.state / "receipts" / (VERSIONS[8] + ".json")).exists())
        self.assertEqual(self.rows("private_archive_refs")[0]["record_id"], VERSIONS[8])

    def test_receipt_upload_failure_keeps_local_receipt(self):
        self.current()
        path, _ = self.receipt(VERSIONS[1])
        for failure in ["upload", "content", "metadata", "acl"]:
            with self.subTest(failure=failure):
                with self.assertRaises((ValueError, RuntimeError)):
                    self.engine(FakeSpaces(failure=failure)).archive_receipts(NOW)
                self.assertTrue(path.exists())

    def test_receipt_changed_or_newly_pinned_during_upload_stays_hot(self):
        self.current()
        changed, changed_value = self.receipt(VERSIONS[1])
        newly_current, current_value = self.receipt(VERSIONS[2])

        def concurrent():
            changed.write_text(json.dumps({**changed_value, "new": "preserve"}))
            (self.publisher.state / "publication.json").write_text(json.dumps(current_value))

        result = self.engine(FakeSpaces(callback=concurrent)).archive_receipts(NOW)
        self.assertEqual(result["changed_receipts_preserved"], 2)
        self.assertTrue(changed.exists())
        self.assertTrue(newly_current.exists())
        self.assertEqual(self.rows("private_archive_refs"), [])

    def test_receipt_archive_is_recoverable_and_repeat_has_no_more_uploads(self):
        self.current()
        old, _ = self.receipt(VERSIONS[1])
        original = old.read_bytes()
        self.receipt(VERSIONS[2], date=RECENT)
        client = FakeSpaces()
        engine = self.engine(client)
        engine.archive_receipts(NOW)
        record = json.loads(gzip.decompress(next(iter(client.objects.values()))[0]))["records"][0]
        self.assertEqual(record["content"].encode(), original)
        self.assertEqual(record["sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(engine.archive_receipts(NOW)["receipts_archived"], 0)
        self.assertEqual(len(client.puts), 1)

    def test_unknown_or_malformed_control_state_prevents_receipt_pruning(self):
        self.current()
        path, _ = self.receipt(VERSIONS[1])
        for value in ["not json", "{}", "[]"]:
            with self.subTest(value=value):
                (self.publisher.state / "publication.json").write_text(value)
                with self.assertRaises(ValueError):
                    self.engine().archive_receipts(NOW)
                self.assertTrue(path.exists())

    def test_symlinked_receipt_or_control_or_database_is_rejected(self):
        self.current()
        target = self.publisher.state / "external.json"
        target.write_text('{}')
        receipt = self.publisher.state / "receipts" / (VERSIONS[1] + ".json")
        receipt.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.engine().archive_receipts(NOW)
        receipt.unlink()
        control = self.publisher.state / "transition.json"
        control.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.engine().archive_receipts(NOW)
        control.unlink()
        database = self.publisher.database
        real = self.publisher.state / "real.sqlite3"
        database.rename(real)
        database.symlink_to(real)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.engine().archive_evidence(NOW)

    def test_invalid_retention_limits_are_rejected(self):
        for settings in [{"batch_size": 0}, {"hot_days": 0}, {"max_bundle_bytes": 128}, {"enabled": 1}, {"unknown": True}]:
            with self.subTest(settings=settings):
                original = self.publisher.cfg["retention"].copy()
                try:
                    with self.assertRaises(ValueError):
                        self.engine(**settings)
                finally:
                    self.publisher.cfg["retention"] = original
