"""Cold private evidence stays recoverable before any hot data is compacted."""

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import gzip
import hashlib
import json
import sqlite3
import time
import unittest
from unittest.mock import patch

from botocore.exceptions import EndpointConnectionError

from archive_store import ArchiveStore
from onionflash.store import Store
import publish
import retention
from tests.fakes import FakeS3, make_publisher


NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
OLD = "2026-01-01T12:00:00Z"
RECENT = "2026-11-09T12:00:00Z"


class RetentionTests(unittest.TestCase):
    def setUp(self):
        directory, self.p = make_publisher(self)
        # A state directory moved to another disk and linked into place keeps working.
        self.p.state.rename(directory / "disk")
        self.p.state.symlink_to(directory / "disk")
        self.p.cfg["archive"]["enabled"] = self.p.cfg["retention"]["enabled"] = True
        self.s3 = FakeS3()
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        self.p.delay = lambda attempt: None
        self.exposure = None  # what an unsigned request for a private object finds
        exposure = patch.object(self.p, "unsigned_read_problem", side_effect=lambda *request: self.exposure)
        exposure.start()
        self.addCleanup(exposure.stop)
        with Store(self.p.database) as store, store.db:
            store.db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)",
                             ("story", "story", "public story must remain", OLD, OLD))

    @contextmanager
    def db(self):
        with closing(sqlite3.connect(self.p.database)) as db, db:
            db.row_factory = sqlite3.Row
            yield db

    def item(self, identity="item", *, status="ignored", date=OLD, content="private body", story=None, **fields):
        payload = {"title": "Source title", "content": content, "kind": "metrics_timeline", "updated_at": date,
                   "url": "https://example.org/" + identity, **fields}
        with self.db() as db:
            db.execute("INSERT INTO items(id,url,source_id,payload,content_hash,cluster_key,first_seen,last_seen,"
                       "status,story_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                       (identity, payload["url"], "source", json.dumps(payload), hashlib.sha256(content.encode()).hexdigest(),
                        "cluster:" + identity, OLD, date, status, story))
        return payload

    def analysis(self, key="cache", date=OLD):
        with self.db() as db:
            db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                       (key, '{"evidence":["private quote"]}', date, '{"content":"collected source snapshot"}', "item"))

    def rows(self, table):
        with self.db() as db:
            return [dict(row) for row in db.execute("SELECT * FROM " + table)]

    def hot(self, identity):
        return "content" in json.loads(next(row for row in self.rows("items") if row["id"] == identity)["payload"])

    def sweep(self):
        return retention.archive_evidence(self.p.archive, NOW)

    def run_cycle(self):
        with patch.object(self.p, "application", return_value="{}"), patch.object(self.p, "publish"):
            return self.p.run_cycle()

    def test_only_old_ignored_items_no_story_uses_lose_their_body_after_a_private_bundle(self):
        payload = self.item()
        self.item("published", status="published", story="story")  # the review gate reloads a story's writer
        self.item("story-source", story="story")  # ignored on a later revision, still one of a story's sources
        self.item("issue", kind="gitlab_issue", source_content="private issue description")  # comments refresh it
        self.item("pending", status="pending")
        self.item("held", status="rejected")  # an operator retry analyzes the body again
        self.item("recent", date=RECENT)
        self.analysis()
        before, analysis = {row["id"]: row for row in self.rows("items")}, self.rows("analyses")[0]
        result = self.sweep()
        self.assertEqual((result["records_archived"], result["items_compacted"], result["analyses_evicted"]), (2, 1, 1))
        current = {row["id"]: row for row in self.rows("items")}
        self.assertEqual({key: row for key, row in current.items() if key != "item"},
                         {key: row for key, row in before.items() if key != "item"})
        self.assertEqual({**current["item"], "payload": None}, {**before["item"], "payload": None})
        self.assertEqual(json.loads(current["item"]["payload"]), {k: v for k, v in payload.items() if k != "content"})
        bundle = json.loads(gzip.decompress(self.s3.objects[result["object_key"]]["body"]))
        self.assertEqual(bundle["records"], [{"kind": "item", "row": before["item"]}, {"kind": "analysis", "row": analysis}])
        self.assertEqual(self.rows("analyses"), [])
        self.assertEqual(len(self.rows("private_archive_refs")), 2)
        self.assertEqual(len(self.rows("stories")), 1)
        self.assertEqual(self.sweep()["records_archived"], 0)
        self.assertEqual(len(self.s3.puts), 1)

    def test_an_unverified_or_readable_bundle_never_compacts(self):
        self.item()
        self.analysis()
        before = self.rows("items"), self.rows("analyses")
        changes = {"content": lambda stored: stored.update(body=b"x" + stored["body"][1:]),
                   "metadata": lambda stored: stored.update(metadata={"sha256": "0" * 64}),
                   "acl": lambda stored: stored.update(acl="public-read")}
        for failure in ["upload", "content", "metadata", "acl", "readable"]:
            with self.subTest(failure=failure):
                self.p.archive.client = s3 = FakeS3()
                s3.fail_puts = 99 if failure == "upload" else 0
                s3.on_put = lambda key: changes.get(failure, lambda stored: None)(s3.objects[key])
                self.exposure = "was publicly readable" if failure == "readable" else None
                with self.assertRaises((ValueError, EndpointConnectionError)):
                    self.sweep()
                self.assertEqual((self.rows("items"), self.rows("analyses")), before)
                self.assertEqual(self.rows("private_archive_refs"), [])
        self.assertEqual(s3.objects, {})  # a new object that anyone can read is deleted

    def test_rows_changed_during_the_upload_are_kept(self):
        self.item()
        self.analysis()

        def concurrent(key):
            with self.db() as db:
                db.execute("UPDATE items SET status='pending' WHERE id='item'")
                db.execute("UPDATE analyses SET result='new analysis' WHERE cache_key='cache'")

        self.s3.on_put = concurrent
        self.assertEqual(self.sweep()["changed_records_preserved"], 2)
        self.assertTrue(self.hot("item"))
        self.assertEqual(self.rows("analyses")[0]["result"], "new analysis")
        self.assertEqual(self.rows("private_archive_refs"), [])

    def test_a_damaged_item_is_skipped_and_the_rest_of_the_batch_continues(self):
        # Broken JSON, an Infinity that only SQLite's strict check rejects, a non-object, a BLOB, and
        # three objects to SQLite that compaction cannot read and write back: a lone surrogate as bytes,
        # which are not UTF-8, or as a JSON escape, and text after a NUL byte, where SQLite stops reading.
        for identity, damage in {"broken": "'{'", "infinity": "replace(payload,'\"Source title\"','Infinity')",
                                 "array": "'[1]'", "blob": "CAST(payload AS BLOB)",
                                 "bytes": "replace(payload,'Source',x'eda080')",
                                 "escaped": "replace(payload,'Source','\\ud800')", "extra": "payload||char(0)||'x'"}.items():
            self.item(identity)
            with self.db() as db:
                db.execute("UPDATE items SET payload=" + damage + " WHERE id=?", (identity,))

        def damaged():
            with self.db() as db:
                db.text_factory = bytes  # bytes that are not UTF-8 fail a read as text
                return [tuple(row) for row in db.execute("SELECT *,typeof(payload) FROM items WHERE id!='item'")]

        before = damaged()
        self.item()
        with patch.object(retention, "BATCH_SIZE", 1), self.assertLogs("onionflash.deploy", level="WARNING") as logged:
            # The skipped items sort first and take no place; diagnostics contain no source payload.
            result = self.sweep()
        self.assertEqual({record.args[0] for record in logged.records}, {"bytes", "escaped", "extra"})
        self.assertEqual({record.args[1] for record in logged.records},
                         {"UnicodeDecodeError", "UnicodeEncodeError", "JSONDecodeError"})
        self.assertNotIn("private body", "\n".join(logged.output))
        self.assertNotIn("Source title", "\n".join(logged.output))
        self.assertEqual((result["records_archived"], result["items_compacted"]), (1, 1))
        self.assertEqual([row["record_id"] for row in self.rows("private_archive_refs")], ["item"])
        self.assertEqual(damaged(), before)
        with self.assertLogs("onionflash.deploy", level="WARNING"):
            self.assertEqual((self.sweep()["records_archived"], len(self.s3.puts)), (0, 1))

    def test_new_item_revision_can_be_archived_without_losing_earlier_proof(self):
        self.item()
        self.sweep()
        with self.db() as db:
            db.execute("UPDATE items SET content_hash=?,payload=? WHERE id='item'",
                       ("b" * 64, '{"content":"second body","updated_at":"2026-01-02T00:00:00Z"}'))
        self.sweep()
        refs = self.rows("private_archive_refs")
        self.assertEqual(len(refs), 2)
        self.assertNotEqual(refs[0]["object_key"], refs[1]["object_key"])

    def test_archive_references_retain_original_destinations_after_bucket_change(self):
        self.item("old-item")
        self.analysis("old-analysis")
        first = self.sweep()
        first_refs = self.rows("private_archive_refs")
        location_a = {"endpoint_url": "https://nyc3.digitaloceanspaces.com", "region": "nyc3", "bucket": "onionflash-data"}
        self.assertEqual(len(first_refs), 2)
        self.assertTrue(all(json.loads(row["destination_json"]) == location_a for row in first_refs))
        self.assertEqual(first["destination"], location_a)
        self.p.cfg["archive"].update(endpoint_url="https://second.example.org", region="ams3", bucket="second-onionflash",
                                     access_key_env="NEVER_ARCHIVE_KEY_NAME", secret_key_env="NEVER_ARCHIVE_SECRET_NAME")
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        self.item("new-item")
        self.analysis("new-analysis")
        second = self.sweep()
        refs = self.rows("private_archive_refs")
        for old in first_refs:
            self.assertIn(old, refs)
            self.assertIn(old["object_key"], self.s3.objects)
        location_b = {"endpoint_url": "https://second.example.org", "region": "ams3", "bucket": "second-onionflash"}
        new_refs = [row for row in refs if row not in first_refs]
        self.assertEqual(len(new_refs), 2)
        self.assertTrue(all(json.loads(row["destination_json"]) == location_b for row in new_refs))
        self.assertEqual(second["destination"], location_b)
        self.assertNotIn("NEVER_ARCHIVE", json.dumps({"refs": refs, "first": first, "second": second}))

    def test_a_limited_batch_keeps_the_excess_hot_and_asks_for_another_run(self):
        self.item("a")
        self.item("b")
        with patch.object(retention, "BATCH_SIZE", 1):
            result = self.sweep()
        self.assertEqual((result["records_archived"], result["batch_complete"]), (1, False))
        self.assertEqual(sum(map(self.hot, ("a", "b"))), 1)
        self.item("large", content="x" * 10000)
        with patch.object(retention, "MAX_BUNDLE_BYTES", 2048):
            result = self.sweep()
            self.assertEqual((result["records_archived"], result["over_limit"], result["batch_complete"]), (1, 1, False))
            # Without progress the batch is complete, so the daily schedule resumes.
            result = self.sweep()
            self.assertEqual((result["records_archived"], result["over_limit"], result["batch_complete"]), (0, 1, True))
        self.assertTrue(self.hot("large"))

    def test_scheduled_runs_continue_a_limited_sweep_and_otherwise_wait_for_the_interval(self):
        def run():
            self.assertEqual(self.run_cycle()["phases"]["retention"], "ok")
            return publish.read_json(self.p.state / "retention.json")

        self.item("a")
        self.item("large", content="x" * 10000)
        with patch.object(retention, "MAX_BUNDLE_BYTES", 2048):
            self.assertEqual([(r["records_archived"], r["batch_complete"]) for r in (run(), run())], [(1, False), (0, True)])
        self.item("new")
        run()
        self.assertTrue(self.hot("new"))
        self.p.cfg["archive"]["bucket"] = "another-onionflash"
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        self.assertEqual(run()["records_archived"], 2)  # a new destination does not wait
        self.item("later")
        record = publish.read_json(self.p.state / "retention.json")
        publish.atomic_json(self.p.state / "retention.json", {**record, "timestamp": time.time() - retention.INTERVAL_SECONDS})
        self.assertEqual(run()["records_archived"], 1)

    def test_disabled_retention_never_sweeps(self):
        self.item()
        self.p.cfg["retention"]["enabled"] = False
        self.assertNotIn("retention", self.run_cycle()["phases"])
        self.assertTrue(self.hot("item"))
        self.assertFalse((self.p.state / "retention.json").exists())

    def test_analyses_referenced_by_active_or_story_items_or_story_payload_stay_hot(self):
        self.item("ignored-free", status="ignored", story=None)
        self.item("published-item", status="published", story="story")
        self.item("ignored-story-source", status="ignored", story="story")
        self.item("pending-item", status="pending", story=None)
        self.item("held-item", status="rejected", story=None)
        with self.db() as db:
            for key, src in [("evict-ok", "ignored-free"), ("keep-pub", "published-item"),
                             ("keep-src", "ignored-story-source"), ("keep-pend", "pending-item"),
                             ("keep-held", "held-item"), ("keep-story-pin", None)]:
                db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                           (key, '{"evidence":["q"]}', OLD, '{"content":"snap"}', src))
            db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)",
                       ("pinned", "pinned", '{"editorial_analysis_key":"keep-story-pin"}', OLD, OLD))
        result = self.sweep()
        self.assertEqual((result["items_compacted"], result["analyses_evicted"], result["changed_records_preserved"]), (1, 1, 0))
        self.assertEqual({row["cache_key"] for row in self.rows("analyses")},
                         {"keep-pub", "keep-src", "keep-pend", "keep-held", "keep-story-pin"})

    def test_analysis_pinned_during_upload_is_preserved(self):
        self.item("race-item", status="ignored", story=None)
        with self.db() as db:
            db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                       ("race-analysis", '{"evidence":["q"]}', OLD, '{"content":"snap"}', "race-item"))

        def link_during_put(key):
            with self.db() as db:
                db.execute("UPDATE items SET story_id='story' WHERE id='race-item'")

        self.s3.on_put = link_during_put
        result = self.sweep()
        self.assertEqual(result["analyses_evicted"], 0)
        self.assertGreaterEqual(result["changed_records_preserved"], 1)
        self.assertEqual([row["cache_key"] for row in self.rows("analyses")], ["race-analysis"])

    def test_damaged_analyses_are_skipped_without_logging_private_payload(self):
        self.item("clean-item")
        self.analysis("clean-cache")
        with self.db() as db:
            db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                       ("bad-utf8", '{"secret":"sensitive text"}', OLD, '{"content":"private"}', None))
            db.execute("UPDATE analyses SET result=replace(result,'secret',x'eda080') WHERE cache_key='bad-utf8'")
            db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                       ("bad-blob", b"raw-blob-secret", OLD, '{"content":"private"}', None))
            db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                       ("bad-nan", '{"ok":true}', OLD, '{"content":"private"}', None))
            db.execute("ALTER TABLE analyses ADD COLUMN score REAL")
            db.execute("UPDATE analyses SET score=1e999 WHERE cache_key='bad-nan'")
        with patch.object(retention, "BATCH_SIZE", 2), self.assertLogs("onionflash.deploy", level="WARNING") as logged:
            result = self.sweep()
        self.assertEqual((result["items_compacted"], result["analyses_evicted"], result["changed_records_preserved"]), (1, 1, 0))
        self.assertEqual({r.args[0] for r in logged.records}, {"bad-utf8", "bad-blob", "bad-nan"})
        joined = "\n".join(logged.output)
        self.assertNotIn("sensitive text", joined)
        self.assertNotIn("raw-blob-secret", joined)
        self.assertNotIn("private", joined)

    def test_item_corrupted_to_invalid_utf8_during_upload_is_preserved_without_operational_error(self):
        self.item("corrupted-during-put", content="confidential item body")
        self.item("healthy-item", content="healthy item body")

        def corrupt_during_put(key):
            with self.db() as db:
                db.execute(
                    "UPDATE items SET payload=replace(payload,'confidential',CAST(x'eda080' AS TEXT)) "
                    "WHERE id='corrupted-during-put'"
                )

        self.s3.on_put = corrupt_during_put
        result = self.sweep()
        self.assertEqual(
            (result["records_archived"], result["items_compacted"], result["changed_records_preserved"]),
            (1, 1, 1),
        )
        self.assertEqual([row["record_id"] for row in self.rows("private_archive_refs")], ["healthy-item"])
        with self.db() as db:
            healthy_payload = json.loads(db.execute("SELECT payload FROM items WHERE id='healthy-item'").fetchone()[0])
        self.assertNotIn("content", healthy_payload)

        # Pinned analyses skip _decode_row entirely before reading full analysis rows:
        self.analysis("pinned-key")
        self.analysis("unpinned-key")
        decode_calls = []
        real_decode = retention._decode_row

        def spy_decode(db, table, columns, where, param, *, key_col):
            if table == "analyses":
                decode_calls.append(param)
            return real_decode(db, table, columns, where, param, key_col=key_col)

        with patch.object(retention, "_decode_row", side_effect=spy_decode), self.db() as db:
            yielded = list(retention._analyses(db, retention.QUERIES[1][1], "9999-12-31T23:59:59Z", {"pinned-key"}))
        self.assertEqual([r["cache_key"] for r in yielded], ["unpinned-key"])
        self.assertEqual(len(decode_calls), 1)


if __name__ == "__main__":
    unittest.main()
