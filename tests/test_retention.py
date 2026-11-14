"""Cold private evidence stays recoverable before any hot data is compacted."""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import gzip, hashlib, json, sqlite3, time, unittest
from unittest.mock import patch

from botocore.exceptions import EndpointConnectionError

from archive_store import ArchiveStore
from onionflash.store import Store
import publish
import retention
from tests.fakes import DeployTestCase, FakeS3

NOW = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
OLD = "2026-01-01T12:00:00Z"
RECENT = "2026-11-09T12:00:00Z"


class RetentionTests(DeployTestCase):
    def setUp(self):
        super().setUp()
        self.p.state.rename(self.directory / "disk")
        self.p.state.symlink_to(self.directory / "disk")
        self.p.cfg["archive"]["enabled"] = self.p.cfg["retention"]["enabled"] = True
        self.s3 = FakeS3()
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        self.p.delay = lambda attempt: None
        self.exposure = None
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
            db.execute(
                "INSERT INTO items(id,url,source_id,payload,content_hash,cluster_key,first_seen,last_seen,status,story_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (identity, payload["url"], "source", json.dumps(payload), hashlib.sha256(content.encode()).hexdigest(),
                 "cluster:" + identity, OLD, date, status, story),
            )
        return payload

    def analysis(self, key="cache", date=OLD, source_item_id="item"):
        with self.db() as db:
            db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                       (key, '{"evidence":["private quote"]}', date, '{"content":"collected source snapshot"}', source_item_id))

    def rows(self, table):
        with self.db() as db:
            return [dict(row) for row in db.execute("SELECT * FROM " + table)]

    def hot(self, identity):
        with self.db() as db:
            return "content" in json.loads(db.execute("SELECT payload FROM items WHERE id=?", (identity,)).fetchone()[0])

    def sweep(self):
        return retention.archive_evidence(self.p.archive, NOW)

    def run_cycle(self):
        with patch.object(self.p, "application", return_value="{}"), patch.object(self.p, "publish"):
            return self.p.run_cycle()

    def test_only_old_ignored_unreferenced_items_and_unpinned_analyses_compact(self):
        payload = self.item()
        self.item("published", status="published", story="story")
        self.item("story-source", story="story")
        self.item("issue", kind="gitlab_issue", source_content="private issue description")
        self.item("pending", status="pending")
        self.item("held", status="rejected")
        self.item("recent", date=RECENT)
        self.analysis()
        for key, src in [("keep-pub", "published"), ("keep-src", "story-source"), ("keep-pend", "pending"),
                         ("keep-held", "held"), ("keep-story-pin", None)]:
            self.analysis(key, source_item_id=src)
        with self.db() as db:
            db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)",
                       ("pinned", "pinned", '{"editorial_analysis_key":"keep-story-pin"}', OLD, OLD))
        before_item = next(row for row in self.rows("items") if row["id"] == "item")
        before_analysis = next(row for row in self.rows("analyses") if row["cache_key"] == "cache")
        result = self.sweep()
        self.assertEqual((result["records_archived"], result["items_compacted"], result["analyses_evicted"]), (2, 1, 1))
        current = {row["id"]: row for row in self.rows("items")}
        self.assertEqual(json.loads(current["item"]["payload"]), {k: v for k, v in payload.items() if k != "content"})
        self.assertTrue(all(self.hot(k) for k in ("published", "story-source", "issue", "pending", "held", "recent")))
        bundle = json.loads(gzip.decompress(self.s3.objects[result["object_key"]]["body"]))
        self.assertEqual(bundle["records"], [{"kind": "item", "row": before_item}, {"kind": "analysis", "row": before_analysis}])
        self.assertEqual({row["cache_key"] for row in self.rows("analyses")},
                         {"keep-pub", "keep-src", "keep-pend", "keep-held", "keep-story-pin"})
        self.assertEqual(self.sweep()["records_archived"], 0)
        self.assertEqual(len(self.s3.puts), 1)

    def test_unverified_or_readable_bundle_never_compacts(self):
        self.item()
        self.analysis()
        before = self.rows("items"), self.rows("analyses")
        changes = {"content": lambda s: s.update(body=b"x" + s["body"][1:]),
                   "metadata": lambda s: s.update(metadata={"sha256": "0" * 64}),
                   "acl": lambda s: s.update(acl="public-read")}
        for failure in ("upload", "content", "metadata", "acl", "readable"):
            with self.subTest(failure=failure):
                self.p.archive.client = s3 = FakeS3()
                s3.fail_puts = 99 if failure == "upload" else 0
                s3.on_put = lambda key, f=failure: changes.get(f, lambda _: None)(s3.objects[key])
                self.exposure = "was publicly readable" if failure == "readable" else None
                with self.assertRaises((ValueError, EndpointConnectionError)):
                    self.sweep()
                self.assertEqual((self.rows("items"), self.rows("analyses")), before)
                self.assertEqual(self.rows("private_archive_refs"), [])
        self.assertEqual(s3.objects, {})

    def test_concurrent_mutations_and_corruption_during_upload_preserve_rows(self):
        self.item()
        self.analysis()
        self.item("race-item")
        self.analysis("race-analysis", source_item_id="race-item")
        self.item("corrupted-during-put", content="confidential item body")
        self.item("healthy-item", content="healthy item body")

        def concurrent(key):
            with self.db() as db:
                db.execute("UPDATE items SET status='pending' WHERE id='item'")
                db.execute("UPDATE analyses SET result='new analysis' WHERE cache_key='cache'")
                db.execute("UPDATE items SET story_id='story' WHERE id='race-item'")
                db.execute("UPDATE items SET payload=replace(payload,'confidential',CAST(x'eda080' AS TEXT)) WHERE id='corrupted-during-put'")

        self.s3.on_put = concurrent
        result = self.sweep()
        self.assertEqual((result["records_archived"], result["items_compacted"], result["analyses_evicted"], result["changed_records_preserved"]),
                         (1, 1, 0, 5))
        self.assertTrue(self.hot("item"))
        self.assertFalse(self.hot("healthy-item"))
        self.assertEqual({r["cache_key"] for r in self.rows("analyses")}, {"cache", "race-analysis"})
        self.assertEqual({r["record_id"] for r in self.rows("private_archive_refs")}, {"healthy-item"})

    def test_damaged_rows_are_skipped_without_payload_leaks_and_pinned_analyses_skip_decode(self):
        for identity, damage in {
            "broken": "'{'", "infinity": "replace(payload,'\"Source title\"','Infinity')",
            "array": "'[1]'", "blob": "CAST(payload AS BLOB)", "bytes": "replace(payload,'Source',x'eda080')",
            "escaped": "replace(payload,'Source','\\ud800')", "extra": "payload||char(0)||'x'",
        }.items():
            self.item(identity)
            with self.db() as db:
                db.execute("UPDATE items SET payload=" + damage + " WHERE id=?", (identity,))
        self.item("clean-item")
        self.analysis("clean-cache", source_item_id="clean-item")
        with self.db() as db:
            db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)",
                       ("pin-story", "pin-story", '{"editorial_analysis_key":"pinned-bad-utf8"}', OLD, OLD))
            for key, res in [("bad-utf8", '{"secret":"sensitive text"}'), ("pinned-bad-utf8", '{"secret":"pinned"}'),
                             ("bad-blob", b"raw-blob-secret"), ("bad-nan", '{"ok":true}')]:
                db.execute("INSERT INTO analyses(cache_key,result,created_at,source_payload,source_item_id) VALUES(?,?,?,?,?)",
                           (key, res, OLD, '{"content":"private"}', None))
            db.execute("UPDATE analyses SET result=replace(result,'secret',x'eda080') WHERE cache_key IN ('bad-utf8','pinned-bad-utf8')")
            db.execute("ALTER TABLE analyses ADD COLUMN score REAL")
            db.execute("UPDATE analyses SET score=1e999 WHERE cache_key='bad-nan'")
        with patch.object(retention, "BATCH_SIZE", 2), self.assertLogs("onionflash.deploy", level="WARNING") as logged:
            result = self.sweep()
        self.assertEqual((result["items_compacted"], result["analyses_evicted"]), (1, 1))
        # Pinned analyses skip full row decoding, so pinned-bad-utf8 is neither archived nor warned on.
        self.assertEqual({r.args[1] for r in logged.records}, {"bytes", "escaped", "extra", "bad-utf8", "bad-blob", "bad-nan"})
        joined = "\n".join(logged.output)
        for secret in ("private body", "Source title", "sensitive text", "raw-blob-secret", "private"):
            self.assertNotIn(secret, joined)

    def test_revisions_bucket_changes_batch_limits_and_schedule_intervals(self):
        self.item("old-item")
        self.analysis("old-analysis", source_item_id="old-item")
        first = self.sweep()
        with self.db() as db:
            db.execute("UPDATE items SET content_hash=?,payload=? WHERE id='old-item'",
                       ("b" * 64, '{"content":"second body","updated_at":"2026-01-02T00:00:00Z"}'))
        self.p.cfg["archive"].update(endpoint_url="https://second.example.org", region="ams3", bucket="second-onionflash",
                                     access_key_env="NEVER_ARCHIVE_KEY_NAME", secret_key_env="NEVER_ARCHIVE_SECRET_NAME")
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        second = self.sweep()
        refs = self.rows("private_archive_refs")
        self.assertEqual(len(refs), 3)
        self.assertEqual(json.loads(refs[0]["destination_json"])["bucket"], "onionflash-data")
        self.assertEqual(json.loads(refs[-1]["destination_json"])["bucket"], "second-onionflash")
        self.assertNotIn("NEVER_ARCHIVE", json.dumps({"refs": refs, "first": first, "second": second}))

        self.item("a")
        self.item("large", content="x" * 10000)
        def run():
            self.assertEqual(self.run_cycle()["phases"]["retention"], "ok")
            return publish.read_json(self.p.state / "retention.json")
        with patch.object(retention, "MAX_BUNDLE_BYTES", 2048):
            self.assertEqual([(r["records_archived"], r["batch_complete"]) for r in (run(), run())], [(1, False), (0, True)])
        self.item("new")
        run()
        self.assertTrue(self.hot("new"))
        record = publish.read_json(self.p.state / "retention.json")
        publish.atomic_json(self.p.state / "retention.json", {**record, "timestamp": time.time() - retention.INTERVAL_SECONDS})
        self.assertEqual(run()["records_archived"], 2)

        self.item("disabled-item")
        self.p.cfg["retention"]["enabled"] = False
        self.assertNotIn("retention", self.run_cycle()["phases"])
        self.assertTrue(self.hot("disabled-item"))


if __name__ == "__main__":
    unittest.main()
