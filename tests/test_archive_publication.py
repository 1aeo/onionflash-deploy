"""Cross-layer checks for permanent storage and guarded Worker activation."""
from contextlib import closing
import hashlib, json, shutil, sqlite3, unittest
from unittest.mock import patch

from onionflash.cli import build
from onionflash.store import Store, backup_database
import publish
from tests.fakes import DeployTestCase, NEW, OLD


class PermanentPublicationTests(DeployTestCase):
    """Integration tests covering permanent archive synchronization and Worker publication guards."""

    OUTPUT_DIR = "public"

    def setUp(self):
        """Enable archive storage and run an initial baseline site build."""
        super().setUp()
        self.enable_archive()
        self.rebuild()

    def rebuild(self, *, force=False):
        """Run one application build pass against the publisher's SQLite store."""
        with Store(self.p.database) as store:
            return build(self.p.app, store, force=force)

    def add_stories(self, *identities):
        """Insert published stories for identities and rebuild the public output."""
        with self.db() as db:
            for identity in identities:
                story = {
                    "id": identity, "title": "Earlier story", "summary": "An earlier published report.",
                    "why_it_matters": "", "category": "network", "priority": "normal", "confidence": "reported",
                    "state": "developing", "tags": ["network"], "sources": [{"url": "https://example.org/report"}],
                    "published_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z",
                }
                db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)",
                           (story["id"], story["id"], json.dumps(story), story["published_at"], story["updated_at"]))
        self.rebuild()

    def test_worker_bundle_contents_and_static_feed_budget_checks(self):
        """Verify hot snapshot contents, archive Worker bindings, and static feed validation."""
        self.add_stories("alpha")
        result = self.p.archive.sync()
        snapshot = self.root / "snapshot"
        manifest = self.p.archive.snapshot(snapshot, result["generation"])
        ledger = self.p.publication_readiness()
        self.assertEqual(manifest, {path: sha for path, sha in ledger.items() if path != "news/alpha/story.json"})
        self.assertFalse((snapshot / "news").exists())
        self.assertTrue((snapshot / "tags/security.xml").is_file())
        info = {"manifest_sha256": result["manifest_sha256"]}
        config = json.loads(self.p.make_config(self.p.prepare_assets(snapshot), archive=info).read_text())
        self.assertEqual(config["vars"], {"BASE_PATH": "/onionflash", "ARCHIVE_ORIGIN": self.p.archive.cfg["public_origin"],
                                          "ARCHIVE_MANIFEST_SHA": info["manifest_sha256"]})
        self.assertNotIn("run_worker_first", config["assets"])

        with patch.object(self.p, "command") as command, patch.object(self.p, "current_version", return_value=None):
            with patch.object(publish, "MAX_FILES", 3), self.assertRaisesRegex(ValueError, "file count budget"):
                self.p.publish()
            feed = self.p.public / "feed.xml"
            feed.write_text("<html>not a feed</html>")
            body = feed.read_bytes()
            with self.db() as db:
                db.execute("UPDATE public_artifacts SET sha256=?,size=? WHERE path='feed.xml'",
                           (hashlib.sha256(body).hexdigest(), len(body)))
            with self.assertRaisesRegex(ValueError, "Expected RSS feed"):
                self.p.publish()
        command.assert_not_called()

    def test_sync_readiness_migration_backfill_and_incomplete_snapshot_guards(self):
        """Refuse publication while archive sync, initial migration queue, or local files are incomplete."""
        with patch.object(self.p, "command") as command:
            with patch.object(self.p.archive, "sync", return_value={"ready": False, "pending": 1}), \
                    self.assertRaisesRegex(ValueError, "catching up"):
                self.p.publish()

            publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {"news/legacy/index.html": "f" * 64}})
            with self.assertRaisesRegex(ValueError, "previously published article"):
                self.p.publish()
            (self.p.state / "publication.json").unlink()

            with self.db() as db:
                db.execute("INSERT INTO public_render_queue(story_id) VALUES('still-rendering')")
            with self.assertRaisesRegex(ValueError, "articles pending"):
                self.p.publish()
            with self.db() as db:
                db.execute("DELETE FROM public_render_queue")

            self.p.cfg["archive"]["enabled"] = False
            with self.db() as db:
                db.execute("INSERT INTO public_artifacts(path,sha256,size,content_type,updated_at) VALUES(?,?,?,?,?)",
                           ("news/older/index.html", "a" * 64, 10, "text/html", "2026-11-10T00:00:00Z"))
            with self.assertRaisesRegex(ValueError, "snapshot is incomplete"):
                self.p.publish()
        command.assert_not_called()

    def test_restored_older_database_cannot_drop_published_permalink_in_archive_or_static_mode(self):
        """Reject publishing a restored older database missing previously published permalinks."""
        for archive_enabled in (True, False):
            with self.subTest(archive_enabled=archive_enabled):
                if not archive_enabled:
                    self.setUp()
                self.p.cfg["archive"]["enabled"] = archive_enabled
                self.add_stories("alpha")
                older = self.root / f"older-{archive_enabled}.sqlite3"
                backup_database(self.p.database, older)
                self.add_stories("bravo")
                self.fake_cloudflare(OLD)
                receipt = self.p.publish()
                self.assertEqual({p for p in receipt["manifest"] if publish.ARTICLE.fullmatch(p)},
                                 {"news/alpha/index.html", "news/bravo/index.html"})
                with closing(sqlite3.connect(older)) as source, closing(sqlite3.connect(self.p.database)) as target:
                    source.backup(target)
                self.rebuild(force=True)
                if not archive_enabled:
                    self.assertTrue((self.p.public / "news/bravo/index.html").is_file())
                with self.assertRaisesRegex(ValueError, "previously published article"):
                    self.p.publish()
                self.assertEqual(self.p.command.call_count, 1)

    def test_local_reconstruction_receipt_binding_verification_and_identity(self):
        """Verify incremental rebuilds, archive receipt bindings, and publication_identity digests."""
        self.add_stories("r0", "r1", "r2")
        self.p.publication_readiness(self.p.validate(self.p.public))
        original = self.p.archive.sync()
        key = "news/r2/index.html"
        old_sha = self.p.archive.snapshot(self.root / "before", original["generation"])[key]
        shutil.rmtree(self.p.public)
        self.p.app["storage"]["render_batch_size"] = 1
        self.assertEqual(self.rebuild(force=True)["articles_pending"], 2)
        self.assertFalse((self.p.public / key).exists())
        ready = self.p.archive.sync()
        self.assertTrue(ready["ready"])
        self.assertEqual(self.p.archive.snapshot(self.root / "after", ready["generation"])[key], old_sha)

        while self.rebuild().get("articles_pending"):
            pass
        self.fake_cloudflare(OLD)
        result = self.p.publish()
        self.assertEqual(result["version_id"], NEW)
        self.assertEqual(self.p.verify.call_args.kwargs["archive"], result["archive"])
        self.assertEqual(self.p.command.call_count, 1)
        self.p.activate.assert_called_once()
        self.assertEqual(json.loads((self.p.state / "wrangler.json").read_text())["vars"]["ARCHIVE_MANIFEST_SHA"],
                         result["archive"]["manifest_sha256"])

        fake_root = self.root / "code"
        fake_root.mkdir()
        worker = fake_root / "worker.mjs"
        worker.write_text("version one")
        with patch.object(publish, "ROOT", fake_root):
            first = self.p.publication_identity({"index.html": "same"})
            worker.write_text("version two")
            second = self.p.publication_identity({"index.html": "same"})
            self.p.base_path = "/different"
            third = self.p.publication_identity({"index.html": "same"})
            manifests = {self.p.publication_identity({"index.html": "same"}, {"manifest_sha256": c * 64}) for c in "ab"}
        self.assertEqual(len({first, second, third, *manifests}), 5)


if __name__ == "__main__":
    unittest.main()
