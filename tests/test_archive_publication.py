"""Cross-layer checks for permanent storage and guarded Worker activation."""
import copy
from contextlib import closing, nullcontext
import gzip
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import publish
from archive_store import ArchiveStore, HOT_FILES
from onionflash import editorial
from onionflash.cli import build
from onionflash.config import DEFAULTS
from onionflash.store import Store
from test_archive_store import FakeS3
from test_recovery import FakeSpaces, OLD, NEW


class PermanentPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        env = patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "a" * 32,
            "CLOUDFLARE_API_TOKEN": "fake-cloudflare-credential", "CLOUDFLARE_ZONE_TOKEN": "fake-zone-credential",
            "CLOUDFLARE_ZONE_ID": "b" * 32, "DO_SPACES_KEY": "fake-storage-key", "DO_SPACES_SECRET": "fake-storage-secret"})
        env.start(); self.addCleanup(env.stop)
        (self.root / "deployment.toml").write_text((publish.ROOT / "deployment.example.toml").read_text())
        (self.root / "onionflash.toml").write_text('[site]\nbase_url="https://example.com/onionflash/"\n[storage]\ndatabase="state/news.sqlite3"\noutput_dir="public"\n[[sources]]\nid="test"\nurl="https://example.org/feed.xml"\n')
        self.p = publish.Publisher(self.root / "deployment.toml")
        self.p.cfg["archive"]["enabled"] = True
        self.s3 = FakeS3()
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        self.addCleanup(self.p.close)
        cfg = copy.deepcopy(DEFAULTS)
        cfg["site"]["base_url"] = self.p.base_url
        cfg["storage"].update(database=str(self.p.database), output_dir=str(self.p.public))
        self.app_config = cfg
        with Store(self.p.database) as store:
            build(cfg, store)

    def test_only_hot_files_enter_cloudflare_and_worker_paths_are_scoped(self):
        result = self.p.archive.sync()
        snapshot = self.root / "snapshot"
        hot = self.p.archive.snapshot_hot(snapshot, result["generation"])
        self.assertEqual(set(self.p.validate(snapshot, hot_only=True)), HOT_FILES)
        info = {"manifest_sha256": result["manifest_sha256"]}
        config = json.loads(self.p.make_config(self.p.prepare_assets(snapshot), archive=info).read_text())
        self.assertEqual(config["vars"]["ARCHIVE_MANIFEST_SHA"], info["manifest_sha256"])
        self.assertIn("/onionflash/news/*", config["assets"]["run_worker_first"])
        self.assertNotIn("/onionflash/*", config["assets"]["run_worker_first"])
        self.assertEqual(set(hot), HOT_FILES)

    def test_incomplete_storage_sync_never_activates_a_worker(self):
        with patch.object(self.p.archive, "sync", return_value={"ready": False, "pending": 1}), patch.object(self.p, "command") as command:
            with self.assertRaisesRegex(ValueError, "catching up"):
                self.p.publish()
        command.assert_not_called()

    def test_migration_preserves_previously_published_article_paths(self):
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {"news/legacy/index.html": "f" * 64}})
        with patch.object(self.p, "command") as command:
            with self.assertRaisesRegex(ValueError, "not been verified"):
                self.p.publish()
        command.assert_not_called()

    def test_initial_migration_waits_for_the_bounded_article_backfill(self):
        with Store(self.p.database) as store:
            store.db.execute("INSERT INTO public_render_queue(story_id) VALUES('still-rendering')")
            store.db.commit()
        with patch.object(self.p, "command") as command:
            with self.assertRaisesRegex(ValueError, "articles pending"):
                self.p.publish()
        command.assert_not_called()

    def test_legacy_publication_cannot_drop_a_known_artifact_during_restore(self):
        self.p.cfg["archive"]["enabled"] = False
        with Store(self.p.database) as store:
            store.db.execute("INSERT INTO public_artifacts(path,sha256,size,content_type,updated_at) VALUES(?,?,?,?,?)",
                             ('news/older/index.html', 'a' * 64, 10, 'text/html', '2026-11-10T00:00:00Z'))
            store.db.commit()
        with patch.object(self.p, "command") as command:
            with self.assertRaisesRegex(ValueError, "snapshot is incomplete"):
                self.p.publish()
        command.assert_not_called()

    def test_existing_spaces_copies_remain_ready_during_local_reconstruction(self):
        with Store(self.p.database) as store:
            for number in range(3):
                story = {"id": f"r{number}", "title": "Earlier story", "summary": "An earlier published report.",
                    "why_it_matters": "", "category": "network", "priority": "normal", "confidence": "reported",
                    "state": "developing", "tags": ["network"], "sources": [{"url": "https://example.org/report"}],
                    "published_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z"}
                store.db.execute("INSERT INTO stories VALUES(?,?,?,?,?)", (story["id"], story["id"], json.dumps(story), story["published_at"], story["updated_at"]))
            store.db.commit()
            build(self.app_config, store)
        original = self.p.archive.sync()
        key = "news/r2/index.html"
        old_sha = self.p.archive.manifest([key], original["generation"])[key]
        shutil.rmtree(self.p.public)
        self.app_config["storage"]["render_batch_size"] = 1
        with Store(self.p.database) as store:
            result = build(self.app_config, store, force=True)
        self.assertEqual(result["articles_pending"], 2)
        self.assertFalse((self.p.public / key).exists())
        ready = self.p.archive.sync()
        self.assertTrue(ready["ready"])
        self.assertEqual(self.p.archive.manifest([key], ready["generation"])[key], old_sha)
        self.assertIn(self.p.archive.cfg["public_prefix"] + "objects/" + old_sha, self.s3.objects)

    def test_prepared_archive_is_bound_to_the_publication_receipt(self):
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {}})
        commands = []
        def command(arguments, metadata=None):
            commands.append(arguments)
            if metadata:
                metadata.write_text(json.dumps({"type": "version-upload", "version_id": NEW,
                                               "preview_url": "https://candidate.example.workers.dev"}) + "\n")
        with patch.object(self.p, "current_version", return_value=OLD), patch.object(self.p, "command", side_effect=command), patch.object(self.p, "api", return_value={"subdomain": "example"}), patch.object(self.p, "verify") as verify, patch.object(self.p, "app_version", return_value="test"), patch.object(self.p, "activate") as activate:
            result = self.p.publish()
        self.assertEqual(result["version_id"], NEW)
        self.assertIn("archive", result)
        self.assertEqual(set(result["manifest"]), HOT_FILES | {"feed.xml", "feed.json"})
        self.assertEqual(verify.call_args.kwargs["archive"], result["archive"])
        self.assertEqual(len(commands), 1)
        activate.assert_called_once()
        self.assertEqual(publish.read_json(self.p.state / "receipts" / (NEW + ".json"))["archive"], result["archive"])

    def test_large_feeds_are_verified_with_head_not_buffered(self):
        sha = "f" * 64
        body_read = Mock(side_effect=AssertionError("HEAD must not read the feed body"))
        response = SimpleNamespace(status=200, headers={"ETag": '"' + sha + '"', "Content-Length": "100000000"},
                                   geturl=lambda: self.p.base_url + "feed.xml", read=body_read)
        self.p.net["max_response_bytes"] = 1
        with patch("publish.VERIFY_PATHS", ("feed.xml",)), \
                patch("publish.urlopen", return_value=nullcontext(response)) as request:
            self.p.verify(self.p.base_url, {"feed.xml": sha}, archive={"manifest_sha256": "a" * 64})
        self.assertEqual(request.call_args.args[0].get_method(), "HEAD")
        body_read.assert_not_called()

    def test_worker_and_binding_changes_invalidate_unchanged_asset_identity(self):
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
        self.assertEqual(len({first, second, third}), 3)

    def test_sqlite_only_backups_preserve_the_permanent_manifest_catalog(self):
        story_id = "operator-corrected-story"
        stamp = "2026-11-01T00:00:00Z"
        story = {"id": story_id, "title": "Reported authority incident", "summary": "An operator reported an incident.",
                 "why_it_matters": "Original wording", "sources": [{"url": "https://example.org/report", "title": "Report"}],
                 "published_at": stamp, "updated_at": stamp}
        with Store(self.p.database) as store:
            with store.db:
                store.db.execute("INSERT INTO stories VALUES(?,?,?,?,?)", (story_id, "operator-correction", json.dumps(story), stamp, stamp))
            build(self.app_config, store)
            original = editorial.show(store.db, story_id)
            editorial.correct(store.db, story_id, {"why_it_matters": "Corrected, source-supported wording."},
                              reason="Removed unsupported advice", expect_hash=original["hash"])
            build(self.app_config, store)
        sync = self.p.archive.sync()
        snapshot = self.root / "initial-snapshot"
        hot = self.p.archive.snapshot_hot(snapshot, sync["generation"])
        manifest = {**hot, **self.p.archive.manifest(["feed.xml", "feed.json"], sync["generation"])}
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": manifest,
            "archive": {"generation": sync["generation"], "manifest_sha256": sync["manifest_sha256"]}})
        self.p.cfg["spaces"]["enabled"] = True
        backups = FakeSpaces()
        with patch("boto3.client", return_value=backups), patch.object(self.p, "request", side_effect=HTTPError("unsigned", 403, "private", {}, None)) as privacy_check:
            self.p.backup(force=True)
        restored = self.root / "restored.sqlite3"
        restored.write_bytes(gzip.decompress(backups.uploads[0][2]))
        with closing(sqlite3.connect(restored)) as database:
            self.assertEqual(database.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(database.execute("SELECT value FROM metadata WHERE key='archive_manifest_sha256'").fetchone()[0], sync["manifest_sha256"])
            self.assertGreater(database.execute("SELECT COUNT(*) FROM public_artifacts").fetchone()[0], 0)
            self.assertEqual(database.execute("SELECT locked FROM editorial_controls WHERE story_id=?", (story_id,)).fetchone()[0], 1)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM editorial_revisions WHERE story_id=?", (story_id,)).fetchone()[0], 1)
        with Store(restored) as store:
            before = editorial.show(store.db, story_id)
            with store.db:
                store.db.execute("UPDATE stories SET payload='{}' WHERE id=?", (story_id,))
            self.assertEqual(editorial.show(store.db, story_id)["hash"], before["hash"])
        self.assertEqual(privacy_check.call_args.args[1], "HEAD")
        self.assertNotIn("token", privacy_check.call_args.kwargs)
