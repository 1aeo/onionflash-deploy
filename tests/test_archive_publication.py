"""Cross-layer checks for permanent storage and guarded Worker activation."""
import copy
from contextlib import closing, nullcontext
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import publish
from archive_store import ArchiveStore, HOT_FILES
from onionflash.cli import build
from onionflash.config import DEFAULTS
from onionflash.store import Store, backup_database
from test_archive_store import FakeS3
from test_recovery import OLD, NEW


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
        # Storage is faked, so its anonymous access is too: public objects are readable and private ones refused.
        anonymous = patch.object(self.p, "unsigned_read_problem", return_value=None)
        anonymous.start(); self.addCleanup(anonymous.stop)
        cfg = copy.deepcopy(DEFAULTS)
        cfg["site"]["base_url"] = self.p.base_url
        cfg["storage"].update(database=str(self.p.database), output_dir=str(self.p.public))
        self.app_config = cfg
        with Store(self.p.database) as store:
            build(cfg, store)

    def add_stories(self, *identities):
        with Store(self.p.database) as store:
            for identity in identities:
                story = {"id": identity, "title": "Earlier story", "summary": "An earlier published report.",
                    "why_it_matters": "", "category": "network", "priority": "normal", "confidence": "reported",
                    "state": "developing", "tags": ["network"], "sources": [{"url": "https://example.org/report"}],
                    "published_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z"}
                store.db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)", (story["id"], story["id"], json.dumps(story), story["published_at"], story["updated_at"]))
            store.db.commit()
            build(self.app_config, store)

    def test_only_hot_files_enter_cloudflare_and_worker_paths_are_scoped(self):
        result = self.p.archive.sync()
        snapshot = self.root / "snapshot"
        hot = self.p.archive.snapshot_hot(snapshot, result["generation"])
        info = {"manifest_sha256": result["manifest_sha256"]}
        config = json.loads(self.p.make_config(self.p.prepare_assets(snapshot), archive=info).read_text())
        self.assertEqual(config["vars"]["ARCHIVE_MANIFEST_SHA"], info["manifest_sha256"])
        self.assertIn("/onionflash/news/*", config["assets"]["run_worker_first"])
        self.assertIn("/onionflash/topics/*", config["assets"]["run_worker_first"])
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
            with self.assertRaisesRegex(ValueError, "previously published article"):
                self.p.publish()
        command.assert_not_called()

    def test_a_restored_older_database_cannot_drop_a_published_permalink(self):
        self.add_stories("alpha")
        older = self.root / "older.sqlite3"
        backup_database(self.p.database, older)
        self.add_stories("bravo")
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {}})
        def upload(arguments, metadata):
            metadata.write_text(json.dumps({"type": "version-upload", "version_id": NEW,
                                            "preview_url": "https://candidate.example.workers.dev"}))
        live = lambda: publish.read_json(self.p.state / "publication.json")["version_id"]
        with patch.object(self.p, "current_version", side_effect=live), patch.object(self.p, "command", side_effect=upload) as command, \
                patch.object(self.p, "api", return_value={"subdomain": "example"}), patch.object(self.p, "verify"), \
                patch.object(self.p, "app_version", return_value="test"), patch.object(self.p, "activate"):
            self.assertEqual(self.p.publish()["articles"], ["news/alpha/index.html", "news/bravo/index.html"])
            # The documented recovery: restore the older backup, then rebuild from it.
            with closing(sqlite3.connect(older)) as source, closing(sqlite3.connect(self.p.database)) as target:
                source.backup(target)
            with Store(self.p.database) as store:
                build(self.app_config, store, force=True)
            with self.assertRaisesRegex(ValueError, "previously published article"):
                self.p.publish()
        self.assertEqual(command.call_count, 1)  # nothing was uploaded after the restore

    def test_a_restored_older_database_cannot_drop_a_static_permalink_behind_a_leftover_page(self):
        self.p.cfg["archive"]["enabled"] = False
        self.add_stories("alpha")
        older = self.root / "older.sqlite3"
        backup_database(self.p.database, older)
        self.add_stories("bravo")
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {}})
        def upload(arguments, metadata):
            metadata.write_text(json.dumps({"type": "version-upload", "version_id": NEW,
                                            "preview_url": "https://candidate.example.workers.dev"}))
        live = lambda: publish.read_json(self.p.state / "publication.json")["version_id"]
        with patch.object(self.p, "current_version", side_effect=live), patch.object(self.p, "command", side_effect=upload) as command, \
                patch.object(self.p, "api", return_value={"subdomain": "example"}), patch.object(self.p, "verify"), \
                patch.object(self.p, "app_version", return_value="test"), patch.object(self.p, "activate"):
            self.p.publish()
            self.assertIn("news/bravo/index.html", publish.read_json(self.p.state / "publication.json")["manifest"])
            with closing(sqlite3.connect(older)) as source, closing(sqlite3.connect(self.p.database)) as target:
                source.backup(target)
            with Store(self.p.database) as store:
                build(self.app_config, store, force=True)
            # The newer page stays in the output, but the restored database no longer has it.
            self.assertTrue((self.p.public / "news/bravo/index.html").is_file())
            with self.assertRaisesRegex(ValueError, "previously published article"):
                self.p.publish()
        self.assertEqual(command.call_count, 1)

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
        self.add_stories("r0", "r1", "r2")
        self.p.publication_readiness(self.p.validate(self.p.public))
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
        response = SimpleNamespace(headers={"ETag": '"' + sha + '"', "Content-Length": "100000000"}, read=body_read)
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
