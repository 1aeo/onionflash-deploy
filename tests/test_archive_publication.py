"""Cross-layer checks for permanent storage and guarded Worker activation."""
import copy
from contextlib import closing
import hashlib
import json
import shutil
import sqlite3
import unittest
from unittest.mock import patch

import publish
from archive_store import ArchiveStore
from onionflash.cli import build
from onionflash.config import DEFAULTS
from onionflash.store import Store, backup_database
from tests.fakes import FakeS3, NEW, OLD, make_publisher


class PermanentPublicationTests(unittest.TestCase):
    def setUp(self):
        self.root, self.p = make_publisher(self, output_dir="public")
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

    def test_every_file_but_the_permalinks_enters_the_worker_bundle(self):
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
        # No bundled asset shadows a permalink, so the Worker answers them without run_worker_first.
        self.assertNotIn("run_worker_first", config["assets"])

    def test_the_worker_bundle_gets_the_static_feed_and_budget_checks(self):
        self.add_stories("alpha")
        with patch.object(self.p, "command") as command, patch.object(self.p, "current_version", return_value=None):
            with patch.object(publish, "MAX_FILES", 3), self.assertRaisesRegex(ValueError, "file count budget"):
                self.p.publish()
            feed = self.p.public / "feed.xml"
            feed.write_text("<html>not a feed</html>")
            body = feed.read_bytes()
            with Store(self.p.database) as store:  # as if the core had rendered it, so the ledger hash matches
                store.db.execute("UPDATE public_artifacts SET sha256=?,size=? WHERE path='feed.xml'",
                                 (hashlib.sha256(body).hexdigest(), len(body)))
                store.db.commit()
            with self.assertRaisesRegex(ValueError, "Expected RSS feed"):
                self.p.publish()
        command.assert_not_called()

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
            self.assertEqual({path for path in self.p.publish()["manifest"] if path.startswith("news/")},
                             {"news/alpha/index.html", "news/bravo/index.html"})
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
        old_sha = self.p.archive.snapshot(self.root / "before", original["generation"])[key]
        shutil.rmtree(self.p.public)
        self.app_config["storage"]["render_batch_size"] = 1
        with Store(self.p.database) as store:
            result = build(self.app_config, store, force=True)
        self.assertEqual(result["articles_pending"], 2)
        self.assertFalse((self.p.public / key).exists())
        ready = self.p.archive.sync()
        self.assertTrue(ready["ready"])
        self.assertEqual(self.p.archive.snapshot(self.root / "after", ready["generation"])[key], old_sha)
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
        self.assertEqual(result["manifest"], self.p.publication_readiness())
        self.assertEqual(verify.call_args.kwargs["archive"], result["archive"])
        self.assertEqual(len(commands), 1)
        activate.assert_called_once()
        self.assertEqual(publish.read_json(self.p.state / "receipts" / (NEW + ".json"))["archive"], result["archive"])
        # The uploaded version binds the root this receipt records.
        config = json.loads((self.p.state / "wrangler.json").read_text())
        self.assertEqual(config["vars"]["ARCHIVE_MANIFEST_SHA"], result["archive"]["manifest_sha256"])

    def test_checks_also_read_one_article_through_permanent_storage(self):
        bodies = {path: path.encode() for path in ("index.html", "feed.xml", "feed.json", "news/b/index.html", "news/a/index.html")}
        manifest = {path: publish.hashlib.sha256(body).hexdigest() for path, body in bodies.items()}
        read = []
        def request(url):
            read.append(url.removeprefix(self.p.base_url))
            return bodies[read[-1]]
        with patch.object(self.p, "request", side_effect=request):
            self.p.verify(self.p.base_url, manifest)
            self.assertEqual(read, ["index.html", "feed.xml", "feed.json"])
            read.clear()
            self.p.verify(self.p.base_url, manifest, archive={"manifest_sha256": "f" * 64})
        self.assertEqual(read, ["index.html", "feed.xml", "feed.json", "news/a/index.html"])

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
            roots = {self.p.publication_identity({"index.html": "same"}, {"manifest_sha256": c * 64}) for c in "ab"}
        self.assertEqual(len({first, second, third, *roots}), 5)
