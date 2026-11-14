"""Cross-layer checks for permanent storage and guarded Worker activation."""
from contextlib import closing
import copy, hashlib, json, shutil, sqlite3, unittest
from unittest.mock import patch

from archive_store import ArchiveStore
from onionflash.cli import build
from onionflash.config import DEFAULTS
from onionflash.store import Store, backup_database
import publish
from tests.fakes import DeployTestCase, FakeS3, NEW, OLD


class PermanentPublicationTests(DeployTestCase):
    OUTPUT_DIR = "public"

    def setUp(self):
        super().setUp()
        self.p.cfg["archive"]["enabled"] = True
        self.s3 = FakeS3()
        self.p._archive = ArchiveStore(self.p, client=self.s3)
        self.addCleanup(self.p.close)
        anonymous = patch.object(self.p, "unsigned_read_problem", return_value=None)
        anonymous.start()
        self.addCleanup(anonymous.stop)
        cfg = copy.deepcopy(DEFAULTS)
        cfg["site"]["base_url"] = self.p.base_url
        cfg["storage"].update(database=str(self.p.database), output_dir=str(self.p.public))
        self.app_config = cfg
        with Store(self.p.database) as store:
            build(cfg, store)

    def add_stories(self, *identities):
        with Store(self.p.database) as store:
            for identity in identities:
                story = {
                    "id": identity, "title": "Earlier story", "summary": "An earlier published report.",
                    "why_it_matters": "", "category": "network", "priority": "normal", "confidence": "reported",
                    "state": "developing", "tags": ["network"], "sources": [{"url": "https://example.org/report"}],
                    "published_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z",
                }
                store.db.execute("INSERT INTO stories(id,cluster_key,payload,published_at,updated_at) VALUES(?,?,?,?,?)",
                                 (story["id"], story["id"], json.dumps(story), story["published_at"], story["updated_at"]))
            store.db.commit()
            build(self.app_config, store)

    def _mock_publish(self):
        def upload(arguments, metadata=None):
            if metadata:
                metadata.write_text(json.dumps({"type": "version-upload", "version_id": NEW, "preview_url": "https://candidate.example.workers.dev"}) + "\n")
        live = lambda: publish.read_json(self.p.state / "publication.json")["version_id"]
        return (
            patch.object(self.p, "current_version", side_effect=live),
            patch.object(self.p, "command", side_effect=upload),
            patch.object(self.p, "api", return_value={"subdomain": "example"}),
            patch.object(self.p, "verify"),
            patch.object(self.p, "app_version", return_value="test"),
            patch.object(self.p, "activate"),
        )

    def test_worker_bundle_contents_and_static_feed_budget_checks(self):
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
            with Store(self.p.database) as store:
                store.db.execute("UPDATE public_artifacts SET sha256=?,size=? WHERE path='feed.xml'",
                                 (hashlib.sha256(body).hexdigest(), len(body)))
                store.db.commit()
            with self.assertRaisesRegex(ValueError, "Expected RSS feed"):
                self.p.publish()
        command.assert_not_called()

    def test_sync_readiness_migration_backfill_and_incomplete_snapshot_guards(self):
        with patch.object(self.p.archive, "sync", return_value={"ready": False, "pending": 1}), patch.object(self.p, "command") as command:
            with self.assertRaisesRegex(ValueError, "catching up"):
                self.p.publish()
        command.assert_not_called()

        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {"news/legacy/index.html": "f" * 64}})
        with patch.object(self.p, "command") as command, self.assertRaisesRegex(ValueError, "previously published article"):
            self.p.publish()
        command.assert_not_called()
        (self.p.state / "publication.json").unlink()

        with Store(self.p.database) as store:
            store.db.execute("INSERT INTO public_render_queue(story_id) VALUES('still-rendering')")
            store.db.commit()
        with patch.object(self.p, "command") as command, self.assertRaisesRegex(ValueError, "articles pending"):
            self.p.publish()
        command.assert_not_called()
        with Store(self.p.database) as store:
            store.db.execute("DELETE FROM public_render_queue")
            store.db.commit()

        self.p.cfg["archive"]["enabled"] = False
        with Store(self.p.database) as store:
            store.db.execute("INSERT INTO public_artifacts(path,sha256,size,content_type,updated_at) VALUES(?,?,?,?,?)",
                             ("news/older/index.html", "a" * 64, 10, "text/html", "2026-11-10T00:00:00Z"))
            store.db.commit()
        with patch.object(self.p, "command") as command, self.assertRaisesRegex(ValueError, "snapshot is incomplete"):
            self.p.publish()
        command.assert_not_called()

    def test_restored_older_database_cannot_drop_published_permalink_in_archive_or_static_mode(self):
        for archive_enabled in (True, False):
            with self.subTest(archive_enabled=archive_enabled):
                self.setUp()
                self.p.cfg["archive"]["enabled"] = archive_enabled
                self.add_stories("alpha")
                older = self.root / f"older-{archive_enabled}.sqlite3"
                backup_database(self.p.database, older)
                self.add_stories("bravo")
                publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {}})
                p_ver, p_cmd, p_api, p_verify, p_app, p_act = self._mock_publish()
                with p_ver, p_cmd as command, p_api, p_verify, p_app, p_act:
                    receipt = self.p.publish()
                    self.assertEqual({p for p in receipt["manifest"] if publish.ARTICLE.fullmatch(p)},
                                     {"news/alpha/index.html", "news/bravo/index.html"})
                    with closing(sqlite3.connect(older)) as source, closing(sqlite3.connect(self.p.database)) as target:
                        source.backup(target)
                    with Store(self.p.database) as store:
                        build(self.app_config, store, force=True)
                    if not archive_enabled:
                        self.assertTrue((self.p.public / "news/bravo/index.html").is_file())
                    with self.assertRaisesRegex(ValueError, "previously published article"):
                        self.p.publish()
                self.assertEqual(command.call_count, 1)

    def test_local_reconstruction_receipt_binding_verification_and_identity(self):
        self.add_stories("r0", "r1", "r2")
        self.p.publication_readiness(self.p.validate(self.p.public))
        original = self.p.archive.sync()
        key = "news/r2/index.html"
        old_sha = self.p.archive.snapshot(self.root / "before", original["generation"])[key]
        shutil.rmtree(self.p.public)
        self.app_config["storage"]["render_batch_size"] = 1
        with Store(self.p.database) as store:
            self.assertEqual(build(self.app_config, store, force=True)["articles_pending"], 2)
        self.assertFalse((self.p.public / key).exists())
        ready = self.p.archive.sync()
        self.assertTrue(ready["ready"])
        self.assertEqual(self.p.archive.snapshot(self.root / "after", ready["generation"])[key], old_sha)

        with Store(self.p.database) as store:
            while build(self.app_config, store).get("articles_pending"):
                pass
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": {}})
        p_ver, p_cmd, p_api, p_verify, p_app, p_act = self._mock_publish()
        with p_ver, p_cmd as command, p_api, p_verify as verify, p_app, p_act as activate:
            result = self.p.publish()
        self.assertEqual(result["version_id"], NEW)
        self.assertEqual(verify.call_args.kwargs["archive"], result["archive"])
        self.assertEqual(command.call_count, 1)
        activate.assert_called_once()
        self.assertEqual(json.loads((self.p.state / "wrangler.json").read_text())["vars"]["ARCHIVE_MANIFEST_SHA"],
                         result["archive"]["manifest_sha256"])

        bodies = {p: p.encode() for p in ("index.html", "feed.xml", "feed.json", "news/b/index.html", "news/a/index.html")}
        manifest = {p: hashlib.sha256(b).hexdigest() for p, b in bodies.items()}
        read = []
        with patch.object(self.p, "request", side_effect=lambda url: (read.append(url.removeprefix(self.p.base_url)), bodies[read[-1]])[1]):
            self.p.verify(self.p.base_url, manifest)
            self.assertEqual(read, ["index.html", "feed.xml", "feed.json"])
            read.clear()
            self.p.verify(self.p.base_url, manifest, archive={"manifest_sha256": "f" * 64})
            self.assertEqual(read, ["index.html", "feed.xml", "feed.json", "news/a/index.html"])

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
