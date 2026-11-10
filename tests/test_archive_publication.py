"""Cross-layer checks for permanent storage and guarded Worker activation."""
import copy
import io
import json
import os
from pathlib import Path
import socket
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import httpx

import publish
from archive_store import ArchiveStore, HOT_FILES
from onionflash.cli import build
from onionflash.config import DEFAULTS
from onionflash.store import Store
from onionflash.transport import HttpClient
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

    def test_prepared_archive_is_bound_to_the_publication_receipt(self):
        commands = []
        def command(arguments, metadata=None):
            commands.append(arguments)
            if metadata:
                metadata.write_text(json.dumps({"type": "version-upload", "version_id": NEW,
                                               "preview_url": "https://candidate.example.workers.dev"}) + "\n")
        with patch.object(self.p, "current_version", return_value=OLD), patch.object(self.p, "command", side_effect=command), patch.object(self.p, "api", return_value={"subdomain": "example"}), patch.object(self.p, "verify") as verify, patch.object(self.p, "app_version", return_value="test"), patch.object(self.p, "transition", side_effect=lambda target, *args: target):
            result = self.p.publish()
        self.assertEqual(result["version_id"], NEW)
        self.assertIn("archive", result)
        self.assertEqual(set(result["manifest"]), HOT_FILES | {"feed.xml", "feed.json"})
        self.assertEqual(verify.call_args.kwargs["archive"], result["archive"])
        self.assertEqual(len(commands), 1)

    def test_large_feeds_are_verified_with_head_not_buffered(self):
        self.p.cf["verify_paths"] = ["feed.xml"]
        calls = []
        sha = "f" * 64
        def handle(request):
            calls.append(request.method)
            return httpx.Response(200, headers={"etag": '"' + sha + '"', "content-length": "100000000"})
        resolver = lambda host, port, *args: [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", port))]
        self.p._http_client = HttpClient(self.p.http_settings(), transport=httpx.MockTransport(handle), resolver=resolver)
        self.p.verify(self.p.base_url, {"feed.xml": sha}, archive={"manifest_sha256": "a" * 64})
        self.assertEqual(calls, ["HEAD"])

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

    def test_archive_backups_keep_database_and_hot_assets_without_full_site(self):
        sync = self.p.archive.sync()
        snapshot = self.root / "initial-snapshot"
        hot = self.p.archive.snapshot_hot(snapshot, sync["generation"])
        manifest = {**hot, **self.p.archive.manifest(["feed.xml", "feed.json"], sync["generation"])}
        publish.atomic_json(self.p.state / "publication.json", {"version_id": OLD, "manifest": manifest,
            "archive": {"generation": sync["generation"], "manifest_sha256": sync["manifest_sha256"]}})
        self.p.cfg["spaces"]["enabled"] = True
        backups = FakeSpaces()
        with patch("boto3.client", return_value=backups):
            self.p.backup(force=True)
        with tarfile.open(fileobj=io.BytesIO(backups.uploads[0][2]), mode="r:gz") as tar:
            names = tar.getnames()
            self.assertIn("onionflash.sqlite3", names)
            self.assertIn("public/index.html", names)
            self.assertNotIn("public/feed.xml", names)
            report = json.load(tar.extractfile("snapshot-manifest.json"))
            self.assertTrue(report["matches_publication"])
            self.assertEqual(report["archive"]["manifest_sha256"], sync["manifest_sha256"])
