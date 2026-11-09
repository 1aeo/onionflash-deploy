import io
from contextlib import redirect_stdout
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import publish


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        environment = patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "a" * 32,
                                               "CLOUDFLARE_API_TOKEN": "unit-test-not-a-real-secret",
                                               "CLOUDFLARE_ZONE_TOKEN": "zone-test-not-a-real-secret",
                                               "CLOUDFLARE_ZONE_ID": "b" * 32})
        environment.start()
        self.addCleanup(environment.stop)
        config = (publish.ROOT / "deployment.example.toml").read_text()
        (self.directory / "deployment.toml").write_text(config)
        (self.directory / "onionflash.toml").write_text('''[site]
base_url = "https://example.com/onionflash/"
[storage]
database = "state/news.sqlite3"
output_dir = "build/public"
''')
        self.publisher = publish.Publisher(self.directory / "deployment.toml")

    def public_tree(self):
        directory = self.directory / "public"
        directory.mkdir()
        for name in publish.PUBLIC_FILES:
            body = b"safe generated content"
            if name == "feed.xml":
                body = b"<rss version='2.0'><channel><title>News</title></channel></rss>"
            elif name == "feed.json":
                body = b'{"items":[]}'
            (directory / name).write_bytes(body)
        return directory

    def test_exact_generated_allowlist_rejects_private_evidence(self):
        public = self.public_tree()
        (public / "private-evidence.json").write_text('{"private":true}')
        with self.assertRaisesRegex(ValueError, "Unexpected public file"):
            self.publisher.validate(public)

    def test_public_symlinks_and_credential_values_are_rejected(self):
        public = self.public_tree()
        (public / "index.html").unlink()
        (public / "index.html").symlink_to(self.directory / "onionflash.toml")
        with self.assertRaisesRegex(ValueError, "Symlinks"):
            self.publisher.validate(public)
        (public / "index.html").unlink()
        (public / "index.html").write_text(os.environ["CLOUDFLARE_API_TOKEN"])
        with self.assertRaisesRegex(ValueError, "Credential"):
            self.publisher.validate(public)

    def test_invalid_story_and_topic_paths_not_publishable(self):
        self.assertFalse(publish.public_path_allowed("news/../secret/index.html"))
        self.assertFalse(publish.public_path_allowed("tags/private.xml"))
        self.assertTrue(publish.public_path_allowed("news/abc_123/index.html"))
        self.assertTrue(publish.public_path_allowed("tags/security.xml"))

    def test_subdirectory_assets_use_plain_paths_and_root_headers(self):
        public = self.public_tree()
        assets = self.publisher.prepare_assets(public)
        self.assertTrue((assets / "onionflash/index.html").is_file())
        self.assertTrue((assets / "_headers").is_file())
        self.assertFalse((assets / "onionflash/_headers").exists())
        config = json.loads(self.publisher.make_config(assets).read_text())
        self.assertNotIn("base_path", config["assets"])
        self.assertEqual(config["vars"]["BASE_PATH"], "/onionflash")

    def test_fractional_retry_limit_is_rejected(self):
        path = self.directory / "deployment.toml"
        path.write_text(path.read_text().replace("attempts = 4", "attempts = 1.5"))
        with self.assertRaises(ValueError):
            publish.Publisher(path)

    def test_virtualenv_executable_path_preserves_symlink(self):
        binary = self.directory / ".venv/bin/python"
        binary.parent.mkdir(parents=True)
        binary.symlink_to("/usr/bin/python3")
        self.assertEqual(self.publisher.path(".venv/bin/python"), binary.parent.resolve() / "python")

    def test_retry_is_exponential_and_bounded(self):
        self.publisher.net.update(attempts=3, backoff_seconds=2, backoff_cap_seconds=3, jitter_seconds=0)
        attempts = []
        def fail():
            attempts.append(1)
            raise URLError("connection unavailable")
        with patch("publish.time.sleep") as sleep:
            with self.assertRaises(RuntimeError):
                self.publisher.retry(fail)
        self.assertEqual(len(attempts), 3)
        self.assertEqual([x.args[0] for x in sleep.call_args_list], [2, 3])

    def test_permanent_errors_are_not_retried(self):
        error = HTTPError("https://example.com", 401, "unauthorized", {}, io.BytesIO())
        with patch("publish.time.sleep") as sleep:
            with self.assertRaises(HTTPError):
                self.publisher.retry(lambda: (_ for _ in ()).throw(error))
        sleep.assert_not_called()

    def test_retry_after_is_capped(self):
        self.publisher.net.update(attempts=2, backoff_cap_seconds=3, jitter_seconds=0)
        error = HTTPError("https://example.com", 429, "rate limited", {"Retry-After": "999999"}, io.BytesIO())
        with patch("publish.time.sleep") as sleep:
            with self.assertRaises(RuntimeError):
                self.publisher.retry(lambda: (_ for _ in ()).throw(error))
        sleep.assert_called_once_with(3)

    def test_credentials_file_is_literal_and_private(self):
        path = self.directory / "literal.env"
        path.write_text('ONIONFLASH_TEST_LITERAL="$(do-not-execute)"\n')
        path.chmod(0o600)
        with patch.dict(os.environ, {}, clear=False):
            publish.load_env(path)
            self.assertEqual(os.environ["ONIONFLASH_TEST_LITERAL"], "$(do-not-execute)")
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "mode 600"):
            publish.load_env(path)

    def test_route_conflicts_are_detected_before_any_mutation(self):
        p = self.publisher
        publish.atomic_json(p.state / "publication.json", {"origin_url": "https://test.workers.dev/onionflash", "manifest": {}})
        routes = [{"id": "existing", "pattern": "example.com/onionflash*", "script": "another-project"}]
        with patch.object(p, "api", return_value=routes) as api, patch.object(p, "verify"):
            with self.assertRaisesRegex(ValueError, "another Worker"):
                p.routes()
        self.assertEqual(len(api.call_args_list), 1)

    def test_route_verify_failure_removes_only_routes_just_created(self):
        p = self.publisher
        publish.atomic_json(p.state / "publication.json", {"origin_url": "https://test.workers.dev/onionflash", "manifest": {}})
        calls = []
        def api(path, method="GET", data=None, zone=False):
            calls.append((path, method))
            if method == "GET":
                return [{"id": "preexisting", "pattern": "example.com/other/*", "script": "another-project"}]
            if method == "POST":
                return {"id": "new-route"}
            return None
        with patch.object(p, "api", side_effect=api), patch.object(p, "verify", side_effect=[None, ValueError("bad live output")]):
            with self.assertRaisesRegex(ValueError, "bad live"):
                p.routes()
        deleted = [path for path, method in calls if method == "DELETE"]
        self.assertEqual(deleted, ["/zones/" + "b" * 32 + "/workers/routes/new-route"])

    def test_publication_hold_prevents_all_network_mutations(self):
        publish.atomic_json(self.publisher.state / "publication-hold.json", {"reason": "rollback"})
        with patch.object(self.publisher, "api") as api:
            with self.assertRaisesRegex(ValueError, "on hold"):
                self.publisher.publish()
        api.assert_not_called()

    def test_unchanged_content_reconciles_remote_version(self):
        p = self.publisher
        p.public.mkdir(parents=True)
        (p.public / "index.html").write_text("safe")
        manifest = {"index.html": "dummy"}
        identity = publish.hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        publish.atomic_json(p.state / "publication.json", {"content_hash": identity, "version_id": "expected"})
        with patch.object(p, "validate", return_value=manifest), patch.object(p, "current_version", return_value="unexpected"), patch.object(p, "command") as command:
            with self.assertRaisesRegex(ValueError, "differs"):
                p.publish()
        command.assert_not_called()

    def test_backup_invalid_retention_rejected_before_storage_access(self):
        self.publisher.cfg["spaces"].update(enabled=True, retention_days=0)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.publisher.backup()

    def test_secret_redaction(self):
        self.assertNotIn(os.environ["CLOUDFLARE_API_TOKEN"], self.publisher.clean_error("failed " + os.environ["CLOUDFLARE_API_TOKEN"]))

    def test_custom_credential_names_are_redacted(self):
        with patch.dict(os.environ, {"MY_AUTH": "custom-sensitive-value"}):
            self.publisher.cf["api_token_env"] = "MY_AUTH"
            self.assertNotIn("custom-sensitive-value", self.publisher.clean_error("custom-sensitive-value"))
            public = self.public_tree()
            (public / "index.html").write_text("custom-sensitive-value")
            with self.assertRaisesRegex(ValueError, "Credential"):
                self.publisher.validate(public)

    def test_status_remains_readable_during_a_running_job(self):
        publish.atomic_json(self.publisher.state / "publication.json", {"version_id": "observed-version", "manifest": {"private": "omit"}})
        output = io.StringIO()
        with publish.lock(self.publisher.state / "run.lock"), patch("publish.Publisher", return_value=self.publisher), patch("sys.argv", ["publish.py", "status"]), redirect_stdout(output):
            self.assertEqual(publish.main(), 0)
        self.assertIn('"running": true', output.getvalue())
        self.assertIn("observed-version", output.getvalue())
        self.assertNotIn('"manifest"', output.getvalue())


if __name__ == "__main__":
    unittest.main()
