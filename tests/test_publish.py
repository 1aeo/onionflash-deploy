import io
from contextlib import redirect_stdout
import json
import os
import socket
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
import httpx
from onionflash.transport import HttpClient

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
[[sources]]
id = "test"
url = "https://example.org/feed.xml"
''')
        self.publisher = publish.Publisher(self.directory / "deployment.toml")
        self.addCleanup(self.publisher.close)

    def use_http(self, handler):
        waits = []
        resolver = lambda host, port, *args: [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", port))]
        self.publisher._http_client = HttpClient(self.publisher.http_settings(),
            transport=httpx.MockTransport(handler), resolver=resolver, sleep=waits.append,
            same_origin_redirects=True)
        return waits

    def public_tree(self):
        directory = self.directory / "public"
        directory.mkdir()
        for name in publish.public_files():
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
        def fail(request):
            attempts.append(1)
            raise httpx.ConnectError("connection unavailable", request=request)
        waits = self.use_http(fail)
        with self.assertRaises(URLError):
            self.publisher.request("https://example.com")
        self.assertEqual(len(attempts), 3)
        self.assertEqual(waits, [2, 3])

    def test_permanent_errors_are_not_retried(self):
        waits = self.use_http(lambda request: httpx.Response(401))
        with self.assertRaises(HTTPError):
            self.publisher.request("https://example.com")
        self.assertEqual(waits, [])

    def test_retry_after_is_capped(self):
        self.publisher.net.update(attempts=2, backoff_cap_seconds=3, jitter_seconds=0)
        waits = self.use_http(lambda request: httpx.Response(429, headers={"Retry-After": "999999"}))
        with self.assertRaises(HTTPError):
            self.publisher.request("https://example.com")
        self.assertEqual(waits, [3])

    def test_shared_transport_keeps_api_auth_on_same_origin_only(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(302, headers={"Location": "https://other.example/"})
        self.use_http(handler)
        with self.assertRaisesRegex(ValueError, "cross-origin"):
            self.publisher.request("https://example.com/", token="test-credential")
        self.assertEqual(len(calls), 1)

    def test_shared_transport_supports_route_cleanup_delete(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={"success": True, "result": None})
        self.use_http(handler)
        self.assertIsNone(self.publisher.api("/test-route", method="DELETE"))
        self.assertEqual(calls[0].method, "DELETE")

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

    def test_hash_inside_an_unquoted_value_is_kept(self):
        path = self.directory / "hash.env"
        path.write_text("ONIONFLASH_TEST_HASH=abc#def123\n")
        path.chmod(0o600)
        with patch.dict(os.environ, {}, clear=False):
            publish.load_env(path)
            self.assertEqual(os.environ["ONIONFLASH_TEST_HASH"], "abc#def123")

    def test_application_paths_come_from_the_application_loader(self):
        (self.directory / "onionflash.toml").write_text(
            '[site]\nbase_url = "https://example.com/onionflash"\n'
            '[storage]\ndatabase = "${OF_TEST_STATE}/news.sqlite3"\n'
            '[[sources]]\nid = "test"\nurl = "https://example.org/feed.xml"\n')
        with patch.dict(os.environ, {"OF_TEST_STATE": str(self.directory / "elsewhere")}):
            p = publish.Publisher(self.directory / "deployment.toml")
        self.addCleanup(p.close)
        self.assertEqual(p.database, (self.directory / "elsewhere" / "news.sqlite3").resolve())
        self.assertEqual(p.public, (self.directory / "build" / "public").resolve())  # application default
        self.assertEqual(p.base_url, "https://example.com/onionflash/")

    def test_public_layout_follows_the_application(self):
        from onionflash.render import ASSET_FILES
        from onionflash.schema import CATEGORIES
        self.assertTrue(set(ASSET_FILES) <= publish.public_files())
        self.assertTrue(all(publish.public_path_allowed(f"tags/{topic}.xml") for topic in CATEGORIES))
        self.assertFalse(publish.public_path_allowed("tags/unknown.xml"))

    def test_status_prints_one_json_document(self):
        publish.atomic_json(self.publisher.state / "last-run.json", {"ok": True})
        publish.atomic_json(self.publisher.state / "publication.json", {"version_id": "v", "manifest": {"a": "b"}})
        output = io.StringIO()
        with redirect_stdout(output):
            self.publisher.status()
        report = json.loads(output.getvalue())
        self.assertEqual(report["last-run.json"], {"ok": True})
        self.assertNotIn("manifest", report["publication.json"])

    def test_child_processes_get_only_their_own_credentials(self):
        import subprocess
        captured = []
        def fake(arguments, **kwargs):
            captured.append(kwargs["env"])
            return subprocess.CompletedProcess(arguments, 0, stdout="{}", stderr="")
        extra = {"DO_SPACES_KEY": "spaces-key-value", "DO_SPACES_SECRET": "spaces-secret-value",
                 "ONIONFLASH_LLM_API_KEY": "llm-key-value"}
        with patch.dict(os.environ, extra), patch("publish.subprocess.run", side_effect=fake):
            self.publisher.command(["whoami"])
            self.publisher.application("check")
        wrangler, app = captured
        self.assertEqual(wrangler["CLOUDFLARE_API_TOKEN"], self.publisher.token)
        self.assertFalse({"DO_SPACES_KEY", "DO_SPACES_SECRET", "ONIONFLASH_LLM_API_KEY"} & set(wrangler))
        self.assertEqual(app["ONIONFLASH_LLM_API_KEY"], "llm-key-value")
        self.assertFalse({"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET"} & set(app))

    def test_default_zone_token_stays_out_of_child_processes(self):
        import subprocess
        del self.publisher.cf["zone_token_env"]  # api() then falls back to CLOUDFLARE_ZONE_TOKEN
        captured = []
        def fake(arguments, **kwargs):
            captured.append(kwargs["env"])
            return subprocess.CompletedProcess(arguments, 0, stdout="{}", stderr="")
        with patch("publish.subprocess.run", side_effect=fake):
            self.publisher.command(["whoami"])
            self.publisher.application("check")
        self.assertTrue(all("CLOUDFLARE_ZONE_TOKEN" not in env for env in captured))

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
        identity = p.publication_identity(manifest)
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
