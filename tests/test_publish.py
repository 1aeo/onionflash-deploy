import io
from contextlib import closing, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
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
[[sources]]
id = "test"
url = "https://example.org/feed.xml"
''')
        self.publisher = publish.Publisher(self.directory / "deployment.toml")

    def serve(self, handle):
        """A local HTTP server whose every request is passed to handle(handler)."""
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                handle(self)
            do_POST = do_DELETE = do_GET
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        environment = patch.dict(os.environ, {"no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"})
        environment.start()
        self.addCleanup(environment.stop)
        return f"http://127.0.0.1:{server.server_port}"

    @staticmethod
    def reply(handler, body, status=200, headers=()):
        handler.send_response(status)
        for name, value in headers:
            handler.send_header(name, value)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

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
        for path in ["news/abc_123/index.html", "news/abc_123/story.json", "page/2/index.html", "tags/security.xml",
                     "topics/security/index.html", "topics/security/page/2/index.html"]:
            self.assertTrue(publish.public_path_allowed(path), path)
        for path in ["news/../secret/index.html", "tags/private.xml", "app.js", "topics/private/index.html",
                     "topics/security/../private/index.html", "page/0/index.html", "topics/security/page/02/index.html",
                     "topics/security/page/1000000000/index.html", "topics/security/app.js", "page/1/story.json"]:
            self.assertFalse(publish.public_path_allowed(path), path)

    def test_subdirectory_assets_use_plain_paths_and_root_headers(self):
        public = self.public_tree()
        assets = self.publisher.prepare_assets(public)
        self.assertTrue((assets / "onionflash/index.html").is_file())
        self.assertTrue((assets / "_headers").is_file())
        self.assertFalse((assets / "onionflash/_headers").exists())
        config = json.loads(self.publisher.make_config(assets).read_text())
        self.assertNotIn("base_path", config["assets"])
        self.assertEqual(config["vars"]["BASE_PATH"], "/onionflash")

    def test_retired_settings_in_an_existing_file_are_ignored(self):
        path = self.directory / "deployment.toml"
        path.write_text(path.read_text().replace("[cloudflare]\n", '[network]\nattempts = 99\n\n'
                                                 '[cloudflare]\napi_token_env = "OTHER_NAME"\nverify_paths = ["x"]\n')
                        .replace("[archive]\n", '[archive]\nenv_file = "archive-secrets.env"\nshard_prefix_chars = 3\n'
                                 'worker_timeout_ms = 100\n'))
        with self.assertLogs(publish.LOG, "WARNING") as logs:
            p = publish.Publisher(path)
        self.assertEqual(p.net, publish.NETWORK)
        self.assertEqual(p.token, os.environ["CLOUDFLARE_API_TOKEN"])
        self.assertEqual([record.args for record in logs.records],
                         [("archive", "env_file"), ("archive", "shard_prefix_chars"), ("archive", "worker_timeout_ms")])
        self.assertFalse(publish.RETIRED["archive"] & set(p.cfg["archive"]))

    def test_virtualenv_executable_path_preserves_symlink(self):
        binary = self.directory / ".venv/bin/python"
        binary.parent.mkdir(parents=True)
        binary.symlink_to("/usr/bin/python3")
        self.assertEqual(self.publisher.path(".venv/bin/python"), binary.parent.resolve() / "python")

    def test_api_token_is_never_sent_to_a_redirect_target(self):
        seen = []
        def target(handler):
            seen.append(handler.headers.get("Authorization"))
            self.reply(handler, b"ok")
        target_url = self.serve(target)
        def origin(handler):
            seen.append(handler.headers.get("Authorization"))
            self.reply(handler, b"", 302, [("Location", target_url + "/")])
        self.assertEqual(self.publisher.request(self.serve(origin) + "/", token="test-credential"), b"ok")
        self.assertEqual(seen, ["Bearer test-credential", None])

    def test_api_sends_method_and_token_and_bounds_responses(self):
        seen = []
        def handler(request):
            seen.append((request.command, request.headers.get("Authorization")))
            self.reply(request, b'{"success": true, "result": null}')
        self.publisher.api_base = self.serve(handler)
        self.assertIsNone(self.publisher.api("/routes/1", method="DELETE"))
        self.assertEqual(seen, [("DELETE", "Bearer unit-test-not-a-real-secret")])
        self.publisher.net["max_response_bytes"] = 10
        with self.assertRaisesRegex(ValueError, "size limit"):
            self.publisher.api("/routes/1")

    def test_verification_retries_transient_failures_but_not_client_errors(self):
        p = self.publisher
        body = b"live content"
        self.assertEqual(publish.VERIFY_PATHS, ("index.html", "feed.xml", "feed.json"))
        self.assertGreaterEqual(publish.NETWORK["max_response_bytes"], publish.MAX_FILE_BYTES)
        manifest = {name: publish.hashlib.sha256(body).hexdigest() for name in publish.VERIFY_PATHS}
        transient = [HTTPError("u", 503, "busy", {}, None), URLError("reset"), HTTPError("u", 404, "not yet", {}, None)]
        with patch.object(p, "request", side_effect=transient + [body] * 3), patch("publish.time.sleep") as sleep:
            p.verify("https://example.com/onionflash", manifest)
        self.assertEqual(sleep.call_count, 3)
        with patch.object(p, "request", side_effect=HTTPError("u", 403, "forbidden", {}, None)), \
                patch("publish.time.sleep") as sleep:
            with self.assertRaises(HTTPError):
                p.verify("https://example.com/onionflash", manifest)
        sleep.assert_not_called()

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
                 "ONIONFLASH_LLM_API_KEY": "llm-key-value", "CLOUDFLARE_ZONE_TOKEN": "zone-token-value"}
        with patch.dict(os.environ, extra), patch("publish.subprocess.run", side_effect=fake):
            self.publisher.command(["whoami"])
            self.publisher.application("check")
        wrangler, app = captured
        self.assertEqual(wrangler["CLOUDFLARE_API_TOKEN"], self.publisher.token)
        self.assertFalse({"CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET", "ONIONFLASH_LLM_API_KEY"} & set(wrangler))
        self.assertEqual(app["ONIONFLASH_LLM_API_KEY"], "llm-key-value")
        self.assertFalse({"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET"} & set(app))

    def test_route_conflicts_are_detected_before_any_mutation(self):
        p = self.publisher
        publish.atomic_json(p.state / "publication.json", {"manifest": {}})
        routes = [{"id": "existing", "pattern": "example.com/onionflash*", "script": "another-project"}]
        with patch.object(p, "api", return_value=routes) as api, patch.object(p, "verify"), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"):
            with self.assertRaisesRegex(ValueError, "another Worker"):
                p.routes()
        self.assertEqual(len(api.call_args_list), 1)

    def test_route_verify_failure_removes_only_routes_just_created(self):
        p = self.publisher
        publish.atomic_json(p.state / "publication.json", {"manifest": {}})
        calls = []
        def api(path, method="GET", data=None, zone=False):
            calls.append((path, method))
            if method == "GET":
                return [{"id": "preexisting", "pattern": "example.com/other/*", "script": "another-project"}]
            if method == "POST":
                return {"id": "new-route"}
            return None
        with patch.object(p, "api", side_effect=api), patch.object(p, "verify", side_effect=[None, ValueError("bad live output")]), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"):
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

    def test_output_without_the_core_ledger_is_never_published(self):
        self.publisher.public.mkdir(parents=True)
        with patch.object(self.publisher, "api") as api, self.assertRaisesRegex(ValueError, "Run the core build"):
            self.publisher.publish()
        api.assert_not_called()

    def test_unchanged_content_checks_active_version_without_uploading(self):
        p = self.publisher
        p.public.mkdir(parents=True)
        ledger = {"index.html": "a" * 64}
        with closing(sqlite3.connect(p.database)) as db:
            db.executescript("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);"
                             "CREATE TABLE public_artifacts(path TEXT PRIMARY KEY, sha256 TEXT);")
            db.executemany("INSERT INTO public_artifacts VALUES(?,?)", ledger.items())
            db.commit()
        publish.atomic_json(p.state / "publication.json", {"content_hash": p.publication_identity(ledger),
                                                           "manifest": ledger, "version_id": "expected"})
        # The ledger still matches the last validated publication: no copy, scan or upload.
        with patch("publish.shutil.copytree") as copy, patch.object(p, "validate") as validate, \
                patch.object(p, "current_version", return_value="expected") as version, \
                patch.object(p, "command") as command, patch.object(p, "api") as api:
            self.assertEqual(p.publish()["version_id"], "expected")
        copy.assert_not_called()
        validate.assert_not_called()
        version.assert_called_once()
        command.assert_not_called()
        api.assert_not_called()

        with patch.object(p, "current_version", return_value="external"), patch.object(p, "command") as command:
            with self.assertRaisesRegex(ValueError, "differs.*live external, recorded expected"):
                p.publish()
        command.assert_not_called()

        # A hash cleared by a rollback or an interrupted run, or one from another Worker,
        # sends the same ledger through the full copy and validation.
        for receipt in ({"manifest": ledger, "version_id": "expected"},
                        {"content_hash": "0" * 64, "manifest": ledger, "version_id": "expected"}):
            publish.atomic_json(p.state / "publication.json", receipt)
            with patch.object(p, "validate", side_effect=ValueError("validated")), \
                    patch.object(p, "current_version", return_value="expected"), self.assertRaisesRegex(ValueError, "validated"):
                p.publish()

    def test_backup_invalid_retention_rejected_before_storage_access(self):
        self.publisher.cfg["spaces"].update(enabled=True, retention_days=0)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.publisher.backup()

    def test_invalid_remote_version_cannot_be_used_as_a_receipt_path(self):
        response = {"deployments": [{"versions": [{"version_id": "../outside", "percentage": 100}]}]}
        with patch.object(self.publisher, "api", return_value=response):
            with self.assertRaisesRegex(ValueError, "invalid version"):
                self.publisher.current_version()

    def test_invalid_rollback_version_is_rejected_before_mutation(self):
        publish.atomic_json(self.publisher.state / "publication.json", {"version_id": "v", "previous_version_id": "../outside"})
        with patch.object(self.publisher, "activate") as activate:
            with self.assertRaisesRegex(ValueError, "Invalid previous version"):
                self.publisher.rollback()
        activate.assert_not_called()
        self.assertFalse((self.publisher.state / "publication-hold.json").exists())

    def test_routes_explain_a_receipt_without_files(self):
        publish.atomic_json(self.publisher.state / "publication.json", {"version_id": "live-version"})
        with self.assertRaisesRegex(ValueError, "publish once, then attach routes"):
            self.publisher.routes()

    def test_secret_redaction(self):
        self.assertNotIn(os.environ["CLOUDFLARE_API_TOKEN"], self.publisher.clean_error("failed " + os.environ["CLOUDFLARE_API_TOKEN"]))
        # A configured credential is redacted and scanned for even when its name looks like no
        # secret and it was set in the environment rather than a credential file.
        self.publisher.cfg.setdefault("archive", {})["access_key_env"] = "ARCHIVE_IDENTITY"
        with patch.dict(os.environ, {"ARCHIVE_IDENTITY": "archive-identity-value"}):
            self.assertEqual(self.publisher.clean_error("failed archive-identity-value"), "failed [redacted]")
            self.assertIn("archive-identity-value", self.publisher.secret_values())

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
