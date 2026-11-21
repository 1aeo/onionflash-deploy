"""Unit tests for Publisher validation, HTTP verification, routing, scheduling, and budgets."""
from http.client import BadStatusLine
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json, os, subprocess, threading, unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import publish
from tests.fakes import DeployTestCase, NEW, OLD, app_toml, seed_public_site


class DeploymentTests(DeployTestCase):
    """Tests for configuration validation, credential isolation, HTTP requests, and CLI status."""

    def serve(self, handle):
        """Start a loopback HTTP server dispatching GET, POST, and DELETE requests to handle."""
        class Handler(BaseHTTPRequestHandler):
            """Loopback HTTP request handler delegating methods to handle."""

            def do_GET(self):
                """Forward incoming HTTP requests to the test callback."""
                handle(self)
            do_POST = do_DELETE = do_GET
            def log_message(self, *args):
                """Suppress default stderr request logging during tests."""
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        env = patch.dict(os.environ, {"no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"})
        env.start()
        self.addCleanup(env.stop)
        return f"http://127.0.0.1:{server.server_port}"

    @staticmethod
    def reply(handler, body, status=200, headers=()):
        """Write status, headers, Content-Length, and body to a BaseHTTPRequestHandler."""
        handler.send_response(status)
        for name, value in headers:
            handler.send_header(name, value)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def test_public_validation_allowlist_symlinks_credentials_and_assets(self):
        """Enforce public path allowlists, symlink/credential rejection, and asset staging layout."""
        from onionflash.render import ASSET_FILES
        from onionflash.schema import CATEGORIES
        self.assertTrue(set(ASSET_FILES) <= publish.public_files())
        self.assertTrue(all(publish.public_path_allowed(f"tags/{topic}.xml") for topic in CATEGORIES))
        self.assertFalse(publish.public_path_allowed("tags/unknown.xml"))
        for path in ("news/abc_123/index.html", "news/abc_123/story.json", "page/2/index.html",
                     "tags/security.xml", "topics/security/index.html", "topics/security/page/2/index.html"):
            self.assertTrue(publish.public_path_allowed(path), path)
        for path in ("news/../secret/index.html", "tags/private.xml", "app.js", "topics/private/index.html",
                     "topics/security/../private/index.html", "page/0/index.html", "topics/security/page/02/index.html",
                     "topics/security/page/1000000000/index.html", "topics/security/app.js", "page/1/story.json"):
            self.assertFalse(publish.public_path_allowed(path), path)

        public = seed_public_site(self.directory / "public")
        (public / "private-evidence.json").write_text('{"private":true}')
        with self.assertRaisesRegex(ValueError, "Unexpected public file"):
            self.publisher.validate(public)
        (public / "private-evidence.json").unlink()

        (public / "index.html").unlink()
        (public / "index.html").symlink_to(self.directory / "onionflash.toml")
        with self.assertRaisesRegex(ValueError, "Symlinks"):
            self.publisher.validate(public)
        (public / "index.html").unlink()
        (public / "index.html").write_text(os.environ["CLOUDFLARE_API_TOKEN"])
        with self.assertRaisesRegex(ValueError, "Credential"):
            self.publisher.validate(public)
        (public / "index.html").write_text("safe")

        assets = self.publisher.prepare_assets(public)
        self.assertTrue((assets / "onionflash/index.html").is_file())
        self.assertTrue((assets / "_headers").is_file())
        self.assertFalse((assets / "onionflash/_headers").exists())
        config = json.loads(self.publisher.make_config(assets).read_text())
        self.assertNotIn("base_path", config["assets"])
        self.assertEqual(config["vars"]["BASE_PATH"], "/onionflash")

    def test_unknown_and_invalid_settings_fail_validation(self):
        """Reject unknown configuration keys, non-boolean flags, and invalid Spaces settings."""
        path = self.directory / "deployment.toml"
        base = path.read_text()
        for target, replacement, expected in [
            ("interval_minutes = 5", 'statedir = "state"\ninterval_minutes = 5', "Unknown deployment settings: statedir"),
            ("[cloudflare]\n", '[cloudflare]\nzone = "extra"\n', "Unknown cloudflare settings: zone"),
            ("[spaces]\n", "[spaces]\nenable = true\n", "Unknown spaces settings: enable"),
            ("[spaces]\n", "[spaces]\nrequire = true\n", "Unknown spaces settings: require"),
            ("[archive]\n", '[archive]\nenv_file = "old.env"\nshard_prefix_chars = 2\n', "Unknown archive settings: env_file, shard_prefix_chars"),
            ("[retention]\n", "[retention]\nhot_days = 90\n", "Unknown retention settings: hot_days"),
            ("[retention]\n", "[retention]\nenable = true\n", "Unknown retention settings: enable"),
            ('[spaces]\nenabled = false', '[spaces]\nenabled = "false"', "spaces.enabled must be a boolean"),
            ("required = false", "required = 1", "spaces.required must be a boolean"),
            ("required = false", "required = true", "Required Spaces backups must also set spaces.enabled = true"),
            ('compatibility_date = "2026-11-08"', "compatibility_date = 2026-11-08", "Invalid Cloudflare compatibility_date"),
            ('compatibility_date = "2026-11-08"', 'compatibility_date = "2026-99-99"', "Invalid Cloudflare compatibility_date"),
            ("interval_minutes = 5", "interval_minutes = 0", "interval_minutes must be 1..59"),
            ("interval_minutes = 5", "interval_minutes = 60", "interval_minutes must be 1..59"),
            ("interval_minutes = 5", "interval_minutes = true", "interval_minutes must be 1..59"),
            ('env_file = "secrets.env"', "env_file = 123", "env_file must be a non-empty string"),
            ('state_dir = "state"', 'state_dir = ""', "state_dir must be a non-empty string"),
            ('app_config = "onionflash.toml"', 'app_config = ""', "app_config must be a non-empty string"),
            ('app_python = ".venv/bin/python"', "app_python = 0", "app_python must be a non-empty string"),
            ('app_source = "../onionflash"', 'app_source = "   "', "app_source must be a non-empty string"),
        ]:
            with self.subTest(replacement=replacement):
                path.write_text(base.replace(target, replacement))
                with self.assertRaisesRegex(ValueError, expected):
                    publish.Publisher(path)
                with self.assertLogs(publish.LOG, "ERROR") as check_logs:
                    self.assertEqual(self.run_cli("--config", str(path), "check", patch_publisher=False)[0], 1)
                self.assertTrue(any(expected in line for line in check_logs.output))
        path.write_text(base)

        self.publisher.cfg["spaces"].update(enabled=True, retention_days=0)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.publisher.backup()
        self.publisher.cfg["spaces"].update(retention_days=30, region="")
        with patch.object(self.publisher, "application", return_value="ok"), self.assertLogs(publish.LOG, "ERROR") as logs:
            self.assertEqual(self.run_cli("check", fake_toolchain=True)[0], 1)
        self.assertIn("Spaces region", logs.output[0])

    def test_non_string_credential_names_and_non_regular_env_files_fail_cleanly(self):
        """Reject non-string archive credential names, non-table sections, and non-regular env files."""
        path = self.directory / "deployment.toml"
        base = path.read_text()
        no_archive_table = "archive = false\n" + base.split("[archive]\n")[0] + "[retention]\nenabled = false\n"
        live_secret = "live-publish-secret-token"
        for bad_toml in (
            base.replace('secret_key_env = "DO_SPACES_SECRET"', "secret_key_env = 123"),
            base.replace('secret_key_env = "DO_SPACES_SECRET"', 'secret_key_env = ["BAD"]'),
            no_archive_table,
        ):
            path.write_text(bad_toml)
            with patch.dict(os.environ, {"CLOUDFLARE_API_TOKEN": live_secret}), self.assertLogs(publish.LOG, "ERROR") as bad_logs:
                self.assertEqual(self.run_cli("--config", str(path), "check", patch_publisher=False)[0], 1)
                self.assertEqual(os.environ["CLOUDFLARE_API_TOKEN"], live_secret)
            self.assertFalse(any("TypeError" in m or "AttributeError" in m for m in bad_logs.output))
        path.write_text(base)

        for bad_key in (123, ["BAD"]):
            self.publisher.cfg["archive"].update(enabled=True, secret_key_env=bad_key)
            (self.publisher.state / "last-run.json").unlink(missing_ok=True)
            with patch.dict(os.environ, {"CLOUDFLARE_API_TOKEN": live_secret}), \
                    patch.object(self.publisher, "application", return_value="ok"), \
                    self.assertLogs(publish.LOG, "ERROR") as locked_logs:
                self.assertEqual(self.run_cli("check", fake_toolchain=True)[0], 1)
            last_run = publish.read_json(self.publisher.state / "last-run.json")
            self.assertEqual((last_run["ok"], last_run["command"]), (False, "check"))
            self.assertIn("credential variable", last_run["error"])
            self.assertNotIn(live_secret, "\n".join(locked_logs.output) + last_run["error"])
        self.publisher.cfg["archive"].update(enabled=False, secret_key_env="DO_SPACES_SECRET")

        env_dir = self.directory / "dir.env"
        env_dir.mkdir(mode=0o700)
        broken_link = self.directory / "broken.env"
        broken_link.symlink_to(self.directory / "nonexistent.env")
        for bad_env in (env_dir, broken_link):
            with self.subTest(bad_env=bad_env.name), \
                    self.assertRaisesRegex(ValueError, "Credential file must be a regular file with mode 600"):
                publish.load_env(bad_env)

    def test_clean_https_url_rejects_empty_userinfo_bad_ports_spaces_and_backslashes(self):
        """Reject empty credentials, invalid or zero ports, whitespace, control chars, and backslashes."""
        for bad_url in ("https://@example.com/", "https://:@example.com/", "https://example.com:abc/",
                        "https://example.com:99999/", "https://example.com:0/", "https://example.com:/",
                        "https://example.com/a b/", "https://example.com\\path", "https://example.com/?",
                        "https://example.com/#", 123):
            with self.subTest(bad_url=bad_url):
                with self.assertRaisesRegex(ValueError, "clean HTTPS URL"):
                    publish.clean_https_url(bad_url, "Endpoint")
                with self.assertRaisesRegex(ValueError, "clean HTTPS URL"):
                    publish.validate_s3_location({"endpoint_url": bad_url, "bucket": "valid-bucket", "region": "nyc3"}, "Spaces")

    def test_dotenv_paths_redaction_and_process_credential_isolation(self):
        """Verify literal dotenv parsing, symlinked python paths, secret redaction, and child env isolation."""
        env_path = self.directory / "literal.env"
        env_path.write_text('ONIONFLASH_TEST_LITERAL="$(do-not-execute)"\nONIONFLASH_TEST_HASH=abc#def123\n')
        env_path.chmod(0o600)
        with patch.dict(os.environ, {}, clear=False):
            publish.load_env(env_path)
            self.assertEqual(os.environ["ONIONFLASH_TEST_LITERAL"], "$(do-not-execute)")
            self.assertEqual(os.environ["ONIONFLASH_TEST_HASH"], "abc#def123")
        env_path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "mode 600"):
            publish.load_env(env_path)

        binary = self.directory / ".venv/bin/python"
        binary.parent.mkdir(parents=True)
        binary.symlink_to("/usr/bin/python3")
        self.assertEqual(self.publisher.path(".venv/bin/python"), binary.parent.resolve() / "python")

        (self.directory / "onionflash.toml").write_text(app_toml(base_url="https://example.com/onionflash", database="${OF_TEST_STATE}/news.sqlite3"))
        with patch.dict(os.environ, {"OF_TEST_STATE": str(self.directory / "elsewhere")}):
            loaded = publish.Publisher(self.directory / "deployment.toml")
        self.assertEqual((loaded.database, loaded.public, loaded.base_url),
                         ((self.directory / "elsewhere/news.sqlite3").resolve(), (self.directory / "build/public").resolve(), "https://example.com/onionflash/"))

        self.assertNotIn(os.environ["CLOUDFLARE_API_TOKEN"], self.publisher.clean_error("failed " + os.environ["CLOUDFLARE_API_TOKEN"]))
        self.publisher.cfg.setdefault("archive", {})["access_key_env"] = "ARCHIVE_IDENTITY"
        with patch.dict(os.environ, {"ARCHIVE_IDENTITY": "archive-identity-value"}):
            self.assertEqual(self.publisher.clean_error("failed archive-identity-value"), "failed [redacted]")
            self.assertIn("archive-identity-value", self.publisher.secret_values())

        token = "secret-llm-boundary-credential"
        stderr = "x" * 50 + token + "y" * 985
        failed = subprocess.CompletedProcess([], 1, stdout="", stderr=stderr)
        with patch.dict(os.environ, {"ONIONFLASH_LLM_API_KEY": token}), patch("publish.subprocess.run", return_value=failed):
            with self.assertRaises(RuntimeError) as caught:
                self.publisher.application("run")
        self.assertNotIn(token[-15:], str(caught.exception))
        self.assertIn("[redacted]", str(caught.exception))

        captured = []
        fake = lambda args, **kw: (captured.append(kw["env"]), subprocess.CompletedProcess(args, 0, stdout="{}", stderr=""))[1]
        extra = {"DO_SPACES_KEY": "spaces-key-value", "DO_SPACES_SECRET": "spaces-secret-value",
                 "ONIONFLASH_LLM_API_KEY": "llm-key-value", "CLOUDFLARE_ZONE_TOKEN": "zone-token-value"}
        with patch.dict(os.environ, extra), patch("publish.subprocess.run", side_effect=fake):
            self.publisher.command(["whoami"])
            self.publisher.application("check")
            self.publisher.app_version()
            self.publisher.git_commit(self.directory)
        wrangler, app, version_env, git_env = captured
        self.assertEqual(wrangler["CLOUDFLARE_API_TOKEN"], self.publisher.token)
        self.assertFalse({"CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET", "ONIONFLASH_LLM_API_KEY"} & set(wrangler))
        self.assertEqual(app["ONIONFLASH_LLM_API_KEY"], "llm-key-value")
        self.assertFalse({"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET"} & set(app))
        for child_env in (version_env, git_env):
            self.assertFalse({"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET", "ONIONFLASH_LLM_API_KEY"} & set(child_env))

    def test_provenance_isolation_route_interrupt_rollback_and_backup_client_close(self):
        """Isolate git_commit/app_version, roll back routes on interrupt, and close backup S3 clients."""
        for err in (FileNotFoundError("missing"), subprocess.TimeoutExpired(["cmd"], 10)):
            with patch("publish.subprocess.run", side_effect=err):
                self.assertIsNone(self.publisher.git_commit(self.directory))
                self.assertEqual(self.publisher.app_version(), "unknown")

        target_repo, foreign_repo = self.directory / "target-repo", self.directory / "foreign-repo"
        for repo, content in ((target_repo, "target\n"), (foreign_repo, "foreign\n")):
            repo.mkdir()
            (repo / "file.txt").write_text(content)
            clean_git_env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
            for git_args in (["init", "-q"], ["add", "file.txt"], ["commit", "-qm", "init"]):
                subprocess.run(["git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=t@example.invalid",
                                "-c", "commit.gpgsign=false", *git_args], env=clean_git_env, check=True, capture_output=True)
        expected_sha = subprocess.run(["git", "-C", str(target_repo), "rev-parse", "HEAD"], env=clean_git_env, check=True, capture_output=True, text=True).stdout.strip()
        with patch.dict(os.environ, {"GIT_DIR": str(foreign_repo / ".git"), "GIT_WORK_TREE": str(foreign_repo)}):
            self.assertEqual(self.publisher.git_commit(target_repo), expected_sha)

        p = self.publisher
        publish.atomic_json(p.state / "publication.json", {"manifest": {}})
        calls = []
        def route_api(path, method="GET", data=None, zone=False):
            """Record Cloudflare route API calls and return synthetic list or create payloads."""
            calls.append((path, method))
            return [] if method == "GET" else ({"id": "new-route"} if method == "POST" else None)
        with patch.object(p, "api", side_effect=route_api), patch.object(p, "verify", side_effect=[None, KeyboardInterrupt()]), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"), \
                self.assertRaises(KeyboardInterrupt):
            p.routes()
        self.assertEqual([path for path, m in calls if m == "DELETE"], ["/zones/" + "b" * 32 + "/workers/routes/new-route"])
        def failing_delete_api(path, method="GET", data=None, zone=False):
            """Fail route deletion after creation to exercise chained cleanup reporting."""
            if method == "DELETE":
                raise RuntimeError("delete unavailable")
            return route_api(path, method, data, zone)
        with patch.object(p, "api", side_effect=failing_delete_api), patch.object(p, "verify", side_effect=[None, ValueError("bad live output")]), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"), \
                self.assertRaisesRegex(RuntimeError, "bad live output.*removing route new-route failed.*delete unavailable") as route_err:
            p.routes()
        self.assertIsInstance(route_err.exception.__cause__, ValueError)

        from tests.fakes import FakeS3
        self.publisher.cfg["spaces"]["enabled"] = True
        self.seed_public()
        closed = []
        s3 = FakeS3()
        s3.close = lambda: closed.append(True)
        with patch("boto3.client", return_value=s3), patch.object(self.publisher, "unsigned_read_problem", return_value=None):
            self.publisher.backup(force=True)
        self.assertEqual(closed, [True])

    def test_http_redirect_token_stripping_api_bounds_and_verify_retries(self):
        """Strip bearer tokens across redirects, bound API responses, and retry transient verify errors."""
        seen = []
        target_url = self.serve(lambda h: (seen.append(h.headers.get("Authorization")), self.reply(h, b"ok")))
        origin_url = self.serve(lambda h: (seen.append(h.headers.get("Authorization")), self.reply(h, b"", 302, [("Location", target_url + "/")])))
        self.assertEqual(self.publisher.request(origin_url + "/", token="test-credential"), b"ok")
        self.assertEqual(seen, ["Bearer test-credential", None])

        api_calls = []
        self.publisher.api_base = self.serve(lambda req: (api_calls.append((req.command, req.headers.get("Authorization"))), self.reply(req, b'{"success": true, "result": null}')))
        self.assertIsNone(self.publisher.api("/routes/1", method="DELETE"))
        self.assertEqual(api_calls, [("DELETE", "Bearer unit-test-not-a-real-secret")])
        self.publisher.net["max_response_bytes"] = 10
        with self.assertRaisesRegex(ValueError, "size limit"):
            self.publisher.api("/routes/1")

        p, body = self.publisher, b"live content"
        manifest = dict.fromkeys(publish.VERIFY_PATHS, publish.hashlib.sha256(body).hexdigest())
        transient = [HTTPError("u", 503, "busy", {}, None), URLError("reset"), HTTPError("u", 404, "not yet", {}, None)]
        with patch.object(p, "request", side_effect=transient + [body] * 3), patch("publish.time.sleep") as sleep:
            p.verify("https://example.com/onionflash", manifest)
        self.assertEqual(sleep.call_count, 3)
        with patch.object(p, "request", side_effect=HTTPError("u", 403, "forbidden", {}, None)), patch("publish.time.sleep") as sleep:
            with self.assertRaises(HTTPError):
                p.verify("https://example.com/onionflash", manifest)
        sleep.assert_not_called()

        stale = [b"previous content"] * (publish.VERIFY_ATTEMPTS - 1)
        with patch.object(p, "request", side_effect=stale + [body] * 3), patch("publish.time.sleep") as sleep:
            p.verify("https://example.com/onionflash", manifest)
        self.assertTrue(110 < sum(c.args[0] for c in sleep.call_args_list) < 130)
        with patch.object(p, "request", return_value=b"previous content"), patch("publish.time.sleep"):
            with self.assertRaisesRegex(ValueError, "Published hash mismatch: index.html"):
                p.verify("https://example.com/onionflash", manifest)

    def test_verify_sends_no_cache_headers_on_every_asset_request(self):
        """Send Cache-Control and Pragma no-cache headers on static and archive verify probes."""
        assets = {
            "index.html": b"<html>index</html>",
            "feed.xml": b"<rss version='2.0'></rss>",
            "feed.json": b'{"items":[]}',
            "news/bravo/index.html": b"<article>bravo</article>",
            "news/alpha/index.html": b"<article>alpha</article>",
        }
        manifest = {path: publish.hashlib.sha256(body).hexdigest() for path, body in assets.items()}
        received = []
        base = self.serve(lambda req: (
            received.append((req.path, req.headers.get("Cache-Control"), req.headers.get("Pragma"), req.headers.get("Accept-Encoding"))),
            self.reply(req, assets[req.path.removeprefix("/onionflash/")]),
        ))
        for archive, extra in ((None, ()), ({"manifest_sha256": "a" * 64}, ("news/alpha/index.html",))):
            received.clear()
            self.publisher.verify(base + "/onionflash", manifest, archive=archive)
            self.assertEqual(
                received,
                [(f"/onionflash/{path}", "no-cache", "no-cache", "identity") for path in (*publish.VERIFY_PATHS, *extra)],
            )

    def test_request_preserves_lowercase_caller_content_type_header(self):
        """Avoid duplicating Content-Type or User-Agent when caller supplies lowercase header names."""
        captured = []
        base = self.serve(lambda req: (
            captured.append((req.headers.get_all("Content-Type"), req.headers.get("User-Agent"))),
            self.reply(req, b"{}"),
        ))
        self.publisher.request(
            base + "/post",
            method="POST",
            data={"detail": "test"},
            headers={"content-type": "application/problem+json", "user-agent": "CustomPublisher"},
        )
        self.assertEqual(captured, [(["application/problem+json"], "CustomPublisher")])

    def test_routes_versions_unchanged_shortcut_offline_status_and_schedule(self):
        """Verify zone route rollback, unchanged-content fast path, offline commands, and crontab escaping."""
        p = self.publisher
        for seed in (None, {"version_id": "live-version"}):
            if seed:
                publish.atomic_json(p.state / "publication.json", seed)
            self.assertRaisesRegex(ValueError, "publish once, then attach routes", p.routes)

        publish.atomic_json(p.state / "publication.json", {"manifest": {}})
        conflict = [{"id": "existing", "pattern": "example.com/onionflash*", "script": "another-project"}]
        with patch.object(p, "api", return_value=conflict) as api, patch.object(p, "verify"), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"), \
                self.assertRaisesRegex(ValueError, "another Worker"):
            p.routes()
        self.assertEqual(api.call_count, 1)

        calls = []
        def route_api(path, method="GET", data=None, zone=False):
            """Record Cloudflare route API calls and return synthetic list or create payloads."""
            calls.append((path, method))
            return [{"id": "preexisting", "pattern": "example.com/other/*", "script": "other"}] if method == "GET" else ({"id": "new-route"} if method == "POST" else None)
        with patch.object(p, "api", side_effect=route_api), patch.object(p, "verify", side_effect=[None, ValueError("bad live output")]), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"), \
                self.assertRaisesRegex(ValueError, "bad live"):
            p.routes()
        self.assertEqual([path for path, m in calls if m == "DELETE"], ["/zones/" + "b" * 32 + "/workers/routes/new-route"])

        with patch.object(p, "api", return_value={"deployments": [{"versions": [{"version_id": "../outside", "percentage": 100}]}]}), \
                self.assertRaisesRegex(ValueError, "invalid version"):
            p.current_version()
        publish.atomic_json(p.state / "publication.json", {"version_id": "v", "previous_version_id": "../outside"})
        with patch.object(p, "activate") as activate, self.assertRaisesRegex(ValueError, "Invalid previous version"):
            p.rollback()
        activate.assert_not_called()

        publish.atomic_json(p.state / "publication-hold.json", {"reason": "rollback"})
        with patch.object(p, "api") as api, self.assertRaisesRegex(ValueError, "on hold"):
            p.publish()
        api.assert_not_called()
        (p.state / "publication-hold.json").unlink()

        p.public.mkdir(parents=True)
        with patch.object(p, "api") as api, self.assertRaisesRegex(ValueError, "Run the core build"):
            p.publish()
        api.assert_not_called()

        self.seed_public()
        ledger = p.publication_readiness()
        publish.atomic_json(p.state / "publication.json", {"content_hash": p.publication_identity(ledger), "manifest": ledger, "version_id": "expected"})
        with patch("publish.shutil.copytree") as copy_tree, patch.object(p, "validate") as validate, \
                patch.object(p, "current_version", return_value="expected"), patch.object(p, "command") as command:
            self.assertEqual(p.publish()["version_id"], "expected")
        copy_tree.assert_not_called()
        validate.assert_not_called()
        command.assert_not_called()
        with patch.object(p, "current_version", return_value="external"), self.assertRaisesRegex(ValueError, "differs.*live external, recorded expected"):
            p.publish()
        for rec in ({"manifest": ledger, "version_id": "expected"}, {"content_hash": "0" * 64, "manifest": ledger, "version_id": "expected"}):
            publish.atomic_json(p.state / "publication.json", rec)
            with patch.object(p, "validate", side_effect=ValueError("validated")), patch.object(p, "current_version", return_value="expected"), \
                    self.assertRaisesRegex(ValueError, "validated"):
                p.publish()

        publish.atomic_json(p.state / "last-run.json", {"ok": True})
        with publish.lock(p.state / "run.lock"):
            code, out = self.run_cli("status")
        report = json.loads(out)
        self.assertEqual((code, report["running"], report["last-run.json"]), (0, True, {"ok": True}))
        self.assertNotIn("manifest", report["publication.json"])

        with patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "", "CLOUDFLARE_API_TOKEN": ""}):
            for cmd in ("status", "hold", "resume"):
                self.assertEqual(self.run_cli("--config", str(self.directory / "deployment.toml"), cmd, patch_publisher=False)[0], 0)
            with self.assertRaisesRegex(ValueError, "CLOUDFLARE_ACCOUNT_ID"):
                publish.Publisher(self.directory / "deployment.toml", offline=True).current_version()

        percent_dir = self.directory / "dir%name"
        percent_dir.mkdir()
        p.cfg["app_python"] = str(percent_dir / "python")
        cron_calls = []
        fake_cron = lambda args, **kw: (cron_calls.append((args, kw)), subprocess.CompletedProcess(args, 0, stdout="", stderr=""))[1]
        with patch("publish.subprocess.run", side_effect=fake_cron):
            p.schedule()
        self.assertIn(r"dir\%name", cron_calls[1][1]["input"])
        p.cfg["app_python"] = "bad\npython"
        with patch("publish.subprocess.run", side_effect=fake_cron), self.assertRaisesRegex(ValueError, "newlines"):
            p.schedule()

    def test_schedule_rejects_boolean_interval_and_malformed_crontab_markers(self):
        """Reject boolean interval_minutes, reversed/partial crontab markers, and preserve unrelated comments."""
        p = self.publisher
        cron_calls = []
        fake_cron = lambda args, **kw: (cron_calls.append((args, kw)), subprocess.CompletedProcess(args, 0, stdout="", stderr=""))[1]
        with patch("publish.subprocess.run", side_effect=fake_cron):
            p.schedule()
        self.assertTrue(cron_calls[1][1]["input"].startswith("# BEGIN ONIONFLASH\n"))

        existing = "# Nightly maintenance\n0 1 * * * /usr/bin/true\n# BEGIN ONIONFLASH\n*/15 * * * * old\n# END ONIONFLASH\n"
        cron_calls.clear()
        with patch("publish.subprocess.run", side_effect=lambda args, **kw: (cron_calls.append((args, kw)), subprocess.CompletedProcess(args, 0, stdout=existing, stderr=""))[1]):
            p.schedule()
        self.assertTrue(cron_calls[1][1]["input"].startswith("# Nightly maintenance\n0 1 * * * /usr/bin/true\n# BEGIN ONIONFLASH\n"))
        self.assertNotIn("*/15 * * * * old", cron_calls[1][1]["input"])

        for bad_cron in (
            "# END ONIONFLASH\n*/5 * * * * x\n# BEGIN ONIONFLASH\n",
            "# BEGIN ONIONFLASH_OTHER\n*/5 * * * * x\n# END ONIONFLASH_OTHER\n",
            "0 * * * * /usr/bin/true # BEGIN ONIONFLASH\n# END ONIONFLASH\n",
        ):
            cron_calls.clear()
            with self.subTest(bad_cron=bad_cron), \
                    patch("publish.subprocess.run", side_effect=lambda args, out=bad_cron, **kw: (cron_calls.append(args), subprocess.CompletedProcess(args, 0, stdout=out, stderr=""))[1]), \
                    self.assertRaisesRegex(ValueError, "Ambiguous existing Onion Flash schedule"):
                p.schedule()
            self.assertEqual(cron_calls, [["crontab", "-l"]])

        p.cfg["interval_minutes"] = True
        with patch("publish.subprocess.run", side_effect=fake_cron), self.assertRaisesRegex(ValueError, "interval_minutes must be 1..59"):
            p.schedule()

    def test_application_budget_contract(self):
        """Honor run_command_budget_seconds for run and collection budget for other app subcommands."""
        from onionflash import config
        ok = subprocess.CompletedProcess([], 0, stdout="{}", stderr="")
        for bound in (420, 850, 3850):
            with self.subTest(bound=bound), patch.object(config, "run_command_budget_seconds", return_value=bound) as budget, \
                    patch("publish.subprocess.run", return_value=ok) as run:
                self.publisher.application("run")
                budget.assert_called_once_with(self.publisher.app)
                self.assertEqual(run.call_args.kwargs["timeout"], bound + 300)
        for command in ("check", "status", "retry", "build"):
            with self.subTest(command=command), patch.object(config, "run_command_budget_seconds", side_effect=AssertionError("Run only")), \
                    patch("publish.subprocess.run", return_value=ok) as run:
                self.publisher.application(command)
                self.assertEqual(run.call_args.kwargs["timeout"], self.publisher.app["collection"]["run_budget_seconds"] + 300)
        absent = object()
        helper = getattr(config, "run_command_budget_seconds", absent)
        try:
            if helper is not absent:
                del config.run_command_budget_seconds
            with patch("publish.subprocess.run", return_value=ok) as run:
                self.publisher.application("run")
            self.assertEqual(run.call_args.kwargs["timeout"], self.publisher.app["collection"]["run_budget_seconds"] + 300)
        finally:
            if helper is not absent:
                config.run_command_budget_seconds = helper
        with patch.object(config, "run_command_budget_seconds", None), \
                patch("publish.subprocess.run") as run, self.assertRaises(TypeError):
            self.publisher.application("run")
        run.assert_not_called()
        with patch.object(config, "run_command_budget_seconds", side_effect=ValueError("Invalid app budget")), \
                patch("publish.subprocess.run") as run, self.assertRaisesRegex(ValueError, "Invalid app budget"):
            self.publisher.application("run")
        run.assert_not_called()

    def test_s3_checksum_config_and_http_exception_retries(self):
        """Configure when_required S3 checksums for Spaces and archive clients and retry HTTPException."""
        from archive_store import ArchiveStore
        spaces_client = publish.s3_client(
            self.publisher, self.publisher.cfg["spaces"], publish.SPACES_KEY, publish.SPACES_SECRET, max_attempts=4
        )
        self.addCleanup(spaces_client.close)
        archive_client = ArchiveStore(self.publisher).client
        self.addCleanup(archive_client.close)
        for client in (spaces_client, archive_client):
            self.assertEqual(client._client_config.request_checksum_calculation, "when_required")
            self.assertEqual(client._client_config.response_checksum_validation, "when_required")

        p, body = self.publisher, b"live content"
        manifest = dict.fromkeys(publish.VERIFY_PATHS, publish.hashlib.sha256(body).hexdigest())
        with patch.object(p, "request", side_effect=[BadStatusLine("bad"), body, body, body]), \
                patch("publish.time.sleep") as sleep:
            p.verify("https://example.com/onionflash", manifest)
        self.assertEqual(sleep.call_count, 1)
        with patch.object(p, "request", side_effect=BadStatusLine("bad")), patch("publish.time.sleep") as sleep:
            self.assertEqual(p.unsigned_read_problem("https://example.com/private"), "could not be checked for privacy (BadStatusLine)")
            self.assertEqual(sleep.call_count, p.net["attempts"] - 1)

    def test_base_url_canonicalization_route_conflicts_and_route_migration(self):
        """Normalize uppercase hosts, reject ports/dots/underscore segments, and migrate routes safely."""
        deploy_path = self.directory / "deployment.toml"
        app_path = self.directory / "onionflash.toml"
        for bad_url, expected in [
            ("https://example.com:443/onionflash/", "default HTTPS port and a canonical hostname"),
            ("https://example.com:8443/onionflash/", "default HTTPS port and a canonical hostname"),
            ("https://[::1]/onionflash/", "default HTTPS port and a canonical hostname"),
            ("https://example.com/_headers/", "reserved underscore segment"),
            ("https://example.com/_redirects/", "reserved underscore segment"),
            ("https://example.com/_headers/sub/", "reserved underscore segment"),
        ]:
            with self.subTest(bad_url=bad_url):
                app_path.write_text(app_toml(base_url=bad_url))
                with self.assertRaisesRegex(ValueError, expected):
                    publish.Publisher(deploy_path)
                with self.assertLogs(publish.LOG, "ERROR"):
                    self.assertEqual(self.run_cli("--config", str(deploy_path), "check", patch_publisher=False)[0], 1)

        real_load = publish.app_config.load_config
        def load_raw_base_url(raw_base_url):
            """Return a patched load_config preserving raw_base_url before app-level normalization."""
            def _load(path):
                cfg = real_load(path)
                cfg["site"]["base_url"] = raw_base_url
                return cfg
            return _load

        app_path.write_text(app_toml(base_url="https://example.com/onionflash/"))
        with patch.object(publish.app_config, "load_config", side_effect=load_raw_base_url("https://example.com./onionflash/")):
            with self.assertRaisesRegex(ValueError, "default HTTPS port and a canonical hostname"):
                publish.Publisher(deploy_path)
            with self.assertLogs(publish.LOG, "ERROR"):
                self.assertEqual(self.run_cli("--config", str(deploy_path), "check", patch_publisher=False)[0], 1)

        with patch.object(publish.app_config, "load_config", side_effect=load_raw_base_url("https://Example.COM/news/")):
            raw_upper = publish.Publisher(deploy_path)
        self.assertEqual((raw_upper.host, raw_upper.base_url, raw_upper.route_pattern),
                         ("example.com", "https://example.com/news/", "example.com/news*"))

        app_path.write_text(app_toml(base_url="https://Example.COM/news/"))
        upper = publish.Publisher(deploy_path)
        self.assertEqual((upper.host, upper.base_url, upper.route_pattern),
                         ("example.com", "https://example.com/news/", "example.com/news*"))
        publish.atomic_json(upper.state / "publication.json", {"manifest": {}})
        conflict = [{"id": "existing", "pattern": "example.com/news*", "script": "another-worker"}]
        with patch.object(upper, "api", return_value=conflict), patch.object(upper, "verify"), \
                patch.object(upper, "origin_url", return_value="https://test.workers.dev/news"), \
                self.assertRaisesRegex(ValueError, "another Worker: example.com/news\\*"):
            upper.routes()

        app_path.write_text(app_toml())
        self.seed_public()
        self.fake_cloudflare(OLD)
        publish.atomic_json(self.p.state / "routes.json", {"patterns": ["old.example.com/onionflash*"]})
        verified_urls = []
        self._patch("verify", side_effect=lambda url, manifest, archive=None: verified_urls.append(url))
        self.p.publish()
        self.assertEqual(verified_urls[-1], "https://news.example.workers.dev/onionflash")
        with patch.object(self.p, "api", side_effect=lambda path, method="GET", data=None, zone=False:
                          [] if method == "GET" else {"id": "migrated-route"}):
            self.p.routes()
        self.assertEqual(publish.read_json(self.p.state / "routes.json")["patterns"], ["example.com/onionflash*"])
        self.add_article("after-route")
        self.live[0] = NEW
        publish.atomic_json(self.p.state / "publication.json", {"version_id": NEW, "manifest": {"index.html": "0" * 64}})
        self.p.publish()
        self.assertEqual(verified_urls[-1], "https://example.com/onionflash/")

    def test_redact_orders_overlapping_credentials_longest_first(self):
        """Replace longer overlapping credentials before shorter substrings during redaction."""
        short = "abcdefgh"
        prefix_match = "abcdefgh_tail_fragment"
        middle_match = "xx_abcdefgh_secret_tail"
        env = {
            "TEST_SHORT_TOKEN": short,
            "TEST_PREFIX_SECRET": prefix_match,
            "TEST_SUBSTR_API_KEY": middle_match,
        }
        with patch.dict(os.environ, env):
            redacted = publish.redact(f"err1={middle_match} err2={prefix_match} err3={short}")
        self.assertEqual(redacted, "err1=[redacted] err2=[redacted] err3=[redacted]")
        for leaked in ("xx_", "_secret_tail", "_tail_fragment", short):
            self.assertNotIn(leaked, redacted)

    def test_schedule_preserves_unicode_separators_rejects_backslashes_and_reports_stderr(self):
        """Keep Unicode separators inside comments, reject backslash paths, and redact crontab stderr."""
        p = self.publisher
        existing = "# formfeed\x0cMAILTO=bad@example.com\n# linesep\u2028SHELL=/bin/sh\n0 1 * * * /usr/bin/true\n"
        cron_calls = []
        with patch("publish.subprocess.run", side_effect=lambda args, **kw: (
            cron_calls.append((args, kw)), subprocess.CompletedProcess(args, 0, stdout=existing, stderr="")
        )[1]):
            p.schedule()
        installed_lines = cron_calls[1][1]["input"].split("\n")
        self.assertEqual(installed_lines[:3], [
            "# formfeed\x0cMAILTO=bad@example.com",
            "# linesep\u2028SHELL=/bin/sh",
            "0 1 * * * /usr/bin/true",
        ])
        self.assertTrue(cron_calls[1][1]["capture_output"])

        for bad_python in (str(self.directory / r"dir\%name/python"), str(self.directory / r"dir\name/python")):
            p.cfg["app_python"] = bad_python
            cron_calls.clear()
            with self.subTest(bad_python=bad_python), \
                    patch("publish.subprocess.run", side_effect=lambda args, **kw: (
                        cron_calls.append(args), subprocess.CompletedProcess(args, 0, stdout="", stderr="")
                    )[1]), \
                    self.assertRaisesRegex(ValueError, "newlines or backslashes"):
                p.schedule()
            self.assertEqual(cron_calls, [["crontab", "-l"]])

        p.cfg["app_python"] = ".venv/bin/python"
        secret = os.environ["CLOUDFLARE_API_TOKEN"]
        def fail_install(args, **kw):
            """Return an empty crontab on -l and raise CalledProcessError with stderr on install."""
            if args == ["crontab", "-l"]:
                return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
            raise subprocess.CalledProcessError(1, args, output="", stderr=f"crontab: bad minute {secret}\n")
        with patch("publish.subprocess.run", side_effect=fail_install), \
                self.assertRaisesRegex(RuntimeError, r"Cannot install crontab: crontab: bad minute \[redacted\]") as err:
            p.schedule()
        self.assertNotIn(secret, str(err.exception))


if __name__ == "__main__":
    unittest.main()
