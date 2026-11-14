from contextlib import closing, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io, json, os, sqlite3, subprocess, threading, unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import publish
from tests.fakes import DeployTestCase, seed_public_site


class DeploymentTests(DeployTestCase):
    def serve(self, handle):
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
        env = patch.dict(os.environ, {"no_proxy": "127.0.0.1", "NO_PROXY": "127.0.0.1"})
        env.start()
        self.addCleanup(env.stop)
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
        return seed_public_site(self.directory / "public")

    def test_public_validation_allowlist_symlinks_credentials_and_assets(self):
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

        public = self.public_tree()
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
        path = self.directory / "deployment.toml"
        base = path.read_text()
        for target, replacement, expected in [
            ("[archive]\n", '[archive]\nenv_file = "old.env"\nshard_prefix_chars = 2\n', "Unknown archive settings: env_file, shard_prefix_chars"),
            ("[retention]\n", "[retention]\nhot_days = 90\n", "Unknown retention settings: hot_days"),
            ("[retention]\n", "[retention]\nenable = true\n", "Unknown retention settings: enable"),
            ('[spaces]\nenabled = false', '[spaces]\nenabled = "false"', "spaces.enabled must be a boolean"),
            ("required = false", "required = 1", "spaces.required must be a boolean"),
        ]:
            with self.subTest(replacement=replacement):
                path.write_text(base.replace(target, replacement))
                with self.assertRaisesRegex(ValueError, expected):
                    publish.Publisher(path)
        path.write_text(base)
        self.publisher.cfg["spaces"].update(enabled=True, retention_days=0)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.publisher.backup()
        self.publisher.cfg["spaces"].update(retention_days=30, region="")
        with patch.object(self.publisher, "application", return_value="ok"), \
                patch.object(self.publisher, "node", Path(__file__)), patch.object(self.publisher, "wrangler", Path(__file__)), \
                self.assertRaisesRegex(ValueError, "Spaces region"):
            self.publisher.execute("check")

    def test_dotenv_paths_redaction_and_process_credential_isolation(self):
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

        (self.directory / "onionflash.toml").write_text(
            '[site]\nbase_url = "https://example.com/onionflash"\n[storage]\ndatabase = "${OF_TEST_STATE}/news.sqlite3"\n'
            '[[sources]]\nid = "test"\nurl = "https://example.org/feed.xml"\n'
        )
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
        wrangler, app = captured
        self.assertEqual(wrangler["CLOUDFLARE_API_TOKEN"], self.publisher.token)
        self.assertFalse({"CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET", "ONIONFLASH_LLM_API_KEY"} & set(wrangler))
        self.assertEqual(app["ONIONFLASH_LLM_API_KEY"], "llm-key-value")
        self.assertFalse({"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET"} & set(app))

    def test_http_redirect_token_stripping_api_bounds_and_verify_retries(self):
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

    def test_routes_versions_unchanged_shortcut_offline_status_and_schedule(self):
        p = self.publisher
        publish.atomic_json(p.state / "publication.json", {"version_id": "live-version"})
        with self.assertRaisesRegex(ValueError, "publish once, then attach routes"):
            p.routes()

        publish.atomic_json(p.state / "publication.json", {"manifest": {}})
        conflict = [{"id": "existing", "pattern": "example.com/onionflash*", "script": "another-project"}]
        with patch.object(p, "api", return_value=conflict) as api, patch.object(p, "verify"), \
                patch.object(p, "origin_url", return_value="https://test.workers.dev/onionflash"), \
                self.assertRaisesRegex(ValueError, "another Worker"):
            p.routes()
        self.assertEqual(api.call_count, 1)

        calls = []
        def route_api(path, method="GET", data=None, zone=False):
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

        ledger = {"index.html": "a" * 64}
        with closing(sqlite3.connect(p.database)) as db:
            db.executescript("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT); CREATE TABLE public_artifacts(path TEXT PRIMARY KEY, sha256 TEXT);")
            db.executemany("INSERT INTO public_artifacts VALUES(?,?)", ledger.items())
            db.commit()
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
        output = io.StringIO()
        with publish.lock(p.state / "run.lock"), patch("publish.Publisher", return_value=p), patch("sys.argv", ["publish.py", "status"]), redirect_stdout(output):
            self.assertEqual(publish.main(), 0)
        report = json.loads(output.getvalue())
        self.assertEqual((report["running"], report["last-run.json"]), (True, {"ok": True}))
        self.assertNotIn("manifest", report["publication.json"])

        with patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "", "CLOUDFLARE_API_TOKEN": ""}):
            for cmd in ("status", "hold", "resume"):
                with patch("sys.argv", ["publish.py", "--config", str(self.directory / "deployment.toml"), cmd]), redirect_stdout(io.StringIO()):
                    self.assertEqual(publish.main(), 0)
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


if __name__ == "__main__":
    unittest.main()
