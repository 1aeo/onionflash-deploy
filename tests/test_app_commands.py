"""Tests for offline operator CLI routing, secret stripping, locking, and deploy.sh dispatch."""
from contextlib import redirect_stderr, redirect_stdout
import io, json, os, shutil, socket, subprocess, sys, unittest
from unittest.mock import patch

import app_commands
from onionflash import cli
import publish
from tests.fakes import init_deploy_dir


class AppCommandTests(unittest.TestCase):
    """Tests for app_commands.py argument validation, credential isolation, and lock handling."""

    def setUp(self):
        """Create an isolated deployment directory with mode-0600 secrets.env."""
        self.directory = init_deploy_dir(self, env={}, clear_env=True, extra='[llm]\nbase_url = "${ONIONFLASH_LLM_BASE_URL}"\n')
        self.config = self.directory / "deployment.toml"
        self.app_config = self.directory / "onionflash.toml"
        self.secrets = {
            "CLOUDFLARE_ACCOUNT_ID": "a" * 32,
            "CLOUDFLARE_API_TOKEN": "private-cloudflare-token",
            "CLOUDFLARE_ZONE_TOKEN": "private-zone-token",
            "DO_SPACES_KEY": "private-spaces-key",
            "DO_SPACES_SECRET": "private-spaces-secret",
            "ONIONFLASH_LLM_API_KEY": "private-llm-key",
            "ONIONFLASH_LLM_BASE_URL": "https://llm.example.org/v1",
        }
        self.env_file = self.directory / "secrets.env"
        self.env_file.write_text("\n".join(f"{key}={value}" for key, value in self.secrets.items()))
        self.env_file.chmod(0o600)

    def run_app(self, *arguments):
        """Invoke app_commands.main against self.config with arguments."""
        return app_commands.main([str(self.config), *arguments])

    def assert_app_fails(self, *arguments, code=1):
        """Assert run_app exits with code without invoking the core CLI and return stderr."""
        with patch("onionflash.cli.main") as invoke, redirect_stderr(io.StringIO()) as error:
            self.assertEqual(self.run_app(*arguments), code)
        invoke.assert_not_called()
        return error.getvalue()

    def test_routing_injects_fixed_configuration_after_command_or_action(self):
        """Inject --config immediately after the command or operator action."""
        cases = [([cmd, action, "--reason", "operator reason"], 2)
                 for cmd, actions in cli.OPERATOR_ACTIONS.items() for action in actions]
        cases += [(["build", "--force"], 1), (["status"], 1)]
        for arguments, position in cases:
            with self.subTest(arguments=arguments), patch("onionflash.cli.main", return_value=0) as invoke:
                self.assertEqual(self.run_app(*arguments), 0)
                self.assertEqual(invoke.call_args.args[0], [*arguments[:position], "--config", str(self.app_config), *arguments[position:]])

    def test_invalid_command_action_or_config_override_never_initializes_publisher(self):
        """Reject disallowed commands, missing actions, and --config overrides before Publisher init."""
        for arguments in [
            ("run",), ("check",), ("story",), ("rejected",), ("decisions",), ("story", "delete"), ("decisions", "delete"),
            ("status", "--config", "elsewhere.toml"), ("build", "--config=elsewhere.toml"),
            ("story", "show", "--conf=elsewhere.toml"), ("rejected", "list", "--c", "elsewhere.toml"),
            ("--config", "elsewhere.toml", "status"),
        ]:
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                    patch("app_commands.Publisher") as publisher, self.assertRaises(SystemExit) as error:
                self.run_app(*arguments)
            self.assertEqual(error.exception.code, 2)
            publisher.assert_not_called()

    def test_dotenv_resolves_config_strips_credentials_for_core_and_redacts_errors(self):
        """Strip deployment and LLM secrets during core CLI execution and restore/redact afterward."""
        self.config.write_text(self.config.read_text().replace('secret_key_env = "DO_SPACES_SECRET"', 'secret_key_env = "CUSTOM_PRIVATE_VALUE"'))
        os.environ["CUSTOM_PRIVATE_VALUE"] = "custom-private-value"
        private = {"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET",
                   "CUSTOM_PRIVATE_VALUE", "ONIONFLASH_LLM_API_KEY"}

        def inspect(arguments):
            """Verify private env vars are absent while non-secret config interpolation succeeds."""
            from onionflash.config import load_config
            self.assertFalse(private.intersection(os.environ))
            resolved = load_config(arguments[arguments.index("--config") + 1])
            self.assertEqual(resolved["llm"]["base_url"], self.secrets["ONIONFLASH_LLM_BASE_URL"])
            return 7

        with patch("onionflash.cli.main", side_effect=inspect):
            self.assertEqual(self.run_app("status"), 7)
        for key in private - {"CUSTOM_PRIVATE_VALUE"}:
            self.assertEqual(os.environ[key], self.secrets[key])
        self.assertEqual(os.environ["CUSTOM_PRIVATE_VALUE"], "custom-private-value")

        error = io.StringIO()
        with patch("onionflash.cli.main", side_effect=RuntimeError("private-llm-key")), redirect_stderr(error):
            self.assertEqual(self.run_app("status"), 1)
        self.assertNotIn("private-llm-key", error.getvalue())
        self.assertEqual(os.environ["ONIONFLASH_LLM_API_KEY"], self.secrets["ONIONFLASH_LLM_API_KEY"])

    def test_dotenv_permissions_and_setup_error_redaction(self):
        """Refuse world-readable secrets.env and redact leaked secrets from startup errors."""
        self.env_file.chmod(0o644)
        self.assertIn("mode 600", self.assert_app_fails("status"))
        self.env_file.chmod(0o600)
        self.env_file.write_text(self.env_file.read_text() + "\nCUSTOM_PRIVATE_VALUE=custom-private-value\n")
        original = self.app_config.read_text()
        for name, value in (("CLOUDFLARE_API_TOKEN", "private-cloudflare-token"), ("CUSTOM_PRIVATE_VALUE", "custom-private-value")):
            with self.subTest(name=name):
                self.app_config.write_text(original.replace("[llm]\n", f'[llm]\nallowed_networks = ["{value}"]\n'))
                stderr = self.assert_app_fails("status")
                self.assertNotIn(value, stderr)
                self.assertIn("[redacted]", stderr)

    def test_non_string_archive_keys_and_non_table_sections_preserve_environment(self):
        """Reject malformed archive key names or non-table sections without losing env vars."""
        base_deploy = self.config.read_text()
        no_archive_table = "archive = false\n" + base_deploy.split("[archive]\n")[0] + "[retention]\nenabled = false\n"
        live_env = {"CLOUDFLARE_API_TOKEN": "live-cloudflare-secret-token", "EXTRA_API_TOKEN": "live-extra-secret-token"}
        for label, bad_toml in (
            ("int_key", base_deploy.replace('secret_key_env = "DO_SPACES_SECRET"', "secret_key_env = 123")),
            ("list_key", base_deploy.replace('secret_key_env = "DO_SPACES_SECRET"', 'secret_key_env = ["BAD"]')),
            ("bool_section", no_archive_table),
        ):
            with self.subTest(case=label), patch.dict(os.environ, live_env, clear=True):
                self.config.write_text(bad_toml)
                stderr = self.assert_app_fails("status")
                self.assertEqual(os.environ["CLOUDFLARE_API_TOKEN"], live_env["CLOUDFLARE_API_TOKEN"])
                self.assertEqual(os.environ["EXTRA_API_TOKEN"], live_env["EXTRA_API_TOKEN"])
                self.assertNotIn("TypeError", stderr)
                self.assertNotIn("AttributeError", stderr)
        self.config.write_text(base_deploy)

        def failing_names():
            """Yield one valid credential name and then fail mid-iteration."""
            yield "CLOUDFLARE_API_TOKEN"
            raise RuntimeError("boom: live-cloudflare-secret-token")

        with patch.dict(os.environ, live_env, clear=True):
            with app_commands.without_credentials(["CLOUDFLARE_API_TOKEN", 123, ["BAD"], ""]):
                self.assertNotIn("CLOUDFLARE_API_TOKEN", os.environ)
            self.assertEqual(os.environ["CLOUDFLARE_API_TOKEN"], live_env["CLOUDFLARE_API_TOKEN"])
            with self.assertRaises(RuntimeError) as mid_err:
                with app_commands.without_credentials(failing_names()):
                    pass
            self.assertEqual(os.environ["CLOUDFLARE_API_TOKEN"], live_env["CLOUDFLARE_API_TOKEN"])
            self.assertNotIn(live_env["CLOUDFLARE_API_TOKEN"], publish.clean_error(None, mid_err.exception))

    def test_busy_mutations_return_75_but_inspection_and_offline_double_dash_work(self):
        """Return 75 for mutating commands under run.lock while allowing read-only inspection."""
        mutations = [("build",), *((cmd, act) for cmd, acts in cli.OPERATOR_ACTIONS.items() for act in acts - cli.READ_ONLY_ACTIONS)]
        inspections = [("status",), *((cmd, act) for cmd, acts in cli.OPERATOR_ACTIONS.items() for act in acts & cli.READ_ONLY_ACTIONS)]
        with publish.lock(self.directory / "state/run.lock"):
            for arguments in mutations:
                with self.subTest(arguments=arguments):
                    self.assertIn("command not performed", self.assert_app_fails(*arguments, code=75))
            for arguments in inspections:
                with self.subTest(arguments=arguments), patch("onionflash.cli.main", return_value=0) as invoke:
                    self.assertEqual(self.run_app(*arguments), 0)
                invoke.assert_called_once()
        self.env_file.write_text("\n".join(f"{k}={v}" for k, v in self.secrets.items() if not k.startswith("CLOUDFLARE_")))
        self.env_file.chmod(0o600)
        for arguments in (["story", "retract", "--reason=--", "--", "--config"], ["story", "show", "--", "--conf"]):
            with self.subTest(arguments=arguments), patch("onionflash.cli.main", return_value=0) as invoke:
                self.assertEqual(self.run_app(*arguments), 0)
                invoke.assert_called_once()

    def test_real_core_status_build_database_lock_and_shell_forwarding(self):
        """Run end-to-end offline core commands, verify DB lock contention, and test deploy.sh."""
        from onionflash.cli import run_lock
        with patch.object(socket.socket, "connect", side_effect=AssertionError("unexpected network")) as connect, \
                patch("subprocess.run", side_effect=AssertionError("unexpected subprocess")), redirect_stdout(io.StringIO()):
            self.assertEqual(self.run_app("status"), 0)
            self.assertEqual(self.run_app("rejected", "list", "--limit", "1"), 0)
            self.assertEqual(self.run_app("decisions", "summary"), 0)
            self.assertEqual(self.run_app("build"), 0)
            self.assertTrue((self.directory / "build/public/feed.xml").is_file())
            with run_lock(str(self.directory / "state/news.sqlite3")) as acquired:
                self.assertTrue(acquired)
                self.assertEqual(self.run_app("build"), 75)
                self.assertEqual(self.run_app("status"), 0)
        connect.assert_not_called()

        shutil.copyfile(publish.ROOT / "deploy.sh", self.directory / "deploy.sh")
        binary = self.directory / ".venv/bin/python"
        binary.parent.mkdir(parents=True)
        binary.symlink_to(sys.executable)
        for name in ("app_commands.py", "publish.py"):
            (self.directory / name).write_text("import json, sys; print(json.dumps(sys.argv))\n")
        custom_config = str(self.directory / "deployment with spaces.toml")
        for arguments, script, expected in [
            (["app", "story", "show", "--story", "abc"], "app_commands.py", [custom_config, "story", "show", "--story", "abc"]),
            (["backup", "--force"], "publish.py", ["--config", custom_config, "backup", "--force"]),
        ]:
            with self.subTest(arguments=arguments):
                result = subprocess.run(["/bin/bash", str(self.directory / "deploy.sh"), *arguments],
                                        env={"ONIONFLASH_DEPLOY_CONFIG": custom_config}, capture_output=True, text=True, check=True)
                self.assertEqual(json.loads(result.stdout), [str(self.directory / script), *expected])


if __name__ == "__main__":
    unittest.main()
