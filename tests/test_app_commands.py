import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

import app_commands
import publish


class AppCommandTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        self.config = self.directory / "deployment.toml"
        self.config.write_text((publish.ROOT / "deployment.example.toml").read_text())
        self.app_config = self.directory / "onionflash.toml"
        self.app_config.write_text('''[site]
base_url = "https://example.com/onionflash/"
[storage]
database = "state/news.sqlite3"
output_dir = "build/public"
[llm]
base_url = "${ONIONFLASH_LLM_BASE_URL}"
[[sources]]
id = "test"
url = "https://example.org/feed.xml"
''')
        self.secrets = {
            "CLOUDFLARE_ACCOUNT_ID": "a" * 32,
            "CLOUDFLARE_API_TOKEN": "private-cloudflare-token",
            "CLOUDFLARE_ZONE_TOKEN": "private-zone-token",
            "DO_SPACES_KEY": "private-spaces-key",
            "DO_SPACES_SECRET": "private-spaces-secret",
            "ONIONFLASH_LLM_API_KEY": "private-llm-key",
            "ONIONFLASH_LLM_BASE_URL": "https://llm.example.org/v1",
        }
        env_file = self.directory / "secrets.env"
        env_file.write_text("\n".join(f"{key}={value}" for key, value in self.secrets.items()))
        env_file.chmod(0o600)

    def run_app(self, *arguments):
        return app_commands.main([str(self.config), *arguments])

    def test_routing_injects_fixed_configuration_after_command_or_action(self):
        cases = [([command, action, "--reason", "operator reason"], 2)
                 for command, actions in app_commands.ACTIONS.items() for action in actions]
        cases += [(["build", "--force"], 1), (["status"], 1)]
        for arguments, position in cases:
            with self.subTest(arguments=arguments), patch("onionflash.cli.main", return_value=0) as invoke:
                self.assertEqual(self.run_app(*arguments), 0)
                self.assertEqual(invoke.call_args.args[0], [*arguments[:position], "--config",
                                                         str(self.app_config), *arguments[position:]])

    def test_invalid_command_action_or_config_override_never_initializes_publisher(self):
        cases = [("run",), ("check",), ("story",), ("rejected",), ("story", "delete"),
                 ("status", "--config", "elsewhere.toml"), ("build", "--config=elsewhere.toml"),
                 ("story", "show", "--conf=elsewhere.toml"), ("rejected", "list", "--c", "elsewhere.toml"),
                 ("--config", "elsewhere.toml", "status")]
        for arguments in cases:
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                    patch("app_commands.Publisher") as publisher, self.assertRaises(SystemExit) as error:
                self.run_app(*arguments)
            self.assertEqual(error.exception.code, 2)
            publisher.assert_not_called()

    def test_dotenv_resolves_config_but_all_private_credentials_are_removed_for_core(self):
        self.config.write_text(self.config.read_text().replace('secret_key_env = "DO_SPACES_SECRET"',
                                                              'secret_key_env = "CUSTOM_PRIVATE_VALUE"'))
        os.environ["CUSTOM_PRIVATE_VALUE"] = "custom-private-value"
        private = {"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_TOKEN", "DO_SPACES_KEY", "DO_SPACES_SECRET",
                   "CUSTOM_PRIVATE_VALUE", "ONIONFLASH_LLM_API_KEY"}

        def inspect(arguments):
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

    def test_failure_restores_environment_without_logging_secret(self):
        error = io.StringIO()
        with patch("onionflash.cli.main", side_effect=RuntimeError("private-llm-key")), redirect_stderr(error):
            self.assertEqual(self.run_app("status"), 1)
        self.assertNotIn("private-llm-key", error.getvalue())
        self.assertEqual(os.environ["ONIONFLASH_LLM_API_KEY"], self.secrets["ONIONFLASH_LLM_API_KEY"])

    def test_protected_dotenv_permissions_remain_required(self):
        (self.directory / "secrets.env").chmod(0o644)
        with patch("onionflash.cli.main") as invoke, redirect_stderr(io.StringIO()):
            self.assertEqual(self.run_app("status"), 1)
        invoke.assert_not_called()

    def test_busy_mutations_return_75_without_invocation_but_inspection_remains_available(self):
        mutations = [("build",), *(('story', action) for action in ("correct", "retract", "restore", "unlock")),
                     ("rejected", "retry"), ("rejected", "dismiss")]
        inspections = [("status",), ("story", "show"), ("story", "history"),
                       ("rejected", "list"), ("rejected", "show")]
        with publish.lock(self.directory / "state/run.lock"):
            for arguments in mutations:
                error = io.StringIO()
                with self.subTest(arguments=arguments), patch("onionflash.cli.main") as invoke, redirect_stderr(error):
                    self.assertEqual(self.run_app(*arguments), 75)
                invoke.assert_not_called()
                self.assertIn("command not performed", error.getvalue())
            for arguments in inspections:
                with self.subTest(arguments=arguments), patch("onionflash.cli.main", return_value=0) as invoke:
                    self.assertEqual(self.run_app(*arguments), 0)
                invoke.assert_called_once()

    def test_real_core_status_build_and_database_lock(self):
        from onionflash.cli import run_lock
        # Every HTTP client ends in socket.connect. Errors may become exit codes, so the mock is checked too.
        with patch.object(socket.socket, "connect", side_effect=AssertionError("unexpected network")) as connect, \
                patch("subprocess.run", side_effect=AssertionError("unexpected subprocess")), redirect_stdout(io.StringIO()):
            self.assertEqual(self.run_app("status"), 0)
            self.assertEqual(self.run_app("rejected", "list", "--limit", "1"), 0)
            self.assertEqual(self.run_app("build"), 0)
            self.assertTrue((self.directory / "build/public/feed.xml").is_file())
            with run_lock(str(self.directory / "state/news.sqlite3")) as acquired:
                self.assertTrue(acquired)
                self.assertEqual(self.run_app("build"), 75)
                self.assertEqual(self.run_app("status"), 0)
        connect.assert_not_called()

    def test_shell_routes_app_and_preserves_publisher_backup_force_arguments(self):
        shutil.copyfile(publish.ROOT / "deploy.sh", self.directory / "deploy.sh")
        binary = self.directory / ".venv/bin/python"
        binary.parent.mkdir(parents=True)
        binary.symlink_to(sys.executable)
        for name in ("app_commands.py", "publish.py"):
            (self.directory / name).write_text("import json, sys; print(json.dumps(sys.argv))\n")
        custom_config = str(self.directory / "deployment with spaces.toml")
        for arguments, script, expected in [
            (["app", "story", "show", "--story", "abc"], "app_commands.py",
             [custom_config, "story", "show", "--story", "abc"]),
            (["backup", "--force"], "publish.py", ["--config", custom_config, "backup", "--force"]),
        ]:
            with self.subTest(arguments=arguments):
                result = subprocess.run(["/bin/bash", str(self.directory / "deploy.sh"), *arguments],
                                        env={"ONIONFLASH_DEPLOY_CONFIG": custom_config}, capture_output=True, text=True, check=True)
                self.assertEqual(json.loads(result.stdout), [str(self.directory / script), *expected])


if __name__ == "__main__":
    unittest.main()
