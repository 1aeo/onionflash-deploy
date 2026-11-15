"""The publisher respects the application's public completion budget contract."""
import subprocess
from unittest.mock import patch

from onionflash import config
from tests.fakes import DeployTestCase


class ApplicationBudgetTests(DeployTestCase):
    def test_run_includes_the_inflight_completion_allowance(self):
        result = subprocess.CompletedProcess([], 0, stdout="{}", stderr="")
        for timeout in (120, 600, 3600):
            with self.subTest(timeout=timeout):
                self.publisher.app["llm"]["timeout_seconds"] = timeout
                bound = config.run_command_budget_seconds(self.publisher.app)
                with patch("publish.subprocess.run", return_value=result) as run:
                    self.publisher.application("run")
                actual = run.call_args.kwargs["timeout"]
                self.assertEqual(actual, bound + 300)
                self.assertGreater(actual, self.publisher.app["collection"]["run_budget_seconds"] + timeout)

    def test_other_commands_retain_the_smaller_allowance(self):
        result = subprocess.CompletedProcess([], 0, stdout="{}", stderr="")
        for command in ("check", "status", "retry", "build"):
            with self.subTest(command=command):
                with patch.object(config, "run_command_budget_seconds", side_effect=AssertionError("Run only")), patch("publish.subprocess.run", return_value=result) as run:
                    self.publisher.application(command)
                self.assertEqual(run.call_args.kwargs["timeout"], self.publisher.app["collection"]["run_budget_seconds"] + 300)

    def test_missing_legacy_api_retains_old_bound_without_hiding_api_errors(self):
        result = subprocess.CompletedProcess([], 0, stdout="{}", stderr="")
        helper = config.run_command_budget_seconds
        try:
            del config.run_command_budget_seconds
            with patch("publish.subprocess.run", return_value=result) as run:
                self.publisher.application("run")
            self.assertEqual(run.call_args.kwargs["timeout"], self.publisher.app["collection"]["run_budget_seconds"] + 300)
        finally:
            config.run_command_budget_seconds = helper
        with patch.object(config, "run_command_budget_seconds", side_effect=ValueError("Invalid app budget")), patch("publish.subprocess.run") as run:
            with self.assertRaisesRegex(ValueError, "Invalid app budget"):
                self.publisher.application("run")
            run.assert_not_called()

    def test_present_invalid_api_is_not_treated_as_an_older_release(self):
        with patch.object(config, "run_command_budget_seconds", None), patch("publish.subprocess.run") as run:
            with self.assertRaises(TypeError):
                self.publisher.application("run")
            run.assert_not_called()
