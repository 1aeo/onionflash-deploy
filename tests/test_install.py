"""The installer builds the application from its checked-out commit only."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import install


class InstallTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.app = self.root / "app"
        (self.app / "src/pkg").mkdir(parents=True)
        (self.app / "src/pkg/kept.py").write_text("KEPT = 1\n")
        (self.app / ".gitignore").write_text("build/\n*.egg-info/\n")
        for name in ("pyproject.toml", "config.example.toml"):
            (self.app / name).touch()
        git = ["git", "-C", str(self.app), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false"]
        subprocess.run([*git, "init", "-q"], check=True)
        subprocess.run([*git, "add", "-A"], check=True)
        subprocess.run([*git, "commit", "-qm", "test"], check=True)

    def test_pip_installs_only_the_committed_files(self):
        for path in ("build/lib/pkg/cached.py", "src/pkg.egg-info/SOURCES.txt", "src/pkg/untracked.py"):
            (self.app / path).parent.mkdir(parents=True, exist_ok=True)
            (self.app / path).write_text("not committed\n")
        real_run, installed = subprocess.run, {}
        def fake(arguments, **kwargs):
            if arguments[0] in ("git", "tar"):
                return real_run(arguments, **kwargs)
            if "pip" in arguments:
                installed["tree"] = tree = Path(arguments[-1])
                installed["files"] = sorted(str(path.relative_to(tree)) for path in tree.rglob("*") if path.is_file())
                raise subprocess.CalledProcessError(1, arguments)  # stops the installer before it writes into this repository
            return subprocess.CompletedProcess(arguments, 0)
        with patch("sys.argv", ["install.py", str(self.app)]), patch.dict(os.environ, {"ONIONFLASH_DEPLOY_CONFIG": str(self.root / "deployment.toml")}), \
                patch("install.shutil.which", return_value="npm"), patch("install.subprocess.run", side_effect=fake), \
                self.assertRaises(subprocess.CalledProcessError):
            install.main()
        self.assertEqual(installed["files"], [".gitignore", "config.example.toml", "pyproject.toml", "src/pkg/kept.py"])
        self.assertFalse(installed["tree"].exists())

    def test_uncommitted_changes_are_refused(self):
        (self.app / "src/pkg/kept.py").write_text("KEPT = 2\n")
        with self.subTest(staged=False), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
            install.export(self.app, self.root)
        subprocess.run(["git", "-C", str(self.app), "add", "src/pkg/kept.py"], check=True)
        with self.subTest(staged=True), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
            install.export(self.app, self.root)

    def test_a_directory_that_is_not_a_checkout_reports_the_git_error(self):
        plain = self.root / "plain"
        plain.mkdir()
        with patch.dict(os.environ, {"GIT_CEILING_DIRECTORIES": str(self.root)}), self.assertRaisesRegex(SystemExit, "not a git repository"):
            install.export(plain, self.root)
