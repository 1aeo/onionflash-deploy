"""The installer builds the application from its checked-out commit only."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

import install


class InstallTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.app = self.root / "app"
        self.checkout(self.app)

    def git(self, checkout, *arguments):
        environment = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
        return subprocess.run(["git", "-C", str(checkout), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                               "-c", "commit.gpgsign=false", *arguments], env=environment, check=True, capture_output=True, text=True)

    def checkout(self, directory, content="KEPT = 1\n"):
        (directory / "src/pkg").mkdir(parents=True)
        (directory / "src/pkg/kept.py").write_text(content)
        (directory / ".gitignore").write_text("build/\n*.egg-info/\n")
        for name in ("pyproject.toml", "config.example.toml"):
            (directory / name).touch()
        self.git(directory, "init", "-q")
        self.git(directory, "add", "-A")
        self.git(directory, "commit", "-qm", "test")

    def foreign_environment(self, foreign):
        return {
            "GIT_DIR": str(foreign / ".git"),
            "GIT_WORK_TREE": str(foreign),
            "GIT_COMMON_DIR": str(foreign / ".git"),
            "GIT_INDEX_FILE": str(foreign / ".git/index"),
            "GIT_OBJECT_DIRECTORY": str(foreign / ".git/objects"),
            "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(foreign / ".git/objects"),
        }

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
        self.git(self.app, "add", "src/pkg/kept.py")
        with self.subTest(staged=True), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
            install.export(self.app, self.root)

    def test_a_directory_that_is_not_a_checkout_reports_the_git_error(self):
        plain = self.root / "plain"
        plain.mkdir()
        with self.assertRaisesRegex(SystemExit, "not a git repository"):
            install.export(plain, self.root)

    def test_inherited_git_environment_cannot_redirect_export_or_dirty_check(self):
        foreign = self.root / "foreign"
        self.checkout(foreign, "FOREIGN = 1\n")
        exported = self.root / "exported"
        exported.mkdir()
        with patch.dict(os.environ, self.foreign_environment(foreign)):
            install.export(self.app, exported)
            self.assertEqual((exported / "src/pkg/kept.py").read_text(), "KEPT = 1\n")
            (self.app / "src/pkg/kept.py").write_text("KEPT = 2\n")
            for staged in (False, True):
                if staged:
                    self.git(self.app, "add", "src/pkg/kept.py")
                with self.subTest(staged=staged), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
                    install.export(self.app, exported)
        self.assertEqual(self.git(foreign, "status", "--porcelain").stdout, "")
        self.assertEqual((foreign / "src/pkg/kept.py").read_text(), "FOREIGN = 1\n")

    def test_fixture_git_calls_leave_an_inherited_repository_untouched(self):
        foreign = self.root / "foreign"
        self.checkout(foreign, "FOREIGN = 1\n")
        (foreign / "untracked.txt").write_text("leave untracked\n")
        before_head = self.git(foreign, "rev-parse", "HEAD").stdout
        before_status = self.git(foreign, "status", "--porcelain").stdout
        before_index = (foreign / ".git/index").read_bytes()
        fixture = self.root / "fixture"
        with patch.dict(os.environ, self.foreign_environment(foreign)):
            self.checkout(fixture)
            self.assertEqual(self.git(fixture, "show", "HEAD:src/pkg/kept.py").stdout, "KEPT = 1\n")
        self.assertEqual(self.git(foreign, "rev-parse", "HEAD").stdout, before_head)
        self.assertEqual(self.git(foreign, "status", "--porcelain").stdout, before_status)
        self.assertEqual((foreign / ".git/index").read_bytes(), before_index)
        self.assertEqual((foreign / "untracked.txt").read_text(), "leave untracked\n")

    def test_external_deploy_config_points_app_python_to_installed_venv_and_refuses_symlinks(self):
        repo_root = Path(install.__file__).resolve().parent
        deploy_dir = self.root / "deploy"
        deploy_dir.mkdir()
        for name in ("deployment.example.toml", "secrets.env.example", "requirements.txt"):
            shutil.copyfile(repo_root / name, deploy_dir / name)
        external_dir = self.root / "etc-onionflash"
        external_cfg = external_dir / "deployment.toml"
        real_run = subprocess.run

        def fake_run(arguments, **kwargs):
            if arguments[0] in ("git", "tar"):
                return real_run(arguments, **kwargs)
            return subprocess.CompletedProcess(arguments, 0)

        with (
            patch.object(install, "__file__", str(deploy_dir / "install.py")),
            patch.dict(os.environ, {"ONIONFLASH_DEPLOY_CONFIG": str(external_cfg)}),
            patch("install.shutil.which", return_value="npm"),
            patch("install.subprocess.run", side_effect=fake_run),
        ):
            install.main([str(self.app)])

        parsed = tomllib.loads(external_cfg.read_text(encoding="utf-8"))
        self.assertEqual(parsed["app_python"], str(deploy_dir.resolve() / ".venv/bin/python"))
        self.assertEqual((external_cfg.stat().st_mode & 0o777), 0o600)
        self.assertEqual(((external_dir / "secrets.env").stat().st_mode & 0o777), 0o600)

        # Refuses symlink targets without modifying the pointed-to file:
        victim = self.root / "victim.txt"
        victim.write_text("do not overwrite\n", encoding="utf-8")
        symlink_dir = self.root / "symlink-cfg"
        symlink_dir.mkdir()
        (symlink_dir / "secrets.env").symlink_to(victim)
        with (
            patch.object(install, "__file__", str(deploy_dir / "install.py")),
            patch.dict(os.environ, {"ONIONFLASH_DEPLOY_CONFIG": str(symlink_dir / "deployment.toml")}),
            patch("install.shutil.which", return_value="npm"),
            patch("install.subprocess.run", side_effect=fake_run),
            self.assertRaisesRegex(SystemExit, "symlink"),
        ):
            install.main([str(self.app)])
        self.assertEqual(victim.read_text(encoding="utf-8"), "do not overwrite\n")
