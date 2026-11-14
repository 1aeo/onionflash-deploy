"""The installer builds the application from its checked-out commit only."""
from contextlib import redirect_stdout
import io, os, shutil, subprocess, tempfile, tomllib, unittest
from pathlib import Path
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
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        return subprocess.run(
            ["git", "-C", str(checkout), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", *arguments],
            env=env, check=True, capture_output=True, text=True,
        )

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
        git_dir = str(foreign / ".git")
        return {
            "GIT_DIR": git_dir, "GIT_WORK_TREE": str(foreign), "GIT_COMMON_DIR": git_dir,
            "GIT_INDEX_FILE": f"{git_dir}/index", "GIT_OBJECT_DIRECTORY": f"{git_dir}/objects",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES": f"{git_dir}/objects",
        }

    def test_pip_installs_only_committed_files_and_validates_arguments(self):
        with self.assertRaisesRegex(SystemExit, "Usage"):
            install.main([])
        for path in ("build/lib/pkg/cached.py", "src/pkg.egg-info/SOURCES.txt", "src/pkg/untracked.py"):
            (self.app / path).parent.mkdir(parents=True, exist_ok=True)
            (self.app / path).write_text("not committed\n")
        real_run, installed = subprocess.run, {}
        def fake(arguments, **kwargs):
            if arguments[0] in ("git", "tar"):
                return real_run(arguments, **kwargs)
            if "pip" in arguments:
                installed["tree"] = tree = Path(arguments[-1])
                installed["files"] = sorted(str(p.relative_to(tree)) for p in tree.rglob("*") if p.is_file())
                raise subprocess.CalledProcessError(1, arguments)
            return subprocess.CompletedProcess(arguments, 0)
        with patch.dict(os.environ, {"ONIONFLASH_DEPLOY_CONFIG": str(self.root / "deployment.toml")}), \
                patch("install.shutil.which", return_value="npm"), patch("install.subprocess.run", side_effect=fake), \
                self.assertRaises(subprocess.CalledProcessError):
            install.main([str(self.app)])
        self.assertEqual(installed["files"], [".gitignore", "config.example.toml", "pyproject.toml", "src/pkg/kept.py"])
        self.assertFalse(installed["tree"].exists())

    def test_uncommitted_changes_and_non_git_directories_are_refused(self):
        plain = self.root / "plain"
        plain.mkdir()
        with self.assertRaisesRegex(SystemExit, "not a git repository"):
            install.export(plain, self.root)
        (self.app / "src/pkg/kept.py").write_text("KEPT = 2\n")
        for staged in (False, True):
            if staged:
                self.git(self.app, "add", "src/pkg/kept.py")
            with self.subTest(staged=staged), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
                install.export(self.app, self.root)

    def test_inherited_git_environment_cannot_redirect_export_or_fixture_git_calls(self):
        foreign = self.root / "foreign"
        self.checkout(foreign, "FOREIGN = 1\n")
        (foreign / "untracked.txt").write_text("leave untracked\n")
        before_head = self.git(foreign, "rev-parse", "HEAD").stdout
        before_index = (foreign / ".git/index").read_bytes()
        exported = self.root / "exported"
        exported.mkdir()
        with patch.dict(os.environ, self.foreign_environment(foreign)):
            self.checkout(self.root / "fixture")
            install.export(self.app, exported)
            self.assertEqual((exported / "src/pkg/kept.py").read_text(), "KEPT = 1\n")
            (self.app / "src/pkg/kept.py").write_text("KEPT = 2\n")
            for staged in (False, True):
                if staged:
                    self.git(self.app, "add", "src/pkg/kept.py")
                with self.subTest(staged=staged), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
                    install.export(self.app, exported)
        self.assertEqual(self.git(foreign, "rev-parse", "HEAD").stdout, before_head)
        self.assertEqual((foreign / ".git/index").read_bytes(), before_index)
        self.assertEqual((foreign / "src/pkg/kept.py").read_text(), "FOREIGN = 1\n")
        self.assertEqual((foreign / "untracked.txt").read_text(), "leave untracked\n")

    def test_external_deploy_config_enforces_modes_and_refuses_target_and_parent_symlinks(self):
        repo_root = Path(install.__file__).resolve().parent
        deploy_dir = self.root / "deploy-\U0001f40d"
        deploy_dir.mkdir()
        for name in ("deployment.example.toml", "secrets.env.example", "requirements.txt"):
            shutil.copyfile(repo_root / name, deploy_dir / name)
        external_dir = self.root / "etc-onionflash"
        external_cfg = external_dir / "deployment.toml"
        real_run = subprocess.run
        fake_run = lambda args, **kw: real_run(args, **kw) if args[0] in ("git", "tar") else subprocess.CompletedProcess(args, 0)

        def run_install(cfg_path):
            with patch.object(install, "__file__", str(deploy_dir / "install.py")), \
                    patch.dict(os.environ, {"ONIONFLASH_DEPLOY_CONFIG": str(cfg_path)}), \
                    patch("install.shutil.which", return_value="npm"), patch("install.subprocess.run", side_effect=fake_run), \
                    redirect_stdout(io.StringIO()):
                install.main([str(self.app)])

        run_install(external_cfg)
        parsed = tomllib.loads(external_cfg.read_text(encoding="utf-8"))
        self.assertEqual(parsed["app_python"], str(deploy_dir.resolve() / ".venv/bin/python"))
        self.assertIn("\U0001f40d", parsed["app_python"])
        self.assertEqual(external_cfg.stat().st_mode & 0o777, 0o600)
        secrets_file = external_dir / "secrets.env"
        secrets_file.chmod(0o644)
        run_install(external_cfg)
        self.assertEqual(secrets_file.stat().st_mode & 0o777, 0o600)

        victim = self.root / "victim.txt"
        victim.write_text("do not overwrite\n", encoding="utf-8")
        victim.chmod(0o644)
        symlink_dir = self.root / "symlink-cfg"
        symlink_dir.mkdir()
        (symlink_dir / "secrets.env").symlink_to(victim)
        real_sub = self.root / "real-sub"
        real_sub.mkdir()
        sub_victim = real_sub / "secrets.env"
        sub_victim.write_text("keep perms\n", encoding="utf-8")
        sub_victim.chmod(0o644)
        parent_link_dir = self.root / "parent-link-cfg"
        parent_link_dir.mkdir()
        (parent_link_dir / "linked").symlink_to(real_sub, target_is_directory=True)
        (parent_link_dir / "deployment.toml").write_text('env_file = "linked/secrets.env"\n', encoding="utf-8")

        for bad_cfg in (symlink_dir / "deployment.toml", parent_link_dir / "deployment.toml"):
            with self.subTest(bad_cfg=bad_cfg.parent.name), self.assertRaisesRegex(SystemExit, "symlink"):
                run_install(bad_cfg)
        with self.assertRaises(NotADirectoryError):
            with install._open_nofollow(victim / "deployment.toml", os.O_RDONLY):
                pass
        self.assertEqual(victim.read_text(encoding="utf-8"), "do not overwrite\n")
        self.assertEqual(victim.stat().st_mode & 0o777, 0o644)
        self.assertEqual(sub_victim.stat().st_mode & 0o777, 0o644)


if __name__ == "__main__":
    unittest.main()
