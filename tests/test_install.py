"""The installer builds the application from its checked-out commit only."""
from contextlib import redirect_stdout
import io, os, shutil, subprocess, tempfile, tomllib, unittest
from pathlib import Path
from unittest.mock import patch

import install


class InstallTests(unittest.TestCase):
    """Tests for clean Git archive export and symlink-safe configuration installation."""

    def setUp(self):
        """Create a temporary workspace with a clean sample Git application checkout."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.app = self.root / "app"
        self.checkout(self.app)

    def git(self, checkout, *arguments):
        """Run a Git command in checkout with inherited GIT_* variables stripped."""
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        return subprocess.run(
            ["git", "-C", str(checkout), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", *arguments],
            env=env, check=True, capture_output=True, text=True,
        )

    def checkout(self, directory, content="KEPT = 1\n"):
        """Initialize and commit a minimal Python package checkout in directory."""
        (directory / "src/pkg").mkdir(parents=True)
        (directory / "src/pkg/kept.py").write_text(content)
        (directory / ".gitignore").write_text("build/\n*.egg-info/\n")
        for name in ("pyproject.toml", "config.example.toml"):
            (directory / name).touch()
        self.git(directory, "init", "-q")
        self.git(directory, "add", "-A")
        self.git(directory, "commit", "-qm", "test")

    def foreign_environment(self, foreign):
        """Build GIT_* environment overrides pointing at an unrelated repository."""
        git_dir = str(foreign / ".git")
        return {
            "GIT_DIR": git_dir, "GIT_WORK_TREE": str(foreign), "GIT_COMMON_DIR": git_dir,
            "GIT_INDEX_FILE": f"{git_dir}/index", "GIT_OBJECT_DIRECTORY": f"{git_dir}/objects",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES": f"{git_dir}/objects",
        }

    def run_install(self, cfg_path, *, deploy_dir=None, on_pip=None):
        """Run install.main with ONIONFLASH_DEPLOY_CONFIG set and toolchain commands stubbed."""
        real_run = subprocess.run
        def fake_run(args, **kw):
            """Execute git and tar normally while intercepting venv, pip, and npm calls."""
            if args[0] in ("git", "tar"):
                return real_run(args, **kw)
            return on_pip(args) if on_pip and "pip" in args else subprocess.CompletedProcess(args, 0)
        with patch.object(install, "__file__", str((deploy_dir / "install.py") if deploy_dir else install.__file__)), \
                patch.dict(os.environ, {"ONIONFLASH_DEPLOY_CONFIG": str(cfg_path)}), \
                patch("install.shutil.which", return_value="npm"), patch("install.subprocess.run", side_effect=fake_run), \
                redirect_stdout(io.StringIO()):
            install.main([str(self.app)])

    def test_pip_installs_only_committed_files_and_validates_arguments(self):
        """Verify untracked and ignored files are excluded from the temporary pip install tree."""
        with self.assertRaisesRegex(SystemExit, "Usage"):
            install.main([])
        for path in ("build/lib/pkg/cached.py", "src/pkg.egg-info/SOURCES.txt", "src/pkg/untracked.py"):
            (self.app / path).parent.mkdir(parents=True, exist_ok=True)
            (self.app / path).write_text("not committed\n")
        installed = {}
        def on_pip(args):
            """Capture the exported temporary tree contents passed to pip and abort."""
            installed["tree"], installed["files"] = (tree := Path(args[-1])), sorted(str(p.relative_to(tree)) for p in tree.rglob("*") if p.is_file())
            raise subprocess.CalledProcessError(1, args)
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_install(self.root / "deployment.toml", on_pip=on_pip)
        self.assertEqual(installed["files"], [".gitignore", "config.example.toml", "pyproject.toml", "src/pkg/kept.py"])
        self.assertFalse(installed["tree"].exists())

    def _assert_dirty_refused(self, destination):
        (self.app / "src/pkg/kept.py").write_text("KEPT = 2\n")
        for staged in (False, True):
            if staged:
                self.git(self.app, "add", "src/pkg/kept.py")
            with self.subTest(staged=staged), self.assertRaisesRegex(SystemExit, "uncommitted changes"):
                install.export(self.app, destination)

    def test_uncommitted_changes_and_non_git_directories_are_refused(self):
        """Refuse exporting non-Git directories or checkouts with modified or staged files."""
        plain = self.root / "plain"
        plain.mkdir()
        with self.assertRaisesRegex(SystemExit, "not a git repository"):
            install.export(plain, self.root)
        self._assert_dirty_refused(self.root)

    def test_inherited_git_environment_cannot_redirect_export_or_fixture_git_calls(self):
        """Ignore inherited GIT_* variables when exporting the application checkout."""
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
            self._assert_dirty_refused(exported)
        self.assertEqual(self.git(foreign, "rev-parse", "HEAD").stdout, before_head)
        self.assertEqual((foreign / ".git/index").read_bytes(), before_index)
        self.assertEqual((foreign / "src/pkg/kept.py").read_text(), "FOREIGN = 1\n")
        self.assertEqual((foreign / "untracked.txt").read_text(), "leave untracked\n")

    def test_external_deploy_config_enforces_modes_and_refuses_target_and_parent_symlinks(self):
        """Enforce mode 0600 templates, allow symlinked state dirs, and reject symlink targets."""
        repo_root = Path(install.__file__).resolve().parent
        deploy_dir = self.root / "deploy-\U0001f40d"
        deploy_dir.mkdir()
        for name in ("deployment.example.toml", "secrets.env.example", "requirements.txt"):
            shutil.copyfile(repo_root / name, deploy_dir / name)
        external_dir = self.root / "etc-onionflash"
        external_cfg = external_dir / "deployment.toml"
        self.run_install(external_cfg, deploy_dir=deploy_dir)
        parsed = tomllib.loads(external_cfg.read_text(encoding="utf-8"))
        self.assertEqual(parsed["app_python"], str(deploy_dir.resolve() / ".venv/bin/python"))
        self.assertIn("\U0001f40d", parsed["app_python"])
        self.assertEqual(external_cfg.stat().st_mode & 0o777, 0o600)
        secrets_file = external_dir / "secrets.env"
        secrets_file.chmod(0o644)
        moved_state = self.root / "moved-state-disk"
        (external_dir / "state").rename(moved_state)
        (external_dir / "state").symlink_to(moved_state, target_is_directory=True)
        self.run_install(external_cfg, deploy_dir=deploy_dir)
        self.assertEqual(secrets_file.stat().st_mode & 0o777, 0o600)
        self.assertEqual((moved_state / "run.lock").stat().st_mode & 0o777, 0o600)

        victim = self.root / "victim.txt"
        victim.write_text("do not overwrite\n", encoding="utf-8")
        victim.chmod(0o644)
        (moved_state / "run.lock").unlink()
        (moved_state / "run.lock").symlink_to(victim)
        with self.assertRaisesRegex(SystemExit, "symlink"):
            self.run_install(external_cfg, deploy_dir=deploy_dir)
        (external_dir / "state").unlink()
        (external_dir / "state").symlink_to(self.root / "missing-disk", target_is_directory=True)
        with self.assertRaisesRegex(SystemExit, "not an existing directory"):
            self.run_install(external_cfg, deploy_dir=deploy_dir)
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
                self.run_install(bad_cfg, deploy_dir=deploy_dir)
        with self.assertRaises(NotADirectoryError):
            with install._open_nofollow(victim / "deployment.toml", os.O_RDONLY):
                pass
        self.assertEqual(victim.read_text(encoding="utf-8"), "do not overwrite\n")
        self.assertEqual(victim.stat().st_mode & 0o777, 0o644)
        self.assertEqual(sub_victim.stat().st_mode & 0o777, 0o644)

    def test_install_sh_forwards_extra_arguments_and_empty_config_env_falls_back(self):
        """Reject extra CLI arguments in install.sh and treat empty ONIONFLASH_DEPLOY_CONFIG as unset."""
        with self.assertRaisesRegex(SystemExit, "Usage"):
            install.main([str(self.app), "--dry-run"])
        script = Path(install.__file__).resolve().parent / "install.sh"
        extra_arg = subprocess.run(
            ["/bin/bash", str(script), str(self.app), "--dry-run"],
            env={**os.environ, "PYTHON": os.path.abspath(os.sys.executable)},
            capture_output=True, text=True,
        )
        self.assertNotEqual(extra_arg.returncode, 0)
        self.assertIn("Usage:", extra_arg.stderr)

        repo_root = Path(install.__file__).resolve().parent
        deploy_dir = self.root / "empty-env-deploy"
        deploy_dir.mkdir()
        for name in ("deployment.example.toml", "secrets.env.example", "requirements.txt"):
            shutil.copyfile(repo_root / name, deploy_dir / name)
        cwd_dir = self.root / "nested/cwd"
        cwd_dir.mkdir(parents=True)
        prev_cwd = os.getcwd()
        os.chdir(cwd_dir)
        try:
            self.run_install("", deploy_dir=deploy_dir)
        finally:
            os.chdir(prev_cwd)
        self.assertTrue((deploy_dir / "deployment.toml").is_file())
        self.assertEqual((deploy_dir / "deployment.toml").stat().st_mode & 0o777, 0o600)
        self.assertFalse((self.root / "nested/state").exists())

    def test_malformed_deploy_paths_fail_before_locking_and_template_uses_exported_head(self):
        """Reject malformed state_dir/app_config/env_file before locking and copy config from exported HEAD."""
        repo_root = Path(install.__file__).resolve().parent
        deploy_dir = self.root / "validate-deploy"
        deploy_dir.mkdir()
        for name in ("deployment.example.toml", "secrets.env.example", "requirements.txt"):
            shutil.copyfile(repo_root / name, deploy_dir / name)

        for index, (key, raw_value) in enumerate(
            (k, v) for k in ("state_dir", "app_config", "env_file") for v in ("123", '""', '"   "')
        ):
            cfg_dir = self.root / f"bad-cfg-{index}"
            cfg_dir.mkdir()
            cfg_path = cfg_dir / "deployment.toml"
            cfg_path.write_text(f"{key} = {raw_value}\n", encoding="utf-8")
            with self.subTest(key=key, raw_value=raw_value), self.assertRaisesRegex(SystemExit, f"Invalid {key}"):
                self.run_install(cfg_path, deploy_dir=deploy_dir)
            self.assertEqual([p.name for p in cfg_dir.iterdir()], ["deployment.toml"])

        with self.assertRaisesRegex(SystemExit, "Invalid deployment configuration"):
            install._cfg_path_setting([], "state_dir", "state")
        with self.assertRaisesRegex(SystemExit, "Invalid env_file"):
            install._cfg_path_setting({"env_file": "bad\x00name"}, "env_file", "secrets.env")

        (self.app / "config.example.toml").write_text("# committed head template\n", encoding="utf-8")
        self.git(self.app, "add", "config.example.toml")
        self.git(self.app, "commit", "-qm", "commit template")
        real_export = install.export

        def export_then_dirty(app, destination):
            """Export committed HEAD first, then mutate the working copy to detect live-tree reads."""
            real_export(app, destination)
            (Path(app) / "config.example.toml").write_text("# dirty working copy\n", encoding="utf-8")

        clean_dir = self.root / "head-template-cfg"
        with patch("install.export", side_effect=export_then_dirty):
            self.run_install(clean_dir / "deployment.toml", deploy_dir=deploy_dir)
        self.assertEqual((clean_dir / "onionflash.toml").read_text(encoding="utf-8"), "# committed head template\n")


if __name__ == "__main__":
    unittest.main()
