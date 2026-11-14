#!/usr/bin/env python3
"""Install or upgrade under the same lock used by scheduled publication."""
from contextlib import contextmanager
import errno
import fcntl
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile

if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11+ is required")
import tomllib


def export(app, destination):
    """Extract the checked-out commit alone, so local edits, untracked files and
    build caches in the checkout never reach the installed package."""
    git = ["git", "--no-optional-locks", "-C", str(app)]
    environment = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    try:
        if subprocess.run([*git, "status", "--porcelain", "--untracked-files=no"], capture_output=True, check=True, env=environment).stdout:
            raise SystemExit("The application checkout has uncommitted changes; commit or discard them first")
        archive = subprocess.run([*git, "archive", "--format=tar", "HEAD"], capture_output=True, check=True, env=environment).stdout
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"Git could not read the application checkout {app}: {error.stderr.decode(errors='replace').strip()}") from None
    subprocess.run(["tar", "-x", "-f", "-", "-C", str(destination)], input=archive, check=True)


def _open_parent_nofollow(path: Path, *, create: bool = False) -> tuple[int, str]:
    """Traverse every parent component via dir_fd + O_NOFOLLOW, rejecting symlinked directories."""
    target = Path(os.path.abspath(path))
    if not target.name:
        raise SystemExit(f"Invalid configuration target path: {path}")
    dir_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    nofollow_dir_flags = dir_flags | getattr(os, "O_NOFOLLOW", 0)
    dfd = os.open("/", dir_flags)
    try:
        for depth, part in enumerate(target.parent.parts[1:]):
            try:
                next_fd = os.open(part, nofollow_dir_flags, dir_fd=dfd)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, 0o700, dir_fd=dfd)
                next_fd = os.open(part, nofollow_dir_flags, dir_fd=dfd)
            except OSError as error:
                try:
                    lstat_res = os.stat(part, dir_fd=dfd, follow_symlinks=False)
                except OSError:
                    lstat_res = None
                is_link = error.errno in (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP)) or (
                    lstat_res is not None and stat.S_ISLNK(lstat_res.st_mode)
                )
                if not is_link:
                    raise
                # Allow root-owned POSIX top-level system links (/var, /tmp, /etc -> private/*).
                if (
                    os.name == "posix"
                    and depth == 0
                    and part in ("var", "tmp", "etc")
                    and lstat_res is not None
                    and lstat_res.st_uid == 0
                ):
                    next_fd = os.open(part, dir_flags, dir_fd=dfd)
                else:
                    raise SystemExit(f"Refusing symlink directory component in: {path}") from None
            os.close(dfd)
            dfd = next_fd
        return dfd, target.name
    except BaseException:
        os.close(dfd)
        raise


def _read_config_nofollow(path: Path) -> dict:
    try:
        parent_fd, name = _open_parent_nofollow(path, create=False)
    except FileNotFoundError:
        return {}
    try:
        flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            return {}
        except OSError as error:
            is_link = error.errno in (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP))
            if not is_link:
                try:
                    is_link = stat.S_ISLNK(os.stat(name, dir_fd=parent_fd, follow_symlinks=False).st_mode)
                except OSError:
                    is_link = False
            if is_link:
                raise SystemExit(f"Refusing to read symlink config: {path}") from None
            raise
    finally:
        os.close(parent_fd)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise SystemExit(f"Refusing to read non-regular config: {path}")
        return tomllib.load(stream)


def _install_template(source: Path, destination: Path, mode: int, transform=None) -> None:
    data = source.read_bytes()
    if transform is not None:
        data = transform(data)
    parent_fd, name = _open_parent_nofollow(destination, create=True)
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(name, flags, mode, dir_fd=parent_fd)
        except FileExistsError:
            st = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(st.st_mode):
                raise SystemExit(f"Refusing to install over symlink: {destination}") from None
            if not stat.S_ISREG(st.st_mode):
                raise SystemExit(f"Refusing to install over non-regular file: {destination}") from None
            return
        except OSError as error:
            is_link = error.errno in (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP))
            if not is_link:
                try:
                    is_link = stat.S_ISLNK(os.stat(name, dir_fd=parent_fd, follow_symlinks=False).st_mode)
                except OSError:
                    is_link = False
            if is_link:
                raise SystemExit(f"Refusing to install over symlink: {destination}") from None
            raise
    finally:
        os.close(parent_fd)
    with os.fdopen(fd, "wb") as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(data)


def _chmod_nofollow(path: Path, mode: int) -> None:
    parent_fd, name = _open_parent_nofollow(path, create=False)
    try:
        flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(name, flags, dir_fd=parent_fd)
        except OSError as error:
            is_link = error.errno in (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP))
            if not is_link:
                try:
                    is_link = stat.S_ISLNK(os.stat(name, dir_fd=parent_fd, follow_symlinks=False).st_mode)
                except OSError:
                    is_link = False
            if is_link:
                raise SystemExit(f"Refusing to chmod symlink: {path}") from None
            raise
    finally:
        os.close(parent_fd)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SystemExit(f"Refusing to chmod non-regular file: {path}")
        os.fchmod(fd, mode)
    finally:
        os.close(fd)


@contextmanager
def _lock_nofollow(path: Path):
    parent_fd, name = _open_parent_nofollow(path, create=True)
    try:
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(name, flags, 0o600, dir_fd=parent_fd)
        except OSError as error:
            is_link = error.errno in (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP))
            if not is_link:
                try:
                    is_link = stat.S_ISLNK(os.stat(name, dir_fd=parent_fd, follow_symlinks=False).st_mode)
                except OSError:
                    is_link = False
            if is_link:
                raise SystemExit(f"Refusing to lock symlink: {path}") from None
            raise
    finally:
        os.close(parent_fd)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SystemExit(f"Refusing to lock non-regular file: {path}")
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def main(argv=None):
    os.umask(0o077)
    arguments = sys.argv[1:] if argv is None else list(argv)
    root = Path(__file__).resolve().parent
    if len(arguments) != 1:
        raise SystemExit("Usage: ./install.sh /path/to/onionflash-checkout")
    app = Path(arguments[0]).resolve()
    if not (app / "pyproject.toml").is_file() or not (app / "config.example.toml").is_file():
        raise SystemExit("Usage: ./install.sh /path/to/onionflash-checkout")
    if not shutil.which("npm"):
        raise SystemExit("Node.js and npm are required for the publishing toolchain")
    config_path = Path(os.path.abspath(os.environ.get("ONIONFLASH_DEPLOY_CONFIG", root / "deployment.toml")))
    config_dir = config_path.parent
    cfg = _read_config_nofollow(config_path)
    parent_fd, _ = _open_parent_nofollow(config_path, create=True)
    os.close(parent_fd)
    state = Path(os.path.abspath(config_dir / cfg.get("state_dir", "state")))
    state_lock = state / "run.lock"
    app_cfg_path = Path(os.path.abspath(config_dir / cfg.get("app_config", "onionflash.toml")))
    secrets_path = Path(os.path.abspath(config_dir / cfg.get("env_file", "secrets.env")))
    with _lock_nofollow(state_lock), tempfile.TemporaryDirectory(prefix="onionflash-") as tree:
        export(app, tree)
        venv = root / ".venv"
        venv_python = venv / "bin/python"
        if not venv.is_dir():
            subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        subprocess.run([str(venv_python), "-m", "pip", "install", "--disable-pip-version-check", "-r", str(root / "requirements.txt"), tree], check=True)
        subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=root, check=True)

        def render_deploy_config(raw: bytes) -> bytes:
            if config_dir.resolve() == root.resolve():
                return raw
            text = raw.decode("utf-8")
            default_line = 'app_python = ".venv/bin/python"'
            if default_line not in text:
                raise ValueError("deployment.example.toml is missing default app_python setting")
            toml_python = json.dumps(str(venv_python), ensure_ascii=False).replace("\x7f", "\\u007f")
            return text.replace(default_line, f"app_python = {toml_python}", 1).encode("utf-8")

        _install_template(root / "deployment.example.toml", config_path, 0o600, transform=render_deploy_config)
        _install_template(app / "config.example.toml", app_cfg_path, 0o600)
        _install_template(root / "secrets.env.example", secrets_path, 0o600)
        _chmod_nofollow(secrets_path, 0o600)
    print("Installed. Edit onionflash.toml, deployment.toml, and secrets.env.")
    print("Then: ./deploy.sh check; ./deploy.sh run; ./deploy.sh routes; ./deploy.sh schedule")


if __name__ == "__main__":
    main()
