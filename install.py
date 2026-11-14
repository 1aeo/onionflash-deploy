#!/usr/bin/env python3
"""Install or upgrade under the same lock used by scheduled publication."""
import errno
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
from publish import lock


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


def _read_config_nofollow(path: Path) -> dict:
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return {}
    except OSError as error:
        if error.errno == errno.ELOOP or path.is_symlink():
            raise SystemExit(f"Refusing to read symlink config: {path}") from None
        raise
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise SystemExit(f"Refusing to read non-regular config: {path}")
        return tomllib.load(stream)


def _install_template(source: Path, destination: Path, mode: int, transform=None) -> None:
    data = source.read_bytes()
    if transform is not None:
        data = transform(data)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(destination, flags, mode)
    except FileExistsError:
        st = destination.lstat()
        if stat.S_ISLNK(st.st_mode):
            raise SystemExit(f"Refusing to install over symlink: {destination}") from None
        if not stat.S_ISREG(st.st_mode):
            raise SystemExit(f"Refusing to install over non-regular file: {destination}") from None
        return
    except OSError as error:
        if error.errno == errno.ELOOP or destination.is_symlink():
            raise SystemExit(f"Refusing to install over symlink: {destination}") from None
        raise
    with os.fdopen(fd, "wb") as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(data)


def _chmod_nofollow(path: Path, mode: int) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as error:
        if error.errno == errno.ELOOP or path.is_symlink():
            raise SystemExit(f"Refusing to chmod symlink: {path}") from None
        raise
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SystemExit(f"Refusing to chmod non-regular file: {path}")
        os.fchmod(fd, mode)
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
    if config_path.is_symlink():
        raise SystemExit(f"Refusing to read symlink config: {config_path}")
    config_dir = config_path.parent
    config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    cfg = _read_config_nofollow(config_path)
    state = Path(os.path.abspath(config_dir / cfg.get("state_dir", "state")))
    app_cfg_path = Path(os.path.abspath(config_dir / cfg.get("app_config", "onionflash.toml")))
    secrets_path = Path(os.path.abspath(config_dir / cfg.get("env_file", "secrets.env")))
    with lock(state / "run.lock"), tempfile.TemporaryDirectory(prefix="onionflash-") as tree:
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
            return text.replace(default_line, f"app_python = {json.dumps(str(venv_python))}", 1).encode("utf-8")

        _install_template(root / "deployment.example.toml", config_path, 0o600, transform=render_deploy_config)
        _install_template(app / "config.example.toml", app_cfg_path, 0o600)
        _install_template(root / "secrets.env.example", secrets_path, 0o600)
        _chmod_nofollow(secrets_path, 0o600)
    print("Installed. Edit onionflash.toml, deployment.toml, and secrets.env.")
    print("Then: ./deploy.sh check; ./deploy.sh run; ./deploy.sh routes; ./deploy.sh schedule")


if __name__ == "__main__":
    main()
