#!/usr/bin/env python3
"""Install or upgrade under the same lock used by scheduled publication."""
import os
from pathlib import Path
import shutil
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
    cfg = tomllib.loads(config_path.read_text()) if config_path.exists() else {}
    state = Path(os.path.abspath(config_dir / cfg.get("state_dir", "state")))
    app_cfg_path = Path(os.path.abspath(config_dir / cfg.get("app_config", "onionflash.toml")))
    secrets_path = Path(os.path.abspath(config_dir / cfg.get("env_file", "secrets.env")))
    with lock(state / "run.lock"), tempfile.TemporaryDirectory(prefix="onionflash-") as tree:
        export(app, tree)
        venv = root / ".venv"
        if not venv.is_dir():
            subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        subprocess.run([str(venv / "bin/python"), "-m", "pip", "install", "--disable-pip-version-check", "-r", str(root / "requirements.txt"), tree], check=True)
        subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=root, check=True)
        for source, destination in [(root / "deployment.example.toml", config_path),
                                    (app / "config.example.toml", app_cfg_path),
                                    (root / "secrets.env.example", secrets_path)]:
            if destination.is_symlink():
                raise SystemExit(f"Refusing to install over symlink: {destination}")
            if not destination.exists():
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                shutil.copyfile(source, destination)
        if secrets_path.is_symlink():
            raise SystemExit(f"Refusing to chmod symlink: {secrets_path}")
        os.chmod(secrets_path, 0o600)
    print("Installed. Edit onionflash.toml, deployment.toml, and secrets.env.")
    print("Then: ./deploy.sh check; ./deploy.sh run; ./deploy.sh routes; ./deploy.sh schedule")


if __name__ == "__main__":
    main()
