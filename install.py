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
    try:
        if subprocess.run([*git, "status", "--porcelain", "--untracked-files=no"], capture_output=True, check=True).stdout:
            raise SystemExit("The application checkout has uncommitted changes; commit or discard them first")
        archive = subprocess.run([*git, "archive", "--format=tar", "HEAD"], capture_output=True, check=True).stdout
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"Git could not read the application checkout {app}: {error.stderr.decode(errors='replace').strip()}") from None
    subprocess.run(["tar", "-x", "-f", "-", "-C", str(destination)], input=archive, check=True)


def main():
    os.umask(0o077)
    root = Path(__file__).resolve().parent
    app = Path(sys.argv[1]).resolve()
    if not (app / "pyproject.toml").is_file() or not (app / "config.example.toml").is_file():
        raise SystemExit("Usage: ./install.sh /path/to/onionflash-checkout")
    if not shutil.which("npm"):
        raise SystemExit("Node.js and npm are required for the publishing toolchain")
    config_path = Path(os.environ.get("ONIONFLASH_DEPLOY_CONFIG", root / "deployment.toml")).resolve()
    cfg = tomllib.loads(config_path.read_text()) if config_path.exists() else {}
    state = (config_path.parent / cfg.get("state_dir", "state")).resolve()
    with lock(state / "run.lock"), tempfile.TemporaryDirectory(prefix="onionflash-") as tree:
        export(app, tree)
        venv = root / ".venv"
        if not venv.is_dir():
            subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        subprocess.run([str(venv / "bin/python"), "-m", "pip", "install", "--disable-pip-version-check", "-r", str(root / "requirements.txt"), tree], check=True)
        subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=root, check=True)
        for source, destination in [(root / "deployment.example.toml", config_path),
                                    (app / "config.example.toml", root / "onionflash.toml"),
                                    (root / "secrets.env.example", root / "secrets.env")]:
            if not destination.exists():
                shutil.copyfile(source, destination)
        os.chmod(root / "secrets.env", 0o600)
    print("Installed. Edit onionflash.toml, deployment.toml, and secrets.env.")
    print("Then: ./deploy.sh check; ./deploy.sh run; ./deploy.sh routes; ./deploy.sh schedule")


if __name__ == "__main__":
    main()
