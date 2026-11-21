#!/usr/bin/env python3
"""Install or upgrade under the same lock used by scheduled publication."""
from contextlib import contextmanager
import errno, fcntl, json, os, shutil, stat, subprocess, sys, tempfile
from pathlib import Path

if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11+ is required")
import tomllib

NOFOLLOW, NONBLOCK, DIRECTORY = getattr(os, "O_NOFOLLOW", 0), getattr(os, "O_NONBLOCK", 0), getattr(os, "O_DIRECTORY", 0)
LINK_ERRNOS = (errno.ELOOP, getattr(errno, "EMLINK", errno.ELOOP))


def export(app, destination):
    """Extract only the checked-out commit so untracked files and caches never install."""
    git = ["git", "--no-optional-locks", "-C", str(app)]
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    try:
        if subprocess.run([*git, "status", "--porcelain", "--untracked-files=no"], capture_output=True, check=True, env=env).stdout:
            raise SystemExit("The application checkout has uncommitted changes; commit or discard them first")
        archive = subprocess.run([*git, "archive", "--format=tar", "HEAD"], capture_output=True, check=True, env=env).stdout
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"Git could not read the application checkout {app}: {error.stderr.decode(errors='replace').strip()}") from None
    subprocess.run(["tar", "-x", "-f", "-", "-C", str(destination)], input=archive, check=True)


def _is_link(error: OSError, name: str, dir_fd: int) -> tuple[bool, os.stat_result | None]:
    try:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except OSError:
        st = None
    return error.errno in LINK_ERRNOS or bool(st and stat.S_ISLNK(st.st_mode)), st


def _open_parent_nofollow(target: Path, *, create: bool = False) -> int:
    dir_flags = os.O_RDONLY | DIRECTORY
    dfd = os.open("/", dir_flags)
    try:
        for depth, part in enumerate(target.parent.parts[1:]):
            try:
                next_fd = os.open(part, dir_flags | NOFOLLOW, dir_fd=dfd)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, 0o700, dir_fd=dfd)
                next_fd = os.open(part, dir_flags | NOFOLLOW, dir_fd=dfd)
            except OSError as error:
                is_link, st = _is_link(error, part, dfd)
                if not is_link:
                    raise
                if os.name == "posix" and depth == 0 and part in ("var", "tmp", "etc") and st and st.st_uid == 0:
                    next_fd = os.open(part, dir_flags, dir_fd=dfd)
                else:
                    raise SystemExit(f"Refusing symlink directory component in: {target}") from None
            os.close(dfd)
            dfd = next_fd
        return dfd
    except BaseException:
        os.close(dfd)
        raise


@contextmanager
def _open_nofollow(path: Path, flags: int, *, create_parents: bool = False,
                   missing_ok: bool = False, allow_symlink_parents: bool = False):
    target = Path(os.path.abspath(path))
    if not target.name:
        raise SystemExit(f"Invalid configuration target path: {path}")
    fd = parent_fd = None
    try:
        if allow_symlink_parents:
            if create_parents:
                try:
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                except FileExistsError:
                    raise SystemExit(f"Parent path is not an existing directory: {target.parent}") from None
            parent_fd = os.open(target.parent, os.O_RDONLY | DIRECTORY)
        else:
            parent_fd = _open_parent_nofollow(target, create=create_parents)
        fd = os.open(target.name, flags | NOFOLLOW, 0o600, dir_fd=parent_fd)
    except FileNotFoundError:
        if not missing_ok:
            raise
    except OSError as error:
        if parent_fd is not None and not isinstance(error, FileExistsError) and _is_link(error, target.name, parent_fd)[0]:
            raise SystemExit(f"Refusing symlink target: {path}") from None
        raise
    finally:
        if parent_fd is not None:
            os.close(parent_fd)
    if fd is None:
        yield None
        return
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SystemExit(f"Refusing non-regular file: {path}")
        yield fd
    finally:
        os.close(fd)


def _cfg_path_setting(cfg: object, key: str, default: str) -> str:
    if not isinstance(cfg, dict):
        raise SystemExit("Invalid deployment configuration")
    value = cfg.get(key, default)
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise SystemExit(f"Invalid {key} in deployment.toml: must be a non-empty string")
    return value


def _install_template(data: bytes, destination: Path, *, enforce_mode: bool = False) -> None:
    try:
        with _open_nofollow(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, create_parents=True) as fd:
            os.fchmod(fd, 0o600)
            with open(fd, "wb", closefd=False) as stream:
                stream.write(data)
    except FileExistsError:
        with _open_nofollow(destination, os.O_RDONLY | NONBLOCK) as fd:
            if enforce_mode:
                os.fchmod(fd, 0o600)


def main(argv=None):
    """Install the committed application, Node toolchain, and mode-0600 config templates under run.lock."""
    os.umask(0o077)
    arguments = sys.argv[1:] if argv is None else list(argv)
    root = Path(__file__).resolve().parent
    app = Path(arguments[0]).resolve() if len(arguments) == 1 else None
    if app is None or not (app / "pyproject.toml").is_file() or not (app / "config.example.toml").is_file():
        raise SystemExit("Usage: ./install.sh /path/to/onionflash-checkout")
    if not shutil.which("npm"):
        raise SystemExit("Node.js and npm are required for the publishing toolchain")
    config_path = Path(os.path.abspath(os.environ.get("ONIONFLASH_DEPLOY_CONFIG") or root / "deployment.toml"))
    config_dir = config_path.parent
    cfg = {}
    with _open_nofollow(config_path, os.O_RDONLY | NONBLOCK, missing_ok=True) as fd:
        if fd is not None:
            with open(fd, "rb", closefd=False) as stream:
                cfg = tomllib.load(stream)
    state = config_dir / _cfg_path_setting(cfg, "state_dir", "state")
    app_cfg_name = _cfg_path_setting(cfg, "app_config", "onionflash.toml")
    env_file_name = _cfg_path_setting(cfg, "env_file", "secrets.env")
    venv, venv_python = root / ".venv", root / ".venv/bin/python"
    with _open_nofollow(state / "run.lock", os.O_RDWR | os.O_CREAT, create_parents=True, allow_symlink_parents=True) as lock_fd, tempfile.TemporaryDirectory(prefix="onionflash-") as tree:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            export(app, tree)
            exported_config = Path(tree) / "config.example.toml"
            if not exported_config.is_file():
                raise SystemExit("The application commit is missing config.example.toml")
            if not venv.is_dir():
                subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
            subprocess.run([str(venv_python), "-m", "pip", "install", "--disable-pip-version-check", "-r", str(root / "requirements.txt"), tree], check=True)
            subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=root, check=True)
            deploy_raw = (root / "deployment.example.toml").read_bytes()
            if config_dir.resolve() != root.resolve():
                default_line, text = 'app_python = ".venv/bin/python"', deploy_raw.decode("utf-8")
                if default_line not in text:
                    raise ValueError("deployment.example.toml is missing default app_python setting")
                toml_python = json.dumps(str(venv_python), ensure_ascii=False).replace("\x7f", "\\u007f")
                deploy_raw = text.replace(default_line, f"app_python = {toml_python}", 1).encode("utf-8")
            _install_template(deploy_raw, config_path)
            _install_template(exported_config.read_bytes(), config_dir / app_cfg_name)
            _install_template((root / "secrets.env.example").read_bytes(), config_dir / env_file_name, enforce_mode=True)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
    print("Installed. Edit onionflash.toml, deployment.toml, and secrets.env.\nThen: ./deploy.sh check; ./deploy.sh run; ./deploy.sh routes; ./deploy.sh schedule")


if __name__ == "__main__":
    main()
