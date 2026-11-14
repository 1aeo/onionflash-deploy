#!/usr/bin/env python3
"""Run offline application commands with the deployment's protected configuration."""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import os
from pathlib import Path
import sys

from onionflash import cli
from publish import BusyLock, Publisher, lock, redact


@contextmanager
def without_credentials(names):
    saved = {name: os.environ.pop(name) for name in names if name in os.environ}
    try:
        yield
    finally:
        for name in names:
            os.environ.pop(name, None)
        os.environ.update(saved)


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("deployment_config", type=Path)
    parser.add_argument("command", choices=(*cli.OPERATOR_ACTIONS, "build", "status"))
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    for argument in args.arguments:
        if argument == "--":
            break
        option = argument.split("=", 1)[0]
        if len(option) > 2 and option.startswith("--") and "--config".startswith(option):
            parser.error("Application configuration comes from deployment.toml; do not pass --config")
    prefix, rest = [args.command], args.arguments
    mutating = args.command == "build"
    if args.command in cli.OPERATOR_ACTIONS:
        if not rest or rest[0] not in cli.OPERATOR_ACTIONS[args.command]:
            parser.error(args.command + " requires an action: " + ", ".join(sorted(cli.OPERATOR_ACTIONS[args.command])))
        prefix.append(rest[0])
        mutating = rest[0] not in cli.READ_ONLY_ACTIONS
        rest = rest[1:]
    publisher = None
    try:
        publisher = Publisher(args.deployment_config, offline=True)
        arguments = [*prefix, "--config", str(publisher.app_config), *rest]
        guard = lock(publisher.state / "run.lock", nonblocking=True) if mutating else nullcontext()
        with guard, without_credentials(publisher.private_names()):
            return cli.main(arguments)
    except BusyLock:
        print("onionflash app: another deployment is running; command not performed; retry after completion", file=sys.stderr)
        return 75
    except Exception as exc:
        message = publisher.clean_error(exc) if publisher else redact(f"{type(exc).__name__}: {exc}")
        print("onionflash app: " + message, file=sys.stderr)
        return 1
    finally:
        if publisher is not None:
            publisher.close()


if __name__ == "__main__":
    sys.exit(main())
