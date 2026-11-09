#!/usr/bin/env bash
set -euo pipefail
umask 077
DEPLOY_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec "$DEPLOY_DIR/.venv/bin/python" "$DEPLOY_DIR/publish.py" --config "${ONIONFLASH_DEPLOY_CONFIG:-$DEPLOY_DIR/deployment.toml}" "$@"
