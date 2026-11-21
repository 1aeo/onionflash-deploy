#!/usr/bin/env bash
set -euo pipefail
DEPLOY_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PYTHON="${PYTHON:-python3}"
if (($#)); then
    exec "$PYTHON" "$DEPLOY_DIR/install.py" "$@"
else
    exec "$PYTHON" "$DEPLOY_DIR/install.py" "$DEPLOY_DIR/../onionflash"
fi
