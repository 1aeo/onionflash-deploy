#!/usr/bin/env bash
set -euo pipefail
DEPLOY_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PYTHON="${PYTHON:-python3}"
exec "$PYTHON" "$DEPLOY_DIR/install.py" "${1:-$DEPLOY_DIR/../onionflash}"
