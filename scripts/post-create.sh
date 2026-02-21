#!/usr/bin/env bash
set -euo pipefail

echo "[post-create] Installing dev requirements..."

python -m pip install -r requirements-dev.txt

# If this repo is a Python package, install it in editable mode
if [ -f pyproject.toml ] || [ -f setup.py ] || [ -f setup.cfg ]; then
  echo "[post-create] Installing trading-runtime in editable mode..."
  python -m pip install -e .[dev]
fi

echo "[post-create] Running import-linter..."
lint-imports || true

echo "[post-create] Dev container setup completed."
