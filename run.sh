#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "$0")" && pwd)
VENV_DIR="$PROJECT_ROOT/.venv"
PYTHON_BIN="python3"

usage() {
  echo "Usage: $0 -c <config.yaml> [--recreate-venv]" >&2
  exit 1
}

CONFIG=""
RECREATE_VENV=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      CONFIG="$2"; shift 2 ;;
    --recreate-venv)
      RECREATE_VENV=true; shift ;;
    -h|--help)
      usage ;;
    *)
      echo "Unknown argument: $1" >&2
      usage ;;
  esac
done

if [[ -z "$CONFIG" ]]; then
  echo "Error: config file is required (-c)." >&2
  usage
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "Error: config file not found: $CONFIG" >&2
  exit 2
fi

if [[ "$RECREATE_VENV" == true && -d "$VENV_DIR" ]]; then
  rm -rf "$VENV_DIR"
fi

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtual environment at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip >/dev/null
python -m pip install pyatlan python-dotenv pyyaml >/dev/null

exec python "$PROJECT_ROOT/create_lineage_interactive.py" --config "$CONFIG"


