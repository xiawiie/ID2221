#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="$root/src"
export SPARK_LOCAL_IP=127.0.0.1
cd "$root"
exec "$root/.venv-wsl/bin/python" -m urban_data "$@"
