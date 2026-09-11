#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

if ! command -v java >/dev/null; then
  echo "OpenJDK 17 is required. Install it in Ubuntu before continuing." >&2
  exit 1
fi

python3 -m venv "$root/.venv-wsl"
"$root/.venv-wsl/bin/python" -m pip install -r "$root/requirements.txt"
