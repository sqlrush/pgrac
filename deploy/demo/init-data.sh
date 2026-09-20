#!/usr/bin/env bash
# Author: SqlRush <sqlrush@gmail.com>
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec "$script_dir/pgrac-demo" "$@" init-data
