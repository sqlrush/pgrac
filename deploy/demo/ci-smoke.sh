#!/usr/bin/env bash
# Real native four-Pod lifecycle rehearsal. Author: SqlRush <sqlrush@gmail.com>
set -euo pipefail
demo_dir=$(cd -- "$(dirname -- "$0")" && pwd)
demo_image=${1:?provide the locally built image reference}
demo_name=${2:-ci-demo}
demo=(sudo "$demo_dir/pgrac-demo" --name "$demo_name" --image "$demo_image")
python3 -m unittest discover -s "$demo_dir/tests" -v
"${demo[@]}" check
"${demo[@]}" up
"${demo[@]}" up
"${demo[@]}" init-data --rows 10000
if "${demo[@]}" init-data --rows 10000; then
    echo 'ERROR: repeated initialization unexpectedly accepted' >&2
    exit 1
fi
"${demo[@]}" verify
"${demo[@]}" bench --clients 2 --seconds 30
"${demo[@]}" stop
"${demo[@]}" up
"${demo[@]}" verify
"${demo[@]}" bench --clients 2 --seconds 30
"${demo[@]}" stop
"${demo[@]}" stop
"${demo[@]}" status
echo 'Native four-Pod lifecycle rehearsal PASS (not formal PRE).'
