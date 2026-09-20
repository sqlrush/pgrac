#!/usr/bin/env bash
# Real native four-Pod lifecycle rehearsal. Author: SqlRush <sqlrush@gmail.com>
set -euo pipefail
demo_dir=$(cd -- "$(dirname -- "$0")" && pwd)
cd -- "$demo_dir"
demo_image=${1:?provide the locally built image reference}
demo_name=${2:-ci-demo}
demo=(sudo "$demo_dir/pgrac-demo" --name "$demo_name" --image "$demo_image")
check_process_security() {
    for node in 0 1 2 3; do
        sudo podman exec "pgrac-$demo_name-$node-db" setpriv --no-new-privs python3 -c '
import pathlib, re, sys
sys.path.insert(0, "/opt/pgrac/demo")
from demo_lib import security_status
statuses = []
for path in pathlib.Path("/proc").glob("[0-9]*/status"):
    try:
        text = path.read_text()
    except FileNotFoundError:
        continue
    if re.search(r"^Name:\s+postgres$", text, re.M):
        statuses.append(security_status(text, int(re.search(r"^Uid:\s+(\d+)", text, re.M)[1])))
assert statuses, "no actual PostgreSQL process checked"
print("PostgreSQL process security PASS:", len(statuses))'
    done
}
python3 -m unittest discover -s "$demo_dir/tests" -v
"${demo[@]}" check
"${demo[@]}" up
"${demo[@]}" up
check_process_security
"${demo[@]}" init-data --rows 10000
if "${demo[@]}" init-data --rows 10000; then
    echo 'ERROR: repeated initialization unexpectedly accepted' >&2
    exit 1
fi
"${demo[@]}" verify
"${demo[@]}" bench --clients 2 --seconds 30
"${demo[@]}" stop
"${demo[@]}" up
check_process_security
"${demo[@]}" verify
"${demo[@]}" bench --clients 2 --seconds 30
"${demo[@]}" stop
"${demo[@]}" stop
"${demo[@]}" status
echo 'Native four-Pod lifecycle rehearsal PASS (not formal PRE).'
