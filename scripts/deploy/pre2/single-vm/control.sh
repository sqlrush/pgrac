#!/bin/bash
set -euo pipefail
BUNDLE_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
LIMACTL=${PGRAC_LIMACTL:-/opt/homebrew/bin/limactl}
PGRAC_VM=${PGRAC_VM:-pgrac-pre2-single}
if [[ ! "$PGRAC_VM" =~ ^pgrac-pre2-single(-[a-zA-Z0-9_-]+)?$ ]]; then
  echo 'Use pgrac-pre2-single or pgrac-pre2-single-<suffix>.' >&2
  exit 2
fi
test -x "$LIMACTL" || { echo 'Install Lima or set PGRAC_LIMACTL.' >&2; exit 2; }
ACTION=${1:-start}
case "$ACTION" in
 start)
  if [ "$(/usr/bin/uname -m)" != arm64 ] || [ "$(/usr/sbin/sysctl -n hw.logicalcpu)" -lt 16 ] || \
     [ "$(/usr/sbin/sysctl -n hw.memsize)" -lt 103079215104 ]; then
    echo 'This image requires an Apple Silicon Mac with at least 16 CPU cores and 96 GiB RAM.' >&2
    exit 2
  fi
  if ! "$LIMACTL" list --format '{{.Name}}' | /usr/bin/grep -Fxq "$PGRAC_VM"; then
    cd "$BUNDLE_DIR"
    /usr/bin/shasum -a 256 -c IMAGE-SHA256SUMS
    "$LIMACTL" create --tty=false --name="$PGRAC_VM" "$BUNDLE_DIR/pre2-single.yaml"
  fi
  "$LIMACTL" start --tty=false "$PGRAC_VM"
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-single start
  ;;
 test|example|status)
  if [ "$ACTION" = test ]; then ACTION=example; fi
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-single "$ACTION"
  ;;
 benchmark)
  SECONDS_PER_POINT=${PGRAC_SECONDS:-60}
  if [[ ! "$SECONDS_PER_POINT" =~ ^[0-9]+$ ]] || [ "$SECONDS_PER_POINT" -lt 1 ] || [ "$SECONDS_PER_POINT" -gt 180 ]; then
    echo 'PGRAC_SECONDS must be between 1 and 180.' >&2; exit 2
  fi
  reply=$("$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-single launch-sweep --seconds "$SECONDS_PER_POINT")
  job=$(printf '%s' "$reply" | /usr/bin/plutil -extract job raw -o - -)
  echo "Benchmark job: $job (continues inside the VM if this terminal disconnects)"
  while true; do
    reply=$("$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-single job-status --job "$job")
    state=$(printf '%s' "$reply" | /usr/bin/plutil -extract status raw -o - -)
    if [ "$state" = DONE ]; then
      printf '%s\n' "$reply" > "benchmark-$job.json"
      printf '%s\n' "$reply"
      rc=$(printf '%s' "$reply" | /usr/bin/plutil -extract rc raw -o - -)
      exit "$rc"
    fi
    sleep 10
  done
  ;;
 stop)
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-single stop
  "$LIMACTL" stop "$PGRAC_VM"
  ;;
 *) echo 'Usage: ./control.sh start|test|status|benchmark|stop' >&2; exit 2 ;;
esac
