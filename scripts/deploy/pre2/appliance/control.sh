#!/bin/bash
# Host-side entry point shipped beside the self-contained ARM64 disk image.
set -euo pipefail
BUNDLE_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
LIMACTL=${PGRAC_LIMACTL:-/opt/homebrew/bin/limactl}
PGRAC_VM=${PGRAC_VM:-pgrac-pre2-demo}
case "$PGRAC_VM" in
  pgrac-pre2-demo|pgrac-pre2-demo-[a-zA-Z0-9_-]*) ;;
  *) echo 'Instance name must be pgrac-pre2-demo or pgrac-pre2-demo-<suffix>' >&2; exit 2 ;;
esac
if [ ! -x "$LIMACTL" ]; then
  echo 'Install Lima first, or set PGRAC_LIMACTL to its absolute executable path.' >&2
  exit 2
fi
ACTION=${1:-start}
case "$ACTION" in
 start)
  if ! "$LIMACTL" list --format '{{.Name}}' | /usr/bin/grep -Fxq "$PGRAC_VM"; then
   cd "$BUNDLE_DIR"
   /usr/bin/shasum -a 256 -c IMAGE-SHA256SUMS
   "$LIMACTL" create --tty=false --name="$PGRAC_VM" "$BUNDLE_DIR/pre2-appliance.yaml"
  fi
  "$LIMACTL" start --tty=false "$PGRAC_VM"
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-demo start
  ;;
 test|example)
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-demo example
  ;;
 stop)
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-demo stop --poweroff
  # Runs only after all database/cluster/guest shutdowns have succeeded.
  "$LIMACTL" stop "$PGRAC_VM"
  ;;
 status)
  "$LIMACTL" shell "$PGRAC_VM" -- sudo -n /usr/local/sbin/pgrac-demo status
  ;;
 *) echo 'Usage: ./control.sh start|test|stop|status' >&2; exit 2 ;;
esac
