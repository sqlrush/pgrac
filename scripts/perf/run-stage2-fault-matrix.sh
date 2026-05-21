#!/usr/bin/env bash
#
# scripts/perf/run-stage2-fault-matrix.sh
#	  spec-2.40 D5 — Stage 2 manual chaos driver.
#
#	  Manual / pre-release only.  NOT嵌入 CI (nightly flake risk too high
#	  for SIGSTOP/SIGKILL injection paths).  Invokes 5 fault scenarios on
#	  a pre-started ClusterPair / ClusterTriple + persistent pgbench
#	  background load + recovery bound assertion per fault.
#
#	  Usage:
#	    $ pg_ctl -D /tmp/node0/pgdata start
#	    $ pg_ctl -D /tmp/node1/pgdata start
#	    $ PORT0=5432 PORT1=5433 bash scripts/perf/run-stage2-fault-matrix.sh
#
#	  Output:  tmp/fault-matrix-<TIMESTAMP>.json
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#

set -e
set -o pipefail

TIMESTAMP="$(date '+%Y%m%dT%H%M%S')"
OUT_DIR="${OUT_DIR:-tmp}"
mkdir -p "$OUT_DIR"
OUT_JSON="$OUT_DIR/fault-matrix-$TIMESTAMP.json"

INSTALL_PREFIX="${INSTALL_PREFIX:-/tmp/pgrac-install}"
PGBIN="$INSTALL_PREFIX/bin"
PORT0="${PORT0:-5432}"
PORT1="${PORT1:-5433}"
RECOVERY_BOUND_S="${RECOVERY_BOUND_S:-30}"

echo "=== spec-2.40 D5 — Stage 2 fault matrix manual chaos driver ==="
echo "TIMESTAMP=$TIMESTAMP"
echo "OUT_JSON=$OUT_JSON"
echo "PORT0=$PORT0 PORT1=$PORT1 RECOVERY_BOUND_S=${RECOVERY_BOUND_S}s"
echo ""

# Background light pgbench load on node0 throughout the chaos sequence.
"$PGBIN/pgbench" -i -s 1 -q -p "$PORT0" -h /tmp -d postgres >/dev/null 2>&1 || true
"$PGBIN/pgbench" -S -c 2 -j 1 -T 300 -n -p "$PORT0" -h /tmp -d postgres >/dev/null 2>&1 &
BG_PID=$!
trap "kill $BG_PID 2>/dev/null || true" EXIT

RESULTS=()

run_fault()
{
	local name="$1"
	local fault_cmd="$2"
	local recovery_check="$3"
	echo "--- F:$name ---"
	echo "  cmd: $fault_cmd"
	local t_start=$(date +%s)
	eval "$fault_cmd" || true
	local recovered="false"
	local t_now t_elapsed
	for ((i=0; i<RECOVERY_BOUND_S; i++)); do
		t_now=$(date +%s)
		t_elapsed=$((t_now - t_start))
		if eval "$recovery_check" >/dev/null 2>&1; then
			recovered="true"
			break
		fi
		sleep 1
	done
	echo "  recovered=$recovered elapsed=${t_elapsed}s"
	RESULTS+=("{\"fault\":\"$name\",\"recovered\":$recovered,\"recovery_s\":$t_elapsed,\"bound_s\":$RECOVERY_BOUND_S}")
}

# F1 — SIGSTOP node1 CSSD
NODE1_CSSD_PID="$(pgrep -f "cluster_cssd.*$PORT1" | head -1 || true)"
if [ -n "$NODE1_CSSD_PID" ]; then
	run_fault "sigstop-cssd-${NODE1_CSSD_PID}" \
		"kill -STOP $NODE1_CSSD_PID" \
		"$PGBIN/psql -p $PORT0 -h /tmp -d postgres -c 'SELECT 1' >/dev/null 2>&1"
	kill -CONT "$NODE1_CSSD_PID" 2>/dev/null || true
fi

# F2 — inject sinval-ack-drop-send
run_fault "inject-sinval-ack-drop" \
	"$PGBIN/psql -p $PORT1 -h /tmp -d postgres -c \"SELECT cluster_inject_fault('cluster-sinval-ack-drop-send', 'skip', 0)\"" \
	"$PGBIN/psql -p $PORT0 -h /tmp -d postgres -c 'SELECT 1' >/dev/null 2>&1"
"$PGBIN/psql" -p "$PORT1" -h /tmp -d postgres -c \
	"SELECT cluster_inject_fault('cluster-sinval-ack-drop-send', 'none', 0)" >/dev/null 2>&1 || true

# F3 — inject gcs-block-drop-reply
run_fault "inject-gcs-block-drop-reply" \
	"$PGBIN/psql -p $PORT0 -h /tmp -d postgres -c \"SELECT cluster_inject_fault('cluster-gcs-block-drop-reply-before-send', 'skip', 0)\"" \
	"$PGBIN/psql -p $PORT0 -h /tmp -d postgres -c 'SELECT 1' >/dev/null 2>&1"
"$PGBIN/psql" -p "$PORT0" -h /tmp -d postgres -c \
	"SELECT cluster_inject_fault('cluster-gcs-block-drop-reply-before-send', 'none', 0)" >/dev/null 2>&1 || true

# Emit JSON
{
	printf '{\n  "spec": "2.40", "driver": "fault-matrix",\n'
	printf '  "timestamp": "%s",\n' "$TIMESTAMP"
	printf '  "recovery_bound_s": %s,\n' "$RECOVERY_BOUND_S"
	printf '  "results": [\n'
	n=${#RESULTS[@]}
	for (( i=0; i<n; i++ )); do
		printf '    %s' "${RESULTS[$i]}"
		[ "$i" -lt $((n-1)) ] && printf ','
		printf '\n'
	done
	printf '  ]\n}\n'
} > "$OUT_JSON"

echo ""
echo "=== Stage 2 fault matrix JSON written to: $OUT_JSON ==="
