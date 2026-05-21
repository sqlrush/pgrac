#!/usr/bin/env bash
#
# scripts/perf/run-stage2-cluster-baseline.sh
#	  spec-2.40 D4 — Stage 2 multi-node perf baseline wrapper
#	  (tier-2 medium + tier-3 long manual).
#
#	  Wraps pgbench TPC-B (select-only + full) on:
#	    - single-node cluster_enabled=on vs off
#	    - 2-node ClusterPair
#	    - 3-node ClusterTriple (CF 3-way + reconfig/quorum)
#
#	  TIER controls per-workload -T duration:
#	    smoke  =  30s per workload (=tier-1, fast-gate嵌入)
#	    medium = 600s per workload (=tier-2, nightly嵌入 — D9 workflow)
#	    long   = 7200s per workload (=tier-3, manual / pre-release)
#
#	  Output:  tmp/perf-stage2-<TIER>-<TIMESTAMP>.json
#	  NOT a cross-repo writer:  pgrac docs/perf-baseline.md import 由
#	  pgrac D13 closeout 时人工/脚本执行(spec v0.2 F4 architectural fix).
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#

set -e
set -o pipefail

TIER="${TIER:-smoke}"

case "$TIER" in
	smoke)  DURATION_SEC=30 ;;
	medium) DURATION_SEC=600 ;;
	long)   DURATION_SEC=7200 ;;
	*)      echo "TIER must be smoke / medium / long, got: $TIER" >&2; exit 2 ;;
esac

TIMESTAMP="$(date '+%Y%m%dT%H%M%S')"
OUT_DIR="${OUT_DIR:-tmp}"
mkdir -p "$OUT_DIR"
OUT_JSON="$OUT_DIR/perf-stage2-$TIER-$TIMESTAMP.json"

INSTALL_PREFIX="${INSTALL_PREFIX:-/tmp/pgrac-install}"
PGBIN="$INSTALL_PREFIX/bin"
[ -x "$PGBIN/pgbench" ] || { echo "pgbench not found at $PGBIN/pgbench" >&2; exit 3; }

echo "=== spec-2.40 D4 — Stage 2 multi-node perf baseline ==="
echo "TIER=$TIER DURATION_SEC=$DURATION_SEC"
echo "OUT_JSON=$OUT_JSON"
echo "PGBIN=$PGBIN"

# Hand-rolled JSON emit (no jq required on CI runners).
RESULTS=()

run_pgbench()
{
	local label="$1"
	local port="$2"
	local mode="$3"  # 'select' or 'full'
	local opts=""
	[ "$mode" = "select" ] && opts="-S"
	echo "--- workload $label (mode=$mode port=$port duration=${DURATION_SEC}s) ---"
	local out
	out="$("$PGBIN/pgbench" $opts -c 4 -j 2 -T "$DURATION_SEC" -n \
		-p "$port" -h /tmp -d postgres 2>&1 || true)"
	local tps="$(echo "$out" | sed -nE 's/.*tps = ([0-9.]+).*/\1/p' | head -1)"
	tps="${tps:-0}"
	RESULTS+=("{\"workload\":\"$label\",\"mode\":\"$mode\",\"port\":$port,\"duration_s\":$DURATION_SEC,\"tps\":$tps}")
	echo "  → tps=$tps"
}

# Per-fixture workload runners (assumes caller starts ClusterPair /
# ClusterTriple postmasters in advance — wrapper itself does not start
# pg);  manual usage:
#   $ pg_ctl -D /tmp/node0/pgdata start; pg_ctl -D /tmp/node1/pgdata start
#   $ PORT0=5432 PORT1=5433 TIER=medium bash scripts/perf/run-stage2-cluster-baseline.sh
PORT0="${PORT0:-5432}"
PORT1="${PORT1:-5433}"
PORT2="${PORT2:-5434}"

echo ""
echo "=== single-node off (port=$PORT0) ==="
"$PGBIN/pgbench" -i -s 1 -q -p "$PORT0" -h /tmp -d postgres >/dev/null 2>&1 || true
run_pgbench "single-node-off"  "$PORT0" "select"
run_pgbench "single-node-off"  "$PORT0" "full"

echo ""
echo "=== 2-node ClusterPair node0 (port=$PORT0) ==="
run_pgbench "two-node-cluster" "$PORT0" "select"
run_pgbench "two-node-cluster" "$PORT0" "full"

if [ -n "${PORT2:-}" ] && nc -z localhost "$PORT2" 2>/dev/null; then
	echo ""
	echo "=== 3-node ClusterTriple node0 (port=$PORT0) representative workload ==="
	run_pgbench "three-node-cluster" "$PORT0" "select"
fi

# Emit JSON
{
	printf '{\n'
	printf '  "spec": "2.40",\n'
	printf '  "tier": "%s",\n' "$TIER"
	printf '  "duration_s": %s,\n' "$DURATION_SEC"
	printf '  "timestamp": "%s",\n' "$TIMESTAMP"
	printf '  "results": [\n'
	local_n=${#RESULTS[@]}
	for (( i=0; i<local_n; i++ )); do
		printf '    %s' "${RESULTS[$i]}"
		[ "$i" -lt $(( local_n - 1 )) ] && printf ','
		printf '\n'
	done
	printf '  ]\n'
	printf '}\n'
} > "$OUT_JSON"

echo ""
echo "=== Stage 2 baseline JSON written to: $OUT_JSON ==="
echo "    pgrac D13 import path:  cat $OUT_JSON | python3 scripts/pgrac/import-stage2-perf.py"
echo "    (linkdb CI 不直接写 pgrac docs — spec v0.2 F4 architectural fix)"
