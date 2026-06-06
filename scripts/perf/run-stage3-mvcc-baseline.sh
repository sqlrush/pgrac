#!/usr/bin/env bash
#
# scripts/perf/run-stage3-mvcc-baseline.sh
#	  spec-3.17 D3 — Stage 3 MVCC perf baseline wrapper
#	  (tier-2 medium + tier-3 long manual).
#
#	  Wraps pgbench TPC-B (select-only + full) plus an MVCC undo-heavy
#	  UPDATE workload on:
#	    - single-node cluster_enabled=on vs off
#	    - 2-node ClusterPair (trend only)
#
#	  TIER controls per-workload -T duration:
#	    smoke  =  30s per workload (=tier-1)
#	    medium = 600s per workload (=tier-2, nightly trend)
#	    long   = 7200s per workload (=tier-3, manual / pre-release)
#
#	  REPORT-ONLY:  every TPS/ratio here is a trend datapoint, NOT a gate.
#	  The Stage 3 performance bar (single-node MVCC write overhead, 2-node
#	  Cache Fusion decay) is owned by spec-3.18 — this wrapper only collects
#	  the honest baseline.  Correctness is gated by the t/226 capability TAP
#	  and the per-spec e2e t/213-225, not by these numbers.
#
#	  Output:  tmp/perf-stage3-<TIER>-<TIMESTAMP>.json
#	  NOT a cross-repo writer:  pgrac docs/perf-baseline.md import is run
#	  manually at closeout (mirrors spec-2.40 D4 architectural separation).
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
OUT_JSON="$OUT_DIR/perf-stage3-$TIER-$TIMESTAMP.json"

INSTALL_PREFIX="${INSTALL_PREFIX:-/tmp/pgrac-install}"
PGBIN="$INSTALL_PREFIX/bin"
[ -x "$PGBIN/pgbench" ] || { echo "pgbench not found at $PGBIN/pgbench" >&2; exit 3; }

echo "=== spec-3.17 D3 — Stage 3 MVCC perf baseline (REPORT-ONLY; perf bar = spec-3.18) ==="
echo "TIER=$TIER DURATION_SEC=$DURATION_SEC"
echo "OUT_JSON=$OUT_JSON"
echo "PGBIN=$PGBIN"

# An MVCC undo-heavy custom pgbench script:  repeatedly UPDATE the same
# accounts so every transaction emits undo + drives CR / delayed cleanout.
UNDO_SCRIPT="$OUT_DIR/stage3-undo-heavy-$TIMESTAMP.sql"
cat > "$UNDO_SCRIPT" <<'SQL'
\set aid random(1, 100000 * :scale)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = :aid;
UPDATE pgbench_accounts SET abalance = abalance - 1 WHERE aid = :aid;
END;
SQL

# Hand-rolled JSON emit (no jq required on CI runners).
RESULTS=()

run_pgbench()
{
	local label="$1"
	local port="$2"
	local mode="$3"  # 'select' / 'full' / 'undo-heavy'
	local -a opts=()
	[ "$mode" = "select" ] && opts=(-S)
	[ "$mode" = "undo-heavy" ] && opts=(-f "$UNDO_SCRIPT")
	echo "--- workload $label (mode=$mode port=$port duration=${DURATION_SEC}s) ---"
	local out
	out="$("$PGBIN/pgbench" "${opts[@]}" -c 4 -j 2 -T "$DURATION_SEC" -n \
		-p "$port" -h /tmp -d postgres 2>&1)"
	local tps
	tps="$(echo "$out" | sed -nE 's/.*tps = ([0-9.]+).*/\1/p' | head -1)"
	if [ -z "$tps" ]; then
		echo "$out"
		echo "ERROR: pgbench output did not contain a TPS line for workload $label" >&2
		return 1
	fi
	RESULTS+=("{\"workload\":\"$label\",\"mode\":\"$mode\",\"port\":$port,\"duration_s\":$DURATION_SEC,\"tps\":$tps}")
	echo "  → tps=$tps (report-only)"
}

# Manual usage (wrapper does not start postmasters):
#   $ pg_ctl -D /tmp/node0/pgdata start    # cluster.enabled=off baseline
#   $ PORT0=5432 TIER=medium bash scripts/perf/run-stage3-mvcc-baseline.sh
# For the on/2-node legs start the matching cluster fixtures first.
PORT0="${PORT0:-5432}"
PORT1="${PORT1:-5433}"

echo ""
echo "=== single-node (port=$PORT0) — select / full / undo-heavy ==="
"$PGBIN/pgbench" -i -s 1 -q -p "$PORT0" -h /tmp -d postgres >/dev/null
run_pgbench "single-node" "$PORT0" "select"
run_pgbench "single-node" "$PORT0" "full"
run_pgbench "single-node" "$PORT0" "undo-heavy"

if [ -n "${PORT1:-}" ] && nc -z localhost "$PORT1" 2>/dev/null; then
	echo ""
	echo "=== 2-node ClusterPair node0 (port=$PORT0) trend ==="
	run_pgbench "two-node-cluster" "$PORT0" "full"
	run_pgbench "two-node-cluster" "$PORT0" "undo-heavy"
fi

# Emit JSON
{
	printf '{\n'
	printf '  "spec": "3.17",\n'
	printf '  "tier": "%s",\n' "$TIER"
	printf '  "duration_s": %s,\n' "$DURATION_SEC"
	printf '  "timestamp": "%s",\n' "$TIMESTAMP"
	printf '  "gate": "report-only; Stage 3 perf bar owned by spec-3.18",\n'
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

rm -f "$UNDO_SCRIPT"

echo ""
echo "=== Stage 3 MVCC baseline JSON written to: $OUT_JSON ==="
echo "    REPORT-ONLY trend; spec-3.18 owns the Stage 3 performance bar."
echo "    (linkdb CI does not write pgrac docs — manual import at closeout)"
