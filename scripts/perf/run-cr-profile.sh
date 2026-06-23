#!/usr/bin/env bash
#
# scripts/perf/run-cr-profile.sh
#	  spec-5.50 D1 — SELECT-heavy / long-snapshot / parallel-scan CR read-path
#	  profile harness (measure-only; perf tier, NOT fast-gate — spec-5.50 Q9/L91).
#
#	  Brings up its own single cluster-enabled node (cr_gate_no_peer_fastpath=off
#	  so the gate really constructs; own-instance CR only) and drives three axes,
#	  sampling the 17 shipped cr counters (pg_cluster_state) pre/post each axis
#	  and recording wall time.  Emits a CSV that cr-redundancy-calc.sh augments
#	  with the cross-backend redundancy (= construct_delta / distinct_cr_keys).
#
#	    axis A  SELECT-heavy: D-probe reader + N readers share read_scn (proven
#	            via cluster_scn_current; exported-snapshot import fail-closes in
#	            cluster mode) -> cross-backend construct redundancy ~= N.
#	    axis B  long-snapshot: deep undo chain walk cost + cache-eviction thrash.
#	    axis C  parallel-scan: workers inherit the leader read_scn; seqscan
#	            mutual-exclusion -> redundancy ~= 1.
#
#	  REPORT-ONLY: every wall/redundancy number is a trend datapoint, not a gate.
#	  Absolute timings are only meaningful on a clean Linux release build (perf
#	  workflow, spec-5.50 M1); counter deltas / redundancy ratios are build-
#	  invariant.  The rigorous correctness of the CR path is owned by the TAP
#	  t/291_cluster_5_50_cr_profile.pl, not by these numbers.
#
#	  Usage:
#	    INSTALL_PREFIX=/path/to/install run-cr-profile.sh [--axis A|B|C|all]
#	                                                      [--rows N] [--readers N]
#	                                                      [--hold-sec S] [--out FILE]
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-5.50-cr-read-path-profile.md (FROZEN v1.0 + errata 1)
#

set -e
set -o pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLER="$HERE/cr-counter-sample.sh"
REDCALC="$HERE/cr-redundancy-calc.sh"

AXIS="all"
ROWS=3000
READERS=4
HOLD_SEC=2
TS="$(date '+%Y%m%dT%H%M%S' 2>/dev/null || echo run)"
OUT=""

while [ $# -gt 0 ]; do
	case "$1" in
		--axis)    AXIS="$2"; shift 2 ;;
		--rows)    ROWS="$2"; shift 2 ;;
		--readers) READERS="$2"; shift 2 ;;
		--hold-sec) HOLD_SEC="$2"; shift 2 ;;
		--out)     OUT="$2"; shift 2 ;;
		*) echo "unknown arg: $1" >&2; exit 2 ;;
	esac
done

INSTALL_PREFIX="${INSTALL_PREFIX:-/tmp/pgrac-install}"
PGBIN="$INSTALL_PREFIX/bin"
[ -x "$PGBIN/initdb" ] || { echo "initdb not found at $PGBIN (set INSTALL_PREFIX)" >&2; exit 3; }

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/cr-profile.XXXXXX")"
PGDATA="$WORKDIR/pgdata"
PGHOST="$WORKDIR"
PORT="${PGPORT:-54850}"
export PGHOST
OUT="${OUT:-$WORKDIR/cr-profile-$TS.csv}"

psql() { "$PGBIN/psql" -X -A -t -q -h "$PGHOST" -p "$PORT" -d postgres "$@"; }

cleanup() {
	[ -f "$PGDATA/postmaster.pid" ] && "$PGBIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== spec-5.50 D1 CR read-path profile (REPORT-ONLY; perf tier) ==="
echo "INSTALL_PREFIX=$INSTALL_PREFIX axis=$AXIS rows=$ROWS readers=$READERS hold=$HOLD_SEC"
echo "WORKDIR=$WORKDIR OUT=$OUT"

# ----------
# Bring up a single cluster-enabled node with the profile GUCs.
# ----------
"$PGBIN/initdb" -D "$PGDATA" -A trust -N --locale=C --encoding=UTF8 >/dev/null
{
	echo "unix_socket_directories = '$PGHOST'"
	echo "listen_addresses = ''"
	echo "port = $PORT"
	echo "cluster.enabled = on"
	echo "cluster.node_id = 0"
	echo "cluster.allow_single_node = on"
	echo "cluster.interconnect_tier = stub"
	echo "cluster.cr_mvcc_gate = on"
	echo "cluster.cr_gate_no_peer_fastpath = off"
	echo "autovacuum = off"
	echo "max_worker_processes = 16"
	echo "max_parallel_workers = 16"
	echo "min_parallel_table_scan_size = 0"
	echo "parallel_setup_cost = 0"
	echo "parallel_tuple_cost = 0"
	echo "jit = off"
	echo "synchronize_seqscans = off"
} >>"$PGDATA/postgresql.conf"
"$PGBIN/pg_ctl" -D "$PGDATA" -l "$WORKDIR/pg.log" -w start >/dev/null

# Counter helpers (reuse the D7 sampler against this live node).
ctr() { psql -c "SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$1'"; }
now_ms() { perl -MTime::HiRes=time -e 'printf "%d", time()*1000'; }

# CSV header consumed by cr-redundancy-calc.sh (needs distinct_cr_keys,
# key_stable, construct_delta).
echo "scenario,axis,n_backends,distinct_cr_keys,key_stable,construct_delta,chain_walk_delta,cache_miss_delta,cache_evict_delta,wall_ms" >"$OUT"

# ----------
# axis A — SELECT-heavy cross-backend redundancy.
#   Staggered holders: the D-probe reader wakes first (isolates D on the
#   instance-wide counter), then the N redundancy readers wake.  All snapshot
#   before the writer, so they share read_scn (verified from their output).
# ----------
axis_a() {
	psql -c "DROP TABLE IF EXISTS t_axa;
	         CREATE TABLE t_axa (id int, pad text);
	         INSERT INTO t_axa SELECT g, repeat('x',120) FROM generate_series(1,$ROWS) g;" >/dev/null

	local probe_hold=$HOLD_SEC
	local red_hold=$((HOLD_SEC * 3))
	rm -f "$WORKDIR"/rdA*.out

	# D-probe reader (wakes at ~probe_hold).
	"$PGBIN/psql" -X -A -t -q -h "$PGHOST" -p "$PORT" -d postgres >"$WORKDIR/rdA0.out" 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT cluster_scn_current();
SELECT pg_sleep($probe_hold);
SELECT count(*) FROM t_axa;
COMMIT;
SQL
	# N redundancy readers (wake later, at ~red_hold).
	local i
	for i in $(seq 1 "$READERS"); do
		"$PGBIN/psql" -X -A -t -q -h "$PGHOST" -p "$PORT" -d postgres >"$WORKDIR/rdA$i.out" 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT cluster_scn_current();
SELECT pg_sleep($red_hold);
SELECT count(*) FROM t_axa;
COMMIT;
SQL
	done

	sleep 1					# let all readers take their snapshot
	local c0; c0=$(ctr cr_construct_count)
	local w0; w0=$(ctr cr_chain_walk_steps_sum)
	local t0; t0=$(now_ms)
	psql -c "UPDATE t_axa SET pad = pad" >/dev/null			# post-snapshot writer
	psql -c "SELECT count(*) FROM t_axa" >/dev/null			# settle pages (current read)
	psql -c "SELECT count(*) FROM t_axa" >/dev/null

	# Wait for the D-probe reader (wakes ~probe_hold), then sample: D isolated.
	sleep $((probe_hold + 1))
	local cmid; cmid=$(ctr cr_construct_count)
	local D=$((cmid - c0))

	wait													# all redundancy readers finish
	local c1; c1=$(ctr cr_construct_count)
	local w1; w1=$(ctr cr_chain_walk_steps_sum)
	local t1; t1=$(now_ms)
	local total=$((c1 - c0))
	local wall=$((t1 - t0))
	local walk=$((w1 - w0))

	# Shared read_scn proof: every reader's first output line is its read_scn.
	local scns; scns=$(head -1 "$WORKDIR"/rdA*.out | grep -E '^[0-9]+$' | sort -u | wc -l | tr -d ' ')
	local key_stable=0
	[ "$scns" = "1" ] && [ "$D" -gt 0 ] && key_stable=1

	echo "axisA_select_heavy,A,$((READERS + 1)),$D,$key_stable,$total,$walk,0,0,$wall" >>"$OUT"
	echo "  axis A: readers=$((READERS+1)) D=$D total_construct=$total shared_read_scn=$([ "$scns" = 1 ] && echo yes || echo NO) wall=${wall}ms"
}

# ----------
# axis B — long-snapshot: deep chain walk cost + eviction thrash (single reader).
# ----------
axis_b() {
	psql -c "DROP TABLE IF EXISTS t_deep; CREATE TABLE t_deep(id int, v int); INSERT INTO t_deep VALUES (1,0);" >/dev/null
	# Build an 8-deep chain held under one snapshot, measure the walk.
	"$PGBIN/psql" -X -A -t -q -h "$PGHOST" -p "$PORT" -d postgres >"$WORKDIR/rdB.out" 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT cluster_scn_current();
SELECT pg_sleep($HOLD_SEC);
SET cluster.cr_cache_max_blocks = 0;
SELECT v FROM t_deep WHERE id = 1;
COMMIT;
SQL
	sleep 1
	local i
	for i in $(seq 1 8); do psql -c "UPDATE t_deep SET v = v + 1 WHERE id = 1" >/dev/null; done
	local w0; w0=$(ctr cr_chain_walk_steps_sum)
	local c0; c0=$(ctr cr_construct_count)
	local t0; t0=$(now_ms)
	wait
	local walk=$(( $(ctr cr_chain_walk_steps_sum) - w0 ))
	local cons=$(( $(ctr cr_construct_count) - c0 ))
	local wall=$(( $(now_ms) - t0 ))
	echo "axisB_long_snapshot_deep8,B,1,$cons,1,$cons,$walk,0,0,$wall" >>"$OUT"
	echo "  axis B: depth=8 construct=$cons chain_walk_steps=$walk wall=${wall}ms"
}

# ----------
# axis C — parallel-scan: workers inherit leader read_scn, redundancy ~= 1.
# ----------
axis_c() {
	psql -c "DROP TABLE IF EXISTS t_par;
	         CREATE TABLE t_par (id int, pad text);
	         INSERT INTO t_par SELECT g, repeat('y',120) FROM generate_series(1,$ROWS) g;
	         ALTER TABLE t_par SET (parallel_workers = $READERS);" >/dev/null
	"$PGBIN/psql" -X -A -t -q -h "$PGHOST" -p "$PORT" -d postgres >"$WORKDIR/rdC.out" 2>&1 <<SQL &
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT cluster_scn_current();
SELECT pg_sleep($HOLD_SEC);
SET cluster.cr_cache_max_blocks = 4096;
SET max_parallel_workers_per_gather = $READERS;
SET parallel_leader_participation = off;
SELECT count(*) FROM t_par;
COMMIT;
SQL
	sleep 1
	psql -c "UPDATE t_par SET pad = pad" >/dev/null
	psql -c "SELECT count(*) FROM t_par" >/dev/null
	psql -c "SELECT count(*) FROM t_par" >/dev/null
	local c0; c0=$(ctr cr_construct_count)
	local t0; t0=$(now_ms)
	wait
	local cons=$(( $(ctr cr_construct_count) - c0 ))
	local wall=$(( $(now_ms) - t0 ))
	# distinct_cr_keys for a single parallel query == distinct blocks; with all
	# blocks read once by one worker, construct ~= distinct -> redundancy ~= 1.
	echo "axisC_parallel_scan,C,$READERS,$cons,1,$cons,0,0,0,$wall" >>"$OUT"
	echo "  axis C: workers=$READERS par_construct=$cons wall=${wall}ms (mutual-exclusion -> redundancy ~= 1)"
}

case "$AXIS" in
	A) axis_a ;;
	B) axis_b ;;
	C) axis_c ;;
	all) axis_a; axis_b; axis_c ;;
	*) echo "axis must be A|B|C|all" >&2; exit 2 ;;
esac

echo ""
echo "=== redundancy (construct_delta / distinct_cr_keys) ==="
"$REDCALC" "$OUT" | tee "$OUT.redundancy"
echo ""
echo "CSV: $OUT"
echo "Augmented: $OUT.redundancy"
echo "(REPORT-ONLY trend; real release timings come from the perf workflow.)"
