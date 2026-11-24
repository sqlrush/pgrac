#!/bin/bash
#-------------------------------------------------------------------------
#
# run_bench.sh
#	  Driver for the own-xid verdict resolver micro-benchmark.
#
#	  Creates a scratch single-node instance, makes committed and aborted
#	  xids, restarts cleanly, then for each undo pool size times the scan
#	  and the verdict core warm and cold.  Results are written one JSON
#	  object per line, prefixed with the run coordinates.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/modules/test_pgrac_verdict_bench/run_bench.sh
#
# NOTES
#	  Required environment:
#	    VB_INSTALL    install prefix holding bin/postgres and the module
#	    VB_WORK       scratch directory (data directory and results)
#	  Optional environment (defaults in parentheses):
#	    VB_TAG        run label (run)
#	    VB_PORT       server port (54731)
#	    VB_TIERS      undo pool sizes, ascending ("1 16 64 256")
#	    VB_ROUNDS     warm rounds per size (3)
#	    VB_ITERS      scan/verdict calls per warm round (2000)
#	    VB_COLD       cold single-call cycles per size (20)
#	    VB_PARALLEL   after the last size, also run this many concurrent
#	                  verdict loops and a commit workload beside them (0)
#	    VB_INTERPOSE  path to vb_interpose.dylib; preloads the libc counter
#	    VB_CLEANER    value for cluster.undo_cleaner_enabled (on)
#
#	  The instance needs no writable cluster formation: it never binds a
#	  transaction table slot.  Segment files come from the product's own
#	  file publication code (see test_pgrac_vb_provision).
#
#-------------------------------------------------------------------------
set -euo pipefail

: "${VB_INSTALL:?set VB_INSTALL}"
: "${VB_WORK:?set VB_WORK}"
TAG=${VB_TAG:-run}
PORT=${VB_PORT:-54731}
TIERS=${VB_TIERS:-"1 16 64 256"}
ROUNDS=${VB_ROUNDS:-3}
ITERS=${VB_ITERS:-2000}
COLD=${VB_COLD:-20}
PARALLEL=${VB_PARALLEL:-0}
INTERPOSE=${VB_INTERPOSE:-}
CLEANER=${VB_CLEANER:-on}

BIN=$VB_INSTALL/bin
DATA=$VB_WORK/data-$TAG
SOCK=
OUT=$VB_WORK/results-$TAG.jsonl
LOG=$VB_WORK/server-$TAG.log
PSQL=()

fail() { echo "FAIL: $*" >&2; exit 1; }

server_started=0
cleanup() {
	run_rc=$?
	trap - EXIT
	if [ "$server_started" = 1 ]; then
		"$BIN/pg_ctl" -D "$DATA" -m fast -w stop >> "$LOG" 2>&1 || run_rc=1
	fi
	if [ -n "$SOCK" ]; then
		rmdir "$SOCK" 2>/dev/null || true
	fi
	exit "$run_rc"
}
trap cleanup EXIT

start_server() {
	# The preload variable must reach postgres directly: macOS strips it from
	# the environment of protected system binaries such as nohup.
	if [ -n "$INTERPOSE" ]; then
		(trap '' HUP; DYLD_INSERT_LIBRARIES=$INTERPOSE exec "$BIN/postgres" -D "$DATA") >> "$LOG" 2>&1 &
	else
		(trap '' HUP; exec "$BIN/postgres" -D "$DATA") >> "$LOG" 2>&1 &
	fi
	server_pid=$!
	server_started=1
	for _ in $(seq 1 100); do
		kill -0 "$server_pid" 2>/dev/null || fail "new postgres exited (see $LOG)"
		if "$BIN/pg_isready" -h "$SOCK" -p "$PORT" -U postgres -q; then
			IFS= read -r data_pid < "$DATA/postmaster.pid" || fail "missing postmaster PID"
			[ "$data_pid" = "$server_pid" ] || fail "postmaster PID mismatch"
			reported_data=$("${PSQL[@]}" -c "SHOW data_directory") || fail "server identity"
			[ "$(cd "$reported_data" && pwd -P)" = "$(cd "$DATA" && pwd -P)" ] \
				|| fail "server data directory mismatch"
			return 0
		fi
		sleep 0.2
	done
	fail "server did not start (see $LOG)"
}

stop_server() {
	"$BIN/pg_ctl" -D "$DATA" -m fast -w stop > /dev/null 2>&1 || fail "server did not stop"
	server_started=0
}

wait_workers() {
	worker_failed=0
	for worker_pid in "$@"; do
		wait "$worker_pid" || worker_failed=1
	done
	[ "$worker_failed" = 0 ] || fail "a verdict worker failed"
}

mkdir -p "$VB_WORK" || fail "cannot create work directory"
[ -e "$DATA" ] && fail "$DATA already exists; pick another VB_TAG"
SOCK=$(mktemp -d "/tmp/vb-$PORT.XXXXXX") || fail "cannot create private socket directory"
PSQL=("$BIN/psql" -h "$SOCK" -p "$PORT" -U postgres -X -At -v ON_ERROR_STOP=1 postgres)
: > "$OUT"

"$BIN/initdb" -D "$DATA" -A trust -U postgres > "$VB_WORK/initdb-$TAG.log" 2>&1 \
	|| fail "initdb failed (see $VB_WORK/initdb-$TAG.log)"
cat >> "$DATA/postgresql.conf" <<EOF
port = $PORT
unix_socket_directories = '$SOCK'
listen_addresses = ''
shared_buffers = 256MB
autovacuum = off
cluster.enabled = on
cluster.node_id = 0
cluster.allow_single_node = on
cluster.interconnect_tier = stub
cluster.undo_cleaner_enabled = $CLEANER
EOF

start_server
"${PSQL[@]}" -c "CREATE EXTENSION test_pgrac_verdict_bench" > /dev/null || fail "extension"

# Committed xids fall in (xid_a, xid_b), aborted ones in (xid_b, xid_c).
printf 'SELECT txid_current();\n' > "$VB_WORK/commit-$TAG.sql"
printf 'BEGIN;\nSELECT txid_current();\nROLLBACK;\n' > "$VB_WORK/abort-$TAG.sql"
xid_a=$("${PSQL[@]}" -c "SELECT txid_current()") || fail "xid_a"
"$BIN/pgbench" -h "$SOCK" -p "$PORT" -U postgres -n -c 1 -t 4096 \
	-f "$VB_WORK/commit-$TAG.sql" postgres > /dev/null 2>&1 || fail "commit workload"
xid_b=$("${PSQL[@]}" -c "SELECT txid_current()") || fail "xid_b"
"$BIN/pgbench" -h "$SOCK" -p "$PORT" -U postgres -n -c 1 -t 1024 \
	-f "$VB_WORK/abort-$TAG.sql" postgres > /dev/null 2>&1 || fail "abort workload"
xid_c=$("${PSQL[@]}" -c "SELECT txid_current()") || fail "xid_c"

# A clean restart puts every xid above below the startup checkpoint.
stop_server
start_server

xid_sets="
SELECT coalesce(array_agg(x::text::xid), '{}')::text AS cxids FROM (
  SELECT x FROM generate_series($xid_a + 1, $xid_b - 1) x
  WHERE x % 16 = 0 AND pg_xact_status(x::text::xid8) = 'committed' LIMIT 200) s \\gset
SELECT coalesce(array_agg(x::text::xid), '{}')::text AS axids FROM (
  SELECT x FROM generate_series($xid_b + 1, $xid_c - 1) x
  WHERE x % 16 = 0 AND pg_xact_status(x::text::xid8) = 'aborted' LIMIT 50) s \\gset
"
emit() {	# emit <tier> <phase> <round> <kind> <mode> <xid variable> <iterations> <latch>
	printf "SELECT 'tier=%s phase=%s round=%s kind=%s ' || test_pgrac_vb_run('%s', :'%s'::xid[], %s, %s);\n" \
		"$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8"
}

for tier in $TIERS; do
	"${PSQL[@]}" -c "SELECT test_pgrac_vb_provision($tier, 'publish')" > /dev/null || fail "provision $tier"
	present=$("${PSQL[@]}" -c "SELECT test_pgrac_vb_segments()") || fail "segments"
	[ "$present" = "$tier" ] || fail "expected $tier segments, found $present"
	echo "tier=$tier phase=env round=0 kind=uptime {\"uptime\":\"$(uptime | sed 's/.*load averages*: //')\"}" >> "$OUT"
	{
		echo "$xid_sets"
		emit "$tier" warmup 0 scan scan cxids 200 false
		emit "$tier" warmup 0 leaf_ordinary_committed leaf_ordinary cxids 200 true
		if [ -n "$INTERPOSE" ]; then
			emit "$tier" control 0 scan_lock_control scan_lock_control cxids 4 false
		fi
		for round in $(seq 1 "$ROUNDS"); do
			emit "$tier" hot "$round" scan scan cxids "$ITERS" false
			emit "$tier" hot "$round" leaf_ordinary_committed leaf_ordinary cxids "$ITERS" true
			emit "$tier" hot "$round" leaf_ordinary_aborted leaf_ordinary axids "$ITERS" true
			emit "$tier" hot "$round" leaf_legacy_committed leaf_legacy cxids "$ITERS" true
			emit "$tier" hot "$round" clog_leg clog_leg cxids $((ITERS * 100)) true
		done
		# BSD seq counts down from 1 to 0; zero must explicitly disable eviction.
		if [ "$COLD" -gt 0 ]; then
			for cycle in $(seq 1 "$COLD"); do
				printf "SELECT 'tier=%s phase=evict round=%s kind=evict ' || test_pgrac_vb_evict();\n" \
					"$tier" "$cycle"
				emit "$tier" cold "$cycle" leaf_ordinary_committed leaf_ordinary cxids 1 true
				emit "$tier" recheck "$cycle" leaf_ordinary_committed leaf_ordinary cxids 1 true
			done
		fi
	} > "$VB_WORK/tier-$TAG-$tier.sql"
	"${PSQL[@]}" -f "$VB_WORK/tier-$TAG-$tier.sql" >> "$OUT" || fail "tier $tier (see $OUT)"
	echo "tier $tier done: $(uptime | sed 's/.*load averages*: //')"
done

# Optional: concurrent verdict loops, alone and beside a commit workload.
if [ "$PARALLEL" -gt 0 ]; then
	last=${TIERS##* }
	loop_sql="$VB_WORK/loop-$TAG.sql"
	{
		echo "$xid_sets"
		emit "$last" parallel 0 leaf_ordinary_committed leaf_ordinary cxids "$ITERS" true
	} > "$loop_sql"
	commit_tps() {	# commit_tps <label>
		PGOPTIONS="-c synchronous_commit=off" "$BIN/pgbench" -h "$SOCK" -p "$PORT" -U postgres \
			-n -c 8 -j 4 -T 8 -f "$VB_WORK/commit-$TAG.sql" postgres 2> /dev/null \
			| awk -v label="$1" -v tier="$last" '/^tps/ {
				printf "tier=%s phase=commit round=0 kind=%s {\"tps\":%s}\n", tier, label, $3 }' >> "$OUT"
	}
	for round in 1 2 3; do
		commit_tps "commit_alone_r$round"
	done
	for workers in 1 "$PARALLEL"; do
		for round in 1 2 3; do
			pids=""
			for _ in $(seq 1 "$workers"); do
				"${PSQL[@]}" -f "$loop_sql" | sed "s/phase=parallel round=0/phase=parallel$workers round=$round/" \
					>> "$OUT.part.$$.$RANDOM" &
				pids="$pids $!"
			done
			# shellcheck disable=SC2086
			wait_workers $pids
			cat "$OUT.part.$$."* >> "$OUT" 2> /dev/null
			rm -f "$OUT.part.$$."*
		done
	done
	for round in 1 2 3; do
		pids=""
		for _ in $(seq 1 "$PARALLEL"); do
			"${PSQL[@]}" -f "$loop_sql" | sed "s/phase=parallel round=0/phase=mixed$PARALLEL round=$round/" \
				>> "$OUT.part.$$.$RANDOM" &
			pids="$pids $!"
		done
		commit_tps "commit_beside_${PARALLEL}_verdict_loops_r$round"
		# shellcheck disable=SC2086
		wait_workers $pids
		cat "$OUT.part.$$."* >> "$OUT" 2> /dev/null
		rm -f "$OUT.part.$$."*
	done
fi

stop_server
echo "results: $OUT"
