#!/usr/bin/env bash
#
# scripts/perf/cr-counter-sample.sh
#	  spec-5.50 D7 — pg_cluster_state CR counter sampler + delta (zero catalog).
#
#	  Samples the 17 per-instance CR counters already exposed by the existing
#	  pg_cluster_state SRF (category='cr'; dump_cr in cluster_debug.c) and
#	  computes per-scenario pre/post deltas.  Reuses the shipped SRF only —
#	  NO new SRF / view / column, so catversion does NOT bump (spec-5.50 D7
#	  option A, r2 P1).
#
#	  Modes:
#	    sample PORT [HOST] [DB]   emit `key,value` for the 17 cr counters
#	                             (sorted; fails loud if not exactly 17 rows so
#	                             an --enable-cluster=off or drifted build cannot
#	                             silently produce empty deltas).
#	    delta PRE.csv POST.csv    emit `key,delta` (= post - value), missing
#	                             pre key treated as 0.
#	    --self-test               delta math cases, no DB.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-5.50-cr-read-path-profile.md (FROZEN v1.0)
#

set -e
set -o pipefail

PSQL="${PSQL:-psql}"
EXPECTED_CR_ROWS="${EXPECTED_CR_ROWS:-17}"

# ----------
# cr_counter_sample PORT [HOST] [DB] -- dump the 17 cr counters as key,value.
#   Live (needs a running cluster backend).  Fails loud if the row count is not
#   EXPECTED_CR_ROWS (catches enable-cluster=off / counter drift).
# ----------
cr_counter_sample()
{
	local port="$1" host="${2:-/tmp}" db="${3:-postgres}"
	local rows
	rows="$("$PSQL" -At -F',' -h "$host" -p "$port" -d "$db" -c \
		"SELECT key, value FROM pg_cluster_state WHERE category='cr' ORDER BY key")"
	local n
	n="$(printf '%s\n' "$rows" | grep -c ',' || true)"
	if [ "$n" -ne "$EXPECTED_CR_ROWS" ]; then
		echo "cr-counter-sample: expected $EXPECTED_CR_ROWS cr rows, got $n (enable-cluster off? counter drift?)" >&2
		return 5
	fi
	printf '%s\n' "$rows"
}

# ----------
# cr_counter_delta PRE POST -- emit `key,delta` (= post - pre) for every key in
#   POST; a key absent from PRE is treated as 0.  Pure text join, no DB.
# ----------
cr_counter_delta()
{
	local pre="$1" post="$2"
	awk -F',' '
		FNR == NR { pre[$1] = $2; next }
		NF >= 2   { printf("%s,%d\n", $1, $2 - (($1 in pre) ? pre[$1] : 0)) }
	' "$pre" "$post"
}

main()
{
	local mode="${1:-}"
	case "$mode" in
		sample)
			shift
			cr_counter_sample "$@"
			;;
		delta)
			cr_counter_delta "$2" "$3"
			;;
		--self-test)
			self_test
			;;
		*)
			echo "usage: cr-counter-sample.sh {sample PORT [HOST] [DB] | delta PRE POST | --self-test}" >&2
			return 2
			;;
	esac
}

# ----------
# self_test -- delta math on fixed key,value files (no DB).
# ----------
self_test()
{
	local tmp fails=0 ncase=0
	tmp="$(mktemp -d)"
	trap 'rm -rf "$tmp"' RETURN

	# Case 1: simple positive delta + an unchanged counter.
	printf 'cr_cache_hit_count,5\ncr_construct_count,100\n' >"$tmp/pre1"
	printf 'cr_cache_hit_count,5\ncr_construct_count,140\n' >"$tmp/post1"
	ncase=$((ncase + 1))
	local got want
	got="$(cr_counter_delta "$tmp/pre1" "$tmp/post1" | sort)"
	want="$(printf 'cr_cache_hit_count,0\ncr_construct_count,40\n' | sort)"
	if [ "$got" = "$want" ]; then echo "ok $ncase - basic delta"; else
		echo "not ok $ncase - basic delta"; echo "#got=$got"; echo "#want=$want"; fails=$((fails+1)); fi

	# Case 2: key absent in pre is treated as 0 (delta = post).
	printf 'cr_construct_count,10\n' >"$tmp/pre2"
	printf 'cr_construct_count,10\ncr_chain_walk_steps_sum,7\n' >"$tmp/post2"
	ncase=$((ncase + 1))
	got="$(cr_counter_delta "$tmp/pre2" "$tmp/post2" | sort)"
	want="$(printf 'cr_chain_walk_steps_sum,7\ncr_construct_count,0\n' | sort)"
	if [ "$got" = "$want" ]; then echo "ok $ncase - missing-pre treated as 0"; else
		echo "not ok $ncase - missing-pre treated as 0"; echo "#got=$got"; echo "#want=$want"; fails=$((fails+1)); fi

	echo "1..$ncase"
	if [ "$fails" -ne 0 ]; then echo "# $fails/$ncase FAILED" >&2; return 1; fi
	echo "# all $ncase cases passed"
}

main "$@"
