#!/usr/bin/env bash
#
# scripts/perf/cr-redundancy-calc.sh
#	  spec-5.50 D3 — cross-backend / cross-worker CR construction redundancy
#	  calculator (measure-only; consumes the run-cr-profile.sh CSV).
#
#	  The shared-pool dedup target is the EXACT 5-field CR cache key
#	  {rlocator, forknum, blockno, read_scn, base_page_lsn}
#	  (src/include/cluster/cluster_cr_cache.h:68) — NOT the block.  So the
#	  redundancy denominator is distinct_cr_keys, never distinct_blocks
#	  (spec-5.50 r2 P0 + errata 1).  distinct_blocks is only an equivalent
#	  shorthand when the harness PROVED same read_scn (exported snapshot) and
#	  stable base_page_lsn (quiescent window), i.e. distinct_cr_keys == K; that
#	  proof is carried per-row as key_stable.
#
#	    redundancy        = construct_delta / distinct_cr_keys
#	    dedup_savings_pct = (1 - 1/redundancy) * 100   (ideal shared-pool gain)
#	    verdict           = measured | inconclusive
#
#	  When key_stable != 1 (proof not established) or distinct_cr_keys <= 0,
#	  the row is INCONCLUSIVE and MUST NOT feed the value gate (spec-5.50
#	  §3.3 R-1 / §3.4; r3 P1 "证不了 → inconclusive、不用于 GO").  This is the
#	  honesty guard: a block-denominator or an unproven-key number would hand
#	  spec-5.51 a false shared-pool ROI.
#
#	  Pure post-processing of counter deltas: NO database, NO product code,
#	  NO catalog.  `--self-test` runs the U3 math cases with no cluster.
#
#	  Usage:
#	    cr-redundancy-calc.sh [INPUT.csv]      # stdin if INPUT omitted
#	    cr-redundancy-calc.sh --self-test      # U3 unit cases, no DB
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-5.50-cr-read-path-profile.md (FROZEN v1.0 + errata 1)
#

set -e
set -o pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWK_ENGINE="$HERE/cr-redundancy-calc.awk"

# ----------
# compute -- run the awk engine over a CSV stream (stdin) and emit the
# augmented CSV (original columns + redundancy, dedup_savings_pct, verdict).
# ----------
compute()
{
	[ -r "$AWK_ENGINE" ] || {
		echo "cr-redundancy-calc: engine not found: $AWK_ENGINE" >&2
		return 3
	}
	awk -F',' -f "$AWK_ENGINE"
}

# ----------
# self_test -- U3: redundancy math is correct on known counter deltas.
#   Feeds fixed CSV rows and asserts the emitted redundancy / verdict, with
#   NO cluster running.  Proves the P0 denominator + inconclusive guard before
#   any real profile run trusts the number.
# ----------
self_test()
{
	local fails=0 ncase=0

	# $1 case name; $2 input row (after the header); $3 expected redundancy
	# field; $4 expected verdict field.
	check_case()
	{
		local name="$1" row="$2" want_red="$3" want_verdict="$4"
		ncase=$((ncase + 1))
		local header="scenario,axis,distinct_cr_keys,key_stable,construct_delta"
		local out got_red got_verdict
		out="$(printf '%s\n%s\n' "$header" "$row" | compute | tail -1)"
		# augmented columns are appended: ...,redundancy,dedup_savings_pct,verdict
		got_verdict="$(echo "$out" | awk -F',' '{print $NF}')"
		got_red="$(echo "$out" | awk -F',' '{print $(NF-2)}')"
		if [ "$got_red" = "$want_red" ] && [ "$got_verdict" = "$want_verdict" ]; then
			echo "ok $ncase - $name (redundancy=$got_red verdict=$got_verdict)"
		else
			echo "not ok $ncase - $name"
			echo "#   want redundancy=$want_red verdict=$want_verdict"
			echo "#   got  redundancy=$got_red verdict=$got_verdict"
			echo "#   full: $out"
			fails=$((fails + 1))
		fi
	}

	# Case 1: cross-backend, proof established, 4 backends each construct the
	#         10 hot keys once -> redundancy 4.00 (shared pool would save 75%).
	check_case "cross-backend proven N=4" \
		"axisA_n4,A,10,1,40" "4.00" "measured"

	# Case 2: same numbers but the key-stability proof FAILED -> inconclusive
	#         (the denominator might be inflated by differing read_scn /
	#         base_page_lsn; must not feed the value gate).
	check_case "unproven key -> inconclusive" \
		"axisA_unstable,A,10,0,40" "INCONCLUSIVE" "inconclusive"

	# Case 3: parallel seqscan mutual-exclusion -> each key built once -> 1.00
	#         (no same-key cross-worker redundancy; does NOT justify a pool).
	check_case "parallel mutual-exclusion ~= 1x" \
		"axisC_par,C,100,1,100" "1.00" "measured"

	# Case 4: no CR work at all (no keys touched) -> nothing to dedup.
	check_case "zero keys -> inconclusive" \
		"noop,A,0,1,0" "INCONCLUSIVE" "inconclusive"

	# Case 5: contradiction — constructs reported but zero distinct keys.
	#         Cannot divide; never silently treat as measured.
	check_case "keys=0 but constructs>0 -> inconclusive" \
		"bad,A,0,1,5" "INCONCLUSIVE" "inconclusive"

	# Case 6: fractional redundancy (real measured 2.5x) formats to 2 dp.
	check_case "fractional redundancy 2.50x" \
		"axisA_n_frac,A,4,1,10" "2.50" "measured"

	# Case 7: a header missing a required column is a hard error, never a
	#         silent guess (honesty guard).
	ncase=$((ncase + 1))
	local mc_rc=0
	printf 'scenario,axis,construct_delta\nx,A,40\n' | compute >/dev/null 2>&1 || mc_rc=$?
	if [ "$mc_rc" -eq 4 ]; then
		echo "ok $ncase - missing required column hard-errors (exit 4)"
	else
		echo "not ok $ncase - missing required column should exit 4, got $mc_rc"
		fails=$((fails + 1))
	fi

	echo "1..$ncase"
	if [ "$fails" -ne 0 ]; then
		echo "# $fails/$ncase cases FAILED" >&2
		return 1
	fi
	echo "# all $ncase cases passed"
}

main()
{
	case "${1:-}" in
		--self-test)
			self_test
			;;
		-h | --help)
			sed -n '3,40p' "${BASH_SOURCE[0]}" | sed 's/^#\t*//; s/^# //; s/^#//'
			;;
		"")
			compute
			;;
		*)
			compute <"$1"
			;;
	esac
}

main "$@"
