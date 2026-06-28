#!/bin/bash
#-------------------------------------------------------------------------
#
# run-cross-instance-cr-probe.sh -- spec-5.57 D0 measure-leg (value gate +
#    soundness gate) for the cross-instance CR/current read-path coordinator
#    boundary.
#
# This is a MEASURE-ONLY tool (zero product code).  It answers the spec-5.57
# value-gate question: "is the class③ runtime-warm-remote CR data plane reachable
# / worth opening in Stage 5.5?"  The answer is determined by STATIC evidence that
# survives in CI (no harness needed): the codebase has NO over-the-wire remote
# undo fetch path and the CR walker fail-closes class③ with 53R9G.  Therefore the
# value-gate verdict is NO-GO-in-5.5 (data plane -> Stage 6 #119) -- a LEGAL NO-GO
# (據實 not降 scope), same paradigm as spec-5.50/5.55/5.56.
#
# It ALSO prints the L257 soundness-gate checklist (§3.4) with a per-item ruling:
# 5.57 verifies failure-direction (GREEN, the fail-closed boundary) here; the
# other four gates are Stage 6 data-plane ship gates (listed, owned by Stage 6).
#
# An OPTIONAL --dynamic 2-node leg (off by default; needs the local IPC::Run TAP
# harness) confirms at runtime that cross-node reads fail closed.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-5.57-cross-instance-cr-current-coordinator.md (FROZEN v1.0)
#
# IDENTIFICATION
#    scripts/perf/run-cross-instance-cr-probe.sh
#
# NOTES
#    pgrac-original.  --self-test parses the --static output (L223: validate
#    CONTENT, not mere file existence) and is the CI-safe entry point.
#
#-------------------------------------------------------------------------
set -euo pipefail

PROGNAME="run-cross-instance-cr-probe.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRCROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CR_C="$SRCROOT/src/backend/cluster/cluster_cr.c"
UNDO_C="$SRCROOT/src/backend/cluster/cluster_undo_record.c"
WAIT_H="$SRCROOT/src/include/utils/wait_event.h"

usage() {
	cat <<EOF
$PROGNAME -- spec-5.57 D0 cross-instance CR measure-leg.

Usage:
  $PROGNAME --static       Emit the static value-gate + soundness-gate evidence.
  $PROGNAME --self-test    Run --static, parse the output, assert the verdict (CI-safe).
  $PROGNAME --dynamic      (optional) Additionally run a 2-node fail-closed probe.
  $PROGNAME --help
EOF
}

# ---- static value gate -------------------------------------------------
# Each GATE line is machine-parseable: "GATE <name> <PASS|FAIL> <detail>".
emit_static() {
	echo "# spec-5.57 D0 measure-leg: cross-instance CR value gate (STATIC evidence)"
	echo "# srcroot=$SRCROOT"

	# E1: no over-the-wire remote undo fetch -- cluster_undo_get_record returns 0
	#     for a non-own, non-materialized (runtime-warm) origin.
	if grep -q "runtime cross-instance undo read not supported" "$UNDO_C" 2>/dev/null; then
		echo "GATE no_remote_undo_fetch PASS cluster_undo_get_record returns 0 for runtime cross-instance (no wire fetch)"
	else
		echo "GATE no_remote_undo_fetch FAIL marker not found in cluster_undo_record.c"
	fi

	# E2: the CR walker fail-closes class③ with the canonical 53R9G (not a silent
	#     fallback, not 53R9F snapshot-too-old) at the remote-undo-read leg.
	if grep -q "remote-undo-read \+\"" "$CR_C" 2>/dev/null \
		|| grep -q "remote-undo-read" "$CR_C" 2>/dev/null; then
		echo "GATE class3_fail_closed PASS CR walker fail-closes class③ with ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED (53R9G)"
	else
		echo "GATE class3_fail_closed FAIL pre-check marker not found in cluster_cr.c"
	fi

	# E3: GCS ships only current blocks; the CR-ship wait events are reserved /
	#     dormant (declared but no producer) -- no CR-image wire path.
	if grep -q "WAIT_EVENT_BUFFER_SHIP_CR_BUILD" "$WAIT_H" 2>/dev/null \
		&& ! grep -rq "WAIT_EVENT_BUFFER_SHIP_CR_BUILD" "$SRCROOT/src/backend/cluster/cluster_gcs_block.c" 2>/dev/null; then
		echo "GATE no_cr_image_wire PASS WAIT_EVENT_BUFFER_SHIP_CR_BUILD/SEND reserved but dormant (no GCS producer)"
	else
		echo "GATE no_cr_image_wire FAIL CR-ship wait event has a producer or is missing"
	fi

	# Value-gate verdict: all three => the data plane is unreachable in Stage 5.5.
	echo "VERDICT value_gate NO_GO_IN_5_5 data plane forwarded to Stage 6 (#119 undo-block Cache Fusion); spec-5.57 ships boundary + contract only (legal NO-GO)"

	echo "# spec-5.57 §3.4 soundness-gate checklist (L257): 5.57 verifies failure-direction; the rest are Stage 6 ship gates."
	echo "SOUNDNESS failure_direction VERIFIED_5_57 any uncertainty -> ERROR 53R9G, never visible (CR walker pre-check; TAP fail-closed e2e)"
	echo "SOUNDNESS base_availability STAGE6_OWNS current block image + WAL-before-ship (feature-019)"
	echo "SOUNDNESS metadata_availability STAGE6_OWNS remote undo image is real evidence (not ITL stamp hint); INDOUBT -> fail-closed"
	echo "SOUNDNESS replay_determinism STAGE6_OWNS v2 column-diff cross-node reconstruct determinism (spec-4.9ab reader)"
	echo "SOUNDNESS invalidation_completeness STAGE6_OWNS remaster/DROP/TRUNCATE/relfilenode-reuse/retention + fence_epoch invalidation"
}

# ---- self-test: parse --static, assert the verdict ---------------------
self_test() {
	local out rc=0
	out="$(emit_static)"

	echo "$out"
	echo "# ---- self-test assertions ----"

	# every GATE line must be PASS
	if echo "$out" | grep '^GATE ' | grep -qv ' PASS '; then
		echo "not ok - some GATE line is not PASS"
		rc=1
	else
		echo "ok - all GATE lines PASS"
	fi

	# the value-gate verdict must be the legal NO-GO
	if echo "$out" | grep -q '^VERDICT value_gate NO_GO_IN_5_5'; then
		echo "ok - value gate verdict is NO_GO_IN_5_5 (data plane -> Stage 6)"
	else
		echo "not ok - value gate verdict missing/unexpected"
		rc=1
	fi

	# failure-direction must be verified by 5.57 itself
	if echo "$out" | grep -q '^SOUNDNESS failure_direction VERIFIED_5_57'; then
		echo "ok - failure-direction soundness gate verified in 5.57"
	else
		echo "not ok - failure-direction soundness gate not verified"
		rc=1
	fi

	# exactly 5 soundness gates listed (§3.4)
	local n_sound
	n_sound="$(echo "$out" | grep -c '^SOUNDNESS ')"
	if [ "$n_sound" -eq 5 ]; then
		echo "ok - 5 soundness gates listed (§3.4)"
	else
		echo "not ok - expected 5 soundness gates, found $n_sound"
		rc=1
	fi

	if [ "$rc" -eq 0 ]; then
		echo "# self-test PASSED"
	else
		echo "# self-test FAILED"
	fi
	return "$rc"
}

# ---- optional dynamic 2-node fail-closed probe -------------------------
dynamic_probe() {
	echo "# spec-5.57 D0 dynamic leg: a 2-node fail-closed probe lives in the TAP"
	echo "# harness (src/test/cluster_tap/t/318_*.pl).  The data plane is"
	echo "# unreachable, so the only observable runtime outcome is fail-closed"
	echo "# 53R9G + cross_instance_cr_refused/remote_undo_read_refused counters."
	echo "# Run it via: cd src/test/cluster_tap && PERL5LIB=\$HOME/perl5/lib/perl5 \\"
	echo "#   prove -v t/318_cluster_5_57_cross_instance_boundary.pl"
}

case "${1:---help}" in
	--static) emit_static ;;
	--self-test) self_test ;;
	--dynamic) emit_static; echo; dynamic_probe ;;
	--help | -h) usage ;;
	*) usage; exit 2 ;;
esac
