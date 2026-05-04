#!/bin/bash
#-------------------------------------------------------------------------
#
# check-scn-cmp-gate.sh
#    CI helper: ban raw SCN comparisons (spec-1.15 Q8 + L4).
#
#    Cluster code must use scn_time_cmp / scn_total_cmp /
#    scn_recovery_cmp instead of direct < / > / <= / >= on SCN values.
#    Direct comparisons on raw uint64-encoded SCN would incorrectly let
#    the high 8-bit node_id field dominate ordering.
#
# IDENTIFICATION
#    scripts/ci/check-scn-cmp-gate.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Strategy: grep for `<` / `>` / `<=` / `>=` adjacent to identifiers
#    that look like SCN-typed variables (heuristic: variable name ends
#    with `scn` or `_scn` or contains `_scn_`).  False positives are
#    suppressed via SCN_CMP_OK comments on the same line.
#
#    Whitelist (allowed contexts):
#      - cluster_scn.c / cluster_scn.h: implementation-internal (uses
#        local_scn fields, not full SCN; cmp helpers themselves)
#      - test_cluster_scn.c: unit tests asserting cmp behavior; raw
#        comparisons are intentional and reviewed
#      - lines tagged with /* SCN_CMP_OK: <reason> */
#
#-------------------------------------------------------------------------

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

violations=0

# Search cluster code only; SCN type is internal to cluster subsystem.
files=$(git ls-files 'src/backend/cluster/*.c' 'src/include/cluster/*.h' \
    | grep -vE 'cluster_scn\.(c|h)$' \
    | grep -vE '^src/test/' || true)

for f in $files; do
    # Look for comparison operators on identifiers ending in _scn
    # (e.g. "local_scn < other_scn", "block_scn >= snapshot_scn").
    matches=$(grep -nE '\b[a-zA-Z_][a-zA-Z0-9_]*_scn[ \t]*(<|>|<=|>=)[^=]' "$f" \
        | grep -v 'SCN_CMP_OK' \
        | grep -vE '^[^:]+:[0-9]+:[ \t]*\*' \
        || true)
    if [ -n "$matches" ]; then
        echo "ERROR: raw SCN comparison detected in $f:"
        echo "$matches"
        echo "  Use scn_time_cmp / scn_total_cmp / scn_recovery_cmp,"
        echo "  or annotate with /* SCN_CMP_OK: <reason> */ if intentional."
        violations=$((violations + 1))
    fi
done

if [ "$violations" -gt 0 ]; then
    echo ""
    echo "FAIL: $violations file(s) contain raw SCN comparisons."
    echo "spec-1.15 Q8 + L4: high node_id bits would dominate ordering."
    exit 1
fi

echo "OK: no raw SCN comparisons found in cluster code."
exit 0
