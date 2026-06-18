#!/bin/bash
#-------------------------------------------------------------------------
#
# check-ges-mode-gate.sh
#    CI helper: route GES enqueue conflict decisions through the frozen
#    contract (spec-5.1a).
#
#    New cluster code must decide GES mode compatibility via
#    ges_modes_compatible() (which reads the frozen ges_mode_compat_matrix),
#    not by calling PG's DoLockModesConflict() directly.  This keeps every
#    conflict decision on the single frozen source of truth.
#
# IDENTIFICATION
#    scripts/ci/check-ges-mode-gate.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Match key is file + symbol + reason (never a line number, which would
#    go stale on any edit).  Two exemption mechanisms:
#
#      - Whitelisted files (the contract's own home + transitional callers
#        that spec-5.1b migrates to ges_modes_compatible):
#          cluster_ges_mode_backend.c -- the frozen-vs-PG self-check itself
#          cluster_ges.c              -- transitional; spec-5.1b migrates
#          cluster_grd.c              -- transitional; spec-5.1b migrates
#        spec-5.1b removes each transitional entry as it migrates.
#
#      - Lines annotated /* GES_MODE_OK: <reason> */ for one-off exceptions.
#
#-------------------------------------------------------------------------

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

violations=0

# Whitelisted files (by basename): the contract home + transitional callers.
whitelist='cluster_ges_mode_backend\.c$|cluster_ges\.c$|cluster_grd\.c$'

files=$(git ls-files 'src/backend/cluster/*.c' | grep -vE "$whitelist" || true)

for f in $files; do
	# Match calls to DoLockModesConflict(.  Skip comment lines (leading
	# '*') and lines annotated GES_MODE_OK.
	# grep -n on a single file yields "LINENO:CONTENT"; skip block-comment
	# lines (CONTENT starts with optional whitespace then '*' or '/*').
	matches=$(grep -nE 'DoLockModesConflict[ \t]*\(' "$f" \
		| grep -v 'GES_MODE_OK' \
		| grep -vE '^[0-9]+:[[:space:]]*/?\*' \
		|| true)
	if [ -n "$matches" ]; then
		echo "ERROR: raw DoLockModesConflict() in $f:"
		echo "$matches"
		echo "  Use ges_modes_compatible() (spec-5.1a frozen contract),"
		echo "  or annotate with /* GES_MODE_OK: <reason> */ if intentional."
		violations=$((violations + 1))
	fi
done

if [ "$violations" -gt 0 ]; then
	echo ""
	echo "FAIL: $violations file(s) decide GES conflicts off the frozen contract."
	echo "spec-5.1a: all GES mode conflict decisions go through ges_modes_compatible()."
	exit 1
fi

echo "OK: no raw DoLockModesConflict() outside the GES mode contract."
exit 0
