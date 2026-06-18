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
#    Exemption is PER CALL SITE, not per file (a file-level whitelist would
#    let a NEW raw call slip the gate in exactly the files spec-5.1b touches).
#    Every legitimate call carries an inline /* GES_MODE_OK: <reason> */ on
#    the same line; the gate skips only those lines (and genuine comments).
#    spec-5.1b D1/D8 migrated the transitional cluster_ges.c / cluster_grd.c
#    grant-path call sites to ges_modes_compatible() and removed their
#    annotations, so the ONLY remaining exempt call sites are:
#      cluster_ges_mode_backend.c -- the frozen-vs-PG self-check itself
#        (it must call DoLockModesConflict directly to verify the matrix).
#    Any NEW bare DoLockModesConflict() in the grant path now fails the gate.
#
#-------------------------------------------------------------------------

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

violations=0

# No file-level exemptions: every cluster .c is scanned; legitimate calls are
# exempted only by an inline /* GES_MODE_OK: <reason> */ on the call line.
files=$(git ls-files 'src/backend/cluster/*.c' || true)

for f in $files; do
	# A call is exempt if GES_MODE_OK appears on the call line OR within the
	# next 2 lines (clang-format wraps a long call + its trailing annotation
	# onto the call's continuation line, so the window must look forward).
	# Genuine comment lines ('*'+space/slash continuation, or '/*' / '//') that
	# merely mention DoLockModesConflict in prose are skipped; a pointer-deref
	# statement like "*ptr = DoLockModesConflict(...)" is NOT a comment.
	matches=$(awk '
		function is_comment(s) { return s ~ /^[[:space:]]*(\*[[:space:]\/]|\/[*\/])/ }
		{ L[NR] = $0 }
		END {
			for (i = 1; i <= NR; i++) {
				if (L[i] ~ /DoLockModesConflict[ \t]*\(/ && !is_comment(L[i])) {
					ok = 0
					for (j = i; j <= i + 2 && j <= NR; j++)
						if (L[j] ~ /GES_MODE_OK/) { ok = 1; break }
					if (!ok) printf "%d:%s\n", i, L[i]
				}
			}
		}' "$f")
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
