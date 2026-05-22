#!/bin/bash
#-------------------------------------------------------------------------
#
# check-no-clog-overlay.sh
#    CI lint: ban the v0.1 CLOG-overlay foundation path (spec-3.1 L176;
#    spec-3.2 v0.3 amend to narrow banned list per lessons SSOT v1.66).
#
#    spec-3.1 v0.1 erroneously proposed a CLOG-overlay-as-foundation
#    design.  Two identifiers from that direction remain permanently
#    rejected (AD-006 第五轮 / feature-069 lock):
#      - cluster_clog_overlay        (any CLOG-mirroring overlay)
#      - SharedInvalCLOGStatusMsg    (CLOG status piggyback on sinval)
#
#    spec-3.2 v1.0 ships PGRAC_IC_MSG_TT_STATUS_HINT = 20 as a legitimate
#    independent wire frame for cross-node TT status propagation (NOT
#    CLOG;  embeds 24B ClusterTTStatusKey exact key, sender = LMON-only
#    per L172;  receiver install_local to spec-3.1 TT overlay).  L176
#    v1.66 amend therefore MOVES this identifier OUT of the banned list.
#
# IDENTIFICATION
#    scripts/ci/check-no-clog-overlay.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Strategy: grep -rnE the three banned identifiers under src/.
#    Allowed contexts:
#      - any line carrying the marker `/* SPEC_3_1_LINT_OK: */`
#        (e.g. a future hardening spec that intentionally references
#        the rejected name in a removal comment).
#
#    Anchors L176 in code, complementing the docs/spec-drafting-lessons
#    L176 entry and feature-069 §"AD-006 第五轮" guardrail.
#
#-------------------------------------------------------------------------

set -euo pipefail

BANNED_RE='cluster_clog_overlay|SharedInvalCLOGStatusMsg'

# Use git ls-files so we only scan tracked files; bypasses build artifacts
# and untracked log directories.
matches=$(git ls-files 'src/*' 2>/dev/null \
	| xargs grep -nE "$BANNED_RE" 2>/dev/null \
	| grep -v 'SPEC_3_1_LINT_OK' \
	|| true)

if [ -n "$matches" ]; then
	echo "ERROR: spec-3.1 L176 lint failed — banned CLOG-overlay identifiers found:" >&2
	echo "$matches" >&2
	echo "" >&2
	echo "spec-3.1 v0.1 -> v1.0 hard-redirected the foundation from" >&2
	echo "CLOG-overlay to ITL/TT exact-key (AD-006 第五轮 / feature-069)." >&2
	echo "Two names remain permanently rejected:" >&2
	echo "  - cluster_clog_overlay" >&2
	echo "  - SharedInvalCLOGStatusMsg" >&2
	echo "(spec-3.2 v1.0 moved PGRAC_IC_MSG_TT_STATUS_HINT OUT of the" >&2
	echo "banned list — see lessons SSOT v1.66 L176 amend.)" >&2
	echo "See: specs/spec-3.1-cluster-xid-status-foundation.md §0.1" >&2
	echo "     specs/spec-3.2-mvcc-cluster-path-tt-status-wire.md §0.1" >&2
	echo "     docs/spec-drafting-lessons.md L176 (v1.66)" >&2
	exit 1
fi

echo "spec-3.1 L176 lint passed (no CLOG-overlay identifiers; v1.66 amend)."
