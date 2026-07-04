#!/bin/bash
#-------------------------------------------------------------------------
#
# check-release-notes-principle0.sh
#    CI helper: enforce that public release artifacts carry no private
#    design reasoning.  Public release notes and the beta manual page
#    describe only what the software does and how to use it; they must
#    not reference internal design documents, decision history, or
#    design rationale.
#
#    Run from repository root.
#
# IDENTIFICATION
#    scripts/ci/check-release-notes-principle0.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    A leak of internal design reasoning into a public release artifact is
#    permanent and cannot be retracted, so this gate is fail-closed: any
#    banned pattern in a scanned file is a hard failure.
#
#    Scanned files (default surface):
#      - docs/release-notes/*.md   (public release notes)
#      - docs/user-guide/beta.md   (public beta scope / limitations page)
#
#    The scan is deliberately scoped to the public release surface, not the
#    whole docs tree, so unrelated pre-existing manual pages are unaffected.
#
#    A file list may be passed as arguments to override the default surface;
#    this supports a positive self-test (feed a file that contains a banned
#    pattern and confirm the gate fails).
#
#    Banned patterns (private design reasoning that must never appear in a
#    public release artifact):
#      - spec-<n>        internal specification references
#      - AD-<n>          architecture-decision references
#      - feature-<n>     internal feature-document references
#      - Q<n>:           design Q&A markers
#      - Hardening vX.Y  internal hardening-round markers
#      - the star glyph  recommended-option markers
#      - Stage <n>       internal roadmap/stage-plan references
#
#    Exits non-zero on any hit.
#
#-------------------------------------------------------------------------

set -u
set -o pipefail

# ----------
# Resolve the files to scan.
# ----------
files=("$@")
if [ "${#files[@]}" -eq 0 ]; then
	shopt -s nullglob
	files=(docs/release-notes/*.md docs/user-guide/beta.md)
	shopt -u nullglob
fi

if [ "${#files[@]}" -eq 0 ]; then
	echo "check-release-notes-principle0: no files to scan (expected docs/release-notes/*.md)" >&2
	exit 1
fi

# ----------
# Banned patterns.  Each entry is "ERE<TAB>human description".
# The star glyph is matched by its UTF-8 bytes so the source stays ASCII.
# ----------
star=$(printf '\xe2\x98\x85')

patterns=(
	"spec-[0-9]	internal specification reference (spec-N.M)"
	"AD-[0-9]	architecture-decision reference (AD-NNN)"
	"feature-[0-9]	internal feature-document reference (feature-NNN)"
	"Q[0-9]+[[:space:]]*[:：]	design Q&A marker (QN:)"
	"Hardening v[0-9]	internal hardening-round marker (Hardening vX.Y)"
	"${star}	recommended-option marker (star glyph)"
	"Stage[ -]?[0-9]	internal roadmap/stage-plan reference (Stage N)"
)

fail=0

for f in "${files[@]}"; do
	if [ ! -f "$f" ]; then
		echo "check-release-notes-principle0: file not found: $f" >&2
		fail=1
		continue
	fi

	for entry in "${patterns[@]}"; do
		pat=${entry%%$'\t'*}
		desc=${entry#*$'\t'}
		# -i so spec/AD/feature match regardless of case.
		if hits=$(grep -niE "$pat" "$f"); then
			echo "PRINCIPLE-0 VIOLATION in $f: $desc" >&2
			echo "$hits" | sed 's/^/    /' >&2
			fail=1
		fi
	done
done

if [ "$fail" -ne 0 ]; then
	echo "" >&2
	echo "check-release-notes-principle0: FAIL — public release artifact contains private design reasoning." >&2
	echo "Public release notes and the beta page must describe only what/how, never why/design history." >&2
	exit 1
fi

echo "check-release-notes-principle0: OK — scanned ${#files[@]} file(s), no design-reasoning leak."
exit 0
