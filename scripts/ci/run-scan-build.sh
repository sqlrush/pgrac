#!/bin/bash
#-------------------------------------------------------------------------
#
# run-scan-build.sh
#    CI helper: run Clang Static Analyzer (scan-build) against the
#    pgrac cluster subsystem build.
#
#    Spec:   pgrac/specs/spec-0.27.5-static-analysis.md
#    Design: pgrac/docs/ci-static-analysis.md
#
#    Scope:  wraps `make -C src/backend/cluster` only.  Wrapping the
#    full PG build would surface thousands of PG-upstream false
#    positives that are not pgrac's responsibility (PG has its own
#    Coverity scan).  See docs/ci-static-analysis.md §2.2.
#
# IDENTIFICATION
#    scripts/ci/run-scan-build.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Exit codes:
#      0  - always (warn-only at stage 0.27.5 - 0.30; HTML reports in
#           scan-build-report/ carry the actual findings)
#      2  - scan-build binary not found
#
#    Enabled checkers:
#      security      use-after-free / buffer overrun / integer overflow
#      deadcode      dead code / unreachable branches
#      nullability   null deref via path
#
#    Disabled checkers:
#      deadcode.DeadStores  PG idiom `int x = 0; x = real_value;`
#                           triggers too many false positives
#
#-------------------------------------------------------------------------

set -uo pipefail

if ! command -v scan-build >/dev/null 2>&1; then
  echo "ERROR: scan-build not installed" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT" || exit 2

OUT_DIR=scan-build-report
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "## scan-build $(scan-build --help 2>&1 | head -1)"

# Force a clean build so scan-build sees every translation unit.  The
# full configure/build was already run by the calling CI step.
make -C src/backend/cluster clean

# scan-build wraps the compiler.  --status-bugs would return non-zero
# when bugs are found, but we are warn-only at stage 0.27.5 - 0.30, so
# capture the count without failing the step.
scan-build \
  --use-cc=clang \
  -o "$OUT_DIR" \
  -enable-checker security \
  -enable-checker deadcode \
  -enable-checker nullability \
  -disable-checker deadcode.DeadStores \
  --keep-empty \
  make -C src/backend/cluster \
  2>&1 | tee "$OUT_DIR/scan-build.log" \
  || true

REPORT_COUNT=$(find "$OUT_DIR" -name 'report-*.html' 2>/dev/null | wc -l | tr -d ' ')
echo "## scan-build summary"
echo "scan-build issued $REPORT_COUNT bug reports"
echo "Open: $OUT_DIR/index.html (or any report-*.html)"

# Always succeed; CI artifact carries the findings.
exit 0
