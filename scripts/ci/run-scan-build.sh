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
#      0  - analysis completed (findings retain the existing warn-only policy)
#      2  - scan-build binary not found
#      other - preparation, compiler or analyzer execution failed
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

set -euo pipefail

if ! command -v scan-build >/dev/null 2>&1; then
  echo "ERROR: scan-build not installed" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT" || exit 2

CI_ANALYZE_JOBS="${CI_ANALYZE_JOBS:-$(getconf _NPROCESSORS_ONLN)}"
if ! [[ "$CI_ANALYZE_JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: CI_ANALYZE_JOBS must be a positive integer" >&2
  exit 2
fi

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
# Disabled checkers:
#   deadcode.DeadStores  - PG idiom `int x = 0; x = real_value;` (false positives)
#   security.insecureAPI - Annex K bounds-checked APIs (memcpy_s etc.) are not
#                          available in glibc; all PG/pgrac uses plain memcpy/
#                          memset by design.  Re-evaluate at Stage 6+ production
#                          hardening if a security-conscious libc is adopted.
scan-build \
  --use-cc=clang \
  -o "$OUT_DIR" \
  -enable-checker deadcode \
  -enable-checker nullability \
  -disable-checker deadcode.DeadStores \
  -disable-checker security.insecureAPI \
  --keep-empty \
  make -j"$CI_ANALYZE_JOBS" -C src/backend/cluster \
  2>&1 | tee "$OUT_DIR/scan-build.log"

REPORT_COUNT=$(find "$OUT_DIR" -name 'report-*.html' 2>/dev/null | wc -l | tr -d ' ')
echo "## scan-build summary"
echo "scan-build issued $REPORT_COUNT bug reports"
echo "Open: $OUT_DIR/index.html (or any report-*.html)"

# Only successful execution reaches here; artifacts carry analyzer findings.
exit 0
