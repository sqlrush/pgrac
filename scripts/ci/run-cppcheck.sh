#!/bin/bash
#-------------------------------------------------------------------------
#
# run-cppcheck.sh
#    CI helper: run cppcheck against pgrac cluster sources.
#
#    Spec:   pgrac/specs/spec-0.27.5-static-analysis.md
#    Design: pgrac/docs/ci-static-analysis.md
#
#    Scans:
#      - src/backend/cluster/  (~10 .c files, ~3000 LOC)
#      - src/include/cluster/  (~10 .h files)
#      - src/test/cluster_unit/  (~14 .c files)
#
#    Excludes PG-upstream code by design (PG has its own cppcheck
#    buildfarm + Coverity scan; pgrac is only responsible for cluster
#    subsystem and PGRAC MODIFICATIONS sections).
#
# IDENTIFICATION
#    scripts/ci/run-cppcheck.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Exit codes:
#      0  - always (warn-only at stage 0.27.5 - 0.30; the SARIF
#           artifact carries the actual findings, not the exit code)
#      2  - cppcheck binary not found
#
#    Configuration:
#      --enable=warning,style,performance,portability
#        (skip 'unusedFunction' because PG SRF registration triggers
#         massive false positives; skip 'information' for noise; skip
#         'missingInclude' because cppcheck cannot resolve PG's
#         hand-rolled include layout reliably)
#      --inline-suppr      enable inline /* cppcheck-suppress xxx */
#      --suppressions-list scripts/ci/cppcheck-suppressions.txt
#      --xml --xml-version=2  machine-readable for artifact upload
#      -DUSE_PGRAC_CLUSTER=1  match enable-cluster build mode
#
#-------------------------------------------------------------------------

set -euo pipefail

if ! command -v cppcheck >/dev/null 2>&1; then
  echo "ERROR: cppcheck not installed" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CLUSTER_DIRS=(
  src/backend/cluster
  src/include/cluster
  src/test/cluster_unit
)

# Suppressions file: every entry must have a "Reason:" comment above
# (enforced by check-comment-headers.sh).  See spec-0.27.5 §3.4.
SUPPRESSIONS=scripts/ci/cppcheck-suppressions.txt

echo "## cppcheck $(cppcheck --version)"
echo "Scanning: ${CLUSTER_DIRS[*]}"

# --error-exitcode=0 keeps the exit non-fatal (warn-only).  Findings
# go to cppcheck.xml; a human-readable summary is built downstream.
cppcheck \
  --enable=warning,style,performance,portability \
  --suppress=unusedFunction \
  --suppress=missingInclude \
  --suppress=missingIncludeSystem \
  --inline-suppr \
  --suppressions-list="$SUPPRESSIONS" \
  --error-exitcode=0 \
  --xml --xml-version=2 \
  -I src/include \
  -DUSE_PGRAC_CLUSTER=1 \
  --quiet \
  "${CLUSTER_DIRS[@]}" \
  2> cppcheck.xml

# Build human-readable summary.
if [ -x scripts/ci/cppcheck-xml-to-summary.py ]; then
  python3 scripts/ci/cppcheck-xml-to-summary.py cppcheck.xml \
    > cppcheck-summary.txt
  echo "## cppcheck summary"
  cat cppcheck-summary.txt
else
  echo "## cppcheck summary (raw)"
  echo "Findings logged to cppcheck.xml"
  grep -c '<error ' cppcheck.xml || true
fi

exit 0
