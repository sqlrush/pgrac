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

# Suppressions: passed as --suppress= CLI flags (one per finding).
# We avoid --suppressions-list because cppcheck 2.13's file-parser is
# very strict about format and rejects mixed-comment files with "Failed
# to add suppression. No id." even when valid entries are present.
# Each suppression below has a one-line "Reason:" comment.  See
# pgrac/docs/ci-static-analysis-baseline-stage0.27.md for the baseline
# scan that generated each entry.

# Reason: PG framework noise -- skip checks that fire on PG conventions.
GLOBAL_SUPP=(
  --suppress=unusedFunction
  --suppress=missingInclude
  --suppress=missingIncludeSystem
  --suppress=unmatchedSuppression
)

# Reason: PG-upstream headers reachable via -I src/include but outside
# spec-0.27.5 §1.2 scope (pgrac is only responsible for cluster code).
PG_HEADER_SUPP=(
  --suppress=preprocessorErrorDirective:src/include/storage/s_lock.h
  --suppress=nullPointerRedundantCheck:src/include/nodes/pg_list.h
  --suppress=nullPointerRedundantCheck:src/include/storage/bufpage.h
  --suppress=constParameterPointer:src/include/lib/ilist.h
  # Reason: PG-upstream xlogreader.h reachable via cluster_smgr.c -> smgr.h
  # transitive include; not pgrac code per spec-0.27.5 §1.2 scope.
  --suppress=constParameterPointer:src/include/access/xlogreader.h
  # Reason: spec-1.10.1 (F1 hardening) made cluster_startup_phase.h include
  # storage/lwlock.h to expose ClusterPhaseSharedState; lwlock.h transitively
  # pulls in port/atomics/generic.h.  Two PG-upstream findings surface there
  # (constParameterPointer + integerOverflowCond on signed -INT_MIN); both
  # are out of pgrac scope per spec-0.27.5 §1.2.
  --suppress=constParameterPointer:src/include/port/atomics/generic.h
  --suppress=integerOverflowCond:src/include/port/atomics/generic.h
)

# Reason: PG's Assert() is non-trapping by spec to cppcheck (it has no
# noreturn attribute on the failure path), so cppcheck treats
# `Assert(p != NULL); p->member` as "p was checked redundantly" rather
# than "Assert is the runtime guard".  Same pattern PG itself uses
# throughout, e.g. mdread / mdwrite -- spec-0.27.5 baseline already
# suppressed two PG headers for the same reason.  Adding our own files
# keeps the policy consistent.
SHAREDFS_SUPP=(
  --suppress=nullPointerRedundantCheck:src/backend/cluster/storage/cluster_shared_fs_local.c
  # constParameterCallback: cppcheck flags vtable callbacks that
  # don't write through `handle` as candidates for `const
  # ClusterSharedFsHandle *`.  But the parameter type is fixed by
  # ClusterSharedFsOps' function-pointer signatures (cluster_shared_fs.h
  # §2.1), so any const-correctness adjustment would have to flow
  # through the entire vtable contract -- spec-1.1 deliberately keeps
  # the vtable shape fixed across stages.  Suppress at the callback
  # implementation file.
  --suppress=constParameterCallback:src/backend/cluster/storage/cluster_shared_fs_local.c
  # Reason: stage-1.3 cluster_shmem_register_region uses the same
  # Assert(region != NULL); region->name pattern that triggers the
  # PG-Assert false positive (PG's Assert is non-trapping to cppcheck).
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_shmem.c
  # Reason: spec-2.3 D1 cluster_ic_envelope.h uses pg_attribute_packed()
  # macro from c.h to honor spec-2.0 §4 frozen offsets (uint64 epoch at
  # offset 12 / scn at offset 20 are non-8-aligned naturally; without
  # __attribute__((packed)) the struct grows to 40 bytes).  cppcheck 2.13
  # cannot expand this PG macro and parses the trailing identifier
  # `ClusterICEnvelope` after `pg_attribute_packed()` as a syntax error.
  # The compile-time guarantee is the 4 StaticAssertDecl in
  # cluster_ic_envelope.c (sizeof + 3 offsetof); cppcheck's syntax
  # confusion is purely a macro-expansion gap, not a code defect.
  --suppress=syntaxError:src/include/cluster/cluster_ic_envelope.h
)

echo "## cppcheck $(cppcheck --version)"
echo "Scanning: ${CLUSTER_DIRS[*]}"

# --error-exitcode=0 keeps the exit non-fatal (warn-only).  Findings
# go to cppcheck.xml; a human-readable summary is built downstream.
cppcheck \
  --enable=warning,style,performance,portability \
  "${GLOBAL_SUPP[@]}" \
  "${PG_HEADER_SUPP[@]}" \
  "${SHAREDFS_SUPP[@]}" \
  --inline-suppr \
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

# Stage 0.30: strict-mode baseline diff.  Exits non-zero if any new
# finding compared to the baseline established at spec-0.27.5 §6
# (currently 0 findings).  The CI workflow no longer wraps this step
# in continue-on-error, so a regression here fails the Security job.
if [ -x scripts/ci/baseline-diff.py ]; then
  echo "## baseline-diff (strict mode)"
  python3 scripts/ci/baseline-diff.py \
    --baseline scripts/ci/cppcheck-baseline.md \
    --current cppcheck.xml
fi
