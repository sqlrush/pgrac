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

# Reason: TwoPhaseCallback ABI (twophase_rmgr.h) fixes the recover callback
# signature to (TransactionId, uint16, void *, uint32); recdata cannot be
# const-qualified without breaking the shared callback table type (spec-3.15).
CLUSTER_ABI_SUPP=(
  --suppress=constParameterPointer:src/backend/cluster/cluster_tt_2pc.c
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
  # Reason: spec-4.5a D1 cluster_shared_fs_sharedfs.c mirrors the local
  # backend vtable callbacks (read/write/nblocks/truncate/immedsync) and
  # hits the identical constParameterCallback false positive -- the handle
  # parameter type is fixed by the ClusterSharedFsOps function-pointer
  # signatures and cannot be const-widened without changing the vtable
  # contract.  Same rationale and treatment as the local backend above.
  --suppress=constParameterCallback:src/backend/cluster/storage/cluster_shared_fs_sharedfs.c
  # Reason: spec-4.5a D1 sharedfs read/write/nblocks/truncate/immedsync use
  # the same Assert(handle != NULL && handle->opened); handle->vfd pattern as
  # the local backend -- PG Assert is non-trapping to cppcheck, so the guard
  # reads as a redundant null check.  Same false positive, same treatment.
  --suppress=nullPointerRedundantCheck:src/backend/cluster/storage/cluster_shared_fs_sharedfs.c
  # Reason: spec-4.5a D2 PgracSharedControl._pad is deterministic alignment
  # padding (the struct is written/read whole via pg_pwrite/pg_pread and its
  # CRC covers [0, offsetof(crc)), so padding must be zeroed, not omitted);
  # cppcheck cannot see the raw-I/O use of the member.
  --suppress=unusedStructMember:src/backend/cluster/storage/cluster_shared_fs_sharedfs.c
  # Reason: spec-4.5a D12 the unit test's MirrorSharedControl deliberately
  # mirrors the on-disk PgracSharedControl ABI byte-for-byte (_pad / crc
  # included) so sizeof/offset asserts pin the layout; the mirror members
  # are never read by name -- same false-positive class as above.
  --suppress=unusedStructMember:src/test/cluster_unit/test_cluster_shared_fs_sharedfs.c
  # Reason: stage-1.3 cluster_shmem_register_region uses the same
  # Assert(region != NULL); region->name pattern that triggers the
  # PG-Assert false positive (PG's Assert is non-trapping to cppcheck).
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_shmem.c
  # Reason: same Assert(ptr != NULL); ptr->field defensive pattern as
  # cluster_shmem.c above -- PG Assert is non-trapping to cppcheck, so it
  # flags the post-Assert null check as redundant in these cluster files.
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_dl_lock.c
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_hw_snapshot.c
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_ts_lock.c
  # Reason: same PG-Assert-non-trapping pattern -- Assert(lease != NULL);
  # lease->next in cluster_oid_lease_consume (spec-6.14 D6).
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_oid_lease.c
  # Reason: spec-5.22e D5 cluster_undo_horizon_cluster_floor uses the same
  # Assert(out_floor != NULL && out_reason != NULL && out_blame_node != NULL);
  # then writes *out_floor / *out_reason / *out_blame_node -- PG Assert is
  # non-trapping to cppcheck, so the four post-Assert out-param writes read as
  # redundant null checks (identical false positive to cluster_shmem.c above).
  --suppress=nullPointerRedundantCheck:src/backend/cluster/cluster_undo_horizon.c
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
  # Reason: spec-2.5 D1 cluster_cssd.h uses pg_attribute_packed() macro on
  # ClusterCssdHeartbeatPayload (12-byte wire format).  Without packed the
  # struct grows to 16 bytes (uint64 forces 8-byte struct alignment).
  # cppcheck 2.20 cannot expand pg_attribute_packed() and reports syntaxError
  # on the trailing identifier `ClusterCssdHeartbeatPayload`.  Compile-time
  # ABI lock = StaticAssertDecl(sizeof == 12) in cluster_cssd.c.
  --suppress=syntaxError:src/include/cluster/cluster_cssd.h
)

# Reason: standalone cluster unit tests define PG-core and pgrac stubs with
# signatures that intentionally mirror non-const production APIs.  cppcheck
# 2.20 flags many stub parameters as constParameterPointer candidates because
# the stub bodies are no-ops.  Changing those signatures would stop matching
# the production symbol contracts under test, so suppress this style-only
# finding for cluster_unit test stubs.
UNIT_STUB_SUPP=(
  --suppress=constParameterPointer:src/test/cluster_unit/test_cluster_*.c
  # spec-3.18 D1/D7: test_cluster_undo_buf reuses a single `img` scratch
  # pointer to hold cluster_undo_buf_pin() results across U-cases.  The
  # const/redundant-assignment/null-recheck findings are artifacts of that
  # scratch-reuse + the UT_ASSERT_NOT_NULL() macro, not production code;
  # the shmem ShmemInitStruct stub's malloc() cannot fail in a single-
  # threaded unit test (abort-on-OOM is the harness default).
  --suppress=constVariablePointer:src/test/cluster_unit/test_cluster_undo_buf.c
  --suppress=nullPointerRedundantCheck:src/test/cluster_unit/test_cluster_undo_buf.c
  --suppress=redundantAssignment:src/test/cluster_unit/test_cluster_undo_buf.c
  --suppress=unreadVariable:src/test/cluster_unit/test_cluster_undo_buf.c
  --suppress=nullPointerOutOfMemory:src/test/cluster_unit/test_cluster_undo_buf.c
  # spec-3.18 D2: the FPI/delta apply tests fill their output buffer via
  # cluster_undo_apply_block_write_{fpi,delta}, which cppcheck cannot model;
  # the CI cppcheck version flags reads of the filled image (oh->block_lsn /
  # bh->block_lsn) as uninitvar despite the buffer being memset/zero-init.
  --suppress=uninitvar:src/test/cluster_unit/test_cluster_stage3_acceptance.c
  # spec-3.1 D9: tt_status test stub parameters mirror real prototypes
  # whose pointer args can't be widened to const without breaking callers.
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_tt_status.c
  # spec-3.2 D10: cluster_visibility_inject test stub same rationale.
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_visibility_fork.c
  # spec-1.1 / spec-1.2 unit stubs mirror ClusterSharedFsOps callback
  # signatures.  cppcheck 2.20 suggests const-widening the no-op test
  # callbacks, but doing so would stop matching the production vtable ABI.
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_shared_fs.c
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_smgr.c
  # spec-3.4a D5/D11: ClusterItlTouchCallback and foreach use PG-style
  # `void *arg` callback ABI; const-widening would break production signatures.
  --suppress=constParameterCallback:src/backend/cluster/cluster_itl_touch.c
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_itl_touch.c
  # spec-3.4c/3.4d: local cluster_unit stubs mirror production out-parameter
  # APIs; const-widening the stubs would stop matching the symbol contracts.
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_visibility_inject.c
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_itl_cleanout_perf.c
  --suppress=constParameterCallback:src/test/cluster_unit/test_cluster_heap_lock_tuple.c
)

echo "## cppcheck $(cppcheck --version)"
echo "Scanning: ${CLUSTER_DIRS[*]}"

# --error-exitcode=0 keeps the exit non-fatal (warn-only).  Findings
# go to cppcheck.xml; a human-readable summary is built downstream.
cppcheck \
  --enable=warning,style,performance,portability \
  "${GLOBAL_SUPP[@]}" \
  "${CLUSTER_ABI_SUPP[@]}" \
  "${PG_HEADER_SUPP[@]}" \
  "${SHAREDFS_SUPP[@]}" \
  "${UNIT_STUB_SUPP[@]}" \
  --inline-suppr \
  --error-exitcode=0 \
  --xml --xml-version=2 \
  -I src/include \
  -DUSE_PGRAC_CLUSTER=1 \
  '-DHEAPAM_SOURCE_PATH="src/backend/access/heap/heapam.c"' \
  '-DTT_LOCAL_SOURCE_PATH="src/backend/cluster/cluster_tt_local.c"' \
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
