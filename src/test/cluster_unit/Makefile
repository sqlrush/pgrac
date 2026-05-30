#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for pgrac cluster unit tests
#
# IDENTIFICATION
#    src/test/cluster_unit/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile.  Each test_*.c compiles to a
#    standalone executable that links cluster_version.o (the only
#    cluster object with no PG-internal dependencies).  See
#    spec-0.4-unit-test-framework.md §9.2 for design rationale.
#
#-------------------------------------------------------------------------

subdir = src/test/cluster_unit
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# Test source files (each becomes a standalone executable)
TESTS = test_cluster_basic test_cluster_version test_cluster_backend_types \
        test_cluster_wait_events test_cluster_errcodes test_cluster_guc \
        test_cluster_shmem test_cluster_signal test_cluster_views \
        test_cluster_gviews test_cluster_ic test_cluster_ic_envelope \
        test_cluster_ic_router \
        test_cluster_conf \
        test_cluster_ic_mock test_cluster_inject test_cluster_pgstat \
        test_cluster_debug test_cluster_shared_fs test_cluster_smgr \
        test_cluster_scn test_cluster_block_format test_cluster_itl_slot \
        test_cluster_buffer_desc test_cluster_pcm_lock test_cluster_bufmgr_pcm_hook test_cluster_gcs_dispatch test_cluster_gcs_block test_cluster_gcs_block_retransmit test_cluster_gcs_block_2way test_cluster_gcs_block_3way test_cluster_gcs_block_lost_write test_cluster_sinval test_cluster_sinval_ack test_cluster_stage2_acceptance test_cluster_tt_status test_cluster_tt_status_hint test_cluster_visibility_fork test_cluster_visibility_decide_scn test_cluster_snapshot_source test_cluster_itl_touch test_cluster_itl_wal test_cluster_uba \
        test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision \
        test_cluster_xlog test_cluster_tt_slot test_cluster_undo_segment \
        test_cluster_epoch test_cluster_fence test_cluster_reconfig \
        test_cluster_ges test_cluster_grd test_cluster_lmd test_cluster_lock_acquire \
        test_cluster_tt_slot_allocator test_cluster_itl_reader_real_triple \
        test_cluster_itl_cleanout test_cluster_visibility_inject test_cluster_itl_cleanout_perf \
        test_cluster_heap_lock_tuple test_cluster_perf_gates \
        test_cluster_subtrans test_cluster_multixact test_cluster_undo_format \
        test_cluster_undo_record test_cluster_undo_lifecycle test_cluster_cr

# Path to the cluster_version object (no PG deps, safe to link standalone).
CLUSTER_VERSION_O = $(top_builddir)/src/backend/cluster/cluster_version.o

# Path to cluster_guc.o + cluster_elog.o (needed by test_cluster_guc only).
# These reference PG backend symbols (DefineCustomIntVariable, etc.) that
# the test stubs out locally.
CLUSTER_GUC_O = $(top_builddir)/src/backend/cluster/cluster_guc.o
CLUSTER_ELOG_O = $(top_builddir)/src/backend/cluster/cluster_elog.o

# Include path for unit_test.h (this dir) and cluster headers.
override CPPFLAGS := -I. -I$(top_srcdir)/src/include $(CPPFLAGS)


# ----------
# Default target: build all tests
# ----------
all: $(TESTS)

CLUSTER_SHMEM_O = $(top_builddir)/src/backend/cluster/cluster_shmem.o
CLUSTER_SIGNAL_O = $(top_builddir)/src/backend/cluster/cluster_signal.o
CLUSTER_VIEWS_O = $(top_builddir)/src/backend/cluster/cluster_views.o
CLUSTER_IC_O = $(top_builddir)/src/backend/cluster/cluster_ic.o
CLUSTER_IC_ENVELOPE_O = $(top_builddir)/src/backend/cluster/cluster_ic_envelope.o
CLUSTER_IC_ROUTER_O = $(top_builddir)/src/backend/cluster/cluster_ic_router.o
CLUSTER_CONF_O = $(top_builddir)/src/backend/cluster/cluster_conf.o
CLUSTER_INJECT_O = $(top_builddir)/src/backend/cluster/cluster_inject.o
CLUSTER_PGSTAT_O = $(top_builddir)/src/backend/cluster/cluster_pgstat.o
CLUSTER_DEBUG_O = $(top_builddir)/src/backend/cluster/cluster_debug.o
CLUSTER_SHARED_FS_O = $(top_builddir)/src/backend/cluster/storage/cluster_shared_fs.o
CLUSTER_SHARED_FS_STUB_O = $(top_builddir)/src/backend/cluster/storage/cluster_shared_fs_stub.o
CLUSTER_SHARED_FS_LOCAL_O = $(top_builddir)/src/backend/cluster/storage/cluster_shared_fs_local.o
CLUSTER_SMGR_O = $(top_builddir)/src/backend/cluster/storage/cluster_smgr.o
CLUSTER_STARTUP_PHASE_O = $(top_builddir)/src/backend/cluster/cluster_startup_phase.o
CLUSTER_LMON_O = $(top_builddir)/src/backend/cluster/cluster_lmon.o
CLUSTER_LCK_O = $(top_builddir)/src/backend/cluster/cluster_lck.o
CLUSTER_DIAG_O = $(top_builddir)/src/backend/cluster/cluster_diag.o
CLUSTER_STATS_O = $(top_builddir)/src/backend/cluster/cluster_stats.o
CLUSTER_CSSD_O = $(top_builddir)/src/backend/cluster/cluster_cssd.o
CLUSTER_QVOTEC_O = $(top_builddir)/src/backend/cluster/cluster_qvotec.o
CLUSTER_FENCE_O = $(top_builddir)/src/backend/cluster/cluster_fence.o
CLUSTER_RECONFIG_O = $(top_builddir)/src/backend/cluster/cluster_reconfig.o
CLUSTER_VOTING_DISK_IO_O = $(top_builddir)/src/backend/cluster/cluster_voting_disk_io.o
CLUSTER_QUORUM_DECISION_O = $(top_builddir)/src/backend/cluster/cluster_quorum_decision.o
CLUSTER_SCN_O = $(top_builddir)/src/backend/cluster/cluster_scn.o
CLUSTER_GES_O = $(top_builddir)/src/backend/cluster/cluster_ges.o
CLUSTER_GRD_O = $(top_builddir)/src/backend/cluster/cluster_grd.o
CLUSTER_LMD_O = $(top_builddir)/src/backend/cluster/cluster_lmd.o
CLUSTER_LOCK_ACQUIRE_O = $(top_builddir)/src/backend/cluster/cluster_lock_acquire.o
# spec-2.30 D9: test_cluster_pcm_lock links cluster_pcm_lock.o to exercise
# the transition validator + counter accessors standalone (20 PG-runtime
# deps stubbed in the test binary).
CLUSTER_PCM_LOCK_O = $(top_builddir)/src/backend/cluster/cluster_pcm_lock.o
CLUSTER_GCS_O = $(top_builddir)/src/backend/cluster/cluster_gcs.o

# Most tests link only cluster_version.o.  test_cluster_guc /
# test_cluster_shmem / test_cluster_signal / test_cluster_views /
# test_cluster_gviews / test_cluster_ic / test_cluster_conf have
# separate rules because they also link additional cluster_*.o
# objects (the test files stub the PG backend symbols those
# objects reference).
SIMPLE_TESTS = $(filter-out test_cluster_guc test_cluster_shmem test_cluster_signal test_cluster_views test_cluster_gviews test_cluster_ic test_cluster_conf test_cluster_ic_mock test_cluster_inject test_cluster_pgstat test_cluster_debug test_cluster_shared_fs test_cluster_smgr test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision test_cluster_scn test_cluster_epoch test_cluster_fence test_cluster_reconfig test_cluster_ges test_cluster_grd test_cluster_lmd test_cluster_lock_acquire,$(TESTS))

# spec-2.4 D16: test_cluster_epoch links cluster_epoch.o standalone.
# cluster_epoch.c references ShmemInitStruct + cluster_shmem_register_region
# + pg_atomic_*; the test stubs all of these and exercises only the
# get_current accessor + initial-state contract.
CLUSTER_EPOCH_O = $(top_builddir)/src/backend/cluster/cluster_epoch.o
test_cluster_epoch: test_cluster_epoch.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_EPOCH_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_EPOCH_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

$(SIMPLE_TESTS): %: %.c unit_test.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_VERSION_O) -o $@

# test_cluster_guc additionally links cluster_guc.o + cluster_elog.o
# (the test stubs DefineCustomIntVariable so no PG backend is needed).
test_cluster_guc: test_cluster_guc.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_GUC_O) $(CLUSTER_ELOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GUC_O) $(CLUSTER_ELOG_O) -o $@

# test_cluster_shmem additionally links cluster_shmem.o + cluster_guc.o +
# cluster_elog.o (the test stubs RequestAddinShmemSpace, ShmemInitStruct,
# GetCurrentTimestamp, and elog so no PG backend is needed).
test_cluster_shmem: test_cluster_shmem.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_SHMEM_O) $(CLUSTER_GUC_O) $(CLUSTER_ELOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHMEM_O) $(CLUSTER_GUC_O) $(CLUSTER_ELOG_O) -o $@

# test_cluster_signal links cluster_signal.o.  cluster_signal.c calls
# SetLatch(MyLatch) which needs PG backend; the test stubs both.
test_cluster_signal: test_cluster_signal.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_SIGNAL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SIGNAL_O) -o $@

# test_cluster_views links cluster_views.o.  cluster_views.c calls
# fmgr / tuplestore / wait event lookup machinery; the test stubs all
# of them since the unit test only takes the SRF address (never invokes).
test_cluster_views: test_cluster_views.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_VIEWS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VIEWS_O) -o $@

# test_cluster_gviews shares cluster_views.o with test_cluster_views.
# It additionally provides a local cluster_node_id symbol (real backend
# gets it from cluster_guc.o; unit test stubs it as a plain int).
test_cluster_gviews: test_cluster_gviews.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_VIEWS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VIEWS_O) -o $@

# test_cluster_ic links cluster_ic.o standalone.  cluster_ic.c references
# pg_crc32c hardware functions and the ereport machinery (errstart /
# errcode / errmsg / errfinish) plus cluster_node_id; the test stubs all
# of them since the unit test only takes function addresses (never
# invokes a path that triggers ereport or CRC computation).
test_cluster_ic: test_cluster_ic.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_IC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_O) -o $@

# spec-2.3 D8: test_cluster_ic_envelope links cluster_ic_envelope.o
# standalone + the libpgport_srv archive (for the real pg_crc32c
# dispatch that cluster_ic_envelope.c uses).  ExceptionalCondition
# is stubbed in the test source (Assert-on-fail abort).
test_cluster_ic_envelope: test_cluster_ic_envelope.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_ENVELOPE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_ENVELOPE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.3 D4: test_cluster_ic_router links cluster_ic_router.o +
# cluster_ic_envelope.o (envelope_build called inside send_envelope) +
# libpgport_srv.a for CRC32C.  Stubs ereport family + cluster_ic_send_bytes
# + MyBackendType + MemoryContext + cluster_node_id locally.  Tests
# register_msg_type / get_msg_type_info / count_registered;send + dispatch
# behavior probed only at the validation layer (no actual wire send because
# the vtable stub is short-circuited via dest=self).
test_cluster_ic_router: test_cluster_ic_router.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_ROUTER_O) $(CLUSTER_IC_ENVELOPE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_ROUTER_O) $(CLUSTER_IC_ENVELOPE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# test_cluster_conf links cluster_conf.o standalone.  cluster_conf.c
# references ereport / ShmemInitStruct / AllocateFile / FreeFile /
# cluster_node_id / cluster_config_file / fmgr glue; all stubbed in
# test_cluster_conf.c since the unit test only takes addresses and
# reads constants (never invokes the parser body or SRF body).
test_cluster_conf: test_cluster_conf.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_CONF_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CONF_O) -o $@

# test_cluster_ic_mock links cluster_ic.o standalone (same as
# test_cluster_ic but exercises the mock vtable additions and the four
# cluster_ic_mock_* SRF entry points shipped at stage 0.26).  The unit
# test only takes addresses and reads constants -- the SRF bodies and
# any ereport / palloc / tuplestore paths are not invoked, so the
# stubs in test_cluster_ic_mock.c suffice.
test_cluster_ic_mock: test_cluster_ic_mock.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_IC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_O) -o $@

# test_cluster_inject links cluster_inject.o standalone.  cluster_inject.c
# references ereport / pg_atomic / pg_usleep / superuser / fmgr / tuple-
# store machinery; all stubbed in test_cluster_inject.c.  Tests pin the
# registry size, lookup, arm/disarm transitions via the public assign
# hook, and the should_skip / SRF linkability invariants.  End-to-end
# SRF + view behaviour is covered by cluster_tap t/015_inject.pl.
test_cluster_inject: test_cluster_inject.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_INJECT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_INJECT_O) -o $@

# test_cluster_pgstat links cluster_pgstat.o standalone.  cluster_pgstat.c
# references cluster_injection_armed_count (mirror sync) + ereport +
# fmgr / tuplestore machinery; all stubbed locally.  Tests pin the
# helper API (lookup / inc / set / read), lazy-init idempotency,
# sequential atomic accumulation, NULL safety, and SRF entry-point
# linkability.  End-to-end SQL behaviour is covered by cluster_tap
# t/016_perfmon.pl.
test_cluster_pgstat: test_cluster_pgstat.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_PGSTAT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_PGSTAT_O) -o $@

# test_cluster_debug links cluster_debug.o standalone.  cluster_debug.c
# is a cross-module aggregator that reads from seven other cluster_*.o
# files; the test stubs every public symbol it references (the SRF
# body itself is never invoked, so the stubs return inert defaults).
# Tests pin SRF entry-point linkability and the iterator API contracts
# (out-of-range index returns false; NULL out-pointers do not crash).
# End-to-end behaviour is covered by cluster_tap t/017_debug.pl.
test_cluster_debug: test_cluster_debug.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_DEBUG_O) $(CLUSTER_STARTUP_PHASE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_DEBUG_O) $(CLUSTER_STARTUP_PHASE_O) -o $@

# test_cluster_shared_fs links the three storage/ objects standalone:
# cluster_shared_fs.o (registry + dispatch), cluster_shared_fs_stub.o
# (placeholder vtable), cluster_shared_fs_local.o (single-node
# passthrough vtable).  The test stubs every PG backend symbol those
# objects reference (ereport, palloc, fd.c VFD layer, before_shmem_exit,
# GetRelationPath) because the unit test only takes addresses and reads
# static vtable contents.  End-to-end runtime behaviour is verified by
# cluster_tap t/018_shared_fs.pl.
test_cluster_shared_fs: test_cluster_shared_fs.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_O) \
		$(CLUSTER_SHARED_FS_STUB_O) $(CLUSTER_SHARED_FS_LOCAL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_O) \
		$(CLUSTER_SHARED_FS_STUB_O) $(CLUSTER_SHARED_FS_LOCAL_O) -o $@

# test_cluster_smgr links cluster_smgr.o + the three cluster_shared_fs
# objects standalone.  cluster_smgr.c references HTAB / md.c / fd.c /
# TablespaceCreateDbspace / ereport; the test stubs each one because
# the unit test only address-takes the sixteen f_smgr callback
# prototypes and exercises the four short-circuit branches of
# cluster_smgr_which_for() via direct function call (no smgrsw
# dispatch).  End-to-end runtime behaviour (PG 219 GUC=on) is
# verified by cluster_tap t/019_smgr_cluster.pl + make check
# --temp-config.
test_cluster_smgr: test_cluster_smgr.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_SMGR_O) \
		$(CLUSTER_SHARED_FS_O) $(CLUSTER_SHARED_FS_STUB_O) \
		$(CLUSTER_SHARED_FS_LOCAL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SMGR_O) \
		$(CLUSTER_SHARED_FS_O) $(CLUSTER_SHARED_FS_STUB_O) \
		$(CLUSTER_SHARED_FS_LOCAL_O) -o $@

# test_cluster_startup_phase links cluster_startup_phase.o standalone.
# cluster_startup_phase.c references ereport / GetCurrentTimestamp /
# timestamptz_to_str / cluster_phase legacy mirror / IsUnderPostmaster
# / cluster_inject.  The test stubs every one because the unit test
# only takes function addresses + exercises pure-function string
# lookups; behavior tests (transition rejection, Postmaster-once
# Assert) live in TAP t/060_postmaster_phases.pl where postmaster
# orchestration is real.
test_cluster_startup_phase: test_cluster_startup_phase.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_STARTUP_PHASE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_STARTUP_PHASE_O) -o $@

# test_cluster_lmon links cluster_lmon.o standalone (spec-1.11 Sprint A).
# cluster_lmon.c references shmem / lwlock / pqsignal / latch / proc /
# procsignal / interrupt / timestamp / memutils / ps_status helpers.
# Stubs in test_cluster_lmon.c cover every external; behavior tests
# (LMON spawn, phase 1 ready, kill -9 crash recovery) live in TAP
# t/061_lmon_skeleton.pl where postmaster orchestration is real.
test_cluster_lmon: test_cluster_lmon.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMON_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMON_O) -o $@

# test_cluster_lck links cluster_lck.o standalone (spec-1.12 Sprint A).
# Mirrors test_cluster_lmon pattern: cluster_lck.c references shmem /
# lwlock / pqsignal / latch / proc / procsignal / interrupt / timestamp
# / memutils / ps_status helpers; stubs in test_cluster_lck.c cover
# every external; behavior tests (LCK spawn, phase 2 ready, kill -9
# crash recovery) live in TAP t/062_lck_skeleton.pl where postmaster
# orchestration is real.
test_cluster_lck: test_cluster_lck.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LCK_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LCK_O) -o $@


# ----------
# Run all tests sequentially
# ----------
check: all
	@echo "# pgrac cluster unit tests"
	@total=0; failed=0; \
	for t in $(TESTS); do \
		echo "# Running $$t"; \
		./$$t || failed=$$((failed + 1)); \
		total=$$((total + 1)); \
	done; \
	if [ $$failed -ne 0 ]; then \
		echo "# $$failed of $$total binaries failed"; \
		exit 1; \
	else \
		echo "# All $$total binaries passed"; \
	fi


# ----------
# Standard PG targets
# ----------
clean distclean maintainer-clean:
	rm -f $(TESTS)

install installdirs uninstall installcheck:
	@:

.PHONY: all check clean distclean maintainer-clean install installdirs \
        uninstall installcheck

# test_cluster_diag links cluster_diag.o standalone (spec-1.13 Sprint A).
test_cluster_diag: test_cluster_diag.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_DIAG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_DIAG_O) -o $@

# test_cluster_stats links cluster_stats.o standalone (spec-1.14 Sprint A).
test_cluster_stats: test_cluster_stats.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_STATS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_STATS_O) -o $@

# test_cluster_cssd links cluster_cssd.o standalone (spec-2.5 Sprint A
# Step 2).  cluster_cssd.c references shmem / lwlock / proc_exit /
# GetCurrentTimestamp;all stubbed in test_cluster_cssd.c since the
# unit test exercises status_to_string / payload byte-layout / shmem
# init idempotency / handler 4-硬约束 source-grep / deadband factor
# math.  Runtime CssdMain integration tests live in TAP 065 + 085
# (Step 7).
test_cluster_cssd: test_cluster_cssd.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_CSSD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CSSD_O) -o $@

# test_cluster_qvotec links cluster_qvotec.o standalone (spec-2.6 Sprint
# A Step 1).  cluster_qvotec.c references shmem / proc_exit /
# GetCurrentTimestamp / WaitLatch;all stubbed in test_cluster_qvotec.c
# since the unit test exercises ClusterVotingSlot byte-layout +
# ClusterQvotecShmem byte-layout + lifecycle accessor surface +
# in_quorum lease helper logic + ProcSignal flag set/clear.  Real
# poll cycle / disk I/O / multiplexer integration tested in TAP
# 095 + 096 (Step 5).
test_cluster_qvotec: test_cluster_qvotec.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_QVOTEC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_QVOTEC_O) -o $@

# spec-2.28 Sprint A Step 1 — cluster_fence.o standalone link.  Stubs all
# PG runtime deps (shmem / lwlock / timestamp / errlog).  T-fence-1 verifies
# GUC default + shmem layout init + flag default + 6 API survival.  Real
# ProcSignal handler / postgres.c hook / LMON broadcast / postmaster
# trigger land Steps 2-3.
test_cluster_fence: test_cluster_fence.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_FENCE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_FENCE_O) -o $@

# spec-2.29 Sprint A Step 1: test_cluster_reconfig links cluster_reconfig.o
# + cluster_epoch.o standalone.  Foundation layer — shmem layout + sizeof
# bounds (P2.8) + observe_remote CAS-loop semantics (D18b) +
# advance_for_reconfig pre/post snapshot (D18) + CLUSTER_EPOCH_OBSERVE_MAX_JUMP
# constant.  Real LMON tick body / ProcessInterrupts wiring land Step 2.
test_cluster_reconfig: test_cluster_reconfig.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_RECONFIG_O) $(CLUSTER_EPOCH_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_RECONFIG_O) $(CLUSTER_EPOCH_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.6 Sprint A Step 2 D3: cluster_voting_disk_io.o standalone test.
# Real syscalls (open / pread / pwrite / fdatasync) on a temp file —
# no PG runtime needed.  CRC32C symbols (pg_comp_crc32c hardware
# pointer + slow / sb8 / armv8 / sse42 backends) come from
# libpgport_srv.a which selects the correct backend per platform
# at configure time (was: hardcoded pg_crc32c_armv8_srv.o which
# only exists on aarch64 — fast-gate Linux x86 broke).
test_cluster_voting_disk_io: test_cluster_voting_disk_io.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_VOTING_DISK_IO_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VOTING_DISK_IO_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.6 Sprint A Step 2 D4: cluster_quorum_decision.o standalone test.
# Pure logic — no shmem, no I/O, no logging.  Synthetic slot matrix
# tests majority math + collision direction (Q6 v0.2 newer-self-FATAL).
test_cluster_quorum_decision: test_cluster_quorum_decision.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_QUORUM_DECISION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_QUORUM_DECISION_O) -o $@

# test_cluster_scn links cluster_scn.o standalone (spec-1.15 Sprint A).
# cluster_scn.c references shmem / lwlock / fmgr / superuser / ereport
# / GetCurrentTimestamp / cluster_injection_should_skip; all stubbed in
# test_cluster_scn.c since the unit test only exercises pure-function
# cmp helpers (scn_time_cmp / scn_total_cmp / scn_recovery_cmp) and
# the inline encoding macros.  Runtime advance/observe paths (LWLock-
# protected SCN counter) are TAP-tested in t/065_scn_encoding.pl.
test_cluster_scn: test_cluster_scn.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_SCN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SCN_O) -o $@

# spec-2.13 D6: test_cluster_ges links cluster_ges.o standalone.
# cluster_ges.c references ShmemInitStruct + cluster_shmem_register_region
# + pg_atomic_*; test stubs all of these (union force-align for shmem
# buffer per L105) and exercises the 2 handler stubs + 2 accessors real
# behavior (T-ges-1 a/b/c/d/e).
test_cluster_ges: test_cluster_ges.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_O) -o $@

# spec-2.14 D11: test_cluster_grd links cluster_grd.o standalone.
# cluster_grd.c references ShmemInitStruct + cluster_shmem_register_region
# + cluster_conf_lookup_node + pg_atomic_*; test stubs all of these
# (union force-align L105 + mock declared list) and exercises ClusterResId
# encode/decode + hash distribution + sparse-node master mapping + 4-class
# is_cluster_aware classifier (T-grd-1 a/b/c/d/e/f).
test_cluster_grd: test_cluster_grd.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.19 D13: test_cluster_lmd links cluster_lmd.o standalone.
# cluster_lmd.c references ShmemInitStruct + cluster_shmem_register_region
# + LWLock + ConditionVariable + pg_atomic_* + GetCurrentTimestamp; test
# stubs all of these (union force-align for shmem buffer per L105) and
# exercises HC4 exact-predicate (T-lmd-5 critical — `>= LMD_READY` false-
# positive regression防御 / L124 NEW lesson) + HC2 4-state semantic split
# + HC3 producer wake + HC6 skeleton "no graph maintenance" + L122
# alphabetic ordering.
test_cluster_lmd: test_cluster_lmd.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.20 D13: test_cluster_lock_acquire links cluster_lock_acquire.o
# standalone.  cluster_lock_acquire.c references cluster_lms_is_ready /
# cluster_lms_enabled / cluster_grd / pg_atomic_*; test stubs all PG
# backend symbols locally and exercises S1-S7 individual step API + top-
# level dispatch + HC1 fail-closed + I1 monotonic forward transition.
test_cluster_lock_acquire: test_cluster_lock_acquire.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LOCK_ACQUIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LOCK_ACQUIRE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.30 D9: test_cluster_pcm_lock links cluster_pcm_lock.o standalone.
# cluster_pcm_lock.c references 20 PG-runtime symbols (ShmemInit* / LWLock* /
# hash_* / GetCurrentTimestamp / ereport / NBuffers / pg_atomic_* /
# cluster_node_id / cluster_shmem_register_region / cluster_injection_*).
# Test stubs all of these and drives a tiny fake shared HTAB so the real
# acquire/release/upgrade/downgrade/query paths are exercised standalone.
test_cluster_pcm_lock: test_cluster_pcm_lock.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_PCM_LOCK_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_PCM_LOCK_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.31 D9 v0.5: test_cluster_bufmgr_pcm_hook verifies the bufmgr
# LockBuffer/LockBufferForCleanup PCM hook layer in isolation by using a
# fake LockBuffer wrapper that mirrors the D3 hook structure.  Links
# cluster_pcm_lock.o (real acquire/release behavior) + reuses the same
# stub family as test_cluster_pcm_lock.
test_cluster_bufmgr_pcm_hook: test_cluster_bufmgr_pcm_hook.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_PCM_LOCK_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_PCM_LOCK_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.32 D9: test_cluster_gcs_dispatch verifies GCS request protocol
# skeleton invariants (msg_type enum 12/13, payload ABI lock, dispatch
# handler symbols, observability accessors).  Link-surface coverage only;
# behavioral coverage in cluster_tap t/110_gcs_loopback.pl.
test_cluster_gcs_dispatch: test_cluster_gcs_dispatch.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GCS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GCS_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.33 D13: test_cluster_gcs_block verifies GCS block-shipping
# substrate compile-time invariants (msg_type 14/15, payload ABI sizes,
# field offsets, deterministic hash math, wait events + tranches
# distinct, BLCKSZ cross-check).  Pure header-only checks — does NOT
# link cluster_gcs_block.o.  Behavioral coverage (HC82 / HC83 / HC84 /
# HC88 / HC89 paths) lives in cluster_tap t/111.
test_cluster_gcs_block: test_cluster_gcs_block.c unit_test.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.34 D13: test_cluster_gcs_block_retransmit verifies reliability
# hardening compile-time invariants (status enum 7→8, dedup entry 8312B
# fixed-size, dedup result enum, retry math, LWTRANCHE distinct, wait
# events distinct, 85 cluster wait events).  Header-only;  behavioral
# coverage in cluster_tap t/112.
test_cluster_gcs_block_retransmit: test_cluster_gcs_block_retransmit.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.35 D17:  test_cluster_gcs_block_2way verifies CF 2-way protocol
# compile-time invariants (msg_type 16, status 8, GcsBlockForwardPayload
# 64B, reply header reserved 重解读 sizeof 不变, dedup FORWARDED_DUPLICATE
# return 5, helper round-trip).  Header-only;  behavioral coverage in
# cluster_tap t/113.
test_cluster_gcs_block_2way: test_cluster_gcs_block_2way.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.36 D11/D12:  test_cluster_gcs_block_3way verifies CF 3-way
# protocol compile-time invariants (msg_type 17/18, reply status 9/10/11,
# GcsBlockInvalidate{,Ack}Payload 64B, GrdEntry sizeof 248, dedup state
# INVALIDATE_IN_FLIGHT=6, S barrier helpers, broadcast decision tree
# branches).  Header-only;  behavioral coverage in cluster_tap t/114
# (2-node) + t/115 (3-node).
test_cluster_gcs_block_3way: test_cluster_gcs_block_3way.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.37 D13:  test_cluster_gcs_block_lost_write verifies PI watermark
# + lost-write detection MVP compile-time invariants (reply status 12,
# GcsBlockForwardPayload reserved 重解读 sizeof 64B 不变, GrdEntry sizeof
# 256, all 6 watermark helper prototypes linkable, 4 counter accessors,
# GUC variable).  Header-only;  behavioral coverage in cluster_tap t/116.
test_cluster_gcs_block_lost_write: test_cluster_gcs_block_lost_write.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.38 D11/D12:  test_cluster_sinval verifies SI Broadcaster skeleton
# compile-time invariants (msg_type 7,  LMON-mediated SINVAL fanout
# producer mask,  SinvalBroadcastHeader sizeof 24B,  SharedInvalidation-
# Message sizeof 16B PG ABI lock,  CLUSTER_SINVAL_BATCH_MAX 64,  all 6
# helper prototypes linkable,  9 counter accessors,  SinvalBcastProcess
# AuxProcType valid,  B_SINVAL_BCAST BackendType valid,  53R94 SQLSTATE
# encodable,  5 NEW regression test contracts for HC132/HC133/HC134).
# Header-only;  behavioral coverage in cluster_tap t/117.
test_cluster_sinval: test_cluster_sinval.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.39 D15:  test_cluster_sinval_ack verifies the ack/barrier wire
# ABI + flag encoding + enum invariants (msg_type 19 not colliding with
# spec-2.32 GCS_REQUEST=12, SinvalAckHeader sizeof 24B, status tri-state
# DONE/DROPPED/RESET_PENDING, SINVAL_REQUIRES_ACK + SINVAL_RESET_ALL_
# BROADCAST flag values, SINVAL_KNOWN_FLAGS extension, 6 NEW counter
# accessors linkable, ClusterSinvalAckMode 2 enum values, 53R95 SQLSTATE
# encodable).  Header-only;  behavioral coverage in cluster_tap t/118.
test_cluster_sinval_ack: test_cluster_sinval_ack.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.40 D8:  test_cluster_stage2_acceptance — 8 static contract
# tests verifying Stage 2 final surface snapshot (catversion >= 202605460,
# msg_types 7/12-19 cumulative, capability counter symbols linkable,
# 5 fault inject point name strings, wait events count 91 snapshot,
# SQLSTATE 53R60/90-95 encodable, GUC enum + shmem_max_regions
# boot_val=64 snapshot, RESERVED_0=0 sentinel).  Header-only;
# behavioral coverage in cluster_tap t/200-202.
test_cluster_stage2_acceptance: test_cluster_stage2_acceptance.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.1 D9:  test_cluster_tt_status — 22 真 C 单测 verifying the Undo
# TT status foundation static contract (24B ClusterTTStatusKey with
# explicit _reserved2 v0.4 N9 padding, 32B ClusterUndoTTSlotRef v0.4 M4,
# 5-value ClusterTTStatus enum, public API + GUC defaults + LWTRANCHE
# slot + self-consumer counter linkable, ITL reader linkable, no
# SUBCOMMITTED in spec-3.1 enum).  Header-only;  behavioral coverage in
# cluster_tap t/203.
test_cluster_tt_status: test_cluster_tt_status.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.2 D9:  test_cluster_tt_status_hint — 18 static contract tests
# verifying cross-node TT status hint wire ABI (32B with embedded
# ClusterTTStatusKey at offset 8, msg_type 20 distinct from sinval/ack,
# producer mask LMON, 6 counter getters incl. drop_unknown_version_count
# v0.3 N5, GUC enum 2-value v0.3 删 commit_only, 53R97 SQLSTATE
# encodable, L176 v1.66 amend positive presence).  Header-only;
# behavioral coverage in cluster_tap t/204.
test_cluster_tt_status_hint: test_cluster_tt_status_hint.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.2 D10:  test_cluster_visibility_fork — 12 static contract tests
# verifying HeapTupleSatisfiesMVCC cluster fork inputs (ClusterUndoTTSlotRef
# 32B + field offsets + placeholder/self-origin sentinels + build_key field
# carry + 53R97 encodable + D5b inject API linkable + ENABLE_INJECTION
# stub return-false + CLUSTER_ITL_SLOT_UNALLOCATED = 255 + no is_xid_local_
# origin symbol enforcement + ClusterTTStatus 5-value enum stable).  No
# HeapTupleSatisfiesMVCC behavioral testing (requires real PG backend;
# behavioral coverage in cluster_tap t/204).
test_cluster_visibility_fork: test_cluster_visibility_fork.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.3 D11:  test_cluster_visibility_decide_scn — 22 static +
# behavioral mock tests verifying cluster_visibility_decide_by_scn() and
# the ClusterVisibilityDecision enum (L180).  Covers InvalidScn ->
# UNKNOWN sentinel handling, scn_time_cmp ordering across node_id high
# bits (R1 P0), and the COMMITTED / ABORTED / CLEANED_OUT dispatch
# mocks.  Standalone executable; behavioral coverage of the full
# HeapTupleSatisfiesMVCC fork in cluster_tap t/205.  scn_time_cmp comes
# from cluster_scn.o, which we link to support the cross-node SCN
# comparison tests.
test_cluster_visibility_decide_scn: test_cluster_visibility_decide_scn.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.3 D12:  test_cluster_snapshot_source — 18 static + ABI tests
# verifying SnapshotData explicit 24B cluster tail (D1), the SnapshotSource
# enum (LOCAL=0, CLUSTER=1), and the ClusterTTStatusHint V1/V2 wire ABI
# layout (D8: V1 32B + V2 40B with commit_scn at offset 32).
test_cluster_snapshot_source: test_cluster_snapshot_source.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4a D11: test_cluster_itl_touch — 23 static + ABI tests
# verifying ClusterItlTouchHandle 24B layout, writer API symbols
# (alloc_or_reuse / stamp_*), xact hook symbols (precommit_finish /
# abort_finish), subxact guard, ItlFlags enum, and the
# no-Buffer-pin/no-RegisterXactCallback negative compile assertions.
# Behavioral coverage of register/foreach/reset list dynamics belongs
# to cluster_tap t/206 because they require a PG memory context.
test_cluster_itl_touch: test_cluster_itl_touch.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4a D12: test_cluster_itl_wal — 18 static ABI tests for the
# heap WAL ITL delta block-local array layout (xl_heap_itl_delta 24B
# + xl_heap_itl_delta_block 4B header + flexible array), the
# XLH_*_ITL_DELTA flag bit (1<<7) positions, and no-collision with
# existing XLH_*_* flags.
test_cluster_itl_wal: test_cluster_itl_wal.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4b D10: test_cluster_uba — 24 static + inline-helper tests
# covering the UBA encode/decode helpers + offset/id sentinel separation
# + uba_origin_node_id derivation + spec-3.4b constants.
CLUSTER_UBA_O = $(top_builddir)/src/backend/cluster/cluster_uba.o
test_cluster_uba: test_cluster_uba.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UBA_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UBA_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4b D11: test_cluster_tt_slot_allocator — 30 tests linking the
# real cluster_tt_slot.o + mocked ShmemInitStruct / LWLock / ereport so
# allocator behaviour (3-tier recycle, OVERFLOW, multi-instance, fail-
# closed) is exercised end-to-end without postmaster.
CLUSTER_TT_SLOT_O = $(top_builddir)/src/backend/cluster/cluster_tt_slot.o
test_cluster_tt_slot_allocator: test_cluster_tt_slot_allocator.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_TT_SLOT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_TT_SLOT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4b D12: test_cluster_itl_reader_real_triple — 18 tests for
# the 3-branch reader (FREE / UBA_is_invalid legacy / real decode) on
# a hand-constructed Page + ItlSlotData fixture.
CLUSTER_ITL_O = $(top_builddir)/src/backend/cluster/cluster_itl.o
test_cluster_itl_reader_real_triple: test_cluster_itl_reader_real_triple.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_ITL_O) $(CLUSTER_UBA_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_ITL_O) $(CLUSTER_UBA_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4c D10: test_cluster_itl_cleanout — 20 tests covering the
# lazy cleanout helper symbol surface + can_stamp() pure-data
# boundary cases (NULL / wrong flags / xid mismatch / commit_scn
# already set / invalid expected_scn) + lazy() outer-guard
# short-circuits reachable via InvalidBuffer / InvalidScn /
# InvalidTransactionId / slot_idx out-of-bound.  Links real
# cluster_itl_cleanout.o; bufmgr APIs are locally stubbed.
CLUSTER_ITL_CLEANOUT_O = $(top_builddir)/src/backend/cluster/cluster_itl_cleanout.o
test_cluster_itl_cleanout: test_cluster_itl_cleanout.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_ITL_CLEANOUT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_ITL_CLEANOUT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4c D11: test_cluster_visibility_inject — 16 tests covering
# the D5b inject UDF surface + A1 install_local + F4 delete_exact
# (D6) + F5 lookup_exact verify chain at the symbol / struct-layout
# contract level.  Real overlay behaviour is exercised by
# cluster_tap t/208.  Local stubs only — no PG core link required.
test_cluster_visibility_inject: test_cluster_visibility_inject.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4c D13: test_cluster_itl_cleanout_perf — 8 tests covering
# the D14 per-page aggregate layout (ClusterItlPagedHandle
# slot_indices bound = CLUSTER_ITL_INITRANS_DEFAULT, foreach_per_page
# symbol linkable) + ClusterItlTouchHandle 24B regression so the
# qsort comparator's offset assumptions stay frozen.  Behavioural
# coverage of dedupe / qsort / batch stamp lives in cluster_tap.
test_cluster_itl_cleanout_perf: test_cluster_itl_cleanout_perf.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4d D13: test_cluster_heap_lock_tuple — 19 tests covering the
# spec-3.4d ABI / lock-only ITL enum / SQLSTATE / WAL flag / counter
# symbol contracts.  Behavioral coverage of heap_lock_tuple full path
# (raw_xmax scan + ACTIVE TT install + wait_policy fail-closed) lives
# in cluster_tap t/209.  Local stubs only (no PG core link required).
test_cluster_heap_lock_tuple: test_cluster_heap_lock_tuple.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.4e D5: test_cluster_perf_gates — 10 pure ABI / threshold /
# symbol-link tests for 5-tier × 4 workload class perf gate contract.
# No SQL UDF / backend behavior / grep assertions (spec-3.4e F5).
test_cluster_perf_gates: test_cluster_perf_gates.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.5 D12: test_cluster_subtrans — 12 pure ABI / offsetof / sizeof /
# enum / SQLSTATE / symbol-link tests for SUBTRANS cross-node visibility
# module.  Behavioural lifecycle / emit / lazy follow coverage lives in
# cluster_tap t/210 + t/211.
test_cluster_subtrans: test_cluster_subtrans.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.6 D10: test_cluster_multixact — 16 pure ABI / offsetof / sizeof /
# enum / SQLSTATE / symbol-link tests for MULTIXACT reader/member-resolution
# module + V4 sidecar wire ABI + ITL marker page-format flag.
# Behavioural lifecycle / overlay install / resolve_visibility coverage
# lives in cluster_tap t/212.
test_cluster_multixact: test_cluster_multixact.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.7 D12: test_cluster_undo_format — 12 pure ABI / offsetof /
# sizeof / magic / enum / SQLSTATE tests for Undo Record Format +
# Allocator module + HC211-HC217 ABI lock.  Behavioural / DML emit /
# allocator runtime coverage lives in cluster_tap t/213 (D13).
test_cluster_undo_format: test_cluster_undo_format.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.7 D12: test_cluster_undo_record — 12 encode/decode round-trip
# tests for UndoRecordHeader + 4 payloads + slot dir addressing.
test_cluster_undo_record: test_cluster_undo_record.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.8 D12: test_cluster_undo_lifecycle — 10 pure ABI / enum /
# SQLSTATE / sizeof / offsetof tests for Undo Segment Lifecycle MVP +
# Autoextend.  Behavioural lifecycle / autoextend / concurrency coverage
# lives in cluster_tap t/214.
test_cluster_undo_lifecycle: test_cluster_undo_lifecycle.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.9 D10: test_cluster_cr — ABI / SQLSTATE + the 4 inverse-apply
# helpers (cluster_cr_apply.o) on a synthetic ITL page.  Chain walker /
# tuple remap / visibility / MVCC gate coverage lives in cluster_tap t/215.
CLUSTER_CR_APPLY_O = $(top_builddir)/src/backend/cluster/cluster_cr_apply.o
test_cluster_cr: test_cluster_cr.c unit_test.h \
		$(CLUSTER_CR_APPLY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_APPLY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@
