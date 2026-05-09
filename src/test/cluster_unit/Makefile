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
        test_cluster_buffer_desc test_cluster_pcm_lock \
        test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision \
        test_cluster_xlog test_cluster_tt_slot test_cluster_undo_segment \
        test_cluster_epoch

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
CLUSTER_VOTING_DISK_IO_O = $(top_builddir)/src/backend/cluster/cluster_voting_disk_io.o
CLUSTER_QUORUM_DECISION_O = $(top_builddir)/src/backend/cluster/cluster_quorum_decision.o
PG_CRC32C_O = $(top_builddir)/src/port/pg_crc32c_armv8_srv.o
CLUSTER_SCN_O = $(top_builddir)/src/backend/cluster/cluster_scn.o

# Most tests link only cluster_version.o.  test_cluster_guc /
# test_cluster_shmem / test_cluster_signal / test_cluster_views /
# test_cluster_gviews / test_cluster_ic / test_cluster_conf have
# separate rules because they also link additional cluster_*.o
# objects (the test files stub the PG backend symbols those
# objects reference).
SIMPLE_TESTS = $(filter-out test_cluster_guc test_cluster_shmem test_cluster_signal test_cluster_views test_cluster_gviews test_cluster_ic test_cluster_conf test_cluster_ic_mock test_cluster_inject test_cluster_pgstat test_cluster_debug test_cluster_shared_fs test_cluster_smgr test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision test_cluster_scn test_cluster_epoch,$(TESTS))

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

# spec-2.6 Sprint A Step 2 D3: cluster_voting_disk_io.o standalone test.
# Real syscalls (open / pread / pwrite / fdatasync) on a temp file —
# no PG runtime needed.  pg_crc32c.o linked for INIT/COMP/FIN_CRC32C
# macros.
test_cluster_voting_disk_io: test_cluster_voting_disk_io.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_VOTING_DISK_IO_O) $(PG_CRC32C_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VOTING_DISK_IO_O) $(PG_CRC32C_O) -o $@

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
