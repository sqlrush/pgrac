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
        test_cluster_gcs_reqid \
        test_cluster_xnode_profile test_cluster_adg \
        test_cluster_xnode_lever test_cluster_ges_handoff \
        test_cluster_reverse_key test_cluster_hw_lease \
        test_cluster_wait_events test_cluster_errcodes test_cluster_guc \
        test_cluster_shmem test_cluster_signal test_cluster_views \
        test_cluster_gviews test_cluster_ic test_cluster_ic_envelope \
        test_cluster_ic_router test_cluster_ic_rdma \
        test_cluster_conf \
        test_cluster_ic_mock test_cluster_inject test_cluster_pgstat \
        test_cluster_debug test_cluster_shared_fs test_cluster_shared_fs_sharedfs test_cluster_shared_fs_block_device test_cluster_smgr \
        test_cluster_scn test_cluster_block_format test_cluster_itl_slot \
        test_cluster_buffer_desc test_cluster_pcm_lock test_cluster_bufmgr_pcm_hook test_cluster_gcs_dispatch test_cluster_gcs_block test_cluster_gcs_block_retransmit test_cluster_gcs_block_2way test_cluster_gcs_block_3way test_cluster_gcs_block_lost_write test_cluster_sinval test_cluster_sinval_ack test_cluster_stage2_acceptance test_cluster_tt_status test_cluster_tt_status_hint test_cluster_visibility_fork test_cluster_visibility_decide_scn test_cluster_snapshot_source test_cluster_itl_touch test_cluster_itl_wal test_cluster_uba \
        test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision \
        test_cluster_xlog test_cluster_tt_slot test_cluster_undo_segment \
        test_cluster_epoch test_cluster_fence test_cluster_reconfig \
        test_cluster_ges test_cluster_grd test_cluster_grd_starvation test_cluster_lmd test_cluster_lmd_graph test_cluster_lmd_wait_state test_cluster_cancel_token test_cluster_lmd_probe_collector test_cluster_lock_acquire test_cluster_advisory \
        test_cluster_tt_slot_allocator test_cluster_itl_reader_real_triple \
        test_cluster_itl_cleanout test_cluster_visibility_inject test_cluster_itl_cleanout_perf \
        test_cluster_heap_lock_tuple test_cluster_perf_gates \
        test_cluster_subtrans test_cluster_multixact test_cluster_undo_format \
        test_cluster_undo_record test_cluster_undo_lifecycle test_cluster_cr test_cluster_cr_cache test_cluster_cr_key test_cluster_cr_pool test_cluster_cr_lifecycle test_cluster_cr_admit test_cluster_cr_admit_stat test_cluster_cr_tuple test_cluster_cr_tuple_stat test_cluster_resolver_cache test_cluster_cr_coordinator test_cluster_tt_durable test_cluster_terminal_authority test_cluster_sf_dep \
        test_cluster_retention test_cluster_undo_cleaner test_cluster_visibility_variants test_cluster_tt_2pc \
        test_cluster_stage3_acceptance test_cluster_undo_buf test_cluster_undo_extent \
        test_cluster_wal_thread test_cluster_wal_state test_cluster_recovery_plan \
        test_cluster_backup \
        test_cluster_recovery_worker test_cluster_recovery_merge test_cluster_remote_xact \
        test_cluster_block_apply test_cluster_thread_apply test_cluster_thread_replay \
        test_cluster_thread_driver test_cluster_thread_orchestrator \
        test_cluster_write_fence test_cluster_stage4_acceptance \
        test_cluster_stage5_5_cr_acceptance \
        test_cluster_stage5_integrated_acceptance \
        test_cluster_stage5_beta_acceptance \
        test_cluster_ges_mode \
        test_cluster_sequence \
        test_cluster_hw \
        test_cluster_dl \
        test_cluster_extend_gate \
        test_cluster_ir \
        test_cluster_ts \
        test_cluster_ko \
        test_cluster_hw_snapshot \
        test_cluster_cf_authority \
        test_cluster_cf_storage \
        test_cluster_cf_enqueue \
        test_cluster_cf_phase2 \
        test_cluster_cf_stats \
        test_cluster_hang \
        test_cluster_hang_resolve \
        test_cluster_cr_server_policy \
        test_cluster_runtime_visibility \
        test_cluster_touched_peers \
        test_cluster_clean_leave \
        test_cluster_membership \
        test_cluster_node_remove \
        test_cluster_hang_acceptance \
        test_cluster_xid_stripe

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
CLUSTER_SHARED_FS_SHAREDFS_O = $(top_builddir)/src/backend/cluster/storage/cluster_shared_fs_sharedfs.o
CLUSTER_SHARED_FS_BLOCK_DEVICE_O = $(top_builddir)/src/backend/cluster/storage/cluster_shared_fs_block_device.o
CLUSTER_PR_SCSI_O = $(top_builddir)/src/backend/cluster/storage/cluster_pr_scsi.o
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
CLUSTER_ADG_O = $(top_builddir)/src/backend/cluster/cluster_adg.o
CLUSTER_LNS_O = $(top_builddir)/src/backend/cluster/cluster_lns.o
CLUSTER_GES_O = $(top_builddir)/src/backend/cluster/cluster_ges.o
CLUSTER_GRD_O = $(top_builddir)/src/backend/cluster/cluster_grd.o
CLUSTER_LMD_O = $(top_builddir)/src/backend/cluster/cluster_lmd.o
CLUSTER_LMD_GRAPH_O = $(top_builddir)/src/backend/cluster/cluster_lmd_graph.o
CLUSTER_LMD_WAIT_STATE_O = $(top_builddir)/src/backend/cluster/cluster_lmd_wait_state.o
CLUSTER_CANCEL_TOKEN_O = $(top_builddir)/src/backend/cluster/cluster_cancel_token.o
CLUSTER_LMD_PROBE_COLLECTOR_O = $(top_builddir)/src/backend/cluster/cluster_lmd_probe_collector.o
CLUSTER_LOCK_ACQUIRE_O = $(top_builddir)/src/backend/cluster/cluster_lock_acquire.o
# spec-2.30 D9: test_cluster_pcm_lock links cluster_pcm_lock.o to exercise
# the transition validator + counter accessors standalone (20 PG-runtime
# deps stubbed in the test binary).
CLUSTER_PCM_LOCK_O = $(top_builddir)/src/backend/cluster/cluster_pcm_lock.o
CLUSTER_GCS_O = $(top_builddir)/src/backend/cluster/cluster_gcs.o

# spec-5.15 D9: test_cluster_membership links cluster_membership.o (pure layer:
# monotonic-incarnation vet + membership-state decision table).  The pure layer
# has no PG-backend dependencies, so nothing is stubbed beyond the Assert
# ExceptionalCondition hook.
CLUSTER_MEMBERSHIP_O = $(top_builddir)/src/backend/cluster/cluster_membership.o
test_cluster_membership: test_cluster_membership.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_MEMBERSHIP_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_MEMBERSHIP_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.5 D0/D1: test_cluster_backup links only the dependency-light
# manifest/PITR helper object.  Runtime SQL/shmem code stays in
# cluster_backup.o and is intentionally not pulled into this pure test.
CLUSTER_BACKUP_MANIFEST_O = $(top_builddir)/src/backend/cluster/cluster_backup_manifest.o
test_cluster_backup: test_cluster_backup.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_BACKUP_MANIFEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_BACKUP_MANIFEST_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Most tests link only cluster_version.o.  test_cluster_guc /
# test_cluster_shmem / test_cluster_signal / test_cluster_views /
# test_cluster_gviews / test_cluster_ic / test_cluster_conf have
# separate rules because they also link additional cluster_*.o
# objects (the test files stub the PG backend symbols those
# objects reference).
SIMPLE_TESTS = $(filter-out test_cluster_guc test_cluster_shmem test_cluster_signal test_cluster_views test_cluster_gviews test_cluster_ic test_cluster_conf test_cluster_ic_mock test_cluster_inject test_cluster_pgstat test_cluster_debug test_cluster_shared_fs test_cluster_shared_fs_sharedfs test_cluster_shared_fs_block_device test_cluster_smgr test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision test_cluster_scn test_cluster_adg test_cluster_epoch test_cluster_fence test_cluster_reconfig test_cluster_ges test_cluster_grd test_cluster_grd_starvation test_cluster_lmd test_cluster_lmd_graph test_cluster_lmd_wait_state test_cluster_cancel_token test_cluster_lmd_probe_collector test_cluster_lock_acquire test_cluster_advisory test_cluster_terminal_authority test_cluster_retention test_cluster_visibility_variants test_cluster_tt_2pc test_cluster_stage3_acceptance test_cluster_undo_buf test_cluster_block_apply test_cluster_thread_apply test_cluster_thread_replay test_cluster_thread_driver test_cluster_thread_orchestrator test_cluster_write_fence test_cluster_stage4_acceptance test_cluster_stage5_integrated_acceptance test_cluster_stage5_beta_acceptance test_cluster_ges_mode test_cluster_sequence test_cluster_hw test_cluster_dl test_cluster_extend_gate test_cluster_ir test_cluster_ts test_cluster_ko test_cluster_hw_snapshot test_cluster_cf_authority test_cluster_cf_storage test_cluster_cf_enqueue test_cluster_cf_phase2 test_cluster_cf_stats test_cluster_hang test_cluster_hang_resolve test_cluster_cr_server_policy test_cluster_touched_peers test_cluster_clean_leave test_cluster_membership test_cluster_node_remove test_cluster_resolver_cache test_cluster_backup test_cluster_hang_acceptance test_cluster_gcs_reqid test_cluster_runtime_visibility test_cluster_xid_stripe test_cluster_bufmgr_pcm_hook test_cluster_cr test_cluster_cr_admit test_cluster_cr_admit_stat test_cluster_cr_cache test_cluster_cr_coordinator test_cluster_cr_key test_cluster_cr_lifecycle test_cluster_cr_pool test_cluster_cr_tuple test_cluster_cr_tuple_stat test_cluster_gcs_block test_cluster_gcs_block_2way test_cluster_gcs_block_3way test_cluster_gcs_block_lost_write test_cluster_gcs_block_retransmit test_cluster_gcs_dispatch test_cluster_ges_handoff test_cluster_heap_lock_tuple test_cluster_hw_lease test_cluster_ic_envelope test_cluster_ic_router test_cluster_itl_cleanout test_cluster_itl_cleanout_perf test_cluster_itl_reader_real_triple test_cluster_itl_touch test_cluster_itl_wal test_cluster_multixact test_cluster_pcm_lock test_cluster_perf_gates test_cluster_recovery_merge test_cluster_recovery_plan test_cluster_recovery_worker test_cluster_reverse_key test_cluster_sinval test_cluster_sinval_ack test_cluster_snapshot_source test_cluster_stage2_acceptance test_cluster_stage5_5_cr_acceptance test_cluster_subtrans test_cluster_tt_durable test_cluster_tt_slot_allocator test_cluster_tt_status test_cluster_tt_status_hint test_cluster_uba test_cluster_undo_format test_cluster_undo_lifecycle test_cluster_undo_record test_cluster_visibility_decide_scn test_cluster_visibility_fork test_cluster_visibility_inject test_cluster_wal_state test_cluster_wal_thread test_cluster_xnode_lever test_cluster_xnode_profile,$(TESTS))

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

# spec-5.1a D6: test_cluster_ges_mode links cluster_ges_mode.o (pure layer:
# const matrix + compat / convert-class / DLM-alias / PG-name helpers).
# The pure layer has no PG-backend dependencies, so nothing is stubbed.
CLUSTER_GES_MODE_O = $(top_builddir)/src/backend/cluster/cluster_ges_mode.o
test_cluster_ges_mode: test_cluster_ges_mode.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_GES_MODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_MODE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.4 D11: test_cluster_sequence links cluster_sequence.o (pure layer:
# SQ resid encoding + direction-aware batch allocator + instance-cache
# slicing predicate).  The pure layer has no PG-backend dependencies, so
# nothing is stubbed beyond the Assert ExceptionalCondition hook.
CLUSTER_SEQUENCE_O = $(top_builddir)/src/backend/cluster/cluster_sequence.o
test_cluster_sequence: test_cluster_sequence.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_SEQUENCE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SEQUENCE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7a D1: test_cluster_hw links cluster_hw.o (pure layer: HW resid
# encoding + three-state extend classifier + master-side batch allocator
# math).  The pure layer has no PG-backend dependencies, so nothing is
# stubbed beyond the Assert ExceptionalCondition hook.
CLUSTER_HW_O = $(top_builddir)/src/backend/cluster/cluster_hw.o
test_cluster_hw: test_cluster_hw.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7 D4: test_cluster_dl links cluster_dl.o (pure DL resid encoder) +
# cluster_hw.o (cluster_hw_resid_encode, for the DL-vs-HW distinctness test).
CLUSTER_DL_O = $(top_builddir)/src/backend/cluster/cluster_dl.o
test_cluster_dl: test_cluster_dl.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_DL_O) $(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_DL_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7 §3.1d (v1.5 amend): test_cluster_extend_gate links cluster_extend_gate.o
# (pure liveness engage classifier + wrapper).  The wrapper's six runtime
# accessors (cluster_cf_assess_liveness / cluster_lms_is_ready /
# cluster_lms_wait_for_ready / cluster_qvotec_get_disks_total_count /
# cluster_cssd_get_status / cluster_cssd_get_alive_peer_count) and the
# cluster_ges_request_timeout_ms GUC are mocked in the test, so nothing else is
# linked.
CLUSTER_EXTEND_GATE_O = $(top_builddir)/src/backend/cluster/cluster_extend_gate.o
test_cluster_extend_gate: test_cluster_extend_gate.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_EXTEND_GATE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_EXTEND_GATE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7 D8: test_cluster_ir links cluster_ir.o (pure IR resid encoder +
# bootstrap predicate) + cluster_dl.o + cluster_hw.o (resid encoders, for the
# IR-vs-DL/HW namespace-distinctness tests).  The pure layer has no PG-backend
# dependencies, so nothing is stubbed beyond the Assert ExceptionalCondition hook.
CLUSTER_IR_O = $(top_builddir)/src/backend/cluster/cluster_ir.o
test_cluster_ir: test_cluster_ir.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_IR_O) $(CLUSTER_DL_O) $(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IR_O) $(CLUSTER_DL_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7 D5: test_cluster_ts links cluster_ts.o (pure TT resid encoders) +
# cluster_ir.o/cluster_dl.o/cluster_hw.o (resid encoders, for the TT-vs-others
# namespace-distinctness test).  Pure layer, no PG-backend deps beyond the Assert
# hook.
CLUSTER_TS_O = $(top_builddir)/src/backend/cluster/cluster_ts.o
test_cluster_ts: test_cluster_ts.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_TS_O) $(CLUSTER_IR_O) $(CLUSTER_DL_O) $(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_TS_O) $(CLUSTER_IR_O) $(CLUSTER_DL_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7 D6: test_cluster_ko links cluster_ko.o (pure KO resid encoder) +
# cluster_dl.o/cluster_ir.o/cluster_hw.o (resid encoders, for the KO-vs-others
# namespace-distinctness test).  Pure layer, no PG-backend deps beyond the Assert
# hook.
CLUSTER_KO_O = $(top_builddir)/src/backend/cluster/cluster_ko.o
test_cluster_ko: test_cluster_ko.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_KO_O) $(CLUSTER_DL_O) $(CLUSTER_IR_O) $(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_KO_O) $(CLUSTER_DL_O) $(CLUSTER_IR_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.11 D9: test_cluster_hang links cluster_hang_policy.o (the pure Hang
# Manager decision/policy layer: threshold / wait-source tag / exclusion /
# duration kind / quality / top-N store / consistent-snapshot protocol).  No
# PG-backend deps beyond the Assert hook + the two DIAG LWLock stubs; the
# runtime gathering (cluster_hang.o) is exercised by cluster_tap t/30x.
CLUSTER_HANG_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_hang_policy.o
test_cluster_hang: test_cluster_hang.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_HANG_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_HANG_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.12 D9: test_cluster_hang_resolve links cluster_hang_resolve_policy.o
# (the pure Hang Manager disposition decision layer: victim score + comparator,
# HARD-skip / lock-holder classification, fail-CLOSED gate, tier->signal map,
# mode predicates, root-blocker ascent, confirmation/hysteresis machine,
# gate->counter mapping) plus cluster_hang_policy.o (cluster_hang_sample_actionable
# + the store ops the root-ascent / actionable tests reuse).  No PG-backend deps
# beyond the Assert hook + the two DIAG LWLock stubs; -lm for log() in the score.
CLUSTER_HANG_RESOLVE_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_hang_resolve_policy.o
test_cluster_hang_resolve: test_cluster_hang_resolve.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_HANG_RESOLVE_POLICY_O) $(CLUSTER_HANG_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_HANG_RESOLVE_POLICY_O) $(CLUSTER_HANG_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -lm -o $@

# spec-5.20 D8: test_cluster_hang_acceptance links the same two pure Hang
# Manager policy objects as the 5.11/5.12 unit tests — cluster_hang_resolve_policy.o
# (tier->signal no-SIGKILL map, L5) + cluster_hang_policy.o (cluster_hang_sample_
# actionable safe-direction gate, L6).  The other four tests (L1-L4) are pure
# compile-time enum / macro / oid / roster snapshots.  -lm for log() pulled in
# by the resolve-policy score.
test_cluster_hang_acceptance: test_cluster_hang_acceptance.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_HANG_RESOLVE_POLICY_O) $(CLUSTER_HANG_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_HANG_RESOLVE_POLICY_O) $(CLUSTER_HANG_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -lm -o $@

# spec-6.12b: test_cluster_cr_server_policy links cluster_cr_server_policy.o
# (the pure FULL/PARTIAL/DENY split over write_scn-DESC chain origins).  No
# PG-backend deps: the policy is branch-only integer logic.
CLUSTER_CR_SERVER_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_cr_server_policy.o
test_cluster_cr_server_policy: test_cluster_cr_server_policy.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_CR_SERVER_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CR_SERVER_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.12i: test_cluster_runtime_visibility links cluster_runtime_visibility_policy.o
# (the pure D-i2 live-authority window gate: epoch match + hwm >= anchor_lsn,
# fail-closed on every doubt).  No PG-backend deps: branch-only integer logic.
CLUSTER_RUNTIME_VIS_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_runtime_visibility_policy.o
test_cluster_runtime_visibility: test_cluster_runtime_visibility.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_RUNTIME_VIS_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_RUNTIME_VIS_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.14 D1: test_cluster_touched_peers links cluster_touched_peers.o (the
# pure per-tx touched bitmap: stamp / poison / intersect / reset / DSM merge).
# The shmem counter hook (cluster_reconfig_note_touched_stamp) and cluster_node_id
# are stubbed locally in the test, so only the bitmap object + libpgport are
# needed; the runtime stamping callsites + D4 dispatch are covered by cluster_tap.
CLUSTER_TOUCHED_PEERS_O = $(top_builddir)/src/backend/cluster/cluster_touched_peers.o
test_cluster_touched_peers: test_cluster_touched_peers.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_TOUCHED_PEERS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_TOUCHED_PEERS_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.13 D15: test_cluster_clean_leave links cluster_clean_leave_policy.o (the
# pure clean-leave decision layer: phase-FSM transition validity, writable-only
# quiesce gate, version-coherent leave check, leave-intent marker validation).
# No PG-backend deps beyond the Assert hook; libpgport for the marker CRC32C.
# The runtime driver (shmem / voting-disk marker I/O / ProcSignal / GES-GCS
# drain / LMON orchestration) is covered by cluster_tap t/NNN (2-node).
CLUSTER_CLEAN_LEAVE_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_clean_leave_policy.o
test_cluster_clean_leave: test_cluster_clean_leave.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_CLEAN_LEAVE_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CLEAN_LEAVE_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.18 D18: test_cluster_node_remove links cluster_node_remove_policy.o (the
# pure permanent-removal decision layer: phase-FSM transition validity, precheck
# behaviour matrix, INV-LF2 ordering gate, fence-arm-timeout classify, version-
# coherent contest classify, INV-LF7 crash-recovery matrix, three-phase removal-
# marker validate/authority/carry-forward, IC payload integrity).  No PG-backend
# deps beyond the Assert hook; libpgport for the marker/payload CRC32C.  The
# runtime driver (shmem / voting-disk marker I/O / fence-arm / shrink commit / IC /
# LMON / GRD-PCM cleanup) is covered by cluster_tap t/NNN (2-node).
CLUSTER_NODE_REMOVE_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_node_remove_policy.o
test_cluster_node_remove: test_cluster_node_remove.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_NODE_REMOVE_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_NODE_REMOVE_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.7 D3 S1: test_cluster_hw_snapshot links cluster_hw_snapshot.o (the HW
# authority durable-snapshot envelope: serialize / deserialize+validate /
# identity / generation-supersede).  CRC32C comes from libpgport; the pure
# layer has no PG-backend dependencies, so nothing is stubbed beyond the
# Assert ExceptionalCondition hook.  The durable file round trip (temp +
# durable_rename) is added to this binary with D3 S3.
CLUSTER_HW_SNAPSHOT_O = $(top_builddir)/src/backend/cluster/cluster_hw_snapshot.o
test_cluster_hw_snapshot: test_cluster_hw_snapshot.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_HW_SNAPSHOT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_HW_SNAPSHOT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.6 Da1: test_cluster_cf_authority links cluster_cf_authority.o and
# exercises the pure classify/decide logic plus a real durable_rename read/
# write round trip against a temp dir.  The fd.c openers, pg_fsync,
# durable_rename and the ereport machinery are stubbed locally; CRC32C comes
# from libpgport, so no PG backend is needed.
CLUSTER_CF_AUTHORITY_O = $(top_builddir)/src/backend/cluster/cluster_cf_authority.o
test_cluster_cf_authority: test_cluster_cf_authority.c unit_test.h \
		$(CLUSTER_CF_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CF_AUTHORITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.14a D1: test_cluster_gcs_reqid exercises the pure request-id domain
# helpers (cluster_gcs_reqid.h, c.h-only); links libpgport for pg_printf.
test_cluster_gcs_reqid: test_cluster_gcs_reqid.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.6 Da2: test_cluster_cf_storage links cluster_cf_storage.o (migration
# + symlink + storage rename-contract) plus cluster_cf_authority.o (the
# migrate path writes the shared authority).  Same local stub set as the
# authority test; CRC32C from libpgport, no PG backend.
CLUSTER_CF_STORAGE_O = $(top_builddir)/src/backend/cluster/cluster_cf_storage.o
test_cluster_cf_storage: test_cluster_cf_storage.c unit_test.h \
		$(CLUSTER_CF_STORAGE_O) $(CLUSTER_CF_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CF_STORAGE_O) $(CLUSTER_CF_AUTHORITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.6 Db1: test_cluster_cf_enqueue links cluster_cf_enqueue.o (the pure
# resid encoder).  No PG backend deps beyond the Assert hook.
CLUSTER_CF_ENQUEUE_O = $(top_builddir)/src/backend/cluster/cluster_cf_enqueue.o
test_cluster_cf_enqueue: test_cluster_cf_enqueue.c unit_test.h \
		$(CLUSTER_CF_ENQUEUE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CF_ENQUEUE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.6 Dc4: test_cluster_cf_stats links cluster_cf_stats.o (the CF
# observability counters); ShmemInitStruct + region register stubbed locally.
CLUSTER_CF_STATS_O = $(top_builddir)/src/backend/cluster/cluster_cf_stats.o
test_cluster_cf_stats: test_cluster_cf_stats.c unit_test.h \
		$(CLUSTER_CF_STATS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CF_STATS_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.6 increment (iii): test_cluster_cf_phase2 links cluster_cf_phase2.o
# (the cross-node rename-contract rendezvous).  The probe/ack file I/O and the
# rendezvous decision are exercised against a temp dir; ereport / fd.c openers /
# GetCurrentTimestamp / the verify_or_fail deps are stubbed locally; CRC32C +
# pg_usleep from libpgport, no PG backend.
CLUSTER_CF_PHASE2_O = $(top_builddir)/src/backend/cluster/cluster_cf_phase2.o
test_cluster_cf_phase2: test_cluster_cf_phase2.c unit_test.h \
		$(CLUSTER_CF_PHASE2_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CF_PHASE2_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.2 D0: test_cluster_terminal_authority links the pure Smart Fusion
# terminal-authority decision layer.  It has no backend dependencies beyond the
# Assert hook.
CLUSTER_TERMINAL_AUTHORITY_O = $(top_builddir)/src/backend/cluster/cluster_terminal_authority.o
test_cluster_terminal_authority: test_cluster_terminal_authority.c unit_test.h \
		$(CLUSTER_TERMINAL_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_TERMINAL_AUTHORITY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.12 D6: test_cluster_retention links cluster_undo_retention.o (the two
# pure retention predicates) standalone.  cluster_undo_retention.c references
# only scn_time_cmp(), which the test stubs locally, so no PG backend is needed.
CLUSTER_UNDO_RETENTION_O = $(top_builddir)/src/backend/cluster/cluster_undo_retention.o

# spec-3.14 D1/§2.2: test_cluster_visibility_variants links the resolver
# object (pure verdict functions). cluster_visibility_resolve.c references
# only ITL/TT/subtrans helpers behind the verdict layer; the verdict
# functions themselves need no PG backend, so we stub nothing.
CLUSTER_VIS_VERDICT_O = $(top_builddir)/src/backend/cluster/cluster_visibility_verdict.o

# spec-3.15 D1: pure 2PC record serialize/parse (CRC32C from libpgport).
CLUSTER_TT_2PC_RECORD_O = $(top_builddir)/src/backend/cluster/cluster_tt_2pc_record.o
test_cluster_tt_2pc: test_cluster_tt_2pc.c unit_test.h \
		$(CLUSTER_TT_2PC_RECORD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_TT_2PC_RECORD_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@
test_cluster_visibility_variants: test_cluster_visibility_variants.c unit_test.h \
		$(CLUSTER_VIS_VERDICT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VIS_VERDICT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@
test_cluster_retention: test_cluster_retention.c unit_test.h \
		$(CLUSTER_UNDO_RETENTION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_UNDO_RETENTION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.17 D6: test_cluster_stage3_acceptance — 8 static contract tests
# (L1-L8) pinning the Stage 3 MVCC final surface (catversion / undo opcodes
# + XLR_INFO_MASK / dump categories / SQLSTATE / wait-events count / TT
# enums / BIND reserved).  L7 makes a real call into cluster_tt_slot_recyclable
# so it links cluster_undo_retention.o (scn_time_cmp stubbed locally, same as
# test_cluster_retention).  Header-only otherwise.
test_cluster_stage3_acceptance: test_cluster_stage3_acceptance.c unit_test.h ../../../src/include/cluster/cluster_views.h \
		$(CLUSTER_UNDO_RETENTION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_UNDO_RETENTION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.14 D6: test_cluster_stage4_acceptance — 10 static contract tests
# (L1-L10) pinning the Stage 4 WAL + recovery + fence final surface
# (catversion / undo opcodes + XLR_INFO_MASK / recovery+fence dump categories /
# recovery+fence SQLSTATE / wait-events count / fence marker_kind + enforcement
# enums / thread-recovery scope enum / undo writeback-boundary enum / dead-bitmap
# width invariant + bit convention).  Header-only — pure enum/macro/computation,
# no external calls, no extra object linked.
test_cluster_stage4_acceptance: test_cluster_stage4_acceptance.c unit_test.h ../../../src/include/cluster/cluster_views.h \
		../../../src/include/cluster/cluster_write_fence.h \
		../../../src/include/cluster/cluster_thread_recovery.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.19 D8: test_cluster_stage5_integrated_acceptance — 6 static contract
# tests (L1-L6) pinning the Stage 5 (through 5.19) integrated-acceptance
# surface (catversion / undo opcodes + XLR_INFO_MASK / multi-node write-path
# dump categories / acceptance SQLSTATE / wait-events count + multi-node wait
# set / heap-ITL WAL delta 48B width invariant).  Header-only — pure enum/
# macro/struct/computation, no external calls, no extra object linked.
test_cluster_stage5_integrated_acceptance: test_cluster_stage5_integrated_acceptance.c unit_test.h \
		../../../src/include/cluster/cluster_views.h \
		../../../src/include/access/heapam_xlog.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.21 D3: test_cluster_stage5_beta_acceptance — 6 static contract tests
# (L1-L6) pinning the Stage 5 (through 5.21) beta close-out release surface
# (catversion floor / undo opcodes + XLR_INFO_MASK / reconfig + write-path wait
# roster / beta-acceptance SQLSTATE / wait-events count / 7 RAC core presence
# sentinels).  Header-only — pure enum/macro/struct/computation, no external
# calls, no extra object linked.
test_cluster_stage5_beta_acceptance: test_cluster_stage5_beta_acceptance.c unit_test.h \
		../../../src/include/cluster/cluster_views.h \
		../../../src/include/cluster/cluster_reconfig.h \
		../../../src/include/cluster/cluster_gcs_block.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.58 D8: test_cluster_stage5_5_cr_acceptance — 6 static contract tests
# (L1-L6) pinning the Stage 5.5 CR read-path band (5.51-5.57) final surface
# (catversion / CR SQLSTATE 53R9F+53R9G / cr+cr_pool+resolver_cache+cr_coord dump
# category roster / admission-policy enum / cross-instance coordinator mode+counter
# enums / wait-events count).  Header-only — pure enum/macro/computation, no
# external calls, no extra object linked.
test_cluster_stage5_5_cr_acceptance: test_cluster_stage5_5_cr_acceptance.c unit_test.h \
		../../../src/include/cluster/cluster_views.h \
		../../../src/include/cluster/cluster_cr_admit.h \
		../../../src/include/cluster/cluster_cr_coordinator_stat.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.18 D1:  test_cluster_undo_buf links the real undo buffer pool object
# and stubs ShmemInitStruct / LWLock* / cluster_undo_smgr_{read,write}_block
# locally (single-threaded pool-logic test).
CLUSTER_UNDO_BUF_O = $(top_builddir)/src/backend/cluster/storage/cluster_undo_buf.o
test_cluster_undo_buf: test_cluster_undo_buf.c unit_test.h $(CLUSTER_UNDO_BUF_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_UNDO_BUF_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.10 D3a: test_cluster_block_apply links the real single-block
# redo-apply framework object and stubs RestoreBlockImage + the
# merged-recovery window externs (read by the inline PageSetLSN hook) +
# ExceptionalCondition locally.  Exercises cluster_block_apply_decide
# (pure routing truth table) + cluster_block_apply_one dispatch
# (NOOP / UNSUPPORTED empty starter matrix / FPI restore) with fabricated
# decoded records.  The byte-for-byte differential against PG real redo
# (real WAL + heap delta handlers) lands as a cluster_tap in D3b/D6.
CLUSTER_BLOCK_APPLY_O = $(top_builddir)/src/backend/cluster/cluster_block_apply.o
test_cluster_block_apply: test_cluster_block_apply.c unit_test.h $(CLUSTER_BLOCK_APPLY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_BLOCK_APPLY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.11 D1: test_cluster_thread_apply links the real online thread-recovery
# apply wrapper object and stubs cluster_block_apply_one (its byte-for-byte
# correctness is t/256's job) + the merged-recovery window externs (read by the
# inline PageGetLSN/PageSetLSN hook) + ExceptionalCondition locally.  Exercises
# cluster_thread_apply_decide (pure LSN-gate truth table) + the wrapper's
# NOOP / DONE(idempotent skip) / APPLY / BLOCKED(fail-closed) result mapping
# with fabricated decoded records.  The byte-for-byte differential against PG
# real redo lands as a cluster_tap in t/258 (D6).
CLUSTER_THREAD_APPLY_O = $(top_builddir)/src/backend/cluster/cluster_thread_recovery_apply.o
test_cluster_thread_apply: test_cluster_thread_apply.c unit_test.h $(CLUSTER_THREAD_APPLY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_THREAD_APPLY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.11 D1 increment 3a: test_cluster_thread_replay exercises the PURE
# block-classification gate (cluster_thread_replay_classify_block, a header
# inline) -- the corruption-critical TARGET / OUT_OF_SCOPE / BLOCKED decision
# the RMW replay engine makes before touching any page.  No cluster object is
# needed (the gate is inline); only libpgcommon/libpgport for the unit harness.
# The engine's byte-for-byte streaming behaviour lands as cluster_tap t/259.
test_cluster_thread_replay: test_cluster_thread_replay.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.11 D1 increment 3b-1: test_cluster_thread_driver exercises the driver's
# PURE decision helpers -- cluster_thread_recovery_should_rethrow (the R13 elevel
# rethrow boundary, amend 1) and cluster_thread_recovery_decide_scope (the
# capability/scope gate).  Both are header inlines, so no cluster object is
# needed; only libpgcommon/libpgport for the unit harness.  The driver's I/O
# behaviour (foreign reader + R13 demotion + fail-closed) lands as cluster_tap
# t/260.
test_cluster_thread_driver: test_cluster_thread_driver.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.11 D1 increment 3b-2: test_cluster_thread_orchestrator exercises the
# orchestrator's PURE on_unrecoverable escalation gate
# (cluster_thread_recovery_decide_on_blocked, Q5).  A header inline, so no
# cluster object is needed; only libpgcommon/libpgport.  The orchestrator's I/O
# behaviour (combined visibility pass + durability barrier + 3-way authority +
# partial-apply + R13) lands as cluster_tap t/261.
test_cluster_thread_orchestrator: test_cluster_thread_orchestrator.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.12 D3: test_cluster_write_fence exercises the PURE cooperative write-fence
# judge cluster_write_fence_decide (a header inline -- no cluster object needed;
# only libpgcommon/libpgport for the unit harness).  The token region + qvotec
# refresh + hot-path gating land as cluster_tap (D8).
test_cluster_write_fence: test_cluster_write_fence.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
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
test_cluster_views: test_cluster_views.c unit_test.h ../../../src/include/cluster/cluster_views.h $(CLUSTER_VERSION_O) \
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

# test_cluster_xnode_profile links cluster_xnode_profile.o standalone (spec-5.59
# D10).  The test defines the GUC variable itself (cluster_guc.o is not
# linked) and stubs ShmemInitStruct / cluster_shmem_register_region; the
# real shmem registration is exercised by cluster_tap t/020.
CLUSTER_XNODE_PROFILE_O = $(top_builddir)/src/backend/cluster/cluster_xnode_profile.o
test_cluster_xnode_profile: test_cluster_xnode_profile.c unit_test.h $(CLUSTER_XNODE_PROFILE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_XNODE_PROFILE_O) -o $@

# test_cluster_xnode_lever links cluster_xnode_lever.o standalone (spec-6.12
# U-c).  The test defines the two GUC gate variables + MyProc itself and
# stubs ShmemInitStruct / cluster_shmem_register_region; the real shmem
# registration is exercised by cluster_tap t/020.
CLUSTER_XNODE_LEVER_O = $(top_builddir)/src/backend/cluster/cluster_xnode_lever.o
test_cluster_xnode_lever: test_cluster_xnode_lever.c unit_test.h $(CLUSTER_XNODE_LEVER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_XNODE_LEVER_O) -o $@

# test_cluster_reverse_key links cluster_reverse_key.o standalone (spec-6.12
# U-f): locks the byte-reversal math (involution / known patterns / width
# gate / order-not-preserved).  The ereport / lsyscache / fmgr externs used
# by the scankey transform are stubbed locally and never fire; the transform
# + planner contract are exercised end-to-end by cluster_regress reverse_key.
CLUSTER_REVERSE_KEY_O = $(top_builddir)/src/backend/cluster/cluster_reverse_key.o
test_cluster_reverse_key: test_cluster_reverse_key.c unit_test.h $(CLUSTER_REVERSE_KEY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_REVERSE_KEY_O) -o $@

# test_cluster_hw_lease links cluster_hw_lease.o standalone (spec-6.12d
# U-d): locks the slot-table bookkeeping (install/consume roundtrip, pair
# isolation, replacement/LRU orphan accounting).  LWLock/shmem stubbed
# (single-threaded); the real cross-node path is cluster_tap t/337.
CLUSTER_HW_LEASE_O = $(top_builddir)/src/backend/cluster/cluster_hw_lease.o
test_cluster_hw_lease: test_cluster_hw_lease.c unit_test.h $(CLUSTER_HW_LEASE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_HW_LEASE_O) -o $@

# test_cluster_ges_handoff links the pure verifier (spec-6.12e1) + the frozen
# GES mode matrix (cluster_ges_mode.o) so the compatibility rule is the real
# one.  Neither object needs shmem / runtime symbols.
CLUSTER_GES_HANDOFF_POLICY_O = $(top_builddir)/src/backend/cluster/cluster_ges_handoff_policy.o
test_cluster_ges_handoff: test_cluster_ges_handoff.c unit_test.h \
		$(CLUSTER_GES_HANDOFF_POLICY_O) $(CLUSTER_GES_MODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_GES_HANDOFF_POLICY_O) $(CLUSTER_GES_MODE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

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
		$(CLUSTER_SHARED_FS_STUB_O) $(CLUSTER_SHARED_FS_LOCAL_O) \
		$(CLUSTER_SHARED_FS_SHAREDFS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_O) \
		$(CLUSTER_SHARED_FS_STUB_O) $(CLUSTER_SHARED_FS_LOCAL_O) \
		$(CLUSTER_SHARED_FS_SHAREDFS_O) -o $@

# spec-4.5a D12: runtime unit for the shared_fs backend + shared-root
# sentinel.  Links ONLY cluster_shared_fs_sharedfs.o with functional
# stubs (real open/pread/pwrite/software-crc32c), no server needed.
test_cluster_shared_fs_sharedfs: test_cluster_shared_fs_sharedfs.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_SHAREDFS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_SHAREDFS_O) -o $@

# spec-6.0a: runtime unit for the raw block_device backend over a
# temporary regular file that stands in for a block device.  Links only
# the provider object; WAL/GES entry points are stubbed by the test.
test_cluster_shared_fs_block_device: test_cluster_shared_fs_block_device.c unit_test.h \
		$(CLUSTER_SHARED_FS_BLOCK_DEVICE_O) $(CLUSTER_PR_SCSI_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_SHARED_FS_BLOCK_DEVICE_O) $(CLUSTER_PR_SCSI_O) -o $@

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
		$(CLUSTER_SHARED_FS_LOCAL_O) \
		$(CLUSTER_SHARED_FS_SHAREDFS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SMGR_O) \
		$(CLUSTER_SHARED_FS_O) $(CLUSTER_SHARED_FS_STUB_O) \
		$(CLUSTER_SHARED_FS_LOCAL_O) \
		$(CLUSTER_SHARED_FS_SHAREDFS_O) -o $@

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
		./$$t || { failed=$$((failed + 1)); echo "# FAILED-BINARY: $$t (exit $$?)"; }; \
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
# spec-5.15 D2: cluster_reconfig.o now references cluster_membership_attach (and
# the membership-state accessors in the tick), so the standalone test links
# cluster_membership.o too; cluster_qvotec_in_quorum is stubbed in the test.
test_cluster_reconfig: test_cluster_reconfig.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_RECONFIG_O) $(CLUSTER_EPOCH_O) \
		$(CLUSTER_TOUCHED_PEERS_O) $(CLUSTER_MEMBERSHIP_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_RECONFIG_O) $(CLUSTER_EPOCH_O) \
		$(CLUSTER_TOUCHED_PEERS_O) $(CLUSTER_MEMBERSHIP_O) \
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

# spec-6.4 D1: test_cluster_adg links the dependency-light ADG pure
# primitives.  SCN comparators are stubbed locally so this binary does not pull
# cluster_scn.o's backend/shmem dependencies.
test_cluster_adg: test_cluster_adg.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_ADG_O) $(CLUSTER_LNS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_ADG_O) $(CLUSTER_LNS_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.13 D6: test_cluster_ges links cluster_ges.o standalone.
# cluster_ges.c references ShmemInitStruct + cluster_shmem_register_region
# + pg_atomic_*; test stubs all of these (union force-align for shmem
# buffer per L105) and exercises the 2 handler stubs + 2 accessors real
# behavior (T-ges-1 a/b/c/d/e).
# spec-5.1b D1: cluster_ges.o now references ges_modes_compatible (frozen
# matrix), so link the pure cluster_ges_mode.o (standalone-linkable, no new deps).
test_cluster_ges: test_cluster_ges.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_O) $(CLUSTER_GES_MODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_O) $(CLUSTER_GES_MODE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.14 D11: test_cluster_grd links cluster_grd.o standalone.
# cluster_grd.c references ShmemInitStruct + cluster_shmem_register_region
# + cluster_conf_lookup_node + pg_atomic_*; test stubs all of these
# (union force-align L105 + mock declared list) and exercises ClusterResId
# encode/decode + hash distribution + sparse-node master mapping + 4-class
# is_cluster_aware classifier (T-grd-1 a/b/c/d/e/f).
test_cluster_grd: test_cluster_grd.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_O) $(CLUSTER_GES_MODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_O) $(CLUSTER_GES_MODE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.10 D9: test_cluster_grd_starvation links cluster_grd.o standalone
# (same stub surface as test_cluster_grd; the spec-5.8 LMD wait-edge API is
# modelled by the in-file ut_wfg spy).
test_cluster_grd_starvation: test_cluster_grd_starvation.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_O) $(CLUSTER_GES_MODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_O) $(CLUSTER_GES_MODE_O) \
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

# spec-5.8 D1a: test_cluster_lmd_graph links cluster_lmd_graph.o standalone.
# cluster_lmd_graph.c references ShmemInitStruct + ShmemInitHash + hash_search
# + hash_seq_* + LWLock + pg_atomic_* + add_size + hash_estimate_size; the test
# stubs all of these (a faithful fake HTAB keyed on infoP->keysize) and pins
# the multi-edge schema: same waiter + multiple distinct blockers keeps every
# edge, remove-by-waiter deletes all of them.
test_cluster_lmd_graph: test_cluster_lmd_graph.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_GRAPH_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_GRAPH_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.8 D1d: test_cluster_lmd_wait_state links cluster_lmd_wait_state.o
# standalone.  The wait-state object depends only on pg_atomic (libpgport);
# the test exercises publish / clear / read / reset + wait_seq ABA on a
# ClusterLmdProcWaitState directly (no PGPROC / shmem needed).
test_cluster_lmd_wait_state: test_cluster_lmd_wait_state.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_WAIT_STATE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_WAIT_STATE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.9 D3: test_cluster_cancel_token links cluster_cancel_token.o
# standalone.  The match logic (consume_against) only needs the spinlock +
# the counter-inc stubs; the consume() wrapper's globals (MyProc / flag /
# wait-state read / epoch) are stubbed locally and never exercised here.
test_cluster_cancel_token: test_cluster_cancel_token.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_CANCEL_TOKEN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CANCEL_TOKEN_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.8 D8: test_cluster_lmd_probe_collector links cluster_lmd_probe_collector.o
# standalone.  The shmem REPORT collector depends on ShmemInitStruct + LWLock +
# pg_atomic + a member-admit helper; the test stubs all of these (fake region +
# faithful local member_admit copy) and pins the complete / duplicate / stale /
# unexpected / overflow / multi-node / reset contract (Rule 8.A).
test_cluster_lmd_probe_collector: test_cluster_lmd_probe_collector.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_PROBE_COLLECTOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_PROBE_COLLECTOR_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.20 D13: test_cluster_lock_acquire links cluster_lock_acquire.o
# standalone.  cluster_lock_acquire.c references cluster_lms_is_ready /
# cluster_lms_enabled / cluster_grd / pg_atomic_*; test stubs all PG
# backend symbols locally and exercises S1-S7 individual step API + top-
# level dispatch + HC1 fail-closed + I1 monotonic forward transition.
test_cluster_lock_acquire: test_cluster_lock_acquire.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LOCK_ACQUIRE_O) $(CLUSTER_GES_MODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LOCK_ACQUIRE_O) $(CLUSTER_GES_MODE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.5 D3/D8: test_cluster_advisory links cluster_advisory.o standalone.
# cluster_advisory.c references ShmemInitStruct + cluster_shmem_register_region
# + pg_atomic_* ; the test stubs the shmem allocator (L105 union force-align)
# and exercises the session-scope predicate + observability counters.  Release-
# path / backend-exit drain behaviour is covered by cluster_tap t/286.
CLUSTER_ADVISORY_O = $(top_builddir)/src/backend/cluster/cluster_advisory.o
test_cluster_advisory: test_cluster_advisory.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_ADVISORY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_ADVISORY_O) \
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
test_cluster_gcs_block_retransmit: test_cluster_gcs_block_retransmit.c unit_test.h ../../../src/include/cluster/cluster_views.h \
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
# 5 fault inject point name strings, wait events count 93 snapshot,
# SQLSTATE 53R60/90-95 encodable, GUC enum + shmem_max_regions
# boot_val=64 snapshot, RESERVED_0=0 sentinel).  Header-only;
# behavioral coverage in cluster_tap t/200-202.
test_cluster_stage2_acceptance: test_cluster_stage2_acceptance.c unit_test.h ../../../src/include/cluster/cluster_views.h \
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
CLUSTER_TT_DURABLE_O = $(top_builddir)/src/backend/cluster/cluster_tt_durable.o

# spec-3.11 D2/D9: test_cluster_tt_durable — pure decision predicates
# (redo wrap table / slot match) + lookup / by-xid scan with a mocked
# cluster_undo_smgr.  File-I/O behavior is e2e in cluster_tap t/219.
test_cluster_tt_durable: test_cluster_tt_durable.c unit_test.h \
		$(CLUSTER_TT_DURABLE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_TT_DURABLE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_tt_slot_allocator: test_cluster_tt_slot_allocator.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_TT_SLOT_O) $(CLUSTER_UNDO_RETENTION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_TT_SLOT_O) $(CLUSTER_UNDO_RETENTION_O) \
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


# spec-4.2 D6: test_cluster_wal_state — 14 pure-helper tests for the
# ClusterWalState registry (header/slot 512B layout locks incl. the
# single-source SLOT_OFFSET macro, fill/validate, CRC torn detection,
# EMPTY/CORRUPT/FOREIGN classification).  Header-only inclusion;
# libpgcommon + libpgport supply pg_crc32c.  The header dependency is
# explicit per L229 (layout/StaticAssert snapshots must rebuild when
# the header moves).
test_cluster_wal_state: test_cluster_wal_state.c unit_test.h \
		../../../src/include/cluster/cluster_wal_state.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.5 D11: test_cluster_recovery_merge — 20 pure tests (k-way
# heap ordering scn->lsn->node, G/S/L/U class matrix, xl_scn layout).
# Header-only; scn_recovery_cmp stubbed in-test (cluster_scn.o not
# linked).  Header deps explicit per L229.
test_cluster_recovery_merge: test_cluster_recovery_merge.c unit_test.h \
		../../../src/include/cluster/cluster_recovery_merge.h \
		../../../src/include/access/xlogrecord.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.4 D7: test_cluster_recovery_worker — 15 pure-helper tests
# (round-robin striping conservation, page-header check incl. the P0
# recycled-pageaddr kill, target-page math incl. the exact-segment-
# boundary case).  Header-only inclusion; header deps per L229.
test_cluster_recovery_worker: test_cluster_recovery_worker.c unit_test.h \
		../../../src/include/cluster/cluster_recovery_worker.h \
		../../../src/include/cluster/cluster_wal_state.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.3 D6: test_cluster_recovery_plan — 16 pure-classifier tests
# for the recovery plan (§3.2 truth table 1:1: own-priority family,
# identity-invariant -> UNKNOWN, stale boundary, bitmap addressing,
# sweep conservation + bitmap<->verdict coherence).  Header-only
# inclusion; libpgcommon + libpgport supply pg_crc32c for the slot
# fill helper.  Header dependencies explicit per L229.
test_cluster_recovery_plan: test_cluster_recovery_plan.c unit_test.h \
		../../../src/include/cluster/cluster_recovery_plan.h \
		../../../src/include/cluster/cluster_wal_state.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.1 D8: test_cluster_wal_thread — 12 pure-helper tests for the
# per-thread WAL routing surface (identity mapping / dir name / page
# header validator RL1 matrix / claim fill+validate+corruption).  All
# helpers are header-inline (cluster_wal_thread.h); libpgcommon +
# libpgport supply pg_crc32c (the test_cluster_undo_record linkage
# pattern -- no module .o, no stubs).
test_cluster_wal_thread: test_cluster_wal_thread.c unit_test.h \
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

# spec-3.10 D8: test_cluster_cr_cache — backend-local clock CR cache
# (cluster_cr_cache.o) with malloc-backed MemoryContext stubs.
CLUSTER_CR_CACHE_O = $(top_builddir)/src/backend/cluster/cluster_cr_cache.o
test_cluster_cr_cache: test_cluster_cr_cache.c unit_test.h \
		$(CLUSTER_CR_CACHE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_CACHE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.53 D7: test_cluster_cr_key — CR cache key identity contract
# (cluster_cr_cache_key_equal): per-field necessity, joint sufficiency,
# field-wise (never-memcmp) equality.  Links cluster_cr_cache.o (key primitive).
test_cluster_cr_key: test_cluster_cr_key.c unit_test.h \
		$(CLUSTER_CR_CACHE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_CACHE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.51: test_cluster_cr_pool — shared CR buffer pool substrate
# (cluster_cr_pool.o + cluster_cr_cache.o for canonical key_equal) with
# malloc-backed shmem + no-op LWLock stubs.
CLUSTER_CR_POOL_O = $(top_builddir)/src/backend/cluster/cluster_cr_pool.o
test_cluster_cr_pool: test_cluster_cr_pool.c unit_test.h \
		$(CLUSTER_CR_POOL_O) $(CLUSTER_CR_CACHE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_POOL_O) $(CLUSTER_CR_CACHE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.56: test_cluster_cr_lifecycle — CR pool lifecycle / invalidation contract
# (C1-C4) + per-relation generation table (composite fence, INV-G1/G2/G3, overflow,
# disabled equivalence).  Links cluster_cr_pool.o + cluster_cr_cache.o.
test_cluster_cr_lifecycle: test_cluster_cr_lifecycle.c unit_test.h \
		$(CLUSTER_CR_POOL_O) $(CLUSTER_CR_CACHE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_POOL_O) $(CLUSTER_CR_CACHE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.52: test_cluster_cr_admit — backend-local admission predicate +
# scan-kind marker lifecycle + page-identity churn detector + per-backend
# relcap throttle + clock-pressure gauge (cluster_cr_admit.o).  Stubs
# ParallelWorkerNumber + the spec-5.51 evict/hit counter accessors (read only
# by the pressure gauge); no shmem / LWLock (all admission state is backend-local).
CLUSTER_CR_ADMIT_O = $(top_builddir)/src/backend/cluster/cluster_cr_admit.o
test_cluster_cr_admit: test_cluster_cr_admit.c unit_test.h \
		$(CLUSTER_CR_ADMIT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_ADMIT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.55: test_cluster_resolver_cache — shared resolver cache module
# (cluster_resolver_cache.o) with malloc-backed ShmemInitStruct + no-op LWLock
# stubs and a MOCKED cluster_tt_slot_durable_lookup (gate 2 re-validation) +
# MOCKED ReadNextFullTransactionId (gate 4 own-instance xid epoch).
CLUSTER_RESOLVER_CACHE_O = $(top_builddir)/src/backend/cluster/cluster_resolver_cache.o
test_cluster_resolver_cache: test_cluster_resolver_cache.c unit_test.h \
		$(CLUSTER_RESOLVER_CACHE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_RESOLVER_CACHE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.52 D9: test_cluster_cr_admit_stat — independent admission reason
# counter shmem region (cluster_cr_admit_stat.o) with malloc-backed
# ShmemInitStruct + no-op region-register stubs.
CLUSTER_CR_ADMIT_STAT_O = $(top_builddir)/src/backend/cluster/cluster_cr_admit_stat.o
test_cluster_cr_admit_stat: test_cluster_cr_admit_stat.c unit_test.h \
		$(CLUSTER_CR_ADMIT_STAT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_ADMIT_STAT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.54 D5: test_cluster_cr_tuple_stat — independent tuple-level CR fast-path
# outcome counter shmem region (cluster_cr_tuple_stat.o) with malloc-backed
# ShmemInitStruct + no-op region-register stubs.
CLUSTER_CR_TUPLE_STAT_O = $(top_builddir)/src/backend/cluster/cluster_cr_tuple_stat.o
test_cluster_cr_tuple_stat: test_cluster_cr_tuple_stat.c unit_test.h \
		$(CLUSTER_CR_TUPLE_STAT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_TUPLE_STAT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.54 D7: test_cluster_cr_tuple — D1 static eligibility predicate + D2
# TargetTupleCrState per-record transitions on a synthetic ITL page.  Links
# cluster_cr_tuple.o + cluster_cr_apply.o; stubs scn_time_cmp + the walk-driver
# heavy deps (undo reader / verdict-on-image) the per-record logic does not use.
CLUSTER_CR_TUPLE_O = $(top_builddir)/src/backend/cluster/cluster_cr_tuple.o
test_cluster_cr_tuple: test_cluster_cr_tuple.c unit_test.h \
		$(CLUSTER_CR_TUPLE_O) $(CLUSTER_CR_APPLY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_TUPLE_O) $(CLUSTER_CR_APPLY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.57 D9: test_cluster_cr_coordinator — cross-instance CR/current read-path
# coordinator boundary module (cluster_cr_coordinator_stat.o): independent
# observability counter region (malloc-backed ShmemInitStruct + no-op
# region-register stubs), the pure origin classifier, and the origin<->
# owner_instance namespace conversion.  Stubs cluster_node_id +
# cluster_merged_instance_is_materialized.
CLUSTER_CR_COORDINATOR_STAT_O = $(top_builddir)/src/backend/cluster/cluster_cr_coordinator_stat.o
test_cluster_cr_coordinator: test_cluster_cr_coordinator.c unit_test.h \
		$(CLUSTER_CR_COORDINATOR_STAT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_CR_COORDINATOR_STAT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.15 D2: test_cluster_xid_stripe — U1 truth tables for the xid
# stripe pure derivation layer (widen signed window / origin_slot floor
# semantics / next_striped candidate arithmetic incl. epoch carry +
# special-value skip / herding lag predicate / runtime latch wrappers).
# Links cluster_xid_stripe.o standalone; stubs ReadNextFullTransactionId.
CLUSTER_XID_STRIPE_O = $(top_builddir)/src/backend/cluster/cluster_xid_stripe.o
test_cluster_xid_stripe: test_cluster_xid_stripe.c unit_test.h \
		$(CLUSTER_XID_STRIPE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_XID_STRIPE_O) -o $@
