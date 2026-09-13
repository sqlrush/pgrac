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

ifeq ($(PORTNAME), darwin)
R4_RUNTIME_VIS_TEST_DEAD_STRIP = -Wl,-dead_strip
else
R4_RUNTIME_VIS_TEST_DEAD_STRIP = -Wl,--gc-sections
endif

# Test source files (each becomes a standalone executable)
TESTS = test_cluster_basic test_cluster_version test_cluster_backend_types \
        test_cluster_r4_production_reachability test_cluster_heap_update_temp_lock \
        test_cluster_gcs_reqid test_cluster_ctrc_cleaner test_cluster_ctrc_dispatch test_cluster_heap_receipt \
        test_cluster_ctrc_capacity test_cluster_undo_cleaner_capacity test_cluster_ctrc_itl_reuse \
        test_cluster_xnode_profile test_cluster_adg \
        test_cluster_xnode_lever test_cluster_ges_handoff \
        test_cluster_reverse_key test_cluster_hw_lease test_cluster_pi_shadow \
        test_cluster_wait_events test_cluster_errcodes test_cluster_guc \
        test_cluster_shmem test_cluster_signal test_cluster_views \
        test_cluster_gviews test_cluster_ic test_cluster_ic_envelope \
        test_cluster_ic_router test_cluster_ic_rdma \
        test_cluster_ic_tier1_partial \
        test_cluster_lms_outbound \
        test_cluster_conf \
        test_cluster_ic_mock test_cluster_inject test_cluster_pgstat \
        test_cluster_debug test_cluster_shared_fs test_cluster_shared_fs_sharedfs test_cluster_shared_fs_block_device test_cluster_raw_device_capacity test_cluster_smgr \
        test_cluster_scn test_cluster_scn_frontier test_cluster_block_format test_cluster_itl_slot \
        test_cluster_buffer_desc test_cluster_pcm_lock test_cluster_pcm_own test_cluster_pcm_direct_init test_cluster_resource_x_identity test_cluster_resource_x_node_wire test_cluster_resource_x_retry test_cluster_resource_x_handoff test_cluster_share_barrier test_cluster_heap_barrier test_cluster_bufmgr_pcm_hook test_cluster_gcs_dispatch test_cluster_gcs_block test_cluster_gcs_block_retransmit test_cluster_gcs_block_2way test_cluster_gcs_block_3way test_cluster_gcs_block_lost_write test_cluster_gcs_block_dedup_reclaim test_cluster_gcs_block_dedup_htab test_cluster_sinval test_cluster_sinval_ack test_cluster_stage2_acceptance test_cluster_tt_status test_cluster_tt_status_hint test_cluster_visibility_fork test_cluster_visibility_decide_scn test_cluster_snapshot_source test_cluster_itl_touch test_cluster_active_itl_transfer test_cluster_itl_wal test_cluster_uba \
        test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision \
        test_cluster_xlog test_cluster_xlog_insert_end test_cluster_tt_slot test_cluster_undo_segment \
        test_cluster_epoch test_cluster_epoch_ballot_codec test_cluster_fence test_cluster_reconfig test_cluster_replacement_episode test_cluster_replacement_request test_cluster_replacement_wire test_cluster_undo_root_descriptor test_cluster_marker_async \
        test_cluster_ges test_cluster_ges_reply_wait test_cluster_grd_outbound test_cluster_grd test_cluster_grd_starvation test_cluster_lmd test_cluster_lmd_graph test_cluster_lmd_wait_state test_cluster_cancel_token test_cluster_lmd_probe_collector test_cluster_lock_acquire test_cluster_advisory \
        test_cluster_tt_slot_allocator test_cluster_itl_reader_real_triple \
        test_cluster_hw_handoff test_cluster_lms_native_probe \
        test_cluster_itl_cleanout test_cluster_visibility_inject test_cluster_itl_cleanout_perf \
        test_cluster_heap_lock_tuple test_cluster_heap_epq_wait test_cluster_perf_gates \
	        test_cluster_subtrans test_cluster_multixact test_cluster_multixact_current test_cluster_multixact_current_stats test_cluster_multixact_served test_cluster_mxid_stripe \
		test_cluster_r4_d10_hint_source test_cluster_r4_d10_tt_source test_cluster_r4_d10_multi_source \
        test_cluster_undo_format \
		test_cluster_undo_record test_cluster_undo_lifecycle test_cluster_undo_block0 test_cluster_undo_block0_current test_cluster_undo_smgr_publication test_cluster_terminal_ref_census test_cluster_cr test_cluster_cr_cache test_cluster_cr_key test_cluster_cr_pool test_cluster_cr_lifecycle test_cluster_cr_admit test_cluster_cr_admit_stat test_cluster_cr_tuple test_cluster_cr_tuple_stat test_cluster_resolver_cache test_cluster_cr_coordinator test_cluster_r4_static_model test_cluster_r4_tx_locator test_cluster_r4_tx_outcome test_cluster_r4_tx_enqueue test_cluster_r4_wire_codec test_cluster_r4_route_policy test_cluster_r4_slot_machine test_cluster_r4_lms_controls test_cluster_r4_slot_reservation test_cluster_r4_observe test_cluster_r4_cr_walk test_cluster_r4_multi_subx_2pc test_cluster_r4_activation_record test_cluster_r4_activation_fsm test_cluster_r4_lock_order test_cluster_r4_scratch_resolver test_cluster_tt_durable test_cluster_terminal_authority test_cluster_sf_dep \
        test_cluster_retention test_cluster_undo_cleaner test_cluster_visibility_variants test_cluster_writer_chain test_cluster_tt_2pc \
        test_cluster_stage3_acceptance test_cluster_undo_buf test_cluster_undo_extent \
        test_cluster_wal_thread test_cluster_wal_state test_cluster_wal_state_rmw test_cluster_recovery_plan \
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
        test_cluster_shared_catalog \
        test_cluster_oid_lease \
        test_cluster_xid_authority \
        test_cluster_relmap_authority \
        test_cluster_hw \
        test_cluster_hw_remaster_retry \
        test_cluster_dl \
        test_cluster_extend_gate \
        test_cluster_recovery_serial \
		test_cluster_external_fence test_pgrac_external_fence_protocol \
		test_pgrac_fenced_config test_pgrac_fenced_core \
		test_pgrac_fenced_journal test_pgrac_fenced_provider \
		test_pgrac_fenced_operation test_pgrac_fenced_rejoin \
		test_pgrac_fenced_rejoin_async \
		test_pgrac_fenced_rejoin_coordinator \
		test_pgrac_fenced_session \
		test_pgrac_fenced_schedule test_pgrac_fenced_async \
		test_pgrac_fenced_coordinator \
		test_pgrac_fenced_ipmi test_pgrac_fenced_ipmi_exec \
		test_pgrac_fenced_ctl \
        test_cluster_ts \
        test_cluster_ko \
        test_cluster_hw_snapshot \
        test_cluster_cf_authority \
		test_cluster_control_root \
		test_cluster_wal_retention \
		test_cluster_recovery_duty \
		test_cluster_formation_witness \
		test_cluster_write_fence_durable \
		test_cluster_write_fence_cache \
		test_cluster_recovery_anchor \
		test_cluster_page_edge \
		test_cluster_page_edge_reader \
		test_cluster_rf_route \
		test_cluster_page_detached \
		test_cluster_page_guard \
		test_cluster_page_authority \
		test_cluster_thread_recovery_authority \
		test_cluster_thread_recovery_fabric_plan \
		test_cluster_thread_recovery_fabric_apply \
		test_cluster_thread_recovery_fabric_scan \
		test_cluster_thread_recovery_fabric_execute \
		test_cluster_page_online_plan \
		test_cluster_page_replay_batch \
		test_cluster_page_install \
		test_cluster_page_producer \
		test_cluster_page_anchor_cache \
		test_cluster_page_stable_base \
		test_cluster_side_xact \
		test_cluster_side_online_owner \
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
        test_cluster_xid_stripe \
        test_cluster_undo_resid \
        test_cluster_undo_authority \
        test_cluster_undo_gcs \
        test_cluster_undo_verdict \
		test_cluster_vis_undo_verdict_map \
		test_cluster_undo_horizon \
		test_cluster_lms_shard \
		test_cluster_gcs_block_dedup test_cluster_gcs_block_dedup_r4_route \
        test_cluster_gcs_block_shard

# Path to the cluster_version object (no PG deps, safe to link standalone).
TESTS += test_cluster_tt_abort_owner test_cluster_tt_active_owner
TESTS += test_cluster_cr_native_origin

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
all: $(TESTS) check_resource_x_send_c1

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
CLUSTER_QVOTEC_PGSA_TEST_O = cluster_qvotec_pgsa_test.o
CLUSTER_FENCE_O = $(top_builddir)/src/backend/cluster/cluster_fence.o
CLUSTER_RECONFIG_O = $(top_builddir)/src/backend/cluster/cluster_reconfig.o
CLUSTER_EPOCH_BALLOT_O = $(top_builddir)/src/backend/cluster/cluster_epoch_ballot.o
CLUSTER_REPLACEMENT_EPISODE_O = $(top_builddir)/src/backend/cluster/cluster_replacement_episode.o
CLUSTER_REPLACEMENT_REQUEST_O = $(top_builddir)/src/backend/cluster/cluster_replacement_request.o
CLUSTER_REPLACEMENT_WIRE_O = $(top_builddir)/src/backend/cluster/cluster_replacement_wire.o
CLUSTER_UNDO_ROOT_DESCRIPTOR_O = $(top_builddir)/src/backend/cluster/cluster_undo_root_descriptor.o
CLUSTER_VOTING_DISK_IO_O = $(top_builddir)/src/backend/cluster/cluster_voting_disk_io.o
CLUSTER_QUORUM_DECISION_O = $(top_builddir)/src/backend/cluster/cluster_quorum_decision.o
CLUSTER_SCN_O = $(top_builddir)/src/backend/cluster/cluster_scn.o
CLUSTER_ADG_O = $(top_builddir)/src/backend/cluster/cluster_adg.o
CLUSTER_LNS_O = $(top_builddir)/src/backend/cluster/cluster_lns.o
CLUSTER_GES_O = $(top_builddir)/src/backend/cluster/cluster_ges.o
CLUSTER_GES_REPLY_WAIT_O = $(top_builddir)/src/backend/cluster/cluster_ges_reply_wait.o
CLUSTER_GRD_OUTBOUND_O = $(top_builddir)/src/backend/cluster/cluster_grd_outbound.o
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
CLUSTER_PCM_OWN_O = $(top_builddir)/src/backend/cluster/cluster_pcm_own.o
CLUSTER_PCM_DIRECT_INIT_O = $(top_builddir)/src/backend/cluster/cluster_pcm_direct_init.o
CLUSTER_RESOURCE_X_IDENTITY_O = \
	$(top_builddir)/src/backend/cluster/cluster_resource_x_identity.o
CLUSTER_RESOURCE_X_NODE_WIRE_O = \
	$(top_builddir)/src/backend/cluster/cluster_resource_x_node_wire.o
CLUSTER_RESOURCE_X_RETRY_O = \
	$(top_builddir)/src/backend/cluster/cluster_resource_x_retry.o
CLUSTER_IC_TIER1_O = $(top_builddir)/src/backend/cluster/cluster_ic_tier1.o
CLUSTER_GCS_O = $(top_builddir)/src/backend/cluster/cluster_gcs.o

test_cluster_resource_x_identity: test_cluster_resource_x_identity.c \
		cluster_resource_x_wire_f076.h unit_test.h \
		$(CLUSTER_RESOURCE_X_IDENTITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_resource_x_node_wire: test_cluster_resource_x_node_wire.c unit_test.h \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) $(CLUSTER_RESOURCE_X_NODE_WIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) $(CLUSTER_RESOURCE_X_NODE_WIRE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_resource_x_retry: test_cluster_resource_x_retry.c unit_test.h \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) $(CLUSTER_RESOURCE_X_RETRY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) $(CLUSTER_RESOURCE_X_RETRY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_resource_x_handoff: test_cluster_resource_x_handoff.c unit_test.h \
		$(CLUSTER_RESOURCE_X_IDENTITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

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

# spec-7.3 D1: test_cluster_lms_shard links cluster_lms_shard.o (pure layer:
# BufferTag -> worker shard map).  Links libpgcommon_srv.a for
# hash_bytes_extended, mirroring test_cluster_grd.
CLUSTER_LMS_SHARD_O = $(top_builddir)/src/backend/cluster/cluster_lms_shard.o
CLUSTER_LMS_LIFECYCLE_O = $(top_builddir)/src/backend/cluster/cluster_lms_lifecycle.o
test_cluster_lms_shard: test_cluster_lms_shard.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_LMS_SHARD_O) $(CLUSTER_LMS_LIFECYCLE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMS_SHARD_O) $(CLUSTER_LMS_LIFECYCLE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-7.3 D5: test_cluster_gcs_block_dedup links cluster_gcs_block_dedup.o
# (real per-worker sharded lookup/install/GC/counter paths) + cluster_lms_shard.o,
# with the fake-shmem stub family in the test file.  libpgcommon_srv.a is pulled
# for hash_bytes_extended (cluster_lms_shard), mirroring test_cluster_lms_shard.
CLUSTER_GCS_BLOCK_DEDUP_O = $(top_builddir)/src/backend/cluster/cluster_gcs_block_dedup.o
test_cluster_gcs_block_dedup: test_cluster_gcs_block_dedup.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_GCS_BLOCK_DEDUP_O) $(CLUSTER_LMS_SHARD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GCS_BLOCK_DEDUP_O) $(CLUSTER_LMS_SHARD_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-7.3 D9: test_cluster_gcs_block_shard links cluster_gcs_block_shard.o
# (the REAL staging-path payload -> worker router, extracted as a pure file)
# + cluster_lms_shard.o so the route == shard_for_tag agreement is asserted
# against both real objects.  libpgcommon_srv.a for hash_bytes_extended.
CLUSTER_GCS_BLOCK_SHARD_O = $(top_builddir)/src/backend/cluster/cluster_gcs_block_shard.o
test_cluster_gcs_block_shard: test_cluster_gcs_block_shard.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_GCS_BLOCK_SHARD_O) $(CLUSTER_LMS_SHARD_O) \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) $(CLUSTER_RESOURCE_X_NODE_WIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GCS_BLOCK_SHARD_O) $(CLUSTER_LMS_SHARD_O) \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) $(CLUSTER_RESOURCE_X_NODE_WIRE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
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

TESTS += test_cluster_pcm_aux_consumer test_cluster_pcm_aux_reobserve test_cluster_resource_x_terminal_ingress
TESTS += test_cluster_pcm_aux_mutation

# Most tests link only cluster_version.o.  test_cluster_guc /
# test_cluster_shmem / test_cluster_signal / test_cluster_views /
# test_cluster_gviews / test_cluster_ic / test_cluster_conf have
# separate rules because they also link additional cluster_*.o
# objects (the test files stub the PG backend symbols those
# objects reference).
SIMPLE_TESTS = $(filter-out test_cluster_ic_tier1_partial test_cluster_lms_outbound test_cluster_guc test_cluster_shmem test_cluster_signal test_cluster_views test_cluster_gviews test_cluster_ic test_cluster_conf test_cluster_ic_mock test_cluster_inject test_cluster_pgstat test_cluster_debug test_cluster_shared_fs test_cluster_shared_fs_sharedfs test_cluster_shared_fs_block_device test_cluster_smgr test_cluster_startup_phase test_cluster_lmon test_cluster_lck test_cluster_diag test_cluster_stats test_cluster_cssd test_cluster_qvotec test_cluster_voting_disk_io test_cluster_quorum_decision test_cluster_scn test_cluster_scn_frontier test_cluster_adg test_cluster_epoch test_cluster_epoch_ballot_codec test_cluster_fence test_cluster_reconfig test_cluster_ges test_cluster_grd_outbound test_cluster_grd test_cluster_grd_starvation test_cluster_lmd test_cluster_lmd_graph test_cluster_lmd_wait_state test_cluster_cancel_token test_cluster_lmd_probe_collector test_cluster_lock_acquire test_cluster_advisory test_cluster_terminal_authority test_cluster_retention test_cluster_visibility_variants test_cluster_writer_chain test_cluster_tt_2pc test_cluster_stage3_acceptance test_cluster_undo_buf test_cluster_block_apply test_cluster_thread_apply test_cluster_thread_replay test_cluster_thread_driver test_cluster_thread_orchestrator test_cluster_write_fence test_cluster_write_fence_durable test_cluster_write_fence_cache test_cluster_stage4_acceptance test_cluster_stage5_integrated_acceptance test_cluster_stage5_beta_acceptance test_cluster_ges_mode test_cluster_sequence test_cluster_shared_catalog test_cluster_hw test_cluster_dl test_cluster_extend_gate test_cluster_recovery_serial test_cluster_ts test_cluster_ko test_cluster_hw_snapshot test_cluster_cf_authority test_cluster_control_root test_cluster_recovery_duty test_cluster_formation_witness test_cluster_cf_storage test_cluster_cf_enqueue test_cluster_cf_phase2 test_cluster_cf_stats test_cluster_hang test_cluster_hang_resolve test_cluster_cr_server_policy test_cluster_touched_peers test_cluster_clean_leave test_cluster_membership test_cluster_node_remove test_cluster_resolver_cache test_cluster_backup test_cluster_hang_acceptance test_cluster_gcs_reqid test_cluster_runtime_visibility test_cluster_xid_stripe test_cluster_mxid_stripe test_cluster_share_barrier test_cluster_heap_barrier test_cluster_bufmgr_pcm_hook test_cluster_cr test_cluster_cr_admit test_cluster_cr_admit_stat test_cluster_cr_cache test_cluster_cr_coordinator test_cluster_cr_key test_cluster_cr_lifecycle test_cluster_cr_pool test_cluster_cr_tuple test_cluster_cr_tuple_stat test_cluster_gcs_block test_cluster_gcs_block_2way test_cluster_gcs_block_3way test_cluster_gcs_block_lost_write test_cluster_gcs_block_retransmit test_cluster_gcs_block_dedup_reclaim test_cluster_gcs_block_dedup_htab test_cluster_gcs_dispatch test_cluster_ges_handoff test_cluster_heap_lock_tuple test_cluster_hw_lease test_cluster_ic_envelope test_cluster_ic_router test_cluster_itl_cleanout test_cluster_itl_cleanout_perf test_cluster_itl_reader_real_triple test_cluster_itl_touch test_cluster_active_itl_transfer test_cluster_itl_wal test_cluster_multixact test_cluster_multixact_current test_cluster_multixact_served test_cluster_pcm_lock test_cluster_pcm_own test_cluster_pcm_direct_init test_cluster_perf_gates test_cluster_recovery_merge test_cluster_recovery_plan test_cluster_recovery_worker test_cluster_reverse_key test_cluster_sinval test_cluster_sinval_ack test_cluster_snapshot_source test_cluster_stage2_acceptance test_cluster_stage5_5_cr_acceptance test_cluster_subtrans test_cluster_tt_durable test_cluster_tt_slot_allocator test_cluster_tt_status test_cluster_tt_status_hint test_cluster_uba test_cluster_undo_format test_cluster_undo_lifecycle test_cluster_undo_record test_cluster_undo_block0 test_cluster_undo_smgr_publication test_cluster_visibility_decide_scn test_cluster_visibility_fork test_cluster_visibility_inject test_cluster_wal_state test_cluster_wal_thread test_cluster_xnode_lever test_cluster_xnode_profile test_cluster_pi_shadow test_cluster_oid_lease test_cluster_xid_authority test_cluster_recovery_anchor test_cluster_relmap_authority test_cluster_lms_shard test_cluster_gcs_block_dedup test_cluster_gcs_block_shard test_cluster_undo_resid test_cluster_undo_authority test_cluster_undo_gcs test_cluster_undo_verdict test_cluster_vis_undo_verdict_map test_cluster_undo_horizon test_cluster_r4_static_model test_cluster_r4_tx_locator test_cluster_r4_tx_outcome test_cluster_r4_cr_walk test_cluster_r4_activation_record test_cluster_r4_activation_fsm test_cluster_r4_lock_order,$(TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_cr_native_origin test_cluster_tt_abort_owner test_cluster_tt_active_owner test_cluster_pcm_aux_consumer test_cluster_pcm_aux_reobserve test_cluster_multixact_current_stats \
		test_cluster_r4_production_reachability test_cluster_heap_update_temp_lock \
		test_cluster_pcm_aux_mutation \
		test_cluster_resource_x_identity test_cluster_resource_x_node_wire \
		test_cluster_resource_x_terminal_ingress \
		test_cluster_resource_x_retry \
		test_cluster_resource_x_handoff \
		test_cluster_r4_d10_hint_source test_cluster_r4_d10_tt_source \
		test_cluster_r4_d10_multi_source test_cluster_sf_dep test_cluster_wal_state_rmw \
		test_cluster_ges_reply_wait \
		test_cluster_replacement_episode \
		test_cluster_replacement_request \
		test_cluster_undo_root_descriptor \
		test_cluster_xlog_insert_end \
		test_cluster_undo_block0_current \
		test_cluster_gcs_block_dedup_r4_route test_cluster_r4_route_policy \
		test_cluster_r4_lms_controls test_cluster_r4_slot_reservation \
		test_cluster_r4_observe test_cluster_r4_multi_subx_2pc \
		test_cluster_r4_scratch_resolver,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_raw_device_capacity test_cluster_replacement_wire \
		test_cluster_external_fence test_pgrac_external_fence_protocol \
		test_cluster_wal_retention test_cluster_terminal_ref_census test_cluster_ctrc_cleaner test_cluster_ctrc_dispatch test_cluster_heap_receipt \
		test_cluster_ctrc_capacity test_cluster_undo_cleaner_capacity,$(SIMPLE_TESTS))

SIMPLE_TESTS := $(filter-out test_pgrac_fenced_config test_pgrac_fenced_core \
		test_pgrac_fenced_journal test_pgrac_fenced_provider \
		test_pgrac_fenced_operation test_pgrac_fenced_rejoin \
		test_pgrac_fenced_rejoin_async \
		test_pgrac_fenced_rejoin_coordinator \
		test_pgrac_fenced_session \
		test_pgrac_fenced_schedule test_pgrac_fenced_async \
		test_pgrac_fenced_coordinator \
		test_pgrac_fenced_ipmi test_pgrac_fenced_ipmi_exec \
		test_pgrac_fenced_ctl,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_edge,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_hw_handoff,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_edge_reader,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_heap_epq_wait,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_rf_route,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_detached,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_guard,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_authority,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_thread_recovery_fabric_plan,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_thread_recovery_fabric_apply,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_thread_recovery_fabric_scan,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_thread_recovery_fabric_execute,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_thread_recovery_authority,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_online_plan,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_replay_batch,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_install,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_producer,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_anchor_cache,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_page_stable_base,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_side_xact,$(SIMPLE_TESTS))
SIMPLE_TESTS := $(filter-out test_cluster_side_online_owner,$(SIMPLE_TESTS))

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

# spec-5.7 D8 / RF-ROOT P3: test_cluster_recovery_serial links the pure IR
# encoder plus the typed acquire/release backend.  The test supplies bounded
# backend/GES stubs so
# confirmed versus unconfirmed holder release is exercised without shmem/IPC.
CLUSTER_IR_O = $(top_builddir)/src/backend/cluster/cluster_ir.o
CLUSTER_IR_LOCK_O = $(top_builddir)/src/backend/cluster/cluster_ir_lock.o
test_cluster_recovery_serial: test_cluster_recovery_serial.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_IR_O) $(CLUSTER_IR_LOCK_O) $(CLUSTER_DL_O) $(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IR_O) $(CLUSTER_IR_LOCK_O) \
		$(CLUSTER_DL_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_EXTERNAL_FENCE_O = test_cluster_external_fence_product.o
PGRAC_EXTERNAL_FENCE_PROTOCOL_O = \
	$(top_builddir)/src/common/pgrac_external_fence_protocol_srv.o
$(PGRAC_EXTERNAL_FENCE_PROTOCOL_O): \
		$(top_srcdir)/src/common/pgrac_external_fence_protocol.c
	$(MAKE) -C $(top_builddir)/src/common pgrac_external_fence_protocol_srv.o
$(CLUSTER_EXTERNAL_FENCE_O): \
		$(top_srcdir)/src/backend/cluster/cluster_external_fence.c \
		$(top_srcdir)/src/include/cluster/cluster_external_fence.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT \
		-c $(top_srcdir)/src/backend/cluster/cluster_external_fence.c -o $@
test_cluster_external_fence: test_cluster_external_fence.c unit_test.h \
		$(CLUSTER_EXTERNAL_FENCE_O) $(PGRAC_EXTERNAL_FENCE_PROTOCOL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_EXTERNAL_FENCE_O) \
		$(PGRAC_EXTERNAL_FENCE_PROTOCOL_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

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
CLUSTER_CR_SERVER_C0_TEST_O = test_cluster_cr_server_c0_product.o
$(CLUSTER_CR_SERVER_C0_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_cr_server.c \
		$(top_srcdir)/src/include/cluster/cluster_cr_server.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

test_cluster_cr_server_policy: test_cluster_cr_server_policy.c unit_test.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_CR_SERVER_POLICY_O) $(CLUSTER_CR_SERVER_C0_TEST_O) \
		$(top_builddir)/src/backend/cluster/cluster_undo_retention.o
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CR_SERVER_POLICY_O) $(CLUSTER_CR_SERVER_C0_TEST_O) \
		$(top_builddir)/src/backend/cluster/cluster_undo_retention.o \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
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
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DCLEAN_LEAVE_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_clean_leave.c"' \
		-DCHECKPOINTER_SOURCE_PATH='"$(top_srcdir)/src/backend/postmaster/checkpointer.c"' \
		-DPOSTMASTER_SOURCE_PATH='"$(top_srcdir)/src/backend/postmaster/postmaster.c"' \
		-DAUXPROCESS_SOURCE_PATH='"$(top_srcdir)/src/backend/postmaster/auxprocess.c"' \
		-DPGSTAT_SOURCE_PATH='"$(top_srcdir)/src/backend/utils/activity/pgstat.c"' \
		$< \
		$(CLUSTER_VERSION_O) $(CLUSTER_CLEAN_LEAVE_POLICY_O) \
		$(top_builddir)/src/port/libpgport.a -o $@

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

# spec-5.15A §2.4: exact 96-byte replacement episode local mirror and
# dependency-free structural/identity/generation validation.
test_cluster_epoch_ballot_codec: test_cluster_epoch_ballot_codec.c unit_test.h \
		$(CLUSTER_EPOCH_BALLOT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_EPOCH_BALLOT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_replacement_episode: test_cluster_replacement_episode.c unit_test.h \
		$(CLUSTER_REPLACEMENT_EPISODE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_REPLACEMENT_EPISODE_O) -o $@

# spec-5.15A §2.1: pure exact-64-byte RPLM LE/CRC and reserved1 slice codec.
test_cluster_replacement_request: test_cluster_replacement_request.c unit_test.h \
		$(CLUSTER_REPLACEMENT_REQUEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_REPLACEMENT_REQUEST_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.15A §2.3: dependency-free opcode-18 exact 72-byte LE codec.
test_cluster_replacement_wire: test_cluster_replacement_wire.c unit_test.h \
		$(CLUSTER_REPLACEMENT_WIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_REPLACEMENT_WIRE_O) -o $@

# spec-8.4A §2.1A: pure exact-512-byte PGRD V1 codec and checked
# root-namespace/file-id arithmetic.  CRC32C comes from libpgport.
test_cluster_undo_root_descriptor: test_cluster_undo_root_descriptor.c unit_test.h \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_UNDO_ROOT_DESCRIPTOR_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-ROOT P9 verification (cold-formation cold-formation ruling): pure
# 512-byte formation-commit-marker codec (encode/decode + wire CRC over
# header + compact incarnation table + 48/128 capacity clamp + corrupt
# n_admitted out-of-bounds guard).  CRC32C comes from libpgport; the
# codec has no PG-backend dependencies.
CLUSTER_FORMATION_MARKER_O = $(top_builddir)/src/backend/cluster/cluster_formation_marker.o
test_cluster_formation_marker: test_cluster_formation_marker.c unit_test.h \
		$(CLUSTER_FORMATION_MARKER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_FORMATION_MARKER_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-01: PageVersion semantic type (valid/exact-equality),
# §3.2 expected-before -> result-version admission decision with the §3.3
# shape-1 trusted skip, and the §4.1 closed page/record classifier
# (unknown/ambiguous default UNKNOWN/BLOCKED; rmgr known-set registry
# compiled empty — PGDEL-02 owns the census).  Pure in-memory layer: no
# backend deps beyond the Assert hook.
XLOG_PAGE_EDGE_ENCODER_O = xloginsert_page_edge_encoder.o
xloginsert_page_edge_encoder.o: \
		$(top_srcdir)/src/backend/access/transam/xloginsert.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DPGRAC_XLOG_PAGE_EDGE_ENCODER_ONLY \
		-c $< -o $@

test_cluster_page_edge: test_cluster_page_edge.c unit_test.h \
		$(XLOG_PAGE_EDGE_ENCODER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(XLOG_PAGE_EDGE_ENCODER_O) \
		$(top_builddir)/src/port/libpgport.a -o $@

test_cluster_page_edge_reader: test_cluster_page_edge_reader.c unit_test.h \
		xlogreader_fs.o $(XLOG_PAGE_EDGE_ENCODER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< xlogreader_fs.o \
		$(XLOG_PAGE_EDGE_ENCODER_O) $(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) \
		-lssl -lcrypto -llz4 -lzstd -o $@

CLUSTER_RF_ROUTE_O = $(top_builddir)/src/backend/cluster/cluster_rf_route.o

$(CLUSTER_RF_ROUTE_O): $(top_srcdir)/src/backend/cluster/cluster_rf_route.c \
		$(top_srcdir)/src/include/cluster/cluster_rf_route.h \
		$(top_srcdir)/src/include/cluster/cluster_rf_route_manifest.def
	$(MAKE) -C $(top_builddir)/src/backend/cluster cluster_rf_route.o

test_cluster_rf_route: test_cluster_rf_route.c unit_test.h \
		$(CLUSTER_RF_ROUTE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_RF_ROUTE_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) \
		-lssl -lcrypto -o $@

CLUSTER_PAGE_DETACHED_O = \
		$(top_builddir)/src/backend/cluster/cluster_page_detached.o

CLUSTER_PAGE_INSTALL_O = cluster_page_install_test.o

test_cluster_page_detached: test_cluster_page_detached.c unit_test.h \
		$(CLUSTER_PAGE_DETACHED_O) $(CLUSTER_RF_ROUTE_O) \
		$(CLUSTER_BLOCK_APPLY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_DETACHED_O) \
		$(CLUSTER_RF_ROUTE_O) $(CLUSTER_BLOCK_APPLY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_PAGE_REPLAY_BATCH_O = cluster_page_replay_batch_test.o

CLUSTER_PAGE_GUARD_O = cluster_page_guard_test.o

$(CLUSTER_PAGE_GUARD_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_guard.c \
		$(top_srcdir)/src/include/cluster/cluster_page_guard.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_guard: test_cluster_page_guard.c unit_test.h \
		$(CLUSTER_PAGE_GUARD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_GUARD_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_PAGE_AUTHORITY_O = cluster_page_authority_test.o

$(CLUSTER_PAGE_AUTHORITY_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_authority.c \
		$(top_srcdir)/src/include/cluster/cluster_page_authority.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_authority: test_cluster_page_authority.c unit_test.h \
		$(CLUSTER_PAGE_AUTHORITY_O) $(CLUSTER_PAGE_GUARD_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< $(CLUSTER_PAGE_AUTHORITY_O) \
		$(CLUSTER_PAGE_GUARD_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_THREAD_RECOVERY_AUTHORITY_O = \
		cluster_thread_recovery_authority_test.o \
		cluster_thread_recovery_finalize_test.o

cluster_thread_recovery_authority_test.o: \
		$(top_srcdir)/src/backend/cluster/cluster_thread_recovery_authority.c \
		$(top_srcdir)/src/include/cluster/cluster_thread_recovery_authority.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

cluster_thread_recovery_finalize_test.o: \
		$(top_srcdir)/src/backend/cluster/cluster_thread_recovery_finalize.c \
		$(top_srcdir)/src/include/cluster/cluster_thread_recovery_authority.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_thread_recovery_authority: \
		test_cluster_thread_recovery_authority.c unit_test.h \
		$(CLUSTER_THREAD_RECOVERY_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_THREAD_RECOVERY_AUTHORITY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_O = \
		cluster_thread_recovery_fabric_plan_test.o

$(CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_O): \
		$(top_srcdir)/src/backend/cluster/cluster_thread_recovery_fabric_plan.c \
		$(top_srcdir)/src/include/cluster/cluster_thread_recovery_fabric.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_thread_recovery_fabric_plan: \
		test_cluster_thread_recovery_fabric_plan.c unit_test.h \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_THREAD_RECOVERY_FABRIC_APPLY_O = \
		cluster_thread_recovery_fabric_apply_test.o

$(CLUSTER_THREAD_RECOVERY_FABRIC_APPLY_O): \
		$(top_srcdir)/src/backend/cluster/cluster_thread_recovery_fabric_apply.c \
		$(top_srcdir)/src/include/cluster/cluster_thread_recovery_fabric.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_thread_recovery_fabric_apply: \
		test_cluster_thread_recovery_fabric_apply.c unit_test.h \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_APPLY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_APPLY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_THREAD_RECOVERY_FABRIC_SCAN_O = \
		cluster_thread_recovery_fabric_scan_test.o

$(CLUSTER_THREAD_RECOVERY_FABRIC_SCAN_O): \
		$(top_srcdir)/src/backend/cluster/cluster_thread_recovery_fabric_scan.c \
		$(top_srcdir)/src/include/cluster/cluster_thread_recovery_fabric.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_thread_recovery_fabric_scan: \
		test_cluster_thread_recovery_fabric_scan.c unit_test.h \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_SCAN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_SCAN_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_THREAD_RECOVERY_FABRIC_EXECUTE_O = \
		cluster_thread_recovery_fabric_execute_test.o

$(CLUSTER_THREAD_RECOVERY_FABRIC_EXECUTE_O): \
		$(top_srcdir)/src/backend/cluster/cluster_thread_recovery_fabric_execute.c \
		$(top_srcdir)/src/include/cluster/cluster_thread_recovery_fabric.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_thread_recovery_fabric_execute: \
		test_cluster_thread_recovery_fabric_execute.c unit_test.h \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_EXECUTE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_THREAD_RECOVERY_FABRIC_EXECUTE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_PAGE_ONLINE_PLAN_O = cluster_page_online_plan_test.o

$(CLUSTER_PAGE_ONLINE_PLAN_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_online_plan.c \
		$(top_srcdir)/src/include/cluster/cluster_page_online_plan.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_online_plan: test_cluster_page_online_plan.c unit_test.h \
		$(CLUSTER_PAGE_ONLINE_PLAN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_PAGE_ONLINE_PLAN_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) \
		-lssl -lcrypto -o $@

$(CLUSTER_PAGE_REPLAY_BATCH_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_replay_batch.c \
		$(top_srcdir)/src/include/cluster/cluster_page_replay_batch.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_replay_batch: test_cluster_page_replay_batch.c unit_test.h \
		$(CLUSTER_PAGE_REPLAY_BATCH_O) $(CLUSTER_PAGE_INSTALL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_REPLAY_BATCH_O) \
		$(CLUSTER_PAGE_INSTALL_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

$(CLUSTER_PAGE_INSTALL_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_install.c \
		$(top_srcdir)/src/include/cluster/cluster_page_install.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_install: test_cluster_page_install.c unit_test.h \
		$(CLUSTER_PAGE_INSTALL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_INSTALL_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_PAGE_PRODUCER_O = cluster_page_producer_test.o

$(CLUSTER_PAGE_PRODUCER_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_producer.c \
		$(top_srcdir)/src/include/cluster/cluster_page_producer.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_producer: test_cluster_page_producer.c unit_test.h \
		$(CLUSTER_PAGE_PRODUCER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_PAGE_PRODUCER_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_PAGE_ANCHOR_CACHE_O = \
		$(top_builddir)/src/backend/cluster/cluster_page_anchor_cache.o
CLUSTER_PAGE_STABLE_BASE_O = cluster_page_stable_base_test.o

$(CLUSTER_PAGE_ANCHOR_CACHE_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_anchor_cache.c \
		$(top_srcdir)/src/include/cluster/cluster_page_anchor_cache.h
	$(MAKE) -C $(top_builddir)/src/backend/cluster cluster_page_anchor_cache.o

test_cluster_page_anchor_cache: test_cluster_page_anchor_cache.c unit_test.h \
		$(CLUSTER_PAGE_ANCHOR_CACHE_O) $(CLUSTER_PAGE_STABLE_BASE_O) \
		xlogreader_fs.o
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_PAGE_ANCHOR_CACHE_O) $(CLUSTER_PAGE_STABLE_BASE_O) \
		xlogreader_fs.o $(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) \
		-lssl -lcrypto -llz4 -lzstd -o $@
$(CLUSTER_PAGE_STABLE_BASE_O): \
		$(top_srcdir)/src/backend/cluster/cluster_page_stable_base.c \
		$(top_srcdir)/src/include/cluster/cluster_page_stable_base.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -c $< -o $@

test_cluster_page_stable_base: test_cluster_page_stable_base.c unit_test.h \
		$(CLUSTER_PAGE_STABLE_BASE_O) xlogreader_fs.o
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT $< \
		$(CLUSTER_PAGE_STABLE_BASE_O) xlogreader_fs.o \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) \
		-lssl -lcrypto -llz4 -lzstd -o $@

CLUSTER_PAGE_VERSION_O = $(top_builddir)/src/backend/cluster/cluster_page_version.o
test_cluster_page_version: test_cluster_page_version.c unit_test.h \
		$(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-02: rmgr census (closed-table row assignment + t/256
# differential-evidence state), census -> classifier wiring, and the
# §3.1 hints decoder (identity + record-side facts; no version judgement).
# Links cluster_page_rmgr.o + cluster_page_version.o; the fabricator
# constructs a minimal XLogReaderState, so no backend objects are needed.
CLUSTER_PAGE_RMGR_O = $(top_builddir)/src/backend/cluster/cluster_page_rmgr.o
test_cluster_page_rmgr: test_cluster_page_rmgr.c unit_test.h \
		$(CLUSTER_PAGE_RMGR_O) $(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_RMGR_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-03: §4.1 recovery-action table, §3.5 per-block state
# machine (adjacent advance only) and §8.1 dispatcher verdict.  Pure
# in-memory layer.
CLUSTER_PAGE_RECOVERY_O = $(top_builddir)/src/backend/cluster/cluster_page_recovery.o
test_cluster_page_recovery: test_cluster_page_recovery.c unit_test.h \
		$(CLUSTER_PAGE_RECOVERY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_RECOVERY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-04: CURRENT/PI/STORAGE provenance validators (§5.2/5.3/
# 5.4 conjunctions) + §5.5/§6.2 source selection.  Pure judgement over
# caller-declared typed facts.
CLUSTER_PAGE_SOURCE_O = $(top_builddir)/src/backend/cluster/cluster_page_source.o
test_cluster_page_source: test_cluster_page_source.c unit_test.h \
		$(CLUSTER_PAGE_SOURCE_O) $(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_SOURCE_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-05: in-memory per-block recovery set + §6.3 contributor
# closure + §3.3 shape-2 chain skip.  Pure judgement over the ordered
# contributor list.
CLUSTER_PAGE_SET_O = $(top_builddir)/src/backend/cluster/cluster_page_set.o
test_cluster_page_set: test_cluster_page_set.c unit_test.h \
		$(CLUSTER_PAGE_SET_O) $(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_SET_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-06: §7.1 mutation admission, §7.2 sequence-step gates,
# §7.6 stable-base STOP (permanent RED) and §7.7 crash-matrix verdicts.
# Judgement-only layer; links the recovery state machine too.
CLUSTER_PAGE_APPLY_O = $(top_builddir)/src/backend/cluster/cluster_page_apply.o
test_cluster_page_apply: test_cluster_page_apply.c unit_test.h \
		$(CLUSTER_PAGE_APPLY_O) $(CLUSTER_PAGE_RECOVERY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_APPLY_O) $(CLUSTER_PAGE_RECOVERY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-07: §7.4 FND-10 per-resource handoff judgement + PL-12
# retention denial.  Judgement-only; links the apply layer for the
# ClusterPageProof type.
CLUSTER_PAGE_HANDOFF_O = $(top_builddir)/src/backend/cluster/cluster_page_handoff.o
test_cluster_page_handoff: test_cluster_page_handoff.c unit_test.h \
		$(CLUSTER_PAGE_HANDOFF_O) $(CLUSTER_PAGE_APPLY_O) $(CLUSTER_PAGE_RECOVERY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_HANDOFF_O) $(CLUSTER_PAGE_APPLY_O) \
		$(CLUSTER_PAGE_RECOVERY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-08: §9.1 recovery-only counters (G2-classified),
# vocabulary table and §9.3 attempt dump.  GetCurrentTimestamp comes
# from the port library; no backend objects needed.
CLUSTER_PAGE_STATS_O = $(top_builddir)/src/backend/cluster/cluster_page_stats.o
test_cluster_page_stats: test_cluster_page_stats.c unit_test.h \
		$(CLUSTER_PAGE_STATS_O) $(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_STATS_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-09: PU matrix + unit-level PL face over the delivered
# semantic layers (classifier/decide/closure/source/handoff/crash matrix).
CLUSTER_PAGE_MATRIX_LIBS = $(CLUSTER_PAGE_APPLY_O) $(CLUSTER_PAGE_HANDOFF_O) \
		$(CLUSTER_PAGE_RECOVERY_O) $(CLUSTER_PAGE_SET_O) $(CLUSTER_PAGE_SOURCE_O) \
		$(CLUSTER_PAGE_VERSION_O)
test_cluster_page_recovery_matrix: test_cluster_page_recovery_matrix.c unit_test.h \
		$(CLUSTER_PAGE_MATRIX_LIBS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PAGE_MATRIX_LIBS) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-PAGE PGDEL-10: ABI-baseline guards (page header / WAL record header /
# buffer tag / relfilelocator sizes frozen at the PGRAC HEAD baseline).
test_cluster_page_abi: test_cluster_page_abi.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-06/07/08: RF-PAGE integration verdict, per-resource
# readiness, retention proof exporter.  Links the page version layer.
CLUSTER_SIDE_RECOVERY_O = $(top_builddir)/src/backend/cluster/cluster_side_recovery.o
test_cluster_side_recovery: test_cluster_side_recovery.c unit_test.h \
		$(CLUSTER_SIDE_RECOVERY_O) $(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_RECOVERY_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-01: total route registry (§3.2 matrix) + §2.1 verdicts.
CLUSTER_SIDE_ROUTE_O = $(top_builddir)/src/backend/cluster/cluster_side_route.o
test_cluster_side_route: test_cluster_side_route.c unit_test.h \
		$(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-02: TT/undo decode + preflight primitives (single
# parser; malformed/unknown -> BLOCKED pre-mutation).
CLUSTER_SIDE_UNDO_O = $(top_builddir)/src/backend/cluster/cluster_side_undo.o
test_cluster_side_undo: test_cluster_side_undo.c unit_test.h \
		$(CLUSTER_SIDE_UNDO_O) $(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_UNDO_O) $(CLUSTER_SIDE_ROUTE_O) \
		$(CLUSTER_RF_ROUTE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_SIDE_XACT_O = test_cluster_side_xact_product.o
CLUSTER_REMOTE_XACT_IDENTITY_O = \
	$(top_builddir)/src/backend/cluster/cluster_remote_xact_identity.o
$(CLUSTER_SIDE_XACT_O): $(top_srcdir)/src/backend/cluster/cluster_side_xact.c \
		$(top_srcdir)/src/include/cluster/cluster_side_xact.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DRF_SIDE_XACT_TESTING \
		-ffunction-sections -fdata-sections -c $< -o $@
CLUSTER_SIDE_ONLINE_PLAN_TEST_O = test_cluster_side_online_plan.o
$(CLUSTER_SIDE_ONLINE_PLAN_TEST_O): \
		$(top_srcdir)/src/backend/cluster/cluster_side_online_plan.c \
		$(top_srcdir)/src/include/cluster/cluster_side_online_plan.h \
		$(top_srcdir)/src/include/cluster/cluster_side_xact.h \
		$(top_srcdir)/src/include/cluster/cluster_side_undo.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DRF_SIDE_ONLINE_TESTING -c $< -o $@
SIDE_XACT_XACTDESC_O = test_cluster_side_xact_xactdesc.o
$(SIDE_XACT_XACTDESC_O): $(top_srcdir)/src/backend/access/rmgrdesc/xactdesc.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@
SIDE_XACT_TT_2PC_RECORD_O = $(top_builddir)/src/backend/cluster/cluster_tt_2pc_record.o
test_cluster_side_xact: test_cluster_side_xact.c unit_test.h \
		$(CLUSTER_SIDE_XACT_O) $(CLUSTER_SIDE_ONLINE_PLAN_TEST_O) \
		$(CLUSTER_REMOTE_XACT_IDENTITY_O) \
		$(SIDE_XACT_XACTDESC_O) $(SIDE_XACT_TT_2PC_RECORD_O) \
		$(CLUSTER_SIDE_UNDO_O) $(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_XACT_O) \
		$(CLUSTER_SIDE_ONLINE_PLAN_TEST_O) $(SIDE_XACT_XACTDESC_O) \
		$(CLUSTER_REMOTE_XACT_IDENTITY_O) \
		$(SIDE_XACT_TT_2PC_RECORD_O) $(CLUSTER_SIDE_UNDO_O) \
		$(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) $(LDFLAGS) -lssl -lcrypto -o $@

CLUSTER_SIDE_ONLINE_OWNER_O = \
	$(top_builddir)/src/backend/cluster/cluster_side_online_owner.o
test_cluster_side_online_owner: test_cluster_side_online_owner.c unit_test.h \
		$(CLUSTER_SIDE_ONLINE_OWNER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_ONLINE_OWNER_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-03: PREPARED/in-doubt binding + RECO-style resolution
# judgements.
CLUSTER_SIDE_PREPARED_O = $(top_builddir)/src/backend/cluster/cluster_side_prepared.o
test_cluster_side_prepared: test_cluster_side_prepared.c unit_test.h \
		$(CLUSTER_SIDE_PREPARED_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_PREPARED_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-04: CLOG/MULTIXACT/COMMIT_TS derived-projection
# judgements (verified / rebuildable / fail-closed lookup).
CLUSTER_SIDE_PROJECTION_O = $(top_builddir)/src/backend/cluster/cluster_side_projection.o
test_cluster_side_projection: test_cluster_side_projection.c unit_test.h \
		$(CLUSTER_SIDE_PROJECTION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_PROJECTION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-05: canonical space metadata gates (HWM/extent/bitmap)
# under STOP-RF-SIDE-SPACE-ABI.
CLUSTER_SIDE_SPACE_O = $(top_builddir)/src/backend/cluster/cluster_side_space.o
test_cluster_side_space: test_cluster_side_space.c unit_test.h \
		$(CLUSTER_SIDE_SPACE_O) $(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_SPACE_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-10: observability counters (observational only —
# U-SIDE-16: counters can never change a verdict).
CLUSTER_SIDE_STATS_O = $(top_builddir)/src/backend/cluster/cluster_side_stats.o
test_cluster_side_stats: test_cluster_side_stats.c unit_test.h \
		$(CLUSTER_SIDE_STATS_O) $(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O) \
		$(CLUSTER_SIDE_RECOVERY_O) \
		$(CLUSTER_PAGE_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_STATS_O) $(CLUSTER_SIDE_ROUTE_O) \
		$(CLUSTER_RF_ROUTE_O) $(CLUSTER_SIDE_RECOVERY_O) $(CLUSTER_PAGE_VERSION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# RF-SIDE D-SIDE-09: fault corpus judgement face — U-SIDE-17 + L1..L20
# legs over the delivered judgement layers.
CLUSTER_SIDE_CORPUS_LIBS = $(CLUSTER_PAGE_APPLY_O) $(CLUSTER_PAGE_SET_O) \
		$(CLUSTER_PAGE_SOURCE_O) $(CLUSTER_PAGE_RECOVERY_O) \
		$(CLUSTER_SIDE_PREPARED_O) $(CLUSTER_SIDE_PROJECTION_O) \
		$(CLUSTER_SIDE_RECOVERY_O) $(CLUSTER_SIDE_ROUTE_O) $(CLUSTER_RF_ROUTE_O) \
		$(CLUSTER_SIDE_SPACE_O) $(CLUSTER_PAGE_VERSION_O) \
		$(CLUSTER_PAGE_HANDOFF_O)
test_cluster_side_corpus: test_cluster_side_corpus.c unit_test.h \
		$(CLUSTER_SIDE_CORPUS_LIBS)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_SIDE_CORPUS_LIBS) \
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

CLUSTER_CONTROL_ROOT_O = $(top_builddir)/src/backend/cluster/cluster_control_root.o
# migration_wal_scan (contract) drives a real XLogReader over the thread
# stream.  The reader is compiled -DFRONTEND (pg_waldump style) so the
# unit binary does not drag in backend palloc/elog/ShmemVariableCache
# machinery; err*/BasicOpenFile are stubbed in the test file.
test_cluster_control_root: test_cluster_control_root.c unit_test.h \
		$(CLUSTER_CONTROL_ROOT_O) xlogreader_fs.o
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_CONTROL_ROOT_O) \
		xlogreader_fs.o \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) \
		-lssl -lcrypto -llz4 -lzstd -o $@

xlogreader_fs.o: $(top_srcdir)/src/backend/access/transam/xlogreader.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DFRONTEND -c $< -o $@

CLUSTER_WAL_RETENTION_O = $(top_builddir)/src/backend/cluster/cluster_wal_retention.o
test_cluster_wal_retention: test_cluster_wal_retention.c unit_test.h \
		$(CLUSTER_WAL_RETENTION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_WAL_RETENTION_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_RECOVERY_DUTY_O = $(top_builddir)/src/backend/cluster/cluster_recovery_duty.o
test_cluster_recovery_duty: test_cluster_recovery_duty.c unit_test.h \
		$(CLUSTER_RECOVERY_DUTY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_RECOVERY_DUTY_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

test_cluster_formation_witness: test_cluster_formation_witness.c unit_test.h \
		$(CLUSTER_RECOVERY_DUTY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_RECOVERY_DUTY_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

test_pgrac_external_fence_protocol: \
		test_pgrac_external_fence_protocol.c unit_test.h \
		$(PGRAC_EXTERNAL_FENCE_PROTOCOL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(PGRAC_EXTERNAL_FENCE_PROTOCOL_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_CONFIG_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_config.o
test_pgrac_fenced_config: test_pgrac_fenced_config.c unit_test.h \
		$(PGRAC_FENCED_CONFIG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_CONFIG_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_CORE_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_core.o
test_pgrac_fenced_core: test_pgrac_fenced_core.c unit_test.h \
		$(PGRAC_FENCED_CORE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_CORE_O) -o $@

PGRAC_FENCED_SCHEDULE_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_schedule.o
test_pgrac_fenced_schedule: test_pgrac_fenced_schedule.c unit_test.h \
		$(PGRAC_FENCED_SCHEDULE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_SCHEDULE_O) -o $@

PGRAC_FENCED_JOURNAL_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_journal.o
PGRAC_FENCED_RUNTIME_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_runtime.o
test_pgrac_fenced_journal: test_pgrac_fenced_journal.c unit_test.h \
		$(PGRAC_FENCED_JOURNAL_O) $(PGRAC_FENCED_RUNTIME_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_JOURNAL_O) $(PGRAC_FENCED_RUNTIME_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_IPMI_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_ipmi.o
PGRAC_FENCED_PROVIDER_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_provider.o \
	$(PGRAC_FENCED_IPMI_O)
test_pgrac_fenced_provider: test_pgrac_fenced_provider.c unit_test.h \
		$(PGRAC_FENCED_PROVIDER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_PROVIDER_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_OPERATION_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_operation.o \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_provider.o \
	$(PGRAC_FENCED_IPMI_O) \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_journal.o \
	$(top_builddir)/src/common/pgrac_external_fence_protocol.o
PGRAC_FENCED_SESSION_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_session.o
PGRAC_FENCED_DISPATCH_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_dispatch.o
test_pgrac_fenced_operation: test_pgrac_fenced_operation.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_REJOIN_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_rejoin.o
test_pgrac_fenced_rejoin: test_pgrac_fenced_rejoin.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_REJOIN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_REJOIN_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_REJOIN_ASYNC_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_rejoin_async.o
test_pgrac_fenced_rejoin_async: test_pgrac_fenced_rejoin_async.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_REJOIN_O) \
		$(PGRAC_FENCED_REJOIN_ASYNC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_REJOIN_O) \
		$(PGRAC_FENCED_REJOIN_ASYNC_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

test_pgrac_fenced_session: test_pgrac_fenced_session.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_CORE_O) \
		$(PGRAC_FENCED_SESSION_O) $(PGRAC_FENCED_DISPATCH_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_CORE_O) \
		$(PGRAC_FENCED_SESSION_O) $(PGRAC_FENCED_DISPATCH_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_ASYNC_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_async.o
test_pgrac_fenced_async: test_pgrac_fenced_async.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_ASYNC_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_ASYNC_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_COORDINATOR_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_coordinator.o \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_schedule.o \
	$(PGRAC_FENCED_ASYNC_O) $(PGRAC_FENCED_SESSION_O)
test_pgrac_fenced_coordinator: test_pgrac_fenced_coordinator.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_CORE_O) \
		$(PGRAC_FENCED_COORDINATOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_CORE_O) \
		$(PGRAC_FENCED_COORDINATOR_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_REJOIN_COORDINATOR_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_rejoin_coordinator.o
test_pgrac_fenced_rejoin_coordinator: \
		test_pgrac_fenced_rejoin_coordinator.c unit_test.h \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_CORE_O) \
		$(PGRAC_FENCED_REJOIN_O) $(PGRAC_FENCED_REJOIN_ASYNC_O) \
		$(PGRAC_FENCED_COORDINATOR_O) $(PGRAC_FENCED_REJOIN_COORDINATOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_OPERATION_O) $(PGRAC_FENCED_CORE_O) \
		$(PGRAC_FENCED_REJOIN_O) $(PGRAC_FENCED_REJOIN_ASYNC_O) \
		$(PGRAC_FENCED_COORDINATOR_O) $(PGRAC_FENCED_REJOIN_COORDINATOR_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

pgrac_fenced_live_rejoin_client: pgrac_fenced_live_rejoin_client.c \
		$(top_builddir)/src/common/pgrac_external_fence_protocol.o
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(top_builddir)/src/common/pgrac_external_fence_protocol.o \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

test_pgrac_fenced_ipmi: test_pgrac_fenced_ipmi.c unit_test.h \
		$(PGRAC_FENCED_IPMI_O) \
		$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_provider.o
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_IPMI_O) \
		$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_provider.o \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_IPMI_FEXECVE_O = pgrac_fenced_ipmi_fexecve.o
$(PGRAC_FENCED_IPMI_FEXECVE_O): \
		$(top_srcdir)/src/bin/pgrac_fenced/pgrac_fenced_ipmi.c \
		$(top_srcdir)/src/bin/pgrac_fenced/pgrac_fenced_ipmi.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DHAVE_FEXECVE \
		-I$(top_srcdir)/src/bin/pgrac_fenced -c $< -o $@

test_pgrac_fenced_ipmi_exec: test_pgrac_fenced_ipmi_exec.c unit_test.h \
		$(PGRAC_FENCED_IPMI_FEXECVE_O) \
		$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_provider.o
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_IPMI_FEXECVE_O) \
		$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_provider.o \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

PGRAC_FENCED_CTL_O = \
	$(top_builddir)/src/bin/pgrac_fenced/pgrac_fenced_ctl.o
test_pgrac_fenced_ctl: test_pgrac_fenced_ctl.c unit_test.h \
		$(PGRAC_FENCED_CTL_O) $(PGRAC_FENCED_JOURNAL_O) \
		$(PGRAC_EXTERNAL_FENCE_PROTOCOL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-I$(top_srcdir)/src/bin/pgrac_fenced $< \
		$(PGRAC_FENCED_CTL_O) $(PGRAC_FENCED_JOURNAL_O) \
		$(PGRAC_EXTERNAL_FENCE_PROTOCOL_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

CLUSTER_FENCE_AUTHORITY_O = $(top_builddir)/src/backend/cluster/cluster_fence_authority.o
test_cluster_write_fence_durable: test_cluster_write_fence_durable.c unit_test.h \
		$(CLUSTER_FENCE_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_FENCE_AUTHORITY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

CLUSTER_WRITE_FENCE_O = $(top_builddir)/src/backend/cluster/cluster_write_fence.o
test_cluster_write_fence_cache: test_cluster_write_fence_cache.c unit_test.h \
		$(CLUSTER_WRITE_FENCE_O) $(CLUSTER_MEMBERSHIP_O) $(CLUSTER_EPOCH_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_WRITE_FENCE_O) $(CLUSTER_MEMBERSHIP_O) \
		$(CLUSTER_EPOCH_O) -lpthread \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.6a D1: test_cluster_recovery_anchor links cluster_recovery_anchor.o
# and exercises the pure image classification plus a real torn-safe read/
# write round trip (with .bak fallback) against a temp dir.  The fd.c
# openers, pg_fsync, durable_rename and the ereport machinery are stubbed
# locally; CRC32C comes from libpgport, so no PG backend is needed.
CLUSTER_RECOVERY_ANCHOR_O = $(top_builddir)/src/backend/cluster/cluster_recovery_anchor.o
test_cluster_recovery_anchor: test_cluster_recovery_anchor.c unit_test.h \
		$(CLUSTER_RECOVERY_ANCHOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_RECOVERY_ANCHOR_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.14a D1: test_cluster_gcs_reqid exercises the pure request-id domain
# helpers (cluster_gcs_reqid.h, c.h-only); links libpgport for pg_printf.
test_cluster_gcs_reqid: test_cluster_gcs_reqid.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.14 D1: test_cluster_shared_catalog links cluster_shared_catalog.o
# (the pure vet predicate + "non-temp = shared" routing criterion).  The
# module depends only on c.h, so it links standalone with no PG backend
# symbols and no libpgport.
CLUSTER_SHARED_CATALOG_O = $(top_builddir)/src/backend/cluster/cluster_shared_catalog.o
test_cluster_shared_catalog: test_cluster_shared_catalog.c unit_test.h \
		$(CLUSTER_SHARED_CATALOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_SHARED_CATALOG_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.14 D6: test_cluster_oid_lease links cluster_oid_lease.o (the pure
# lease math + torn-safe authority file I/O).  fd.c openers, pg_fsync,
# durable_rename and the ereport machinery are stubbed locally; CRC32C comes
# from libpgport.  The shmem/refill layer (cluster_oid_lease_shmem.o) needs the
# full backend and is exercised by the TAP suite, not here.
CLUSTER_OID_LEASE_O = $(top_builddir)/src/backend/cluster/cluster_oid_lease.o
test_cluster_oid_lease: test_cluster_oid_lease.c unit_test.h \
		$(CLUSTER_OID_LEASE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_OID_LEASE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.15b D1/D2: test_cluster_xid_authority links cluster_xid_authority.o
# (the shared XID authority + native-era prehistory: torn-safe file I/O,
# seal/flag monotonicity, prehistory publish/adopt byte round trip).  fd.c
# openers, pg_fsync, durable_rename and the ereport machinery are stubbed
# locally; CRC32C comes from libpgport.  The startup wiring (bootstrap adopt,
# StartupXLOG horizon merge, shutdown-checkpoint publish) is TAP territory.
# GCS-race round-2 review F1: the provable_full judge widens through
# cluster_xid_widen, so cluster_xid_stripe.o joins the link (its runtime
# wrapper deps are abort-stubbed; only the pure widen path runs).
CLUSTER_XID_AUTHORITY_O = $(top_builddir)/src/backend/cluster/cluster_xid_authority.o \
	$(top_builddir)/src/backend/cluster/cluster_xid_prehistory.o \
	$(top_builddir)/src/backend/cluster/cluster_xid_stripe.o
test_cluster_xid_authority: test_cluster_xid_authority.c unit_test.h \
		$(CLUSTER_XID_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_XID_AUTHORITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@


# spec-6.14 D5: test_cluster_relmap_authority links cluster_relmap_authority.o
# (the two-phase pending/committed authority substrate).  fd.c + ereport
# stubbed locally; CRC32C from libpgport.  The write-path activation (relmapper
# hook, publish-then-invalidate, crash arbitration) is exercised by TAP.
CLUSTER_RELMAP_AUTHORITY_O = $(top_builddir)/src/backend/cluster/cluster_relmap_authority.o
test_cluster_relmap_authority: test_cluster_relmap_authority.c unit_test.h \
		$(CLUSTER_RELMAP_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_RELMAP_AUTHORITY_O) \
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
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DTT_2PC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_tt_2pc.c"' \
		-DTWOPHASE_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/access/transam/twophase.c"' \
		$< \
		$(CLUSTER_TT_2PC_RECORD_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@
test_cluster_visibility_variants: test_cluster_visibility_variants.c unit_test.h \
		$(CLUSTER_VIS_VERDICT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VIS_VERDICT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.22f D6-1: test_cluster_vis_undo_verdict_map links the same pure
# verdict object (cluster_visibility_verdict.o) for the two D6 fresh-ref
# consumer helpers (cluster_vis_from_undo_verdict + cluster_vis_freshref_
# origin_decision).  Both are dependency-free (no PG backend), so nothing is
# stubbed -- identical link recipe to test_cluster_visibility_variants.
test_cluster_vis_undo_verdict_map: test_cluster_vis_undo_verdict_map.c unit_test.h \
		$(CLUSTER_VIS_VERDICT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VIS_VERDICT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-7.1a D0/U0: test_cluster_writer_chain links the pure terminal-writer
# outcome -> TM_Result mapping standalone (no buffer / no page / no backend).
CLUSTER_WRITER_CHAIN_O = $(top_builddir)/src/backend/cluster/cluster_writer_chain.o
test_cluster_writer_chain: test_cluster_writer_chain.c unit_test.h \
		$(CLUSTER_WRITER_CHAIN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_WRITER_CHAIN_O) \
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
test_cluster_undo_buf: test_cluster_undo_buf.c unit_test.h $(CLUSTER_UNDO_BUF_O) \
		$(top_builddir)/src/backend/cluster/storage/cluster_undo_block0.o \
		$(top_builddir)/src/backend/cluster/storage/cluster_undo_block0_resident.o
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_UNDO_BUF_O) \
		$(top_builddir)/src/backend/cluster/storage/cluster_undo_block0.o \
		$(top_builddir)/src/backend/cluster/storage/cluster_undo_block0_resident.o \
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

# spec-4.11 D1 contract: test_cluster_thread_replay exercises the PURE
# block-classification gate (cluster_thread_replay_classify_block, a header
# inline) -- the corruption-critical TARGET / OUT_OF_SCOPE / BLOCKED decision
# the RMW replay engine makes before touching any page.  No cluster object is
# needed (the gate is inline); only libpgcommon/libpgport for the unit harness.
# The engine's byte-for-byte streaming behaviour lands as cluster_tap t/259.
test_cluster_thread_replay: test_cluster_thread_replay.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.11 D1 contract: test_cluster_thread_driver exercises the driver's
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

# spec-4.11 D1 contract: test_cluster_thread_orchestrator exercises the
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

$(filter-out test_cluster_lms_native_probe,$(SIMPLE_TESTS)): %: %.c unit_test.h $(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_VERSION_O) -o $@

# The wire behavior is inline; an updated decoder must rebuild this binary.
test_cluster_r4_wire_codec: test_cluster_r4_wire_codec.c \
		$(top_srcdir)/src/include/cluster/cluster_gcs_block.h

# M7: execute the production insert-end accessor against the exact WAL page
# boundary mappings.  A function-sectioned xlog object lets dead stripping
# retain only the accessor and its unit-only shared-state fixture.
CLUSTER_XLOG_INSERT_END_TEST_O = test_cluster_xlog_insert_end_product.o
$(CLUSTER_XLOG_INSERT_END_TEST_O): $(top_srcdir)/src/backend/access/transam/xlog.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

test_cluster_xlog_insert_end: test_cluster_xlog_insert_end.c unit_test.h \
		$(CLUSTER_XLOG_INSERT_END_TEST_O)
	$(CC) $(CFLAGS) $(PTHREAD_CFLAGS) $(CPPFLAGS) \
		-DBUFMGR_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/storage/buffer/bufmgr.c"' \
		$< $(CLUSTER_XLOG_INSERT_END_TEST_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(PTHREAD_LIBS) -o $@

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
		$(CLUSTER_DEBUG_O) $(CLUSTER_STARTUP_PHASE_O) \
		$(CLUSTER_RESOURCE_X_IDENTITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_DEBUG_O) $(CLUSTER_STARTUP_PHASE_O) \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

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
# (single-threaded); the real cross-node path is cluster_tap t/353.
CLUSTER_HW_LEASE_O = $(top_builddir)/src/backend/cluster/cluster_hw_lease.o
test_cluster_hw_lease: test_cluster_hw_lease.c unit_test.h $(CLUSTER_HW_LEASE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_HW_LEASE_O) -o $@

# test_cluster_pi_shadow links cluster_pi_shadow.o standalone (spec-6.12h
# D-h3a U-h): locks the ship-SCN recovery-boundary gate truth table
# (UNUSABLE / lineage / post-ship / cross-node ties / raw-compare traps)
# + the shadow slot lifecycle.  Shmem stubbed (malloc-backed); the
# byte-equivalence recovery leg is the D-h3b TAP work.
CLUSTER_PI_SHADOW_O = $(top_builddir)/src/backend/cluster/cluster_pi_shadow.o
test_cluster_pi_shadow: test_cluster_pi_shadow.c unit_test.h $(CLUSTER_PI_SHADOW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_PI_SHADOW_O) -o $@

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
		$(CLUSTER_SHARED_FS_SHAREDFS_O) $(CLUSTER_SHARED_CATALOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_O) \
		$(CLUSTER_SHARED_FS_STUB_O) $(CLUSTER_SHARED_FS_LOCAL_O) \
		$(CLUSTER_SHARED_FS_SHAREDFS_O) $(CLUSTER_SHARED_CATALOG_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-4.5a D12: runtime unit for the shared_fs backend + shared-root
# sentinel.  Links ONLY cluster_shared_fs_sharedfs.o with functional
# stubs (real open/pread/pwrite/software-crc32c), no server needed.
test_cluster_shared_fs_sharedfs: test_cluster_shared_fs_sharedfs.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_SHAREDFS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SHARED_FS_SHAREDFS_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-6.0a: runtime unit for the raw block_device backend over a
# temporary regular file that stands in for a block device.  Links only
# the provider object; WAL/GES entry points are stubbed by the test.
test_cluster_shared_fs_block_device: test_cluster_shared_fs_block_device.c unit_test.h \
		$(CLUSTER_SHARED_FS_BLOCK_DEVICE_O) $(CLUSTER_PR_SCSI_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_SHARED_FS_BLOCK_DEVICE_O) $(CLUSTER_PR_SCSI_O) -o $@

# M5: compile the production capacity path into one focused syscall-boundary
# unit.  Dead stripping keeps unrelated provider/backend duties out.
test_cluster_raw_device_capacity: test_cluster_raw_device_capacity.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/storage/cluster_shared_fs_block_device.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) -o $@

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
		$(CLUSTER_SHARED_FS_SHAREDFS_O) $(CLUSTER_SHARED_CATALOG_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_SMGR_O) $(CLUSTER_SHARED_CATALOG_O) \
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
check: all check_cluster_undo_segment_half_page_oversized \
	check_pcm_x_write_gate_ast check_pcm_x_write_gate_coverage \
	check_resource_x_send_c1_unit \
	check_r11_predecessor_product_manifest_unit \
	check_r11_predecessor_product_manifest \
	check_r11_legacy_source_removed \
	check_r11_source_removal_census_unit \
	check_r11_source_removal_census
	@echo "# pgrac cluster unit tests"
	@total=0; failed=0; \
	for t in $(TESTS); do \
		echo "# Running $$t"; \
		./$$t || { rc=$$?; failed=$$((failed + 1)); echo "# FAILED-BINARY: $$t (exit $$rc)"; }; \
		total=$$((total + 1)); \
	done; \
	if [ $$failed -ne 0 ]; then \
		echo "# $$failed of $$total binaries failed"; \
		exit 1; \
	else \
		echo "# All $$total binaries passed"; \
	fi

check_pcm_x_write_gate_ast:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/test/cluster_unit/test_pcm_x_write_gate_ast.py

check_pcm_x_write_gate_coverage:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/tools/check_pcm_x_write_gate_ast.py \
		--source-root "$(top_srcdir)" --compile-directory "$(CURDIR)" \
		--clang "$(CC)" --cflags '$(CFLAGS)' --cppflags '$(CPPFLAGS)'

check_resource_x_send_c1_unit:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/test/cluster_unit/test_resource_x_send_c1.py

check_resource_x_send_c1:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/tools/check_resource_x_send_c1.py \
		--source-root "$(top_srcdir)" \
		--manifest "$(top_srcdir)/src/test/cluster_unit/data/resource-x-send-c1.tsv" \
		--artifact "$(top_srcdir)/src/test/cluster_unit/data/send-c1-9-plus-2.json" \
		--unit-directory "$(CURDIR)" --cc "$(CC)" \
		--cflags '$(CFLAGS)' --cppflags '$(CPPFLAGS)' \
		--pthread-cflags '$(PTHREAD_CFLAGS)' --pthread-libs '$(PTHREAD_LIBS)'

check_r11_predecessor_product_manifest_unit:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/test/cluster_unit/test_r11_predecessor_product_manifest.py

check_r11_predecessor_product_manifest:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/tools/check_r11_predecessor_product_manifest.py \
		--source-root "$(top_srcdir)" \
		--manifest "$(top_srcdir)/src/test/cluster_unit/data/r11-predecessor-product-manifest-v1.json"

check_r11_legacy_source_removed:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/test/cluster_unit/test_r11_legacy_source_removed.py

check_r11_source_removal_census_unit:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/test/cluster_unit/test_r11_source_removal_census.py

check_r11_source_removal_census:
	PYTHONDONTWRITEBYTECODE=1 python3 \
		$(top_srcdir)/src/tools/check_r11_source_removal_census.py \
		--source-root "$(top_srcdir)" \
		--manifest "$(top_srcdir)/src/test/cluster_unit/data/r11-source-removal-census-v1.json"


# ----------
# Standard PG targets
# ----------
clean distclean maintainer-clean:
	rm -f $(TESTS) pgrac_fenced_live_rejoin_client \
		test_cluster_heap_epq_consumer.inc test_cluster_heap_fetch_consumer.inc \
		test_cluster_heap_lock_output.inc test_cluster_heap_lock_reacquire.inc \
		test_cluster_heap_lock_rebind.inc test_cluster_heap_lock_self.inc \
		test_cluster_heap_lock_strength.inc test_cluster_heap_scan_lifetime.inc \
		test_cluster_heap_scan_consumers.inc test_cluster_heap_snapshot_consumer.inc \
		test_cluster_heap_index_recheck.inc test_cluster_heap_index_recapture.inc \
		test_cluster_heap_copy_entry.inc test_cluster_heap_copy_exit.inc \
		test_cluster_heap_lock_dispatch.inc test_cluster_heap_vm_lock_entry.inc \
		test_cluster_heap_vm_insert.inc test_cluster_heap_vm_tuple_lock.inc \
		test_cluster_heap_vm_update.inc test_cluster_heap_vm_unwind_helpers.inc \
		test_cluster_heap_vm_delete.inc test_cluster_heap_vm_pretoast.inc \
		test_cluster_pcm_snapshot_owner.inc \
		test_cluster_pcm_transition_owner.inc \
		test_cluster_pcm_barrier_proof.inc \
		test_cluster_pcm_preassert_owner.inc test_cluster_pcm_preassert_consumer.inc \
		test_cluster_pcm_pending_consumer.inc \
		test_cluster_pcm_n_predecessor_consumer.inc \
		test_cluster_pcm_source_replay.inc \
		test_cluster_resource_x_terminal_owner.inc \
		test_cluster_resource_x_terminal_gate_consumer.inc \
		test_cluster_resource_x_session_observation.inc \
		test_cluster_resource_x_remote_s_types.inc \
		test_cluster_resource_x_remote_s_staged_gate.inc \
		test_cluster_resource_x_source_final_gate.inc \
		test_cluster_resource_x_source_entry_gate.inc \
		test_cluster_resource_x_source_prepare_gate.inc \
		test_cluster_resource_x_retained_wait_gate.inc \
		test_cluster_resource_x_predecessor_wait_gate.inc \
		test_cluster_resource_x_dispatch_observation.inc \
		test_cluster_pcm_source_finish_tick.inc \
		test_cluster_pcm_eviction_owner.inc test_cluster_pcm_clock_sweep.inc \
		test_cluster_resource_x_eviction_owner.inc \
		test_cluster_resource_x_failed_round_observation.inc \
		test_cluster_pcm_aux_consumer.inc test_cluster_pcm_aux_ledger.inc \
		test_cluster_ctrc_dispatch.inc \
		test_cluster_ctrc_run_pass.inc \
		test_cluster_undo_cleaner_holds.inc \
		$(CLUSTER_UNDO_RECORD_TEST_O) $(CLUSTER_UNDO_ALLOC_TEST_O) \
		$(CLUSTER_QVOTEC_PGSA_TEST_O) $(CLUSTER_R4_RUNTIME_VIS_TEST_O) \
		$(CLUSTER_XLOG_INSERT_END_TEST_O) \
		$(CLUSTER_R4_ROUTE_DEDUP_TEST_O) $(CLUSTER_R4_HOT_HEAPAM_TEST_O) \
		$(CLUSTER_R4_HOT_HANDLER_TEST_O) $(CLUSTER_R4_EXEC_TUPLES_TEST_O) \
		$(CLUSTER_R4_HEAPTUPLE_TEST_O) $(CLUSTER_R4_HOT_VIS_TEST_O) \
		$(CLUSTER_R4_SCRATCH_RESOLVER_TEST_O)

install installdirs uninstall installcheck:
	@:

.PHONY: all check check_cluster_undo_segment_half_page_oversized \
	check_pcm_x_write_gate_ast check_pcm_x_write_gate_coverage clean distclean \
	check_resource_x_send_c1 check_resource_x_send_c1_unit \
	check_r11_predecessor_product_manifest \
	check_r11_predecessor_product_manifest_unit \
	check_r11_legacy_source_removed \
	check_r11_source_removal_census \
	check_r11_source_removal_census_unit \
	maintainer-clean install installdirs uninstall installcheck

# Spec 8.4A U9: this deliberately oversized fixture must be rejected by the
# compile-time first-half-page layout guard.  Accept only the named assertion;
# an unrelated compiler failure is not evidence for the invariant.
check_cluster_undo_segment_half_page_oversized: \
		test_cluster_undo_segment_half_page_oversized.c
	@output="$$($(CC) $(CFLAGS) $(CPPFLAGS) -fsyntax-only $< 2>&1)"; status=$$?; \
	if [ "$$status" -eq 0 ]; then \
		echo "# oversized TT fixture unexpectedly compiled"; \
		exit 1; \
	fi; \
	case "$$output" in \
	  *"spec-8.4A U9 oversized TT fixture must fail"*) \
		echo "# oversized TT fixture rejected by U9 half-page assertion" ;; \
	  *) printf '%s\n' "$$output"; exit 1 ;; \
	esac

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

# R4 pair-CAS compiles the same QVOTEC source into a test-only object.  The
# macro exposes only the fd-parameterized wrapper; the normal product object
# retains no test symbol or fd setter.
$(CLUSTER_QVOTEC_PGSA_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_qvotec.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DCLUSTER_QVOTEC_PGSA_UNIT_TEST -c $< -o $@

test_cluster_qvotec: test_cluster_qvotec.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_QVOTEC_PGSA_TEST_O) \
		$(CLUSTER_EPOCH_BALLOT_O) $(CLUSTER_REPLACEMENT_REQUEST_O) \
		$(CLUSTER_MEMBERSHIP_O) \
		$(CLUSTER_VOTING_DISK_IO_O) $(CLUSTER_UNDO_BLOCK0_O) \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O) \
		$(top_builddir)/src/backend/cluster/cluster_semantic_activation.o
	$(CC) $(CFLAGS) $(CPPFLAGS) -DCLUSTER_QVOTEC_PGSA_UNIT_TEST \
		-DQVOTEC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_qvotec.c"' \
		-DLMON_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_lmon.c"' \
		-DSHMEM_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_shmem.c"' \
		-DSEMANTIC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_semantic_activation.c"' \
		-DSEMANTIC_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_semantic_activation.h"' \
		-DCLUSTER_MAKEFILE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/Makefile"' $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_QVOTEC_PGSA_TEST_O) \
		$(CLUSTER_EPOCH_BALLOT_O) $(CLUSTER_REPLACEMENT_REQUEST_O) \
		$(CLUSTER_MEMBERSHIP_O) \
		$(CLUSTER_VOTING_DISK_IO_O) $(CLUSTER_UNDO_BLOCK0_O) \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O) \
		$(top_builddir)/src/backend/cluster/cluster_semantic_activation.o \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

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
		$(CLUSTER_EPOCH_BALLOT_O) \
		$(CLUSTER_TOUCHED_PEERS_O) $(CLUSTER_MEMBERSHIP_O) \
		$(CLUSTER_REPLACEMENT_EPISODE_O) $(CLUSTER_REPLACEMENT_WIRE_O) \
		$(CLUSTER_FORMATION_MARKER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_RECONFIG_O) $(CLUSTER_EPOCH_O) \
		$(CLUSTER_EPOCH_BALLOT_O) \
		$(CLUSTER_TOUCHED_PEERS_O) $(CLUSTER_MEMBERSHIP_O) \
		$(CLUSTER_REPLACEMENT_EPISODE_O) $(CLUSTER_REPLACEMENT_WIRE_O) \
		$(CLUSTER_FORMATION_MARKER_O) \
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

# spec-7.4 D1: test_cluster_scn_frontier links cluster_scn.o standalone
# (same stub model as test_cluster_scn) and exercises the durable
# pending registry real-behavior paths via the in-process shmem buffer.
test_cluster_scn_frontier: test_cluster_scn_frontier.c unit_test.h \
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
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_O) $(CLUSTER_GES_MODE_O) \
		$(CLUSTER_REPLACEMENT_WIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GES_O) $(CLUSTER_GES_MODE_O) \
		$(CLUSTER_REPLACEMENT_WIRE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Spec 8.4A D1: exact-key cooperative poll snapshots and consumes a
# delivered reply under the reply-table lock without waiting on the CV.
test_cluster_ges_reply_wait: test_cluster_ges_reply_wait.c unit_test.h \
		$(CLUSTER_GES_REPLY_WAIT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_GES_REPLY_WAIT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# P0#3: fixed-capacity, no-overwrite GES cleanup deferred-retry queue.
test_cluster_grd_outbound: test_cluster_grd_outbound.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_OUTBOUND_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GRD_OUTBOUND_O) \
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
$(CLUSTER_LMD_WAIT_STATE_O): $(top_srcdir)/src/include/cluster/cluster_lmd_wait_state.h

test_cluster_lmd_wait_state: test_cluster_lmd_wait_state.c unit_test.h \
		$(top_srcdir)/src/include/cluster/cluster_lmd_wait_state.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_WAIT_STATE_O)
	$(CC) $(CFLAGS) $(PTHREAD_CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMD_WAIT_STATE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a $(PTHREAD_LIBS) -o $@

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

# HW handoff uses production objects in two independent address spaces.
# Section splitting removes unrelated backend entry points, not tested code.
HW_HANDOFF_OBJECTS = $(addprefix hw_handoff_,cluster_grd.o cluster_ges.o \
    cluster_ges_reply_wait.o cluster_lock_acquire.o cluster_ges_mode.o cluster_lmd_wait_state.o)
$(HW_HANDOFF_OBJECTS): hw_handoff_%.o: $(top_srcdir)/src/backend/cluster/%.c \
        $(top_srcdir)/src/include/cluster/cluster_ges.h \
        $(top_srcdir)/src/include/cluster/cluster_lock_acquire.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_hw_handoff: test_cluster_hw_handoff.c test_cluster_grd.c unit_test.h \
        $(HW_HANDOFF_OBJECTS)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(HW_HANDOFF_OBJECTS) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
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
# Compile the unchanged product source with only its clock dependency redirected.
# No test clock or test-only branch enters the production object.
test_cluster_pcm_lock_clock_product.o: \
		$(top_srcdir)/src/backend/cluster/cluster_pcm_lock.c \
		$(top_srcdir)/src/include/cluster/cluster_pcm_lock.h
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-Dclock_gettime=cluster_test_pcm_clock_gettime -c $< -o $@

test_cluster_pcm_wait_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/if \(action != RESOURCE_X_BOOTSTRAP_ROUND_WAIT\)/ { emit=1; begin++ } \
	     emit && /diagnostic_wait_failure = cluster_pcm_rx_take_wait_failure/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@

test_cluster_pcm_predecessor_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/diagnostic_stage = "retained-release-wait";/ { emit=1; begin++ } \
	     emit && /^\t\t\t\t}$$/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@

test_cluster_pcm_ready_scheduler.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_stage_ready_tag\(/ { print "static void"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (found != 1 || emit) exit 1 }' $< > $@

test_cluster_pcm_source_replay.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_retained_pair_replay\(/ { print "static ResourceXApplyResult"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (found != 1 || emit) exit 1 }' $< > $@

test_cluster_pcm_source_replay_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^\t\t\t\treplay_result$$/ { emit=1; begin++ } \
	     emit && /^\t\t\t}/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_source_owner_layout.inc: $(top_srcdir)/src/backend/cluster/cluster_pcm_lock.c Makefile
	awk '/^typedef struct ClusterPcmResourceXLocalHandoff / { emit=1; handoff++ } \
	     /^typedef struct ClusterPcmResourceXLocalOwner / { emit=1; owner++ } \
	     emit { print } /^} ClusterPcmResourceXLocal/ { emit=0 } \
	     END { if (handoff != 1 || owner != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_lock: test_cluster_pcm_lock.c unit_test.h test_cluster_pcm_wait_consumer.inc \
		test_cluster_pcm_predecessor_consumer.inc \
		test_cluster_pcm_source_owner_layout.inc \
		test_cluster_pcm_source_replay.inc \
		test_cluster_pcm_source_replay_consumer.inc \
		test_cluster_pcm_ready_scheduler.inc \
		$(CLUSTER_VERSION_O) test_cluster_pcm_lock_clock_product.o $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(CLUSTER_RESOURCE_X_NODE_WIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DGCS_BLOCK_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_gcs_block.c"' \
		-DPCM_LOCK_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_pcm_lock.c"' \
		$< \
		$(CLUSTER_VERSION_O) test_cluster_pcm_lock_clock_product.o $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(CLUSTER_RESOURCE_X_NODE_WIRE_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.36a C1/D5a: the real cluster_pcm_own object is linked against a
# fake shmem region.  The test also reads the production bufmgr source through
# an explicit build-time path so the D5a lock-order contract is paired with
# the shared decision helpers instead of being represented by a fake bufmgr.
# Compile the two actual bufmgr snapshot owners, verbatim, into this fixture.
# Column-zero definitions/closing braces delimit these two dependency-light
# functions.  Counts fail closed if either definition is moved or duplicated.
test_cluster_pcm_snapshot_owner.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^cluster_pcm_own_snapshot_post_state_locked\(/ { print "static inline void"; emit=1; post++ } \
	     /^cluster_pcm_own_snapshot_locked\(/ { print "static inline void"; emit=1; capture++ } \
	     /^cluster_pcm_own_fence_matches_locked\(/ { print "static inline bool"; emit=1; matches++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (post != 1 || capture != 1 || matches != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_barrier_proof.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^typedef struct ClusterBufmgrBarrierUnwindContext/ { emit=1; context++ } \
	     /^cluster_bufmgr_barrier_prove_empty\(/ { print "static bool"; emit=1; proof++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (context != 1 || proof != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_transition_owner.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^cluster_pcm_own_bump_failure\(/ { print "static ClusterPcmOwnResult"; emit=1; bump_failure++ } \
	     /^cluster_bufmgr_pcm_own_snapshot\(/ { print "ClusterPcmOwnResult"; emit=1; ordinary_snapshot++ } \
	     /^cluster_bufmgr_pcm_own_n_assertion_candidate_exact\(/ { print "ClusterPcmOwnResult"; emit=1; n_candidate++ } \
	     /^cluster_bufmgr_pcm_own_n_retained_release_inflight_exact\(/ { print "bool"; emit=1; retained_candidate++ } \
	     /^cluster_bufmgr_pcm_own_n_storage_candidate_exact\(/ { print "ClusterPcmOwnResult"; emit=1; storage_candidate++ } \
	     /^cluster_bufmgr_pcm_own_n_predecessor_observe_exact\(/ { print "ClusterPcmOwnResult"; emit=1; predecessor_observe++ } \
	     /^cluster_pcm_own_bump_locked\(/ { print "static inline ClusterPcmOwnResult"; emit=1; bump++ } \
	     /^cluster_pcm_own_eviction_commit_locked\(/ { print "static ClusterPcmOwnResult"; emit=1; evict++ } \
	     /^cluster_bufmgr_pcm_current_image_locked\(/ { print "static inline bool"; emit=1; shape++ } \
	     /^cluster_bufmgr_copy_block_for_gcs\(/ { print "bool"; emit=1; wal_copy++ } \
	     /^cluster_bufmgr_pcm_direct_init_known_new_locked\(/ { print "static bool"; emit=1; known_new++ } \
	     /^cluster_bufmgr_pcm_own_snapshot_by_tag\(/ { print "ClusterPcmOwnResult"; emit=1; ordinary_lookup++ } \
	     /^cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact\(/ { print "ClusterPcmOwnResult"; emit=1; direct_lookup++ } \
	     /^cluster_bufmgr_pcm_own_direct_init_sidecar_by_tag_exact\(/ { print "static ResourceXBufferActivationResult"; emit=1; sidecar++ } \
	     /^cluster_bufmgr_pcm_own_activate_x_by_tag\(/ { print "ResourceXBufferActivationResult"; emit=1; activate++ } \
	     /^cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact\(/ { print "ResourceXBufferActivationResult"; emit=1; clear++ } \
	     /^cluster_bufmgr_pcm_own_copy_source_image_exact\(/ { print "static ClusterPcmOwnResult"; emit=1; copy++ } \
	     /^cluster_bufmgr_pcm_own_held_x_revoke_valid\(/ { print "static bool"; emit=1; held_valid++ } \
	     /^cluster_bufmgr_pcm_own_validate_held_x_revoke\(/ { print "ClusterPcmOwnResult"; emit=1; held_validate++ } \
	     /^cluster_bufmgr_pcm_own_adopt_held_x_revoke\(/ { print "ClusterPcmOwnResult"; emit=1; held_adopt++ } \
	     /^cluster_bufmgr_pcm_own_finish_held_x_revoke_retain\(/ { print "ClusterPcmOwnResult"; emit=1; held_finish++ } \
	     /^cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed\(/ { print "ClusterPcmOwnResult"; emit=1; held_abandon++ } \
	     /^cluster_bufmgr_pcm_own_finish_revoke_drop_unpinned\(/ { print "static ClusterPcmOwnResult"; emit=1; drop++ } \
	     /^cluster_bufmgr_pcm_own_finish_revoke_retain\(/ { print "ClusterPcmOwnResult"; emit=1; finish++ } \
	     /^cluster_bufmgr_pcm_own_delivery_hold_begin_exact\(/ { print "ClusterPcmOwnResult"; emit=1; delivery_begin++ } \
	     /^cluster_bufmgr_pcm_own_begin_x_reservation\(/ { print "ClusterPcmOwnResult"; emit=1; normal_reserve++ } \
	     /^cluster_bufmgr_pcm_own_delivery_begin_x_reservation\(/ { print "ClusterPcmOwnResult"; emit=1; delivery_reserve++ } \
	     /^cluster_bufmgr_pcm_own_delivery_snapshot_exact\(/ { print "ClusterPcmOwnResult"; emit=1; delivery_snapshot++ } \
	     /^cluster_bufmgr_pcm_own_delivery_hold_release_exact\(/ { print "ClusterPcmOwnResult"; emit=1; delivery_release++ } \
	     /^cluster_pcm_own_publish_read_image_exact\(/ { print "ClusterPcmOwnResult"; emit=1; read_publish++ } \
	     /^cluster_pcm_own_clear_read_image_exact\(/ { print "static ClusterPcmOwnResult"; emit=1; read_clear++ } \
	     /^cluster_pcm_own_release_read_image_exact\(/ { print "ClusterPcmOwnResult"; emit=1; read_release++ } \
	     /^cluster_pcm_own_reclaim_read_image_exact\(/ { print "ClusterPcmOwnResult"; emit=1; read_reclaim++ } \
	     /^cluster_bufmgr_pcm_own_reclaim_read_image_for_delivery_exact\(/ { print "ClusterPcmOwnResult"; emit=1; read_delivery++ } \
	     /^cluster_pcm_own_abort_grant_reservation\(/ { print "static ClusterPcmOwnResult"; emit=1; grant_abort++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (retained_candidate != 1 || storage_candidate != 1 || predecessor_observe != 1 || read_reclaim != 1 || read_delivery != 1) exit 1 } \
	     END { if (ordinary_snapshot != 1 || n_candidate != 1 || bump_failure != 1 || bump != 1 || evict != 1 || shape != 1 || wal_copy != 1 || known_new != 1 || ordinary_lookup != 1 || direct_lookup != 1 || sidecar != 1 || activate != 1 || clear != 1 || copy != 1 || held_valid != 1 || held_validate != 1 || held_adopt != 1 || held_finish != 1 || held_abandon != 1 || drop != 1 || finish != 1 || delivery_begin != 1 || delivery_snapshot != 1 || delivery_release != 1 || normal_reserve != 1 || delivery_reserve != 1 || read_publish != 1 || read_clear != 1 || read_release != 1 || grant_abort != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_source_finish_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_source_finish_owned\(/ { print "static ResourceXApplyResult"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (found != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_source_finish_tick.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^cluster_gcs_block_resource_x_source_finish_tick\(/ { print "ResourceXApplyResult"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (found != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_eviction_owner.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^cluster_pcm_own_eviction_begin_locked\(/ { print "static ClusterPcmOwnResult"; emit=1; begin++ } \
	     /^cluster_pcm_own_eviction_finish_locked\(/ { print "static ClusterPcmOwnResult"; emit=1; finish++ } \
	     /^cluster_pcm_own_eviction_abort_locked\(/ { print "static ClusterPcmOwnResult"; emit=1; cancel++ } \
	     /^cluster_bufmgr_resource_x_target_evict_locked\(/ { print "static bool"; emit=1; driver++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (begin != 1 || finish != 1 || cancel != 1 || driver != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

# The real clock-sweep loop, excluding the freelist and unrelated strategy setup.
test_cluster_pcm_clock_sweep.inc: $(top_srcdir)/src/backend/storage/buffer/freelist.c Makefile
	awk '/Nothing on the freelist, so run the "clock sweep" algorithm/ { emit=1; begin++ } \
	     emit && /^}/ { emit=0; end++ } emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_preassert_owner.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^typedef enum ResourceXSourceSettlementStage/ { emit=1; stage++ } \
	     /^typedef struct ResourceXFirstFailureEvidence/ { emit=1; evidence++ } \
	     /^gcs_block_pcm_x_resource_x_remote_s_own_result\(/ { print "static ResourceXApplyResult"; emit=1; mapping++ } \
	     /^gcs_block_resource_x_pre_mutation_domain\(/ { print "static ResourceXFailureDomain"; emit=1; domain++ } \
	     /^gcs_block_resource_x_target_install_snapshot_result\(/ { print "static ResourceXApplyResult"; emit=1; snapshot++ } \
	     /^gcs_block_resource_x_target_own_observation_result\(/ { print "static ResourceXApplyResult"; emit=1; observation++ } \
	     /^gcs_block_resource_x_target_install_capture_coherent\(/ { print "static ResourceXApplyResult"; emit=1; capture++ } \
	     /^gcs_block_resource_x_aux_creation_reobserve_coherent\(/ { print "static ResourceXApplyResult"; emit=1; creation++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (stage != 1 || evidence != 1 || mapping != 1 || domain != 1 || snapshot != 1 || observation != 1 || capture != 1 || creation != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_n_predecessor_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/if \(\(own.flags == PCM_OWN_FLAG_REVOKING/ { emit=1; begin++ } \
	     emit && /if \(!target_retained_release_inflight\)/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_preassert_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/if \(!target_retained_release_inflight\)/ { emit=1; begin++ } \
	     emit && /} else if \(own.generation == 0\)/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_pending_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/if \(own.pcm_state == \(uint8\)PCM_STATE_N$$/ { candidate=$$0; pending=1; next } \
	     pending { if ($$0 ~ /&& own.flags == PCM_OWN_FLAG_GRANT_PENDING\) \{/) { print candidate; emit=1; begin++ } pending=0 } \
	     emit && /if \(own.pcm_state == \(uint8\)PCM_STATE_N\) \{/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_delivery_route_owner.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_delivery_arm_exact\(/ { print "static ResourceXApplyResult"; emit=1; arm++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (arm != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_checksum_owner.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_compute_checksum\(/ { print "static uint32"; emit=1; core++ } \
	     /^cluster_gcs_block_compute_checksum\(/ { print "uint32"; emit=1; wrapper++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (core != 1 || wrapper != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_own: test_cluster_pcm_own.c unit_test.h \
		test_cluster_pcm_snapshot_owner.inc test_cluster_pcm_transition_owner.inc \
		test_cluster_pcm_barrier_proof.inc \
		test_cluster_pcm_eviction_owner.inc test_cluster_pcm_clock_sweep.inc \
		test_cluster_pcm_source_finish_consumer.inc \
		test_cluster_pcm_source_finish_tick.inc \
		test_cluster_pcm_preassert_owner.inc test_cluster_pcm_preassert_consumer.inc \
		test_cluster_pcm_n_predecessor_consumer.inc \
		test_cluster_pcm_pending_consumer.inc \
		test_cluster_pcm_checksum_owner.inc test_cluster_pcm_delivery_route_owner.inc $(CLUSTER_PCM_OWN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DBUFMGR_SOURCE_PATH='"$(top_srcdir)/src/backend/storage/buffer/bufmgr.c"' \
		$< $(CLUSTER_PCM_OWN_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# S3-core direct-init proof is a dependency-light, process-local decision
# layer.  The real bufmgr captures/revalidates the snapshots under its header
# spinlock; this test pins the byte-exact operation and rejection matrix.
test_cluster_pcm_direct_init: test_cluster_pcm_direct_init.c unit_test.h \
		$(CLUSTER_PCM_DIRECT_INIT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DBUFMGR_SOURCE_PATH='"$(top_srcdir)/src/backend/storage/buffer/bufmgr.c"' \
		-DVM_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/visibilitymap.c"' \
		-DFSM_SOURCE_PATH='"$(top_srcdir)/src/backend/storage/freespace/freespace.c"' \
		-DHEAPAM_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/heapam.c"' \
		-DHIO_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/hio.c"' \
		$< \
		$(CLUSTER_PCM_DIRECT_INIT_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_pcm_aux_handoff.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^typedef struct ClusterBufmgrPcmAuxPinHandoff/ { emit=1; type++ } \
	     /^cluster_bufmgr_pcm_aux_pin_handoff_(begin|finish_exact)\(/ { print "static bool"; emit=1; functions++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (type != 1 || functions != 2) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

# Compile the production owner decision. The preserved RED compiled the old
# own-snapshot guard verbatim; GREEN now executes its extracted owner helper.
test_cluster_pcm_aux_observation.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_target_own_observation_result\(/ { print "static ResourceXApplyResult"; emit=1; matches++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (matches != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_reobserve: test_cluster_pcm_aux_reobserve.c unit_test.h \
		test_cluster_pcm_aux_handoff.inc test_cluster_pcm_aux_observation.inc \
		test_cluster_pcm_aux_reobserve_owner.inc test_cluster_pcm_aux_reobserve_context.inc \
		test_cluster_pcm_aux_prepare.inc test_cluster_heap_vm_lock_entry.inc
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_pcm_aux_reobserve_owner.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^typedef (enum ClusterPcmXWriterLedgerPhase|struct ClusterPcmXWriterLedgerEntry)/ { emit=1; types++ } \
	     /^static ClusterPcmXWriterLedgerEntry cluster_bufmgr_pcm_x_writer_ledger\[/ { print; array++ } \
	     /^cluster_bufmgr_pcm_x_writer_(find|free_entry)\(/ { print "static ClusterPcmXWriterLedgerEntry *"; emit=1; find++ } \
	     /^cluster_bufmgr_pcm_x_writer_clear\(/ { print "static void"; emit=1; clear++ } \
	     /^ClusterBufferAuxiliaryObservationUnowned\(/ { print "bool"; emit=1; unowned++ } \
	     /^ClusterBufferDirectInitObservationUnowned\(/ { print "bool"; emit=1; creation++ } \
	     /^ClusterLockBufferExclusiveAuxiliaryAware\(/ { print "bool"; emit=1; consumer++ } \
	     /^ClusterLockBufferExclusiveAuxiliaryAliasAware\(/ { print "bool"; emit=1; aliases++ } \
	     /^ClusterLockBufferExclusiveBarrierAware\(/ { print "bool"; emit=1; barrier++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (types != 2 || array != 1 || find != 2 || clear != 1 || unowned != 1 || creation != 1 || consumer != 1 || aliases != 1 || barrier != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_lock_entry.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^cluster_heap_vm_retarget_after_repick\(/ { print "static void"; emit=1; retarget++ } \
	     /^cluster_current_mx_stamp_lock_buffer\(/ { print "static bool"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 1 || retarget != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_reobserve_context.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^cluster_gcs_resource_x_target_acquire_reobserve_exact\(/ { print "ResourceXApplyResult"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_prepare.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^cluster_bufmgr_should_pcm_track\(/ { print "static inline bool"; emit=1; tracked++ } \
	     /^cluster_bufmgr_pcm_x_writer_prepare_target\(/ { print "static ClusterPcmXWriterLedgerEntry *"; emit=1; prepare++ } \
	     emit { print } /^}/ { emit=0 } END { if (tracked != 1 || prepare != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_vm_mutation.inc: $(top_srcdir)/src/backend/access/heap/visibilitymap.c Makefile
	awk '/^#define (MAPSIZE|HEAPBLOCKS_PER_|HEAPBLK_TO_)/ { print; defines++ } \
	     /^visibilitymap_(clear_retry_aware|clear_locked|pin_recent|pin_ok|set_retry_aware)\(/ { print "bool"; emit=1; boolean++ } \
	     /^visibilitymap_pin\(/ { print "void"; emit=1; pin++ } \
	     /^visibilitymap_set_locked\(/ { print "static void"; emit=1; setter++ } \
	     /^visibilitymap_get_status\(/ { print "uint8"; emit=1; status++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (defines != 6 || boolean != 5 || pin != 1 || setter != 1 || status != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_heap_repin.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^cluster_heap_lock_with_vm_repin\(/ { print "void"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_fsm_mutation.inc: $(top_srcdir)/src/backend/storage/freespace/freespace.c Makefile
	awk '/^RecordPageWithFreeSpace\(/ { print "void"; emit=1; outer++ } \
	     /^fsm_set_and_search\(/ { print "static int"; emit=1; inner++ } \
	     emit { print } /^}/ { emit=0 } END { if (outer != 1 || inner != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_mutation_consumer.inc: $(top_srcdir)/src/test/cluster_tap/aux_probe/test_pgrac_aux_consumer.c Makefile
	awk '/^aux_mutation_heap_proof\(/ { print "static Page"; emit=1; proof++ } \
	     /^aux_cycle_internal\(/ { print "static Datum"; emit=1; consumer++ } \
	     emit { print } /^}/ { emit=0 } END { if (proof != 1 || consumer != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_insert.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_insert\(/ { in_insert=1 } \
	     in_insert && !found && /^\tvm_locked = false;/ { emit=1; found++ } \
	     emit { print } emit && /^\t}/ { emit=0; complete++ } \
	     /^}/ { in_insert=0 } END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_tuple_lock.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_lock_tuple\(/ { in_lock=1 } \
	     in_lock && !found && /^\tvm_locked = false;/ { emit=1; found++ } \
	     emit { print } emit && /^\t}/ { emit=0; complete++ } \
	     /^}/ { in_lock=0 } END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_update.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/pre-crit VM content locks \(both heap pages\)/ { armed=1 } \
	     armed && !found && /^\tvm_locked = false;/ { emit=1; found++ } \
	     emit { print } emit && /^\t}/ { emit=0; complete++ } \
	     END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_unwind_helpers.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^cluster_heap_vm_barrier_warm\(/ { print "static void"; emit=1; warm++ } \
	     /^cluster_heap_lock_with_vm_repin\(/ { print "void"; emit=1; repin++ } \
	     emit { print } /^}/ { emit=0 } END { if (warm != 1 || repin != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_delete.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_delete\(/ { in_delete=1 } \
	     in_delete && !found && /^\tvm_locked = false;/ { emit=1; found++ } \
	     emit { print } emit && /^\t}/ { emit=0; complete++ } \
	     /^}/ { in_delete=0 } END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_vm_pretoast.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_update\(/ { in_update=1 } \
	     in_update && !found && /^[[:space:]]*vm_locked = false;/ { emit=1; found++ } \
	     emit { print; s=$$0; opens=gsub(/\{/, "", s); s=$$0; closes=gsub(/}/, "", s); \
	            depth+=opens-closes; if(opens) opened=1; \
	            if(opened && depth==0) { emit=0; complete++ } } \
	     /^}/ { in_update=0 } END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_mutation: test_cluster_pcm_aux_mutation.c unit_test.h \
		test_cluster_heap_vm_insert.inc test_cluster_heap_vm_tuple_lock.inc \
		test_cluster_heap_vm_update.inc test_cluster_heap_vm_unwind_helpers.inc \
		test_cluster_heap_vm_delete.inc test_cluster_heap_vm_pretoast.inc \
		test_cluster_pcm_aux_vm_mutation.inc test_cluster_pcm_aux_heap_repin.inc \
		test_cluster_pcm_aux_fsm_mutation.inc test_cluster_pcm_aux_mutation_consumer.inc \
		test_cluster_pcm_aux_page_space.inc
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_pcm_aux_page_space.inc: $(top_srcdir)/src/backend/storage/page/bufpage.c Makefile
	awk '/^PageGet(Heap)?FreeSpace\(/ { print "Size"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 2) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_consumer.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^typedef enum ClusterBufmgrPcmDirectInitArmResult/ { emit=1; arm_enum++ } \
	     /^cluster_bufmgr_pcm_arm_direct_init\(/ { print "static ClusterBufmgrPcmDirectInitArmResult"; emit=1; arm++ } \
	     /^cluster_bufmgr_pcm_aux_installed_image_matches\(/ { print "static bool"; emit=1 } \
	     /^cluster_bufmgr_pcm_gate_direct_init\(/ { print "static bool"; emit=1; gate++ } \
	     /^LockBufferForAuxiliaryPageInit\(/ { print "static Buffer"; emit=1; consumer++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (arm_enum != 1 || arm != 1 || gate != 1 || consumer != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_ledger.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^typedef (enum ClusterPcmXWriterLedgerPhase|struct ClusterPcmXWriterLedgerEntry)/ { emit=1; types++ } \
	     /^static ClusterPcmXWriterLedgerEntry cluster_bufmgr_pcm_x_writer_ledger\[/ { print; array++ } \
	     /^cluster_bufmgr_pcm_x_writer_(find|free_entry)\(/ { print "static ClusterPcmXWriterLedgerEntry *"; emit=1; find++ } \
	     /^cluster_bufmgr_pcm_x_writer_(entry_exact|activate)\(/ { print "static bool"; emit=1; boolean++ } \
	     /^cluster_bufmgr_pcm_x_writer_(clear|track_target_direct_init|activate_target_direct_init)\(/ { print "static void"; emit=1; action++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (types != 2 || array != 1 || find != 2 || boolean != 2 || action != 3) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_pcm_aux_consumer: test_cluster_pcm_aux_consumer.c unit_test.h \
		test_cluster_pcm_aux_consumer.inc test_cluster_pcm_aux_ledger.inc \
		$(CLUSTER_PCM_DIRECT_INIT_O) $(CLUSTER_RESOURCE_X_IDENTITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_PCM_DIRECT_INIT_O) \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-8.4 W0: source-path-only static/model RED.  Prospective R4 files are
# passed as strings (not prerequisites), so their absence is observed as
# ABSENT by the test instead of becoming a compile or link failure.
test_cluster_r4_static_model: test_cluster_r4_static_model.c unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DCR_SERVER_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_cr_server.h"' \
		-DCR_SERVER_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_cr_server.c"' \
		-DCR_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_cr.c"' \
		-DGCS_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_gcs_block.h"' \
		-DGCS_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_gcs_block.c"' \
		-DPCM_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_pcm_lock.h"' \
		-DPCM_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_pcm_lock.c"' \
		-DBUFMGR_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/storage/buffer/bufmgr.c"' \
		-DIC_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_ic.h"' \
		-DITL_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_itl.h"' \
		-DHEAP_VIS_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/access/heap/heapam_visibility.c"' \
		-DHEAPAM_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/access/heap/heapam.c"' \
		-DVIS_RESOLVE_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_visibility_resolve.c"' \
		-DTT_STATUS_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_tt_status.h"' \
		-DTT_STATUS_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_tt_status.c"' \
		-DTT_HINT_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_tt_status_hint.h"' \
		-DTT_HINT_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_tt_status_hint.c"' \
		-DMULTIXACT_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_multixact.h"' \
		-DMULTIXACT_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_multixact.c"' \
		-DTX_RESOLVE_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_tx_resolve.h"' \
		-DTX_RESOLVE_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_tx_resolve.c"' \
		-DSEMANTIC_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_semantic_activation.h"' \
		-DSEMANTIC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_semantic_activation.c"' \
		-DQVOTEC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_qvotec.c"' \
		$< \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport.a $(LDFLAGS) -lssl -lcrypto -o $@

# spec-8.4 B1/D1: exact caller-selected locator and dormant normalized
# resolver entry points.  This links the production object directly.
CLUSTER_TX_RESOLVE_O = $(top_builddir)/src/backend/cluster/cluster_tx_resolve.o
CLUSTER_R4_TX_LOCATOR_UBA_O = $(top_builddir)/src/backend/cluster/cluster_uba.o
test_cluster_r4_tx_locator: test_cluster_r4_tx_locator.c unit_test.h \
		$(CLUSTER_TX_RESOLVE_O) $(CLUSTER_R4_TX_LOCATOR_UBA_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_TX_RESOLVE_O) $(CLUSTER_R4_TX_LOCATOR_UBA_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-8.4 B1/D5: compile the real runtime-visibility provider into a
# function-sectioned test object.  Dead stripping keeps this existing R4
# target focused on the exact-origin provider without stubbing unrelated
# legacy runtime-visibility services from the same production source file.
CLUSTER_R4_RUNTIME_VIS_TEST_O = test_cluster_r4_runtime_visibility_exact.o
CLUSTER_R4_RUNTIME_VIS_UBA_O = $(top_builddir)/src/backend/cluster/cluster_uba.o
$(CLUSTER_R4_RUNTIME_VIS_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_runtime_visibility.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_r4_tx_outcome: test_cluster_r4_tx_outcome.c unit_test.h \
		$(CLUSTER_R4_RUNTIME_VIS_TEST_O) $(CLUSTER_R4_RUNTIME_VIS_UBA_O) \
		$(CLUSTER_CR_SERVER_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_R4_RUNTIME_VIS_TEST_O) $(CLUSTER_R4_RUNTIME_VIS_UBA_O) \
		$(CLUSTER_CR_SERVER_POLICY_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Real CR shared predicate plus real stripe derivation, with only the
# process/lock/nextXid boundaries supplied by the fixture.
test_cluster_cr_native_origin: test_cluster_cr_native_origin.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_cr.c \
		$(top_builddir)/src/backend/cluster/cluster_xid_stripe.o
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DCLUSTER_CR_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_cr.c"' \
		$< $(top_builddir)/src/backend/cluster/cluster_xid_stripe.o \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Stage 8 R4 D3: compile the real capability store with function sections so
# this focused target executes its shared-lock sampler and existing generation
# fence without pulling unrelated Smart Fusion runtime paths into the fixture.
CLUSTER_R4_SF_DEP_TEST_O = test_cluster_r4_sf_dep_product.o
$(CLUSTER_R4_SF_DEP_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_sf_dep.c \
		$(top_srcdir)/src/include/cluster/cluster_sf_dep.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_sf_dep: test_cluster_sf_dep.c unit_test.h $(CLUSTER_R4_SF_DEP_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_R4_SF_DEP_TEST_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Stage 8 R4 D3: exact REQUEST80/FORWARD96 production decoder seam.  The
# USE_CLUSTER_UNIT symbols call the same static branches as the registered
# handlers; function sections keep the fixture independent of legacy GCS code.
CLUSTER_R4_GCS_BLOCK_TEST_O = test_cluster_r4_gcs_block_product.o
$(CLUSTER_R4_GCS_BLOCK_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c \
		$(top_srcdir)/src/include/cluster/cluster_gcs_block.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT \
		-DCLUSTER_R4_ROUTE_POLICY_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

# Execute the existing read-rearm owners verbatim, not a mirrored test loop.
# Runtime gates/locks are bounded fixture seams; reservation transitions use
# the real cluster_pcm_own object. Fail generation on missing/duplicate owners.
test_cluster_gcs_read_rearm_owner.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c Makefile
	awk '/^cluster_pcm_own_begin_grant_reservation\(/ { print "static ClusterPcmOwnResult"; emit=1; begin++ } \
	     /^cluster_pcm_own_abort_grant_reservation\(/ { print "static ClusterPcmOwnResult"; emit=1; cancel++ } \
	     /^cluster_pcm_own_read\(/ { print "static inline void"; emit=1; observe++ } \
	     /^cluster_bufmgr_resource_x_wait_retry\(/ { print "static bool"; emit=1; wait_owner++ } \
	     /^cluster_bufmgr_pcm_begin_grant_reservation_wait\(/ { print "static ClusterPcmOwnResult"; emit=1; begin_wait++ } \
	     /^typedef enum ClusterBufmgrPcmRetryRearmResult/ { emit=1; result_type++ } \
	     /^cluster_bufmgr_pcm_retry_denied_rearm\(/ { print "static ClusterBufmgrPcmRetryRearmResult"; emit=1; rearm++ } \
	     /^LockBufHdr\(/ { print "uint32"; emit=1; header_lock++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (begin != 1 || cancel != 1 || observe != 1 || wait_owner != 1 || begin_wait != 1 || result_type != 1 || rearm != 1 || header_lock != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_r4_route_policy: test_cluster_r4_route_policy.c unit_test.h \
		test_cluster_pcm_snapshot_owner.inc test_cluster_gcs_read_rearm_owner.inc \
		$(CLUSTER_VERSION_O) $(CLUSTER_R4_GCS_BLOCK_TEST_O) $(CLUSTER_PCM_OWN_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_R4_GCS_BLOCK_TEST_O) $(CLUSTER_PCM_OWN_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Stage 8 R4 D4-B: compile the real LMS source with function sections so the
# focused test seam executes the incarnation publication used by both DATA
# worker main entries without linking unrelated LMS duties.
CLUSTER_R4_LMS_CONTROLS_TEST_O = test_cluster_r4_lms_controls_product.o

test_cluster_lms_native_probe: test_cluster_lms_native_probe.c unit_test.h \
        $(top_srcdir)/src/backend/cluster/cluster_lms.c \
        $(top_srcdir)/src/backend/cluster/cluster_ges_dedup.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DLMS_NATIVE_PROBE_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_lms.c"' \
		-DGES_DEDUP_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_ges_dedup.c"' \
		$< $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

$(CLUSTER_R4_LMS_CONTROLS_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_lms.c \
		$(top_srcdir)/src/include/cluster/cluster_lms.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

test_cluster_r4_lms_controls: test_cluster_r4_lms_controls.c unit_test.h \
		$(CLUSTER_R4_LMS_CONTROLS_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_R4_LMS_CONTROLS_TEST_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) -o $@

# Stage 8 R4 D4-B: execute the real common legacy reservation helper from a
# function-sectioned CR-server object and source-check all four legacy callers.
CLUSTER_R4_SLOT_RESERVATION_TEST_O = test_cluster_r4_slot_reservation_product.o
$(CLUSTER_R4_SLOT_RESERVATION_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_cr_server.c \
		$(top_srcdir)/src/include/cluster/cluster_cr_server.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

CLUSTER_R4_LMS_DATA_PLANE_TEST_O = test_cluster_r4_lms_data_plane_product.o
$(CLUSTER_R4_LMS_DATA_PLANE_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_lms_data_plane.c \
		$(top_srcdir)/src/include/cluster/cluster_lms.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

CLUSTER_R4_SLOT_RESERVATION_UBA_O = $(top_builddir)/src/backend/cluster/cluster_uba.o
CLUSTER_R4_CONTINUATION_CR_O = test_cluster_r4_continuation_cr_product.o
CLUSTER_R4_CONTINUATION_PAGE_O = test_cluster_r4_continuation_page_product.o
$(CLUSTER_R4_CONTINUATION_PAGE_O): $(top_srcdir)/src/backend/storage/page/bufpage.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@
$(CLUSTER_R4_CONTINUATION_CR_O): $(top_srcdir)/src/backend/cluster/cluster_cr.c \
		$(top_srcdir)/src/include/cluster/cluster_cr.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-Dcluster_cr_build_on_holder_step=continuation_real_step \
		-Dcluster_cr_build_on_holder_forget=continuation_real_forget \
		-Dcluster_cr_build_on_holder_pending_locator=continuation_real_pending \
		-Dcluster_cr_r4_extract_resident_record=continuation_real_extract \
		-c $< -o $@

test_cluster_r4_slot_reservation: test_cluster_r4_slot_reservation.c unit_test.h \
		$(CLUSTER_R4_SLOT_RESERVATION_TEST_O) $(CLUSTER_R4_LMS_DATA_PLANE_TEST_O) \
		$(CLUSTER_R4_SLOT_RESERVATION_UBA_O) $(CLUSTER_R4_CONTINUATION_CR_O) \
		test_cluster_r4_multi_resolve_product.o $(CLUSTER_CR_APPLY_O) $(CLUSTER_R4_CONTINUATION_PAGE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DCR_SERVER_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_cr_server.c"' \
		$< $(CLUSTER_R4_SLOT_RESERVATION_TEST_O) $(CLUSTER_R4_LMS_DATA_PLANE_TEST_O) \
		$(CLUSTER_R4_SLOT_RESERVATION_UBA_O) $(CLUSTER_R4_CONTINUATION_CR_O) \
		test_cluster_r4_multi_resolve_product.o $(CLUSTER_CR_APPLY_O) $(CLUSTER_R4_CONTINUATION_PAGE_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Stage 8 R4 D11: exercise the thin observation adapter and the real holder
# result seam.  The fixture supplies only the existing CR-copy/constructor,
# semantic-admission and ClusterCRShared counter boundaries.
CLUSTER_R4_OBSERVE_TEST_O = test_cluster_r4_observe_product.o
$(CLUSTER_R4_OBSERVE_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_r4_observe.c \
		$(top_srcdir)/src/include/cluster/cluster_r4_observe.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_r4_observe: test_cluster_r4_observe.c unit_test.h \
		$(CLUSTER_R4_OBSERVE_TEST_O) $(CLUSTER_R4_SLOT_RESERVATION_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_R4_OBSERVE_TEST_O) $(CLUSTER_R4_SLOT_RESERVATION_TEST_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) -o $@

# Stage 8 R4 D7: execute the public MultiXact resolver from the real product
# source.  The fixture supplies only the native reader and semantic-admission
# boundaries; function sections discard the unrelated exact-xid resolver.
CLUSTER_R4_MULTI_RESOLVE_TEST_O = test_cluster_r4_multi_resolve_product.o
$(CLUSTER_R4_MULTI_RESOLVE_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_tx_resolve.c \
		$(top_srcdir)/src/include/cluster/cluster_tx_resolve.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_r4_multi_subx_2pc: test_cluster_r4_multi_subx_2pc.c unit_test.h \
		$(CLUSTER_R4_MULTI_RESOLVE_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_R4_MULTI_RESOLVE_TEST_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) -o $@

# spec-8.4 B1/D2 + D4-B worker-0 builder: transaction-head and exact
# previous-edge identity plus the real cluster_cr.c step API over a stable
# copied page.  Function sections retain only the builder entry/context;
# cluster_cr_apply.o supplies the existing pure candidate collector.
CLUSTER_R4_CR_WALK_TEST_O = test_cluster_r4_cr_walk_product.o
$(CLUSTER_R4_CR_WALK_TEST_O): test_cluster_r4_cr_walk_product.c \
		$(top_srcdir)/src/backend/cluster/cluster_cr.c \
		$(top_srcdir)/src/include/cluster/cluster_cr.h \
		$(top_srcdir)/src/include/cluster/cluster_undo_record.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_r4_cr_walk: test_cluster_r4_cr_walk.c unit_test.h \
		test_cluster_hot_bufpage_product.o \
		$(CLUSTER_VERSION_O) $(CLUSTER_UBA_O) $(CLUSTER_CR_APPLY_O) \
		$(CLUSTER_R4_CR_WALK_TEST_O) $(CLUSTER_R4_MULTI_RESOLVE_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UBA_O) $(CLUSTER_CR_APPLY_O) \
		$(CLUSTER_HOT_BUFPAGE_TEST_O) \
		$(CLUSTER_R4_CR_WALK_TEST_O) $(CLUSTER_R4_MULTI_RESOLVE_TEST_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# GCS-race round-4c tier1-partial-IO: test_cluster_ic_tier1_partial links
# cluster_ic_tier1.o standalone and drives the PRODUCTION outbound-tail
# queue/drain state machine over a real localhost TCP pair (initial-EAGAIN
# full-frame queue, resume-cursor-vs-capacity regression, close_peer tail
# reset, dead-peer drain fail-closed).  GCS serve-stall round-5 extends it
# with the multi-frame backpressure legs (per-peer outbound FIFO: second
# frame survives a pending tail, submission order preserved, cross-peer
# isolation).
test_cluster_ic_tier1_partial: test_cluster_ic_tier1_partial.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_TIER1_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_IC_TIER1_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# GCS serve-stall round-5: test_cluster_lms_outbound links
# cluster_lms_outbound.o standalone and pins the drain's four-state send
# ownership contract with a scripted cluster_ic_send_envelope mock
# (admitted frame never resubmitted, refused frame retained in per-peer
# order, blocked peer never head-of-line blocks the batch).
CLUSTER_LMS_OUTBOUND_O = $(top_builddir)/src/backend/cluster/cluster_lms_outbound.o
test_cluster_lms_outbound: test_cluster_lms_outbound.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMS_OUTBOUND_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_LMS_OUTBOUND_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.31 D9 v0.5: test_cluster_bufmgr_pcm_hook verifies the bufmgr
# LockBuffer/LockBufferForCleanup PCM hook layer in isolation by using a
# fake LockBuffer wrapper that mirrors the D3 hook structure.  Links
# cluster_pcm_lock.o (real acquire/release behavior) + reuses the same
# stub family as test_cluster_pcm_lock.
test_cluster_bufmgr_pcm_hook: test_cluster_bufmgr_pcm_hook.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_PCM_LOCK_O) $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(CLUSTER_RESOURCE_X_NODE_WIRE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_PCM_LOCK_O) $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(CLUSTER_RESOURCE_X_NODE_WIRE_O) \
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

# Compile the actual terminal ingress verbatim. Dependencies are explicit
# fixture doubles; the master/source decision and call sequence are product C.
test_cluster_resource_x_terminal_owner.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_requester_terminal_try\(/ { print "static ResourceXApplyResult"; emit=1; owner++ } \
	     /^gcs_block_resource_x_target_peer_result_exact\(/ { print "static ResourceXApplyResult"; emit=1 } \
	     /^gcs_block_resource_x_target_peer_matches_exact\(/ { print "static bool"; emit=1; peer++ } \
	     /^gcs_block_resource_x_delivery_finish_exact\(/ { print "static ResourceXApplyResult"; emit=1; finish++ } \
	     /^gcs_block_resource_x_delivery_run_exact\(/ { print "static ResourceXApplyResult"; emit=1; run++ } \
	     /^cluster_gcs_block_resource_x_delivery_tick\(/ { print "ResourceXApplyResult"; emit=1; tick++ } \
	     /^gcs_block_resource_x_cleanup_frame_exact\(/ { print "static ResourceXApplyResult"; emit=1; cleanup++ } \
	     /^gcs_block_resource_x_kind9_ingress\(/ { print "static void"; emit=1; admission++ } \
	     /^gcs_block_resource_x_type17_ingress\(/ { print "static ResourceXApplyResult"; emit=1; type17++ } \
	     /^gcs_block_resource_x_bootstrapped_assert_ingress\(/ { print "static ResourceXApplyResult"; emit=1; assertion++ } \
	     /^gcs_block_resource_x_requester_join_ingress\(/ { print "static ResourceXApplyResult"; emit=1; join++ } \
	     /^cluster_gcs_resource_x_target_context_recheck_result_exact\(/ { print "ResourceXApplyResult"; emit=1; context++ } \
	     /^cluster_gcs_resource_x_target_context_recheck_exact\(/ { print "bool"; emit=1; context_bool++ } \
	     /^gcs_block_resource_x_target_recycle_inputs_exact\(/ { print "static bool"; emit=1; recycle_input++ } \
	     /^gcs_block_resource_x_target_recycle_inputs_result_exact\(/ { print "static ResourceXApplyResult"; emit=1; recycle_input++ } \
	     /^cluster_gcs_resource_x_target_itl_recycle_(begin|finish)_exact\(/ { print "ResourceXApplyResult"; emit=1; recycle++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (owner != 1 || admission != 1 || join != 1 || finish != 1 || run != 1 || tick != 1 || cleanup != 1 || context != 1 || context_bool != 1 || peer != 1 || type17 != 1 || assertion != 1 || recycle_input != 1 || recycle != 2) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_terminal_gate_consumer.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^[[:space:]]*terminal_admission_current$$/ { emit=1; begin++ } \
	     emit && /memset\(&target_install_follow, 0,/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_eviction_owner.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_target_eviction_recheck_result\(/ { print "static ResourceXApplyResult"; emit=1 } \
	     /^cluster_gcs_resource_x_target_evict_(prepare|publish|abort)_exact\(/ { print "ResourceXApplyResult"; emit=1; owner++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (owner != 3 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_failed_round_observation.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/\/\* The cached-X sample can be invalidated by an exact type-17/ { emit=1; begin++ } \
	     emit { print } emit && /break;/ { emit=0; end++ } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_session_observation.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_block_resource_x_gate_session_recheck_result\(/ { print "static ResourceXApplyResult"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (found != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_remote_s_types.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^typedef (enum ResourceXSourceSettlementStage|struct ResourceXFirstFailureEvidence)/ { emit=1; types++ } \
	     /^gcs_block_resource_x_remote_s_authority_result_exact\(/ { print "static ResourceXApplyResult"; emit=1; authority++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (types != 2 || authority != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_remote_s_staged_gate.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/\/\* The type-17.s authority is sampled again after cross-domain staging\./ { emit=1; begin++ } \
	     emit && /if \(!LWLockConditionalAcquire\(content_lock, LW_EXCLUSIVE\)\)/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_source_final_gate.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/failure_stage = "source-final-gate";/ { emit=1; begin++ } \
	     emit && /if \(tagless_target_x && target_x_retain && !semantic_retained\)/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_source_entry_gate.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/\/\* Resource-X formation and the local BufferDesc owner/ { emit=1; begin++ } \
	     emit && /status_result = cluster_pcm_lock_resource_x_holder_status_exact\(/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_source_prepare_gate.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/failure_stage = "source-gate-recheck";/ { emit=1; begin++ } \
	     emit && /memset\(&source_prepare_refusal, 0,/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_retained_wait_gate.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/if \(target_retained_release_inflight\)/ { emit=1; begin++; next } \
	     emit && /diagnostic_stage = "retained-release-wait";/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_predecessor_wait_gate.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/diagnostic_stage = "predecessor-settlement-wait";/ { emit=1; begin++ } \
	     emit && /now_us = gcs_block_pcm_x_monotonic_us\(\);/ { emit=0; end++ } \
	     emit { print } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_dispatch_observation.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/diagnostic_stage = "dispatch-recheck";/ { print prev2; print prev1; emit=1; begin++ } \
	     emit && /if \(action != RESOURCE_X_BOOTSTRAP_ROUND_WAIT\)/ { emit=0; end++ } \
	     emit { print } \
	     { prev2=prev1; prev1=$$0 } \
	     END { if (begin != 1 || end != 1 || emit) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_resource_x_terminal_ingress: test_cluster_resource_x_terminal_ingress.c \
		unit_test.h test_cluster_resource_x_terminal_owner.inc \
		test_cluster_resource_x_eviction_owner.inc \
		test_cluster_resource_x_failed_round_observation.inc \
		test_cluster_resource_x_remote_s_types.inc \
		test_cluster_resource_x_remote_s_staged_gate.inc \
		test_cluster_resource_x_terminal_gate_consumer.inc \
		test_cluster_resource_x_session_observation.inc \
		test_cluster_resource_x_source_final_gate.inc \
		test_cluster_resource_x_source_entry_gate.inc \
		test_cluster_resource_x_source_prepare_gate.inc \
		test_cluster_resource_x_retained_wait_gate.inc \
		test_cluster_resource_x_predecessor_wait_gate.inc \
		test_cluster_resource_x_dispatch_observation.inc \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(top_srcdir)/src/include/cluster/cluster_pcm_lock.h \
		$(top_srcdir)/src/include/cluster/cluster_ic.h \
		$(top_srcdir)/src/include/cluster/cluster_semantic_activation.h \
		$(top_srcdir)/src/include/cluster/cluster_resource_x_node_wire.h
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-2.33 D13: test_cluster_gcs_block verifies GCS block-shipping
# substrate compile-time invariants (msg_type 14/15, payload ABI sizes,
# field offsets, deterministic hash math, wait events + tranches
# distinct, BLCKSZ cross-check).  The GCS substrate checks do not link
# cluster_gcs_block.o.  Other behavioral coverage lives in t/111.
test_cluster_gcs_block: test_cluster_gcs_block.c unit_test.h \
		$(top_srcdir)/src/include/cluster/cluster_pcm_lock.h \
		$(top_srcdir)/src/include/cluster/cluster_pcm_x_bufmgr.h $(CLUSTER_VERSION_O) \
		$(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(CLUSTER_RESOURCE_X_RETRY_O)
	$(CC) $(CFLAGS) $(PTHREAD_CFLAGS) $(CPPFLAGS) \
		-DGCS_BLOCK_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_gcs_block.c"' \
		-DPCM_LOCK_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_pcm_lock.c"' \
		-DRUNTIME_VISIBILITY_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_runtime_visibility.c"' \
		-DBUFMGR_SOURCE_PATH='"$(top_srcdir)/src/backend/storage/buffer/bufmgr.c"' \
		-DLMS_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_lms.c"' \
		-DLMS_OUTBOUND_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_lms_outbound.c"' \
		-DT400_SOURCE_PATH='"$(top_srcdir)/src/test/cluster_tap/t/400_pcm_x_queue_4node_liveness.pl"' \
		-DT406_SOURCE_PATH='"$(top_srcdir)/src/test/cluster_tap/t/406_resource_x_finish_flush_failclosed_4node.pl"' \
		$< \
		$(CLUSTER_VERSION_O) $(CLUSTER_RESOURCE_X_IDENTITY_O) \
		$(CLUSTER_RESOURCE_X_RETRY_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a $(PTHREAD_LIBS) -o $@

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

# spec-7.2a:  test_cluster_gcs_block_dedup_reclaim verifies the eager-reclaim
# safety decision primitives (review r1): in-window reclaim whitelist
# (GcsBlockReplyStatusIsReclaimIdempotent, currently empty/fail-closed) and
# per-entry reclaim verdict (GcsBlockDedupEntryIsReclaimSafe: in-flight never,
# out-of-window 2x safe for all, in-window idempotent-only).  Header-only;
# behavioral coverage (cap-full -> reclaim, evict_count, retransmit-dedup
# correctness) in cluster_tap t/366_gcs_block_dedup_capacity_2node.pl.
test_cluster_gcs_block_dedup_reclaim: test_cluster_gcs_block_dedup_reclaim.c unit_test.h \
		$(CLUSTER_VERSION_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-7.2a review F4:  test_cluster_gcs_block_dedup_htab links the REAL
# cluster_gcs_block_dedup.o and drives lookup_or_register to cap against a
# bounded fake HTAB (with HASH_REMOVE + hole reuse), proving the
# cap-full -> eager-reclaim -> re-register chain (U1-U6) and the D4
# auto-size floor.  The TTL sweep is never called, so evict_count
# assertions attribute to eager reclaim alone.
CLUSTER_GCS_BLOCK_DEDUP_O = $(top_builddir)/src/backend/cluster/cluster_gcs_block_dedup.o
test_cluster_gcs_block_dedup_htab: test_cluster_gcs_block_dedup_htab.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_GCS_BLOCK_DEDUP_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_GCS_BLOCK_DEDUP_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Stage 8 R4 D3 P1-B: compile the real dedup source into a target-local object
# whose instr_time clock_gettime call is redirected to the independent fixture
# monotonic clock.  Other dedup targets continue to link the normal product
# object and observe the normal system clock.
CLUSTER_R4_ROUTE_DEDUP_TEST_O = test_cluster_r4_route_dedup_product.o
$(CLUSTER_R4_ROUTE_DEDUP_TEST_O): \
		$(top_srcdir)/src/backend/cluster/cluster_gcs_block_dedup.c \
		$(top_srcdir)/src/include/cluster/cluster_gcs_block_dedup.h
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-Dclock_gettime=cluster_test_clock_gettime -c $< -o $@

test_cluster_gcs_block_dedup_r4_route: test_cluster_gcs_block_dedup_r4_route.c unit_test.h \
		$(CLUSTER_R4_ROUTE_DEDUP_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_R4_ROUTE_DEDUP_TEST_O) \
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
		$(CLUSTER_VERSION_O) $(CLUSTER_VIS_VERDICT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DHEAPAM_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/heapam.c"' \
		-DHEAPAM_VISIBILITY_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/heapam_visibility.c"' \
		-DHEAPAM_HANDLER_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/heapam_handler.c"' \
		-DNBTINSERT_SOURCE_PATH='"$(top_srcdir)/src/backend/access/nbtree/nbtinsert.c"' \
		-DTT_LOCAL_SOURCE_PATH='"$(top_srcdir)/src/backend/cluster/cluster_tt_local.c"' \
		-DXACT_SOURCE_PATH='"$(top_srcdir)/src/backend/access/transam/xact.c"' \
		$< \
		$(CLUSTER_VERSION_O) $(CLUSTER_VIS_VERDICT_O) \
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

# spec-8.3 D11: test_cluster_active_itl_transfer pins the active-ITL
# current-block transfer contracts.  Header-inline policy checks cover the
# GCS/bufmgr closed decisions; the real cluster_itl_touch.o runs registration,
# dedupe/invalidation and terminal finish against narrow backend-service
# stubs.  The dynamic end-to-end proof lives in cluster_tap t/408.
CLUSTER_ITL_TOUCH_REAL_O = $(top_builddir)/src/backend/cluster/cluster_itl_touch.o
test_cluster_active_itl_transfer: test_cluster_active_itl_transfer.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_ITL_TOUCH_REAL_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_ITL_TOUCH_REAL_O) \
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

# Includes the real local consumer to seed otherwise unreachable partial
# publication states; dead stripping removes unrelated local entry points.
test_cluster_tt_abort_owner: test_cluster_tt_abort_owner.c test_cluster_tt_durable.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_tt_local.c $(CLUSTER_TT_DURABLE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(CLUSTER_TT_DURABLE_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

test_cluster_tt_active_owner: test_cluster_tt_active_owner.c test_cluster_tt_durable.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_tt_local.c \
		$(top_srcdir)/src/backend/cluster/cluster_tt_slot.c $(CLUSTER_TT_DURABLE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(CLUSTER_TT_DURABLE_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
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
test_cluster_itl_undo_capture.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^cluster_heap_capture_undo_prior_lock\(/ { print "static bool"; emit=1; found++ } \
	     emit { print } emit && /^}/ { emit=0; done++ } \
	     END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_itl_inverse_product.o: $(top_srcdir)/src/backend/cluster/cluster_cr_apply.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_itl_reader_real_triple: test_cluster_itl_reader_real_triple.c unit_test.h test_cluster_itl_undo_capture.inc \
		test_cluster_itl_inverse_product.o $(CLUSTER_VERSION_O) $(CLUSTER_ITL_O) $(CLUSTER_UBA_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		test_cluster_itl_inverse_product.o $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
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

# spec-3.6b D1: pure current-DML descriptor/proof validators and compositor.
CLUSTER_MULTIXACT_CURRENT_O = $(top_builddir)/src/backend/cluster/cluster_multixact_current.o
CLUSTER_MULTIXACT_CURRENT_WIRE_O = \
	$(top_builddir)/src/backend/cluster/cluster_multixact_current_wire.o
CLUSTER_MULTIXACT_CURRENT_STATS_O = \
	$(top_builddir)/src/backend/cluster/cluster_multixact_current_stats.o
CLUSTER_MULTIXACT_MATERIALIZER_TEST_O = \
	test_cluster_multixact_materializer_product.o
$(CLUSTER_MULTIXACT_MATERIALIZER_TEST_O): \
		$(top_srcdir)/src/backend/access/transam/multixact.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT \
		-DGetMultiXactIdMembers=cluster_multixact_test_native_members_unused \
		-ffunction-sections -fdata-sections -c $< -o $@

test_cluster_multixact_current: test_cluster_multixact_current.c unit_test.h \
		$(CLUSTER_MULTIXACT_CURRENT_O) $(CLUSTER_MULTIXACT_CURRENT_WIRE_O) \
		$(CLUSTER_CR_SERVER_C0_TEST_O) $(CLUSTER_MULTIXACT_MATERIALIZER_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		-DHEAPAM_CURRENT_MX_SOURCE_PATH='"$(top_srcdir)/src/backend/access/heap/heapam.c"' \
		-DMULTIXACT_SOURCE_PATH='"$(top_srcdir)/src/backend/access/transam/multixact.c"' \
		$(CLUSTER_MULTIXACT_CURRENT_O) $(CLUSTER_MULTIXACT_CURRENT_WIRE_O) \
		$(CLUSTER_CR_SERVER_C0_TEST_O) $(CLUSTER_MULTIXACT_MATERIALIZER_TEST_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(CLUSTER_UNIT_PGPORT) -o $@

# spec-3.6b D8: real shared counter region, restart histogram/max, and
# postmaster-lifetime stats timestamp.
test_cluster_multixact_current_stats: test_cluster_multixact_current_stats.c \
		unit_test.h $(CLUSTER_MULTIXACT_CURRENT_STATS_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_MULTIXACT_CURRENT_STATS_O) \
		$(CLUSTER_UNIT_PGPORT) -o $@

# spec-8.4 R4 D10: real-module behavior tests for the three non-CR
# typed SOURCE dispatch families.  Each fixture replaces only adjacent
# PostgreSQL/runtime boundaries and executes its owning product object.
CLUSTER_R4_D10_HINT_SOURCE_O = $(top_builddir)/src/backend/cluster/cluster_tt_status_hint.o
CLUSTER_R4_D10_TT_SOURCE_O = $(top_builddir)/src/backend/cluster/cluster_tt_status.o
CLUSTER_R4_D10_MULTI_SOURCE_O = $(top_builddir)/src/backend/cluster/cluster_multixact.o

test_cluster_r4_d10_hint_source: test_cluster_r4_d10_hint_source.c unit_test.h \
		$(CLUSTER_R4_D10_HINT_SOURCE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_R4_D10_HINT_SOURCE_O) -o $@

test_cluster_r4_d10_tt_source: test_cluster_r4_d10_tt_source.c unit_test.h \
		$(CLUSTER_R4_D10_TT_SOURCE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_R4_D10_TT_SOURCE_O) -o $@

test_cluster_r4_d10_multi_source: test_cluster_r4_d10_multi_source.c unit_test.h \
		$(CLUSTER_R4_D10_MULTI_SOURCE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< $(CLUSTER_R4_D10_MULTI_SOURCE_O) -o $@

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

# spec-3.7 D12 + spec-8.1 O4: encode/decode tests plus the real read-only
# pool observer and once-per-postmaster publication lifecycle.  Build the two
# product sources with function sections so the standalone unit links only the
# observation slice it executes, rather than requiring stubs for unrelated DML,
# WAL, cleaner and GCS paths in the same large translation units.
CLUSTER_UNDO_RECORD_TEST_O = test_cluster_undo_record_product.o
CLUSTER_UNDO_ALLOC_TEST_O = test_cluster_undo_alloc_product.o
ifeq ($(PORTNAME),darwin)
CLUSTER_UNDO_TEST_GC_SECTIONS = -Wl,-dead_strip
else
CLUSTER_UNDO_TEST_GC_SECTIONS = -Wl,--gc-sections
endif

$(CLUSTER_UNDO_RECORD_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_undo_record.c \
		$(top_srcdir)/src/include/cluster/cluster_undo_record.h \
		$(top_srcdir)/src/include/cluster/cluster_undo_record_api.h \
		$(top_srcdir)/src/include/cluster/storage/cluster_undo_alloc.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

$(CLUSTER_UNDO_ALLOC_TEST_O): $(top_srcdir)/src/backend/cluster/storage/cluster_undo_alloc.c \
		$(top_srcdir)/src/include/cluster/cluster_undo_gcs.h \
		$(top_srcdir)/src/include/cluster/cluster_undo_segment.h \
		$(top_srcdir)/src/include/cluster/storage/cluster_undo_alloc.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

# spec-8.4a D9 (local core): test_cluster_undo_block0 links the real
# cluster_undo_block0.o and pins the dependency-light identity layer:
# direct logical-slot mapping with exact owner-run validation, resolved
# root intent/field-exact comparison, explicit generation presence
# (zero valid, UINT32_MAX exhausted, no wrap) and the frozen resident
# slot-state edge table.  Shmem/frame/WAL/I/O/recovery/activation faces
# are later R4A batches (spec P1+); t/409 wraps this binary.
CLUSTER_UNDO_BLOCK0_O = $(top_builddir)/src/backend/cluster/storage/cluster_undo_block0.o
CLUSTER_UNDO_BLOCK0_CURRENT_O = \
	$(top_builddir)/src/backend/cluster/storage/cluster_undo_block0_current.o
CLUSTER_UNDO_BLOCK0_RESIDENT_O = \
	$(top_builddir)/src/backend/cluster/storage/cluster_undo_block0_resident.o
test_cluster_undo_block0: test_cluster_undo_block0.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_UNDO_BLOCK0_RESIDENT_O)
	$(CC) $(CFLAGS) $(PTHREAD_CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_UNDO_BLOCK0_RESIDENT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a $(PTHREAD_LIBS) -o $@

# Spec 8.4A Candidate-2: the fixture includes the exact current owner source
# so it can pin private key/phase invariants without exporting test-only ABI.
# The fixture stubs every adjacent seam and includes the current owner source, so
# the production objects are compile prerequisites only.  Linking those objects
# as well would create duplicate definitions instead of adding coverage.
test_cluster_undo_block0_current: test_cluster_undo_block0_current.c unit_test.h \
		$(CLUSTER_UNDO_BLOCK0_CURRENT_O) $(CLUSTER_UNDO_BLOCK0_O) \
		$(CLUSTER_UNDO_BLOCK0_RESIDENT_O) $(CLUSTER_GES_REPLY_WAIT_O) $(CLUSTER_GES_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Spec 8.4A P3 first publication: the fixture includes the real smgr source
# and runs typed final-path/no-replace behavior against a private temp dir.
test_cluster_undo_smgr_publication: test_cluster_undo_smgr_publication.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/storage/cluster_undo_smgr.c \
		$(top_srcdir)/src/include/cluster/cluster_undo_smgr.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-8.4 D13: these activation policy targets include the real
# dependency-light product source so static codec/FSM/lock-policy helpers are
# exercised without exporting a test hook.  Their only linked prerequisite
# producer is the exact spec-8.4a fixed-false block-zero snapshot.
CLUSTER_SEMANTIC_ACTIVATION_C = $(top_srcdir)/src/backend/cluster/cluster_semantic_activation.c
CLUSTER_SEMANTIC_ACTIVATION_H = $(top_srcdir)/src/include/cluster/cluster_semantic_activation.h

test_cluster_r4_activation_record: test_cluster_r4_activation_record.c unit_test.h \
		cluster_r4_activation_test_stubs.h \
		$(CLUSTER_SEMANTIC_ACTIVATION_C) $(CLUSTER_SEMANTIC_ACTIVATION_H) \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O) \
		$(top_builddir)/src/common/libpgcommon.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

CLUSTER_R4_OPEN_ROUTE_TEST_O = test_cluster_r4_open_route_product.o
$(CLUSTER_R4_OPEN_ROUTE_TEST_O): $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c \
		$(top_srcdir)/src/include/cluster/cluster_r4_observe.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -DCLUSTER_R4_ROUTE_POLICY_UNIT \
		-Dcluster_gcs_block_r4_requester_count=cluster_gcs_block_r4_requester_count_unused_test \
		-ffunction-sections -fdata-sections -c $< -o $@

test_cluster_r4_activation_fsm: test_cluster_r4_activation_fsm.c unit_test.h \
		cluster_r4_open_route_test_stubs.h $(CLUSTER_R4_OPEN_ROUTE_TEST_O) \
		$(CLUSTER_SEMANTIC_ACTIVATION_C) $(CLUSTER_SEMANTIC_ACTIVATION_H) \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_REPLACEMENT_EPISODE_O) \
		$(CLUSTER_REPLACEMENT_WIRE_O) $(CLUSTER_EPOCH_BALLOT_O) \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_REPLACEMENT_EPISODE_O) \
		$(CLUSTER_REPLACEMENT_WIRE_O) $(CLUSTER_EPOCH_BALLOT_O) \
		$(CLUSTER_UNDO_ROOT_DESCRIPTOR_O) $(CLUSTER_R4_OPEN_ROUTE_TEST_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a $(LDFLAGS) -lssl -lcrypto -o $@

# Stage 8 R4 D6 lifecycle/store RED tranche: link the real heap HOT lifecycle,
# index-handler ownership switch, executor buffer-slot implementation and heap
# tuple copy.  Function sections keep this intermediate tranche independent of
# unrelated backend duties; the real scratch-evaluator tranche is separate.
# USE_CLUSTER_UNIT exposes only the two backend-private product seams here.
CLUSTER_R4_HOT_HEAPAM_TEST_O = test_cluster_r4_hot_heapam_product.o
CLUSTER_R4_HOT_HANDLER_TEST_O = test_cluster_r4_hot_handler_product.o
CLUSTER_R4_HOT_VIS_TEST_O = test_cluster_r4_hot_visibility_product.o
CLUSTER_R4_EXEC_TUPLES_TEST_O = test_cluster_r4_exec_tuples_product.o
CLUSTER_R4_HEAPTUPLE_TEST_O = test_cluster_r4_heaptuple_product.o
CLUSTER_HOT_PRUNE_TEST_O = test_cluster_hot_prune_product.o
CLUSTER_HOT_BUFPAGE_TEST_O = test_cluster_hot_bufpage_product.o
CLUSTER_HOT_HORIZON_O = $(top_builddir)/src/backend/cluster/cluster_undo_horizon.o

$(CLUSTER_HOT_PRUNE_TEST_O): $(top_srcdir)/src/backend/access/heap/pruneheap.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

$(CLUSTER_HOT_BUFPAGE_TEST_O): $(top_srcdir)/src/backend/storage/page/bufpage.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

$(CLUSTER_R4_HOT_HEAPAM_TEST_O): $(top_srcdir)/src/backend/access/heap/heapam.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

$(CLUSTER_R4_HOT_HANDLER_TEST_O): $(top_srcdir)/src/backend/access/heap/heapam_handler.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT -ffunction-sections -fdata-sections \
		-c $< -o $@

$(CLUSTER_R4_HOT_VIS_TEST_O): $(top_srcdir)/src/backend/access/heap/heapam_visibility.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_CLUSTER_UNIT \
		-DCLUSTER_R4_VISIBILITY_EXTERNAL_HINT_STUB \
		-ffunction-sections -fdata-sections \
		-c $< -o $@

$(CLUSTER_R4_EXEC_TUPLES_TEST_O): $(top_srcdir)/src/backend/executor/execTuples.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

$(CLUSTER_R4_HEAPTUPLE_TEST_O): $(top_srcdir)/src/backend/access/common/heaptuple.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_heap_epq_consumer.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^heapam_tuple_lock\(/ { print "static TM_Result"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_fetch_consumer.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_fetch_internal\(/ { print "static bool"; emit=1; found++ } \
	     /^(heap_fetch|cluster_heap_fetch_waitable|cluster_heap_fetch_owned)\(/ { print "bool"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 4) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_dispatch.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_lock_tuple_internal\(/ { in_lock=1 } \
	     in_lock && !found && /if \(mode == LockTupleKeyShare/ { emit=1; found++ } \
	     emit { print } emit && /result = HeapTupleSatisfiesUpdateForWriter/ { emit=0; complete++ } \
	     /^}/ { in_lock=0 } END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_output.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_lock_tuple(_internal)?\(/ { in_lock=1 } \
	     in_lock && /^out_locked:/ { emit=1; found++ } \
	     emit && /if \(BufferIsValid\(vmbuffer\)\)/ { emit=0; done++ } \
	     emit { print } /^}/ { in_lock=0 } \
	     END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_reacquire.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_lock_tuple_internal\(/ { in_lock=1 } \
	     in_lock && !found && /follow_result = heap_lock_updated_tuple_authoritative/ { ready=1 } \
	     ready && /(LockBuffer\(\*buffer, BUFFER_LOCK_EXCLUSIVE\)|heap_lock_tuple_buffer\()/ { emit=1; found++ } \
	     emit && /if \(xmax_infomask_changed/ { emit=0; ready=0; done++ } \
	     emit { print } /^}/ { in_lock=0 } \
	     END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_rebind.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^cluster_heap_rebind_locked_tuple\(/ { print "void"; emit=1; found++ } \
	     /^heap_lock_tuple_buffer\(/ { print "static void"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 2) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_self.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_lock_tuple_internal\(/ { in_lock=1 } \
	     in_lock && /^[[:space:]]*cluster_xwait_remote = cluster_xwait_has_remote_evidence/ { print "#ifdef USE_PGRAC_CLUSTER"; emit=1; found++ } \
	     emit && /^[[:space:]]*require_sleep = true;/ { emit=0; done++ } \
	     emit { print } /^}/ { in_lock=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_strength.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^static const int MultiXactStatusLock/ { emit=1; found++ } \
	     emit { print } emit && /^};/ { emit=0; done++ } \
	     /^#define TUPLOCK_from_mxstatus/ { print; getline; print; macro++ } \
	     END { if (found != 1 || done != 1 || macro != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_scan_lifetime.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^(heapgetpage|heap_rescan)\(/ { print "void"; emit=1; found++ } \
	     /^(initscan|heapgettup_pagemode|heapgettup)\(/ { print "static void"; emit=1; found++ } \
	     /^(heapgettup_start_page|heapgettup_continue_page)\(/ { print "static Page"; emit=1; found++ } \
	     /^(heap_getnextslot|heap_getnextslot_tidrange)\(/ { print "bool"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 9) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_scan_consumers.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^(heapam_scan_bitmap_next_block|heapam_scan_bitmap_next_tuple|heapam_scan_sample_next_tuple|SampleHeapTupleVisible)\(/ { print "static bool"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 4) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_snapshot_consumer.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^heapam_tuple_satisfies_snapshot\(/ { print "static bool"; emit=1; found++ } \
	     emit { print } /^}/ { emit=0 } END { if (found != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_index_recheck.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^heapam_index_build_range_scan\(/ { in_scan=1 } \
	     in_scan && /^[[:space:]]*recheck:/ { ready=1 } \
	     ready && /LockBuffer\(hscan->rs_cbuf, BUFFER_LOCK_SHARE\)/ { emit=1; found++ } \
	     emit && /switch \(HeapTupleSatisfiesVacuum/ { emit=0; ready=0; done++ } \
	     emit { print } /^}/ { in_scan=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_index_recapture.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^heapam_index_build_range_scan\(/ { in_scan=1 } \
	     in_scan && /indexIt = tupleIsAlive = false/ { ready=1; next } \
	     ready && !emit && /^[[:space:]]*}/ { emit=1; found++; next } \
	     emit { print } emit && /LockBuffer\(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK\)/ { emit=0; ready=0; done++ } \
	     /^}/ { in_scan=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_copy_entry.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^heapam_relation_copy_for_cluster\(/ { in_copy=1 } \
	     in_copy && /^[[:space:]]*tuple = ExecFetchSlotHeapTuple/ { emit=1; found++ } \
	     emit && /switch \(HeapTupleSatisfiesVacuum/ { emit=0; done++ } \
	     emit { print } /^}/ { in_copy=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_copy_exit.inc: $(top_srcdir)/src/backend/access/heap/heapam_handler.c Makefile
	awk '/^heapam_relation_copy_for_cluster\(/ { in_copy=1 } \
	     in_copy && /isdead = false; \/\* keep compiler quiet/ { ready=1; next } \
	     ready && !emit && /^[[:space:]]*}/ { emit=1; found++; next } \
	     emit && /^[[:space:]]*if \(isdead\)/ { emit=0; ready=0; done++ } \
	     emit { print } /^}/ { in_copy=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_epq_wait: test_cluster_heap_epq_wait.c unit_test.h test_cluster_heap_epq_consumer.inc test_cluster_heap_fetch_consumer.inc test_cluster_heap_lock_dispatch.inc test_cluster_heap_lock_output.inc test_cluster_heap_lock_reacquire.inc test_cluster_heap_lock_rebind.inc test_cluster_heap_lock_self.inc test_cluster_heap_lock_strength.inc test_cluster_heap_scan_lifetime.inc test_cluster_heap_scan_consumers.inc test_cluster_heap_snapshot_consumer.inc test_cluster_heap_index_recheck.inc test_cluster_heap_index_recapture.inc test_cluster_heap_copy_entry.inc test_cluster_heap_copy_exit.inc $(CLUSTER_HOT_BUFPAGE_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(CLUSTER_HOT_BUFPAGE_TEST_O) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Compile the actual backend branch, not the unit-only reachability seam.
test_cluster_r4_hot_reachability.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_hot_r4_target_reachable\(void\)/ { print "static bool"; emit=1; found++ } \
	     emit { print } emit && /^}/ { emit=0; done++ } \
	     END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_r4_production_reachability: test_cluster_r4_production_reachability.c unit_test.h test_cluster_r4_hot_reachability.inc
	$(CC) $(CFLAGS) $(CPPFLAGS) $< -o $@

test_cluster_heap_update_temp_lock.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^cluster_heap_lock_update_predecessor\(/ { print "static TM_Result"; emit=1; found++ } \
	     emit { print } emit && /^}/ { emit=0; done++ } \
	     END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_update_temp_consumer.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_update\(/ { in_update=1 } in_update && /^\tif \(\(need_toast \|\| newtupsize > pagefree\) && !old_tuple_temp_locked/ { emit=1; found++ } \
	     emit && /^#endif/ { emit=0; done++ } emit { print } \
	     /^}/ { in_update=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_lock_return_receipt.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_lock_tuple_internal\(/ { in_lock=1 } \
	     in_lock && /^out_unlocked:/ { tail=1 } \
	     tail && /if \(undo_receipt.magic/ { print "#ifdef USE_PGRAC_CLUSTER"; emit=1; found++ } \
	     emit { print } emit && /^#endif/ { emit=0; tail=0 } \
	     /^}/ { in_lock=0 } END { if (found != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_update_successor_header.inc: $(top_srcdir)/src/backend/access/heap/heapam.c Makefile
	awk '/^heap_update\(/ { in_update=1 } \
	     in_update && /And also prepare an Xmax value for the new copy/ { planner=1 } \
	     planner && /^#ifdef USE_PGRAC_CLUSTER/ { emit=1; planner=0; found++ } \
	     emit { print } emit && /HeapTupleHeaderSetXmax\(newtup->t_data, xmax_new_tuple\)/ { emit=0; done++ } \
	     /^}/ { in_update=0 } END { if (found != 1 || done != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_heap_update_temp_lock: test_cluster_heap_update_temp_lock.c unit_test.h test_cluster_heap_update_temp_lock.inc test_cluster_heap_update_temp_consumer.inc test_cluster_heap_lock_return_receipt.inc test_cluster_heap_update_successor_header.inc
	$(CC) $(CFLAGS) $(CPPFLAGS) $< -o $@

test_cluster_r4_lock_order: test_cluster_r4_lock_order.c unit_test.h \
		cluster_r4_activation_test_stubs.h \
		$(CLUSTER_SEMANTIC_ACTIVATION_C) $(CLUSTER_SEMANTIC_ACTIVATION_H) \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_CR_APPLY_O) \
		$(CLUSTER_R4_HOT_HEAPAM_TEST_O) $(CLUSTER_R4_HOT_HANDLER_TEST_O) \
		$(CLUSTER_R4_HOT_VIS_TEST_O) $(CLUSTER_R4_EXEC_TUPLES_TEST_O) \
		$(CLUSTER_R4_HEAPTUPLE_TEST_O) $(CLUSTER_WRITER_CHAIN_O) $(CLUSTER_VIS_VERDICT_O) \
		$(CLUSTER_HOT_PRUNE_TEST_O) $(CLUSTER_HOT_BUFPAGE_TEST_O) $(CLUSTER_HOT_HORIZON_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_CR_APPLY_O) \
		$(CLUSTER_R4_HOT_HEAPAM_TEST_O) $(CLUSTER_R4_HOT_HANDLER_TEST_O) \
		$(CLUSTER_R4_HOT_VIS_TEST_O) $(CLUSTER_R4_EXEC_TUPLES_TEST_O) \
		$(CLUSTER_R4_HEAPTUPLE_TEST_O) $(CLUSTER_WRITER_CHAIN_O) $(CLUSTER_VIS_VERDICT_O) \
		$(CLUSTER_HOT_PRUNE_TEST_O) $(CLUSTER_HOT_BUFPAGE_TEST_O) $(CLUSTER_HOT_HORIZON_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Unit37 final receipt: execute the real exact-ref resolver body on its
# peer-origin memo-hit path.  All overlay, wire, native-CLOG and durable
# alternatives are fixture traps and must remain unreachable.
CLUSTER_R4_SCRATCH_RESOLVER_TEST_O = test_cluster_r4_scratch_resolver_product.o

$(CLUSTER_R4_SCRATCH_RESOLVER_TEST_O): \
		$(top_srcdir)/src/backend/cluster/cluster_visibility_resolve.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DCLUSTER_CTRC_UNIT_TEST=1 -c $< -o $@

test_cluster_r4_scratch_resolver: test_cluster_r4_scratch_resolver.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_tt_status_hint.c \
		$(CLUSTER_R4_SCRATCH_RESOLVER_TEST_O) $(CLUSTER_VIS_VERDICT_O) \
		$(CLUSTER_R4_HOT_VIS_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(CLUSTER_R4_SCRATCH_RESOLVER_TEST_O) $(CLUSTER_VIS_VERDICT_O) \
		$(CLUSTER_R4_HOT_VIS_TEST_O) \
		$(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-3.7 D12: test_cluster_undo_record — 12 encode/decode round-trip
# tests for UndoRecordHeader + 4 payloads + slot dir addressing.
test_cluster_undo_ctrc_product.o: $(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DCLUSTER_CTRC_UNIT_TEST=1 \
		-Dcluster_ctrc_receipt_prepare_shared=undo_ctrc_unavailable_prepare_shared \
		-Dcluster_ctrc_receipt_apply_shared=undo_ctrc_unavailable_apply_shared \
		-Dcluster_ctrc_receipt_retarget_itl_shared=undo_ctrc_unavailable_retarget_shared \
		-Dcluster_ctrc_receipt_cancel_shared=undo_ctrc_unavailable_cancel_shared \
		-c $< -o $@

test_cluster_undo_record: test_cluster_undo_record.c unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_undo_retention.c \
		test_cluster_undo_ctrc_product.o \
		$(CLUSTER_UBA_O) \
		../../../src/include/cluster/cluster_undo_record_api.h \
		../../../src/include/cluster/storage/cluster_undo_alloc.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RECORD_TEST_O) $(CLUSTER_UNDO_ALLOC_TEST_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DUNDO_RECORD_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_undo_record.c"' \
		-DUNDO_ALLOC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/storage/cluster_undo_alloc.c"' \
		-DHEAPAM_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/access/heap/heapam.c"' \
		-DUNDO_XLOG_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/storage/cluster_undo_xlog.h"' \
		-DUNDO_XLOG_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/storage/cluster_undo_xlog.c"' \
		-DTT_SLOT_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_tt_slot.h"' \
		$< test_cluster_undo_ctrc_product.o $(CLUSTER_UBA_O) \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_ALLOC_TEST_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a $(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@

# Link the actual heap final planner, not the USE_CLUSTER_UNIT boundary mocks.
test_cluster_heap_receipt_product.o: test_cluster_heap_receipt_product.c \
		$(top_srcdir)/src/backend/access/heap/heapam.c \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h \
		$(top_srcdir)/src/include/cluster/cluster_undo_record_api.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections -c $< -o $@

test_cluster_heap_receipt: test_cluster_heap_receipt.c test_cluster_undo_record.c \
		$(top_srcdir)/src/backend/cluster/cluster_undo_retention.c \
		$(CLUSTER_UBA_O) \
		test_cluster_heap_receipt_product.o $(CLUSTER_HOT_BUFPAGE_TEST_O) \
		test_cluster_undo_ctrc_product.o $(CLUSTER_VERSION_O) $(CLUSTER_UNDO_ALLOC_TEST_O) \
		$(top_srcdir)/src/backend/cluster/cluster_undo_record.c \
		$(top_srcdir)/src/include/cluster/cluster_undo_record_api.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DUNDO_RECORD_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_undo_record.c"' \
		-DUNDO_ALLOC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/storage/cluster_undo_alloc.c"' \
		-DHEAPAM_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/access/heap/heapam.c"' \
		-DUNDO_XLOG_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/storage/cluster_undo_xlog.h"' \
		-DUNDO_XLOG_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/storage/cluster_undo_xlog.c"' \
		-DTT_SLOT_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_tt_slot.h"' \
		$< test_cluster_heap_receipt_product.o $(CLUSTER_HOT_BUFPAGE_TEST_O) $(CLUSTER_UBA_O) \
		test_cluster_undo_ctrc_product.o $(CLUSTER_VERSION_O) $(CLUSTER_UNDO_ALLOC_TEST_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a $(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@

# CTRC focused contract: compile the real dependency-light state engine with
# section GC.  The test supplies only PostgreSQL runtime boundary stubs; all
# grant, receipt, close, ACK, certificate and release decisions are product
# code.  The source manifest is a test input, not a status authority.
CLUSTER_CTRC_TEST_O = test_cluster_terminal_ref_census_product.o

# Shared selectors/reply consumers must not use the dependency-light
# CLUSTER_CTRC_UNIT_TEST variant, which removes the real driver paths.
test_cluster_backend_types: $(top_srcdir)/src/include/miscadmin.h \
	$(top_srcdir)/src/include/storage/proc.h

test_cluster_undo_cleaner_holds.inc: $(top_srcdir)/src/backend/storage/buffer/bufmgr.c \
		$(top_srcdir)/src/backend/cluster/storage/cluster_undo_block0_current.c \
		$(top_srcdir)/src/backend/cluster/storage/cluster_undo_block0_resident.c \
		$(top_srcdir)/src/backend/cluster/cluster_semantic_activation.c Makefile
	awk '/^cluster_(buffer_backend_has_pins|undo_block0_current_backend_has_guards|undo_block0_backend_has_resources|semantic_activation_backend_has_admission)\(/ \
	     { print "bool"; emit=1; count++ } emit { print } /^}/ { emit=0 } \
	     END { if (count != 4) exit 1 }' $(filter %.c,$^) > $@.tmp
	mv $@.tmp $@

test_cluster_undo_cleaner_capacity: test_cluster_undo_cleaner_capacity.c test_cluster_undo_cleaner_holds.inc \
		$(top_srcdir)/src/backend/cluster/cluster_undo_cleaner.c \
		$(top_srcdir)/src/include/cluster/cluster_undo_cleaner.h \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h \
		$(top_srcdir)/src/include/miscadmin.h unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@
test_cluster_ctrc_run_pass.inc: $(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c Makefile
	awk '/^cluster_ctrc_cleaner_run_pass\(/ { print "bool"; emit=1; count++ } \
	     emit { print } /^}/ { emit=0 } END { if (count != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_ctrc_capacity: test_cluster_ctrc_capacity.c test_cluster_ctrc_cleaner.c test_cluster_ctrc_run_pass.inc \
		$(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h unit_test.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@
test_cluster_undo_cleaner: $(top_srcdir)/src/include/cluster/cluster_undo_cleaner.h \
	$(top_srcdir)/src/include/miscadmin.h

test_cluster_ctrc_cleaner: test_cluster_ctrc_cleaner.c test_cluster_ctrc_run_pass.inc unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@

# Whole production cleaner and exact-page guard, not a reconstructed predicate.
test_cluster_ctrc_itl_reuse.inc: $(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c Makefile
	awk '/^ctrc_cleaner_itl_page_exact\(/ { print "static bool"; emit=1; page++ } \
	     /^ctrc_cleaner_clean_itl_receipt\(/ { print "static bool"; emit=1; cleaner++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (page != 1 || cleaner != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_ctrc_itl_reuse: test_cluster_ctrc_itl_reuse.c test_cluster_ctrc_cleaner.c \
		test_cluster_ctrc_run_pass.inc test_cluster_ctrc_itl_reuse.inc unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@

# Verbatim actual dispatcher bodies, with the same fail-if-missing extraction
# discipline as the terminal-ingress fixture above. Only boundary dependencies
# are fixture doubles; shared participant/receipt mutations are production C.
test_cluster_ctrc_dispatch.inc: $(top_srcdir)/src/backend/cluster/cluster_gcs_block.c Makefile
	awk '/^gcs_ctrc_dispatch_prepare\(/ { print "static bool"; emit=1; prepare++ } \
	     /^gcs_ctrc_dispatch_local_certificates\(/ { print "static void"; emit=1; local++ } \
	     /^gcs_ctrc_dispatch_local_closes\(/ { print "static void"; emit=1; closes++ } \
	     /^cluster_gcs_ctrc_dispatch_close\(/ { print "bool"; emit=1; single++ } \
	     /^cluster_gcs_ctrc_dispatch_batch\(/ { print "void"; emit=1; batch++ } \
	     emit { print } /^}/ { emit=0 } \
	     END { if (prepare != 1 || local != 1 || closes != 1 || single != 1 || batch != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_ctrc_dispatch: test_cluster_ctrc_dispatch.c test_cluster_ctrc_cleaner.c test_cluster_ctrc_run_pass.inc \
		test_cluster_ctrc_dispatch.inc unit_test.h \
		$(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h \
		$(top_srcdir)/src/include/cluster/cluster_gcs_block.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections $< \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@

.PHONY: check_ctrc_source_census
check_ctrc_source_census:
	python3 $(top_srcdir)/src/test/cluster_unit/generate_ctrc_source_census.py \
		--check $(top_srcdir)/src/test/cluster_unit/ctrc_source_census.tsv

$(CLUSTER_CTRC_TEST_O): \
		$(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c \
		$(top_srcdir)/src/include/cluster/cluster_terminal_ref_census.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -ffunction-sections -fdata-sections \
		-DCLUSTER_CTRC_UNIT_TEST=1 \
		-c $< -o $@

test_cluster_ctrc_terminal_image.inc: $(top_srcdir)/src/backend/cluster/cluster_terminal_ref_census.c Makefile
	awk '/^ctrc_cleaner_clean_itl_receipt\(/ { in_cleaner=1 } \
	     in_cleaner && /^\tapply_result = cluster_ctrc_itl_cleanout_slot\(/ { emit=1; found++ } \
	     emit { print } emit && /^\tcleanout_lsn = GenericXLogFinish/ { emit=0; complete++ } \
	     /^}/ { in_cleaner=0 } END { if (found != 1 || complete != 1) exit 1 }' $< > $@.tmp
	mv $@.tmp $@

test_cluster_terminal_ref_census: test_cluster_terminal_ref_census.c test_cluster_ctrc_terminal_image.inc \
		ctrc_source_census.tsv heap_reference_producer_census.tsv \
		current_mx_cardinality_census.tsv unit_test.h $(CLUSTER_CTRC_TEST_O) \
		check_ctrc_source_census
	$(CC) $(CFLAGS) $(CPPFLAGS) \
		-DCTRC_SOURCE_MANIFEST_PATH='"$(abspath $(top_srcdir))/src/test/cluster_unit/ctrc_source_census.tsv"' \
		-DHEAP_REFERENCE_PRODUCER_MANIFEST_PATH='"$(abspath $(top_srcdir))/src/test/cluster_unit/heap_reference_producer_census.tsv"' \
		-DCURRENT_MX_CARDINALITY_MANIFEST_PATH='"$(abspath $(top_srcdir))/src/test/cluster_unit/current_mx_cardinality_census.tsv"' \
		-DPGRAC_SOURCE_ROOT_PATH='"$(abspath $(top_srcdir))"' \
		$< $(CLUSTER_CTRC_TEST_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a \
		$(CLUSTER_UNDO_TEST_GC_SECTIONS) -o $@


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

CLUSTER_WAL_STATE_O = $(top_builddir)/src/backend/cluster/cluster_wal_state.o

# RF A1: real common verified-CF/fresh-RMW engine with controlled CF and I/O
# stubs.  Custom linkage keeps it out of SIMPLE_TESTS.
test_cluster_wal_state_rmw: test_cluster_wal_state_rmw.c unit_test.h \
		../../../src/include/cluster/cluster_wal_state.h $(CLUSTER_VERSION_O) $(CLUSTER_WAL_STATE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_WAL_STATE_O) \
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
# spec-6.15 D5a adds the durable stripe record integrity truth tables
# ("PGXS"/"PGXA" CRC + sanity), which pull in libpgport for CRC32C.
# Links cluster_xid_stripe.o standalone; stubs ReadNextFullTransactionId.
CLUSTER_XID_STRIPE_O = $(top_builddir)/src/backend/cluster/cluster_xid_stripe.o
test_cluster_xid_stripe: test_cluster_xid_stripe.c unit_test.h \
		$(CLUSTER_XID_STRIPE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_XID_STRIPE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.22a D1: test_cluster_undo_resid — shared-undo block resource
# identity pure layer (undo resid class byte + encode/decode +
# owner-as-master routing + anti-ABA generation predicate).  Links
# cluster_hw.o for the is_undo cross-class check (real HW encoder).  The
# pure layer has no PG-backend dependencies, so nothing is stubbed beyond
# the Assert ExceptionalCondition hook.
CLUSTER_UNDO_RESID_O = $(top_builddir)/src/backend/cluster/cluster_undo_resid.o
test_cluster_undo_resid: test_cluster_undo_resid.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RESID_O) $(CLUSTER_HW_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RESID_O) $(CLUSTER_HW_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.22d D4-1: test_cluster_undo_authority — dead/absent-owner undo
# serve-authority election pure layer (deterministic lowest-fresh-survivor +
# fail-closed edges).  The pure decide() has no PG-backend / GRD / GUC
# dependencies, so it links standalone (nothing stubbed beyond the Assert
# ExceptionalCondition hook).  The live-snapshot wrapper (real GRD /
# membership / epoch reads) lives in cluster_undo_authority's heavy path and
# is forward-covered by the D4-8 TAP.
CLUSTER_UNDO_AUTHORITY_O = $(top_builddir)/src/backend/cluster/cluster_undo_authority.o
test_cluster_undo_authority: test_cluster_undo_authority.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_AUTHORITY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_AUTHORITY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.22e D5-1: test_cluster_undo_horizon — cluster-wide undo retention
# horizon pure fold (required-MEMBER report views -> recycle floor, U1-U16
# truth table: every unproven edge STALLS with attribution, never a local
# fallback).  Pure decision layer; scn_time_cmp is a byte-faithful local
# stub in the test (test_cluster_cr_tuple.c pattern) so cluster_scn.o's
# shmem/lwlock deps stay out.  The seqlock slot sampling, wire handler
# validation and the LMON tick live in the heavy path (D5-2/D5-3) and are
# covered by the t/370 TAP.
CLUSTER_UNDO_HORIZON_O = $(top_builddir)/src/backend/cluster/cluster_undo_horizon.o
test_cluster_undo_horizon: test_cluster_undo_horizon.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_HORIZON_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_HORIZON_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.22b D2-1/D2-3: test_cluster_undo_gcs — owner-as-master routing layer
# (D2-1: cluster_undo_block_lookup_master / _master_is_self) + the pure reader
# S-grant decision core (D2-3: cluster_undo_grant_armed / _admissible /
# _reader_pcm_*).  Links the D1 undo resid pure object, the D2 routing object,
# and the pure live-authority policy object (cluster_undo_grant_admissible
# reuses cluster_vis_live_authority_covers_policy); the test owns the
# cluster_node_id stub so the routing object links without the GUC layer.  The
# runtime acquire_shared wrapper (heavy fetch/epoch/smgr deps) lives in
# cluster_undo_gcs_grant.o, NOT here — it is forward-covered by the D2-7/D6 TAP.
CLUSTER_UNDO_GCS_O = $(top_builddir)/src/backend/cluster/cluster_undo_gcs.o
test_cluster_undo_gcs: test_cluster_undo_gcs.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RESID_O) $(CLUSTER_UNDO_GCS_O) \
		$(CLUSTER_RUNTIME_VIS_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RESID_O) $(CLUSTER_UNDO_GCS_O) \
		$(CLUSTER_RUNTIME_VIS_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-5.22c D3-1/D3-2: test_cluster_undo_verdict — cross-node xid->commit_scn
# verdict taxonomy (five-value ClusterUndoVerdictKind) + the two pure mapping
# helpers (wire ClusterGcsUndoVerdictPage -> taxonomy, block-proof
# ClusterVisTtProof -> taxonomy), plus the pure contract pins the D3-2 CP3
# wiring composes (D1 resid encode/master, D2 armed gate, the version-coverage
# policy).  All pure decision objects link standalone: the taxonomy mappers +
# covers policy live in the dependency-free runtime-visibility policy object,
# resid encode/master in the D1 object, the armed gate in the D2 routing
# object; the test owns the cluster_node_id stub the routing object needs.
# The read_scn admissibility of a COMMITTED_BOUND is the D3-3 consumer's, and
# the S-grant / verdict orchestration wiring is backend-heavy and
# forward-covered by the D3-7 / D6 TAP.
test_cluster_undo_verdict: test_cluster_undo_verdict.c unit_test.h \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RESID_O) $(CLUSTER_UNDO_GCS_O) \
		$(CLUSTER_RUNTIME_VIS_POLICY_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(CLUSTER_UNDO_RESID_O) $(CLUSTER_UNDO_GCS_O) \
		$(CLUSTER_RUNTIME_VIS_POLICY_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-7.1 D3-a: test_cluster_mxid_stripe — truth tables for the mxid
# stripe pure derivation layer (half-space origin derivation / striped
# candidate arithmetic incl. 2^32 wrap + InvalidMultiXactId skip /
# half-space guardrail predicate / runtime latch wrappers / "PGXM"
# activation-slot extension record integrity, which pulls in libpgport
# for CRC32C).  Links cluster_mxid_stripe.o standalone; stubs the boot
# lazy latch to a no-op.
CLUSTER_MXID_STRIPE_O = $(top_builddir)/src/backend/cluster/cluster_mxid_stripe.o
test_cluster_mxid_stripe: test_cluster_mxid_stripe.c unit_test.h \
		$(CLUSTER_MXID_STRIPE_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_MXID_STRIPE_O) \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# spec-7.1 D3-b (A1): test_cluster_multixact_served — visibility truth table
# for the pure served-verdict combination resolver.  Links
# cluster_multixact_served.o standalone; the test provides a local scn_time_cmp
# stub (as test_cluster_visibility_decide_scn) so cluster_scn.o's PG-core deps
# are not dragged in.
CLUSTER_MULTIXACT_SERVED_O = $(top_builddir)/src/backend/cluster/cluster_multixact_served.o
test_cluster_multixact_served: test_cluster_multixact_served.c unit_test.h \
		$(CLUSTER_MULTIXACT_SERVED_O) $(CLUSTER_VIS_VERDICT_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_MULTIXACT_SERVED_O) \
		$(CLUSTER_VIS_VERDICT_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

# Executable U1-U12 refusal choreography.  The test links the
# dependency-light sequencer that bufmgr.c consumes; no production source is
# opened or scanned by the test process.
BUFMGR_BARRIER_O = $(top_builddir)/src/backend/storage/buffer/bufmgr_barrier.o
test_cluster_share_barrier: test_cluster_share_barrier.c unit_test.h \
		$(CLUSTER_VERSION_O) $(BUFMGR_BARRIER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(BUFMGR_BARRIER_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@

TABLEAM_BARRIER_O = $(top_builddir)/src/backend/access/table/tableam_barrier.o
test_cluster_heap_barrier: test_cluster_heap_barrier.c unit_test.h \
		$(CLUSTER_VERSION_O) $(TABLEAM_BARRIER_O)
	$(CC) $(CFLAGS) $(CPPFLAGS) $< \
		$(CLUSTER_VERSION_O) $(TABLEAM_BARRIER_O) \
		$(top_builddir)/src/common/libpgcommon_srv.a \
		$(top_builddir)/src/port/libpgport_srv.a -o $@
