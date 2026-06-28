#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for the pgrac cluster subsystem (stage 0.2 skeleton)
#
# IDENTIFICATION
#    src/backend/cluster/Makefile
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original Makefile (no derivation from PostgreSQL).
#    Stage 0.2 kept OBJS minimal; stage 1.1 adds the first subsystem
#    subdirectory storage/ (cluster_shared_fs vtable + 2 built-in
#    backends).  Future subdirectories (grd/, gcs/, ges/, scn/, ...)
#    will be added in later feature points the same way.  Each
#    subdirectory's .o files are listed individually under OBJS rather
#    than via SUBDIRS so the disable-cluster mode can omit them as a
#    block (spec-0.3 binary-symbol contract).
#
#-------------------------------------------------------------------------

subdir = src/backend/cluster
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# In --enable-cluster builds we compile every cluster source file.  In
# --disable-cluster builds we still need cluster_views.o so that
# pg_proc.dat's reference to cluster_get_wait_events resolves at link
# time (the SRF body itself is gated by #ifdef USE_PGRAC_CLUSTER and
# returns an empty result set in disable mode).  All other cluster_*.o
# objects are skipped so that the disable-mode postgres binary remains
# symbol-equivalent to vanilla PG -- the spec-0.3 contract verified by
# the CI "Verify postgres binary has NO cluster symbols" step.
ifeq ($(enable_pgrac_cluster), yes)
OBJS = \
	cluster.o \
	cluster_advisory.o \
	cluster_cancel_token.o \
	cluster_cf_authority.o \
	cluster_cf_enqueue.o \
	cluster_cf_phase2.o \
	cluster_cf_stats.o \
	cluster_cf_storage.o \
	cluster_clean_leave.o \
	cluster_clean_leave_policy.o \
	cluster_clean_leave_views.o \
	cluster_conf.o \
	cluster_cssd.o \
	cluster_debug.o \
	cluster_diag.o \
	cluster_elog.o \
	cluster_epoch.o \
	cluster_extend_gate.o \
	cluster_fence.o \
	cluster_write_fence.o \
	cluster_ges.o \
	cluster_ges_dedup.o \
	cluster_ges_mode.o \
	cluster_ges_mode_backend.o \
	cluster_ges_reply_wait.o \
	cluster_grd.o \
	cluster_grd_outbound.o \
	cluster_grd_pending.o \
	cluster_grd_srf.o \
	cluster_grd_work_queue.o \
	cluster_gcs.o \
	cluster_gcs_block.o \
	cluster_gcs_block_dedup.o \
	cluster_reconfig.o \
	cluster_recovery_plan.o \
	cluster_recovery_worker.o \
	cluster_recovery_merge.o \
	cluster_block_apply.o \
	cluster_block_apply_heap.o \
	cluster_block_apply_srf.o \
	cluster_block_recovery.o \
	cluster_block_recovery_srf.o \
	cluster_thread_recovery_apply.o \
	cluster_thread_recovery_apply_srf.o \
	cluster_thread_recovery_replay.o \
	cluster_thread_recovery_replay_srf.o \
	cluster_thread_recovery_driver.o \
	cluster_thread_recovery_driver_srf.o \
	cluster_thread_recovery_orchestrator.o \
	cluster_thread_recovery_orchestrator_srf.o \
	cluster_thread_recovery_worker.o \
	cluster_remote_xact.o \
	cluster_dl.o \
	cluster_dl_lock.o \
	cluster_ir.o \
	cluster_ir_lock.o \
	cluster_ir_srf.o \
	cluster_ts.o \
	cluster_ts_lock.o \
	cluster_ts_srf.o \
	cluster_ko.o \
	cluster_ko_lock.o \
	cluster_ko_srf.o \
	cluster_guc.o \
	cluster_hang.o \
	cluster_hang_policy.o \
	cluster_hang_resolve.o \
	cluster_hang_resolve_policy.o \
	cluster_touched_peers.o \
	cluster_hw.o \
	cluster_hw_shmem.o \
	cluster_hw_ic.o \
	cluster_hw_remaster.o \
	cluster_hw_snapshot.o \
	cluster_ic.o \
	cluster_ic_chunk.o \
	cluster_ic_envelope.o \
	cluster_ic_msg_types_srf.o \
	cluster_ic_router.o \
	cluster_ic_tier1.o \
	cluster_inject.o \
	cluster_lck.o \
	cluster_lock_acquire.o \
	cluster_native_lock_probe.o \
	cluster_lmd.o \
	cluster_lmd_graph.o \
	cluster_lmd_probe_collector.o \
	cluster_lmd_srf.o \
	cluster_lmd_tarjan.o \
	cluster_lmd_wait_state.o \
	cluster_lmon.o \
	cluster_lms.o \
	cluster_membership.o \
	cluster_node_remove.o \
	cluster_node_remove_policy.o \
	cluster_node_remove_views.o \
	cluster_pcm_lock.o \
	cluster_pgstat.o \
	cluster_quorum_decision.o \
	cluster_qvotec.o \
	cluster_scn.o \
	cluster_sequence.o \
	cluster_sequence_shmem.o \
	cluster_voting_disk_io.o \
	cluster_shmem.o \
	cluster_signal.o \
	cluster_sinval.o \
	cluster_sinval_bcast.o \
	cluster_startup_phase.o \
	cluster_itl.o \
	cluster_itl_cleanout.o \
	cluster_itl_touch.o \
	cluster_uba.o \
	cluster_undo_cleaner.o \
	cluster_visibility_resolve.o \
	cluster_visibility_verdict.o \
	cluster_undo_record.o \
	cluster_undo_retention.o \
	cluster_undo_srf.o \
	cluster_cr.o \
	cluster_cr_apply.o \
	cluster_cr_cache.o \
	cluster_cr_pool.o \
	cluster_cr_admit.o \
	cluster_cr_admit_stat.o \
	cluster_cr_tuple.o \
	cluster_cr_tuple_stat.o \
	cluster_cr_coordinator_stat.o \
	cluster_cr_srf.o \
	cluster_resolver_cache.o \
	cluster_tt_slot.o \
	cluster_tt_2pc.o \
	cluster_tt_2pc_record.o \
	cluster_tt_durable.o \
	cluster_tt_durable_stat.o \
	cluster_tt_recovery.o \
	cluster_tt_local.o \
	cluster_tt_status.o \
	cluster_tt_status_hint.o \
	cluster_tx_enqueue.o \
	cluster_subtrans.o \
	cluster_multixact.o \
	cluster_visibility_inject.o \
	cluster_stats.o \
	cluster_version.o \
	cluster_views.o \
	cluster_wal_state.o \
	cluster_wal_thread.o \
	storage/cluster_shared_fs.o \
	storage/cluster_shared_fs_stub.o \
	storage/cluster_shared_fs_local.o \
	storage/cluster_shared_fs_sharedfs.o \
	storage/cluster_smgr.o \
	storage/cluster_undo_alloc.o \
	storage/cluster_undo_buf.o \
	storage/cluster_undo_smgr.o \
	storage/cluster_undo_xlog.o
else
# cluster_conf.o, cluster_debug.o, cluster_views.o, cluster_ic.o,
# cluster_inject.o, cluster_pgstat.o, cluster_scn.o are always linked
# because pg_proc.dat references their SRF entry points
# unconditionally.  In disable-cluster builds the SRF bodies #ifdef out
# and raise ERRCODE_FEATURE_NOT_SUPPORTED, but the function symbols
# must still resolve at link time.  storage/ subdirectory is omitted
# entirely in disable mode because nothing in pg_proc.dat references
# it (spec-1.1 does not introduce a SRF; pg_cluster_state extension
# lives in cluster_debug.c which is always linked).
OBJS = cluster_conf.o cluster_debug.o cluster_ic.o cluster_inject.o cluster_undo_srf.o \
	cluster_cr_srf.o cluster_block_apply_srf.o cluster_block_recovery_srf.o cluster_thread_recovery_apply_srf.o cluster_thread_recovery_replay_srf.o cluster_thread_recovery_driver_srf.o cluster_thread_recovery_orchestrator_srf.o cluster_pgstat.o cluster_scn.o cluster_views.o cluster_ges_mode_backend.o \
	cluster_ir_srf.o cluster_ts_srf.o cluster_ko_srf.o \
	cluster_hang_resolve.o cluster_clean_leave_views.o cluster_node_remove_views.o
# spec-5.12: cluster_hang_resolve.o provides the pg_cluster_hang_victims /
# pg_cluster_hang_resolve SQL symbols (real bodies #ifdef USE_PGRAC_CLUSTER,
# --disable-cluster stubs raise ERRCODE_FEATURE_NOT_SUPPORTED); the symbols
# must resolve at link time in both build modes.
# spec-5.13: cluster_clean_leave_views.o provides the cluster_get_clean_leave_state
# SRF + pg_cluster_clean_leave_request UDF symbols, same unconditional-link reason.
# spec-5.18: cluster_node_remove_views.o provides cluster_get_node_removal_state +
# pg_cluster_remove_node, same unconditional-link reason.
endif

include $(top_srcdir)/src/backend/common.mk
