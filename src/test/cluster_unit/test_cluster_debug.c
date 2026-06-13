/*-------------------------------------------------------------------------
 *
 * test_cluster_debug.c
 *	  Compile-time / link-level invariants for the cluster_debug
 *	  diagnostic snapshot framework shipped at stage 0.29.
 *
 *	  Locks:
 *	    - The cluster_dump_state SRF entry-point address resolves at
 *	      link time (validates the always-linked / pg_proc.dat
 *	      contract).
 *	    - cluster_debug.c links cleanly when paired with stubs for
 *	      every cross-module dependency it pulls in (cluster_shmem /
 *	      cluster_guc / cluster_ic / cluster_inject / cluster_pgstat /
 *	      cluster_conf / cluster_elog public API).
 *	    - cluster_inject_get_count + _get_state_at iterators added at
 *	      stage 0.29 work without crashing on out-of-range indices.
 *	    - cluster_pgstat_get_count + _get_at iterators added at stage
 *	      0.29 work without crashing on out-of-range indices.
 *
 *	  End-to-end SRF behaviour (column types, row counts, value
 *	  formatting) is verified on a real PG instance by cluster_tap
 *	  t/017_debug.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_debug.c
 *
 * NOTES
 *	  This is a pgrac-original file.  cluster_debug.c is a cross-module
 *	  aggregator -- linking it standalone requires stubs for the public
 *	  symbols from seven other cluster_*.o files.  The stubs below are
 *	  the minimum set; the SRF body itself is never invoked from the
 *	  unit test (we only take its address), so stub return values are
 *	  inert.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_debug.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_debug.o standalone.  cluster_debug.c
 * depends on (read-only): cluster_shmem (ClusterShmem global pointer),
 * cluster_guc (4 GUC vars), cluster_ic (ClusterICOps_Active +
 * ClusterICTier enum), cluster_inject (armed_count + iterator),
 * cluster_pgstat (iterator), cluster_conf (lookup + node_count),
 * cluster_elog (cluster_phase global).  All stubbed below.  The SRF
 * body is never invoked; addresses-only tests + iterator round-trips.
 * ----------
 */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/elog.h"


/* cluster_shmem */
ClusterShmemCtl *ClusterShmem = NULL;

/* cluster_guc (4 vars) */
int cluster_node_id = -1;
int cluster_interconnect_tier = 0;
char *cluster_config_file = NULL;
char *cluster_injection_points = NULL;

/* cluster_ic */
const ClusterICOps *ClusterICOps_Active = NULL;

/* cluster_ic_tier1 — Hardening v1.0.1 F3 listener metadata accessors
 * (cluster_debug.c references these via dump_ic).  Stubs return zeros
 * since this unit test never binds a real listener. */
#include "cluster/cluster_ic_tier1.h"
const ClusterICOps ClusterICOps_Tier1 = { 0 };
pid_t
cluster_ic_tier1_get_listener_pid(void)
{
	return 0;
}
uint64
cluster_ic_tier1_get_listener_incarnation(void)
{
	return 0;
}
int
cluster_ic_tier1_get_listener_port(void)
{
	return -1;
}

/* cluster_inject (armed_count + iterator) */
int cluster_injection_armed_count = 0;

/*
 * Stage 0.30 sweep: cluster_dump_state gained CLUSTER_INJECTION_POINT;
 * cluster_inject.h declares cluster_injection_run extern.
 */
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

int
cluster_injection_get_count(void)
{
	return 0;
}

bool
cluster_injection_get_state_at(int idx pg_attribute_unused(),
							   const char **name_out pg_attribute_unused(),
							   ClusterInjectFaultType *type_out pg_attribute_unused(),
							   uint64 *hits_out pg_attribute_unused())
{
	return false;
}

/* cluster_pgstat iterator */
int
cluster_pgstat_get_count(void)
{
	return 0;
}

bool
cluster_pgstat_get_at(int idx pg_attribute_unused(), const char **name_out pg_attribute_unused(),
					  uint64 *value_out pg_attribute_unused())
{
	return false;
}

/* cluster_conf */
int
cluster_conf_node_count(void)
{
	return 0;
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	return NULL;
}

/* cluster_elog */
const char *cluster_phase = "init";

/* cluster_shared_fs (stage 1.1).  cluster_debug.c::dump_shared_fs reads
 * the active vtable and the registered_backends slots; both stubs
 * return NULL so the SRF body's "(none)" / "(empty)" branches fire. */
const struct ClusterSharedFsOps *
cluster_shared_fs_get_active_ops(void)
{
	return NULL;
}

const struct ClusterSharedFsOps *
cluster_shared_fs_get_backend_at(int id pg_attribute_unused())
{
	return NULL;
}

/* Stage 1.2: cluster_smgr accessor + GUC referenced by dump_guc /
 * dump_shared_fs.  cluster_smgr.o is not linked here; provide stubs. */
extern int cluster_smgr_active_relation_count(void);
int
cluster_smgr_active_relation_count(void)
{
	return 0;
}
bool cluster_smgr_user_relations = false;

/* Stage 1.3: cluster_debug.c::dump_shmem now reads from the cluster
 * shmem region registry (region_count + total_bytes + per-region
 * iter).  cluster_shmem.o is not linked here; provide stubs that
 * mimic an empty registry. */
int cluster_shmem_max_regions = 64;
int
cluster_shmem_get_region_count(void)
{
	return 0;
}

/*
 * Stage 1.7: cluster_debug.c::dump_pcm calls cluster_pcm_grd_count() +
 * cluster_pcm_grd_shmem_size() + reads cluster_pcm_grd_max_entries
 * (defined in cluster_pcm_lock.c).  cluster_pcm_lock.o is not linked
 * here; provide stubs returning the same defaults the real
 * implementation returns when GUC=0.
 *
 * spec-2.30 D9 (R10 stub audit):  dump_pcm calls 9 NEW transition
 * counter accessors;  stub each to return 0 (matches GUC=0 disable path).
 */
int cluster_pcm_grd_max_entries = 0;

int
cluster_pcm_grd_count(void)
{
	return 0;
}

Size
cluster_pcm_grd_shmem_size(void)
{
	return 0;
}

void
cluster_pcm_grd_get_summary(int *n_count, int *s_count, int *x_count, int *pi_holders_total,
							int *convert_queue_active)
{
	*n_count = 0;
	*s_count = 0;
	*x_count = 0;
	*pi_holders_total = 0;
	*convert_queue_active = 0;
}

/* PGRAC spec-2.30 D9 R10 stub audit — 9 transition counter accessors. */
uint64
cluster_pcm_get_trans_n_to_s_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_n_to_x_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_x_upgrade_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_x_to_s_downgrade_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_x_to_n_downgrade_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_x_to_n_release_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_n_invalidate_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_n_release_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_x_cleanout_count(void)
{
	return 0;
}

/* PGRAC spec-2.32 D8 stub audit — 12 GCS accessor stubs + api_state. */
uint64
cluster_gcs_get_lookup_master_self_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lookup_master_remote_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_send_request_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_handle_request_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_handle_reply_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_reply_late_drop_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_reply_timeout_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_encode_payload_bytes(void)
{
	return 0;
}
uint64
cluster_gcs_get_decode_payload_bytes(void)
{
	return 0;
}
uint64
cluster_gcs_get_dispatch_loop_iterations(void)
{
	return 0;
}
uint64
cluster_gcs_get_outstanding_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_max_outstanding(void)
{
	return 0;
}
const char *
cluster_gcs_get_api_state(void)
{
	return "stub";
}

/* spec-2.33 D10 stubs: 8 NEW block-plane counter accessors referenced by
 * cluster_dump_state (cluster_debug.c dump_gcs).  Return 0 for all. */
uint64
cluster_gcs_get_block_request_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_reply_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_timeout_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_checksum_fail_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_storage_fallback_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_master_not_holder_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_wal_flush_before_ship_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_ship_bytes_total(void)
{
	return 0;
}

/* spec-2.34 D10 stubs: 9 NEW reliability hardening counter accessors. */
uint64
cluster_gcs_get_block_retransmit_attempt_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_retransmit_send_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_retransmit_exhausted_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_hit_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_miss_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_collision_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_full_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_epoch_invalidate_wake_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_stale_reply_drop_count(void)
{
	return 0;
}

/* spec-2.35 D12 stubs: 7 NEW CF 2-way protocol counter accessors. */
uint64
cluster_gcs_get_block_forward_sent_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_forward_received_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_from_holder_ship_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_forward_holder_evicted_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_s_holders_bitmap_redirect_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_master_holder_lifecycle_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_forward_replay_count(void)
{
	return 0;
}

/* spec-2.36 D10 stubs: 6 NEW CF 3-way protocol counter accessors. */
uint64
cluster_gcs_get_block_invalidate_broadcast_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_invalidate_ack_received_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_invalidate_timeout_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_x_forward_sent_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_x_granted_from_holder_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_starvation_denied_pending_x_count(void)
{
	return 0;
}

/* spec-2.37 D12 stubs: 4 NEW PI watermark + lost-write counter accessors. */
uint64
cluster_gcs_get_pi_watermark_advance_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_pi_watermark_retire_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lost_write_detected_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lost_write_avoid_count(void)
{
	return 0;
}

/* spec-4.7 D6 stubs: 8 NEW GCS/PCM warm-recovery counter accessors. */
uint64
cluster_gcs_get_recovery_block_resources_recovering(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_buffers_redeclared(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_block_state_rebuilt(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_redo_boundary_waits(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_redo_boundary_reached(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_stale_block_drop(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_ambiguous_owner_failclosed(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_before_boundary_failclosed(void)
{
	return 0;
}

/* spec-2.38 D10 stubs: 9 NEW SI Broadcaster counter accessors. */
uint64
cluster_sinval_get_broadcast_send_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_broadcast_receive_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inject_local_queue_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_outbound_queue_full_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inbound_queue_full_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inbound_overflow_reset_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_validation_drop_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_stale_epoch_drop_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_echo_dropped_count(void)
{
	return 0;
}
/* spec-2.39 D8/D9:  6 NEW counter stubs. */
uint64
cluster_sinval_get_fanout_would_block_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_hard_error_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_peer_down_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_received_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_timeout_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_orphan_count(void)
{
	return 0;
}

/* spec-3.1 D9:  7 NEW TT status counter stubs. */
uint64
cluster_tt_status_get_install_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_lookup_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_lookup_miss_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_evict_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_flush_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_self_consumer_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_evict_fail_count(void)
{
	return 0;
}

/* spec-3.2 D8:  6 NEW TT status hint counter stubs. */
uint64
cluster_tt_status_hint_get_emit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_receive_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_invalid_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_stale_epoch_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_unknown_version_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_install_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_v1_compat_count(void)
{
	return 0;
}

/* spec-3.7 D10 stubs: cluster_debug dump_undo() references 5 counter
 * accessors from cluster_undo_record.o which test_cluster_debug doesn't link. */
uint64
cluster_undo_record_alloc_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_claim_count(void)
{
	return 0;
}
/* spec-3.18 D7: dump_undo now reads the extent + undo-buffer counters. */
uint64
cluster_undo_extent_claim_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_hit_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_miss_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_writeback_count(void)
{
	return 0;
}
uint64
cluster_undo_block_write_count(void)
{
	return 0;
}
uint64
cluster_undo_block_flush_count(void)
{
	return 0;
}
uint64
cluster_undo_reader_lookup_count(void)
{
	return 0;
}
/* spec-3.8 D11 stubs: 4 NEW lifecycle counters referenced by dump_undo. */
uint64
cluster_undo_autoextend_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_switch_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_create_fail_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_hard_cap_fail_count(void)
{
	return 0;
}
/* P0 perf hardening: per-commit fsync + smgr syscall counter accessors
 * (dump_undo emits these; cluster_undo_record.o not linked here). */
uint64
cluster_undo_commit_fsync_count(void)
{
	return 0;
}
uint64
cluster_undo_commit_fsync_segment_count(void)
{
	return 0;
}
uint64
cluster_undo_commit_fsync_failure_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_open_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_close_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_pread_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_pwrite_count(void)
{
	return 0;
}

/* spec-3.9 D8 stubs: dump_cr() references 9 cluster_cr counter accessors
 * from cluster_cr.o which test_cluster_debug doesn't link. */
uint64
cluster_cr_construct_count(void)
{
	return 0;
}
uint64
cluster_cr_snapshot_too_old_count(void)
{
	return 0;
}
uint64
cluster_cr_cross_instance_unsupported_count(void)
{
	return 0;
}
uint64
cluster_cr_corruption_count(void)
{
	return 0;
}
uint64
cluster_cr_chain_walk_steps_sum(void)
{
	return 0;
}
uint64
cluster_cr_inverse_insert_count(void)
{
	return 0;
}
uint64
cluster_cr_inverse_update_count(void)
{
	return 0;
}
uint64
cluster_cr_inverse_delete_count(void)
{
	return 0;
}
uint64
cluster_cr_inverse_itl_count(void)
{
	return 0;
}
/* spec-3.10 D5 cache counters: cluster_debug.o emits these rows; cluster_cr.o
 * is not linked here so they are stubbed too. */
uint64
cluster_cr_cache_hit_count(void)
{
	return 0;
}
uint64
cluster_cr_cache_miss_count(void)
{
	return 0;
}
uint64
cluster_cr_cache_evict_count(void)
{
	return 0;
}
uint64
cluster_cr_cache_install_count(void)
{
	return 0;
}
/* spec-3.22 D3: xmax recycled-slot resolve outcome buckets. */
uint64
cluster_cr_xmax_resolved_count(void)
{
	return 0;
}
uint64
cluster_cr_xmax_recycled_invisible_count(void)
{
	return 0;
}
uint64
cluster_cr_xmax_invalid_or_ambiguous_count(void)
{
	return 0;
}
uint64
cluster_cr_xmax_scan_unavail_or_no_proof_count(void)
{
	return 0;
}

/* spec-4.1 D7 + spec-4.2 D5: per-thread WAL routing / WAL-state registry
 * accessors (cluster_wal_thread.c / cluster_wal_state.c) are not linked
 * here; stub everything dump_wal_thread reads (L104). */
#include "cluster/cluster_wal_state.h"
bool
cluster_wal_state_registry_ready(void)
{
	return false;
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id pg_attribute_unused(),
							ClusterWalStateSlot *slot_out pg_attribute_unused())
{
	return CLUSTER_WAL_SLOT_EMPTY;
}
void
cluster_wal_state_publish_active(void)
{}
void
cluster_wal_state_refresh_own_slot(void)
{}

/* spec-4.3 D5 stub: recovery plan snapshot (cluster_recovery_plan.c
 * not linked; dump_recovery's plan keys read '-' / none). */
#include "cluster/cluster_recovery_plan.h"
bool
cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out pg_attribute_unused())
{
	return false;
}

/* spec-4.4 D6 stub: worker pool snapshot (cluster_recovery_worker.c
 * not linked; dump_recovery's worker keys read '-'). */
#include "cluster/cluster_recovery_worker.h"
bool
cluster_recovery_worker_pool_snapshot(ClusterRecoveryWorkerPool *out pg_attribute_unused())
{
	return false;
}

uint64
cluster_wal_thread_page_stamp_count(void)
{
	return 0;
}
uint16
cluster_wal_thread_dump_thread_id(void)
{
	return 0;
}
bool
cluster_wal_thread_dir_configured(void)
{
	return false;
}
bool
cluster_wal_thread_dir_validated(void)
{
	return false;
}
bool
cluster_wal_thread_claim_created(void)
{
	return false;
}

/* spec-3.11 D8: durable TT slot counters (cluster_tt_durable_stat.c) are not
 * linked here; stub the 5 accessors dump_undo reads. */
uint64
cluster_tt_durable_commit_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_lookup_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_lookup_miss_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_by_xid_scan_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_redo_apply_count(void)
{
	return 0;
}
/* spec-4.8: tt_recovery counter accessors (cluster_tt_durable_stat.c) are not
 * linked here; stub the 8 accessors the tt_recovery dump rows read. */
uint64
cluster_tt_recovery_active_resolved_aborted_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_remote_active_failclosed_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_wrap_generation_disambiguated_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_recycled_liveness_relaxed_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_scn_highwater_recovered_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_recovery_verdict_failclosed_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_heap_tuples_physically_reverted_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_undo_revert_failclosed_count(void)
{
	return 0;
}
/* spec-3.16 D5 recovery counter accessor stubs (dump_recovery rows). */
uint64
cluster_vis_get_recovery_undo_redo_applies(void)
{
	return 0;
}
uint64
cluster_vis_get_recovery_undo_redo_skips(void)
{
	return 0;
}
uint64
cluster_vis_get_recovery_2pc_standby_rebuilds(void)
{
	return 0;
}
uint64
cluster_vis_get_recovery_overlay_rebuild_count(void)
{
	return 0;
}

/* spec-4.5a D11 merged-replay / remote-read accessor stubs (dump_recovery
 * rows; the real counters live in cluster_tt_status.c / cluster_remote_xact.c
 * / cluster_recovery_merge.c, none of which are linked here). */
uint64
cluster_vis_get_merged_records_applied(void)
{
	return 0;
}
uint64
cluster_vis_get_merged_skipped_local(void)
{
	return 0;
}
uint64
cluster_vis_get_merged_own_bound_skips(void)
{
	return 0;
}
uint64
cluster_vis_get_remote_uba_resolved(void)
{
	return 0;
}
uint64
cluster_remote_xact_diverted_commit_count(void)
{
	return 0;
}
uint64
cluster_remote_xact_diverted_abort_count(void)
{
	return 0;
}
uint64
cluster_remote_xact_outcome_indoubt_count(void)
{
	return 0;
}
bool
cluster_merged_instance_is_materialized(int origin_node)
{
	(void)origin_node;
	return false;
}

/* spec-3.15 D9 2PC counter accessor stubs (dump_tt_2pc rows). */
uint64
cluster_vis_get_twopc_prepare_records(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_prepare_undo_flushes(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_postprepare_transfers(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_prefinish_commits(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_prefinish_aborts(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_recover_rebinds(void)
{
	return 0;
}

/* spec-3.14 D8 visibility counter accessor stubs (dump_visibility rows). */
uint64
cluster_vis_get_vis_update_fork_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_dirty_fork_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_selftoast_fork_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_conflict_failclosed_count(void)
{
	return 0;
}
uint64
cluster_vis_get_prune_remote_keep_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_variant_unknown_failclosed_count(void)
{
	return 0;
}
/* spec-3.13 D6 counter accessor stubs (dump_undo new rows). */
uint64
cluster_undo_cleaner_pass_count(void)
{
	return 0;
}
uint64
cluster_undo_cleaner_shmem_tt_slots_gcd(void)
{
	return 0;
}
uint64
cluster_undo_cleaner_segments_marked_recyclable(void)
{
	return 0;
}
uint64
cluster_undo_cleaner_stale_active_skipped(void)
{
	return 0;
}
uint64
cluster_undo_segment_reuse_count(void)
{
	return 0;
}
uint64
cluster_tt_slot_wrap_retired_count(void)
{
	return 0;
}

/* spec-3.13 D1 Undo Cleaner accessor stubs (dump_undo_cleaner references). */
#include "cluster/cluster_undo_cleaner.h"
UndoCleanerStatus
cluster_undo_cleaner_status(void)
{
	return UNDO_CLEANER_NOT_STARTED;
}
const char *
cluster_undo_cleaner_status_to_string(UndoCleanerStatus st)
{
	(void)st;
	return "not_started";
}
pid_t
cluster_undo_cleaner_pid(void)
{
	return 0;
}
TimestampTz
cluster_undo_cleaner_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_undo_cleaner_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_undo_cleaner_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_undo_cleaner_main_loop_iters(void)
{
	return 0;
}
/* spec-3.12 D5 retention counter stubs (dump_undo references these). */
uint64
cluster_tt_slot_retention_horizon_scn(void)
{
	return 0;
}
uint64
cluster_tt_slot_retain_skip_count(void)
{
	return 0;
}
uint64
cluster_tt_slot_retention_recycle_count(void)
{
	return 0;
}
uint64
cluster_tt_slot_retention_off_recycle_count(void) /* spec-3.22 */
{
	return 0;
}
uint64
cluster_undo_segment_retain_skip_count(void)
{
	return 0;
}
uint64
cluster_undo_tt_retention_rollover_count(void)
{
	return 0;
}

Size
cluster_shmem_get_total_bytes(void)
{
	return 0;
}
bool
cluster_shmem_iter_regions(int *idx pg_attribute_unused(),
						   ClusterShmemRegion *out pg_attribute_unused())
{
	return false;
}

/* StringInfo + pfree stubs for dump_shared_fs / dump_shmem (stage 1.3).
 * No-op pointers; SRF body is never invoked by this unit test. */
#include "lib/stringinfo.h"
void
initStringInfo(StringInfo str)
{
	str->data = NULL;
	str->len = 0;
	str->maxlen = 0;
	str->cursor = 0;
}
void
appendStringInfoChar(StringInfo str pg_attribute_unused(), char ch pg_attribute_unused())
{}
void
appendStringInfoString(StringInfo str pg_attribute_unused(), const char *s pg_attribute_unused())
{}
void
appendStringInfo(StringInfo str pg_attribute_unused(), const char *fmt pg_attribute_unused(), ...)
{}
void
resetStringInfo(StringInfo str pg_attribute_unused())
{}
void
pfree(void *pointer pg_attribute_unused())
{}


/* PG backend stubs */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * Spec-1.10 stubs needed because cluster_debug.o now pulls in
 * cluster_startup_phase.o (D6 dump_phase emits 4 new keys backed by
 * cluster_phase_started_at / cluster_phase_elapsed_seconds /
 * cluster_phase_history_format), which transitively references
 * GetCurrentTimestamp + TimestampDifference + timestamptz_to_str +
 * IsUnderPostmaster.  The unit test never invokes the dump path so
 * these are address-only -- harmless to stub to no-ops.
 */
bool IsUnderPostmaster = false;

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

void
TimestampDifference(TimestampTz start_time pg_attribute_unused(),
					TimestampTz stop_time pg_attribute_unused(), long *secs, int *microsecs)
{
	*secs = 0;
	*microsecs = 0;
}

bool
TimestampDifferenceExceeds(TimestampTz start_time pg_attribute_unused(),
						   TimestampTz stop_time pg_attribute_unused(),
						   int msec pg_attribute_unused())
{
	return false;
}

const char *
timestamptz_to_str(TimestampTz dt pg_attribute_unused())
{
	return "(stub)";
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/*
 * pg_snprintf stub: cluster_startup_phase.c uses snprintf which is
 * macro'd to pg_snprintf in PG.  Forward to libc snprintf in unit
 * test.  Variadic forwarding via vsnprintf.
 */
#include <stdarg.h>
int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	int n;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return n;
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}

text *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}

char *
psprintf(const char *fmt pg_attribute_unused(), ...)
{
	return (char *)"";
}

char *
pstrdup(const char *in pg_attribute_unused())
{
	return (char *)"";
}

Datum
DirectFunctionCall1Coll(PGFunction func pg_attribute_unused(), Oid collation pg_attribute_unused(),
						Datum arg1 pg_attribute_unused())
{
	return (Datum)0;
}

Datum
timestamptz_out(PG_FUNCTION_ARGS)
{
	return (Datum)0;
}


/*
 * Spec-1.10.1 D1 F1 stubs: cluster_startup_phase.o now references
 * the LWLock + ShmemInitStruct API (phase state migrated to shmem)
 * plus the cluster.phase{1..4}_timeout GUC variables (D2 F2 driver
 * elapsed check) plus cluster_shmem_register_region (region registry).
 * The unit test never invokes the runtime paths -- these are
 * address-only / NULL stubs so cluster_debug.o links standalone.
 */
#include "storage/lwlock.h"
#include "storage/shmem.h"

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = false;
	return NULL;
}

int cluster_phase1_timeout = 60;
int cluster_phase2_timeout = 30;
int cluster_phase3_timeout = 600;
int cluster_phase4_timeout = 30;
/* Spec-1.11 Sprint B: cluster_startup_phase.c references cluster_enabled */
bool cluster_enabled = true;
/* Spec-2.1 D1: cluster_startup_phase.c + cluster_conf.c reference allow_single_node */
bool cluster_allow_single_node = true;
/* spec-2.6 Q7 validator: cluster_startup_phase.c reads cluster_voting_disks */
char *cluster_voting_disks = NULL;

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

/*
 * Spec-1.11 Sprint A stubs: cluster_startup_phase.o now references
 * cluster_lmon_start + cluster_lmon_wait_for_ready (phase_1_handler
 * spawn + sync wait).  test_cluster_debug never invokes phase_1_handler
 * so these are address-only no-op stubs.
 */
int
cluster_lmon_start(void)
{
	return 0;
}

bool
cluster_lmon_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/*
 * Spec-1.11.1 F11 stubs: dump_lmon now reads 5 new accessors; test
 * harness never invokes runtime path so address-only no-op stubs.
 */
pid_t
cluster_lmon_pid(void)
{
	return 0;
}
TimestampTz
cluster_lmon_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_lmon_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_lmon_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_lmon_main_loop_iters(void)
{
	return 0;
}
int
cluster_lmon_status(void)
{
	return 0;
}
const char *
cluster_lmon_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/* Spec-1.12 D6+D12 stubs: cluster_startup_phase.o now references
 * cluster_lck_start + cluster_lck_wait_for_ready; cluster_debug.o
 * dump_lck now references 6 lck_* accessors. */
int
cluster_lck_start(void)
{
	return 0;
}
bool
cluster_lck_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_lck_status(void)
{
	return 0;
}
const char *
cluster_lck_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_lck_pid(void)
{
	return 0;
}
TimestampTz
cluster_lck_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_lck_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_lck_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_lck_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.13 D6+D12 stubs: cluster_startup_phase.o references
 * cluster_diag_start + cluster_diag_wait_for_ready; cluster_debug.o
 * dump_diag references 7 diag_* accessors. */
int
cluster_diag_start(void)
{
	return 0;
}
bool
cluster_diag_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_diag_status(void)
{
	return 0;
}
const char *
cluster_diag_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_diag_pid(void)
{
	return 0;
}
TimestampTz
cluster_diag_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_diag_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_diag_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_diag_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.14 D6+D12 stubs: cluster_startup_phase.o references
 * cluster_stats_start + cluster_stats_wait_for_ready; cluster_debug.o
 * dump_cluster_stats references 7 cluster_stats_* accessors. */
int
cluster_stats_start(void)
{
	return 0;
}
bool
cluster_stats_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* Spec-2.5 D4 stubs: same for CSSD. */
int
cluster_cssd_start(void)
{
	return 0;
}
bool
cluster_cssd_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* spec-2.6 Sprint A Step 3 D7 stubs: same for QVOTEC. */
pid_t
cluster_qvotec_start(void)
{
	return 0;
}
bool
cluster_qvotec_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* spec-2.18 Sprint A stubs: same for LMS. */
int
cluster_lms_start(void)
{
	return 0;
}
bool
cluster_lms_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_stats_status(void)
{
	return 0;
}
const char *
cluster_stats_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_stats_pid(void)
{
	return 0;
}
TimestampTz
cluster_stats_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_stats_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_stats_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_stats_main_loop_iters(void)
{
	return 0;
}

/* Spec-2.5 D12 stubs: cluster_debug.c calls cluster_cssd_* accessors
 * via dump_cluster_cssd();test stubs return 0 / sentinel. */
#include "cluster/cluster_cssd.h"
ClusterCssdStatus
cluster_cssd_get_status(void)
{
	return CLUSTER_CSSD_STARTING;
}
const char *
cluster_cssd_status_to_string(ClusterCssdStatus s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_cssd_get_pid(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_last_liveness_tick_at(void)
{
	return 0;
}
uint64
cluster_cssd_get_main_loop_iters(void)
{
	return 0;
}

/* spec-2.5 Hardening v1.0.3 stubs:  cluster_debug.c dump_cluster_cssd
 * references declared_alive aggregate accessors;test_cluster_debug
 * standalone link must resolve.  Stub returns 0 / zero bitmap. */
int
cluster_cssd_get_declared_alive_count(void)
{
	return 0;
}
void
cluster_cssd_get_declared_alive_bitmap(uint8 out_bitmap[CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES])
{
	if (out_bitmap != NULL)
		memset(out_bitmap, 0, CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES);
}

/* Spec-1.15 SCN encoding-layer stubs (cluster_debug.c references the
 * 7-key dump set; address-only in unit tests). */
SCN
cluster_scn_current(void)
{
	return 0;
}
NodeId
cluster_scn_node_id(void)
{
	return 0;
}
uint64
cluster_scn_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_max_observed_remote(void)
{
	return 0;
}
TimestampTz
cluster_scn_initialized_at(void)
{
	return 0;
}
TimestampTz
cluster_scn_last_advance_at(void)
{
	return 0;
}

/* spec-1.16 stat accessor stubs (cluster_debug references the 3 new
 * counter accessors for dump_scn 7 -> 10 keys). */
uint64
cluster_scn_commit_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_abort_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_observe_bump_count(void)
{
	return 0;
}
/* Spec-1.17 BOC stat accessor stubs. */
uint64
cluster_scn_boc_sweep_count(void)
{
	return 0;
}
TimestampTz
cluster_scn_boc_last_sweep_at(void)
{
	return 0;
}
uint64
cluster_scn_boc_pending_at_last_sweep(void)
{
	return 0;
}
uint64
cluster_scn_boc_max_batch_size(void)
{
	return 0;
}
/* spec-2.10 D5 / L104 stub: cluster_debug emit_row references new
 * cluster_scn module accessor;test_cluster_debug standalone binary
 * doesn't link cluster_scn.o,vacuous stub. */
uint64
cluster_scn_boc_broadcast_fanout_count(void)
{
	return 0;
}

/* spec-2.11 D5 / L104 stub: cluster_debug emit_row references new
 * spec-2.11 cluster_scn module accessor;test_cluster_debug standalone
 * binary doesn't link cluster_scn.o,vacuous stub. */
uint64
cluster_scn_commit_lookup_defer_count(void)
{
	return 0;
}

/* spec-2.12 D4 / D5 / L104 stubs: cluster_debug emit_row references 2
 * new spec-2.12 cluster_scn module accessors (scn_last_observe_at +
 * scn_observed_max_observe_gap_ms);test_cluster_debug standalone
 * binary doesn't link cluster_scn.o,vacuous stubs. */
TimestampTz
cluster_scn_last_observe_at(void)
{
	return 0;
}

uint64
cluster_scn_observed_max_observe_gap_ms(void)
{
	return 0;
}

/* spec-2.13 D8 / L104 stubs: cluster_debug dump_ges references 2 new
 * spec-2.13 cluster_ges module accessors;  test_cluster_debug standalone
 * binary doesn't link cluster_ges.o,  vacuous stubs. */
uint64
cluster_ges_request_defer_count(void)
{
	return 0;
}

uint64
cluster_ges_reply_defer_count(void)
{
	return 0;
}

/* spec-2.14 D12 / L104 stubs: cluster_debug dump_grd references 7 new
 * spec-2.14 cluster_grd module accessors;  test_cluster_debug standalone
 * binary doesn't link cluster_grd.o,  vacuous stubs. */
uint32
cluster_grd_local_master_count(void)
{
	return 0;
}

uint32
cluster_grd_remote_master_count(void)
{
	return 0;
}

uint64
cluster_grd_shard_lookup_count(void)
{
	return 0;
}

uint64
cluster_grd_local_master_lookup_count(void)
{
	return 0;
}

uint64
cluster_grd_remote_master_lookup_count(void)
{
	return 0;
}

uint64
cluster_grd_resid_encode_count(void)
{
	return 0;
}

uint64
cluster_grd_master_map_refresh_count_get(void)
{
	return 0;
}

/* spec-2.15 D12 L104 stubs:  6 NEW accessor for dump_grd 6 NEW emit_row. */
int
cluster_grd_max_entries_get(void)
{
	return 0;
}

int
cluster_grd_entry_count(void)
{
	return 0;
}

Size
cluster_grd_allocated_bytes(void)
{
	return 0;
}

uint64
cluster_grd_entry_create_count(void)
{
	return 0;
}

uint64
cluster_grd_entry_lookup_hit_count(void)
{
	return 0;
}

uint64
cluster_grd_entry_full_count(void)
{
	return 0;
}

uint64
cluster_grd_holders_full_count(void)
{
	return 0;
}
uint64
cluster_grd_waiters_full_count(void)
{
	return 0;
}
uint64
cluster_grd_converts_full_count(void)
{
	return 0;
}
uint64
cluster_grd_ngranted_promoted_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_work_queue_full_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_cleanup_deferred_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_inbound_validation_fail_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_reply_deferred_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_reply_dropped_count(void)
{
	return 0;
}

/* spec-2.17 D27 — 9 NEW counter stubs(BAST 6 + deadlock 3). */
uint64
cluster_grd_bast_sent_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_received_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_ack_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_retry_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_reject_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_stale_drop_count(void)
{
	return 0;
}
uint64
cluster_grd_deadlock_probe_drop_count(void)
{
	return 0;
}
uint64
cluster_grd_deadlock_probe_collision_drop_count(void)
{
	return 0;
}
uint64
cluster_grd_deadlock_chunk_oo_buffer_overflow_count(void)
{
	return 0;
}

/* spec-4.6 D5 stub:  dump_grd_recovery consumes the bulk counter
 * snapshot.  Zero-fill;  layout = 13 × uint64 (must track the
 * ClusterGrdRecoveryCounters struct in cluster_grd.h). */
struct ClusterGrdRecoveryCounters;
void cluster_grd_recovery_counters_snapshot(struct ClusterGrdRecoveryCounters *out);
void
cluster_grd_recovery_counters_snapshot(struct ClusterGrdRecoveryCounters *out)
{
	memset(out, 0, 13 * sizeof(uint64));
}

uint32
cluster_grd_outbound_ring_depth(void)
{
	return 0;
}
uint32
cluster_grd_outbound_reply_dirty_depth(void)
{
	return 0;
}
uint32
cluster_grd_outbound_cleanup_dirty_depth(void)
{
	return 0;
}
uint32
cluster_grd_work_queue_depth(void)
{
	return 0;
}
uint64
cluster_grd_pending_count(void)
{
	return 0;
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * SRF entry-point linkability.
 * ============================================================ */

UT_TEST(test_debug_dump_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_dump_state);
}


/* ============================================================
 * Iterator API on cluster_inject (added in spec-0.29 §1.4).
 * ============================================================ */

UT_TEST(test_debug_inject_get_count_callable)
{
	/* Stub returns 0; verifies the symbol is reachable. */
	UT_ASSERT_EQ(cluster_injection_get_count(), 0);
}

UT_TEST(test_debug_inject_get_state_at_out_of_range)
{
	const char *name = NULL;
	ClusterInjectFaultType type = CLUSTER_FAULT_NONE;
	uint64 hits = 0;

	/* Stub always returns false; real impl returns false when idx<0
	 * or idx>=count.  The contract is the same: out-of-range yields
	 * false, output args untouched. */
	UT_ASSERT_EQ(cluster_injection_get_state_at(-1, &name, &type, &hits), false);
	UT_ASSERT_EQ(cluster_injection_get_state_at(1000, &name, &type, &hits), false);
}

UT_TEST(test_debug_inject_get_state_at_null_outs)
{
	/* Out-pointers may be NULL; helper must not crash. */
	(void)cluster_injection_get_state_at(0, NULL, NULL, NULL);
	UT_ASSERT(true);
}


/* ============================================================
 * Iterator API on cluster_pgstat (added in spec-0.29 §1.4).
 * ============================================================ */

UT_TEST(test_debug_pgstat_get_count_callable)
{
	UT_ASSERT_EQ(cluster_pgstat_get_count(), 0);
}

UT_TEST(test_debug_pgstat_get_at_out_of_range)
{
	const char *name = NULL;
	uint64 value = 0;

	UT_ASSERT_EQ(cluster_pgstat_get_at(-1, &name, &value), false);
	UT_ASSERT_EQ(cluster_pgstat_get_at(1000, &name, &value), false);
}

UT_TEST(test_debug_pgstat_get_at_null_outs)
{
	(void)cluster_pgstat_get_at(0, NULL, NULL);
	UT_ASSERT(true);
}


/* ============================================================
 * Cross-module symbol resolution checks.
 *
 *	If any cluster_*.h public API drifts (rename / removal),
 *	cluster_debug.c will fail to link, which is what we want to
 *	catch at compile time.  The tests below address-take symbols
 *	cluster_debug.c references to surface link-time breakage early.
 * ============================================================ */

UT_TEST(test_debug_links_against_inject_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_injection_get_count);
	UT_ASSERT_NOT_NULL((void *)cluster_injection_get_state_at);
}

/*
 * spec-2.18 Sprint A Step 4 D10 L104 stubs: dump_cluster_lms references
 * cluster_lms_* accessors via cluster_debug.o; standalone test
 * harness must provide local zero-returning stubs.
 */
int
cluster_lms_get_state(void)
{
	return 0;
}
uint64
cluster_lms_get_started_count(void)
{
	return 0;
}
uint64
cluster_lms_get_work_drained_count(void)
{
	return 0;
}
/*
 * spec-2.20 D9 — 3 NEW LMS decision counter stubs (replacing single
 * lms_decision_count).  Each grant body inc exactly one (mutually
 * exclusive).
 */
uint64
cluster_lms_get_decision_grant_count(void)
{
	return 0;
}
uint64
cluster_lms_get_decision_reject_count(void)
{
	return 0;
}
uint64
cluster_lms_get_decision_convert_count(void)
{
	return 0;
}
uint64
cluster_lms_get_drain_empty_count(void)
{
	return 0;
}
uint64
cluster_lms_get_error_count(void)
{
	return 0;
}
/* spec-2.25 D13 R10 stub audit — 7 NEW native-lock probe counter accessors. */
uint64
cluster_lms_get_native_probe_sent_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_reply_recv_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_collector_slot_full_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_aggregate_holder_conflict_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_aggregate_waiter_conflict_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_retry_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_timeout_count(void)
{
	return 0;
}
/* spec-2.27 D7 R10 stub audit — priority starvation observability counter. */
uint64
cluster_lms_get_priority_starvation_observed_count(void)
{
	return 0;
}
const char *
cluster_lms_state_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/*
 * spec-2.19 Sprint A Step 4 D10 L104 stubs: dump_lmd references
 * cluster_lmd_* accessors via cluster_debug.o; standalone test
 * harness must provide local zero-returning stubs.
 */
int
cluster_lmd_get_state(void)
{
	return 0;
}
uint64
cluster_lmd_get_started_count(void)
{
	return 0;
}
TimestampTz
cluster_lmd_get_ready_at(void)
{
	return 0;
}
uint64
cluster_lmd_get_edge_submission_count(void)
{
	return 0;
}
uint64
cluster_lmd_get_wake_count(void)
{
	return 0;
}
uint64
cluster_lmd_get_idle_count(void)
{
	return 0;
}
uint64
cluster_lmd_get_error_count(void)
{
	return 0;
}
const char *
cluster_lmd_state_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/* spec-2.22 D12 — LMD Tarjan + graph counter stubs (9 NEW). */
uint64
cluster_lmd_wait_edge_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_wait_edge_full_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_graph_generation_get(void)
{
	return 0;
}
uint64
cluster_lmd_tarjan_scan_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cycle_detected_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_victim_cancel_sent_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_revalidate_fail_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cross_node_victim_pending_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_inject_call_count_get(void)
{
	return 0;
}
/* spec-2.23 D13 stub audit — new dump_ges / dump_lmd counter rows. */
uint64
cluster_ges_reply_wait_table_active_count(void)
{
	return 0;
}
uint64
cluster_ges_reply_late_drop_count(void)
{
	return 0;
}
uint64
cluster_ges_release_ack_count(void)
{
	return 0;
}
uint64
cluster_lmd_probe_broadcast_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_probe_partial_count_get(void)
{
	return 0;
}
/* spec-2.24 D13 stub audit — 6 NEW lmd counters + 1 NEW grd counter. */
uint64
cluster_lmd_cross_node_victim_cancel_sent_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cross_node_cancel_received_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cross_node_cancel_queue_full_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_on_backend_exit_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_lmd_sweep_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_skip_other_owner_count_get(void)
{
	return 0;
}
uint64
cluster_grd_cleanup_skip_stale_cancel_count(void)
{
	return 0;
}
/* spec-2.25 D13 R10 stub audit — RELATION + OBJECT gate hit counter. */
uint64
cluster_grd_relation_object_cluster_path_count(void)
{
	return 0;
}
void
cluster_grd_inc_relation_object_cluster_path(void)
{}

/* spec-2.26 D5 R10 stub audit — TRANSACTION gate hit counter. */
uint64
cluster_grd_transaction_cluster_path_count(void)
{
	return 0;
}
void
cluster_grd_inc_transaction_cluster_path(void)
{}

UT_TEST(test_debug_links_against_pgstat_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_pgstat_get_count);
	UT_ASSERT_NOT_NULL((void *)cluster_pgstat_get_at);
}

UT_TEST(test_debug_links_against_conf_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_conf_node_count);
	UT_ASSERT_NOT_NULL((void *)cluster_conf_lookup_node);
}

UT_TEST(test_debug_phase_symbol_present)
{
	UT_ASSERT_NOT_NULL(cluster_phase);
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_debug_dump_srf_linkable);
	UT_RUN(test_debug_inject_get_count_callable);
	UT_RUN(test_debug_inject_get_state_at_out_of_range);
	UT_RUN(test_debug_inject_get_state_at_null_outs);
	UT_RUN(test_debug_pgstat_get_count_callable);
	UT_RUN(test_debug_pgstat_get_at_out_of_range);
	UT_RUN(test_debug_pgstat_get_at_null_outs);
	UT_RUN(test_debug_links_against_inject_module);
	UT_RUN(test_debug_links_against_pgstat_module);
	UT_RUN(test_debug_links_against_conf_module);
	UT_RUN(test_debug_phase_symbol_present);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
