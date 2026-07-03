/*-------------------------------------------------------------------------
 *
 * cluster_mrp.h
 *	  ADG Managed Recovery Process (MRP) lifecycle skeleton.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_mrp.h
 *
 * NOTES
 *	  This is a pgrac-original file for spec-6.4 ADG apply master/MRP.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MRP_H
#define CLUSTER_MRP_H

#include "access/xlogdefs.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_xlog.h"
#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/proc.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterMrpState {
	CLUSTER_MRP_NOT_STARTED = 0,
	CLUSTER_MRP_STARTING = 1,
	CLUSTER_MRP_READY = 2,
	CLUSTER_MRP_DRAINING = 3,
	CLUSTER_MRP_STOPPED = 4,
	CLUSTER_MRP_DISABLED = 5
} ClusterMrpState;

#define CLUSTER_MRP_STATE_LAST CLUSTER_MRP_DISABLED

typedef enum ClusterMrpApplyLeaseSubmitResult {
	CLUSTER_MRP_APPLY_LEASE_SUBMIT_ACK = 0,
	CLUSTER_MRP_APPLY_LEASE_SUBMIT_FAILED,
	CLUSTER_MRP_APPLY_LEASE_SUBMIT_TIMEOUT,
	CLUSTER_MRP_APPLY_LEASE_SUBMIT_STALE,
	CLUSTER_MRP_APPLY_LEASE_SUBMIT_NO_QUORUM,
	CLUSTER_MRP_APPLY_LEASE_SUBMIT_INVALID
} ClusterMrpApplyLeaseSubmitResult;

typedef struct ClusterMrpSharedState {
	pg_atomic_uint32 mrp_state;
	pg_atomic_uint32 pid;
	pg_atomic_uint32 apply_master_node_id;
	pg_atomic_uint32 apply_master_term_valid;
	pg_atomic_uint64 apply_master_token_seq;
	pg_atomic_uint64 apply_master_term;
	pg_atomic_uint64 apply_master_generation;
	pg_atomic_uint64 apply_master_epoch;
	pg_atomic_uint64 apply_master_owner_incarnation;
	pg_atomic_uint64 apply_master_term_valid_until_ms;
	pg_atomic_uint64 apply_master_lost_at_ms;
	pg_atomic_uint64 receive_lsn;
	pg_atomic_uint64 apply_lsn;
	pg_atomic_uint64 standby_consistent_scn;
	pg_atomic_uint64 started_count;
	pg_atomic_uint64 idle_count;
	pg_atomic_uint64 apply_batch_count;
	pg_atomic_uint64 error_count;
	pg_atomic_uint64 ready_at_us;
	pg_atomic_uint64 stopped_at_us;
	struct Latch *apply_lease_qvotec_latch;
	struct Latch *apply_latch;
	pg_atomic_uint64 apply_lease_request_seq;
	pg_atomic_uint64 pending_apply_lease_seq;
	pg_atomic_uint64 apply_lease_completion_seq;
	pg_atomic_uint32 apply_lease_result;
	pg_atomic_uint64 apply_lease_write_failed;
	ClusterAdgApplyMasterLease pending_apply_lease;
	pg_atomic_uint32 primary_thread_count;
	pg_atomic_uint64 primary_thread_bitmap[2];
	pg_atomic_uint64 thread_receive_lsn[CLUSTER_WAL_THREAD_MAX + 1];
	pg_atomic_uint64 thread_start_lsn[CLUSTER_WAL_THREAD_MAX + 1];
	pg_atomic_uint64 thread_receive_time_us[CLUSTER_WAL_THREAD_MAX + 1];
	pg_atomic_uint64 thread_apply_lsn[CLUSTER_WAL_THREAD_MAX + 1];
	pg_atomic_uint64 thread_apply_time_us[CLUSTER_WAL_THREAD_MAX + 1];
	pg_atomic_uint64 thread_barrier_scn[CLUSTER_WAL_THREAD_MAX + 1];
	pg_atomic_uint64 thread_barrier_lsn[CLUSTER_WAL_THREAD_MAX + 1];
} ClusterMrpSharedState;

extern bool cluster_mrp_config_allows_start(int dg_role, bool enable_adg);
extern bool cluster_mrp_should_start(void);

extern Size cluster_mrp_shmem_size(void);
extern void cluster_mrp_shmem_init(void);
extern void cluster_mrp_shmem_register(void);
extern ClusterMrpSharedState *cluster_mrp_shared_state(void);

extern ClusterMrpState cluster_mrp_get_state(void);
extern const char *cluster_mrp_state_to_string(ClusterMrpState state);
extern SCN cluster_mrp_standby_consistent_scn(void);
extern int64 cluster_mrp_apply_lag_ms(void);
extern bool cluster_mrp_apply_master_can_apply(void);
extern bool cluster_mrp_apply_master_term_still_valid(uint64 held_term);
extern bool cluster_mrp_read_service_available(void);
extern uint64 cluster_mrp_apply_master_term(void);
extern uint32 cluster_mrp_apply_master_node_id(void);
extern void cluster_mrp_note_primary_thread_count(uint16 primary_thread_count);
extern bool cluster_mrp_rfs_restart_lsn(uint16 thread_id, XLogRecPtr fallback_lsn,
										XLogRecPtr *restart_lsn);
extern int cluster_mrp_streaming_snapshot(uint64 bitmap[2], XLogRecPtr start_lsn[],
										  XLogRecPtr receive_lsn[], XLogRecPtr barrier_lsn[],
										  SCN barrier_scn[]);
extern void cluster_mrp_mark_thread_received_span(uint16 thread_id, XLogRecPtr start_lsn,
												  XLogRecPtr receive_lsn);
extern void cluster_mrp_mark_thread_received(uint16 thread_id, XLogRecPtr receive_lsn);
extern void cluster_mrp_mark_thread_applied(uint16 thread_id, XLogRecPtr apply_lsn);
extern void cluster_mrp_apply_thread_barrier(uint16 thread_id, XLogRecPtr barrier_lsn,
											 SCN thread_safe_scn, uint16 primary_thread_count);
extern void cluster_mrp_mark_child_exit(void);
extern void cluster_mrp_publish_watermarks(XLogRecPtr receive_lsn, XLogRecPtr apply_lsn,
										   uint64 standby_consistent_scn);

extern void cluster_mrp_publish_qvotec_latch(struct Latch *latch);
extern bool cluster_mrp_qvotec_poll_apply_lease_request(ClusterAdgApplyMasterLease *out);
extern void
cluster_mrp_qvotec_complete_apply_lease_request(ClusterMrpApplyLeaseSubmitResult result,
												const ClusterAdgApplyMasterLeaseQuorum *winner);

extern pid_t cluster_postmaster_start_mrp(void);
extern void MrpMain(void) pg_attribute_noreturn();

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_MRP_H */
