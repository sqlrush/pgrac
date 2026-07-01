/*-------------------------------------------------------------------------
 *
 * cluster_adg.h
 *	  Dependency-light ADG standby/read-only correctness primitives.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_adg.h
 *
 * NOTES
 *	  This is the pure foundation for spec-6.4 ADG physical standby:
 *	  standby SCN tracking, pending-commit barriers, apply-master term
 *	  checks, k-way merge selection, read-only admission, and the prepared
 *	  overlay transition used by standby COMMIT PREPARED redo.  Runtime
 *	  shmem, postmaster, WAL receiver/sender, and SQL surfaces wrap these
 *	  primitives in later layers.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ADG_H
#define CLUSTER_ADG_H

#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_xlog.h"
#include "port/pg_crc32c.h"

#define CLUSTER_ADG_MAX_THREADS 16
#define CLUSTER_ADG_PENDING_MAX CLUSTER_SCN_ADG_PENDING_MAX
#define CLUSTER_ADG_APPLY_LEASE_MAGIC 0x41444c53 /* "ADLS" */
#define CLUSTER_ADG_APPLY_LEASE_VERSION 1
#define CLUSTER_ADG_APPLY_LEASE_SLOT_BYTES 512
#define CLUSTER_ADG_REPLY_MAGIC 0x41444752 /* "ADGR" */
#define CLUSTER_ADG_REPLY_VERSION 1
#define CLUSTER_ADG_REPLY_TRAILER_BYTES (4 + 2 + 2 + 8 + 8)

typedef enum ClusterAdgReadDecision {
	CLUSTER_ADG_READ_ALLOW = 0,
	CLUSTER_ADG_READ_WAIT = 1,
	CLUSTER_ADG_READ_LAG_EXCESSIVE = 2,
	CLUSTER_ADG_READ_UNRESOLVABLE = 3
} ClusterAdgReadDecision;

typedef enum ClusterAdgLeaseDecision {
	CLUSTER_ADG_LEASE_VALID = 0,
	CLUSTER_ADG_LEASE_NOT_ATTACHED = 1,
	CLUSTER_ADG_LEASE_NO_QUORUM = 2,
	CLUSTER_ADG_LEASE_TERM_STALE = 3,
	CLUSTER_ADG_LEASE_EXPIRED = 4
} ClusterAdgLeaseDecision;

typedef enum ClusterAdgStandbyConflictDecision {
	CLUSTER_ADG_CONFLICT_NONE = 0,
	CLUSTER_ADG_CONFLICT_WAIT = 1,
	CLUSTER_ADG_CONFLICT_CANCEL_READER = 2
} ClusterAdgStandbyConflictDecision;

typedef struct ClusterAdgThreadStatus {
	bool active;
	bool has_receive;
	bool has_apply;
	bool has_barrier;
	XLogRecPtr receive_lsn;
	XLogRecPtr apply_lsn;
	SCN last_apply_scn;
	SCN barrier_safe_scn;
	int64 receive_time_ms;
	int64 apply_time_ms;
} ClusterAdgThreadStatus;

typedef struct ClusterAdgScnTracker {
	uint16 thread_count;
	SCN standby_consistent_scn;
	uint64 publish_count;
	ClusterAdgThreadStatus threads[CLUSTER_ADG_MAX_THREADS];
} ClusterAdgScnTracker;

typedef struct ClusterAdgPendingCommitRegistry {
	uint16 count;
	bool overflowed;
	SCN pending[CLUSTER_ADG_PENDING_MAX];
} ClusterAdgPendingCommitRegistry;

typedef struct ClusterAdgMergeInput {
	bool available;
	SCN scn;
	XLogRecPtr lsn;
	NodeId node;
} ClusterAdgMergeInput;

typedef struct ClusterAdgApplyMasterLease {
	uint32 magic;
	uint32 version;
	uint64 term;
	int32 owner_node_id;
	int32 reserved;
	int64 lease_expires_at_ms;
	uint64 generation;
	pg_crc32c crc;
} ClusterAdgApplyMasterLease;

StaticAssertDecl(sizeof(ClusterAdgApplyMasterLease) <= CLUSTER_ADG_APPLY_LEASE_SLOT_BYTES,
				 "ADG apply-master lease marker must fit in one voting-disk lease slot");

extern bool cluster_adg_scn_tracker_init(ClusterAdgScnTracker *tracker, uint16 thread_count);
extern bool cluster_adg_mark_received(ClusterAdgScnTracker *tracker, uint16 thread_id,
									  XLogRecPtr receive_lsn, int64 receive_time_ms);
extern bool cluster_adg_mark_applied(ClusterAdgScnTracker *tracker, uint16 thread_id,
									 XLogRecPtr apply_lsn, SCN apply_scn, int64 apply_time_ms);
extern bool cluster_adg_apply_thread_barrier(ClusterAdgScnTracker *tracker, uint16 thread_id,
											 XLogRecPtr apply_lsn, SCN barrier_safe_scn,
											 int64 apply_time_ms);
extern SCN cluster_adg_scn_tracker_consistent_scn(const ClusterAdgScnTracker *tracker);
extern int64 cluster_adg_thread_apply_lag_ms(const ClusterAdgScnTracker *tracker, uint16 thread_id,
											 int64 now_ms);

extern void cluster_adg_pending_init(ClusterAdgPendingCommitRegistry *registry);
extern bool cluster_adg_pending_register(ClusterAdgPendingCommitRegistry *registry, SCN commit_scn);
extern bool cluster_adg_pending_clear(ClusterAdgPendingCommitRegistry *registry, SCN commit_scn);
extern SCN cluster_adg_pending_min_scn(const ClusterAdgPendingCommitRegistry *registry);
extern SCN cluster_adg_thread_safe_scn(const ClusterAdgPendingCommitRegistry *registry,
									   SCN current_scn);

extern ClusterAdgLeaseDecision
cluster_adg_apply_master_lease_check(uint64 held_term, uint64 durable_term, bool lease_attached,
									 bool in_quorum, int64 now_ms, int64 lease_expires_at_ms);
extern uint64 cluster_adg_apply_master_next_term(uint64 durable_term);
extern pg_crc32c cluster_adg_apply_master_lease_crc(const ClusterAdgApplyMasterLease *lease);
extern void cluster_adg_apply_master_lease_init(ClusterAdgApplyMasterLease *lease, uint64 term,
												int32 owner_node_id, int64 lease_expires_at_ms,
												uint64 generation);
extern bool cluster_adg_apply_master_lease_valid(const ClusterAdgApplyMasterLease *lease);
extern bool cluster_adg_apply_master_lease_pack(void *slot512,
												const ClusterAdgApplyMasterLease *lease);
extern bool cluster_adg_apply_master_lease_unpack(const void *slot512,
												  ClusterAdgApplyMasterLease *lease);

extern bool cluster_adg_streaming_merge_select(const ClusterAdgMergeInput inputs[],
											   uint16 input_count, uint16 *out_index);
extern ClusterAdgReadDecision cluster_adg_read_only_decide(bool enable_adg, bool standby_role,
														   SCN requested_read_scn,
														   SCN standby_consistent_scn,
														   int64 apply_lag_ms, int64 max_lag_ms);
extern ClusterAdgStandbyConflictDecision
cluster_adg_standby_conflict_decide(bool enable_adg, bool standby_role, int64 wait_ms,
									int64 max_standby_delay_ms);
extern bool cluster_adg_overlay_resolve_on_commit_prepared(ClusterTTStatus current_status,
														   SCN commit_scn,
														   ClusterTTStatus *out_status,
														   SCN *out_commit_scn);
extern bool cluster_adg_reply_trailer_valid(uint32 magic, uint16 version, uint16 thread_id,
											SCN standby_consistent_scn, uint64 apply_master_term);

#endif /* CLUSTER_ADG_H */
