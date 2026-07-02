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
 *	  Shared ADG physical standby helpers used by the MRP coordinator,
 *	  read-only snapshot admission, rmgr descriptor, and LNS/RFS wire checks.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ADG_H
#define CLUSTER_ADG_H

#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_xlog.h"
#include "port/pg_crc32c.h"

#define CLUSTER_ADG_MAX_THREADS CLUSTER_WAL_THREAD_MAX
#define CLUSTER_ADG_APPLY_LEASE_MAGIC 0x41444c53 /* "ADLS" */
#define CLUSTER_ADG_APPLY_LEASE_VERSION 2
#define CLUSTER_ADG_APPLY_LEASE_SLOT_BYTES 512
#define CLUSTER_ADG_REPLY_MAGIC 0x41444752 /* "ADGR" */
#define CLUSTER_ADG_REPLY_VERSION 1
#define CLUSTER_ADG_REPLY_TRAILER_BYTES (4 + 2 + 2 + 8 + 8)

typedef enum ClusterAdgReadDecision {
	CLUSTER_ADG_READ_ALLOW = 0,
	CLUSTER_ADG_READ_LAG_EXCESSIVE = 1,
	CLUSTER_ADG_READ_UNRESOLVABLE = 2
} ClusterAdgReadDecision;

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

typedef struct ClusterAdgApplyMasterLease {
	uint32 magic;
	uint32 version;
	uint64 term;
	int32 owner_node_id;
	int32 reserved;
	int64 lease_expires_at_ms;
	uint64 generation;
	uint64 lease_epoch;
	uint64 owner_incarnation;
	pg_crc32c crc;
} ClusterAdgApplyMasterLease;

typedef struct ClusterAdgApplyMasterLeaseQuorum {
	bool attached;
	int count;
	uint64 durable_term;
	int32 owner_node_id;
	int64 lease_expires_at_ms;
	uint64 generation;
	uint64 lease_epoch;
	uint64 owner_incarnation;
} ClusterAdgApplyMasterLeaseQuorum;

typedef enum ClusterAdgApplyMasterLeaseCasVerdict {
	CLUSTER_ADG_APPLY_LEASE_CAS_INVALID = 0,
	CLUSTER_ADG_APPLY_LEASE_CAS_RENEW,
	CLUSTER_ADG_APPLY_LEASE_CAS_TAKE_EMPTY,
	CLUSTER_ADG_APPLY_LEASE_CAS_TAKE_EXPIRED,
	CLUSTER_ADG_APPLY_LEASE_CAS_STALE
} ClusterAdgApplyMasterLeaseCasVerdict;

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
extern uint64 cluster_adg_apply_master_next_term(uint64 durable_term);
extern pg_crc32c cluster_adg_apply_master_lease_crc(const ClusterAdgApplyMasterLease *lease);
extern void cluster_adg_apply_master_lease_init(ClusterAdgApplyMasterLease *lease, uint64 term,
												int32 owner_node_id, int64 lease_expires_at_ms,
												uint64 generation);
extern void cluster_adg_apply_master_lease_init_full(ClusterAdgApplyMasterLease *lease, uint64 term,
													 int32 owner_node_id, int64 lease_expires_at_ms,
													 uint64 generation, uint64 lease_epoch,
													 uint64 owner_incarnation);
extern bool cluster_adg_apply_master_lease_valid(const ClusterAdgApplyMasterLease *lease);
extern bool cluster_adg_apply_master_lease_pack(void *slot512,
												const ClusterAdgApplyMasterLease *lease);
extern bool cluster_adg_apply_master_lease_unpack(const void *slot512,
												  ClusterAdgApplyMasterLease *lease);
extern bool cluster_adg_apply_master_lease_quorum(const ClusterAdgApplyMasterLease leases[],
												  const bool valid[], int lease_count, int quorum,
												  ClusterAdgApplyMasterLeaseQuorum *out);
extern ClusterAdgApplyMasterLeaseCasVerdict
cluster_adg_apply_master_lease_cas_verdict(const ClusterAdgApplyMasterLeaseQuorum *current,
										   const ClusterAdgApplyMasterLease *desired, int64 now_ms);
extern int32 cluster_adg_apply_master_candidate_node(const uint8 *alive_bitmap, int bitmap_bytes);
extern bool cluster_adg_apply_master_candidate_allows_owner(const uint8 *alive_bitmap,
															int bitmap_bytes, int32 owner_node_id);
extern bool cluster_adg_apply_master_token_allows_apply(
	uint32 owner_node_id, uint32 valid, uint64 term, uint64 generation, uint64 lease_epoch,
	uint64 owner_incarnation, uint64 valid_until_ms, int32 self_node_id, uint64 current_epoch,
	uint64 local_incarnation, uint64 held_term, int64 now_ms);

extern ClusterAdgReadDecision cluster_adg_read_only_decide(bool enable_adg, bool standby_role,
														   bool read_service_available,
														   SCN standby_consistent_scn,
														   int64 apply_lag_ms, int64 max_lag_ms);
extern bool cluster_adg_reply_trailer_valid(uint32 magic, uint16 version, uint16 thread_id,
											SCN standby_consistent_scn, uint64 apply_master_term);

#endif /* CLUSTER_ADG_H */
