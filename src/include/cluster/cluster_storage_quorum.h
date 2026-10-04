/*-------------------------------------------------------------------------
 *
 * cluster_storage_quorum.h
 *    Storage-component eligibility for database membership.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_storage_quorum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_STORAGE_QUORUM_H
#define CLUSTER_STORAGE_QUORUM_H

#include "cluster/cluster_conf.h"
#include "port/atomics.h"

#define CLUSTER_STORAGE_QUORUM_STATE_BYTES 64

typedef enum ClusterStorageQuorumReason {
	CLUSTER_STORAGE_QUORUM_UNAVAILABLE = 0,
	CLUSTER_STORAGE_QUORUM_CONFIGURATION,
	CLUSTER_STORAGE_QUORUM_NOT_QUORATE,
	CLUSTER_STORAGE_QUORUM_READY
} ClusterStorageQuorumReason;

/* Observation only: zero return code means that no API result was sampled. */
typedef enum ClusterStorageProviderStep {
	CLUSTER_STORAGE_PROVIDER_NONE = 0,
	CLUSTER_STORAGE_PROVIDER_READY,
	CLUSTER_STORAGE_PROVIDER_LOAD,
	CLUSTER_STORAGE_PROVIDER_CMAP_INITIALIZE,
	CLUSTER_STORAGE_PROVIDER_QUORUM_INITIALIZE,
	CLUSTER_STORAGE_PROVIDER_QUORUM_TYPE,
	CLUSTER_STORAGE_PROVIDER_PROFILE,
	CLUSTER_STORAGE_PROVIDER_TRACK_CURRENT,
	CLUSTER_STORAGE_PROVIDER_GETQUORATE,
	CLUSTER_STORAGE_PROVIDER_DISPATCH,
	CLUSTER_STORAGE_PROVIDER_NOT_QUORATE,
	CLUSTER_STORAGE_PROVIDER_NOTIFICATION_MISSING,
	CLUSTER_STORAGE_PROVIDER_NOTIFICATION_INVALID,
	CLUSTER_STORAGE_PROVIDER_NOTIFICATION_MISMATCH
} ClusterStorageProviderStep;

#define CLUSTER_STORAGE_PROVIDER_DIAGNOSTIC(step, result)                                          \
	(((uint32)(step) << 16) | ((uint32)(result) & UINT32_C(0xffff)))

typedef struct ClusterStorageQuorumView {
	ClusterStorageQuorumReason reason;
	uint32 ring_node;
	uint64 ring_sequence;
	uint64 members[2];
	uint64 sampled_us;
	uint64 expires_us;
	uint64 generation;
	uint32 provider_diagnostic;
} ClusterStorageQuorumView;

/* QVOTEC owns publication. Readers cannot refresh the observation. */
typedef struct ClusterStorageQuorumState {
	pg_atomic_uint32 sequence;
	pg_atomic_uint32 reason;
	pg_atomic_uint32 ring_node;
	pg_atomic_uint32 provider_diagnostic;
	pg_atomic_uint64 ring_sequence;
	pg_atomic_uint64 members[2];
	pg_atomic_uint64 sampled_us;
	pg_atomic_uint64 expires_us;
	pg_atomic_uint64 generation;
} ClusterStorageQuorumState;

StaticAssertDecl(sizeof(ClusterStorageQuorumState) == CLUSTER_STORAGE_QUORUM_STATE_BYTES,
				 "storage quorum observation layout");

typedef enum ClusterStorageCheckResult {
	CLUSTER_STORAGE_CHECK_ALLOWED = 0,
	CLUSTER_STORAGE_CHECK_NATIVE,
	CLUSTER_STORAGE_CHECK_INVALID_TARGET,
	CLUSTER_STORAGE_CHECK_UNATTACHED,
	CLUSTER_STORAGE_CHECK_UNSTABLE,
	CLUSTER_STORAGE_CHECK_INVALID_SELF,
	CLUSTER_STORAGE_CHECK_PROVIDER,
	CLUSTER_STORAGE_CHECK_INCOMPLETE,
	CLUSTER_STORAGE_CHECK_CLOCK_BEFORE_SAMPLE,
	CLUSTER_STORAGE_CHECK_EXPIRED,
	CLUSTER_STORAGE_CHECK_SELF_ABSENT,
	CLUSTER_STORAGE_CHECK_TARGET_ABSENT
} ClusterStorageCheckResult;

/* Caller-owned evidence from this check, never an admission token. An odd
 * sequence attempt has no second sample; sequence_after then equals before.
 * If stable is false, view is zero and no current-time sample was taken. */
typedef struct ClusterStorageQuorumCheck {
	ClusterStorageCheckResult result;
	int target_node;
	int self_node;
	bool stable;
	uint32 attempts;
	uint32 sequence_before;
	uint32 sequence_after;
	uint64 now_us;
	ClusterStorageQuorumView view;
} ClusterStorageQuorumCheck;

extern bool cluster_storage_quorum_parse_nodes(const char *text, const uint64 configured[2],
											   uint32 map[CLUSTER_MAX_NODES]);
extern bool cluster_storage_quorum_decode_component(ClusterStorageQuorumView *out, uint32 ring_node,
													uint64 ring_sequence, uint32 quorate,
													const uint32 *ids, uint32 count,
													uint32 local_id, int self_node,
													const uint32 map[CLUSTER_MAX_NODES]);
extern void cluster_storage_quorum_attach(ClusterStorageQuorumState *state, bool initialize);
extern uint64 cluster_storage_quorum_now_us(void);
extern void cluster_storage_quorum_refresh(uint64 now_us, uint64 duration_us);
extern bool cluster_storage_quorum_snapshot(ClusterStorageQuorumView *out);
extern bool cluster_storage_quorum_allows_node(int node_id);
extern bool cluster_storage_quorum_check_node(int node_id, ClusterStorageQuorumCheck *out);
extern bool cluster_storage_quorum_allows_members(uint64 members_lo, uint64 members_hi);
extern void cluster_storage_corosync_sample(ClusterStorageQuorumView *out);
extern void cluster_storage_quorum_check_sql(void);
extern void cluster_storage_quorum_check_interrupts(void);

#endif /* CLUSTER_STORAGE_QUORUM_H */
