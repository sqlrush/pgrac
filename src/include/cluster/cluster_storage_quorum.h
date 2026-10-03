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

typedef struct ClusterStorageQuorumView {
	ClusterStorageQuorumReason reason;
	uint32 ring_node;
	uint64 ring_sequence;
	uint64 members[2];
	uint64 sampled_us;
	uint64 expires_us;
	uint64 generation;
} ClusterStorageQuorumView;

/* QVOTEC owns publication. Readers cannot refresh the observation. */
typedef struct ClusterStorageQuorumState {
	pg_atomic_uint32 sequence;
	pg_atomic_uint32 reason;
	pg_atomic_uint32 ring_node;
	uint32 pad;
	pg_atomic_uint64 ring_sequence;
	pg_atomic_uint64 members[2];
	pg_atomic_uint64 sampled_us;
	pg_atomic_uint64 expires_us;
	pg_atomic_uint64 generation;
} ClusterStorageQuorumState;

StaticAssertDecl(sizeof(ClusterStorageQuorumState) == CLUSTER_STORAGE_QUORUM_STATE_BYTES,
				 "storage quorum observation layout");

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
extern bool cluster_storage_quorum_allows_members(uint64 members_lo, uint64 members_hi);
extern void cluster_storage_corosync_sample(ClusterStorageQuorumView *out);
extern void cluster_storage_quorum_check_sql(void);
extern void cluster_storage_quorum_check_interrupts(void);

#endif /* CLUSTER_STORAGE_QUORUM_H */
