/*-------------------------------------------------------------------------
 *
 * cluster_sf_dep.h
 *	  pgrac spec-6.2 Smart Fusion dependency vector substrate.
 *
 * Spec-6.2 uses per-origin dependency vectors to make early Cache Fusion block
 * transfer safe: a block may be usable in shared buffers before every upstream
 * origin has durably flushed the WAL it depends on, but the block must not be
 * flushed to shared storage and a writer that consumed it must not commit until
 * every vector entry is independently observed durable.
 *
 * This header exposes the bounded vector math, the receiver-side dependency
 * store API, the transaction-local touched-dep API, and the observability
 * counters used by pg_cluster_state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_sf_dep.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SF_DEP_H
#define CLUSTER_SF_DEP_H

#include "access/xlogdefs.h"
#include "c.h"
#include "cluster/cluster_ic_envelope.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"

#define CLUSTER_SF_DEP_MAX_ORIGINS 16
#define CLUSTER_SF_DURABLE_GOSSIP_VERSION 1

typedef struct ClusterSfDurableGossipMsg {
	uint16 msg_version;
	uint16 flags;
	int32 origin_node;
	uint64 durable_lsn;
} ClusterSfDurableGossipMsg;

StaticAssertDecl(sizeof(ClusterSfDurableGossipMsg) == 16,
				 "spec-6.2 durable-LSN gossip wire payload must be 16 bytes");

typedef struct ClusterSfDepVec {
	XLogRecPtr required[CLUSTER_SF_DEP_MAX_ORIGINS];
} ClusterSfDepVec;

typedef struct ClusterSfDepEntry {
	BufferTag tag;
	ClusterSfDepVec vec;
	uint64 installed_scn;
} ClusterSfDepEntry;

static inline void
cluster_sf_dep_vec_reset(ClusterSfDepVec *vec)
{
	int i;

	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
		vec->required[i] = InvalidXLogRecPtr;
}

static inline bool
cluster_sf_dep_origin_valid(int32 origin)
{
	return origin >= 0 && origin < CLUSTER_SF_DEP_MAX_ORIGINS;
}

static inline bool
cluster_sf_dep_vec_is_empty(const ClusterSfDepVec *vec)
{
	int i;

	if (vec == NULL)
		return true;
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
		if (!XLogRecPtrIsInvalid(vec->required[i]))
			return false;
	}
	return true;
}

static inline bool
cluster_sf_dep_vec_set(ClusterSfDepVec *vec, int32 origin, XLogRecPtr required_lsn)
{
	if (vec == NULL || !cluster_sf_dep_origin_valid(origin) || XLogRecPtrIsInvalid(required_lsn))
		return false;
	if (XLogRecPtrIsInvalid(vec->required[origin]) || required_lsn > vec->required[origin])
		vec->required[origin] = required_lsn;
	return true;
}

static inline bool
cluster_sf_dep_vec_union(ClusterSfDepVec *dst, const ClusterSfDepVec *src)
{
	bool changed = false;
	int i;

	if (dst == NULL || src == NULL)
		return false;
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
		XLogRecPtr required = src->required[i];

		if (XLogRecPtrIsInvalid(required))
			continue;
		if (XLogRecPtrIsInvalid(dst->required[i]) || required > dst->required[i]) {
			dst->required[i] = required;
			changed = true;
		}
	}
	return changed;
}

static inline bool
cluster_sf_dep_vec_clear_durable(ClusterSfDepVec *vec, int32 origin, XLogRecPtr durable_lsn)
{
	if (vec == NULL || !cluster_sf_dep_origin_valid(origin) || XLogRecPtrIsInvalid(durable_lsn)
		|| XLogRecPtrIsInvalid(vec->required[origin]) || vec->required[origin] > durable_lsn)
		return false;
	vec->required[origin] = InvalidXLogRecPtr;
	return true;
}

extern Size cluster_sf_dep_shmem_size(void);
extern void cluster_sf_dep_shmem_init(void);
extern void cluster_sf_dep_shmem_register(void);
extern void cluster_sf_dep_register_ic_msg_types(void);

extern void cluster_sf_dep_install_vec(BufferTag tag, const ClusterSfDepVec *vec);
extern bool cluster_sf_dep_lookup_tag(const BufferTag *tag, ClusterSfDepVec *out_vec);
extern bool cluster_sf_dep_is_pending(Buffer buffer, ClusterSfDepVec *out_vec);
extern bool cluster_sf_dep_vec_for_ship(Buffer buffer, ClusterSfDepVec *out_vec);
extern void cluster_sf_dep_clear_durable(int32 origin, XLogRecPtr durable_lsn);
extern int cluster_sf_dep_suspect_origin_dead(int32 origin);

extern void cluster_sf_observe_origin_durable_lsn(int32 origin, XLogRecPtr durable_lsn);
extern XLogRecPtr cluster_sf_observed_origin_durable_lsn(int32 origin);
extern void cluster_sf_publish_origin_durable_lsn(void);
extern void cluster_sf_note_peer_hello_capabilities(int32 peer_id, uint32 capabilities);
extern bool cluster_sf_peer_supports_reply_v2(int32 peer_id);
/* spec-5.22d D4-6: did the peer's HELLO advertise the kind-4 authority-serve
 * protocol capability?  Deliberately NOT gated on any local GUC (unlike the
 * smart-fusion query above): the bit answers "can that binary parse kind 4";
 * the runtime arm gates live elsewhere. */
extern bool cluster_peer_supports_undo_authority_serve(int32 peer_id);
extern void cluster_sf_handle_durable_gossip(const ClusterICEnvelope *env, const void *payload);

extern void cluster_sf_note_dep_touched(Buffer buffer);
extern bool cluster_sf_xact_pending_deps(ClusterSfDepVec *out_vec);
extern void cluster_sf_xact_reset_deps(void);
extern void cluster_sf_xact_commit_brake(void);

extern bool cluster_sf_dep_buffer_flush_blocked(BufferDesc *buf);

extern uint64 cluster_sf_dep_install_count(void);
extern uint64 cluster_sf_dep_touch_count(void);
extern uint64 cluster_sf_dep_dbwr_brake_count(void);
extern uint64 cluster_sf_dep_commit_brake_count(void);
extern uint64 cluster_sf_dep_commit_brake_wait_us(void);
extern uint64 cluster_sf_dep_origin_suspect_count(void);
extern uint64 cluster_sf_dep_lost_failclosed_count(void);
extern uint64 cluster_sf_dep_retry_failclosed_count(void);
extern void cluster_sf_dep_note_lost_failclosed(void);
extern void cluster_sf_dep_note_retry_failclosed(void);

#endif /* CLUSTER_SF_DEP_H */
