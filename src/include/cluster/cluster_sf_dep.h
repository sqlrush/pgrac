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

/*
 * Per-peer HELLO capability record (spec-2.2 additive amendment; spec-5.22e
 * D5 prereq).  Capability state is a property of the CONNECTION that carried
 * the HELLO / PEER_CAPS_REPLY, so the record binds the learned bits to the
 * transport connection generation (tier1: the peer's reconnect_count while
 * that connection was established).  A clear only applies when the caller's
 * generation matches the recorded one: a defensive close of a failed dial or
 * of an OLDER connection can never wipe the surviving connection's record
 * (forward hazard for the 7.3 multi-channel plane).  The helpers are pure so
 * cluster_unit can lock the matrix; cluster_sf_dep.c wraps them in the store
 * LWLock.
 */
typedef struct ClusterSfPeerCap {
	uint32 bits;	   /* learned HELLO capability word (0 when !valid) */
	uint32 generation; /* connection generation at learn time */
	bool valid;		   /* record live?  false reads as "no capability" */
} ClusterSfPeerCap;

static inline void
cluster_sf_peer_cap_note(ClusterSfPeerCap *cap, uint32 bits, uint32 generation)
{
	cap->bits = bits;
	cap->generation = generation;
	cap->valid = true;
}

static inline uint32
cluster_sf_peer_cap_bits(const ClusterSfPeerCap *cap)
{
	return cap->valid ? cap->bits : 0;
}

/* Snapshot the capability-record generation that authenticated every
 * requested bit.  Tier1 records are CONTROL-owned; RDMA records use their
 * registered generation 0 convention.  False always zeroes the output. */
static inline bool
cluster_sf_peer_cap_generation_for_bits(const ClusterSfPeerCap *cap, uint32 required_bits,
										uint32 *generation)
{
	if (generation != NULL)
		*generation = 0;
	if (cap == NULL || !cap->valid || required_bits == 0
		|| (cap->bits & required_bits) != required_bits)
		return false;
	if (generation != NULL)
		*generation = cap->generation;
	return true;
}

/* review P0-2: one record-coherent sample of a capability family.  True iff
 * every required bit is present on the CURRENT connection's record; the
 * optional-bit presence and the record generation come from the SAME read,
 * so a caller sampling this on both sides of an authority pass rejects any
 * reconnect in between (the generation moves) instead of pairing a stale
 * connection's optional bit with a fresh session.  False zeroes both
 * outputs. */
static inline bool
cluster_sf_peer_cap_family_sample(const ClusterSfPeerCap *cap, uint32 required_bits,
								  uint32 optional_bits, bool *optional_out, uint32 *generation_out)
{
	if (optional_out != NULL)
		*optional_out = false;
	if (!cluster_sf_peer_cap_generation_for_bits(cap, required_bits, generation_out))
		return false;
	if (optional_out != NULL)
		*optional_out = (cluster_sf_peer_cap_bits(cap) & optional_bits) == optional_bits
						&& optional_bits != 0;
	return true;
}

/* Returns true iff the record was live for exactly this generation and got
 * cleared; a mismatch (older/newer generation, already invalid) is a no-op. */
static inline bool
cluster_sf_peer_cap_invalidate_gen(ClusterSfPeerCap *cap, uint32 generation)
{
	if (!cap->valid || cap->generation != generation)
		return false;
	cap->valid = false;
	cap->bits = 0;
	return true;
}

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
/* spec-2.2 additive amendment (spec-5.22e D5 prereq): capability learn +
 * clear are generation-bound (see ClusterSfPeerCap above).  Learn sites pass
 * the connection generation that carried the HELLO / PEER_CAPS_REPLY; the
 * tier1 close funnel passes the closing connection's generation so only the
 * matching record is invalidated.  RDMA verify passes generation 0 (its
 * lifecycle never consumed the tier1 clear even before this amendment;
 * registered tier1-only boundary). */
extern void cluster_sf_note_peer_hello_capabilities_gen(int32 peer_id, uint32 capabilities,
														uint32 generation);
extern bool cluster_sf_peer_supports_reply_v2(int32 peer_id);
/* spec-5.22d D4-6: did the peer's HELLO advertise the kind-4 authority-serve
 * protocol capability?  Deliberately NOT gated on any local GUC (unlike the
 * smart-fusion query above): the bit answers "can that binary parse kind 4";
 * the runtime arm gates live elsewhere. */
extern bool cluster_peer_supports_undo_authority_serve(int32 peer_id);
/* spec-5.22e D5-2: undo-horizon report capability (connection-bound; see
 * cluster_sf_note_peer_disconnected_gen) + the close-funnel reset hooks. */
extern bool cluster_sf_peer_supports_undo_horizon(int32 peer_id);
/* TT lane (S3 idle-peer floor pin): idle-unconstrained sentinel report
 * capability (connection-bound, same discipline).  SENDER-side gate only:
 * an old peer keeps receiving the conservative clock sample. */
extern bool cluster_sf_peer_supports_undo_horizon_idle(int32 peer_id);
/* GCS-race round-2 review F6: GCS completion-proof capability
 * (connection-bound, same discipline).  An unknown/old/reconnecting peer
 * reads false, which every consumer treats in the SAFE direction: the
 * requester withholds DONE (TTL backstop), the master pins the legacy
 * protocol-maximum lifetime instead of trusting a hint. */
extern bool cluster_sf_peer_supports_gcs_done(int32 peer_id);
/* GCS-race round-3 P0-1: xid wrap-barrier capability (connection-bound, same
 * discipline).  False for an unknown/old/reconnecting peer, which fails the
 * barrier SAFE: the coordinator withholds DISABLE, the ack bitmap never
 * fills, and the allocation gate keeps refusing epoch>=1 candidates. */
extern bool cluster_sf_peer_supports_xid_native_disable(int32 peer_id);
extern bool cluster_sf_peer_supports_xid_authority_flock(int32 peer_id);
extern bool cluster_sf_peer_supports_gcs_inval_busy(int32 peer_id);
extern bool cluster_sf_peer_supports_pcm_x_convert(int32 peer_id);
extern bool cluster_sf_peer_supports_pcm_x_rebase(int32 peer_id);
/* review P0-2: lock-coherent (CONVERT supported, REBASE bit, record
 * generation) triple for the formation collector's double sample. */
extern bool cluster_sf_peer_pcm_x_capability_sample(int32 peer_id, bool *rebase_out,
													uint32 *generation_out);
extern bool cluster_sf_peer_pcm_x_connection_generation(int32 peer_id, uint32 *generation);
extern void cluster_sf_note_peer_disconnected_gen(int32 peer_id, uint32 generation);
extern void cluster_sf_note_peer_disconnected(int32 peer_id);
extern const char *cluster_sf_peer_capabilities_summary(void);
extern uint64 cluster_sf_caps_reply_reject_count(void);
extern void cluster_sf_note_caps_reply_rejected(void);
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
