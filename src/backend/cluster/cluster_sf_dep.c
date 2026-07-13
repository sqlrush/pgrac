/*-------------------------------------------------------------------------
 *
 * cluster_sf_dep.c
 *	  pgrac spec-6.2 Smart Fusion dependency vector substrate.
 *
 * The data path is deliberately conservative.  With cluster.smart_fusion=off
 * every exported runtime hook is a no-op.  With it on, missing dependency
 * evidence fails closed: dependent buffers are not flushed, and commit brake
 * timeouts abort before the commit record is emitted.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_sf_dep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"

typedef struct ClusterSfDepSlot {
	bool in_use;
	ClusterSfDepEntry entry;
} ClusterSfDepSlot;

typedef struct ClusterSfDepShared {
	LWLock lock;
	int max_entries;
	XLogRecPtr origin_durable[CLUSTER_SF_DEP_MAX_ORIGINS];
	/* generation-bound per-peer HELLO capability records (spec-2.2 additive
	 * amendment; spec-5.22e D5 prereq) */
	ClusterSfPeerCap peer_capabilities[CLUSTER_MAX_NODES];
	pg_atomic_uint64 caps_reply_reject_count;
	pg_atomic_uint64 install_count;
	pg_atomic_uint64 touch_count;
	pg_atomic_uint64 dbwr_brake_count;
	pg_atomic_uint64 commit_brake_count;
	pg_atomic_uint64 commit_brake_wait_us;
	pg_atomic_uint64 origin_suspect_count;
	pg_atomic_uint64 dep_lost_failclosed_count;
	pg_atomic_uint64 retry_failclosed_count;
	ClusterSfDepSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterSfDepShared;

static ClusterSfDepShared *ClusterSfDep = NULL;
static ClusterSfDepVec cluster_sf_xact_vec;

static int cluster_sf_dep_find_locked(const BufferTag *tag);
static int cluster_sf_dep_find_free_locked(void);
static void cluster_sf_dep_validate_or_fail(const ClusterSfDepVec *vec);
static void cluster_sf_xact_prune_durable(ClusterSfDepVec *vec);
static void cluster_sf_dep_count_lost_failclosed(void);
static void cluster_sf_dep_count_retry_failclosed(void);

Size
cluster_sf_dep_shmem_size(void)
{
	int max_entries;

	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return 0;

	/*
	 * spec-5.22e D5-2 (latent-bug fix): the header carries the HELLO
	 * capability store (peer_capabilities + lock), which the undo-horizon
	 * sender/fold, the D4 authority routing gate AND smart fusion all
	 * consume -- it must exist in EVERY cluster build.  Sizing the whole
	 * region to zero under smart_fusion=off (the default) silently killed
	 * every capability query (ClusterSfDep == NULL reads as "no
	 * capability"); the D4 peer-authority gate never noticed only because
	 * its 2-node e2e covers the SELF-authority leg.  Only the dep-vector
	 * slot array stays smart-fusion-gated.
	 */
	if (!cluster_smart_fusion)
		return MAXALIGN(offsetof(ClusterSfDepShared, slots));

	max_entries = NBuffers > 0 ? NBuffers : 1;
	return MAXALIGN(offsetof(ClusterSfDepShared, slots)
					+ (Size)max_entries * sizeof(ClusterSfDepSlot));
}

void
cluster_sf_dep_shmem_init(void)
{
	bool found;
	Size sz = cluster_sf_dep_shmem_size();

	if (sz == 0)
		return;

	ClusterSfDep
		= (ClusterSfDepShared *)ShmemInitStruct("pgrac cluster smart fusion deps", sz, &found);
	if (!found) {
		int i;

		LWLockInitialize(&ClusterSfDep->lock, LWTRANCHE_CLUSTER_SMART_FUSION);
		/* slots exist only under smart_fusion (see shmem_size) */
		ClusterSfDep->max_entries = cluster_smart_fusion ? (NBuffers > 0 ? NBuffers : 1) : 0;
		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
			ClusterSfDep->origin_durable[i] = InvalidXLogRecPtr;
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			ClusterSfDep->peer_capabilities[i].bits = 0;
			ClusterSfDep->peer_capabilities[i].generation = 0;
			ClusterSfDep->peer_capabilities[i].valid = false;
		}
		pg_atomic_init_u64(&ClusterSfDep->caps_reply_reject_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->install_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->touch_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->dbwr_brake_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->commit_brake_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->commit_brake_wait_us, 0);
		pg_atomic_init_u64(&ClusterSfDep->origin_suspect_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->dep_lost_failclosed_count, 0);
		pg_atomic_init_u64(&ClusterSfDep->retry_failclosed_count, 0);
		for (i = 0; i < ClusterSfDep->max_entries; i++) {
			ClusterSfDep->slots[i].in_use = false;
			cluster_sf_dep_vec_reset(&ClusterSfDep->slots[i].entry.vec);
		}
	}
}

static const ClusterShmemRegion cluster_sf_dep_region = {
	.name = "pgrac cluster smart fusion deps",
	.size_fn = cluster_sf_dep_shmem_size,
	.init_fn = cluster_sf_dep_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_sf_dep",
	.reserved_flags = 0,
};

void
cluster_sf_dep_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_sf_dep_region);
}

/*
 * cluster_sf_note_peer_hello_capabilities_gen
 *
 * spec-2.2 additive amendment (spec-5.22e D5 prereq): record the peer's
 * verified HELLO capability word, bound to the connection generation that
 * carried it (tier1: the peer's reconnect_count while the connection was
 * established).  Called from the acceptor's HELLO verify, from the dialer's
 * PEER_CAPS_REPLY handler, and from RDMA verify (generation 0; tier1-only
 * generation boundary, see cluster_sf_dep.h).
 */
void
cluster_sf_note_peer_hello_capabilities_gen(int32 peer_id, uint32 capabilities, uint32 generation)
{
	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ClusterSfDep->lock, LW_EXCLUSIVE);
	cluster_sf_peer_cap_note(&ClusterSfDep->peer_capabilities[peer_id], capabilities, generation);
	LWLockRelease(&ClusterSfDep->lock);
}

bool
cluster_sf_peer_supports_reply_v2(int32 peer_id)
{
	uint32 capabilities;

	if (!cluster_smart_fusion || ClusterSfDep == NULL || peer_id < 0
		|| peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_SMART_FUSION_REPLY_V2) != 0;
}

/*
 * cluster_peer_supports_undo_authority_serve
 *
 * spec-5.22d D4-6 capability gate: true iff the peer's verified HELLO
 * advertised the kind-4 authority-serve protocol bit.  NOT gated on any
 * local GUC (see cluster_sf_dep.h) — an unknown/old peer reads as false and
 * the caller's authority leg fails closed (Rule 8.A).
 */
bool
cluster_peer_supports_undo_authority_serve(int32 peer_id)
{
	uint32 capabilities;

	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_UNDO_AUTHORITY_SERVE_V1) != 0;
}

/*
 * cluster_sf_peer_supports_undo_horizon
 *
 * spec-5.22e D5-2 capability gate: true iff the peer's verified HELLO on the
 * CURRENT connection advertised the undo-horizon report protocol bit.  Same
 * no-local-GUC discipline as the authority-serve query above.  Combined with
 * the disconnect reset below this is connection-bound (Q1' amend): a closed
 * or not-yet-HELLOed connection reads as false, so the sender skips the peer
 * and the fold stalls with NOCAP instead of trusting a stale capability.
 */
bool
cluster_sf_peer_supports_undo_horizon(int32 peer_id)
{
	uint32 capabilities;

	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_UNDO_HORIZON_V1) != 0;
}

/*
 * cluster_sf_peer_supports_gcs_done
 *
 * GCS-race round-2 review F6: true iff the peer's verified HELLO on the
 * CURRENT connection advertised the GCS completion-proof protocol bit.
 * Same connection-bound, no-local-GUC discipline as the queries above; an
 * unknown/old/reconnecting peer reads false and every consumer degrades in
 * the safe direction (requester withholds DONE -> TTL backstop; master
 * pins the legacy protocol-maximum lifetime instead of trusting a hint).
 */
bool
cluster_sf_peer_supports_gcs_done(int32 peer_id)
{
	uint32 capabilities;

	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_GCS_DONE_V1) != 0;
}

/*
 * cluster_sf_peer_supports_xid_native_disable
 *
 * GCS-race round-3 P0-1: true iff the peer's verified HELLO on the CURRENT
 * connection advertised the xid wrap-barrier protocol bit.  Same
 * connection-bound, no-local-GUC discipline as the queries above; an
 * unknown/old/reconnecting peer reads false and the barrier degrades in
 * the safe direction (DISABLE withheld, ack bitmap never fills, the
 * allocation gate keeps refusing epoch>=1 candidates fail-closed).
 */
bool
cluster_sf_peer_supports_xid_native_disable(int32 peer_id)
{
	uint32 capabilities;

	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1) != 0;
}

/*
 * cluster_sf_peer_supports_xid_authority_flock
 *
 * GCS-race round-4 P0-1: true iff the peer's verified HELLO on the CURRENT
 * connection advertised the authority flock/stamped-magic protocol bit
 * (0x40) -- DISTINCT from the barrier bit above, which a round-3b
 * lock-free-writer binary also advertises.  Connection-bound: a declared
 * but unreachable peer reads false, and the wrap barrier holds its gate
 * (such a peer could be exactly an old writer mid-boot).
 */
bool
cluster_sf_peer_supports_xid_authority_flock(int32 peer_id)
{
	uint32 capabilities;

	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_XID_AUTHORITY_FLOCK_V2) != 0;
}

/*
 * cluster_sf_peer_supports_gcs_inval_busy
 *
 * PGRAC ownership-generation wave (ruling ②): true iff the peer's verified
 * HELLO on the CURRENT connection advertised the INVALIDATE RETRYABLE_BUSY
 * negative-ACK protocol bit.  Same connection-bound, no-local-GUC discipline
 * as the queries above; an unknown/old/reconnecting peer reads false and the
 * holder degrades in the safe direction (round-5 park, the master burns its
 * timeout exactly as the pre-BUSY protocol did).
 */
bool
cluster_sf_peer_supports_gcs_inval_busy(int32 peer_id)
{
	uint32 capabilities;

	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	capabilities = cluster_sf_peer_cap_bits(&ClusterSfDep->peer_capabilities[peer_id]);
	LWLockRelease(&ClusterSfDep->lock);
	return (capabilities & PGRAC_IC_HELLO_CAP_GCS_INVAL_BUSY_V1) != 0;
}

/*
 * cluster_sf_note_peer_disconnected_gen
 *
 * spec-5.22e D5-2 (Q1' amend) + spec-2.2 additive amendment: capability
 * state is a property of the CONNECTION that carried the HELLO, not of the
 * peer's identity.  Called from the transport close funnel with the CLOSING
 * connection's generation; clears the record only when the generations
 * match, so a defensive close of a failed dial or of an older connection
 * never wipes a record the surviving connection installed.  The next
 * verified HELLO repopulates.  Clearing is uniformly the conservative
 * direction for every existing consumer: smart-fusion falls back to v1
 * replies, the D4 authority leg fails closed, and the D5 horizon
 * sender/fold skip/stall.
 */
void
cluster_sf_note_peer_disconnected_gen(int32 peer_id, uint32 generation)
{
	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ClusterSfDep->lock, LW_EXCLUSIVE);
	(void)cluster_sf_peer_cap_invalidate_gen(&ClusterSfDep->peer_capabilities[peer_id], generation);
	LWLockRelease(&ClusterSfDep->lock);
}

/*
 * cluster_sf_note_peer_disconnected
 *
 * Unconditional clear -- the fail-closed fallback for close paths that have
 * no connection-generation notion (tier1 keeps it for the Tier1Shmem==NULL
 * defensive branch only).  Losing a live capability record is always the
 * conservative direction; trusting a stale one never is.
 */
void
cluster_sf_note_peer_disconnected(int32 peer_id)
{
	if (ClusterSfDep == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	LWLockAcquire(&ClusterSfDep->lock, LW_EXCLUSIVE);
	ClusterSfDep->peer_capabilities[peer_id].bits = 0;
	ClusterSfDep->peer_capabilities[peer_id].valid = false;
	LWLockRelease(&ClusterSfDep->lock);
}

/*
 * cluster_sf_peer_capabilities_summary
 *
 * Dump-surface line for pg_cluster_state ("ic" / "peer_capabilities"):
 * one "n<id>:bits=0x<hex>,gen=<gen>,v=<valid>" token per declared peer,
 * space-separated.  Diagnostic only; takes the store lock shared.
 */
const char *
cluster_sf_peer_capabilities_summary(void)
{
	static char buf[CLUSTER_MAX_NODES * 48];
	size_t off = 0;
	int i;

	buf[0] = '\0';
	if (ClusterSfDep == NULL)
		return buf;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_MAX_NODES && off + 48 < sizeof(buf); i++) {
		const ClusterSfPeerCap *cap = &ClusterSfDep->peer_capabilities[i];

		if (i == cluster_node_id || cluster_conf_lookup_node(i) == NULL)
			continue;
		off += (size_t)snprintf(buf + off, sizeof(buf) - off, "%sn%d:bits=0x%X,gen=%u,v=%d",
								off > 0 ? " " : "", i, cap->valid ? cap->bits : 0, cap->generation,
								cap->valid ? 1 : 0);
	}
	LWLockRelease(&ClusterSfDep->lock);
	return buf;
}

/* PEER_CAPS_REPLY frames dropped by the dialer-side validation core
 * (spec-2.2 additive amendment §3.4: drop + count, never reconnect). */
void
cluster_sf_note_caps_reply_rejected(void)
{
	if (ClusterSfDep != NULL)
		pg_atomic_fetch_add_u64(&ClusterSfDep->caps_reply_reject_count, 1);
}

uint64
cluster_sf_caps_reply_reject_count(void)
{
	if (ClusterSfDep == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterSfDep->caps_reply_reject_count);
}

void
cluster_sf_handle_durable_gossip(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterSfDurableGossipMsg *msg = (const ClusterSfDurableGossipMsg *)payload;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(ClusterSfDurableGossipMsg))
		return;
	if (msg->msg_version != CLUSTER_SF_DURABLE_GOSSIP_VERSION)
		return;
	if (msg->origin_node != (int32)env->source_node_id)
		return;
	cluster_sf_observe_origin_durable_lsn(msg->origin_node, (XLogRecPtr)msg->durable_lsn);
}

void
cluster_sf_dep_register_ic_msg_types(void)
{
	static const ClusterICMsgTypeInfo durable_gossip_info = {
		.msg_type = PGRAC_IC_MSG_SMART_FUSION_DURABLE,
		.name = "smart_fusion_durable",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = cluster_sf_handle_durable_gossip,
	};

	cluster_ic_register_msg_type(&durable_gossip_info);
}

static int
cluster_sf_dep_find_locked(const BufferTag *tag)
{
	int i;

	for (i = 0; i < ClusterSfDep->max_entries; i++) {
		if (ClusterSfDep->slots[i].in_use
			&& BufferTagsEqual(&ClusterSfDep->slots[i].entry.tag, tag))
			return i;
	}
	return -1;
}

static int
cluster_sf_dep_find_free_locked(void)
{
	int i;

	for (i = 0; i < ClusterSfDep->max_entries; i++) {
		if (!ClusterSfDep->slots[i].in_use)
			return i;
	}
	return -1;
}

static void
cluster_sf_dep_count_lost_failclosed(void)
{
	cluster_sf_dep_note_lost_failclosed();
}

static void
cluster_sf_dep_count_retry_failclosed(void)
{
	cluster_sf_dep_note_retry_failclosed();
}

void
cluster_sf_dep_note_lost_failclosed(void)
{
	if (ClusterSfDep != NULL)
		pg_atomic_fetch_add_u64(&ClusterSfDep->dep_lost_failclosed_count, 1);
}

void
cluster_sf_dep_note_retry_failclosed(void)
{
	if (ClusterSfDep != NULL)
		pg_atomic_fetch_add_u64(&ClusterSfDep->retry_failclosed_count, 1);
}

static void
cluster_sf_dep_validate_or_fail(const ClusterSfDepVec *vec)
{
	if (vec == NULL || cluster_sf_dep_vec_is_empty(vec)) {
		cluster_sf_dep_count_lost_failclosed();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SMART_FUSION_DEP_LOST),
						errmsg("Smart Fusion dependency vector is missing")));
	}
}

void
cluster_sf_dep_install_vec(BufferTag tag, const ClusterSfDepVec *vec)
{
	int idx;

	if (!cluster_smart_fusion)
		return;
	if (ClusterSfDep == NULL) {
		cluster_sf_dep_count_lost_failclosed();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SMART_FUSION_DEP_LOST),
						errmsg("Smart Fusion dependency store is not available")));
	}
	cluster_sf_dep_validate_or_fail(vec);

	LWLockAcquire(&ClusterSfDep->lock, LW_EXCLUSIVE);
	idx = cluster_sf_dep_find_locked(&tag);
	if (idx < 0)
		idx = cluster_sf_dep_find_free_locked();
	if (idx < 0) {
		LWLockRelease(&ClusterSfDep->lock);
		cluster_sf_dep_count_lost_failclosed();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SMART_FUSION_DEP_LOST),
						errmsg("Smart Fusion dependency store is full")));
	}
	ClusterSfDep->slots[idx].in_use = true;
	ClusterSfDep->slots[idx].entry.tag = tag;
	cluster_sf_dep_vec_union(&ClusterSfDep->slots[idx].entry.vec, vec);
	ClusterSfDep->slots[idx].entry.installed_scn = 0;
	pg_atomic_fetch_add_u64(&ClusterSfDep->install_count, 1);
	LWLockRelease(&ClusterSfDep->lock);
}

bool
cluster_sf_dep_lookup_tag(const BufferTag *tag, ClusterSfDepVec *out_vec)
{
	int idx;
	bool found = false;

	if (out_vec != NULL)
		cluster_sf_dep_vec_reset(out_vec);
	if (!cluster_smart_fusion || ClusterSfDep == NULL || tag == NULL)
		return false;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	idx = cluster_sf_dep_find_locked(tag);
	if (idx >= 0 && !cluster_sf_dep_vec_is_empty(&ClusterSfDep->slots[idx].entry.vec)) {
		if (out_vec != NULL)
			*out_vec = ClusterSfDep->slots[idx].entry.vec;
		found = true;
	}
	LWLockRelease(&ClusterSfDep->lock);
	return found;
}

bool
cluster_sf_dep_is_pending(Buffer buffer, ClusterSfDepVec *out_vec)
{
	BufferDesc *buf;

	if (out_vec != NULL)
		cluster_sf_dep_vec_reset(out_vec);
	if (!cluster_smart_fusion || !BufferIsValid(buffer) || BufferIsLocal(buffer))
		return false;

	buf = GetBufferDescriptor(buffer - 1);
	return cluster_sf_dep_lookup_tag(&buf->tag, out_vec);
}

bool
cluster_sf_dep_vec_for_ship(Buffer buffer, ClusterSfDepVec *out_vec)
{
	return cluster_sf_dep_is_pending(buffer, out_vec);
}

void
cluster_sf_dep_clear_durable(int32 origin, XLogRecPtr durable_lsn)
{
	int i;

	if (!cluster_sf_dep_origin_valid(origin) || XLogRecPtrIsInvalid(durable_lsn))
		return;
	if (ClusterSfDep == NULL)
		return;

	LWLockAcquire(&ClusterSfDep->lock, LW_EXCLUSIVE);
	if (XLogRecPtrIsInvalid(ClusterSfDep->origin_durable[origin])
		|| durable_lsn > ClusterSfDep->origin_durable[origin])
		ClusterSfDep->origin_durable[origin] = durable_lsn;

	for (i = 0; i < ClusterSfDep->max_entries; i++) {
		if (!ClusterSfDep->slots[i].in_use)
			continue;
		(void)cluster_sf_dep_vec_clear_durable(&ClusterSfDep->slots[i].entry.vec, origin,
											   durable_lsn);
		if (cluster_sf_dep_vec_is_empty(&ClusterSfDep->slots[i].entry.vec))
			ClusterSfDep->slots[i].in_use = false;
	}
	LWLockRelease(&ClusterSfDep->lock);
}

int
cluster_sf_dep_suspect_origin_dead(int32 origin)
{
	int i;
	int suspect = 0;

	if (!cluster_sf_dep_origin_valid(origin) || ClusterSfDep == NULL)
		return 0;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	for (i = 0; i < ClusterSfDep->max_entries; i++) {
		if (ClusterSfDep->slots[i].in_use
			&& !XLogRecPtrIsInvalid(ClusterSfDep->slots[i].entry.vec.required[origin]))
			suspect++;
	}
	if (suspect > 0)
		pg_atomic_fetch_add_u64(&ClusterSfDep->origin_suspect_count, (uint64)suspect);
	LWLockRelease(&ClusterSfDep->lock);
	return suspect;
}

void
cluster_sf_observe_origin_durable_lsn(int32 origin, XLogRecPtr durable_lsn)
{
	cluster_sf_dep_clear_durable(origin, durable_lsn);
}

XLogRecPtr
cluster_sf_observed_origin_durable_lsn(int32 origin)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (!cluster_sf_dep_origin_valid(origin) || ClusterSfDep == NULL)
		return InvalidXLogRecPtr;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	lsn = ClusterSfDep->origin_durable[origin];
	LWLockRelease(&ClusterSfDep->lock);
	return lsn;
}

void
cluster_sf_publish_origin_durable_lsn(void)
{
	ClusterSfDurableGossipMsg msg;
	XLogRecPtr durable_lsn;
	int peer;

	if (!cluster_smart_fusion || !cluster_sf_dep_origin_valid(cluster_node_id))
		return;
	durable_lsn = GetFlushRecPtr(NULL);
	cluster_sf_observe_origin_durable_lsn(cluster_node_id, durable_lsn);

	memset(&msg, 0, sizeof(msg));
	msg.msg_version = CLUSTER_SF_DURABLE_GOSSIP_VERSION;
	msg.origin_node = cluster_node_id;
	msg.durable_lsn = (uint64)durable_lsn;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		if (peer == cluster_node_id || cluster_conf_lookup_node(peer) == NULL)
			continue;
		if (!cluster_sf_peer_supports_reply_v2(peer))
			continue;
		(void)cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_SMART_FUSION_DURABLE,
													   (uint32)peer, &msg, sizeof(msg));
	}
}

void
cluster_sf_note_dep_touched(Buffer buffer)
{
	ClusterSfDepVec vec;

	if (!cluster_smart_fusion)
		return;
	if (!cluster_sf_dep_is_pending(buffer, &vec))
		return;
	cluster_sf_dep_vec_union(&cluster_sf_xact_vec, &vec);
	if (ClusterSfDep != NULL)
		pg_atomic_fetch_add_u64(&ClusterSfDep->touch_count, 1);
}

static void
cluster_sf_xact_prune_durable(ClusterSfDepVec *vec)
{
	int i;

	if (vec == NULL || ClusterSfDep == NULL)
		return;

	LWLockAcquire(&ClusterSfDep->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
		XLogRecPtr durable;

		if (XLogRecPtrIsInvalid(vec->required[i]))
			continue;
		durable = ClusterSfDep->origin_durable[i];
		if (!XLogRecPtrIsInvalid(durable) && vec->required[i] <= durable)
			vec->required[i] = InvalidXLogRecPtr;
	}
	LWLockRelease(&ClusterSfDep->lock);
}

bool
cluster_sf_xact_pending_deps(ClusterSfDepVec *out_vec)
{
	cluster_sf_xact_prune_durable(&cluster_sf_xact_vec);
	if (out_vec != NULL)
		*out_vec = cluster_sf_xact_vec;
	return !cluster_sf_dep_vec_is_empty(&cluster_sf_xact_vec);
}

void
cluster_sf_xact_reset_deps(void)
{
	cluster_sf_dep_vec_reset(&cluster_sf_xact_vec);
}

void
cluster_sf_xact_commit_brake(void)
{
	TimestampTz deadline;
	TimestampTz start;
	ClusterSfDepVec vec;

	if (!cluster_smart_fusion)
		return;
	if (!cluster_sf_xact_pending_deps(&vec))
		return;

	if (ClusterSfDep == NULL) {
		cluster_sf_dep_count_retry_failclosed();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SMART_FUSION_RETRY),
						errmsg("Smart Fusion commit dependency authority is unavailable")));
	}

	pg_atomic_fetch_add_u64(&ClusterSfDep->commit_brake_count, 1);
	start = GetCurrentTimestamp();
	deadline
		= start + ((TimestampTz)cluster_smart_fusion_commit_brake_timeout_ms) * (TimestampTz)1000;

	for (;;) {
		TimestampTz now;
		long wait_ms;

		cluster_sf_publish_origin_durable_lsn();
		if (!cluster_sf_xact_pending_deps(&vec))
			break;

		CHECK_FOR_INTERRUPTS();
		now = GetCurrentTimestamp();
		if (now >= deadline) {
			pg_atomic_fetch_add_u64(&ClusterSfDep->commit_brake_wait_us, (uint64)(now - start));
			cluster_sf_dep_count_retry_failclosed();
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_SMART_FUSION_RETRY),
					 errmsg("Smart Fusion commit dependency is not durable"),
					 errhint("Retry the transaction after the origin durable LSN advances.")));
		}
		wait_ms = (long)((deadline - now) / 1000);
		if (wait_ms <= 0)
			wait_ms = 1;
		if (wait_ms > cluster_smart_fusion_origin_durable_gossip_ms)
			wait_ms = cluster_smart_fusion_origin_durable_gossip_ms;
		(void)WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
						WAIT_EVENT_CLUSTER_SMART_FUSION_COMMIT_BRAKE);
		ResetLatch(MyLatch);
	}

	pg_atomic_fetch_add_u64(&ClusterSfDep->commit_brake_wait_us,
							(uint64)(GetCurrentTimestamp() - start));
}

bool
cluster_sf_dep_buffer_flush_blocked(BufferDesc *buf)
{
	ClusterSfDepVec vec;

	if (!cluster_smart_fusion)
		return false;
	if (buf == NULL)
		return false;
	if (ClusterSfDep == NULL) {
		return true;
	}
	if (!cluster_sf_dep_lookup_tag(&buf->tag, &vec))
		return false;
	pg_atomic_fetch_add_u64(&ClusterSfDep->dbwr_brake_count, 1);
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_SMART_FUSION_DBWR_BRAKE);
	pgstat_report_wait_end();
	return true;
}

#define SF_DEP_COUNTER_ACCESSOR(fn, field)                                                         \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		if (ClusterSfDep == NULL)                                                                  \
			return 0;                                                                              \
		return pg_atomic_read_u64(&ClusterSfDep->field);                                           \
	}

SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_install_count, install_count)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_touch_count, touch_count)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_dbwr_brake_count, dbwr_brake_count)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_commit_brake_count, commit_brake_count)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_commit_brake_wait_us, commit_brake_wait_us)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_origin_suspect_count, origin_suspect_count)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_lost_failclosed_count, dep_lost_failclosed_count)
SF_DEP_COUNTER_ACCESSOR(cluster_sf_dep_retry_failclosed_count, retry_failclosed_count)
