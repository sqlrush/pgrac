/*-------------------------------------------------------------------------
 *
 * cluster_tt_status_hint.c
 *	  pgrac cross-node Undo TT status hint — emit + receiver + LMON drain.
 *
 *	  spec-3.2 D3 (NEW;Stage 3 第 2 sub-spec).
 *
 *	  This file backs the cross-node propagation channel that lets each
 *	  node's commit/abort install TT status entries on peer overlays.
 *	  Emit is fire-and-forget (no ack) to keep commit hot path
 *	  unaffected;  drain is LMON-mediated (L172 family);  receiver
 *	  validates msg_version + checksum + epoch + anti-spoof +
 *	  reserved-zero + status range, then directly install_local
 *	  with msg.key (no raw-xid rebuild — HC184).
 *
 *	  spec-3.2 §0.1 hard guardrails inherited:
 *	    F1 no is_xid_local_origin heuristic (forbidden everywhere)
 *	    F2 wire payload embeds full ClusterTTStatusKey (24B) — never
 *	       reconstruct from raw xid
 *	    F3 emit side passes the already-minted key from D5 install_status
 *	       (no rebuild)
 *	    F4 receiver anti-spoof — key.origin == env->source_node_id
 *	    F6 no xid range coalescing — each hint carries 1 exact key
 *
 *	  HC contracts (see cluster_tt_status_hint.h):
 *	    HC184 wire ABI 32B = 8B header + 24B embedded key
 *	    HC185 producer mask LMON only
 *	    HC186 receiver anti-spoof key.origin == source_node_id
 *	    HC187 forward-compat reject — V1 receiver drops V2+ msg
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_status_hint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * Outbound ring entry — kept independent from wire payload to allow
 * future flag bits without bumping wire ABI.
 *
 * spec-3.3 D8: commit_scn carried alongside status; senders post
 * spec-3.3 emit V2 (40B wire) so the drain path needs the SCN.
 */
typedef struct ClusterTTStatusHintOutboundEntry {
	ClusterTTStatusKey key;
	SCN commit_scn;			/* spec-3.3 D8 */
	uint16 status;
	uint16 _pad[3];
} ClusterTTStatusHintOutboundEntry;

typedef struct ClusterTTStatusHintOutboundRing {
	pg_atomic_uint32 head;
	pg_atomic_uint32 tail;
	uint32 capacity;
	LWLockPadded lock;
	ClusterTTStatusHintOutboundEntry slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterTTStatusHintOutboundRing;

typedef struct ClusterTTStatusHintState {
	pg_atomic_uint64 emit_count;
	pg_atomic_uint64 receive_count;
	pg_atomic_uint64 drop_invalid_count;
	pg_atomic_uint64 drop_stale_epoch_count;
	pg_atomic_uint64 drop_unknown_version_count;
	pg_atomic_uint64 install_count;
	pg_atomic_uint64 drop_v1_compat_count;	/* spec-3.3 D9: 7th counter */
} ClusterTTStatusHintState;

static ClusterTTStatusHintOutboundRing *ClusterTTHintOutbound = NULL;
static ClusterTTStatusHintState *ClusterTTHintCounters = NULL;

/* ------------------------------------------------------------ */
/* shmem layout                                                 */
/* ------------------------------------------------------------ */

static Size
hint_outbound_struct_size(int capacity)
{
	return offsetof(ClusterTTStatusHintOutboundRing, slots)
		   + mul_size(sizeof(ClusterTTStatusHintOutboundEntry), capacity);
}

Size
cluster_tt_status_hint_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(hint_outbound_struct_size(cluster_tt_status_hint_outbound_capacity))
		   + MAXALIGN(sizeof(ClusterTTStatusHintState));
}

void
cluster_tt_status_hint_shmem_init(void)
{
	bool found;
	Size ring_sz;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ring_sz = MAXALIGN(hint_outbound_struct_size(cluster_tt_status_hint_outbound_capacity));
	ClusterTTHintOutbound = (ClusterTTStatusHintOutboundRing *)ShmemInitStruct(
		"ClusterTTStatusHintOutbound", ring_sz, &found);
	if (!found) {
		pg_atomic_init_u32(&ClusterTTHintOutbound->head, 0);
		pg_atomic_init_u32(&ClusterTTHintOutbound->tail, 0);
		ClusterTTHintOutbound->capacity = cluster_tt_status_hint_outbound_capacity;
		LWLockInitialize(&ClusterTTHintOutbound->lock.lock, LWTRANCHE_CLUSTER_TT_STATUS);
	}

	ClusterTTHintCounters = (ClusterTTStatusHintState *)ShmemInitStruct(
		"ClusterTTStatusHintState", MAXALIGN(sizeof(ClusterTTStatusHintState)), &found);
	if (!found) {
		pg_atomic_init_u64(&ClusterTTHintCounters->emit_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->receive_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->drop_invalid_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->drop_stale_epoch_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->drop_unknown_version_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->install_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->drop_v1_compat_count, 0);
	}
}

static const ClusterShmemRegion cluster_tt_status_hint_region = {
	.name = "pgrac cluster tt status hint outbound",
	.size_fn = cluster_tt_status_hint_shmem_size,
	.init_fn = cluster_tt_status_hint_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_tt_status_hint",
	.reserved_flags = 0,
};

void
cluster_tt_status_hint_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_tt_status_hint_region);
}

/* ------------------------------------------------------------ */
/* IC msg type registration (HC185 producer mask)               */
/* ------------------------------------------------------------ */

static const ClusterICMsgTypeInfo cluster_tt_status_hint_msg_info = {
	.msg_type = PGRAC_IC_MSG_TT_STATUS_HINT,
	.name = "cluster_tt_status_hint",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_TT_STATUS_HINT,
	.broadcast_ok = true,
	.handler = cluster_tt_status_hint_handle_envelope,
};

void
cluster_tt_status_hint_register_msg_type(void)
{
	cluster_ic_register_msg_type(&cluster_tt_status_hint_msg_info);
}

/* ------------------------------------------------------------ */
/* emit path (D4 calls this from xact commit/abort hook)        */
/* ------------------------------------------------------------ */

void
cluster_tt_status_hint_emit(const ClusterTTStatusKey *key, ClusterTTStatus status,
							SCN commit_scn)
{
	uint32 tail;
	uint32 next_tail;

	if (!cluster_enabled || ClusterTTHintOutbound == NULL || key == NULL)
		return;

	/* GUC gate:  disabled mode is a no-op. */
	if (cluster_tt_status_hint_emit_mode == CLUSTER_TT_STATUS_HINT_EMIT_DISABLED)
		return;

	/* Only commit / abort propagate;  in-progress and other states are
	 * never emitted (status range guardrail). */
	if (status != CLUSTER_TT_STATUS_COMMITTED && status != CLUSTER_TT_STATUS_ABORTED)
		return;

	LWLockAcquire(&ClusterTTHintOutbound->lock.lock, LW_EXCLUSIVE);
	tail = pg_atomic_read_u32(&ClusterTTHintOutbound->tail);
	next_tail = (tail + 1) % ClusterTTHintOutbound->capacity;
	if (next_tail == pg_atomic_read_u32(&ClusterTTHintOutbound->head)) {
		/* Full — fire-and-forget MVP loss mode (R10). */
		LWLockRelease(&ClusterTTHintOutbound->lock.lock);
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster TT status hint outbound full; hint dropped"),
						  errhint("Raise cluster.tt_status_hint_outbound_capacity.")));
		return;
	}

	ClusterTTHintOutbound->slots[tail].key = *key;
	ClusterTTHintOutbound->slots[tail].commit_scn = commit_scn;
	ClusterTTHintOutbound->slots[tail].status = (uint16)status;
	ClusterTTHintOutbound->slots[tail]._pad[0] = 0;
	ClusterTTHintOutbound->slots[tail]._pad[1] = 0;
	ClusterTTHintOutbound->slots[tail]._pad[2] = 0;
	pg_atomic_write_u32(&ClusterTTHintOutbound->tail, next_tail);
	LWLockRelease(&ClusterTTHintOutbound->lock.lock);

	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->emit_count, 1);

	/* L174 idempotent latch wake — LMON drain promptly. */
	cluster_lmon_wakeup();
}

/* ------------------------------------------------------------ */
/* LMON drain (L172 family — LMON-only HC185)                   */
/* ------------------------------------------------------------ */

void
cluster_tt_status_hint_drain_outbound(void)
{
	if (ClusterTTHintOutbound == NULL || ClusterTTHintCounters == NULL)
		return;

	if (MyBackendType != B_LMON)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_tt_status_hint_drain_outbound: LMON only"),
						errhint("HC185 + L172 family — tier1 fd LMON-exclusive ownership.")));

	for (;;) {
		ClusterTTStatusHintOutboundEntry local;
		ClusterTTStatusHintMsgV2 msg;
		ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];
		uint32 head;

		LWLockAcquire(&ClusterTTHintOutbound->lock.lock, LW_EXCLUSIVE);
		head = pg_atomic_read_u32(&ClusterTTHintOutbound->head);
		if (head == pg_atomic_read_u32(&ClusterTTHintOutbound->tail)) {
			LWLockRelease(&ClusterTTHintOutbound->lock.lock);
			break;
		}
		local = ClusterTTHintOutbound->slots[head];
		pg_atomic_write_u32(&ClusterTTHintOutbound->head,
							(head + 1) % ClusterTTHintOutbound->capacity);
		LWLockRelease(&ClusterTTHintOutbound->lock.lock);

		/*
		 * Build wire payload — V2 layout (spec-3.3 D8). Senders post
		 * spec-3.3 emit V2 only. V1 backward-compat exists on the
		 * receiver side for the upgrade window.
		 */
		memset(&msg, 0, sizeof(msg));
		msg.msg_version = (uint16)CLUSTER_TT_STATUS_HINT_V2;
		msg.status = local.status;
		msg.flags = 0;
		msg._reserved16 = 0;
		msg.key = local.key;
		msg.commit_scn = local.commit_scn;

		cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_TT_STATUS_HINT, &msg, sizeof(msg), per_peer);
	}
}

/* ------------------------------------------------------------ */
/* receiver path                                                */
/* ------------------------------------------------------------ */

void
cluster_tt_status_hint_handle_envelope(const ClusterICEnvelope *env, const void *payload)
{
	uint16		msg_version;
	uint16		status_raw;
	uint16		flags_raw;
	uint16		reserved16_raw;
	const ClusterTTStatusKey *key;
	SCN			commit_scn;
	uint32		current_epoch;
	bool		v1_compat = false;

	if (ClusterTTHintCounters == NULL || env == NULL || payload == NULL)
		return;

	/*
	 * spec-3.3 D9 (R6 P1): length-check-before-cast. Read the smallest
	 * common prefix (msg_version) first, then validate exact payload
	 * length per version, THEN cast to the version-specific struct.
	 * Casting before length validation could read past a short frame.
	 *
	 * The header prefix (msg_version uint16 at offset 0) is shared by V1
	 * and V2, so reading offsetof(V1, key) == 8 bytes is always safe
	 * once we know payload_length >= 8.
	 */
	if (env->payload_length < (uint32) offsetof(ClusterTTStatusHintMsgV1, key)) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	memcpy(&msg_version, payload, sizeof(uint16));

	if (msg_version == CLUSTER_TT_STATUS_HINT_V1)
	{
		const ClusterTTStatusHintMsgV1 *v1;

		if (env->payload_length != (uint32) sizeof(ClusterTTStatusHintMsgV1))
		{
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v1 = (const ClusterTTStatusHintMsgV1 *) payload;
		status_raw = v1->status;
		flags_raw = v1->flags;
		reserved16_raw = v1->_reserved16;
		key = &v1->key;
		/* V1 wire carries no commit_scn -> install InvalidScn; snapshot
		 * consumer flags such tuples UNKNOWN -> 53R97. */
		commit_scn = InvalidScn;
		v1_compat = true;
	}
	else if (msg_version == CLUSTER_TT_STATUS_HINT_V2)
	{
		const ClusterTTStatusHintMsgV2 *v2;

		if (env->payload_length != (uint32) sizeof(ClusterTTStatusHintMsgV2))
		{
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v2 = (const ClusterTTStatusHintMsgV2 *) payload;
		status_raw = v2->status;
		flags_raw = v2->flags;
		reserved16_raw = v2->_reserved16;
		key = &v2->key;
		commit_scn = v2->commit_scn;
	}
	else
	{
		/* HC187 forward-compat: future V3+ unknown to this receiver. */
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_unknown_version_count, 1);
		return;
	}

	/* Self-loop defense (also asserted by tier1 framework but double-
	 * check). */
	if ((int32) env->source_node_id == cluster_node_id) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/* HC186 anti-spoof — payload-declared origin must match framework-
	 * set envelope source. */
	if ((int32) key->origin_node_id != (int32) env->source_node_id) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/* Epoch fence (HC182 inherited from spec-3.1). */
	current_epoch = (uint32) cluster_epoch_get_current();
	if (key->cluster_epoch != current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_stale_epoch_count, 1);
		return;
	}

	/* Status range. */
	if (status_raw != CLUSTER_TT_STATUS_COMMITTED && status_raw != CLUSTER_TT_STATUS_ABORTED) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/*
	 * spec-3.3 D9/L181: V2 COMMITTED messages must carry a real
	 * commit_scn. V1 compat intentionally installs InvalidScn so the
	 * visibility consumer fails closed with 53R97; do not reject it here.
	 * ABORTED has no commit_scn and must keep the field InvalidScn.
	 */
	if (!v1_compat)
	{
		if (status_raw == CLUSTER_TT_STATUS_COMMITTED && !SCN_VALID(commit_scn))
		{
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
		if (status_raw == CLUSTER_TT_STATUS_ABORTED && SCN_VALID(commit_scn))
		{
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
	}

	/* Reserved fields MUST be zero (M3 anti-tamper). */
	if (flags_raw != 0 || reserved16_raw != 0 || key->_reserved != 0
		|| key->_reserved2 != 0) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->receive_count, 1);

	if (v1_compat)
	{
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_v1_compat_count, 1);
		ereport(WARNING,
				(errmsg("cluster tt_status_hint V1 received from peer; spec-3.3 senders should emit V2"),
				 errhint("Peer is running a pre-spec-3.3 binary; commit_scn=InvalidScn results in UNKNOWN visibility for cross-node COMMITs.")));
	}

	/*
	 * spec-3.3 D9 (L181 chain step 6): install with commit_scn from
	 * the wire (real value for V2 senders, InvalidScn for V1 compat).
	 * No raw-xid rebuild -- HC184 direct install with sender-supplied
	 * key.
	 */
	cluster_tt_status_install_local(key, (ClusterTTStatus) status_raw, commit_scn);
	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->install_count, 1);
}

/* ------------------------------------------------------------ */
/* counter getters                                              */
/* ------------------------------------------------------------ */

#define CLUSTER_TT_HINT_GETTER(name)                                                               \
	uint64 cluster_tt_status_hint_get_##name(void)                                                 \
	{                                                                                              \
		if (ClusterTTHintCounters == NULL)                                                         \
			return 0;                                                                              \
		return pg_atomic_read_u64(&ClusterTTHintCounters->name);                                   \
	}

CLUSTER_TT_HINT_GETTER(emit_count)
CLUSTER_TT_HINT_GETTER(receive_count)
CLUSTER_TT_HINT_GETTER(drop_invalid_count)
CLUSTER_TT_HINT_GETTER(drop_stale_epoch_count)
CLUSTER_TT_HINT_GETTER(drop_unknown_version_count)
CLUSTER_TT_HINT_GETTER(install_count)
CLUSTER_TT_HINT_GETTER(drop_v1_compat_count)

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_tt_status_hint_shmem_size(void)
{
	return 0;
}

void
cluster_tt_status_hint_shmem_init(void)
{}

void
cluster_tt_status_hint_shmem_register(void)
{}

void
cluster_tt_status_hint_emit(const ClusterTTStatusKey *key, ClusterTTStatus status,
							SCN commit_scn)
{
	(void)key;
	(void)status;
	(void)commit_scn;
}

void
cluster_tt_status_hint_drain_outbound(void)
{}

void
cluster_tt_status_hint_handle_envelope(const ClusterICEnvelope *env, const void *payload)
{
	(void)env;
	(void)payload;
}

#define CLUSTER_TT_HINT_GETTER_STUB(name)                                                          \
	uint64 cluster_tt_status_hint_get_##name(void)                                                 \
	{                                                                                              \
		return 0;                                                                                  \
	}

CLUSTER_TT_HINT_GETTER_STUB(emit_count)
CLUSTER_TT_HINT_GETTER_STUB(receive_count)
CLUSTER_TT_HINT_GETTER_STUB(drop_invalid_count)
CLUSTER_TT_HINT_GETTER_STUB(drop_stale_epoch_count)
CLUSTER_TT_HINT_GETTER_STUB(drop_unknown_version_count)
CLUSTER_TT_HINT_GETTER_STUB(install_count)
CLUSTER_TT_HINT_GETTER_STUB(drop_v1_compat_count)

#endif /* USE_PGRAC_CLUSTER */
