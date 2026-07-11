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

#include "access/transam.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_mode.h" /* cluster_peer_mode_enabled (single-node skips peer hint emit) */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tx_enqueue.h" /* spec-5.2 D6: wake TX-enqueue waiters */
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
	SCN commit_scn; /* spec-3.3 D8 */
	uint16 status;
	uint16 has_parent_key; /* PGRAC spec-3.5: nonzero only for SUBCOMMITTED */
	uint16 _pad[2];
	ClusterTTStatusKey parent_key; /* PGRAC spec-3.5: valid iff has_parent_key */
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
	pg_atomic_uint64 v3_downgrade_count;	/* reserved for future mixed-version negotiation */
	pg_atomic_uint64 v4_drop_unknown_count; /* PGRAC spec-3.6 D4: V4 DROP counter */
} ClusterTTStatusHintState;

static ClusterTTStatusHintOutboundRing *ClusterTTHintOutbound = NULL;
static ClusterTTStatusHintState *ClusterTTHintCounters = NULL;

/*
 * PGRAC spec-3.6 D4:  V4 sidecar outbound queue (fixed-size, separate
 * from V2/V3 single-key ring).  Each slot reserves 6168B (header 24 +
 * 256 × 24 members).  Default 1024 slots ≈ 6.0 MiB shmem (GUC
 * cluster.multixact_hint_outbound_slots).
 */
typedef struct ClusterMultiXactHintOutboundRing {
	pg_atomic_uint32 head;
	pg_atomic_uint32 tail;
	uint32 capacity;
	LWLockPadded lock;
	ClusterMultiXactHintOutboundEntry slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterMultiXactHintOutboundRing;

static ClusterMultiXactHintOutboundRing *ClusterMultiXactHintOutbound = NULL;

static Size
v4_outbound_ring_size(int capacity)
{
	return offsetof(ClusterMultiXactHintOutboundRing, slots)
		   + mul_size(sizeof(ClusterMultiXactHintOutboundEntry), capacity);
}

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
		   + MAXALIGN(sizeof(ClusterTTStatusHintState))
		   + MAXALIGN(v4_outbound_ring_size(cluster_multixact_hint_outbound_slots));
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
		pg_atomic_init_u64(&ClusterTTHintCounters->v3_downgrade_count, 0);
		pg_atomic_init_u64(&ClusterTTHintCounters->v4_drop_unknown_count, 0);
	}

	/*
	 * PGRAC spec-3.6 D4:  V4 sidecar outbound queue init.
	 */
	{
		Size v4_ring_sz = MAXALIGN(v4_outbound_ring_size(cluster_multixact_hint_outbound_slots));
		bool v4_found;

		ClusterMultiXactHintOutbound = (ClusterMultiXactHintOutboundRing *)ShmemInitStruct(
			"ClusterMultiXactHintOutbound", v4_ring_sz, &v4_found);
		if (!v4_found) {
			pg_atomic_init_u32(&ClusterMultiXactHintOutbound->head, 0);
			pg_atomic_init_u32(&ClusterMultiXactHintOutbound->tail, 0);
			ClusterMultiXactHintOutbound->capacity = cluster_multixact_hint_outbound_slots;
			LWLockInitialize(&ClusterMultiXactHintOutbound->lock.lock, LWTRANCHE_CLUSTER_TT_STATUS);
		}
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
cluster_tt_status_hint_emit(const ClusterTTStatusKey *key, ClusterTTStatus status, SCN commit_scn)
{
	uint32 tail;
	uint32 next_tail;

	if (!cluster_enabled || ClusterTTHintOutbound == NULL || key == NULL)
		return;

	/* P0 perf hardening: outbound TT status hints are PEER work (enqueue + LMON
	 * wakeup).  A single-node storage deployment installs the local overlay but
	 * emits no peer hint -- gate on peer mode, not just cluster_enabled. */
	if (!cluster_peer_mode_enabled())
		return;

	/* GUC gate:  disabled mode is a no-op. */
	if (cluster_tt_status_hint_emit_mode == CLUSTER_TT_STATUS_HINT_EMIT_DISABLED)
		return;

	/*
	 * COMMITTED / ABORTED are the spec-3.2 baseline.  spec-3.4d added
	 * lock-only ACTIVE hints, represented as IN_PROGRESS + InvalidScn;
	 * spec-3.5 also uses that state for a SUBCOMMITTED child's parent
	 * chain.  Other states remain local-only.
	 */
	if (status != CLUSTER_TT_STATUS_COMMITTED && status != CLUSTER_TT_STATUS_ABORTED
		&& status != CLUSTER_TT_STATUS_IN_PROGRESS)
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
	/* PGRAC spec-3.5: V2-class emit carries no parent_key. */
	ClusterTTHintOutbound->slots[tail].has_parent_key = 0;
	ClusterTTHintOutbound->slots[tail]._pad[0] = 0;
	ClusterTTHintOutbound->slots[tail]._pad[1] = 0;
	memset(&ClusterTTHintOutbound->slots[tail].parent_key, 0,
		   sizeof(ClusterTTHintOutbound->slots[tail].parent_key));
	pg_atomic_write_u32(&ClusterTTHintOutbound->tail, next_tail);
	LWLockRelease(&ClusterTTHintOutbound->lock.lock);

	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->emit_count, 1);

	/* L174 idempotent latch wake — LMON drain promptly. */
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_TT_HINT); /* spec-7.2 D1 */
	cluster_lmon_wakeup();
}

/*
 * cluster_tt_status_hint_emit_subcommitted (PGRAC spec-3.5 D3 NEW)
 *
 *	  Enqueue a SUBCOMMITTED hint with parent_key chain pointer.  Emit
 *	  path is identical to V2 except the slot carries has_parent_key=1
 *	  + parent_key, and the drain loop builds V3 wire payload.  Caller
 *	  must have installed local overlay via
 *	  cluster_tt_status_install_subcommitted() first.
 */
void
cluster_tt_status_hint_emit_subcommitted(const ClusterTTStatusKey *child_key,
										 const ClusterTTStatusKey *parent_key)
{
	uint32 tail;
	uint32 next_tail;

	if (!cluster_enabled || ClusterTTHintOutbound == NULL || child_key == NULL
		|| parent_key == NULL)
		return;

	/* P0 perf hardening: peer hint emit only in peer mode (see _emit). */
	if (!cluster_peer_mode_enabled())
		return;

	if (cluster_tt_status_hint_emit_mode == CLUSTER_TT_STATUS_HINT_EMIT_DISABLED)
		return;

	LWLockAcquire(&ClusterTTHintOutbound->lock.lock, LW_EXCLUSIVE);
	tail = pg_atomic_read_u32(&ClusterTTHintOutbound->tail);
	next_tail = (tail + 1) % ClusterTTHintOutbound->capacity;
	if (next_tail == pg_atomic_read_u32(&ClusterTTHintOutbound->head)) {
		LWLockRelease(&ClusterTTHintOutbound->lock.lock);
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster TT status hint outbound full; SUBCOMMITTED hint dropped"),
						  errhint("Raise cluster.tt_status_hint_outbound_capacity.")));
		return;
	}

	ClusterTTHintOutbound->slots[tail].key = *child_key;
	ClusterTTHintOutbound->slots[tail].commit_scn = InvalidScn;
	ClusterTTHintOutbound->slots[tail].status = (uint16)CLUSTER_TT_STATUS_SUBCOMMITTED;
	ClusterTTHintOutbound->slots[tail].has_parent_key = 1;
	ClusterTTHintOutbound->slots[tail]._pad[0] = 0;
	ClusterTTHintOutbound->slots[tail]._pad[1] = 0;
	ClusterTTHintOutbound->slots[tail].parent_key = *parent_key;
	pg_atomic_write_u32(&ClusterTTHintOutbound->tail, next_tail);
	LWLockRelease(&ClusterTTHintOutbound->lock.lock);

	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->emit_count, 1);
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_TT_HINT); /* spec-7.2 D1 */
	cluster_lmon_wakeup();
}

/*
 * cluster_tt_status_hint_emit_multixact_overlay (PGRAC spec-3.6 D4 NEW)
 *
 *   Enqueue a V4 sidecar emit (multixact composition overlay) for LMON
 *   drain.  Sender member_count > GUC cap → fail-closed (no partial
 *   emit) + overflow counter.  Uses dedicated V4 sidecar outbound queue
 *   (does NOT pollute V2/V3 fixed ring).
 */
void
cluster_tt_status_hint_emit_multixact_overlay(const ClusterMultiXactKey *key, uint16 member_count,
											  const ClusterMultiXactMember *members)
{
	uint32 tail;
	uint32 next_tail;

	if (!cluster_enabled || ClusterMultiXactHintOutbound == NULL || key == NULL || members == NULL)
		return;
	/* P0 perf hardening: peer hint emit only in peer mode (see _emit). */
	if (!cluster_peer_mode_enabled())
		return;
	if (cluster_tt_status_hint_emit_mode == CLUSTER_TT_STATUS_HINT_EMIT_DISABLED)
		return;

	if (member_count == 0 || member_count > cluster_multixact_member_overlay_max_members
		|| member_count > CLUSTER_MULTIXACT_HINT_MAX_MEMBERS) {
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster multixact V4 emit overflow:  member_count %u > cap",
								 (unsigned)member_count),
						  errhint("Raise cluster.multixact_member_overlay_max_members.")));
		return;
	}

	LWLockAcquire(&ClusterMultiXactHintOutbound->lock.lock, LW_EXCLUSIVE);
	tail = pg_atomic_read_u32(&ClusterMultiXactHintOutbound->tail);
	next_tail = (tail + 1) % ClusterMultiXactHintOutbound->capacity;
	if (next_tail == pg_atomic_read_u32(&ClusterMultiXactHintOutbound->head)) {
		LWLockRelease(&ClusterMultiXactHintOutbound->lock.lock);
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster V4 sidecar outbound full; multixact hint dropped"),
						  errhint("Raise cluster.multixact_hint_outbound_slots.")));
		return;
	}

	{
		ClusterMultiXactHintOutboundEntry *slot = &ClusterMultiXactHintOutbound->slots[tail];

		memset(&slot->header, 0, sizeof(slot->header));
		slot->header.msg_version = (uint16)CLUSTER_TT_STATUS_HINT_V4;
		slot->header.payload_kind = 1; /* multixact overlay */
		slot->header.flags = 0;
		slot->header.member_count = member_count;
		slot->header.key = *key;

		memset(slot->members, 0, sizeof(slot->members));
		memcpy(slot->members, members, member_count * sizeof(ClusterMultiXactMember));
	}
	pg_atomic_write_u32(&ClusterMultiXactHintOutbound->tail, next_tail);
	LWLockRelease(&ClusterMultiXactHintOutbound->lock.lock);

	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->emit_count, 1);
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_TT_HINT); /* spec-7.2 D1 */
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
		 * PGRAC spec-3.5: SUBCOMMITTED entries emit V3 with parent_key;
		 * all other states emit V2 (spec-3.3 D8 baseline).  V3 receiver
		 * dispatch on msg_version distinguishes the two formats per
		 * L203 progressive extend convention.
		 *
		 * V3 negotiation note:  this drain emits V3 unconditionally for
		 * SUBCOMMITTED.  V1/V2 receivers will DROP V3 via
		 * drop_unknown_version_count (HC187 forward-compat reject), which
		 * is the correct fail-closed behaviour:  remote reader cluster
		 * exact-ref miss → 53R97 per L199.  v3_downgrade_count is reserved
		 * for a future explicit mixed-version negotiation path; spec-3.5
		 * has no peer-version table, so it never downgrades.
		 */
		if (local.has_parent_key) {
			ClusterTTStatusHintMsgV3 msg3;

			memset(&msg3, 0, sizeof(msg3));
			msg3.msg_version = (uint16)CLUSTER_TT_STATUS_HINT_V3;
			msg3.status = local.status; /* SUBCOMMITTED */
			msg3.flags = 0;
			msg3._reserved16 = 0;
			msg3.key = local.key;
			msg3.commit_scn = local.commit_scn; /* InvalidScn for SUBCOMMITTED */
			msg3.parent_key = local.parent_key;

			cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_TT_STATUS_HINT, &msg3, sizeof(msg3),
											per_peer);
		} else {
			ClusterTTStatusHintMsgV2 msg2;

			memset(&msg2, 0, sizeof(msg2));
			msg2.msg_version = (uint16)CLUSTER_TT_STATUS_HINT_V2;
			msg2.status = local.status;
			msg2.flags = 0;
			msg2._reserved16 = 0;
			msg2.key = local.key;
			msg2.commit_scn = local.commit_scn;

			cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_TT_STATUS_HINT, &msg2, sizeof(msg2),
											per_peer);
		}
	}

	/*
	 * PGRAC spec-3.6 D4:  V4 sidecar outbound queue drain.  Separate
	 * loop because variable members[] cannot share V2/V3 ring.
	 */
	if (ClusterMultiXactHintOutbound != NULL) {
		for (;;) {
			ClusterMultiXactHintOutboundEntry local;
			ClusterICFanoutResult per_peer[CLUSTER_MAX_NODES];
			uint32 head;
			uint16 member_count;
			Size wire_len;

			LWLockAcquire(&ClusterMultiXactHintOutbound->lock.lock, LW_EXCLUSIVE);
			head = pg_atomic_read_u32(&ClusterMultiXactHintOutbound->head);
			if (head == pg_atomic_read_u32(&ClusterMultiXactHintOutbound->tail)) {
				LWLockRelease(&ClusterMultiXactHintOutbound->lock.lock);
				break;
			}
			local = ClusterMultiXactHintOutbound->slots[head];
			pg_atomic_write_u32(&ClusterMultiXactHintOutbound->head,
								(head + 1) % ClusterMultiXactHintOutbound->capacity);
			LWLockRelease(&ClusterMultiXactHintOutbound->lock.lock);

			/*
			 * Wire length = 24B header + member_count × 24B members.
			 * Sender already capped member_count at GUC (emit path);
			 * receiver re-validates per HC208.
			 */
			member_count = local.header.member_count;
			wire_len = sizeof(ClusterTTStatusHintMsgV4Header)
					   + (Size)member_count * sizeof(ClusterMultiXactMember);

			cluster_ic_send_envelope_fanout(PGRAC_IC_MSG_TT_STATUS_HINT, &local, wire_len,
											per_peer);
		}
	}
}

/* ------------------------------------------------------------ */
/* receiver path                                                */
/* ------------------------------------------------------------ */

void
cluster_tt_status_hint_handle_envelope(const ClusterICEnvelope *env, const void *payload)
{
	uint16 msg_version;
	uint16 status_raw;
	uint16 flags_raw;
	uint16 reserved16_raw;
	const ClusterTTStatusKey *key;
	SCN commit_scn;
	uint32 current_epoch;
	bool v1_compat = false;
	/* PGRAC spec-3.5: V3 SUBCOMMITTED carries parent_key. */
	bool is_v3_subcommitted = false;
	ClusterTTStatusKey parent_key_local;

	memset(&parent_key_local, 0, sizeof(parent_key_local));

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
	if (env->payload_length < (uint32)offsetof(ClusterTTStatusHintMsgV1, key)) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	memcpy(&msg_version, payload, sizeof(uint16));

	if (msg_version == CLUSTER_TT_STATUS_HINT_V1) {
		const ClusterTTStatusHintMsgV1 *v1;

		if (env->payload_length != (uint32)sizeof(ClusterTTStatusHintMsgV1)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v1 = (const ClusterTTStatusHintMsgV1 *)payload;
		status_raw = v1->status;
		flags_raw = v1->flags;
		reserved16_raw = v1->_reserved16;
		key = &v1->key;
		/* V1 wire carries no commit_scn -> install InvalidScn; snapshot
		 * consumer flags such tuples UNKNOWN -> 53R97. */
		commit_scn = InvalidScn;
		v1_compat = true;
	} else if (msg_version == CLUSTER_TT_STATUS_HINT_V2) {
		const ClusterTTStatusHintMsgV2 *v2;

		if (env->payload_length != (uint32)sizeof(ClusterTTStatusHintMsgV2)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v2 = (const ClusterTTStatusHintMsgV2 *)payload;
		status_raw = v2->status;
		flags_raw = v2->flags;
		reserved16_raw = v2->_reserved16;
		key = &v2->key;
		commit_scn = v2->commit_scn;
	} else if (msg_version == CLUSTER_TT_STATUS_HINT_V3) {
		/* PGRAC spec-3.5 D3: V3 SUBCOMMITTED with parent_key. */
		const ClusterTTStatusHintMsgV3 *v3;

		if (env->payload_length != (uint32)sizeof(ClusterTTStatusHintMsgV3)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v3 = (const ClusterTTStatusHintMsgV3 *)payload;
		status_raw = v3->status;
		flags_raw = v3->flags;
		reserved16_raw = v3->_reserved16;
		key = &v3->key;
		commit_scn = v3->commit_scn;
		parent_key_local = v3->parent_key;
		is_v3_subcommitted = true;
	} else if (msg_version == CLUSTER_TT_STATUS_HINT_V4) {
		/*
		 * PGRAC spec-3.6 D4:  V4 sidecar multixact composition payload.
		 *
		 *   Strict payload length:  24B header + member_count × 24B
		 *   members[].  Receiver MUST validate exact length match.
		 *   member_count > GUC cap → DROP + overlay_overflow_count +1.
		 *   anti-spoof:  key.origin_node_id == env->source_node_id.
		 *   Install via cluster_multixact_member_overlay_install.
		 *
		 *   This branch is the L204 sidecar dispatch:  V1/V2/V3 single-key
		 *   payload path remains byte-for-byte unchanged above.
		 */
		const ClusterTTStatusHintMsgV4Header *v4hdr;
		const ClusterMultiXactMember *v4_members;
		uint16 v4_member_count;
		Size expected_len;

		if (env->payload_length < (uint32)sizeof(ClusterTTStatusHintMsgV4Header)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v4hdr = (const ClusterTTStatusHintMsgV4Header *)payload;
		v4_member_count = v4hdr->member_count;
		expected_len = (Size)sizeof(ClusterTTStatusHintMsgV4Header)
					   + (Size)v4_member_count * sizeof(ClusterMultiXactMember);

		if ((Size)env->payload_length != expected_len) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		if (v4_member_count == 0
			|| (int)v4_member_count > cluster_multixact_member_overlay_max_members) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		/* anti-spoof HC186 family:  key.origin_node_id == env source */
		if ((int32)v4hdr->key.origin_node_id != (int32)env->source_node_id) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		/* reserved / pad must be zero (M3 anti-tamper) */
		if (v4hdr->flags != 0 || v4hdr->payload_kind != 1 || v4hdr->key._pad16 != 0
			|| v4hdr->key._reserved != 0) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}

		v4_members = (const ClusterMultiXactMember *)((const char *)payload
													  + sizeof(ClusterTTStatusHintMsgV4Header));
		for (uint16 i = 0; i < v4_member_count; i++) {
			const ClusterMultiXactMember *m = &v4_members[i];

			if (m->status > (uint8)MultiXactStatusUpdate || m->_pad8 != 0 || m->_pad16 != 0
				|| m->_reserved2 != 0 || (int32)m->origin_node_id != (int32)env->source_node_id) {
				pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
				return;
			}
		}

		(void)cluster_multixact_member_overlay_install(&v4hdr->key, v4_member_count, v4_members);

		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->receive_count, 1);
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->install_count, 1);
		return; /* V4 path bypasses single-key install below */
	} else {
		/* HC187 forward-compat: future V5+ unknown to this receiver. */
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->v4_drop_unknown_count, 1);
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_unknown_version_count, 1);
		return;
	}

	/* Self-loop defense (also asserted by tier1 framework but double-
	 * check). */
	if ((int32)env->source_node_id == cluster_node_id) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/* HC186 anti-spoof — payload-declared origin must match framework-
	 * set envelope source. */
	if ((int32)key->origin_node_id != (int32)env->source_node_id) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/* Epoch fence (HC182 inherited from spec-3.1). */
	current_epoch = (uint32)cluster_epoch_get_current();
	if (key->cluster_epoch != current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_stale_epoch_count, 1);
		return;
	}

	/*
	 * Status range.  PGRAC spec-3.5: V3 carries SUBCOMMITTED only.
	 * V1/V2 carry terminal COMMITTED/ABORTED plus spec-3.4d lock-only
	 * IN_PROGRESS.  UNKNOWN / CLEANED_OUT remain local-only.
	 */
	if (is_v3_subcommitted) {
		if (status_raw != CLUSTER_TT_STATUS_SUBCOMMITTED) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
	} else if (status_raw != CLUSTER_TT_STATUS_COMMITTED && status_raw != CLUSTER_TT_STATUS_ABORTED
			   && status_raw != CLUSTER_TT_STATUS_IN_PROGRESS) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/*
	 * spec-3.3 D9/L181: V2 COMMITTED messages must carry a real
	 * commit_scn. V1 compat intentionally installs InvalidScn so the
	 * visibility consumer fails closed with 53R97; do not reject it here.
	 * ABORTED has no commit_scn and must keep the field InvalidScn.
	 *
	 * PGRAC spec-3.5 V3 SUBCOMMITTED: commit_scn MUST be InvalidScn
	 * (subxact not finalized).  V2 IN_PROGRESS also carries InvalidScn:
	 * it is a live parent / lock-only marker, not a commit ordering fact.
	 */
	if (!v1_compat && !is_v3_subcommitted) {
		if (status_raw == CLUSTER_TT_STATUS_COMMITTED && !SCN_VALID(commit_scn)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
		if (status_raw == CLUSTER_TT_STATUS_ABORTED && SCN_VALID(commit_scn)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
		if (status_raw == CLUSTER_TT_STATUS_IN_PROGRESS && SCN_VALID(commit_scn)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
	}
	if (is_v3_subcommitted && SCN_VALID(commit_scn)) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}

	/* Reserved fields MUST be zero (M3 anti-tamper). */
	if (flags_raw != 0 || reserved16_raw != 0 || key->_reserved != 0 || key->_reserved2 != 0) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
		return;
	}
	if (is_v3_subcommitted) {
		if (parent_key_local._reserved != 0 || parent_key_local._reserved2 != 0
			|| parent_key_local.origin_node_id != key->origin_node_id
			|| parent_key_local.cluster_epoch != key->cluster_epoch
			|| !TransactionIdIsNormal(parent_key_local.local_xid)) {
			pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_invalid_count, 1);
			return;
		}
	}

	pg_atomic_fetch_add_u64(&ClusterTTHintCounters->receive_count, 1);

	if (v1_compat) {
		pg_atomic_fetch_add_u64(&ClusterTTHintCounters->drop_v1_compat_count, 1);
		ereport(
			WARNING,
			(errmsg(
				 "cluster tt_status_hint V1 received from peer; spec-3.3 senders should emit V2"),
			 errhint("Peer is running a pre-spec-3.3 binary; commit_scn=InvalidScn results in "
					 "UNKNOWN visibility for cross-node COMMITs.")));
	}

	/*
	 * spec-3.3 D9 (L181 chain step 6): install with commit_scn from
	 * the wire (real value for V2 senders, InvalidScn for V1 compat).
	 * No raw-xid rebuild -- HC184 direct install with sender-supplied
	 * key.
	 *
	 * PGRAC spec-3.5:  V3 SUBCOMMITTED dispatches to install_subcommitted
	 * which records parent_key for lazy reader follow.
	 */
	if (is_v3_subcommitted)
		cluster_tt_status_install_subcommitted(key, &parent_key_local);
	else {
		cluster_tt_status_install_local(key, (ClusterTTStatus)status_raw, commit_scn);

		/*
		 * spec-5.2 D6:  a remote holder just became terminal on this node's
		 * TT cache — wake any backend blocked in cluster_tx_enqueue_wait on
		 * that holder so it re-judges (apply-then-wake gives a happens-before:
		 * the woken waiter re-checks the status and sees it terminal).
		 */
		if (status_raw == CLUSTER_TT_STATUS_COMMITTED || status_raw == CLUSTER_TT_STATUS_ABORTED)
			cluster_txw_wake_waiters(key);
	}
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
/* PGRAC spec-3.5 D3 */
CLUSTER_TT_HINT_GETTER(v3_downgrade_count)
/* PGRAC spec-3.6 D4 */
CLUSTER_TT_HINT_GETTER(v4_drop_unknown_count)

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
cluster_tt_status_hint_emit(const ClusterTTStatusKey *key, ClusterTTStatus status, SCN commit_scn)
{
	(void)key;
	(void)status;
	(void)commit_scn;
}

void
cluster_tt_status_hint_emit_subcommitted(const ClusterTTStatusKey *child_key,
										 const ClusterTTStatusKey *parent_key)
{
	(void)child_key;
	(void)parent_key;
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
/* PGRAC spec-3.5 D3 */
CLUSTER_TT_HINT_GETTER_STUB(v3_downgrade_count)
/* PGRAC spec-3.6 D4 */
CLUSTER_TT_HINT_GETTER_STUB(v4_drop_unknown_count)

void
cluster_tt_status_hint_emit_multixact_overlay(const ClusterMultiXactKey *key, uint16 member_count,
											  const ClusterMultiXactMember *members)
{
	(void)key;
	(void)member_count;
	(void)members;
}

#endif /* USE_PGRAC_CLUSTER */
