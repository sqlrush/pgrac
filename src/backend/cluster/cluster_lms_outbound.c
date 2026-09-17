/*-------------------------------------------------------------------------
 *
 * cluster_lms_outbound.c
 *	  pgrac DATA-plane outbound ring — spec-7.2 D4 (Q6-B twin ring).
 *
 *	  Backend-context producers of DATA-plane messages (GCS block
 *	  REQUEST / INVALIDATE) stage frames here;  the LMS data-plane loop
 *	  drains and sends them over its own fds.  This is the DATA twin of
 *	  the CONTROL-plane cluster_grd_outbound ring, kept to the same
 *	  minimal single-tail-FIFO semantics (Q6-B: one consumer, one lock,
 *	  no claim/compaction machinery — the r1-F3 argument against a
 *	  shared dual-reader ring).  Enqueue marks no LMON duty and wakes
 *	  LMS, not LMON.
 *
 *	  Ordering note (INV-7.2-DATA-FIFO): per-peer DATA frames keep a
 *	  single ordered stream because (a) this ring is FIFO, (b) LMS is
 *	  its only consumer, and (c) tier1 owns per-peer ordering below us —
 *	  an ADMITTED frame (send result DONE or WOULD_BLOCK) is delivered
 *	  in submission order by the tier1 outbound FIFO, and a REFUSED
 *	  frame (NOT_ADMITTED) is retained here ahead of anything newer for
 *	  the same peer (GCS serve-stall round-5 ownership contract).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms_outbound.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.2-ic-data-plane-decoupling.md (D4).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h" /* GcsBlockRequestPayload (pre-send hook) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

#ifdef USE_PGRAC_CLUSTER

#define PGRAC_LMS_OUTBOUND_CAPACITY 256
#define PGRAC_LMS_OUTBOUND_PAYLOAD_MAX RESOURCE_X_PROOF_V1_BYTES
#define PGRAC_LMS_RESOURCE_X_PROBE_BUDGET 4
#define PGRAC_LMS_RESOURCE_X_CALLS_PER_TICK 16

typedef enum ClusterLmsOutboundKind {
	CLUSTER_LMS_OUTBOUND_FRAME = 0,
	CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY = 1,
	CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY = 2,
	CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT = 3,
	CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_PENDING = 4,
	CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_READY = 5,
	CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_CANCELLED = 6
} ClusterLmsOutboundKind;

typedef struct ClusterLmsOutboundSlot {
	uint32 dest_node_id;
	uint8 msg_type;
	uint8 kind;
	uint16 payload_len;
	uint32 required_capability;
	uint32 connection_generation;
	uint64 deadline_us;
	BufferTag local_tag;
	uint64 local_slot_cookie;
	uint64 local_own_generation;
	uint64 local_reservation_token;
	uint8 payload[PGRAC_LMS_OUTBOUND_PAYLOAD_MAX];
} ClusterLmsOutboundSlot;

StaticAssertDecl(PGRAC_LMS_OUTBOUND_PAYLOAD_MAX >= RESOURCE_X_PROOF_V1_BYTES,
				 "LMS outbound slot must hold a Resource-X proof ACK");
StaticAssertDecl(sizeof(ClusterLmsOutboundSlot) == 384,
				 "LMS outbound slot capability guard layout changed");

typedef struct ClusterLmsZeroBlockReplyWire {
	GcsBlockReplyHeader header;
	char block_data[GCS_BLOCK_DATA_SIZE];
} ClusterLmsZeroBlockReplyWire;

StaticAssertDecl(sizeof(ClusterLmsZeroBlockReplyWire) == GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE,
				 "staged zero-block reply must preserve the GCS reply wire size");

typedef struct ClusterLmsOutboundState {
	uint32 head; /* next slot to fill */
	uint32 tail; /* next slot to drain */
	uint32 count;
	uint64 next_remote_s_cookie;
	ClusterLmsOutboundSlot ring[PGRAC_LMS_OUTBOUND_CAPACITY];
} ClusterLmsOutboundState;

typedef struct ClusterLmsOutboundSharedState {
	pg_atomic_uint64 resource_x_transport_mutation_sequence;
	ClusterLmsOutboundState rings[CLUSTER_LMS_MAX_WORKERS];
} ClusterLmsOutboundSharedState;

/*
 * spec-7.3 D4 — one ring per DATA worker channel.  rings[0] is worker 0 (the
 * spec-7.2 ring; lms_workers=1 uses only it — byte-identical), rings[c] is
 * worker c.  Sizing is the compile-time cap (CLUSTER_LMS_MAX_WORKERS); the
 * live count follows cluster.lms_workers.  Each ring keeps the Q6-B single-
 * consumer single-tail FIFO semantics, and each has its own lock so worker c
 * drains rings[c] without contending on the other workers.
 */
static ClusterLmsOutboundSharedState *cluster_lms_outbound_shared = NULL;
static ClusterLmsOutboundState *cluster_lms_outbound_rings = NULL;
static LWLock *cluster_lms_outbound_locks[CLUSTER_LMS_MAX_WORKERS];

#define OB_RING(w) (&cluster_lms_outbound_rings[(w)])
#define OB_LOCK(w) (cluster_lms_outbound_locks[(w)])

static Size
cluster_lms_outbound_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterLmsOutboundSharedState));
}

static void
cluster_lms_outbound_shmem_init(void)
{
	bool found;
	int i;

	cluster_lms_outbound_shared = (ClusterLmsOutboundSharedState *)ShmemInitStruct(
		"pgrac cluster lms data outbound", cluster_lms_outbound_shmem_size(), &found);
	cluster_lms_outbound_rings = cluster_lms_outbound_shared->rings;
	if (!found) {
		memset(cluster_lms_outbound_shared, 0, sizeof(*cluster_lms_outbound_shared));
		pg_atomic_init_u64(&cluster_lms_outbound_shared->resource_x_transport_mutation_sequence, 1);
	}

	if (!IsBootstrapProcessingMode())
		for (i = 0; i < CLUSTER_LMS_MAX_WORKERS; i++)
			cluster_lms_outbound_locks[i]
				= &(GetNamedLWLockTranche("ClusterLmsDataOutbound"))[i].lock;
}

static const ClusterShmemRegion cluster_lms_outbound_region = {
	.name = "pgrac cluster lms data outbound",
	.size_fn = cluster_lms_outbound_shmem_size,
	.init_fn = cluster_lms_outbound_shmem_init,
	.lwlock_count = CLUSTER_LMS_MAX_WORKERS,
	.owner_subsys = "cluster_lms_outbound",
	.reserved_flags = 0,
};

/* Every physical Resource-X owner transition advances one shared witness
 * while its owning ring is held EXCLUSIVE.  The sequence is local shared
 * memory evidence for R10's optimistic snapshot, not wire or authority. */
static bool
lms_outbound_resource_x_transport_mutation_mark(void)
{
	uint64 current;

	Assert(cluster_lms_outbound_shared != NULL);
	current
		= pg_atomic_read_u64(&cluster_lms_outbound_shared->resource_x_transport_mutation_sequence);
	for (;;) {
		if (current == 0 || current == UINT64_MAX)
			return false;
		if (pg_atomic_compare_exchange_u64(
				&cluster_lms_outbound_shared->resource_x_transport_mutation_sequence, &current,
				current + 1))
			return true;
	}
}


/* Type 50 reports the holder's irreversible X->N handoff; types 51-56 drive
 * the resulting X grant to completion.  None may cross the final DATA
 * transport boundary while the protocol runtime or node write authority is
 * closed.  Admission/cancel/drain traffic remains independently routable. */
void
cluster_lms_outbound_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lms_outbound_region);
}

/* Named-tranche request (process_shmem_requests window;  I15 pattern). */
void
cluster_lms_outbound_request_lwlocks(void)
{
	RequestNamedLWLockTranche("ClusterLmsDataOutbound", CLUSTER_LMS_MAX_WORKERS);
}

/*
 * cluster_lms_outbound_enqueue — stage one DATA-plane frame for LMS.
 *
 *	Returns false on full ring / oversized payload / pre-shmem call;
 *	callers treat false as WOULD_BLOCK (retry via the normal request
 *	retry machinery).  Publish-before-signal: the slot is visible
 *	before the LMS wakeup fires.
 */
static bool
lms_outbound_enqueue_internal(int worker_id, uint8 msg_type, uint32 dest_node_id,
							  const void *payload, uint16 payload_len, uint32 required_capability,
							  uint32 connection_generation)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	ClusterLmsOutboundSlot *slot;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return false;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;
	if (payload_len > PGRAC_LMS_OUTBOUND_PAYLOAD_MAX)
		return false;

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = msg_type;
	slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_FRAME;
	slot->payload_len = payload_len;
	slot->required_capability = required_capability;
	slot->connection_generation = connection_generation;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);

	cluster_lms_wakeup(worker_id);
	return true;
}

bool
cluster_lms_outbound_enqueue(int worker_id, uint8 msg_type, uint32 dest_node_id,
							 const void *payload, uint16 payload_len)
{
	return lms_outbound_enqueue_internal(worker_id, msg_type, dest_node_id, payload, payload_len, 0,
										 0);
}

/* PGRAC adaptation for a remote non-requester S holder.  The existing DATA
 * ring retains an immutable but unsendable status before the BufferDesc
 * transition.  The caller later flips that exact slot READY only after its
 * exact REVOKING S tuple commits to N, so the ring lock and content lock are
 * never nested.  This is process-local transport retention, not GRD or a
 * second authority registry. */
ClusterPcmOwnResult
cluster_lms_outbound_stage_resource_x_remote_s_status_exact(
	int worker_id, uint32 dest_node_id, const void *payload, uint16 payload_len,
	const ClusterPcmOwnSnapshot *expected_revoking, ClusterLmsRemoteSStatusHandle *handle_out)
{
	ClusterLmsOutboundState *ring;
	ClusterLmsOutboundSlot *slot;
	LWLock *lock;
	uint64 slot_cookie;
	uint32 slot_index;

	if (handle_out != NULL)
		memset(handle_out, 0, sizeof(*handle_out));
	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS
		|| dest_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT || payload == NULL
		|| payload_len != RESOURCE_X_CONTROL_V1_BYTES || expected_revoking == NULL
		|| handle_out == NULL || expected_revoking->pcm_state != (uint8)PCM_STATE_S
		|| expected_revoking->flags != PCM_OWN_FLAG_REVOKING
		|| expected_revoking->reservation_token == 0
		|| expected_revoking->writer_activation_token != 0
		|| expected_revoking->resource_x_activation_generation != 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return CLUSTER_PCM_OWN_BUSY;
	}
	if (ring->next_remote_s_cookie == UINT64_MAX) {
		LWLockRelease(lock);
		return CLUSTER_PCM_OWN_EXHAUSTED;
	}
	slot_index = ring->head;
	slot_cookie = ++ring->next_remote_s_cookie;
	if (!lms_outbound_resource_x_transport_mutation_mark()) {
		LWLockRelease(lock);
		return CLUSTER_PCM_OWN_EXHAUSTED;
	}
	slot = &ring->ring[slot_index];
	memset(slot, 0, sizeof(*slot));
	slot->dest_node_id = dest_node_id;
	slot->msg_type = RESOURCE_X_MSG_BLOCKED_TO_N;
	slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_PENDING;
	slot->payload_len = payload_len;
	slot->local_tag = expected_revoking->tag;
	slot->local_slot_cookie = slot_cookie;
	slot->local_own_generation = expected_revoking->generation;
	slot->local_reservation_token = expected_revoking->reservation_token;
	memcpy(slot->payload, payload, payload_len);
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	handle_out->worker_id = worker_id;
	handle_out->slot_index = slot_index;
	handle_out->tag = expected_revoking->tag;
	handle_out->slot_cookie = slot_cookie;
	handle_out->own_generation = expected_revoking->generation;
	handle_out->reservation_token = expected_revoking->reservation_token;
	LWLockRelease(lock);

	return CLUSTER_PCM_OWN_OK;
}

static bool
lms_outbound_remote_s_status_handle_exact(const ClusterLmsOutboundSlot *slot,
										  const ClusterLmsRemoteSStatusHandle *handle)
{
	return slot != NULL && handle != NULL && handle->slot_cookie != 0
		   && handle->reservation_token != 0 && slot->msg_type == RESOURCE_X_MSG_BLOCKED_TO_N
		   && slot->payload_len == RESOURCE_X_CONTROL_V1_BYTES
		   && slot->local_slot_cookie == handle->slot_cookie
		   && slot->local_own_generation == handle->own_generation
		   && slot->local_reservation_token == handle->reservation_token
		   && BufferTagsEqual(&slot->local_tag, &handle->tag);
}

ClusterPcmOwnResult
cluster_lms_outbound_publish_resource_x_remote_s_status_exact(
	const ClusterLmsRemoteSStatusHandle *handle, const ClusterPcmOwnSnapshot *released_n)
{
	ClusterLmsOutboundState *ring;
	ClusterLmsOutboundSlot *slot;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_STALE;
	LWLock *lock;

	if (handle == NULL || released_n == NULL || handle->worker_id < 0
		|| handle->worker_id >= CLUSTER_LMS_MAX_WORKERS
		|| handle->slot_index >= PGRAC_LMS_OUTBOUND_CAPACITY || handle->own_generation == UINT64_MAX
		|| released_n->pcm_state != (uint8)PCM_STATE_N || released_n->flags != 0
		|| released_n->generation != handle->own_generation + 1
		|| released_n->reservation_token != handle->reservation_token
		|| released_n->writer_activation_token != 0
		|| released_n->resource_x_activation_generation != 0
		|| !BufferTagsEqual(&released_n->tag, &handle->tag))
		return CLUSTER_PCM_OWN_STALE;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(handle->worker_id) == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	ring = OB_RING(handle->worker_id);
	lock = OB_LOCK(handle->worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	slot = &ring->ring[handle->slot_index];
	if (lms_outbound_remote_s_status_handle_exact(slot, handle)
		&& (slot->kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_PENDING
			|| slot->kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_READY)) {
		if (!lms_outbound_resource_x_transport_mutation_mark()) {
			LWLockRelease(lock);
			return CLUSTER_PCM_OWN_EXHAUSTED;
		}
		slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_READY;
		result = CLUSTER_PCM_OWN_OK;
	}
	LWLockRelease(lock);
	if (result == CLUSTER_PCM_OWN_OK)
		cluster_lms_wakeup(handle->worker_id);
	return result;
}

ClusterPcmOwnResult
cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(
	const ClusterLmsRemoteSStatusHandle *handle)
{
	ClusterLmsOutboundState *ring;
	ClusterLmsOutboundSlot *slot;
	ClusterPcmOwnResult result = CLUSTER_PCM_OWN_STALE;
	LWLock *lock;

	if (handle == NULL || handle->worker_id < 0 || handle->worker_id >= CLUSTER_LMS_MAX_WORKERS
		|| handle->slot_index >= PGRAC_LMS_OUTBOUND_CAPACITY || handle->slot_cookie == 0
		|| handle->reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(handle->worker_id) == NULL)
		return CLUSTER_PCM_OWN_NOT_READY;
	ring = OB_RING(handle->worker_id);
	lock = OB_LOCK(handle->worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	slot = &ring->ring[handle->slot_index];
	if (slot->kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_PENDING
		&& lms_outbound_remote_s_status_handle_exact(slot, handle)) {
		if (!lms_outbound_resource_x_transport_mutation_mark()) {
			LWLockRelease(lock);
			return CLUSTER_PCM_OWN_EXHAUSTED;
		}
		slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_CANCELLED;
		result = CLUSTER_PCM_OWN_OK;
	}
	LWLockRelease(lock);
	if (result == CLUSTER_PCM_OWN_OK)
		cluster_lms_wakeup(handle->worker_id);
	return result;
}

/* Stage a wire-version-sensitive frame for one exact HELLO-authenticated
 * connection.  The reliable protocol leg remains outside this ring, so a
 * drain-side guard failure may consume this stale copy and let the periodic
 * producer reconstruct the correct wire version. */
bool
cluster_lms_outbound_enqueue_cap_bound(int worker_id, uint8 msg_type, uint32 dest_node_id,
									   const void *payload, uint16 payload_len,
									   uint32 required_capability, uint32 connection_generation)
{
	if (required_capability == 0 || dest_node_id >= CLUSTER_MAX_NODES)
		return false;
	return lms_outbound_enqueue_internal(worker_id, msg_type, dest_node_id, payload, payload_len,
										 required_capability, connection_generation);
}

static uint64
lms_outbound_monotonic_us(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_MICROSEC(now);
}

static bool
lms_outbound_resource_x_intent_valid(const ResourceXIntentSlot *intent)
{
	if (intent == NULL || intent->logical_generation == 0
		|| intent->logical_generation == UINT64_MAX || intent->authority_generation == 0
		|| intent->authority_generation == UINT64_MAX || intent->first_armed_us == 0
		|| intent->first_armed_us == UINT64_MAX
		|| intent->destination_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| intent->state != RESOURCE_X_INTENT_SLOT_ARMED
		|| intent->body.owner_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || intent->body.reserved != 0)
		return false;
	switch ((ResourceXIntentOwnerKind)intent->body.owner_kind) {
	case RESOURCE_X_INTENT_OWNER_MASTER_BLOCK:
		return intent->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
			   && intent->kind == RESOURCE_X_WIRE_BLOCK_TO_N
			   && intent->body.owner_generation == intent->logical_generation
			   && intent->body.owner_index == intent->destination_node
			   && intent->body.assertion.requester_node != (int32)intent->destination_node;
	case RESOURCE_X_INTENT_OWNER_HOLDER_STATUS:
		return (intent->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
				|| intent->payload_bytes == RESOURCE_X_PROOF_V1_BYTES)
			   && intent->kind == RESOURCE_X_WIRE_BLOCKED_TO_N
			   && intent->body.owner_generation == intent->logical_generation
			   && intent->body.owner_index == 0;
	case RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE:
		return intent->payload_bytes == RESOURCE_X_IMAGE_V1_BYTES
			   && intent->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
			   && intent->body.assertion.requester_node == (int32)intent->destination_node
			   && intent->body.owner_generation == intent->logical_generation
			   && intent->body.owner_index == 0;
	case RESOURCE_X_INTENT_OWNER_MASTER_GRANT:
		return intent->payload_bytes == RESOURCE_X_PROOF_V1_BYTES
			   && intent->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
			   && intent->body.assertion.requester_node == (int32)intent->destination_node
			   && intent->body.owner_generation == intent->authority_generation
			   && intent->body.owner_index == 0;
	case RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT:
		return intent->payload_bytes == RESOURCE_X_SHORT_V1_BYTES
			   && intent->kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT
			   && intent->body.assertion.requester_node == (int32)intent->body.owner_node
			   && intent->body.owner_generation == intent->logical_generation
			   && intent->body.owner_index == 0;
	case RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE:
		return intent->payload_bytes == RESOURCE_X_PROOF_V1_BYTES
			   && intent->kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			   && intent->body.assertion.requester_node != (int32)intent->destination_node
			   && intent->body.owner_generation == intent->logical_generation
			   && intent->body.owner_index == 0;
	default:
		return false;
	}
}

static uint8
lms_outbound_resource_x_intent_msg_type(const ResourceXIntentSlot *intent)
{
	if (intent == NULL)
		return 0;
	switch ((ResourceXWireKind)intent->kind) {
	case RESOURCE_X_WIRE_BLOCK_TO_N:
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
		return RESOURCE_X_MSG_BLOCK_TO_N;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
		return RESOURCE_X_MSG_BLOCKED_TO_N;
	case RESOURCE_X_WIRE_AUTHORITY_GRANT:
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE:
		return RESOURCE_X_MSG_IMAGE_OR_GRANT;
	case RESOURCE_X_WIRE_INSTALL_SETTLEMENT:
		return RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE;
	default:
		return 0;
	}
}

static bool
lms_outbound_resource_x_intent_identity_equal(const ResourceXIntentSlot *left,
											  const ResourceXIntentSlot *right)
{
	return left != NULL && right != NULL && left->logical_generation == right->logical_generation
		   && left->authority_generation == right->authority_generation
		   && left->first_armed_us == right->first_armed_us
		   && left->destination_node == right->destination_node
		   && left->payload_bytes == right->payload_bytes && left->kind == right->kind
		   && memcmp(&left->body, &right->body, sizeof(left->body)) == 0;
}

bool
cluster_lms_outbound_enqueue_resource_x_intent(int worker_id, const ResourceXIntentSlot *intent,
											   uint32 connection_generation, uint64 deadline_us)
{
	ClusterLmsOutboundState *ring;
	ClusterLmsOutboundSlot *slot;
	LWLock *lock;
	uint64 now_us;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS || connection_generation == 0
		|| deadline_us == 0 || !lms_outbound_resource_x_intent_valid(intent)
		|| cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;
	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	now_us = lms_outbound_monotonic_us();
	if (now_us == 0)
		return false;

	/* Publish the ring slot only after the exact semantic owner changes to
	 * STAGED.  Holding the ring lock across that owner mutation prevents a
	 * concurrently polling destination worker from consuming an ARMED slot
	 * before the transition becomes visible. */
	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	memset(slot, 0, sizeof(*slot));
	slot->dest_node_id = intent->destination_node;
	slot->msg_type = lms_outbound_resource_x_intent_msg_type(intent);
	slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT;
	slot->payload_len = sizeof(*intent);
	slot->required_capability = PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	slot->connection_generation = connection_generation;
	slot->deadline_us = deadline_us;
	memcpy(slot->payload, intent, sizeof(*intent));
	if (!lms_outbound_resource_x_transport_mutation_mark()) {
		LWLockRelease(lock);
		return false;
	}
	{
		ResourceXIntentResult stage_result;

		stage_result = cluster_pcm_lock_resource_x_outbound_intent_stage_exact(intent, now_us);
		if (stage_result != RESOURCE_X_INTENT_STAGED) {
			memset(slot, 0, sizeof(*slot));
			LWLockRelease(lock);
			return false;
		}
	}
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);
	cluster_lms_wakeup(worker_id);
	return true;
}

int
cluster_lms_outbound_resource_x_intent_pump(void)
{
	ResourceXIntentSlot intent;
	uint8 payload[RESOURCE_X_IMAGE_V1_BYTES];
	int staged = 0;
	int call;
	bool scan_more = false;

	Assert(MyBackendType == B_LMS);
	for (call = 0; call < PGRAC_LMS_RESOURCE_X_CALLS_PER_TICK; call++) {
		ResourceXIntentProbeResult probe_result;
		uint32 capability_word = 0;
		uint32 connection_generation = 0;
		uint32 examined = 0;
		uint64 deadline_us;
		uint64 now_us;
		uint64 timeout_us;
		int worker_id;
		bool enqueued;
		ResourceXAcquisitionRef delivery;

		probe_result = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			PGRAC_LMS_RESOURCE_X_PROBE_BUDGET, &intent, payload, sizeof(payload), &examined,
			&delivery);
		if (probe_result == RESOURCE_X_INTENT_PROBE_IDLE
			|| probe_result == RESOURCE_X_INTENT_PROBE_COMPLETE)
			break;
		if (probe_result == RESOURCE_X_INTENT_PROBE_CORRUPT)
			break;
		if (probe_result == RESOURCE_X_INTENT_PROBE_MORE) {
			scan_more = true;
			continue;
		}
		scan_more = false;
		if (probe_result == RESOURCE_X_INTENT_PROBE_DELIVERY) {
			/* A local callback is not a packet and consumes the same bounded
			 * call budget. BUSY waits for the existing event-loop tick. */
			(void)cluster_gcs_block_resource_x_delivery_tick(&delivery);
			continue;
		}
		if (probe_result == RESOURCE_X_INTENT_PROBE_SOURCE_FINISH) {
			/* Pin ownership is claimed after all scan locks have been dropped.
			 * A repeated BUSY consumes one call, not a wire slot or self-wake. */
			(void)cluster_gcs_block_resource_x_source_finish_tick(&delivery);
			continue;
		}
		if (probe_result != RESOURCE_X_INTENT_PROBE_FOUND)
			break;
		now_us = lms_outbound_monotonic_us();
		if (now_us == 0)
			continue;
		if ((int32)intent.destination_node == cluster_node_id) {
			if ((cluster_ic_local_capability_word() & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1)
				== 0) {
				(void)cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&intent,
																					 now_us);
				continue;
			}
			connection_generation = 1;
		} else if (!cluster_sf_peer_capability_word_sample(
					   (int32)intent.destination_node, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
					   &capability_word, &connection_generation)
				   || connection_generation == 0) {
			(void)cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&intent, now_us);
			continue;
		}
		worker_id = cluster_lms_shard_for_tag(&intent.body.assertion.resource, cluster_lms_workers);
		/* The physical copy inherits the existing GCS DATA transport
		 * deadline.  Expiry returns only the ring ownership; it is not a
		 * new Resource-X operation timeout or runtime policy. */
		timeout_us = (uint64)Max(cluster_gcs_reply_timeout_ms, 1) * UINT64_C(1000);
		deadline_us = now_us > UINT64_MAX - timeout_us ? UINT64_MAX : now_us + timeout_us;
		enqueued = cluster_lms_outbound_enqueue_resource_x_intent(
			worker_id, &intent, connection_generation, deadline_us);
		if (enqueued)
			staged++;
		else
			(void)cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&intent, now_us);
	}
	/* Preserve the frozen 4-probe/call and 16-call/iteration bounds without
	 * turning a truthful MORE cursor into an artificial 100ms idle period.
	 * The existing LMS latch schedules only the next bounded iteration. */
	if (scan_more)
		cluster_lms_wakeup(0);
	return staged;
}

static bool
lms_outbound_r4_refusal_header_valid(const GcsBlockReplyHeader *header)
{
	int32 forwarding_master;
	int i;

	if (header == NULL || !GcsBlockReplyStatusIsR4Refusal((GcsBlockReplyStatus)header->status)
		|| header->request_id == 0 || header->checksum != 0 || header->sender_node < 0
		|| header->sender_node >= CLUSTER_MAX_NODES || header->requester_backend_id <= 0
		|| header->transition_id != (uint8)PCM_TRANS_N_TO_S)
		return false;
	forwarding_master = GcsBlockReplyHeaderGetForwardingMasterNode(header);
	if (forwarding_master < GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| forwarding_master >= CLUSTER_MAX_NODES)
		return false;
	for (i = 0; i < (int)sizeof(header->reserved_0); i++)
		if (header->reserved_0[i] != 0)
			return false;
	/* A holder replies directly with the real forwarding master retained for
	 * requester authentication. Only a master-produced retry may redirect;
	 * neither a forwarded refusal nor a denial carries a page LSN. */
	if (forwarding_master != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| header->status == (uint8)GCS_BLOCK_REPLY_R4_DENIED)
		return header->page_lsn == 0;
	/* Status 25 optionally carries WRONG_MASTER as node+1.  The encoded
	 * value is therefore either zero or in [1, CLUSTER_MAX_NODES]. */
	return header->page_lsn <= (uint64)CLUSTER_MAX_NODES;
}

static bool
lms_outbound_enqueue_zero_block_reply_internal(int worker_id, uint32 dest_node_id,
											   const GcsBlockReplyHeader *header, bool direct_land,
											   uint32 required_capability,
											   uint32 connection_generation)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	ClusterLmsOutboundSlot *slot;
	bool r4_cap_bound = required_capability != 0;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS || dest_node_id >= CLUSTER_MAX_NODES
		|| header == NULL || (direct_land && (int32)dest_node_id == cluster_node_id))
		return false;
	if (r4_cap_bound) {
		if (direct_land || !lms_outbound_r4_refusal_header_valid(header))
			return false;
	} else if (header->status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X)
		return false;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	slot->kind = (uint8)(direct_land ? CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY
									 : CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY);
	slot->payload_len = sizeof(*header);
	slot->required_capability = required_capability;
	slot->connection_generation = connection_generation;
	memcpy(slot->payload, header, sizeof(*header));
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);

	cluster_lms_wakeup(worker_id);
	return true;
}

/* Stage the legacy Shape-B pending-X denial. */
bool
cluster_lms_outbound_enqueue_zero_block_reply(int worker_id, uint32 dest_node_id,
											  const GcsBlockReplyHeader *header, bool direct_land)
{
	return lms_outbound_enqueue_zero_block_reply_internal(worker_id, dest_node_id, header,
														  direct_land, 0, 0);
}

/* PGRAC R4 adaptation: stage a typed 25/26 refusal on the requester's exact
 * HELLO-authenticated connection.  The existing DATA slot remains 144 bytes;
 * only the 48-byte header is retained until drain expands the zero page. */
bool
cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(int worker_id, uint32 dest_node_id,
														const GcsBlockReplyHeader *header,
														uint32 required_capability,
														uint32 connection_generation)
{
	if (required_capability == 0)
		return false;
	return lms_outbound_enqueue_zero_block_reply_internal(
		worker_id, dest_node_id, header, false, required_capability, connection_generation);
}

/*
 * cluster_lms_outbound_drain_send — one worker drains + sends its own ring.
 *
 *	Bounded batch per call.  worker c only ever touches rings[c], so the
 *	single-consumer-single-tail guarantee holds per worker (spec-7.3 D4).
 *	The GCS block REQUEST pre-send hook (direct-land arm) rides along
 *	with the DATA consumer.
 *
 *	GCS serve-stall round-5 — the drain follows the four-state send
 *	ownership contract (see ClusterICSendResult):
 *
 *	  DONE / WOULD_BLOCK  frame is on the wire or ADMITTED into tier1's
 *	                      per-peer FIFO;  the slot is consumed and the
 *	                      frame is NEVER resubmitted.  (Pre-fix code
 *	                      head-requeued on WOULD_BLOCK — a duplicate
 *	                      frame on the per-peer stream, because tier1
 *	                      had usually retained the original.)
 *	  NOT_ADMITTED        transport refused;  the frame is RETAINED and
 *	                      the peer is marked blocked for the rest of the
 *	                      batch so its later frames keep per-peer order
 *	                      behind it.  Other peers keep flowing — one
 *	                      backpressured peer must not head-of-line block
 *	                      the worker ring (pre-fix code broke the batch).
 *	  HARD_ERROR          peer down — drop;  requesters retry
 *	                      fail-closed.
 *
 *	Retained frames go back at the HEAD in original order after the
 *	batch.  Producers may have refilled the ring meanwhile;  a retained
 *	frame that no longer fits is counted (requeue_drop, expected 0) —
 *	never silently discarded.
 */
int
cluster_lms_outbound_drain_send(int worker_id)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	int sent = 0;
	int scanned = 0;
	ClusterLmsOutboundSlot retained[64];
	int n_retained = 0;
	bool peer_blocked[CLUSTER_MAX_NODES] = { 0 };

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return 0;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return 0;

	Assert(MyBackendType == B_LMS || MyBackendType == B_LMS_WORKER);

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	while (scanned < 64) {
		ClusterLmsOutboundSlot slot;
		ClusterLmsZeroBlockReplyWire zero_reply;
		ResourceXIntentSlot resource_x_intent;
		ResourceXIntentSlot resource_x_current;
		ResourceXWireReject resource_x_reject = RESOURCE_X_WIRE_REJECT_NONE;
		uint8 resource_x_payload[RESOURCE_X_IMAGE_V1_BYTES];
		const void *send_payload;
		uint32 send_payload_len;
		ClusterICSendResult rc;
		uint64 now_us;
		bool resource_x_slot = false;
		bool remote_s_pending_slot = false;
		bool remote_s_status_slot = false;
		bool remote_s_cancelled_slot = false;
		bool resource_x_owned_slot = false;
		bool stop_after_remote_s = false;
		bool got = false;

		LWLockAcquire(lock, LW_EXCLUSIVE);
		if (ring->count > 0) {
			slot = ring->ring[ring->tail];
			remote_s_pending_slot
				= slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_PENDING;
			remote_s_status_slot
				= slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_READY;
			remote_s_cancelled_slot
				= slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_CANCELLED;
			resource_x_owned_slot = slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT;
			if (!remote_s_pending_slot && !remote_s_status_slot) {
				if (resource_x_owned_slot && !lms_outbound_resource_x_transport_mutation_mark()) {
					LWLockRelease(lock);
					return sent;
				}
				ring->tail = (ring->tail + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
				ring->count--;
			}
			got = true;
		}
		LWLockRelease(lock);
		if (!got)
			break;
		scanned++;
		if (remote_s_pending_slot)
			break;
		if (remote_s_cancelled_slot)
			continue;
		memset(&resource_x_intent, 0, sizeof(resource_x_intent));
		resource_x_slot = slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT;
		if (remote_s_status_slot
			&& (slot.msg_type != RESOURCE_X_MSG_BLOCKED_TO_N
				|| slot.payload_len != RESOURCE_X_CONTROL_V1_BYTES
				|| slot.dest_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT))
			break;
		if (resource_x_slot) {
			if (slot.payload_len != sizeof(resource_x_intent))
				continue;
			memcpy(&resource_x_intent, slot.payload, sizeof(resource_x_intent));
			if (slot.msg_type != lms_outbound_resource_x_intent_msg_type(&resource_x_intent))
				continue;
		}
		/* Wire-version-sensitive slots are valid only for the exact
		 * connection generation whose HELLO advertised the required bit.
		 * A drift consumes this stale ring copy without transport admission;
		 * no protocol ACK is generated, so the armed reliable leg retries. */
		if (slot.required_capability != 0
			&& (((int32)slot.dest_node_id == cluster_node_id
				 && (cluster_ic_local_capability_word() & slot.required_capability)
						!= slot.required_capability)
				|| ((int32)slot.dest_node_id != cluster_node_id
					&& !cluster_sf_peer_capability_generation_matches(
						(int32)slot.dest_node_id, slot.required_capability,
						slot.connection_generation)))) {
			cluster_lms_obs_note_outbound_cap_guard_drop(worker_id);
			if (resource_x_slot) {
				(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
			}
			continue;
		}
		send_payload = slot.payload_len > 0 ? slot.payload : NULL;
		send_payload_len = slot.payload_len;
		if (resource_x_slot) {
			now_us = lms_outbound_monotonic_us();
			if (now_us == 0 || now_us >= slot.deadline_us) {
				(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
					&resource_x_intent, now_us);
				continue;
			}
			if (cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(
					&resource_x_intent, &resource_x_current, resource_x_payload,
					sizeof(resource_x_payload))
					!= RESOURCE_X_APPLY_APPLIED
				|| resource_x_current.state != RESOURCE_X_INTENT_SLOT_STAGED
				|| !lms_outbound_resource_x_intent_identity_equal(&resource_x_intent,
																  &resource_x_current))
				continue;
			send_payload = resource_x_payload;
			send_payload_len = resource_x_current.payload_bytes;
			/* The retained owner holds a connection-neutral logical frame.
			 * Rebind only this staged physical copy to the current DATA session;
			 * requester ingress restores the canonical generation before it
			 * compares a proof-bound image CRC. */
			if (!cluster_resource_x_wire_rebind_sender_generation(
					slot.msg_type, resource_x_payload, (uint16)send_payload_len,
					slot.connection_generation, &resource_x_reject)) {
				(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
				continue;
			}
		} else if (remote_s_status_slot) {
			uint32 capability_word = 0;
			uint32 connection_generation = 0;

			memcpy(resource_x_payload, slot.payload, slot.payload_len);
			if ((int32)slot.dest_node_id == cluster_node_id) {
				/* A same-node master/holder has no peer HELLO generation.  The
				 * retained status already echoes the authenticated exact type-17;
				 * preserve that nonzero sender generation and let the ordinary
				 * local envelope ingress revalidate formation/session/capability. */
				if ((cluster_ic_local_capability_word()
					 & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1)
					== 0)
					break;
			} else {
				if (!cluster_sf_peer_capability_word_sample(
						(int32)slot.dest_node_id, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
						&capability_word, &connection_generation)
					|| connection_generation == 0
					|| !cluster_resource_x_wire_rebind_sender_generation(
						slot.msg_type, resource_x_payload, slot.payload_len, connection_generation,
						&resource_x_reject))
					break;
			}
			send_payload = resource_x_payload;
			send_payload_len = slot.payload_len;
		} else if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY
				   || slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY) {
			if (slot.msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
				|| slot.payload_len != sizeof(GcsBlockReplyHeader)) {
				rc = CLUSTER_IC_SEND_HARD_ERROR;
				goto handle_send_result;
			}
			memset(&zero_reply, 0, sizeof(zero_reply));
			memcpy(&zero_reply.header, slot.payload, sizeof(zero_reply.header));
			if (slot.required_capability == 0) {
				if (zero_reply.header.status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X) {
					rc = CLUSTER_IC_SEND_HARD_ERROR;
					goto handle_send_result;
				}
			} else if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY
					   || !lms_outbound_r4_refusal_header_valid(&zero_reply.header)) {
				rc = CLUSTER_IC_SEND_HARD_ERROR;
				goto handle_send_result;
			}
			zero_reply.header.checksum = cluster_gcs_block_compute_checksum(zero_reply.block_data);
			send_payload = &zero_reply;
			send_payload_len = sizeof(zero_reply);
		} else if (slot.kind != (uint8)CLUSTER_LMS_OUTBOUND_FRAME) {
			rc = CLUSTER_IC_SEND_HARD_ERROR;
			goto handle_send_result;
		}

		/* A peer that refused a frame this batch keeps its later frames
		 * queued BEHIND the refused one (per-peer order). */
		if (slot.dest_node_id < CLUSTER_MAX_NODES && peer_blocked[slot.dest_node_id]) {
			if (remote_s_status_slot)
				break;
			if (resource_x_slot) {
				(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
				continue;
			}
			Assert(n_retained < (int)lengthof(retained));
			retained[n_retained++] = slot;
			continue;
		}

		if (slot.msg_type == PGRAC_IC_MSG_GCS_BLOCK_REQUEST
			&& slot.payload_len == sizeof(GcsBlockRequestPayload))
			cluster_gcs_block_lmon_prepare_outbound_request((GcsBlockRequestPayload *)slot.payload,
															(int32)slot.dest_node_id);

		/*
		 * The generic IC send path deliberately treats dest=self as a no-op
		 * DONE.  That is correct for transport diagnostics, but not for a
		 * staged DATA actor: PCM-X frequently maps a tag's resource master to
		 * the requesting node, and its ENQUEUE/ACK must still execute on the
		 * tag-owning LMS worker.  Build the same envelope and dispatch it here,
		 * after dequeue and without the ring lock.  A handler response is
		 * staged back onto this tag's ring, preserving the one-worker FIFO and
		 * avoiding recursive handler execution.
		 */
		if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY)
			rc = cluster_gcs_block_send_direct_zero_reply((int32)slot.dest_node_id,
														  &zero_reply.header);
		else if ((int32)slot.dest_node_id == cluster_node_id) {
			ClusterICEnvelope env;

			if (cluster_ic_envelope_build(&env, slot.msg_type, (uint32)cluster_node_id,
										  slot.dest_node_id, send_payload, send_payload_len)
				&& cluster_ic_dispatch_envelope(&env, send_payload, cluster_node_id))
				rc = CLUSTER_IC_SEND_DONE;
			else
				rc = CLUSTER_IC_SEND_HARD_ERROR;
		} else
			rc = cluster_ic_send_envelope(slot.msg_type, (int32)slot.dest_node_id, send_payload,
										  send_payload_len);

	handle_send_result:
		/* Actual transport result, not admission or enqueue success. */
		cluster_pcm_lock_resource_x_trace_wire(slot.msg_type, (int32)slot.dest_node_id,
											   send_payload, send_payload_len, (int32)rc);
		if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY
			|| slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY)
			cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, rc);
		switch (rc) {
		case CLUSTER_IC_SEND_DONE:
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			/* On the wire or admitted (transport owns a copy). */
			if (resource_x_slot) {
				if (resource_x_intent.body.owner_kind == RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE)
					(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
						&resource_x_intent, lms_outbound_monotonic_us());
				else
					(void)cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
						&resource_x_intent);
			}
			if (remote_s_status_slot) {
				LWLockAcquire(lock, LW_EXCLUSIVE);
				if (ring->count == 0 || memcmp(&ring->ring[ring->tail], &slot, sizeof(slot)) != 0) {
					LWLockRelease(lock);
					return sent;
				}
				if (!lms_outbound_resource_x_transport_mutation_mark()) {
					LWLockRelease(lock);
					return sent;
				}
				memset(&ring->ring[ring->tail], 0, sizeof(ring->ring[ring->tail]));
				ring->tail = (ring->tail + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
				ring->count--;
				LWLockRelease(lock);
			}
			sent++;
			break;
		case CLUSTER_IC_SEND_NOT_ADMITTED:
			if (remote_s_status_slot) {
				cluster_lms_obs_note_outbound_not_admitted(worker_id);
				stop_after_remote_s = true;
				break;
			}
			if (slot.dest_node_id < CLUSTER_MAX_NODES)
				peer_blocked[slot.dest_node_id] = true;
			if (resource_x_slot)
				(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
			else {
				Assert(n_retained < (int)lengthof(retained));
				retained[n_retained++] = slot;
			}
			cluster_lms_obs_note_outbound_not_admitted(worker_id);
			break;
		case CLUSTER_IC_SEND_HARD_ERROR:
			/* The remote-S status is the sole retained proof after the exact
			 * local downgrade, so even a hard transport error keeps its ring
			 * owner.  Generic requests retain their original self-heal rule. */
			if (remote_s_status_slot) {
				stop_after_remote_s = true;
				break;
			}
			if (resource_x_slot)
				(void)cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
			break;
		}
		if (stop_after_remote_s)
			break;
	}

	/* Put retained frames back at the head, original order preserved
	 * (reverse-order head pushes).  count can only have grown from
	 * producers since the dequeues above, so a full ring is possible:
	 * count the drop — the retransmit machinery self-heals, but the S3
	 * gate treats a nonzero delta as a capacity red flag. */
	if (n_retained > 0) {
		int i;

		LWLockAcquire(lock, LW_EXCLUSIVE);
		for (i = n_retained - 1; i >= 0; i--) {
			if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
				cluster_lms_obs_note_outbound_requeue_drop(worker_id);
				continue;
			}
			ring->tail
				= (ring->tail + PGRAC_LMS_OUTBOUND_CAPACITY - 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
			ring->ring[ring->tail] = retained[i];
			ring->count++;
		}
		LWLockRelease(lock);
	}

	return sent;
}

uint32
cluster_lms_outbound_depth(int worker_id)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	uint32 depth;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return 0;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return 0;
	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	LWLockAcquire(lock, LW_SHARED);
	depth = ring->count;
	LWLockRelease(lock);
	return depth;
}

ClusterNormalStopPollResult
cluster_lms_outbound_normal_stop_poll(int *worker_out, uint32 *slot_out, const char **reason_out)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_READY;
	int worker;

	if (worker_out != NULL)
		*worker_out = -1;
	if (slot_out != NULL)
		*slot_out = UINT32_MAX;
	if (reason_out != NULL)
		*reason_out = "OUTBOUND_UNINITIALIZED";
	if (worker_out == NULL || slot_out == NULL || reason_out == NULL
		|| cluster_lms_outbound_shared == NULL || cluster_lms_outbound_rings == NULL
		|| cluster_lms_outbound_rings != cluster_lms_outbound_shared->rings
		|| cluster_lms_workers <= 0 || cluster_lms_workers > CLUSTER_LMS_MAX_WORKERS)
		return CLUSTER_NORMAL_STOP_INVALID;
	for (worker = 0; worker < CLUSTER_LMS_MAX_WORKERS; worker++) {
		if (OB_LOCK(worker) == NULL) {
			*worker_out = worker;
			return CLUSTER_NORMAL_STOP_INVALID;
		}
	}
	/* Same all-ring lock order as the existing transport snapshot. This is
	 * only ring ownership: retained local items and IC-owned copies require
	 * the actor's outer return and its separate transport-private poll. */
	for (worker = 0; worker < CLUSTER_LMS_MAX_WORKERS; worker++)
		LWLockAcquire(OB_LOCK(worker), LW_SHARED);
	*reason_out = "NONE";
	for (worker = 0; worker < CLUSTER_LMS_MAX_WORKERS; worker++) {
		const ClusterLmsOutboundState *ring = OB_RING(worker);
		const char *invalid_reason = NULL;
		uint32 bad_slot = UINT32_MAX;

		if (ring->head >= PGRAC_LMS_OUTBOUND_CAPACITY || ring->tail >= PGRAC_LMS_OUTBOUND_CAPACITY
			|| ring->count > PGRAC_LMS_OUTBOUND_CAPACITY
			|| (ring->tail + ring->count) % PGRAC_LMS_OUTBOUND_CAPACITY != ring->head)
			invalid_reason = "OUTBOUND_RING_GEOMETRY";
		else if (worker >= cluster_lms_workers && ring->count != 0)
			invalid_reason = "OUTBOUND_INACTIVE_RING";
		else {
			for (uint32 offset = 0; offset < ring->count; offset++) {
				uint32 index = (ring->tail + offset) % PGRAC_LMS_OUTBOUND_CAPACITY;
				const ClusterLmsOutboundSlot *slot = &ring->ring[index];

				if (slot->dest_node_id >= CLUSTER_MAX_NODES
					|| slot->kind > CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_CANCELLED
					|| slot->payload_len > PGRAC_LMS_OUTBOUND_PAYLOAD_MAX) {
					invalid_reason = "OUTBOUND_FRAME_INVALID";
					bad_slot = index;
					break;
				}
			}
		}
		if (invalid_reason != NULL) {
			*worker_out = worker;
			*slot_out = bad_slot;
			*reason_out = invalid_reason;
			result = CLUSTER_NORMAL_STOP_INVALID;
			break;
		}
		if (ring->count != 0 && result == CLUSTER_NORMAL_STOP_READY) {
			*worker_out = worker;
			*slot_out = ring->tail;
			*reason_out = "OUTBOUND_FRAME_PENDING";
			result = CLUSTER_NORMAL_STOP_PENDING;
		}
		/* Dequeued slot bytes/cookies are historical storage, not live entries. */
	}
	for (worker = CLUSTER_LMS_MAX_WORKERS - 1; worker >= 0; worker--)
		LWLockRelease(OB_LOCK(worker));
	return result;
}

#ifdef CLUSTER_LMS_OUTBOUND_UNIT_TEST
extern void cluster_lms_outbound_test_geometry(int worker, uint32 head, uint32 tail, uint32 count);
void
cluster_lms_outbound_test_geometry(int worker, uint32 head, uint32 tail, uint32 count)
{
	Assert(worker >= 0 && worker < CLUSTER_LMS_MAX_WORKERS);
	OB_RING(worker)->head = head;
	OB_RING(worker)->tail = tail;
	OB_RING(worker)->count = count;
}
#endif

uint64
cluster_lms_outbound_resource_x_staged_count(void)
{
	ClusterLmsResourceXTransportSnapshot snapshot;

	return cluster_lms_outbound_resource_x_transport_snapshot(&snapshot) ? snapshot.staged_count
																		 : UINT64_MAX;
}

bool
cluster_lms_outbound_resource_x_transport_snapshot(ClusterLmsResourceXTransportSnapshot *out)
{
	uint64 sequence;
	int worker_id;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	/* Absence of any production ring/lock is not an empty proof. */
	if (out == NULL || cluster_lms_outbound_shared == NULL || cluster_lms_outbound_rings == NULL)
		return false;
	for (worker_id = 0; worker_id < CLUSTER_LMS_MAX_WORKERS; worker_id++)
		if (OB_LOCK(worker_id) == NULL)
			return false;

	/* Hold all existing ring locks in worker order.  Producers own exactly one
	 * ring, so this creates one coherent all-ring snapshot without a new lock. */
	for (worker_id = 0; worker_id < CLUSTER_LMS_MAX_WORKERS; worker_id++)
		LWLockAcquire(OB_LOCK(worker_id), LW_SHARED);
	sequence
		= pg_atomic_read_u64(&cluster_lms_outbound_shared->resource_x_transport_mutation_sequence);
	if (sequence == 0 || sequence == UINT64_MAX)
		goto invalid;
	for (worker_id = 0; worker_id < CLUSTER_LMS_MAX_WORKERS; worker_id++) {
		ClusterLmsOutboundState *ring;
		uint32 offset;

		ring = OB_RING(worker_id);
		for (offset = 0; offset < ring->count; offset++) {
			const ClusterLmsOutboundSlot *slot
				= &ring->ring[(ring->tail + offset) % PGRAC_LMS_OUTBOUND_CAPACITY];

			if (slot->kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT
				|| slot->kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_PENDING
				|| slot->kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_REMOTE_S_STATUS_READY)
				out->staged_count++;
		}
	}
	out->mutation_sequence = sequence;
	for (worker_id = CLUSTER_LMS_MAX_WORKERS - 1; worker_id >= 0; worker_id--)
		LWLockRelease(OB_LOCK(worker_id));
	return true;

invalid:
	for (worker_id = CLUSTER_LMS_MAX_WORKERS - 1; worker_id >= 0; worker_id--)
		LWLockRelease(OB_LOCK(worker_id));
	memset(out, 0, sizeof(*out));
	return false;
}

#endif /* USE_PGRAC_CLUSTER */
