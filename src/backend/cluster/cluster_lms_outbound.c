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
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_write_fence.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

#ifdef USE_PGRAC_CLUSTER

#define PGRAC_LMS_OUTBOUND_CAPACITY 256
#define PGRAC_LMS_OUTBOUND_PAYLOAD_MAX 128

typedef enum ClusterLmsOutboundKind {
	CLUSTER_LMS_OUTBOUND_FRAME = 0,
	CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY = 1,
	CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY = 2
} ClusterLmsOutboundKind;

typedef struct ClusterLmsOutboundSlot {
	uint32 dest_node_id;
	uint8 msg_type;
	uint8 kind;
	uint16 payload_len;
	uint32 required_capability;
	uint32 connection_generation;
	uint8 payload[PGRAC_LMS_OUTBOUND_PAYLOAD_MAX];
} ClusterLmsOutboundSlot;

StaticAssertDecl(sizeof(ClusterLmsOutboundSlot) == 144,
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
	ClusterLmsOutboundSlot ring[PGRAC_LMS_OUTBOUND_CAPACITY];
} ClusterLmsOutboundState;

/*
 * spec-7.3 D4 — one ring per DATA worker channel.  rings[0] is worker 0 (the
 * spec-7.2 ring; lms_workers=1 uses only it — byte-identical), rings[c] is
 * worker c.  Sizing is the compile-time cap (CLUSTER_LMS_MAX_WORKERS); the
 * live count follows cluster.lms_workers.  Each ring keeps the Q6-B single-
 * consumer single-tail FIFO semantics, and each has its own lock so worker c
 * drains rings[c] without contending on the other workers.
 */
static ClusterLmsOutboundState *cluster_lms_outbound_rings = NULL;
static LWLock *cluster_lms_outbound_locks[CLUSTER_LMS_MAX_WORKERS];

#define OB_RING(w) (&cluster_lms_outbound_rings[(w)])
#define OB_LOCK(w) (cluster_lms_outbound_locks[(w)])

static Size
cluster_lms_outbound_shmem_size(void)
{
	/* Contiguous array of CLUSTER_LMS_MAX_WORKERS rings; the C array stride is
	 * sizeof(ClusterLmsOutboundState), so size the whole block then MAXALIGN. */
	return MAXALIGN(mul_size(CLUSTER_LMS_MAX_WORKERS, sizeof(ClusterLmsOutboundState)));
}

static void
cluster_lms_outbound_shmem_init(void)
{
	bool found;
	int i;

	cluster_lms_outbound_rings = (ClusterLmsOutboundState *)ShmemInitStruct(
		"pgrac cluster lms data outbound", cluster_lms_outbound_shmem_size(), &found);
	if (!found)
		memset(cluster_lms_outbound_rings, 0,
			   CLUSTER_LMS_MAX_WORKERS * sizeof(ClusterLmsOutboundState));

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


/* Type 50 reports the holder's irreversible X->N handoff; types 51-56 drive
 * the resulting X grant to completion.  None may cross the final DATA
 * transport boundary while the protocol runtime or node write authority is
 * closed.  Admission/cancel/drain traffic remains independently routable. */
static bool
lms_outbound_pcm_x_grant_held(uint8 msg_type)
{
	PcmXRuntimeSnapshot runtime;

	if (msg_type < PGRAC_IC_MSG_PCM_X_IMAGE_READY || msg_type > PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM)
		return false;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE)
		return true;
	return cluster_write_fence_enforcing() && !cluster_write_fence_allowed();
}


static void
lms_outbound_pcm_x_image_ready_note(const ClusterLmsOutboundSlot *slot, const char *boundary,
									int result)
{
	const PcmXGrantPayload *ready;
	PcmXRuntimeSnapshot runtime;
	bool fence_enforcing;
	bool fence_allowed;

	if (slot == NULL || boundary == NULL
		|| (slot->msg_type != PGRAC_IC_MSG_PCM_X_IMAGE_READY
			&& slot->msg_type != PGRAC_IC_MSG_PCM_X_PREPARE_GRANT)
		|| slot->payload_len != sizeof(PcmXGrantPayload))
		return;
	ready = (const PcmXGrantPayload *)slot->payload;
	runtime = cluster_pcm_x_runtime_snapshot();
	fence_enforcing = cluster_write_fence_enforcing();
	fence_allowed = !fence_enforcing || cluster_write_fence_allowed();
	cluster_lms_note_pcm_x_image_ready_boundary(
		slot->msg_type, boundary, result, (int)runtime.state, fence_enforcing, fence_allowed,
		slot->dest_node_id, ready->ref.identity.request_id, ready->ref.handle.ticket_id,
		ready->ref.grant_generation, ready->image.image_id);
}

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

/*
 * Stage a header-only GCS denial from a CONTROL-plane producer.  The DATA
 * owner expands the ABI-mandated zero block immediately before transport
 * admission.  Keep this surface narrow: only the Shape-B pending-X denial
 * may use it, and callers must choose worker[shard(tag)] so it stays on the
 * same per-tag stream as the request it terminates.
 */
bool
cluster_lms_outbound_enqueue_zero_block_reply(int worker_id, uint32 dest_node_id,
											  const GcsBlockReplyHeader *header, bool direct_land)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	ClusterLmsOutboundSlot *slot;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS || dest_node_id >= CLUSTER_MAX_NODES
		|| header == NULL || (direct_land && (int32)dest_node_id == cluster_node_id)
		|| header->status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X)
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
	slot->required_capability = 0;
	slot->connection_generation = 0;
	memcpy(slot->payload, header, sizeof(*header));
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);

	cluster_lms_wakeup(worker_id);
	return true;
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
		const void *send_payload;
		uint32 send_payload_len;
		ClusterICSendResult rc;
		bool got = false;

		LWLockAcquire(lock, LW_EXCLUSIVE);
		if (ring->count > 0) {
			slot = ring->ring[ring->tail];
			ring->tail = (ring->tail + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
			ring->count--;
			got = true;
		}
		LWLockRelease(lock);
		if (!got)
			break;
		scanned++;
		/* Wire-version-sensitive slots are valid only for the exact
		 * connection generation whose HELLO advertised the required bit.
		 * A drift consumes this stale ring copy without transport admission;
		 * no protocol ACK is generated, so the armed reliable leg retries. */
		if (slot.required_capability != 0
			&& !cluster_sf_peer_capability_generation_matches(
				(int32)slot.dest_node_id, slot.required_capability, slot.connection_generation)) {
			lms_outbound_pcm_x_image_ready_note(&slot, "capability-guard", -1);
			cluster_lms_obs_note_outbound_cap_guard_drop(worker_id);
			continue;
		}
		send_payload = slot.payload_len > 0 ? slot.payload : NULL;
		send_payload_len = slot.payload_len;
		if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY
			|| slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY) {
			if (slot.msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
				|| slot.payload_len != sizeof(GcsBlockReplyHeader)) {
				rc = CLUSTER_IC_SEND_HARD_ERROR;
				goto handle_send_result;
			}
			memset(&zero_reply, 0, sizeof(zero_reply));
			memcpy(&zero_reply.header, slot.payload, sizeof(zero_reply.header));
			if (zero_reply.header.status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X) {
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
			lms_outbound_pcm_x_image_ready_note(&slot, "peer-blocked", -2);
			Assert(n_retained < (int)lengthof(retained));
			retained[n_retained++] = slot;
			continue;
		}

		/* Revalidate irreversible PCM-X grant authority immediately before
		 * transport admission.  Retaining also blocks later same-peer frames,
		 * preserving the DATA FIFO while unrelated peers keep flowing. */
		if (lms_outbound_pcm_x_grant_held(slot.msg_type)) {
			lms_outbound_pcm_x_image_ready_note(&slot, "grant-held", -3);
			if (slot.dest_node_id < CLUSTER_MAX_NODES)
				peer_blocked[slot.dest_node_id] = true;
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
		lms_outbound_pcm_x_image_ready_note(&slot, "send-result", (int)rc);
		if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY
			|| slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY)
			cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, rc);
		switch (rc) {
		case CLUSTER_IC_SEND_DONE:
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			/* On the wire or admitted (transport owns a copy). */
			sent++;
			break;
		case CLUSTER_IC_SEND_NOT_ADMITTED:
			if (slot.dest_node_id < CLUSTER_MAX_NODES)
				peer_blocked[slot.dest_node_id] = true;
			Assert(n_retained < (int)lengthof(retained));
			retained[n_retained++] = slot;
			cluster_lms_obs_note_outbound_not_admitted(worker_id);
			break;
		case CLUSTER_IC_SEND_HARD_ERROR:
			/* peer down — drop;  requesters retry fail-closed. */
			break;
		}
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

#endif /* USE_PGRAC_CLUSTER */
