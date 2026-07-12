/*-------------------------------------------------------------------------
 *
 * cluster_gcs.c
 *	  pgrac cluster GCS (Global Cache Service) request protocol skeleton.
 *
 *	  Implements the wire framework for cross-node PCM transition requests:
 *	    - Master lookup helper (placeholder return self in spec-2.32)
 *	    - Sender API: cluster_gcs_send_transition_and_wait
 *	    - Receiver handlers for GCS_REQUEST and GCS_REPLY
 *	    - Per-backend outstanding-request table (LWLock protected)
 *	    - 14 observability counters (dump_gcs surface)
 *
 *	  spec-2.32 single-node loopback only:  production master==self
 *	  short-circuits before any wire send (HC72).  Wire path coverage
 *	  is test-only via cluster_gcs_test_force_remote_master_node + the
 *	  cluster_gcs_test_loopback_dispatch helper (cluster_unit/TAP builds).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_gcs.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.32-gcs-request-protocol-skeleton.md (FROZEN v0.4)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 + AD-002
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h" /* spec-4.7 D7 — peer liveness for recovery-aware routing */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/backendid.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/* ============================================================
 * Shared-memory layout.
 * ============================================================ */

/*
 * Per-backend outstanding-request slot.  MAX_OUTSTANDING_REQUESTS_PER_BACKEND
 * slots per backend (HC74).  reply_received transitions to true exactly
 * once per request_id; sender's ConditionVariableSleep wakes up when the
 * matching reply handler signals reply_cv.
 */
typedef struct ClusterGcsOutstandingSlot {
	bool in_use;
	uint64 request_id;
	uint8 transition_id;
	BufferTag tag;
	int32 master_node;
	bool reply_received;
	GcsReplyPayload reply; /* populated by reply handler */
	ConditionVariable reply_cv;
} ClusterGcsOutstandingSlot;

/*
 * Per-backend block of MAX_OUTSTANDING_REQUESTS_PER_BACKEND slots.
 * Indexed by MyProcNumber (formerly MyBackendId in older PG).  PG 16
 * uses MyProcNumber from postmaster/auxprocess; we use it directly as
 * the per-backend index.
 *
 * Slot reservation/release is protected by a single LWLock for the
 * backend's block (8 slots = small contention surface).
 */
typedef struct ClusterGcsBackendBlock {
	LWLockPadded lock;
	ClusterGcsOutstandingSlot slots[MAX_OUTSTANDING_REQUESTS_PER_BACKEND];
	uint64 next_request_id; /* per-backend monotone counter */
} ClusterGcsBackendBlock;

/*
 * Module-level shmem header.  Sized for MaxBackends; counters are atomic
 * uint64 to allow lock-free read by dump_gcs SQL surface.
 */
typedef struct ClusterGcsShared {
	pg_atomic_uint64 lookup_master_self_count;
	pg_atomic_uint64 lookup_master_remote_count;
	pg_atomic_uint64 send_request_count;
	pg_atomic_uint64 handle_request_count;
	pg_atomic_uint64 handle_reply_count;
	pg_atomic_uint64 reply_late_drop_count;
	pg_atomic_uint64 reply_timeout_count;
	pg_atomic_uint64 encode_payload_bytes;
	pg_atomic_uint64 decode_payload_bytes;
	pg_atomic_uint64 dispatch_loop_iterations;
	pg_atomic_uint32 outstanding_count;
	pg_atomic_uint32 max_outstanding;
	/* Backend blocks live immediately after this header in shmem. */
} ClusterGcsShared;


static ClusterGcsShared *ClusterGcs = NULL;
static ClusterGcsBackendBlock *gcs_backend_blocks = NULL;


/* ============================================================
 * Test-only injection (cluster_unit / TAP builds only).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT
int cluster_gcs_test_force_remote_master_node = -1;
#endif


/* ============================================================
 * Forward decls.
 * ============================================================ */
static ClusterGcsOutstandingSlot *gcs_reserve_slot(BufferTag tag, uint8 transition_id,
												   int32 master_node, uint64 *out_request_id);
static void gcs_release_slot(ClusterGcsOutstandingSlot *slot);
static bool gcs_slot_get_reply(ClusterGcsOutstandingSlot *slot, GcsReplyPayload *reply);
static bool gcs_mark_slot_reply(const GcsReplyPayload *reply);
static ClusterICSendResult gcs_send_envelope_or_loopback(uint8 msg_type, int32 dest_node,
														 const void *payload, uint32 payload_len);
static bool gcs_dispatch_loopback(uint8 msg_type, const void *payload, uint32 payload_len);
static void gcs_send_reply(int32 dest_node, uint64 request_id, uint8 transition_id,
						   GcsReplyStatus status);


/* ============================================================
 * Module init + shmem registration.
 * ============================================================ */

Size
cluster_gcs_shmem_size(void)
{
	Size sz;

	sz = MAXALIGN(sizeof(ClusterGcsShared));
	sz = add_size(sz, mul_size(MaxBackends, sizeof(ClusterGcsBackendBlock)));
	return sz;
}

void
cluster_gcs_shmem_init(void)
{
	bool found;
	char *base;
	int i;
	int j;

	base = (char *)ShmemInitStruct("pgrac cluster gcs", cluster_gcs_shmem_size(), &found);
	ClusterGcs = (ClusterGcsShared *)base;
	gcs_backend_blocks = (ClusterGcsBackendBlock *)(base + MAXALIGN(sizeof(ClusterGcsShared)));

	if (!found) {
		memset(ClusterGcs, 0, sizeof(*ClusterGcs));
		pg_atomic_init_u64(&ClusterGcs->lookup_master_self_count, 0);
		pg_atomic_init_u64(&ClusterGcs->lookup_master_remote_count, 0);
		pg_atomic_init_u64(&ClusterGcs->send_request_count, 0);
		pg_atomic_init_u64(&ClusterGcs->handle_request_count, 0);
		pg_atomic_init_u64(&ClusterGcs->handle_reply_count, 0);
		pg_atomic_init_u64(&ClusterGcs->reply_late_drop_count, 0);
		pg_atomic_init_u64(&ClusterGcs->reply_timeout_count, 0);
		pg_atomic_init_u64(&ClusterGcs->encode_payload_bytes, 0);
		pg_atomic_init_u64(&ClusterGcs->decode_payload_bytes, 0);
		pg_atomic_init_u64(&ClusterGcs->dispatch_loop_iterations, 0);
		pg_atomic_init_u32(&ClusterGcs->outstanding_count, 0);
		pg_atomic_init_u32(&ClusterGcs->max_outstanding, 0);

		for (i = 0; i < MaxBackends; i++) {
			ClusterGcsBackendBlock *blk = &gcs_backend_blocks[i];

			LWLockInitialize(&blk->lock.lock, LWTRANCHE_CLUSTER_GCS);
			blk->next_request_id = 1;
			for (j = 0; j < MAX_OUTSTANDING_REQUESTS_PER_BACKEND; j++) {
				ClusterGcsOutstandingSlot *slot = &blk->slots[j];

				slot->in_use = false;
				slot->request_id = 0;
				slot->reply_received = false;
				ConditionVariableInit(&slot->reply_cv);
			}
		}
	}
}

/* Region descriptor consumed by cluster_shmem registry. */
static const ClusterShmemRegion cluster_gcs_region = {
	.name = "pgrac cluster gcs",
	.size_fn = cluster_gcs_shmem_size,
	.init_fn = cluster_gcs_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_gcs",
	.reserved_flags = 0,
};

void
cluster_gcs_module_init(void)
{
	cluster_shmem_register_region(&cluster_gcs_region);
}


/* ============================================================
 * Master lookup helper.
 * ============================================================ */

/*
 * PGRAC: spec-2.33 D2 — real GRD master cache via deterministic hash mod-N
 * over the declared-node array.  Sparse node_id safe (HC81):  if topology
 * is {0, 2, 5} the helper computes `declared[hash % 3]` and returns one of
 * 0/2/5, never some out-of-topology index like 4.  Single-node (declared
 * count <= 1) falls back to self for HC72 short-circuit.
 *
 * Test injection (USE_CLUSTER_UNIT) preserved from spec-2.32: when the
 * fixture sets cluster_gcs_test_force_remote_master_node >= 0 the helper
 * returns that override unconditionally to drive remote-master code paths
 * in single-node test setups.
 */
/*
 * cluster_gcs_lookup_master_static -- spec-4.7 D7.
 *
 *	The PURE static declared-list hash master (NO recovery re-route, NO
 *	test-force, NO counters).  This is the block's ORIGINAL master — possibly
 *	a dead node.  The recovery-phase gate (cluster_gcs_block_phase_for_tag)
 *	uses it to identify which origin's merged WAL recovery must materialize
 *	before a re-routed survivor master may serve the block (D7).
 */
int
cluster_gcs_lookup_master_static(BufferTag tag)
{
	int declared[CLUSTER_MAX_NODES];
	int declared_count = 0;
	uint32 h;
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (cluster_conf_lookup_node(i) != NULL)
			declared[declared_count++] = i;

	if (declared_count <= 1)
		return cluster_node_id; /* single-node fallback (HC72) */

	h = hash_bytes((const unsigned char *)&tag, sizeof(tag));
	return declared[h % (uint32)declared_count];
}

int
cluster_gcs_lookup_master(BufferTag tag)
{
	int declared[CLUSTER_MAX_NODES];
	int declared_count = 0;
	int master_node;
	uint32 h;
	int i;

#ifdef USE_CLUSTER_UNIT
	if (cluster_gcs_test_force_remote_master_node >= 0
		&& cluster_gcs_test_force_remote_master_node != cluster_node_id) {
		if (ClusterGcs != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcs->lookup_master_remote_count, 1);
		return cluster_gcs_test_force_remote_master_node;
	}
#endif

	/*
	 * Build the declared-node array.  HC81 sparse-node safety:  hash modulo
	 * declared_count (not CLUSTER_MAX_NODES) so the result is always an
	 * index into declared[], i.e. always a real cluster.node_id.
	 */
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (cluster_conf_lookup_node(i) != NULL)
			declared[declared_count++] = i;

	if (declared_count <= 1) {
		if (ClusterGcs != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcs->lookup_master_self_count, 1);
		return cluster_node_id; /* single-node fallback (HC72) */
	}

	h = hash_bytes((const unsigned char *)&tag, sizeof(tag));
	master_node = declared[h % (uint32)declared_count];

	/*
	 * spec-4.7 D7 — recovery-aware re-route.  If the STATIC declared master is
	 * DEAD, route to a live survivor by re-hashing over the live declared
	 * nodes (self is always live).  The HEALTHY path (static master alive)
	 * skips this entirely and returns the static master UNCHANGED — behaviour
	 * is identical to pre-D7 in normal operation;  only the dead-master
	 * recovery case re-routes.  Convergence note:  this routes over the LIVE
	 * CSSD set read per-call (not an epoch-locked snapshot), so during a window
	 * where a peer is SUSPECTED on one survivor but already DEAD on another the
	 * two could pick different survivor masters for the same block.  That does
	 * NOT cause a double-serve:  cross-survivor agreement is enforced DOWNSTREAM
	 * by the RECOVERING gate (cluster_gcs_block_phase_for_tag fail-closes until
	 * the dead origin materializes, by which point CSSD has converged on the
	 * dead set), not by this routing function.  SCOPE (D7):  dead-master recovery routing
	 * ONLY — NO normal-operation dynamic remaster, NO affinity DRM, NO writer
	 * transfer.  Whether the re-routed survivor may actually SERVE the block is
	 * separately gated by cluster_gcs_block_phase_for_tag (RECOVERING until the
	 * dead origin's merged WAL recovery materializes — redo-before-unfreeze).
	 */
	if (master_node != cluster_node_id
		&& cluster_cssd_get_peer_state(master_node) == CLUSTER_CSSD_PEER_DEAD) {
		int live[CLUSTER_MAX_NODES];
		int live_count = 0;

		for (i = 0; i < declared_count; i++)
			if (declared[i] == cluster_node_id
				|| cluster_cssd_get_peer_state(declared[i]) != CLUSTER_CSSD_PEER_DEAD)
				live[live_count++] = declared[i];
		if (live_count >= 1)
			master_node = live[h % (uint32)live_count];
		/* else (all declared dead but self excluded — impossible since self is
		 * live): keep static master; downstream dead-master guard fail-closes. */
	}

	if (ClusterGcs != NULL) {
		if (master_node == cluster_node_id)
			pg_atomic_fetch_add_u64(&ClusterGcs->lookup_master_self_count, 1);
		else
			pg_atomic_fetch_add_u64(&ClusterGcs->lookup_master_remote_count, 1);
	}
	return master_node;
}


/* ============================================================
 * Outstanding-request slot management (HC74).
 * ============================================================ */

static ClusterGcsBackendBlock *
gcs_my_block(void)
{
	int idx;

	/* PG 16 uses MyBackendId (1..MaxBackends) for backend-local addressing.
	 * Convert to 0-based array index. */
	idx = MyBackendId - 1;
	if (idx < 0 || idx >= MaxBackends)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs: MyBackendId=%d out of [1, MaxBackends=%d] range",
							   (int)MyBackendId, MaxBackends)));
	return &gcs_backend_blocks[idx];
}

static ClusterGcsOutstandingSlot *
gcs_reserve_slot(BufferTag tag, uint8 transition_id, int32 master_node, uint64 *out_request_id)
{
	ClusterGcsBackendBlock *blk = gcs_my_block();
	ClusterGcsOutstandingSlot *slot = NULL;
	int i;
	uint32 cur_outstanding;

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_REQUESTS_PER_BACKEND; i++) {
		if (!blk->slots[i].in_use) {
			slot = &blk->slots[i];
			slot->in_use = true;
			slot->reply_received = false;
			slot->request_id = blk->next_request_id++;
			slot->transition_id = transition_id;
			slot->tag = tag;
			slot->master_node = master_node;
			*out_request_id = slot->request_id;
			break;
		}
	}
	LWLockRelease(&blk->lock.lock);

	if (slot == NULL)
		ereport(ERROR, (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
						errmsg("cluster_gcs: outstanding-request table full (max %d per backend)",
							   MAX_OUTSTANDING_REQUESTS_PER_BACKEND),
						errhint("Reduce concurrent buffer acquisitions; outstanding "
								"size GUC lands in spec-2.33+.")));

	cur_outstanding = pg_atomic_add_fetch_u32(&ClusterGcs->outstanding_count, 1);
	{
		/* Track running max via compare-and-swap loop. */
		uint32 old_max;

		do {
			old_max = pg_atomic_read_u32(&ClusterGcs->max_outstanding);
			if (cur_outstanding <= old_max)
				break;
		} while (!pg_atomic_compare_exchange_u32(&ClusterGcs->max_outstanding, &old_max,
												 cur_outstanding));
	}

	return slot;
}

static void
gcs_release_slot(ClusterGcsOutstandingSlot *slot)
{
	ClusterGcsBackendBlock *blk = gcs_my_block();

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	slot->in_use = false;
	slot->reply_received = false;
	slot->request_id = 0;
	slot->transition_id = 0;
	slot->master_node = -1;
	LWLockRelease(&blk->lock.lock);

	pg_atomic_sub_fetch_u32(&ClusterGcs->outstanding_count, 1);
}

static bool
gcs_slot_get_reply(ClusterGcsOutstandingSlot *slot, GcsReplyPayload *reply)
{
	ClusterGcsBackendBlock *blk = gcs_my_block();
	bool got_reply;

	LWLockAcquire(&blk->lock.lock, LW_SHARED);
	got_reply = slot->in_use && slot->reply_received;
	if (got_reply && reply != NULL)
		*reply = slot->reply;
	LWLockRelease(&blk->lock.lock);

	return got_reply;
}

static bool
gcs_mark_slot_reply(const GcsReplyPayload *reply)
{
	int b;
	int i;

	/*
	 * spec-2.32 loopback:  request_id is unique per backend, so scan all
	 * backend blocks for the matching slot.  The scan is LWLock-protected
	 * per block because reply_received/reply are mutated by receiver context
	 * while the sender wait loop polls the same slot.
	 */
	for (b = 0; b < MaxBackends; b++) {
		ClusterGcsBackendBlock *blk = &gcs_backend_blocks[b];

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		for (i = 0; i < MAX_OUTSTANDING_REQUESTS_PER_BACKEND; i++) {
			ClusterGcsOutstandingSlot *slot = &blk->slots[i];

			if (slot->in_use && slot->request_id == reply->request_id) {
				slot->reply = *reply;
				slot->reply_received = true;
				ConditionVariableSignal(&slot->reply_cv);
				LWLockRelease(&blk->lock.lock);
				return true;
			}
		}
		LWLockRelease(&blk->lock.lock);
	}
	return false;
}

static bool
gcs_dispatch_loopback(uint8 msg_type, const void *payload, uint32 payload_len)
{
	ClusterICEnvelope env;

	if (!cluster_ic_envelope_build(&env, msg_type, (uint32)cluster_node_id, (uint32)cluster_node_id,
								   payload, payload_len))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs loopback envelope_build failed")));

	pg_atomic_fetch_add_u64(&ClusterGcs->dispatch_loop_iterations, 1);
	return cluster_ic_dispatch_envelope(&env, payload, cluster_node_id);
}

static ClusterICSendResult
gcs_send_envelope_or_loopback(uint8 msg_type, int32 dest_node, const void *payload,
							  uint32 payload_len)
{
	ClusterICSendResult rc;

	if (dest_node == cluster_node_id)
		return gcs_dispatch_loopback(msg_type, payload, payload_len) ? CLUSTER_IC_SEND_DONE
																	 : CLUSTER_IC_SEND_HARD_ERROR;

	/*
	 * GCS requests can be produced from bufmgr/content-lock backend paths.
	 * CONTROL-plane sockets are owned by LMON, so non-LMON request
	 * producers enqueue to the existing GRD outbound ring and wait on the
	 * normal reply slot.  PGRAC: spec-7.2 D3 — this branch is also the
	 * staging rule for LMS:  a migrated DATA-plane handler that must emit
	 * GCS_REQUEST (a CONTROL message;  the ㉕ X->S downgrade notify and
	 * the BAST-nudge branch) lands here by the same != B_LMON test and
	 * goes through the ring for LMON to send — a direct send would trip
	 * the router's plane gate by design.
	 */
	if (msg_type == PGRAC_IC_MSG_GCS_REQUEST && MyBackendType != B_LMON)
		return cluster_grd_outbound_enqueue_backend_msg(msg_type, (uint32)dest_node, payload,
														(uint16)payload_len)
				   ? CLUSTER_IC_SEND_DONE
				   : CLUSTER_IC_SEND_WOULD_BLOCK;

	rc = cluster_ic_send_envelope(msg_type, dest_node, payload, payload_len);
	return rc;
}


/* ============================================================
 * Sender API (D3).
 * ============================================================ */

/*
 * gcs_transition_and_wait_internal — shared body of the throwing
 * (cluster_gcs_send_transition_and_wait) and non-throwing
 * (cluster_gcs_try_send_transition_and_wait, spec-6.12a ㉕) senders.
 *
 *	Returns the final GcsReplyStatus (GCS_REPLY_GRANTED on success; a
 *	DENIED_* value or the DENIED_INCOMPATIBLE internal-timeout marker
 *	otherwise).  When throw_on_send_fail is false, a transport send failure
 *	is reported as DENIED_INCOMPATIBLE instead of ereport — required by the
 *	㉕ degrade-to-one-shot requester path, where a failed registration must
 *	not abort the query.  *out_final_transition echoes the reply's
 *	transition_id for the throwing wrapper's error messages.
 */
static uint8
gcs_transition_and_wait_internal(BufferTag tag, PcmLockTransition transition_id, int master_node,
								 bool throw_on_send_fail, uint8 *out_final_transition)
{
	ClusterGcsOutstandingSlot *slot;
	uint64 request_id = 0;
	GcsRequestPayload payload;
	GcsReplyPayload final_reply;
	TimestampTz deadline;
	bool send_failed = false;
	uint8 final_status = GCS_REPLY_DENIED_INCOMPATIBLE;

	if (transition_id < PCM_TRANS_N_TO_S || transition_id > PCM_TRANS_S_TO_X_CLEANOUT) {
		/* Caller passed garbage; receiver would reject too.  Fail fast. */
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_gcs_send_transition_and_wait: illegal transition_id=%d",
							   (int)transition_id)));
	}

	slot = gcs_reserve_slot(tag, (uint8)transition_id, master_node, &request_id);

	/* Build payload.  spec-2.32 wire ABI = 48B (HC73). */
	memset(&payload, 0, sizeof(payload));
	payload.request_id = request_id;
	payload.epoch = cluster_epoch_get_current();
	payload.tag = tag;
	payload.sender_node = cluster_node_id;
	payload.transition_id = (uint8)transition_id;

	pg_atomic_fetch_add_u64(&ClusterGcs->encode_payload_bytes, sizeof(payload));
	pg_atomic_fetch_add_u64(&ClusterGcs->send_request_count, 1);

	PG_TRY();
	{
		/*
		 * HC70:  must traverse cluster_ic_send_envelope (production path).
		 * In single-node loopback test the helper bypass replaces this; in
		 * production spec-2.32 master==self short-circuit ensures we never
		 * reach this function.
		 */
		if (gcs_send_envelope_or_loopback(PGRAC_IC_MSG_GCS_REQUEST, master_node, &payload,
										  sizeof(payload))
			!= CLUSTER_IC_SEND_DONE) {
			if (throw_on_send_fail)
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
								errmsg("cluster_gcs: failed to send GCS request to node %d",
									   master_node)));
			send_failed = true;
		}

		/*
		 * Wait for reply or internal safety deadline.  Public timeout GUC
		 * for GCS control messages + retransmit lands in spec-2.34+; current
		 * 5s deadline catches receiver bugs without exposing a user-visible
		 * knob yet.
		 */
		if (!send_failed) {
			deadline = GetCurrentTimestamp()
					   + ((TimestampTz)GCS_REPLY_INTERNAL_DEADLINE_MS) * (TimestampTz)1000;

			ConditionVariablePrepareToSleep(&slot->reply_cv);
			for (;;) {
				TimestampTz now;
				long timeout_ms;

				if (gcs_slot_get_reply(slot, NULL))
					break;

				now = GetCurrentTimestamp();
				if (now >= deadline) {
					pg_atomic_fetch_add_u64(&ClusterGcs->reply_timeout_count, 1);
					break;
				}
				timeout_ms = (long)((deadline - now) / 1000);
				if (timeout_ms <= 0)
					timeout_ms = 1;
				(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
												  WAIT_EVENT_GCS_REPLY_WAIT);
			}
			ConditionVariableCancelSleep();

			if (gcs_slot_get_reply(slot, &final_reply)) {
				final_status = final_reply.status;
				if (out_final_transition)
					*out_final_transition = final_reply.transition_id;
			} else {
				final_status = GCS_REPLY_DENIED_INCOMPATIBLE; /* internal timeout marker */
			}
		}
	}
	PG_CATCH();
	{
		gcs_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_release_slot(slot);

	return final_status;
}

/* PGRAC: spec-6.12a ㉕ — non-throwing registration attempt (see header). */
bool
cluster_gcs_try_send_transition_and_wait(BufferTag tag, PcmLockTransition transition_id,
										 int master_node)
{
	uint8 final_transition = (uint8)transition_id;

	return gcs_transition_and_wait_internal(tag, transition_id, master_node,
											/* throw_on_send_fail */ false, &final_transition)
		   == GCS_REPLY_GRANTED;
}

void
cluster_gcs_send_transition_and_wait(BufferTag tag, PcmLockTransition transition_id,
									 int master_node)
{
	uint8 final_status;
	uint8 final_transition = (uint8)transition_id;

	final_status
		= gcs_transition_and_wait_internal(tag, transition_id, master_node,
										   /* throw_on_send_fail */ true, &final_transition);

	if (final_status == GCS_REPLY_GRANTED) {
		/*
		 * HC77:  master-side handler already applied the transition; sender
		 * must not double-apply.  Caller (cluster_pcm_lock_acquire / etc.)
		 * returns success and lets the bufmgr success path update
		 * BufferDesc as it would for a local transition (HC76).
		 */
		return;
	}

	/* All non-GRANTED outcomes ereport so the bufmgr caller doesn't pollute
	 * BufferDesc with stale ownership (HC76). */

	switch ((GcsReplyStatus)final_status) {
	case GCS_REPLY_DENIED_VALIDATOR_REJECT:
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_gcs: master rejected transition_id=%d as illegal "
							   "(spec-2.30 validator)",
							   (int)final_transition)));
		break;
	case GCS_REPLY_DENIED_EPOCH_STALE:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_gcs: request rejected due to stale epoch"),
						errhint("Reconfig epoch invalidation handling lands in spec-2.34+.")));
		break;
	case GCS_REPLY_DENIED_INCOMPATIBLE:
	default:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster_gcs: transition_id=%d denied or timed out (status=%d)",
						(int)final_transition, (int)final_status),
				 errhint("Public GCS control timeout GUC + retransmit land in spec-2.34+.")));
		break;
	case GCS_REPLY_GRANTED:
		Assert(false); /* handled above */
		break;
	}
}


/*
 * PGRAC: spec-6.12a ㉕ — fire-and-forget transition notify (see header).
 *
 *	No slot, no wait: request_id 0 marks the reply as unmatchable so the
 *	sender-side reply handler HC74-drops it.  Callable from the LMON
 *	IC-dispatch context (which must never block on a reply CV it would have
 *	to deliver itself).  The caller flips its own local state ONLY when this
 *	returns true — a false return leaves the cluster in the exact pre-call
 *	state on both nodes.
 */
bool
cluster_gcs_send_transition_nowait(BufferTag tag, PcmLockTransition transition_id, int master_node)
{
	GcsRequestPayload payload;

	if (transition_id < PCM_TRANS_N_TO_S || transition_id > PCM_TRANS_S_TO_X_CLEANOUT)
		return false;
	if (master_node < 0 || master_node == cluster_node_id)
		return false;

	memset(&payload, 0, sizeof(payload));
	payload.request_id = 0; /* deliberate: no slot; reply HC74-drops */
	payload.epoch = cluster_epoch_get_current();
	payload.tag = tag;
	payload.sender_node = cluster_node_id;
	payload.transition_id = (uint8)transition_id;

	pg_atomic_fetch_add_u64(&ClusterGcs->encode_payload_bytes, sizeof(payload));
	pg_atomic_fetch_add_u64(&ClusterGcs->send_request_count, 1);

	/*
	 * PGRAC: spec-7.2 D3 — route through the loopback/ring-aware helper
	 * instead of a raw send:  post-flip this fire-and-forget notify is
	 * emitted from the LMS-context FORWARD/BAST paths, where GCS_REQUEST
	 * (CONTROL plane) must stage into the outbound ring for LMON (the
	 * D0-①b staging case).  LMON callers keep the direct send.
	 */
	return gcs_send_envelope_or_loopback(PGRAC_IC_MSG_GCS_REQUEST, master_node, &payload,
										 sizeof(payload))
		   == CLUSTER_IC_SEND_DONE;
}


/* ============================================================
 * Receiver handlers (D4) — HC70+HC71 real handlers, non-stub.
 * ============================================================ */

static void
gcs_send_reply(int32 dest_node, uint64 request_id, uint8 transition_id, GcsReplyStatus status)
{
	GcsReplyPayload reply;

	memset(&reply, 0, sizeof(reply));
	reply.request_id = request_id;
	reply.transition_id = transition_id;
	reply.status = (uint8)status;
	reply.sender_node = cluster_node_id;
	reply.epoch = cluster_epoch_get_current();

	pg_atomic_fetch_add_u64(&ClusterGcs->encode_payload_bytes, sizeof(reply));

	(void)gcs_send_envelope_or_loopback(PGRAC_IC_MSG_GCS_REPLY, dest_node, &reply, sizeof(reply));
}

void
cluster_gcs_handle_request_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsRequestPayload *req;
	uint64 current_epoch;

	(void)env;

	if (payload == NULL || env == NULL || env->payload_length != sizeof(GcsRequestPayload))
		return; /* malformed; dispatcher already counts */

	req = (const GcsRequestPayload *)payload;

	pg_atomic_fetch_add_u64(&ClusterGcs->handle_request_count, 1);
	pg_atomic_fetch_add_u64(&ClusterGcs->decode_payload_bytes, sizeof(*req));

	/* HC75:  transition_id range check + validator. */
	if (req->transition_id < PCM_TRANS_N_TO_S || req->transition_id > PCM_TRANS_S_TO_X_CLEANOUT) {
		gcs_send_reply(req->sender_node, req->request_id, req->transition_id,
					   GCS_REPLY_DENIED_VALIDATOR_REJECT);
		return;
	}

	/* HC73:  epoch freshness check. */
	current_epoch = cluster_epoch_get_current();
	if (req->epoch < current_epoch) {
		gcs_send_reply(req->sender_node, req->request_id, req->transition_id,
					   GCS_REPLY_DENIED_EPOCH_STALE);
		return;
	}

	/* HC77: master-side handler is the single transition-apply owner. */
	if (!cluster_pcm_lock_apply_gcs_transition(req->tag, (PcmLockTransition)req->transition_id,
											   req->sender_node)) {
		if (req->transition_id == PCM_TRANS_N_TO_X
			|| req->transition_id == PCM_TRANS_S_TO_X_UPGRADE)
			(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
		gcs_send_reply(req->sender_node, req->request_id, req->transition_id,
					   GCS_REPLY_DENIED_INCOMPATIBLE);
		return;
	}
	if (req->transition_id == PCM_TRANS_N_TO_X || req->transition_id == PCM_TRANS_S_TO_X_UPGRADE)
		(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);

	gcs_send_reply(req->sender_node, req->request_id, req->transition_id, GCS_REPLY_GRANTED);
}

void
cluster_gcs_handle_reply_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsReplyPayload *reply;

	(void)env;

	if (payload == NULL || env == NULL || env->payload_length != sizeof(GcsReplyPayload))
		return;

	reply = (const GcsReplyPayload *)payload;

	pg_atomic_fetch_add_u64(&ClusterGcs->handle_reply_count, 1);
	pg_atomic_fetch_add_u64(&ClusterGcs->decode_payload_bytes, sizeof(*reply));

	if (!gcs_mark_slot_reply(reply)) {
		/* HC74: unknown request_id = stale/late reply; local drop. */
		pg_atomic_fetch_add_u64(&ClusterGcs->reply_late_drop_count, 1);
		return;
	}
}


/* ============================================================
 * Dispatch table registration (D4).
 * ============================================================ */

static const ClusterICMsgTypeInfo gcs_request_info = {
	.msg_type = PGRAC_IC_MSG_GCS_REQUEST,
	.name = "gcs_request",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BACKEND | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_request_envelope,
};

static const ClusterICMsgTypeInfo gcs_reply_info = {
	.msg_type = PGRAC_IC_MSG_GCS_REPLY,
	.name = "gcs_reply",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BACKEND | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_reply_envelope,
};

void
cluster_gcs_register_msg_types(void)
{
	cluster_ic_register_msg_type(&gcs_request_info);
	cluster_ic_register_msg_type(&gcs_reply_info);
}


/* ============================================================
 * Test-only loopback helper.
 *
 *	Builds a ClusterICEnvelope around the supplied payload then invokes
 *	cluster_ic_dispatch_envelope directly.  This is necessary because
 *	cluster_ic_send_envelope short-circuits when dest_node_id ==
 *	cluster_node_id (cluster_ic_router.c:202 self-target no-op).  Used
 *	by test_cluster_gcs_dispatch to exercise the receiver pipeline.
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT
void
cluster_gcs_test_loopback_dispatch(uint8 msg_type, const void *payload, uint32 payload_len)
{
	(void)gcs_dispatch_loopback(msg_type, payload, payload_len);
}
#endif


/* ============================================================
 * Observability accessors (dump_gcs surface).
 * ============================================================ */

#define GCS_ACCESSOR_U64(name, field)                                                              \
	uint64 name(void)                                                                              \
	{                                                                                              \
		return ClusterGcs != NULL ? pg_atomic_read_u64(&ClusterGcs->field) : 0;                    \
	}

GCS_ACCESSOR_U64(cluster_gcs_get_lookup_master_self_count, lookup_master_self_count)
GCS_ACCESSOR_U64(cluster_gcs_get_lookup_master_remote_count, lookup_master_remote_count)
GCS_ACCESSOR_U64(cluster_gcs_get_send_request_count, send_request_count)
GCS_ACCESSOR_U64(cluster_gcs_get_handle_request_count, handle_request_count)
GCS_ACCESSOR_U64(cluster_gcs_get_handle_reply_count, handle_reply_count)
GCS_ACCESSOR_U64(cluster_gcs_get_reply_late_drop_count, reply_late_drop_count)
GCS_ACCESSOR_U64(cluster_gcs_get_reply_timeout_count, reply_timeout_count)
GCS_ACCESSOR_U64(cluster_gcs_get_encode_payload_bytes, encode_payload_bytes)
GCS_ACCESSOR_U64(cluster_gcs_get_decode_payload_bytes, decode_payload_bytes)
GCS_ACCESSOR_U64(cluster_gcs_get_dispatch_loop_iterations, dispatch_loop_iterations)

uint64
cluster_gcs_get_outstanding_count(void)
{
	return ClusterGcs != NULL ? pg_atomic_read_u32(&ClusterGcs->outstanding_count) : 0;
}

uint64
cluster_gcs_get_max_outstanding(void)
{
	return ClusterGcs != NULL ? pg_atomic_read_u32(&ClusterGcs->max_outstanding) : 0;
}

const char *
cluster_gcs_get_api_state(void)
{
	return ClusterGcs != NULL ? "active" : "stub";
}

#endif /* USE_PGRAC_CLUSTER */
