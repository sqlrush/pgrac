/*-------------------------------------------------------------------------
 *
 * test_cluster_lms_outbound.c
 *	  Reliable-handoff contract of the DATA-plane outbound ring drain
 *	  (GCS serve-stall round-5).
 *
 *	  cluster_lms_outbound_drain_send() dequeues staged frames and hands
 *	  them to cluster_ic_send_envelope.  The send result is a four-state
 *	  ownership contract, and the drain must honor it exactly:
 *
 *	    DONE         frame is on the wire            -> ring slot consumed
 *	    WOULD_BLOCK  transport ADMITTED the frame
 *	                 (owns a private copy; drains on
 *	                 WL_SOCKET_WRITEABLE)            -> ring slot consumed;
 *	                                                    NEVER resubmit
 *	    NOT_ADMITTED transport refused the frame
 *	                 (peer mid-HELLO / FIFO full)    -> retain in ring, in
 *	                                                    per-peer order
 *	    HARD_ERROR   peer dead                       -> drop; requesters
 *	                                                    self-heal by retry
 *
 *	  Pre-fix drain treated WOULD_BLOCK as "not sent": it head-requeued
 *	  the whole frame and broke the batch.  Both halves were defects:
 *
 *	    U1  the requeued frame was ALSO admitted by tier1 (partial write
 *	        or tail queue), so the next drain put a duplicate frame on
 *	        the per-peer byte stream;
 *	    U2  the batch break parked every frame behind the blocked peer,
 *	        so one backpressured peer head-of-line blocked all others
 *	        sharing the worker ring.
 *
 *	  This binary links cluster_lms_outbound.o standalone and mocks
 *	  cluster_ic_send_envelope with a scripted per-peer result + a call
 *	  log, so the ownership contract is pinned deterministically.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lms_outbound.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope prototype */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_write_fence.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ============================================================
 * PG-runtime stubs.
 * ============================================================ */

ProcessingMode Mode = NormalProcessing;
BackendType MyBackendType = B_LMS;
int cluster_node_id = 0;
static PcmXRuntimeState ut_pcm_x_runtime_state = PCM_X_RUNTIME_ACTIVE;
static bool ut_write_fence_enforcing = false;
static bool ut_write_fence_allowed = true;

PcmXRuntimeSnapshot
cluster_pcm_x_runtime_snapshot(void)
{
	PcmXRuntimeSnapshot snapshot = { 0 };

	snapshot.state = ut_pcm_x_runtime_state;
	if (snapshot.state == PCM_X_RUNTIME_ACTIVE) {
		snapshot.master_session_incarnation = 1;
		snapshot.gate_generation = 1;
	}
	return snapshot;
}

bool
cluster_write_fence_enforcing(void)
{
	return ut_write_fence_enforcing;
}

bool
cluster_write_fence_allowed(void)
{
	return ut_write_fence_allowed;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	void *p = malloc(size);

	(void)name;
	UT_ASSERT(p != NULL);
	memset(p, 0, size);
	*foundPtr = false;
	return p;
}

static const ClusterShmemRegion *ut_captured_region = NULL;

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	ut_captured_region = region;
}

/* Named-tranche plumbing: hand back a static lock array. */
static LWLockPadded ut_locks[CLUSTER_LMS_MAX_WORKERS];

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
{
	(void)tranche_name;
	return ut_locks;
}

void
RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
{
	(void)tranche_name;
	(void)num_lwlocks;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	(void)lock;
	(void)mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	(void)lock;
}

/* LMS wakeup + GCS pre-send hook: count-only stubs. */
static int ut_wakeup_count = 0;

void
cluster_lms_wakeup(int worker_id)
{
	(void)worker_id;
	ut_wakeup_count++;
}

/* Drain honesty counters (shmem-backed in production): count-only stubs. */
static int ut_not_admitted_count = 0;
static int ut_requeue_drop_count = 0;

void
cluster_lms_obs_note_outbound_not_admitted(int worker_id)
{
	(void)worker_id;
	ut_not_admitted_count++;
}

void
cluster_lms_obs_note_outbound_requeue_drop(int worker_id)
{
	(void)worker_id;
	ut_requeue_drop_count++;
}

void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult rc)
{
	(void)family;
	(void)rc;
}

static int ut_prepare_hook_count = 0;

void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req, int32 dest_node)
{
	(void)req;
	(void)dest_node;
	ut_prepare_hook_count++;
}

/* ============================================================
 * cluster_ic_send_envelope mock: scripted per-peer result + call log.
 * ============================================================ */

#define UT_PEER_X 3
#define UT_PEER_Y 5
#define UT_MSG_TYPE 42

typedef struct UtSentRec {
	uint8 msg_type;
	int32 dest;
	uint8 marker; /* first payload byte identifies the frame */
	uint32 payload_len;
	GcsBlockReplyHeader reply_header;
} UtSentRec;

static UtSentRec ut_sent_log[64];
static int ut_sent_n = 0;
static ClusterICSendResult ut_peer_rc[CLUSTER_MAX_NODES];
static int ut_local_dispatch_count = 0;
static uint8 ut_local_dispatch_marker = 0;
static int ut_direct_zero_reply_count = 0;
static GcsBlockReplyHeader ut_direct_zero_reply_header;

bool
cluster_ic_envelope_build(ClusterICEnvelope *out_env, uint8 msg_type, uint32 source_node_id,
						  uint32 dest_node_id, const void *payload, uint32 payload_length)
{
	memset(out_env, 0, sizeof(*out_env));
	out_env->msg_type = msg_type;
	out_env->source_node_id = source_node_id;
	out_env->dest_node_id = dest_node_id;
	out_env->payload_length = payload_length;
	(void)payload;
	return true;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer_id)
{
	UT_ASSERT(env != NULL);
	UT_ASSERT_EQ((int32)env->source_node_id, cluster_node_id);
	UT_ASSERT_EQ((int32)env->dest_node_id, cluster_node_id);
	UT_ASSERT_EQ(peer_id, cluster_node_id);
	ut_local_dispatch_count++;
	ut_local_dispatch_marker = env->payload_length > 0 ? *(const uint8 *)payload : 0;
	return true;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	if (ut_sent_n < (int)lengthof(ut_sent_log)) {
		ut_sent_log[ut_sent_n].msg_type = msg_type;
		ut_sent_log[ut_sent_n].dest = dest_node_id;
		ut_sent_log[ut_sent_n].marker = payload_len > 0 ? *(const uint8 *)payload : 0;
		ut_sent_log[ut_sent_n].payload_len = payload_len;
		if (msg_type == PGRAC_IC_MSG_GCS_BLOCK_REPLY
			&& payload_len >= sizeof(GcsBlockReplyHeader))
			memcpy(&ut_sent_log[ut_sent_n].reply_header, payload, sizeof(GcsBlockReplyHeader));
	}
	ut_sent_n++;
	UT_ASSERT(dest_node_id >= 0 && dest_node_id < CLUSTER_MAX_NODES);
	return ut_peer_rc[dest_node_id];
}

uint32
cluster_gcs_block_compute_checksum(const char *block_data)
{
	(void)block_data;
	return UINT32_C(0xA55A7E11);
}

ClusterICSendResult
cluster_gcs_block_send_direct_zero_reply(int32 dest_node, const GcsBlockReplyHeader *header)
{
	ut_direct_zero_reply_count++;
	ut_direct_zero_reply_header = *header;
	return ut_peer_rc[dest_node];
}

static int
ut_count_marker(uint8 marker)
{
	int n = 0;
	int i;

	for (i = 0; i < ut_sent_n && i < (int)lengthof(ut_sent_log); i++)
		if (ut_sent_log[i].marker == marker)
			n++;
	return n;
}

static void
ut_reset_log(void)
{
	ut_sent_n = 0;
	ut_local_dispatch_count = 0;
	ut_local_dispatch_marker = 0;
	ut_direct_zero_reply_count = 0;
	memset(&ut_direct_zero_reply_header, 0, sizeof(ut_direct_zero_reply_header));
	ut_pcm_x_runtime_state = PCM_X_RUNTIME_ACTIVE;
	ut_write_fence_enforcing = false;
	ut_write_fence_allowed = true;
	memset(ut_sent_log, 0, sizeof(ut_sent_log));
}

static bool
ut_enqueue_typed_marker(int worker_id, uint8 msg_type, int32 dest, uint8 marker)
{
	return cluster_lms_outbound_enqueue(worker_id, msg_type, (uint32)dest, &marker, 1);
}

static bool
ut_enqueue_marker(int worker_id, int32 dest, uint8 marker)
{
	return ut_enqueue_typed_marker(worker_id, UT_MSG_TYPE, dest, marker);
}

/* ============================================================
 * Tests.
 * ============================================================ */

/* U0: ring shmem up through the production region hooks. */
UT_TEST(test_ring_shmem_init)
{
	int i;

	cluster_lms_outbound_shmem_register();
	UT_ASSERT(ut_captured_region != NULL);
	ut_captured_region->init_fn();

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		ut_peer_rc[i] = CLUSTER_IC_SEND_DONE;

	UT_ASSERT(ut_enqueue_marker(0, UT_PEER_X, 0x01));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(0), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(0), 0);
	UT_ASSERT_EQ(ut_count_marker(0x01), 1);
}

/*
 * U1 (RED): a frame the transport ADMITTED (WOULD_BLOCK) must never be
 * resubmitted.  Pre-fix drain head-requeued it, and the next drain sent
 * a second copy onto the per-peer stream.
 */
UT_TEST(test_admitted_frame_is_never_resubmitted)
{
	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(1, UT_PEER_X, 0xA1));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_WOULD_BLOCK;
	(void)cluster_lms_outbound_drain_send(1);

	/* The transport owns the frame now; a later drain must not resend. */
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(1);

	UT_ASSERT_EQ(ut_count_marker(0xA1), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(1), 0);
}

/*
 * U2 (RED): a backpressured peer must not head-of-line block other peers
 * sharing the worker ring.  Pre-fix drain broke the batch on the first
 * WOULD_BLOCK, so Y's frame sat parked behind X's.
 */
UT_TEST(test_blocked_peer_does_not_starve_other_peer)
{
	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(2, UT_PEER_X, 0xB1));
	UT_ASSERT(ut_enqueue_marker(2, UT_PEER_Y, 0xB2));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_WOULD_BLOCK; /* admitted */
	ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(2);

	UT_ASSERT_EQ(ut_count_marker(0xB1), 1);
	UT_ASSERT_EQ(ut_count_marker(0xB2), 1); /* Y sent in the SAME batch */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
}

/*
 * U3: a REFUSED frame (NOT_ADMITTED) is retained — never dropped — and a
 * later drain delivers it once the transport admits it.
 */
UT_TEST(test_refused_frame_retained_and_delivered)
{
	int refused0 = ut_not_admitted_count;

	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(3, UT_PEER_X, 0xC1));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	(void)cluster_lms_outbound_drain_send(3);
	UT_ASSERT_EQ(ut_count_marker(0xC1), 1);			   /* attempted once */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(3), 1);	   /* retained */
	UT_ASSERT_EQ(ut_not_admitted_count - refused0, 1); /* counted */

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(3);
	UT_ASSERT_EQ(ut_count_marker(0xC1), 2); /* re-attempted exactly once */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(3), 0);
}

/*
 * U4: after a peer refuses a frame, its LATER frames in the same batch
 * must not be attempted (they queue in order BEHIND the refused one) while
 * other peers keep flowing;  the next drain then delivers the retained
 * frames in original submission order.
 */
UT_TEST(test_blocked_peer_batch_keeps_per_peer_order)
{
	int i;
	int d1_idx = -1;
	int d2_idx = -1;

	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(4, UT_PEER_X, 0xD1));
	UT_ASSERT(ut_enqueue_marker(4, UT_PEER_X, 0xD2));
	UT_ASSERT(ut_enqueue_marker(4, UT_PEER_Y, 0xD3));

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(4);

	UT_ASSERT_EQ(ut_count_marker(0xD1), 1); /* attempted + refused */
	UT_ASSERT_EQ(ut_count_marker(0xD2), 0); /* never attempted past D1 */
	UT_ASSERT_EQ(ut_count_marker(0xD3), 1); /* Y flowed in the same batch */
	UT_ASSERT_EQ(cluster_lms_outbound_depth(4), 2);

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	(void)cluster_lms_outbound_drain_send(4);
	UT_ASSERT_EQ(ut_count_marker(0xD1), 2);
	UT_ASSERT_EQ(ut_count_marker(0xD2), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(4), 0);

	/* Submission order preserved: D1's redelivery precedes D2's. */
	for (i = 0; i < ut_sent_n && i < (int)lengthof(ut_sent_log); i++) {
		if (ut_sent_log[i].marker == 0xD2 && d2_idx < 0)
			d2_idx = i;
		if (ut_sent_log[i].marker == 0xD1)
			d1_idx = i; /* last D1 attempt (the delivery) */
	}
	UT_ASSERT(d1_idx >= 0 && d2_idx >= 0);
	UT_ASSERT(d1_idx < d2_idx);
}

/*
 * PCM-X can hash a tag to the local node's master.  A DATA frame staged by a
 * backend must still execute on that tag's LMS worker: the generic IC send
 * self-shortcut reports DONE without dispatching, which would otherwise turn
 * ENQUEUE/ACK into a silent no-op.  The worker therefore loopback-dispatches
 * self frames and never hands them to the transport.
 */
UT_TEST(test_self_frame_dispatches_on_owning_worker)
{
	ut_reset_log();

	UT_ASSERT(ut_enqueue_marker(5, cluster_node_id, 0xE1));
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(5), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(5), 0);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_local_dispatch_count, 1);
	UT_ASSERT_EQ(ut_local_dispatch_marker, 0xE1);
}


/* A fail-closed runtime must retain a grant leg before transport admission.
 * Later frames for the same peer stay behind it, while unrelated peers keep
 * flowing.  Core has no recovery proof that could make these old-incarnation
 * frames runnable, so repeated drains must keep them parked. */
UT_TEST(test_pcm_x_grant_frame_waits_for_active_runtime)
{
	ut_reset_log();
	ut_pcm_x_runtime_state = PCM_X_RUNTIME_RECOVERY_BLOCKED;

	UT_ASSERT(ut_enqueue_typed_marker(6, PGRAC_IC_MSG_PCM_X_PREPARE_GRANT, UT_PEER_X, 0xF1));
	UT_ASSERT(ut_enqueue_marker(6, UT_PEER_X, 0xF2));
	UT_ASSERT(ut_enqueue_marker(6, UT_PEER_Y, 0xF3));
	(void)cluster_lms_outbound_drain_send(6);

	UT_ASSERT_EQ(ut_count_marker(0xF1), 0);
	UT_ASSERT_EQ(ut_count_marker(0xF2), 0);
	UT_ASSERT_EQ(ut_count_marker(0xF3), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(6), 2);

	(void)cluster_lms_outbound_drain_send(6);
	UT_ASSERT_EQ(ut_count_marker(0xF1), 0);
	UT_ASSERT_EQ(ut_count_marker(0xF2), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(6), 2);
}


UT_TEST(test_pcm_x_grant_frame_waits_behind_write_fence)
{
	ut_reset_log();
	ut_write_fence_enforcing = true;
	ut_write_fence_allowed = false;

	UT_ASSERT(ut_enqueue_typed_marker(7, PGRAC_IC_MSG_PCM_X_COMMIT_X, UT_PEER_X, 0xF4));
	UT_ASSERT(ut_enqueue_marker(7, UT_PEER_Y, 0xF5));
	(void)cluster_lms_outbound_drain_send(7);

	UT_ASSERT_EQ(ut_count_marker(0xF4), 0);
	UT_ASSERT_EQ(ut_count_marker(0xF5), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(7), 1);

	ut_write_fence_allowed = true;
	(void)cluster_lms_outbound_drain_send(7);
	UT_ASSERT_EQ(ut_count_marker(0xF4), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(7), 0);
}

/*
 * Shape-B denial replay is driven by LMON, which owns only plane 0.  The
 * reply is an ABI-sized header + zero block, so LMON stages its compact
 * header on the tag's DATA ring and the owning LMS worker expands and sends
 * it.  A direct LMON send is a production FATAL under the plane guard.
 */
UT_TEST(test_zero_block_reply_is_expanded_by_data_owner)
{
	GcsBlockReplyHeader hdr;

	ut_reset_log();
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = UINT64_C(0x1122334455667788);
	hdr.epoch = UINT64_C(41);
	hdr.sender_node = 1;
	hdr.requester_backend_id = 17;
	hdr.transition_id = PCM_TRANS_N_TO_X;
	hdr.status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply(2, UT_PEER_X, &hdr, false));
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 1);
	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 0);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 1);
	UT_ASSERT_EQ(ut_sent_n, 1);
	UT_ASSERT_EQ((int)ut_sent_log[0].payload_len, (int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);

	ut_peer_rc[UT_PEER_X] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(2), 1);
	UT_ASSERT_EQ(cluster_lms_outbound_depth(2), 0);
	UT_ASSERT_EQ(ut_sent_n, 2);
	UT_ASSERT_EQ((int)ut_sent_log[1].msg_type, (int)PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ((int)ut_sent_log[1].payload_len, (int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	UT_ASSERT_EQ(ut_sent_log[1].reply_header.request_id, hdr.request_id);
	UT_ASSERT_EQ((int)ut_sent_log[1].reply_header.status,
				 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ(ut_sent_log[1].reply_header.checksum, UINT32_C(0xA55A7E11));
}

UT_TEST(test_direct_zero_block_reply_uses_data_owner_direct_lane)
{
	GcsBlockReplyHeader hdr;

	ut_reset_log();
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = UINT64_C(0x8877665544332211);
	hdr.status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;

	UT_ASSERT(cluster_lms_outbound_enqueue_zero_block_reply(3, UT_PEER_Y, &hdr, true));
	ut_peer_rc[UT_PEER_Y] = CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(cluster_lms_outbound_drain_send(3), 1);
	UT_ASSERT_EQ(ut_direct_zero_reply_count, 1);
	UT_ASSERT_EQ(ut_sent_n, 0);
	UT_ASSERT_EQ(ut_direct_zero_reply_header.request_id, hdr.request_id);
	UT_ASSERT_EQ(ut_direct_zero_reply_header.checksum, UINT32_C(0xA55A7E11));
}

int
main(void)
{
	UT_PLAN(10);

	UT_RUN(test_ring_shmem_init);
	UT_RUN(test_admitted_frame_is_never_resubmitted);
	UT_RUN(test_blocked_peer_does_not_starve_other_peer);
	UT_RUN(test_refused_frame_retained_and_delivered);
	UT_RUN(test_blocked_peer_batch_keeps_per_peer_order);
	UT_RUN(test_self_frame_dispatches_on_owning_worker);
	UT_RUN(test_pcm_x_grant_frame_waits_for_active_runtime);
	UT_RUN(test_pcm_x_grant_frame_waits_behind_write_fence);
	UT_RUN(test_zero_block_reply_is_expanded_by_data_owner);
	UT_RUN(test_direct_zero_block_reply_uses_data_owner_direct_lane);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
