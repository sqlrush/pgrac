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
} UtSentRec;

static UtSentRec ut_sent_log[64];
static int ut_sent_n = 0;
static ClusterICSendResult ut_peer_rc[CLUSTER_MAX_NODES];

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	if (ut_sent_n < (int)lengthof(ut_sent_log)) {
		ut_sent_log[ut_sent_n].msg_type = msg_type;
		ut_sent_log[ut_sent_n].dest = dest_node_id;
		ut_sent_log[ut_sent_n].marker = payload_len > 0 ? *(const uint8 *)payload : 0;
	}
	ut_sent_n++;
	UT_ASSERT(dest_node_id >= 0 && dest_node_id < CLUSTER_MAX_NODES);
	return ut_peer_rc[dest_node_id];
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
	memset(ut_sent_log, 0, sizeof(ut_sent_log));
}

static bool
ut_enqueue_marker(int worker_id, int32 dest, uint8 marker)
{
	return cluster_lms_outbound_enqueue(worker_id, UT_MSG_TYPE, (uint32)dest, &marker, 1);
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

int
main(void)
{
	UT_PLAN(5);

	UT_RUN(test_ring_shmem_init);
	UT_RUN(test_admitted_frame_is_never_resubmitted);
	UT_RUN(test_blocked_peer_does_not_starve_other_peer);
	UT_RUN(test_refused_frame_retained_and_delivered);
	UT_RUN(test_blocked_peer_batch_keeps_per_peer_order);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
