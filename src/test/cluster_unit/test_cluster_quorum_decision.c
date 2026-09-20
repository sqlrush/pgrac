/*-------------------------------------------------------------------------
 *
 * test_cluster_quorum_decision.c
 *	  spec-2.6 Sprint A Step 2 D4 unit tests — pure logic majority math
 *	  + collision detection (Q6 v0.2 newer-self-FATAL).
 *
 *	  decide_quorum_view() takes a synthetic slot matrix + per-disk
 *	  io_states and returns a ClusterQuorumDecision.  No I/O, no
 *	  shmem;easy to fuzz with synthetic inputs.
 *
 *	  T-d-1  3 disks all OK + same epoch + 2 alive nodes  → OK + bitmap
 *	  T-d-2  3 disks 2 OK + 1 FAILED                       → OK (still majority)
 *	  T-d-3  3 disks 1 OK + 2 FAILED                       → UNCERTAIN
 *	  T-d-4  3 disks 0 OK (all FAILED)                     → LOST
 *	  T-d-5  epoch_max picks largest current_epoch         → matches max
 *	  T-d-6  collision Q6: self.inc > slot.inc → FATAL_NEWER_SELF
 *	  T-d-7  collision Q6: self.inc < slot.inc → OBSERVED_OLDER
 *	  T-d-8  collision Q6: self.inc == slot.inc → NONE
 *	  T-d-9  generation == 0 (never written) skip both alive + collision
 *	  T-d-10 NULL inputs / 0 disks → LOST defensive
 *	  T-d-14 fresh non-ALIVE self slot skips collision (clean tombstone)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_quorum_decision.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_quorum_decision.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* Synthetic "now" timestamp for the test fixture (microseconds since
 * epoch — value is arbitrary; only the relation to slot.heartbeat_ts_us
 * matters for the freshness gate). */
#define TEST_NOW_US 1700000000000000ULL
#define TEST_HEARTBEAT_TIMEOUT_US 5000000ULL /* 5 seconds */

/*
 * Helper: populate a slot with sensible defaults for a particular
 * (disk_idx, node_id) pair.  heartbeat_ts_us defaults to TEST_NOW_US
 * (slot is FRESH).  Tests that need a stale slot call make_slot_stale
 * or override slot->heartbeat_ts_us directly after the helper.
 */
static void
make_slot(ClusterVotingSlot *s, uint32 disk_idx, uint32 node_id, uint64 incarnation,
		  uint64 generation, uint64 epoch, uint64 flags)
{
	memset(s, 0, sizeof(*s));
	s->magic = CLUSTER_VOTING_SLOT_MAGIC;
	s->version = CLUSTER_VOTING_SLOT_VERSION;
	s->node_id = node_id;
	s->incarnation = incarnation;
	s->disk_index = disk_idx;
	s->generation = generation;
	s->current_epoch = epoch;
	s->flags = flags;
	s->heartbeat_ts_us = TEST_NOW_US; /* fresh by default */
}

#define N_DISKS 3
#define N_NODES 4 /* small for tests; production CLUSTER_MAX_NODES = 128 */


UT_TEST(test_d_1_three_disks_all_ok_majority)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;
	uint32 d, n;

	memset(slots, 0, sizeof(slots));
	for (d = 0; d < N_DISKS; d++) {
		for (n = 0; n < N_NODES; n++) {
			/* Only nodes 0 and 1 alive. */
			if (n < 2)
				make_slot(&slots[d * N_NODES + n], d, n, 1000 + n, 5, 42,
						  CLUSTER_VOTING_SLOT_FLAG_ALIVE);
		}
	}

	UT_ASSERT_EQ(decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
									/*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US,
									TEST_HEARTBEAT_TIMEOUT_US, &out),
				 CLUSTER_QVOTEC_QUORUM_OK);
	UT_ASSERT_EQ(out.disks_ok_count, 3);
	UT_ASSERT_EQ(out.disks_total_count, 3);
	UT_ASSERT_EQ(out.epoch_max, 42);
	/* node 0 + 1 alive → bits 0 and 1 set. */
	UT_ASSERT_EQ(out.alive_bitmap[0], 0x03);
}


UT_TEST(test_d_2_two_of_three_disks_ok_still_majority)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_FAILED };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	make_slot(&slots[0 * N_NODES + 0], 0, 0, 1000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(&slots[1 * N_NODES + 0], 1, 0, 1000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	UT_ASSERT_EQ(decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
									/*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US,
									TEST_HEARTBEAT_TIMEOUT_US, &out),
				 CLUSTER_QVOTEC_QUORUM_OK);
	UT_ASSERT_EQ(out.disks_ok_count, 2);
}


UT_TEST(test_d_3_one_of_three_disks_ok_uncertain)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_FAILED, CLUSTER_VOTING_DISK_IO_TORN };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	make_slot(&slots[0 * N_NODES + 0], 0, 0, 1000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	UT_ASSERT_EQ(decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
									/*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US,
									TEST_HEARTBEAT_TIMEOUT_US, &out),
				 CLUSTER_QVOTEC_QUORUM_UNCERTAIN);
	UT_ASSERT_EQ(out.disks_ok_count, 1);
}


UT_TEST(test_d_4_zero_disks_ok_lost)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_FAILED, CLUSTER_VOTING_DISK_IO_FAILED,
			CLUSTER_VOTING_DISK_IO_TORN };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));

	UT_ASSERT_EQ(decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
									/*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US,
									TEST_HEARTBEAT_TIMEOUT_US, &out),
				 CLUSTER_QVOTEC_QUORUM_LOST);
	UT_ASSERT_EQ(out.disks_ok_count, 0);
}


UT_TEST(test_d_5_epoch_max_largest_observed)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Different epochs across disks/nodes — max is 99. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, 1000, 5, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(&slots[1 * N_NODES + 1], 1, 1, 1001, 5, 99, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(&slots[2 * N_NODES + 2], 2, 2, 1002, 5, 50, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	UT_ASSERT_EQ(out.epoch_max, 99);
}


UT_TEST(test_d_6_collision_q6_newer_self_fatal)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Other instance with node_id=0, lower incarnation = older serving. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, /*inc*/ 500, 5, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	/* self is newer comer (inc 1000 > slot inc 500). */
	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_FATAL_NEWER_SELF);
	UT_ASSERT_EQ(out.collision_other_node_id, 0);
	UT_ASSERT_EQ(out.collision_other_incarnation, 500);
}


UT_TEST(test_d_7_collision_q6_observed_older_when_self_smaller)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Other instance with HIGHER incarnation = the newer comer. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, /*inc*/ 2000, 5, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	/* self is older — Q6 v0.2 says we continue, observe older slot.
	 * (The other side will self-FATAL since it's the newer comer.) */
	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_OBSERVED_OLDER);
}


UT_TEST(test_d_8_collision_same_incarnation_no_collision)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Slot for self.node_id=0 with same incarnation — this is our own
	 * slot from the previous cycle, not a collision. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, /*inc*/ 1000, 5, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_NONE);
}


UT_TEST(test_d_9_generation_zero_skips_alive_and_collision)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* generation = 0 — fresh format, never written by qvotec.  Even
	 * if magic / version / node_id are valid, the slot must be
	 * skipped (no collision check, no alive bit). */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, /*inc*/ 500,
			  /*generation*/ 0, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	/* No alive bit set despite the flag, no collision detected. */
	UT_ASSERT_EQ(out.alive_bitmap[0], 0x00);
	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_NONE);
}


UT_TEST(test_d_14_non_alive_self_slot_skips_collision)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/*
	 * Clean shutdown writes a fresh tombstone with flags=0 before qvotec
	 * closes voting disks.  A fast restart must not treat that prior
	 * incarnation as a live same-node peer collision.
	 */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, /*inc*/ 500,
			  /*generation*/ 5, 99, /*flags*/ 0);

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_NONE);
	UT_ASSERT_EQ(out.alive_bitmap[0], 0x00);
	/* Tombstones still carry epoch input for future boot-epoch recovery. */
	UT_ASSERT_EQ(out.epoch_max, 99);
}


UT_TEST(test_d_11_stale_alive_slot_excluded_from_bitmap)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Node 0 is fresh + ALIVE. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, 1000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(&slots[1 * N_NODES + 0], 1, 0, 1000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(&slots[2 * N_NODES + 0], 2, 0, 1000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	/* Node 1 is STALE (heartbeat 60s old, timeout is 5s) — crashed peer
	 * left ALIVE flag behind.  P2.1 freshness gate must EXCLUDE it. */
	make_slot(&slots[0 * N_NODES + 1], 0, 1, 2000, 5, 42, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	slots[0 * N_NODES + 1].heartbeat_ts_us = TEST_NOW_US - 60000000ULL;

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	/* Node 0 alive ✓, node 1 NOT alive (stale) → bit 0 only. */
	UT_ASSERT_EQ(out.alive_bitmap[0], 0x01);
}


UT_TEST(test_d_12_stale_collision_ignored)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Stale slot with our node_id and DIFFERENT incarnation — would be a
	 * Q6 collision if fresh, but it is stale (the other instance died).
	 * P2.1 freshness gate must drop it from collision detection. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, /*inc*/ 500, 5, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	slots[0 * N_NODES + 0].heartbeat_ts_us = TEST_NOW_US - 60000000ULL;

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_NONE);
}


UT_TEST(test_d_13_epoch_recovery_includes_stale_slots)
{
	ClusterVotingSlot slots[N_DISKS * N_NODES];
	ClusterVotingDiskIoState io_states[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision out;

	memset(slots, 0, sizeof(slots));
	/* Fresh slot at epoch 10. */
	make_slot(&slots[0 * N_NODES + 0], 0, 0, 1000, 5, 10, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	/* Stale slot at HIGHER epoch 99 (a crashed peer that ran longer). */
	make_slot(&slots[1 * N_NODES + 1], 1, 1, 2000, 5, 99, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	slots[1 * N_NODES + 1].heartbeat_ts_us = TEST_NOW_US - 60000000ULL;

	decide_quorum_view(slots, io_states, N_DISKS, N_NODES,
					   /*self*/ 0, /*self_inc*/ 1000, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US, &out);

	/* Boot-time epoch recovery must see the stale 99 — "boot epoch =
	 * max(observed) + 1" includes crashed peers' last epochs.  Only
	 * alive_bitmap + collision are gated by freshness. */
	UT_ASSERT_EQ(out.epoch_max, 99);
	/* But alive_bitmap must NOT count the stale slot. */
	UT_ASSERT_EQ(out.alive_bitmap[0], 0x01); /* only node 0 fresh */
}


UT_TEST(test_d_10_null_inputs_lost_defensive)
{
	ClusterQuorumDecision out;

	/* NULL slots → LOST. */
	UT_ASSERT_EQ(decide_quorum_view(NULL, NULL, 3, N_NODES, 0, 1000, TEST_NOW_US,
									TEST_HEARTBEAT_TIMEOUT_US, &out),
				 CLUSTER_QVOTEC_QUORUM_LOST);

	/* 0 disks → LOST. */
	{
		ClusterVotingSlot dummy_slot = { 0 };
		ClusterVotingDiskIoState dummy_io = CLUSTER_VOTING_DISK_IO_OK;

		UT_ASSERT_EQ(decide_quorum_view(&dummy_slot, &dummy_io, 0, N_NODES, 0, 1000, TEST_NOW_US,
										TEST_HEARTBEAT_TIMEOUT_US, &out),
					 CLUSTER_QVOTEC_QUORUM_LOST);
	}
}


/* Capture the real QVOTEC publication block, extracted at build time. The
 * quorum decision remains the production function linked above. Only the
 * shared-memory publication boundary is replaced by a checked receiver. */
static struct {
	uint64 incarnation[CLUSTER_MAX_NODES];
	uint64 generation[CLUSTER_MAX_NODES];
	bool fresh[CLUSTER_MAX_NODES];
	bool publishing;
	bool in_quorum;
	int begin_count;
	int end_count;
} bootstrap_capture;

static void
capture_bootstrap_begin(void)
{
	UT_ASSERT(!bootstrap_capture.publishing);
	bootstrap_capture.publishing = true;
	bootstrap_capture.begin_count++;
}

static void
capture_bootstrap_end(void)
{
	UT_ASSERT(bootstrap_capture.publishing);
	bootstrap_capture.publishing = false;
	bootstrap_capture.end_count++;
}

static void
capture_bootstrap_slot(int32 node, uint64 incarnation, uint64 generation,
					   uint64 epoch pg_attribute_unused())
{
	UT_ASSERT(bootstrap_capture.publishing);
	UT_ASSERT(node >= 0 && node < CLUSTER_MAX_NODES);
	bootstrap_capture.incarnation[node] = incarnation;
	bootstrap_capture.generation[node] = generation;
}

static void
capture_bootstrap_fresh(int32 node, bool fresh)
{
	UT_ASSERT(bootstrap_capture.publishing);
	UT_ASSERT(node >= 0 && node < CLUSTER_MAX_NODES);
	bootstrap_capture.fresh[node] = fresh;
}

static void
capture_bootstrap_quorum(bool in_quorum)
{
	UT_ASSERT(bootstrap_capture.publishing);
	bootstrap_capture.in_quorum = in_quorum;
}

static void
publish_bootstrap_fixture(ClusterVotingSlot *qvotec_slot_matrix,
						  const ClusterVotingDiskIoState *io_states, uint64 now_us,
						  uint64 heartbeat_timeout_us)
{
	const int qvotec_n_disks = N_DISKS;
	ClusterQuorumDecision decision;
	uint32 node;
	int i;

	memset(&bootstrap_capture, 0, sizeof(bootstrap_capture));
	(void)decide_quorum_view(qvotec_slot_matrix, io_states, N_DISKS, CLUSTER_MAX_NODES, 0, 1000,
							 now_us, heartbeat_timeout_us, &decision);
#define cluster_reconfig_bootstrap_publish_begin capture_bootstrap_begin
#define cluster_reconfig_bootstrap_publish_end capture_bootstrap_end
#define cluster_reconfig_record_observed_slot capture_bootstrap_slot
#define cluster_reconfig_record_observed_fresh_alive capture_bootstrap_fresh
#define cluster_reconfig_bootstrap_publish_in_quorum capture_bootstrap_quorum
#include "test_cluster_qvotec_bootstrap_publish.inc"
#undef cluster_reconfig_bootstrap_publish_begin
#undef cluster_reconfig_bootstrap_publish_end
#undef cluster_reconfig_record_observed_slot
#undef cluster_reconfig_record_observed_fresh_alive
#undef cluster_reconfig_bootstrap_publish_in_quorum
	UT_ASSERT_EQ(bootstrap_capture.begin_count, 1);
	UT_ASSERT_EQ(bootstrap_capture.end_count, 1);
	UT_ASSERT(!bootstrap_capture.publishing);
}

UT_TEST(test_bootstrap_cannot_pair_clean_old_identity_with_new_alive_bit)
{
	ClusterVotingSlot slots[N_DISKS * CLUSTER_MAX_NODES] = { 0 };
	ClusterVotingDiskIoState io[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterQuorumDecision decision;
	int disk;

	/* An ordinary clean restart resets per-process generation. A read can
	 * cross the new process's per-disk writes: one new ALIVE slot, two old
	 * clean tombstones with larger generations. Node-level alive is true,
	 * but that is not evidence that the selected OLD identity is alive. */
	make_slot(&slots[1], 0, 1, 2000, 1, 0, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	for (disk = 1; disk < N_DISKS; disk++)
		make_slot(&slots[disk * CLUSTER_MAX_NODES + 1], disk, 1, 1000, 900 + disk, 0, 0);
	(void)decide_quorum_view(slots, io, N_DISKS, CLUSTER_MAX_NODES, 0, 1000, TEST_NOW_US,
							 TEST_HEARTBEAT_TIMEOUT_US, &decision);
	UT_ASSERT((decision.alive_bitmap[0] & 2) != 0);
	publish_bootstrap_fixture(slots, io, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US);
	UT_ASSERT(bootstrap_capture.in_quorum);
	UT_ASSERT_EQ(bootstrap_capture.incarnation[1], UINT64_C(1000));
	UT_ASSERT_EQ(bootstrap_capture.generation[1], UINT64_C(902));
	UT_ASSERT(!bootstrap_capture.fresh[1]);

	/* The next ordinary poll converges after all disks carry the new writer.
	 * No consumer-side identity rewrite, extra protocol or timeout needed. */
	for (disk = 0; disk < N_DISKS; disk++)
		make_slot(&slots[disk * CLUSTER_MAX_NODES + 1], disk, 1, 2000, 4 + disk, 0,
				  CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	publish_bootstrap_fixture(slots, io, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US);
	UT_ASSERT_EQ(bootstrap_capture.incarnation[1], UINT64_C(2000));
	UT_ASSERT(bootstrap_capture.fresh[1]);
}

UT_TEST(test_bootstrap_selected_disk_must_be_trusted)
{
	ClusterVotingSlot slots[N_DISKS * CLUSTER_MAX_NODES] = { 0 };
	ClusterVotingDiskIoState io[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_FAILED };

	make_slot(&slots[1], 0, 1, 2000, 1, 0, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(&slots[2 * CLUSTER_MAX_NODES + 1], 2, 1, 1000, 900, 0,
			  CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	publish_bootstrap_fixture(slots, io, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US);
	UT_ASSERT(bootstrap_capture.in_quorum);
	UT_ASSERT_EQ(bootstrap_capture.incarnation[1], UINT64_C(1000));
	UT_ASSERT(!bootstrap_capture.fresh[1]);
}

UT_TEST(test_bootstrap_selected_heartbeat_boundary_and_missing_identity)
{
	ClusterVotingSlot slots[N_DISKS * CLUSTER_MAX_NODES] = { 0 };
	ClusterVotingDiskIoState io[N_DISKS]
		= { CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK, CLUSTER_VOTING_DISK_IO_OK };
	ClusterVotingSlot *selected = &slots[2 * CLUSTER_MAX_NODES + 1];
	const uint64 timestamps[] = { 0, TEST_NOW_US - TEST_HEARTBEAT_TIMEOUT_US - 1,
								  TEST_NOW_US - TEST_HEARTBEAT_TIMEOUT_US, TEST_NOW_US + 1, 0 };
	const bool expected[] = { false, false, true, true, true };
	int scenario;

	make_slot(&slots[1], 0, 1, 2000, 1, 0, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	make_slot(selected, 2, 1, 1000, 900, 0, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	for (scenario = 0; scenario < lengthof(timestamps); scenario++) {
		selected->heartbeat_ts_us = timestamps[scenario];
		publish_bootstrap_fixture(slots, io, TEST_NOW_US,
								  scenario == 4 ? 0 : TEST_HEARTBEAT_TIMEOUT_US);
		UT_ASSERT_EQ(bootstrap_capture.fresh[1], expected[scenario]);
	}
	memset(slots, 0, sizeof(slots));
	publish_bootstrap_fixture(slots, io, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US);
	UT_ASSERT_EQ(bootstrap_capture.incarnation[1], UINT64_C(0));
	UT_ASSERT(!bootstrap_capture.fresh[1]);
	make_slot(selected, 2, 2, 1000, 900, 0, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	publish_bootstrap_fixture(slots, io, TEST_NOW_US, TEST_HEARTBEAT_TIMEOUT_US);
	UT_ASSERT_EQ(bootstrap_capture.incarnation[1], UINT64_C(0));
	UT_ASSERT(!bootstrap_capture.fresh[1]);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_d_1_three_disks_all_ok_majority);
	UT_RUN(test_d_2_two_of_three_disks_ok_still_majority);
	UT_RUN(test_d_3_one_of_three_disks_ok_uncertain);
	UT_RUN(test_d_4_zero_disks_ok_lost);
	UT_RUN(test_d_5_epoch_max_largest_observed);
	UT_RUN(test_d_6_collision_q6_newer_self_fatal);
	UT_RUN(test_d_7_collision_q6_observed_older_when_self_smaller);
	UT_RUN(test_d_8_collision_same_incarnation_no_collision);
	UT_RUN(test_d_9_generation_zero_skips_alive_and_collision);
	UT_RUN(test_d_14_non_alive_self_slot_skips_collision);
	UT_RUN(test_d_11_stale_alive_slot_excluded_from_bitmap);
	UT_RUN(test_d_12_stale_collision_ignored);
	UT_RUN(test_d_13_epoch_recovery_includes_stale_slots);
	UT_RUN(test_d_10_null_inputs_lost_defensive);
	UT_RUN(test_bootstrap_cannot_pair_clean_old_identity_with_new_alive_bit);
	UT_RUN(test_bootstrap_selected_disk_must_be_trusted);
	UT_RUN(test_bootstrap_selected_heartbeat_boundary_and_missing_identity);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
