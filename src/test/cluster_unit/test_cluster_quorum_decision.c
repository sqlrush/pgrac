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


/*
 * Helper: populate a slot with sensible defaults for a particular
 * (disk_idx, node_id) pair.  Tests override fields they care about.
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
									/*self*/ 0, /*self_inc*/ 1000, &out),
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
									/*self*/ 0, /*self_inc*/ 1000, &out),
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
									/*self*/ 0, /*self_inc*/ 1000, &out),
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
									/*self*/ 0, /*self_inc*/ 1000, &out),
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
					   /*self*/ 0, /*self_inc*/ 1000, &out);

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
					   /*self*/ 0, /*self_inc*/ 1000, &out);

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
					   /*self*/ 0, /*self_inc*/ 1000, &out);

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
					   /*self*/ 0, /*self_inc*/ 1000, &out);

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
					   /*self*/ 0, /*self_inc*/ 1000, &out);

	/* No alive bit set despite the flag, no collision detected. */
	UT_ASSERT_EQ(out.alive_bitmap[0], 0x00);
	UT_ASSERT_EQ(out.collision_state, CLUSTER_COLLISION_NONE);
}


UT_TEST(test_d_10_null_inputs_lost_defensive)
{
	ClusterQuorumDecision out;

	/* NULL slots → LOST. */
	UT_ASSERT_EQ(decide_quorum_view(NULL, NULL, 3, N_NODES, 0, 1000, &out),
				 CLUSTER_QVOTEC_QUORUM_LOST);

	/* 0 disks → LOST. */
	{
		ClusterVotingSlot dummy_slot;
		ClusterVotingDiskIoState dummy_io = CLUSTER_VOTING_DISK_IO_OK;

		UT_ASSERT_EQ(decide_quorum_view(&dummy_slot, &dummy_io, 0, N_NODES, 0, 1000, &out),
					 CLUSTER_QVOTEC_QUORUM_LOST);
	}
}


int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_d_1_three_disks_all_ok_majority);
	UT_RUN(test_d_2_two_of_three_disks_ok_still_majority);
	UT_RUN(test_d_3_one_of_three_disks_ok_uncertain);
	UT_RUN(test_d_4_zero_disks_ok_lost);
	UT_RUN(test_d_5_epoch_max_largest_observed);
	UT_RUN(test_d_6_collision_q6_newer_self_fatal);
	UT_RUN(test_d_7_collision_q6_observed_older_when_self_smaller);
	UT_RUN(test_d_8_collision_same_incarnation_no_collision);
	UT_RUN(test_d_9_generation_zero_skips_alive_and_collision);
	UT_RUN(test_d_10_null_inputs_lost_defensive);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
