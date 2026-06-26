/*-------------------------------------------------------------------------
 *
 * test_cluster_touched_peers.c
 *	  Unit tests for spec-5.14 per-transaction touched_peers bitmap.
 *
 *	  Exercises the pure backend-local bitmap logic of
 *	  cluster_touched_peers.c: stamp (self no-op / valid remote set /
 *	  invalid->poison), intersect (fail-closed), reset, per-kind stamp
 *	  counting, and the parallel-worker -> leader DSM merge (incl. the
 *	  poison-only worker propagation, review round 2 P1).  The
 *	  ProcessInterrupts dispatch (D4) and the shmem counter region (D6)
 *	  live in cluster_reconfig.c and are covered by test_cluster_reconfig
 *	  (U6/U8) + cluster_tap t/293.
 *
 *	  Test IDs map to spec-5.14 §4.1:
 *	    U1  stamp idempotent / self no-op / valid remote set / 128-bit
 *	    U2  intersect: empty / single / disjoint / NULL->true / poison->true
 *	    U2b stamp invalid/oob remote -> poison (not no-op); self -> no-op
 *	    U3  reset clears bitmap + poison fully
 *	    U4  per-kind stamp counting (note hook receives correct kind)
 *	    U5  DSM merge OR (incl. poison-only worker -> leader poison)
 *	    U7  StaticAssert touched bytes == dead_bitmap bytes
 *	    U9  mark_uncertain poisons + reset clears it (review P1-B)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_touched_peers.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.14-fail-stop-reconfig.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_touched_peers.h"
#include "cluster/cluster_reconfig.h" /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES */

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


/* ----------
 * Stubs needed to link cluster_touched_peers.o standalone.
 * ----------
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* The self node id the production stamp() reads; tests set it directly. */
int cluster_node_id = 5;

/* Stand-in for the shmem counter hook (real one lives in cluster_reconfig.c).
 * Records what kind each real stamp reported so U4 can assert the wiring. */
static uint64 stub_stamp_by_kind[CLUSTER_TOUCH_KIND_COUNT];
static uint64 stub_stamp_total;

void
cluster_reconfig_note_touched_stamp(int kind)
{
	if (kind >= 0 && kind < CLUSTER_TOUCH_KIND_COUNT)
		stub_stamp_by_kind[kind]++;
	stub_stamp_total++;
}

static void
stub_counters_reset(void)
{
	memset(stub_stamp_by_kind, 0, sizeof(stub_stamp_by_kind));
	stub_stamp_total = 0;
}


/* white-box bit reader via the public export snapshot */
static bool
touched_bit_is_set(int node)
{
	ClusterTouchedPeersSnapshot snap;

	cluster_touched_peers_export_to_dsm(&snap);
	return (snap.bitmap[node / 8] & (1u << (node % 8))) != 0;
}

static bool
touched_is_poisoned(void)
{
	ClusterTouchedPeersSnapshot snap;

	cluster_touched_peers_export_to_dsm(&snap);
	return snap.unknown_touched;
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * U1 — stamp idempotent / self no-op / valid remote / 128-bit
 * ============================================================ */
UT_TEST(test_stamp_basic)
{
	cluster_node_id = 5;
	cluster_touched_peers_reset();

	/* valid remote -> newly set returns true, bit set */
	UT_ASSERT(cluster_touched_peers_stamp(3, CLUSTER_TOUCH_GES_LOCK) == true);
	UT_ASSERT(touched_bit_is_set(3));
	UT_ASSERT(!touched_is_poisoned());

	/* idempotent: same node again -> false (already set), bit still set */
	UT_ASSERT(cluster_touched_peers_stamp(3, CLUSTER_TOUCH_GES_LOCK) == false);
	UT_ASSERT(touched_bit_is_set(3));

	/* self node -> no-op, no bit, returns false */
	UT_ASSERT(cluster_touched_peers_stamp(5, CLUSTER_TOUCH_GES_LOCK) == false);
	UT_ASSERT(!touched_bit_is_set(5));
	UT_ASSERT(!touched_is_poisoned());

	/* 128-bit coverage: lowest and highest valid node ids */
	UT_ASSERT(cluster_touched_peers_stamp(0, CLUSTER_TOUCH_SCN) == true);
	UT_ASSERT(touched_bit_is_set(0));
	UT_ASSERT(cluster_touched_peers_stamp(CLUSTER_TOUCHED_PEERS_BITS - 1, CLUSTER_TOUCH_SCN)
			  == true);
	UT_ASSERT(touched_bit_is_set(CLUSTER_TOUCHED_PEERS_BITS - 1));
}


/* ============================================================
 * U2 — intersect: empty / single / disjoint / NULL / poison
 * ============================================================ */
UT_TEST(test_intersect)
{
	uint8 dead[CLUSTER_TOUCHED_PEERS_BYTES];

	cluster_node_id = 5;
	cluster_touched_peers_reset();

	/* empty touched ∩ empty dead = false */
	memset(dead, 0, sizeof(dead));
	UT_ASSERT(cluster_touched_peers_intersects(dead) == false);

	/* single overlapping bit -> true */
	cluster_touched_peers_stamp(9, CLUSTER_TOUCH_GCS_BLOCK);
	dead[9 / 8] = (uint8)(1u << (9 % 8));
	UT_ASSERT(cluster_touched_peers_intersects(dead) == true);

	/* disjoint dead -> false */
	memset(dead, 0, sizeof(dead));
	dead[10 / 8] = (uint8)(1u << (10 % 8));
	UT_ASSERT(cluster_touched_peers_intersects(dead) == false);

	/* NULL dead_bitmap -> fail-closed true (INV-TP3) */
	UT_ASSERT(cluster_touched_peers_intersects(NULL) == true);

	/* poison set -> true for ANY dead (even all-zero) */
	cluster_touched_peers_reset();
	cluster_touched_peers_poison(CLUSTER_TOUCH_VISIBILITY);
	memset(dead, 0, sizeof(dead));
	UT_ASSERT(cluster_touched_peers_intersects(dead) == true);
}


/* ============================================================
 * U2b — invalid/oob remote -> poison; self -> no-op
 * ============================================================ */
UT_TEST(test_stamp_invalid_poisons)
{
	uint8 dead[CLUSTER_TOUCHED_PEERS_BYTES];

	cluster_node_id = 5;
	memset(dead, 0, sizeof(dead));

	/* negative node id -> poison (NOT silent no-op) */
	cluster_touched_peers_reset();
	cluster_touched_peers_stamp(-1, CLUSTER_TOUCH_GCS_BLOCK);
	UT_ASSERT(touched_is_poisoned());
	UT_ASSERT(cluster_touched_peers_intersects(dead) == true);

	/* out-of-range node id (== bit count) -> poison */
	cluster_touched_peers_reset();
	cluster_touched_peers_stamp(CLUSTER_TOUCHED_PEERS_BITS, CLUSTER_TOUCH_GCS_BLOCK);
	UT_ASSERT(touched_is_poisoned());

	/* self node id -> no-op, must NOT poison */
	cluster_touched_peers_reset();
	cluster_touched_peers_stamp(5, CLUSTER_TOUCH_GCS_BLOCK);
	UT_ASSERT(!touched_is_poisoned());
	UT_ASSERT(cluster_touched_peers_intersects(dead) == false);
}


/* ============================================================
 * U3 — reset clears bitmap + poison fully
 * ============================================================ */
UT_TEST(test_reset)
{
	uint8 dead[CLUSTER_TOUCHED_PEERS_BYTES];
	int i;

	cluster_node_id = 5;
	cluster_touched_peers_reset();

	cluster_touched_peers_stamp(1, CLUSTER_TOUCH_GES_LOCK);
	cluster_touched_peers_stamp(100, CLUSTER_TOUCH_SINVAL);
	cluster_touched_peers_poison(CLUSTER_TOUCH_SCN);

	cluster_touched_peers_reset();

	/* every bit clear + poison clear */
	for (i = 0; i < CLUSTER_TOUCHED_PEERS_BITS; i++)
		UT_ASSERT(!touched_bit_is_set(i));
	UT_ASSERT(!touched_is_poisoned());

	/* even an all-ones dead set no longer intersects */
	memset(dead, 0xFF, sizeof(dead));
	UT_ASSERT(cluster_touched_peers_intersects(dead) == false);
}


/* ============================================================
 * U4 — per-kind stamp counting (note hook wiring)
 * ============================================================ */
UT_TEST(test_per_kind_counter)
{
	cluster_node_id = 5;
	cluster_touched_peers_reset();
	stub_counters_reset();

	cluster_touched_peers_stamp(3, CLUSTER_TOUCH_GES_LOCK);
	cluster_touched_peers_stamp(7, CLUSTER_TOUCH_SCN);
	cluster_touched_peers_stamp(9, CLUSTER_TOUCH_SINVAL);
	/* a repeat of an already-set node still counts as a stamp op */
	cluster_touched_peers_stamp(3, CLUSTER_TOUCH_GES_LOCK);
	/* self stamp must NOT count */
	cluster_touched_peers_stamp(5, CLUSTER_TOUCH_GES_LOCK);

	UT_ASSERT_EQ((int)stub_stamp_by_kind[CLUSTER_TOUCH_GES_LOCK], 2);
	UT_ASSERT_EQ((int)stub_stamp_by_kind[CLUSTER_TOUCH_SCN], 1);
	UT_ASSERT_EQ((int)stub_stamp_by_kind[CLUSTER_TOUCH_SINVAL], 1);
	UT_ASSERT_EQ((int)stub_stamp_by_kind[CLUSTER_TOUCH_GCS_BLOCK], 0);
	UT_ASSERT_EQ((int)stub_stamp_total, 4);
}


/* ============================================================
 * U5 — DSM merge OR (incl. poison-only worker -> leader poison)
 * ============================================================ */
UT_TEST(test_dsm_merge)
{
	ClusterTouchedPeersSnapshot worker;
	uint8 dead[CLUSTER_TOUCHED_PEERS_BYTES];

	cluster_node_id = 5;

	/* worker with a concrete bit -> leader gets the bit */
	cluster_touched_peers_reset();
	memset(&worker, 0, sizeof(worker));
	worker.bitmap[7 / 8] = (uint8)(1u << (7 % 8));
	cluster_touched_peers_merge_from_dsm(&worker);
	UT_ASSERT(touched_bit_is_set(7));
	UT_ASSERT(!touched_is_poisoned());

	/* poison-only worker (no bits, unknown_touched=true) -> leader poisoned
	 * (review round 2 P1: poison must survive the merge) */
	cluster_touched_peers_reset();
	memset(&worker, 0, sizeof(worker));
	worker.unknown_touched = true;
	cluster_touched_peers_merge_from_dsm(&worker);
	UT_ASSERT(touched_is_poisoned());
	memset(dead, 0, sizeof(dead));
	UT_ASSERT(cluster_touched_peers_intersects(dead) == true);

	/* OR semantics: leader pre-set bit 2, worker sets bit 3 -> both set */
	cluster_touched_peers_reset();
	cluster_touched_peers_stamp(2, CLUSTER_TOUCH_GES_LOCK);
	memset(&worker, 0, sizeof(worker));
	worker.bitmap[3 / 8] = (uint8)(1u << (3 % 8));
	cluster_touched_peers_merge_from_dsm(&worker);
	UT_ASSERT(touched_bit_is_set(2));
	UT_ASSERT(touched_bit_is_set(3));
}


/* ============================================================
 * U7 — touched bitmap width == dead_bitmap width (anti-drift)
 * ============================================================ */
UT_TEST(test_width_invariant)
{
	UT_ASSERT_EQ(CLUSTER_TOUCHED_PEERS_BYTES, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
}


/* ============================================================
 * U9 — mark_uncertain poisons (review P1-B); reset clears it.
 * ============================================================ */
UT_TEST(test_mark_uncertain)
{
	uint8 dead[CLUSTER_TOUCHED_PEERS_BYTES];

	cluster_node_id = 5;
	cluster_touched_peers_reset();
	memset(dead, 0, sizeof(dead));

	/* empty -> no intersect */
	UT_ASSERT(cluster_touched_peers_intersects(dead) == false);

	/* mark_uncertain -> intersects any fail-stop (fail-closed) */
	cluster_touched_peers_mark_uncertain();
	UT_ASSERT(touched_is_poisoned());
	UT_ASSERT(cluster_touched_peers_intersects(dead) == true);

	/* reset clears the poison */
	cluster_touched_peers_reset();
	UT_ASSERT(!touched_is_poisoned());
	UT_ASSERT(cluster_touched_peers_intersects(dead) == false);
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_stamp_basic);
	UT_RUN(test_intersect);
	UT_RUN(test_stamp_invalid_poisons);
	UT_RUN(test_reset);
	UT_RUN(test_per_kind_counter);
	UT_RUN(test_dsm_merge);
	UT_RUN(test_width_invariant);
	UT_RUN(test_mark_uncertain);
	UT_DONE();
}
