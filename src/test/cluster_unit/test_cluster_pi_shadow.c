/*-------------------------------------------------------------------------
 *
 * test_cluster_pi_shadow.c
 *	  Standalone unit tests for the spec-6.12h D-h3a Past Image ship-SCN
 *	  shadow table and pure recovery-boundary gate (cluster_pi_shadow.o).
 *
 *	  U-h coverage (the pure half; the byte-equivalence of a PI-based
 *	  recovery rebuild against storage + full redo is the D-h3b TAP leg):
 *	    1) region math + slot lifecycle: size = MAXALIGN(NBuffers * 8),
 *	       zero-init reads InvalidScn everywhere, stamp/read/clear
 *	       roundtrip, and the detached (NULL) guards fail closed.
 *	    2) UNUSABLE rows: an invalid ship stamp (clock unarmed at
 *	       conversion) or an invalid xl_scn (legacy-thread record) makes
 *	       the boundary unprovable regardless of the other operand.
 *	    3) lineage rows (same node): counter below OR EQUAL to the ship
 *	       stamp is SKIP — equality is always the lineage side because
 *	       the post-ship chain is strictly above (observe = max+1).
 *	    4) post-ship rows (same node): strictly greater counter is
 *	       APPLY, including the minimal +1 adjacency.
 *	    5) cross-node counter ties: SKIP in both node-id directions —
 *	       node_id never breaks ties.
 *	    6) raw-compare traps: a HIGHER node id with a SMALLER counter
 *	       must SKIP and a LOWER node id with a BIGGER counter must
 *	       APPLY (a raw uint64 SCN compare would get both wrong —
 *	       node-id bits dominate the encoding).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pi_shadow.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_pi_shadow.o only; PG backend symbols stubbed locally
 *	  (shmem backed by plain calloc -- the unit harness is
 *	  single-threaded).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_pi_shadow.h"
#include "cluster/cluster_shmem.h"

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


/* ============================================================
 * Stubs needed to link cluster_pi_shadow.o standalone.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* Shared-buffer count (bufmgr.o not linked). */
int NBuffers = 64;

/* Shmem: back the region with plain malloc. */
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	*foundPtr = false;
	return calloc(1, size);
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}


/* ============================================================
 * Helpers
 * ============================================================ */

static SCN
scn_of(NodeId node, uint64 local)
{
	return scn_encode(node, local);
}


/* ============================================================
 * Tests
 * ============================================================ */

UT_TEST(test_region_math_and_slot_lifecycle)
{
	SCN *saved;
	int i;

	UT_ASSERT_EQ(cluster_pi_shadow_shmem_size(), MAXALIGN((Size)NBuffers * sizeof(SCN)));

	/* zero-init == InvalidScn in every slot (fail-closed baseline) */
	for (i = 0; i < NBuffers; i++)
		UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(i)));

	/* stamp / read / clear roundtrip; neighbours untouched */
	cluster_pi_shadow_stamp(3, scn_of(1, 100));
	UT_ASSERT_EQ(cluster_pi_shadow_read(3), scn_of(1, 100));
	UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(2)));
	UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(4)));
	cluster_pi_shadow_clear(3);
	UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(3)));

	/* re-stamp overwrites (a re-converted PI always carries the CURRENT
	 * conversion's stamp) */
	cluster_pi_shadow_stamp(5, scn_of(0, 7));
	cluster_pi_shadow_stamp(5, scn_of(0, 9));
	UT_ASSERT_EQ(cluster_pi_shadow_read(5), scn_of(0, 9));

	/* detached guards: read fails closed to InvalidScn, stamp is a no-op */
	saved = ClusterPiShadow;
	ClusterPiShadow = NULL;
	cluster_pi_shadow_stamp(1, scn_of(1, 1));
	UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(1)));
	ClusterPiShadow = saved;
	UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(1)));

	/* negative buf_id guard */
	cluster_pi_shadow_stamp(-1, scn_of(1, 1));
	UT_ASSERT(!SCN_VALID(cluster_pi_shadow_read(-1)));
}

UT_TEST(test_gate_unusable_rows)
{
	/* unarmed conversion stamp: nothing provable, whatever the record */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(0, 5), InvalidScn), CLUSTER_PI_GATE_UNUSABLE);
	UT_ASSERT_EQ(cluster_pi_recovery_gate(InvalidScn, InvalidScn), CLUSTER_PI_GATE_UNUSABLE);

	/* unstamped (legacy-thread) record against a valid stamp */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(InvalidScn, scn_of(0, 5)), CLUSTER_PI_GATE_UNUSABLE);
}

UT_TEST(test_gate_lineage_rows_same_node)
{
	/* below the stamp */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 99), scn_of(1, 100)), CLUSTER_PI_GATE_SKIP);
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 1), scn_of(1, 100)), CLUSTER_PI_GATE_SKIP);

	/* equality IS lineage: the last lineage record may share the counter
	 * with the conversion sample (xl_scn = scn_current, no advance
	 * between), while the post-ship chain is strictly above (observe =
	 * max+1) -- so the boundary itself must SKIP */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 100), scn_of(1, 100)), CLUSTER_PI_GATE_SKIP);
}

UT_TEST(test_gate_post_ship_rows_same_node)
{
	/* minimal strict step: one observe/advance above the stamp */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 101), scn_of(1, 100)), CLUSTER_PI_GATE_APPLY);

	/* far side */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 100000), scn_of(1, 100)),
				 CLUSTER_PI_GATE_APPLY);
}

UT_TEST(test_gate_cross_node_counter_ties)
{
	/* equal counters, different nodes: SKIP both ways -- node_id never
	 * breaks ties (the post-ship side is strictly greater by the
	 * observe(+1) chain, so a counter tie can only be lineage) */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(2, 100), scn_of(1, 100)), CLUSTER_PI_GATE_SKIP);
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 100), scn_of(2, 100)), CLUSTER_PI_GATE_SKIP);
}

UT_TEST(test_gate_raw_compare_traps)
{
	/* higher node id, SMALLER counter: raw uint64 order would call this
	 * "greater" (node bits dominate) and wrongly re-apply a lineage
	 * record -- the counter dimension says SKIP */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(7, 50), scn_of(1, 100)), CLUSTER_PI_GATE_SKIP);

	/* lower node id, BIGGER counter: raw order would call this "smaller"
	 * and wrongly skip a post-ship record -- the counter says APPLY */
	UT_ASSERT_EQ(cluster_pi_recovery_gate(scn_of(1, 200), scn_of(7, 100)), CLUSTER_PI_GATE_APPLY);
}

int
main(void)
{
	/* stand-in for cluster_pi_shadow_shmem_init over malloc-backed memory */
	cluster_pi_shadow_shmem_init();

	UT_PLAN(6);

	UT_RUN(test_region_math_and_slot_lifecycle);
	UT_RUN(test_gate_unusable_rows);
	UT_RUN(test_gate_lineage_rows_same_node);
	UT_RUN(test_gate_post_ship_rows_same_node);
	UT_RUN(test_gate_cross_node_counter_ties);
	UT_RUN(test_gate_raw_compare_traps);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
