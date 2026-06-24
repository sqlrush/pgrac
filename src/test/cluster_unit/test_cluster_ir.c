/*-------------------------------------------------------------------------
 *
 * test_cluster_ir.c
 *	  Unit tests for the IR (instance-recovery owner) pure layer (spec-5.7 D8,
 *	  §3.4).
 *
 *	  Covers the IR resource-id encoder -- per (dead_node, episode_epoch)
 *	  identity, the 0xF5 namespace marker, the full 64-bit epoch carried so a new
 *	  reconfig episode is a DISTINCT resid (IR-M2), non-collision with the SQ
 *	  (0xF0) / CF (0xF1) / HW (0xF2) / DL (0xF3) namespaces -- and the IR-M5
 *	  bootstrap-phase predicate (the epoch-accepted gate).  The GES-enforced
 *	  acquire/release + the recovery-worker mutation gate (53RA9) are exercised by
 *	  the mechanism-level driver (t/295); this file pins the pure layer that names
 *	  the recovery-owner resource and gates when it may be claimed.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ir.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_sequence.h"
#include "storage/relfilelocator.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ======================================================================
 * U1 -- IR resid encoding: per (dead_node, epoch), namespace 0xF5, no fork.
 * The 64-bit epoch splits into field2 (low) / field3 (high).
 * ====================================================================== */
UT_TEST(test_ir_resid_encode)
{
	ClusterResId r;
	uint64 epoch = (UINT64CONST(0x1234) << 32) | UINT64CONST(0xABCD0001);

	memset(&r, 0xEE, sizeof(r));
	cluster_ir_resid_encode(2, epoch, &r);

	UT_ASSERT_EQ(r.field1, 2);			/* dead_node_id */
	UT_ASSERT_EQ(r.field2, 0xABCD0001); /* epoch low 32 */
	UT_ASSERT_EQ(r.field3, 0x1234);		/* epoch high 32 */
	UT_ASSERT_EQ(r.field4, 0);			/* IR is per (node,epoch): no fork */
	UT_ASSERT_EQ(r.type, CLUSTER_IR_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF5);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* namespace 0xF5 is distinct from SQ / CF / HW / DL */
UT_TEST(test_ir_resid_namespace_distinct)
{
	ClusterResId r;

	cluster_ir_resid_encode(0, 1, &r);

	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE); /* != 0xF0 */
	UT_ASSERT_NE(r.type, CLUSTER_CF_RESID_TYPE); /* != 0xF1 */
	UT_ASSERT_NE(r.type, CLUSTER_HW_RESID_TYPE); /* != 0xF2 */
	UT_ASSERT_NE(r.type, CLUSTER_DL_RESID_TYPE); /* != 0xF3 */
}

/* IR-M2: a new reconfig episode = a new epoch = a DISTINCT resid, so an old
 * owner's lock naturally does not alias the new episode's. */
UT_TEST(test_ir_resid_epoch_distinct)
{
	ClusterResId e1, e2;

	cluster_ir_resid_encode(3, 100, &e1);
	cluster_ir_resid_encode(3, 101, &e2); /* same dead node, next episode */

	UT_ASSERT_EQ(e1.field1, e2.field1);			  /* same dead node */
	UT_ASSERT_NE(e1.field2, e2.field2);			  /* distinct epoch -> distinct resid */
	UT_ASSERT_EQ(e1.type, CLUSTER_IR_RESID_TYPE); /* both IR */
}

/* the high half of a >32-bit epoch lands in field3 (no truncation/aliasing). */
UT_TEST(test_ir_resid_epoch_high_half)
{
	ClusterResId lo, hi;

	cluster_ir_resid_encode(1, UINT64CONST(0x00000000FFFFFFFF), &lo);
	cluster_ir_resid_encode(1, UINT64CONST(0x0000000100000000), &hi);

	/* low half identical only on field2; the 1<<32 difference must show in field3 */
	UT_ASSERT_NE(lo.field3, hi.field3);
}

/* ======================================================================
 * U2 -- IR-M5 bootstrap predicate: acquire only when the launch episode epoch
 * is the CURRENT accepted epoch (and a real episode, != 0).
 * ====================================================================== */
UT_TEST(test_ir_bootstrap_ready)
{
	/* accepted episode matches -> ready */
	UT_ASSERT(cluster_ir_bootstrap_ready(42, 42));
	/* epoch advanced past the launch (stale) -> NOT ready (defer; L235 abort) */
	UT_ASSERT(!cluster_ir_bootstrap_ready(42, 43));
	/* launch epoch ahead of accepted (impossible, but fail closed) -> NOT ready */
	UT_ASSERT(!cluster_ir_bootstrap_ready(43, 42));
	/* no episode (epoch 0) is never "accepted" -> NOT ready */
	UT_ASSERT(!cluster_ir_bootstrap_ready(0, 0));
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_ir_resid_encode);
	UT_RUN(test_ir_resid_namespace_distinct);
	UT_RUN(test_ir_resid_epoch_distinct);
	UT_RUN(test_ir_resid_epoch_high_half);
	UT_RUN(test_ir_bootstrap_ready);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
