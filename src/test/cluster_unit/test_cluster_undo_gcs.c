/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_gcs.c
 *	  Unit tests for the shared-undo block GCS integration data plane
 *	  (spec-5.22b D2).
 *
 *	  D2-1 (this increment) pins owner-as-master routing: the master of an
 *	  undo resource IS the encoded owner_node (never a shard hash), and the
 *	  master==self predicate selects the local fast path.  The grant / PI /
 *	  physical-migration legs (D2-2 .. D2-5) add their own truth-table cases
 *	  to this file as they land.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_gcs.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2, §4.1)
 *
 *	  cluster_undo_gcs.o references the cluster_node_id global (self node);
 *	  the test supplies its own definition so the routing object links
 *	  standalone without the full GUC layer.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_scn.h" /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_resid.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Stub self-node global.  cluster_undo_gcs.o resolves cluster_node_id from
 * cluster_guc.c in a real backend; here the test owns it so master_is_self
 * can be driven against a known self node.
 */
int cluster_node_id = 0;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ======================================================================
 * U1 -- owner-as-master routing: lookup_master returns the encoded
 * owner_node, never a hash-derived node (spec-5.22b §2.1, Q1)
 * ====================================================================== */
UT_TEST(test_undo_gcs_lookup_master_is_owner)
{
	ClusterResId r;

	cluster_undo_resid_encode(2, 7, 129, 3, &r);
	UT_ASSERT_EQ(cluster_undo_block_lookup_master(&r), 2);

	/* the master must not vary with the non-owner identity dimensions
	 * (a shard-hash master would move with segment/block/generation) */
	cluster_undo_resid_encode(2, 9999, 424242, 17, &r);
	UT_ASSERT_EQ(cluster_undo_block_lookup_master(&r), 2);

	cluster_undo_resid_encode(0, 1, 1, 0, &r);
	UT_ASSERT_EQ(cluster_undo_block_lookup_master(&r), 0);

	cluster_undo_resid_encode(SCN_MAX_VALID_NODE_ID, 1, 1, 1, &r);
	UT_ASSERT_EQ(cluster_undo_block_lookup_master(&r), SCN_MAX_VALID_NODE_ID);
}

/* ======================================================================
 * U2 -- master_is_self: owner_node == cluster_node_id selects the local
 * fast path (true); a foreign owner routes remote (false) (spec-5.22b §2.1)
 * ====================================================================== */
UT_TEST(test_undo_gcs_master_is_self)
{
	ClusterResId r;

	cluster_node_id = 2;
	cluster_undo_resid_encode(2, 7, 129, 3, &r); /* owner == self */
	UT_ASSERT(cluster_undo_block_master_is_self(&r));

	cluster_undo_resid_encode(3, 7, 129, 3, &r); /* owner 3 != self 2 */
	UT_ASSERT(!cluster_undo_block_master_is_self(&r));

	cluster_node_id = 0;
	cluster_undo_resid_encode(0, 1, 1, 0, &r); /* owner == self */
	UT_ASSERT(cluster_undo_block_master_is_self(&r));

	cluster_undo_resid_encode(1, 1, 1, 0, &r); /* owner 1 != self 0 */
	UT_ASSERT(!cluster_undo_block_master_is_self(&r));

	cluster_node_id = SCN_MAX_VALID_NODE_ID;
	cluster_undo_resid_encode(SCN_MAX_VALID_NODE_ID, 4, 4, 4, &r);
	UT_ASSERT(cluster_undo_block_master_is_self(&r));
}

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(test_undo_gcs_lookup_master_is_owner);
	UT_RUN(test_undo_gcs_master_is_self);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
