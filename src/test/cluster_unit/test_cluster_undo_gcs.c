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
#include "cluster/storage/cluster_undo_alloc.h" /* ClusterUndoPathIntent */

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

/* ======================================================================
 * U3 -- physical-path root decision (D2-2): a RUNTIME_SHARED (own-instance
 * live) undo segment migrates to the shared cluster_fs root ONLY under
 * peer-mode AND cluster.undo_gcs_coherence; every other mode combination
 * stays on the local DataDir (inert, zero behaviour change) (spec-5.22b
 * §2.3/§3.5, Hardening v1.0.1 裁决 A, Q8/Q9)
 * ====================================================================== */
UT_TEST(test_undo_gcs_path_runtime_shared_mode_branch)
{
	/* migrate: own runtime undo, peer-mode, coherence on */
	UT_ASSERT(cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, true /*peer*/,
												 true /*coherence*/));

	/* coherence off => inert, stay local (回归安全, 回 6.12i 无主权路径) */
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, true /*peer*/,
												  false /*coherence*/));

	/* single-node / non-peer-mode => local regardless of coherence */
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, false /*peer*/,
												  true /*coherence*/));
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, false /*peer*/,
												  false /*coherence*/));
}

/* ======================================================================
 * U10 -- path-intent layering (P1-3 hard contract): a MATERIALIZED_LOCAL
 * (dead-origin materialized copy that recovery rebuilt in the local
 * DataDir) NEVER migrates to the shared root, in ANY mode.  D2 must not
 * redirect dead-origin materialized reads, else dead-origin recovery / CR
 * regress (spec-5.22b §3.6, R1, Q9)
 * ====================================================================== */
UT_TEST(test_undo_gcs_path_materialized_never_migrates)
{
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL,
												  true /*peer*/, true /*coherence*/));
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL,
												  true /*peer*/, false /*coherence*/));
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL,
												  false /*peer*/, true /*coherence*/));
	UT_ASSERT(!cluster_undo_path_uses_shared_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL,
												  false /*peer*/, false /*coherence*/));
}

/* ======================================================================
 * U11 -- per-call intent derivation (spec-5.22b D2-2, threading strategy B):
 * an undo segment whose owner IS this node maps to RUNTIME_SHARED (own live
 * undo); any foreign owner maps to MATERIALIZED_LOCAL (dead-origin
 * materialized copy).  This is the single derivation every smgr call site
 * uses, so a misclassification cannot hide at an individual site.
 * owner_instance == cluster_node_id + 1 is "self" (the +1 sentinel offset).
 * ====================================================================== */
UT_TEST(test_undo_gcs_intent_for_owner)
{
	cluster_node_id = 0; /* self owner_instance = 1 */
	UT_ASSERT_EQ(cluster_undo_intent_for_owner(1), CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(cluster_undo_intent_for_owner(2), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
	UT_ASSERT_EQ(cluster_undo_intent_for_owner(3), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);

	cluster_node_id = 2; /* self owner_instance = 3 */
	UT_ASSERT_EQ(cluster_undo_intent_for_owner(3), CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(cluster_undo_intent_for_owner(1), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
	UT_ASSERT_EQ(cluster_undo_intent_for_owner(4), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);

	cluster_node_id = 0; /* restore stub default */
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_undo_gcs_lookup_master_is_owner);
	UT_RUN(test_undo_gcs_master_is_self);
	UT_RUN(test_undo_gcs_path_runtime_shared_mode_branch);
	UT_RUN(test_undo_gcs_path_materialized_never_migrates);
	UT_RUN(test_undo_gcs_intent_for_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
