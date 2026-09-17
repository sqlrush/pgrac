/*-------------------------------------------------------------------------
 * test_cluster_retained_remote.c
 *   Reject missing outputs before entering remote authority services.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_verdict.h"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 0;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	abort();
}

/* These boundaries must not be reached by a rejected argument list. */
static bool
fixture_admission(ClusterTxResolveMode mode, const ClusterSemanticAdmissionToken *admission)
{
	abort();
}

static void
fixture_stamp(ClusterTxResolution *out, const ClusterSemanticAdmissionToken *admission)
{
	abort();
}

static ClusterUndoVerdictResult
fixture_verdict(int origin, uint32 segment, TransactionId xid, TransactionId ref_xid, uint32 slot,
				uint32 epoch, SCN committed, SCN snapshot)
{
	abort();
}

#define cluster_runtime_visibility_admission_current fixture_admission
#define cluster_runtime_visibility_origin_candidate_stamp fixture_stamp
#define cluster_undo_verdict_resolve_freshref_c1b_pair fixture_verdict
/* Generated verbatim from the production function, not a copied model. */
#include "test_cluster_retained_remote.inc"

UT_TEST(test_missing_output_is_unknown_without_authority_access)
{
	ClusterTxLocator locator = { 0 };
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_terminal_census_retained_remote_exact(
					 &locator, InvalidScn, NULL, NULL, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(cluster_runtime_visibility_resolve_terminal_census_retained_remote_exact(
					 NULL, InvalidScn, NULL, NULL, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_missing_output_is_unknown_without_authority_access);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
