/*-------------------------------------------------------------------------
 *
 * test_cluster_operation_refusal_boundary.inc
 *   Unavailable origin boundary for legacy-only CR fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_operation_refusal_boundary.inc
 *
 * NOTES
 *   Operation-worker tests link the real runtime, never these doubles.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_runtime_visibility.h"

bool
cluster_runtime_visibility_operation_origin_read_begin(
	ClusterRuntimeVisibilityOperationRead *state pg_attribute_unused(),
	const ClusterTxLocator *locator pg_attribute_unused(),
	const ClusterSavepointOperation *expected pg_attribute_unused(),
	const ClusterItlSlotData *head pg_attribute_unused(),
	const BufferTag *tag pg_attribute_unused(), ClusterTxResolveMode mode pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused(),
	ClusterTxResolveReason *reason)
{
	UT_ASSERT(false);
	*reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	return false;
}

ClusterRuntimeVisibilityOperationStep
cluster_runtime_visibility_operation_origin_read_poll(
	ClusterRuntimeVisibilityOperationRead *state pg_attribute_unused(),
	ClusterSavepointReaderIdentity *identity pg_attribute_unused(),
	ClusterTxResolution *top pg_attribute_unused(),
	ClusterSavepointReaderRecord *operation pg_attribute_unused(),
	char page[BLCKSZ] pg_attribute_unused(), ClusterTxResolveReason *reason)
{
	UT_ASSERT(false);
	*reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	return CLUSTER_RUNTIME_VISIBILITY_OPERATION_FAILED;
}

void
cluster_runtime_visibility_operation_origin_read_cancel(
	ClusterRuntimeVisibilityOperationRead *state pg_attribute_unused())
{
	UT_ASSERT(false);
}

#ifdef OPERATION_REFUSAL_BOUNDARY_ADMISSION
bool
cluster_semantic_activation_recheck(
	const ClusterSemanticAdmissionToken *token pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}
#endif
