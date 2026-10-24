/*-------------------------------------------------------------------------
 * cluster_relmap_pending_side.h
 *    Original sealed SIDE inputs for relmap structural pending rebuild.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_PENDING_SIDE_H
#define CLUSTER_RELMAP_PENDING_SIDE_H

#include "cluster/cluster_relmap_pending.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_side_online_plan.h"

/* One original physical participant. A multi-participant plan appears once
 * per participant; its complete operation vector is enumerated only once. */
typedef struct ClusterRelmapPendingSideSource
{
	const RfSideOnlinePlanV1 *plan;
	RfContributorStreamCutV1 cut;
	ClusterWalSourceRef source;
} ClusterRelmapPendingSideSource;

/*
 * Extract TRUNCATE, every native COMMIT's typed DROP and native ABORT's
 * original paired CREATE keys from the complete retained operation vector,
 * including history before native redo. Require
 * all physical participants of each supplied sealed RECOVERY plan, matching
 * its original cut and full bound ROOT/claim. Inspect-only, unbound, repeated
 * origins and missing participants refuse; an absent input vector is not an
 * explicit empty source. The original owner still certifies completeness of
 * the OVERALL selected source/plan set and its final revalidation. Cut graph
 * counts do not supply source, base, floor or last-issued authority.
 *
 * Select the requested global/local database namespace; valid records for
 * other database namespaces remain the caller's separate obligations. CREATE
 * and ADVANCE are not DROP/TRUNCATE. Repeated physical locators are ambiguous
 * and refuse, never deduplicated or resolved by a new terminal owner. Each
 * ABORT pending-delete locator requires the original sealed native/typed
 * CREATE pairing; it contributes a DROP reservation, not a tombstone or
 * completed deletion. Missing pairing and PREPARED remain NOT_READY; they
 * must never become a successful empty directory.
 *
 * At most CLUSTER_RELMAP_PENDING_CAPACITY descriptors are returned. Outputs
 * and count are unchanged on any failure. Output may be NULL only at zero
 * capacity. Inputs must remain immutable and disjoint from the outputs for
 * this call. Results are provisional input to the original pending builder,
 * not evidence of unfinished physical deletion, MAIN file existence, terminal
 * truth or release. Whole ROOT DRAINED and independent native/TT/SPACE/copy
 * proofs remain mandatory; no live consumer or v2 activation is introduced.
 */
extern ClusterRelmapPendingResult cluster_relmap_pending_collect_side(
	const ClusterRelmapV2Namespace *expected, const uint8 storage_uuid[16],
	const ClusterRelmapPendingSideSource *sources, size_t source_count,
	ClusterRelmapPendingDescriptor *out, size_t capacity, size_t *count_out);

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_RELMAP_PENDING_SIDE_H */
