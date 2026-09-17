/*-------------------------------------------------------------------------
 *
 * cluster_side_space.c
 *	  RF-SIDE D-SIDE-05 — canonical space metadata gates under
 *	  STOP-RF-SIDE-SPACE-ABI (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-05 / §2.5 / §5.1 U-SIDE-11/12/18.
 *
 *	  The mutation gate is unconditionally closed while the space ABI is
 *	  unapproved; the metadata page-version gates delegate to the RF-PAGE
 *	  §3.2 decision.  Pure judgements, zero mutation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_space.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_space.h"

bool
cluster_side_space_metadata_mutation_allowed(ClusterSideSpaceKind kind)
{
	/*
	 * STOP-RF-SIDE-SPACE-ABI: no approved canonical space page/redo
	 * ABI exists, so no mutation path may open — for ANY kind (HWM /
	 * extent / bitmap) and with NO override parameter anywhere in the
	 * API (U-SIDE-18: the STOP cannot be turned off by config or test).
	 * "never raise shmem and call complete" (spec §3.2 HW row).
	 */
	if (kind != CLUSTER_SIDE_SPACE_HWM && kind != CLUSTER_SIDE_SPACE_EXTENT
		&& kind != CLUSTER_SIDE_SPACE_BITMAP)
		return false;
	return false;
}

ClusterPageApplyVerdict
cluster_side_space_metadata_page_verdict(ClusterSideSpaceKind kind,
										 const ClusterPageVersion *current_working,
										 const ClusterPageVersion *expected_before,
										 const ClusterPageVersion *result_version,
										 const ClusterPageVersion *trusted_source_version)
{
	/*
	 * §2.5-3: the metadata update must first pass the RF-PAGE class /
	 * PageVersion / expected-before / source-provenance gates; a
	 * mismatch or unknown class is BLOCKED, never a blind max or
	 * last-writer-wins.  This delegates to the single §3.2 decision
	 * (U-SIDE-12 negative tests: result-skip only on a trusted exact
	 * result; expected-before apply only on an exact match).  The kind
	 * is validated for the contract but does not change the maths.
	 */
	if (kind != CLUSTER_SIDE_SPACE_HWM && kind != CLUSTER_SIDE_SPACE_EXTENT
		&& kind != CLUSTER_SIDE_SPACE_BITMAP)
		return CLUSTER_PAGE_APPLY_BLOCKED;
	return cluster_page_version_decide(current_working, expected_before, result_version,
									   trusted_source_version);
}
