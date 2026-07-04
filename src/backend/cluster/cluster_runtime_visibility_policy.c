/*-------------------------------------------------------------------------
 *
 * cluster_runtime_visibility_policy.c
 *	  pgrac spec-6.12 wave 6.12i — pure live-authority gate (no shmem, no
 *	  locks, no elog) so cluster_unit exercises every branch standalone.
 *
 *	  See cluster_runtime_visibility.h for why active runtime needs a live
 *	  authority source distinct from the recovery-time materialized marker
 *	  ("active state != recovery state").  This file is the D-i2 window
 *	  predicate only; the fetch (D-i1) and the resolve wiring (D-i2/D-i3)
 *	  land in cluster_runtime_visibility.c (CP2/CP3).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_runtime_visibility_policy.c
 *
 * NOTES
 *	  The predicate is fail-closed by construction: it returns true only when
 *	  ALL admit conditions hold, and false on every doubt.  A recycled remote
 *	  ITL slot whose authority does not provably cover this page version must
 *	  resolve to STALE_OR_AMBIGUOUS (53R97), never visible (规则 8.A).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_runtime_visibility.h"

/*
 * cluster_vis_live_authority_covers_policy
 *
 * Pure D-i2 window gate.  See header for the fail-closed contract.  The
 * three admit conditions are ANDed; any single failure returns false so the
 * caller keeps the pre-existing 53R97 fail-closed boundary (this wave only
 * widens "resolve when provable", never widens "resolve when unprovable").
 */
bool
cluster_vis_live_authority_covers_policy(XLogRecPtr anchor_lsn, ClusterLiveAuthority auth,
										 uint32 local_epoch)
{
	/*
	 * (1) Same reconfig generation.  Authority sampled under a different
	 * membership epoch cannot be trusted: a reconfig may have remastered or
	 * fenced the origin between sampling and use (D-i3 crash-shrink).
	 */
	if (auth.origin_epoch != local_epoch)
		return false;

	/*
	 * (2) Authority actually present.  An undo-block reply that did not carry
	 * a live authority (older peer / off path) leaves live_hwm_lsn invalid ->
	 * fail closed, never guess.
	 */
	if (XLogRecPtrIsInvalid(auth.live_hwm_lsn))
		return false;

	/*
	 * (3) Durable coverage of THIS page version.  live_hwm_lsn is the origin's
	 * durable-and-TT-applied high-water; only if it is at or beyond the
	 * tuple's page LSN has the origin's durable TT reconciled this version.
	 * Semantically equivalent to the recovery-side recovered_through >=
	 * anchor_lsn gate, but sourced from a live durable watermark rather than a
	 * merge-complete marker (spec-6.12 §2.11 torn-history equivalence).
	 */
	if (auth.live_hwm_lsn < anchor_lsn)
		return false;

	return true;
}
