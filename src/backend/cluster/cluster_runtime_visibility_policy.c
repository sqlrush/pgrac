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
#include "cluster/cluster_undo_segment.h" /* UndoSegmentHeaderData, TTSlot */

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
										 uint64 local_epoch)
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

/*
 * cluster_vis_tt_block_positive_proof
 *
 * CP3 positive-proof scan over a D-i1-fetched TT header block.  See the
 * header comment for the full proof discipline (positive proof only; 0-match
 * / multi-match / non-terminal / malformed all refuse).  The block bytes are
 * the origin's own shipped copy, so every refusal is a clean NONE — never an
 * elog — and the caller keeps the 53R97 fail-closed boundary.
 */
ClusterVisTtProof
cluster_vis_tt_block_positive_proof(const char *block, uint32 expected_segment_id,
									uint8 expected_owner_instance, TransactionId xid,
									SCN *out_commit_scn, uint16 *out_wrap)
{
	const UndoSegmentHeaderData *hdr;
	int nmatch = 0;
	const TTSlot *match = NULL;

	if (out_commit_scn != NULL)
		*out_commit_scn = InvalidScn;
	if (out_wrap != NULL)
		*out_wrap = TT_WRAP_INVALID;

	if (block == NULL || !TransactionIdIsNormal(xid))
		return CLUSTER_VIS_TT_PROOF_NONE;

	/*
	 * Header identity sanity: the bytes must provably be the asked-for TT.
	 * A mismatched segment_id / owner (raced segment reuse on the origin, a
	 * stale cache pairing, a mis-routed reply) or an over-range slot count
	 * means the scan below would not be evidence about `xid` — refuse.
	 */
	hdr = (const UndoSegmentHeaderData *)block;
	if (hdr->segment_id != expected_segment_id)
		return CLUSTER_VIS_TT_PROOF_NONE;
	if (hdr->owner_instance != expected_owner_instance)
		return CLUSTER_VIS_TT_PROOF_NONE;
	if (hdr->tt_slots_count > TT_SLOTS_PER_SEGMENT)
		return CLUSTER_VIS_TT_PROOF_NONE;

	for (int i = 0; i < (int)hdr->tt_slots_count; i++) {
		const TTSlot *slot = &hdr->tt_slots[i];

		/*
		 * Any status byte outside the known range poisons the whole block:
		 * we cannot claim to have understood a TT whose slots we cannot
		 * parse (this is a shipped COPY — refuse, never PANIC).
		 */
		if (slot->status > (uint8)TT_SLOT_RECYCLABLE)
			return CLUSTER_VIS_TT_PROOF_NONE;

		/* UNUSED carries no transaction; its xid bytes are placeholder. */
		if (slot->status == (uint8)TT_SLOT_UNUSED)
			continue;

		/*
		 * Every OCCUPIED status (ACTIVE / COMMITTED / ABORTED / RECYCLABLE)
		 * counts as a match candidate: same-xid residue in a RECYCLABLE slot
		 * is ambiguity evidence even though it can never itself resolve.
		 */
		if (slot->xid == xid) {
			nmatch++;
			match = slot;
		}
	}

	if (nmatch != 1)
		return CLUSTER_VIS_TT_PROOF_NONE;

	if (match->status == (uint8)TT_SLOT_COMMITTED && SCN_VALID(match->commit_scn)) {
		if (out_commit_scn != NULL)
			*out_commit_scn = match->commit_scn;
		if (out_wrap != NULL)
			*out_wrap = match->wrap;
		return CLUSTER_VIS_TT_PROOF_COMMITTED;
	}
	if (match->status == (uint8)TT_SLOT_ABORTED) {
		if (out_wrap != NULL)
			*out_wrap = match->wrap;
		return CLUSTER_VIS_TT_PROOF_ABORTED;
	}

	/* ACTIVE / RECYCLABLE / COMMITTED-without-scn: not a terminal proof. */
	return CLUSTER_VIS_TT_PROOF_NONE;
}
