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

#include "access/multixact.h"		   /* MultiXactStatusForUpdate / MaxMultiXactStatus (D3-b) */
#include "cluster/cluster_gcs_block.h" /* ClusterGcsUndo{,Multi}VerdictPage (CP5 / D3-b) */
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_scn.h"		  /* scn_time_cmp / SCN_VALID (spec-7.1a D3) */
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
cluster_vis_live_authority_covers_policy(SCN demand_scn, ClusterLiveAuthority auth,
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
	 * fail closed, never guess.  The SCN co-sample must be present too: an
	 * older peer ships zero trailer bytes = InvalidScn -> refuse.
	 */
	if (XLogRecPtrIsInvalid(auth.live_hwm_lsn))
		return false;
	if (!SCN_VALID(auth.authority_scn))
		return false;

	/*
	 * (3) SCN-total-order conclusiveness (spec-7.1a D3, replacing the former
	 * `live_hwm_lsn < anchor_lsn` compare that was unsound across per-thread
	 * WAL streams: a page last written by another thread carries an LSN from
	 * a different stream, so the raw compare false-refused live resolutions
	 * and could false-pass -- both measured/analyzed in gaps §C.2).  The
	 * caller demands conclusiveness for demand_scn (its snapshot read_scn,
	 * or its own clock sampled BEFORE the fetch): the origin allocates its
	 * commit SCNs from the clock this authority co-sampled and durably
	 * stamps the TT BEFORE the commit record, so authority_scn at/after the
	 * demand proves every commit the demand could have observed is already
	 * in the shipped content (AD-008 Lamport total order across threads and
	 * nodes).  A refusal self-heals: the shipped SCNs are Lamport-observed
	 * by the consumer, so the next demand is at/after this authority.
	 */
	if (!SCN_VALID(demand_scn))
		return false;
	if (scn_time_cmp(auth.authority_scn, demand_scn) < 0)
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
 *
 * PROOF_COMMITTED is EVIDENCE, not a verdict (spec-7.1a hardening): durable
 * COMMITTED stamps land at pre-commit (2PC COMMIT PREPARED stamps before the
 * commit record — cluster_tt_durable.h), so a stamped-then-crashed xid is
 * in-doubt.  Consumers must finalize a COMMITTED proof through the origin's
 * C1b CLOG cross-check (the verdict leg); only PROOF_ABORTED may be consumed
 * directly (an abort stamp is terminal and irreversible).
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

/*
 * cluster_vis_undo_verdict_page_usable
 *
 * CP5 (D-i4) pure structural validation of a shipped verdict page.  See the
 * header for the contract: every field must be exactly consistent with the
 * claimed verdict kind, or the page is not evidence about `asked_xid` and
 * the caller keeps the 53R97 fail-closed boundary.  The read_scn
 * admissibility of a BELOW_HORIZON bound (requester leg (e)) is NOT decided
 * here — it needs scn_time_cmp, which is deliberately outside this
 * dependency-free policy object; the consumer applies it.
 */
bool
cluster_vis_undo_verdict_page_usable(const struct ClusterGcsUndoVerdictPage *v,
									 TransactionId asked_xid)
{
	if (v == NULL || !TransactionIdIsNormal(asked_xid))
		return false;

	if (v->magic != CLUSTER_GCS_UNDO_VERDICT_MAGIC
		|| v->version != CLUSTER_GCS_UNDO_VERDICT_VERSION)
		return false;

	/* The echo is the widened xid: upper 32 bits MUST be zero (a non-zero
	 * high word means a corrupted or foreign carrier, never a valid echo). */
	if (v->xid_echo != (uint64)asked_xid)
		return false;

	for (size_t i = 0; i < sizeof(v->reserved_0); i++)
		if (v->reserved_0[i] != 0)
			return false;
	for (size_t i = 0; i < sizeof(v->reserved_1); i++)
		if (v->reserved_1[i] != 0)
			return false;

	switch (v->verdict) {
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
		/* Exact terminal commit: needs the exact scn, carries no bound. */
		return SCN_VALID(v->commit_scn) && !SCN_VALID(v->horizon_scn);
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
		/* Bound-only commit: needs the horizon, must NOT claim an exact scn
		 * (a stray commit_scn here could leak into stamp/cache paths). */
		return !SCN_VALID(v->commit_scn) && SCN_VALID(v->horizon_scn);
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		/* Terminal abort carries no scn of any kind. */
		return !SCN_VALID(v->commit_scn) && !SCN_VALID(v->horizon_scn);
	default:
		return false; /* unknown kind: refuse, never guess */
	}
}

/*
 * cluster_vis_undo_multi_verdict_page_usable — see header for the contract.
 */
bool
cluster_vis_undo_multi_verdict_page_usable(const struct ClusterGcsUndoMultiVerdictPage *v,
										   MultiXactId asked_mxid)
{
	uint16 i;

	if (v == NULL || !MultiXactIdIsValid(asked_mxid))
		return false;

	if (v->magic != CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC
		|| v->version != CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION)
		return false;

	/* The echo is the widened mxid: upper 32 bits MUST be zero (a non-zero
	 * high word means a corrupted or foreign carrier, never a valid echo). */
	if (v->mxid_echo != (uint64)asked_mxid)
		return false;

	/* Only a fully-proven SERVED page carries consumable members; every other
	 * status ships as a DENIED reply (no page), so it should never reach here.
	 * Re-check defensively (8.A): never consume an UNPROVABLE / NOT_MINE page. */
	if (v->status != (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED)
		return false;

	/* A real multi always has members; the origin refuses < 2 as NO_MEMBERS.
	 * Reject 0 (nothing to consume) and anything past the wire capacity. */
	if (v->nmembers == 0 || v->nmembers > CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS)
		return false;

	for (i = 0; i < sizeof(v->reserved_0); i++)
		if (v->reserved_0[i] != 0)
			return false;

	for (i = 0; i < v->nmembers; i++) {
		const ClusterGcsUndoMultiVerdictMember *m = &v->members[i];

		/* member_status must be a real MultiXactStatus (0..5). */
		if (m->member_status > MaxMultiXactStatus)
			return false;

		/* Lock-only members (<= MultiXactStatusForUpdate, status 0-3) never
		 * gate visibility (A2): they carry no verdict and no scn of any kind. */
		if (m->member_status <= MultiXactStatusForUpdate) {
			if (m->verdict != 0 || SCN_VALID(m->commit_scn) || SCN_VALID(m->horizon_scn))
				return false;
			continue;
		}

		/* Updater members (4-5): the verdict kind must be known and its scn
		 * fields consistent with the kind, mirroring the single-verdict page
		 * (any updater without a proven terminal refuses the WHOLE page). */
		switch (m->verdict) {
		case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
			/* Exact terminal commit: needs the exact scn, carries no bound. */
			if (!SCN_VALID(m->commit_scn) || SCN_VALID(m->horizon_scn))
				return false;
			break;
		case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
			/* Bound-only commit: needs the horizon, must NOT claim an exact
				 * scn (a stray commit_scn could leak into stamp/cache paths). */
			if (SCN_VALID(m->commit_scn) || !SCN_VALID(m->horizon_scn))
				return false;
			break;
		case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
			/* Terminal abort carries no scn of any kind. */
			if (SCN_VALID(m->commit_scn) || SCN_VALID(m->horizon_scn))
				return false;
			break;
		default:
			return false; /* unknown kind on an updater: refuse, never guess */
		}
	}

	return true;
}
