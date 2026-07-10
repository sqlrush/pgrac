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

#include "cluster/cluster_gcs_block.h" /* ClusterGcsUndoVerdictPage (CP5) */
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_undo_segment.h" /* UndoSegmentHeaderData, TTSlot */
#include "cluster/cluster_undo_verdict.h" /* D3 verdict taxonomy + mappers */

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
 * cluster_undo_verdict_from_wire_page
 *
 * D3-1 pure mapper: a CP5 origin verdict page -> the five-value taxonomy.
 * Fail-closed by construction: a page that does not structurally answer
 * asked_xid (cluster_vis_undo_verdict_page_usable) yields UNKNOWN_FAIL_CLOSED,
 * so the caller keeps the 53R97 boundary (Rule 8.A).  A BELOW_HORIZON page
 * maps to COMMITTED_BOUND unconditionally -- the read_scn admissibility of
 * that bound (requester leg (e)) needs scn_time_cmp and is applied by the
 * D3-3 consumer, deliberately outside this dependency-free policy object (see
 * cluster_vis_undo_verdict_page_usable NOTES).  Pure: no I/O, no elog.
 */
ClusterUndoVerdictResult
cluster_undo_verdict_from_wire_page(const struct ClusterGcsUndoVerdictPage *v,
									TransactionId asked_xid)
{
	ClusterUndoVerdictResult r
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };

	/* Structural gate first: an unusable page is not evidence about the xid. */
	if (!cluster_vis_undo_verdict_page_usable(v, asked_xid))
		return r;

	switch (v->verdict) {
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
		r.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		r.commit_scn = (SCN)v->commit_scn;
		r.wrap = v->wrap;
		return r;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
		/* Bound-only commit: kind == BOUND is itself the "never stamp the
			 * scn as exact" marker.  The read_scn gate is the consumer's. */
		r.kind = CLUSTER_UNDO_VERDICT_COMMITTED_BOUND;
		r.commit_scn = (SCN)v->horizon_scn;
		return r;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		r.kind = CLUSTER_UNDO_VERDICT_ABORTED;
		return r;

	default:
		/* page_usable() already fenced the kind range; defense in depth. */
		return r;
	}
}

/*
 * cluster_vis_undo_authority_verdict_page_usable
 *
 * spec-5.22d D4-6 structural validation of an AUTHORITY-served verdict page
 * (version 2 provenance).  Same field discipline as the v1 gate above with
 * two deliberate tightenings: ONLY version 2 is evidence (the v1 gate's
 * strict ==1 refuses version 2 and this gate refuses version 1, so neither
 * provenance can masquerade as the other), and COMMITTED_BELOW_HORIZON is
 * refused outright — the authority block0 prove has no horizon leg, so a
 * bound-carrying page is malformed by construction.  Pure: no I/O, no elog.
 */
bool
cluster_vis_undo_authority_verdict_page_usable(const struct ClusterGcsUndoVerdictPage *v,
											   TransactionId asked_xid)
{
	if (v == NULL || !TransactionIdIsNormal(asked_xid))
		return false;

	if (v->magic != CLUSTER_GCS_UNDO_VERDICT_MAGIC
		|| v->version != CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY)
		return false;

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
		return SCN_VALID(v->commit_scn) && !SCN_VALID(v->horizon_scn);
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		return !SCN_VALID(v->commit_scn) && !SCN_VALID(v->horizon_scn);
	default:
		/* incl. BELOW_HORIZON: not producible by the block0 prove — refuse */
		return false;
	}
}

/*
 * cluster_vis_undo_authority_reply_binding_ok
 *
 * spec-5.22d D4-6 (8.A amend): an authority reply is evidence ONLY under the
 * full binding — the reply sender IS the elected authority and the reply
 * epoch IS the stamped request epoch, EXACTLY.  The transport's HC100 check
 * admits hdr.epoch >= request_epoch (a general stale-reply pre-filter); on
 * the authority leg a newer epoch means the election itself may have moved,
 * so >= is deliberately not enough.  Pure.
 */
bool
cluster_vis_undo_authority_reply_binding_ok(int32 sender_node, int32 authority_node,
											uint64 reply_epoch, uint64 stamped_epoch)
{
	if (authority_node < 0 || sender_node < 0)
		return false;
	if (sender_node != authority_node)
		return false;
	return reply_epoch == stamped_epoch;
}

/*
 * cluster_undo_authority_verdict_page_fill
 *
 * spec-5.22d D4-6 serve-side wire-page fill: one prove result -> one version
 * 2 page, the exact inverse of the requester's authority gate above so the
 * two wire ends can never disagree about what an authority page carries.
 * Refuses every kind the block0 prove cannot legitimately produce (UNKNOWN /
 * BOUND / IN_PROGRESS — the caller ships DENIED and the requester keeps the
 * 53R97 fail-closed, Rule 8.A).  Zeroes the whole struct first, so a poison
 * or stale byte can never leak onto the wire.  Pure.
 */
bool
cluster_undo_authority_verdict_page_fill(struct ClusterGcsUndoVerdictPage *v, TransactionId xid,
										 const ClusterUndoVerdictResult *r)
{
	if (v == NULL || r == NULL || !TransactionIdIsNormal(xid))
		return false;

	switch (r->kind) {
	case (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT:
		if (!SCN_VALID(r->commit_scn))
			return false; /* an EXACT claim without a scn proves nothing */
		break;
	case (uint8)CLUSTER_UNDO_VERDICT_ABORTED:
		break;
	default:
		return false;
	}

	memset(v, 0, sizeof(*v));
	v->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY;
	v->xid_echo = (uint64)xid;
	v->commit_scn = InvalidScn;
	v->horizon_scn = InvalidScn;
	v->wrap = r->wrap;
	if (r->kind == (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT) {
		v->verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
		v->commit_scn = (uint64)r->commit_scn;
	} else
		v->verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
	return true;
}

/*
 * cluster_undo_verdict_from_authority_wire_page
 *
 * spec-5.22d D4-6 requester-side mapper: a version-2 AUTHORITY-served page
 * -> the taxonomy.  Fail-closed by construction: a page that does not pass
 * the authority structural gate (wrong provenance/version, smuggled bound,
 * echo mismatch) yields UNKNOWN_FAIL_CLOSED so the caller keeps the 53R97
 * boundary (Rule 8.A).  Pure: no I/O, no elog.
 */
ClusterUndoVerdictResult
cluster_undo_verdict_from_authority_wire_page(const struct ClusterGcsUndoVerdictPage *v,
											  TransactionId asked_xid)
{
	ClusterUndoVerdictResult r
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };

	if (!cluster_vis_undo_authority_verdict_page_usable(v, asked_xid))
		return r;

	switch (v->verdict) {
	case (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
		r.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		r.commit_scn = (SCN)v->commit_scn;
		r.wrap = v->wrap;
		return r;

	case (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		r.kind = CLUSTER_UNDO_VERDICT_ABORTED;
		return r;

	default:
		/* the authority gate already fenced the kind range; defense in depth */
		return r;
	}
}

/*
 * cluster_undo_verdict_from_block_proof
 *
 * D3-1 pure mapper: a CP3 single-block positive proof (exact xid+wrap slot
 * match on the shipped block0 bytes) -> the taxonomy.  COMMITTED carries the
 * true commit SCN (EXACT); ABORTED is terminal; NONE is UNKNOWN_FAIL_CLOSED,
 * which the orchestrator reads as "fall to the CP5 origin verdict", never as
 * a terminal answer to the caller (Rule 8.A).  Pure.
 */
ClusterUndoVerdictResult
cluster_undo_verdict_from_block_proof(ClusterVisTtProof proof, SCN commit_scn, uint16 wrap)
{
	ClusterUndoVerdictResult r
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };

	switch (proof) {
	case CLUSTER_VIS_TT_PROOF_COMMITTED:
		r.kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		r.commit_scn = commit_scn;
		r.wrap = wrap;
		return r;

	case CLUSTER_VIS_TT_PROOF_ABORTED:
		r.kind = CLUSTER_UNDO_VERDICT_ABORTED;
		return r;

	case CLUSTER_VIS_TT_PROOF_NONE:
	default:
		return r;
	}
}

/*
 * cluster_undo_verdict_from_resolve
 *
 * D3-3 Amendment-2 folding point: collapse the runtime resolver's
 * (ok, committed, commit_scn, is_bound) outcome into the taxonomy so the
 * legacy is_bound boolean never escapes to a consumer.  is_bound maps to
 * COMMITTED_BOUND (never COMMITTED_EXACT), so a switch that only handles
 * EXACT falls to fail-closed on a bound -- a horizon bound is never stamped
 * or cached as an exact commit SCN.  Pure.
 */
ClusterUndoVerdictResult
cluster_undo_verdict_from_resolve(bool ok, bool committed, SCN commit_scn, bool is_bound)
{
	ClusterUndoVerdictResult r
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };

	if (!ok)
		return r; /* fetch / covers / deny miss -> fail-closed */
	if (!committed) {
		r.kind = CLUSTER_UNDO_VERDICT_ABORTED;
		return r;
	}
	r.kind = is_bound ? CLUSTER_UNDO_VERDICT_COMMITTED_BOUND : CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	r.commit_scn = commit_scn;
	return r;
}
