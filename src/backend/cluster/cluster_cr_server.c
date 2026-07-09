/*-------------------------------------------------------------------------
 *
 * cluster_cr_server.c
 *	  pgrac spec-6.12b — CR-server runtime: the LMON↔LMS slot handoff and
 *	  the LMON-side result shipping for cross-instance CR construction.
 *
 *	  Split of labour (see cluster_cr_server.h banner):
 *	    LMON (IC dispatch)   validates + parks the CR request in a shmem
 *	                         slot (cluster_lms_cr_submit) — light work
 *	                         only, the dispatch loop never walks undo.
 *	    LMS  (aux process)   drains PENDING slots and constructs the CR
 *	                         page (cluster_lms_cr_drain →
 *	                         cluster_cr_construct_page_for_server); every
 *	                         construction error becomes a DENIED result,
 *	                         never an LMS exit (fail-closed at the
 *	                         requester, Rule 8.A).
 *	    LMON (tick)          ships READY results direct to the requester
 *	                         (cluster_lms_cr_ship_ready) — the 72-byte
 *	                         outbound ring cannot carry a page and only
 *	                         LMON owns the IC connections.
 *
 *	  The slot state word is an atomic; each transition has exactly one
 *	  writer (LMON: FREE→PENDING, READY→FREE; LMS: PENDING→BUSY→READY),
 *	  so no lock is needed beyond the CAS on FREE→PENDING.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_server.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/multixact.h" /* GetMultiXactIdMembers / MultiXactMember (spec-7.1 D3-b) */
#include "access/transam.h"	  /* TransactionIdDidCommit/DidAbort (D-i4 CLOG cross) */
#include "access/xlog.h"	  /* GetFlushRecPtr (spec-6.12i live_hwm_lsn) */
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_elog.h" /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h" /* PGRAC: spec-7.2 D1 READY-publish wakeup */
#include "cluster/cluster_scn.h"  /* cluster_scn_current (spec-7.1a authority_scn co-sample) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_durable.h"		 /* resolve_by_xid (D-i4 complete scan) */
#include "cluster/cluster_tt_slot.h"		 /* max_recycle_horizon (D-i4 bound) */
#include "cluster/cluster_undo_record_api.h" /* tt_retention_rollover_count */
#include "cluster/cluster_mxid_stripe.h"	 /* cluster_mxid_is_mine (spec-7.1 D3-b) */
#include "cluster/cluster_undo_smgr.h"		 /* cluster_undo_smgr_read_block */
#include "cluster/cluster_xid_stripe.h"		 /* cluster_xid_is_mine (spec-6.15 D4) */
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/*
 * Shmem: the slot table + the published LMS latch pointer (set by LmsMain
 * at entry so the LMON submit path can cut the 100 ms idle-poll latency;
 * a stale pointer after an LMS crash only risks a spurious latch set on a
 * reused PGPROC — benign).
 */
typedef struct ClusterCrServerShared {
	pg_atomic_uint64 lms_latch_ptr; /* (uintptr_t) Latch*; 0 = not running */
	ClusterLmsCrSlot slots[CLUSTER_LMS_CR_SLOTS];
} ClusterCrServerShared;

static ClusterCrServerShared *CrServerShared = NULL;

static Size
cluster_cr_server_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCrServerShared));
}

static void
cluster_cr_server_shmem_init(void)
{
	bool found;

	CrServerShared
		= ShmemInitStruct("pgrac cluster cr server", cluster_cr_server_shmem_size(), &found);

	if (!found) {
		memset(CrServerShared, 0, sizeof(*CrServerShared));
		pg_atomic_init_u64(&CrServerShared->lms_latch_ptr, 0);
		for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
			pg_atomic_init_u32(&CrServerShared->slots[i].state, CLUSTER_LMS_CR_FREE);
	}
}

static const ClusterShmemRegion cluster_cr_server_region = {
	.name = "pgrac cluster cr server",
	.size_fn = cluster_cr_server_shmem_size,
	.init_fn = cluster_cr_server_shmem_init,
	.lwlock_count = 0, /* atomic slot states; no lock */
	.owner_subsys = "cluster_cr_server",
	.reserved_flags = 0,
};

void
cluster_cr_server_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_server_region);
}

/* LmsMain lifecycle hooks: publish / retract the LMS wake latch. */
void
cluster_cr_server_publish_lms_latch(struct Latch *latch)
{
	if (CrServerShared != NULL)
		pg_atomic_write_u64(&CrServerShared->lms_latch_ptr, (uint64)(uintptr_t)latch);
}

static void
cr_server_wake_lms(void)
{
	uint64 raw;

	if (CrServerShared == NULL)
		return;
	raw = pg_atomic_read_u64(&CrServerShared->lms_latch_ptr);
	if (raw != 0)
		SetLatch((Latch *)(uintptr_t)raw);
}

/*
 * cluster_lms_cr_submit — LMON dispatch side.  Park a CR request.
 *
 *	The caller (the GCS_BLOCK_FORWARD handler) has already range-checked
 *	the transition id and knows the payload carries the CR flag.  false =
 *	data plane off / no capacity: the caller replies the fail-closed
 *	DENIED immediately (the requester keeps 53R9G — Rule 8.A).
 */
bool
cluster_lms_cr_submit(const GcsBlockForwardPayload *fwd)
{
	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_cr_data_plane)
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_FREE;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_CR;

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * cluster_lms_undo_fetch_submit — LMON dispatch side (spec-6.12i D-i1).
 *
 *	Park an undo-TT fetch request.  The caller (the GCS_BLOCK_FORWARD
 *	handler) branches on the undo-fetch flag BEFORE any GRD / holder logic
 *	can interpret the synthetic tag.  false = wave GUC off on this node /
 *	malformed synthetic tag / no capacity: the caller replies the
 *	fail-closed DENIED immediately (the requester keeps 53R97 — Rule 8.A).
 */
bool
cluster_lms_undo_fetch_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_FREE;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = InvalidScn; /* no snapshot semantics on this kind */
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_FETCH;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		slot->undo_xid = InvalidTransactionId;
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * cluster_lms_undo_verdict_submit — LMON dispatch side (spec-6.12i D-i4 /
 * spec-6.15 D4).
 *
 *	Park a complete-scan verdict request.  The asked-for xid rides the
 *	widened watermark carrier: any non-zero upper 32 bits or a non-normal
 *	32-bit value is a malformed carrier — refuse (the caller replies the
 *	fail-closed DENIED; the requester keeps 53R97, Rule 8.A).  The synthetic
 *	tag is validated for shape only; the verdict scan is complete over ALL
 *	self-owned segments, so the tag's segment does not scope the answer.
 */
bool
cluster_lms_undo_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	uint64 carrier;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	carrier = (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
	if (carrier > (uint64)PG_UINT32_MAX || !TransactionIdIsNormal((TransactionId)carrier))
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_FREE;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = InvalidScn; /* the carrier held the xid, not a snapshot */
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		slot->undo_xid = (TransactionId)carrier;
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * spec-7.1 D3-b (Q-D3b1): a MultiXactId is the same 32-bit width as a
 * TransactionId, so the multi-verdict request carries the asked-for MXID in
 * the SAME slot->undo_xid field the single-xid verdict uses -- disambiguated
 * purely by slot->req_kind (KIND_UNDO_MULTI_VERDICT).  Assert the width so a
 * future MultiXactId widening cannot silently truncate the carrier.
 */
StaticAssertDecl(sizeof(MultiXactId) == sizeof(TransactionId),
				 "spec-7.1 D3-b reuses the undo_xid carrier width for MultiXactId (Q-D3b1)");

/*
 * cluster_lms_undo_multi_verdict_submit — LMON dispatch side (spec-7.1 D3-b).
 *
 *	Park a validated multi member-verdict request.  Same shape as the single
 *	verdict submit, but the widened watermark carrier holds a MultiXactId
 *	(not a TransactionId): a non-zero upper 32 bits or an invalid mxid is a
 *	malformed carrier — refuse (the caller replies the fail-closed DENIED; the
 *	requester keeps 53R97, Rule 8.A).  The synthetic tag is validated for
 *	shape only; the member scan is complete over the multi's own pg_multixact.
 */
bool
cluster_lms_undo_multi_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	uint64 carrier;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	carrier = (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
	if (carrier > (uint64)PG_UINT32_MAX || !MultiXactIdIsValid((MultiXactId)carrier))
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_FREE;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = InvalidScn; /* the carrier held the mxid, not a snapshot */
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		slot->undo_xid = (TransactionId)carrier; /* Q-D3b1: carries the MXID */
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * lms_undo_fetch_serve — LMS side of one KIND_UNDO_FETCH slot (spec-6.12i).
 *
 *	Samples the live authority triple FIRST, then reads the block: the
 *	watermark is then conservative relative to the shipped content (every
 *	TT stamp the authority claims coverage for is already in the bytes the
 *	requester receives; a stamp landing between sample and read only makes
 *	the content newer than claimed, which is additive and safe — a stamp
 *	can never be retracted).  live_hwm_lsn = GetFlushRecPtr() is sound as
 *	the "durable AND TT-applied" high-water because the durable TT stamp is
 *	a pre-commit targeted pwrite issued BEFORE the commit record is even
 *	inserted (cluster_tt_durable.h): any commit whose record LSN is at or
 *	below the flush pointer has a peer-readable TT stamp.
 *
 *	Only block 0 (the TT-bearing segment header) is served: undo DATA
 *	blocks can lag their pool image under the spec-3.25 D1b keep-clean
 *	WAL deferral, so shipping them from the file would not be origin-fresh
 *	— refuse fail-closed (feature #119 full undo-block CF is the
 *	downstream forward of this slice).
 *
 *	true = result_page holds the block and slot->undo_auth the co-sampled
 *	triple; false = refuse (caller ships DENIED — requester keeps 53R97).
 */
static bool
lms_undo_fetch_serve(ClusterLmsCrSlot *slot)
{
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (slot->undo_block_no != 0)
		return false;
	if (slot->undo_segment_id == 0 || slot->undo_segment_id > UINT16_MAX)
		return false;

	/* Co-sample the authority triple BEFORE the content read (see above). */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();
	/* PGRAC: spec-7.1a D3 -- co-sample the origin SCN clock with the same
	 * pre-content-read ordering (a stamp landing after the sample only makes
	 * the content newer than claimed; additive and safe). */
	slot->undo_auth.authority_scn = cluster_scn_current();

	/* Serve only SELF-owned undo: the owner derives from this node's own
	 * id, never from the wire (a forged request cannot redirect the read). */
	return cluster_undo_smgr_read_block(slot->undo_segment_id, (uint8)(cluster_node_id + 1),
										slot->undo_block_no, slot->result_page);
}

/*
 * lms_resolve_own_xid_verdict — shared core resolving ONE own xid's terminal
 * verdict over this node's COMPLETE durable TT + CLOG (spec-6.12i D-i4 /
 * spec-6.15 D4 / spec-7.1 D1-serve, D3-b).
 *
 *	Used by BOTH the single-xid verdict serve and the multi member-verdict
 *	serve so the terminal decision lives in exactly one place (no fork).  The
 *	caller must have already gated cluster_xid_is_mine(xid) and co-sampled the
 *	live authority triple (the coverage claim never exceeds the scanned durable
 *	state).  Fills *out_verdict + the scn/wrap fields on a proven terminal and
 *	returns a reason so each caller attributes its OWN census (this core bumps
 *	NO counter):
 *	  RESOLVED_SCN        exact COMMITTED match: CLOG must confirm (C1b — the
 *	                      TT stamp is pre-commit, a stamp without a commit
 *	                      record is in-doubt).  Acceptance-gate PASS ->
 *	                      COMMITTED_EXACT{commit_scn, wrap}; a wrap-suspect scn
 *	                      (spec-7.1a hardening) ships COMMITTED_BELOW_HORIZON{H}
 *	                      over max(scn, gated-recycle horizon) instead of
 *	                      refusing (no gated recycle -> refuse, like zero-match).
 *	  RECYCLED_ZERO_MATCH the slot is provably gone: the spec-3.22 retention
 *	                      origin legs (a)-(d) + the monotonic max recycle
 *	                      horizon sampled AFTER the scan, then CLOG decides:
 *	                        COMMITTED -> COMMITTED_BELOW_HORIZON{H} (recycle was
 *	                        horizon-gated, so the lost commit_scn is <= H);
 *	                        explicit ABORTED -> ABORTED; neither -> refuse.
 *	  XID_MATCH_INVALID_SCN  spec-7.1 D1 serve: our own xid matched but carries
 *	                      no stamped commit_scn (delayed-cleanout window).  8.A
 *	                      positive proof only: ONLY an explicit CLOG abort
 *	                      upgrades to a positive ABORTED (the pure, unit-tested
 *	                      cluster_cr_server_invalid_scn_verdict); a committed-
 *	                      but-unstamped / in-flight / 2PC / crashed-without-abort
 *	                      xid stays fail-closed (we never fabricate a scn).
 *	  anything else       AMBIGUOUS_WRAP / SCAN_UNAVAILABLE -> refuse.
 */
typedef enum LmsOwnXidReason {
	LMS_OWN_XID_PROVEN = 0,		   /* out_* holds a proven terminal */
	LMS_OWN_XID_PROVEN_UPGRADE,	   /* proven ABORTED via the invalid_scn CLOG upgrade */
	LMS_OWN_XID_REFUSE_OTHER,	   /* not-committed / wrap-suspect / retention-fail / ambiguous */
	LMS_OWN_XID_REFUSE_ZERO_MATCH, /* recycled 0-match with no explicit CLOG terminal */
	LMS_OWN_XID_REFUSE_INVALID_SCN /* delayed-cleanout, not provably aborted */
} LmsOwnXidReason;

static LmsOwnXidReason
lms_resolve_own_xid_verdict(TransactionId xid, uint8 *out_verdict, SCN *out_commit_scn,
							SCN *out_horizon_scn, uint16 *out_wrap)
{
	SCN scn = InvalidScn;
	SCN horizon = InvalidScn;
	uint16 wrap = 0;

	*out_verdict = 0;
	*out_commit_scn = InvalidScn;
	*out_horizon_scn = InvalidScn;
	*out_wrap = 0;

	switch (
		cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY, &scn, NULL, NULL, &wrap)) {
	case CLUSTER_TT_DURABLE_RESOLVED_SCN:
		if (!TransactionIdDidCommit(xid))
			return LMS_OWN_XID_REFUSE_OTHER; /* C1b: stamped-then-crashed is in-doubt */
		if (cluster_cr_accept_resolved_scn(scn)) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
			*out_commit_scn = scn;
			*out_wrap = wrap;
			return LMS_OWN_XID_PROVEN;
		}

		/*
		 * PGRAC: spec-7.1a hardening -- wrap-suspect stamped scn (below the
		 * retention horizon), reached routinely now that the requester
		 * finalizes EVERY shipped COMMITTED stamp here instead of concluding
		 * on the fetch fast leg.  The EXACT value cannot be shipped (a
		 * same-valued xid recurrence across TT wrap could own a different
		 * scn), but "committed at/below a FROZEN bound" still can:
		 *   - a LIVE recurrence would be a second by-value match ->
		 *     AMBIGUOUS_WRAP (refused below), so the single live match has
		 *     no live rival;
		 *   - a RECYCLED recurrence's lost scn is at/below the max gated-
		 *     recycle horizon (the zero-match arm's own bound);
		 *   - this slot's own stamped scn bounds itself.
		 * Ship BELOW_HORIZON over the max of both candidates -- the same
		 * frozen, non-clock-chasing consumer contract as the zero-match arm
		 * (never cached; judged against the requester's read_scn, leg (e)).
		 * No gated recycle this incarnation -> the recycled-recurrence
		 * candidate is unboundable -> refuse, exactly like zero-match.
		 * Absorbed into the shared core (spec-7.1 D3-b integration) so BOTH
		 * the single-xid serve and each multi member-verdict resolve through
		 * exactly this bound -- no serve forks on the wrap-suspect leg.
		 */
		if (!cluster_cr_retention_proof_origin_legs(&horizon))
			return LMS_OWN_XID_REFUSE_OTHER;
		horizon = cluster_tt_slot_max_recycle_horizon();
		if (!SCN_VALID(horizon))
			return LMS_OWN_XID_REFUSE_OTHER;
		if (scn_time_cmp(scn, horizon) > 0)
			horizon = scn;
		*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
		*out_horizon_scn = horizon;
		return LMS_OWN_XID_PROVEN;

	case CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH:
		if (!cluster_cr_retention_proof_origin_legs(&horizon))
			return LMS_OWN_XID_REFUSE_OTHER;
		horizon = cluster_tt_slot_max_recycle_horizon();
		if (!SCN_VALID(horizon))
			return LMS_OWN_XID_REFUSE_OTHER;
		if (TransactionIdDidCommit(xid)) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
			*out_horizon_scn = horizon;
			return LMS_OWN_XID_PROVEN;
		}
		if (TransactionIdDidAbort(xid)) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
			return LMS_OWN_XID_PROVEN;
		}
		return LMS_OWN_XID_REFUSE_ZERO_MATCH; /* neither explicit CLOG state -> refuse */

	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
		if (cluster_cr_server_invalid_scn_verdict(TransactionIdDidAbort(xid))
			== CLUSTER_CR_INVALID_SCN_ABORTED) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
			return LMS_OWN_XID_PROVEN_UPGRADE;
		}
		return LMS_OWN_XID_REFUSE_INVALID_SCN;

	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
	default:
		return LMS_OWN_XID_REFUSE_OTHER;
	}
}

/*
 * lms_undo_verdict_serve — LMS side of one KIND_UNDO_VERDICT slot
 * (spec-6.12i D-i4 / spec-6.15 D4).
 *
 *	The complete-scan verdict: the requester's single fetched TT block came
 *	back 0-match, which proves nothing (the xid's slot may live in another
 *	segment) — so the ORIGIN, the one node whose durable TT and CLOG are
 *	authoritative for its own xids, answers over its COMPLETE own state:
 *
 *	  1. spec-6.15 D4 self-check: serve ONLY xids the stripe derivation
 *	     proves OURS (cluster_xid_is_mine; false whenever underivable —
 *	     striping off / below the activation floor / not our congruence
 *	     class).  Above the floor the xid value space is globally unique,
 *	     so a requester that derived the wrong origin, or a pre-striping
 *	     xid whose origin is unknowable, is refused instead of answered
 *	     from the wrong authority (the original 6.12i P0).
 *	  2. co-sample the live authority triple BEFORE the scan (the coverage
 *	     claim then never exceeds the scanned durable state — same
 *	     conservative direction as the fetch serve above).
 *	  3. complete by-xid scan over ALL self-owned durable TT headers
 *	     (cluster_tt_slot_durable_resolve_by_xid, WRAP_ANY):
 *	       RESOLVED_SCN        exact COMMITTED match: CLOG must confirm
 *	                           (C1b — the TT stamp is pre-commit, a stamp
 *	                           without a commit record is in-doubt) and the
 *	                           wrap-suspect acceptance gate must pass ->
 *	                           COMMITTED_EXACT{commit_scn, wrap}.
 *	       RECYCLED_ZERO_MATCH the slot is provably gone: evaluate the
 *	                           spec-3.22 retention origin legs (a)-(d) and
 *	                           sample the horizon AFTER the scan (the
 *	                           monotonicity ordering contract), then let
 *	                           CLOG decide the terminal state:
 *	                             COMMITTED -> COMMITTED_BELOW_HORIZON{H}
 *	                             (recycle was horizon-gated, so the lost
 *	                             commit_scn is provably <= H);
 *	                             explicit ABORTED -> ABORTED.
 *	                           Neither (in-progress / crash-lost with no
 *	                           CLOG abort) -> refuse.
 *	       anything else       XID_MATCH_INVALID_SCN (delayed cleanout:
 *	                           recent, not recycled, scn unknown) /
 *	                           AMBIGUOUS_WRAP / SCAN_UNAVAILABLE -> refuse.
 *
 *	true = result_page holds the ClusterGcsUndoVerdictPage and
 *	slot->undo_auth the co-sampled triple; false = refuse (caller ships
 *	DENIED — requester keeps 53R97).  Runs under the drain's PG_TRY: a CLOG
 *	page truncated under an old xid, an unreadable segment, or any other
 *	throw becomes a refusal, never an LMS exit.
 */
static bool
lms_undo_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoVerdictPage *v = (ClusterGcsUndoVerdictPage *)slot->result_page;
	TransactionId xid = slot->undo_xid;
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!TransactionIdIsNormal(xid)) {
		cluster_vis53r97_note_srv_other();
		return false;
	}

	/* spec-6.15 D4: only answer for provably-own xids (see banner). */
	if (!cluster_xid_is_mine(xid)) {
		cluster_vis53r97_note_srv_other();
		return false;
	}

	/* Co-sample the authority triple BEFORE the scan (see banner). */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();
	/* PGRAC: spec-7.1a D3 -- co-sample the origin SCN clock with the same
	 * pre-content-read ordering (a stamp landing after the sample only makes
	 * the content newer than claimed; additive and safe). */
	slot->undo_auth.authority_scn = cluster_scn_current();

	/* Resolve the terminal via the shared core; attribute the census leg. */
	switch (lms_resolve_own_xid_verdict(xid, &verdict, &commit_scn, &horizon_scn, &wrap)) {
	case LMS_OWN_XID_PROVEN:
		break;
	case LMS_OWN_XID_PROVEN_UPGRADE:
		cluster_vis53r97_note_live_upgrade_hit(); /* spec-7.1 D1 serve upgrade */
		break;
	case LMS_OWN_XID_REFUSE_ZERO_MATCH:
		cluster_vis53r97_note_srv_zero_match();
		return false;
	case LMS_OWN_XID_REFUSE_INVALID_SCN:
		cluster_vis53r97_note_srv_invalid_scn();
		return false;
	case LMS_OWN_XID_REFUSE_OTHER:
	default:
		cluster_vis53r97_note_srv_other();
		return false;
	}

	memset(slot->result_page, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v->xid_echo = (uint64)xid;
	v->verdict = verdict;
	v->commit_scn = commit_scn;
	v->horizon_scn = horizon_scn;
	v->wrap = wrap;
	return true;
}

/*
 * lms_undo_multi_verdict_serve — LMS side of one KIND_UNDO_MULTI_VERDICT slot
 * (spec-7.1 D3-b).
 *
 *	The requester's member overlay structurally missed a FOREIGN multixact
 *	xmax (the updater had no compose-time TT binding, IN-12), so THIS node —
 *	the sole owner of the multi's pg_multixact members — answers over its own
 *	state:
 *	  1. serve ONLY multis the stripe derivation proves ours
 *	     (cluster_mxid_is_mine; underivable / foreign -> refuse, mirroring the
 *	     single-xid D4 self-check);
 *	  2. co-sample the live authority triple BEFORE enumerating (same
 *	     conservative ordering as the single serve);
 *	  3. GetMultiXactIdMembers over our own pg_multixact; a set < 2 or over the
 *	     wire capacity is refused (never truncate a member set, 8.A);
 *	  4. for each UPDATER member (status 4-5) resolve its terminal via the
 *	     shared lms_resolve_own_xid_verdict; lock-only members (A2) record
 *	     {xid, status} only.  A member xid that is not provably ours (a foreign
 *	     xid that locked/updated our row) is not resolvable from our TT/CLOG,
 *	     so any unprovable UPDATER makes the WHOLE multi UNPROVABLE -> refuse
 *	     (the requester keeps 53R97; the residue is the feature #119 forward).
 *
 *	true = result_page holds a SERVED ClusterGcsUndoMultiVerdictPage; false =
 *	refuse (caller ships DENIED).  Runs under the drain's PG_TRY: a truncated
 *	pg_multixact / CLOG page or any throw becomes a refusal, never an LMS exit.
 */
static bool
lms_undo_multi_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoMultiVerdictPage *v = (ClusterGcsUndoMultiVerdictPage *)slot->result_page;
	MultiXactId mxid = (MultiXactId)slot->undo_xid; /* Q-D3b1: carrier holds the mxid */
	MultiXactMember *members = NULL;
	int nmembers;
	int i;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!MultiXactIdIsValid(mxid))
		return false;
	/* spec-7.1 D3-b: only answer for provably-own multis (see banner). */
	if (!cluster_mxid_is_mine(mxid))
		return false;

	/* Co-sample the authority triple BEFORE enumerating the members. */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();
	slot->undo_auth.authority_scn = cluster_scn_current();

	nmembers = GetMultiXactIdMembers(mxid, &members, false, false);
	if (nmembers < 2 || members == NULL) {
		if (members != NULL)
			pfree(members);
		return false; /* < 2 members: not a real multi / unreadable set */
	}
	if (nmembers > CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS) {
		pfree(members);
		return false; /* over wire capacity -> refuse (never truncate) */
	}

	memset(slot->result_page, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION;
	v->mxid_echo = (uint64)mxid;
	v->nmembers = (uint16)nmembers;

	for (i = 0; i < nmembers; i++) {
		ClusterGcsUndoMultiVerdictMember *out = &v->members[i];
		TransactionId member_xid = members[i].xid;
		uint8 status = (uint8)members[i].status;

		out->xid = member_xid;
		out->member_status = status;

		/* Lock-only members never gate visibility (A2): {xid, status} only —
		 * no terminal needed even for a foreign lock-only member. */
		if (status <= (uint8)MultiXactStatusForUpdate)
			continue;

		/* Updater member (4-5): its terminal decides tuple visibility, so it
		 * must be provably ours.  A foreign / underivable updater xid or an
		 * unprovable terminal makes the WHOLE multi UNPROVABLE (8.A). */
		if (!TransactionIdIsNormal(member_xid) || !cluster_xid_is_mine(member_xid)) {
			pfree(members);
			return false;
		}
		switch (lms_resolve_own_xid_verdict(member_xid, &out->verdict, &out->commit_scn,
											&out->horizon_scn, &out->wrap)) {
		case LMS_OWN_XID_PROVEN:
			break;
		case LMS_OWN_XID_PROVEN_UPGRADE:
			cluster_vis53r97_note_live_upgrade_hit(); /* spec-7.1 D1 serve upgrade (multi member) */
			break;
		default:
			pfree(members);
			return false; /* an unprovable updater -> whole multi UNPROVABLE */
		}
	}

	pfree(members);
	v->status = (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED;
	return true;
}

/*
 * cluster_lms_cr_drain — LMS main-loop side.  Construct every PENDING slot.
 *
 *	The current block must be resident here (this node's undo holds the
 *	newest chains, so it was — or recently was — the writer); a stable copy
 *	is taken with the raw-pin ship helper.  Every failure (not resident,
 *	interleaved homes, snapshot-too-old, corruption, injection) becomes a
 *	DENIED result: the requester keeps its unchanged 53R9G fail-closed and
 *	LMS itself NEVER exits over a serve (PG_TRY + FlushErrorState).
 */
void
cluster_lms_cr_drain(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_PENDING;
		PGAlignedBlock cur_copy;
		XLogRecPtr page_lsn = InvalidXLogRecPtr;
		bool partial = false;
		bool constructed = false;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_BUSY))
			continue;
		pg_read_barrier(); /* pair with the submit-side publish barrier */

		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;

		/*
		 * spec-6.12i D-i1 / D-i4 — undo-TT fetch + undo-verdict kinds.  No
		 * construction: read the self-owned TT header block (FETCH), or run
		 * the complete own-TT by-xid scan + CLOG cross-check (VERDICT); both
		 * co-sample the live authority triple.  Every refusal (GUC raced
		 * off, non-header block, bad segment, non-own xid, unprovable
		 * outcome, read failure, injection) keeps the DENIED status — the
		 * requester keeps its unchanged 53R97 fail-closed (Rule 8.A).  The
		 * injection point is shared by all three kinds: it models "the
		 * origin's undo serve plane is down", which refuses fetches, single
		 * verdicts and multi-member verdicts (spec-7.1 D3-b) alike.
		 */
		if (slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_FETCH
			|| slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT
			|| slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT) {
			bool served = false;
			uint8 result_status;
			ClusterCrServerStat served_stat;
			ClusterCrServerStat denied_stat;

			switch (slot->req_kind) {
			case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:
				result_status = (uint8)GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT;
				served_stat = CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_SERVED;
				denied_stat = CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_DENIED;
				break;
			case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT:
				result_status = (uint8)GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT;
				served_stat = CLUSTER_CR_SERVER_STAT_VERDICT_SERVED;
				denied_stat = CLUSTER_CR_SERVER_STAT_VERDICT_DENIED;
				break;
			default: /* CLUSTER_LMS_SLOT_KIND_UNDO_FETCH */
				result_status = (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT;
				served_stat = CLUSTER_CR_SERVER_STAT_UNDO_SERVED;
				denied_stat = CLUSTER_CR_SERVER_STAT_UNDO_DENIED;
				break;
			}

			CLUSTER_INJECTION_POINT("cluster-lms-undo-fetch");
			if (!cluster_injection_should_skip("cluster-lms-undo-fetch")) {
				PG_TRY();
				{
					switch (slot->req_kind) {
					case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:
						served = lms_undo_multi_verdict_serve(slot);
						break;
					case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT:
						served = lms_undo_verdict_serve(slot);
						break;
					default:
						served = lms_undo_fetch_serve(slot);
						break;
					}
				}
				PG_CATCH();
				{
					/* Fail-closed serve; keep LMS alive (as the CR kind). */
					served = false;
					MemoryContextSwitchTo(TopMemoryContext);
					FlushErrorState();
				}
				PG_END_TRY();
			}

			if (served) {
				slot->result_status = result_status;
				cluster_cr_server_stat_bump(served_stat);
			} else {
				cluster_cr_server_stat_bump(denied_stat);
			}

			pg_write_barrier();
			pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_READY);
			/* PGRAC: spec-7.2 D1 -- wake the shipper (LMON) right away;
			 * without this the READY result sat until LMON's next natural
			 * wakeup (typ. 100-250ms, worst case one heartbeat).
			 * Publish-before-signal: READY store above precedes the kick. */
			cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_SHIP_READY);
			cluster_lmon_wakeup();
			continue;
		}

		/* spec-6.12b injection — force the DENIED serve path. */
		CLUSTER_INJECTION_POINT("cluster-lms-cr-construct");
		if (!cluster_injection_should_skip("cluster-lms-cr-construct")
			&& cluster_crossnode_cr_data_plane
			&& cluster_bufmgr_copy_block_for_gcs(slot->tag, &page_lsn, cur_copy.data)) {
			PG_TRY();
			{
				cluster_cr_construct_page_for_server(cur_copy.data, slot->read_scn, slot->tag,
													 slot->result_page, &partial);
				constructed = true;
			}
			PG_CATCH();
			{
				/*
				 * Fail-closed serve: keep the DENIED status and keep LMS
				 * alive.  The taxonomy counters were already bumped by the
				 * construction wrapper; drop the error state entirely (an
				 * aux-process ERROR would otherwise escalate to exit).
				 */
				constructed = false;
				MemoryContextSwitchTo(TopMemoryContext);
				FlushErrorState();
			}
			PG_END_TRY();
		}

		if (constructed) {
			slot->result_status = (uint8)(partial ? GCS_BLOCK_REPLY_CR_RESULT_PARTIAL
												  : GCS_BLOCK_REPLY_CR_RESULT_FULL);
			cluster_cr_server_stat_bump(partial ? CLUSTER_CR_SERVER_STAT_PARTIAL
												: CLUSTER_CR_SERVER_STAT_FULL);
		} else {
			cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_DENIED);
		}

		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_READY);
		/* PGRAC: spec-7.2 D1 -- wake the shipper (see the undo/verdict
		 * READY publish above for the rationale). */
		cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_SHIP_READY);
		cluster_lmon_wakeup();
	}
}

/*
 * cluster_lms_cr_ship_ready — LMON tick side.  Ship READY results.
 *
 *	Builds the standard GCS_BLOCK_REPLY (header + BLCKSZ page) with the
 *	HC109 forwarding-master echo the requester's HC108 chain expects, and
 *	frees the slot.  A DENIED result ships a zero page under a matching
 *	checksum (the requester never consumes DENIED bytes).
 */
void
cluster_lms_cr_ship_ready(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_READY;
		uint32 header_len;
		uint32 total;
		char *buf;
		GcsBlockReplyHeader *hdr;

		if (pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_READY)
			continue;
		(void)expected; /* single shipper (LMON tick); state re-checked above */
		pg_read_barrier();

		header_len = (uint32)sizeof(GcsBlockReplyHeader);
		total = header_len + GCS_BLOCK_DATA_SIZE;
		/* spec-6.12i: a served undo-TT fetch / undo verdict appends the
		 * authority trailer (tt_generation); DENIED undo replies stay
		 * v1-sized. */
		if (GcsBlockReplyStatusCarriesUndoAuthTrailer((GcsBlockReplyStatus)slot->result_status))
			total += (uint32)sizeof(ClusterGcsUndoAuthTrailer);
		buf = (char *)palloc0(total);
		hdr = (GcsBlockReplyHeader *)buf;
		hdr->request_id = slot->request_id;
		hdr->page_lsn = 0;
		hdr->epoch = cluster_epoch_get_current();
		hdr->sender_node = cluster_node_id;
		hdr->requester_backend_id = slot->requester_backend;
		hdr->transition_id = slot->transition_id;
		hdr->status = slot->result_status;
		GcsBlockReplyHeaderSetForwardingMasterNode(hdr, slot->reply_master_node);

		if (hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_FULL
			|| hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL)
			memcpy(buf + header_len, slot->result_page, BLCKSZ);
		else if (GcsBlockReplyStatusCarriesUndoAuthTrailer((GcsBlockReplyStatus)hdr->status)) {
			ClusterGcsUndoAuthTrailer *trailer
				= (ClusterGcsUndoAuthTrailer *)(buf + header_len + GCS_BLOCK_DATA_SIZE);

			/*
			 * spec-6.12i co-sample carriage: the authority the LMS sampled
			 * ATOMICALLY with the content read overrides the ship-time
			 * header fields — epoch carries the sampled origin epoch (the
			 * HC100 `hdr.epoch >= request_epoch` check then drops a
			 * mid-reconfig reply, which IS the D-i3 fail-closed) and
			 * page_lsn carries live_hwm_lsn.
			 */
			hdr->epoch = slot->undo_auth.origin_epoch;
			hdr->page_lsn = slot->undo_auth.live_hwm_lsn;
			memcpy(buf + header_len, slot->result_page, BLCKSZ);
			ClusterGcsUndoAuthTrailerSetTtGeneration(trailer, slot->undo_auth.tt_generation);
			ClusterGcsUndoAuthTrailerSetAuthorityScn(trailer,
													 (uint64)slot->undo_auth.authority_scn);
		}
		hdr->checksum = cluster_gcs_block_compute_checksum(buf + header_len);

		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, slot->requester_node, buf,
									   total);
		pfree(buf);

		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_FREE);
	}
}

#endif /* USE_PGRAC_CLUSTER */
