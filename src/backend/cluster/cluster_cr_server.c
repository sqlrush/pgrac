/*-------------------------------------------------------------------------
 *
 * cluster_cr_server.c
 *	  pgrac spec-6.12b / spec-7.3 D6 — cross-instance CR-server runtime.
 *
 *	  spec-6.12b split the origin serve across LMON (validate + park in a
 *	  shmem slot) → LMS (drain + construct) → LMON (tick-ship), because the
 *	  IC dispatch loop could not walk undo I/O, the 72-byte outbound ring
 *	  could not carry a page, and only LMON owned the IC connections.
 *
 *	  spec-7.3 D6: when the GCS block family is on the DATA plane (the LMS
 *	  worker pool owns the DATA connections + a page-carrying outbound ring),
 *	  the worker[shard] that receives a GCS_BLOCK_FORWARD serves the request
 *	  INLINE (cluster_gcs_block_forward_serve_inline) — no park → poll → ship
 *	  indirection, no worker-0 handoff, no 100 ms idle latency; a slow
 *	  construction only stalls that worker's shard (1/N).  The park-serve
 *	  path is RETAINED for the CONTROL-plane fallback: a node whose data
 *	  plane is off (no data_addr) still dispatches the FORWARD in LMON, and
 *	  LMON must not walk undo I/O in its tight IC loop — so it parks, LMS
 *	  worker 0 drains, and LMON ships (the light-work rule, unchanged).
 *
 *	  Both paths share cr_serve_slot() (the PG_TRY → DENIED serve envelope)
 *	  and cr_build_and_send_reply() (the reply build).  8.A envelope: CR
 *	  construction (cluster_cr.c) re-throws on any uncertainty; the wrapper
 *	  converts it to a fail-closed DENIED, never a worker/LMS exit or a
 *	  wrong-order construction.
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
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b/i)
 *	  Spec: spec-7.3-lms-worker-pool.md (D6 — inline serve on the DATA plane;
 *	  D7 — fence ×N: the inline serve refuses to ship on a write-fenced node)
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
#include "cluster/cluster_gcs_block_dedup.h" /* PGRAC: spec-7.3 P2-1 — note_misroute */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope */
#include "cluster/cluster_ic_tier1.h"  /* PGRAC: spec-7.3 P2-1 — my DATA channel */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h"		 /* PGRAC: spec-7.2 D1 READY-publish wakeup */
#include "cluster/cluster_lms.h"		 /* PGRAC: spec-7.3 D8 per-worker serve counters */
#include "cluster/cluster_lms_shard.h"	 /* PGRAC: spec-7.3 P2-1 — tag->worker shard */
#include "cluster/cluster_mxid_stripe.h" /* cluster_mxid_is_mine (spec-7.1 D3-b) */
#include "cluster/cluster_scn.h" /* cluster_scn_current (spec-7.1a authority_scn co-sample) */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_conf.h"			 /* CLUSTER_MAX_NODES (D4-6 owner range) */
#include "cluster/cluster_tt_durable.h"		 /* resolve_by_xid (D-i4 complete scan) */
#include "cluster/cluster_tt_slot.h"		 /* max_recycle_horizon (D-i4 bound) */
#include "cluster/cluster_undo_authority.h"	 /* authority lookup + block0 prove (D4-6) */
#include "cluster/cluster_undo_record_api.h" /* tt_retention_rollover_count */
#include "cluster/cluster_undo_smgr.h"		 /* cluster_undo_smgr_read_block */
#include "cluster/cluster_write_fence.h"	 /* PGRAC: spec-7.3 D7 fence ×N gate */
#include "cluster/cluster_xid_stripe.h"		 /* cluster_xid_is_mine (spec-6.15 D4) */
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h" /* PGRAC: spec-7.3 D8 serve duration (GetCurrentTimestamp) */

/*
 * Shmem: the slot table + the published LMS latch pointer (set by LmsMain
 * at entry so the LMON submit path can cut the 100 ms idle-poll latency;
 * a stale pointer after an LMS crash only risks a spurious latch set on a
 * reused PGPROC — benign).  Used by the CONTROL-plane park-serve path only
 * (spec-7.3 D6: the DATA plane serves inline and never parks).
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
 * cluster_lms_cr_submit — CONTROL-plane park.  The caller (the GCS_BLOCK_
 * FORWARD handler running in LMON when the family is on the control plane)
 * has already range-checked the transition id and knows the payload carries
 * the CR flag.  false = data plane off / no capacity: the caller replies the
 * fail-closed DENIED immediately (the requester keeps 53R9G — Rule 8.A).
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
 * cluster_lms_undo_fetch_submit — CONTROL-plane park (spec-6.12i D-i1).
 *
 *	Park an undo-TT fetch request.  The caller branches on the undo-fetch
 *	flag BEFORE any GRD / holder logic can interpret the synthetic tag.
 *	false = wave GUC off on this node / malformed synthetic tag / no capacity:
 *	the caller replies the fail-closed DENIED immediately (the requester keeps
 *	53R97 — Rule 8.A).
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
		slot->undo_owner = -1; /* D4-6: never inherit a recycled slot's owner */
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
 * cluster_lms_undo_verdict_submit — CONTROL-plane park (spec-6.12i D-i4 /
 * spec-6.15 D4 / spec-5.22d D4-6).
 *
 *	Park a complete-scan verdict request (kinds 2/3) or a kind-4 dead-owner
 *	AUTHORITY verdict request.  The asked-for xid rides the widened
 *	watermark carrier: any non-zero upper 32 bits or a non-normal 32-bit
 *	value is a malformed carrier — refuse (the caller replies the
 *	fail-closed DENIED; the requester keeps 53R97, Rule 8.A).  The synthetic
 *	tag is validated for shape only; on kinds 2/3 the verdict scan is
 *	complete over ALL self-owned segments (the tag's segment does not scope
 *	the answer) and the owner carrier MUST be empty, on kind 4 the dead
 *	OWNER is decoded from tag.relNumber (range-checked, never self — the
 *	live-owner kinds answer own xids) and the serve-side triple check owns
 *	all trust decisions.
 */
bool
cluster_lms_undo_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	int32 wire_owner = -1;
	uint64 carrier;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	if (GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(fwd)) {
		/* kind 4: decode + range-check the owner carrier; a request naming
		 * US as the (dead) owner is malformed — our own xids are answered
		 * by the live-owner kinds, never by an authority detour. */
		if (!GcsBlockUndoAuthorityFetchTagDecodeOwner(fwd->tag, &wire_owner))
			return false;
		if (wire_owner < 0 || wire_owner >= CLUSTER_MAX_NODES || wire_owner == cluster_node_id)
			return false;
	} else if (fwd->tag.relNumber != (RelFileNumber)0)
		return false; /* owner-served kinds must leave the carrier empty */

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
		slot->undo_authoritative = GcsBlockForwardPayloadIsUndoVerdictAuthoritative(fwd);
		slot->undo_owner = wire_owner; /* -1 on kinds 2/3; the dead owner on kind 4 */
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

		/* Reserve producer-only FILLING first (spec-7.1 integration review):
		 * landing directly on PENDING would let the LMS drain acquire the
		 * slot before the request fields below are written. */
		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_FILLING))
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
	return cluster_undo_smgr_read_block(cluster_undo_intent_for_owner((uint8)(cluster_node_id + 1)),
										slot->undo_segment_id, (uint8)(cluster_node_id + 1),
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

	/*
	 * spec-6.15 D4: only answer for provably-own xids (see banner) -- UNLESS
	 * the request is spec-5.22f D6-7 AUTHORITATIVE (origin chosen from the fresh
	 * ref's physical ITL binding, so the requester already proved this is the
	 * correct owner; the underivable own seed xid is answered over the durable-TT
	 * + CLOG authority below, the positive proof unchanged).
	 */
	if (!slot->undo_authoritative && !cluster_xid_is_mine(xid)) {
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

	/*
	 * Zero the full BLCKSZ wire page (the reply checksum covers it all) and
	 * fill the verdict fields from the values the shared core just proved.
	 * The master==self local verdict resolve (D3-4 fill_page) runs the SAME
	 * lms_resolve_own_xid_verdict core, so the served and self answers over
	 * one xid can never diverge (Rule 8.A).
	 */
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
 * lms_undo_authority_verdict_serve — LMS side of one kind-4 dead-owner
 * AUTHORITY verdict slot (spec-5.22d D4-6).
 *
 *	WE are asked to serve a verdict about a DEAD owner's xid from that
 *	owner's durable shared block0 — never from our own TT (the xid is not
 *	ours).  The wire is never trusted: the spec-5.22d §2.4 triple check must
 *	pass before a byte is answered —
 *
 *	  (a) request epoch == our CURRENT epoch (slot->epoch carries fwd.epoch,
 *	      which the requester stamped from its current epoch; any reconfig
 *	      since then invalidates the election the request was routed under);
 *	  (b) re-derive: the elected authority for (owner, current epoch) is
 *	      US (this implicitly re-proves the owner is dead-decided — a live
 *	      or undecided owner never elects an authority);
 *	  (c) the shared block0 prove passes (cluster_undo_authority_block0_
 *	      prove — the SAME core the requester's self-authority leg runs, so
 *	      the wire-served and self answers over one (owner, segment, xid)
 *	      can never diverge, Rule 8.A; it re-checks the epoch axis, reads
 *	      the owner's block0 under the AUTHORITY_BLOCK0 intent and demands
 *	      the exact xid+wrap positive proof, bumping the undo_authority_*
 *	      counters itself).
 *
 *	The reply page carries the version-2 AUTHORITY provenance (an old
 *	requester's strict ==1 gate refuses it).  slot->undo_auth: origin_epoch
 *	is our current epoch — the ship path copies it into hdr.epoch, which the
 *	requester binds strictly == its stamped epoch (8.A amend); live_hwm_lsn
 *	and tt_generation ride as ZERO — they describe an origin's OWN live TT
 *	plane, which does not exist for a dead owner; the prove already
 *	internalized generation/wrap coverage and the requester's authority leg
 *	ignores both.
 *
 *	true = result_page holds the version-2 ClusterGcsUndoVerdictPage;
 *	false = refuse (caller ships DENIED — requester keeps 53R97).  Runs
 *	under the drain's PG_TRY: any throw becomes a refusal, never an LMS
 *	exit.
 */
static bool
lms_undo_authority_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoVerdictPage *v = (ClusterGcsUndoVerdictPage *)slot->result_page;
	TransactionId xid = slot->undo_xid;
	uint64 cur_epoch;
	int32 authority = -1;
	ClusterUndoVerdictResult r;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!TransactionIdIsNormal(xid))
		return false;
	if (slot->undo_owner < 0 || slot->undo_owner == cluster_node_id)
		return false;

	/* (a) the request's epoch must still be OUR current epoch */
	cur_epoch = cluster_epoch_get_current();
	if (slot->epoch != cur_epoch)
		return false;

	/* (b) re-derive: the elected authority for (owner, cur) must be US */
	if (cluster_undo_serve_authority_lookup(slot->undo_owner, cur_epoch, &authority)
			!= CLUSTER_UNDO_AUTHORITY_OK
		|| authority != cluster_node_id)
		return false;

	/* the reply's epoch carrier (see banner); hwm/tt_gen deliberately zero */
	slot->undo_auth.origin_epoch = cur_epoch;
	slot->undo_auth.live_hwm_lsn = 0;
	slot->undo_auth.tt_generation = 0;

	/* (c) shared block0 prove core (the D4-4 self leg's implementation) */
	r = cluster_undo_authority_block0_prove(slot->undo_owner, slot->undo_segment_id, xid,
											cur_epoch);

	memset(slot->result_page, 0, BLCKSZ);
	return cluster_undo_authority_verdict_page_fill(v, xid, &r);
}

/*
 * cluster_lms_undo_verdict_fill_page -- resolve an OWN xid into a verdict page
 * over the COMPLETE own durable TT (cluster_tt_slot_durable_resolve_by_xid)
 * cross-checked against the OWN CLOG (AD-006: CLOG is the committed-ness
 * authority; the TT carries commit_scn).  The caller has already zeroed the
 * page buffer (origin: the full BLCKSZ wire page; D3-4 self: sizeof the
 * struct) and owns any authority co-sampling; this fills only
 * magic/version/xid_echo and the verdict taxonomy fields.  true = *v holds
 * the verdict; false = refuse (in-doubt / ambiguous / not-own / unresolvable
 * -> caller keeps the 53R97 fail-closed boundary, Rule 8.A).  Shared so the
 * origin-served verdict and the master==self local verdict are byte-for-byte
 * the same decision.
 */
bool
cluster_lms_undo_verdict_fill_page(TransactionId xid, bool authoritative,
								   ClusterGcsUndoVerdictPage *v)
{
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;

	if (!TransactionIdIsNormal(xid))
		return false;

	/* spec-6.15 D4: only answer for provably-own xids (see banner) -- UNLESS the
	 * caller is spec-5.22f D6-7 AUTHORITATIVE (physical-binding fresh ref), in
	 * which case the durable-TT + CLOG core below is the authority for an
	 * underivable own xid.  The derived (recycled) and master==self callers pass
	 * false and keep the self-check. */
	if (!authoritative && !cluster_xid_is_mine(xid))
		return false;

	/* One decision core for the served and self answers (Rule 8.A): the
	 * spec-7.1 refactor moved the scan + C1b CLOG cross-check + invalid-scn
	 * abort upgrade into lms_resolve_own_xid_verdict; this wrapper only
	 * shapes the page.  Census attribution is the caller's (the self leg
	 * keeps the rtvis counters; this core bumps none). */
	switch (lms_resolve_own_xid_verdict(xid, &verdict, &commit_scn, &horizon_scn, &wrap)) {
	case LMS_OWN_XID_PROVEN:
	case LMS_OWN_XID_PROVEN_UPGRADE:
		break;
	default:
		return false; /* refuse: caller keeps 53R97 fail-closed */
	}

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
 * cr_serve_slot — serve one populated request carrier.  Shared by the
 * CONTROL-plane park drain and the D6 DATA-plane inline serve.  Sets
 * slot->result_status and fills result_page / undo_auth; every failure
 * (interleaved homes, snapshot-too-old, corruption, non-own xid, read
 * failure, injection, wave GUC off) becomes a DENIED result under the
 * PG_TRY -> DENIED envelope — the worker/LMS NEVER exits over a serve and
 * NEVER constructs out of order (Rule 8.A).  Does not touch the slot state
 * word or ship (callers own those).  Assumes the carrier is already
 * validated + populated (submit / serve_inline decode the synthetic address
 * + carrier before calling).
 */
static void
cr_serve_slot(ClusterLmsCrSlot *slot)
{
	slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;

	/*
	 * spec-6.12i D-i1 / D-i4 — undo-TT fetch + undo-verdict kinds.  No
	 * construction: read the self-owned TT header block (FETCH), or run the
	 * complete own-TT by-xid scan + CLOG cross-check (VERDICT); both co-sample
	 * the live authority triple.  The injection point is shared by both kinds:
	 * it models "the origin's undo serve plane is down", refusing fetches and
	 * verdicts alike.
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
					/* spec-5.22d D4-6: a verdict slot carrying a dead owner
					 * is the kind-4 authority serve (block0 prove), never
					 * the own-TT scan. */
					served = slot->undo_owner >= 0 ? lms_undo_authority_verdict_serve(slot)
												   : lms_undo_verdict_serve(slot);
					break;
				default:
					served = lms_undo_fetch_serve(slot);
					break;
				}
			}
			PG_CATCH();
			{
				/* Fail-closed serve; keep the worker/LMS alive. */
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
		return;
	}

	/* spec-6.12b CR construction. */
	{
		PGAlignedBlock cur_copy;
		XLogRecPtr page_lsn = InvalidXLogRecPtr;
		bool partial = false;
		bool constructed = false;

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
				 * Fail-closed serve: keep the DENIED status and keep the
				 * worker/LMS alive.  The taxonomy counters were already bumped
				 * by the construction wrapper; drop the error state entirely
				 * (an aux-process ERROR would otherwise escalate to exit).
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
	}
}

/*
 * cr_build_and_send_reply — build the standard GCS_BLOCK_REPLY (header +
 * BLCKSZ page, + ClusterGcsUndoAuthTrailer for served undo kinds) with the
 * HC109 forwarding-master echo the requester's HC108 chain expects, and send
 * it to the requester.  A DENIED result ships a zero page under a matching
 * checksum (the requester never consumes DENIED bytes).  Shared by the
 * CONTROL-plane LMON ship and the D6 DATA-plane inline serve.
 */
static void
cr_build_and_send_reply(const ClusterLmsCrSlot *slot)
{
	uint32 header_len = (uint32)sizeof(GcsBlockReplyHeader);
	uint32 total = header_len + GCS_BLOCK_DATA_SIZE;
	char *buf;
	GcsBlockReplyHeader *hdr;

	/* spec-6.12i: a served undo-TT fetch / undo verdict appends the authority
	 * trailer (tt_generation); DENIED undo replies stay v1-sized. */
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
		 * spec-6.12i co-sample carriage: the authority sampled ATOMICALLY
		 * with the content read overrides the ship-time header fields —
		 * epoch carries the sampled origin epoch (the HC100 `hdr.epoch >=
		 * request_epoch` check then drops a mid-reconfig reply, which IS the
		 * D-i3 fail-closed) and page_lsn carries live_hwm_lsn.
		 */
		hdr->epoch = slot->undo_auth.origin_epoch;
		hdr->page_lsn = slot->undo_auth.live_hwm_lsn;
		memcpy(buf + header_len, slot->result_page, BLCKSZ);
		ClusterGcsUndoAuthTrailerSetTtGeneration(trailer, slot->undo_auth.tt_generation);
		ClusterGcsUndoAuthTrailerSetAuthorityScn(trailer, (uint64)slot->undo_auth.authority_scn);
	}
	hdr->checksum = cluster_gcs_block_compute_checksum(buf + header_len);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, slot->requester_node, buf, total);
	pfree(buf);
}

/*
 * cluster_lms_cr_drain — CONTROL-plane park drain (LMS worker 0 main loop).
 * Serve every PENDING slot into a READY result (errors become DENIED; LMS
 * never exits over a serve).  DATA-plane requests are served inline and never
 * reach this table, so on a data-plane node the loop is a no-op.
 */
void
cluster_lms_cr_drain(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_PENDING;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_BUSY))
			continue;
		pg_read_barrier(); /* pair with the submit-side publish barrier */

		cr_serve_slot(slot);

		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_READY);
		/* PGRAC: spec-7.2 D1 -- wake the shipper (LMON) right away; without
		 * this the READY result sat until LMON's next natural wakeup (typ.
		 * 100-250ms).  Publish-before-signal: READY store above precedes the
		 * kick. */
		cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_SHIP_READY);
		cluster_lmon_wakeup();
	}
}

/*
 * cluster_lms_cr_ship_ready — CONTROL-plane ship (LMON tick).  Ship every
 * READY result to its requester and free the slot.  DATA-plane requests are
 * shipped inline (see cluster_gcs_block_forward_serve_inline) and never reach
 * this table.
 */
void
cluster_lms_cr_ship_ready(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];

		if (pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_READY)
			continue;
		pg_read_barrier();

		cr_build_and_send_reply(slot);

		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_FREE);
	}
}

/*
 * spec-7.3 D6 — scratch context for one inline serve.  CR construction and
 * the reply build palloc transient state; the inline serve runs inside the
 * worker's long-lived DATA-dispatch loop, so it must reset its own scratch
 * rather than leak into that context.  Lazily created per worker process.
 */
static MemoryContext CrServeScratchCtx = NULL;

static MemoryContext
cr_serve_scratch_context(void)
{
	if (CrServeScratchCtx == NULL)
		CrServeScratchCtx = AllocSetContextCreate(TopMemoryContext, "cluster cr inline serve",
												  ALLOCSET_DEFAULT_SIZES);
	return CrServeScratchCtx;
}

/*
 * cluster_gcs_block_forward_serve_inline — spec-7.3 D6 entry.  When the GCS
 * block family is on the DATA plane, the worker[shard] that received a
 * GCS_BLOCK_FORWARD CR / undo-fetch / undo-verdict request serves it inline
 * and ships the reply on its own DATA channel — no shmem slot, no worker-0
 * poll handoff, no 100 ms idle latency.  The forward handler routes to this
 * only when cluster_gcs_block_family_on_data_plane(): on the CONTROL plane
 * the request must go through the light-work park path instead (LMON must not
 * walk undo I/O in its dispatch loop).
 *
 *	Populates the request carrier from the forward payload (decoding the
 *	synthetic undo address / xid carrier as the submit path does), then serves
 *	it through the shared cr_serve_slot() envelope and ships exactly one reply
 *	(the result, or a fail-closed DENIED on any refusal / wave-GUC-off /
 *	malformed request — the requester keeps its unchanged 53R9G / 53R97, Rule
 *	8.A).  Everything runs inside a per-call scratch context that is reset on
 *	return so the long-lived DATA-dispatch loop never accumulates transients.
 */
void
cluster_gcs_block_forward_serve_inline(const GcsBlockForwardPayload *fwd, ClusterLmsCrSlotKind kind)
{
	ClusterLmsCrSlot slot;
	MemoryContext old;
	uint32 segment_id = 0;
	uint32 block_no = 0;
	bool inject_refuse;
	TimestampTz serve_started_at;

	if (fwd == NULL)
		return;

	/*
	 * PGRAC: spec-7.3 D5 (review P2-1) — per-worker shard routing guard,
	 * FORWARD inline-serve entry.  Same invariant as the REQUEST dedup
	 * guard (cluster_gcs_block.c): a block-family frame's tag routes to
	 * exactly one LMS worker (worker[shard(tag)], D4), and this serve runs
	 * in the worker whose DATA channel received the envelope.  A mismatch
	 * is a mis-route (per-tag order break, 8.A) that cannot happen without
	 * a code bug — fail closed (drop without serving; the requester
	 * retransmits within its budget and 53R90/53R9G fail-closes) rather
	 * than serve a tag this worker does not own.
	 */
	{
		int recv_worker = cluster_ic_tier1_my_data_channel();
		int tag_shard = cluster_lms_shard_for_tag(&fwd->tag, cluster_lms_workers);

		Assert(tag_shard == recv_worker);
		if (tag_shard != recv_worker) {
			static bool misroute_logged = false;

			cluster_gcs_block_dedup_note_misroute();
			if (!misroute_logged) {
				misroute_logged = true;
				ereport(LOG,
						(errmsg_internal("gcs block forward misrouted to LMS worker %d (tag shard "
										 "%d); dropping (spec-7.3 P2-1 8.A fail-closed)",
										 recv_worker, tag_shard)));
			}
			return;
		}
	}

	/* Populate the request carrier from the forward payload (was submit). */
	memset(&slot, 0, sizeof(slot));
	slot.tag = fwd->tag;
	slot.request_id = fwd->request_id;
	slot.epoch = fwd->epoch;
	slot.requester_node = fwd->original_requester_node;
	slot.requester_backend = fwd->requester_backend_id;
	slot.reply_master_node = fwd->master_node;
	slot.transition_id = fwd->transition_id;
	slot.result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
	slot.req_kind = (uint8)kind;
	/* spec-5.22d D4-6: -1 = live-owner kinds; only a decoded kind-4 owner
	 * carrier below may set it (memset left 0, which is a VALID node id —
	 * never let it leak into the authority routing). */
	slot.undo_owner = -1;

	/*
	 * PGRAC: spec-7.3 D7 — fence ×N.  A write-fenced node must not let block
	 * images / undo bytes / commit verdicts leave: while the cooperative
	 * write-fence is enforcing but this node is NOT authorized for the live
	 * epoch, any served payload could be stale relative to the cluster's
	 * authoritative state — a split-brain read (CR image) or a false-committed
	 * verdict (stale commit_scn) surface (Rule 8.A).  The worker-0 park ship
	 * path keeps this gate (cluster_lms.c fence-gates cr_ship_ready); D6's move
	 * to inline serve dropped it.  Restoring it HERE — ahead of the kind switch
	 * and any construct/read — covers every worker (0..7) and all three kinds
	 * uniformly.  Refuse by shipping the pre-set DENIED result WITHOUT reading
	 * or constructing anything; the requester retransmits within its budget and
	 * 53R90 fail-closes if the fence outlasts it — never a stale ship.  The
	 * injection forces the same branch deterministically for the TAP fence leg.
	 */
	CLUSTER_INJECTION_POINT("cluster-lms-cr-fence-refuse");
	/* Consume a pending injection arm unconditionally (F6-1 local-var idiom):
	 * evaluating it as the second || operand would let a genuine fence
	 * short-circuit past the consume, leaking the arm to a later call. */
	inject_refuse = cluster_injection_should_skip("cluster-lms-cr-fence-refuse");
	if ((cluster_write_fence_enforcing() && !cluster_write_fence_allowed()) || inject_refuse) {
		cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_FENCE_REFUSED);
		old = MemoryContextSwitchTo(cr_serve_scratch_context());
		cr_build_and_send_reply(&slot); /* slot.result_status == DENIED */
		MemoryContextSwitchTo(old);
		MemoryContextReset(CrServeScratchCtx);
		cluster_lms_obs_note_direct_reply(); /* spec-7.3 D8 */
		return;
	}

	switch (kind) {
	case CLUSTER_LMS_SLOT_KIND_CR:
		slot.read_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		break;

	case CLUSTER_LMS_SLOT_KIND_UNDO_FETCH:
		slot.read_scn = InvalidScn;
		/* A malformed tag leaves segment_id 0 -> lms_undo_fetch_serve refuses. */
		(void)GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no);
		slot.undo_segment_id = segment_id;
		slot.undo_block_no = block_no;
		slot.undo_xid = InvalidTransactionId;
		break;

	case CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT: {
		uint64 carrier = (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);

		slot.read_scn = InvalidScn;
		(void)GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no);
		slot.undo_segment_id = segment_id;
		slot.undo_block_no = block_no;
		/* A malformed carrier (upper 32 bits set / non-normal) leaves xid
		 * Invalid -> lms_undo_verdict_serve refuses. */
		slot.undo_xid
			= (carrier <= (uint64)PG_UINT32_MAX && TransactionIdIsNormal((TransactionId)carrier))
				  ? (TransactionId)carrier
				  : InvalidTransactionId;
		/* spec-5.22f D6-7: the AUTHORITATIVE sub-flag widens the own-xid
		 * gate on the serve side (same decode as the park path). */
		slot.undo_authoritative = GcsBlockForwardPayloadIsUndoVerdictAuthoritative(fwd);
		/* spec-5.22d D4-6: kind-4 dead-owner AUTHORITY verdict — decode +
		 * range-check the owner carrier exactly as the park path does; a
		 * malformed or self-naming carrier leaves the xid Invalid so the
		 * serve refuses (our own xids are answered by the live-owner
		 * kinds, never by an authority detour). */
		if (GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(fwd)) {
			int32 wire_owner = -1;

			if (!GcsBlockUndoAuthorityFetchTagDecodeOwner(fwd->tag, &wire_owner) || wire_owner < 0
				|| wire_owner >= CLUSTER_MAX_NODES || wire_owner == cluster_node_id)
				slot.undo_xid = InvalidTransactionId;
			else
				slot.undo_owner = wire_owner;
		} else if (fwd->tag.relNumber != (RelFileNumber)0) {
			/* owner-served kinds must leave the owner carrier empty */
			slot.undo_xid = InvalidTransactionId;
		}
		break;
	}
	}

	old = MemoryContextSwitchTo(cr_serve_scratch_context());
	/* PGRAC: spec-7.3 D8 — time the serve into the calling worker's duration
	 * histogram (the R4 slow-shard backstop:  a slow CR construction stalls
	 * only this shard, and the per-worker hist is where that shows). */
	serve_started_at = GetCurrentTimestamp();
	cr_serve_slot(&slot);
	cluster_lms_obs_note_inline_serve((uint64)(GetCurrentTimestamp() - serve_started_at));
	/* A caught construction throw left CurrentMemoryContext at TopMemoryContext;
	 * normalize back to the scratch context before the reply build. */
	MemoryContextSwitchTo(cr_serve_scratch_context());

	/*
	 * PGRAC: spec-7.3 D7 (review P1-1) — re-check the fence at SHIP time.
	 * The gate above runs BEFORE construction; the serve itself walks undo
	 * I/O / TT scans (ms-scale), so a qvotec lease that expires DURING the
	 * serve would otherwise let the just-constructed image / verdict leave
	 * the now-fenced node — stale bytes the cluster's authoritative state
	 * has moved past (Rule 8.A).  The park ship path never had this window
	 * (cluster_lms.c gates cr_ship_ready at ship time, per tick); restore
	 * that timing here by discarding the constructed result and shipping
	 * the fail-closed DENIED instead — the requester retransmits within its
	 * budget and 53R90/53R9G fail-closes if the fence outlasts it.  The
	 * probe is a pure in-memory time comparison (no I/O on the hot path).
	 * The injection forces this branch deterministically for the TAP
	 * TOCTOU leg (a genuine mid-serve lease expiry is not schedulable from
	 * TAP); same unconditional-consume idiom as the gate above (F6-1).
	 */
	CLUSTER_INJECTION_POINT("cluster-lms-cr-fence-recheck");
	inject_refuse = cluster_injection_should_skip("cluster-lms-cr-fence-recheck");
	if ((cluster_write_fence_enforcing() && !cluster_write_fence_allowed()) || inject_refuse) {
		slot.result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_FENCE_REFUSED);
	}
	cr_build_and_send_reply(&slot);
	MemoryContextSwitchTo(old);
	MemoryContextReset(CrServeScratchCtx);
	cluster_lms_obs_note_direct_reply(); /* spec-7.3 D8 */
}

#endif /* USE_PGRAC_CLUSTER */
