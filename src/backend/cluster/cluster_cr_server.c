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

#include "access/transam.h" /* TransactionIdDidCommit/DidAbort (D-i4 CLOG cross) */
#include "access/xlog.h"	/* GetFlushRecPtr (spec-6.12i live_hwm_lsn) */
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_elog.h" /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h" /* PGRAC: spec-7.2 D1 READY-publish wakeup */
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_durable.h"		 /* resolve_by_xid (D-i4 complete scan) */
#include "cluster/cluster_tt_slot.h"		 /* max_recycle_horizon (D-i4 bound) */
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
 * lms_undo_fetch_serve — serve one KIND_UNDO_FETCH request (spec-6.12i).
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

	/* Serve only SELF-owned undo: the owner derives from this node's own
	 * id, never from the wire (a forged request cannot redirect the read). */
	return cluster_undo_smgr_read_block(slot->undo_segment_id, (uint8)(cluster_node_id + 1),
										slot->undo_block_no, slot->result_page);
}

/*
 * lms_undo_verdict_serve — serve one KIND_UNDO_VERDICT request (spec-6.12i
 * D-i4 / spec-6.15 D4).
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
 *	DENIED — requester keeps 53R97).  Runs under the serve PG_TRY: a CLOG
 *	page truncated under an old xid, an unreadable segment, or any other
 *	throw becomes a refusal, never a worker/LMS exit.
 */
static bool
lms_undo_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoVerdictPage *v = (ClusterGcsUndoVerdictPage *)slot->result_page;
	TransactionId xid = slot->undo_xid;
	SCN scn = InvalidScn;
	SCN horizon = InvalidScn;
	uint16 wrap = 0;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!TransactionIdIsNormal(xid))
		return false;

	/* spec-6.15 D4: only answer for provably-own xids (see banner). */
	if (!cluster_xid_is_mine(xid))
		return false;

	/* Co-sample the authority triple BEFORE the scan (see banner). */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();

	memset(slot->result_page, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v->xid_echo = (uint64)xid;
	v->commit_scn = InvalidScn;
	v->horizon_scn = InvalidScn;

	switch (
		cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY, &scn, NULL, NULL, &wrap)) {
	case CLUSTER_TT_DURABLE_RESOLVED_SCN:
		if (!TransactionIdDidCommit(xid))
			return false; /* C1b: stamped-then-crashed is in-doubt */
		if (!cluster_cr_accept_resolved_scn(scn))
			return false; /* wrap-suspect -> refuse */
		v->verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
		v->commit_scn = scn;
		v->wrap = wrap;
		return true;

	case CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH:
		/*
		 * Origin legs AFTER the complete scan (ordering contract).  The
		 * SHIPPED bound is NOT the live horizon (its no-reader fallback is
		 * the current SCN clock, which Lamport-chases the observing
		 * requester forever — leg (e) would never converge) but the
		 * monotonic max gate horizon over the recycles that actually
		 * happened: the asked-for xid's slot was one of them, so its lost
		 * commit_scn is at/below that max; and the max freezes between
		 * recycles, so the requester's observe catches up.  InvalidScn
		 * (no gated recycle this incarnation — e.g. all recycles predate a
		 * restart) refuses fail-closed.
		 */
		if (!cluster_cr_retention_proof_origin_legs(&horizon))
			return false;
		horizon = cluster_tt_slot_max_recycle_horizon();
		if (!SCN_VALID(horizon))
			return false;
		if (TransactionIdDidCommit(xid)) {
			v->verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
			v->horizon_scn = horizon;
			return true;
		}
		if (TransactionIdDidAbort(xid)) {
			v->verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
			return true;
		}
		return false; /* neither explicit CLOG state -> refuse */

	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
	default:
		return false;
	}
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
		|| slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT) {
		bool is_verdict = (slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT);
		bool served = false;

		CLUSTER_INJECTION_POINT("cluster-lms-undo-fetch");
		if (!cluster_injection_should_skip("cluster-lms-undo-fetch")) {
			PG_TRY();
			{
				served = is_verdict ? lms_undo_verdict_serve(slot) : lms_undo_fetch_serve(slot);
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
			slot->result_status = (uint8)(is_verdict ? GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
													 : GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT);
			cluster_cr_server_stat_bump(is_verdict ? CLUSTER_CR_SERVER_STAT_VERDICT_SERVED
												   : CLUSTER_CR_SERVER_STAT_UNDO_SERVED);
		} else {
			cluster_cr_server_stat_bump(is_verdict ? CLUSTER_CR_SERVER_STAT_VERDICT_DENIED
												   : CLUSTER_CR_SERVER_STAT_UNDO_DENIED);
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
	if (slot->result_status == (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT
		|| slot->result_status == (uint8)GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT)
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
	else if (hdr->status == (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT
			 || hdr->status == (uint8)GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT) {
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

	if (fwd == NULL)
		return;

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
		break;
	}
	}

	old = MemoryContextSwitchTo(cr_serve_scratch_context());
	cr_serve_slot(&slot);
	/* A caught construction throw left CurrentMemoryContext at TopMemoryContext;
	 * normalize back to the scratch context before the reply build. */
	MemoryContextSwitchTo(cr_serve_scratch_context());
	cr_build_and_send_reply(&slot);
	MemoryContextSwitchTo(old);
	MemoryContextReset(CrServeScratchCtx);
}

#endif /* USE_PGRAC_CLUSTER */
