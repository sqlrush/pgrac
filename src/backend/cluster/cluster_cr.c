/*-------------------------------------------------------------------------
 *
 * cluster_cr.c
 *	  pgrac own-instance Consistent Read (CR) block construction.
 *
 *	  Stage 3 第 13 sub-spec (spec-3.9).  Top-level CR machinery:
 *	    - backend-local 8 KB scratch slot + non-reentrant guard
 *	    - ClusterCRShared shmem region (9 atomic counters)
 *	    - 2-layer API: cluster_cr_lookup_or_construct (top, spec-3.10 cache
 *	      hook) / cluster_cr_construct_block (bottom, always constructs)
 *	    - chain walker driver (Step 3) + tuple remap + CR-image visibility
 *	      helper (Step 4.5)
 *
 *	  Inverse-apply helpers live in cluster_cr_apply.c (Step 4).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_catalog_trace.h"

#include "access/htup_details.h"
#include "access/transam.h"	   /* spec-3.11 D6/C1b: TransactionIdDidCommit, TransactionIdIsNormal */
#include "access/xact.h"	   /* spec-3.19 D3: TransactionIdIsCurrentTransactionId */
#include "storage/procarray.h" /* spec-3.19 D3: TransactionIdIsInProgress */
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "utils/timestamp.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h" /* spec-3.20 D3.A: RelFileLocatorEquals (F8 block-scope) */
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_admit.h" /* spec-5.52 D2: insert-side admission gate */
#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_cr_history.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_cr_cache.h"
#include "cluster/cluster_cr_coordinator_stat.h" /* spec-5.57 D2/D3: coordinator boundary */
#include "cluster/cluster_cr_pool.h"			 /* spec-5.51 D4: shared L2 CR pool */
#include "cluster/cluster_cr_tuple.h" /* spec-5.54: tuple-level / verdict-only fast path */
#include "cluster/cluster_conf.h"	  /* spec-3.24 D1: cluster_conf_has_peers */
#include "cluster/cluster_guc.h"	  /* cluster_cr_chain_walk_max_steps, cluster_node_id */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_recovery_merge.h" /* PGRAC: spec-4.5a is_materialized */
#include "cluster/cluster_remote_xact.h" /* PGRAC: spec-4.5a G5 outcome */ /* spec-3.21: cluster_itl_get_tt_ref (xmax overlay key) */
#include "cluster/cluster_touched_peers.h" /* PGRAC: spec-5.14 D2 class 4 */
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_resolver_cache.h"		/* spec-5.55 D0: by-xid scan measure hook */
#include "cluster/cluster_tt_durable.h"			/* spec-3.11 D6: watermark by-xid resolve */
#include "cluster/cluster_tt_slot.h"			/* spec-3.22: retention_off_recycle_count */
#include "cluster/cluster_undo_retention.h"		/* spec-3.22: retention horizon proof */
#include "cluster/cluster_visibility_resolve.h" /* spec-3.21: cluster_vis_cr_xmax_verdict */
#include "cluster/cluster_uba.h"
#include "cluster/cluster_cr_server.h" /* spec-6.12b CR-server data plane */
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D3: profiling probes */
#include "cluster/cluster_xid_stripe.h"


/*
 * ClusterCRShared -- per-instance shmem counters (spec-3.9 §2.5).
 *
 *	Atomic counters bumped with pg_atomic_fetch_add_u64 from the
 *	constructing backend, read lock-free by dump_state / pg_cluster_state,
 *	plus ONE embedded LWLock (region lwlock_count = 1): the GCS-race
 *	round-3b native-prehistory consume/disable drain lock (see the field
 *	comment below) -- counters stay lock-free.
 */
typedef struct ClusterCRShared {
	pg_atomic_uint64 cr_construct_count;
	pg_atomic_uint64 cr_snapshot_too_old_count;
	pg_atomic_uint64 cr_cross_instance_unsupported_count;
	pg_atomic_uint64 cr_corruption_count;
	pg_atomic_uint64 cr_chain_walk_steps_sum;
	pg_atomic_uint64 cr_inverse_insert_count;
	pg_atomic_uint64 cr_inverse_update_count;
	pg_atomic_uint64 cr_inverse_delete_count;
	pg_atomic_uint64 cr_inverse_itl_count;
	/* spec-3.10 D5: CR block cache (4 counters, 9 -> 13 cr-category rows). */
	pg_atomic_uint64 cr_cache_hit_count;
	pg_atomic_uint64 cr_cache_miss_count;
	pg_atomic_uint64 cr_cache_evict_count;
	pg_atomic_uint64 cr_cache_install_count;
	/*
	 * spec-3.22 D3: xmax recycled-slot resolve outcome split (4 buckets) so the
	 * 53R9F drop is observable and the residual fail-closed is monitorable
	 * (feeds spec-3.23).  resolved = exact commit_scn compared; recycled_invisible
	 * = durable 0-match proven below horizon -> invisible; invalid_or_ambiguous =
	 * delayed-cleanout / wrap residue fail-closed; scan_unavail_or_no_proof =
	 * degraded scan or a recycled 0-match without a valid retention proof.
	 */
	pg_atomic_uint64 cr_xmax_resolved_count;
	pg_atomic_uint64 cr_xmax_recycled_invisible_count;
	pg_atomic_uint64 cr_xmax_invalid_or_ambiguous_count;
	pg_atomic_uint64 cr_xmax_scan_unavail_or_no_proof_count;
	/*
	 * spec-6.12b: CR-server data plane (6 counters).  requester side: how the
	 * remote fetch resolved (full page / partial + local continue / failed ->
	 * unchanged 53R9G).  server side (bumped in the LMS construction and the
	 * LMON submit path): requests parked, results shipped full/partial, and
	 * refusals (GUC off, no free slot, block not resident, interleaved homes,
	 * construction error -> all fail-closed to the requester).
	 */
	pg_atomic_uint64 cr_remote_full_count;
	pg_atomic_uint64 cr_remote_partial_count;
	pg_atomic_uint64 cr_remote_failed_count;
	pg_atomic_uint64 cr_server_full_count;
	pg_atomic_uint64 cr_server_partial_count;
	pg_atomic_uint64 cr_server_denied_count;
	/*
	 * spec-6.12i: undo-TT fetch data plane (5 counters).  requester side:
	 * wire fetches that returned block + co-sampled authority, cache hits
	 * (L2 CR pool bytes + same-epoch per-backend authority memo), and
	 * fail-closed misses (GUC off / bad UBA / non-header block / timeout /
	 * DENIED / checksum / trailer missing -> the caller keeps 53R97).
	 * server side (bumped in the LMS drain): TT header blocks served with a
	 * co-sampled authority triple, and refusals.
	 */
	pg_atomic_uint64 rtvis_undo_fetch_wire_count;
	pg_atomic_uint64 rtvis_undo_fetch_cache_hit_count;
	pg_atomic_uint64 rtvis_undo_fetch_failclosed_count;
	pg_atomic_uint64 cr_server_undo_served_count;
	pg_atomic_uint64 cr_server_undo_denied_count;
	/*
	 * spec-6.12i CP3: recycled-slot active-runtime RESOLUTION verdicts (a
	 * level above the fetch counters): terminal proofs returned vs
	 * fail-closed refusals (fetch failed / covers gate / 0-match /
	 * multi-match / non-terminal -> the caller keeps 53R97).
	 */
	pg_atomic_uint64 rtvis_resolve_committed_count;
	pg_atomic_uint64 rtvis_resolve_aborted_count;
	pg_atomic_uint64 rtvis_resolve_failclosed_count;
	/*
	 * spec-6.12i CP5 (D-i4) / spec-6.15 D4: origin-verdict leg (5 requester
	 * counters + 2 server counters).  requester side: verdict wire round
	 * trips, verdict-leg fail-closed refusals (wire / covers gate / page
	 * validation), exact terminal verdicts consumed, below-horizon bounds
	 * consumed (leg (e) admissible), and below-horizon bounds REFUSED by
	 * leg (e) (read_scn behind the shipped horizon — the clock-skew
	 * diagnostic; the observe makes the next snapshot admissible).
	 * server side (LMS drain): complete-scan verdicts served vs refused.
	 */
	pg_atomic_uint64 rtvis_verdict_wire_count;
	pg_atomic_uint64 rtvis_verdict_failclosed_count;
	pg_atomic_uint64 rtvis_verdict_exact_count;
	pg_atomic_uint64 rtvis_verdict_below_horizon_count;
	pg_atomic_uint64 rtvis_verdict_inadmissible_count;
	pg_atomic_uint64 cr_server_verdict_served_count;
	pg_atomic_uint64 cr_server_verdict_denied_count;
	pg_atomic_uint64 cr_server_fence_refused_count; /* spec-7.3 D7 fence ×N refuse */
	/*
	 * spec-7.1 D3-b (server side): multixact member-verdict batches served vs
	 * refused (any unprovable updater member refuses the whole multi).
	 */
	pg_atomic_uint64 cr_server_multi_verdict_served_count;
	pg_atomic_uint64 cr_server_multi_verdict_denied_count;
	/*
	 * spec-6.15 D4: a recycled remote ref whose tuple xid the stripe
	 * derivation could not attribute (striping off / below the activation
	 * floor) — the active-runtime resolution never even asks the wire and
	 * the caller keeps 53R97 (t/347 L2 asserts this stays 0 on a fully
	 * striped cluster).
	 */
	pg_atomic_uint64 rtvis_underivable_failclosed_count;
	/*
	 * spec-5.22f D6-3: fresh-remote-ITL-ref widening outcome counters.  A
	 * fresh ref (local_xid == raw_xid, origin != self) that missed the overlay
	 * and was resolved by the D3 cross-node verdict (classify_ref_guts D6
	 * branch): resolved = a terminal COMMITTED/ABORTED verdict consumed
	 * (the L1 root-cause-#1 hard assert lands here, proving the fresh-ref path
	 * ran rather than an overlay / ordinary-propagation hit); failclosed = a
	 * STALE integrity mismatch or an UNKNOWN verdict kept 53R97.
	 */
	pg_atomic_uint64 vis_freshref_verdict_resolved_count;
	pg_atomic_uint64 vis_freshref_verdict_failclosed_count;
	/*
	 * GCS-race round-2 RC-E: native-prehistory LOCAL routing.
	 * native_prehistory_covered_hw is a WRITE-ONCE latch (not a counter):
	 * the startup process stores the sealed native high-water here after
	 * the post-recovery verify proved the local pg_xact byte-matches the
	 * sealed prehistory blob over the whole surviving native range.  0
	 * means "not proven this boot" and keeps the resolver fail-closed for
	 * below-floor xids (53R97).  rtvis_native_prehistory_local_count counts
	 * recycled-ref resolutions the latch routed to the local adopted CLOG
	 * instead of failing closed.
	 */
	pg_atomic_uint64 native_prehistory_covered_hw;
	pg_atomic_uint64 rtvis_native_prehistory_local_count;
	/*
	 * GCS-race round-3 P0-1: one-way latch disable.  Set (0 -> 1, never
	 * cleared) when the wrap barrier tells this node that raw 32-bit values
	 * below the native high-water are about to be reused by epoch>=1 xids,
	 * so the adopted prehistory can no longer answer alias-free.  A set
	 * disable both zeroes covered_hw and makes any concurrent latch store
	 * void (store-then-recheck in the latch closes the race); readers then
	 * stay fail-closed (53R97) for below-floor refs forever.
	 */
	pg_atomic_uint64 native_prehistory_disabled;
	/*
	 * GCS-race round-3b review P0: consume/disable drain lock.  The remote
	 * raw-xid reuse hazard has NO local CLOG write to synchronize on (the
	 * first epoch-1 xid is allocated on the WRAPPING node and only that
	 * node's pg_xact changes), so relaxed atomics + barriers cannot prove a
	 * local reader observes the latch drop before consuming.  Readers hold
	 * this SHARED across proof -> adopted-CLOG read -> verdict production;
	 * cluster_cr_native_prehistory_disable() clears the latch under
	 * EXCLUSIVE, and the wb DISABLE handler only ACKs after the release.
	 * The exclusive acquisition therefore drains every in-flight reader
	 * BEFORE the ACK leaves (their verdicts predate any epoch-1 allocation
	 * = genuinely native), and every later reader's SHARED acquisition
	 * pairs with the release (sees the latch down -> fail-closed).
	 */
	LWLock native_prehistory_lock;
	/*
	 * spec-5.22d D4-4: dead/absent-owner authority block0 serve outcomes.
	 * serve_hit = self-as-authority served a terminal verdict off the dead
	 * owner's shared block0 (the D4-8 L1 hard assert lands here, proving the
	 * Route B serve ran rather than a materialized-copy read); fail_closed =
	 * the precision chain refused (peer/no authority, epoch moved, block0
	 * unreadable, or no slot proof) and the caller kept 53R97;
	 * epoch_stale_reject (D4-5) = the specific epoch axis of a refusal (the
	 * claim's reconfig epoch no longer is the current one) -- bumped IN
	 * ADDITION to fail_closed, so fail_closed stays the total-refusal count
	 * (D4-8 L3 asserts the axis, not just the total).
	 */
	pg_atomic_uint64 undo_authority_serve_hit_count;
	pg_atomic_uint64 undo_authority_fail_closed_count;
	pg_atomic_uint64 undo_authority_epoch_stale_reject_count;
	/* spec-5.22d A1 (D4-8): complete-scan refusal attribution — the durable
	 * segment-set enumeration/parse was incomplete (refuse-all), or the set
	 * held more than one match for the asked xid (ambiguity). */
	pg_atomic_uint64 undo_authority_scan_incomplete_reject_count;
	pg_atomic_uint64 undo_authority_multi_match_reject_count;
	/*
	 * spec-7.1 D0/D5: 53R97 per-leg attribution.  Every fail-closed refusal
	 * on the cross-instance read path is attributed to the leg that produced
	 * it, so the census (D0 before / D4 after) can prove each leg's zeroing
	 * by the mechanism that closed it instead of a lump total.  Server side
	 * (origin LMS verdict serve): invalid_scn = durable by-xid hit with an
	 * unstamped commit_scn (delayed cleanout window); zero_match = provably
	 * recycled slot whose CLOG state is neither explicit commit nor abort;
	 * srv_other = every remaining refusal (ambiguous wrap, scan unavailable,
	 * non-own xid, wrap-suspect, in-doubt stamped-then-crashed).  Requester
	 * side: covers_refuse = live-authority covers gate refused the verdict
	 * pairing; multi_unresolvable = a multixact xmax whose member set or
	 * origin cannot be proven; xmax_unprovable = a deleting xmax whose
	 * terminal state resolution ended unproven (fail-closed -1).
	 */
	pg_atomic_uint64 vis53r97_leg_invalid_scn_refuse_count;
	pg_atomic_uint64 vis53r97_leg_zero_match_refuse_count;
	pg_atomic_uint64 vis53r97_leg_srv_other_refuse_count;
	pg_atomic_uint64 vis53r97_leg_covers_refuse_count;
	pg_atomic_uint64 vis53r97_leg_multi_unresolvable_count;
	pg_atomic_uint64 vis53r97_leg_xmax_unprovable_count;
	/*
	 * spec-7.1 D1 requester 半边: the xmin overlay-miss leg (census S5's
	 * largest 53R97 source) asks the origin for a by-xid verdict before
	 * failing closed.  ask = every such verdict ask issued; hit = the ask
	 * proved a terminal state (positive interread).  ask far exceeding the
	 * distinct-missed-xid count over a census window is the same-xid
	 * thundering-herd signal (N backends miss one hot xid -> N parallel asks
	 * to one origin; singleflight is a 7.2/7.3 concern -- D5 only makes it
	 * observable, IN-9 执行条件 2).
	 */
	pg_atomic_uint64 vis53r97_leg_xmin_overlay_verdict_ask_count;
	pg_atomic_uint64 vis53r97_leg_xmin_overlay_verdict_hit_count;
	/*
	 * spec-7.1 D3-b requester 半边: a foreign multixact xmax whose member
	 * overlay missed asked the origin for a batched member verdict.  ask =
	 * every request sent; hit = a request whose SERVED page resolved to a
	 * definite VISIBLE/INVISIBLE.  unprovable = ask - hit (a DENIED / timeout /
	 * invalid page, or a SERVED page still UNKNOWN at this read_scn) -> 53R97.
	 */
	pg_atomic_uint64 vis53r97_leg_multi_member_serve_ask_count;
	pg_atomic_uint64 vis53r97_leg_multi_member_serve_hit_count;
	/*
	 * spec-7.1 D1 serve 半边: the origin LMS verdict serve upgraded a durable
	 * XID_MATCH_INVALID_SCN hit (unstamped commit_scn, delayed-cleanout window)
	 * to a positive ABORTED answer by cross-checking CLOG (authoritative for
	 * our own xid).  hit = the leg resolved positively (aborted-unstamped, the
	 * IN-5 real population); the miss counterpart is invalid_scn_refuse_count
	 * above (an INVALID_SCN hit that stays fail-closed: in-flight, 2PC-prepared,
	 * crash window, or committed-but-unstamped -- no fabricated commit_scn).
	 * 8.A: positive proof only; direction of every refuse leg is unchanged.
	 */
	pg_atomic_uint64 vis53r97_leg_live_upgrade_hit_count;
	/* Stage 8 R4 D11: observation-only feature counters. */
	pg_atomic_uint64 r4_event_counts[CLUSTER_R4_OBSERVATION_EVENT_COUNT];
} ClusterCRShared;

StaticAssertDecl(CLUSTER_R4_OBSERVATION_EVENT_COUNT == 71,
				 "R4 counter layout changes require a fresh-restart/dump binding");

static ClusterCRShared *CRShared = NULL;

/*
 * Backend-local CR scratch slot (spec-3.9 Q3 / I-cr-1).
 *
 *	Single 8 KB reusable page allocated once in TopMemoryContext on first
 *	construction.  cluster_cr_construct_block returns a pointer into this
 *	slot; the pointer is valid only until the next construction call.
 *
 *	cr_in_progress is the non-reentrant guard (I-lock-3): nested CR
 *	construction in the same backend would clobber the shared scratch, so
 *	we Assert against it and keep the flag balanced across ereport via
 *	PG_TRY/PG_CATCH.
 */
static char *cr_scratch = NULL;
static bool cr_in_progress = false;

/*
 * R4 holder construction is resumed only by LMS DATA worker 0.  The shared
 * slot contains immutable transport identity and page bytes; candidate order
 * and the walk cursor are process-local and keyed by the persistent slot
 * generation.  Every implemented walk consumes that frozen candidate state
 * without rereading a live buffer.
 */
typedef struct ClusterR4CrBuildContext {
	bool in_use;
	uint64 slot_generation;
	uint64 builder_incarnation;
	ClusterCRCandidateChain candidates[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterTxLocator locators[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterTxLocator pending_locator;
	UBA *visited_ubas;
	UBA immediate_previous_uba;
	UndoRecordHeader immediate_previous_record;
	uint8 candidate_count;
	uint8 candidate_cursor;
	uint8 covered_candidate_mask;
	bool pending_locator_valid;
	bool have_previous;
	uint8 record_format; /* 0 undecided, 1 legacy transaction walk, 2 page history */
	uint32 max_steps;
	uint32 visited_count;
} ClusterR4CrBuildContext;

StaticAssertDecl(CLUSTER_ITL_INITRANS_DEFAULT <= 8,
				 "R4 covered-head mask must represent every candidate");

static ClusterR4CrBuildContext CrR4BuildContexts[CLUSTER_LMS_CR_SLOTS];

/* One builder call's operands, never a shared/wire carrier or authority. */
typedef struct CrR4UndoTrace {
	uint32 slot_index;
	const ClusterR4CrSlotExtension *extension;
	const ClusterR4CrBuildContext *context;
	Page page;
	ClusterTxLocator locator;
	UBA uba;
	UndoRecordHeader record;
	Size record_length;
	bool locator_known, uba_known, read_returned, record_header_known, resident_foreign;
	int apply_record, locator_reason;
} CrR4UndoTrace;

/* Owned byte projections only. The original server PG_TRY catches LOG ERROR
 * and terminalizes its exact builder key. No LOG is added to error cleanup. */
static void
cr_r4_undo_trace(const CrR4UndoTrace *v, const char *event, const char *predicate, int outcome)
{
	static uint64 event_seq, critical_skipped;
	const ClusterR4CrSlotExtension *e;
	const ClusterR4CrBuildContext *c;
	const BufferTag *tag;
	UndoRecordHeader empty_record = {0};
	ClusterTxLocator empty_locator = {0};
	const UndoRecordHeader *r;
	const ClusterTxLocator *l;
	PageHeader page;
	ClusterItlSlotData slot = {0};
	ClusterItlPageHeader itl_header = {0};
	HeapTupleHeaderData tuple = {0};
	bool page_known, slot_known, item_known = false, tuple_known = false;
	bool uba_decoded, locator_reason_known = false;
	uint32 segment = 0, block = 0;
	uint16 tt_offset = 0, row_offset = 0;
	unsigned item_flags = 0;
	Size item_offset = 0, item_length = 0;
	instr_time now;

	if (v == NULL || !cluster_trace_normal_stop)
		return;
	if (CritSectionCount != 0) {
		if (critical_skipped != UINT64_MAX) critical_skipped++;
		return;
	}
	e = v->extension; c = v->context; tag = &e->route_proof.tag;
	r = v->record_header_known ? &v->record : &empty_record;
	l = v->locator_known ? &v->locator : &empty_locator;
	page = (PageHeader)v->page;
	page_known = PageHasItl(v->page) && page->pd_lower >= SizeOfPageHeaderData
		&& page->pd_lower <= page->pd_upper && page->pd_upper <= page->pd_special
		&& page->pd_special <= BLCKSZ && BLCKSZ - page->pd_special >= CLUSTER_ITL_SPECIAL_SIZE
		&& PageGetMaxOffsetNumber(v->page) <= MaxHeapTuplesPerPage;
	slot_known = page_known && v->locator_known && l->itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT;
	if (page_known)
		memcpy(&itl_header, (char *)v->page + page->pd_special + CLUSTER_ITL_ARRAY_SIZE,
			   sizeof(itl_header));
	if (slot_known)
		memcpy(&slot, (char *)v->page + page->pd_special + l->itl_slot_index * sizeof(slot),
			   sizeof(slot));
	if (page_known && v->record_header_known
		&& r->target_locator.spcOid == tag->spcOid && r->target_locator.dbOid == tag->dbOid
		&& r->target_locator.relNumber == tag->relNumber && r->target_fork == tag->forkNum
		&& r->target_block == tag->blockNum && r->target_offset >= FirstOffsetNumber
		&& r->target_offset <= PageGetMaxOffsetNumber(v->page)) {
		ItemId item = PageGetItemId(v->page, r->target_offset);

		item_known = true; item_flags = item->lp_flags;
		item_offset = ItemIdGetOffset(item); item_length = ItemIdGetLength(item);
		if (ItemIdIsNormal(item) && item_length >= SizeofHeapTupleHeader
			&& item_offset >= page->pd_upper && item_offset <= page->pd_special
			&& item_length <= page->pd_special - item_offset) {
			memcpy(&tuple, (char *)v->page + item_offset, SizeofHeapTupleHeader);
			tuple_known = tuple.t_hoff >= SizeofHeapTupleHeader && tuple.t_hoff <= item_length;
		}
	}
	/* Reclassify only copied scalar guards; never re-run the native validator. */
	if (strcmp(predicate, "RECORD_VALIDATION") == 0 && v->record_header_known) {
		if (v->record_length != sizeof(*r) + r->payload_length) predicate = "RECORD_LENGTH";
		else if ((r->flags & ~UNDO_REC_FLAG_HAS_ITL_HISTORY)
				 != (UBA_is_invalid(r->prev_uba) ? UNDO_REC_FLAG_FIRST_IN_TX : 0))
			predicate = "RECORD_FLAGS";
		else if (!SCN_VALID(r->write_scn)) predicate = "RECORD_SCN";
		else {
			predicate = c->have_previous ? "PREVIOUS_EDGE_IDENTITY" : "HEAD_IDENTITY";
			locator_reason_known = v->locator_reason >= 0;
		}
	} else if (strcmp(event, "record.validated") == 0)
		locator_reason_known = v->locator_reason >= 0;
	uba_decoded = v->uba_known && uba_decode(v->uba, &segment, &block, &tt_offset, &row_offset);
	if (event_seq != UINT64_MAX) event_seq++;
	INSTR_TIME_SET_CURRENT(now);
	ereport(LOG, (errmsg_internal(
		"PGRAC_FAMILY=R4_UNDO event=%s predicate=%s outcome=%d "
		"event_seq=" UINT64_FORMAT " sequence_scope=process sequence_domain=R4_UNDO "
		"node_id=%d pid=%d wall_us=" INT64_FORMAT " monotonic_us=" UINT64_FORMAT
		" identity_source=run_manifest run_id_known=0 boot_identity_known=0 "
		"process_incarnation_known=0 request_id_known=0 root_identity_known=0 full_xid_known=0 "
		"authoritative=0 transport_receipt=0 "
		"slot=%u slot_generation=" UINT64_FORMAT " builder_incarnation=" UINT64_FORMAT
		" builder_pid=%d builder_worker=%u edge_incarnation=" UINT64_FORMAT
		" edge_pid=%d edge_worker=%u edge_role=%u "
		"spc=%u db=%u rel=%u fork=%d block=%u read_scn=" UINT64_FORMAT
		" formation_epoch=" UINT64_FORMAT " activation_generation=" UINT64_FORMAT
		" master_authority_generation=" UINT64_FORMAT " master_transition=" UINT64_FORMAT
		" real_master=%d selected_holder=%d expected_page_scn=" UINT64_FORMAT
		" copied_page_lsn=" UINT64_FORMAT " copied_page_scn=" UINT64_FORMAT
		" candidate_cursor=%u candidate_count=%u covered_mask=%u record_format=%u "
		"build_steps=%u max_steps=%u visited=%u have_previous=%d "
		"locator_scope=operation_copy locator_known=%d locator_xid=%u locator_tt_wrap=%u "
		"locator_itl_kind=%u locator_itl_slot=%u locator_uba=" UINT64_FORMAT "/" UINT64_FORMAT
		" uba_known=%d uba=" UINT64_FORMAT "/" UINT64_FORMAT
		" uba_decoded=%d uba_segment=%u uba_block=%u uba_tt_offset=%u uba_row_offset=%u "
		"foreign_request_id=" UINT64_FORMAT " origin_formation_epoch=" UINT64_FORMAT
		" origin_tt_generation=" UINT64_FORMAT " origin_authority_scn=" UINT64_FORMAT
		" record_source=%s read_return_known=%d record_length=%zu record_header_known=%d "
		"record_type=%u record_flags=%u record_payload_length=%u record_xid=%u "
		"record_origin=%u record_tt_segment=%u record_tt_slot=%u record_tt_wrap_plus1=%u "
		"record_write_scn=" UINT64_FORMAT " record_prev=" UINT64_FORMAT "/" UINT64_FORMAT
		" target_spc=%u target_db=%u target_rel=%u target_fork=%d target_block=%u target_offset=%u "
		"image_scope=owned_builder page_known=%d page_lsn=" UINT64_FORMAT
		" page_scn=" UINT64_FORMAT " page_watermark=" UINT64_FORMAT
		" page_itl_known=%d page_itl_xid=%u page_itl_wrap=%u page_itl_flags=%u "
		"page_itl_write_scn=" UINT64_FORMAT " page_itl_uba=" UINT64_FORMAT "/" UINT64_FORMAT
		" target_item_known=%d target_item_flags=%u target_item_offset=%zu target_item_length=%zu "
		"target_tuple_known=%d tuple_xmin=%u tuple_xmax=%u tuple_ctid=%u/%u tuple_mask=%u/%u "
		"apply_record=%d locator_reason_known=%d locator_reason=%d "
		"trace_critical_skipped=" UINT64_FORMAT " trace_sequence_exhausted=%d",
		event, predicate, outcome, event_seq, cluster_node_id, MyProcPid,
		(int64)GetCurrentTimestamp(), (uint64)INSTR_TIME_GET_MICROSEC(now),
		v->slot_index, e->slot_generation, e->owner.builder_incarnation,
		e->owner.builder_pid, e->owner.builder_worker_id, e->owner.edge_owner_incarnation,
		e->owner.edge_owner_pid, e->owner.edge_owner_worker_id, e->owner.edge_owner_role,
		tag->spcOid, tag->dbOid, tag->relNumber, tag->forkNum, tag->blockNum,
		(uint64)e->route_proof.read_scn, e->route_proof.formation_epoch,
		e->route_proof.activation_generation, e->route_proof.master_authority_generation,
		e->route_proof.master_resource_transition_count, e->route_proof.real_master_node,
		e->route_proof.selected_holder_node, (uint64)e->route_proof.expected_page_scn,
		(uint64)e->copied_page_lsn, (uint64)e->copied_page_scn,
		c->candidate_cursor, c->candidate_count, c->covered_candidate_mask, c->record_format,
		e->build_steps, c->max_steps, c->visited_count, c->have_previous,
		v->locator_known, l->xid, l->tt_wrap, l->itl_kind, l->itl_slot_index,
		l->uba.raw[0], l->uba.raw[1], v->uba_known,
		v->uba_known ? v->uba.raw[0] : (uint64)0, v->uba_known ? v->uba.raw[1] : (uint64)0,
		uba_decoded, segment, block, tt_offset, row_offset,
		e->foreign_request_id, e->origin_formation_epoch, e->origin_tt_generation,
		(uint64)e->origin_authority_scn,
		!v->read_returned ? "unread" : v->resident_foreign ? "resident_foreign_copy" : "local_copy",
		v->read_returned, v->read_returned ? v->record_length : (Size)0,
		v->record_header_known, r->record_type, r->flags, r->payload_length, r->xid,
		r->origin_node_id, r->tt_slot_segment_id, r->tt_slot_id, r->tt_wrap_plus1,
		(uint64)r->write_scn, r->prev_uba.raw[0], r->prev_uba.raw[1],
		r->target_locator.spcOid, r->target_locator.dbOid, r->target_locator.relNumber,
		r->target_fork, r->target_block, r->target_offset,
		page_known, page_known ? (uint64)PageGetLSN(v->page) : (uint64)0,
		page_known ? (uint64)page->pd_block_scn : (uint64)0,
		(uint64)itl_header.itl_recycle_watermark_scn, slot_known, slot.xid, slot.wrap, slot.flags,
		(uint64)slot.write_scn, slot.undo_segment_head.raw[0], slot.undo_segment_head.raw[1],
		item_known, item_flags, item_offset, item_length,
		tuple_known, tuple_known ? HeapTupleHeaderGetRawXmin(&tuple) : 0,
		tuple_known ? HeapTupleHeaderGetRawXmax(&tuple) : 0,
		tuple_known ? ItemPointerGetBlockNumberNoCheck(&tuple.t_ctid) : 0,
		tuple_known ? ItemPointerGetOffsetNumberNoCheck(&tuple.t_ctid) : 0,
		tuple_known ? tuple.t_infomask : 0, tuple_known ? tuple.t_infomask2 : 0,
		v->apply_record, locator_reason_known, locator_reason_known ? v->locator_reason : -1,
		critical_skipped, event_seq == UINT64_MAX)));
}


/*
 * spec-5.54 D2: a SEPARATE backend-local scratch page for the tuple-level
 * verdict-only fast path, distinct from cr_scratch so the fast path never shares
 * (and never has to reset) the full-block construct slot or its cr_in_progress
 * guard (Q5 / U18).  The fast path does not call cluster_cr_construct_block, so
 * the two scratch slots are never live simultaneously.
 */
static char *cr_tuple_scratch = NULL;

static bool
cr_r4_bytes_zero(const void *ptr, Size len)
{
	const unsigned char *bytes = (const unsigned char *)ptr;
	Size i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static void
cr_r4_clear_foreign_request(ClusterR4CrSlotExtension *extension, ClusterR4CrBuildContext *context)
{
	extension->foreign_request_id = 0;
	memset(&extension->foreign_uba, 0, sizeof(extension->foreign_uba));
	extension->origin_formation_epoch = 0;
	extension->origin_live_hwm_lsn = 0;
	extension->origin_tt_generation = 0;
	extension->origin_authority_scn = InvalidScn;
	extension->foreign_origin_node = 0;
	extension->foreign_segment_id = 0;
	extension->foreign_block_no = 0;
	extension->foreign_xid = InvalidTransactionId;
	extension->foreign_wrap = 0;
	extension->foreign_tt_slot_offset = 0;
	extension->foreign_row_offset = 0;
	memset(&context->pending_locator, 0, sizeof(context->pending_locator));
	context->pending_locator_valid = false;
}

static bool
cr_r4_foreign_request_matches(uint32 slot_index, uint64 slot_generation,
							  const ClusterR4CrSlotExtension *extension,
							  const ClusterR4CrBuildContext *context,
							  const ClusterTxLocator *locator)
{
	const ClusterTxLocator *pending = &context->pending_locator;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	NodeId origin;

	if (!context->pending_locator_valid || extension->build_steps != context->visited_count
		|| extension->foreign_request_id
			   != cluster_cr_r4_dependency_request_id(slot_index, slot_generation,
													  extension->build_steps)
		|| extension->foreign_request_id == 0 || pending->xid != locator->xid
		|| pending->tt_wrap != locator->tt_wrap || pending->itl_kind != locator->itl_kind
		|| pending->itl_slot_index != locator->itl_slot_index
		|| !uba_decode(pending->uba, &segment_id, &block_no, &tt_slot_offset, &row_offset)
		|| block_no == 0)
		return false;
	origin = uba_origin_node_id(pending->uba);
	return origin != InvalidNodeId && origin != (NodeId)cluster_node_id
		   && extension->foreign_uba.raw[0] == pending->uba.raw[0]
		   && extension->foreign_uba.raw[1] == pending->uba.raw[1]
		   && extension->origin_formation_epoch == extension->route_proof.formation_epoch
		   && extension->foreign_origin_node == (int32)origin
		   && extension->foreign_segment_id == segment_id && extension->foreign_block_no == block_no
		   && extension->foreign_xid == pending->xid
		   && (pending->tt_wrap == TT_WRAP_INVALID ? (extension->foreign_wrap == TT_WRAP_INVALID
													  || extension->foreign_wrap <= TT_WRAP_MAX)
												   : extension->foreign_wrap == pending->tt_wrap)
		   && extension->foreign_tt_slot_offset == tt_slot_offset
		   && extension->foreign_row_offset == row_offset;
}

/* Page-slot reuse is not TT-slot reuse.  Only the addressed record may
 * establish an initially unknown canonical TT generation; exact callers and
 * every later predecessor keep their original generation check. */
static bool
cr_r4_canonical_record_locator(const ClusterTxLocator *request, UBA record_uba,
							   const UndoRecordHeader *record, ClusterTxLocator *out)
{
	ClusterTxLocator canonical = *request;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	if (canonical.tt_wrap == TT_WRAP_INVALID) {
		if (record->tt_wrap_plus1 == 0)
			return false;
		canonical.tt_wrap = (uint16)(record->tt_wrap_plus1 - 1);
		if (canonical.tt_wrap > TT_WRAP_MAX)
			return false;
	}
	if (!cluster_undo_record_validate_identity(&canonical, record_uba, record, &reason))
		return false;
	*out = canonical;
	return true;
}

bool
cluster_cr_r4_prepare_resident_records(const char resident_undo_page[BLCKSZ],
									 const ClusterTxLocator *request_locators,
									 uint32 request_count, ClusterR4ResidentRecordBatch *out)
{
	UndoBlockHeader header;
	ClusterR4ResidentRecordBatch candidate;
	uint32 batch_segment = 0;
	uint32 batch_block = 0;
	uint32 slot_dir_low;
	uint32 i;

	if (resident_undo_page == NULL || request_locators == NULL || out == NULL
		|| request_count == 0 || request_count > CLUSTER_R4_RESIDENT_RECORD_BATCH_MAX)
		return false;
	memcpy(&header, resident_undo_page, sizeof(header));
	if (header.magic != PGRAC_UNDO_BLOCK_MAGIC
		|| header.block_version != UNDO_BLOCK_VERSION_1 || header.slot_count == 0
		|| header.slot_count > (BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry))
		return false;
	slot_dir_low = BLCKSZ - (uint32)header.slot_count * sizeof(UndoSlotDirEntry);
	if (header.free_offset < sizeof(UndoBlockHeader) || header.free_offset > slot_dir_low)
		return false;

	memset(&candidate, 0, sizeof(candidate));
	for (i = 0; i < request_count; i++) {
		const ClusterTxLocator *request = &request_locators[i];
		ClusterR4ResidentRecord *record = &candidate.records[i];
		UndoSlotDirEntry slot;
		uint32 segment_id;
		uint32 block_no;
		uint16 tt_slot_offset;
		uint16 row_offset;

		if (!uba_decode(request->uba, &segment_id, &block_no, &tt_slot_offset, &row_offset)
			|| block_no == 0 || row_offset >= header.slot_count)
			return false;
		if (i == 0) {
			batch_segment = segment_id;
			batch_block = block_no;
		} else if (segment_id != batch_segment || block_no != batch_block)
			return false;

		memcpy(&slot,
			   resident_undo_page + BLCKSZ - ((uint32)row_offset + 1) * sizeof(UndoSlotDirEntry),
			   sizeof(slot));
		if (slot.record_offset < sizeof(UndoBlockHeader)
			|| slot.record_length < sizeof(UndoRecordHeader)
			|| slot.record_offset > header.free_offset
			|| slot.record_length > header.free_offset - slot.record_offset)
			return false;
		memcpy(&record->header, resident_undo_page + slot.record_offset, sizeof(record->header));
		if (slot.record_type != record->header.record_type || slot.flags != record->header.flags
			|| slot.record_length != sizeof(UndoRecordHeader) + (size_t)record->header.payload_length
			|| !cr_r4_canonical_record_locator(request, request->uba, &record->header,
												 &record->canonical_locator))
			return false;
		record->record_offset = slot.record_offset;
		record->record_length = slot.record_length;
	}
	candidate.count = request_count;
	memcpy(out, &candidate, sizeof(candidate));
	return true;
}

bool
cluster_cr_r4_extract_resident_record(const char resident_undo_page[BLCKSZ],
									  const ClusterTxLocator *request_locator,
									  char record_out[BLCKSZ], size_t *record_length_out,
									  ClusterTxLocator *canonical_locator_out)
{
	ClusterR4ResidentRecordBatch batch;
	const ClusterR4ResidentRecord *record;

	if (resident_undo_page == NULL || request_locator == NULL || record_out == NULL
		|| record_length_out == NULL || canonical_locator_out == NULL)
		return false;
	if (!cluster_cr_r4_prepare_resident_records(resident_undo_page, request_locator, 1, &batch))
		return false;
	record = &batch.records[0];
	memmove(record_out, resident_undo_page + record->record_offset, record->record_length);
	*record_length_out = record->record_length;
	*canonical_locator_out = record->canonical_locator;
	return true;
}

/* Scalar evidence from the already-copied page/request only: no extra lookup,
 * pin or authority acquisition. A missing local record is not by itself proof
 * that retention expired. The existing refusal remains identical at the cap. */
static void
cr_r4_history_refusal_log(const char *site, uint32 slot_index,
						  const ClusterR4CrSlotExtension *extension, SCN watermark, const UBA *uba,
						  TransactionId xid)
{
	static uint32 emitted;
	const BufferTag *tag = &extension->route_proof.tag;

	if (emitted >= 64)
		return;
	emitted++;
	elog(LOG,
		 "PGRAC_FAMILY=R4_HISTORY_REFUSAL PGRAC_SITE=%s node=%d slot=%u"
		 " slot_generation=" UINT64_FORMAT " builder_incarnation=" UINT64_FORMAT
		 " tag=%u/%u/%u/%u/%u read_scn=" UINT64_FORMAT " watermark=" UINT64_FORMAT
		 " build_steps=%u uba=" UINT64_FORMAT "/" UINT64_FORMAT " xid=%u detail_limit=%d",
		 site, cluster_node_id, slot_index, extension->slot_generation,
		 extension->owner.builder_incarnation, tag->spcOid, tag->dbOid, tag->relNumber,
		 (unsigned)tag->forkNum, tag->blockNum, (uint64)extension->route_proof.read_scn,
		 (uint64)watermark, extension->build_steps, uba == NULL ? (uint64)0 : uba->raw[0],
		 uba == NULL ? (uint64)0 : uba->raw[1], xid, emitted == 64);
}

static bool
cr_r4_history_reselect(ClusterR4CrBuildContext *context, Page page, SCN read_scn)
{
	int count = cluster_cr_collect_candidate_chains(
		ClusterPageGetItlSlots(page), read_scn, context->candidates, CLUSTER_ITL_INITRANS_DEFAULT);
	int i;

	if (count > 1)
		qsort(context->candidates, count, sizeof(context->candidates[0]),
			  cluster_cr_chain_cmp_by_write_scn_desc);
	context->candidate_count = (uint8)count;
	context->candidate_cursor = 0;
	context->covered_candidate_mask = 0;
	context->have_previous = false;
	for (i = 0; i < count; i++) {
		ClusterTxResolveReason reason;

		if (!cluster_tx_locator_from_itl(page, context->candidates[i].slot_idx,
										 &context->locators[i], &reason))
			return false;
		context->locators[i].tt_wrap = TT_WRAP_INVALID;
	}
	return true;
}

/* Only private construction observations: no TT ownership or retention pin. */
typedef struct CrR4OperationTop {
	struct CrR4OperationTop *next;
	ClusterTTSlotPhysicalLocator canonical;
	uint32 generation;
	ClusterTxOutcome terminal_outcome;
	FullTransactionId top_fxid;
	uint64 last_sequence;
	SCN terminal_commit_scn;
	bool conservative;
} CrR4OperationTop;

/* Private evidence of a qualified kept INSERT, not a TID reuse grant. */
typedef struct CrR4KeptCreation {
	struct CrR4KeptCreation *next;
	TransactionId xid;
	OffsetNumber offset;
	uint16 length;
} CrR4KeptCreation;

typedef struct CrR4OperationContext {
	bool enabled, reading, operation_mode, pending;
	bool current_owner;
	uint64 slot_generation, builder_incarnation;
	ClusterR4CrRouteProof route;
	ClusterSemanticAdmissionToken admission;
	ClusterRuntimeVisibilityOperationRead read;
	ClusterTxLocator locator;
	PGAlignedBlock source;
	ClusterSavepointReaderIdentity identity;
	ClusterTxResolution top;
	ClusterSavepointReaderRecord operation;
	CrR4OperationTop *tops;
	CrR4KeptCreation *creations;
	uint32 top_count;
	PGAlignedBlock *walk;
	char *result_page; /* Borrowed only during the admitted call. */
	uint8 kept_slots;
	uint8 kept_offsets[(MaxHeapTuplesPerPage + 7) / 8];
	uint8 kept_modified_offsets[(MaxHeapTuplesPerPage + 7) / 8];
} CrR4OperationContext;

static CrR4OperationContext CrR4OperationContexts[CLUSTER_LMS_CR_SLOTS];

static void
cr_r4_operation_abort(CrR4OperationContext *operation)
{
	if (operation->reading)
		cluster_runtime_visibility_operation_origin_read_cancel(&operation->read);
	operation->reading = operation->pending = false;
	operation->current_owner = false;
	operation->result_page = NULL;
}

static void
cr_r4_operation_forget(uint32 index, uint64 generation)
{
	CrR4OperationContext *operation = &CrR4OperationContexts[index];

	if (!operation->enabled || operation->slot_generation != generation)
		return;
	cr_r4_operation_abort(operation);
	while (operation->tops != NULL) {
		CrR4OperationTop *next = operation->tops->next;

		pfree(operation->tops);
		operation->tops = next;
	}
	while (operation->creations != NULL) {
		CrR4KeptCreation *next = operation->creations->next;

		pfree(operation->creations);
		operation->creations = next;
	}
	if (operation->walk != NULL)
		pfree(operation->walk);
	memset(operation, 0, sizeof(*operation));
}

long
cluster_cr_build_on_holder_wait_timeout(long idle_timeout_ms)
{
	if (idle_timeout_ms <= 0)
		return idle_timeout_ms;
	for (unsigned i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
		if (CrR4OperationContexts[i].pending)
			return Min(idle_timeout_ms, 1L);
	return idle_timeout_ms;
}

/* The original current adapter permits one guard per resource per process.
 * Keep a reader's three phases together; competing slots wait locally. */
bool
cluster_cr_build_on_holder_origin_active(void)
{
	for (unsigned i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
		if (CrR4OperationContexts[i].current_owner)
			return true;
	return false;
}

/* A repeated canonical slot must keep the same complete transaction identity
 * and visit older operation sequences. ACTIVE remains conservative even if a
 * later original TT sample observes COMMITTED for this construction. A
 * terminal sample cannot change under that same complete identity. */
static bool
cr_r4_operation_top(CrR4OperationContext *operation, uint32 limit, bool *conservative)
{
	const ClusterSavepointReaderIdentity *identity = &operation->identity;
	CrR4OperationTop *top;

	for (top = operation->tops; top != NULL; top = top->next) {
		if (top->canonical.segment_id != identity->canonical.segment_id
			|| top->canonical.xid != identity->canonical.xid
			|| top->canonical.slot_offset != identity->canonical.slot_offset
			|| top->canonical.wrap != identity->canonical.wrap
			|| top->generation != identity->canonical_generation)
			continue;
		if (!FullTransactionIdEquals(top->top_fxid, identity->top_fxid)
			|| identity->operation_sequence >= top->last_sequence)
			return false;
		break;
	}
	if (top == NULL) {
		if (operation->top_count >= limit)
			return false;
		top = MemoryContextAllocZero(TopMemoryContext, sizeof(*top));
		top->canonical = identity->canonical;
		top->generation = identity->canonical_generation;
		top->top_fxid = identity->top_fxid;
		top->next = operation->tops;
		operation->tops = top;
		operation->top_count++;
	}
	if (top->terminal_outcome != CLUSTER_TX_UNKNOWN
		&& (top->terminal_outcome != operation->top.outcome
			|| (top->terminal_outcome == CLUSTER_TX_COMMITTED
				&& top->terminal_commit_scn != operation->top.commit_scn)))
		return false;
	if (operation->top.outcome != CLUSTER_TX_IN_PROGRESS) {
		top->terminal_outcome = operation->top.outcome;
		top->terminal_commit_scn = operation->top.commit_scn;
	}
	top->last_sequence = identity->operation_sequence;
	top->conservative |= operation->top.outcome == CLUSTER_TX_IN_PROGRESS;
	*conservative = top->conservative;
	return true;
}

/* Every tuple an inverse can change on this page. Disjointness is only a
 * conservative projection boundary, never a transaction visibility proof. */
static unsigned
cr_r4_operation_offsets(const UndoRecordHeader *record, const BufferTag *tag, const char *body,
						OffsetNumber offsets[2])
{
	unsigned count = 0;

	if (record->target_block == tag->blockNum)
		offsets[count++] = record->target_offset;
	if (record->record_type == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update;

		memcpy(&update, body, sizeof(update));
		if (update.new_block == tag->blockNum)
			offsets[count++] = update.new_offset;
	} else if (record->record_type != UNDO_RECORD_INSERT
			   && record->record_type != UNDO_RECORD_DELETE
			   && record->record_type != UNDO_RECORD_ITL)
		return 0;
	for (unsigned i = 0; i < count; i++)
		if (offsets[i] < FirstOffsetNumber || offsets[i] > MaxHeapTuplesPerPage)
			return 0;
	return count;
}

/* Two modifiers may share a TID only for the same immutable creation. Their
 * xmax/command/HOT headers differ, but the creator and stored value cannot. */
static bool
cr_r4_operation_same_creation(char walk_bytes[BLCKSZ], char result_bytes[BLCKSZ],
							  OffsetNumber offset)
{
	Page pages[2] = { (Page)walk_bytes, (Page)result_bytes };
	HeapTupleHeader tuples[2];
	uint16 lengths[2];
	const uint16 attributes = HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL | HEAP_HASOID_OLD;

	for (unsigned i = 0; i < lengthof(pages); i++) {
		PageHeader header = (PageHeader)pages[i];
		ItemId item;

		if (offset > PageGetMaxOffsetNumber(pages[i]))
			return false;
		item = PageGetItemId(pages[i], offset);
		if (!ItemIdIsNormal(item) || ItemIdGetLength(item) < SizeofHeapTupleHeader
			|| ItemIdGetOffset(item) < header->pd_upper
			|| (size_t)ItemIdGetOffset(item) + ItemIdGetLength(item) > header->pd_special)
			return false;
		lengths[i] = ItemIdGetLength(item);
		tuples[i] = (HeapTupleHeader)PageGetItem(pages[i], item);
		if (tuples[i]->t_hoff < SizeofHeapTupleHeader || tuples[i]->t_hoff > lengths[i]
			|| (tuples[i]->t_infomask & HEAP_XMAX_IS_MULTI) != 0)
			return false;
	}
	return lengths[0] == lengths[1] && tuples[0]->t_hoff == tuples[1]->t_hoff
		   && HeapTupleHeaderGetRawXmin(tuples[0]) == HeapTupleHeaderGetRawXmin(tuples[1])
		   && ((tuples[0]->t_infomask ^ tuples[1]->t_infomask) & attributes) == 0
		   && ((tuples[0]->t_infomask2 ^ tuples[1]->t_infomask2) & HEAP_NATTS_MASK) == 0
		   && memcmp((char *)tuples[0] + SizeofHeapTupleHeader,
					 (char *)tuples[1] + SizeofHeapTupleHeader, lengths[0] - SizeofHeapTupleHeader)
				  == 0;
}

/* A newer kept UPDATE/DELETE covers the prior xmax of this same version.
 * It cannot cover an older creation or an overlapping replacement TID. */
static bool
cr_r4_operation_covers_target(const CrR4OperationContext *operation, const UndoRecordHeader *record,
							  const BufferTag *tag, const OffsetNumber offsets[2], unsigned count)
{
	unsigned target_bit = record->target_offset - FirstOffsetNumber;

	if (operation->walk == NULL || record->target_block != tag->blockNum
		|| record->record_type == UNDO_RECORD_INSERT
		|| (operation->kept_modified_offsets[target_bit / 8] & (1U << (target_bit % 8))) == 0)
		return false;
	for (unsigned i = 0; i < count; i++) {
		unsigned bit = offsets[i] - FirstOffsetNumber;

		if (offsets[i] != record->target_offset
			&& (operation->kept_offsets[bit / 8] & (1U << (bit % 8))) != 0)
			return false;
	}
	return cr_r4_operation_same_creation(operation->walk->data, operation->result_page,
										 record->target_offset);
}

static bool
cr_r4_operation_creation_matches(const CrR4OperationContext *operation,
								 const CrR4KeptCreation *creation)
{
	Page page = (Page)operation->result_page;
	PageHeader header = (PageHeader)page;
	ItemId item;
	HeapTupleHeader tuple;

	if (creation->offset < FirstOffsetNumber || creation->offset > PageGetMaxOffsetNumber(page))
		return false;
	item = PageGetItemId(page, creation->offset);
	if (!ItemIdIsNormal(item) || ItemIdGetLength(item) < SizeofHeapTupleHeader
		|| ItemIdGetLength(item) != creation->length || ItemIdGetOffset(item) < header->pd_upper
		|| (size_t)ItemIdGetOffset(item) + ItemIdGetLength(item) > header->pd_special)
		return false;
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	return tuple->t_hoff >= SizeofHeapTupleHeader && tuple->t_hoff <= creation->length
		   && (tuple->t_infomask & HEAP_XMAX_IS_MULTI) == 0
		   && HeapTupleHeaderGetRawXmin(tuple) == creation->xid;
}

/* Record only the first kept operation at this offset, after its original
 * inverse and admission recheck. Later versions cannot replace this witness. */
static bool
cr_r4_operation_remember_creation(CrR4OperationContext *operation, const UndoRecordHeader *record)
{
	CrR4KeptCreation creation = { .xid = record->xid, .offset = record->target_offset };
	CrR4KeptCreation *stored;
	Page page = (Page)operation->result_page;
	UndoInsertPayload insert;
	unsigned bit = record->target_offset - FirstOffsetNumber;

	if (record->record_type != UNDO_RECORD_INSERT
		|| (operation->kept_offsets[bit / 8] & (1U << (bit % 8))) != 0)
		return true;
	if (record->target_offset > PageGetMaxOffsetNumber(page))
		return false;
	creation.length = ItemIdGetLength(PageGetItemId(page, record->target_offset));
	memcpy(&insert, record + 1, sizeof(insert));
	if (!cr_r4_operation_creation_matches(operation, &creation)
		|| (insert.inserted_tuple_len != 0 && insert.inserted_tuple_len != creation.length))
		return false;
	stored = MemoryContextAllocZero(TopMemoryContext, sizeof(*stored));
	*stored = creation;
	stored->next = operation->creations;
	operation->creations = stored;
	return true;
}

/* An absent walk TID is useful only with an explicit distinct kept creator. */
static bool
cr_r4_operation_covers_creation(const CrR4OperationContext *operation,
								const UndoRecordHeader *record, const BufferTag *tag)
{
	Page walk = operation->walk != NULL ? (Page)operation->walk->data : NULL;

	if (walk == NULL || record->record_type != UNDO_RECORD_INSERT
		|| record->target_block != tag->blockNum || record->target_offset < FirstOffsetNumber
		|| record->target_offset > PageGetMaxOffsetNumber(walk)
		|| ItemIdIsUsed(PageGetItemId(walk, record->target_offset)))
		return false;
	for (const CrR4KeptCreation *creation = operation->creations; creation != NULL;
		 creation = creation->next)
		if (creation->offset == record->target_offset && creation->xid != record->xid)
			return cr_r4_operation_creation_matches(operation, creation);
	return false;
}

/* A LIVE overlap needs an exact walk predecessor or a distinct kept creation.
 * The inverse's idempotent absent-creation success alone proves no lineage.
 * Reject unqualified MX before an inverse can clear its flag; raw numeric
 * equality is not a member proof. The inverse still checks raw identities. */
static bool
cr_r4_operation_projection_safe(const CrR4OperationContext *operation, char page_bytes[BLCKSZ],
								const UndoRecordHeader *record, const BufferTag *tag,
								const OffsetNumber offsets[2], unsigned count, bool keep,
								bool *covered_target)
{
	Page page = (Page)(operation->walk != NULL ? operation->walk->data : page_bytes);
	PageHeader header = (PageHeader)page;
	bool overlap = false;
	bool covered_creation = false;

	*covered_target = false;
	for (unsigned i = 0; i < count; i++) {
		unsigned bit = offsets[i] - FirstOffsetNumber;

		overlap |= (operation->kept_offsets[bit / 8] & (1U << (bit % 8))) != 0;
	}
	if (overlap) {
		covered_creation = cr_r4_operation_covers_creation(operation, record, tag);
		if (!keep && !covered_creation
			&& !cr_r4_operation_covers_target(operation, record, tag, offsets, count))
			return false;
		*covered_target = !keep || covered_creation;
	}
	for (unsigned i = 0; i < count; i++) {
		ItemId item;
		HeapTupleHeader tuple;

		if (offsets[i] > PageGetMaxOffsetNumber(page)
			|| !ItemIdIsNormal(PageGetItemId(page, offsets[i]))) {
			if (overlap && !covered_creation)
				return false;
			continue;
		}
		item = PageGetItemId(page, offsets[i]);
		if (ItemIdGetLength(item) < SizeofHeapTupleHeader
			|| ItemIdGetOffset(item) < header->pd_upper
			|| (size_t)ItemIdGetOffset(item) + ItemIdGetLength(item) > header->pd_special)
			return false;
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > ItemIdGetLength(item)
			|| (tuple->t_infomask & HEAP_XMAX_IS_MULTI) != 0)
			return false;
	}
	return true;
}

/* The whole record has already passed its original inverse on the walk.
 * Project only a disjoint replacement removal; leave the kept root intact. */
static bool
cr_r4_operation_project_covered(const CrR4OperationContext *operation,
								const UndoRecordHeader *record, const BufferTag *tag,
								const ClusterTxLocator *locator, char candidate[BLCKSZ])
{
	if (record->record_type == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update;

		memcpy(&update, record + 1, sizeof(update));
		if (update.new_block == tag->blockNum
			&& !cluster_cr_history_remove_creation(candidate, update.new_offset, record->xid, 0))
			return false;
	}
	if ((operation->kept_slots & (1U << locator->itl_slot_index)) == 0)
		ClusterPageGetItlSlots((Page)candidate)[locator->itl_slot_index]
			= ClusterPageGetItlSlots((Page)operation->walk->data)[locator->itl_slot_index];
	return true;
}

/* Walk every qualified record, but keep visible LIVE effects in the result.
 * A newer full modification may cover an older mutation of the same root.
 * The result's newest kept ITL is independent of the older walk cursor. */
static bool
cr_r4_operation_project(CrR4OperationContext *operation, ClusterR4CrSlotExtension *extension,
						const ClusterTxLocator *locator, const UndoRecordHeader *record,
						char page[BLCKSZ], bool keep)
{
	const BufferTag *tag = &extension->route_proof.tag;
	const char *body = (const char *)(record + 1);
	OffsetNumber offsets[2];
	unsigned count;
	bool covered_target;
	ClusterItlSlotData after;
	PGAlignedBlock candidate;

	count = cr_r4_operation_offsets(record, tag, body, offsets);
	if (count == 0
		|| !cr_r4_operation_projection_safe(operation, page, record, tag, offsets, count, keep,
											&covered_target))
		return false;
	if (operation->walk == NULL && !keep)
		return cluster_cr_history_inverse_operation_identity(page, tag, locator, locator->uba,
															 record, body, record->payload_length,
															 &operation->identity)
			   && cluster_semantic_activation_recheck(&operation->admission);
	if (operation->walk == NULL) {
		operation->walk = MemoryContextAllocZero(TopMemoryContext, sizeof(*operation->walk));
		memcpy(operation->walk->data, page, BLCKSZ);
	}
	page = operation->walk->data;
	after = ClusterPageGetItlSlots((Page)page)[locator->itl_slot_index];
	if (!cluster_cr_history_inverse_operation_identity(page, tag, locator, locator->uba, record,
													   body, record->payload_length,
													   &operation->identity))
		return false;
	if (keep) {
		if (!cluster_semantic_activation_recheck(&operation->admission)
			|| !cr_r4_operation_remember_creation(operation, record))
			return false;
		if ((operation->kept_slots & (1U << locator->itl_slot_index)) == 0)
			ClusterPageGetItlSlots((Page)operation->result_page)[locator->itl_slot_index] = after;
		operation->kept_slots |= 1U << locator->itl_slot_index;
		for (unsigned i = 0; i < count; i++) {
			unsigned bit = offsets[i] - FirstOffsetNumber;

			if ((operation->kept_offsets[bit / 8] & (1U << (bit % 8))) == 0
				&& offsets[i] == record->target_offset && record->target_block == tag->blockNum
				&& (record->record_type == UNDO_RECORD_UPDATE
					|| record->record_type == UNDO_RECORD_DELETE))
				operation->kept_modified_offsets[bit / 8] |= 1U << (bit % 8);
			operation->kept_offsets[bit / 8] |= 1U << (bit % 8);
		}
		return true;
	}
	memcpy(candidate.data, operation->result_page, BLCKSZ);
	if (covered_target) {
		if (!cr_r4_operation_project_covered(operation, record, tag, locator, candidate.data))
			return false;
	} else {
		ClusterPageGetItlSlots((Page)candidate.data)[locator->itl_slot_index] = after;
		if (!cluster_cr_history_inverse_operation_identity(
				candidate.data, tag, locator, locator->uba, record, body, record->payload_length,
				&operation->identity))
			return false;
	}
	if (!cluster_semantic_activation_recheck(&operation->admission))
		return false;
	if ((operation->kept_slots & (1U << locator->itl_slot_index)) != 0)
		ClusterPageGetItlSlots((Page)candidate.data)[locator->itl_slot_index]
			= ClusterPageGetItlSlots((Page)operation->result_page)[locator->itl_slot_index];
	memcpy(operation->result_page, candidate.data, BLCKSZ);
	return true;
}

static bool
cr_r4_operation_apply(CrR4OperationContext *operation, ClusterR4CrBuildContext *context,
					  ClusterTxLocator *locator, ClusterR4CrSlotExtension *extension,
					  char page[BLCKSZ], char record_bytes[BLCKSZ])
{
	UndoRecordHeader *record;
	size_t length;
	bool conservative, keep;

	if (operation->result_page == NULL
		|| memcmp(operation->result_page, operation->source.data, BLCKSZ) != 0
		|| operation->top.outcome == CLUSTER_TX_PREPARED
		|| (operation->top.outcome != CLUSTER_TX_IN_PROGRESS
			&& operation->top.outcome != CLUSTER_TX_COMMITTED
			&& operation->top.outcome != CLUSTER_TX_ABORTED)
		|| !cluster_savepoint_reader_extract_identity(&operation->identity,
													  &operation->top.locator_echo, record_bytes,
													  record_bytes, &length, &operation->operation)
		|| !cr_r4_operation_top(operation, context->max_steps, &conservative))
		return false;
	record = (UndoRecordHeader *)record_bytes;
	keep = !conservative
		   && operation->operation.operation.effect_state == CLUSTER_SAVEPOINT_EFFECT_LIVE
		   && operation->top.outcome == CLUSTER_TX_COMMITTED
		   && scn_time_cmp(operation->top.commit_scn, extension->route_proof.read_scn) <= 0;
	*locator = operation->top.locator_echo;
	if (!cr_r4_operation_project(operation, extension, locator, record, page, keep))
		return false;
	operation->operation_mode = true;
	context->record_format = 2;
	return true;
}

/* false is a typed refusal, including a foreign operation. The preliminary
 * getter only selects this branch; original current TT/DATA must requalify it. */
static ClusterR4CrBuildStepResult
cr_r4_operation_step(uint32 index, ClusterR4CrBuildContext *context, ClusterTxLocator *locator,
					 ClusterR4CrSlotExtension *extension, char page[BLCKSZ],
					 char record_bytes[BLCKSZ], bool *applied)
{
	CrR4OperationContext *operation = &CrR4OperationContexts[index];
	ClusterTxResolveReason reason;
	ClusterRuntimeVisibilityOperationStep step;

	*applied = false;
	if (!operation->enabled || operation->result_page == NULL || context->have_previous
		|| context->record_format == 1
		|| uba_origin_node_id(locator->uba) != (NodeId)cluster_node_id)
		return CLUSTER_R4_CR_STEP_FAIL;
	if (!operation->reading) {
		operation->locator = *locator;
		memcpy(operation->source.data, operation->result_page, BLCKSZ);
		if (!cluster_runtime_visibility_operation_origin_read_begin(
				&operation->read, locator, NULL,
				&ClusterPageGetItlSlots((Page)page)[locator->itl_slot_index],
				&extension->route_proof.tag, CLUSTER_TX_RESOLVE_VISIBILITY, &operation->admission,
				&reason))
			return CLUSTER_R4_CR_STEP_FAIL;
		operation->reading = true;
		return CLUSTER_R4_CR_STEP_PENDING;
	}
	if (memcmp(locator, &operation->locator, sizeof(*locator)) != 0)
		return CLUSTER_R4_CR_STEP_FAIL;
	if (!operation->current_owner && cluster_cr_build_on_holder_origin_active())
		return CLUSTER_R4_CR_STEP_PENDING;
	operation->current_owner = true;
	step = cluster_runtime_visibility_operation_origin_read_poll(
		&operation->read, &operation->identity, &operation->top, &operation->operation,
		record_bytes, &reason);
	if (step == CLUSTER_RUNTIME_VISIBILITY_OPERATION_PENDING)
		return CLUSTER_R4_CR_STEP_PENDING;
	operation->reading = false;
	operation->current_owner = false;
	if (step != CLUSTER_RUNTIME_VISIBILITY_OPERATION_COMPLETE
		|| !cr_r4_operation_apply(operation, context, locator, extension, page, record_bytes))
		return CLUSTER_R4_CR_STEP_FAIL;
	*applied = true;
	return CLUSTER_R4_CR_STEP_PENDING;
}

/* Once an operation is seen, its restored old head is not a read-SCN horizon.
 * Inspect every retained head; an old legacy survivor is explicitly refused
 * below, until the complete mixed survivor projection is available. */
static bool
cr_r4_operation_reselect(ClusterR4CrBuildContext *context, Page page)
{
	const ClusterItlSlotData *slots = ClusterPageGetItlSlots(page);
	unsigned count = 0;

	for (unsigned i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (slots[i].flags == ITL_FLAG_FREE)
			continue;
		if (!SCN_VALID(slots[i].write_scn))
			return false;
		context->candidates[count++]
			= (ClusterCRCandidateChain){ .slot_idx = i,
										 .xid = slots[i].xid,
										 .write_scn = slots[i].write_scn,
										 .undo_segment_head = slots[i].undo_segment_head };
	}
	qsort(context->candidates, count, sizeof(context->candidates[0]),
		  cluster_cr_chain_cmp_by_write_scn_desc);
	context->candidate_count = count;
	context->candidate_cursor = context->covered_candidate_mask = 0;
	context->have_previous = false;
	for (unsigned i = 0; i < count; i++) {
		if (!cluster_tx_locator_from_itl(page, context->candidates[i].slot_idx,
										 &context->locators[i], NULL))
			return false;
		context->locators[i].tt_wrap = TT_WRAP_INVALID;
	}
	return true;
}

static ClusterR4CrBuildStepResult
cr_r4_operation_continue(uint32 index, ClusterR4CrBuildContext *context, ClusterTxLocator *locator,
						 ClusterR4CrSlotExtension *extension, char page[BLCKSZ],
						 char record_bytes[BLCKSZ], CrR4UndoTrace *trace,
						 ClusterCrBuildReason *reason)
{
	CrR4OperationContext *operation = &CrR4OperationContexts[index];
	bool applied;

	*reason = CLUSTER_CR_BUILD_BAD_UNDO;
	if (cr_r4_operation_step(index, context, locator, extension, page, record_bytes, &applied)
		== CLUSTER_R4_CR_STEP_FAIL)
		return CLUSTER_R4_CR_STEP_FAIL;
	if (applied
		&& !cr_r4_operation_reselect(
			context, (Page)(operation->walk != NULL ? operation->walk->data : page)))
		return CLUSTER_R4_CR_STEP_FAIL;
	*reason = CLUSTER_CR_BUILD_NONE;
	if (!applied || context->candidate_count > 0)
		return CLUSTER_R4_CR_STEP_PENDING;
	if (trace != NULL)
		trace->page = (Page)operation->result_page;
	cr_r4_undo_trace(trace, "build.full", "OPERATION_WALK_COMPLETED", 1);
	return CLUSTER_R4_CR_STEP_FULL;
}

ClusterR4CrBuildStepResult
cluster_cr_build_on_holder_step_admitted(uint32 index, uint64 generation, bool foreign_ready,
										 ClusterR4CrSlotExtension *extension, char page[BLCKSZ],
										 const char foreign_page[BLCKSZ],
										 const ClusterSemanticAdmissionToken *admission,
										 ClusterCrBuildReason *reason)
{
	CrR4OperationContext *operation;
	ClusterR4CrBuildStepResult result;
	bool same_key;

	if (reason == NULL)
		return CLUSTER_R4_CR_STEP_FAIL;
	*reason = CLUSTER_CR_BUILD_PROTOCOL;
	if (index >= CLUSTER_LMS_CR_SLOTS || generation == 0 || extension == NULL)
		return CLUSTER_R4_CR_STEP_FAIL;
	operation = &CrR4OperationContexts[index];
	same_key = operation->enabled && operation->slot_generation == generation
			   && operation->builder_incarnation == extension->owner.builder_incarnation;
	if (admission == NULL || !admission->entered
		|| admission->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| admission->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| admission->formation_epoch != extension->route_proof.formation_epoch
		|| admission->record_generation != extension->route_proof.activation_generation
		|| !cluster_semantic_activation_recheck(admission)
		|| (operation->enabled
			&& (!same_key || memcmp(&operation->admission, admission, sizeof(*admission)) != 0
				|| memcmp(&operation->route, &extension->route_proof, sizeof(operation->route))
					   != 0))) {
		if (same_key)
			cr_r4_operation_abort(operation);
		return CLUSTER_R4_CR_STEP_FAIL;
	}
	if (!operation->enabled) {
		operation->enabled = true;
		operation->slot_generation = generation;
		operation->builder_incarnation = extension->owner.builder_incarnation;
		memcpy(&operation->admission, admission, sizeof(*admission));
		memcpy(&operation->route, &extension->route_proof, sizeof(operation->route));
	}
	operation->result_page = page;
	result = cluster_cr_build_on_holder_step(index, generation, foreign_ready, extension, page,
											 foreign_page, reason);
	operation->result_page = NULL;
	operation->pending = result == CLUSTER_R4_CR_STEP_PENDING;
	if (result == CLUSTER_R4_CR_STEP_FAIL || result == CLUSTER_R4_CR_STEP_RETRY)
		cr_r4_operation_abort(operation);
	return result;
}

/*
 * cluster_cr_build_on_holder_step -- build from the immutable slot page only.
 *
 * The current executable slice freezes candidate order plus every locator and
 * closes the natural zero-candidate case and holder-local transaction chains
 * in write_scn-descending order.  Foreign chains remain explicitly
 * fail-closed; they can never be mistaken for a completed CR image.  The
 * process-local context survives every typed result until exact terminal
 * cleanup calls forget().
 */
ClusterR4CrBuildStepResult
cluster_cr_build_on_holder_step(uint32 slot_index, uint64 slot_generation, bool foreign_undo_ready,
								ClusterR4CrSlotExtension *extension, char caller_page[BLCKSZ],
								const char foreign_undo_page[BLCKSZ],
								ClusterCrBuildReason *reason_out)
{
	char *result_page = caller_page;
	ClusterR4CrBuildContext *context;
	CrR4UndoTrace trace_storage, *trace = NULL;
	PageHeader page_header;
	bool supplied_foreign_record;
	int candidate_count = 0;
	int i;

	if (reason_out == NULL)
		return CLUSTER_R4_CR_STEP_FAIL;
	*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
	if (slot_index >= CLUSTER_LMS_CR_SLOTS || slot_generation == 0 || extension == NULL
		|| result_page == NULL || foreign_undo_page == NULL)
		return CLUSTER_R4_CR_STEP_FAIL;
	context = &CrR4BuildContexts[slot_index];
	if (extension->slot_generation != slot_generation || extension->owner.builder_incarnation == 0
		|| extension->flags != 0
		|| !cr_r4_bytes_zero(extension->reserved, sizeof(extension->reserved)))
		return CLUSTER_R4_CR_STEP_FAIL;
	if (context->in_use) {
		ClusterTxLocator *locator;

		if ((!foreign_undo_ready && !CrR4OperationContexts[slot_index].reading
			 && !CrR4OperationContexts[slot_index].operation_mode)
			|| context->slot_generation != slot_generation
			|| context->builder_incarnation != extension->owner.builder_incarnation
			|| context->candidate_cursor >= context->candidate_count)
			return CLUSTER_R4_CR_STEP_FAIL;
		locator = &context->locators[context->candidate_cursor];
		if (foreign_undo_ready
			&& !cr_r4_foreign_request_matches(slot_index, slot_generation, extension, context,
											  locator))
			return CLUSTER_R4_CR_STEP_FAIL;
	} else if (foreign_undo_ready)
		return CLUSTER_R4_CR_STEP_FAIL;

	if (CrR4OperationContexts[slot_index].walk != NULL)
		result_page = CrR4OperationContexts[slot_index].walk->data;

	if (cluster_trace_normal_stop) {
		memset(&trace_storage, 0, sizeof(trace_storage));
		trace = &trace_storage;
		trace->slot_index = slot_index;
		trace->extension = extension;
		trace->context = context;
		trace->page = (Page)result_page;
		trace->apply_record = trace->locator_reason = -1;
	}

	page_header = (PageHeader)result_page;
	if (!PageHasItl((Page)result_page) || page_header->pd_lower < SizeOfPageHeaderData
		|| page_header->pd_lower > page_header->pd_upper
		|| page_header->pd_upper > page_header->pd_special
		|| page_header->pd_special < SizeOfPageHeaderData || page_header->pd_special > BLCKSZ
		|| BLCKSZ - page_header->pd_special < CLUSTER_ITL_SPECIAL_SIZE) {
		cr_r4_undo_trace(trace, "build.reject", "PAGE_LAYOUT", 0);
		*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
		return CLUSTER_R4_CR_STEP_FAIL;
	}


	if (!context->in_use) {
		SCN recycle_watermark_scn
			= ClusterPageGetItlHeader((Page)result_page)->itl_recycle_watermark_scn;

		/* A surviving candidate cannot restore history already discarded by
		 * another slot. FULL must obey the consumer's same snapshot boundary. */
		if (SCN_VALID(recycle_watermark_scn)
			&& scn_time_cmp(recycle_watermark_scn, extension->route_proof.read_scn) > 0) {
			*reason_out = CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD;
			cr_r4_undo_trace(trace, "build.reject", "PAGE_WATERMARK", 0);
			cr_r4_history_refusal_log("PAGE_WATERMARK", slot_index, extension,
									  recycle_watermark_scn, NULL, InvalidTransactionId);
			return CLUSTER_R4_CR_STEP_FAIL;
		}
		memset(context, 0, sizeof(*context));
		context->slot_generation = slot_generation;
		context->builder_incarnation = extension->owner.builder_incarnation;
		candidate_count = cluster_cr_collect_candidate_chains(
			ClusterPageGetItlSlots((Page)result_page), extension->route_proof.read_scn,
			context->candidates, CLUSTER_ITL_INITRANS_DEFAULT);
		if (candidate_count > 1)
			qsort(context->candidates, candidate_count, sizeof(context->candidates[0]),
				  cluster_cr_chain_cmp_by_write_scn_desc);
		context->candidate_count = (uint8)candidate_count;
		context->in_use = true;

		if (candidate_count == 0) {
			cr_r4_undo_trace(trace, "build.full", "ZERO_CANDIDATES", 1);
			*reason_out = CLUSTER_CR_BUILD_NONE;
			return CLUSTER_R4_CR_STEP_FULL;
		}
		for (i = 0; i < candidate_count; i++) {
			ClusterTxResolveReason locator_reason = CLUSTER_TX_RESOLVE_PROTOCOL;

			if (!cluster_tx_locator_from_itl((Page)result_page, context->candidates[i].slot_idx,
											 &context->locators[i], &locator_reason)) {
				cr_r4_undo_trace(trace, "build.reject", "CANDIDATE_LOCATOR", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_LOCATOR;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			/* The immutable page retains its own ABA incarnation.  That
			 * number cannot predict the independently recycled TT slot. */
			context->locators[i].tt_wrap = TT_WRAP_INVALID;
		}
		if (cluster_cr_chain_walk_max_steps <= 0) {
			cr_r4_undo_trace(trace, "build.reject", "CHAIN_LIMIT_CONFIG", 0);
			*reason_out = CLUSTER_CR_BUILD_CHAIN_LIMIT;
			return CLUSTER_R4_CR_STEP_FAIL;
		}
		context->max_steps = (uint32)cluster_cr_chain_walk_max_steps;
		context->visited_ubas
			= MemoryContextAllocZero(TopMemoryContext, sizeof(UBA) * (Size)context->max_steps);
	}
	cr_r4_undo_trace(trace, "build.begin", "FROZEN_CONTEXT", -1);
	supplied_foreign_record = foreign_undo_ready;

	while (context->candidate_cursor < context->candidate_count) {
		ClusterTxLocator *locator = &context->locators[context->candidate_cursor];
		ClusterTxResolveReason locator_reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		PGAlignedBlock record_buffer;
		BufferTag target_tag;
		UBA current_uba = supplied_foreign_record ? context->pending_locator.uba : locator->uba;
		NodeId origin;

		/* Only a frozen head already validated in an earlier chain may be
		 * skipped. A repeated predecessor within a chain is still a cycle. */
		if ((context->covered_candidate_mask & (1U << context->candidate_cursor)) != 0) {
			Assert(!supplied_foreign_record && !context->have_previous);
			context->candidate_cursor++;
			continue;
		}

		for (;;) {
			UndoRecordHeader *record;
			size_t record_length;
			ClusterTxLocator canonical_locator;
			uint32 visited_index;
			bool apply_record;
			bool chain_complete;
			bool horizon_reached;
			bool record_from_foreign_page = supplied_foreign_record;
			bool record_from_operation = CrR4OperationContexts[slot_index].reading;
			bool terminal;
			uint32 foreign_segment;
			uint32 foreign_block;
			uint16 foreign_tt_offset;
			uint16 foreign_row;

			if (trace != NULL) {
				trace->locator = *locator;
				trace->locator_known = trace->uba_known = true;
				trace->uba = current_uba;
				trace->read_returned = trace->record_header_known = false;
				trace->record_length = 0;
				trace->resident_foreign = record_from_foreign_page;
				trace->apply_record = trace->locator_reason = -1;
			}
			origin = uba_origin_node_id(current_uba);
			if (origin == InvalidNodeId) {
				cr_r4_undo_trace(trace, "undo.reject", "UBA_ORIGIN", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			if (!record_from_foreign_page && !record_from_operation) {
				if (extension->build_steps >= context->max_steps) {
					cr_r4_undo_trace(trace, "undo.reject", "CHAIN_LIMIT", 0);
					*reason_out = CLUSTER_CR_BUILD_CHAIN_LIMIT;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				extension->build_steps++;
				for (visited_index = 0; visited_index < context->visited_count; visited_index++) {
					if (cluster_undo_record_uba_equal(context->visited_ubas[visited_index],
													  current_uba)) {
						cr_r4_undo_trace(trace, "undo.reject", "VISITED_UBA", 0);
						*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
						return CLUSTER_R4_CR_STEP_FAIL;
					}
				}
				if (context->visited_count >= context->max_steps) {
					*reason_out = CLUSTER_CR_BUILD_CHAIN_LIMIT;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				context->visited_ubas[context->visited_count++] = current_uba;
			}
			if (origin != (NodeId)cluster_node_id && !record_from_foreign_page
				&& !record_from_operation) {
				if (!uba_decode(current_uba, &foreign_segment, &foreign_block, &foreign_tt_offset,
								&foreign_row)
					|| foreign_block == 0
					|| cluster_cr_r4_dependency_request_id(slot_index, slot_generation,
														   extension->build_steps)
						   == 0
					|| context->pending_locator_valid || extension->foreign_request_id != 0
					|| !UBA_is_invalid(extension->foreign_uba)
					|| extension->origin_formation_epoch != 0 || extension->origin_live_hwm_lsn != 0
					|| extension->origin_tt_generation != 0
					|| SCN_VALID(extension->origin_authority_scn)
					|| extension->foreign_origin_node != 0 || extension->foreign_segment_id != 0
					|| extension->foreign_block_no != 0
					|| TransactionIdIsValid(extension->foreign_xid) || extension->foreign_wrap != 0
					|| extension->foreign_tt_slot_offset != 0
					|| extension->foreign_row_offset != 0) {
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}

				extension->foreign_request_id = cluster_cr_r4_dependency_request_id(
					slot_index, slot_generation, extension->build_steps);
				extension->foreign_uba = current_uba;
				extension->origin_formation_epoch = extension->route_proof.formation_epoch;
				extension->foreign_origin_node = (int32)origin;
				extension->foreign_segment_id = foreign_segment;
				extension->foreign_block_no = foreign_block;
				extension->foreign_xid = locator->xid;
				extension->foreign_wrap = locator->tt_wrap;
				extension->foreign_tt_slot_offset = foreign_tt_offset;
				extension->foreign_row_offset = foreign_row;
				context->pending_locator = *locator;
				context->pending_locator.uba = current_uba;
				context->pending_locator_valid = true;
				cr_r4_undo_trace(trace, "undo.need", "PREPARED_FOREIGN_REQUEST", -1);
				*reason_out = CLUSTER_CR_BUILD_NONE;
				return CLUSTER_R4_CR_STEP_NEED_UNDO;
			}
			if (origin == (NodeId)cluster_node_id && record_from_foreign_page) {
				cr_r4_undo_trace(trace, "undo.reject", "LOCAL_ORIGIN_FOREIGN_PAGE", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}

			if (record_from_operation)
				return cr_r4_operation_continue(slot_index, context, locator, extension,
												result_page, record_buffer.data, trace, reason_out);
			cr_r4_undo_trace(trace, "undo.read.begin", "COPY_BOUNDARY", -1);
			if (record_from_foreign_page) {
				record_length = 0;
				if (cluster_cr_r4_extract_resident_record(
						foreign_undo_page, &context->pending_locator, record_buffer.data,
						&record_length, &canonical_locator)
					&& (extension->foreign_wrap == TT_WRAP_INVALID
						|| extension->foreign_wrap == canonical_locator.tt_wrap)) {
					context->pending_locator = canonical_locator;
					*locator = canonical_locator;
				} else
					record_length = 0;
			} else
				record_length = cluster_undo_get_record(current_uba, record_buffer.data,
														sizeof(record_buffer.data));
			if (trace != NULL) {
				trace->locator = *locator;
				trace->read_returned = true;
				trace->record_length = record_length;
				trace->record_header_known = record_length >= sizeof(UndoRecordHeader)
					&& record_length <= sizeof(record_buffer.data);
				if (trace->record_header_known)
					memcpy(&trace->record, record_buffer.data, sizeof(trace->record));
			}
			cr_r4_undo_trace(trace, "undo.read.end", "COPIED_BYTES_UNVALIDATED", -1);
			if (record_length == 0) {
				cr_r4_undo_trace(trace, "record.missing", "COPY_RETURNED_ZERO", 0);
				*reason_out = record_from_foreign_page ? CLUSTER_CR_BUILD_BAD_UNDO
													   : CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD;
				if (!record_from_foreign_page)
					cr_r4_history_refusal_log(
						"LOCAL_RECORD_UNAVAILABLE", slot_index, extension,
						ClusterPageGetItlHeader((Page)result_page)->itl_recycle_watermark_scn,
						&current_uba, locator->xid);
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			if (record_length < sizeof(UndoRecordHeader)) {
				cr_r4_undo_trace(trace, "record.header.reject", "SHORT_HEADER", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			record = (UndoRecordHeader *)record_buffer.data;
			if ((record->flags & UNDO_REC_FLAG_HAS_OPERATION) != 0) {
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				if (record_from_foreign_page)
					return CLUSTER_R4_CR_STEP_FAIL;
				return cr_r4_operation_continue(slot_index, context, locator, extension,
												result_page, record_buffer.data, trace, reason_out);
			}
			if (CrR4OperationContexts[slot_index].walk != NULL
				|| (CrR4OperationContexts[slot_index].operation_mode
					&& scn_time_cmp(
						   ClusterPageGetItlSlots((Page)result_page)[locator->itl_slot_index]
							   .write_scn,
						   extension->route_proof.read_scn)
						   <= 0)) {
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			record = (UndoRecordHeader *)record_buffer.data;
			terminal = UBA_is_invalid(record->prev_uba);
			if (context->record_format == 0)
				context->record_format
					= (record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) != 0 ? 2 : 1;
			if (((record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) != 0)
				!= (context->record_format == 2)) {
				cr_r4_undo_trace(trace, "record.format.reject", "MIXED_RECORD_FORMAT", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			if (!context->have_previous && locator->tt_wrap == TT_WRAP_INVALID) {
				if (!cr_r4_canonical_record_locator(locator, current_uba, record,
													&canonical_locator)) {
					cr_r4_undo_trace(trace, "record.canonical.reject", "CANONICAL_LOCATOR", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				*locator = canonical_locator;
			}
			if (trace != NULL)
				trace->locator = *locator;
			if (record_length != sizeof(UndoRecordHeader) + record->payload_length
				|| (record->flags & ~UNDO_REC_FLAG_HAS_ITL_HISTORY)
					   != (terminal ? UNDO_REC_FLAG_FIRST_IN_TX : 0)
				|| !SCN_VALID(record->write_scn)
				|| (context->have_previous ? !cluster_undo_record_validate_prev_edge(
												 locator, context->immediate_previous_uba,
												 &context->immediate_previous_record, current_uba,
												 record, &locator_reason)
										   : !cluster_undo_record_validate_identity(
												 locator, current_uba, record, &locator_reason))) {
				if (trace != NULL)
					trace->locator_reason = (int)locator_reason;
				cr_r4_undo_trace(trace, "record.validate.reject", "RECORD_VALIDATION", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			if (trace != NULL)
				trace->locator_reason = (int)locator_reason;
			cr_r4_undo_trace(trace, "record.validated", "ORIGINAL_VALIDATOR_RETURN_TRUE", 1);
			if (context->record_format == 2) {
				cr_r4_undo_trace(trace, "history.inverse.begin", "PAGE_HISTORY", -1);
				if (!cluster_cr_history_inverse(
						result_page, &extension->route_proof.tag, locator, current_uba, record,
						record_buffer.data + sizeof(*record), record->payload_length)) {
					cr_r4_undo_trace(trace, "history.inverse.reject", "INVERSE_RETURN_FALSE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				cr_r4_undo_trace(trace, "history.inverse.end", "INVERSE_RETURN_TRUE", 1);
				if (record_from_foreign_page) {
					cr_r4_clear_foreign_request(extension, context);
					supplied_foreign_record = false;
				}
				if (!(CrR4OperationContexts[slot_index].operation_mode
						  ? cr_r4_operation_reselect(context, (Page)result_page)
						  : cr_r4_history_reselect(context, (Page)result_page,
												   extension->route_proof.read_scn))) {
					*reason_out = CLUSTER_CR_BUILD_BAD_LOCATOR;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				break; /* reselect after every page operation, not an entire xid */
			}
			horizon_reached = scn_time_cmp(record->write_scn, extension->route_proof.read_scn) <= 0;
			if (!RelFileNumberIsValid(record->target_locator.relNumber)
				|| !OffsetNumberIsValid(record->target_offset)) {
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}
			InitBufferTag(&target_tag, &record->target_locator, record->target_fork,
						  record->target_block);
			apply_record
				= !horizon_reached && BufferTagsEqual(&target_tag, &extension->route_proof.tag);
			if (trace != NULL)
				trace->apply_record = apply_record;
			cr_r4_undo_trace(trace, "legacy.inspect", "VALIDATED_RECORD", -1);
			switch (record->record_type) {
			case UNDO_RECORD_INSERT: {
				const UndoInsertPayload *payload
					= (const UndoInsertPayload *)(record_buffer.data + sizeof(UndoRecordHeader));

				if (record->payload_length != sizeof(*payload)
					|| (apply_record
						&& !cluster_cr_apply_insert_inverse(result_page, record, payload))) {
					cr_r4_undo_trace(trace, "legacy.reject", "INSERT_PAYLOAD_OR_INVERSE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				break;
			}
			case UNDO_RECORD_UPDATE: {
				const UndoUpdatePayload *payload
					= (const UndoUpdatePayload *)(record_buffer.data + sizeof(UndoRecordHeader));
				const char *old_tuple;
				const HeapTupleHeaderData *old_header;
				bool successor_absent;
				bool successor_present;

				if (record->payload_length < sizeof(*payload) || payload->flags != 0
					|| payload->old_tuple_offset != sizeof(*payload)
					|| payload->old_tuple_length < SizeofHeapTupleHeader
					|| (size_t)payload->old_tuple_offset + payload->old_tuple_length
						   != record->payload_length) {
					cr_r4_undo_trace(trace, "legacy.reject", "UPDATE_PAYLOAD_SHAPE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				/* Ordinary UPDATE records its replacement TID.  Validate that
				 * pair without treating it as authority to visit another page or
				 * delete an arbitrary occupant.  The existing target inverse and
				 * candidate-xmin prune own those effects.  Both-invalid remains
				 * the historical in-place representation. */
				successor_absent = payload->new_block == InvalidBlockNumber
								   && payload->new_offset == InvalidOffsetNumber;
				successor_present = payload->new_block != InvalidBlockNumber
									&& payload->new_offset >= FirstOffsetNumber
									&& payload->new_offset <= MaxHeapTuplesPerPage
									&& (payload->new_block != record->target_block
										|| payload->new_offset != record->target_offset);
				old_tuple = (const char *)payload + payload->old_tuple_offset;
				old_header = (const HeapTupleHeaderData *)old_tuple;
				if ((!successor_absent && !successor_present)
					|| old_header->t_hoff < SizeofHeapTupleHeader
					|| old_header->t_hoff > payload->old_tuple_length) {
					cr_r4_undo_trace(trace, "legacy.reject", "UPDATE_SUCCESSOR_OR_HEADER", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				if (apply_record
					&& !cluster_cr_apply_update_inverse(result_page, record, payload, old_tuple,
														payload->old_tuple_length)) {
					cr_r4_undo_trace(trace, "legacy.reject", "UPDATE_INVERSE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				break;
			}
			case UNDO_RECORD_DELETE: {
				const UndoDeletePayload *payload
					= (const UndoDeletePayload *)(record_buffer.data + sizeof(UndoRecordHeader));
				const char *full_tuple;

				if (record->payload_length < sizeof(*payload)
					|| payload->full_tuple_offset != sizeof(*payload)
					|| payload->full_tuple_length == 0
					|| (size_t)payload->full_tuple_offset + payload->full_tuple_length
						   != record->payload_length) {
					cr_r4_undo_trace(trace, "legacy.reject", "DELETE_PAYLOAD_SHAPE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				full_tuple = (const char *)payload + payload->full_tuple_offset;
				if (apply_record
					&& !cluster_cr_apply_delete_inverse(result_page, record, payload, full_tuple,
														payload->full_tuple_length)) {
					cr_r4_undo_trace(trace, "legacy.reject", "DELETE_INVERSE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				break;
			}
			case UNDO_RECORD_ITL: {
				const UndoItlPayload *payload
					= (const UndoItlPayload *)(record_buffer.data + sizeof(UndoRecordHeader));

				if (record->payload_length != sizeof(*payload)
					|| payload->itl_slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT
					|| (apply_record
						&& !cluster_cr_apply_itl_inverse(result_page, record, payload))) {
					cr_r4_undo_trace(trace, "legacy.reject", "ITL_PAYLOAD_SLOT_OR_INVERSE", 0);
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				break;
			}
			default:
				cr_r4_undo_trace(trace, "legacy.reject", "RECORD_TYPE", 0);
				*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
				return CLUSTER_R4_CR_STEP_FAIL;
			}

			cr_r4_undo_trace(trace, "legacy.accept", "ORIGINAL_SWITCH_COMPLETED", 1);

			/* Several page slots can anchor the same transaction chain. Mark
			 * an older head only after this record and its inverse are proven;
			 * neither matching xid alone nor an unvalidated UBA is coverage. */
			for (i = context->candidate_cursor + 1; i < context->candidate_count; i++) {
				if (!cluster_undo_record_uba_equal(context->candidates[i].undo_segment_head,
												   current_uba))
					continue;
				/* The page target is stamped before receipt APPLY; consuming
				 * the undo record takes a later SCN. These are ordered events,
				 * not equal identity fields. Exact UBA and canonical transaction
				 * identity remain mandatory; reversed order is still invalid. */
				if (scn_time_cmp(context->candidates[i].write_scn, record->write_scn) > 0
					|| !cr_r4_canonical_record_locator(&context->locators[i], current_uba, record,
													   &canonical_locator)) {
					*reason_out = CLUSTER_CR_BUILD_BAD_UNDO;
					return CLUSTER_R4_CR_STEP_FAIL;
				}
				context->covered_candidate_mask |= (uint8)(1U << i);
			}

			chain_complete = terminal || horizon_reached;
			if (chain_complete) {
				context->candidate_cursor++;
				context->have_previous = false;
				memset(&context->immediate_previous_uba, 0,
					   sizeof(context->immediate_previous_uba));
				memset(&context->immediate_previous_record, 0,
					   sizeof(context->immediate_previous_record));
			} else {
				context->immediate_previous_record = *record;
				context->immediate_previous_uba = current_uba;
				context->have_previous = true;
				current_uba = record->prev_uba;
			}
			if (record_from_foreign_page) {
				cr_r4_clear_foreign_request(extension, context);
				supplied_foreign_record = false;
			}
			if (chain_complete)
				break;
		}
	}

	if (trace != NULL) {
		trace->locator_known = trace->uba_known = false;
		trace->read_returned = trace->record_header_known = false;
		trace->apply_record = trace->locator_reason = -1;
	}
	if (context->record_format != 2)
		(void)cluster_cr_prune_post_snapshot_versions(result_page, context->candidates,
													  context->candidate_count);
	cr_r4_undo_trace(trace, "build.full", "ORIGINAL_WALK_COMPLETED", 1);
	*reason_out = CLUSTER_CR_BUILD_NONE;
	return CLUSTER_R4_CR_STEP_FULL;
}

bool
cluster_cr_build_on_holder_pending_locator(uint32 slot_index, uint64 slot_generation,
										   ClusterTxLocator *locator_out)
{
	ClusterR4CrBuildContext *context;

	if (locator_out != NULL)
		memset(locator_out, 0, sizeof(*locator_out));
	if (slot_index >= CLUSTER_LMS_CR_SLOTS || slot_generation == 0 || locator_out == NULL)
		return false;
	context = &CrR4BuildContexts[slot_index];
	if (!context->in_use || context->slot_generation != slot_generation
		|| !context->pending_locator_valid)
		return false;
	*locator_out = context->pending_locator;
	return true;
}

void
cluster_cr_build_on_holder_forget(uint32 slot_index, uint64 slot_generation)
{
	ClusterR4CrBuildContext *context;

	if (slot_index >= CLUSTER_LMS_CR_SLOTS || slot_generation == 0)
		return;
	cr_r4_operation_forget(slot_index, slot_generation);
	context = &CrR4BuildContexts[slot_index];
	if (!context->in_use || context->slot_generation != slot_generation)
		return;
	if (context->visited_ubas != NULL)
		pfree(context->visited_ubas);
	memset(context, 0, sizeof(*context));
}


/* ============================================================
 * Shmem region (L206 5-step)
 * ============================================================ */

Size
cluster_cr_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCRShared));
}

void
cluster_cr_shmem_init(void)
{
	bool found;
	int i;

	CRShared = ShmemInitStruct("ClusterCRShared", cluster_cr_shmem_size(), &found);

	if (!found) {
		pg_atomic_init_u64(&CRShared->cr_construct_count, 0);
		pg_atomic_init_u64(&CRShared->cr_snapshot_too_old_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cross_instance_unsupported_count, 0);
		pg_atomic_init_u64(&CRShared->cr_corruption_count, 0);
		pg_atomic_init_u64(&CRShared->cr_chain_walk_steps_sum, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_insert_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_update_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_delete_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_itl_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_hit_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_miss_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_evict_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_install_count, 0);
		pg_atomic_init_u64(&CRShared->cr_remote_full_count, 0);
		pg_atomic_init_u64(&CRShared->cr_remote_partial_count, 0);
		pg_atomic_init_u64(&CRShared->cr_remote_failed_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_full_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_partial_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_denied_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_undo_fetch_wire_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_undo_fetch_cache_hit_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_undo_fetch_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_undo_served_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_undo_denied_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_resolve_committed_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_resolve_aborted_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_resolve_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_wire_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_exact_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_below_horizon_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_inadmissible_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_verdict_served_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_verdict_denied_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_multi_verdict_served_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_multi_verdict_denied_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_fence_refused_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_underivable_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->vis_freshref_verdict_resolved_count, 0);
		pg_atomic_init_u64(&CRShared->vis_freshref_verdict_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->native_prehistory_covered_hw, 0);
		pg_atomic_init_u64(&CRShared->rtvis_native_prehistory_local_count, 0);
		pg_atomic_init_u64(&CRShared->native_prehistory_disabled, 0);
		LWLockInitialize(&CRShared->native_prehistory_lock, LWTRANCHE_CLUSTER_NATIVE_PREHISTORY);
		pg_atomic_init_u64(&CRShared->undo_authority_serve_hit_count, 0);
		pg_atomic_init_u64(&CRShared->undo_authority_fail_closed_count, 0);
		pg_atomic_init_u64(&CRShared->undo_authority_epoch_stale_reject_count, 0);
		pg_atomic_init_u64(&CRShared->undo_authority_scan_incomplete_reject_count, 0);
		pg_atomic_init_u64(&CRShared->undo_authority_multi_match_reject_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_invalid_scn_refuse_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_zero_match_refuse_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_srv_other_refuse_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_covers_refuse_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_multi_unresolvable_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_xmax_unprovable_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_xmin_overlay_verdict_ask_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_xmin_overlay_verdict_hit_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_multi_member_serve_ask_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_multi_member_serve_hit_count, 0);
		pg_atomic_init_u64(&CRShared->vis53r97_leg_live_upgrade_hit_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_resolved_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_recycled_invisible_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_invalid_or_ambiguous_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_scan_unavail_or_no_proof_count, 0);
		for (i = 0; i < CLUSTER_R4_OBSERVATION_EVENT_COUNT; i++)
			pg_atomic_init_u64(&CRShared->r4_event_counts[i], 0);
	}
}

static const ClusterShmemRegion cluster_cr_region = {
	.name = "pgrac cluster cr counters",
	.size_fn = cluster_cr_shmem_size,
	.init_fn = cluster_cr_shmem_init,
	.lwlock_count = 1, /* native_prehistory_lock (round-3b drain); counters lock-free */
	.owner_subsys = "cluster_cr",
	.reserved_flags = 0,
};

void
cluster_cr_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_region);
}


/* ============================================================
 * Counter accessors (spec-3.9 §2.1)
 * ============================================================ */

#define CR_COUNTER_ACCESSOR(fn, field)                                                             \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		if (CRShared == NULL)                                                                      \
			return 0;                                                                              \
		return pg_atomic_read_u64(&CRShared->field);                                               \
	}

CR_COUNTER_ACCESSOR(cluster_cr_construct_count, cr_construct_count)
CR_COUNTER_ACCESSOR(cluster_cr_snapshot_too_old_count, cr_snapshot_too_old_count)
CR_COUNTER_ACCESSOR(cluster_cr_cross_instance_unsupported_count,
					cr_cross_instance_unsupported_count)

/* PGRAC: spec-6.12b — CR-server side counter bump (LMS/LMON contexts). */
void
cluster_cr_server_stat_bump(ClusterCrServerStat which)
{
	if (CRShared == NULL)
		return;
	switch (which) {
	case CLUSTER_CR_SERVER_STAT_FULL:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_full_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_PARTIAL:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_partial_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_denied_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_UNDO_SERVED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_undo_served_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_UNDO_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_undo_denied_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_VERDICT_SERVED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_verdict_served_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_VERDICT_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_verdict_denied_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_SERVED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_multi_verdict_served_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_multi_verdict_denied_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_FENCE_REFUSED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_fence_refused_count, 1);
		break;
	}
}

/* PGRAC: spec-6.12i — requester-side undo-TT fetch outcome bumps (backend
 * context, cluster_runtime_visibility.c). */
void
cluster_rtvis_undo_fetch_note_wire(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_undo_fetch_wire_count, 1);
}

void
cluster_rtvis_undo_fetch_note_cache_hit(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_undo_fetch_cache_hit_count, 1);
}

void
cluster_rtvis_undo_fetch_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_undo_fetch_failclosed_count, 1);
}

/* PGRAC: spec-6.12i CP3 — resolution-verdict bumps (backend context). */
void
cluster_rtvis_resolve_note_committed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_resolve_committed_count, 1);
}

void
cluster_rtvis_resolve_note_aborted(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_resolve_aborted_count, 1);
}

void
cluster_rtvis_resolve_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_resolve_failclosed_count, 1);
}

/* PGRAC: spec-6.12i CP5 (D-i4) — origin-verdict leg bumps (backend context,
 * cluster_runtime_visibility.c). */
void
cluster_rtvis_verdict_note_wire(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_wire_count, 1);
}

void
cluster_rtvis_verdict_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_failclosed_count, 1);
}

void
cluster_rtvis_verdict_note_exact(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_exact_count, 1);
}

void
cluster_rtvis_verdict_note_below_horizon(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_below_horizon_count, 1);
}

void
cluster_rtvis_verdict_note_inadmissible(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_inadmissible_count, 1);
}

/* PGRAC: spec-6.15 D4 — underivable-origin refusal (classify_ref). */
void
cluster_rtvis_note_underivable_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_underivable_failclosed_count, 1);
}

/*
 * PGRAC: GCS-race round-2 RC-E — native-prehistory coverage latch.
 *
 * The setter runs exactly once per boot, in the startup process after the
 * post-recovery verify proved local pg_xact == sealed prehistory blob over
 * [oldestXid, native_hw).  Backends fork after (or read the atomic after) the
 * store, so a nonzero read is always a fully-verified high-water; readers
 * that observe 0 simply stay fail-closed (never wrong, at worst 53R97).
 */
void
cluster_cr_native_prehistory_latch(uint64 native_hw_full)
{
	if (CRShared == NULL)
		return;
	/* Round-3 P0-1: a disabled latch is void.  Store-then-recheck: if the
	 * one-way disable lands between the check and the store, the recheck
	 * zeroes the freshly stored value, so no reader can win a nonzero
	 * covered_hw after the disable is set. */
	if (pg_atomic_read_u64(&CRShared->native_prehistory_disabled) != 0)
		return;
	pg_atomic_write_u64(&CRShared->native_prehistory_covered_hw, native_hw_full);
	if (pg_atomic_read_u64(&CRShared->native_prehistory_disabled) != 0)
		pg_atomic_write_u64(&CRShared->native_prehistory_covered_hw, 0);
}

uint64
cluster_cr_native_prehistory_covered_hw(void)
{
	if (CRShared == NULL)
		return 0;
	if (pg_atomic_read_u64(&CRShared->native_prehistory_disabled) != 0)
		return 0;
	return pg_atomic_read_u64(&CRShared->native_prehistory_covered_hw);
}

/*
 * PGRAC: GCS-race round-3 P0-1 — one-way native-prehistory latch disable.
 *
 * Called by the LMON wrap barrier (own margin trigger, a peer's DISABLE
 * broadcast, or the 1Hz authority-flag mirror).  Order matters: the disable
 * marker is set BEFORE covered_hw is zeroed so a concurrent latch store
 * cannot resurrect a nonzero value past its own recheck.  Idempotent.
 *
 * GCS-race round-3b review P0 (drain): the stores run under the
 * native_prehistory_lock held EXCLUSIVE.  The hazard is REMOTE raw-xid
 * reuse -- the first epoch-1 xid is allocated on the wrapping peer and
 * writes only that peer's pg_xact, so there is NO local CLOG write a
 * barrier-based reader recheck could synchronize with; relaxed atomics
 * alone cannot prove a local consumer observes this latch drop in time.
 * The exclusive acquisition instead DRAINS every reader holding the lock
 * SHARED across its proof -> adopted-CLOG read -> verdict window: their
 * verdicts complete before this store (and the caller's ACK only leaves
 * AFTER we return, i.e. after the release), so they predate any epoch-1
 * allocation and are genuinely native; every reader arriving after the
 * release pairs with it via its SHARED acquisition and sees the latch
 * down -> fail-closed.  LWLock acquire/release provide the memory
 * ordering (PG's sanctioned primitives; no bare-barrier pairing claims).
 */
void
cluster_cr_native_prehistory_disable(void)
{
	if (CRShared == NULL)
		return;
	LWLockAcquire(&CRShared->native_prehistory_lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&CRShared->native_prehistory_disabled, 1);
	pg_atomic_write_u64(&CRShared->native_prehistory_covered_hw, 0);
	LWLockRelease(&CRShared->native_prehistory_lock);
}

/*
 * GCS-race round-3b — reader side of the consume/disable drain.
 *
 * classify_ref_guts holds this SHARED across the native-prehistory consume
 * window (provable check -> XactTruncationLock CLOG read -> verdict fill).
 * Callers already hold the buffer content lock; lock order is
 * content_lock -> native_prehistory_lock -> XidGenLock (inside
 * ReadNextFullTransactionId) -> XactTruncationLock -> SLRU ControlLock,
 * and no path acquires any of those while holding this lock exclusively
 * (the disable side takes ONLY this lock).
 */
void
cluster_cr_native_prehistory_reader_lock(void)
{
	if (CRShared != NULL)
		LWLockAcquire(&CRShared->native_prehistory_lock, LW_SHARED);
}

void
cluster_cr_native_prehistory_reader_unlock(void)
{
	if (CRShared != NULL)
		LWLockRelease(&CRShared->native_prehistory_lock);
}

bool
cluster_cr_native_prehistory_disabled(void)
{
	if (CRShared == NULL)
		return false;
	return pg_atomic_read_u64(&CRShared->native_prehistory_disabled) != 0;
}

bool
cluster_cr_native_origin_epoch0_provable(TransactionId xid)
{
	FullTransactionId next;

	if (CRShared == NULL || !TransactionIdIsNormal(xid)
		|| pg_atomic_read_u64(&CRShared->native_prehistory_disabled) != 0)
		return false;
	Assert(LWLockHeldByMeInMode(&CRShared->native_prehistory_lock, LW_SHARED));
	next = ReadNextFullTransactionId();
	return FullTransactionIdIsValid(next) && EpochFromFullTransactionId(next) == 0
		   && (uint64)xid < U64FromFullTransactionId(next) && cluster_xid_is_mine(xid);
}

void
cluster_rtvis_note_native_prehistory_local(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_native_prehistory_local_count, 1);
}

/* PGRAC: spec-5.22f D6-3 — fresh-remote-ITL-ref widening outcome bumps
 * (classify_ref_guts, backend context). */
void
cluster_vis_freshref_verdict_note_resolved(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis_freshref_verdict_resolved_count, 1);
}

void
cluster_vis_freshref_verdict_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis_freshref_verdict_failclosed_count, 1);
}

/* PGRAC: spec-5.22d D4-4 — dead-owner authority block0 serve bumps
 * (cluster_runtime_visibility.c precision chain, backend context). */
void
cluster_undo_authority_note_serve_hit(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->undo_authority_serve_hit_count, 1);
}

void
cluster_undo_authority_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->undo_authority_fail_closed_count, 1);
}

void
cluster_undo_authority_note_epoch_stale_reject(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->undo_authority_epoch_stale_reject_count, 1);
}

/* PGRAC: spec-5.22d A1 (D4-8) — complete-scan refusal attribution. */
void
cluster_undo_authority_note_scan_incomplete_reject(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->undo_authority_scan_incomplete_reject_count, 1);
}

void
cluster_undo_authority_note_multi_match_reject(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->undo_authority_multi_match_reject_count, 1);
}
/* PGRAC: spec-7.1 D0/D5 — 53R97 per-leg attribution bumps (see the struct
 * comment for each leg's meaning; server legs run in LMS drain context,
 * requester legs in backend context — all plain atomic adds). */
void
cluster_vis53r97_note_srv_invalid_scn(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_invalid_scn_refuse_count, 1);
}

void
cluster_vis53r97_note_srv_zero_match(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_zero_match_refuse_count, 1);
}

void
cluster_vis53r97_note_srv_other(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_srv_other_refuse_count, 1);
}

void
cluster_vis53r97_note_covers_refuse(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_covers_refuse_count, 1);
}

void
cluster_vis53r97_note_multi_unresolvable(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_multi_unresolvable_count, 1);
}

void
cluster_vis53r97_note_xmax_unprovable(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_xmax_unprovable_count, 1);
}

/* spec-7.1 D1 requester 半边: xmin overlay-miss verdict ask / hit. */
void
cluster_vis53r97_note_xmin_overlay_verdict_ask(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_xmin_overlay_verdict_ask_count, 1);
}

void
cluster_vis53r97_note_xmin_overlay_verdict_hit(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_xmin_overlay_verdict_hit_count, 1);
}

/* spec-7.1 D3-b requester 半边: foreign multixact member-verdict ask / hit. */
void
cluster_vis53r97_note_multi_member_serve_ask(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_multi_member_serve_ask_count, 1);
}

void
cluster_vis53r97_note_multi_member_serve_hit(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_multi_member_serve_hit_count, 1);
}

/* spec-7.1 D1 serve 半边: INVALID_SCN durable hit upgraded to positive ABORTED
 * via CLOG cross-check (the miss counterpart is note_srv_invalid_scn). */
void
cluster_vis53r97_note_live_upgrade_hit(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->vis53r97_leg_live_upgrade_hit_count, 1);
}
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_xmin_overlay_verdict_ask_count,
					vis53r97_leg_xmin_overlay_verdict_ask_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_xmin_overlay_verdict_hit_count,
					vis53r97_leg_xmin_overlay_verdict_hit_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_multi_member_serve_ask_count,
					vis53r97_leg_multi_member_serve_ask_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_multi_member_serve_hit_count,
					vis53r97_leg_multi_member_serve_hit_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_live_upgrade_hit_count,
					vis53r97_leg_live_upgrade_hit_count)
CR_COUNTER_ACCESSOR(cluster_cr_corruption_count, cr_corruption_count)
CR_COUNTER_ACCESSOR(cluster_cr_chain_walk_steps_sum, cr_chain_walk_steps_sum)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_insert_count, cr_inverse_insert_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_update_count, cr_inverse_update_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_delete_count, cr_inverse_delete_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_itl_count, cr_inverse_itl_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_hit_count, cr_cache_hit_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_miss_count, cr_cache_miss_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_evict_count, cr_cache_evict_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_install_count, cr_cache_install_count)

void
cluster_cr_r4_event_bump(uint32 event)
{
	if (CRShared != NULL && event < CLUSTER_R4_OBSERVATION_EVENT_COUNT)
		pg_atomic_fetch_add_u64(&CRShared->r4_event_counts[event], 1);
}

uint64
cluster_cr_r4_event_count(uint32 event)
{
	if (CRShared == NULL || event >= CLUSTER_R4_OBSERVATION_EVENT_COUNT)
		return 0;
	return pg_atomic_read_u64(&CRShared->r4_event_counts[event]);
}

/* spec-6.12b: CR-server data plane (6 counters). */
CR_COUNTER_ACCESSOR(cluster_cr_remote_full_count, cr_remote_full_count)
CR_COUNTER_ACCESSOR(cluster_cr_remote_partial_count, cr_remote_partial_count)
CR_COUNTER_ACCESSOR(cluster_cr_remote_failed_count, cr_remote_failed_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_full_count, cr_server_full_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_partial_count, cr_server_partial_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_denied_count, cr_server_denied_count)
/* spec-6.12i: undo-TT fetch data plane (5 counters). */
CR_COUNTER_ACCESSOR(cluster_rtvis_undo_fetch_wire_count, rtvis_undo_fetch_wire_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_undo_fetch_cache_hit_count, rtvis_undo_fetch_cache_hit_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_undo_fetch_failclosed_count, rtvis_undo_fetch_failclosed_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_undo_served_count, cr_server_undo_served_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_undo_denied_count, cr_server_undo_denied_count)
/* spec-6.12i CP3: resolution-verdict counters. */
CR_COUNTER_ACCESSOR(cluster_rtvis_resolve_committed_count, rtvis_resolve_committed_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_resolve_aborted_count, rtvis_resolve_aborted_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_resolve_failclosed_count, rtvis_resolve_failclosed_count)
/* spec-6.12i CP5 (D-i4): origin-verdict leg counters. */
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_wire_count, rtvis_verdict_wire_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_failclosed_count, rtvis_verdict_failclosed_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_exact_count, rtvis_verdict_exact_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_below_horizon_count, rtvis_verdict_below_horizon_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_inadmissible_count, rtvis_verdict_inadmissible_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_verdict_served_count, cr_server_verdict_served_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_verdict_denied_count, cr_server_verdict_denied_count)
/* spec-7.1 D3-b: origin multixact member-verdict serve counters. */
CR_COUNTER_ACCESSOR(cluster_cr_server_multi_verdict_served_count,
					cr_server_multi_verdict_served_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_multi_verdict_denied_count,
					cr_server_multi_verdict_denied_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_fence_refused_count, cr_server_fence_refused_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_underivable_failclosed_count, rtvis_underivable_failclosed_count)
/* spec-5.22f D6-3: fresh-remote-ITL-ref widening outcome counters. */
CR_COUNTER_ACCESSOR(cluster_vis_freshref_verdict_resolved_count,
					vis_freshref_verdict_resolved_count)
/* GCS-race round-2 RC-E: native-prehistory LOCAL routing counter. */
CR_COUNTER_ACCESSOR(cluster_rtvis_native_prehistory_local_count,
					rtvis_native_prehistory_local_count)
CR_COUNTER_ACCESSOR(cluster_vis_freshref_verdict_failclosed_count,
					vis_freshref_verdict_failclosed_count)
/* spec-5.22d D4-4/D4-5: dead-owner authority block0 serve counters. */
CR_COUNTER_ACCESSOR(cluster_undo_authority_serve_hit_count, undo_authority_serve_hit_count)
CR_COUNTER_ACCESSOR(cluster_undo_authority_fail_closed_count, undo_authority_fail_closed_count)
CR_COUNTER_ACCESSOR(cluster_undo_authority_epoch_stale_reject_count,
					undo_authority_epoch_stale_reject_count)
CR_COUNTER_ACCESSOR(cluster_undo_authority_scan_incomplete_reject_count,
					undo_authority_scan_incomplete_reject_count)
CR_COUNTER_ACCESSOR(cluster_undo_authority_multi_match_reject_count,
					undo_authority_multi_match_reject_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_invalid_scn_refuse_count,
					vis53r97_leg_invalid_scn_refuse_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_zero_match_refuse_count,
					vis53r97_leg_zero_match_refuse_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_srv_other_refuse_count,
					vis53r97_leg_srv_other_refuse_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_covers_refuse_count, vis53r97_leg_covers_refuse_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_multi_unresolvable_count,
					vis53r97_leg_multi_unresolvable_count)
CR_COUNTER_ACCESSOR(cluster_vis53r97_leg_xmax_unprovable_count, vis53r97_leg_xmax_unprovable_count)
/* spec-3.22 D3: xmax recycled-slot resolve outcome buckets. */
CR_COUNTER_ACCESSOR(cluster_cr_xmax_resolved_count, cr_xmax_resolved_count)
CR_COUNTER_ACCESSOR(cluster_cr_xmax_recycled_invisible_count, cr_xmax_recycled_invisible_count)
CR_COUNTER_ACCESSOR(cluster_cr_xmax_invalid_or_ambiguous_count, cr_xmax_invalid_or_ambiguous_count)
CR_COUNTER_ACCESSOR(cluster_cr_xmax_scan_unavail_or_no_proof_count,
					cr_xmax_scan_unavail_or_no_proof_count)


/* ============================================================
 * Scratch slot helpers
 * ============================================================ */

/*
 * Ensure the backend-local scratch page is allocated (once, in
 * TopMemoryContext so it survives the lifetime of the backend).
 */
static void
cr_scratch_ensure(void)
{
	if (cr_scratch == NULL)
		cr_scratch = MemoryContextAllocZero(TopMemoryContext, BLCKSZ);
}


/*
 * cr_coordinator_refuse_runtime_remote -- spec-5.57 D2: the single fail-closed
 *	refusal routine for a class③ (runtime-warm remote) CR origin.  Shared by the
 *	CR-side pre-check (the real boundary, on a real remote UBA) AND the W2 test
 *	injection (a synthetic class③), so the injection exercises the exact runtime
 *	behavior the production pre-check uses.
 *
 *	One class③ refusal is BOTH the boundary headline (cross_instance_cr_refused)
 *	and specifically the remote-undo-read leg (remote_undo_read_refused); both
 *	bump by construction.  The rare W1 header-mismatch belt bumps only
 *	cross_instance_cr_refused, giving the invariant
 *	cross_instance_cr_refused >= remote_undo_read_refused.
 *
 *	NON-DEGRADABLE (rule 8.A, §2.2): the ereport fires under ANY GUC value; the
 *	GUC only gates the advisory counters / probe / LOG-once.  This function never
 *	returns (it always ereport(ERROR)s).  The data plane is Stage 6 (#119).
 */
static void
pg_attribute_noreturn() cr_coordinator_refuse_runtime_remote(
	int origin_node, const char *refusal_site,
	const ClusterSemanticAdmissionResult *source_admission)
{
	/* Only the source-dispatch refusal has an admission result. Its output
	 * is consumable only on OK; in that branch this refusal means !fetched.
	 * Preserve unknown elsewhere without inspecting non-OK output bytes. */
	bool fetched_known = source_admission != NULL
						 && *source_admission == CLUSTER_SEMANTIC_ADMISSION_OK;

	if (cluster_cross_instance_cr_coordinator != CR_COORD_MODE_OFF) {
		cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_CR_REFUSED);
		cluster_cr_coordinator_stat_bump(CR_COORD_REMOTE_UNDO_READ_REFUSED);
	}
	/* D0 measure-leg: count the class③ hit (behavior unchanged -- still fails). */
	if (cluster_cross_instance_cr_probe)
		cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE);
	/* forward mode: LOG-once that the data plane is Stage 6 (L213: once per
	 * backend so the hot path is never flooded). */
	if (cluster_cross_instance_cr_coordinator == CR_COORD_MODE_FORWARD) {
		static bool forward_logged = false;

		if (!forward_logged) {
			forward_logged = true;
			elog(LOG, "cluster.cross_instance_cr_coordinator=forward is a contract "
					  "placeholder: cross-instance CR/undo data plane lands in Stage 6 "
					  "(#119); reads stay fail-closed (Spec: spec-5.57)");
		}
	}
	ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
					errmsg("cluster CR cross-instance UBA encountered at the remote-undo-read "
						   "leg (origin_node_id=%d, local=%d)",
						   origin_node, cluster_node_id),
					errdetail("refusal_site=%s source_admission_known=%d source_admission=%d "
							  "source_fetched_known=%d source_fetched=%d",
							  refusal_site, source_admission != NULL,
							  source_admission != NULL ? (int)*source_admission : -1,
							  fetched_known, fetched_known ? 0 : -1),
					errhint("Own-instance CR only unless the origin was materialized by merged "
							"recovery; the runtime cross-instance CR/undo data plane lands in "
							"Stage 6 (#119 undo-block Cache Fusion); see Spec: spec-5.57.")));
}


/* ============================================================
 * Test injection hooks (spec-3.9 Step 7; SKIP-style precondition)
 * ============================================================ */

/*
 * cr_check_error_injections -- if a CR error injection point is armed, raise
 *	the CR code's OWN precise SQLSTATE (NOT the framework's generic XX000).
 *	Called at the top of the chain walker so it fires deterministically
 *	regardless of the actual undo chain (spec-3.9 v0.4 F8/F10).
 */
static void
cr_check_error_injections(void)
{
	uint64 param = 0;

	if (cluster_cr_injection_armed("cr_snapshot_too_old", &param))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
						errmsg("snapshot too old: CR cannot reconstruct (injected; segment %u)",
							   (uint32)param),
						errhint("test injection cr_snapshot_too_old; disarm with "
								"cluster_inject_fault('cr_snapshot_too_old','none',0).")));

	if (cluster_cr_injection_armed("cr_cross_instance", &param))
		/* spec-5.57 D2/D3: synthetic class③ refusal -- drive the SAME fail-closed
		 * routine the production pre-check uses (53R9G + both coordinator counters
		 * + probe/forward), so the TAP injection legs exercise the real boundary
		 * behavior deterministically (param = synthetic origin_node_id). */
		cr_coordinator_refuse_runtime_remote((int)param, "injection", NULL);

	if (cluster_cr_injection_armed("cr_corruption", &param)) {
		const char *kind = (param == 1)	  ? "uba_decode"
						   : (param == 2) ? "crc"
						   : (param == 3) ? "unknown_record_type"
						   : (param == 4) ? "chain_break"
										  : "unknown";

		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR undo chain corruption (injected; kind=%s)", kind),
						errhint("test injection cr_corruption param 1..4.")));
	}
}


/* ============================================================
 * Chain walker driver (body lands in Step 3)
 * ============================================================ */

/*
 * cr_walk_chain -- walk one per-transaction undo chain from start_uba backward
 *	and inverse-apply every undo record newer than read_scn onto scratch_page.
 *	*steps is a running total across all candidate chains in one construction
 *	(spec-3.10); cluster_cr_construct_block_into calls this once per chain.
 *
 *	Stop conditions + chain terminal taxonomy (spec-3.9 §3.1 I-chain-1..4):
 *	  - I-chain-1  write_scn not later than read_scn: the unconditional
 *	               normal stop; this record + everything older is already in
 *	               the snapshot.
 *	  - I-chain-2  invalid prev_uba (chain end) is a legal base state ONLY
 *	               when no record was applied (empty chain) or the last
 *	               applied record was an INSERT (the row was created after
 *	               read_scn and inverse-INSERT made it LP_UNUSED).
 *	  - I-chain-3  reaching chain end while still newer than read_scn after an
 *	               UPDATE/DELETE/ITL record => the older base needed to reach
 *	               read_scn is unreachable (most likely retention recycled)
 *	               => 53R9F snapshot_too_old (fail-closed, NEVER
 *	               silent-success).
 *	  - missing record (reader returns 0) => 53R9F.
 *	  - cross-instance origin => 53R9G.
 *	  - malformed UBA / short record / unknown record_type / step cap
 *	    exceeded => data_corrupted.
 *
 *	The inverse-apply bodies live in cluster_cr_apply.c (Step 4); the
 *	helpers return bool and the caller MUST ereport on false (I-fail-4).
 *
 *	Caller already memcpy'd the buffer page into scratch_page and holds the
 *	non-reentrant guard; this function performs no locking (I-lock-2).
 */
static void
cr_walk_chain(char *scratch_page, UBA start_uba, SCN read_scn,
			  const ClusterCRCandidateChain *chains, int nchains, uint32 *steps, uint32 max_steps,
			  RelFileLocator cur_locator, ForkNumber cur_fork, BlockNumber cur_block)
{
	UBA uba = start_uba;
	PGAlignedBlock record_buf;
	ClusterXpScope xp_scope = { .active = false }; /* PGRAC: spec-5.59 D3 profiling */

	/* PGRAC: spec-5.59 D3 profiling -- nested breakdown under
	 * CLXP_R_CR_CONSTRUCT; every ereport(ERROR) path below simply loses the
	 * sample (scope is a stack variable, no cleanup needed). */
	cluster_xp_begin(&xp_scope, CLXP_R_CR_CHAIN_WALK);

	while (!UBA_is_invalid(uba)) {
		UndoRecordHeader *hdr;
		size_t len;
		uint32 seg;
		uint32 blk;
		uint16 tt_off;
		uint16 row_off;

		if (++(*steps) > max_steps)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR chain walk exceeded %u steps", max_steps),
							errhint("chain walk infinite loop suspected; raise "
									"cluster.cr_chain_walk_max_steps if a hot row legitimately "
									"has a longer in-snapshot undo chain.")));

		if (!uba_decode(uba, &seg, &blk, &tt_off, &row_off))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR encountered a malformed UBA in the undo chain")));

		/*
		 * spec-5.57 D2 (Q11-A): CR-side remote-UBA pre-check -- the read-path
		 * coordinator boundary, applied BEFORE the undo read.  Derive the
		 * segment owner from the UBA and classify it (§0.1).  A runtime-warm
		 * cross-instance origin (class③: not own, not merged-materialized) is
		 * the net-new boundary: fail closed HERE with the canonical 53R9G,
		 * rather than letting cluster_undo_get_record() return 0 and conflate it
		 * with own-instance retention-recycled undo (53R9F below).  This is the
		 * W3 hardening: the remote-undo-read leg now fails closed with the SAME
		 * errcode as the W1 walker wall (errcode consolidation, CR-9).  It is
		 * NON-DEGRADABLE -- the ereport fires under any GUC value; the GUC only
		 * gates the advisory counters (rule 8.A, §2.2).  Own-instance is the
		 * common OLTP path: classify returns OWN and we fall straight through.
		 * The data plane (real remote undo fetch) is Stage 6 (#119).
		 * See Spec: spec-5.57 §3.1 (W3) / §2.1 (three roles).
		 */
		{
			NodeId cr_origin = uba_origin_node_id(uba);
			ClusterCrCoordOriginClass cr_origin_class
				= cluster_cr_coordinator_classify_origin(cr_origin);

			if (cr_origin_class == CR_COORD_ORIGIN_RUNTIME_REMOTE) {
				/* class③: fail closed via the shared refusal routine (53R9G +
				 * both coordinator counters; non-degradable, §2.2). */
				cr_coordinator_refuse_runtime_remote((int)cr_origin, "chain_walk", NULL);
			} else if (cr_origin_class == CR_COORD_ORIGIN_MATERIALIZED_REMOTE) {
				/* class②: merged-materialized remote, served from the local tree
				 * (already shipped, spec-4.5a D8).  Count the serve (advisory). */
				if (cluster_cross_instance_cr_coordinator != CR_COORD_MODE_OFF)
					cluster_cr_coordinator_stat_bump(CR_COORD_MATERIALIZED_REMOTE_SERVED);
			}
		}

		len = cluster_undo_get_record(uba, record_buf.data, sizeof(record_buf.data));
		if (len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("snapshot too old: CR cannot read undo record at segment %u block %u "
							"(recycled or retention exceeded)",
							seg, blk),
					 errhint("Own-instance retention (cluster.undo_retention_horizon_enabled) "
							 "keeps undo for active readers; this fires only past the best-"
							 "effort capacity bound or when the reader's read_scn predates the "
							 "horizon.")));
		if (len < sizeof(UndoRecordHeader))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR read a short undo record (%zu < %zu bytes)", len,
								   sizeof(UndoRecordHeader))));

		hdr = (UndoRecordHeader *)record_buf.data;
		if ((hdr->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("page-history undo cannot enter a legacy transaction-chain walk")));

		/* Own-instance, or a merged-materialized remote instance whose undo
		 * lives in the local pg_undo/instance_<origin> tree (spec-4.5a D8).
		 * Anything else stays fail-closed (Spec: spec-5.57 §3.1 W1).
		 *
		 * This is the W1 belt: the spec-5.57 D2 segment-derived pre-check above
		 * (on uba_origin_node_id) catches the common class③ case BEFORE the undo
		 * read, so this header-origin check now fires only on the rare segment-
		 * vs-header mismatch (D8 §2.5 cross-check) -- defense-in-depth, never a
		 * silent assumption.
		 *
		 * spec-5.56 C4 (reconfig contract, §3.3): this carve-out is ALSO the
		 * fail-closed boundary that keeps the THIRD origin class — runtime warm
		 * remote (not own, not merged-materialized) — OUT of the CR pool: it never
		 * constructs (ERROR) so it never caches.  The two pool-eligible classes are
		 * reconfig-INVARIANT and need NO membership/remaster invalidation: (①)
		 * own-instance pages/undo are unchanged by reconfig; (②) merged-materialized
		 * remote undo is durable in the local tree with a reconfig-invariant
		 * merge_recovered_lsn authority, and an origin rejoin's NEW writes are new
		 * versions => new key => MISS (already fenced by C1/key).  read_scn is a
		 * GLOBAL SCN (AD-008), not a membership epoch (INV-C2).  spec-5.57 freezes
		 * this class③ fail-closed as the read-path coordinator boundary (CR-9); the
		 * runtime-warm-remote data plane lands in Stage 6 (#119). */
		if (hdr->origin_node_id != (uint16)cluster_node_id
			&& !cluster_merged_instance_is_materialized((int)hdr->origin_node_id)) {
			if (cluster_cross_instance_cr_coordinator != CR_COORD_MODE_OFF)
				cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_CR_REFUSED);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
					 errmsg("cluster CR cross-instance UBA encountered "
							"(origin_node_id=%u, local=%d)",
							hdr->origin_node_id, cluster_node_id),
					 errhint("Own-instance CR only unless the origin was materialized by "
							 "merged recovery; the runtime cross-instance CR/undo data plane "
							 "lands in Stage 6 (#119 undo-block Cache Fusion); see "
							 "Spec: spec-5.57.")));
		}

		/* I-chain-1: normal SCN stop. */
		if (scn_time_cmp(hdr->write_scn, read_scn) <= 0)
			break;

		/*
		 * spec-3.20 D3.B: every physical INSERT/UPDATE/DELETE/ITL undo record is
		 * written with a valid target relation (cluster_undo_record.c sets it
		 * from the heap relation's locator).  A missing/invalid target locator is
		 * undo corruption, NOT a skippable cross-block record -- fail closed
		 * rather than silently drop it (which would hide the corruption and could
		 * leave a post-read_scn version unrolled).
		 */
		if (!RelFileNumberIsValid(hdr->target_locator.relNumber))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR undo record has an invalid target relation"),
							errhint("undo chain corruption suspected; retry the transaction.")));

		/*
		 * spec-3.20 D3.A (F8): block-scope filter.  The prev_uba chain is
		 * transaction-GLOBAL -- a single txn (e.g. a TPC-B statement updating
		 * pgbench_accounts + _tellers + _branches + inserting _history) chains
		 * undo records across multiple relations/forks/blocks.  We are
		 * reconstructing ONE block (cur_locator/cur_fork/cur_block), so a record
		 * targeting any other relation/fork/block must NOT be inverse-applied to
		 * this scratch page: doing so lands a foreign old image at target_offset
		 * (DIFFLEN -> fail-closed, or -- worse -- a same-length SILENT overwrite
		 * that returns a wrong CR image, the spec-3.20 D0 P0/8.A finding).
		 *
		 * Skip-apply but KEEP walking prev_uba: the global chain can hold a
		 * deeper record that DOES belong to this block (records between read_scn
		 * and this block's head write_scn for other blocks are interleaved).
		 * Stopping here would truncate this block's legitimate history.  The
		 * header already carries the full physical target (HC213).
		 */
		if (!RelFileLocatorEquals(hdr->target_locator, cur_locator) || hdr->target_fork != cur_fork
			|| hdr->target_block != cur_block) {
			uba = hdr->prev_uba;
			continue;
		}

		switch (hdr->record_type) {
		case UNDO_RECORD_INSERT: {
			const UndoInsertPayload *p
				= (const UndoInsertPayload *)(record_buf.data + sizeof(UndoRecordHeader));

			if (!cluster_cr_apply_insert_inverse(scratch_page, hdr, p))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR insert inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_insert_count, 1);
			break;
		}
		case UNDO_RECORD_UPDATE: {
			const UndoUpdatePayload *p
				= (const UndoUpdatePayload *)(record_buf.data + sizeof(UndoRecordHeader));
			/* payload offsets are PAYLOAD-relative (heapam.c sets them to
			 * sizeof(...Payload)); base them at the payload start, not the
			 * record start which includes the UndoRecordHeader. */
			const char *old_bytes = (const char *)p + p->old_tuple_offset;

			if (sizeof(UndoRecordHeader) + (size_t)p->old_tuple_offset + p->old_tuple_length > len)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR update payload old-tuple bytes out of bounds")));
			if (!cluster_cr_apply_update_inverse(scratch_page, hdr, p, old_bytes,
												 p->old_tuple_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR update inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_update_count, 1);
			break;
		}
		case UNDO_RECORD_DELETE: {
			const UndoDeletePayload *p
				= (const UndoDeletePayload *)(record_buf.data + sizeof(UndoRecordHeader));
			/* payload-relative offset (see UPDATE branch). */
			const char *full_bytes = (const char *)p + p->full_tuple_offset;

			if (sizeof(UndoRecordHeader) + (size_t)p->full_tuple_offset + p->full_tuple_length
				> len)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("cluster CR delete payload full-tuple bytes out of bounds")));
			if (!cluster_cr_apply_delete_inverse(scratch_page, hdr, p, full_bytes,
												 p->full_tuple_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR delete inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_delete_count, 1);
			break;
		}
		case UNDO_RECORD_ITL: {
			const UndoItlPayload *p
				= (const UndoItlPayload *)(record_buf.data + sizeof(UndoRecordHeader));

			if (len < sizeof(UndoRecordHeader) + sizeof(UndoItlPayload))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR ITL undo record too short")));
			if (!cluster_cr_apply_itl_inverse(scratch_page, hdr, p))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR ITL inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_itl_count, 1);
			break;
		}
		default:
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR encountered unknown undo record type %u "
								   "(version skew or corruption)",
								   hdr->record_type)));
		}

		/*
		 * Keep the transient CR image close to the read_scn shape.  A hot row can
		 * have many post-read_scn intermediate versions on the same page
		 * (pgbench_tellers is the small-table repro): if the walk re-adds every
		 * intermediate image and defers pruning to the very end, the scratch page
		 * can temporarily overflow or present a foreign NORMAL tuple at a later
		 * restore offset.  Any tuple whose xmin is one of the candidate xids did
		 * not exist at read_scn, so it is safe to remove immediately after each
		 * inverse step, then compact before the next older undo record.
		 */
		(void)cluster_cr_prune_post_snapshot_versions(scratch_page, chains, nchains);
		PageRepairFragmentation((Page)scratch_page);

		uba = hdr->prev_uba;
	}

	/*
	 * Clean chain-end is a LEGITIMATE terminal: the ITL undo_segment_head chain
	 * is PER-TRANSACTION (threads every undo record under that slot's xact, NOT
	 * per-row), so an invalid prev_uba simply means the xact's undo is
	 * exhausted and the tuples it touched are back at read_scn.  A genuine
	 * truncation (retention recycled a still-needed older record) instead
	 * surfaces as cluster_undo_get_record() == 0 -> 53R9F inside the loop.
	 * *steps is a running total across all candidate chains; the caller
	 * (cluster_cr_construct_block_into) accumulates cr_chain_walk_steps_sum
	 * once after every chain completes.
	 */

	/* PGRAC: spec-5.59 D3 profiling */
	cluster_xp_end(&xp_scope);
}


/*
 * cr_resolve_kept_tuples_durable -- spec-3.11 D6/C4/C5.
 *
 * Called only when the block's ITL recycle watermark exceeds read_scn: a
 * completed DATA ITL slot whose write_scn is newer than the snapshot was
 * recycled out of this block, so cluster_cr_collect_candidate_chains above may
 * not have captured that writer and a post-read_scn tuple version it created
 * could survive the candidate prune as a false-visible (spec-3.10 §v0.5 A).
 *
 * For every still-NORMAL tuple whose xmin is NOT one of the live candidate
 * xids, resolve xmin's commit_scn from the durable TT by xid (spec-3.11 D2):
 *
 *   - committed (CLOG-confirmed -- C1b) and commit_scn is later than read_scn:
 *     the evicted post-read_scn creator -> prune (LP_UNUSED).
 *   - committed and commit_scn is not later than read_scn: a legitimate
 *     pre-read_scn version -> keep.
 *   - not committed per CLOG (aborted / still in flight at this read): the
 *     creator's row was not visible at read_scn -> prune.
 *   - durable lookup miss / ambiguous: cannot prove either way -> fail closed
 *     (53R9F).  Never leave a possibly-post-read_scn version visible (规则 8.A).
 *
 * This retires the spec-3.10 blanket fail-closed for every kept tuple whose
 * durable slot is still resolvable; only a tuple whose own slot was already
 * recycled (lookup miss) still fails closed -- spec-3.12 retention shrinks that
 * window.  Own-instance only (the watermark is a local-block property; remote
 * origins resolve via Cache Fusion in Stage 4).
 *
 * Mutates dst_page (LP_UNUSED marks); the caller leaves them unrepaired (the CR
 * image is read-only and visibility scans skip LP_UNUSED, matching prune-AFTER).
 */
static void
cr_resolve_kept_tuples_durable(char *dst_page, SCN read_scn, const ClusterCRCandidateChain *chains,
							   int nchains)
{
	Page page = (Page)dst_page;
	OffsetNumber off;
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
		ItemId lp = PageGetItemId(page, off);
		HeapTupleHeader htup;
		TransactionId xmin;
		SCN durable_scn = InvalidScn;
		bool is_candidate = false;
		int c;

		if (!ItemIdIsNormal(lp))
			continue;

		htup = (HeapTupleHeader)PageGetItem(page, lp);
		xmin = HeapTupleHeaderGetRawXmin(htup);

		/* Frozen / bootstrap xids predate any cluster snapshot -> visible. */
		if (!TransactionIdIsNormal(xmin))
			continue;

		/* xmin already covered by the candidate prune/walk -> nothing to do. */
		for (c = 0; c < nchains; c++) {
			if (chains[c].xid == xmin) {
				is_candidate = true;
				break;
			}
		}
		if (is_candidate)
			continue;

		/*
		 * xmin fell outside the (possibly incomplete) candidate set.  Resolve it
		 * durably by xid; a miss / ambiguity is unresolvable -> fail closed.
		 */
		if (!cluster_tt_slot_durable_lookup_by_xid(xmin, &durable_scn))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
							errmsg("cluster CR cannot reconstruct block: durable TT slot for "
								   "writer xid %u is unavailable after ITL slot reuse",
								   xmin),
							errhint("retry the transaction with a fresh snapshot")));

		/*
		 * C1b (规则 8.A): the durable slot is stamped at pre-commit, so a
		 * COMMITTED stamp alone does not prove the xact committed.  Confirm via
		 * CLOG: an xact that stamped then aborted (or is still in flight at this
		 * read) was not visible at read_scn -> prune.
		 */
		if (!TransactionIdDidCommit(xmin)) {
			ItemIdSetUnused(lp);
			continue;
		}

		/* Committed after the snapshot -> evicted post-read_scn creator -> prune. */
		if (scn_time_cmp(durable_scn, read_scn) > 0)
			ItemIdSetUnused(lp);
		/* else committed at/before read_scn -> legitimate pre-read_scn -> keep. */
	}
}


/* ============================================================
 * 2-layer public API
 * ============================================================ */

/*
 * cr_construct_from_copy -- construction core over an ALREADY-COPIED page
 *	(spec-6.12b refactor of the spec-3.9/3.10/3.11 body; the Buffer wrapper
 *	below memcpy's the live page first and keeps its exact pre-6.12b
 *	semantics).
 *
 *	Modes:
 *	  local  (server_mode=false)  the classic backend construction.  NEW:
 *	         when cluster.crossnode_cr_data_plane is on and the NEWEST
 *	         candidate chain's undo home is a class-(3) runtime-remote
 *	         origin (the spec-5.57 boundary that otherwise refuses 53R9G),
 *	         fetch the CR result from that origin's CR-server first: a FULL
 *	         result replaces dst outright; a PARTIAL result replaces dst and
 *	         the construction CONTINUES here on the shipped page (candidates
 *	         re-derive from its ITL state; a still-foreign chain hits the
 *	         class-(3) walk backstop -> 53R9G).  A failed fetch falls into
 *	         the unchanged cr_coordinator_refuse_runtime_remote (Rule 8.A).
 *	  server (server_mode=true)   the LMS-side construction: classify the
 *	         write_scn-DESC chain homes (cluster_cr_server_split_classify),
 *	         refuse an interleave (53R9G -> the LMS wrapper turns any error
 *	         into a DENIED reply), peel only the self-home DESC prefix and
 *	         report *out_partial so the requester knows to continue.
 *
 *	The caller owns cr_in_progress + PG_TRY taxonomy + the profiling scope;
 *	this helper throws on every failure path (I-fail-4 discipline).
 */
/* The older synchronous entry points may also encounter newly prepared
 * records. Reuse the same page inverse and eight-head selection, not their
 * transaction-global/prune-by-xid algorithm. Remote work retains the existing
 * coordinator boundary; the R4 LMS path above owns kind-4 continuations. */
static bool
cr_construct_retained_history(char *page, SCN read_scn, const BufferTag *tag, bool server_mode,
							  bool *out_partial, uint32 *steps)
{
	ClusterR4CrBuildContext context = { 0 };
	PGAlignedBlock bytes;
	UBA *visited;
	bool selected = false;
	bool handled = false;

	if (!cr_r4_history_reselect(&context, (Page)page, read_scn))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid CR page-history locator")));
	if (context.candidate_count == 0)
		return false;
	if (cluster_cr_chain_walk_max_steps <= 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid CR history step budget")));
	visited = palloc(sizeof(UBA) * (Size)cluster_cr_chain_walk_max_steps);
	PG_TRY();
	{
		while (context.candidate_count > 0) {
			ClusterTxLocator locator = context.locators[0];
			ClusterTxLocator canonical;
			NodeId origin = uba_origin_node_id(locator.uba);
			UndoRecordHeader *record;
			size_t length;
			uint32 i;

			if (cluster_cr_coordinator_classify_origin(origin) == CR_COORD_ORIGIN_RUNTIME_REMOTE) {
				if (!selected)
					break; /* the existing remote-first dispatcher still owns it */
				if (!server_mode)
					cr_coordinator_refuse_runtime_remote((int)origin, "retained_history", NULL);
				if (out_partial != NULL)
					*out_partial = true;
				break;
			}
			if (*steps >= (uint32)cluster_cr_chain_walk_max_steps)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("CR page-history step budget exceeded")));
			for (i = 0; i < *steps; i++)
				if (cluster_undo_record_uba_equal(visited[i], locator.uba))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED), errmsg("CR page-history cycle")));
			length = cluster_undo_get_record(locator.uba, bytes.data, BLCKSZ);
			if (length < sizeof(UndoRecordHeader))
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
								errmsg("required CR page history is unavailable")));
			record = (UndoRecordHeader *)bytes.data;
			if (!selected && (record->flags & UNDO_REC_FLAG_HAS_ITL_HISTORY) == 0)
				break; /* explicit legacy layout, not a fallback after a failure */
			selected = handled = true;
			if (SCN_VALID(ClusterPageGetItlHeader((Page)page)->itl_recycle_watermark_scn)
				&& scn_time_cmp(ClusterPageGetItlHeader((Page)page)->itl_recycle_watermark_scn,
								read_scn)
					   > 0)
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
								errmsg("CR page history was discarded before retention")));
			visited[(*steps)++] = locator.uba;
			if (length != sizeof(*record) + record->payload_length
				|| !cr_r4_canonical_record_locator(&locator, locator.uba, record, &canonical)
				|| !cluster_cr_history_inverse(page, tag, &canonical, locator.uba, record,
										  bytes.data + sizeof(*record), record->payload_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("invalid retained CR page history")));
			if (!cr_r4_history_reselect(&context, (Page)page, read_scn))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("invalid prior CR page-history locator")));
		}
		if (handled && context.candidate_count == 0 && out_partial != NULL)
			*out_partial = false;
	}
	PG_FINALLY();
	{
		pfree(visited);
	}
	PG_END_TRY();
	return handled;
}

/* Validate the exact head through the original inverse even when LIVE survives
 * the snapshot. All mutations remain private until the complete result exists. */
bool
cluster_cr_apply_operation_origin_admitted(const char source_page[BLCKSZ], const BufferTag *tag,
										   SCN read_scn, const ClusterTxLocator *locator,
										   const ClusterSavepointOperation *expected,
										   const ClusterSemanticAdmissionToken *admission,
										   char result_page[BLCKSZ], ClusterTxResolution *top_out,
										   ClusterSavepointReaderRecord *operation_out,
										   ClusterTxResolveReason *reason_out)
{
	PGAlignedBlock data, scratch;
	ClusterTxResolution top;
	ClusterSavepointReaderRecord operation;
	ClusterSavepointReaderIdentity identity;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	const PageHeaderData *header = (const PageHeaderData *)source_page;
	const UndoRecordHeader *record;
	size_t length;
	bool inverse;

	if (top_out != NULL)
		memset(top_out, 0, sizeof(*top_out));
	if (operation_out != NULL)
		memset(operation_out, 0, sizeof(*operation_out));
	if (reason_out != NULL)
		*reason_out = reason;
	if (source_page == NULL || tag == NULL || locator == NULL || admission == NULL
		|| result_page == NULL || top_out == NULL || operation_out == NULL
		|| !SCN_VALID(read_scn)) {
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		goto failed;
	}
	reason = CLUSTER_TX_RESOLVE_BAD_COMPOSITION;
	if (!PageHasItl((Page)source_page) || header->pd_lower < SizeOfPageHeaderData
		|| header->pd_lower > header->pd_upper || header->pd_upper > header->pd_special
		|| header->pd_special < SizeOfPageHeaderData || header->pd_special > BLCKSZ
		|| BLCKSZ - header->pd_special < CLUSTER_ITL_SPECIAL_SIZE
		|| locator->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT)
		goto failed;
	if (expected != NULL) {
		if (!cluster_savepoint_reader_identity_from_operation(expected, locator, &identity)
			|| cluster_runtime_visibility_resolve_operation_origin_admitted(
				   locator, expected, CLUSTER_TX_RESOLVE_VISIBILITY, admission, &top, &operation,
				   data.data, &reason)
				   == CLUSTER_TX_UNKNOWN)
			goto failed;
	} else if (cluster_runtime_visibility_resolve_operation_head_origin_admitted(
				   locator, &ClusterPageGetItlSlots((Page)source_page)[locator->itl_slot_index],
				   tag, CLUSTER_TX_RESOLVE_VISIBILITY, admission, &identity, &top, &operation,
				   data.data, &reason)
			   == CLUSTER_TX_UNKNOWN)
		goto failed;
	if (top.outcome == CLUSTER_TX_PREPARED) {
		reason = CLUSTER_TX_RESOLVE_TWOPHASE_CONFLICT;
		goto failed;
	}
	reason = CLUSTER_TX_RESOLVE_BAD_COMPOSITION;
	/* The extractor supports overlap and leaves an aligned record at DATA's
	 * front, avoiding another block-sized buffer or any new acquisition. */
	if (!cluster_savepoint_reader_extract_identity(&identity, &top.locator_echo, data.data,
												   data.data, &length, &operation))
		goto failed;
	record = (const UndoRecordHeader *)data.data;
	memcpy(scratch.data, source_page, BLCKSZ);
	if (!cluster_cr_history_inverse_operation_identity(
			scratch.data, tag, &top.locator_echo, locator->uba, record, data.data + sizeof(*record),
			record->payload_length, &identity))
		goto failed;
	inverse = operation.operation.effect_state == CLUSTER_SAVEPOINT_EFFECT_DISCARDED
			  || top.outcome != CLUSTER_TX_COMMITTED || scn_time_cmp(top.commit_scn, read_scn) > 0;
	if (!inverse)
		memcpy(scratch.data, source_page, BLCKSZ);
	if (!cluster_semantic_activation_recheck(admission)) {
		reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
		goto failed;
	}
	memcpy(result_page, scratch.data, BLCKSZ);
	*top_out = top;
	*operation_out = operation;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;

failed:
	if (reason_out != NULL)
		*reason_out = reason;
	return false;
}

static void
cr_construct_from_copy(char *dst_page, SCN read_scn, RelFileLocator cur_locator,
					   ForkNumber cur_fork, BlockNumber cur_block, bool server_mode,
					   bool *out_partial)
{
	Page page;
	const ClusterItlSlotData *slots;
	ClusterCRCandidateChain chains[CLUSTER_ITL_INITRANS_DEFAULT];
	int nchains;
	int peel_n;
	uint32 steps = 0;
	uint32 max_steps = (uint32)cluster_cr_chain_walk_max_steps;
	bool watermark_exceeds = false;

	if (out_partial != NULL)
		*out_partial = false;

	/* I-lock-1/2/4: caller holds the content lock (or made a stable copy);
	 * we only read page bytes in dst -- no buffer lock, no WAL, no dirty.
	 * The wait event covers the undo I/O of the chain walks. */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_CR_CONSTRUCT);

	/* deterministic wait-event window for TAP L8 (armed us sleep). */
	{
		uint64 delay_us = 0;

		if (cluster_cr_injection_armed("cr_construct_delay_us", &delay_us) && delay_us > 0)
			pg_usleep((long)delay_us);
	}

	/* spec-3.9 Step 7: injection-forced taxonomy fires FIRST (TAP L4/L5/L6),
	 * before any page inspection, so it is page-state-independent. */
	cr_check_error_injections();

	page = (Page)dst_page;
	if (!PageHasItl(page))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR target page has no ITL special area")));

	/*
	 * spec-3.10 (S)v0.5 / spec-3.11 D6: slot-reuse gate.  If a completed DATA
	 * ITL slot whose write_scn is newer than this reader's snapshot was
	 * recycled out of this block (its undo-chain anchor overwritten), the
	 * per-page candidate set may be incomplete and a post-read_scn tuple
	 * version could survive the candidate prune as a false-visible.
	 */
	{
		SCN recycle_wm = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;

		watermark_exceeds = SCN_VALID(recycle_wm) && scn_time_cmp(recycle_wm, read_scn) > 0;
	}

	/* Snapshot candidate chains BEFORE mutation, then peel newest-first. */
	slots = ClusterPageGetItlSlots(page);
	nchains = cluster_cr_collect_candidate_chains(slots, read_scn, chains,
												  CLUSTER_ITL_INITRANS_DEFAULT);
	if (nchains > 1)
		qsort(chains, nchains, sizeof(chains[0]), cluster_cr_chain_cmp_by_write_scn_desc);
	peel_n = nchains;

	if (server_mode) {
		/*
		 * PGRAC: spec-6.12b -- CR-server split.  One chain = one transaction
		 * = one undo home, so classifying the DESC-ordered head origins fully
		 * decides the one-hop serve (see cluster_cr_server.h).  DENY throws
		 * the canonical cross-instance SQLSTATE; the LMS wrapper converts any
		 * throw into a DENIED reply (fail-closed at the requester, Rule 8.A).
		 */
		int32 origins[CLUSTER_ITL_INITRANS_DEFAULT];
		int prefix = 0;
		ClusterCrServerSplit split;

		for (int i = 0; i < nchains; i++)
			origins[i] = (int32)uba_origin_node_id(chains[i].undo_segment_head);
		split = cluster_cr_server_split_classify(origins, nchains, (int32)cluster_node_id, &prefix);
		if (split == CLUSTER_CR_SPLIT_DENY)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
					 errmsg("cluster CR server cannot construct: candidate chain homes "
							"interleave across instances"),
					 errhint("The one-page one-hop CR protocol preserves the write_scn-DESC "
							 "peel order only for a self-home prefix; the requester keeps the "
							 "fail-closed refusal.")));
		peel_n = prefix;
		if (out_partial != NULL)
			*out_partial = (split == CLUSTER_CR_SPLIT_PARTIAL);
	} else if (cluster_crossnode_cr_data_plane && nchains > 0) {
		/*
		 * PGRAC: spec-6.12b -- requester-side remote-first.  Only when the
		 * NEWEST chain is class-(3) remote: the DESC peel must start there, so
		 * a local peel could never come first (a local-prefix mix would need a
		 * page round-trip mid-construction -- out of the one-hop protocol;
		 * those interleaves keep the in-walk class-(3) backstop -> 53R9G).
		 */
		NodeId head_origin = uba_origin_node_id(chains[0].undo_segment_head);

		if (cluster_cr_coordinator_classify_origin(head_origin) == CR_COORD_ORIGIN_RUNTIME_REMOTE) {
			ClusterR4SourceCrRequest source_request;
			ClusterR4SourceCrResult source_result;
			ClusterSemanticAdmissionResult source_admission;
			BufferTag tag;

			InitBufferTag(&tag, &cur_locator, cur_fork, cur_block);
			memset(&source_request, 0, sizeof(source_request));
			source_request.tag = tag;
			source_request.read_scn = read_scn;
			source_request.origin_node = (int32)head_origin;
			source_request.dst_page = dst_page;
			source_admission = cluster_r4_source_cr_dispatch(CLUSTER_R4_SOURCE_CR_FETCH,
															   &source_request, &source_result);
			if (source_admission != CLUSTER_SEMANTIC_ADMISSION_OK
				|| !source_result.fetched) {
				if (CRShared != NULL)
					pg_atomic_fetch_add_u64(&CRShared->cr_remote_failed_count, 1);
				/* Unchanged spec-5.57 refusal: 53R9G + coordinator counters. */
				cr_coordinator_refuse_runtime_remote((int)head_origin, "source_dispatch",
													 &source_admission);
			}

			if (!source_result.partial) {
				/* FULL: dst holds the origin-finished CR page (it already ran
				 * prune + walk + durable resolve there). */
				if (CRShared != NULL) {
					pg_atomic_fetch_add_u64(&CRShared->cr_remote_full_count, 1);
					pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);
				}
				pgstat_report_wait_end();
				return;
			}

			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_remote_partial_count, 1);

			/* PARTIAL: continue on the shipped page -- re-derive everything
			 * from its content (the peeled prefix chains' ITL slots were
			 * inverse-restored, so they no longer collect as candidates). */
			page = (Page)dst_page;
			if (!PageHasItl(page))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR server shipped a page without an ITL "
									   "special area")));
			{
				SCN recycle_wm = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;

				watermark_exceeds = SCN_VALID(recycle_wm) && scn_time_cmp(recycle_wm, read_scn) > 0;
			}
			slots = ClusterPageGetItlSlots(page);
			nchains = cluster_cr_collect_candidate_chains(slots, read_scn, chains,
														  CLUSTER_ITL_INITRANS_DEFAULT);
			if (nchains > 1)
				qsort(chains, nchains, sizeof(chains[0]), cluster_cr_chain_cmp_by_write_scn_desc);
			peel_n = nchains;
		}
	}

	{
		BufferTag tag;

		InitBufferTag(&tag, &cur_locator, cur_fork, cur_block);
		if (cr_construct_retained_history(dst_page, read_scn, &tag, server_mode, out_partial,
										  &steps))
			goto construction_done;
	}

	/*
	 * If the recycle watermark says the candidate set may be incomplete,
	 * resolve evicted post-read_scn tuple creators before walking undo.
	 * Otherwise a recycled-slot tuple can remain NORMAL at the target
	 * offset and block an older UPDATE/DELETE restore with a length/identity
	 * mismatch.  The post-walk call below remains as the final fail-closed
	 * guard for tuples materialized by the chain walk itself.
	 */
	if (watermark_exceeds) {
		if (cluster_tt_durable_lookup)
			cr_resolve_kept_tuples_durable(dst_page, read_scn, chains, nchains);
		else
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
							errmsg("cluster CR cannot reconstruct block: ITL slot reused "
								   "after snapshot"),
							errhint("retry the transaction with a fresh snapshot")));
	}

	/*
	 * spec-3.10 (S)v0.6: prune post-read_scn versions BOTH before and after
	 * the inverse-apply (v0.4 pruned only after).  prune-FIRST +
	 * PageRepairFragmentation frees the line pointer a later INSERT reused so
	 * the inverse-apply can re-add the old image at its read_scn offnum; the
	 * prune-AFTER strips intermediate versions the walk restored.  In the
	 * spec-6.12b server-PARTIAL mode the prune still uses the FULL candidate
	 * list (pruning is xmin matching, no undo I/O) while the walk below peels
	 * only the self-home prefix -- the requester's continue-run re-prunes
	 * idempotently with its own re-collected list.
	 */
	(void)cluster_cr_prune_post_snapshot_versions(dst_page, chains, nchains);
	PageRepairFragmentation((Page)dst_page);

	for (int i = 0; i < peel_n; i++)
		cr_walk_chain(dst_page, chains[i].undo_segment_head, read_scn, chains, nchains, &steps,
					  max_steps, cur_locator, cur_fork, cur_block);

	(void)cluster_cr_prune_post_snapshot_versions(dst_page, chains, nchains);

	/*
	 * spec-3.11 D6/C4: if a completed post-read_scn DATA slot was evicted
	 * (watermark > read_scn), the candidate set above may miss its writer.
	 * Resolve every kept tuple whose xmin is not a live candidate against the
	 * durable TT by xid, pruning evicted post-read_scn versions and failing
	 * closed only on an unresolvable (recycled) slot.
	 */
	if (watermark_exceeds)
		cr_resolve_kept_tuples_durable(dst_page, read_scn, chains, nchains);

construction_done:
	pgstat_report_wait_end();

	if (CRShared != NULL) {
		pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);
		pg_atomic_fetch_add_u64(&CRShared->cr_chain_walk_steps_sum, steps);
	}
}

/*
 * cluster_cr_construct_block_into -- full-block CR into a caller-provided
 *	BLCKSZ destination (the CR cache victim slot in spec-3.10, or the backend
 *	scratch via the public wrapper).  memcpy the live page, then inverse-apply
 *	EVERY candidate ITL chain (write_scn newer than read_scn) in write_scn-DESC
 *	order so the whole block is rolled back to read_scn (Oracle block-level
 *	CR).  Body lives in cr_construct_from_copy (spec-6.12b refactor); the
 *	local-mode semantics are unchanged.
 *
 *	I-lock-3 non-reentrant + I-fail-1 balanced guard (PG_TRY resets
 *	cr_in_progress and re-throws the precise SQLSTATE).  Returns dst_page.
 */
static const char *
cluster_cr_construct_block_into(Buffer buf, SCN read_scn, char *dst_page)
{
	/* Init silences cppcheck uninitvar across the PG_RE_THROW longjmp. */
	const char *result = NULL;
	ClusterXpScope xp_scope; /* PGRAC: spec-5.59 D3 profiling */

	Assert(BufferIsValid(buf));
	Assert(dst_page != NULL);
	Assert(!cr_in_progress); /* I-lock-3 non-reentrant */

	/* PGRAC: spec-5.59 D3 profiling -- time the whole CR construction at its
	 * single choke point (cache-fill and scratch paths both land here); an
	 * ereport(ERROR) re-thrown by the PG_CATCH below loses the sample,
	 * acceptable. */
	cluster_xp_begin(&xp_scope, CLXP_R_CR_CONSTRUCT);

	cr_in_progress = true;

	PG_TRY();
	{
		RelFileLocator cur_locator;
		ForkNumber cur_fork;
		BlockNumber cur_block;

		/* spec-3.20 D3.A (F8): the physical identity of the block we are
		 * reconstructing, so cr_walk_chain can drop undo records the same
		 * transaction wrote against OTHER relations/forks/blocks. */
		BufferGetTag(buf, &cur_locator, &cur_fork, &cur_block);

		memcpy(dst_page, BufferGetPage(buf), BLCKSZ);
		cr_construct_from_copy(dst_page, read_scn, cur_locator, cur_fork, cur_block,
							   false /* local mode */, NULL);
		result = dst_page;
	}
	PG_CATCH();
	{
		/*
		 * spec-3.9 Step 6: centralized error-taxonomy counter bump.  Every
		 * failure path ereports a precise SQLSTATE (53R9F / 53R9G /
		 * data_corrupted); geterrcode() bumps the matching counter once per
		 * failed construction.  Injection-forced failures flow through here too.
		 */
		if (CRShared != NULL) {
			int sqlerr = geterrcode();

			if (sqlerr == ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD)
				pg_atomic_fetch_add_u64(&CRShared->cr_snapshot_too_old_count, 1);
			else if (sqlerr == ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_cross_instance_unsupported_count, 1);
			else if (sqlerr == ERRCODE_DATA_CORRUPTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_corruption_count, 1);
		}

		pgstat_report_wait_end();
		cr_in_progress = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	cr_in_progress = false;
	cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 */
	return result;
}

/*
 * cluster_cr_construct_page_for_server -- spec-6.12b LMS-side entry: full CR
 *	construction over a STABLE COPY of the current page (cur_page; the LMS
 *	drain made it with the raw-pin/content-lock copy helper), peeling only
 *	the self-home write_scn-DESC prefix.  *out_partial reports whether a
 *	foreign suffix remains for the requester.  Throws on every failure
 *	(interleave, snapshot-too-old, corruption); the LMS drain wrapper
 *	converts any throw into a DENIED reply -- fail-closed at the requester,
 *	never an LMS crash (Rule 8.A).
 */
void
cluster_cr_construct_page_for_server(const char *cur_page, SCN read_scn, BufferTag tag,
									 char *dst_page, bool *out_partial)
{
	Assert(cur_page != NULL && dst_page != NULL);
	Assert(!cr_in_progress); /* LMS is single-threaded; guard stays honest */

	cr_in_progress = true;

	PG_TRY();
	{
		RelFileLocator cur_locator = BufTagGetRelFileLocator(&tag);

		memcpy(dst_page, cur_page, BLCKSZ);
		cr_construct_from_copy(dst_page, read_scn, cur_locator, tag.forkNum, tag.blockNum,
							   true /* server mode */, out_partial);
	}
	PG_CATCH();
	{
		/* Same taxonomy bumps as the local wrapper (spec-3.9 Step 6). */
		if (CRShared != NULL) {
			int sqlerr = geterrcode();

			if (sqlerr == ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD)
				pg_atomic_fetch_add_u64(&CRShared->cr_snapshot_too_old_count, 1);
			else if (sqlerr == ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_cross_instance_unsupported_count, 1);
			else if (sqlerr == ERRCODE_DATA_CORRUPTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_corruption_count, 1);
		}

		pgstat_report_wait_end();
		cr_in_progress = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	cr_in_progress = false;
}

const char *
cluster_cr_construct_block(Buffer buf, SCN read_scn)
{
	/* Public fallback: construct into the backend-local scratch.  Used by the
	 * test SRF and as the cache-disabled path (spec-3.10 (S)3.1). */
	cr_scratch_ensure();
	return cluster_cr_construct_block_into(buf, read_scn, cr_scratch);
}

/*
 * cr_build_cache_key -- CR cache identity for `buf` at `read_scn`.  base_page_lsn
 * is the live page LSN (bumped by every WAL-logged physical change incl.
 * HOT-prune / VACUUM), the version guard that forces a miss after a relayout
 * (spec-3.10 §3.2).  memset zeroes padding so the key compares cleanly.
 */
static ClusterCRCacheKey
cr_build_cache_key(Buffer buf, SCN read_scn)
{
	ClusterCRCacheKey key;

	memset(&key, 0, sizeof(key));
	BufferGetTag(buf, &key.rlocator, &key.forknum, &key.blockno);
	key.read_scn = read_scn;
	key.base_page_lsn = PageGetLSN(BufferGetPage(buf));
	return key;
}

/*
 * cr_note_retention_if_advanced -- spec-5.56 D2 (C3): PURE OBSERVATION on a
 * cache hit.  Reads the EXISTING undo retention horizon (no pin, no extension)
 * and, if it has advanced PAST this served image's read_scn, bumps the
 * retention_horizon_advance_noted counter.  This does NOT affect serving: the
 * materialized image is a key pure function (AD-006 / INV-R1), so recycling the
 * source undo never rewrites the already-materialized bytes — the image stays the
 * correct as-of-read_scn page.  The counter only quantifies that the §3.2
 * retention-recycle scenario really occurs (it is naturally near-unreachable
 * because an active reader@X pins the horizon <= X, INV-R2; U5 forces it via the
 * retention-off / forced-advance leg, F0-13b).  Cheap: a bool guard first, then
 * one atomic read only when retention is on. */
static inline void
cr_note_retention_if_advanced(SCN read_scn)
{
	SCN horizon;

	if (!cluster_undo_retention_horizon_enabled)
		return; /* retention off: nothing to compare (no hot-path cost) */
	horizon = cluster_undo_retention_horizon();
	if (SCN_VALID(horizon) && scn_time_cmp(horizon, read_scn) > 0)
		cluster_cr_pool_note_retention_horizon_advance();
}

/*
 * spec-5.53 D3 — MISS-forcing rules (the eight categories; rule 8.A: any one
 * uncertain → MISS → reconstruct, never a stale hit).  Every category below maps
 * to a concrete mechanism already present on this path and an observable
 * counter; the set is exhaustive (no relfilenode lifecycle / page version /
 * snapshot / slot-reuse case can over-hit):
 *
 *   trigger                              mechanism                         counter
 *   1 DROP → different relid reuse       pool_epoch bump (D2b floor)       epoch_mismatch
 *   2 TRUNCATE (same relid, new          old relfilenode unlink → bump     epoch_mismatch
 *     relfilenode)
 *   3 VACUUM FULL / CLUSTER (same        swap → old unlink → bump          epoch_mismatch
 *     relid, swapped relfilenode)
 *   4 page rewrite (HOT-prune /          base_page_lsn in key → key differs base_lsn_mismatch
 *     VACUUM relayout)
 *   5 base_page_lsn churn (hint bit /    base_page_lsn in key → key differs base_lsn_mismatch
 *     neighbour-row change)
 *   6 different read_scn (new snapshot)  read_scn in key → key differs      key_mismatch
 *   7 slot reuse / hash collision        field-wise key_equal + slot gen    key/generation_mismatch
 *   8 belt source unavailable / uncertain fail-closed MISS (D0=RED: no belt; epoch_mismatch
 *                                        the epoch floor is the fence)
 *
 * INV-K2 (fail-closed): if any signal is unavailable / inconsistent / uncertain,
 * MISS and reconstruct (cluster_cr_construct_block_into, spec-3.9) — a CR cache
 * may ALWAYS miss safely; it must NEVER false-hit.
 *
 * spec-5.53 D4 — ABA / lifecycle combination invariant.  A served image must
 * pass ALL of these independent guards; any single one failing forces a MISS, so
 * no single mechanism is load-bearing alone (defence in depth):
 *   (a) field-wise key_equal               (canonical, never memcmp; F0-6)
 *   (b) pool_epoch == current              (D2b floor; L1 per-entry + L2 per-slot
 *                                           = "L1+L2 same fence", D2c)
 *   (c) slot.generation == handle.generation  (two-phase publish ABA; F0-21)
 *   (d) commit-time epoch recheck          (serve-but-don't-cache if epoch moved
 *                                           during construction; spec-5.51 §3.4)
 * (D0=RED → there is no catalog-incarnation belt; correctness rests on the
 * provably-complete bump completeness of (b), honestly NOT a self-validating
 * token — see cluster_cr_cache.h / spec-5.53 §3.2.)
 */
const char *
cluster_cr_lookup_or_construct(Buffer buf, SCN read_scn)
{
	/*
	 * spec-3.10 D3 + spec-5.51 D4: two-level CR cache.
	 *
	 *  L1 = backend-local cache (cluster_cr_cache.c).  L2 = per-instance shared
	 *  CR pool (cluster_cr_pool.c), behind L1, consulted on an L1 miss.  L2 is
	 *  disabled by default (cluster_cr_pool_current_epoch() == 0); when disabled
	 *  this degrades EXACTLY to the spec-3.10 L1-only path (zero behavior
	 *  change).
	 *
	 *  Lifecycle epoch gate (spec-5.51 §3.4/§3.5, all returning paths checked):
	 *   - entry: capture start_epoch; if it changed since this backend last saw
	 *     it, reset L1 (a DROP/TRUNCATE/sinval since the last call may have left
	 *     stale-epoch L1 entries).
	 *   - L1 hit: recheck the epoch before returning; if it advanced mid-lookup,
	 *     reset L1 and fall through to reconstruct (never return a stale L1 hit).
	 *   - construct/publish: two-phase, construction OUTSIDE the L2 lock; an
	 *     ereport aborts the L2 reservation (PG_CATCH).
	 *   - commit: recheck the epoch; if it advanced during construction, serve
	 *     the (correct, just-built) image to the caller but skip caching it in
	 *     L1 (serve-but-skip-cache) so no stale-epoch entry persists.
	 */
	ClusterCRCacheKey key;
	uint64 start_epoch;
	uint64 start_rel_gen = 0; /* spec-5.56 D4: per-relation gen captured for ①/②;
							   * 0 = gen table disabled / locator unregistered */
	uint64 install_gen = 0;	  /* spec-5.56 D4: gen to stamp the constructed image */
	bool skip_cache = false;  /* spec-5.56 D4: gen-table full => serve-but-skip-cache */
	const char *hit;
	char *slot;
	bool evicted = false;
	int miss_reason = CR_CACHE_MISS_NONE;

	/* Shared CR reuse belongs to the native buffer arena and its retained
	 * scan/snapshot owner. Legacy private caches use a current-page LSN key
	 * which cannot identify versions from different WAL threads. Keep the
	 * original uncached constructor for callers outside the native CR scope. */
	if (cluster_shared_config)
		return cluster_cr_construct_block(buf, read_scn);

	key = cr_build_cache_key(buf, read_scn);
	start_epoch = cluster_cr_pool_current_epoch(); /* 0 when L2 disabled */
	/* spec-5.56 D4: capture the locator's current per-relation generation for the
	 * composite {pool_epoch, rel_gen} fence (P1-c).  0 when the gen table is
	 * disabled or the locator is unregistered (then the fence is epoch-only and a
	 * registered entry, which always has gen >= 1, simply MISSes). */
	if (cluster_cr_pool_rel_generation_enabled())
		(void)cluster_cr_pool_rel_generation(key.rlocator, &start_rel_gen);

	/*
	 * spec-5.53 D2c: per-entry L1 lifecycle-epoch fence ("L1+L2 same fence").
	 * cluster_cr_cache_lookup serves an entry only when its stamped epoch ==
	 * start_epoch; a stale-epoch exact-key match is reported and counted, never
	 * served (rule 8.A: a relfilenode reuse can never serve a stale image).  This
	 * replaces the spec-5.51 D4 coarse whole-L1 flush with a finer per-entry
	 * reject (stale entries clock-evict lazily), mirroring the L2 per-slot epoch
	 * gate.  spec-5.53 D5: the linear L1 scan also attributes a miss to a churned
	 * page version / diverged snapshot for the over-miss diagnostics.
	 */
	/* spec-5.56 ①: L1 lookup under the COMPOSITE fence (epoch + start_rel_gen). */
	hit = cluster_cr_cache_lookup_fenced(&key, start_epoch, start_rel_gen, &miss_reason);
	switch (miss_reason) {
	case CR_CACHE_MISS_EPOCH:
		cluster_cr_pool_note_l1_epoch_mismatch(); /* L1 lifecycle fence fired */
		break;
	case CR_CACHE_MISS_BASE_LSN:
		cluster_cr_pool_note_base_lsn_mismatch();
		break;
	case CR_CACHE_MISS_KEY:
		cluster_cr_pool_note_key_mismatch();
		break;
	default:
		break;
	}
	if (hit != NULL) {
		/* spec-5.51 §3.4 + spec-5.56 ②: recheck the COMPOSITE fence for the
		 * within-call race (a global epoch bump OR a per-relation gen bump between
		 * the lookup and here makes the {start_epoch, start_rel_gen}-stamped entry
		 * stale).  start_epoch == 0 means the pool is disabled (no fence). */
		bool stale = false;

		if (start_epoch != 0) {
			if (cluster_cr_pool_current_epoch() != start_epoch) {
				stale = true;
			} else if (cluster_cr_pool_rel_generation_enabled()) {
				uint64 cur_gen = 0;

				(void)cluster_cr_pool_rel_generation(key.rlocator, &cur_gen);
				if (cur_gen != start_rel_gen)
					stale = true;
			}
		}
		if (!stale) {
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_cache_hit_count, 1);
			cr_note_retention_if_advanced(read_scn); /* spec-5.56 D2: observe only */
			return hit;
		}
		/* lifecycle event advanced mid-lookup: drop this now-stale hit and rebuild
		 * at the new (epoch, rel_gen) (the entry keeps its old stamp, so it can
		 * never be re-served). */
		cluster_cr_pool_note_l1_epoch_mismatch();
		start_epoch = cluster_cr_pool_current_epoch();
		start_rel_gen = 0;
		if (cluster_cr_pool_rel_generation_enabled())
			(void)cluster_cr_pool_rel_generation(key.rlocator, &start_rel_gen);
	}
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_miss_count, 1);

	slot = cluster_cr_cache_victim_slot(&key, start_epoch, &evicted);
	if (evicted && CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_evict_count, 1);

	/* spec-5.51 D4: L2 probe (copy-out into the L1 victim slot).  On an L2 hit,
	 * the image is now in L1; commit + return.  spec-5.56 ③ + P1-fix: lookup_copy
	 * returns the per-relation gen the copied bytes are valid for, CAPTURED UNDER
	 * THE POOL LOCK at copy time.  Stamp the L1 entry with THAT matched gen — NOT a
	 * fresh re-read of the current gen.  A racing unlink between the L2 copy and
	 * this commit advances the locator's current gen; re-reading it would brand the
	 * stale gen-N bytes with gen N+1, and a later L1 lookup at N+1 would pass the
	 * composite fence and serve a stale-lifecycle page (8.A false-visible).
	 * Stamping with the matched gen is self-fencing: any unlink past it (gen
	 * advances) makes the L1 entry MISS, so the stale page is never served. */
	{
		uint64 hit_gen = 0;

		if (start_epoch != 0 && cluster_cr_pool_lookup_copy_gen(&key, slot, &hit_gen)) {
			cluster_cr_cache_commit_slot_gen(hit_gen);
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_cache_install_count, 1);
			cr_note_retention_if_advanced(read_scn); /* spec-5.56 D2: observe only */
			return slot;
		}
	}

	/* L1+L2 miss: construct.  Two-phase L2 insert with construction outside the
	 * L2 lock; an ereport mid-construction must release the L2 reservation. */
	{
		ClusterCRPoolHandle ph;
		bool reserved = false;

		/*
		 * spec-5.56 D4 / INV-G3 (register-at-install): register the locator in the
		 * per-relation generation table BEFORE caching into L1 OR L2, so ANY cached
		 * entry's locator is tracked and a later unlink fences it (the global epoch
		 * does NOT bump for a tracked unlink in GO mode).  Covers BOTH the L2
		 * reserve below AND the bare-construct L1 commit (the admission-reject /
		 * L2-disabled path still commits to L1).  A register failure (gen table
		 * full) -> serve-but-skip-cache: construct and return the correct image
		 * WITHOUT caching it in L1 or L2 (so the untracked locator has no entry and
		 * its unlink is a safe no-op, P2-d′; rel_gen_table_overflow++ in register).
		 * Disabled -> install_gen stays 0 (epoch-only fence; spec-5.53 behavior).
		 */
		if (start_epoch != 0 && cluster_cr_pool_rel_generation_enabled()) {
			if (!cluster_cr_pool_register_locator(key.rlocator, &install_gen))
				skip_cache = true;
		}

		/*
		 * spec-5.52 D2: insert-side admission gate.  Only reserve+publish into
		 * L2 when the admission predicate admits this image; a reject bypasses
		 * the pool and falls through to the bare-construct else branch (F0-2),
		 * serving the SAME correct image (INV-A1).  admit_all (the default) ->
		 * always admit == spec-5.51 v1 (zero behavior change).  The predicate is
		 * advisory only and never affects the served image.
		 */
		if (!skip_cache && start_epoch != 0) {
			ClusterCRAdmitCtx actx = { .scan_kind = cluster_cr_admit_current_scan_kind() };

			if (cluster_cr_pool_admit(&key, &actx))
				reserved = cluster_cr_pool_reserve_gen(&key, install_gen, &ph);
			/* spec-5.52 D9: record the admission decision (advisory counter). */
			cluster_cr_admit_stat_bump(cluster_cr_admit_last_reason());
		}

		if (reserved) {
			PG_TRY();
			{
				(void)cluster_cr_construct_block_into(buf, read_scn, slot);
				cluster_cr_pool_publish(&ph, slot);
				cluster_cr_admit_note_published(&key); /* spec-5.52 D5: relcap */
				/* publish consumed the reservation;  the PG_CATCH only runs on
				 * an exception before this point, so no post-block abort check
				 * is needed (no dead `reserved = false` here). */
			}
			PG_CATCH();
			{
				cluster_cr_pool_abort(&ph);
				PG_RE_THROW();
			}
			PG_END_TRY();
		} else {
			/* L2 disabled / admission reject / skip-cache: bare construct
			 * (spec-3.10 path).  Still committed to L1 below unless skip_cache. */
			(void)cluster_cr_construct_block_into(buf, read_scn, slot);
		}
	}

	/* spec-5.56 D4: gen table full -> serve-but-skip-cache.  The freshly-built
	 * image is correct; just do NOT cache it (no L1 commit) so the untracked
	 * locator keeps zero entries (INV-G3) and its later unlink is a safe no-op. */
	if (skip_cache)
		return slot;

	/* spec-5.51 §3.4 + spec-5.56 ④: commit-time COMPOSITE recheck.  If a lifecycle
	 * event happened during construction — the global epoch advanced (coarse) OR
	 * this relation's per-relation generation advanced (a per-relation unlink in GO
	 * mode, compared against install_gen per P2-e′ so a first-registration is not
	 * falsely skipped) — serve the freshly-built (correct) image but do NOT commit
	 * it to L1 (the slot stays invalid -> not served later), so no stale entry
	 * persists. */
	if (start_epoch != 0) {
		bool gen_moved = false;

		if (cluster_cr_pool_rel_generation_enabled()) {
			uint64 cur_gen = 0;

			(void)cluster_cr_pool_rel_generation(key.rlocator, &cur_gen);
			gen_moved = (cur_gen != install_gen);
		}
		if (cluster_cr_pool_current_epoch() != start_epoch || gen_moved)
			return slot;
	}

	cluster_cr_cache_commit_slot_gen(install_gen);
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_install_count, 1);
	return slot;
}


/* ============================================================
 * Tuple remap (CR scratch page → HeapTupleData wrapper)
 * ============================================================ */

bool
cluster_cr_remap_tuple(const char *cr_page, OffsetNumber off, HeapTupleData *out_htup)
{
	Page page = (Page)cr_page;
	ItemId itemid;

	if (cr_page == NULL || out_htup == NULL)
		return false;
	if (off < FirstOffsetNumber || off > PageGetMaxOffsetNumber(page))
		return false;

	itemid = PageGetItemId(page, off);
	if (!ItemIdIsNormal(itemid))
		return false; /* LP_UNUSED / LP_DEAD / LP_REDIRECT: CR-removed or not a tuple */

	out_htup->t_len = ItemIdGetLength(itemid);
	out_htup->t_data = (HeapTupleHeader)PageGetItem(page, itemid);
	out_htup->t_tableOid = InvalidOid;
	ItemPointerSetOffsetNumber(&out_htup->t_self, off);
	/* Block number of t_self is filled by the caller (it knows the buffer's
	 * block); CR scratch alone does not carry it. */
	return true;
}


/* ============================================================
 * Tuple-level cluster visibility helpers (spec-3.9 Step 4.5)
 *
 *   spec-3.9 §5 places cluster_visibility_decide_tuple in the generic
 *   cluster visibility helper layer and cluster_visibility_decide_cr_tuple
 *   in cluster_cr.c.  linkdb has no standalone generic cluster_visibility.c
 *   (only cluster_visibility_inject.c), so both helpers live here for now;
 *   FLAG FOR USER CODEREVIEW — if a dedicated cluster_visibility.c is
 *   wanted, decide_tuple moves there unchanged.
 *
 *   Both decide visibility WITHOUT PG-native ProcArray/CLOG (AD-012 例外 9
 *   / spec-3.9 I-fail-2/3).  They use SCN-based decisions and the tuple
 *   header committed/invalid bits.
 *
 *   VISIBILITY SEMANTICS (MVP — flag for codereview):
 *     - decide_tuple is for the 3-tier fast-path exits, where the block is
 *       already at/before read_scn so the physical tuple IS the read_scn
 *       version.  When the tuple's ITL slot carries a valid commit_scn it
 *       defers to cluster_visibility_decide_by_scn(commit_scn, read_scn);
 *       otherwise it uses the tuple's xmin-committed / xmax-invalid bits.
 *     - decide_cr_tuple is for a CR image already reconstructed to read_scn:
 *       post-read_scn changes are undone, so the row is VISIBLE iff its
 *       image xmin is committed/frozen and its xmax is invalid.  No buffer,
 *       no hint-bit writes (I-lock-4).
 * ============================================================ */

/*
 * Pure tuple-header visibility approximation (no syscall, no ProcArray).
 *   VISIBLE iff xmin is committed/frozen AND xmax is invalid.
 */
static ClusterVisibilityDecision
cr_decide_by_infomask(HeapTupleHeader htup)
{
	bool xmin_committed = (htup->t_infomask & HEAP_XMIN_COMMITTED) != 0;
	bool xmax_invalid = (htup->t_infomask & HEAP_XMAX_INVALID) != 0
						|| !TransactionIdIsValid(HeapTupleHeaderGetRawXmax(htup));

	if (xmin_committed && xmax_invalid)
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}

ClusterVisibilityDecision
cluster_visibility_decide_tuple(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tup = htup->t_data;

	/*
	 * If the tuple's ITL slot carries an authoritative commit_scn, decide by
	 * SCN against the snapshot read_scn (cluster semantics).  Else fall back
	 * to the tuple-header committed/invalid bits.  Never consult
	 * ProcArray/CLOG (I-fail-2).
	 */
	if (BufferIsValid(buffer)) {
		Page page = BufferGetPage(buffer);

		if (PageHasItl(page) && tup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& tup->t_itl_slot_idx < CLUSTER_ITL_INITRANS_DEFAULT) {
			const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[tup->t_itl_slot_idx];

			if (SCN_VALID(slot->commit_scn) && SCN_VALID(snapshot->read_scn))
				return cluster_visibility_decide_by_scn(slot->commit_scn, snapshot->read_scn);
		}
	}

	return cr_decide_by_infomask(tup);
}

/*
 * spec-3.22: the xmax recycled-slot resolve outcome.  The exact scratch/overlay
 * sources plus the durable by-xid resolve (ClusterTTDurableResolve) collapse here
 * into the four cases the gate acts on.  INVALID_OR_AMBIGUOUS and SCAN_UNAVAILABLE
 * both fail closed; they stay distinct only for the D3 counter buckets.
 */
typedef enum ClusterCrXmaxResolve {
	CLUSTER_CR_XMAX_RESOLVED_SCN,		  /* exact commit_scn -> compare to read_scn */
	CLUSTER_CR_XMAX_RECYCLED,			  /* durable 0-match -> invisible IFF proof holds */
	CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS, /* delayed-cleanout / wrap residue -> fail closed */
	CLUSTER_CR_XMAX_SCAN_UNAVAILABLE	  /* degraded / unreadable scan -> fail closed */
} ClusterCrXmaxResolve;

/*
 * cluster_cr_retention_proof_valid -- spec-3.22 §2.2: prove that a durable
 * 0-match (the deleter's TT slot was recycled) implies the deleter committed
 * below the retention horizon, hence before this reader's snapshot, hence the
 * delete is visible at read_scn (the CR tuple is INVISIBLE).  Returns true only
 * when EVERY leg holds; otherwise a 0-match is just missing information and the
 * caller fails closed (规则 8.A -- never a guess, never Option A):
 *
 *	(a) own-instance node (a durable scan was actually possible);
 *	(b) the retention GUC is on right now;
 *	(c) no COMMITTED slot was recycled ungated this incarnation -- an off-window
 *	    could have recycled a committed-after slot, so a 0-match would not prove
 *	    below-horizon (spec-3.22 retention_off_recycle_count);
 *	(d) the current horizon is valid (cluster enabled -- a real lower bound);
 *	(e) the horizon is not newer than this reader's read_scn -- i.e. the reader is
 *	    protected by the horizon, so a slot recycled below the horizon committed
 *	    before this reader's snapshot.  A normal backend reader publishes its
 *	    read_scn, so the horizon (min over live readers) never exceeds it; this
 *	    leg fails closed only for an offline/stale read_scn (R8).
 */
static bool
cluster_cr_retention_proof_valid(SCN read_scn)
{
	SCN horizon = InvalidScn;

	if (!cluster_cr_retention_proof_origin_legs(&horizon))
		return false; /* (a)-(d) */

	/* (e) horizon must not be newer than read_scn (scn_time_cmp keeps the gate). */
	return scn_time_cmp(horizon, read_scn) <= 0;
}

/*
 * cluster_cr_retention_proof_origin_legs -- spec-6.12i CP5 (D-i4): the
 * ORIGIN-side legs (a)-(d) of the spec-3.22 retention proof, extracted so
 * the origin-verdict serve (cluster_cr_server.c) can evaluate them for a
 * cross-instance requester whose leg (e) — "horizon not newer than MY
 * read_scn" — can only be decided at the requester against the shipped
 * horizon.  Returns true with *out_horizon = the current horizon when every
 * origin leg holds:
 *
 *	(a) own-instance node (a durable scan was actually possible here);
 *	(b) the retention GUC is on right now;
 *	(c) no COMMITTED slot was ever recycled ungated this incarnation;
 *	(d) the current horizon is valid.
 *
 * ORDERING CONTRACT (soundness of the shipped bound): the caller must
 * evaluate these legs — in particular sample the horizon — AFTER its
 * complete by-xid scan finished.  The horizon is monotonic, so any slot
 * recycled before a scanned block was read had its commit_scn at or below
 * the recycle-time horizon, itself at or below this sample; sampling BEFORE
 * the scan would let a recycle land between sample and scan whose
 * commit_scn exceeds the shipped bound (false-visible direction, Rule 8.A).
 */
bool
cluster_cr_retention_proof_origin_legs(SCN *out_horizon)
{
	SCN horizon;

	if (out_horizon != NULL)
		*out_horizon = InvalidScn;

	if (cluster_node_id < 0)
		return false; /* (a) */
	if (!cluster_undo_retention_horizon_enabled)
		return false; /* (b) currently off */
	if (cluster_tt_slot_retention_off_recycle_count() != 0)
		return false; /* (c) an ungated recycle happened this incarnation */

	horizon = cluster_undo_retention_horizon();
	if (!SCN_VALID(horizon))
		return false; /* (d) cluster disabled / no horizon */

	if (out_horizon != NULL)
		*out_horizon = horizon;
	return true;
}

/* spec-3.22 D3: xmax resolve outcome counters (lock-free; bumped at the gate). */
static void
cluster_cr_count_xmax_resolved(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_resolved_count, 1);
}
static void
cluster_cr_count_xmax_recycled_invisible(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_recycled_invisible_count, 1);
}
static void
cluster_cr_count_xmax_invalid_or_ambiguous(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_invalid_or_ambiguous_count, 1);
}
static void
cluster_cr_count_xmax_scan_unavail_or_no_proof(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_scan_unavail_or_no_proof_count, 1);
}

/*
 * cluster_cr_resolved_scn_is_acceptable -- spec-4.8 D3 acceptance PREDICATE for a
 *	Source 3 by-xid RESOLVED match: the pure verdict, with NO observability side
 *	effect.  Extracted (spec-5.55 D6) so the fresh-scan path AND a shared-resolver-
 *	cache memo hit reach the SAME verdict (gate (3), verdict-equivalent by
 *	construction -- they cannot drift).
 *
 *	The durable scan runs WRAP_ANY (a recycled scratch ITL slot carries no
 *	binding wrap), so a single COMMITTED match cannot tell a genuine commit from a
 *	2^32-wrapped raw-xid collision.  When retention is unreliable AND the match is
 *	below the CURRENT horizon it is wrap-suspect -> NOT acceptable (规则 8.A: a
 *	wrong deleter scn would false-hide a live row).  The reliability proof is the
 *	EXACT sticky condition (horizon enabled AND no retention-off recycle this
 *	incarnation), not an abstraction -- losing the sticky leg would re-trust a
 *	retention-off-window recycle.  Returns true to ACCEPT (RESOLVED).
 */
static bool
cluster_cr_resolved_scn_is_acceptable(SCN scn)
{
	bool retention_reliable = cluster_undo_retention_horizon_enabled
							  && cluster_tt_slot_retention_off_recycle_count() == 0;

	return !cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn,
											 cluster_undo_retention_horizon(), retention_reliable);
}

/*
 * cluster_cr_accept_resolved_scn -- the AUTHORITATIVE acceptance gate: the pure
 *	predicate above PLUS the existing wrap_generation_disambiguated observability
 *	bump on a fail-closed verdict.  The authoritative fresh-scan path bumps the
 *	base counter exactly once.  Memo TRUST is disabled.
 *
 *	The MEASURE-mode memo probe (spec-5.55 Hardening v1.1, finding #1) must NOT use
 *	this wrapper: it is a non-authoritative shadow measurement that still falls
 *	through to the authoritative scan, so bumping here would DOUBLE-count the base
 *	wrap_generation_disambiguated counter on a wrap-suspect hit -- it calls the
 *	pure predicate instead.  Returns true to ACCEPT (RESOLVED).
 *
 *	spec-6.12i CP5 (D-i4) exports this gate: the origin-verdict serve
 *	(cluster_cr_server.c) vets its own complete-scan RESOLVED_SCN match with
 *	the SAME wrap-suspect acceptance before shipping a COMMITTED_EXACT
 *	verdict cross-instance (an authoritative resolution here too, so the
 *	observability bump semantics are identical).
 */
bool
cluster_cr_accept_resolved_scn(SCN scn)
{
	if (cluster_cr_resolved_scn_is_acceptable(scn))
		return true;
	cluster_tt_recovery_count_wrap_generation_disambiguated();
	return false; /* suspect -> fail closed */
}

/*
 * cluster_cr_resolve_xmax_commit_scn -- resolve the EXACT commit_scn of a
 * committed own-instance deleter (cr_xmax) recorded on a CR image, for the
 * spec-3.21 xmax-side visibility decision.  Returns true + *out_scn on success.
 *
 *	Used only for the committed-at/before-read_scn branch: the committed-AFTER-
 *	read_scn case is decided VISIBLE in the caller via the live slot (tier-2 has
 *	already proved the live deleter wrote after read_scn, hence its commit is also
 *	after read_scn -- the sound direction of write_scn, NOT the P1-a inverse).  So
 *	this resolver is reached only when the LIVE slot was recycled to a newer writer.
 *
 *	Exact-key source order (spec-3.21 D2; never a CLOG/write_scn proxy -- P1-a):
 *	  1. the CR SCRATCH page ITL slot at itl_idx, IFF slot.xid == cr_xmax;
 *	  2. the BOC / spec-3.6 overlay by exact key (scratch ITL ref + local_xid);
 *	  3. the durable TT by exact xid -- survives an ITL slot recycle.
 *	A slot/xid mismatch or InvalidScn at every source -> false, and the caller
 *	fails closed (53R9F); rule 8.A: an unresolved deleter is NEVER treated as a
 *	committed-before-read_scn delete (which would false-hide a live row).
 */
static ClusterCrXmaxResolve
cluster_cr_resolve_xmax_commit_scn(const char *cr_page, uint8 itl_idx, TransactionId cr_xmax,
								   SCN *out_scn)
{
	Page page = (Page)cr_page; /* read-only ITL access on the scratch image */
	uint32 expected_wrap = CLUSTER_TT_WRAP_ANY;
	/* spec-5.55 D1: Source 3 RESOLVED also reports the matched durable slot
	 * identity, cached as a position hint by the shared resolver cache (D5). */
	uint16 resolved_seg = 0;
	uint16 resolved_slot = 0;
	uint16 resolved_wrap = 0;
	uint64 resolver_scan_epoch;

	*out_scn = InvalidScn;

	/* Sources 1+2 require the scratch slot to still hold cr_xmax exactly. */
	if (itl_idx != CLUSTER_ITL_SLOT_UNALLOCATED && itl_idx < CLUSTER_ITL_INITRANS_DEFAULT
		&& PageHasItl(page)) {
		const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[itl_idx];

		if (slot->xid == cr_xmax) {
			ClusterUndoTTSlotRef ref;

			/*
			 * Source 1: the rolled-back scratch slot's own commit stamp.  NB: a
			 * lock-only ITL undo record for this same slot index, inverse-applied
			 * during the chain walk, can restore prev_commit_scn (= InvalidScn)
			 * over cr_xmax's later-stamped commit_scn while leaving slot.xid ==
			 * cr_xmax (cluster_cr_apply_itl_inverse restores commit_scn/flags/
			 * undo_segment_head but not xid).  In that case this and Source 2
			 * (whose ref came from the same rolled-back slot) both miss, and
			 * Source 3 (durable scan by exact xid) is the true authoritative
			 * fallback.  Never a wrong scn -- a stale InvalidScn just falls through.
			 */
			if (SCN_VALID(slot->commit_scn)) {
				*out_scn = slot->commit_scn;
				return CLUSTER_CR_XMAX_RESOLVED_SCN;
			}

			/* Source 2: BOC / overlay by the exact key (ref + local_xid). */
			if (cluster_itl_get_tt_ref(page, itl_idx, &ref)) {
				ClusterTTStatusKey key;
				ClusterTTStatusResult result;
				ClusterTTStatusSourceRequest source_request;
				ClusterTTStatusSourceResult source_result;
				bool found;

				memset(&key, 0, sizeof(key));
				key.origin_node_id = ref.origin_node_id;
				key.undo_segment_id = ref.undo_segment_id;
				key.tt_slot_id = ref.tt_slot_id;
				key.cluster_epoch = ref.cluster_epoch;
				key.local_xid = cr_xmax;

				memset(&source_request, 0, sizeof(source_request));
				source_request.key = &key;
				found = cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &source_request,
														  &source_result)
							== CLUSTER_SEMANTIC_ADMISSION_OK
						&& source_result.bool_value;
				result = source_result.lookup;
				if (found && result.authoritative
					&& (result.status == CLUSTER_TT_STATUS_COMMITTED
						|| result.status == CLUSTER_TT_STATUS_CLEANED_OUT)
					&& SCN_VALID(result.commit_scn)) {
					*out_scn = result.commit_scn;
					return CLUSTER_CR_XMAX_RESOLVED_SCN;
				}
			}
		}
	}

	/*
	 * spec-5.55 D6: diagnostic resolver hint.  TRUST is unconditionally disabled;
	 * even a revalidated, acceptable hint must fall through to the full Source 3
	 * scan.  The single carrier does not establish retained identity provenance.
	 * origin == own node (Source 3 is own-instance, gate (5)).
	 */
	{
		SCN hint_scn = InvalidScn;

		if (cluster_resolver_cache_probe((uint16)cluster_node_id, cr_xmax, &hint_scn)) {
			if (cluster_resolver_cache_trust()) {
				/*
				 * Dormant TRUST branch: cluster_resolver_cache_trust() always
				 * returns false.  This path cannot be enabled by configuration.
				 */
				bool accepted = cluster_cr_accept_resolved_scn(hint_scn);

				/*
				 * spec-5.55 Hardening v1.1 (finding #2): the memo-path fail-closed
				 * branch is a defensive 2^32-wrap guard the natural MVCC workload
				 * cannot reach (a resolved deleter is always recent -> scn >=
				 * horizon -> never suspect).  This injection forces it closed so a
				 * TAP test proves the branch routes (count_acceptance(false) ->
				 * INVALID_OR_AMBIGUOUS) and the 8.A invariant still holds.  It does
				 * NOT bump the base wrap_generation_disambiguated counter (no real
				 * wrap occurred).  Disarmed in production -> byte-identical.
				 */
				if (accepted
					&& cluster_cr_injection_armed("cluster-cr-resolver-memo-suspect", NULL))
					accepted = false;

				cluster_resolver_cache_count_acceptance(accepted);
				if (!accepted) {
					*out_scn = InvalidScn; /* same fail-closed as the fresh-scan branch */
					return CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS;
				}
				*out_scn = hint_scn; /* gate (1): durable re-read */
				return CLUSTER_CR_XMAX_RESOLVED_SCN;
			}

			/*
			 * MEASURE (spec-5.55 Hardening v1.1, finding #1): record the would-hit
			 * acceptance outcome with the PURE predicate, NOT the side-effecting
			 * gate.  This shadow probe falls through to the authoritative scan
			 * below, which bumps wrap_generation_disambiguated itself; using the
			 * wrapper here would double-count that base TT counter on a wrap-suspect
			 * hit.  The verdict is identical (the wrapper is predicate + side
			 * effect), so the resolver-cache acceptance counters stay accurate.
			 */
			cluster_resolver_cache_count_acceptance(
				cluster_cr_resolved_scn_is_acceptable(hint_scn));
			/* never trust the hint -- fall through to the authoritative scan. */
		}
	}

	/*
	 * Source 3: durable TT by exact xid (survives ITL slot recycle).  spec-3.22:
	 * consume the finer-grained resolve enum so a 0-match (RECYCLED_ZERO_MATCH ->
	 * provably below horizon, IF the gate's retention proof holds) is no longer
	 * conflated with a delayed-cleanout / wrap / unreadable miss (all fail closed).
	 */
	/* Preserve scan provenance: a late result must not become a new-epoch hint. */
	resolver_scan_epoch = cluster_resolver_xid_epoch();
	switch (cluster_tt_slot_durable_resolve_by_xid(cr_xmax, expected_wrap, out_scn, &resolved_seg,
												   &resolved_slot, &resolved_wrap)) {
	case CLUSTER_TT_DURABLE_RESOLVED_SCN:
		/*
		 * spec-4.8 D3 acceptance (now the shared cluster_cr_accept_resolved_scn
		 * helper, spec-5.55 D6, gate (3)): the durable scan ran WRAP_ANY so a
		 * single COMMITTED match below the horizon with unreliable retention is
		 * wrap-suspect -> fail closed (规则 8.A: a wrong deleter scn would false-
		 * hide a live row).  A diagnostic memo hit above used only the pure predicate.
		 */
		if (!cluster_cr_accept_resolved_scn(*out_scn)) {
			*out_scn = InvalidScn;
			return CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS;
		}
		/*
		 * spec-5.55 D5: cache the matched {seg,slot,wrap} position hint (own-
		 * instance gate (5) + COMMITTED gate (6) + acceptance-passed) so a peer
		 * backend can re-validate it in O(1) for diagnostics before its full scan.
		 * No-op unless the memo region is live.
		 */
		cluster_resolver_cache_install((uint16)cluster_node_id, cr_xmax, resolved_seg,
									   resolved_slot, resolved_wrap, *out_scn, resolver_scan_epoch);
		return CLUSTER_CR_XMAX_RESOLVED_SCN; /* *out_scn set by the resolve */
	case CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH:
		*out_scn = InvalidScn;
		return CLUSTER_CR_XMAX_RECYCLED;
	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
		*out_scn = InvalidScn;
		return CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS;
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
	default:
		*out_scn = InvalidScn;
		return CLUSTER_CR_XMAX_SCAN_UNAVAILABLE;
	}
}

ClusterVisibilityDecision
cluster_visibility_decide_cr_tuple(HeapTuple htup, Snapshot snapshot)
{
	HeapTupleHeader tup = htup->t_data;

	(void)snapshot; /* CR image already reconstructed to read_scn */

	/*
	 * The CR image tuple is the row as of read_scn: the chain walker has
	 * undone every change newer than read_scn.  Reasoning about
	 * visibility on the reconstructed image:
	 *
	 *   - A tuple that was INSERTED after read_scn was inverse-INSERTed to
	 *     LP_UNUSED, so it never reaches here (cluster_cr_remap_tuple returns
	 *     false -> the gate caller treats it invisible).  Therefore any tuple
	 *     present in the image existed at read_scn.
	 *   - A tuple DELETED after read_scn was inverse-DELETEd, restoring the
	 *     pre-delete image with xmax cleared -> visible.
	 *   - A tuple DELETED at/before read_scn keeps its xmax in the image
	 *     (the delete is not undone, SCN stop) -> invisible.
	 *
	 * So this image-level decision reduces to "is the reconstructed tuple
	 * still live (xmax invalid) as of read_scn".  We deliberately do NOT
	 * require the HEAP_XMIN_COMMITTED hint bit: it is a lazy optimization that
	 * is often unset in a captured undo image, and the chain walk already
	 * encodes the xmin-after-read_scn case via inverse-INSERT.
	 *
	 * The xmin-side (creation) check — a tuple still present in the image whose
	 * inserting xact committed AFTER read_scn (the UPDATE new-version and the
	 * H6 insert-then-late-commit cases) — is handled by the caller gate
	 * (cluster_cr_satisfies_mvcc, spec-3.9 Hardening L214) using the tuple's
	 * live ITL slot write_scn, because that needs the buffer/slot which this
	 * image-only helper deliberately does not take (I-lock-4 / I-fail-3).
	 */
	if (tup->t_infomask & HEAP_XMAX_INVALID)
		return CLUSTER_VISIBILITY_VISIBLE;
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tup)))
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}


/* ============================================================
 * MVCC 3-tier short-circuit gate (spec-3.9 Step 5)
 * ============================================================ */

/*
 * spec-3.24 D1: no-peer + session-local CR-gate fast path.  The pure verdict
 * (cluster_cr_no_peer_fastpath_decide) is unit-tested as a full truth table;
 * the eligible() wrapper binds it to live GUC / topology / snapshot state and
 * yields to a forced-CR test override.  See cluster_cr.h for the soundness
 * contract (AD-012 例外 9 row #1).
 */
/* Backend-lifetime cache for cluster_merged_any_remote_materialized(). */
static bool fastpath_any_materialized_known = false;
static bool fastpath_any_materialized = false;

bool
cluster_cr_no_peer_fastpath_decide(bool gate_on, bool has_peers, bool session_local,
								   bool has_materialized_remote)
{
	return gate_on && !has_peers && session_local && !has_materialized_remote;
}

bool
cluster_cr_no_peer_fastpath_eligible(Snapshot snapshot)
{
	/*
	 * Fail-closed: only a CLUSTER-source snapshot can take this path.  The
	 * caller (HeapTupleSatisfiesMVCC) already gates on cluster_source, but a
	 * LOCAL / static snapshot carries cluster_snapshot_session_local as plain
	 * padding -- guard here too so no other caller can fast-path one.
	 */
	if (snapshot == NULL || snapshot->cluster_source != (uint8)SNAPSHOT_SOURCE_CLUSTER)
		return false;

#ifdef ENABLE_INJECTION
	/* A forced-CR test must still exercise the cluster path. */
	if (cluster_test_force_visibility_cluster_path)
		return false;
#endif

	/*
	 * Fail-closed topology check: the fast path requires the topology to be
	 * KNOWN single-node.  cluster_conf_has_peers() alone is fail-OPEN here:
	 * it returns false while ClusterConfShmem is NULL or not yet populated
	 * (conf not loaded), which would route a multi-node deployment through
	 * PG-native during that window (t/203/208/209 ClusterPair nodes caught
	 * exactly this).  cluster_conf_node_count() == 1 holds only after a
	 * successful load that declared exactly this node (single-node degraded
	 * fallback included); 0 (not loaded) and >1 (peers) are both ineligible.
	 */
	if (cluster_conf_node_count() != 1)
		return false;

	/*
	 * spec-4.5a G6 (8.A): merged recovery may have materialized a dead
	 * peer's data on this node.  Those tuples carry foreign xids that must
	 * resolve through the origin-qualified authority (durable TT /
	 * pg_xact_remote); the PG-native body would alias them into this
	 * node's own CLOG (AD-012 例外 9).  One registry scan per backend:
	 * materialization only changes during startup recovery (no live
	 * backends), so the cache cannot go stale in the unsafe direction.
	 */
	if (!fastpath_any_materialized_known) {
		fastpath_any_materialized = cluster_merged_any_remote_materialized();
		fastpath_any_materialized_known = true;
	}

	return cluster_cr_no_peer_fastpath_decide(
		cluster_cr_gate_no_peer_fastpath, cluster_conf_has_peers(),
		snapshot->cluster_snapshot_session_local != 0, fastpath_any_materialized);
}

/*
 * cluster_cr_verdict_on_image -- decide the per-tuple visibility verdict of the
 * queried offnum on a reconstructed read_scn CR image (spec-3.9/3.21/3.22 logic,
 * extracted in spec-5.54 D4 so the full-block gate AND the tuple-level fast path
 * run the IDENTICAL verdict on their respective images -> INV-T1 bit-equivalence
 * by construction).  `slot` is the LIVE ITL slot of the queried tuple (drives the
 * tier-2 + live-slot shortcuts), NOT a scratch slot.  Returns DECIDED + out_visible,
 * FAILCLOSED (materialized-remote INDOUBT only), or ereports the precise SQLSTATE
 * on an own-instance unresolvable deleter (I-fail-1).  No CR construct here (the
 * caller already built cr_page), so it is reentrancy-safe for the fast path.
 */
static ClusterCrVerdict cluster_cr_verdict_on_image(const char *cr_page, OffsetNumber offnum,
													const ClusterItlSlotData *slot,
													Snapshot snapshot, bool remote_materialized,
													int32 remote_origin, bool *out_visible);

ClusterCrVerdict
cluster_cr_satisfies_mvcc(HeapTuple htup, Snapshot snapshot, Buffer buffer, bool *out_visible)
{
	HeapTupleHeader tup = htup->t_data;
	Page page;
	PageHeader phdr;
	uint8 itl_idx;
	const ClusterItlSlotData *slot;
	const char *cr_page;
	bool remote_materialized = false;
	int32 remote_origin = -1;

	if (unlikely(cluster_trace_visibility_enabled))
		cluster_catalog_trace_event("legacy_cr.entry", "gate_enabled", cluster_cr_mvcc_gate);

	/*
	 * Master switch (default on after spec-3.9 Hardening v1.0.1).  Turning it
	 * off is a diagnostic escape hatch; normal cluster snapshots take the CR
	 * path when the page/ITL SCN gates prove the tuple was modified after
	 * read_scn.
	 */
	if (!cluster_cr_mvcc_gate)
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	if (!cluster_enabled || !BufferIsValid(buffer)
		|| snapshot->cluster_source != (uint8)SNAPSHOT_SOURCE_CLUSTER)
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	page = BufferGetPage(buffer);
	if (unlikely(cluster_trace_visibility_enabled)) {
		cluster_catalog_trace_event("legacy_cr.page", "has_itl", PageHasItl(page));
		cluster_catalog_trace_event("legacy_cr.page", "block_scn",
									((PageHeader)page)->pd_block_scn);
	}
	if (!PageHasItl(page))
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	itl_idx = tup->t_itl_slot_idx;
	if (itl_idx == CLUSTER_ITL_SLOT_UNALLOCATED || itl_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	phdr = (PageHeader)page;

	/* A snapshot always needs a real transaction SCN. Shared-control page
	 * headers instead carry opaque mutation tokens: never order those
	 * against read_scn. Continue to the existing exact creator/ITL/origin
	 * checks; removing a coarse shortcut does not grant visibility. */
	if (!SCN_VALID(snapshot->read_scn))
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);
	if (!cluster_shared_config
		&& (!SCN_VALID(phdr->pd_block_scn)
			|| scn_time_cmp(phdr->pd_block_scn, snapshot->read_scn) <= 0))
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	slot = &ClusterPageGetItlSlots(page)[itl_idx];
	if (unlikely(cluster_trace_visibility_enabled))
		cluster_catalog_trace_slot("legacy_cr.selected", slot);

	/* This shortcut derives creator authority from the selected DATA slot.
	 * After an update or slot reuse the tuple index can name another writer,
	 * including a local lock owner for a foreign creator. Its UBA/SCN cannot
	 * authorize native xmin status or a verdict for this tuple. Decline the
	 * shortcut; the MVCC caller retains its exact creator/history resolver. */
	if (!TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tup)) || slot->flags < ITL_FLAG_ACTIVE
		|| slot->flags > ITL_FLAG_NEEDS_CLEANOUT
		|| !TransactionIdEquals(slot->xid, HeapTupleHeaderGetRawXmin(tup)))
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	/* Tier 2 (ITL gate): this tuple's own change is already in the snapshot. */
	if (!SCN_VALID(slot->write_scn))
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);
	if (scn_time_cmp(slot->write_scn, snapshot->read_scn) <= 0)
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);

	/* Tier 3 (spec-4.5a D8): own-instance, OR a remote instance whose undo
	 * this node MATERIALIZED during merged recovery (persistent
	 * merge_recovered_lsn authority).  A non-materialized remote origin
	 * keeps today's fall-through to the spec-3.2/3.3 remote-xid block;
	 * runtime warm cross-instance CR stays 4.6/4.7. */
	if (UBA_is_invalid(slot->undo_segment_head))
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);
	{
		int32 tuple_origin = (int32)uba_origin_node_id(slot->undo_segment_head);

		if (tuple_origin != cluster_node_id) {
			if (!cluster_merged_instance_is_materialized((int)tuple_origin))
				CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_NOT_APPLICABLE);
			remote_materialized = true;
			remote_origin = tuple_origin;
			/* spec-5.14 D2 class 4: this CR verdict consumes the remote origin's
			 * volatile xact outcome (via cluster_remote_outcome_durable_checked
			 * below) — stamp so a fail-stop of that origin aborts this tx. */
			cluster_touched_peers_stamp(remote_origin, CLUSTER_TOUCH_VISIBILITY);
		}
	}

	/*
	 * spec-3.19 D3: live-tuple xmin guard (fail-closed toward invisible).
	 *
	 * The CR image occupant of this offset can differ from the LIVE occupant
	 * after HOT line-pointer reuse + 8-slot ITL recycling, so the historical-
	 * image decision below must NOT be applied to a live tuple that is not
	 * itself a committed, fully-finished version.  Without this guard the gate
	 * reports the historical version's visibility for a live version whose own
	 * xmin HeapTupleSatisfiesUpdate then rejects -> TM_Invisible ("attempted to
	 * update invisible tuple", spec-3.19 D0/D1).
	 *
	 * The verdict must match what HeapTupleSatisfiesUpdate uses, NOT a CLOG-only
	 * test: the disagreement window is the COMMIT-IN-PROGRESS gap.  A committing
	 * xact sets its CLOG commit bit (TransactionIdDidCommit -> true) BEFORE it
	 * leaves the ProcArray (ProcArrayEndTransaction).  During that gap the native
	 * SatisfiesUpdate path still sees the writer via TransactionIdIsInProgress and
	 * returns TM_Invisible (D0 captured xmin{commit=1 inprog=1}).  So the live
	 * version is a valid visible update target only when its xmin is BOTH
	 * committed AND no longer in progress; otherwise (still in progress, in the
	 * commit gap, or aborted) it is visible to no snapshot -> invisible.
	 *
	 * Own-instance only (tier-3 above): the live xmin's CLOG + ProcArray state is
	 * local-authoritative.  Our own write is excluded (self-modification is
	 * handled by the native path).  The construct-time prune already drops
	 * post-read_scn *committed* versions; this guard closes the in-progress /
	 * commit-gap / aborted versions the prune cannot key off commit_scn.  The
	 * snapshot's correct older version is found by the normal chain walk.
	 */
	{
		TransactionId live_xmin = HeapTupleHeaderGetRawXmin(tup);

		/*
		 * spec-4.5a D8/G5: for a materialized-remote tuple the live xmin is
		 * the PEER's xid; the local ProcArray/CLOG would alias across
		 * instances (AD-012 例外 9), so resolve the creator's outcome from
		 * the per-origin authority (D10c) instead.  ABORTED creator ->
		 * invisible; COMMITTED -> the guard does not fire (a committed,
		 * finished creator is a valid version, fall through to construct);
		 * INDOUBT (B stamped its TT then crashed) -> fail closed (规则 8.A).
		 */
		if (remote_materialized) {
			SCN rscn;

			/*
			 * spec-4.5a G6 (P1 #2): wrap-check the creator's outcome against
			 * the origin's durable TT slots (by-xid scan, wrap-qualified) -- a
			 * bare (origin,xid) verdict could alias a same-xid wraparound
			 * overwrite of the outcome store.  Unprovable -> fail closed.
			 */
			switch (cluster_remote_outcome_durable_checked(remote_origin, live_xmin, &rscn)) {
			case CLUSTER_REMOTE_XACT_COMMITTED:
				break; /* finished creator -> construct CR image below */
			case CLUSTER_REMOTE_XACT_ABORTED:
				*out_visible = false;
				CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
			case CLUSTER_REMOTE_XACT_INDOUBT:
			default:
				CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_FAILCLOSED);
			}
		} else if (TransactionIdIsNormal(live_xmin)
				   && !TransactionIdIsCurrentTransactionId(live_xmin)
				   && (TransactionIdIsInProgress(live_xmin)
					   || !TransactionIdDidCommit(live_xmin))) {
			*out_visible = false;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		}
	}

	/*
	 * Gate fired: construct the read_scn block image (full-block CR, spec-3.10)
	 * and decide on the historical tuple.  cluster_cr_lookup_or_construct never
	 * returns NULL — it ereports the precise SQLSTATE on failure (I-fail-1).
	 * itl_idx is no longer passed (full-block rolls back every candidate chain);
	 * the queried tuple's live `slot` (already resolved above) still drives the
	 * tier-2 + xmin-side checks.
	 */
	/*
	 * spec-5.54 D3: tuple-level / verdict-only fast path (GUC, default off).  The
	 * 3-tier gate + live-xmin guard above have already fired; before materializing
	 * the whole read_scn block, try reconstructing ONLY the queried offnum from the
	 * single candidate chain and run the IDENTICAL verdict.  A DECIDED result is
	 * bit-equivalent to full-block (INV-T1); eligibility-false / NOT_APPLICABLE
	 * falls through to the authoritative full-block path below (fail-safe, INV-T2).
	 * Own-instance only: materialized-remote is excluded by eligibility, so the
	 * fast path is skipped for it and the full-block path keeps its 4.5a handling.
	 */
	if (cluster_cr_tuple_level_fastpath && !remote_materialized) {
		ClusterCRCandidateChain fp_chains[CLUSTER_ITL_INITRANS_DEFAULT];
		int fp_nchains;
		ClusterCRTupleOutcome fp_reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;

		fp_nchains
			= cluster_cr_collect_candidate_chains(ClusterPageGetItlSlots(page), snapshot->read_scn,
												  fp_chains, CLUSTER_ITL_INITRANS_DEFAULT);
		if (cluster_cr_tuple_eligible(buffer, htup, snapshot, slot, fp_nchains, &fp_reason)) {
			ClusterCrVerdict fp_v
				= cluster_cr_tuple_verdict(buffer, htup, snapshot, out_visible, &fp_reason);

			cluster_cr_tuple_stat_bump(fp_reason);
			if (fp_v != CLUSTER_CR_NOT_APPLICABLE)
				CLUSTER_CATALOG_TRACE_RETURN(
					fp_v); /* DECIDED -> bit-equivalent to full-block (INV-T1) */
		} else {
			cluster_cr_tuple_stat_bump(fp_reason);
		}
		/* not eligible / NOT_APPLICABLE -> fall through to authoritative full-block */
	}

	cr_page = cluster_cr_lookup_or_construct(buffer, snapshot->read_scn);
	return cluster_cr_verdict_on_image(cr_page, ItemPointerGetOffsetNumber(&htup->t_self), slot,
									   snapshot, remote_materialized, remote_origin, out_visible);
}

static ClusterCrVerdict
cluster_cr_verdict_on_image(const char *cr_page, OffsetNumber offnum,
							const ClusterItlSlotData *slot, Snapshot snapshot,
							bool remote_materialized, int32 remote_origin, bool *out_visible)
{
	HeapTupleData cr_htup;
	ClusterVisibilityDecision decision;

	if (!cluster_cr_remap_tuple(cr_page, offnum, &cr_htup)) {
		/* CR-removed (inverse-INSERT made it LP_UNUSED): the row did not
		 * exist at read_scn -> invisible. */
		*out_visible = false;
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
	}

	if (unlikely(cluster_trace_visibility_enabled))
		cluster_catalog_trace_header("legacy_cr.image", cr_htup.t_data, snapshot);

	/*
	 * spec-3.9 Hardening (L214): xmin-side (creation) visibility.
	 *
	 * spec-3.10 NOTE: this per-tuple check is now SUPERSEDED by the construct-
	 * time cluster_cr_prune_post_snapshot_versions() (full-block CR), which
	 * marks LP_UNUSED every post-read_scn-created tuple BEFORE remap — so any
	 * tuple reaching here has a pre-read_scn creator and cr_xmin != slot->xid,
	 * i.e. this branch never fires (verified across t/215 + t/216).  Kept as a
	 * zero-cost defense-in-depth backstop; removable in a future cleanup
	 * (spec-3.10 §v0.4 D-A).  L214 alone could NOT handle same-row-multi-update
	 * (a doubly-updated tuple's live slot is its latest modifier, not creator).
	 *
	 * cluster_visibility_decide_cr_tuple is xmax-only and assumes every tuple
	 * still present in the CR image existed at read_scn.  That holds for a
	 * fresh INSERT (inverse-INSERTed to LP_UNUSED -> remap false above) and a
	 * DELETE (inverse-DELETE clears xmax), but NOT for the NEW physical
	 * version produced by an UPDATE: the walker inverse-applies the update
	 * onto the OLD tuple's slot, leaving the new version present in the image
	 * with xmin = the updating xact.  If that xact committed after read_scn
	 * the new version did not exist at the snapshot and must be invisible, or
	 * the snapshot would see BOTH the old and the new value (caught by t/215
	 * L10).  This also closes the H6 "inserted-before-read_scn-but-committed-
	 * after" case.
	 *
	 * The tuple's own ITL slot is the writer that produced this version, and
	 * the tier-2 gate above already established slot.write_scn is newer than read_scn.  If
	 * that post-snapshot writer IS the image tuple's creator (slot.xid ==
	 * xmin) the row was CREATED after the snapshot -> invisible.  We key off
	 * write_scn, NOT commit_scn: under delayed cleanout a just-committed slot
	 * is still ACTIVE with commit_scn = InvalidScn, but write_scn is always
	 * stamped at write time, and commit_scn is not earlier than write_scn, so a
	 * write_scn newer than read_scn already implies the creator committed after
	 * the snapshot.  For
	 * a delete-marked old tuple slot.xid == xmax (the deleter), not xmin, so a
	 * genuine inverse-DELETE / inverse-UPDATE restore is left untouched.
	 */
	{
		TransactionId cr_xmin = HeapTupleHeaderGetRawXmin(cr_htup.t_data);

		if (TransactionIdIsValid(cr_xmin) && cr_xmin == slot->xid) {
			*out_visible = false;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		}
	}

	decision = cluster_visibility_decide_cr_tuple(&cr_htup, snapshot);
	if (decision == CLUSTER_VISIBILITY_VISIBLE) {
		/* xmax invalid -> the row was never deleted -> visible. */
		*out_visible = true;
		CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
	}

	/*
	 * spec-3.21 D1: the CR image carries a VALID xmax.  The pre-3.21 code
	 * returned invisible here ("any valid xmax -> deleted at read_scn"), but a
	 * valid xmax only proves SOME xact wrote a delete/lock mark -- not that the
	 * delete COMMITTED at/before read_scn.  When the deleter is lock-only,
	 * in-progress, aborted, or committed AFTER read_scn, the row was LIVE at the
	 * snapshot and must be VISIBLE.  Mis-hiding it produced silent hot-row
	 * UPDATE 0 / lost updates (D0.6: 538 in-progress false-invisibles).  Resolve
	 * the deleter's commit-state vs read_scn instead (own-instance, tier-3).
	 */
	{
		HeapTupleHeader cr_tup = cr_htup.t_data;
		uint16 cr_infomask = cr_tup->t_infomask;
		TransactionId cr_xmax;
		ClusterTTStatus xmax_status;
		ClusterVisibilityDecision scn_decision = CLUSTER_VISIBILITY_UNKNOWN;

		/*
		 * A lock-only xmax never deletes the tuple (spec-3.21 P1-c).
		 * HEAP_XMAX_IS_LOCKED_ONLY also catches HEAP_LOCKED_UPGRADED (the legacy
		 * multi-locker pattern has HEAP_XMAX_LOCK_ONLY set), so it is handled
		 * here -- and HeapTupleGetUpdateXid below (reached only on the MULTI
		 * branch) is thus never called on a lock-only multi (its internal
		 * Assert(!LOCK_ONLY) holds).
		 */
		if (HEAP_XMAX_IS_LOCKED_ONLY(cr_infomask)) {
			*out_visible = true;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		}

		/* MultiXact: decode the UPDATE member; a lockers-only multi has no
		 * update xid -> visible.  Never treat the multi value as a plain xid. */
		if (cr_infomask & HEAP_XMAX_IS_MULTI)
			cr_xmax = HeapTupleGetUpdateXid(cr_tup);
		else
			cr_xmax = HeapTupleHeaderGetRawXmax(cr_tup);
		if (unlikely(cluster_trace_visibility_enabled))
			cluster_catalog_trace_event("legacy_cr.image", "effective_xmax", cr_xmax);


		if (!TransactionIdIsValid(cr_xmax)) {
			/* lockers-only multi: no update xid -> never a delete. */
			*out_visible = true;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		}

		/*
		 * spec-4.5a D8/G5: a materialized-remote deleter's commit-state comes
		 * from the per-origin authority (D10c), NOT the local raw-xid
		 * ProcArray/CLOG/durable-TT (AD-012 例外 9).  COMMITTED -> visible at
		 * read_scn IFF the commit SCN is after read_scn (delete committed
		 * after the snapshot -> row was live -> VISIBLE); ABORTED deleter ->
		 * the row was never deleted -> VISIBLE; INDOUBT -> fail closed.
		 *
		 * PGRAC serve-stall round-6: checked BEFORE the raw current-xid test
		 * below -- on a materialized chain a raw match can be a below-floor
		 * collision with the merged peer's xid; "our own delete" may only be
		 * concluded on the own-instance arm, where tier-3 (chain UBA origin ==
		 * self) makes the raw xid alias-free.
		 */
		if (remote_materialized) {
			/*
			 * The per-origin authority gives the deleter's outcome; for a
			 * COMMITTED deleter the SCN comparison feeds the SAME verdict
			 * table the own-instance path uses (decide_by_scn(commit_scn,
			 * read_scn) VISIBLE => the delete is visible at read_scn =>
			 * tuple INVISIBLE), so the direction is the audited one.
			 *
			 * spec-4.5a G6 (P1 #2): wrap-check the deleter against the
			 * origin's durable TT slots (by-xid scan, wrap-qualified), so a
			 * same-xid wraparound overwrite of the outcome store cannot alias
			 * the deleter; unprovable -> fail closed (规则 8.A).
			 */
			SCN rscn;

			switch (cluster_remote_outcome_durable_checked(remote_origin, cr_xmax, &rscn)) {
			case CLUSTER_REMOTE_XACT_COMMITTED:
				xmax_status = CLUSTER_TT_STATUS_COMMITTED;
				scn_decision = cluster_visibility_decide_by_scn(rscn, snapshot->read_scn);
				break;
			case CLUSTER_REMOTE_XACT_ABORTED:
				xmax_status = CLUSTER_TT_STATUS_ABORTED;
				break;
			case CLUSTER_REMOTE_XACT_INDOUBT:
			default:
				CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_FAILCLOSED);
			}
		}
		/* Own-instance authoritative classification of the deleting xact
		 * (tier-3: chain UBA origin == self, so the raw xid is alias-free). */
		else if (TransactionIdIsCurrentTransactionId(cr_xmax)) {
			if (unlikely(cluster_trace_visibility_enabled))
				cluster_catalog_trace_event("legacy_cr.xmax", "self", cr_xmax);
			/* our own delete (native handles self) */
			*out_visible = true;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		} else if (TransactionIdIsInProgress(cr_xmax))
			xmax_status = CLUSTER_TT_STATUS_IN_PROGRESS;
		else if (!TransactionIdDidCommit(cr_xmax))
			xmax_status = CLUSTER_TT_STATUS_ABORTED;
		else {
			/* Committed deleter: invisible IFF the delete is visible at read_scn. */
			SCN xmax_cscn;

			/*
			 * Live-slot shortcut: if the LIVE tuple's ITL slot still holds cr_xmax
			 * (the deleter), tier-2 above already proved the slot wrote after
			 * read_scn, so its commit (at or after the write) is also after
			 * read_scn -- the delete committed AFTER the snapshot -> the row was
			 * live at read_scn -> VISIBLE.  (Sound direction of write_scn; P1-a
			 * forbids only the inverse.)  This is the common RR-snapshot +
			 * concurrent-commit case (t/229 L6) whose deleter slot has not been
			 * recycled; its commit_scn need not be stamped yet.
			 */
			if (slot->xid == cr_xmax) {
				*out_visible = true;
				CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
			}

			/*
			 * The live slot was recycled to a newer writer (slot->xid != cr_xmax):
			 * resolve cr_xmax's commit-state by exact xid (P1-a: no proxy).  A
			 * durable 0-match (RECYCLED) is provably below the retention horizon --
			 * hence before this snapshot, hence the delete is visible -> the CR
			 * tuple is INVISIBLE -- but only when the retention proof holds; all
			 * other resolves either compare an exact commit_scn or fail closed
			 * (53R9F).  spec-3.22 §2.2 / 规则 8.A: a 0-match without proof is missing
			 * information, never an Option-A guess.
			 */
			switch (cluster_cr_resolve_xmax_commit_scn(cr_page, cr_tup->t_itl_slot_idx, cr_xmax,
													   &xmax_cscn)) {
			case CLUSTER_CR_XMAX_RESOLVED_SCN:
				cluster_cr_count_xmax_resolved();
				xmax_status = CLUSTER_TT_STATUS_COMMITTED;
				scn_decision = cluster_visibility_decide_by_scn(xmax_cscn, snapshot->read_scn);
				break; /* exits this resolve switch; the verdict switch below decides */

			case CLUSTER_CR_XMAX_RECYCLED:
				if (cluster_cr_retention_proof_valid(snapshot->read_scn)) {
					cluster_cr_count_xmax_recycled_invisible();
					/* spec-4.8 D4 (task#84): surface the recycled-slot liveness
					 * relax under the tt_recovery category too.  The §3.5 proof
					 * chain's own-instance legs (a-e) ARE this 3.22 retention
					 * proof + D3's generation gate (condition 6); a 53R9F that
					 * this proof turns into a sound INVISIBLE is the relax.  Cross-node
					 * INDOUBT has no relax arm: recovered_through >= page LSN proves
					 * the deleter's WRITE is covered, not its (later) COMMIT LSN, so an
					 * INDOUBT could be an as-yet-unrecovered commit -> relaxing it would
					 * be false-visible (规则 8.A).  Q5 strict fallback applies; forward. */
					cluster_tt_recovery_count_recycled_liveness_relaxed();
					*out_visible = false; /* deleter proven below horizon -> invisible */
					CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
				}
				cluster_cr_count_xmax_scan_unavail_or_no_proof();
				ereport(
					ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR cannot resolve commit_scn for recycled deleting xmax %u",
							cr_xmax),
					 errhint("the deleter's TT slot was recycled but the retention proof is "
							 "unavailable (retention off, invalid horizon, or a read_scn older "
							 "than the horizon); retry with a fresh snapshot.")));
				pg_unreachable();

			case CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS:
				cluster_cr_count_xmax_invalid_or_ambiguous();
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
								errmsg("cluster CR cannot resolve commit_scn for deleting xmax %u",
									   cr_xmax),
								errhint("delayed cleanout (commit_scn not yet stamped) or xid-wrap "
										"residue; retry with a fresh snapshot.")));
				pg_unreachable();

			case CLUSTER_CR_XMAX_SCAN_UNAVAILABLE:
			default:
				cluster_cr_count_xmax_scan_unavail_or_no_proof();
				ereport(
					ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR durable scan unavailable for deleting xmax %u", cr_xmax),
					 errhint("degraded node or an unreadable undo segment prevented a complete "
							 "by-xid scan; retry with a fresh snapshot.")));
			}
		}

		switch (cluster_vis_cr_xmax_verdict(xmax_status, scn_decision)) {
		case CVV_VISIBLE:
			*out_visible = true;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		case CVV_INVISIBLE:
			*out_visible = false;
			CLUSTER_CATALOG_TRACE_RETURN(CLUSTER_CR_DECIDED);
		case CVV_FAILCLOSED_UNKNOWN:
		default:
			/*
			 * cluster_vis_cr_xmax_verdict only ever returns VISIBLE / INVISIBLE /
			 * FAILCLOSED_UNKNOWN; the wait/conflict/gone verdicts (CVV_BEING_
			 * MODIFIED / CVV_GONE_* / CVV_FAILCLOSED_CONFLICT) belong to the
			 * SatisfiesUpdate/Dirty forks, not this snapshot-read gate.  Any of
			 * them reaching here would be a verdict-table bug -> fail closed.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR xmax visibility unresolved for deleting xmax %u", cr_xmax),
					 errhint("deleter commit_scn could not be proven against read_scn; retry.")));
		}
	}

	pg_unreachable(); /* every verdict branch above returns or ereports */
}

/*
 * cluster_cr_tuple_eligible -- spec-5.54 D1 Buffer wrapper over the pure static
 * predicate.  Resolves the queried offnum + the tuple's undo origin from the live
 * buffer + slot, then defers to cluster_cr_tuple_eligible_page.  Precondition
 * (gate-enforced): the 3-tier gate already proved slot->undo_segment_head valid.
 */
bool
cluster_cr_tuple_eligible(Buffer buf, HeapTuple htup, Snapshot snapshot,
						  const ClusterItlSlotData *slot, int nchains,
						  ClusterCRTupleOutcome *reason)
{
	Page page = BufferGetPage(buf);
	OffsetNumber off = ItemPointerGetOffsetNumber(&htup->t_self);
	int32 tuple_origin = (int32)uba_origin_node_id(slot->undo_segment_head);

	return cluster_cr_tuple_eligible_page(page, off, snapshot->read_scn, nchains, tuple_origin,
										  cluster_node_id, reason);
}

/*
 * cluster_cr_tuple_verdict -- spec-5.54 D2/D4 single-chain target-offnum verdict.
 *
 * Reconstructs ONLY the queried offnum on the separate backend-local fast scratch
 * by walking the single candidate chain (TargetTupleCrState contract), then runs
 * the IDENTICAL cluster_cr_verdict_on_image the full-block gate runs -> a DECIDED
 * verdict is bit-equivalent to full-block (INV-T1).  Any walk uncertainty
 * (foreign identity / no space / malformed / missing undo / cliff / cross-instance)
 * returns NOT_APPLICABLE so the caller defers to the authoritative full-block path
 * (INV-T2/T3); the fast path never guesses and never fail-closes from its narrower
 * view (full-block re-walks the same single chain and produces the precise
 * SQLSTATE for a genuine terminal).
 *
 * I-lock invariants are inherited from the full-block path: the caller holds the
 * content lock, we only read live-page bytes + the undo chain into the scratch,
 * no buffer lock / no WAL / no dirty (spec-3.9 I-lock-1/2/4).
 */
ClusterCrVerdict
cluster_cr_tuple_verdict(Buffer buf, HeapTuple htup, Snapshot snapshot, bool *out_visible,
						 ClusterCRTupleOutcome *out_reason)
{
	Page live_page = BufferGetPage(buf);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(&htup->t_self);
	ClusterCRCandidateChain chains[CLUSTER_ITL_INITRANS_DEFAULT];
	int nchains;
	TransactionId candidate_xid;
	const ClusterItlSlotData *live_slot;
	uint8 itl_idx;
	UBA uba;
	uint32 steps = 0;
	uint32 max_steps = (uint32)cluster_cr_chain_walk_max_steps;
	PGAlignedBlock record_buf;
	RelFileLocator cur_locator;
	ForkNumber cur_fork;
	BlockNumber cur_block;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME_VERDICT;

	/*
	 * Re-collect the single candidate chain (cheap O(ITL)); eligibility already
	 * proved nchains==1 + own-instance + watermark, but re-derive the chain head +
	 * candidate xid here and fail-safe to full-block if anything changed.
	 */
	nchains
		= cluster_cr_collect_candidate_chains(ClusterPageGetItlSlots(live_page), snapshot->read_scn,
											  chains, CLUSTER_ITL_INITRANS_DEFAULT);
	if (nchains != 1) {
		if (out_reason != NULL)
			*out_reason = CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN;
		return CLUSTER_CR_NOT_APPLICABLE;
	}
	candidate_xid = chains[0].xid;

	/* The queried tuple's LIVE ITL slot drives cluster_cr_verdict_on_image's
	 * tier-2 / live-slot shortcuts (the same slot the gate resolved). */
	itl_idx = htup->t_data->t_itl_slot_idx;
	if (itl_idx == CLUSTER_ITL_SLOT_UNALLOCATED || itl_idx >= CLUSTER_ITL_INITRANS_DEFAULT) {
		if (out_reason != NULL)
			*out_reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
		return CLUSTER_CR_NOT_APPLICABLE;
	}
	live_slot = &ClusterPageGetItlSlots(live_page)[itl_idx];

	/* Materialize the target on the SEPARATE fast scratch (never cr_scratch). */
	if (cr_tuple_scratch == NULL)
		cr_tuple_scratch = MemoryContextAllocZero(TopMemoryContext, BLCKSZ);
	memcpy(cr_tuple_scratch, live_page, BLCKSZ);

	BufferGetTag(buf, &cur_locator, &cur_fork, &cur_block);

	/* prune-before (target only): mirror full-block prune-first restricted to the
	 * queried offnum (F0-25). */
	cluster_cr_tuple_prune_target(cr_tuple_scratch, offnum, candidate_xid);

	uba = chains[0].undo_segment_head;
	while (!UBA_is_invalid(uba)) {
		const UndoRecordHeader *hdr;
		size_t len;
		uint32 seg;
		uint32 blk;
		uint16 tt_off;
		uint16 row_off;

		if (++steps > max_steps) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_CLIFF; /* full-block re-walk -> data_corrupted */
			goto fallback;
		}
		if (!uba_decode(uba, &seg, &blk, &tt_off, &row_off)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			goto fallback;
		}
		len = cluster_undo_get_record(uba, record_buf.data, sizeof(record_buf.data));
		if (len < sizeof(UndoRecordHeader)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN; /* missing/short undo -> full-block */
			goto fallback;
		}
		hdr = (const UndoRecordHeader *)record_buf.data;

		if (hdr->origin_node_id != (uint16)cluster_node_id
			&& !cluster_merged_instance_is_materialized((int)hdr->origin_node_id)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_CROSS_BLOCK; /* cross-instance UBA -> full-block */
			goto fallback;
		}

		/* I-chain-1: normal SCN stop -> read_scn reached (clean chain end). */
		if (scn_time_cmp(hdr->write_scn, snapshot->read_scn) <= 0)
			break;

		if (!RelFileNumberIsValid(hdr->target_locator.relNumber)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			goto fallback;
		}

		/* spec-3.20 D3.A (F8) block-scope filter: skip-apply records for other
		 * relations/forks/blocks but KEEP walking prev_uba. */
		if (!RelFileLocatorEquals(hdr->target_locator, cur_locator) || hdr->target_fork != cur_fork
			|| hdr->target_block != cur_block) {
			uba = hdr->prev_uba;
			continue;
		}

		if (cluster_cr_tuple_apply_record(cr_tuple_scratch, offnum, record_buf.data, len, &reason)
			== CR_TUPLE_APPLY_FALLBACK)
			goto fallback;

		/* per-step target prune (mirror full-block per-step whole-page prune). */
		cluster_cr_tuple_prune_target(cr_tuple_scratch, offnum, candidate_xid);
		uba = hdr->prev_uba;
	}

	/* prune-after (target only). */
	cluster_cr_tuple_prune_target(cr_tuple_scratch, offnum, candidate_xid);

	/*
	 * Run the IDENTICAL verdict the full-block gate runs.  Own-instance only (the
	 * fast path is never eligible for a materialized-remote tuple), so pass
	 * remote_materialized=false; an own-instance unresolvable deleter ereports the
	 * same SQLSTATE the full-block path would (authoritative).
	 */
	if (out_reason != NULL)
		*out_reason = CR_TUPLE_OUTCOME_VERDICT;
	return cluster_cr_verdict_on_image(cr_tuple_scratch, offnum, live_slot, snapshot,
									   /*remote_materialized*/ false, /*remote_origin*/ -1,
									   out_visible);

fallback:
	if (out_reason != NULL)
		*out_reason = reason;
	return CLUSTER_CR_NOT_APPLICABLE;
}
