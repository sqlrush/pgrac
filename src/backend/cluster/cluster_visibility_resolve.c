/*-------------------------------------------------------------------------
 *
 * cluster_visibility_resolve.c
 *	  pgrac single tuple-xid cluster status resolver (spec-3.14 D1).
 *
 *	  See cluster_visibility_resolve.h for the architectural rationale
 *	  (L212 anti-divergence: one evidence/status resolver, five variant
 *	  policies).  This file holds the resolver body extracted from the
 *	  spec-3.2/3.3 HeapTupleSatisfiesMVCC fork; the MVCC fork is
 *	  refactored to call it (spec-3.14 step 2, behaviour-equivalent on
 *	  the happy path, strictly earlier fail-closed on the slot-reuse
 *	  edge thanks to the explicit local_xid == raw_xid check).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_visibility_resolve.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.14-remaining-visibility-paths.md (FROZEN v0.2) §2.1.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h" /* GCS-race round-3b P0-2: inline native-prehistory status */
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lwlock.h" /* GCS-race round-3b: XactTruncationLock CLOG gate */
#include "storage/proc.h"
#include "utils/wait_event.h" /* spec-6.14 D10b ClusterCatalogVisResolve */

#include "cluster/cluster_catalog_stats.h" /* spec-6.14 D10b counters */
#include "cluster/cluster_cr.h"			   /* spec-6.15 D4: underivable counter */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"				/* cluster_node_id, subtrans depth */
#include "cluster/cluster_itl.h"				/* get_tt_ref / lock ref / multixact origin */
#include "cluster/cluster_itl_slot.h"			/* CLUSTER_ITL_SLOT_UNALLOCATED */
#include "cluster/cluster_recovery_merge.h"		/* spec-4.5a G6: materialized authority gate */
#include "cluster/cluster_remote_xact.h"		/* spec-4.5a G6: wrap-checked authority */
#include "cluster/cluster_runtime_visibility.h" /* spec-6.12i CP3: active-runtime resolve */
#include "cluster/cluster_subtrans.h"			/* SUBCOMMITTED parent follow */
#include "cluster/cluster_tt_durable.h"			/* spec-4.8 D2 remote_active_failclosed counter */
#include "cluster/cluster_tt_status.h"			/* lookup_exact / Key / Result */
#include "cluster/cluster_touched_peers.h"		/* spec-5.14 D2 class 4 */
#include "cluster/cluster_tx_resolve.h"			/* exact DATA->canonical TT fallback */
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_wal_state.h"	   /* CLUSTER_WAL_STATE_SLOT_COUNT */
#include "cluster/cluster_xid_authority.h" /* GCS-race round-2 RC-E: native-prehistory gate */
#include "cluster/cluster_xid_stripe.h"	   /* spec-6.15 D4: origin derivation */
#include "cluster/cluster_xnode_lever.h"   /* spec-6.12c: terminal memo + D0 counters */
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D3: profiling probes */

/*
 * Backend-lifetime cache over cluster_merged_instance_is_materialized().
 * The STALE branch consults it per foreign recycled ref; the marker is the
 * authority that this origin's heap state COMPLETELY covers the merge window
 * (a partial-merge residual store/durable without a marker must NOT be read,
 * per the cluster_recovery_merge.c marker contract).  Materialization only
 * happens during startup recovery, so the cache cannot go stale in the unsafe
 * (false-negative) direction.
 */
static int8 vis_origin_materialized_cache[CLUSTER_WAL_STATE_SLOT_COUNT]; /* 0 ? / 1 / -1 */
static bool vis_freshref_first_unproven_logged = false;
static bool vis_scratch_history_first_unproven_logged = false;

/* Round-3b RISK-1: the pure prehistory status mapper mirrors the CLOG
 * alphabet without dragging clog.h into the standalone unit layer; pin the
 * mirror to the real constants here, at the consumer that has both. */
StaticAssertDecl(CLUSTER_NATIVE_CLOG_IN_PROGRESS == TRANSACTION_STATUS_IN_PROGRESS,
				 "prehistory status mirror drifted from clog.h");
StaticAssertDecl(CLUSTER_NATIVE_CLOG_COMMITTED == TRANSACTION_STATUS_COMMITTED,
				 "prehistory status mirror drifted from clog.h");
StaticAssertDecl(CLUSTER_NATIVE_CLOG_ABORTED == TRANSACTION_STATUS_ABORTED,
				 "prehistory status mirror drifted from clog.h");
StaticAssertDecl(CLUSTER_NATIVE_CLOG_SUB_COMMITTED == TRANSACTION_STATUS_SUB_COMMITTED,
				 "prehistory status mirror drifted from clog.h");

static bool
vis_origin_materialized(int origin)
{
	if (origin < 0 || origin >= CLUSTER_WAL_STATE_SLOT_COUNT)
		return false;
	if (vis_origin_materialized_cache[origin] == 0)
		vis_origin_materialized_cache[origin]
			= cluster_merged_instance_is_materialized(origin) ? 1 : -1;
	return vis_origin_materialized_cache[origin] == 1;
}


/*
 * PGRAC: spec-6.14 D8 -- catalog-safe no-recursion guard.
 *
 * Under cluster.shared_catalog, catalog tuples themselves resolve through
 * this file, so the resolve path must stay catalog-free: a syscache /
 * systable access from inside a resolve would take a fresh catalog snapshot
 * whose tuples resolve through this same path -- the spec-3.3 startup
 * circularity (catalog scan -> cluster_tt lookup -> catalog scan), now
 * reachable at steady state.  The TT / undo substrate honours this by
 * design (spec-3.27 identity-only: undo-segment identity and xid->node
 * routing are configuration-derived, never catalog-derived); this counter
 * turns that contract into an enforced invariant.  GetCatalogSnapshot
 * checks it and fail-stops instead of recursing.
 *
 * The counter is bumped around every ref classification (local evidence
 * included -- the contract is "the resolver is catalog-free", not "remote
 * lookups are").  An ERROR escaping mid-resolve unwinds past the decrement,
 * so (Sub)AbortTransaction resets it via cluster_vis_resolve_abort_reset().
 */
static int cluster_vis_resolve_depth = 0;

/*
 * PGRAC: a bound is not an exact commit SCN and must never enter the exact
 * terminal memo.  This separate, one-entry derivative reuses only the already
 * proven predicate committed(X) && commit_scn(X) <= H <= R for the SAME R.
 * No shared authority, raw-xid-only identity, TTL or page stamping is involved.
 * Consult it only after the normal fresh-ref/origin/eligibility classifiers.
 */
static struct {
	bool valid;
	LocalTransactionId lxid;
	uint64 epoch;
	SCN read_scn;
	SCN horizon_scn;
	ClusterUndoTTSlotRef ref;
} vis_snapshot_bound;

static bool
vis_snapshot_bound_context(const ClusterUndoTTSlotRef *ref, SCN read_scn)
{
	return cluster_page_scn_shortcut && MyProc != NULL && LocalTransactionIdIsValid(MyProc->lxid)
		   && SCN_VALID(read_scn) && ref->cluster_epoch == cluster_epoch_get_current();
}

static bool
vis_snapshot_bound_probe(const ClusterUndoTTSlotRef *ref, SCN read_scn, ClusterVisResolve *out)
{
	const ClusterUndoTTSlotRef *saved = &vis_snapshot_bound.ref;

	if (!vis_snapshot_bound.valid)
		return false;
	if (!vis_snapshot_bound_context(ref, read_scn) || vis_snapshot_bound.lxid != MyProc->lxid
		|| vis_snapshot_bound.epoch != cluster_epoch_get_current()
		|| vis_snapshot_bound.read_scn != read_scn || saved->origin_node_id != ref->origin_node_id
		|| saved->undo_segment_id != ref->undo_segment_id || saved->tt_slot_id != ref->tt_slot_id
		|| saved->cluster_epoch != ref->cluster_epoch || saved->local_xid != ref->local_xid
		|| saved->has_cached_status != ref->has_cached_status
		|| saved->cached_commit_scn != ref->cached_commit_scn) {
		vis_snapshot_bound.valid = false;
		return false;
	}
	out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	out->status = CLUSTER_TT_STATUS_COMMITTED;
	out->commit_scn = vis_snapshot_bound.horizon_scn;
	out->commit_scn_is_bound = true;
	return true;
}

static void
vis_snapshot_bound_install(const ClusterUndoTTSlotRef *ref, SCN read_scn, SCN horizon_scn)
{
	vis_snapshot_bound.valid = false;
	if (!vis_snapshot_bound_context(ref, read_scn) || !SCN_VALID(horizon_scn)
		|| scn_time_cmp(horizon_scn, read_scn) > 0)
		return;
	vis_snapshot_bound.lxid = MyProc->lxid;
	vis_snapshot_bound.epoch = ref->cluster_epoch;
	vis_snapshot_bound.read_scn = read_scn;
	vis_snapshot_bound.horizon_scn = horizon_scn;
	vis_snapshot_bound.ref = *ref;
	vis_snapshot_bound.valid = true;
}

static bool
cluster_vis_from_exact_tx_resolution(ClusterTxOutcome outcome,
									 const ClusterTxResolution *resolution, ClusterVisResolve *out)
{
	if (resolution == NULL || out == NULL || outcome != resolution->outcome)
		return false;

	out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	out->commit_scn_is_bound = false;
	switch (outcome) {
	case CLUSTER_TX_COMMITTED:
		if (!SCN_VALID(resolution->commit_scn))
			break;
		out->status = CLUSTER_TT_STATUS_COMMITTED;
		out->commit_scn = resolution->commit_scn;
		return true;
	case CLUSTER_TX_ABORTED:
		out->status = CLUSTER_TT_STATUS_ABORTED;
		out->commit_scn = InvalidScn;
		return true;
	case CLUSTER_TX_IN_PROGRESS:
		out->status = CLUSTER_TT_STATUS_IN_PROGRESS;
		out->commit_scn = InvalidScn;
		return true;
	case CLUSTER_TX_PREPARED:
	case CLUSTER_TX_UNKNOWN:
	default:
		break;
	}
	out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;
	return false;
}

bool
cluster_vis_resolve_in_flight(void)
{
	return cluster_vis_resolve_depth > 0;
}

void
cluster_vis_resolve_abort_reset(void)
{
	cluster_vis_resolve_depth = 0;
	vis_snapshot_bound.valid = false;
}


/*
 * Fill `out` from an authoritative remote exact ref.  Performs the TT
 * overlay lookup + SUBCOMMITTED parent follow, leaving a terminal-or-
 * in-progress status.  Lookup miss / non-authoritative -> UNKNOWN (the
 * caller fail-closes; the evidence stays REMOTE so there is no PG-native
 * fallback, C-V2).
 */
/*
 * resolve_live_overlay_miss_via_origin -- spec-7.1a D4 (live overlay pull).
 *
 *	A LIVE remote ITL ref (slot still bound to raw_xid) whose overlay lookup
 *	missed used to stay UNKNOWN forever when the origin's tt_status_hint
 *	propagation was lost (HC181: fail-closed but never self-healing; gaps
 *	§C.3).  Pull the origin's complete own-TT verdict on demand instead --
 *	the same shipped live-IC verdict machinery the recycled-slot path uses
 *	(no shared-undo data plane involved; the origin answers only for its own
 *	xids and only with terminal outcomes).  A genuinely in-progress holder
 *	still resolves UNKNOWN here (no verdict is served) and the caller keeps
 *	the fail-closed retry, which converges once the holder terminates.
 *
 *	Fills *out and installs the exact-key memo only for exact (non-bound)
 *	terminal outcomes; a below-horizon bound is snapshot-relative and is
 *	returned with commit_scn_is_bound so the consumer never stamps it.
 */
static void
resolve_live_overlay_miss_via_origin(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
									 SCN read_scn, const ClusterTTStatusKey *key,
									 ClusterVisResolve *out)
{
	bool committed = false;
	SCN scn = InvalidScn;
	bool is_bound = false;

	if (!cluster_crossnode_write_write)
		return; /* keep the pre-7.1a UNKNOWN floor byte-identical */
	if ((int32)ref->origin_node_id == cluster_node_id)
		return;

	cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_ASK);
	if (!cluster_runtime_visibility_try_resolve_remote(
			(int)ref->origin_node_id, (uint32)ref->undo_segment_id, raw_xid, read_scn,
			false /* keep the serve-side stripe self-check (pre-D6-7 behavior) */, &committed, &scn,
			&is_bound))
		return; /* stay UNKNOWN -> caller 53R97 fail-closed */

	cluster_vis_bump_overlay_refresh_count(); /* spec-7.1a D6 */
	cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_TERMINAL);
	if (committed) {
		out->status = CLUSTER_TT_STATUS_COMMITTED;
		out->commit_scn = scn;
		out->commit_scn_is_bound = is_bound;
		if (!is_bound)
			cluster_vis_memo_install(key, (uint8)CLUSTER_TT_STATUS_COMMITTED, scn);
	} else {
		out->status = CLUSTER_TT_STATUS_ABORTED;
		out->commit_scn = InvalidScn;
		cluster_vis_memo_install(key, (uint8)CLUSTER_TT_STATUS_ABORTED, InvalidScn);
	}
}

static void
resolve_from_remote_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, SCN read_scn,
						ClusterVisResolve *out)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterTTStatusSourceRequest source_request;
	ClusterTTStatusSourceResult source_result;
	bool defer_slotless_origin_pull;
	ClusterXpScope xp_scope = { .active = false }; /* PGRAC: spec-5.59 D3 profiling */

	cluster_xp_begin(&xp_scope, CLXP_R_TT_VISIBILITY_RESOLVE);

	if (out == NULL || ref == NULL) {
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;
	}

	out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	/*
	 * ADG physical standby replay is not the generic cross-node raw-xid case:
	 * pg_xact and the heap page both come from the same primary WAL stream.
	 * If the replayed ITL slot already carries a committed status plus a real
	 * commit_scn, CLOG-confirm it and use that exact page evidence.  This keeps
	 * prepared/in-progress xids fail-closed and leaves ordinary remote-origin
	 * reads on the overlay/durable-TT path below.
	 *
	 * spec-6.4 F2: the slot must still be bound to this tuple's xid.  A
	 * recycled ITL slot carries some OTHER transaction's commit_scn, and
	 * returning it here would hand the caller a false SCN for raw_xid.
	 */
	if (cluster_enable_adg && cluster_dg_role == CLUSTER_DG_ROLE_STANDBY && RecoveryInProgress()
		&& ref->local_xid == raw_xid && ref->has_cached_status && SCN_VALID(ref->cached_commit_scn)
		&& TransactionIdDidCommit(raw_xid)) {
		out->status = CLUSTER_TT_STATUS_COMMITTED;
		out->commit_scn = ref->cached_commit_scn;
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;
	}

	memset(&key, 0, sizeof(key));
	key.origin_node_id = ref->origin_node_id;
	key.undo_segment_id = ref->undo_segment_id;
	key.tt_slot_id = ref->tt_slot_id;
	key.cluster_epoch = ref->cluster_epoch;
	key.local_xid = raw_xid;

	/*
	 * PGRAC: spec-6.12c -- replay a TERMINAL outcome this same top-level
	 * transaction already obtained from an authoritative lookup under the
	 * exact key.  Terminal outcomes are immutable, so a hit answers exactly
	 * what the lookup below would answer; anything non-terminal was never
	 * installed and re-resolves.  GUC off -> probe is a no-op.
	 */
	cluster_lever_c_note_resolve();
	{
		uint8 memo_status;
		SCN memo_scn;

		if (cluster_vis_memo_probe(&key, &memo_status, &memo_scn)) {
			out->status = memo_status;
			out->commit_scn = memo_scn;
			cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
			return;
		}
	}

	/*
	 * The legacy overlay-miss pull carries no exact slot identity and may only
	 * return a snapshot-relative bound.  A pair-eligible fresh ref has a
	 * stronger frozen route: ordinary authoritative exact-slot first, then C1b
	 * only if that result is unproven.  Do not let the slotless pull shadow that
	 * route; eligibility is rechecked immediately before the exact request.
	 */
	defer_slotless_origin_pull = cluster_vis_freshref_c1b_pair_request_eligible(
		raw_xid, ref->local_xid, ref->has_cached_status, ref->cached_commit_scn, ref->cluster_epoch,
		cluster_epoch_get_current(), (int32)ref->origin_node_id, cluster_node_id,
		(uint32)ref->undo_segment_id, (uint32)ref->tt_slot_id);

	memset(&source_request, 0, sizeof(source_request));
	source_request.key = &key;
	if (cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &source_request, &source_result)
			!= CLUSTER_SEMANTIC_ADMISSION_OK
		|| !source_result.bool_value || !source_result.lookup.authoritative) {
		/* PGRAC: spec-6.12c D0 -- lookup performed; no terminal verdict. */
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_OVERLAY_MISS);
		cluster_lever_c_note_tt_lookup(ref->has_cached_status, false);
		/* PGRAC: spec-7.1a D4 -- overlay miss on a LIVE remote ref: pull the
		 * origin verdict instead of staying UNKNOWN forever (HC181). */
		if (!defer_slotless_origin_pull)
			resolve_live_overlay_miss_via_origin(raw_xid, ref, read_scn, &key, out);
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;					   /* UNKNOWN -> caller 53R97 (C-V2: no PG-native fallback) */
	}
	result = source_result.lookup;

	/*
	 * A nonterminal peer overlay is a propagation hint, not a current TT
	 * observation.  Even a cached SUBCOMMITTED parent chain can be stale.
	 * Leave UNKNOWN so classify_ref_guts uses the existing exact origin
	 * verdict (including authoritative live status) and DATA-to-TT fallback.
	 * Do not memoize this provisional hit or turn it into MVCC polarity.
	 */
	if (result.status == CLUSTER_TT_STATUS_IN_PROGRESS
		|| result.status == CLUSTER_TT_STATUS_SUBCOMMITTED) {
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_OVERLAY_LIVE);
		cluster_lever_c_note_tt_lookup(ref->has_cached_status, false);
		cluster_xp_end(&xp_scope);
		return;
	}

	if (!result.authoritative) {
		out->status = CLUSTER_TT_STATUS_UNKNOWN;
		/* PGRAC: spec-6.12c D0 -- lookup performed; no terminal verdict. */
		cluster_lever_c_note_tt_lookup(ref->has_cached_status, false);
		/* PGRAC: spec-7.1a D4 -- subtrans-chain miss: same origin pull. */
		if (!defer_slotless_origin_pull)
			resolve_live_overlay_miss_via_origin(raw_xid, ref, read_scn, &key, out);
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;
	}

	out->status = result.status;
	out->commit_scn = result.commit_scn;
	if (result.status == CLUSTER_TT_STATUS_ABORTED
		|| ((result.status == CLUSTER_TT_STATUS_COMMITTED
			 || result.status == CLUSTER_TT_STATUS_CLEANED_OUT)
			&& SCN_VALID(result.commit_scn)))
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_OVERLAY_TERMINAL);

	/*
	 * PGRAC: spec-6.12c -- D0 stamp-evidence classification + memo install.
	 * stamp_contradicted counts an ITL cached-COMMITTED stamp that the TT
	 * terminal verdict disagrees with (ABORTED, or a different commit SCN
	 * identity): direct evidence that trusting page stamps alone for
	 * committed-ness would be unsound (C1b -- committed-ness authority
	 * stays with CLOG/TT; the stamp only caches the SCN value).
	 */
	cluster_lever_c_note_tt_lookup(ref->has_cached_status,
								   ref->has_cached_status
									   && (result.status == CLUSTER_TT_STATUS_ABORTED
										   || (result.status == CLUSTER_TT_STATUS_COMMITTED
											   && ref->cached_commit_scn != result.commit_scn)));
	cluster_vis_memo_install(&key, (uint8)result.status, result.commit_scn);

	/* PGRAC: spec-5.59 D3 profiling */
	cluster_xp_end(&xp_scope);
}

static void
cluster_vis_log_freshref_unproven(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
								  bool freshref_pair, ClusterUndoVerdictResult v,
								  const ClusterTxLocator *exact_locator,
								  ClusterTxOutcome exact_outcome,
								  ClusterTxResolveReason exact_reason)
{
	if (vis_freshref_first_unproven_logged || ref == NULL)
		return;
	vis_freshref_first_unproven_logged = true;
	elog(LOG,
		 "PGRAC fresh-ref first unproven: xid=%u origin=%u segment=%u slot=%u "
		 "ref_epoch=%u cached=%d cached_scn=" UINT64_FORMAT
		 " freshref_pair=%d undo_verdict=%d exact_locator=%d "
		 "exact_outcome=%d exact_reason=%s",
		 raw_xid, (unsigned)ref->origin_node_id, (unsigned)ref->undo_segment_id,
		 (unsigned)ref->tt_slot_id, ref->cluster_epoch, ref->has_cached_status,
		 (uint64)ref->cached_commit_scn, freshref_pair, (int)v.kind, exact_locator != NULL,
		 (int)exact_outcome, cluster_tx_resolve_reason_name(exact_reason));
}


/*
 * Classify a freshly-read ITL ref into LOCAL / REMOTE / STALE and, when
 * REMOTE, resolve its status.  spec-3.14 R10 exact-key discipline:
 *	  tt_slot_id == 0           -> placeholder (spec-3.1) -> NONE-equiv:
 *	                               treated as no evidence by caller.
 *	  origin == self            -> LOCAL (PG CLOG resolves), even when
 *	                               local_xid no longer matches because a
 *	                               local hot-page slot was recycled.
 *	  origin != self &&
 *	  local_xid != raw_xid      -> remote slot recycled to another owner ->
 *	                               STALE_OR_AMBIGUOUS (caller 53R97).
 *	  origin != self            -> REMOTE (overlay resolve).
 */
static void
classify_ref_guts(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, XLogRecPtr anchor_lsn,
				  SCN read_scn, const ClusterTxLocator *exact_locator, ClusterVisResolve *out)
{
	if (out == NULL || ref == NULL)
		return;

	out->ref = *ref;

	if (ref->tt_slot_id == 0) {
		/* spec-3.1 placeholder slot: not authoritative evidence. */
		out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
		return;
	}

	/*
	 * On an ADG standby the replayed page's ITL slot can carry the commit
	 * evidence before the overlay / durable-TT paths can resolve the xid.
	 * The page itself is the authority, but only for an exact, terminal ITL
	 * binding: the slot must still belong to this tuple-side xid and must
	 * carry a committed cached SCN.  ACTIVE, ABORTED, invalid-SCN, and
	 * recycled slots continue through the ordinary local/remote fail-closed
	 * paths below.
	 *
	 * spec-6.4 F3: additionally cross-check the local CLOG (replayed from
	 * the same WAL stream) instead of trusting page provenance alone.
	 * Reads only run on the Apply Master, whose pg_xact is current through
	 * its read point, so a committed xid confirms here; anything else falls
	 * through to the fail-closed paths.
	 */
	if (cluster_enable_adg && cluster_dg_role == CLUSTER_DG_ROLE_STANDBY && RecoveryInProgress()
		&& ref->local_xid == raw_xid && ref->has_cached_status && SCN_VALID(ref->cached_commit_scn)
		&& TransactionIdDidCommit(raw_xid)) {
		out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
		out->status = CLUSTER_TT_STATUS_COMMITTED;
		out->commit_scn = ref->cached_commit_scn;
		return;
	}

	if ((int32)ref->origin_node_id == cluster_node_id) {
		/*
		 * Own-instance evidence is deliberately routed to PG-native CLOG.
		 * ITL data slots are only an 8-slot page cache and are normally
		 * recycled on local hot pages; treating a local_xid mismatch here as
		 * remote-unknown would make ordinary local UPDATE/DELETE/SELECT fail
		 * closed.  Remote safety still depends on an explicit remote-origin
		 * ref, which is checked below.
		 *
		 * PGRAC: spec-6.15 D7 — that single-writer-era shortcut is only sound
		 * for the xids the value space cannot prove foreign.  On a
		 * bidirectionally-written page OUR xact recycles a slot whose previous
		 * occupant was the PEER's xid: the slot's current owner proves nothing
		 * about raw_xid's origin, and routing a provably-foreign xid to
		 * PG-native CLOG is the same trust-the-ref false-resolve D4 closed in
		 * the remote direction (false-abort of a committed foreign deleter /
		 * false-invisible of a committed foreign inserter).  Derive from the
		 * value instead and fall through to the remote machinery; below the
		 * floor / striping off keeps the pre-striping LOCAL routing.
		 */
		if (ref->local_xid == raw_xid || !cluster_xid_provably_foreign(raw_xid)) {
			out->evidence = CLUSTER_VIS_EVIDENCE_LOCAL;
			return;
		}
		/* Stamp the DERIVED peer (INV-TP1/TP2): the ref names ourselves. */
		cluster_touched_peers_stamp(cluster_xid_origin_slot(raw_xid), CLUSTER_TOUCH_VISIBILITY);
	} else {
		/*
		 * spec-5.14 D2 class 4: past the self-check this is a genuine
		 * remote-origin ITL ref — the visibility verdict (whether via
		 * resolve_from_remote_ref or the STALE wrap-checked remote authority
		 * below) now depends on that peer's volatile TT / undo state.  Stamp
		 * conservatively so a fail-stop of the origin aborts this transaction
		 * (INV-TP1/TP2).  Read-only; resolve logic unchanged.
		 */
		cluster_touched_peers_stamp((int32)ref->origin_node_id, CLUSTER_TOUCH_VISIBILITY);
	}

	/*
	 * PGRAC (GCS-race round-2 RC-E): native-prehistory gate.  An ITL ref,
	 * whether still bound or recycled, does not create TT authority for a
	 * native seed xid.  When raw_xid is provably NATIVE-ERA (below the sealed
	 * native high-water, no wraparound yet, coverage verified this boot), the
	 * local adopted CLOG is alias-free authority for it: the native era predates
	 * every cluster-era allocation (stripe floor >= native_hw), and the
	 * post-recovery verify proved this node's pg_xact byte-matches the seed's
	 * sealed truth.  Every doubt leg (latch unset, wrap recurrence, value >= hw,
	 * including the whole [native_hw, stripe floor) gap) falls through to the
	 * existing fail-closed machinery (53R97).
	 *
	 * PGRAC (GCS-race round-3b P0-2): consume the adopted CLOG INLINE instead of
	 * returning LOCAL evidence.  LOCAL defers the verdict to a PG-native CLOG
	 * read the caller performs OUTSIDE this gate: between the provable check and
	 * that later read, the xid wrap barrier can complete and the first epoch-1
	 * xid can REUSE this raw value -- a false verdict with no fail-closed net.
	 *
	 * Drain proof (round-3b review P0): the epoch-1 reuser is allocated on the
	 * WRAPPING PEER and writes only that peer's pg_xact, so there is NO local
	 * CLOG write for a barrier-based recheck to synchronize with; relaxed
	 * atomics cannot prove this backend observes the latch drop in time.  Instead
	 * the whole consume window (provable check -> CLOG read -> verdict fill) runs
	 * under the native_prehistory_lock held SHARED, and the wrap-barrier DISABLE
	 * clears the latch under EXCLUSIVE and only ACKs after the release
	 * (cluster_cr.c).  A reader that saw the latch up therefore finished its
	 * verdict BEFORE the ACK left this node -- i.e. before the first epoch-1 xid
	 * could exist anywhere -- and a reader arriving after the release pairs with
	 * it and fails closed.  The unlocked covered_hw pre-filter only skips the
	 * lock when the latch is provably down/never up; staleness in either direction
	 * is safe (spurious lock = re-checked under the lock; spurious skip =
	 * fail-closed).
	 *
	 * Truncation gate (round-3b review P1): the boot verify covers [oldestXid,
	 * native_hw) as of the seal, but VACUUM may later advance oldestClogXid past
	 * still-referenced native xids and truncate their pg_xact segments; a raw
	 * TransactionIdGetStatus would then surface an SLRU could-not-access ERROR.
	 * Mirror the upstream pg_xact_status pattern (xid8funcs.c): lookups of
	 * arbitrary xids hold XactTruncationLock SHARED from the oldestClogXid test
	 * through lookup completion, and the advance side (AdvanceOldestClogXid,
	 * varsup.c) takes it EXCLUSIVE -- the advance drains in-flight lookups, later
	 * lookups see the new floor and refuse, and only then is the physical truncate
	 * safe (the truncate itself runs unlocked by design).  A below-oldestClogXid
	 * xid falls through to the ordinary fail-closed leg (53R97).
	 *
	 * COMMITTED surfaces as a REMOTE terminal verdict with commit_scn = (SCN) 1:
	 * the native era predates every cluster snapshot and all real SCNs are >= 1,
	 * so scn_time_cmp((SCN) 1, read_scn) <= 0 decides VISIBLE for every valid
	 * read_scn; commit_scn_is_bound forbids stamping the fabricated value into an
	 * ITL slot.  ABORTED and IN_PROGRESS (crash-aborted under the seal proof) map
	 * ABORTED; SUB_COMMITTED and anything outside the CLOG alphabet map UNRESOLVED
	 * -> fail-closed (round-3b RISK-1 -- the boot verify's SUB_COMMITTED refusal
	 * is never trusted at runtime).  C-V1's no-CLOG-on-remote rule targets
	 * cross-instance raw-xid aliasing; below the sealed native high-water the
	 * adopted CLOG is alias-free by construction.
	 */
	if (cluster_cr_native_prehistory_covered_hw() != 0) {
		bool prehistory_resolved = false;

		cluster_cr_native_prehistory_reader_lock();
		if (cluster_xid_native_prehistory_provable_full(
				U64FromFullTransactionId(ReadNextFullTransactionId()),
				cluster_cr_native_prehistory_covered_hw(), raw_xid)) {
			XLogRecPtr clog_lsn = InvalidXLogRecPtr;

			LWLockAcquire(XactTruncationLock, LW_SHARED);
			if (!TransactionIdPrecedes(raw_xid, ShmemVariableCache->oldestClogXid)) {
				XidStatus native_status = TransactionIdGetStatus(raw_xid, &clog_lsn);

				/*
				 * Round-3b RISK-1: explicit status mapping.  The boot
				 * verify refused to latch on any SUB_COMMITTED byte,
				 * but that claim is never trusted at runtime:
				 * SUB_COMMITTED and out-of-alphabet values map
				 * UNRESOLVED and fall through fail-closed (53R97)
				 * instead of being folded into ABORTED.
				 */
				switch (cluster_native_prehistory_map_status((int)native_status)) {
				case CLUSTER_NATIVE_PREHISTORY_COMMITTED:
					cluster_rtvis_note_native_prehistory_local();
					out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
					out->status = CLUSTER_TT_STATUS_COMMITTED;
					out->commit_scn = (SCN)1;
					out->commit_scn_is_bound = true;
					prehistory_resolved = true;
					break;
				case CLUSTER_NATIVE_PREHISTORY_ABORTED:
					/* literal ABORTED, or IN_PROGRESS = crash-aborted
					 * under the seal proof */
					cluster_rtvis_note_native_prehistory_local();
					out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
					out->status = CLUSTER_TT_STATUS_ABORTED;
					out->commit_scn = InvalidScn;
					out->commit_scn_is_bound = false;
					prehistory_resolved = true;
					break;
				case CLUSTER_NATIVE_PREHISTORY_UNRESOLVED:
					break; /* fail closed below */
				}
			}
			LWLockRelease(XactTruncationLock);
		}
		cluster_cr_native_prehistory_reader_unlock();

		if (prehistory_resolved)
			return;
		/* Latch down under the lock (wrap barrier fired), not provable,
		 * or CLOG truncated below raw_xid: no trustworthy native bytes
		 * -- fail closed below. */
	}

	if (ref->local_xid != raw_xid) {
		/*
		 * The available evidence says REMOTE, but the slot no longer belongs
		 * to this tuple-side xid.  Do NOT fall through to PG-native local CLOG;
		 * that is the false-resolve this resolver exists to prevent.
		 *
		 * spec-4.5a G6 (P1 #1/#3): a foreign STALE ref (the peer reused this
		 * heap slot for a later xid before crashing -- normal within an undo
		 * chain) is resolved by the WRAP-CHECKED by-xid authority, NOT a bare
		 * (origin,xid) lookup.  cluster_remote_outcome_durable_checked scans
		 * the origin's durable TT slots for raw_xid with the outcome's wrap;
		 * exactly one wrap-qualified match is the proof, so a same-valued
		 * wrapped xid (different generation) cannot alias.  INDOUBT (no proof)
		 * stays fail-closed (53R97) -- never a bare (origin,xid) guess.
		 *
		 * Gate on the materialized marker FIRST (mirroring the tt_status /
		 * CR-tier-3 consumers): the marker proves this origin's merge COMPLETED,
		 * so its on-disk outcome store + durable TT cover the whole window.  A
		 * partial-merge residual (FATAL mid-replay, then cluster.merged_recovery
		 * =off) leaves a store/durable WITHOUT a marker; reading it would surface
		 * pre-FATAL xids as COMMITTED while post-FATAL committed changes never
		 * materialized -> torn-history false-visible.  No marker -> fail closed.
		 */
		/* PGRAC: spec-6.15 D7 — the own-owner recycled fall-through reaches
		 * here with ref->origin == self; the materialized (crash-recovery)
		 * authority is a PEER-origin face only, so gate it out — the armed
		 * active-runtime branch below derives the true origin itself. */
		if ((int32)ref->origin_node_id != cluster_node_id
			&& vis_origin_materialized((int)ref->origin_node_id)) {
			SCN scn;

			/*
			 * spec-4.8 D2: tighten the coarse bool is_materialized gate to an
			 * LSN gate (4.7 D5 / Q5 lesson -- "materialized" alone is too weak).
			 * is_materialized only proves the origin's merge published a marker;
			 * a materialized-but-under-recovered origin may yield a STALE TT
			 * outcome for a page version its redo has not yet reconciled.  If
			 * this tuple's page LSN is beyond the origin's recovered_through
			 * (the lost-write / under-recovery window, spec-2.37 / 4.7 D5), the
			 * durable outcome -- COMMITTED *or* ABORTED -- is untrustworthy:
			 * trusting it risks a false-visible (stale COMMITTED) or a
			 * false-invisible (a commit record the origin has not yet replayed
			 * read as a 0-match ABORTED).  Fail closed (规则 8.A), never resolve.
			 * anchor_lsn == 0 (unwritten page) skips the gate -> pre-D2
			 * is_materialized-only behaviour (no regression).
			 */
			if (!cluster_tt_recovery_remote_authority_covers(
					cluster_merged_instance_recovered_through((int)ref->origin_node_id),
					(uint64)anchor_lsn)) {
				out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
				cluster_tt_recovery_count_remote_active_failclosed();
				return;
			}

			switch (cluster_cf_terminal_authority
						? cluster_remote_outcome_terminal_authorized(
							  (int)ref->origin_node_id, raw_xid, ref->cluster_epoch,
							  cluster_epoch_get_current(), false, true, &scn)
						: cluster_remote_outcome_durable_checked((int)ref->origin_node_id, raw_xid,
																 &scn)) {
			case CLUSTER_REMOTE_XACT_COMMITTED:
				out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
				out->status = CLUSTER_TT_STATUS_COMMITTED;
				out->commit_scn = scn;
				return;
			case CLUSTER_REMOTE_XACT_ABORTED:
				out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
				out->status = CLUSTER_TT_STATUS_ABORTED;
				out->commit_scn = InvalidScn;
				return;
			case CLUSTER_REMOTE_XACT_INDOUBT:
			default:
				break; /* unprovable -> STALE fail-closed below */
			}
		} else if (cluster_crossnode_runtime_visibility) {
			/*
			 * PGRAC: spec-6.12i CP3/CP5 (D-i2/D-i4 wiring) — ACTIVE-runtime
			 * resolution.
			 *
			 * The origin is NOT materialized (it is live — the materialized
			 * marker is a crash-recovery artifact), so the recovery-side
			 * authority above is structurally unavailable and this branch
			 * used to be unconditionally fail-closed (53R97; the observed
			 * cross-node concurrent-write collapse).
			 *
			 * PGRAC: spec-6.15 D4 — the origin to ask is derived from the
			 * XID ITSELF (cluster_xid_origin_slot: stripe congruence above
			 * the activation floor), NEVER from ref->origin_node_id: the
			 * ref names the slot's CURRENT owner, and after a recycle that
			 * is unrelated to the tuple-side xid — trusting it was the
			 * original 6.12i false-resolve P0 (overlapping per-node xid
			 * value spaces made "ask the ref's origin by xid" match another
			 * node's same-valued xid).  Striping makes the value space
			 * globally unique above the floor, so:
			 *   derived == self  -> the xid is OURS (a remote writer merely
			 *                       recycled our slot): route LOCAL — the
			 *                       PG-native CLOG path is alias-free for a
			 *                       provably-own xid;
			 *   derived == peer  -> ask THAT peer: D-i1 fetch + covers gate
			 *                       + positive proof, then the D-i4
			 *                       complete-scan origin verdict on a
			 *                       0-match (read_scn decides below-horizon
			 *                       admissibility, leg (e));
			 *   underivable (-1) -> striping off / below the floor: keep
			 *                       the unchanged 53R97 (never guess).
			 * Every unproven outcome falls through to STALE_OR_AMBIGUOUS ->
			 * 53R97 (Rule 8.A: this wave only widens "resolve when
			 * provable").
			 */
			int derived_origin = cluster_xid_origin_slot(raw_xid);

			if (derived_origin == cluster_node_id) {
				out->evidence = CLUSTER_VIS_EVIDENCE_LOCAL;
				return;
			}
			if (derived_origin >= 0) {
				bool committed = false;
				SCN scn = InvalidScn;
				bool is_bound = false;

				cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_ASK);
				if (cluster_runtime_visibility_try_resolve_remote(
						derived_origin, (uint32)ref->undo_segment_id, raw_xid, read_scn,
						false /* derived origin: keep the stripe self-check (6.12i P0) */,
						&committed, &scn, &is_bound)) {
					out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
					out->status
						= committed ? CLUSTER_TT_STATUS_COMMITTED : CLUSTER_TT_STATUS_ABORTED;
					out->commit_scn = committed ? scn : InvalidScn;
					out->commit_scn_is_bound = committed ? is_bound : false;
					cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_TERMINAL);
					return;
				}
			} else {
				cluster_rtvis_note_underivable_failclosed();
			}
			cluster_tt_recovery_count_remote_active_failclosed();
		}
		out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
		cluster_vis_bump_vis_variant_unknown_failclosed_count();
		return;
	}

	resolve_from_remote_ref(raw_xid, ref, read_scn, out);

	/*
	 * PGRAC: spec-5.22f D6-2 — fresh-remote-ITL-ref widening (root-cause #1
	 * seed/joiner visibility consumer).
	 *
	 * A fresh remote ITL ref reaches here (origin != self, local_xid ==
	 * raw_xid: the slot still binds raw_xid, so ref->origin_node_id +
	 * undo_segment_id are the tuple page's PHYSICAL ITL binding = the xid's
	 * true owner + CP3 block0 locator).  A fresh joiner reading a seed-committed
	 * tuple has an EMPTY TT overlay, so resolve_from_remote_ref left
	 * {REMOTE, UNKNOWN} on the miss -> 53R97 -> false-invisible (命门 2).  Ask
	 * the live owner via the D3 cross-node verdict instead of fail-closing.
	 *
	 * Unlike the recycled branch (:436), which MUST derive the origin from the
	 * xid value (a recycled ref names the slot's NEW owner, unrelated to
	 * raw_xid — the 6.12i alias P0), the fresh ref's ref->origin_node_id is
	 * authoritative.  cluster_xid_origin_slot is only a derivable-time integrity
	 * cross-check (Q2 / Option B, P1-a): underivable (below-floor pre-striping
	 * seed / striping off) STILL asks the verdict — Rule 8.A safety is anchored
	 * INSIDE D3 (wrap-suspect anti-ABA / covers / serve gates fail-close every
	 * unproven leg), never on a "no alias" assumption.  crossnode off / a
	 * derivable mismatch / an UNKNOWN verdict all keep STALE_OR_AMBIGUOUS ->
	 * 53R97 (Rule 8.A: this branch only widens "resolve when provable").
	 */
	if (out->status == CLUSTER_TT_STATUS_UNKNOWN && cluster_crossnode_runtime_visibility
		&& (int32)ref->origin_node_id != cluster_node_id) {
		int derived = cluster_xid_origin_slot(raw_xid); /* derivable-time check only */

		switch (cluster_vis_freshref_origin_decision(derived, (int32)ref->origin_node_id)) {
		case CLUSTER_VIS_FRESHREF_ORIGIN_STALE:
			/* derivable mismatch: striping bug / page corruption / alias.  No
			 * verdict is asked, so only the dedicated fresh-ref counter moves
			 * -- mirroring the recycled underivable leg (:458), which likewise
			 * does not touch the shared rtvis_resolve totals. */
			out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
			out->diagnostic_reason = "IDENTITY_STALE";
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_IDENTITY_STALE);
			cluster_vis_freshref_verdict_note_failclosed();
			break;

		case CLUSTER_VIS_FRESHREF_ORIGIN_ASK:
		default: {
			ClusterTxResolution exact_resolution;
			ClusterTxResolveReason exact_reason;
			ClusterTxOutcome exact_outcome;
			/* origin = ref->origin_node_id (physical binding, authoritative);
			 * every unproven leg inside D3 returns UNKNOWN_FAIL_CLOSED.
			 * cluster_undo_verdict_resolve -> try_resolve_remote already bumps
			 * the shared rtvis_resolve_{committed,aborted,failclosed} totals
			 * internally (cluster_runtime_visibility.c:424/427/444/446/449), so
			 * the dedicated fresh-ref counter is the ONLY extra bump here -- an
			 * explicit rtvis_resolve_note here would double-count. */
			bool freshref_pair = cluster_vis_freshref_c1b_pair_request_eligible(
				raw_xid, ref->local_xid, ref->has_cached_status, ref->cached_commit_scn,
				ref->cluster_epoch, cluster_epoch_get_current(), (int32)ref->origin_node_id,
				cluster_node_id, (uint32)ref->undo_segment_id, (uint32)ref->tt_slot_id);
			ClusterUndoVerdictResult v;

			if (freshref_pair && vis_snapshot_bound_probe(ref, read_scn, out)) {
				cluster_vis_freshref_verdict_note_resolved();
				return;
			}
			if (!freshref_pair)
				vis_snapshot_bound.valid = false;
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_ASK);
			v = freshref_pair ? cluster_undo_verdict_resolve_freshref_c1b_pair(
									(int)ref->origin_node_id, (uint32)ref->undo_segment_id, raw_xid,
									ref->local_xid, (uint32)ref->tt_slot_id, ref->cluster_epoch,
									ref->cached_commit_scn, read_scn)
							  : cluster_undo_verdict_resolve(
									(int)ref->origin_node_id, (uint32)ref->undo_segment_id, raw_xid,
									(uint32)ref->tt_slot_id, read_scn,
									true /* fresh ref: physical-binding authority */);

			if (cluster_vis_from_undo_verdict(v, out)) {
				cluster_vis_evidence_note(v.kind == CLUSTER_UNDO_VERDICT_IN_PROGRESS
											  ? CLUSTER_VIS_METRIC_ORIGIN_LIVE
											  : CLUSTER_VIS_METRIC_ORIGIN_TERMINAL);
				/* O2: an origin-proven exact terminal is immutable and may use
				 * the existing backend-local, lxid-bound EXACT memo.  A bound
				 * remains snapshot-relative and never enters that exact memo. */
				if ((v.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT && SCN_VALID(v.commit_scn))
					|| v.kind == CLUSTER_UNDO_VERDICT_ABORTED) {
					ClusterTTStatusKey memo_key;

					memset(&memo_key, 0, sizeof(memo_key));
					memo_key.origin_node_id = ref->origin_node_id;
					memo_key.undo_segment_id = ref->undo_segment_id;
					memo_key.tt_slot_id = ref->tt_slot_id;
					memo_key.cluster_epoch = ref->cluster_epoch;
					memo_key.local_xid = raw_xid;
					cluster_vis_memo_install(
						&memo_key,
						v.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
							? (uint8)CLUSTER_TT_STATUS_COMMITTED
							: (uint8)CLUSTER_TT_STATUS_ABORTED,
						v.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT ? v.commit_scn : InvalidScn);
				}
				if (freshref_pair && v.kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND)
					vis_snapshot_bound_install(ref, read_scn, v.commit_scn);
				cluster_vis_freshref_verdict_note_resolved();
				return;
			}

			/* A page UBA names the DATA record, not necessarily the canonical
			 * TT segment.  Only the page-derived full locator may enter the
			 * existing status-22 resolver, whose origin choreography freezes the
			 * DATA record, samples the same-owner canonical TT, then byte-exactly
			 * revalidates DATA while holding at most one 0xFB SCUR. */
			memset(&exact_resolution, 0, sizeof(exact_resolution));
			exact_reason = CLUSTER_TX_RESOLVE_PROTOCOL;
			if (exact_locator != NULL)
				cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_ASK);
			exact_outcome
				= exact_locator == NULL
					  ? CLUSTER_TX_UNKNOWN
					  : cluster_tx_resolve_exact(exact_locator, CLUSTER_TX_RESOLVE_VISIBILITY,
												 &exact_resolution, &exact_reason);
			if (cluster_vis_from_exact_tx_resolution(exact_outcome, &exact_resolution, out)) {
				cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ROUTE_BYPASS);
				cluster_vis_evidence_note(CLUSTER_VIS_METRIC_DURABLE_ROUTE_GAP);
				cluster_vis_evidence_note(exact_outcome == CLUSTER_TX_IN_PROGRESS
											  ? CLUSTER_VIS_METRIC_ORIGIN_LIVE
											  : CLUSTER_VIS_METRIC_ORIGIN_TERMINAL);
				cluster_vis_freshref_verdict_note_resolved();
				return;
			}
			/* Both reduced-key verdicts and the exact DATA->TT resolver are
			 * unproven.  The latter never returns BOUND or installs a memo. */
			cluster_vis_log_freshref_unproven(raw_xid, ref, freshref_pair, v, exact_locator,
											  exact_outcome, exact_reason);
			if (exact_locator != NULL) {
				if (exact_reason == CLUSTER_TX_RESOLVE_TIMEOUT) {
					out->diagnostic_reason = "AUTHORITY_DEADLINE_EXPIRED";
					cluster_vis_evidence_note(CLUSTER_VIS_METRIC_AUTHORITY_TIMEOUT);
				} else if (exact_reason == CLUSTER_TX_RESOLVE_RF_DEFERRED
						   || exact_reason == CLUSTER_TX_RESOLVE_AUTHORITY_STALE) {
					out->diagnostic_reason = "FORMATION_STALE";
					cluster_vis_evidence_note(CLUSTER_VIS_METRIC_FORMATION_STALE);
				} else if (exact_reason == CLUSTER_TX_RESOLVE_XID_MISMATCH
						   || exact_reason == CLUSTER_TX_RESOLVE_WRAP_MISMATCH
						   || exact_reason == CLUSTER_TX_RESOLVE_SLOT_MISMATCH) {
					out->diagnostic_reason = "IDENTITY_STALE";
					cluster_vis_evidence_note(CLUSTER_VIS_METRIC_IDENTITY_STALE);
				} else if (exact_reason == CLUSTER_TX_RESOLVE_BAD_LOCATOR
						   || exact_reason == CLUSTER_TX_RESOLVE_BAD_UBA
						   || exact_reason == CLUSTER_TX_RESOLVE_PROTOCOL) {
					out->diagnostic_reason = "MALFORMED";
					cluster_vis_evidence_note(CLUSTER_VIS_METRIC_MALFORMED);
				}
			}
			cluster_vis_freshref_verdict_note_failclosed();
			break;
		}
		}
	}
}

static bool
cluster_vis_exact_locators_for_ref(Page page, uint8 slot_index, ClusterVisXidKind which,
								   TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
								   ClusterTxLocator *visibility_locator_out,
								   ClusterTxLocator *row_wait_locator_out)
{
	ClusterTxLocator candidate;
	ClusterUndoTTSlotRef candidate_ref;
	ClusterTxResolveReason reason;

	if (visibility_locator_out != NULL)
		memset(visibility_locator_out, 0, sizeof(*visibility_locator_out));
	if (row_wait_locator_out != NULL)
		memset(row_wait_locator_out, 0, sizeof(*row_wait_locator_out));
	if (page == NULL || ref == NULL || visibility_locator_out == NULL
		|| row_wait_locator_out == NULL || !TransactionIdIsNormal(raw_xid))
		return false;

	/* The tuple or the canonical DATA/LOCK selector already chose this slot.
	 * A reduced TT ref loses the UBA record address and ITL kind: rescanning
	 * it can conflate a transaction's DATA and LOCK_ONLY records, or replace
	 * a recycled tuple slot with an unrelated surviving carrier. */
	if (!cluster_itl_get_tt_ref(page, slot_index, &candidate_ref)
		|| candidate_ref.local_xid != raw_xid || candidate_ref.local_xid != ref->local_xid
		|| candidate_ref.origin_node_id != ref->origin_node_id
		|| candidate_ref.undo_segment_id != ref->undo_segment_id
		|| candidate_ref.tt_slot_id != ref->tt_slot_id
		|| candidate_ref.cluster_epoch != ref->cluster_epoch
		|| !cluster_tx_locator_from_itl(page, slot_index, &candidate, &reason))
		return false;
	if ((which == CLUSTER_VIS_XMAX_LOCK_ONLY) != ITL_FLAG_IS_LOCK_ONLY(candidate.itl_kind))
		return false;

	*row_wait_locator_out = candidate;
	/* Visibility Candidate-2 derives the canonical wrap at the origin;
	 * ROW_WAIT instead retains the exact page witness. */
	candidate.tt_wrap = TT_WRAP_INVALID;
	*visibility_locator_out = candidate;
	return true;
}

/*
 * classify_ref -- spec-6.14 D8 wrapper: run the classification under the
 * catalog-safe no-recursion counter (see cluster_vis_resolve_depth above).
 */
static void
classify_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, XLogRecPtr anchor_lsn,
			 SCN read_scn, const ClusterTxLocator *exact_locator, ClusterVisResolve *out)
{
	cluster_vis_resolve_depth++;
	classify_ref_guts(raw_xid, ref, anchor_lsn, read_scn, exact_locator, out);
	cluster_vis_resolve_depth--;
	if (out != NULL && ref != NULL && ref->local_xid != raw_xid
		&& out->evidence != CLUSTER_VIS_EVIDENCE_LOCAL
		&& out->evidence != CLUSTER_VIS_EVIDENCE_NONE) {
		bool terminal = out->evidence == CLUSTER_VIS_EVIDENCE_REMOTE
						&& (out->status == CLUSTER_TT_STATUS_ABORTED
							|| ((out->status == CLUSTER_TT_STATUS_COMMITTED
								 || out->status == CLUSTER_TT_STATUS_CLEANED_OUT)
								&& SCN_VALID(out->commit_scn)));

		out->diagnostic_reason
			= terminal ? "RECYCLED_TERMINAL_PROVEN" : "RECYCLED_AUTHORITY_UNPROVABLE";
		cluster_vis_evidence_note(terminal ? CLUSTER_VIS_METRIC_RECYCLED_TERMINAL
										   : CLUSTER_VIS_METRIC_RECYCLED_UNPROVABLE);
	} else if (out != NULL && out->status == CLUSTER_TT_STATUS_UNKNOWN
			   && out->evidence != CLUSTER_VIS_EVIDENCE_LOCAL
			   && out->evidence != CLUSTER_VIS_EVIDENCE_NONE && out->diagnostic_reason == NULL) {
		out->diagnostic_reason = "AUTHORITY_UNAVAILABLE";
		cluster_vis_evidence_note(CLUSTER_VIS_METRIC_AUTHORITY_UNAVAILABLE);
	}
}

static void
classify_page_ref(Page page, uint8 slot_index, ClusterVisXidKind which, TransactionId raw_xid,
				  const ClusterUndoTTSlotRef *ref, XLogRecPtr anchor_lsn, SCN read_scn,
				  ClusterVisResolve *out)
{
	ClusterTxLocator locator;
	ClusterTxLocator row_wait_locator;
	const ClusterTxLocator *exact_locator = NULL;

	if (cluster_vis_exact_locators_for_ref(page, slot_index, which, raw_xid, ref, &locator,
										   &row_wait_locator))
		exact_locator = &locator;
	classify_ref(raw_xid, ref, anchor_lsn, read_scn, exact_locator, out);
	if (exact_locator != NULL && out->evidence == CLUSTER_VIS_EVIDENCE_REMOTE) {
		out->row_wait_locator = row_wait_locator;
		out->row_wait_locator_valid = true;
	}
}


void
cluster_visibility_resolve_from_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
									XLogRecPtr anchor_lsn, ClusterVisResolve *out)
{
	cluster_visibility_resolve_from_ref_scn(raw_xid, ref, anchor_lsn, InvalidScn, out);
}


void
cluster_visibility_resolve_from_ref_scn(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
										XLogRecPtr anchor_lsn, SCN read_scn, ClusterVisResolve *out)
{
	if (out == NULL)
		return;

	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	classify_ref(raw_xid, ref, anchor_lsn, read_scn, NULL, out);
}

void
cluster_visibility_resolve_scratch_scn(Page page, uint8 slot_index, TransactionId raw_xid,
									   SCN read_scn, ClusterVisResolve *out)
{
	ClusterUndoTTSlotRef ref;
	ClusterTxLocator locator;
	ClusterTxLocator unused_row_wait;
	const ClusterItlSlotData *slot;
	ClusterVisXidKind locator_kind;

	if (out == NULL)
		return;
	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->diagnostic_reason = "MALFORMED";
	if (page == NULL || !SCN_VALID(read_scn) || !TransactionIdIsNormal(raw_xid) || !PageHasItl(page)
		|| PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE
		|| slot_index >= CLUSTER_ITL_INITRANS_DEFAULT)
		return;
	slot = &ClusterPageGetItlSlots(page)[slot_index];
	if (slot->flags < ITL_FLAG_ACTIVE || slot->flags > ITL_FLAG_LOCK_ONLY_ABORTED
		|| !TransactionIdIsNormal(slot->xid) || UBA_is_invalid(slot->undo_segment_head)
		|| !cluster_itl_get_tt_ref(page, slot_index, &ref) || ref.local_xid != slot->xid
		|| ref.tt_slot_id == 0)
		return;
	/* A different occupant can only supply a historical route hint. Its
	 * ordinary LOCK role is structurally valid, but never DATA authority for
	 * the same xid; both cases still use the existing exact locator checks. */
	locator_kind = ref.local_xid != raw_xid && ITL_FLAG_IS_LOCK_ONLY(slot->flags)
					   ? CLUSTER_VIS_XMAX_LOCK_ONLY
					   : CLUSTER_VIS_XMIN;
	if (!cluster_vis_exact_locators_for_ref(page, slot_index, locator_kind, ref.local_xid, &ref,
											&locator, &unused_row_wait)
		|| locator.xid != ref.local_xid)
		return;

	if (ref.local_xid != raw_xid) {
		uint64 epoch = cluster_epoch_get_current();
		int origin = cluster_xid_origin_slot(raw_xid);
		ClusterUndoVerdictResult historical = {
			.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED,
			.commit_scn = InvalidScn,
		};

		/* A recycled last-writer ref is not this transaction's identity.
		 * Derive the original origin, keep its stripe self-check, and ask
		 * only for a terminal outcome. Never create a live physical binding
		 * or route our own scratch xid into native tuple visibility. */
		out->ref = ref;
		out->diagnostic_reason = "RECYCLED_AUTHORITY_UNPROVABLE";
		if (origin >= 0 && origin < CLUSTER_MAX_NODES && epoch <= UINT32_MAX
			&& ref.cluster_epoch == (uint32)epoch) {
			cluster_vis_resolve_depth++;
			PG_TRY();
			{
				if (origin != cluster_node_id)
					cluster_touched_peers_stamp(origin, CLUSTER_TOUCH_VISIBILITY);
				cluster_vis_evidence_note(CLUSTER_VIS_METRIC_ORIGIN_ASK);
				historical = cluster_undo_verdict_resolve(origin, ref.undo_segment_id, raw_xid, 0,
														  read_scn, false);
				if (cluster_epoch_get_current() == epoch
					&& (historical.kind == CLUSTER_UNDO_VERDICT_ABORTED
						|| (historical.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
							&& SCN_VALID(historical.commit_scn))
						|| (historical.kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND
							&& SCN_VALID(historical.commit_scn)
							&& scn_time_cmp(historical.commit_scn, read_scn) <= 0)))
					(void)cluster_vis_from_undo_verdict(historical, out);
			}
			PG_FINALLY();
			{
				cluster_vis_resolve_depth--;
			}
			PG_END_TRY();
		}
		if (out->evidence == CLUSTER_VIS_EVIDENCE_REMOTE) {
			out->diagnostic_reason = "RECYCLED_TERMINAL_PROVEN";
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_RECYCLED_TERMINAL);
		} else {
			cluster_vis_evidence_note(CLUSTER_VIS_METRIC_RECYCLED_UNPROVABLE);
			if (!vis_scratch_history_first_unproven_logged) {
				vis_scratch_history_first_unproven_logged = true;
				elog(LOG,
					 "PGRAC scratch history first unproven: xid=%u writer_xid=%u "
					 "derived_origin=%d segment=%u ref_epoch=%u epoch=" UINT64_FORMAT
					 " verdict=%d read_scn=" UINT64_FORMAT,
					 raw_xid, ref.local_xid, origin, (unsigned)ref.undo_segment_id,
					 ref.cluster_epoch, epoch, (int)historical.kind, (uint64)read_scn);
			}
		}
		return;
	}

	/* Keep the physical DATA address, not just the reduced TT ref. The
	 * existing classifier can then reach its DATA-to-canonical-TT fallback. */
	out->diagnostic_reason = NULL;
	classify_ref(raw_xid, &ref, PageGetLSN(page), read_scn, &locator, out);
	if (out->evidence == CLUSTER_VIS_EVIDENCE_LOCAL) {
		/* LOCAL normally delegates to native tuple visibility. That is not
		 * a valid fallback for a foreign-produced immutable scratch image.
		 * The same exact origin service also handles our own DATA records. */
		cluster_vis_resolve_depth++;
		PG_TRY();
		{
			ClusterTxResolution resolution;
			ClusterTxResolveReason reason;
			ClusterTxOutcome outcome;
			ClusterUndoVerdictResult retained = {
				.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED,
				.commit_scn = InvalidScn,
			};
			bool pair = ref.has_cached_status && SCN_VALID(ref.cached_commit_scn);
			bool completed = false;

			/* The DATA record may outlive its canonical TT occupant. Use
			 * the same origin C1b conjunction as the remote retained-ref
			 * path, not the page stamp alone or native tuple visibility. */
			if (pair) {
				retained = cluster_undo_verdict_resolve_freshref_c1b_pair(
					(int)ref.origin_node_id, ref.undo_segment_id, raw_xid, ref.local_xid,
					ref.tt_slot_id, ref.cluster_epoch, ref.cached_commit_scn, read_scn);
				if (retained.kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
					&& retained.commit_scn == ref.cached_commit_scn
					&& cluster_epoch_get_current() == (uint64)ref.cluster_epoch)
					completed = cluster_vis_from_undo_verdict(retained, out);
				else if (retained.kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND
						 && SCN_VALID(retained.commit_scn)
						 && scn_time_cmp(retained.commit_scn, read_scn) <= 0
						 && cluster_epoch_get_current() == (uint64)ref.cluster_epoch)
					/* Read-only proof: preserve the bound marker, never install
					 * it in an exact memo or stamp the immutable scratch page. */
					completed = cluster_vis_from_undo_verdict(retained, out);
				else if (retained.kind != CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED) {
					out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
					out->status = CLUSTER_TT_STATUS_UNKNOWN;
					out->commit_scn = InvalidScn;
					out->diagnostic_reason = "RETAINED_PROOF_UNPROVEN";
					cluster_vis_log_freshref_unproven(raw_xid, &ref, pair, retained, &locator,
													  CLUSTER_TX_UNKNOWN,
													  CLUSTER_TX_RESOLVE_PROTOCOL);
					completed = true;
				}
			}

			if (!completed) {
				memset(&resolution, 0, sizeof(resolution));
				reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				outcome = cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_VISIBILITY,
												   &resolution, &reason);
				if (!cluster_vis_from_exact_tx_resolution(outcome, &resolution, out)) {
					out->diagnostic_reason = cluster_tx_resolve_reason_name(reason);
					cluster_vis_log_freshref_unproven(raw_xid, &ref, pair, retained, &locator,
													  outcome, reason);
				}
			}
		}
		PG_FINALLY();
		{
			cluster_vis_resolve_depth--;
		}
		PG_END_TRY();
	}
}


void
cluster_visibility_resolve_tuple(Buffer buffer, HeapTupleHeader htup, TransactionId raw_xid,
								 ClusterVisXidKind which, ClusterVisResolve *out)
{
	cluster_visibility_resolve_tuple_scn(buffer, htup, raw_xid, which, InvalidScn, out);
}


void
cluster_visibility_resolve_tuple_scn(Buffer buffer, HeapTupleHeader htup, TransactionId raw_xid,
									 ClusterVisXidKind which, SCN read_scn, ClusterVisResolve *out)
{
	Page page;
	ClusterUndoTTSlotRef ref;
	XLogRecPtr anchor_lsn;
	uint8 slot_index;
	bool is_catalog_page = false;

	if (out == NULL)
		return;

	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	if (!BufferIsValid(buffer))
		return;
	if (htup == NULL)
		return;
	page = BufferGetPage(buffer);
	if (!PageHasItl(page))
		return;

	/*
	 * spec-6.14 D10b: catalog-page resolutions are the shared-catalog
	 * cross-node MVCC surface -- count them and advertise the (possibly
	 * remote-consulting) classification under a dedicated wait event.
	 * Only unhinted / foreign-evidence tuples reach the resolver, so the
	 * counter bump is off any hot path.
	 */
	if (cluster_shared_catalog) {
		RelFileLocator rlocator;
		ForkNumber forknum;
		BlockNumber blknum;

		BufferGetTag(buffer, &rlocator, &forknum, &blknum);
		is_catalog_page = rlocator.relNumber < (RelFileNumber)FirstNormalObjectId;
	}
	if (is_catalog_page) {
		cluster_catalog_stats_vis_resolve_inc();
		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_CATALOG_VIS_RESOLVE);
	}

	/* spec-4.8 D2: the tuple's page LSN is the recovered_through anchor for the
	 * cross-node TT authority gate (classify_ref). */
	anchor_lsn = PageGetLSN(page);

	switch (which) {
	case CLUSTER_VIS_XMIN:
		/* The tuple's own ITL slot records the last writer of this version. */
		if (htup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& cluster_itl_get_tt_ref(page, htup->t_itl_slot_idx, &ref)) {
			if (ref.local_xid == raw_xid)
				classify_page_ref(page, htup->t_itl_slot_idx, which, raw_xid, &ref, anchor_lsn,
								  read_scn, out);
			else {
				/*
				 * An updated old tuple points at its xmax writer, not its
				 * original xmin creator.  Prefer an exact surviving data
				 * slot for xmin; only if none is available classify the
				 * recycled last-writer ref through the existing fail-closed
				 * remote-outcome path.
				 */
				ClusterUndoTTSlotRef xmin_ref;

				if (cluster_itl_find_data_slot_index_by_xid(page, raw_xid, &slot_index)
					&& cluster_itl_get_tt_ref(page, slot_index, &xmin_ref))
					classify_page_ref(page, slot_index, which, raw_xid, &xmin_ref, anchor_lsn,
									  read_scn, out);
				else
					classify_page_ref(page, htup->t_itl_slot_idx, which, raw_xid, &ref, anchor_lsn,
									  read_scn, out);
			}
		} else if (cluster_itl_find_data_slot_index_by_xid(page, raw_xid, &slot_index)
				   && cluster_itl_get_tt_ref(page, slot_index, &ref))
			classify_page_ref(page, slot_index, which, raw_xid, &ref, anchor_lsn, read_scn, out);
		break;

	case CLUSTER_VIS_XMAX_UPDATE:
		/* The tuple's own ITL slot is the authority for its last updater. */
		if (htup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& cluster_itl_get_tt_ref(page, htup->t_itl_slot_idx, &ref))
			classify_page_ref(page, htup->t_itl_slot_idx, which, raw_xid, &ref, anchor_lsn,
							  read_scn, out);
		break;

	case CLUSTER_VIS_XMAX_LOCK_ONLY:
		/* Lock-only xmax: the writer slot is found by xmax, not by the
		 * tuple's own slot index (spec-3.4d D1). */
		if (cluster_itl_find_lock_slot_index_by_xmax(page, raw_xid, &slot_index)
			&& cluster_itl_get_tt_ref(page, slot_index, &ref)) {
			/* Preserve the lock-only reader's no-cached-verdict contract. */
			ref.cached_commit_scn = InvalidScn;
			ref.has_cached_status = false;
			classify_page_ref(page, slot_index, which, raw_xid, &ref, anchor_lsn, read_scn, out);
		}
		break;

	case CLUSTER_VIS_XMAX_MULTI: {
		/* Marker-only evidence: member visibility is policy, resolved by
		 * the caller through the 3.6 overlay. */
		uint16 marker_origin = 0;

		if (cluster_itl_find_multixact_origin_by_xmax(page, (MultiXactId)raw_xid, &marker_origin)) {
			out->multi_marker_origin = marker_origin;
			out->multi_marker_is_remote = ((int32)marker_origin != cluster_node_id);
			out->evidence = out->multi_marker_is_remote ? CLUSTER_VIS_EVIDENCE_REMOTE
														: CLUSTER_VIS_EVIDENCE_LOCAL;
		}
		break;
	}
	}

	/*
	 * spec-6.14 D10b: a STALE/AMBIGUOUS verdict on a catalog page is the
	 * fail-closed outcome -- the caller raises the 53R97-family ERROR
	 * rather than guessing (the fail-closed posture itself is untouched;
	 * this only counts it).
	 */
	if (is_catalog_page) {
		pgstat_report_wait_end();
		if (out->evidence == CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS)
			cluster_catalog_stats_vis_unknown_inc();
	}
}


/*
 * spec-3.14 D5: cheap remote-writer evidence test (no overlay lookup).
 */
bool
cluster_tuple_has_remote_evidence(Buffer buffer, HeapTupleHeader tuple)
{
	Page page;
	ClusterUndoTTSlotRef ref;
	TransactionId raw_xmax;

	if (!BufferIsValid(buffer))
		return false;
	page = BufferGetPage(buffer);
	if (!PageHasItl(page))
		return false;

	/* The tuple's own slot records the last writer (insert or update). */
	if (tuple->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
		&& cluster_itl_get_tt_ref(page, tuple->t_itl_slot_idx, &ref) && ref.tt_slot_id != 0
		&& (int32)ref.origin_node_id != cluster_node_id)
		return true;

	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return false;

	raw_xmax = HeapTupleHeaderGetRawXmax(tuple);

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		uint16 marker_origin = 0;

		if (cluster_itl_find_multixact_origin_by_xmax(page, (MultiXactId)raw_xmax, &marker_origin)
			&& (int32)marker_origin != cluster_node_id)
			return true;
	} else if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		if (cluster_itl_find_lock_tt_ref_by_xmax(page, raw_xmax, &ref) && ref.tt_slot_id != 0
			&& (int32)ref.origin_node_id != cluster_node_id)
			return true;
	}

	return false;
}

#endif /* USE_PGRAC_CLUSTER */
