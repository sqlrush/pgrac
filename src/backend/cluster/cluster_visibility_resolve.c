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

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
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
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_wal_state.h"	   /* CLUSTER_WAL_STATE_SLOT_COUNT */
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

bool
cluster_vis_resolve_in_flight(void)
{
	return cluster_vis_resolve_depth > 0;
}

void
cluster_vis_resolve_abort_reset(void)
{
	cluster_vis_resolve_depth = 0;
}


/*
 * Fill `out` from an authoritative remote exact ref.  Performs the TT
 * overlay lookup + SUBCOMMITTED parent follow, leaving a terminal-or-
 * in-progress status.  Lookup miss / non-authoritative -> UNKNOWN (the
 * caller fail-closes; the evidence stays REMOTE so there is no PG-native
 * fallback, C-V2).
 */
static void
resolve_from_remote_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
						ClusterVisResolve *out)
{
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
	ClusterXpScope xp_scope; /* PGRAC: spec-5.59 D3 profiling */

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

	if (!cluster_tt_status_lookup_exact(&key, &result) || !result.authoritative) {
		/* PGRAC: spec-6.12c D0 -- lookup performed; no terminal verdict. */
		cluster_lever_c_note_tt_lookup(ref->has_cached_status, false);
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;					   /* UNKNOWN -> caller 53R97 (C-V2: no PG-native fallback) */
	}

	/*
	 * spec-3.5: follow a SUBCOMMITTED subxact to its parent so the caller
	 * sees a terminal-or-in-progress status, never SUBCOMMITTED itself.
	 * Depth exceeded / parent miss -> non-authoritative -> UNKNOWN.
	 */
	if (result.status == CLUSTER_TT_STATUS_SUBCOMMITTED && result.has_parent_key)
		result = cluster_subtrans_lookup_parent(&result, cluster_subtrans_max_chain_depth);

	if (!result.authoritative) {
		out->status = CLUSTER_TT_STATUS_UNKNOWN;
		/* PGRAC: spec-6.12c D0 -- lookup performed; no terminal verdict. */
		cluster_lever_c_note_tt_lookup(ref->has_cached_status, false);
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;
	}

	out->status = result.status;
	out->commit_scn = result.commit_scn;

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
				  SCN read_scn, ClusterVisResolve *out)
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

				if (cluster_runtime_visibility_try_resolve_remote(
						derived_origin, (uint32)ref->undo_segment_id, raw_xid, read_scn, &committed,
						&scn, &is_bound)) {
					out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
					out->status
						= committed ? CLUSTER_TT_STATUS_COMMITTED : CLUSTER_TT_STATUS_ABORTED;
					out->commit_scn = committed ? scn : InvalidScn;
					out->commit_scn_is_bound = committed ? is_bound : false;
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

	resolve_from_remote_ref(raw_xid, ref, out);
}

/*
 * classify_ref -- spec-6.14 D8 wrapper: run the classification under the
 * catalog-safe no-recursion counter (see cluster_vis_resolve_depth above).
 */
static void
classify_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, XLogRecPtr anchor_lsn,
			 SCN read_scn, ClusterVisResolve *out)
{
	cluster_vis_resolve_depth++;
	classify_ref_guts(raw_xid, ref, anchor_lsn, read_scn, out);
	cluster_vis_resolve_depth--;
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

	classify_ref(raw_xid, ref, anchor_lsn, read_scn, out);
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
	case CLUSTER_VIS_XMAX_UPDATE:
		/* The tuple's own ITL slot records the last writer of this version. */
		if (htup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& cluster_itl_get_tt_ref(page, htup->t_itl_slot_idx, &ref))
			classify_ref(raw_xid, &ref, anchor_lsn, read_scn, out);
		break;

	case CLUSTER_VIS_XMAX_LOCK_ONLY:
		/* Lock-only xmax: the writer slot is found by xmax, not by the
		 * tuple's own slot index (spec-3.4d D1). */
		if (cluster_itl_find_lock_tt_ref_by_xmax(page, raw_xid, &ref))
			classify_ref(raw_xid, &ref, anchor_lsn, read_scn, out);
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
