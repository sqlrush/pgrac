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

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"			/* cluster_node_id, subtrans depth */
#include "cluster/cluster_itl.h"			/* get_tt_ref / lock ref / multixact origin */
#include "cluster/cluster_itl_slot.h"		/* CLUSTER_ITL_SLOT_UNALLOCATED */
#include "cluster/cluster_recovery_merge.h" /* spec-4.5a G6: materialized authority gate */
#include "cluster/cluster_remote_xact.h"	/* spec-4.5a G6: wrap-checked authority */
#include "cluster/cluster_subtrans.h"		/* SUBCOMMITTED parent follow */
#include "cluster/cluster_tt_durable.h"		/* spec-4.8 D2 remote_active_failclosed counter */
#include "cluster/cluster_tt_status.h"		/* lookup_exact / Key / Result */
#include "cluster/cluster_touched_peers.h"	/* spec-5.14 D2 class 4 */
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_wal_state.h"	   /* CLUSTER_WAL_STATE_SLOT_COUNT */
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

	if (!cluster_tt_status_lookup_exact(&key, &result) || !result.authoritative) {
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
		cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 profiling */
		return;
	}

	out->status = result.status;
	out->commit_scn = result.commit_scn;

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
classify_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, XLogRecPtr anchor_lsn,
			 ClusterVisResolve *out)
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
		 */
		out->evidence = CLUSTER_VIS_EVIDENCE_LOCAL;
		return;
	}

	/*
	 * spec-5.14 D2 class 4: past the self-check this is a genuine remote-origin
	 * ITL ref — the visibility verdict (whether via resolve_from_remote_ref or
	 * the STALE wrap-checked remote authority below) now depends on that peer's
	 * volatile TT / undo state.  Stamp conservatively so a fail-stop of the
	 * origin aborts this transaction (INV-TP1/TP2).  Read-only; resolve logic
	 * unchanged.
	 */
	cluster_touched_peers_stamp((int32)ref->origin_node_id, CLUSTER_TOUCH_VISIBILITY);

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
		if (vis_origin_materialized((int)ref->origin_node_id)) {
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
		}
		out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
		cluster_vis_bump_vis_variant_unknown_failclosed_count();
		return;
	}

	resolve_from_remote_ref(raw_xid, ref, out);
}


void
cluster_visibility_resolve_from_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
									XLogRecPtr anchor_lsn, ClusterVisResolve *out)
{
	if (out == NULL)
		return;

	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	classify_ref(raw_xid, ref, anchor_lsn, out);
}


void
cluster_visibility_resolve_tuple(Buffer buffer, HeapTupleHeader htup, TransactionId raw_xid,
								 ClusterVisXidKind which, ClusterVisResolve *out)
{
	Page page;
	ClusterUndoTTSlotRef ref;
	XLogRecPtr anchor_lsn;

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

	/* spec-4.8 D2: the tuple's page LSN is the recovered_through anchor for the
	 * cross-node TT authority gate (classify_ref). */
	anchor_lsn = PageGetLSN(page);

	switch (which) {
	case CLUSTER_VIS_XMIN:
	case CLUSTER_VIS_XMAX_UPDATE:
		/* The tuple's own ITL slot records the last writer of this version. */
		if (htup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& cluster_itl_get_tt_ref(page, htup->t_itl_slot_idx, &ref))
			classify_ref(raw_xid, &ref, anchor_lsn, out);
		break;

	case CLUSTER_VIS_XMAX_LOCK_ONLY:
		/* Lock-only xmax: the writer slot is found by xmax, not by the
		 * tuple's own slot index (spec-3.4d D1). */
		if (cluster_itl_find_lock_tt_ref_by_xmax(page, raw_xid, &ref))
			classify_ref(raw_xid, &ref, anchor_lsn, out);
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
