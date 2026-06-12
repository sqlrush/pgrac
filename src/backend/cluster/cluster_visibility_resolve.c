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
#include "storage/bufmgr.h"
#include "storage/bufpage.h"

#include "cluster/cluster_guc.h"			/* cluster_node_id, subtrans depth */
#include "cluster/cluster_itl.h"			/* get_tt_ref / lock ref / multixact origin */
#include "cluster/cluster_itl_slot.h"		/* CLUSTER_ITL_SLOT_UNALLOCATED */
#include "cluster/cluster_recovery_merge.h" /* spec-4.5a G6: materialized gate */
#include "cluster/cluster_remote_xact.h"	/* spec-4.5a G6: outcome fallback */
#include "cluster/cluster_subtrans.h"		/* SUBCOMMITTED parent follow */
#include "cluster/cluster_tt_status.h"		/* lookup_exact / Key / Result */
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_wal_state.h" /* CLUSTER_WAL_STATE_SLOT_COUNT */

/*
 * Backend-lifetime cache over cluster_merged_instance_is_materialized().
 * The STALE fallback below runs per TUPLE on recycled remote-origin slots;
 * the uncached check opens the authority-marker file each time.  An origin
 * only becomes materialized during startup recovery (no live backends), so
 * a cached verdict cannot go stale in the unsafe (false-negative) direction.
 */
static int8 materialized_origin_cache[CLUSTER_WAL_STATE_SLOT_COUNT]; /* 0 ? / 1 yes / -1 no */

static bool
merged_origin_materialized_cached(int origin)
{
	if (origin < 0 || origin >= CLUSTER_WAL_STATE_SLOT_COUNT)
		return false;
	if (materialized_origin_cache[origin] == 0)
		materialized_origin_cache[origin]
			= cluster_merged_instance_is_materialized(origin) ? 1 : -1;
	return materialized_origin_cache[origin] == 1;
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

	if (out == NULL || ref == NULL)
		return;

	out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = ref->origin_node_id;
	key.undo_segment_id = ref->undo_segment_id;
	key.tt_slot_id = ref->tt_slot_id;
	key.cluster_epoch = ref->cluster_epoch;
	key.local_xid = raw_xid;

	if (!cluster_tt_status_lookup_exact(&key, &result) || !result.authoritative)
		return; /* UNKNOWN -> caller 53R97 (C-V2: no PG-native fallback) */

	/*
	 * spec-3.5: follow a SUBCOMMITTED subxact to its parent so the caller
	 * sees a terminal-or-in-progress status, never SUBCOMMITTED itself.
	 * Depth exceeded / parent miss -> non-authoritative -> UNKNOWN.
	 */
	if (result.status == CLUSTER_TT_STATUS_SUBCOMMITTED && result.has_parent_key)
		result = cluster_subtrans_lookup_parent(&result, cluster_subtrans_max_chain_depth);

	if (!result.authoritative) {
		out->status = CLUSTER_TT_STATUS_UNKNOWN;
		return;
	}

	out->status = result.status;
	out->commit_scn = result.commit_scn;
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
classify_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, ClusterVisResolve *out)
{
	if (out == NULL || ref == NULL)
		return;

	out->ref = *ref;

	if (ref->tt_slot_id == 0) {
		/* spec-3.1 placeholder slot: not authoritative evidence. */
		out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
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

	if (ref->local_xid != raw_xid) {
		/*
		 * The available evidence says REMOTE, but the slot no longer belongs
		 * to this tuple-side xid.  Do NOT fall through to PG-native local CLOG;
		 * that is the false-resolve this resolver exists to prevent.
		 *
		 * spec-4.5a G6: when this node MATERIALIZED the origin's state, the
		 * per-origin outcome store still speaks for raw_xid even though the
		 * page slot was recycled -- it is keyed by (origin, xid), not by
		 * slot.  A recycled slot is the NORM on a merged page (the dead peer
		 * never refreshes it), so without this fallback the first slot reuse
		 * turns every read of the OLDER version into a permanent 53R97.
		 * INDOUBT / no entry keeps today's fail-closed verdict.  Residual:
		 * the store spans one cold-crash window, so a same-valued pre-window
		 * xid would need a full 32-bit wraparound inside that window's pages
		 * (task#90 wrap-generation family, same posture as WRAP_ANY readers).
		 */
		if (merged_origin_materialized_cached((int)ref->origin_node_id)) {
			SCN outcome_scn;

			switch (
				cluster_remote_commit_outcome((int)ref->origin_node_id, raw_xid, &outcome_scn)) {
			case CLUSTER_REMOTE_XACT_COMMITTED:
				out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
				out->status = CLUSTER_TT_STATUS_COMMITTED;
				out->commit_scn = outcome_scn;
				return;
			case CLUSTER_REMOTE_XACT_ABORTED:
				out->evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
				out->status = CLUSTER_TT_STATUS_ABORTED;
				out->commit_scn = InvalidScn;
				return;
			case CLUSTER_REMOTE_XACT_INDOUBT:
			default:
				break; /* fall through to STALE fail-closed */
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
									ClusterVisResolve *out)
{
	if (out == NULL)
		return;

	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	classify_ref(raw_xid, ref, out);
}


void
cluster_visibility_resolve_tuple(Buffer buffer, HeapTupleHeader htup, TransactionId raw_xid,
								 ClusterVisXidKind which, ClusterVisResolve *out)
{
	Page page;
	ClusterUndoTTSlotRef ref;

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

	switch (which) {
	case CLUSTER_VIS_XMIN:
	case CLUSTER_VIS_XMAX_UPDATE:
		/* The tuple's own ITL slot records the last writer of this version. */
		if (htup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& cluster_itl_get_tt_ref(page, htup->t_itl_slot_idx, &ref))
			classify_ref(raw_xid, &ref, out);
		break;

	case CLUSTER_VIS_XMAX_LOCK_ONLY:
		/* Lock-only xmax: the writer slot is found by xmax, not by the
		 * tuple's own slot index (spec-3.4d D1). */
		if (cluster_itl_find_lock_tt_ref_by_xmax(page, raw_xid, &ref))
			classify_ref(raw_xid, &ref, out);
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
