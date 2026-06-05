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

#include "cluster/cluster_guc.h"	   /* cluster_node_id, subtrans depth */
#include "cluster/cluster_itl.h"	   /* get_tt_ref / lock ref / multixact origin */
#include "cluster/cluster_itl_slot.h"  /* CLUSTER_ITL_SLOT_UNALLOCATED */
#include "cluster/cluster_subtrans.h"  /* SUBCOMMITTED parent follow */
#include "cluster/cluster_tt_status.h" /* lookup_exact / Key / Result */
#include "cluster/cluster_visibility_resolve.h"


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
 *	  local_xid != raw_xid      -> slot recycled to another owner ->
 *	                               STALE_OR_AMBIGUOUS (caller 53R97).
 *	  origin == self            -> LOCAL (PG CLOG resolves).
 *	  origin != self            -> REMOTE (overlay resolve).
 */
static void
classify_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref, ClusterVisResolve *out)
{
	out->ref = *ref;

	if (ref->tt_slot_id == 0) {
		/* spec-3.1 placeholder slot: not authoritative evidence. */
		out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
		return;
	}

	if (ref->local_xid != raw_xid) {
		/*
		 * The slot no longer belongs to this xid (recycled).  R10: do NOT
		 * fall through to PG-native; that is the false-resolve this
		 * resolver exists to prevent.
		 */
		out->evidence = CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS;
		return;
	}

	if ((int32)ref->origin_node_id == cluster_node_id) {
		out->evidence = CLUSTER_VIS_EVIDENCE_LOCAL;
		return;
	}

	resolve_from_remote_ref(raw_xid, ref, out);
}


void
cluster_visibility_resolve_from_ref(TransactionId raw_xid, const ClusterUndoTTSlotRef *ref,
									ClusterVisResolve *out)
{
	Assert(out != NULL);
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

	Assert(out != NULL);
	memset(out, 0, sizeof(*out));
	out->evidence = CLUSTER_VIS_EVIDENCE_NONE;
	out->status = CLUSTER_TT_STATUS_UNKNOWN;
	out->commit_scn = InvalidScn;

	if (!BufferIsValid(buffer))
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

#endif /* USE_PGRAC_CLUSTER */
