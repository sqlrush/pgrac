/*-------------------------------------------------------------------------
 *
 * heapam_visibility.c
 *	  Tuple visibility rules for tuples stored in heap.
 *
 * NOTE: all the HeapTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, MarkBufferDirtyHint is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * NOTE: When using a non-MVCC snapshot, we must check
 * TransactionIdIsInProgress (which looks in the PGPROC array) before
 * TransactionIdDidCommit (which look in pg_xact).  Otherwise we have a race
 * condition: we might decide that a just-committed transaction crashed,
 * because none of the tests succeed.  xact.c is careful to record
 * commit/abort in pg_xact before it unsets MyProc->xid in the PGPROC array.
 * That fixes that problem, but it also means there is a window where
 * TransactionIdIsInProgress and TransactionIdDidCommit will both return true.
 * If we check only TransactionIdDidCommit, we could consider a tuple
 * committed when a later GetSnapshotData call will still think the
 * originating transaction is in progress, which leads to application-level
 * inconsistency.  The upshot is that we gotta check TransactionIdIsInProgress
 * first in all code paths, except for a few cases where we are looking at
 * subtransactions of our own main transaction and so there can't be any race
 * condition.
 *
 * We can't use TransactionIdDidAbort here because it won't treat transactions
 * that were in progress during a crash as aborted.  We determine that
 * transactions aborted/crashed through process of elimination instead.
 *
 * When using an MVCC snapshot, we rely on XidInMVCCSnapshot rather than
 * TransactionIdIsInProgress, but the logic is otherwise the same: do not
 * check pg_xact until after deciding that the xact is no longer in progress.
 *
 *
 * Summary of visibility functions:
 *
 *	 HeapTupleSatisfiesMVCC()
 *		  visible to supplied snapshot, excludes current command
 *	 HeapTupleSatisfiesUpdate()
 *		  visible to instant snapshot, with user-supplied command
 *		  counter and more complex result
 *	 HeapTupleSatisfiesSelf()
 *		  visible to instant snapshot and current command
 *	 HeapTupleSatisfiesDirty()
 *		  like HeapTupleSatisfiesSelf(), but includes open transactions
 *	 HeapTupleSatisfiesVacuum()
 *		  visible to any running transaction, used by VACUUM
 *	 HeapTupleSatisfiesNonVacuumable()
 *		  Snapshot-style API for HeapTupleSatisfiesVacuum
 *	 HeapTupleSatisfiesToast()
 *		  visible unless part of interrupted vacuum, used for TOAST
 *	 HeapTupleSatisfiesAny()
 *		  all tuples are visible
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_visibility.c
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *
 *	  Cluster MVCC visibility fork: tuples carrying cluster ITL evidence are
 *	  resolved through the cluster TT/undo authority instead of native
 *	  CLOG/ProcArray (which alias across instances); unprovable outcomes fail
 *	  closed.  Spec trail: spec-3.2 .. spec-3.16 (fork + resolver layers),
 *	  spec-4.5a (CR gate / G6 xmax gate), spec-6.12i (active-runtime remote
 *	  resolution), spec-6.15 D7 (mixed-tuple foreign-xmax discipline: native
 *	  answers and hint stamps are void for provably-foreign xids; hint-stamp
 *	  suppression in SetHintBits, distrust of foreign-class hint bits, and
 *	  foreign-xmax routing in the Self / Dirty / MVCC native bodies and the
 *	  update-path helper), spec-7.1 D1 (live-slot overlay-miss verdict ask)
 *	  and spec-7.1 D3-a (multixact xmax positive resolution: origin derived
 *	  from the striped mxid value, member overlay resolve in the MVCC D6
 *	  branch and the G6 keeps-visible multi arm; underivable stays the
 *	  fail-closed floor).  Serve-stall round-6: every variant fork resolves
 *	  ITL evidence BEFORE the raw current-xid test and routes through
 *	  cluster_vis_evidence_route -- a raw TransactionIdIsCurrentTransactionId
 *	  match can be a striping-off/below-floor collision with a remote
 *	  writer's xid and must never divert REMOTE/STALE evidence to the
 *	  native self/cmin paths (spec-3.14 / spec-5.22f contract).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"

#ifdef USE_PGRAC_CLUSTER
/* spec-3.2 D5:  MVCC cluster visibility fork. */
#include "cluster/cluster_catalog_stats.h"		/* vis_unknown counter (spec-6.14 D10b) */
#include "cluster/cluster_epoch.h"				/* cluster_epoch_get_current (spec-3.3 D10) */
#include "cluster/cluster_guc.h"				/* cluster_enabled, cluster_node_id */
#include "cluster/cluster_itl.h"				/* cluster_itl_get_tt_ref */
#include "cluster/cluster_itl_cleanout.h"		/* cluster_itl_cleanout_lazy (spec-3.4c D4) */
#include "cluster/cluster_itl_slot.h"			/* CLUSTER_ITL_SLOT_UNALLOCATED */
#include "cluster/cluster_tt_slot.h"			/* ClusterUndoTTSlotRef */
#include "cluster/cluster_subtrans.h"			/* spec-3.5 D8 lookup_parent */
#include "cluster/cluster_multixact.h"			/* spec-3.6 D6 reader overlay */
#include "cluster/cluster_mxid_stripe.h"		/* spec-7.1 D3-a mxid origin derivation */
#include "cluster/cluster_tt_status.h"			/* lookup_exact / Key / Result */
#include "cluster/cluster_visibility_inject.h"	/* D5b test-only inject helper */
#include "cluster/cluster_cr.h"					/* spec-3.9 D5 CR 3-tier MVCC gate */
#include "cluster/cluster_runtime_visibility.h" /* spec-7.1 D1: try_resolve_remote */
#include "cluster/cluster_visibility_resolve.h" /* spec-3.14 D1 单一解析器 */
#include "cluster/cluster_mode.h"				/* spec-3.14 storage-mode gate */
#include "cluster/cluster_xid_stripe.h"			/* spec-6.15 D7 foreign-xmax discipline */
#endif

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC: spec-6.15 D7 — mixed-tuple deleter discipline.
 *
 * The cluster visibility fork routes a tuple by its XMIN: a local-class
 * xmin falls through to the PG-native body.  But such a "mixed" tuple may
 * still carry ANOTHER node's xid as its DELETER (bidirectional writes on
 * one shared block), and every native answer about that xmax —
 * TransactionIdDidCommit / TransactionIdIsInProgress / XidInMVCCSnapshot —
 * is void for a foreign-class xid (per-node CLOG carries no foreign bits;
 * the no-clog-overlay contract forbids mirroring them).  Trusting those
 * answers false-aborts a committed foreign deleter: the superseded version
 * stays visible next to its successor (silent duplicate rows), and the
 * HEAP_XMAX_INVALID hint stamped from it poisons the SHARED page for every
 * reader on every node.
 *
 * Above the spec-6.15 activation floor the striped value space proves a
 * foreign xid from the value alone, so the native body can now detect the
 * mixed case and route the xmax decision to the cluster resolver instead
 * (falling closed 53R97 when no proof exists — never a guess).
 */
static inline bool
cluster_plain_xmax_provably_foreign(HeapTupleHeader tuple)
{
	if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) || HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return false;
	return cluster_xid_provably_foreign(HeapTupleHeaderGetRawXmax(tuple));
}

/*
 * Instant-snapshot ("as of now") verdict for a provably-foreign plain
 * deleter, shared by the Self and Dirty native bodies.  Every unproven
 * outcome fails closed (规则 8.A) — never a fall-through to native.
 */
typedef enum ClusterForeignXmaxState {
	CLUSTER_FOREIGN_XMAX_DELETED,	  /* deleter proven committed */
	CLUSTER_FOREIGN_XMAX_NOT_DELETED, /* deleter proven aborted */
	CLUSTER_FOREIGN_XMAX_IN_PROGRESS, /* deleter proven still running */
} ClusterForeignXmaxState;

static ClusterForeignXmaxState
cluster_foreign_xmax_state(Buffer buffer, HeapTupleHeader tuple, TransactionId raw_xmax)
{
	ClusterVisResolve xr;

	cluster_visibility_resolve_tuple(buffer, tuple, raw_xmax, CLUSTER_VIS_XMAX_UPDATE, &xr);
	if (xr.evidence == CLUSTER_VIS_EVIDENCE_REMOTE) {
		switch (xr.status) {
		case CLUSTER_TT_STATUS_COMMITTED:
		case CLUSTER_TT_STATUS_CLEANED_OUT:
			return CLUSTER_FOREIGN_XMAX_DELETED;
		case CLUSTER_TT_STATUS_ABORTED:
			return CLUSTER_FOREIGN_XMAX_NOT_DELETED;
		case CLUSTER_TT_STATUS_IN_PROGRESS:
		case CLUSTER_TT_STATUS_SUBCOMMITTED:
			return CLUSTER_FOREIGN_XMAX_IN_PROGRESS;
		default:
			break;
		}
	}
	ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
					errmsg("cluster TT status unknown for xmax %u", raw_xmax),
					errhint("Remote commit_scn not yet propagated, or TT overlay "
							"missed/stale; retry or abort.")));
	return CLUSTER_FOREIGN_XMAX_NOT_DELETED; /* unreachable */
}
#else /* !USE_PGRAC_CLUSTER */
#define cluster_plain_xmax_provably_foreign(tuple) false
#endif


/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since pre-9.0 VACUUM FULL always used
 * synchronous commits and didn't move tuples that weren't previously
 * hinted.  (This is not known by this subroutine, but is applied by its
 * callers.)  Note: old-style VACUUM FULL is gone, but we have to keep this
 * module's support for MOVED_OFF/MOVED_IN flag bits for as long as we
 * support in-place update from pre-9.0 databases.
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void
SetHintBits(HeapTupleHeader tuple, Buffer buffer, uint16 infomask, TransactionId xid)
{
#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: spec-6.15 D7 — never stamp a hint whose truth this node cannot
	 * know: commit/abort bits for another node's xid class come from native
	 * CLOG / ProcArray answers that are void for foreign xids (the origin
	 * stamps its own xids with authority).  Class-only test, no floor proof:
	 * over-suppression only costs an optional hint.  Multixact ids and
	 * pre-9.0 xvac relics are exempt.
	 */
	if ((infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID)) != 0
		&& !(tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		&& cluster_xid_foreign_class_cheap(HeapTupleHeaderGetRawXmax(tuple)))
		return;
	if ((infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) != 0
		&& !(tuple->t_infomask & (HEAP_MOVED_OFF | HEAP_MOVED_IN))
		&& cluster_xid_foreign_class_cheap(HeapTupleHeaderGetRawXmin(tuple)))
		return;
#endif

	if (TransactionIdIsValid(xid)) {
		/* NB: xid must be known committed here! */
		XLogRecPtr commitLSN = TransactionIdGetCommitLSN(xid);

		if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN)
			&& BufferGetLSNAtomic(buffer) < commitLSN) {
			/* not flushed and no LSN interlock, so don't set hint */
			return;
		}
	}

	tuple->t_infomask |= infomask;
	MarkBufferDirtyHint(buffer, true);
}

#ifdef USE_PGRAC_CLUSTER
/*
 * cluster_heap_stamp_released_xmax_invalid -- authority-backed XMAX_INVALID.
 *
 *	spec-7.1a D0/D2: SetHintBits above deliberately suppresses xmax hints
 *	whose xid is another node's class (spec-6.15 D7) because ordinary hint
 *	stamping is backed by native CLOG/ProcArray answers that are void for a
 *	foreign xid.  The cluster writer/locker paths however must still
 *	physically clear a RELEASED (lock-only terminal) or ABORTED remote xmax
 *	once CLUSTER authority (TT / live-IC verdict) proved it gone -- leaving
 *	it in place makes compute_new_xmax_infomask treat the dead remote xid as
 *	a live locker and fold it into a node-local MultiXact that aliases on
 *	every peer (L349).  This is the one sanctioned bypass: the fact comes
 *	from cluster authority, never from a native reading of the foreign xid,
 *	and marking released/aborted needs no commit-LSN interlock (see the
 *	SetHintBits header: aborted hints are always safe).
 */
void
cluster_heap_stamp_released_xmax_invalid(HeapTupleHeader tuple, Buffer buffer)
{
	tuple->t_infomask |= HEAP_XMAX_INVALID;
	MarkBufferDirtyHint(buffer, true);
}
#endif

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer, uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, buffer, infomask, xid);
}


/*
 * HeapTupleSatisfiesSelf
 *		True iff heap tuple is valid "for itself".
 *
 * See SNAPSHOT_MVCC's definition for the intended behaviour.
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
#ifdef USE_PGRAC_CLUSTER
/*
 * spec-3.14 D4: HeapTupleSatisfiesSelf cluster fork (OBS-4).
 *
 *	Self visibility = "committed now": xmin committed AND not deleted by
 *	a committed xmax.  No snapshot ordering.  Tuple ITL evidence gate
 *	(the static SnapshotSelf is always LOCAL).  When xmin is remote we
 *	must not fall through (PG would re-judge it via local CLOG); local
 *	xmax is judged with PG primitives (correct for a local xid).
 *
 *	PGRAC serve-stall round-6: ITL evidence is resolved BEFORE the raw
 *	current-xid test on both xid legs (cluster_vis_evidence_route) -- a
 *	raw match can be a striping-off/below-floor collision with a remote
 *	writer's xid, so only the no-remote-evidence routes may treat it as
 *	our own transaction.
 */
static bool
cluster_satisfies_self_fork(HeapTuple htup, Buffer buffer, bool *visible)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId raw_xid = HeapTupleHeaderGetRawXmin(tuple);
	TransactionId raw_xmax;
	ClusterVisResolve r;

	if (!cluster_storage_mode_enabled() || !BufferIsValid(buffer))
		return false;
	if (!PageHasItl(BufferGetPage(buffer)))
		return false;

	cluster_visibility_resolve_tuple(buffer, tuple, raw_xid, CLUSTER_VIS_XMIN, &r);
	switch (cluster_vis_evidence_route(r.evidence, TransactionIdIsCurrentTransactionId(raw_xid))) {
	case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT status unknown for xid %u", raw_xid),
						errhint("Remote commit_scn not yet propagated; retry or abort.")));
		break;
	case CLUSTER_VIS_ROUTE_NATIVE_SELF:
	case CLUSTER_VIS_ROUTE_NATIVE:
		return false; /* own insert / local xmin: PG-native (xmax judged there) */
	case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
		break;
	}

	switch (cluster_vis_self_verdict(r.status)) {
	case CVV_INVISIBLE:
		*visible = false;
		return true;
	case CVV_FAILCLOSED_UNKNOWN:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT status unknown for xid %u", raw_xid),
						errhint("Remote commit_scn not yet propagated; retry or abort.")));
		break;
	default:
		break; /* CVV_VISIBLE: xmin committed, check xmax for deletion */
	}

	/* A lock-only or absent xmax never deletes the tuple. */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) || HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		*visible = true;
		return true;
	}

	raw_xmax = HeapTupleHeaderGetRawXmax(tuple);

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		/* Multixact deleter: conservative -- treat as not-yet-committed
		 * delete (still visible).  A committed cross-node multixact update
		 * is rare on a self-snapshot path; correctness errs visible. */
		*visible = true;
		return true;
	}

	{
		ClusterVisResolve xr;

		cluster_visibility_resolve_tuple(buffer, tuple, raw_xmax, CLUSTER_VIS_XMAX_UPDATE, &xr);
		switch (cluster_vis_evidence_route(xr.evidence,
										   TransactionIdIsCurrentTransactionId(raw_xmax))) {
		case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
			raw_xid = raw_xmax;
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster TT status unknown for xid %u", raw_xid),
							errhint("Remote commit_scn not yet propagated; retry or abort.")));
			break;
		case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
			switch (cluster_vis_self_verdict(xr.status)) {
			case CVV_VISIBLE:
				*visible = false; /* deleter committed -> deleted */
				return true;
			case CVV_FAILCLOSED_UNKNOWN: {
				raw_xid = raw_xmax;
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
								errmsg("cluster TT status unknown for xid %u", raw_xid),
								errhint("Remote commit_scn not yet propagated; retry or abort.")));
				break;
			}
			default:
				*visible = true; /* deleter in-progress/aborted -> not deleted */
				return true;
			}
			break;
		case CLUSTER_VIS_ROUTE_NATIVE_SELF:
			*visible = false; /* deleted by our own xact */
			return true;
		case CLUSTER_VIS_ROUTE_NATIVE:
			break;
		}
		/* local xmax: PG primitives are correct for a local xid. */
		if (TransactionIdDidCommit(raw_xmax))
			*visible = false;
		else
			*visible = true;
		return true;
	}
}
#endif /* USE_PGRAC_CLUSTER */


static bool
HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef USE_PGRAC_CLUSTER
	{
		bool cluster_vis;

		if (cluster_satisfies_self_fork(htup, buffer, &cluster_vis)) {
			cluster_vis_bump_vis_selftoast_fork_count();
			return cluster_vis;
		}
	}
#endif

	if (!HeapTupleHeaderXminCommitted(tuple)) {
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac)) {
				if (TransactionIdDidCommit(xvac)) {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac)) {
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
				else {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
			}
		} else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
			if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) /* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
				return true;
			}

			return false;
		} else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
		else {
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	/* PGRAC: spec-6.15 D7 — a provably-foreign deleter's hints may be poison
	 * stamps from the native no-authority era (see the mixed-tuple banner);
	 * never trust them: fall through to the cluster resolver below. */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
		&& !cluster_plain_xmax_provably_foreign(tuple))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		/* PGRAC: spec-6.15 D7 — foreign COMMITTED hint: resolve below. */
		if (!cluster_plain_xmax_provably_foreign(tuple))
			return false; /* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		if (TransactionIdIsInProgress(xmax))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;
		/* it must have aborted or crashed */
		return true;
	}

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: spec-6.15 D7 — mixed tuple: local-class xmin fell through to
	 * this native body, but the deleter is provably another node's xid, so
	 * every native answer below is void for it.  Resolve it instead. */
	if (cluster_plain_xmax_provably_foreign(tuple)) {
		if (cluster_foreign_xmax_state(buffer, tuple, HeapTupleHeaderGetRawXmax(tuple))
			== CLUSTER_FOREIGN_XMAX_DELETED)
			return false; /* updated by other */
		return true;	  /* in-progress or aborted deleter: still visible to self */
	}
#endif

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
	return false;
}

/*
 * HeapTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
static bool
HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	return true;
}

/*
 * HeapTupleSatisfiesToast
 *		True iff heap tuple is valid as a TOAST row.
 *
 * See SNAPSHOT_TOAST's definition for the intended behaviour.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.  However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
#ifdef USE_PGRAC_CLUSTER
/*
 * spec-3.14 D4: HeapTupleSatisfiesToast cluster fork (OBS-5).
 *
 *	Toast chunks are insert-only and read only after the main row passed
 *	visibility, so PG-native is permissive: only an ABORTED writer hides
 *	the chunk; it never inspects xmax.  We mirror that -- xmin evidence
 *	only.  Snapshot-source cannot gate (the toast snapshot is the static
 *	SnapshotToast, always LOCAL), so the gate is tuple ITL evidence.
 */
static bool
cluster_satisfies_toast_fork(HeapTuple htup, Buffer buffer, bool *visible)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId raw_xid = HeapTupleHeaderGetRawXmin(tuple);
	ClusterVisResolve r;

	if (!cluster_storage_mode_enabled() || !BufferIsValid(buffer))
		return false;
	if (!PageHasItl(BufferGetPage(buffer)))
		return false;

	/* PGRAC serve-stall round-6: resolve ITL evidence BEFORE the raw
	 * current-xid test (a raw match can be a below-floor collision with a
	 * remote xid); only the no-remote-evidence routes fall to PG-native. */
	cluster_visibility_resolve_tuple(buffer, tuple, raw_xid, CLUSTER_VIS_XMIN, &r);
	switch (cluster_vis_evidence_route(r.evidence, TransactionIdIsCurrentTransactionId(raw_xid))) {
	case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT status unknown for xid %u", raw_xid),
						errhint("Remote commit_scn not yet propagated; retry or abort.")));
		break;
	case CLUSTER_VIS_ROUTE_NATIVE_SELF:
	case CLUSTER_VIS_ROUTE_NATIVE:
		return false;
	case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
		break;
	}

	switch (cluster_vis_toast_verdict(r.status)) {
	case CVV_INVISIBLE:
		*visible = false;
		return true;
	case CVV_FAILCLOSED_UNKNOWN:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT status unknown for xid %u", raw_xid),
						errhint("Remote commit_scn not yet propagated; retry or abort.")));
		break;
	default:
		*visible = true; /* CVV_VISIBLE */
		return true;
	}
	return true; /* unreachable */
}
#endif /* USE_PGRAC_CLUSTER */


static bool
HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef USE_PGRAC_CLUSTER
	{
		bool cluster_vis;

		if (cluster_satisfies_toast_fork(htup, buffer, &cluster_vis)) {
			cluster_vis_bump_vis_selftoast_fork_count();
			return cluster_vis;
		}
	}
#endif

	if (!HeapTupleHeaderXminCommitted(tuple)) {
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac)) {
				if (TransactionIdDidCommit(xvac)) {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac)) {
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
				else {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
			}
		}

		/*
		 * An invalid Xmin can be left behind by a speculative insertion that
		 * is canceled by super-deleting the tuple.  This also applies to
		 * TOAST tuples created during speculative insertion.
		 */
		else if (!TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)))
			return false;
	}

	/* otherwise assume the tuple is valid for TOAST. */
	return true;
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *	This function returns a more detailed result code than most of the
 *	functions in this file, since UPDATE needs to know more than "is it
 *	visible?".  It also allows for user-supplied CommandId rather than
 *	relying on CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	TM_Invisible: the tuple didn't exist at all when the scan started, e.g. it
 *	was created by a later CommandId.
 *
 *	TM_Ok: The tuple is valid and visible, so it may be updated.
 *
 *	TM_SelfModified: The tuple was updated by the current transaction, after
 *	the current scan started.
 *
 *	TM_Updated: The tuple was updated by a committed transaction (including
 *	the case where the tuple was moved into a different partition).
 *
 *	TM_Deleted: The tuple was deleted by a committed transaction.
 *
 *	TM_BeingModified: The tuple is being updated by an in-progress transaction
 *	other than the current transaction.  (Note: this includes the case where
 *	the tuple is share-locked by a MultiXact, even if the MultiXact includes
 *	the current transaction.  Callers that want to distinguish that case must
 *	test for it themselves.)
 */
#ifdef USE_PGRAC_CLUSTER
/*
 * spec-3.14 D2a: HeapTupleSatisfiesUpdate cluster fork.
 *
 *	Closes prospecting hole #1 (lost update): without this, a remote
 *	in-progress xmax falls into the PG-native body and is judged via the
 *	LOCAL CLOG -- a remote running writer reads as "not in progress" ->
 *	TM_Ok -> lost update.  SatisfiesUpdate has no snapshot, so the gate
 *	is tuple ITL evidence (D1), not snapshot_source.
 *
 *	Returns true + *res when the cluster path decides; false to fall
 *	through to the PG-native body (own xid / no remote evidence / fully
 *	local tuple -- all correct on the local CLOG).
 *
 *	C-V4: a remote in-progress writer maps to TM_BeingModified here; the
 *	caller-side wait bridge (D2b) translates it to 53R9H after the ABA
 *	recheck, so wait_policy (SKIP LOCKED / NOWAIT) is preserved.  The
 *	lock-only remote path reuses the spec-3.4d bridge unchanged.
 */
static bool
cluster_satisfies_update_fork(HeapTuple htup, Buffer buffer, TM_Result *res)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
	TransactionId raw_xmax;
	ClusterVisResolve r;
	bool xmin_remote_visible = false;
	bool lock_only;
	bool is_delete;
	ClusterVisXidKind kind;

	if (!cluster_storage_mode_enabled() || !BufferIsValid(buffer))
		return false;
	if (!PageHasItl(BufferGetPage(buffer)))
		return false;

	/* ---- xmin ----
	 *
	 * PGRAC serve-stall round-6: ITL evidence is resolved BEFORE the raw
	 * current-xid test (cluster_vis_evidence_route).  A raw match can be a
	 * striping-off/below-floor collision with a remote inserter's xid; the
	 * pre-round-6 early return handed that collision to the PG-native cmin
	 * path -> "attempted to update invisible tuple".  Only the no-remote-
	 * evidence route treats the raw match as our own insert.  VACUUM freeze is
	 * stronger: it proves xmin committed even after that historical ITL slot is
	 * recycled, so only the exact FROZEN bit pair skips xmin resolution.
	 */
	if (cluster_vis_xmin_needs_resolution(tuple->t_infomask)) {
		cluster_visibility_resolve_tuple(buffer, tuple, raw_xmin, CLUSTER_VIS_XMIN, &r);
		switch (
			cluster_vis_evidence_route(r.evidence, TransactionIdIsCurrentTransactionId(raw_xmin))) {
		case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster TT slot recycled for xmin %u", raw_xmin),
							errhint("ITL slot no longer maps to this xid; retry.")));
			break;
		case CLUSTER_VIS_ROUTE_NATIVE_SELF:
			/* Our own insert: PG-native handles cmin / self-modified correctly. */
			return false;
		case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
			switch (cluster_vis_update_xmin_verdict(r.status)) {
			case CVV_INVISIBLE:
				*res = TM_Invisible;
				return true;
			case CVV_FAILCLOSED_UNKNOWN:
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
								errmsg("cluster TT status unknown for xmin %u", raw_xmin),
								errhint("Remote commit_scn not yet propagated; retry or abort.")));
				break;
			case CVV_VISIBLE:
				xmin_remote_visible = true;
				break;
			default:
				break;
			}
			break;
		case CLUSTER_VIS_ROUTE_NATIVE:
			break;
		}
	}
	/* xmin NONE/LOCAL: leave xmin to PG unless we must intercept the xmax. */

	/* ---- xmax ---- */
	if (tuple->t_infomask & HEAP_XMAX_INVALID) {
		if (xmin_remote_visible) {
			*res = TM_Ok; /* live remote-committed tuple, no deleter */
			return true;
		}
		/* PGRAC: spec-6.15 D7 — a provably-foreign INVALID hint may be a
		 * poison stamp from the native no-authority era; never trust it:
		 * fall through to the xmax resolve below. */
		if (!cluster_plain_xmax_provably_foreign(tuple))
			return false; /* local xmin: PG-native */
	}

	raw_xmax = HeapTupleHeaderGetRawXmax(tuple);

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		ClusterVisResolve mr;

		cluster_visibility_resolve_tuple(buffer, tuple, raw_xmax, CLUSTER_VIS_XMAX_MULTI, &mr);
		if (mr.multi_marker_is_remote) {
			*res = TM_BeingModified; /* remote multixact -> D2b bridge 53R9H */
			return true;
		}
		if (xmin_remote_visible)
			/* local multixact over a remote-inserted tuple: rare; cannot
			 * judge members against the remote xmin safely here. Conservative
			 * fail-closed (53R9H, retryable) rather than risk a wrong verdict. */
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT),
							errmsg("cross-node write conflict on multixact-locked tuple"),
							errhint("Retry the transaction; cross-node multixact wait lands "
									"at Stage 5.2.")));
		return false; /* fully local: PG-native */
	}

	lock_only = HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask);
	is_delete = !lock_only && !HeapTupleHeaderIndicatesMovedPartitions(tuple)
				&& ItemPointerEquals(&htup->t_self, &tuple->t_ctid);
	kind = lock_only ? CLUSTER_VIS_XMAX_LOCK_ONLY : CLUSTER_VIS_XMAX_UPDATE;

	/* PGRAC serve-stall round-6: resolve ITL evidence BEFORE the raw
	 * current-xid test (see the xmin leg above).  A raw match can be a
	 * below-floor collision with a remote deleter/locker; the pre-round-6
	 * early return misread that as TM_SelfModified. */
	cluster_visibility_resolve_tuple(buffer, tuple, raw_xmax, kind, &r);
	switch (cluster_vis_evidence_route(r.evidence, TransactionIdIsCurrentTransactionId(raw_xmax))) {
	case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT slot recycled for xmax %u", raw_xmax),
						errhint("ITL slot no longer maps to this xid; retry.")));
		break;
	case CLUSTER_VIS_ROUTE_NATIVE_SELF:
		/* Our own xmax: PG-native (self-modified). */
		if (xmin_remote_visible) {
			*res = TM_SelfModified;
			return true;
		}
		return false;
	case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
		switch (cluster_vis_update_xmax_verdict(r.status, is_delete)) {
		case CVV_VISIBLE:
			cluster_vis_bump_xmax_resolved_count(); /* spec-7.1a D6 */
			*res = TM_Ok;
			return true;
		case CVV_GONE_UPDATED:
			cluster_vis_bump_xmax_resolved_count(); /* spec-7.1a D6 */
			*res = TM_Updated;
			return true;
		case CVV_GONE_DELETED:
			cluster_vis_bump_xmax_resolved_count(); /* spec-7.1a D6 */
			*res = TM_Deleted;
			return true;
		case CVV_BEING_MODIFIED:
			*res = TM_BeingModified; /* -> D2b bridge (writer 53R9H / lock-only 53R98) */
			return true;
		case CVV_FAILCLOSED_UNKNOWN:
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster TT status unknown for xmax %u", raw_xmax),
							errhint("Remote commit_scn not yet propagated; retry or abort.")));
			break;
		default:
			break;
		}
		break;
	case CLUSTER_VIS_ROUTE_NATIVE:
		break;
	}

	/*
	 * PGRAC: spec-6.15 D7 — the resolve above produced no proof (NONE
	 * evidence).  A provably-foreign xmax (deleter OR locker) must never be
	 * handed back to native primitives — their answers are void for it, and
	 * "aborted by elimination" is exactly the false-abort that poisoned the
	 * shared page.  Fail closed instead.
	 */
	if (cluster_xid_provably_foreign(raw_xmax))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT status unknown for xmax %u", raw_xmax),
						errhint("Remote commit_scn not yet propagated; retry or abort.")));

	/*
	 * xmax NONE/LOCAL.  If xmin is remote-committed we MUST NOT fall through
	 * (the PG body would re-judge the remote xmin via local CLOG, hole #1);
	 * judge the local xmax with PG primitives (correct for a local xid).
	 */
	if (xmin_remote_visible) {
		if (TransactionIdIsInProgress(raw_xmax))
			*res = TM_BeingModified;
		else if (TransactionIdDidCommit(raw_xmax))
			*res = is_delete ? TM_Deleted : TM_Updated;
		else
			*res = TM_Ok; /* xmax aborted */
		return true;
	}

	return false; /* fully local tuple: PG-native */
}
#endif /* USE_PGRAC_CLUSTER */

TM_Result
HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef USE_PGRAC_CLUSTER
	{
		TM_Result cluster_res;

		if (cluster_satisfies_update_fork(htup, buffer, &cluster_res)) {
			cluster_vis_bump_vis_update_fork_count();
			return cluster_res;
		}
	}
#endif

	if (!HeapTupleHeaderXminCommitted(tuple)) {
		if (HeapTupleHeaderXminInvalid(tuple))
			return TM_Invisible;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return TM_Invisible;
			if (!TransactionIdIsInProgress(xvac)) {
				if (TransactionIdDidCommit(xvac)) {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return TM_Invisible;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac)) {
				if (TransactionIdIsInProgress(xvac))
					return TM_Invisible;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
				else {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return TM_Invisible;
				}
			}
		} else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
			if (HeapTupleHeaderGetCmin(tuple) >= curcid)
				return TM_Invisible; /* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
				return TM_Ok;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
				TransactionId xmax;

				xmax = HeapTupleHeaderGetRawXmax(tuple);

				/*
				 * Careful here: even though this tuple was created by our own
				 * transaction, it might be locked by other transactions, if
				 * the original version was key-share locked when we updated
				 * it.
				 */

				if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
					if (MultiXactIdIsRunning(xmax, true))
						return TM_BeingModified;
					else
						return TM_Ok;
				}

				/*
				 * If the locker is gone, then there is nothing of interest
				 * left in this Xmax; otherwise, report the tuple as
				 * locked/updated.
				 */
				if (!TransactionIdIsInProgress(xmax))
					return TM_Ok;
				return TM_BeingModified;
			}

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* deleting subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax)) {
					if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
						return TM_BeingModified;
					return TM_Ok;
				} else {
					if (HeapTupleHeaderGetCmax(tuple) >= curcid)
						return TM_SelfModified; /* updated after scan started */
					else
						return TM_Invisible; /* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
				return TM_Ok;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return TM_SelfModified; /* updated after scan started */
			else
				return TM_Invisible; /* updated before scan started */
		} else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)))
			return TM_Invisible;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
		else {
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			return TM_Invisible;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
		return TM_Ok;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return TM_Ok;
		if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
			return TM_Updated; /* updated by other */
		else
			return TM_Deleted; /* deleted by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		TransactionId xmax;

		if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
			return TM_Ok;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
				return TM_BeingModified;

			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			return TM_Ok;
		}

		xmax = HeapTupleGetUpdateXid(tuple);
		if (!TransactionIdIsValid(xmax)) {
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
				return TM_BeingModified;
		}

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax)) {
			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return TM_SelfModified; /* updated after scan started */
			else
				return TM_Invisible; /* updated before scan started */
		}

		if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
			return TM_BeingModified;

		if (TransactionIdDidCommit(xmax)) {
			if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
				return TM_Updated;
			else
				return TM_Deleted;
		}

		/*
		 * By here, the update in the Xmax is either aborted or crashed, but
		 * what about the other members?
		 */

		if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false)) {
			/*
			 * There's no member, even just a locker, alive anymore, so we can
			 * mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			return TM_Ok;
		} else {
			/* There are lockers running */
			return TM_BeingModified;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return TM_BeingModified;
		if (HeapTupleHeaderGetCmax(tuple) >= curcid)
			return TM_SelfModified; /* updated after scan started */
		else
			return TM_Invisible; /* updated before scan started */
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
		return TM_BeingModified;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		return TM_Ok;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		return TM_Ok;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
	if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid))
		return TM_Updated; /* updated by other */
	else
		return TM_Deleted; /* deleted by other */
}

/*
 * HeapTupleSatisfiesDirty
 *		True iff heap tuple is valid including effects of open transactions.
 *
 * See SNAPSHOT_DIRTY's definition for the intended behaviour.
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.
 * Similarly for snapshot->xmax and the tuple's xmax.  If the tuple was
 * inserted speculatively, meaning that the inserter might still back down
 * on the insertion without aborting the whole transaction, the associated
 * token is also returned in snapshot->speculativeToken.
 */
#ifdef USE_PGRAC_CLUSTER
/*
 * spec-3.14 D3: HeapTupleSatisfiesDirty cluster fork (OBS-3).
 *
 *	Dirty (used by nbtinsert uniqueness, RI, replication) has NO
 *	wait_policy layer and reports an in-progress writer back via
 *	snapshot->xmin/xmax for the caller to wait on.  A REMOTE in-progress
 *	writer cannot be reported as a local wait handle (poisoning), so it
 *	fail-closes to 53R9H.  Tuple ITL evidence gate.
 */
static bool
cluster_satisfies_dirty_fork(HeapTuple htup, Snapshot snapshot, Buffer buffer, bool *visible)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId raw_xid = HeapTupleHeaderGetRawXmin(tuple);
	TransactionId raw_xmax;
	ClusterVisResolve r;

	if (!cluster_storage_mode_enabled() || !BufferIsValid(buffer))
		return false;
	if (!PageHasItl(BufferGetPage(buffer)))
		return false;

	/* PGRAC serve-stall round-6: resolve ITL evidence BEFORE the raw
	 * current-xid test (a raw match can be a below-floor collision with a
	 * remote xid); only the no-remote-evidence routes fall to PG-native. */
	cluster_visibility_resolve_tuple(buffer, tuple, raw_xid, CLUSTER_VIS_XMIN, &r);
	switch (cluster_vis_evidence_route(r.evidence, TransactionIdIsCurrentTransactionId(raw_xid))) {
	case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("cluster TT status unknown for xid %u", raw_xid),
						errhint("Remote commit_scn not yet propagated; retry or abort.")));
		break;
	case CLUSTER_VIS_ROUTE_NATIVE_SELF:
	case CLUSTER_VIS_ROUTE_NATIVE:
		return false; /* own insert / local / no evidence: PG-native */
	case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
		switch (cluster_vis_dirty_verdict(r.status, false, false)) {
		case CVV_FAILCLOSED_CONFLICT:
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT),
					 errmsg("cross-node write conflict: remote in-progress xmin %u", raw_xid),
					 errhint("Retry; cross-node wait lands at Stage 5.2.")));
			break;
		case CVV_INVISIBLE:
			*visible = false;
			return true;
		case CVV_FAILCLOSED_UNKNOWN:
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster TT status unknown for xid %u", raw_xid),
							errhint("Remote commit_scn not yet propagated; retry or abort.")));
			break;
		default:
			break; /* CVV_VISIBLE: xmin committed, check xmax */
		}
		break;
	}

	/* xmin remote committed: judge xmax (cannot fall through). */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) || HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		*visible = true;
		return true;
	}

	raw_xmax = HeapTupleHeaderGetRawXmax(tuple);

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		ClusterVisResolve mr;

		cluster_visibility_resolve_tuple(buffer, tuple, raw_xmax, CLUSTER_VIS_XMAX_MULTI, &mr);
		if (mr.multi_marker_is_remote)
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT),
							errmsg("cross-node write conflict: remote multixact xmax"),
							errhint("Retry; cross-node wait lands at Stage 5.2.")));
		*visible = true; /* local multixact over remote xmin: conservative visible */
		return true;
	}

	{
		ClusterVisResolve xr;

		/* PGRAC serve-stall round-6: resolve BEFORE the raw current-xid
		 * test (see the xmin leg above). */
		cluster_visibility_resolve_tuple(buffer, tuple, raw_xmax, CLUSTER_VIS_XMAX_UPDATE, &xr);
		switch (cluster_vis_evidence_route(xr.evidence,
										   TransactionIdIsCurrentTransactionId(raw_xmax))) {
		case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
			raw_xid = raw_xmax;
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster TT status unknown for xid %u", raw_xid),
							errhint("Remote commit_scn not yet propagated; retry or abort.")));
			break;
		case CLUSTER_VIS_ROUTE_NATIVE_SELF:
			*visible = false; /* deleted by self */
			return true;
		case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
			switch (cluster_vis_dirty_verdict(xr.status, true, false)) {
			case CVV_FAILCLOSED_CONFLICT:
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT),
						 errmsg("cross-node write conflict: remote in-progress xmax %u", raw_xmax),
						 errhint("Retry; cross-node wait lands at Stage 5.2.")));
				break;
			case CVV_GONE_UPDATED:
			case CVV_GONE_DELETED:
				*visible = false;
				return true;
			case CVV_FAILCLOSED_UNKNOWN:
				raw_xid = raw_xmax;
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
								errmsg("cluster TT status unknown for xid %u", raw_xid),
								errhint("Remote commit_scn not yet propagated; retry or abort.")));
				break;
			default:
				*visible = true;
				return true;
			}
			break;
		case CLUSTER_VIS_ROUTE_NATIVE:
			break;
		}
		/* local xmax. */
		if (TransactionIdIsInProgress(raw_xmax)) {
			snapshot->xmax = raw_xmax;
			*visible = true;
		} else if (TransactionIdDidCommit(raw_xmax))
			*visible = false;
		else
			*visible = true;
		return true;
	}
}
#endif /* USE_PGRAC_CLUSTER */


static bool
HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = 0;

#ifdef USE_PGRAC_CLUSTER
	{
		bool cluster_vis;

		if (cluster_satisfies_dirty_fork(htup, snapshot, buffer, &cluster_vis)) {
			cluster_vis_bump_vis_dirty_fork_count();
			return cluster_vis;
		}
	}
#endif

	if (!HeapTupleHeaderXminCommitted(tuple)) {
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!TransactionIdIsInProgress(xvac)) {
				if (TransactionIdDidCommit(xvac)) {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac)) {
				if (TransactionIdIsInProgress(xvac))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
				else {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
			}
		} else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
			if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) /* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
				return true;
			}

			return false;
		} else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))) {
			/*
			 * Return the speculative token to caller.  Caller can worry about
			 * xmax, since it requires a conclusively locked row version, and
			 * a concurrent update to this tuple is a conflict of its
			 * purposes.
			 */
			if (HeapTupleHeaderIsSpeculative(tuple)) {
				snapshot->speculativeToken = HeapTupleHeaderGetSpeculativeToken(tuple);

				Assert(snapshot->speculativeToken != 0);
			}

			snapshot->xmin = HeapTupleHeaderGetRawXmin(tuple);
			/* XXX shouldn't we fall through to look at xmax? */
			return true; /* in insertion by other */
		} else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
		else {
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	/* PGRAC: spec-6.15 D7 — a provably-foreign deleter's hints may be poison
	 * stamps from the native no-authority era (see the mixed-tuple banner);
	 * never trust them: fall through to the cluster resolver below. */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
		&& !cluster_plain_xmax_provably_foreign(tuple))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		/* PGRAC: spec-6.15 D7 — foreign COMMITTED hint: resolve below. */
		if (!cluster_plain_xmax_provably_foreign(tuple))
			return false; /* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		if (TransactionIdIsInProgress(xmax)) {
			snapshot->xmax = xmax;
			return true;
		}
		if (TransactionIdDidCommit(xmax))
			return false;
		/* it must have aborted or crashed */
		return true;
	}

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: spec-6.15 D7 — mixed tuple: local-class xmin fell through to
	 * this native body, but the deleter is provably another node's xid, so
	 * every native answer below is void for it.  An in-flight foreign
	 * deleter cannot be reported via snapshot->xmax either (native
	 * XactLockTableWait cannot wait on another node's xid) — fail closed
	 * retryable; the cross-node wait rides the writer's D2b bridge. */
	if (cluster_plain_xmax_provably_foreign(tuple)) {
		switch (cluster_foreign_xmax_state(buffer, tuple, HeapTupleHeaderGetRawXmax(tuple))) {
		case CLUSTER_FOREIGN_XMAX_DELETED:
			return false; /* updated by other */
		case CLUSTER_FOREIGN_XMAX_NOT_DELETED:
			return true;
		case CLUSTER_FOREIGN_XMAX_IN_PROGRESS:
		default:
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT),
							errmsg("cross-node write conflict on tuple with in-progress "
								   "remote deleter"),
							errhint("Retry the transaction.")));
		}
	}
#endif

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple))) {
		if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			snapshot->xmax = HeapTupleHeaderGetRawXmax(tuple);
		return true;
	}

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
	return false; /* updated by other */
}

#ifdef USE_PGRAC_CLUSTER
/*
 * cluster_remote_live_xmax_keeps_visible -- spec-4.5a G6 steady-state xmax gate.
 *
 *	The cluster xmin-resolution branch of HeapTupleSatisfiesMVCC returns its
 *	verdict directly and NEVER falls through to the PG-native xmax check, so a
 *	tuple whose remote xmin resolved visible has not yet been tested for a
 *	committed DELETER.  Without this, every superseded version on a
 *	merged-materialized page stays visible (all chain versions returned at
 *	once).  Works on the LIVE tuple (the block is at/before read_scn, so the
 *	live row IS the as-of-snapshot row -- no CR construction needed):
 *	  - REMOTE deleter: per-origin authority + the spec-3.21 verdict table
 *	    (mirrors the non-CR xmax arm of cluster_cr_satisfies_mvcc);
 *	  - OWN deleter (slot-bound; post-merge local DML on a materialized row
 *	    is ordinary operation) / our own in-progress delete: PG-native
 *	    snapshot semantics, alias-free for own xids;
 *	  - anything unprovable (recycled slot, no evidence, foreign multixact,
 *	    in-doubt outcome): fail closed.
 *
 *	Returns: 1 keep the xmin verdict (no delete visible at this snapshot);
 *	0 the delete is visible at this snapshot -> tuple invisible; -1 deleter
 *	outcome unprovable -> caller fail-closes (53R97, 规则 8.A).
 */
static int
cluster_remote_live_xmax_keeps_visible(Buffer buffer, HeapTupleHeader tuple, Snapshot snapshot)
{
	TransactionId xmax;
	ClusterVisResolve xr;
	ClusterVisibilityDecision scn_decision = CLUSTER_VISIBILITY_UNKNOWN;

	/* PGRAC: spec-6.15 D7 — a provably-foreign INVALID hint may be a poison
	 * stamp from the native no-authority era; never trust it, resolve. */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) && !cluster_plain_xmax_provably_foreign(tuple))
		return 1;
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return 1; /* a lock-only xmax never deletes the row */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		/*
		 * PGRAC multi-xmax alias floor + spec-7.1 D3-a positive
		 * resolution: a multixact id is node-local and its members come
		 * from the LOCAL pg_multixact, which ALIASES for a foreign multi
		 * once the local counter passes that id (AD-012 例外 9; the
		 * spec-7.1 census probe-F silent false-visible).  The spec-3.6
		 * D7b page marker cannot distinguish the origin (it stamps the
		 * reader's own id); the striped mxid VALUE can (D3-a Route B):
		 * derived own -> native member decode is alias-free above the
		 * activation floor; derived foreign -> member overlay resolve
		 * (OBS-1 truth table, same helper as the MVCC D6 branch);
		 * underivable or overlay-unprovable -> -1, the caller's
		 * retryable 53R97 ERROR (fail-closed direction unchanged).
		 * (Lock-only multis returned 1 above: a lock never deletes the
		 * row, no member decode is needed.)
		 */
		if (cluster_peer_mode_enabled()) {
			int mx_origin = -1;

			if (cluster_multi_xmax_remote_resolve)
				mx_origin = cluster_mxid_origin_slot((MultiXactId)HeapTupleHeaderGetRawXmax(tuple));

			if (mx_origin < 0) {
				/* PGRAC: spec-7.1 D0 census — foreign-multi refuse leg. */
				cluster_multixact_note_underivable_read();
				cluster_vis53r97_note_multi_unresolvable();
				return -1;
			}
			if (mx_origin != cluster_node_id) {
				bool mx_hit = false;

				switch (cluster_multixact_remote_xmax_resolve(
					(uint16)mx_origin, (MultiXactId)HeapTupleHeaderGetRawXmax(tuple), snapshot,
					&mx_hit)) {
				case CLUSTER_VISIBILITY_VISIBLE:
					return 1; /* no committed updater hides the row */
				case CLUSTER_VISIBILITY_INVISIBLE:
					return 0; /* the delete is visible at this snapshot */
				case CLUSTER_VISIBILITY_UNKNOWN:
				default:
					cluster_vis53r97_note_multi_unresolvable();
					return -1; /* overlay miss / member unprovable */
				}
			}
			/* derived own slot: fall through to the alias-free native
			 * update-xid decode below. */
		}
		xmax = HeapTupleGetUpdateXid(tuple);
	} else
		xmax = HeapTupleHeaderGetRawXmax(tuple);
	if (!TransactionIdIsValid(xmax))
		return 1; /* lockers-only multi: no update xid -> never a delete */

	/* PGRAC: spec-6.12i CP5 — the _scn variant threads the snapshot read_scn
	 * down so a below-horizon origin verdict is judged admissible (leg (e))
	 * inside the resolver; a returned bound decides correctly against THIS
	 * read_scn (see commit_scn_is_bound).
	 *
	 * PGRAC serve-stall round-6: resolved BEFORE the raw current-xid test
	 * (cluster_vis_evidence_route) -- a raw match can be a below-floor
	 * collision with a remote deleter's xid, and reading the remote tuple's
	 * cmax against the LOCAL command counter is meaningless. */
	cluster_visibility_resolve_tuple_scn(buffer, tuple, xmax, CLUSTER_VIS_XMAX_UPDATE,
										 snapshot->read_scn, &xr);

	switch (cluster_vis_evidence_route(xr.evidence, TransactionIdIsCurrentTransactionId(xmax))) {
	case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:
		if (xr.status == CLUSTER_TT_STATUS_COMMITTED || xr.status == CLUSTER_TT_STATUS_CLEANED_OUT)
			scn_decision = cluster_visibility_decide_by_scn(xr.commit_scn, snapshot->read_scn);

		switch (cluster_vis_cr_xmax_verdict(xr.status, scn_decision)) {
		case CVV_VISIBLE:
			cluster_vis_bump_xmax_resolved_count(); /* spec-7.1a D6 */
			return 1;
		case CVV_INVISIBLE:
			cluster_vis_bump_xmax_resolved_count(); /* spec-7.1a D6 */
			return 0;
		default:
			/* PGRAC: spec-7.1 D0 census — deleting-xmax unproven leg. */
			cluster_vis53r97_note_xmax_unprovable();
			return -1; /* unknown / unresolved -> fail closed */
		}
		break;
	case CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN:
		/* PGRAC: spec-7.1 D0 census — deleting-xmax unproven leg. */
		cluster_vis53r97_note_xmax_unprovable();
		return -1; /* recycled/ambiguous deleter evidence -> fail closed */
	case CLUSTER_VIS_ROUTE_NATIVE_SELF:
		/* Our own in-progress delete of the (remote-origin) row: native
		 * command-id semantics apply -- deleted before this scan started
		 * means invisible. */
		if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			return 1; /* deleted after scan started */
		return 0;	  /* deleted before scan started */
	case CLUSTER_VIS_ROUTE_NATIVE:
		break;
	}

	/*
	 * Own-instance deleter (post-merge local DML on a materialized row is
	 * ordinary operation).  The xid is OURS only when the slot evidence still
	 * binds it (origin == self AND slot xid == xmax): then the local
	 * ProcArray / CLOG / snapshot are authoritative with no cross-instance
	 * aliasing (AD-012 例外 9 row #1) -- mirror the PG-native xmax half.  The
	 * on-page HEAP_XMAX_COMMITTED hint is deliberately NOT trusted here: on a
	 * shared page it may have been stamped by the peer for a same-valued
	 * foreign xid.  A LOCAL slot recycled past xmax (slot xid != xmax) or no
	 * slot evidence at all leaves the deleter's origin unprovable -> fail
	 * closed (规则 8.A).
	 *
	 * PGRAC: spec-6.15 D7 — above the activation floor the striped value
	 * space alone proves ownership (the same proof behind the D4
	 * derived==self LOCAL route), so a slot recycled past our OWN xmax no
	 * longer leaves the deleter unprovable: cluster_xid_is_mine is accepted
	 * as the slot binding's value-space equivalent.
	 */
	if (xr.evidence == CLUSTER_VIS_EVIDENCE_LOCAL
		&& (xr.ref.local_xid == xmax || cluster_xid_is_mine(xmax))) {
		if (XidInMVCCSnapshot(xmax, snapshot))
			return 1; /* still in progress per this snapshot */
		if (!TransactionIdDidCommit(xmax))
			return 1; /* aborted or crashed -> never deleted */
		return 0;	  /* our committed delete, visible to this snapshot */
	}

	/* PGRAC: spec-7.1 D0 census — deleting-xmax unproven leg. */
	cluster_vis53r97_note_xmax_unprovable();
	return -1; /* origin unprovable -> fail closed */
}
#endif

/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 * See SNAPSHOT_MVCC's definition for the intended behaviour.
 *
 * Notice that here, we will not update the tuple status hint bits if the
 * inserting/deleting transaction is still running according to our snapshot,
 * even if in reality it's committed or aborted by now.  This is intentional.
 * Checking the true transaction state would require access to high-traffic
 * shared data structures, creating contention we'd rather do without, and it
 * would not change the result of our visibility check anyway.  The hint bits
 * will be updated by the first visitor that has a snapshot new enough to see
 * the inserting/deleting transaction as done.  In the meantime, the cost of
 * leaving the hint bits unset is basically that each HeapTupleSatisfiesMVCC
 * call will need to run TransactionIdIsCurrentTransactionId in addition to
 * XidInMVCCSnapshot (but it would have to do the latter anyway).  In the old
 * coding where we tried to set the hint bits as soon as possible, we instead
 * did TransactionIdIsInProgress in each call --- to no avail, as long as the
 * inserting/deleting transaction was still running --- which was more cycles
 * and more contention on ProcArrayLock.
 */
static bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC (spec-3.2 D5 + spec-3.3 D10): MVCC cluster visibility fork.
	 *
	 * Strict authoritative-exact-TT-ref-only gate (spec-3.2 §0.1 F1 +
	 * §3.3 + L176 spirit).  All early-exits fall through to the PG-
	 * native body below — NO goto label (v0.3 M1 inline early-return
	 * style).
	 *
	 * Hot path discipline (L177): cluster path is fail-fast no-wait;
	 * production hot path goes here only when ITL ref carries valid
	 * remote origin + non-zero tt_slot_id (i.e. after spec-3.4 ITL
	 * writable activation; spec-3.2 阶段 production silent invisible
	 * via PG-native body — v0.3 N1).
	 *
	 * v0.3 N4: BufferIsValid guard prevents BufferGetPage(InvalidBuffer)
	 * null-deref segfault in catalog scan / tqueue.c / heap_fetch caller
	 * contexts (L178 candidate).
	 *
	 * spec-3.3 D10 entry gates:
	 *   1. snapshot->cluster_source == LOCAL -> skip cluster path
	 *      (catalog scan / logical decoding / static snapshots). Falls
	 *      through to PG-native body; behaviour matches spec-3.2.
	 *   2. snapshot->read_epoch != current cluster epoch -> reconfig
	 *      invalidated this snapshot; raise 53R97 immediately. Caller
	 *      retries with a fresh snapshot under the new epoch.
	 *   3. case COMMITTED/CLEANED_OUT: use cluster_visibility_decide_by_scn()
	 *      with the snapshot's read_scn instead of unconditional 53R97
	 *      (spec-3.3 D10 reverses spec-3.2 §Hardening v1.0.1 D1).
	 */
	/*
	 * PGRAC (spec-3.24 D1): a no-peer + session-local cluster snapshot is judged
	 * by the PG-native MVCC body below (AD-012 例外 9 row #1: local xid + local
	 * snapshot -> ProcArray-native).  The fast path must skip the WHOLE fork,
	 * not just the CR gate: skipping only the CR gate would leave the SCN
	 * resolver to hide a post-read_scn current version without reconstructing
	 * the older one (spec-3.21 lost-update), whereas the PG-native body finds
	 * the correct older version via the heap update chain.  Defaults off
	 * (fail-closed) and yields to a forced-CR test override.
	 *
	 * spec-4.5a G6 (8.A): the fast path is additionally void per TUPLE
	 * when the tuple carries remote-writer evidence (foreign-origin ITL
	 * ref).  A single-node-form instance can read shared pages written by
	 * a peer (merged-materialized here, or simply present on the shared
	 * root); judging such a tuple PG-native would consult the LOCAL
	 * pg_xact by raw xid (cross-instance alias, AD-012 例外 9) and stamp
	 * wrong hint bits onto the shared page.  A foreign-origin tuple always
	 * takes the cluster fork; its per-origin gates fail closed (53R97)
	 * when this node holds no authority for that origin.
	 *
	 * Hot-standby reads stand DOWN the per-tuple escape: a physical
	 * standby continuously replays its primary's WHOLE state -- including
	 * pg_xact -- so the local CLOG IS authoritative for every replayed
	 * xid regardless of the ITL slot's origin stamp (t/242 L9 RL1; the
	 * AD-012 例外 9 premise does not hold on a same-xid-space replica).
	 * Merged recovery cannot overlap this: it runs only in crash
	 * recovery, where no hot-standby backends exist.  Residual: a standby
	 * OF a merged survivor would see materialized foreign tuples with no
	 * replicated authority (pg_xact_remote / markers are not WAL-logged);
	 * that combo is out of the cold-crash scope -- 4.6/4.7.
	 */
	if (cluster_enabled && BufferIsValid(buffer)
		&& snapshot->cluster_source == (uint8)SNAPSHOT_SOURCE_CLUSTER
		&& (!cluster_cr_no_peer_fastpath_eligible(snapshot)
			|| (!RecoveryInProgress() && cluster_tuple_has_remote_evidence(buffer, tuple)))) {
		TransactionId raw_xmin = HeapTupleHeaderGetRawXmin(tuple);
		ClusterUndoTTSlotRef ref = { 0 };
		bool ref_filled = false;

		/*
		 * PGRAC spec-3.9 D5: own-instance CR 3-tier short-circuit gate.
		 *
		 *   Fires only for a local-origin tuple whose block (pd_block_scn)
		 *   AND own ITL slot (write_scn) are both newer than the snapshot
		 *   read_scn — i.e. the physical row has changed after the snapshot
		 *   and must be read through a reconstructed historical block image.
		 *   The page-gate / ITL-gate tiers inside the helper keep the common
		 *   fast path free of CR construction.  When the gate does not fire
		 *   (remote tuple, block at/before snapshot, or no local undo chain)
		 *   it returns false and we continue to the existing spec-3.2/3.3
		 *   remote-xid path + PG-native body below, unchanged.
		 */
		{
			bool cr_visible;
			bool cr_eligible = true;

#ifdef ENABLE_INJECTION
			/* When a test is forcing the injected remote-xid visibility path
			 * (t/208), the CR gate stands down so the injected overlay path
			 * below is exercised as intended. */
			if (cluster_test_force_visibility_cluster_path)
				cr_eligible = false;
#endif

			if (cr_eligible) {
				/*
				 * PGRAC: spec-4.5a D8 (P0-2) -- the CR gate is tri-state.
				 * FAILCLOSED means a materialized-remote chain could not be
				 * lawfully resolved (raw-xid CLOG/ProcArray would alias
				 * across instances); falling through to the remote-xid /
				 * native paths would answer from the wrong model, so error
				 * out instead (规则 8.A).
				 */
				switch (cluster_cr_satisfies_mvcc(htup, snapshot, buffer, &cr_visible)) {
				case CLUSTER_CR_DECIDED:
					return cr_visible;
				case CLUSTER_CR_FAILCLOSED:
					/*
						 * spec-4.5a G5 complete: the per-origin authority IS
						 * consulted (D10c), so a FAILCLOSED here is a genuine
						 * IN-DOUBT outcome -- the peer stamped its durable TT
						 * slot then crashed before its commit/abort record, so
						 * the transaction is neither committed nor aborted.
						 * Fail closed (规则 8.A): never visible, never silently
						 * invisible.  Same 53R97 surface as the steady-state TT
						 * path below so callers see one fail-closed contract.
						 */
					ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
									errmsg("cluster TT status unknown for a materialized "
										   "remote-origin tuple"),
									errhint("The peer's transaction was materialized by merged "
											"recovery but its commit outcome is in-doubt (stamped "
											"then crashed before commit/abort); retry after the "
											"origin completes recovery, or abort.")));
					break; /* unreachable */
				case CLUSTER_CR_NOT_APPLICABLE:
					break; /* continue to the remote-xid / native paths */
				}
			}
		}

		/*
		 * spec-3.3 D10 (Q8 / R6): reconfig epoch fence. A snapshot
		 * captured under epoch N becomes invalid the moment a peer
		 * triggers reconfig and epoch advances. Compare via uint64 -- no
		 * (uint32) cast (R9 P2).
		 */
		if (snapshot->read_epoch != cluster_epoch_get_current())
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster snapshot stale across reconfig"),
							errhint("snapshot.read_epoch=" UINT64_FORMAT
									" current epoch=" UINT64_FORMAT "; retry transaction.",
									snapshot->read_epoch, cluster_epoch_get_current())));

#ifdef ENABLE_INJECTION
		/* spec-3.2 D5b: test-only inject hook overrides placeholder
		 * reader when cluster_test_force_visibility_cluster_path = on. */
		if (cluster_test_force_visibility_cluster_path)
			ref_filled = cluster_test_lookup_visibility_inject(raw_xmin, &ref);
#endif

		/*
		 * spec-3.14 D1:  xmin status resolution extracted to the shared
		 * cluster_visibility_resolve_* layer (L212).  Behaviour-equivalent
		 * to the prior inline body on the happy path; the explicit
		 * local_xid == raw_xid check now fail-closes a recycled slot at the
		 * ref layer (STALE_OR_AMBIGUOUS -> 53R97) instead of after a futile
		 * overlay miss.  The SCN decision + lazy cleanout (MVCC policy) stay
		 * here; only the evidence/status resolution moved.
		 */
		{
			ClusterVisResolve res;

			/* PGRAC: spec-6.12i CP5 — _scn variants: the snapshot read_scn
			 * reaches the active-runtime resolver (below-horizon verdict
			 * admissibility, leg (e)). */
			if (ref_filled)
				cluster_visibility_resolve_from_ref_scn(raw_xmin, &ref,
														PageGetLSN(BufferGetPage(buffer)),
														snapshot->read_scn, &res);
			else
				cluster_visibility_resolve_tuple_scn(buffer, tuple, raw_xmin, CLUSTER_VIS_XMIN,
													 snapshot->read_scn, &res);

			if (res.evidence == CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS)
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
								errmsg("cluster TT slot recycled for xid %u", raw_xmin),
								errhint("ITL slot no longer maps to this xid (slot reused); "
										"retry the transaction with a fresh snapshot.")));

			if (res.evidence == CLUSTER_VIS_EVIDENCE_REMOTE) {
				switch (res.status) {
				case CLUSTER_TT_STATUS_ABORTED:
					return false;
				case CLUSTER_TT_STATUS_IN_PROGRESS:
				case CLUSTER_TT_STATUS_SUBCOMMITTED:
					/* Remote in-progress (or parent still in-progress after
					 * lazy follow) -- snapshot can't see it. */
					return false;
				case CLUSTER_TT_STATUS_COMMITTED:
				case CLUSTER_TT_STATUS_CLEANED_OUT: {
					ClusterVisibilityDecision decision;

					decision = cluster_visibility_decide_by_scn(res.commit_scn, snapshot->read_scn);
					switch (decision) {
					case CLUSTER_VISIBILITY_VISIBLE: {
						/*
						 * spec-3.4c D4 + F1: opportunistic lazy cleanout
						 * (hint-style, no WAL; L177 no-wait).  Stamp the page
						 * slot commit_scn when the overlay says COMMITTED but
						 * the on-page slot is still ACTIVE.
						 */
						Page page = BufferGetPage(buffer);

						/* PGRAC: spec-6.12i CP5 — a horizon-BOUND scn (below-
						 * horizon verdict) must never be stamped as an exact
						 * commit_scn: a later reader with a smaller read_scn
						 * would decide committed-after off the stamp ->
						 * false-invisible (Rule 8.A).  Bound resolves skip
						 * the cleanout; the verdict itself already decided
						 * this read. */
						if (!res.commit_scn_is_bound && PageHasItl(page)
							&& tuple->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
							&& tuple->t_itl_slot_idx < CLUSTER_ITL_INITRANS_DEFAULT) {
							ClusterItlSlotData *slot
								= &ClusterPageGetItlSlots(page)[tuple->t_itl_slot_idx];

							if (slot->flags == ITL_FLAG_ACTIVE)
								(void)cluster_itl_cleanout_lazy(buffer, tuple->t_itl_slot_idx,
																raw_xmin, res.commit_scn);
						}

						/*
						 * spec-4.5a G6: xmin visible, but a remote tuple's
						 * delete (xmax) side has not been checked -- the
						 * cluster path returns here and never falls through
						 * to the native xmax test, so resolve a committed
						 * remote deleter before answering visible.
						 */
						switch (cluster_remote_live_xmax_keeps_visible(buffer, tuple, snapshot)) {
						case 1:
							return true;
						case 0:
							return false;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
									 errmsg("cluster TT status unknown for deleting xmax of xid %u",
											raw_xmin),
									 errhint("Remote deleter commit state not yet propagated; "
											 "retry or abort.")));
						}
						return true; /* unreachable */
					}
					case CLUSTER_VISIBILITY_INVISIBLE:
						return false;
					case CLUSTER_VISIBILITY_UNKNOWN:
						break; /* fall through to 53R97 */
					}
					break;
				}
				default:
					break;
				}

				/*
				 * PGRAC: spec-7.1 D1 requester 半边 (IN-9).  An overlay miss
				 * means the V4 hint wire has not ARRIVED, not that raw_xmin
				 * has no terminal state -- the origin's durable TT is
				 * authoritative (Fast Commit stamps it pre-commit).  Before
				 * failing closed, ask the origin for a complete by-xid
				 * verdict.  This is the SAME buffer-content-lock context and
				 * verdict wire as the shipped recycled-slot ask (6.12i
				 * try_resolve_remote called from classify_ref): res came back
				 * from cluster_visibility_resolve_tuple_scn above, so its
				 * internal depth guard already balanced -- no new lock is
				 * held across this wire (Impl note IN-9 锁上下文).  positive
				 * proof only: any refuse / UNKNOWN / covers-miss falls through
				 * to the 53R97 below (Rule 8.A; direction unchanged).  The
				 * covers SCN-domain gate (spec-7.1a) is applied inside
				 * try_resolve_remote against the snapshot read_scn, so an
				 * origin authority that does not provably cover read_scn still
				 * fails closed.
				 */
				if (cluster_crossnode_runtime_visibility && res.ref.undo_segment_id != 0) {
					int d1_origin = cluster_xid_origin_slot(raw_xmin);
					bool d1_committed = false;
					bool d1_is_bound = false;
					SCN d1_scn = InvalidScn;

					if (d1_origin >= 0 && d1_origin != cluster_node_id) {
						cluster_vis53r97_note_xmin_overlay_verdict_ask();
						if (cluster_runtime_visibility_try_resolve_remote(
								d1_origin, (uint32)res.ref.undo_segment_id, raw_xmin,
								snapshot->read_scn,
								false /* derived origin: keep the stripe self-check */,
								&d1_committed, &d1_scn, &d1_is_bound)) {
							if (!d1_committed) {
								/* origin proved ABORTED -> this xmin was never
								 * visible to any snapshot. */
								cluster_vis53r97_note_xmin_overlay_verdict_hit();
								return false;
							}
							switch (cluster_visibility_decide_by_scn(d1_scn, snapshot->read_scn)) {
							case CLUSTER_VISIBILITY_VISIBLE:
								cluster_vis53r97_note_xmin_overlay_verdict_hit();
								/* xmin visible via verdict; the remote deleter
								 * (xmax) side still must be checked before
								 * answering visible (spec-4.5a G6, mirrors the
								 * COMMITTED-overlay arm above). */
								switch (cluster_remote_live_xmax_keeps_visible(buffer, tuple,
																			   snapshot)) {
								case 1:
									return true;
								case 0:
									return false;
								default:
									break; /* xmax unprovable -> fall to 53R97 */
								}
								break;
							case CLUSTER_VISIBILITY_INVISIBLE:
								cluster_vis53r97_note_xmin_overlay_verdict_hit();
								return false;
							case CLUSTER_VISIBILITY_UNKNOWN:
								break; /* undecidable at this read_scn -> 53R97 */
							}
						}
					}
				}

				/*
				 * HC181 fail-closed.  L177 NO WAIT.  Reaches here on overlay
				 * lookup miss (with the D1 verdict ask above also unable to
				 * prove a terminal state), COMMITTED with UNKNOWN SCN
				 * decision, or an unknown status enum value.  evidence is
				 * REMOTE so there is NO PG-native fallback (C-V2 / L199).
				 * This leg's residual is attributable as
				 * (xmin_overlay_verdict_ask - hit): every ask that did not
				 * prove a terminal state fell through to here (D5 / census).
				 */
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
								errmsg("cluster TT status unknown for xid %u", raw_xmin),
								errhint("Remote commit_scn not yet propagated, or TT "
										"overlay missed/stale; retry or abort.")));
			}
		}
		/* else: ref placeholder / local origin / no ITL slot ->
		 * fall through to PG-native body (v0.3 N1 silent invisible
		 * for cross-node tuple in production). */

		/*
		 * PGRAC spec-3.6 v0.3 D6:  MultiXact xmax reader branch.
		 *
		 *   Scope:  HeapTupleSatisfiesMVCC() only (per OBS-1/F5).  Other
		 *   HeapTupleSatisfies* paths remain PG-native.
		 *
		 *   Path (spec-7.1 D3-a positive resolution):
		 *     1. tuple xmax has HEAP_XMAX_IS_MULTI flag (updater-
		 *        bearing only: lock-only multis never cut visibility
		 *        and an XMAX_INVALID-hinted multi is never decoded --
		 *        both stay readable natively)
		 *     2. derive the origin slot from the striped mxid VALUE
		 *        (cluster_mxid_origin_slot, half-space window above
		 *        the durable activation floor) -- NEVER from the D7b
		 *        page marker, which stamps the READER's own id and
		 *        would call every multi "local" (the probe-F silent
		 *        false-visible this branch's floor was shipped for)
		 *     3. underivable (below floor / unlatched / beyond the
		 *        half-space / cluster.multi_xmax_remote_resolve off)
		 *        -> 53R9C fail-closed floor, direction unchanged
		 *     4. derived foreign -> cluster member overlay lookup +
		 *        OBS-1 resolve; overlay miss / UNKNOWN -> 53R9C (per
		 *        L199 NO PG-native fallback; the member-serve wire
		 *        that would answer a miss positively is a later
		 *        deliverable of this spec)
		 *     5. derived OWN slot -> fall through to PG-native: above
		 *        the floor only this node ever issues its congruence
		 *        class, so the local SLRU member decode is provably
		 *        alias-free
		 *
		 *   IN_PROGRESS authoritative state in resolve_visibility ->
		 *   VISIBLE (per OBS-1 truth table:  uncommitted Update/
		 *   NoKeyUpdate not yet hides tuple).  UNKNOWN -> 53R9C.
		 *   Spec: spec-7.1-cross-instance-positive-interread.md (D3-a)
		 */
		if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) != 0) {
			TransactionId raw_xmax_multi = HeapTupleHeaderGetRawXmax(tuple);

			if (MultiXactIdIsValid((MultiXactId)raw_xmax_multi)
				&& !(tuple->t_infomask & HEAP_XMAX_INVALID)
				&& !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) && cluster_peer_mode_enabled()) {
				int mx_origin = -1;

				if (cluster_multi_xmax_remote_resolve)
					mx_origin = cluster_mxid_origin_slot((MultiXactId)raw_xmax_multi);

				if (mx_origin < 0) {
					/* D3-0 floor: origin not provable -> fail closed */
					cluster_multixact_note_underivable_read();
					ereport(ERROR,
							(errcode(ERRCODE_CLUSTER_MULTIXACT_MEMBER_OVERLAY_MISS),
							 errmsg("cluster multixact %u cannot be attributed to an origin node",
									(unsigned)raw_xmax_multi),
							 errhint("Multixact ids are node-local and this tuple's deleting "
									 "multixact cannot be proven local; cross-node multixact "
									 "resolution needs mxid striping and "
									 "cluster.multi_xmax_remote_resolve. Retry the transaction.")));
				}
				if (mx_origin != cluster_node_id) {
					bool mx_hit = false;

					switch (cluster_multixact_remote_xmax_resolve(
						(uint16)mx_origin, (MultiXactId)raw_xmax_multi, snapshot, &mx_hit)) {
					case CLUSTER_VISIBILITY_VISIBLE:
						return true;
					case CLUSTER_VISIBILITY_INVISIBLE:
						return false;
					case CLUSTER_VISIBILITY_UNKNOWN:
					default:
						if (!mx_hit)
							ereport(ERROR, (errcode(ERRCODE_CLUSTER_MULTIXACT_MEMBER_OVERLAY_MISS),
											errmsg("cluster multixact member overlay miss for "
												   "remote multixact id %u from node %d",
												   (unsigned)raw_xmax_multi, mx_origin),
											errhint("Remote multixact member overlay is not "
													"available;  retry the transaction after "
													"the origin emits a fresh overlay.")));
						ereport(ERROR, (errcode(ERRCODE_CLUSTER_MULTIXACT_MEMBER_OVERLAY_MISS),
										errmsg("cluster multixact resolve visibility UNKNOWN "
											   "for multixact id %u from node %d",
											   (unsigned)raw_xmax_multi, mx_origin),
										errhint("Member commit_scn not yet propagated;  retry "
												"transaction.")));
					}
				}
				/* mx_origin == cluster_node_id: provably OUR multixact
				 * above the activation floor -> the PG-native member
				 * decode below is alias-free. */
			}
			/* lock-only / XMAX_INVALID / invalid mid / non-peer ->
			 * fall through to PG-native MultiXact resolution below. */
		}
	}

	/*
	 * PGRAC (spec-6.14 D8): under cluster.shared_catalog the LOCAL-source
	 * escape above is only lawful for tuples with no foreign-writer
	 * evidence.  Catalog pages live in the single shared tree, so a LOCAL
	 * MVCC snapshot -- the catalog snapshot before the services-ready gate
	 * opens (boot / shutdown / recovery windows) -- can meet a tuple whose
	 * xmin/xmax belongs to another node.  Judging it by the PG-native body
	 * below would consult the LOCAL pg_xact / ProcArray by raw xid (a
	 * cross-instance alias, AD-012 例外 9) and stamp wrong hint bits onto
	 * the shared page.  There is no lawful verdict under LOCAL semantics
	 * (a foreign xid is incomparable with a local snapshot), so fail closed
	 * (53R97, retryable) -- strictly safer than any convergence guess.
	 * shared_catalog=off keeps this branch dead: catalog pages then never
	 * carry foreign tuples, and user-table LOCAL windows (logical decoding
	 * historic reads) keep their shipped spec-3.2 N1 posture.
	 */
	if (cluster_shared_catalog && cluster_enabled && BufferIsValid(buffer)
		&& snapshot->cluster_source == (uint8)SNAPSHOT_SOURCE_LOCAL
		&& cluster_tuple_has_remote_evidence(buffer, tuple)) {
		/* spec-6.14 D10b: count the fail-closed outcome (dump key
		 * catalog.vis_unknown_count) before raising. */
		cluster_catalog_stats_vis_unknown_inc();
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						errmsg("foreign-origin tuple cannot be judged under a LOCAL snapshot "
							   "with cluster.shared_catalog enabled"),
						errhint("Catalog services on this node are not ready yet (starting up, "
								"shutting down, or in recovery); retry once the cluster reaches "
								"the RUNNING phase.")));
	}
#endif /* USE_PGRAC_CLUSTER */

	if (!HeapTupleHeaderXminCommitted(tuple)) {
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/* Used by pre-9.0 binary upgrades */
		if (tuple->t_infomask & HEAP_MOVED_OFF) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return false;
			if (!XidInMVCCSnapshot(xvac, snapshot)) {
				if (TransactionIdDidCommit(xvac)) {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
			}
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (!TransactionIdIsCurrentTransactionId(xvac)) {
				if (XidInMVCCSnapshot(xvac, snapshot))
					return false;
				if (TransactionIdDidCommit(xvac))
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
				else {
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
			}
		} else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false; /* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) /* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true; /* updated after scan started */
				else
					return false; /* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true; /* deleted after scan started */
			else
				return false; /* deleted before scan started */
		} else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
		else {
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			return false;
		}
	} else {
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple)
			&& XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false; /* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	/* PGRAC: spec-6.15 D7 — a provably-foreign deleter's INVALID hint may be
	 * a poison stamp from the native no-authority era (see the mixed-tuple
	 * banner); never trust it: route to the cluster resolver below. */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
		&& !cluster_plain_xmax_provably_foreign(tuple))
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax)) {
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true; /* deleted after scan started */
			else
				return false; /* deleted before scan started */
		}
		if (XidInMVCCSnapshot(xmax, snapshot))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false; /* updating transaction committed */
		/* it must have aborted or crashed */
		return true;
	}

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: spec-6.15 D7 — mixed tuple: the local-class xmin fell through
	 * to this native body, but the deleter is provably another node's xid.
	 * Native CLOG / snapshot answers (and both on-page hint bits) are void
	 * for it — trusting them false-aborts a committed foreign deleter, so
	 * the superseded version stays visible next to its successor.  Route
	 * the whole xmax decision to the cluster resolver (same G6 gate the
	 * remote-xmin arm uses); unprovable stays fail-closed 53R97. */
	if (cluster_plain_xmax_provably_foreign(tuple)) {
		switch (cluster_remote_live_xmax_keeps_visible(buffer, tuple, snapshot)) {
		case 1:
			return true;
		case 0:
			return false;
		default:
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
							errmsg("cluster TT status unknown for deleting xmax of xid %u",
								   HeapTupleHeaderGetRawXmax(tuple)),
							errhint("Remote deleter commit state not yet propagated; "
									"retry or abort.")));
		}
	}
#endif

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple))) {
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true; /* deleted after scan started */
			else
				return false; /* deleted before scan started */
		}

		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple))) {
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
	} else {
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true; /* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}


/*
 * HeapTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from
 * GetOldestNonRemovableTransactionId()).  Tuples deleted by XIDs >=
 * OldestXmin are deemed "recently dead"; they might still be visible to some
 * open transaction, so we can't remove them, even if we see that the deleting
 * transaction has committed.
 */
HTSV_Result
HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin, Buffer buffer)
{
	TransactionId dead_after = InvalidTransactionId;
	HTSV_Result res;

	res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &dead_after);

	if (res == HEAPTUPLE_RECENTLY_DEAD) {
		Assert(TransactionIdIsValid(dead_after));

		if (TransactionIdPrecedes(dead_after, OldestXmin))
			res = HEAPTUPLE_DEAD;
	} else
		Assert(!TransactionIdIsValid(dead_after));

	return res;
}

/*
 * Work horse for HeapTupleSatisfiesVacuum and similar routines.
 *
 * In contrast to HeapTupleSatisfiesVacuum this routine, when encountering a
 * tuple that could still be visible to some backend, stores the xid that
 * needs to be compared with the horizon in *dead_after, and returns
 * HEAPTUPLE_RECENTLY_DEAD. The caller then can perform the comparison with
 * the horizon.  This is e.g. useful when comparing with different horizons.
 *
 * Note: HEAPTUPLE_DEAD can still be returned here, e.g. if the inserting
 * transaction aborted.
 */
HTSV_Result
HeapTupleSatisfiesVacuumHorizon(HeapTuple htup, Buffer buffer, TransactionId *dead_after)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);
	Assert(dead_after != NULL);

	*dead_after = InvalidTransactionId;

#ifdef USE_PGRAC_CLUSTER
	/*
	 * P0-28 / spec-3.14 §10: a peer may retain this exact TID across a
	 * PCM-X wait.  ITL evidence is not a durable prune-horizon token: its slot
	 * may be reused before that peer reacquires the page, after which native
	 * xmin/CLOG checks can misclassify and recycle the target line pointer.
	 * Keep every shared-storage tuple until a real cluster horizon exists.
	 */
	if (cluster_vis_prune_must_defer(cluster_storage_mode_enabled()
									  && BufferIsValid(buffer) && !BufferIsLocal(buffer),
								  false)) {
		cluster_vis_bump_prune_remote_keep_count();
		return HEAPTUPLE_LIVE;
	}
#endif

	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!HeapTupleHeaderXminCommitted(tuple)) {
		if (HeapTupleHeaderXminInvalid(tuple))
			return HEAPTUPLE_DEAD;
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_OFF) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			if (TransactionIdDidCommit(xvac)) {
				SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
				return HEAPTUPLE_DEAD;
			}
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
		}
		/* Used by pre-9.0 binary upgrades */
		else if (tuple->t_infomask & HEAP_MOVED_IN) {
			TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

			if (TransactionIdIsCurrentTransactionId(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (TransactionIdIsInProgress(xvac))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			if (TransactionIdDidCommit(xvac))
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
			else {
				SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
				return HEAPTUPLE_DEAD;
			}
		} else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple))) {
			if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* only locked? run infomask-only check first, for performance */
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) || HeapTupleHeaderIsOnlyLocked(tuple))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* inserted and then deleted by same xact */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			/* deleting subtransaction must have aborted */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		} else if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple))) {
			/*
			 * It'd be possible to discern between INSERT/DELETE in progress
			 * here by looking at xmax - but that doesn't seem beneficial for
			 * the majority of callers and even detrimental for some. We'd
			 * rather have callers look at/wait for xmin than xmax. It's
			 * always correct to return INSERT_IN_PROGRESS because that's
			 * what's happening from the view of other backends.
			 */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		} else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
		else {
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			return HEAPTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.  Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)) {
		/*
		 * "Deleting" xact really only locked it, so the tuple is live in any
		 * case.  However, we should make sure that either XMAX_COMMITTED or
		 * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
		 * examining the tuple for future xacts.
		 */
		if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
				/*
				 * If it's a pre-pg_upgrade tuple, the multixact cannot
				 * possibly be running; otherwise have to check.
				 */
				if (!HEAP_LOCKED_UPGRADED(tuple->t_infomask)
					&& MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
					return HEAPTUPLE_LIVE;
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			} else {
				if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
					return HEAPTUPLE_LIVE;
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			}
		}

		/*
		 * We don't really care whether xmax did commit, abort or crash. We
		 * know that xmax did lock the tuple, but it did not and will never
		 * actually update it.
		 */

		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		TransactionId xmax = HeapTupleGetUpdateXid(tuple);

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsInProgress(xmax))
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (TransactionIdDidCommit(xmax)) {
			/*
			 * The multixact might still be running due to lockers.  Need to
			 * allow for pruning if below the xid horizon regardless --
			 * otherwise we could end up with a tuple where the updater has to
			 * be removed due to the horizon, but is not pruned away.  It's
			 * not a problem to prune that tuple, because any remaining
			 * lockers will also be present in newer tuple versions.
			 */
			*dead_after = xmax;
			return HEAPTUPLE_RECENTLY_DEAD;
		} else if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false)) {
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed.
			 * Mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		}

		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
		if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
		else {
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			return HEAPTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Deleter committed, allow caller to check if it was recent enough that
	 * some open transactions could still see the tuple.
	 */
	*dead_after = HeapTupleHeaderGetRawXmax(tuple);
	return HEAPTUPLE_RECENTLY_DEAD;
}


/*
 * HeapTupleSatisfiesNonVacuumable
 *
 *	True if tuple might be visible to some transaction; false if it's
 *	surely dead to everyone, ie, vacuumable.
 *
 *	See SNAPSHOT_NON_VACUUMABLE's definition for the intended behaviour.
 *
 *	This is an interface to HeapTupleSatisfiesVacuum that's callable via
 *	HeapTupleSatisfiesSnapshot, so it can be used through a Snapshot.
 *	snapshot->vistest must have been set up with the horizon to use.
 */
static bool
HeapTupleSatisfiesNonVacuumable(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	TransactionId dead_after = InvalidTransactionId;
	HTSV_Result res;

#ifdef USE_PGRAC_CLUSTER
	/* spec-3.14 D5: remote writer evidence -> not removable (keep). */
	if (cluster_storage_mode_enabled() && cluster_tuple_has_remote_evidence(buffer, htup->t_data)) {
		cluster_vis_bump_prune_remote_keep_count();
		return true;
	}
#endif

	res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &dead_after);

	if (res == HEAPTUPLE_RECENTLY_DEAD) {
		Assert(TransactionIdIsValid(dead_after));

		if (GlobalVisTestIsRemovableXid(snapshot->vistest, dead_after))
			res = HEAPTUPLE_DEAD;
	} else
		Assert(!TransactionIdIsValid(dead_after));

	return res != HEAPTUPLE_DEAD;
}


/*
 * HeapTupleIsSurelyDead
 *
 *	Cheaply determine whether a tuple is surely dead to all onlookers.
 *	We sometimes use this in lieu of HeapTupleSatisfiesVacuum when the
 *	tuple has just been tested by another visibility routine (usually
 *	HeapTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *	should already be set.  We assume that if no hint bits are set, the xmin
 *	or xmax transaction is still running.  This is therefore faster than
 *	HeapTupleSatisfiesVacuum, because we consult neither procarray nor CLOG.
 *	It's okay to return false when in doubt, but we must return true only
 *	if the tuple is removable.
 */
bool
HeapTupleIsSurelyDead(HeapTuple htup, GlobalVisState *vistest)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/*
	 * If the inserting transaction is marked invalid, then it aborted, and
	 * the tuple is definitely dead.  If it's marked neither committed nor
	 * invalid, then we assume it's still alive (since the presumption is that
	 * all relevant hint bits were just set moments ago).
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
		return HeapTupleHeaderXminInvalid(tuple);

	/*
	 * If the inserting transaction committed, but any deleting transaction
	 * aborted, the tuple is still alive.
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return false;

	/*
	 * If the XMAX is just a lock, the tuple is still alive.
	 */
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return false;

	/*
	 * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
	 * know without checking pg_multixact.
	 */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		return false;

	/* If deleter isn't known to have committed, assume it's still running. */
	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		return false;

	/* Deleter committed, so tuple is dead if the XID is old enough. */
	return GlobalVisTestIsRemovableXid(vistest, HeapTupleHeaderGetRawXmax(tuple));
}

/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same visibility rules laid out
 * at the top of this file.
 */
bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)
{
	TransactionId xmax;

	/* if there's no valid Xmax, then there's obviously no update either */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;

	if (tuple->t_infomask & HEAP_XMAX_LOCK_ONLY)
		return true;

	/* invalid xmax means no update */
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	/*
	 * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
	 * necessarily have been updated
	 */
	if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI))
		return false;

	/* ... but if it's a multi, then perhaps the updating Xid aborted. */
	xmax = HeapTupleGetUpdateXid(tuple);

	/* not LOCKED_ONLY, so it has to have an xmax */
	Assert(TransactionIdIsValid(xmax));

	if (TransactionIdIsCurrentTransactionId(xmax))
		return false;
	if (TransactionIdIsInProgress(xmax))
		return false;
	if (TransactionIdDidCommit(xmax))
		return false;

	/*
	 * not current, not in progress, not committed -- must have aborted or
	 * crashed
	 */
	return true;
}

/*
 * check whether the transaction id 'xid' is in the pre-sorted array 'xip'.
 */
static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
	return num > 0 && bsearch(&xid, xip, num, sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * See the comments for HeapTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't need to support HEAP_MOVED_(IN|OFF) for now because we only support
 * reading catalog pages which couldn't have been created in an older version.
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
static bool
HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef USE_PGRAC_CLUSTER
	/* spec-3.14 D6: logical decoding of cluster-modified tuples is not
	 * supported until Stage 6/#95 (cross-node CID mapping).  Local tuples
	 * are unaffected. */
	if (cluster_storage_mode_enabled() && cluster_tuple_has_remote_evidence(buffer, tuple))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("logical decoding of cluster-modified tables is not supported"),
						errhint("Cross-node historic visibility lands at Stage 6/#95.")));
#endif

	/* inserting transaction aborted */
	if (HeapTupleHeaderXminInvalid(tuple)) {
		Assert(!TransactionIdDidCommit(xmin));
		return false;
	}
	/* check if it's one of our txids, toplevel is also in there */
	else if (TransactionIdInArray(xmin, snapshot->subxip, snapshot->subxcnt)) {
		bool resolved;
		CommandId cmin = HeapTupleHeaderGetRawCommandId(tuple);
		CommandId cmax = InvalidCommandId;

		/*
		 * another transaction might have (tried to) delete this tuple or
		 * cmin/cmax was stored in a combo CID. So we need to lookup the
		 * actual values externally.
		 */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot, htup,
												 buffer, &cmin, &cmax);

		/*
		 * If we haven't resolved the combo CID to cmin/cmax, that means we
		 * have not decoded the combo CID yet. That means the cmin is
		 * definitely in the future, and we're not supposed to see the tuple
		 * yet.
		 *
		 * XXX This only applies to decoding of in-progress transactions. In
		 * regular logical decoding we only execute this code at commit time,
		 * at which point we should have seen all relevant combo CIDs. So
		 * ideally, we should error out in this case but in practice, this
		 * won't happen. If we are too worried about this then we can add an
		 * elog inside ResolveCminCmaxDuringDecoding.
		 *
		 * XXX For the streaming case, we can track the largest combo CID
		 * assigned, and error out based on this (when unable to resolve combo
		 * CID below that observed maximum value).
		 */
		if (!resolved)
			return false;

		Assert(cmin != InvalidCommandId);

		if (cmin >= snapshot->curcid)
			return false; /* inserted after scan started */
						  /* fall through */
	}
	/* committed before our xmin horizon. Do a normal visibility check. */
	else if (TransactionIdPrecedes(xmin, snapshot->xmin)) {
		Assert(!(HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin)));

		/* check for hint bit first, consult clog afterwards */
		if (!HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin))
			return false;
		/* fall through */
	}
	/* beyond our xmax horizon, i.e. invisible */
	else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax)) {
		return false;
	}
	/* check if it's a committed transaction in [xmin, xmax) */
	else if (TransactionIdInArray(xmin, snapshot->xip, snapshot->xcnt)) {
		/* fall through */
	}

	/*
	 * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
	 * invisible.
	 */
	else {
		return false;
	}

	/* at this point we know xmin is visible, go on to check xmax */

	/* xid invalid or aborted */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;
	/* locked tuples are always visible */
	else if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	/*
	 * We can see multis here if we're looking at user tables or if somebody
	 * SELECT ... FOR SHARE/UPDATE a system table.
	 */
	else if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
		xmax = HeapTupleGetUpdateXid(tuple);
	}

	/* check if it's one of our txids, toplevel is also in there */
	if (TransactionIdInArray(xmax, snapshot->subxip, snapshot->subxcnt)) {
		bool resolved;
		CommandId cmin;
		CommandId cmax = HeapTupleHeaderGetRawCommandId(tuple);

		/* Lookup actual cmin/cmax values */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot, htup,
												 buffer, &cmin, &cmax);

		/*
		 * If we haven't resolved the combo CID to cmin/cmax, that means we
		 * have not decoded the combo CID yet. That means the cmax is
		 * definitely in the future, and we're still supposed to see the
		 * tuple.
		 *
		 * XXX This only applies to decoding of in-progress transactions. In
		 * regular logical decoding we only execute this code at commit time,
		 * at which point we should have seen all relevant combo CIDs. So
		 * ideally, we should error out in this case but in practice, this
		 * won't happen. If we are too worried about this then we can add an
		 * elog inside ResolveCminCmaxDuringDecoding.
		 *
		 * XXX For the streaming case, we can track the largest combo CID
		 * assigned, and error out based on this (when unable to resolve combo
		 * CID below that observed maximum value).
		 */
		if (!resolved || cmax == InvalidCommandId)
			return true;

		if (cmax >= snapshot->curcid)
			return true; /* deleted after scan started */
		else
			return false; /* deleted before scan started */
	}
	/* below xmin horizon, normal transaction state is valid */
	else if (TransactionIdPrecedes(xmax, snapshot->xmin)) {
		Assert(!(tuple->t_infomask & HEAP_XMAX_COMMITTED && !TransactionIdDidCommit(xmax)));

		/* check hint bit first */
		if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
			return false;

		/* check clog */
		return !TransactionIdDidCommit(xmax);
	}
	/* above xmax horizon, we cannot possibly see the deleting transaction */
	else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
		return true;
	/* xmax is between [xmin, xmax), check known committed array */
	else if (TransactionIdInArray(xmax, snapshot->xip, snapshot->xcnt))
		return false;
	/* xmax is between [xmin, xmax), but known not to have committed yet */
	else
		return true;
}

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid, and buffer at least share locked.
 *
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
bool
HeapTupleSatisfiesVisibility(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	switch (snapshot->snapshot_type) {
	case SNAPSHOT_MVCC:
		return HeapTupleSatisfiesMVCC(htup, snapshot, buffer);
	case SNAPSHOT_SELF:
		return HeapTupleSatisfiesSelf(htup, snapshot, buffer);
	case SNAPSHOT_ANY:
		return HeapTupleSatisfiesAny(htup, snapshot, buffer);
	case SNAPSHOT_TOAST:
		return HeapTupleSatisfiesToast(htup, snapshot, buffer);
	case SNAPSHOT_DIRTY:
		return HeapTupleSatisfiesDirty(htup, snapshot, buffer);
	case SNAPSHOT_HISTORIC_MVCC:
		return HeapTupleSatisfiesHistoricMVCC(htup, snapshot, buffer);
	case SNAPSHOT_NON_VACUUMABLE:
		return HeapTupleSatisfiesNonVacuumable(htup, snapshot, buffer);
	}

	return false; /* keep compiler quiet */
}
