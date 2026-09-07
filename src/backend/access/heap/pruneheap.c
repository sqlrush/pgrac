/*-------------------------------------------------------------------------
 *
 * pruneheap.c
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/pruneheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_undo_verdict.h"
#include "cluster/cluster_xid_stripe.h"
#include "storage/buf_internals.h"
#endif

/* Working data for heap_page_prune and subroutines */
typedef struct
{
	Relation	rel;

	/* State used to test tuple visibility; Initialized for the relation */
	TransactionId oldest_xmin;
	GlobalVisState *vistest;

	/*
	 * Thresholds set by TransactionIdLimitedForOldSnapshots() if they have
	 * been computed (done on demand, and only if
	 * OldSnapshotThresholdActive()). The first time a tuple is about to be
	 * removed based on the limited horizon, old_snap_used is set to true, and
	 * SetOldSnapshotThresholdTimestamp() is called. See
	 * heap_prune_satisfies_vacuum().
	 */
	TimestampTz old_snap_ts;
	TransactionId old_snap_xmin;
	bool		old_snap_used;

	TransactionId new_prune_xid;	/* new prune hint value for page */
	TransactionId snapshotConflictHorizon;	/* latest xid removed */
	const bool *proved_dead;		/* stack-local shared HOT proof, or NULL */
	int			nredirected;	/* numbers of entries in arrays below */
	int			ndead;
	int			nunused;
	/* arrays that accumulate indexes of items to be changed */
	OffsetNumber redirected[MaxHeapTuplesPerPage * 2];
	OffsetNumber nowdead[MaxHeapTuplesPerPage];
	OffsetNumber nowunused[MaxHeapTuplesPerPage];

	/*
	 * marked[i] is true if item i is entered in one of the above arrays.
	 *
	 * This needs to be MaxHeapTuplesPerPage + 1 long as FirstOffsetNumber is
	 * 1. Otherwise every access would need to subtract 1.
	 */
	bool		marked[MaxHeapTuplesPerPage + 1];

	/*
	 * Tuple visibility is only computed once for each tuple, for correctness
	 * and efficiency reasons; see comment in heap_page_prune() for details.
	 * This is of type int8[], instead of HTSV_Result[], so we can use -1 to
	 * indicate no visibility has been computed, e.g. for LP_DEAD items.
	 *
	 * Same indexing as ->marked.
	 */
	int8		htsv[MaxHeapTuplesPerPage + 1];
} PruneState;

/* Local functions */
static HTSV_Result heap_prune_satisfies_vacuum(PruneState *prstate,
											   HeapTuple tup,
											   Buffer buffer);
static int	heap_prune_chain(Buffer buffer,
							 OffsetNumber rootoffnum,
							 PruneState *prstate);
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect(PruneState *prstate,
									   OffsetNumber offnum, OffsetNumber rdoffnum);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum);
static void page_verify_redirects(Page page);
static int heap_page_prune_internal(Relation relation, Buffer buffer,
								   TransactionId oldest_xmin, GlobalVisState *vistest,
								   TransactionId old_snap_xmin, TimestampTz old_snap_ts,
								   int *nnewlpdead, OffsetNumber *off_loc,
								   const bool *proved_dead);

#ifdef USE_PGRAC_CLUSTER
static bool
heap_page_has_current_mx_predecessor(Page page)
{
	OffsetNumber offnum;
	OffsetNumber maxoff;

	if (!cluster_peer_mode_enabled())
		return false;
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId itemid = PageGetItemId(page, offnum);
		HeapTupleHeader tuple;

		if (!ItemIdIsNormal(itemid))
			continue;
		tuple = (HeapTupleHeader) PageGetItem(page, itemid);
		if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) != 0
			&& !cluster_ctrc_native_current_mx_mutation_allowed(
				true,
				cluster_mxid_origin_slot(
					(MultiXactId) HeapTupleHeaderGetRawXmax(tuple))))
			return true;
	}
	return false;
}

/* Shared HOT reclamation uses the same required-member retention fold as
 * undo recycling. A local xmin, elapsed time or an absent peer is not proof. */
static bool
cluster_heap_prune_floor(uint64 epoch, ClusterUndoHorizonFloor *floor)
{
	ClusterUndoHorizonReportView views[CLUSTER_MAX_NODES];
	uint8 required[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	ClusterUndoHorizonStallReason reason;
	int32 blame;
	uint64 sampled_epoch;
	SCN local_floor = cluster_undo_retention_horizon();
	int nviews = cluster_undo_horizon_sample_views(views, CLUSTER_MAX_NODES);

	return cluster_undo_horizon_required_members(required, &sampled_epoch)
		&& sampled_epoch == epoch
		&& cluster_undo_horizon_cluster_floor(
			local_floor, views, nviews, required, cluster_node_id, sampled_epoch,
			(uint64) GetCurrentTimestamp(), (uint32) cluster_lmon_main_loop_interval,
			floor, &reason, &blame) == CLUSTER_UNDO_HORIZON_FOLD_OK
		&& !cluster_undo_horizon_epoch_fence_tripped(epoch);
}

/* Validate every item the native chain walker may inspect, before resolving
 * anything or passing a page to that walker. This is not an Assert-only gate. */
static bool
cluster_heap_prune_page_valid(Page page, BlockNumber block)
{
	PageHeader header = (PageHeader) page;
	OffsetNumber off;
	OffsetNumber maxoff;

	if (!PageHasItl(page) || PageGetPageSize(page) != BLCKSZ
		|| header->pd_lower < SizeOfPageHeaderData
		|| header->pd_lower > header->pd_upper
		|| header->pd_upper > header->pd_special
		|| header->pd_special > BLCKSZ - CLUSTER_ITL_ARRAY_SIZE
		|| (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0)
		return false;
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff > MaxHeapTuplesPerPage)
		return false;
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId item = PageGetItemId(page, off);

		if (ItemIdIsNormal(item))
		{
			HeapTupleHeader tuple;
			unsigned int start = ItemIdGetOffset(item);
			unsigned int length = ItemIdGetLength(item);

			if (start != MAXALIGN(start) || start < header->pd_upper
				|| length < SizeofHeapTupleHeader
				|| start + length > header->pd_special)
				return false;
			tuple = (HeapTupleHeader) PageGetItem(page, item);
			if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > length)
				return false;
		}
		else if (ItemIdIsRedirected(item))
		{
			OffsetNumber target = ItemIdGetRedirect(item);

			if (target < FirstOffsetNumber || target > maxoff
				|| !ItemIdIsNormal(PageGetItemId(page, target)))
				return false;
		}
	}
	/* The native planner assumes a well-formed HOT graph. Prove that every
	 * followed edge stays on this page and terminates, even for an unproved
	 * tuple. A malformed cycle must never overrun its fixed chain array. */
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		OffsetNumber next = off;
		unsigned int steps = 0;

		while (ItemIdIsNormal(PageGetItemId(page, next)))
		{
			HeapTupleHeader tuple = (HeapTupleHeader)
				PageGetItem(page, PageGetItemId(page, next));

			if (!HeapTupleHeaderIsHotUpdated(tuple))
				break;
			if (++steps > maxoff || !ItemPointerIsValid(&tuple->t_ctid)
				|| ItemPointerGetBlockNumber(&tuple->t_ctid) != block)
				return false;
			next = ItemPointerGetOffsetNumber(&tuple->t_ctid);
			if (next < FirstOffsetNumber || next > maxoff
				|| !ItemIdIsNormal(PageGetItemId(page, next)))
				return false;
		}
	}
	return true;
}

/* Only a committed updater with a real same-page HOT successor can make an
 * old version dead. Never make an orphan, DELETE or lock-only tuple DEAD. */
static bool
cluster_heap_prune_candidate(Page page, BlockNumber block, OffsetNumber off)
{
	ItemId item = PageGetItemId(page, off);
	HeapTupleHeader tuple;
	HeapTupleHeader successor;
	OffsetNumber next;

	if (!ItemIdIsNormal(item))
		return false;
	tuple = (HeapTupleHeader) PageGetItem(page, item);
	if (!HeapTupleHeaderIsHotUpdated(tuple)
		|| (tuple->t_infomask & (HEAP_MOVED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI))
		|| HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)
		|| !TransactionIdIsNormal(HeapTupleHeaderGetRawXmax(tuple))
		|| tuple->t_itl_slot_idx >= CLUSTER_ITL_INITRANS_DEFAULT
		|| !ItemPointerIsValid(&tuple->t_ctid)
		|| ItemPointerGetBlockNumber(&tuple->t_ctid) != block)
		return false;
	next = ItemPointerGetOffsetNumber(&tuple->t_ctid);
	if (next < FirstOffsetNumber || next > PageGetMaxOffsetNumber(page)
		|| next == off || !ItemIdIsNormal(PageGetItemId(page, next)))
		return false;
	successor = (HeapTupleHeader) PageGetItem(page, PageGetItemId(page, next));
	return HeapTupleHeaderIsHeapOnly(successor)
		&& HeapTupleHeaderGetRawXmin(successor) == HeapTupleHeaderGetRawXmax(tuple);
}

static bool
cluster_heap_prune_pcm(Buffer buffer, ClusterPcmOwnSnapshot *pcm)
{
	return cluster_reconfig_self_join_gate_verdict() == CLUSTER_JOIN_GATE_ALLOW
		&& cluster_bufmgr_pcm_own_snapshot(GetBufferDescriptor(buffer - 1), pcm)
			== CLUSTER_PCM_OWN_OK
		&& pcm->pcm_state == PCM_STATE_X && pcm->flags == 0;
}

/* One opportunistic owner: capture under current-X/cleanup, resolve with no
 * content lock, then revalidate the entire image and authority. No background
 * owner, cross-call proof cache or conversion/wait loop is introduced. */
static void
cluster_heap_page_prune_opt(Relation relation, Buffer buffer)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoHorizonFloor floor;
	ClusterPcmOwnSnapshot captured_pcm;
	ClusterPcmOwnSnapshot live_pcm;
	PGAlignedBlock image;
	ClusterUndoVerdictResult verdicts[MaxHeapTuplesPerPage + 1];
	bool proved_dead[MaxHeapTuplesPerPage + 1];
	Snapshot snapshot;
	Page live = BufferGetPage(buffer);
	Page page = (Page) image.data;
	BlockNumber block = BufferGetBlockNumber(buffer);
	OffsetNumber off;
	OffsetNumber maxoff;
	SCN read_scn;
	uint64 epoch;
	Size minfree;
	volatile bool locked = false;

	if (!cluster_peer_mode_enabled() || cluster_enable_adg
		|| cluster_recmerge_window_active || !cluster_undo_retention_horizon_enabled
		|| cluster_reconfig_self_join_gate_verdict() != CLUSTER_JOIN_GATE_ALLOW
		|| relation->rd_rel->relkind != RELKIND_RELATION
		|| relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT
		|| IsCatalogRelation(relation) || !ActiveSnapshotSet()
		|| !TransactionIdIsValid(((PageHeader) live)->pd_prune_xid))
		return;
	snapshot = GetActiveSnapshot();
	if (snapshot->snapshot_type != SNAPSHOT_MVCC
		|| snapshot->cluster_source != SNAPSHOT_SOURCE_CLUSTER
		|| !SCN_VALID(snapshot->read_scn)
		|| (snapshot->read_epoch == 0 && cluster_conf_node_count() != 4))
		return;
	read_scn = snapshot->read_scn;
	epoch = snapshot->read_epoch;
	minfree = Max(RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR),
				  BLCKSZ / 10);
	if (!PageIsFull(live) && PageGetHeapFreeSpace(live) >= minfree)
		return;
	memset(&admission, 0, sizeof(admission));
	if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return;
	PG_TRY();
	{
		do
		{
			int ndeleted;
			int nnewlpdead;

			if (admission.formation_epoch != epoch
				|| !cluster_heap_prune_floor(epoch, &floor)
				|| !ConditionalLockBufferForCleanup(buffer))
				break;
			locked = true;
			if (!cluster_semantic_activation_recheck_r4_terminal_census(&admission)
				|| !cluster_heap_prune_pcm(buffer, &captured_pcm)
				|| !cluster_heap_prune_page_valid(live, block)
				|| heap_page_has_current_mx_predecessor(live))
				break;
			memcpy(image.data, live, BLCKSZ);
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			locked = false;
			maxoff = PageGetMaxOffsetNumber(page);
			memset(verdicts, 0, sizeof(verdicts));
			memset(proved_dead, 0, sizeof(proved_dead));
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				HeapTupleHeader tuple;
				const ClusterItlSlotData *itl;
				TransactionId xid;
				uint32 segment;
				uint32 undo_block;
				uint16 slot;
				uint16 row;
				int origin;
				bool fresh;
				OffsetNumber prior;

				if (!cluster_heap_prune_candidate(page, block, off))
					continue;
				tuple = (HeapTupleHeader) PageGetItem(page, PageGetItemId(page, off));
				xid = HeapTupleHeaderGetRawXmax(tuple);
				itl = &ClusterPageGetItlSlots(page)[tuple->t_itl_slot_idx];
				if (!uba_decode(itl->undo_segment_head, &segment, &undo_block, &slot, &row))
					continue;
				fresh = itl->xid == xid;
				origin = fresh ? uba_origin_node_id(itl->undo_segment_head)
					: cluster_xid_origin_slot(xid);
				if (origin < 0 || origin >= CLUSTER_MAX_NODES
					|| (fresh && itl->flags != ITL_FLAG_ACTIVE
						&& itl->flags != ITL_FLAG_COMMITTED
						&& itl->flags != ITL_FLAG_NEEDS_CLEANOUT))
					continue;
				/* Exact same page locator inputs share one verdict, not just
				 * an xid whose physical incarnation might be different. */
				for (prior = FirstOffsetNumber; prior < off; prior++)
				{
					HeapTupleHeader old;

					if (!cluster_heap_prune_candidate(page, block, prior))
						continue;
					old = (HeapTupleHeader) PageGetItem(page, PageGetItemId(page, prior));
					if (HeapTupleHeaderGetRawXmax(old) == xid
						&& old->t_itl_slot_idx == tuple->t_itl_slot_idx)
						break;
				}
				if (prior < off)
					verdicts[off] = verdicts[prior];
				else if (fresh && SCN_VALID(itl->commit_scn))
					verdicts[off] = cluster_undo_verdict_resolve_freshref_c1b_pair(
						origin, segment, xid, itl->xid, (uint32) slot + 1,
						(uint32) epoch, itl->commit_scn, read_scn);
				else
					verdicts[off] = cluster_undo_verdict_resolve(
						origin, segment, xid, fresh ? (uint32) slot + 1 : 0,
						read_scn, fresh);
			}
			if (!cluster_heap_prune_floor(epoch, &floor))
				break;
			for (off = FirstOffsetNumber; off <= maxoff; off++)
				proved_dead[off] = (verdicts[off].kind == CLUSTER_UNDO_VERDICT_COMMITTED_EXACT
					|| verdicts[off].kind == CLUSTER_UNDO_VERDICT_COMMITTED_BOUND)
					&& SCN_VALID(verdicts[off].commit_scn)
					&& scn_time_cmp(verdicts[off].commit_scn, floor.scn) < 0;
			if (!ConditionalLockBufferForCleanup(buffer))
				break;
			locked = true;
			if (!cluster_semantic_activation_recheck_r4_terminal_census(&admission)
				|| !cluster_heap_prune_pcm(buffer, &live_pcm)
				|| !cluster_pcm_own_snapshot_equal_exact(&captured_pcm, &live_pcm)
				|| memcmp(image.data, live, BLCKSZ) != 0
				|| cluster_recmerge_window_active
				|| cluster_undo_horizon_epoch_fence_tripped(epoch))
				break;
			ndeleted = heap_page_prune_internal(relation, buffer, InvalidTransactionId,
				NULL, InvalidTransactionId, 0, &nnewlpdead, NULL, proved_dead);
			if (ndeleted > nnewlpdead)
				pgstat_update_heap_dead_tuples(relation, ndeleted - nnewlpdead);
		} while (false);
	}
	PG_FINALLY();
	{
		if (locked)
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();
}
#endif


/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 */
void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	TransactionId limited_xmin = InvalidTransactionId;
	TimestampTz limited_ts = 0;
	Size		minfree;

	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record
	 * soon anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_storage_mode_enabled() && !BufferIsLocal(buffer))
	{
		cluster_heap_page_prune_opt(relation, buffer);
		return;
	}
#endif

	/*
	 * XXX: Magic to keep old_snapshot_threshold tests appear "working". They
	 * currently are broken, and discussion of what to do about them is
	 * ongoing. See
	 * https://www.postgresql.org/message-id/20200403001235.e6jfdll3gh2ygbuc%40alap3.anarazel.de
	 */
	if (old_snapshot_threshold == 0)
		SnapshotTooOldMagicForTest();

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 *
	 * It is OK to check the old snapshot limit before acquiring the cleanup
	 * lock because the worst that can happen is that we are not quite as
	 * aggressive about the cleanup (by however many transaction IDs are
	 * consumed between this point and acquiring the lock).  This allows us to
	 * save significant overhead in the case where the page is found not to be
	 * prunable.
	 *
	 * Even if old_snapshot_threshold is set, we first check whether the page
	 * can be pruned without. Both because
	 * TransactionIdLimitedForOldSnapshots() is not cheap, and because not
	 * unnecessarily relying on old_snapshot_threshold avoids causing
	 * conflicts.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
	{
		if (!OldSnapshotThresholdActive())
			return;

		if (!TransactionIdLimitedForOldSnapshots(GlobalVisTestNonRemovableHorizon(vistest),
												 relation,
												 &limited_xmin, &limited_ts))
			return;

		if (!TransactionIdPrecedes(prune_xid, limited_xmin))
			return;
	}

	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 HEAP_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
	{
		/* OK, try to get exclusive buffer lock */
		if (!ConditionalLockBufferForCleanup(buffer))
			return;

		/*
		 * Now that we have buffer lock, get accurate information about the
		 * page's free space, and recheck the heuristic about whether to
		 * prune.
		 */
		if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		{
			int			ndeleted,
						nnewlpdead;

			ndeleted = heap_page_prune(relation, buffer, InvalidTransactionId,
									   vistest, limited_xmin,
									   limited_ts, &nnewlpdead, NULL);

			/*
			 * Report the number of tuples reclaimed to pgstats.  This is
			 * ndeleted minus the number of newly-LP_DEAD-set items.
			 *
			 * We derive the number of dead tuples like this to avoid totally
			 * forgetting about items that were set to LP_DEAD, since they
			 * still need to be cleaned up by VACUUM.  We only want to count
			 * heap-only tuples that just became LP_UNUSED in our report,
			 * which don't.
			 *
			 * VACUUM doesn't have to compensate in the same way when it
			 * tracks ndeleted, since it will set the same LP_DEAD items to
			 * LP_UNUSED separately.
			 */
			if (ndeleted > nnewlpdead)
				pgstat_update_heap_dead_tuples(relation,
											   ndeleted - nnewlpdead);
		}

		/* And release buffer lock */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * We avoid reuse of any free space created on the page by unrelated
		 * UPDATEs/INSERTs by opting to not update the FSM at this point.  The
		 * free space should be reused by UPDATEs to *this* page.
		 */
	}
}


/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.  Note that we
 * don't update the FSM information for page on caller's behalf.  Caller might
 * also need to account for a reduction in the length of the line pointer
 * array following array truncation by us.
 *
 * vistest and oldest_xmin are used to distinguish whether tuples are DEAD or
 * RECENTLY_DEAD (see heap_prune_satisfies_vacuum and
 * HeapTupleSatisfiesVacuum). If oldest_xmin is provided by the caller, it is
 * used before consulting GlobalVisState.
 *
 * old_snap_xmin / old_snap_ts need to either have been set by
 * TransactionIdLimitedForOldSnapshots, or InvalidTransactionId/0
 * respectively.
 *
 * Sets *nnewlpdead for caller, indicating the number of items that were
 * newly set LP_DEAD during prune operation.
 *
 * off_loc is the offset location required by the caller to use in error
 * callback.
 *
 * Returns the number of tuples deleted from the page during this call.
 */
int
heap_page_prune(Relation relation, Buffer buffer,
				TransactionId oldest_xmin,
				GlobalVisState *vistest,
				TransactionId old_snap_xmin,
				TimestampTz old_snap_ts,
				int *nnewlpdead,
				OffsetNumber *off_loc)
{
	return heap_page_prune_internal(relation, buffer, oldest_xmin, vistest,
		old_snap_xmin, old_snap_ts, nnewlpdead, off_loc, NULL);
}

static int
heap_page_prune_internal(Relation relation, Buffer buffer,
						 TransactionId oldest_xmin, GlobalVisState *vistest,
						 TransactionId old_snap_xmin, TimestampTz old_snap_ts,
						 int *nnewlpdead, OffsetNumber *off_loc, const bool *proved_dead)
{
	int			ndeleted = 0;
	Page		page = BufferGetPage(buffer);
	BlockNumber blockno = BufferGetBlockNumber(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;
	HeapTupleData tup;

	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	/* An unproved shared tuple remains a candidate for a later invocation.
	 * Do not clear the scheduling hint using native/local GlobalVis. */
	if (proved_dead != NULL)
		prstate.new_prune_xid = ((PageHeader) page)->pd_prune_xid;
	prstate.rel = relation;
	prstate.oldest_xmin = oldest_xmin;
	prstate.vistest = vistest;
	prstate.old_snap_xmin = old_snap_xmin;
	prstate.old_snap_ts = old_snap_ts;
	prstate.old_snap_used = false;
	prstate.snapshotConflictHorizon = InvalidTransactionId;
	prstate.proved_dead = proved_dead;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	memset(prstate.marked, 0, sizeof(prstate.marked));

	maxoff = PageGetMaxOffsetNumber(page);
	tup.t_tableOid = RelationGetRelid(prstate.rel);

#ifdef USE_PGRAC_CLUSTER
	/* Pruning can remove a tuple or HOT predecessor.  A current-MX tuple is
	 * left byte-for-byte intact until CTRC cleanout has installed a successor
	 * descriptor/terminal projection and discharged the exact old target. */
	if (heap_page_has_current_mx_predecessor(page))
	{
		*nnewlpdead = 0;
		if (off_loc != NULL)
			*off_loc = InvalidOffsetNumber;
		return 0;
	}
#endif

	/*
	 * Determine HTSV for all tuples.
	 *
	 * This is required for correctness to deal with cases where running HTSV
	 * twice could result in different results (e.g. RECENTLY_DEAD can turn to
	 * DEAD if another checked item causes GlobalVisTestIsRemovableFullXid()
	 * to update the horizon, INSERT_IN_PROGRESS can change to DEAD if the
	 * inserting transaction aborts, ...). That in turn could cause
	 * heap_prune_chain() to behave incorrectly if a tuple is reached twice,
	 * once directly via a heap_prune_chain() and once following a HOT chain.
	 *
	 * It's also good for performance. Most commonly tuples within a page are
	 * stored at decreasing offsets (while the items are stored at increasing
	 * offsets). When processing all tuples on a page this leads to reading
	 * memory at decreasing offsets within a page, with a variable stride.
	 * That's hard for CPU prefetchers to deal with. Processing the items in
	 * reverse order (and thus the tuples in increasing order) increases
	 * prefetching efficiency significantly / decreases the number of cache
	 * misses.
	 */
	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		HeapTupleHeader htup;

		/* Nothing to do if slot doesn't contain a tuple */
		if (!ItemIdIsNormal(itemid))
		{
			prstate.htsv[offnum] = -1;
			continue;
		}

		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		tup.t_data = htup;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&(tup.t_self), blockno, offnum);

		/*
		 * Set the offset number so that we can display it along with any
		 * error that occurred while processing this tuple.
		 */
		if (off_loc)
			*off_loc = offnum;

		prstate.htsv[offnum] = proved_dead != NULL
			? (proved_dead[offnum] ? HEAPTUPLE_DEAD : HEAPTUPLE_LIVE)
			: heap_prune_satisfies_vacuum(&prstate, &tup, buffer);
	}

	/* Scan the page */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		/* Ignore items already processed as part of an earlier chain */
		if (prstate.marked[offnum])
			continue;

		/* see preceding loop */
		if (off_loc)
			*off_loc = offnum;

		/* Nothing to do if slot is empty or already dead */
		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
			continue;

		/* Process this item or chain of items */
		ndeleted += heap_prune_chain(buffer, offnum, &prstate);
	}

	/* Clear the offset information once we have processed the given page. */
	if (off_loc)
		*off_loc = InvalidOffsetNumber;

	/* The proved HOT-only entry never removes an index root or updates a
	 * shared hint on a zero-reclamation pass. Other entrypoints are unchanged. */
	if (proved_dead != NULL && (prstate.ndead != 0 || ndeleted == 0))
	{
		*nnewlpdead = 0;
		return 0;
	}

	/* Any error while applying the changes is critical */
	START_CRIT_SECTION();

	/* Have we found any prunable items? */
	if (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0)
	{
		/*
		 * Apply the planned item changes, then repair page fragmentation, and
		 * update the page's hint bit about whether it has free line pointers.
		 */
		heap_page_prune_execute(buffer,
								prstate.redirected, prstate.nredirected,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused);

		/*
		 * Update the page's pd_prune_xid field to either zero, or the lowest
		 * XID of any soon-prunable tuple.
		 */
		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

		/*
		 * Also clear the "page is full" flag, since there's no point in
		 * repeating the prune/defrag process until something else happens to
		 * the page.
		 */
		PageClearFull(page);

		MarkBufferDirty(buffer);

		/*
		 * Emit a WAL XLOG_HEAP2_PRUNE record showing what we did
		 */
		if (RelationNeedsWAL(relation))
		{
			xl_heap_prune xlrec;
			XLogRecPtr	recptr;

			xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(relation);
			xlrec.snapshotConflictHorizon = prstate.snapshotConflictHorizon;
			xlrec.nredirected = prstate.nredirected;
			xlrec.ndead = prstate.ndead;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);

			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

			/*
			 * The OffsetNumber arrays are not actually in the buffer, but we
			 * pretend that they are.  When XLogInsert stores the whole
			 * buffer, the offset arrays need not be stored too.
			 */
			if (prstate.nredirected > 0)
				XLogRegisterBufData(0, (char *) prstate.redirected,
									prstate.nredirected *
									sizeof(OffsetNumber) * 2);

			if (prstate.ndead > 0)
				XLogRegisterBufData(0, (char *) prstate.nowdead,
									prstate.ndead * sizeof(OffsetNumber));

			if (prstate.nunused > 0)
				XLogRegisterBufData(0, (char *) prstate.nowunused,
									prstate.nunused * sizeof(OffsetNumber));

			recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PRUNE);

			PageSetLSN(BufferGetPage(buffer), recptr);
		}
	}
	else
	{
		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}
	}

	END_CRIT_SECTION();

	/* Record number of newly-set-LP_DEAD items for caller */
	*nnewlpdead = prstate.ndead;

	return ndeleted;
}


/*
 * Perform visibility checks for heap pruning.
 *
 * This is more complicated than just using GlobalVisTestIsRemovableXid()
 * because of old_snapshot_threshold. We only want to increase the threshold
 * that triggers errors for old snapshots when we actually decide to remove a
 * row based on the limited horizon.
 *
 * Due to its cost we also only want to call
 * TransactionIdLimitedForOldSnapshots() if necessary, i.e. we might not have
 * done so in heap_page_prune_opt() if pd_prune_xid was old enough. But we
 * still want to be able to remove rows that are too new to be removed
 * according to prstate->vistest, but that can be removed based on
 * old_snapshot_threshold. So we call TransactionIdLimitedForOldSnapshots() on
 * demand in here, if appropriate.
 */
static HTSV_Result
heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)
{
	HTSV_Result res;
	TransactionId dead_after;

	res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

	if (res != HEAPTUPLE_RECENTLY_DEAD)
		return res;

	/*
	 * If we are already relying on the limited xmin, there is no need to
	 * delay doing so anymore.
	 */
	if (prstate->old_snap_used)
	{
		Assert(TransactionIdIsValid(prstate->old_snap_xmin));

		if (TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
			res = HEAPTUPLE_DEAD;
		return res;
	}

	/*
	 * For VACUUM, we must be sure to prune tuples with xmax older than
	 * oldest_xmin -- a visibility cutoff determined at the beginning of
	 * vacuuming the relation. oldest_xmin is used for freezing determination
	 * and we cannot freeze dead tuples' xmaxes.
	 */
	if (TransactionIdIsValid(prstate->oldest_xmin) &&
		NormalTransactionIdPrecedes(dead_after, prstate->oldest_xmin))
		return HEAPTUPLE_DEAD;

	/*
	 * Determine whether or not the tuple is considered dead when compared
	 * with the provided GlobalVisState. On-access pruning does not provide
	 * oldest_xmin. And for vacuum, even if the tuple's xmax is not older than
	 * oldest_xmin, GlobalVisTestIsRemovableXid() could find the row dead if
	 * the GlobalVisState has been updated since the beginning of vacuuming
	 * the relation.
	 */
	if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
		return HEAPTUPLE_DEAD;

	/*
	 * If GlobalVisTestIsRemovableXid() is not sufficient to find the row dead
	 * and old_snapshot_threshold is enabled, try to use the lowered horizon.
	 */
	if (OldSnapshotThresholdActive())
	{
		/* haven't determined limited horizon yet, requests */
		if (!TransactionIdIsValid(prstate->old_snap_xmin))
		{
			TransactionId horizon =
				GlobalVisTestNonRemovableHorizon(prstate->vistest);

			TransactionIdLimitedForOldSnapshots(horizon, prstate->rel,
												&prstate->old_snap_xmin,
												&prstate->old_snap_ts);
		}

		if (TransactionIdIsValid(prstate->old_snap_xmin) &&
			TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
		{
			/*
			 * About to remove row based on snapshot_too_old. Need to raise
			 * the threshold so problematic accesses would error.
			 */
			Assert(!prstate->old_snap_used);
			SetOldSnapshotThresholdTimestamp(prstate->old_snap_ts,
											 prstate->old_snap_xmin);
			prstate->old_snap_used = true;
			res = HEAPTUPLE_DEAD;
		}
	}

	return res;
}


static void
heap_prune_advance_conflict_horizon(HeapTupleHeader tuple, PruneState *prstate)
{
	if (prstate->proved_dead != NULL)
	{
		TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

		/* The shared entry has already proved this normal updater committed.
		 * Carry its xid conservatively into the unchanged WAL field; do not
		 * ask the requester's CLOG about a possibly foreign, unhinted xmin.
		 * This is not another deletion authority or an exact SCN stamp. */
		Assert(TransactionIdIsNormal(xmax));
		Assert((tuple->t_infomask & (HEAP_MOVED | HEAP_XMAX_IS_MULTI)) == 0);
		if (TransactionIdFollows(xmax, prstate->snapshotConflictHorizon))
			prstate->snapshotConflictHorizon = xmax;
	}
	else
		HeapTupleHeaderAdvanceConflictHorizon(tuple, &prstate->snapshotConflictHorizon);
}

/*
 * Prune specified line pointer or a HOT chain originating at line pointer.
 *
 * If the item is an index-referenced tuple (i.e. not a heap-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain.  We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, our visibility test is just too coarse to detect it.
 *
 * In general, pruning must never leave behind a DEAD tuple that still has
 * tuple storage.  VACUUM isn't prepared to deal with that case.  That's why
 * VACUUM prunes the same heap page a second time (without dropping its lock
 * in the interim) when it sees a newly DEAD tuple that we initially saw as
 * in-progress.  Retrying pruning like this can only happen when an inserting
 * transaction concurrently aborts.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * We don't actually change the page here. We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
static int
heap_prune_chain(Buffer buffer, OffsetNumber rootoffnum, PruneState *prstate)
{
	int			ndeleted = 0;
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId priorXmax = InvalidTransactionId;
	ItemId		rootlp;
	HeapTupleHeader htup;
	OffsetNumber latestdead = InvalidOffsetNumber,
				maxoff = PageGetMaxOffsetNumber(dp),
				offnum;
	OffsetNumber chainitems[MaxHeapTuplesPerPage];
	int			nchain = 0,
				i;

	rootlp = PageGetItemId(dp, rootoffnum);

	/*
	 * If it's a heap-only tuple, then it is not the start of a HOT chain.
	 */
	if (ItemIdIsNormal(rootlp))
	{
		Assert(prstate->htsv[rootoffnum] != -1);
		htup = (HeapTupleHeader) PageGetItem(dp, rootlp);

		if (HeapTupleHeaderIsHeapOnly(htup))
		{
			/*
			 * If the tuple is DEAD and doesn't chain to anything else, mark
			 * it unused immediately.  (If it does chain, we can only remove
			 * it as part of pruning its chain.)
			 *
			 * We need this primarily to handle aborted HOT updates, that is,
			 * XMIN_INVALID heap-only tuples.  Those might not be linked to by
			 * any chain, since the parent tuple might be re-updated before
			 * any pruning occurs.  So we have to be able to reap them
			 * separately from chain-pruning.  (Note that
			 * HeapTupleHeaderIsHotUpdated will never return true for an
			 * XMIN_INVALID tuple, so this code will work even when there were
			 * sequential updates within the aborted transaction.)
			 *
			 * Note that we might first arrive at a dead heap-only tuple
			 * either here or while following a chain below.  Whichever path
			 * gets there first will mark the tuple unused.
			 */
			if (prstate->htsv[rootoffnum] == HEAPTUPLE_DEAD &&
				!HeapTupleHeaderIsHotUpdated(htup))
			{
				heap_prune_record_unused(prstate, rootoffnum);
				heap_prune_advance_conflict_horizon(htup, prstate);
				ndeleted++;
			}

			/* Nothing more to do */
			return ndeleted;
		}
	}

	/* Start from the root tuple */
	offnum = rootoffnum;

	/* while not end of the chain */
	for (;;)
	{
		ItemId		lp;
		bool		tupdead,
					recent_dead;

		/* Sanity check (pure paranoia) */
		if (offnum < FirstOffsetNumber)
			break;

		/*
		 * An offset past the end of page's line pointer array is possible
		 * when the array was truncated (original item must have been unused)
		 */
		if (offnum > maxoff)
			break;

		/* If item is already processed, stop --- it must not be same chain */
		if (prstate->marked[offnum])
			break;

		lp = PageGetItemId(dp, offnum);

		/* Unused item obviously isn't part of the chain */
		if (!ItemIdIsUsed(lp))
			break;

		/*
		 * If we are looking at the redirected root line pointer, jump to the
		 * first normal tuple in the chain.  If we find a redirect somewhere
		 * else, stop --- it must not be same chain.
		 */
		if (ItemIdIsRedirected(lp))
		{
			if (nchain > 0)
				break;			/* not at start of chain */
			chainitems[nchain++] = offnum;
			offnum = ItemIdGetRedirect(rootlp);
			continue;
		}

		/*
		 * Likewise, a dead line pointer can't be part of the chain. (We
		 * already eliminated the case of dead root tuple outside this
		 * function.)
		 */
		if (ItemIdIsDead(lp))
			break;

		Assert(ItemIdIsNormal(lp));
		Assert(prstate->htsv[offnum] != -1);
		htup = (HeapTupleHeader) PageGetItem(dp, lp);

		/*
		 * Check the tuple XMIN against prior XMAX, if any
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
			break;

		/*
		 * OK, this tuple is indeed a member of the chain.
		 */
		chainitems[nchain++] = offnum;

		/*
		 * Check tuple's visibility status.
		 */
		tupdead = recent_dead = false;

		switch ((HTSV_Result) prstate->htsv[offnum])
		{
			case HEAPTUPLE_DEAD:
				tupdead = true;
				break;

			case HEAPTUPLE_RECENTLY_DEAD:
				recent_dead = true;

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_LIVE:
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in heapam.c.
				 */
				break;

			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}

		/*
		 * Remember the last DEAD tuple seen.  We will advance past
		 * RECENTLY_DEAD tuples just in case there's a DEAD one after them;
		 * but we can't advance past anything else.  We have to make sure that
		 * we don't miss any DEAD tuples, since DEAD tuples that still have
		 * tuple storage after pruning will confuse VACUUM.
		 */
		if (tupdead)
		{
			latestdead = offnum;
			heap_prune_advance_conflict_horizon(htup, prstate);
		}
		else if (!recent_dead)
			break;

		/*
		 * If the tuple is not HOT-updated, then we are at the end of this
		 * HOT-update chain.
		 */
		if (!HeapTupleHeaderIsHotUpdated(htup))
			break;

		/* HOT implies it can't have moved to different partition */
		Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

		/*
		 * Advance to next chain member.
		 */
		Assert(ItemPointerGetBlockNumber(&htup->t_ctid) ==
			   BufferGetBlockNumber(buffer));
		offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
		priorXmax = HeapTupleHeaderGetUpdateXid(htup);
	}

	/*
	 * If we found a DEAD tuple in the chain, adjust the HOT chain so that all
	 * the DEAD tuples at the start of the chain are removed and the root line
	 * pointer is appropriately redirected.
	 */
	if (OffsetNumberIsValid(latestdead))
	{
		/*
		 * Mark as unused each intermediate item that we are able to remove
		 * from the chain.
		 *
		 * When the previous item is the last dead tuple seen, we are at the
		 * right candidate for redirection.
		 */
		for (i = 1; (i < nchain) && (chainitems[i - 1] != latestdead); i++)
		{
			heap_prune_record_unused(prstate, chainitems[i]);
			ndeleted++;
		}

		/*
		 * If the root entry had been a normal tuple, we are deleting it, so
		 * count it in the result.  But changing a redirect (even to DEAD
		 * state) doesn't count.
		 */
		if (ItemIdIsNormal(rootlp))
			ndeleted++;

		/*
		 * If the DEAD tuple is at the end of the chain, the entire chain is
		 * dead and the root line pointer can be marked dead.  Otherwise just
		 * redirect the root to the correct chain member.
		 */
		if (i >= nchain)
			heap_prune_record_dead(prstate, rootoffnum);
		else
			heap_prune_record_redirect(prstate, rootoffnum, chainitems[i]);
	}
	else if (nchain < 2 && ItemIdIsRedirected(rootlp))
	{
		/*
		 * We found a redirect item that doesn't point to a valid follow-on
		 * item.  This can happen if the loop in heap_page_prune caused us to
		 * visit the dead successor of a redirect item before visiting the
		 * redirect item.  We can clean up by setting the redirect item to
		 * DEAD state.
		 */
		heap_prune_record_dead(prstate, rootoffnum);
	}

	return ndeleted;
}

/* Record lowest soon-prunable XID */
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/* Record line pointer to be redirected */
static void
heap_prune_record_redirect(PruneState *prstate,
						   OffsetNumber offnum, OffsetNumber rdoffnum)
{
	Assert(prstate->nredirected < MaxHeapTuplesPerPage);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
	prstate->nredirected++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	Assert(!prstate->marked[rdoffnum]);
	prstate->marked[rdoffnum] = true;
}

/* Record line pointer to be marked dead */
static void
heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->ndead < MaxHeapTuplesPerPage);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}

/* Record line pointer to be marked unused */
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->nunused < MaxHeapTuplesPerPage);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}


/*
 * Perform the actual page changes needed by heap_page_prune.
 * It is expected that the caller has a full cleanup lock on the
 * buffer.
 */
void
heap_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	HeapTupleHeader htup PG_USED_FOR_ASSERTS_ONLY;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* Update all redirected line pointers */
	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		ItemId		tolp PG_USED_FOR_ASSERTS_ONLY;

#ifdef USE_ASSERT_CHECKING

		/*
		 * Any existing item that we set as an LP_REDIRECT (any 'from' item)
		 * must be the first item from a HOT chain.  If the item has tuple
		 * storage then it can't be a heap-only tuple.  Otherwise we are just
		 * maintaining an existing LP_REDIRECT from an existing HOT chain that
		 * has been pruned at least once before now.
		 */
		if (!ItemIdIsRedirected(fromlp))
		{
			Assert(ItemIdHasStorage(fromlp) && ItemIdIsNormal(fromlp));

			htup = (HeapTupleHeader) PageGetItem(page, fromlp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* We shouldn't need to redundantly set the redirect */
			Assert(ItemIdGetRedirect(fromlp) != tooff);
		}

		/*
		 * The item that we're about to set as an LP_REDIRECT (the 'from'
		 * item) will point to an existing item (the 'to' item) that is
		 * already a heap-only tuple.  There can be at most one LP_REDIRECT
		 * item per HOT chain.
		 *
		 * We need to keep around an LP_REDIRECT item (after original
		 * non-heap-only root tuple gets pruned away) so that it's always
		 * possible for VACUUM to easily figure out what TID to delete from
		 * indexes when an entire HOT chain becomes dead.  A heap-only tuple
		 * can never become LP_DEAD; an LP_REDIRECT item or a regular heap
		 * tuple can.
		 *
		 * This check may miss problems, e.g. the target of a redirect could
		 * be marked as unused subsequently. The page_verify_redirects() check
		 * below will catch such problems.
		 */
		tolp = PageGetItemId(page, tooff);
		Assert(ItemIdHasStorage(tolp) && ItemIdIsNormal(tolp));
		htup = (HeapTupleHeader) PageGetItem(page, tolp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * An LP_DEAD line pointer must be left behind when the original item
		 * (which is dead to everybody) could still be referenced by a TID in
		 * an index.  This should never be necessary with any individual
		 * heap-only tuple item, though. (It's not clear how much of a problem
		 * that would be, but there is no reason to allow it.)
		 */
		if (ItemIdHasStorage(lp))
		{
			Assert(ItemIdIsNormal(lp));
			htup = (HeapTupleHeader) PageGetItem(page, lp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* Whole HOT chain becomes dead */
			Assert(ItemIdIsRedirected(lp));
		}
#endif

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * Only heap-only tuples can become LP_UNUSED during pruning.  They
		 * don't need to be left in place as LP_DEAD items until VACUUM gets
		 * around to doing index vacuuming.
		 */
		Assert(ItemIdHasStorage(lp) && ItemIdIsNormal(lp));
		htup = (HeapTupleHeader) PageGetItem(page, lp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);

	/*
	 * Now that the page has been modified, assert that redirect items still
	 * point to valid targets.
	 */
	page_verify_redirects(page);
}


/*
 * If built with assertions, verify that all LP_REDIRECT items point to a
 * valid item.
 *
 * One way that bugs related to HOT pruning show is redirect items pointing to
 * removed tuples. It's not trivial to reliably check that marking an item
 * unused will not orphan a redirect item during heap_prune_chain() /
 * heap_page_prune_execute(), so we additionally check the whole page after
 * pruning. Without this check such bugs would typically only cause asserts
 * later, potentially well after the corruption has been introduced.
 *
 * Also check comments in heap_page_prune_execute()'s redirection loop.
 */
static void
page_verify_redirects(Page page)
{
#ifdef USE_ASSERT_CHECKING
	OffsetNumber offnum;
	OffsetNumber maxoff;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		OffsetNumber targoff;
		ItemId		targitem;
		HeapTupleHeader htup;

		if (!ItemIdIsRedirected(itemid))
			continue;

		targoff = ItemIdGetRedirect(itemid);
		targitem = PageGetItemId(page, targoff);

		Assert(ItemIdIsUsed(targitem));
		Assert(ItemIdIsNormal(targitem));
		Assert(ItemIdHasStorage(targitem));
		htup = (HeapTupleHeader) PageGetItem(page, targitem);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
	}
#endif
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxHeapTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		HeapTupleHeader htup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (HeapTupleHeaderIsHeapOnly(htup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			/* Sanity check (pure paranoia) */
			if (offnum < FirstOffsetNumber)
				break;

			/*
			 * An offset past the end of page's line pointer array is possible
			 * when the array was truncated
			 */
			if (offnum > maxoff)
				break;

			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
	}
}
