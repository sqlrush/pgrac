/*-------------------------------------------------------------------------
 *
 * clog.c
 *		PostgreSQL transaction-commit-log manager
 *
 * This module replaces the old "pg_log" access code, which treated pg_log
 * essentially like a relation, in that it went through the regular buffer
 * manager.  The problem with that was that there wasn't any good way to
 * recycle storage space for transactions so old that they'll never be
 * looked up again.  Now we use specialized access code so that the commit
 * log can be broken into relatively small, independent segments.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CLOG page is initialized to zeroes.  Other writes of CLOG come from
 * recording of transaction commit or abort in xact.c, which generates its
 * own XLOG records for these events and will re-perform the status update
 * on redo; so we need make no additional XLOG entry here.  For synchronous
 * transaction commits, the XLOG is guaranteed flushed through the XLOG commit
 * record before we are called to log a commit, so the WAL rule "write xlog
 * before data" is satisfied automatically.  However, for async commits we
 * must track the latest LSN affecting each CLOG page, so that we can flush
 * XLOG that far and satisfy the WAL rule.  We don't have to worry about this
 * for aborts (whether sync or async), since the post-crash assumption would
 * be that such transactions failed anyway.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/clog.c
 *
 * PGRAC: shared-origin startup verifies unused status instead of erasing
 * conflicting retained input. Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "common/cluster_recovery_bootstrap.h"
#include "storage/fd.h"
#include <sys/stat.h>
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/sync.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_cf_enqueue.h"
#endif

/*
 * Defines for CLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CLOG page numbering also wraps around at 0xFFFFFFFF/CLOG_XACTS_PER_PAGE,
 * and CLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCLOG (see CLOGPagePrecedes).
 */

/* We need two bits per xact, so four xacts fit in a byte */
#define CLOG_BITS_PER_XACT	2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK	((1 << CLOG_BITS_PER_XACT) - 1)

#ifdef USE_PGRAC_CLUSTER
StaticAssertDecl(CLOG_XACTS_PER_PAGE == CLUSTER_NATIVE_CLOG_PER_PAGE,
				 "native CLOG inspection geometry");
#endif

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid)	(TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid)	((xid) % (TransactionId) CLOG_XACTS_PER_BYTE)

/* We store the latest async LSN for each group of transactions */
#define CLOG_XACTS_PER_LSN_GROUP	32	/* keep this a power of 2 */
#define CLOG_LSNS_PER_PAGE	(CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, xid)	((slotno) * CLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_LSN_GROUP)

/*
 * The number of subtransactions below which we consider to apply clog group
 * update optimization.  Testing reveals that the number higher than this can
 * hurt performance.
 */
#define THRESHOLD_SUBTRANS_CLOG_OPT	5

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData XactCtlData;

#define XactCtl (&XactCtlData)


static int	ZeroCLOGPage(int pageno, bool writeXlog);
static bool CLOGPagePrecedes(int page1, int page2);
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno, TransactionId oldestXact,
								 Oid oldestXactDb);
static void TransactionIdSetPageStatus(TransactionId xid, int nsubxids,
									   TransactionId *subxids, XidStatus status,
									   XLogRecPtr lsn, int pageno,
									   bool all_xact_same_page);
static void TransactionIdSetStatusBit(TransactionId xid, XidStatus status,
									  XLogRecPtr lsn, int slotno);
static void set_status_by_pages(int nsubxids, TransactionId *subxids,
								XidStatus status, XLogRecPtr lsn);
static bool TransactionGroupUpdateXidStatus(TransactionId xid,
											XidStatus status, XLogRecPtr lsn, int pageno);
static void TransactionIdSetPageStatusInternal(TransactionId xid, int nsubxids,
											   TransactionId *subxids, XidStatus status,
											   XLogRecPtr lsn, int pageno);


#ifdef USE_PGRAC_CLUSTER
/* Disabled-cluster PostgreSQL remains raw. A managed or creation selection
 * also applies before serving; UNKNOWN on an enabled cluster must refuse. */
static bool
CLOGNativeIORequired(void)
{
	return cluster_enabled || cluster_control_native_files_mode_v2() !=
		CLUSTER_NATIVE_FILES_UNKNOWN_V2;
}

static bool
CLOGNativeFileAcquire(int pageno, ClusterControlNativeFileBorrowV2 *borrow)
{
	ClusterControlFileKeyV2 key = {0};

	if (pageno < 0 || pageno >= 131072 || cluster_node_id < 0 ||
		cluster_node_id > PG_UINT16_MAX ||
		cluster_control_native_files_mode_v2() == CLUSTER_NATIVE_FILES_UNKNOWN_V2)
		return false;
	key.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	key.origin = (uint16) cluster_node_id;
	key.object_id = (uint64) pageno / SLRU_PAGES_PER_SEGMENT;
	return cluster_control_native_file_acquire_v2(&key, borrow);
}

static void
CLOGConfigureManagedIO(SlruCtl ctl)
{
	ctl->clog_native_required = CLOGNativeIORequired;
	ctl->clog_native_acquire = CLOGNativeFileAcquire;
	ctl->clog_native_current = cluster_control_native_file_current_v2;
	ctl->clog_native_release = cluster_control_native_file_release_v2;
	ctl->clog_native_read = SimpleLruNativeClogReadV2;
	ctl->clog_native_write = SimpleLruNativeClogWriteV2;
}

/* These lifecycle callers still need their original complete selected-range
 * owner. A raw record, cutoff or segno-only request cannot supply it. */
static void
CLOGRequireLegacyLifecycle(void)
{
	if (CLOGNativeIORequired())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed CLOG lifecycle requires its selected control owner")));
}

/* Actual gap files are protected through issuer completion. The two successor
 * identities are presence only; neither their metadata nor this borrow grants
 * page birth or access to RESERVED through the ordinary SLRU I/O path. */
struct ClusterClogAllocationV2 {
	uint64 first, count;
	ClusterControlNativeFileBorrowV2 files[FLEXIBLE_ARRAY_MEMBER];
};

static bool
CLOGAllocationSegments(FullTransactionId first, FullTransactionId last,
					   uint64 *begin, uint64 *gap, uint64 *total)
{
	uint64 a = U64FromFullTransactionId(first), b = U64FromFullTransactionId(last);
	uint64 per = (uint64) CLOG_XACTS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;

	if (!FullTransactionIdIsNormal(first) || !FullTransactionIdIsNormal(last)
		|| a == UINT64_MAX || b == UINT64_MAX || b < a || b - a > PG_INT32_MAX)
		return false;
	*begin = (a / per) % 4096;
	*gap = b / per - a / per + 1;
	*total = Min(*gap + 2, UINT64CONST(4096));
	return true;
}

static bool
CLOGAllocationPresence(uint64 begin, uint64 count)
{
	uint64 tail = Min(count, 4096 - begin);

	return cluster_node_id >= 0 && cluster_node_id < 128
		&& cluster_control_native_files_clog_range_v2(cluster_node_id, begin, begin + tail - 1)
			== CLUSTER_NATIVE_FILES_OK_V2
		&& (count == tail || cluster_control_native_files_clog_range_v2(
			cluster_node_id, 0, count - tail - 1) == CLUSTER_NATIVE_FILES_OK_V2);
}

void
CLOGFinishAllocation(ClusterClogAllocationV2 **state)
{
	ClusterClogAllocationV2 *value = *state;

	if (!value)
		return;
	for (uint64 i = 0; i < value->count; i++)
		if (value->files[i].private_self
			&& !cluster_control_native_file_release_v2(&value->files[i]))
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not return CLOG allocation file identity")));
	*state = NULL;
	pfree(value);
}

bool
CLOGAllocationRequired(void)
{
	return CLOGNativeIORequired();
}

bool
CLOGAllocationRangeReady(FullTransactionId first, FullTransactionId last,
						 ClusterClogAllocationV2 **state)
{
	uint64 begin, gap, total;
	ClusterClogAllocationV2 *value;

	if (!LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE)
		|| !CLOGAllocationSegments(first, last, &begin, &gap, &total)
		|| !CLOGAllocationPresence(begin, total))
		return false;
	value = *state;
	if (!value) {
		value = palloc0(offsetof(ClusterClogAllocationV2, files) + gap * sizeof(value->files[0]));
		value->first = begin;
		value->count = gap;
		*state = value;
		for (uint64 i = 0; i < gap; i++) {
			ClusterControlFileKeyV2 key = {0};

			key.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
			key.origin = cluster_node_id;
			key.object_id = (begin + i) % 4096;
			if (!cluster_control_native_file_acquire_v2(&key, &value->files[i])) {
				CLOGFinishAllocation(state);
				return false;
			}
		}
	}
	for (uint64 i = 0; i < gap; i++) {
		uint64 at = ((begin + i) % 4096 + 4096 - value->first) % 4096;

		if (at >= value->count || !cluster_control_native_file_current_v2(&value->files[at]))
			return false;
	}
	return true;
}

void
CLOGPrepareForAllocation(FullTransactionId first, FullTransactionId last,
						ClusterClogAllocationV2 **state)
{
	struct CLOGPrepareWork {
		ClusterControlNativeClogRangeV2 *ranges[2];
		bool attempted;
	} *work;
	uint64 begin, gap, total, tail;

	if (CritSectionCount || LWLockHeldByMe(XidGenLock) || LWLockHeldByMe(XactSLRULock)
		|| LWLockHeldByMe(XactTruncationLock) || LWLockHeldByMe(ControlFileLock)
		|| LWLockHeldByMe(RelationMappingLock) || cluster_cf_held(ShareLock)
		|| cluster_cf_held(ExclusiveLock) || cluster_node_id < 0 || cluster_node_id >= 128
		|| !CLOGAllocationSegments(first, last, &begin, &gap, &total))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("CLOG allocation preparation requires the original unlocked issuer")));
	CLOGFinishAllocation(state);
	tail = Min(total, 4096 - begin);
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		work->attempted = true;
		if (!cluster_cf_lock(ExclusiveLock))
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("CLOG allocation control owner is unavailable")));
		/* Publish both physical halves before borrowing either: publication
		 * cannot replace a table with live borrows. No full-XID epoch is inferred. */
		for (unsigned half = 0; half < (total == tail ? 1 : 2); half++) {
			uint64 lo = half ? 0 : begin, hi = half ? total - tail - 1 : begin + tail - 1;
			ClusterControlNativeResultV2 result
				= cluster_control_native_files_reserve_clog_range_locked_v2(
					cluster_node_id, lo, hi, &work->ranges[half]);

			if (result != CLUSTER_NATIVE_FILES_OK_V2 || !work->ranges[half]
				|| work->ranges[half]->first_segment != lo
				|| work->ranges[half]->last_segment != hi
				|| work->ranges[half]->count != hi - lo + 1)
				ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
								errmsg("could not prepare the complete CLOG allocation range")));
		}
		*state = palloc0(offsetof(ClusterClogAllocationV2, files)
						+ gap * sizeof((*state)->files[0]));
		(*state)->first = begin;
		(*state)->count = gap;
		for (uint64 i = 0; i < gap; i++) {
			uint64 segment = (begin + i) % 4096;
			ClusterControlNativeClogRangeV2 *range = work->ranges[segment < begin ? 1 : 0];
			ClusterControlNativeClogEntryV2 *entry = &range->entries[segment - range->first_segment];
			ClusterControlNativeFileBorrowV2 *borrow = &(*state)->files[i];
			ClusterControlFileKeyV2 key = {0};
			uint8 reference[CLUSTER_CLOG_FILE_REFERENCE_BYTES];

			key.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
			key.origin = cluster_node_id;
			key.object_id = segment;
			if ((entry->reserved
				 ? cluster_control_native_file_reserved_acquire_locked_v2(&range->config,
					CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, range->startup_generation,
					entry->batch_uuid, &entry->expected, borrow) != CLUSTER_NATIVE_FILES_OK_V2
				 : !cluster_control_native_file_acquire_v2(&key, borrow))
				|| borrow->expected.kind != key.kind || borrow->expected.origin != key.origin
				|| borrow->expected.object_id != segment
				|| !cluster_recovery_file_identity_encode_v2(&entry->expected, reference, sizeof(reference))
				|| !cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), &borrow->expected)
				|| borrow->startup_generation != range->startup_generation
				|| borrow->table_generation != work->ranges[total == tail ? 0 : 1]->table_generation
				|| !cluster_control_native_file_current_v2(borrow))
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("CLOG allocation lost its selected creator identity")));
		}
		if (!CLOGAllocationPresence(begin, total))
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("CLOG allocation range changed during preparation")));
	}
	PG_FINALLY();
	{
		for (unsigned half = 0; half < 2; half++)
			cluster_control_native_clog_range_destroy_v2(&work->ranges[half]);
		if (work->attempted && cluster_cf_held(ExclusiveLock)
			&& cluster_cf_unlock_confirmed(ExclusiveLock) != CLUSTER_CF_RELEASE_CONFIRMED)
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not return CLOG allocation control owner")));
		pfree(work);
	}
	PG_END_TRY();
}
#endif

/*
 * TransactionIdSetTreeStatus
 *
 * Record the final state of transaction entries in the commit log for
 * a transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be
 * the top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * lsn must be the WAL location of the commit record when recording an async
 * commit.  For a synchronous commit it can be InvalidXLogRecPtr, since the
 * caller guarantees the commit record is already flushed in that case.  It
 * should be InvalidXLogRecPtr for abort cases, too.
 *
 * In the commit case, atomicity is limited by whether all the subxids are in
 * the same CLOG page as xid.  If they all are, then the lock will be grabbed
 * only once, and the status will be set to committed directly.  Otherwise
 * we must
 *	 1. set sub-committed all subxids that are not on the same page as the
 *		main xid
 *	 2. atomically set committed the main xid and the subxids on the same page
 *	 3. go over the first bunch again and set them committed
 * Note that as far as concurrent checkers are concerned, main transaction
 * commit as a whole is still atomic.
 *
 * Example:
 *		TransactionId t commits and has subxids t1, t2, t3, t4
 *		t is on page p1, t1 is also on p1, t2 and t3 are on p2, t4 is on p3
 *		1. update pages2-3:
 *					page2: set t2,t3 as sub-committed
 *					page3: set t4 as sub-committed
 *		2. update page1:
 *					page1: set t,t1 as committed
 *		3. update pages2-3:
 *					page2: set t2,t3 as committed
 *					page3: set t4 as committed
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; functions in transam.c are the intended callers.
 *
 * XXX Think about issuing POSIX_FADV_WILLNEED on pages that we will need,
 * but aren't yet in cache, as well as hinting pages not to fall out of
 * cache yet.
 */
void
TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids, XidStatus status, XLogRecPtr lsn)
{
	int			pageno = TransactionIdToPage(xid);	/* get page of parent */
	int			i;

	Assert(status == TRANSACTION_STATUS_COMMITTED ||
		   status == TRANSACTION_STATUS_ABORTED);

	/*
	 * See how many subxids, if any, are on the same page as the parent, if
	 * any.
	 */
	for (i = 0; i < nsubxids; i++)
	{
		if (TransactionIdToPage(subxids[i]) != pageno)
			break;
	}

	/*
	 * Do all items fit on a single page?
	 */
	if (i == nsubxids)
	{
		/*
		 * Set the parent and all subtransactions in a single call
		 */
		TransactionIdSetPageStatus(xid, nsubxids, subxids, status, lsn,
								   pageno, true);
	}
	else
	{
		int			nsubxids_on_first_page = i;

		/*
		 * If this is a commit then we care about doing this correctly (i.e.
		 * using the subcommitted intermediate status).  By here, we know
		 * we're updating more than one page of clog, so we must mark entries
		 * that are *not* on the first page so that they show as subcommitted
		 * before we then return to update the status to fully committed.
		 *
		 * To avoid touching the first page twice, skip marking subcommitted
		 * for the subxids on that first page.
		 */
		if (status == TRANSACTION_STATUS_COMMITTED)
			set_status_by_pages(nsubxids - nsubxids_on_first_page,
								subxids + nsubxids_on_first_page,
								TRANSACTION_STATUS_SUB_COMMITTED, lsn);

		/*
		 * Now set the parent and subtransactions on same page as the parent,
		 * if any
		 */
		pageno = TransactionIdToPage(xid);
		TransactionIdSetPageStatus(xid, nsubxids_on_first_page, subxids, status,
								   lsn, pageno, false);

		/*
		 * Now work through the rest of the subxids one clog page at a time,
		 * starting from the second page onwards, like we did above.
		 */
		set_status_by_pages(nsubxids - nsubxids_on_first_page,
							subxids + nsubxids_on_first_page,
							status, lsn);
	}
}

/*
 * Helper for TransactionIdSetTreeStatus: set the status for a bunch of
 * transactions, chunking in the separate CLOG pages involved. We never
 * pass the whole transaction tree to this function, only subtransactions
 * that are on different pages to the top level transaction id.
 */
static void
set_status_by_pages(int nsubxids, TransactionId *subxids,
					XidStatus status, XLogRecPtr lsn)
{
	int			pageno = TransactionIdToPage(subxids[0]);
	int			offset = 0;
	int			i = 0;

	Assert(nsubxids > 0);		/* else the pageno fetch above is unsafe */

	while (i < nsubxids)
	{
		int			num_on_page = 0;
		int			nextpageno;

		do
		{
			nextpageno = TransactionIdToPage(subxids[i]);
			if (nextpageno != pageno)
				break;
			num_on_page++;
			i++;
		} while (i < nsubxids);

		TransactionIdSetPageStatus(InvalidTransactionId,
								   num_on_page, subxids + offset,
								   status, lsn, pageno, false);
		offset = i;
		pageno = nextpageno;
	}
}

/*
 * Record the final state of transaction entries in the commit log for all
 * entries on a single page.  Atomic only on this page.
 */
static void
TransactionIdSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids, XidStatus status,
						   XLogRecPtr lsn, int pageno,
						   bool all_xact_same_page)
{
	/* Can't use group update when PGPROC overflows. */
	StaticAssertDecl(THRESHOLD_SUBTRANS_CLOG_OPT <= PGPROC_MAX_CACHED_SUBXIDS,
					 "group clog threshold less than PGPROC cached subxids");

	/*
	 * When there is contention on XactSLRULock, we try to group multiple
	 * updates; a single leader process will perform transaction status
	 * updates for multiple backends so that the number of times XactSLRULock
	 * needs to be acquired is reduced.
	 *
	 * For this optimization to be safe, the XID and subxids in MyProc must be
	 * the same as the ones for which we're setting the status.  Check that
	 * this is the case.
	 *
	 * For this optimization to be efficient, we shouldn't have too many
	 * sub-XIDs and all of the XIDs for which we're adjusting clog should be
	 * on the same page.  Check those conditions, too.
	 */
	if (all_xact_same_page && xid == MyProc->xid &&
		nsubxids <= THRESHOLD_SUBTRANS_CLOG_OPT &&
		nsubxids == MyProc->subxidStatus.count &&
		(nsubxids == 0 ||
		 memcmp(subxids, MyProc->subxids.xids,
				nsubxids * sizeof(TransactionId)) == 0))
	{
		/*
		 * If we can immediately acquire XactSLRULock, we update the status of
		 * our own XID and release the lock.  If not, try use group XID
		 * update.  If that doesn't work out, fall back to waiting for the
		 * lock to perform an update for this transaction only.
		 */
		if (LWLockConditionalAcquire(XactSLRULock, LW_EXCLUSIVE))
		{
			/* Got the lock without waiting!  Do the update. */
			TransactionIdSetPageStatusInternal(xid, nsubxids, subxids, status,
											   lsn, pageno);
			LWLockRelease(XactSLRULock);
			return;
		}
		else if (TransactionGroupUpdateXidStatus(xid, status, lsn, pageno))
		{
			/* Group update mechanism has done the work. */
			return;
		}

		/* Fall through only if update isn't done yet. */
	}

	/* Group update not applicable, or couldn't accept this page number. */
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	TransactionIdSetPageStatusInternal(xid, nsubxids, subxids, status,
									   lsn, pageno);
	LWLockRelease(XactSLRULock);
}

/*
 * Record the final state of transaction entry in the commit log
 *
 * We don't do any locking here; caller must handle that.
 */
static void
TransactionIdSetPageStatusInternal(TransactionId xid, int nsubxids,
								   TransactionId *subxids, XidStatus status,
								   XLogRecPtr lsn, int pageno)
{
	int			slotno;
	int			i;

	Assert(status == TRANSACTION_STATUS_COMMITTED ||
		   status == TRANSACTION_STATUS_ABORTED ||
		   (status == TRANSACTION_STATUS_SUB_COMMITTED && !TransactionIdIsValid(xid)));
	Assert(LWLockHeldByMeInMode(XactSLRULock, LW_EXCLUSIVE));

	/*
	 * If we're doing an async commit (ie, lsn is valid), then we must wait
	 * for any active write on the page slot to complete.  Otherwise our
	 * update could reach disk in that write, which will not do since we
	 * mustn't let it reach disk until we've done the appropriate WAL flush.
	 * But when lsn is invalid, it's OK to scribble on a page while it is
	 * write-busy, since we don't care if the update reaches disk sooner than
	 * we think.
	 */
	slotno = SimpleLruReadPage(XactCtl, pageno, XLogRecPtrIsInvalid(lsn), xid);

	/*
	 * Set the main transaction id, if any.
	 *
	 * If we update more than one xid on this page while it is being written
	 * out, we might find that some of the bits go to disk and others don't.
	 * If we are updating commits on the page with the top-level xid that
	 * could break atomicity, so we subcommit the subxids first before we mark
	 * the top-level commit.
	 */
	if (TransactionIdIsValid(xid))
	{
		/* Subtransactions first, if needed ... */
		if (status == TRANSACTION_STATUS_COMMITTED)
		{
			for (i = 0; i < nsubxids; i++)
			{
				Assert(XactCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
				TransactionIdSetStatusBit(subxids[i],
										  TRANSACTION_STATUS_SUB_COMMITTED,
										  lsn, slotno);
			}
		}

		/* ... then the main transaction */
		TransactionIdSetStatusBit(xid, status, lsn, slotno);
	}

	/* Set the subtransactions */
	for (i = 0; i < nsubxids; i++)
	{
		Assert(XactCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		TransactionIdSetStatusBit(subxids[i], status, lsn, slotno);
	}

	XactCtl->shared->page_dirty[slotno] = true;
}

/*
 * ClusterClogAdoptNativeStatus
 *		PGRAC (GCS-race round-2 RC-E): SLRU-coherent repair write of one
 *		native-era commit status during the post-recovery prehistory verify.
 *
 * A pre-seed base-backup joiner adopts the sealed native prehistory into
 * its pg_xact files before recovery; replaying the backup's WAL window can
 * then re-zero a CLOG page the adopt had filled (CLOG extend records), so
 * xids beyond the replay window on that page read IN_PROGRESS again.  The
 * StartupXLOG-tail verify detects those holes against the CRC-validated
 * blob and repairs them HERE -- through the SLRU, never behind its back --
 * before the coverage latch may enable native-era LOCAL routing.
 *
 * No WAL: the authoritative WAL for these bits is the seed node's own
 * native-era history, already durable in the sealed blob; a crash before
 * the next CLOG flush just re-runs the same idempotent verify+repair on
 * the following boot.  Runs single-threaded (startup process before
 * backends are admitted), status must be terminal, and the slot must
 * currently read IN_PROGRESS (TransactionIdSetStatusBit asserts that).
 */
void
ClusterClogAdoptNativeStatus(TransactionId xid, XidStatus status)
{
	int			pageno = TransactionIdToPage(xid);
	int			slotno;

	Assert(TransactionIdIsNormal(xid));

	/*
	 * Terminal-only, enforced at RUNTIME (round-2 review F2 / calibration 1):
	 * a sealed prehistory can legitimately carry SUB_COMMITTED bits (a
	 * native-era crash mid-subcommit leaves them behind, and the blob has no
	 * child->parent map to resolve them), and an Assert vanishes in
	 * production builds.  Materializing a non-terminal status as local truth
	 * would launder an unprovable state; fail closed instead.
	 */
	if (status != TRANSACTION_STATUS_COMMITTED &&
		status != TRANSACTION_STATUS_ABORTED)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot adopt non-terminal native-era commit status %d for transaction %u",
						(int) status, xid),
				 errhint("The sealed native prehistory carries an unresolvable status; "
						 "this node must stay fail-closed.")));

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	slotno = SimpleLruReadPage(XactCtl, pageno, true, xid);
	TransactionIdSetStatusBit(xid, status, InvalidXLogRecPtr, slotno);
	XactCtl->shared->page_dirty[slotno] = true;
	LWLockRelease(XactSLRULock);
}

/*
 * When we cannot immediately acquire XactSLRULock in exclusive mode at
 * commit time, add ourselves to a list of processes that need their XIDs
 * status update.  The first process to add itself to the list will acquire
 * XactSLRULock in exclusive mode and set transaction status as required
 * on behalf of all group members.  This avoids a great deal of contention
 * around XactSLRULock when many processes are trying to commit at once,
 * since the lock need not be repeatedly handed off from one committing
 * process to the next.
 *
 * Returns true when transaction status has been updated in clog; returns
 * false if we decided against applying the optimization because the page
 * number we need to update differs from those processes already waiting.
 */
static bool
TransactionGroupUpdateXidStatus(TransactionId xid, XidStatus status,
								XLogRecPtr lsn, int pageno)
{
	volatile PROC_HDR *procglobal = ProcGlobal;
	PGPROC	   *proc = MyProc;
	uint32		nextidx;
	uint32		wakeidx;

	/* We should definitely have an XID whose status needs to be updated. */
	Assert(TransactionIdIsValid(xid));

	/*
	 * Add ourselves to the list of processes needing a group XID status
	 * update.
	 */
	proc->clogGroupMember = true;
	proc->clogGroupMemberXid = xid;
	proc->clogGroupMemberXidStatus = status;
	proc->clogGroupMemberPage = pageno;
	proc->clogGroupMemberLsn = lsn;

	nextidx = pg_atomic_read_u32(&procglobal->clogGroupFirst);

	while (true)
	{
		/*
		 * Add the proc to list, if the clog page where we need to update the
		 * current transaction status is same as group leader's clog page.
		 *
		 * There is a race condition here, which is that after doing the below
		 * check and before adding this proc's clog update to a group, the
		 * group leader might have already finished the group update for this
		 * page and becomes group leader of another group. This will lead to a
		 * situation where a single group can have different clog page
		 * updates.  This isn't likely and will still work, just maybe a bit
		 * less efficiently.
		 */
		if (nextidx != INVALID_PGPROCNO &&
			ProcGlobal->allProcs[nextidx].clogGroupMemberPage != proc->clogGroupMemberPage)
		{
			/*
			 * Ensure that this proc is not a member of any clog group that
			 * needs an XID status update.
			 */
			proc->clogGroupMember = false;
			pg_atomic_write_u32(&proc->clogGroupNext, INVALID_PGPROCNO);
			return false;
		}

		pg_atomic_write_u32(&proc->clogGroupNext, nextidx);

		if (pg_atomic_compare_exchange_u32(&procglobal->clogGroupFirst,
										   &nextidx,
										   (uint32) proc->pgprocno))
			break;
	}

	/*
	 * If the list was not empty, the leader will update the status of our
	 * XID. It is impossible to have followers without a leader because the
	 * first process that has added itself to the list will always have
	 * nextidx as INVALID_PGPROCNO.
	 */
	if (nextidx != INVALID_PGPROCNO)
	{
		int			extraWaits = 0;

		/* Sleep until the leader updates our XID status. */
		pgstat_report_wait_start(WAIT_EVENT_XACT_GROUP_UPDATE);
		for (;;)
		{
			/* acts as a read barrier */
			PGSemaphoreLock(proc->sem);
			if (!proc->clogGroupMember)
				break;
			extraWaits++;
		}
		pgstat_report_wait_end();

		Assert(pg_atomic_read_u32(&proc->clogGroupNext) == INVALID_PGPROCNO);

		/* Fix semaphore count for any absorbed wakeups */
		while (extraWaits-- > 0)
			PGSemaphoreUnlock(proc->sem);
		return true;
	}

	/* We are the leader.  Acquire the lock on behalf of everyone. */
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

	/*
	 * Now that we've got the lock, clear the list of processes waiting for
	 * group XID status update, saving a pointer to the head of the list.
	 * Trying to pop elements one at a time could lead to an ABA problem.
	 */
	nextidx = pg_atomic_exchange_u32(&procglobal->clogGroupFirst,
									 INVALID_PGPROCNO);

	/* Remember head of list so we can perform wakeups after dropping lock. */
	wakeidx = nextidx;

	/* Walk the list and update the status of all XIDs. */
	while (nextidx != INVALID_PGPROCNO)
	{
		PGPROC	   *nextproc = &ProcGlobal->allProcs[nextidx];

		/*
		 * Transactions with more than THRESHOLD_SUBTRANS_CLOG_OPT sub-XIDs
		 * should not use group XID status update mechanism.
		 */
		Assert(nextproc->subxidStatus.count <= THRESHOLD_SUBTRANS_CLOG_OPT);

		TransactionIdSetPageStatusInternal(nextproc->clogGroupMemberXid,
										   nextproc->subxidStatus.count,
										   nextproc->subxids.xids,
										   nextproc->clogGroupMemberXidStatus,
										   nextproc->clogGroupMemberLsn,
										   nextproc->clogGroupMemberPage);

		/* Move to next proc in list. */
		nextidx = pg_atomic_read_u32(&nextproc->clogGroupNext);
	}

	/* We're done with the lock now. */
	LWLockRelease(XactSLRULock);

	/*
	 * Now that we've released the lock, go back and wake everybody up.  We
	 * don't do this under the lock so as to keep lock hold times to a
	 * minimum.
	 */
	while (wakeidx != INVALID_PGPROCNO)
	{
		PGPROC	   *wakeproc = &ProcGlobal->allProcs[wakeidx];

		wakeidx = pg_atomic_read_u32(&wakeproc->clogGroupNext);
		pg_atomic_write_u32(&wakeproc->clogGroupNext, INVALID_PGPROCNO);

		/* ensure all previous writes are visible before follower continues. */
		pg_write_barrier();

		wakeproc->clogGroupMember = false;

		if (wakeproc != MyProc)
			PGSemaphoreUnlock(wakeproc->sem);
	}

	return true;
}

/*
 * Sets the commit status of a single transaction.
 *
 * Must be called with XactSLRULock held
 */
static void
TransactionIdSetStatusBit(TransactionId xid, XidStatus status, XLogRecPtr lsn, int slotno)
{
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	char	   *byteptr;
	char		byteval;
	char		curval;

	byteptr = XactCtl->shared->page_buffer[slotno] + byteno;
	curval = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

	/*
	 * When replaying transactions during recovery we still need to perform
	 * the two phases of subcommit and then commit. However, some transactions
	 * are already correctly marked, so we just treat those as a no-op which
	 * allows us to keep the following Assert as restrictive as possible.
	 */
	if (InRecovery && status == TRANSACTION_STATUS_SUB_COMMITTED &&
		curval == TRANSACTION_STATUS_COMMITTED)
		return;

	/*
	 * Current state change should be from 0 or subcommitted to target state
	 * or we should already be there when replaying changes during recovery.
	 */
	Assert(curval == 0 ||
		   (curval == TRANSACTION_STATUS_SUB_COMMITTED &&
			status != TRANSACTION_STATUS_IN_PROGRESS) ||
		   curval == status);

	/* note this assumes exclusive access to the clog page */
	byteval = *byteptr;
	byteval &= ~(((1 << CLOG_BITS_PER_XACT) - 1) << bshift);
	byteval |= (status << bshift);
	*byteptr = byteval;

	/*
	 * Update the group LSN if the transaction completion LSN is higher.
	 *
	 * Note: lsn will be invalid when supplied during InRecovery processing,
	 * so we don't need to do anything special to avoid LSN updates during
	 * recovery. After recovery completes the next clog change will set the
	 * LSN correctly.
	 */
	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			lsnindex = GetLSNIndex(slotno, xid);

		if (XactCtl->shared->group_lsn[lsnindex] < lsn)
			XactCtl->shared->group_lsn[lsnindex] = lsn;
	}
}

/*
 * Interrogate the state of a transaction in the commit log.
 *
 * Aside from the actual commit status, this function returns (into *lsn)
 * an LSN that is late enough to be able to guarantee that if we flush up to
 * that LSN then we will have flushed the transaction's commit record to disk.
 * The result is not necessarily the exact LSN of the transaction's commit
 * record!	For example, for long-past transactions (those whose clog pages
 * already migrated to disk), we'll return InvalidXLogRecPtr.  Also, because
 * we group transactions on the same clog page to conserve storage, we might
 * return the LSN of a later transaction that falls into the same group.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionLogFetch() in transam.c is the intended caller.
 */
XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	int			pageno = TransactionIdToPage(xid);
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	int			slotno;
	int			lsnindex;
	char	   *byteptr;
	XidStatus	status;

	/* lock is acquired by SimpleLruReadPage_ReadOnly */

	slotno = SimpleLruReadPage_ReadOnly(XactCtl, pageno, xid);
	byteptr = XactCtl->shared->page_buffer[slotno] + byteno;

	status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

	lsnindex = GetLSNIndex(slotno, xid);
	*lsn = XactCtl->shared->group_lsn[lsnindex];

	LWLockRelease(XactSLRULock);

	return status;
}

/*
 * Number of shared CLOG buffers.
 *
 * On larger multi-processor systems, it is possible to have many CLOG page
 * requests in flight at one time which could lead to disk access for CLOG
 * page if the required page is not found in memory.  Testing revealed that we
 * can get the best performance by having 128 CLOG buffers, more than that it
 * doesn't improve performance.
 *
 * Unconditionally keeping the number of CLOG buffers to 128 did not seem like
 * a good idea, because it would increase the minimum amount of shared memory
 * required to start, which could be a problem for people running very small
 * configurations.  The following formula seems to represent a reasonable
 * compromise: people with very low values for shared_buffers will get fewer
 * CLOG buffers as well, and everyone else will get 128.
 */
Size
CLOGShmemBuffers(void)
{
	return Min(128, Max(4, NBuffers / 512));
}

/*
 * Initialization of shared memory for CLOG
 */
Size
CLOGShmemSize(void)
{
	return SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE);
}

void
CLOGShmemInit(void)
{
	XactCtl->PagePrecedes = CLOGPagePrecedes;
#ifdef USE_PGRAC_CLUSTER
	CLOGConfigureManagedIO(XactCtl);
#endif
	SimpleLruInit(XactCtl, "Xact", CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE,
				  XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER,
				  SYNC_HANDLER_CLOG);
	SlruPagePrecedesUnitTests(XactCtl, CLOG_XACTS_PER_PAGE);
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CLOG segment.  (The CLOG directory is assumed to
 * have been created by initdb, and CLOGShmemInit must have been
 * called already.)
 */
void
BootStrapCLOG(void)
{
	int			slotno;

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCLOGPage(0, false);

	/* Make sure it's written out */
	SimpleLruWritePage(XactCtl, slotno);
	Assert(!XactCtl->shared->page_dirty[slotno]);

	LWLockRelease(XactSLRULock);
}

/* The original BootStrapXLOG caller supplies its independently allocated
 * complete identity and actual initial nextXid. The creator, not these bytes,
 * owns the unpublished directory and closed-writer context. No bootstrap WAL.
 * Leave the cache empty: its later managed read must acquire file provenance. */
bool
BootStrapCLOGManagedV2(const ClusterRecoveryFileIdentityV2 *expected,
					 FullTransactionId actual_initial_next, ClusterRecoveryBootFileV2 *out)
{
#if defined(USE_PGRAC_CLUSTER) && !defined(WIN32)
	char path[MAXPGPATH];
	uint8 reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];
	ClusterRecoveryFileIdentityV2 captured;
	ClusterRecoveryBootFileV2 observed;
	volatile int fd = -1;
	volatile bool ok = false;
	uintptr_t a = (uintptr_t)expected;
	uintptr_t b = (uintptr_t)out;

	if (!IsBootstrapProcessingMode() || IsUnderPostmaster || InRecovery || !enableFsync
		|| expected == NULL || out == NULL
		|| (a <= b ? b - a < sizeof(*expected) : a - b < sizeof(*out))
		|| expected->kind != CLUSTER_RECOVERY_FILE_CLOG_V2 || expected->object_id != 0
		|| expected->database_oid != 0 || expected->tablespace_oid != 0
		|| expected->system_identifier != GetSystemIdentifier()
		|| U64FromFullTransactionId(actual_initial_next) != FirstNormalTransactionId
		|| !cluster_recovery_file_identity_encode_v2(expected, reference, sizeof(reference))
		|| XactCtl->Dir[0] == '\0'
		|| strnlen(XactCtl->Dir, sizeof(XactCtl->Dir)) == sizeof(XactCtl->Dir))
		return false;
	captured = *expected;
	if (snprintf(path, sizeof(path), "%s/0000", XactCtl->Dir) >= sizeof(path))
		return false;
	PG_TRY();
	{
		fd = OpenTransientFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY | O_NOFOLLOW);
		if (fd >= 0) {
			struct stat before, named;

			ok = cluster_clog_bootstrap_v2(fd, &captured, actual_initial_next, &observed)
				&& fstat(fd, &before) == 0;
			if (ok) {
				/* The whole file was synchronized and read back before the name
				 * is made durable. Original owner excludes directory replacement. */
				fsync_fname(XactCtl->Dir, true);
				ok = lstat(path, &named) == 0 && S_ISREG(named.st_mode)
					&& before.st_dev == named.st_dev && before.st_ino == named.st_ino
					&& before.st_size == named.st_size;
			}
		}
	}
	PG_FINALLY();
	{
		if (fd >= 0 && CloseTransientFile(fd) != 0)
			ok = false;
	}
	PG_END_TRY();
	if (!ok)
		return false;
	*out = observed;
	return true;
#else
	(void)expected; (void)actual_initial_next; (void)out;
	return false;
#endif
}

/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is true, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCLOGPage(int pageno, bool writeXlog)
{
	int			slotno;

	slotno = SimpleLruZeroPage(XactCtl, pageno);

	if (writeXlog)
		WriteZeroPageXlogRec(pageno);

	return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 */
void
StartupCLOG(void)
{
	TransactionId xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	int			pageno = TransactionIdToPage(xid);

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

	/*
	 * Initialize our idea of the latest page number.
	 */
	XactCtl->shared->latest_page_number = pageno;

	LWLockRelease(XactSLRULock);
}

#ifdef USE_PGRAC_CLUSTER
/* The original Startup owner supplies nextXid before connections can allocate.
 * A raw page match is insufficient: consume the selected native page and its
 * actual issuer birth in the same FullXid interval. No epoch is manufactured
 * for the page, and a shared startup still cannot erase retained status. */
static void
CLOGTrimNative(FullTransactionId next)
{
	TransactionId xid = XidFromFullTransactionId(next);
	int pageno = TransactionIdToPage(xid);
	volatile bool body_complete = false;

	if (!(MyBackendType == B_STARTUP
		  || (!cluster_shared_config && !IsUnderPostmaster && !IsPostmasterEnvironment
			  && MyBackendType == B_STANDALONE_BACKEND && IsInitProcessingMode()))
		|| InRecovery || !enableFsync
		|| !TransactionIdIsNormal(xid) || U64FromFullTransactionId(next) == PG_UINT64_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("native CLOG trim requires the original completed startup cut")));
	if (TransactionIdToPgIndex(xid) == 0)
		return; /* The next page need not have been born yet. */

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	PG_TRY();
	{
		int slotno = SimpleLruReadPage(XactCtl, pageno, false, xid);
		ClusterClogPageIdentityV2 *page;
		TransactionId birth;

		/* The managed reader keeps this SAME family lock at return. It has
		 * checked the original file reference and full frame, including a
		 * resident hit; the control owner must drain that lock before reuse. */
		if (!CLOGNativeIORequired() || !FullTransactionIdEquals(next, ShmemVariableCache->nextXid)
			|| slotno < 0 || slotno >= XactCtl->shared->num_slots
			|| XactCtl->shared->clog_page_identity == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("native CLOG startup cut changed before trim")));
		page = &XactCtl->shared->clog_page_identity[slotno];
		birth = XidFromFullTransactionId(page->identity.birth_full_xid);
		if (!page->identity.birth_observed || page->identity.logical_page != (uint64)pageno
			|| !TransactionIdIsNormal(birth)
			|| (birth % CLOG_XACTS_PER_PAGE != 0 && birth != FirstNormalTransactionId)
			|| TransactionIdToPage(birth) != pageno
			|| EpochFromFullTransactionId(page->identity.birth_full_xid) != EpochFromFullTransactionId(next)
			|| FullTransactionIdFollows(page->identity.birth_full_xid, next))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("native CLOG page birth does not match the startup FullXid cut")));
		if (cluster_shared_config) {
			if (!cluster_native_clog_suffix_unused(XactCtl->shared->page_buffer[slotno], xid))
				ereport(FATAL,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("shared CLOG startup has transaction status beyond nextXid")));
		} else {
			int byteno = TransactionIdToByte(xid);
			int bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
			char *byteptr = XactCtl->shared->page_buffer[slotno] + byteno;

			/* Preserve the original prefix, birth and source/group LSNs. The
			 * original dirty-page writer emits and flushes the full native WAL
			 * image before physical writeback; no raw ZERO record is emitted. */
			*byteptr &= (1 << bshift) - 1;
			MemSet(byteptr + 1, 0, BLCKSZ - byteno - 1);
			XactCtl->shared->page_dirty[slotno] = true;
		}
		body_complete = true;
	}
	PG_FINALLY();
	{
		/* ERROR reset the holdoff while this original family lock stayed
		 * held. Discard any subsequent open-catch reacquire count before
		 * compensating this release; caller abort cleanup owns older locks. */
		if (!body_complete) {
			InterruptHoldoffCount = 0;
			HOLD_INTERRUPTS();
		}
		LWLockRelease(XactSLRULock);
	}
	PG_END_TRY();
}
#endif

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCLOG(void)
{
	TransactionId xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	int			pageno = TransactionIdToPage(xid);

#ifdef USE_PGRAC_CLUSTER
	if (CLOGNativeIORequired()) {
		CLOGTrimNative(ShmemVariableCache->nextXid);
		return;
	}
	/* A completed qualified input, not a guessed crash horizon, owns this
	 * read-only startup cut. InRecovery would permit a missing SLRU page to
	 * be fabricated as zero, so it must already have ended. */
	if (cluster_shared_config
		&& (MyBackendType != B_STARTUP || InRecovery || !TransactionIdIsNormal(xid)))
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("shared CLOG startup requires a completed recovery input")));
#endif
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

	/*
	 * Zero out the remainder of the current clog page.  Under normal
	 * circumstances it should be zeroes already, but it seems at least
	 * theoretically possible that XLOG replay will have settled on a nextXID
	 * value that is less than the last XID actually used and marked by the
	 * previous database lifecycle (since subtransaction commit writes clog
	 * but makes no WAL entry).  Let's just be safe. (We need not worry about
	 * pages beyond the current one, since those will be zeroed when first
	 * used.  For the same reason, there is no need to do anything when
	 * nextXid is exactly at a page boundary; and it's likely that the
	 * "current" page doesn't exist yet in that case.)
	 */
	if (TransactionIdToPgIndex(xid) != 0)
	{
		int			byteno = TransactionIdToByte(xid);
		int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
		int			slotno;
		char	   *byteptr;

		slotno = SimpleLruReadPage(XactCtl, pageno, false, xid);
		byteptr = XactCtl->shared->page_buffer[slotno] + byteno;

#ifdef USE_PGRAC_CLUSTER
		if (cluster_shared_config) {
			bool unused
				= cluster_native_clog_suffix_unused(XactCtl->shared->page_buffer[slotno], xid);
			LWLockRelease(XactSLRULock);
			if (!unused)
				ereport(FATAL,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("shared CLOG startup has transaction status beyond nextXid")));
			return;
		}
#endif
		/* Zero so-far-unused positions in the current byte */
		*byteptr &= (1 << bshift) - 1;
		/* Zero the rest of the page */
		MemSet(byteptr + 1, 0, BLCKSZ - byteno - 1);

		XactCtl->shared->page_dirty[slotno] = true;
	}

	LWLockRelease(XactSLRULock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCLOG(void)
{
	/*
	 * Write dirty CLOG pages to disk.  This may result in sync requests
	 * queued for later handling by ProcessSyncRequests(), as part of the
	 * checkpoint.
	 */
	TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(true);
	SimpleLruWriteAll(XactCtl, true);
	TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(true);
}


/*
 * Make sure that CLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
static void
ExtendCLOGInternal(TransactionId newestXact, FullTransactionId birth)
{
	int			pageno;
	int			slotno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
#ifdef USE_PGRAC_CLUSTER
	if (CLOGNativeIORequired())
	{
		volatile bool body_complete = false;

		PG_TRY();
		{
			(void) SimpleLruNativeClogZeroV2(XactCtl, birth);
			body_complete = true;
		}
		PG_FINALLY();
		{
			/* ERROR has reset the holdoff for this still-held family lock. */
			if (!body_complete)
				HOLD_INTERRUPTS();
			LWLockRelease(XactSLRULock);
		}
		PG_END_TRY();
		return;
	}
#endif

	/* Zero the page and make an XLOG entry about it. Raw-only callers leave
	 * birth unknown; only the original allocator supplies a complete identity. */
	slotno = ZeroCLOGPage(pageno, true);
	if (FullTransactionIdIsValid(birth))
	{
		ClusterClogPageIdentityV2 *page = &XactCtl->shared->clog_page_identity[slotno];

		page->identity.logical_page = pageno;
		page->identity.page_generation = U64FromFullTransactionId(birth);
		page->identity.birth_observed = true;
		page->identity.birth_full_xid = birth;
	}

	LWLockRelease(XactSLRULock);
}


void
ExtendCLOG(TransactionId newestXact)
{
	ExtendCLOGInternal(newestXact, InvalidFullTransactionId);
}

void
ExtendCLOGFull(FullTransactionId newestXact)
{
	if (!FullTransactionIdIsValid(newestXact) ||
		!TransactionIdIsNormal(XidFromFullTransactionId(newestXact)) ||
		U64FromFullTransactionId(newestXact) == PG_UINT64_MAX ||
		!LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE) ||
		XactCtl->shared == NULL || XactCtl->shared->clog_page_identity == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("CLOG page birth requires the original allocator identity")));

	ExtendCLOGInternal(XidFromFullTransactionId(newestXact), newestXact);
}


/*
 * Remove all CLOG segments before the one holding the passed transaction ID
 *
 * Before removing any CLOG data, we must flush XLOG to disk, to ensure
 * that any recently-emitted FREEZE_PAGE records have reached disk; otherwise
 * a crash and restart might leave us with some unfrozen tuples referencing
 * removed CLOG data.  We choose to emit a special TRUNCATE XLOG record too.
 * Replaying the deletion from XLOG is not critical, since the files could
 * just as well be removed later, but doing so prevents a long-running hot
 * standby server from acquiring an unreasonably bloated CLOG directory.
 *
 * Since CLOG segments hold a large number of transactions, the opportunity to
 * actually remove a segment is fairly rare, and so it seems best not to do
 * the XLOG flush unless we have confirmed that there is a removable segment.
 */
void
TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid)
{
	int			cutoffPage;

#ifdef USE_PGRAC_CLUSTER
	CLOGRequireLegacyLifecycle();
#endif
	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!SlruScanDirectory(XactCtl, SlruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

	/*
	 * Advance oldestClogXid before truncating clog, so concurrent xact status
	 * lookups can ensure they don't attempt to access truncated-away clog.
	 *
	 * It's only necessary to do this if we will actually truncate away clog
	 * pages.
	 */
	AdvanceOldestClogXid(oldestXact);

	/*
	 * Write XLOG record and flush XLOG to disk. We record the oldest xid
	 * we're keeping information about here so we can ensure that it's always
	 * ahead of clog truncation in case we crash, and so a standby finds out
	 * the new valid xid before the next checkpoint.
	 */
	WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid);

	/* Now we can remove the old CLOG segment(s) */
	SimpleLruTruncate(XactCtl, cutoffPage);
}


/*
 * Decide whether a CLOG page number is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, TransactionIdPrecedes()
 * would get weird about permanent xact IDs.  So, offset both such that xid1,
 * xid2, and xid2 + CLOG_XACTS_PER_PAGE - 1 are all normal XIDs; this offset
 * is relevant to page 0 and to the page preceding page 0.
 *
 * The page containing oldestXact-2^31 is the important edge case.  The
 * portion of that page equaling or following oldestXact-2^31 is expendable,
 * but the portion preceding oldestXact-2^31 is not.  When oldestXact-2^31 is
 * the first XID of a page and segment, the entire page and segment is
 * expendable, and we could truncate the segment.  Recognizing that case would
 * require making oldestXact, not just the page containing oldestXact,
 * available to this callback.  The benefit would be rare and small, so we
 * don't optimize that edge case.
 */
static bool
CLOGPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId + 1;
	xid2 = ((TransactionId) page2) * CLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId + 1;

	return (TransactionIdPrecedes(xid1, xid2) &&
			TransactionIdPrecedes(xid1, xid2 + CLOG_XACTS_PER_PAGE - 1));
}


/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroPageXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	(void) XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes
 * in TruncateCLOG().
 */
static void
WriteTruncateXlogRec(int pageno, TransactionId oldestXact, Oid oldestXactDb)
{
	XLogRecPtr	recptr;
	xl_clog_truncate xlrec;

	xlrec.pageno = pageno;
	xlrec.oldestXact = oldestXact;
	xlrec.oldestXactDb = oldestXactDb;

	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), sizeof(xl_clog_truncate));
	recptr = XLogInsert(RM_CLOG_ID, CLOG_TRUNCATE);
	XLogFlush(recptr);
}

/*
 * CLOG resource manager's routines
 */
void
clog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

#ifdef USE_PGRAC_CLUSTER
	if (CLOGNativeIORequired())
	{
		/* This original rmgr call owns Startup's retained source/cut. WAL
		 * supplies neither a selected file registration nor foreign authority. */
		if (info != CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2 ||
			XLogRecHasAnyBlockRefs(record) ||
			record->ReadRecPtr == InvalidXLogRecPtr ||
			record->EndRecPtr == PG_UINT64_MAX || record->EndRecPtr <= record->ReadRecPtr)
			CLOGRequireLegacyLifecycle();
		SimpleLruNativeClogReplayV2(XactCtl, XLogRecGetData(record),
								  XLogRecGetDataLen(record), record->EndRecPtr);
		return;
	}
#endif
	/* Backup blocks are not used in clog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == CLOG_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

		slotno = ZeroCLOGPage(pageno, false);
		SimpleLruWritePage(XactCtl, slotno);
		Assert(!XactCtl->shared->page_dirty[slotno]);

		LWLockRelease(XactSLRULock);
	}
	else if (info == CLOG_TRUNCATE)
	{
		xl_clog_truncate xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_clog_truncate));

		AdvanceOldestClogXid(xlrec.oldestXact);

		SimpleLruTruncate(XactCtl, xlrec.pageno);
	}
	else
		elog(PANIC, "clog_redo: unknown op code %u", info);
}

/*
 * Entrypoint for sync.c to sync clog files.
 */
int
clogsyncfiletag(const FileTag *ftag, char *path)
{
#ifdef USE_PGRAC_CLUSTER
	if (CLOGNativeIORequired())
	{
		/* Preserve an observable failed sync request; never open a replacement
		 * generation using an old raw segment-only tag. */
		snprintf(path, MAXPGPATH, "%s/%04X", XactCtl->Dir, ftag->segno);
		errno = ESTALE;
		return -1;
	}
#endif
	return SlruSyncFileTag(XactCtl, ftag, path);
}
