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
/*-------------------------------------------------------------------------
 * PGRAC MODIFICATIONS
 *     Modified by: SqlRush <sqlrush@gmail.com>
 *
 *     Add original-origin recovery I/O, declared Startup CLOG inputs and
 *     the dormant FullXid page-birth producer.
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/proc.h"
#include "storage/sync.h"

#ifdef USE_PGRAC_CLUSTER
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "cluster/cluster_guc.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_side_projection.h"
#include "cluster/cluster_undo_recovery.h"
#include "storage/fd.h"
#include "storage/shmem.h"
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

#ifdef USE_PGRAC_CLUSTER
#define CLOG_RECOVERY_ORIGINS 128
#define CLOG_RECOVERY_BUFFERS 2
static SlruCtlData ClogRecoveryCtl[CLOG_RECOVERY_ORIGINS];
static LWLockPadded *ClogRecoveryLocks;
typedef struct ClogRelmapMutation {
	pg_atomic_uint64 generation;
	pg_atomic_uint32 writers;
} ClogRelmapMutation;
static ClogRelmapMutation *ClogRelmapMutations;

static void clog_recovery_shmem_init(void);
static void clog_relmap_native_mutation(int origin);
static void clog_relmap_native_mutation_end(int origin);
#endif


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

#ifdef USE_PGRAC_CLUSTER
	clog_relmap_native_mutation(cluster_node_id);
#endif
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
#ifdef USE_PGRAC_CLUSTER
	clog_relmap_native_mutation_end(cluster_node_id);
#endif
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

#ifdef USE_PGRAC_CLUSTER
/* Only the original startup owner may restore a failed origin's native bits.
 * Its complete terminal WAL, base/era and serialization proofs remain caller
 * obligations; this storage API cannot turn an observed xid into authority. */
static bool
clog_recovery_initialized(int origin)
{
	return cluster_shared_config && AmStartupProcess() && origin >= 0
		   && origin < CLOG_RECOVERY_ORIGINS && origin != cluster_node_id
		   && ClogRecoveryLocks != NULL && ClogRecoveryCtl[origin].shared != NULL;
}

static bool
clog_recovery_permitted(int origin)
{
	return InRecovery && clog_recovery_initialized(origin)
		   && cluster_undo_recovery_cold_origin_authorized_v1(origin);
}

static bool
clog_recovery_terminal_permitted(int origin, TransactionId xid, int count, XidStatus status)
{
	/* The native controller remains the sole cache for our own origin.
	 * Only an exact durable C0 ABORT may use it through this API. */
	if (origin == cluster_node_id)
		return cluster_shared_config && AmStartupProcess() && origin >= 0
			&& origin < CLOG_RECOVERY_ORIGINS && XactCtl->shared != NULL
			&& count == 0 && TransactionIdIsNormal(xid) && status == TRANSACTION_STATUS_ABORTED
			&& cluster_undo_recovery_abort_clog_authorized_v1(origin, xid);
	return clog_recovery_initialized(origin)
		   && ((InRecovery && cluster_undo_recovery_cold_origin_authorized_v1(origin))
			   || (count == 0 && TransactionIdIsNormal(xid) && status == TRANSACTION_STATUS_ABORTED
				   && cluster_undo_recovery_abort_clog_authorized_v1(origin, xid)));
}

static void
clog_recovery_cache_discard(SlruCtl ctl)
{
	SlruShared shared = ctl->shared;

	if (!LWLockHeldByMe(shared->ControlLock))
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
	for (int slot = 0; slot < shared->num_slots; slot++) {
		if ((shared->page_status[slot] == SLRU_PAGE_READ_IN_PROGRESS
			 || shared->page_status[slot] == SLRU_PAGE_WRITE_IN_PROGRESS)
			&& LWLockHeldByMe(&shared->buffer_locks[slot].lock))
			LWLockRelease(&shared->buffer_locks[slot].lock);
		shared->page_status[slot] = SLRU_PAGE_EMPTY;
		shared->page_initial_full_xid[slot] = 0;
		shared->page_dirty[slot] = false;
	}
	LWLockRelease(shared->ControlLock);
}

/* FULL replay reloads its target from disk without dropping other actual
 * resident pages. Original SLRU eviction still loses their birth input.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
clog_recovery_cache_discard_page(SlruCtl ctl, int page)
{
	SlruShared shared = ctl->shared;

	if (!LWLockHeldByMe(shared->ControlLock))
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
	for (int slot = 0; slot < shared->num_slots; slot++) {
		if (shared->page_number[slot] != page)
			continue;
		if ((shared->page_status[slot] == SLRU_PAGE_READ_IN_PROGRESS
			 || shared->page_status[slot] == SLRU_PAGE_WRITE_IN_PROGRESS)
			&& LWLockHeldByMe(&shared->buffer_locks[slot].lock))
			LWLockRelease(&shared->buffer_locks[slot].lock);
		shared->page_status[slot] = SLRU_PAGE_EMPTY;
		shared->page_initial_full_xid[slot] = 0;
		shared->page_dirty[slot] = false;
	}
	LWLockRelease(shared->ControlLock);
}

/* A successful terminal read/write consumed its targets from disk. Only
 * untouched clean FULL slots retain their actual native initialization input;
 * cache residency does not certify era or continuous physical lifetime.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
clog_recovery_cache_discard_unknown(SlruCtl ctl)
{
	SlruShared shared = ctl->shared;

	if (!LWLockHeldByMe(shared->ControlLock))
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
	for (int slot = 0; slot < shared->num_slots; slot++) {
		if (shared->page_status[slot] == SLRU_PAGE_VALID
			&& !shared->page_dirty[slot] && shared->page_initial_full_xid[slot] != 0)
			continue;
		if ((shared->page_status[slot] == SLRU_PAGE_READ_IN_PROGRESS
			 || shared->page_status[slot] == SLRU_PAGE_WRITE_IN_PROGRESS)
			&& LWLockHeldByMe(&shared->buffer_locks[slot].lock))
			LWLockRelease(&shared->buffer_locks[slot].lock);
		shared->page_status[slot] = SLRU_PAGE_EMPTY;
		shared->page_initial_full_xid[slot] = 0;
		shared->page_dirty[slot] = false;
	}
	LWLockRelease(shared->ControlLock);
}

static bool
clog_native_directory_ready(int origin)
{
	const char *children[3] = { "native_side", NULL, "pg_xact" };
	int dirs[4] = { -1, -1, -1, -1 };
	char origin_name[32];
	char expected[MAXPGPATH];
	struct stat st, routed;
	int length;
	bool ok = false;

	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] != '/')
		return false;
	snprintf(origin_name, sizeof(origin_name), "origin_%d", origin);
	children[1] = origin_name;
	length = snprintf(expected, sizeof(expected), "%s/native_side/%s/pg_xact",
					  cluster_shared_data_dir, origin_name);
	if (length < 0 || length >= sizeof(expected))
		return false;
	if (origin == cluster_node_id) {
		char target[MAXPGPATH];
		ssize_t size;

		/* initdb installs this exact native SIDE route; do not redirect
		 * the controller or silently accept a different origin's link. */
		if (strcmp(XactCtl->Dir, "pg_xact") != 0)
			return false;
		size = readlink(XactCtl->Dir, target, sizeof(target) - 1);
		if (size < 0 || size >= sizeof(target) - 1)
			return false;
		target[size] = '\0';
		if (strcmp(target, expected) != 0)
			return false;
	} else if (strcmp(expected, ClogRecoveryCtl[origin].Dir) != 0)
		return false;
	dirs[0] = open(cluster_shared_data_dir, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (dirs[0] < 0)
		goto done;
	for (int i = 1; i < 4; i++) {
		dirs[i]
			= openat(dirs[i - 1], children[i - 1], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (dirs[i] < 0)
			goto done;
	}
	for (int i = 0; i < 4; i++)
		if (fstat(dirs[i], &st) != 0 || !S_ISDIR(st.st_mode) || st.st_uid != geteuid()
			|| (st.st_mode & (S_IWGRP | S_IWOTH)) != 0)
			goto done;
	if (lstat(expected, &routed) != 0 || !S_ISDIR(routed.st_mode) || routed.st_dev != st.st_dev
		|| routed.st_ino != st.st_ino)
		goto done;
	if (origin == cluster_node_id
		&& (stat(XactCtl->Dir, &routed) != 0 || !S_ISDIR(routed.st_mode)
			|| routed.st_dev != st.st_dev || routed.st_ino != st.st_ino))
		goto done;
	ok = true;
done:
	for (int i = 3; i >= 0; i--)
		if (dirs[i] >= 0 && close(dirs[i]) != 0)
			ok = false;
	return ok;
}

static bool
clog_recovery_terminal_directory_ready(int origin, TransactionId xid, int count, XidStatus status)
{
	/* Revalidation can ERROR; no raw directory fd crosses it. */
	return clog_recovery_terminal_permitted(origin, xid, count, status)
		&& clog_native_directory_ready(origin)
		&& clog_recovery_terminal_permitted(origin, xid, count, status);
}

static bool
clog_recovery_directory_ready(int origin)
{
	/* Invalid/nonterminal has no C0 permission; ZERO/TRUNCATE keep their
	 * ordinary origin qualification at every directory revalidation. */
	return clog_recovery_terminal_directory_ready(origin, InvalidTransactionId, 0,
												  TRANSACTION_STATUS_IN_PROGRESS);
}

/* Terminal replay requires a real complete page. Only a separate, proved
 * native ZERO/TRUNCATE owner may justify an absent base. */
static bool
clog_recovery_page_identity(SlruCtl ctl, int page, struct stat *out)
{
	char path[MAXPGPATH];
	int length = snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, page / SLRU_PAGES_PER_SEGMENT);

	return length >= 0 && length < sizeof(path) && lstat(path, out) == 0 && S_ISREG(out->st_mode)
		   && out->st_nlink == 1 && out->st_uid == geteuid()
		   && (out->st_mode & (S_IWGRP | S_IWOTH)) == 0
		   && out->st_size >= (off_t)(page % SLRU_PAGES_PER_SEGMENT + 1) * BLCKSZ;
}

static bool
clog_recovery_same_file(const struct stat *before, const struct stat *after)
{
	return before->st_dev == after->st_dev && before->st_ino == after->st_ino
		   && before->st_size == after->st_size;
}

/* Generic sync helpers may tolerate directory errors or ignore a sync-fd
 * close error. Neither establishes cold recovery durability. */
static bool
clog_recovery_sync(SlruCtl ctl, int page, const struct stat *expected)
{
	bool directory = page < 0;
	char path[MAXPGPATH];
	struct stat before, opened, after;
	volatile int fd = -1;
	volatile bool ok = false;
	int length;

	length = directory
				 ? snprintf(path, sizeof(path), "%s", ctl->Dir)
				 : snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, page / SLRU_PAGES_PER_SEGMENT);
	if (length < 0 || length >= sizeof(path) || lstat(path, &before) != 0
		|| (directory ? !S_ISDIR(before.st_mode) : !S_ISREG(before.st_mode))
		|| before.st_uid != geteuid() || (before.st_mode & (S_IWGRP | S_IWOTH)) != 0
		|| (!directory
			&& (before.st_nlink != 1 || expected == NULL
				|| !clog_recovery_same_file(&before, expected))))
		return false;
	PG_TRY();
	{
		fd = OpenTransientFile(path, (directory ? O_RDONLY | O_DIRECTORY : O_RDWR) | O_NOFOLLOW
										 | O_CLOEXEC | PG_BINARY);
		if (fd >= 0 && fstat(fd, &opened) == 0 && clog_recovery_same_file(&before, &opened)) {
			pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
			ok = pg_fsync(fd) == 0;
			pgstat_report_wait_end();
			ok = ok && fstat(fd, &after) == 0 && clog_recovery_same_file(&before, &after)
				 && lstat(path, &after) == 0 && clog_recovery_same_file(&before, &after);
		}
	}
	PG_FINALLY();
	{
		pgstat_report_wait_end();
		if (fd >= 0 && CloseTransientFile(fd) != 0)
			ok = false;
	}
	PG_END_TRY();
	return ok;
}

/* Read only a clean, resident native FULL initialization whose entire page
 * equals the original fd observation in its matching origin controller. No
 * disk reload, raw query or other controller supplies a missing generation;
 * this is not lifetime authority. */
static bool clog_full_birth_valid(const xl_clog_zeropage_full *birth);

static uint64
clog_relmap_resident_birth(SlruCtl ctl, int page, const uint8 *image)
{
	SlruShared shared = ctl->shared;
	uint64 value = 0;

	LWLockAcquire(shared->ControlLock, LW_SHARED);
	for (int slot = 0; slot < shared->num_slots; slot++) {
		xl_clog_zeropage_full birth;

		if (shared->page_status[slot] != SLRU_PAGE_VALID
			|| shared->page_number[slot] != page || shared->page_dirty[slot]
			|| memcmp(shared->page_buffer[slot], image, BLCKSZ) != 0)
			continue;
		birth.pageno = page;
		birth.reserved_zero = 0;
		birth.first_full_xid = shared->page_initial_full_xid[slot];
		if (clog_full_birth_valid(&birth))
			value = birth.first_full_xid;
		break;
	}
	LWLockRelease(shared->ControlLock);
	return value;
}

/* A declared relmap participant borrows the original whole replay episode.
 * This is disk input custody, not native mutation or terminal authority. */
struct ClusterRelmapClogBaseHandleV1 {
	pid_t pid;
	int fd;
	bool failed;
	bool invalidated;
	uint64 mutation_generation;
	struct ClusterRelmapClogBaseHandleV1 *next;
	ClusterRecoveryRelmapSourceV1 source;
	ClusterRelmapClogBaseV1 base;
};

/* Nonowning process-local bookkeeping for the original handles. A native
 * write can restore identical bytes, so inode/image equality cannot revive
 * an observation across that write. The shared sequence below also observes
 * other backends of this postmaster; neither is inter-instance exclusion or
 * an era/history/lifetime proof. The original destructor still owns the fd. */
static ClusterRelmapClogBaseHandleV1 *ClogRelmapBases;

static void
clog_relmap_native_mutation(int origin)
{
	ClusterRelmapClogBaseHandleV1 *held;
	ClogRelmapMutation *mutation;
	uint32 writers;
	uint64 generation;

	if (!cluster_shared_config)
		return;
	for (held = ClogRelmapBases; held != NULL; held = held->next)
		if (held->pid == getpid()
			&& held->source.selected.source.claim.identity.origin_thread_id == origin + 1)
			held->invalidated = true;
	if (ClogRelmapMutations == NULL || origin < 0 || origin >= CLOG_RECOVERY_ORIGINS)
		return;
	mutation = &ClogRelmapMutations[origin];
	/* Reserve before changing the sequence: a new read must not sample an
	 * attempt that has announced itself but has not yet changed any bytes.
	 * CAS operations are full barriers. Saturation never wraps into a grant. */
	writers = pg_atomic_read_u32(&mutation->writers);
	while (writers != UINT32_MAX
		&& !pg_atomic_compare_exchange_u32(&mutation->writers, &writers, writers + 1))
		;
	generation = pg_atomic_read_u64(&mutation->generation);
	while (generation != UINT64_MAX
		&& !pg_atomic_compare_exchange_u64(&mutation->generation, &generation, generation + 1))
		;
}

static void
clog_relmap_native_mutation_end(int origin)
{
	ClogRelmapMutation *mutation;
	uint32 writers;
	if (!cluster_shared_config || ClogRelmapMutations == NULL
		|| origin < 0 || origin >= CLOG_RECOVERY_ORIGINS)
		return;
	mutation = &ClogRelmapMutations[origin];
	writers = pg_atomic_read_u32(&mutation->writers);
	while (writers != 0 && writers != UINT32_MAX
		&& !pg_atomic_compare_exchange_u32(&mutation->writers, &writers, writers - 1))
		;
}

/* SLRU calls these only around an actual physical write attempt. Controller
 * identity, rather than directory spelling, identifies original CLOG caches. */
int
cluster_clog_relmap_writeback_begin(SlruCtl ctl)
{
	int origin;
	if (!cluster_shared_config)
		return -1;
	if (ctl == XactCtl) {
		if (cluster_node_id < 0 || cluster_node_id >= CLOG_RECOVERY_ORIGINS)
			return -1;
		origin = cluster_node_id;
	} else {
		for (origin = 0; origin < CLOG_RECOVERY_ORIGINS; ++origin)
			if (ctl == &ClogRecoveryCtl[origin])
				break;
		if (origin == CLOG_RECOVERY_ORIGINS)
			return -1;
	}
	clog_relmap_native_mutation(origin);
	return origin;
}

void
cluster_clog_relmap_writeback_end(int origin)
{
	clog_relmap_native_mutation_end(origin);
}

static bool
clog_relmap_mutation_snapshot(int origin, uint64 *generation)
{
	ClogRelmapMutation *mutation;
	uint64 value;
	if (ClogRelmapMutations == NULL || origin < 0 || origin >= CLOG_RECOVERY_ORIGINS)
		return false;
	mutation = &ClogRelmapMutations[origin];
	if (pg_atomic_read_u32(&mutation->writers) != 0)
		return false;
	pg_read_barrier();
	value = pg_atomic_read_u64(&mutation->generation);
	pg_read_barrier();
	if (value == 0 || value == UINT64_MAX || pg_atomic_read_u32(&mutation->writers) != 0)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u64(&mutation->generation) != value)
		return false;
	*generation = value;
	return true;
}

/* Delayed SLRU writeback can follow the logical mutation reservation.
 * Observe the actual native write interval using only the original ctl
 * identity, never a caller-provided origin or directory spelling. This
 * reuses attempt accounting; it is not continuous physical exclusion. */
static void
clog_relmap_slru_write_observe(SlruCtl ctl, bool begin)
{
	int origin = -1;

	if (!cluster_shared_config)
		return;
	if (ctl == XactCtl)
		origin = cluster_node_id;
	else
		for (int i = 0; i < CLOG_RECOVERY_ORIGINS; ++i)
			if (ctl == &ClogRecoveryCtl[i]) {
				origin = i;
				break;
			}
	if (origin < 0 || origin >= CLOG_RECOVERY_ORIGINS)
		return;
	if (begin)
		clog_relmap_native_mutation(origin);
	else
		clog_relmap_native_mutation_end(origin);
}

static bool
clog_relmap_mutation_matches(int origin, uint64 generation)
{
	uint64 current;
	return clog_relmap_mutation_snapshot(origin, &current) && current == generation;
}

static void
clog_relmap_base_unlink(ClusterRelmapClogBaseHandleV1 *held)
{
	ClusterRelmapClogBaseHandleV1 **link;

	for (link = &ClogRelmapBases; *link != NULL; link = &(*link)->next)
		if (*link == held) {
			*link = held->next;
			return;
		}
}

size_t
cluster_clog_relmap_base_custody_bytes_v1(void)
{
	return sizeof(ClusterRelmapClogBaseHandleV1);
}

static bool
clog_relmap_overlaps(const void *a, size_t an, const void *b, size_t bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

static bool
clog_relmap_source_current(const ClusterRecoveryRelmapSourceV1 *source, int origin)
{
	return cluster_shared_config && AmStartupProcess() && origin >= 0
		&& origin < CLOG_RECOVERY_ORIGINS
		&& source->selected.source.claim.identity.origin_thread_id == origin + 1
		&& (origin == cluster_node_id ? XactCtl->shared != NULL
			: ClogRecoveryLocks != NULL && ClogRecoveryCtl[origin].shared != NULL)
		&& cluster_wal_inputs_relmap_source_current_v1(source);
}

static bool
clog_relmap_base_read(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 requested_full_xid, ClusterRelmapClogBaseV1 *out,
	ClusterRelmapClogBaseHandleV1 **custody)
{
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRelmapClogBaseV1 base = {0};
	ClusterRelmapClogBaseHandleV1 *held;
	SlruCtl ctl;
	SlruCtlData disk;
	LWLock *lock;
	struct stat before, opened, after;
	char path[MAXPGPATH];
	int origin, page, length;
	uint64 mutation_generation = 0;
	bool existing;
	volatile int fd = -1;
	volatile bool locked = false, ok = false;

	if (out == NULL || custody == NULL || source == NULL
		|| clog_relmap_overlaps(out, sizeof(*out), source, sizeof(*source))
		|| clog_relmap_overlaps(out, sizeof(*out), custody, sizeof(*custody))
		|| clog_relmap_overlaps(custody, sizeof(*custody), source, sizeof(*source)))
		return false;
	held = *custody;
	if (held != NULL
		&& (clog_relmap_overlaps(out, sizeof(*out), held, sizeof(*held))
			|| clog_relmap_overlaps(custody, sizeof(*custody), held, sizeof(*held))
			|| clog_relmap_overlaps(source, sizeof(*source), held, sizeof(*held))))
		return false;
	memset(out, 0, sizeof(*out));
	memcpy(&original, source, sizeof(original));
	existing = held != NULL;
	/* Caller-owned native locks/critical sections are not an entered read.
	 * This also keeps whole-ROOT revalidation outside every native lock. */
	if (!enableFsync || CritSectionCount != 0
		|| !TransactionIdIsNormal((TransactionId)requested_full_xid)
		|| original.selected.source.claim.identity.origin_thread_id == 0
		|| original.selected.source.claim.identity.origin_thread_id > CLOG_RECOVERY_ORIGINS
		|| LWLockHeldByMe(XidGenLock) || LWLockHeldByMe(XactTruncationLock)
		|| LWLockHeldByMe(XactSLRULock)
		|| cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return false;
	origin = original.selected.source.claim.identity.origin_thread_id - 1;
	if (!cluster_shared_config || !AmStartupProcess()
		|| (origin == cluster_node_id ? XactCtl->shared == NULL || ShmemVariableCache == NULL
			: ClogRecoveryLocks == NULL || ClogRecoveryCtl[origin].shared == NULL))
		return false;
	ctl = origin == cluster_node_id ? XactCtl : &ClogRecoveryCtl[origin];
	lock = origin == cluster_node_id ? XactTruncationLock
		: &ClogRecoveryLocks[origin * 2 + 1].lock;
	if (LWLockHeldByMe(lock) || LWLockHeldByMe(ctl->shared->ControlLock))
		return false;
	if (existing) {
		if (held->pid != getpid() || held->failed)
			return false;
		held->failed = true;
		if (held->invalidated || held->fd < 0 || held->base.queried_full_xid != requested_full_xid
			|| memcmp(&held->source, &original, sizeof(original)) != 0) {
			cluster_wal_inputs_relmap_source_fail_v1(&held->source);
			return false;
		}
	}
	if (!clog_relmap_source_current(&original, origin)) {
		if (existing)
			cluster_wal_inputs_relmap_source_fail_v1(&original);
		return false;
	}
	page = TransactionIdToPage((TransactionId)requested_full_xid);
	/* For our own origin use the original native controller/lock, but read
	 * the same canonical SIDE route directly, never its possibly dirty cache. */
	memset(&disk, 0, sizeof(disk));
	length = snprintf(disk.Dir, sizeof(disk.Dir), "%s/native_side/origin_%d/pg_xact",
		cluster_shared_data_dir == NULL ? "" : cluster_shared_data_dir, origin);
	if (length < 0 || length >= sizeof(disk.Dir)) {
		cluster_wal_inputs_relmap_source_fail_v1(&original);
		return false;
	}
	length = snprintf(path, sizeof(path), "%s/%04X", disk.Dir, page / SLRU_PAGES_PER_SEGMENT);
	if (length < 0 || length >= sizeof(path)) {
		cluster_wal_inputs_relmap_source_fail_v1(&original);
		return false;
	}
	if (!existing) {
		held = calloc(1, sizeof(*held));
		if (held == NULL) {
			cluster_wal_inputs_relmap_source_fail_v1(&original);
			return false;
		}
		held->pid = getpid(); held->fd = -1; held->failed = true;
		memcpy(&held->source, &original, sizeof(original));
		held->next = ClogRelmapBases;
		ClogRelmapBases = held;
	}

	PG_TRY();
	{
		/* The original getter releases XidGenLock before native or ROOT I/O.
		 * Only this instance owns the corresponding native next counter. */
		base.native_next_observed = origin == cluster_node_id;
		if (base.native_next_observed)
			base.next_full_xid = U64FromFullTransactionId(ReadNextFullTransactionId());
		ok = clog_relmap_mutation_snapshot(origin, &mutation_generation)
			&& (!existing || held->mutation_generation == mutation_generation)
			&& cluster_wal_inputs_relmap_source_revalidate_v1(&original);
		if (ok) {
			LWLockAcquire(lock, LW_EXCLUSIVE);
			locked = true;
			/* Only this instance's native truncation lock protects this raw
			 * horizon. Foreign controls have no corresponding VariableCache. */
			base.truncation_horizon_observed = origin == cluster_node_id;
			if (base.truncation_horizon_observed)
				base.oldest_clog_xid = ShmemVariableCache->oldestClogXid;
			ok = !held->invalidated && clog_relmap_mutation_matches(origin, mutation_generation)
				&& (!existing || (base.truncation_horizon_observed == held->base.truncation_horizon_observed
					&& base.oldest_clog_xid == held->base.oldest_clog_xid))
				&& (!existing || (base.native_next_observed == held->base.native_next_observed
					&& base.next_full_xid == held->base.next_full_xid))
				&& clog_relmap_source_current(&original, origin)
				&& clog_native_directory_ready(origin)
				&& clog_recovery_page_identity(&disk, page, &before);
			if (ok) {
				fd = existing ? held->fd
					: OpenTransientFile(path, O_RDWR | O_NOFOLLOW | O_CLOEXEC | PG_BINARY);
				ok = fd >= 0 && fstat(fd, &opened) == 0
					&& clog_recovery_same_file(&before, &opened)
					&& (!existing || (held->base.device == opened.st_dev
						&& held->base.inode == opened.st_ino && held->base.size == opened.st_size))
					&& clog_recovery_sync(&disk, -1, NULL);
			}
			if (ok) {
				pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
				ok = pg_fsync(fd) == 0;
				pgstat_report_wait_end();
			}
			if (ok) {
				ok = pg_pread(fd, base.image, BLCKSZ,
					(off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ) == BLCKSZ;
				if (ok)
					pgstat_count_slru_page_read(ctl->shared->slru_stats_idx);
				ok = ok && fstat(fd, &after) == 0 && clog_recovery_same_file(&before, &after)
					&& clog_recovery_page_identity(&disk, page, &after)
					&& clog_recovery_same_file(&before, &after)
					&& clog_native_directory_ready(origin)
					&& clog_relmap_source_current(&original, origin)
					&& clog_relmap_mutation_matches(origin, mutation_generation)
					&& (!base.truncation_horizon_observed
						|| base.oldest_clog_xid == ShmemVariableCache->oldestClogXid)
					&& (!existing || memcmp(base.image, held->base.image, BLCKSZ) == 0);
			}
			if (ok) {
				base.resident_first_full_xid = clog_relmap_resident_birth(ctl, page, base.image);
				base.resident_birth_observed = base.resident_first_full_xid != 0;
			}
			ok = ok && (!existing
				|| (base.resident_birth_observed == held->base.resident_birth_observed
					&& base.resident_first_full_xid == held->base.resident_first_full_xid));
			LWLockRelease(lock);
			locked = false;
		}
		/* There is no native lock or borrowed-output publication over ROOT I/O. */
		ok = ok && cluster_wal_inputs_relmap_source_revalidate_v1(&original)
			&& memcmp(source, &original, sizeof(original)) == 0 && !held->invalidated
			&& (!base.native_next_observed
				|| base.next_full_xid == U64FromFullTransactionId(ReadNextFullTransactionId()))
			&& clog_relmap_mutation_matches(origin, mutation_generation)
			&& base.resident_first_full_xid
				== clog_relmap_resident_birth(ctl, page, base.image)
			&& clog_relmap_mutation_matches(origin, mutation_generation);
		if (!ok)
			cluster_wal_inputs_relmap_source_fail_v1(&original);
	}
	PG_CATCH();
	{
		pgstat_report_wait_end();
		if (locked && LWLockHeldByMe(lock)) {
			/* elog ERROR clears the original lock's interrupt holdoff. */
			HOLD_INTERRUPTS();
			LWLockRelease(lock);
		}
		if (fd >= 0 && !existing)
			(void)CloseTransientFile(fd);
		if (!existing) {
			clog_relmap_base_unlink(held);
			free(held);
		}
		cluster_wal_inputs_relmap_source_fail_v1(&original);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ok) {
		if (fd >= 0 && !existing)
			(void)CloseTransientFile(fd);
		if (!existing) {
			clog_relmap_base_unlink(held);
			free(held);
		}
		return false;
	}
	base.queried_full_xid = requested_full_xid;
	base.origin = origin; base.page_number = page;
	base.status = (base.image[TransactionIdToByte((TransactionId)requested_full_xid)]
		>> (TransactionIdToBIndex((TransactionId)requested_full_xid) * CLOG_BITS_PER_XACT))
		& CLOG_XACT_BITMASK;
	base.device = before.st_dev; base.inode = before.st_ino; base.size = before.st_size;
	held->fd = fd;
	held->mutation_generation = mutation_generation;
	memcpy(&held->base, &base, sizeof(base));
	held->failed = false;
	*custody = held;
	memcpy(out, &base, sizeof(base));
	return true;
}

bool
cluster_clog_relmap_base_read_v1(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 requested_full_xid, ClusterRelmapClogBaseV1 *out,
	ClusterRelmapClogBaseHandleV1 **custody)
{
	ClusterRecoveryRelmapSourceV1 original;
	bool ok;
	if (source == NULL)
		return false;
	memcpy(&original, source, sizeof(original));
	PG_TRY();
	{
		ok = clog_relmap_base_read(source, requested_full_xid, out, custody);
	}
	PG_CATCH();
	{
		cluster_wal_inputs_relmap_source_fail_v1(&original);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ok;
}

void
cluster_clog_relmap_base_release_v1(ClusterRelmapClogBaseHandleV1 **custody)
{
	ClusterRelmapClogBaseHandleV1 *held;
	if (custody == NULL || *custody == NULL || (*custody)->pid != getpid())
		return;
	held = *custody;
	*custody = NULL;
	clog_relmap_base_unlink(held);
	if (held->fd >= 0)
		(void)CloseTransientFile(held->fd);
	free(held);
}

typedef enum ClogRecoveryAccess {
	CLOG_RECOVERY_PREFLIGHT,
	CLOG_RECOVERY_WRITE,
	CLOG_RECOVERY_VERIFY
} ClogRecoveryAccess;

static bool
clog_recovery_status(SlruCtl ctl, int slot, TransactionId xid, XidStatus status,
					 ClogRecoveryAccess access)
{
	uint8 *byte = (uint8 *)ctl->shared->page_buffer[slot] + TransactionIdToByte(xid);
	int shift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	XidStatus current = (*byte >> shift) & CLOG_XACT_BITMASK;

	if (access == CLOG_RECOVERY_VERIFY)
		return current == status;
	/* As in native replay, a completed child is never demoted by its
	 * SUB_COMMITTED phase. Conflicting terminals are runtime refusals. */
	if (status == TRANSACTION_STATUS_SUB_COMMITTED && current == TRANSACTION_STATUS_COMMITTED)
		return true;
	if (current != TRANSACTION_STATUS_IN_PROGRESS && current != TRANSACTION_STATUS_SUB_COMMITTED
		&& current != status)
		return false;
	if (access == CLOG_RECOVERY_WRITE) {
		*byte = (*byte & ~(CLOG_XACT_BITMASK << shift)) | (status << shift);
		ctl->shared->page_dirty[slot] = true;
	}
	return true;
}

/* Read the disk bit independently of the original native cache. A cached
 * ABORT cannot certify persistence, or conceal a conflicting disk COMMIT. */
static bool
clog_recovery_disk_abort(SlruCtl ctl, TransactionId xid, const struct stat *expected, bool verify)
{
	int page = TransactionIdToPage(xid);
	char path[MAXPGPATH];
	struct stat opened, after;
	volatile int fd = -1;
	volatile bool ok = false;
	uint8 byte;
	int length = snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, page / SLRU_PAGES_PER_SEGMENT);
	off_t offset = (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ + TransactionIdToByte(xid);

	if (length < 0 || length >= sizeof(path))
		return false;
	PG_TRY();
	{
		fd = OpenTransientFile(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | PG_BINARY);
		if (fd >= 0 && fstat(fd, &opened) == 0 && S_ISREG(opened.st_mode)
			&& opened.st_nlink == 1 && opened.st_uid == geteuid()
			&& (opened.st_mode & (S_IWGRP | S_IWOTH)) == 0
			&& clog_recovery_same_file(expected, &opened)
			&& pg_pread(fd, &byte, 1, offset) == 1) {
			XidStatus status = (byte >> (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT))
				& CLOG_XACT_BITMASK;

			ok = (verify ? status == TRANSACTION_STATUS_ABORTED
				: status != TRANSACTION_STATUS_COMMITTED)
				&& fstat(fd, &after) == 0 && clog_recovery_same_file(expected, &after)
				&& clog_recovery_page_identity(ctl, page, &after)
				&& clog_recovery_same_file(expected, &after);
		}
	}
	PG_FINALLY();
	{
		if (fd >= 0 && CloseTransientFile(fd) != 0)
			ok = false;
	}
	PG_END_TRY();
	return ok;
}

static bool
clog_recovery_own_base_current(SlruCtl disk, int origin, TransactionId xid,
	const struct stat *before)
{
	struct stat after, routed;
	int page = TransactionIdToPage(xid);

	return clog_recovery_terminal_directory_ready(origin, xid, 0, TRANSACTION_STATUS_ABORTED)
		&& clog_recovery_page_identity(disk, page, &after)
		&& clog_recovery_same_file(before, &after)
		&& clog_recovery_page_identity(XactCtl, page, &routed)
		&& clog_recovery_same_file(before, &routed);
}

static bool
clog_recovery_own_abort(int origin, TransactionId xid, bool verify)
{
	SlruCtlData disk = *XactCtl;
	struct stat before;
	int page = TransactionIdToPage(xid);
	int length;
	volatile bool ok = false;

	/* The absolute copy is only for disk identity/readback/sync. All cache
	 * access and writes use the unchanged native controller and its locks. */
	if (!clog_recovery_terminal_directory_ready(origin, xid, 0, TRANSACTION_STATUS_ABORTED))
		return false;
	length = snprintf(disk.Dir, sizeof(disk.Dir), "%s/native_side/origin_%d/pg_xact",
		cluster_shared_data_dir, origin);
	if (length < 0 || length >= sizeof(disk.Dir)
		|| !clog_recovery_page_identity(&disk, page, &before)
		|| !clog_recovery_own_base_current(&disk, origin, xid, &before)
		|| !clog_recovery_disk_abort(&disk, xid, &before, verify))
		return false;
	PG_TRY();
	{
		int slot;

		LWLockAcquire(XactCtl->shared->ControlLock, LW_EXCLUSIVE);
		slot = SimpleLruReadPage(XactCtl, page, true, xid);
		ok = clog_recovery_own_base_current(&disk, origin, xid, &before)
			&& clog_recovery_disk_abort(&disk, xid, &before, verify)
			&& clog_recovery_terminal_permitted(origin, xid, 0, TRANSACTION_STATUS_ABORTED)
			&& clog_recovery_status(XactCtl, slot, xid, TRANSACTION_STATUS_ABORTED,
				verify ? CLOG_RECOVERY_VERIFY : CLOG_RECOVERY_PREFLIGHT);
		if (ok && !verify) {
			(void)clog_recovery_status(XactCtl, slot, xid, TRANSACTION_STATUS_ABORTED,
				CLOG_RECOVERY_WRITE);
			SimpleLruWritePage(XactCtl, slot);
		}
	}
	PG_FINALLY();
	{
		/* Native SLRU owns its I/O state. Never discard this shared cache:
		 * unrelated transactions may still have dirty bits or group LSNs. */
		if (LWLockHeldByMe(XactCtl->shared->ControlLock))
			LWLockRelease(XactCtl->shared->ControlLock);
	}
	PG_END_TRY();
	if (!ok || !clog_recovery_own_base_current(&disk, origin, xid, &before))
		return false;
	if (verify)
		return clog_recovery_disk_abort(&disk, xid, &before, true)
			&& clog_recovery_own_base_current(&disk, origin, xid, &before);
	return clog_recovery_sync(&disk, page, &before) && clog_recovery_sync(&disk, -1, NULL)
		&& clog_recovery_own_base_current(&disk, origin, xid, &before)
		&& clog_recovery_own_abort(origin, xid, true);
}

static bool
clog_recovery_page(int origin, int page, TransactionId top, int count,
				   const TransactionId *children, XidStatus status, ClogRecoveryAccess access)
{
	SlruCtl ctl = &ClogRecoveryCtl[origin];
	struct stat before, after;
	int slot;
	bool ok;

	clog_recovery_cache_discard_page(ctl, page);
	if (!clog_recovery_terminal_directory_ready(origin, top, count, status)
		|| !clog_recovery_page_identity(ctl, page, &before))
		return false;
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	slot = SimpleLruReadPage(ctl, page, true, top);
	ok = clog_recovery_terminal_permitted(origin, top, count, status)
		 && clog_recovery_page_identity(ctl, page, &after)
		 && clog_recovery_same_file(&before, &after);
	if (ok && TransactionIdIsValid(top))
		ok = clog_recovery_status(ctl, slot, top, status, access);
	for (int i = 0; ok && i < count; i++)
		ok = clog_recovery_status(ctl, slot, children[i], status, access);
	if (ok && access == CLOG_RECOVERY_WRITE) {
		ok = clog_recovery_terminal_permitted(origin, top, count, status)
			 && clog_recovery_page_identity(ctl, page, &after)
			 && clog_recovery_same_file(&before, &after);
		if (ok)
			SimpleLruWritePage(ctl, slot);
	}
	LWLockRelease(ctl->shared->ControlLock);
	if (!ok)
		return false;
	if (access == CLOG_RECOVERY_WRITE
		&& (!clog_recovery_terminal_permitted(origin, top, count, status)
			|| !clog_recovery_sync(ctl, page, &before)))
		return false;
	return clog_recovery_terminal_directory_ready(origin, top, count, status)
		   && clog_recovery_page_identity(ctl, page, &after)
		   && clog_recovery_same_file(&before, &after);
}

static bool
clog_recovery_children(int origin, int count, const TransactionId *children, XidStatus status,
					   ClogRecoveryAccess access)
{
	for (int first = 0; first < count;) {
		int last = first + 1;
		int page = TransactionIdToPage(children[first]);

		while (last < count && TransactionIdToPage(children[last]) == page)
			last++;
		if (!clog_recovery_page(origin, page, InvalidTransactionId, last - first, children + first,
								status, access))
			return false;
		first = last;
	}
	return true;
}

static bool
clog_recovery_tree(int origin, TransactionId xid, int count, const TransactionId *children,
				   XidStatus status, bool verify)
{
	int page = TransactionIdToPage(xid);
	int same = 0;
	ClogRecoveryAccess access = verify ? CLOG_RECOVERY_VERIFY : CLOG_RECOVERY_PREFLIGHT;
	const TransactionId *other;

	while (same < count && TransactionIdToPage(children[same]) == page)
		same++;
	other = count > same ? children + same : NULL;
	/* Preflight the whole tree before any terminal mutation. This is not
	 * a replacement for complete WAL/base/era proof by the recovery owner. */
	if (!clog_recovery_page(origin, page, xid, same, children, status, access)
		|| !clog_recovery_children(origin, count - same, other, status, access))
		return false;
	if (verify)
		return true;
	if (status == TRANSACTION_STATUS_COMMITTED
		&& !clog_recovery_children(origin, count - same, other, TRANSACTION_STATUS_SUB_COMMITTED,
								   CLOG_RECOVERY_WRITE))
		return false;
	if (!clog_recovery_page(origin, page, xid, same, children, status, CLOG_RECOVERY_WRITE)
		|| !clog_recovery_children(origin, count - same, other, status, CLOG_RECOVERY_WRITE))
		return false;
	if (!clog_recovery_sync(&ClogRecoveryCtl[origin], -1, NULL))
		return false;
	return clog_recovery_terminal_directory_ready(origin, xid, count, status)
		   && clog_recovery_tree(origin, xid, count, children, status, true);
}

static bool
clog_recovery_access(int origin, TransactionId xid, int nsubxids, const TransactionId *subxids,
					 XidStatus status, bool verify)
{
	volatile bool locked = false;
	volatile bool ok = false;
	volatile bool finished = false;
	LWLock *lock;
	TransactionId previous = xid;

	if (!clog_recovery_terminal_permitted(origin, xid, nsubxids, status)
		|| !TransactionIdIsNormal(xid) || nsubxids < 0
		|| nsubxids > PG_INT32_MAX / (int)sizeof(TransactionId) || (nsubxids > 0 && subxids == NULL)
		|| (status != TRANSACTION_STATUS_COMMITTED && status != TRANSACTION_STATUS_ABORTED))
		return false;
	for (int i = 0; i < nsubxids; i++) {
		if (!TransactionIdIsNormal(subxids[i]) || !TransactionIdPrecedes(previous, subxids[i])
			|| !TransactionIdPrecedes(xid, subxids[i]))
			return false;
		previous = subxids[i];
	}
	if (!verify)
		clog_relmap_native_mutation(origin);
	lock = origin == cluster_node_id ? NULL : &ClogRecoveryLocks[origin * 2 + 1].lock;
	PG_TRY();
	{
		if (origin == cluster_node_id)
			ok = clog_recovery_own_abort(origin, xid, verify);
		else {
			LWLockAcquire(lock, LW_EXCLUSIVE);
			locked = true;
			if (clog_recovery_terminal_directory_ready(origin, xid, nsubxids, status))
				ok = clog_recovery_tree(origin, xid, nsubxids, subxids, status, verify);
		}
		finished = true;
	}
	PG_FINALLY();
	{
		if (locked) {
			SlruCtl ctl = &ClogRecoveryCtl[origin];

			if (finished && ok)
				clog_recovery_cache_discard_unknown(ctl);
			else {
				/* ERROR clears holdoffs for locks still owned by native I/O. */
				if (!finished) {
					if (LWLockHeldByMe(ctl->shared->ControlLock))
						HOLD_INTERRUPTS();
					for (int slot = 0; slot < ctl->shared->num_slots; slot++)
						if ((ctl->shared->page_status[slot] == SLRU_PAGE_READ_IN_PROGRESS
							 || ctl->shared->page_status[slot] == SLRU_PAGE_WRITE_IN_PROGRESS)
							&& LWLockHeldByMe(&ctl->shared->buffer_locks[slot].lock))
							HOLD_INTERRUPTS();
				}
				clog_recovery_cache_discard(ctl);
			}
			if (!finished)
				HOLD_INTERRUPTS();
			LWLockRelease(lock);
		}
		if (!verify)
			clog_relmap_native_mutation_end(origin);
	}
	PG_END_TRY();
	return ok;
}

/* Persist only the original origin's native terminal bits. Own-origin C0
 * uses the original native cache; foreign replay uses its separate controller.
 * Allocation counters, TT, remote projection and serving gates are untouched.
 * Author: SqlRush <sqlrush@gmail.com> */
bool
cluster_clog_native_recovery_apply_v1(int origin, TransactionId xid, int nsubxids,
									  const TransactionId *subxids, XidStatus status)
{
	return clog_recovery_access(origin, xid, nsubxids, subxids, status, false);
}

/* Independent disk readback (and native cache check for own-origin C0),
 * not a visibility certificate. */
bool
cluster_clog_native_recovery_verify_v1(int origin, TransactionId xid, int nsubxids,
									   const TransactionId *subxids, XidStatus status)
{
	return clog_recovery_access(origin, xid, nsubxids, subxids, status, true);
}

/* Retained ZERO may create a page; terminal replay alone never may. */
static bool
clog_recovery_zero(int origin, int page, bool verify, bool full)
{
	SlruCtl ctl = &ClogRecoveryCtl[origin];
	char path[MAXPGPATH];
	struct stat before, after;
	bool exists, ok;
	int slot;

	if (full)
		clog_recovery_cache_discard_page(ctl, page);
	else
		clog_recovery_cache_discard(ctl);
	if (!clog_recovery_directory_ready(origin))
		return false;
	snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, page / SLRU_PAGES_PER_SEGMENT);
	exists = lstat(path, &before) == 0;
	if ((!exists && (errno != ENOENT || verify))
		|| (exists
			&& (!S_ISREG(before.st_mode) || before.st_nlink != 1 || before.st_uid != geteuid()
				|| (before.st_mode & (S_IWGRP | S_IWOTH)) != 0))
		|| (verify && !clog_recovery_page_identity(ctl, page, &before)))
		return false;
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	ok = clog_recovery_permitted(origin);
	if (ok && verify) {
		slot = SimpleLruReadPage(ctl, page, true, InvalidTransactionId);
		for (int i = 0; i < BLCKSZ && ok; i++)
			ok = ctl->shared->page_buffer[slot][i] == 0;
	} else if (ok) {
		slot = SimpleLruZeroPage(ctl, page);
		SimpleLruWritePage(ctl, slot);
	}
	LWLockRelease(ctl->shared->ControlLock);
	if (!ok || !clog_recovery_page_identity(ctl, page, &after)
		|| (exists && (before.st_dev != after.st_dev || before.st_ino != after.st_ino))
		|| after.st_size
			   != (verify ? before.st_size
						  : Max(exists ? before.st_size : 0,
								(off_t)(page % SLRU_PAGES_PER_SEGMENT + 1) * BLCKSZ)))
		return false;
	if (verify)
		return clog_recovery_directory_ready(origin);
	if (!clog_recovery_permitted(origin) || !clog_recovery_sync(ctl, page, &after)
		|| !clog_recovery_sync(ctl, -1, NULL))
		return false;
	return clog_recovery_zero(origin, page, true, full);
}

typedef struct ClogRecoveryTruncate {
	int origin;
	int cutoff;
	int phase; /* 0: all eligible files preflight; 1: delete; 2: absence */
	bool ok;
} ClogRecoveryTruncate;

static bool
clog_recovery_truncate_file(SlruCtl ctl, char *filename, int segpage, void *arg)
{
	ClogRecoveryTruncate *scan = arg;
	char canonical[16], path[MAXPGPATH];
	struct stat st;

	snprintf(canonical, sizeof(canonical), "%04X", segpage / SLRU_PAGES_PER_SEGMENT);
	if (segpage < 0 || (uint32)segpage > UINT32_MAX / CLOG_XACTS_PER_PAGE
		|| strcmp(filename, canonical) != 0 || !clog_recovery_permitted(scan->origin)) {
		scan->ok = false;
		return true;
	}
	/* Use the native whole-segment predicate, including its half-range edge. */
	if (!SlruScanDirCbReportPresence(ctl, filename, segpage, &scan->cutoff))
		return false;
	snprintf(path, sizeof(path), "%s/%s", ctl->Dir, filename);
	if (scan->phase == 2 || lstat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_nlink != 1
		|| st.st_uid != geteuid() || (st.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
		scan->ok = false;
		return true;
	}
	if (scan->phase == 1) {
		SlruDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT);
		/* Native unlink is best effort; it is not recovery completion. */
		if (lstat(path, &st) == 0 || errno != ENOENT) {
			scan->ok = false;
			return true;
		}
	}
	return false;
}

static bool
clog_recovery_truncate(int origin, int page, bool verify)
{
	SlruCtl ctl = &ClogRecoveryCtl[origin];
	ClogRecoveryTruncate scan = { origin, page, verify ? 2 : 0, true };

	clog_recovery_cache_discard(ctl);
	for (; scan.phase <= 2; scan.phase++) {
		if (!clog_recovery_directory_ready(origin))
			return false;
		(void)SlruScanDirectory(ctl, clog_recovery_truncate_file, &scan);
		if (!scan.ok)
			return false;
		if (scan.phase == 1 && !clog_recovery_sync(ctl, -1, NULL))
			return false;
	}
	return clog_recovery_directory_ready(origin);
}

/* Validate the complete retained native birth before original native locks or
 * I/O. These bytes retain a producer input, not an issuer or lifetime grant. */
static bool
clog_recovery_full_record_shape(const ClusterSideProjectionOperationV1 *op,
								const uint8 *payload, uint32 length)
{
	xl_clog_zeropage_full birth;
	TransactionId raw;

	if (op == NULL || op->kind != CLUSTER_SIDE_PROJECTION_CLOG
		|| op->action != CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE
		|| op->normalized_info != CLOG_ZEROPAGE_FULL
		|| TransactionIdIsValid(op->oldest_xid) || OidIsValid(op->oldest_database)
		|| payload == NULL || length != sizeof(birth))
		return false;
	memcpy(&birth, payload, sizeof(birth));
	raw = (TransactionId)birth.first_full_xid;
	return birth.reserved_zero == 0 && birth.pageno >= 0 && birth.pageno == op->page_number
		&& TransactionIdIsNormal(raw)
		&& (raw % CLOG_XACTS_PER_PAGE == 0 || raw == FirstNormalTransactionId)
		&& raw / CLOG_XACTS_PER_PAGE == (uint32)birth.pageno;
}

static bool
clog_recovery_own_full_permitted(int origin)
{
	return cluster_shared_config && AmStartupProcess() && InRecovery
		&& origin >= 0 && origin < CLOG_RECOVERY_ORIGINS && origin == cluster_node_id
		&& XactCtl->shared != NULL && ClogRecoveryLocks != NULL
		&& cluster_undo_recovery_cold_origin_authorized_v1(origin);
}

static bool
clog_recovery_own_full_directory_ready(int origin)
{
	return clog_recovery_own_full_permitted(origin) && clog_native_directory_ready(origin)
		&& clog_recovery_own_full_permitted(origin);
}

/* The original shared cache cannot be discarded to force a physical read. */
static bool
clog_recovery_disk_zero(SlruCtl disk, int page, const struct stat *expected)
{
	PGAlignedBlock bytes;
	char path[MAXPGPATH];
	struct stat opened, after;
	volatile int fd = -1;
	volatile bool ok = false;
	int length = snprintf(path, sizeof(path), "%s/%04X", disk->Dir,
		page / SLRU_PAGES_PER_SEGMENT);
	off_t offset = (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ;

	if (length < 0 || length >= sizeof(path))
		return false;
	PG_TRY();
	{
		fd = OpenTransientFile(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | PG_BINARY);
		if (fd >= 0 && fstat(fd, &opened) == 0 && S_ISREG(opened.st_mode)
			&& opened.st_nlink == 1 && opened.st_uid == geteuid()
			&& (opened.st_mode & (S_IWGRP | S_IWOTH)) == 0
			&& clog_recovery_same_file(expected, &opened)
			&& pg_pread(fd, bytes.data, BLCKSZ, offset) == BLCKSZ) {
			ok = true;
			for (int i = 0; i < BLCKSZ && ok; i++)
				ok = bytes.data[i] == 0;
			ok = ok && fstat(fd, &after) == 0 && clog_recovery_same_file(expected, &after)
				&& clog_recovery_page_identity(disk, page, &after)
				&& clog_recovery_same_file(expected, &after);
		}
	}
	PG_FINALLY();
	{
		if (fd >= 0 && CloseTransientFile(fd) != 0)
			ok = false;
	}
	PG_END_TRY();
	return ok;
}

static bool
clog_recovery_own_full_zero(int origin, int page, bool verify)
{
	SlruCtlData disk;
	char path[MAXPGPATH];
	struct stat before, after, routed;
	bool exists;
	volatile bool ok = false, finished = false;
	int length;

	if (!clog_recovery_own_full_directory_ready(origin))
		return false;
	/* Only disk identity/readback uses this absolute copy. Native cache and
	 * writes keep XactCtl, its locks and unrelated dirty/group-LSN state. */
	disk = *XactCtl;
	length = snprintf(disk.Dir, sizeof(disk.Dir), "%s/native_side/origin_%d/pg_xact",
		cluster_shared_data_dir, origin);
	if (length < 0 || length >= sizeof(disk.Dir))
		return false;
	length = snprintf(path, sizeof(path), "%s/%04X", disk.Dir, page / SLRU_PAGES_PER_SEGMENT);
	if (length < 0 || length >= sizeof(path))
		return false;
	exists = lstat(path, &before) == 0;
	if ((!exists && (errno != ENOENT || verify))
		|| (exists && (!S_ISREG(before.st_mode) || before.st_nlink != 1
			|| before.st_uid != geteuid() || (before.st_mode & (S_IWGRP | S_IWOTH)) != 0))
		|| (verify && !clog_recovery_page_identity(&disk, page, &before)))
		return false;
	PG_TRY();
	{
		int slot;

		LWLockAcquire(XactCtl->shared->ControlLock, LW_EXCLUSIVE);
		ok = clog_recovery_own_full_directory_ready(origin);
		if (ok && verify) {
			slot = SimpleLruReadPage(XactCtl, page, true, InvalidTransactionId);
			for (int i = 0; i < BLCKSZ && ok; i++)
				ok = XactCtl->shared->page_buffer[slot][i] == 0;
		} else if (ok) {
			slot = SimpleLruZeroPage(XactCtl, page);
			SimpleLruWritePage(XactCtl, slot);
		}
		finished = true;
	}
	PG_FINALLY();
	{
		/* Leave native SLRU's error/dirty/I/O ownership intact. */
		if (LWLockHeldByMe(XactCtl->shared->ControlLock)) {
			/* elog ERROR clears the original lock's interrupt holdoff. */
			if (!finished)
				HOLD_INTERRUPTS();
			LWLockRelease(XactCtl->shared->ControlLock);
		}
	}
	PG_END_TRY();
	if (!ok || !clog_recovery_own_full_directory_ready(origin)
		|| !clog_recovery_page_identity(&disk, page, &after)
		|| (exists && (before.st_dev != after.st_dev || before.st_ino != after.st_ino))
		|| after.st_size != (verify ? before.st_size
			: Max(exists ? before.st_size : 0,
				(off_t)(page % SLRU_PAGES_PER_SEGMENT + 1) * BLCKSZ))
		|| !clog_recovery_page_identity(XactCtl, page, &routed)
		|| !clog_recovery_same_file(&after, &routed))
		return false;
	if (verify)
		return clog_recovery_disk_zero(&disk, page, &after)
			&& clog_recovery_own_full_directory_ready(origin);
	return clog_recovery_sync(&disk, page, &after) && clog_recovery_sync(&disk, -1, NULL)
		&& clog_recovery_own_full_zero(origin, page, true);
}

static bool
clog_recovery_own_full_record(int origin, const ClusterSideProjectionOperationV1 *op,
	bool verify, const uint8 *payload, uint32 length)
{
	volatile bool locked = false, native_locked = false, ok = false, finished = false;
	xl_clog_zeropage_full birth;
	LWLock *lock;

	/* Own legacy ZERO/TRUNCATE and C0 terminal permissions remain separate. */
	if (payload == NULL || length != sizeof(birth))
		return false;
	memcpy(&birth, payload, sizeof(birth));
	if (!clog_recovery_full_record_shape(op, (const uint8 *)&birth, sizeof(birth))
		|| !clog_recovery_own_full_directory_ready(origin))
		return false;
	if (!verify)
		clog_relmap_native_mutation(origin);
	lock = &ClogRecoveryLocks[origin * 2 + 1].lock;
	PG_TRY();
	{
		LWLockAcquire(lock, LW_EXCLUSIVE);
		locked = true;
		ok = clog_recovery_own_full_zero(origin, op->page_number, verify);
		if (ok && !verify) {
			SlruShared shared = XactCtl->shared;

			/* Only this successful apply supplies initialization input. The
			 * original sync and independent disk read have already completed;
			 * verification alone must never restore birth from historical WAL.
			 * Keep the original cold lock and recheck the resident native page. */
			native_locked = true;
			LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
			ok = false;
			if (clog_recovery_own_full_permitted(origin)) {
				for (int slot = 0; slot < shared->num_slots; slot++) {
					bool zero = true;

					if (shared->page_status[slot] != SLRU_PAGE_VALID
						|| shared->page_number[slot] != birth.pageno || shared->page_dirty[slot])
						continue;
					for (int i = 0; i < BLCKSZ && zero; i++)
						zero = shared->page_buffer[slot][i] == 0;
					if (zero) {
						shared->page_initial_full_xid[slot] = birth.first_full_xid;
						ok = true;
					}
					break;
				}
			}
			LWLockRelease(shared->ControlLock);
			native_locked = false;
		}
		finished = true;
	}
	PG_FINALLY();
	{
		if (native_locked && LWLockHeldByMe(XactCtl->shared->ControlLock)) {
			/* ERROR clears holdoffs for both original native and cold locks. */
			if (!finished)
				HOLD_INTERRUPTS();
			LWLockRelease(XactCtl->shared->ControlLock);
		}
		if (locked) {
			if (!finished)
				HOLD_INTERRUPTS();
			LWLockRelease(lock);
		}
		if (!verify)
			clog_relmap_native_mutation_end(origin);
	}
	PG_END_TRY();
	return ok;
}

/* Complete only the existing founder FULL redo's physical write. Source
 * attempt/current custody stays with the original native record executor. */
bool
cluster_clog_native_recovery_full_sync_v1(int origin, const XLogReaderState *record)
{
	ClusterSideProjectionOperationV1 op = { 0 };
	xl_clog_zeropage_full birth;
	SlruCtlData disk;
	struct stat before, after;
	const DecodedXLogRecord *saved;
	XLogRecord header;
	XLogRecPtr start, end;
	uint64 generation;
	int length;

	if (!enableFsync || CritSectionCount != 0
		|| !clog_recovery_own_full_directory_ready(origin)
		|| LWLockHeldByMe(XactSLRULock) || LWLockHeldByMe(XactTruncationLock)
		|| LWLockHeldByMe(XidGenLock)
		|| LWLockHeldByMe(&ClogRecoveryLocks[origin * 2 + 1].lock)
		|| cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock)
		|| record == NULL || record->record == NULL
		|| XLogRecGetRmid(record) != RM_CLOG_ID
		|| (XLogRecGetInfo(record) & ~XLR_INFO_MASK) != CLOG_ZEROPAGE_FULL
		|| XLogRecHasAnyBlockRefs(record) || XLogRecGetData(record) == NULL
		|| XLogRecGetDataLen(record) != sizeof(birth)
		|| record->EndRecPtr <= record->ReadRecPtr)
		return false;
	memcpy(&birth, XLogRecGetData(record), sizeof(birth));
	op.kind = CLUSTER_SIDE_PROJECTION_CLOG;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	op.normalized_info = CLOG_ZEROPAGE_FULL;
	op.page_number = birth.pageno;
	if (!clog_recovery_full_record_shape(&op, (const uint8 *)&birth, sizeof(birth))
		|| !clog_relmap_mutation_snapshot(origin, &generation))
		return false;
	saved = record->record;
	memcpy(&header, &saved->header, sizeof(header));
	start = record->ReadRecPtr; end = record->EndRecPtr;
	disk = *XactCtl;
	length = snprintf(disk.Dir, sizeof(disk.Dir), "%s/native_side/origin_%d/pg_xact",
		cluster_shared_data_dir, origin);
	if (length < 0 || length >= sizeof(disk.Dir)
		|| !clog_recovery_page_identity(&disk, birth.pageno, &before)
		|| !clog_recovery_own_full_record(origin, &op, true, (const uint8 *)&birth, sizeof(birth))
		|| !clog_relmap_mutation_matches(origin, generation)
		|| !clog_recovery_sync(&disk, birth.pageno, &before)
		|| !clog_recovery_sync(&disk, -1, NULL)
		|| !clog_recovery_own_full_directory_ready(origin)
		|| !clog_recovery_own_full_record(origin, &op, true, (const uint8 *)&birth, sizeof(birth))
		|| !clog_recovery_page_identity(&disk, birth.pageno, &after)
		|| !clog_recovery_same_file(&before, &after)
		|| !clog_relmap_mutation_matches(origin, generation))
		return false;
	return record->record == saved && record->ReadRecPtr == start && record->EndRecPtr == end
		&& memcmp(&saved->header, &header, sizeof(header)) == 0 && saved->max_block_id < 0
		&& saved->main_data != NULL && saved->main_data_len == sizeof(birth)
		&& memcmp(saved->main_data, &birth, sizeof(birth)) == 0;
}

static bool
clog_recovery_record(int origin, const ClusterSideProjectionOperationV1 *op, bool verify,
					 const uint8 *payload, uint32 length)
{
	volatile bool locked = false, ok = false, finished = false, retain_cache = false;
	xl_clog_zeropage_full birth;
	LWLock *lock;
	SlruCtl ctl;

	if (origin == cluster_node_id)
		return clog_recovery_own_full_record(origin, op, verify, payload, length);
	if (!clog_recovery_permitted(origin) || op == NULL || op->kind != CLUSTER_SIDE_PROJECTION_CLOG
		|| op->page_number < 0 || (uint32)op->page_number > UINT32_MAX / CLOG_XACTS_PER_PAGE)
		return false;
	if (op->normalized_info == CLOG_ZEROPAGE_FULL) {
		if (!clog_recovery_full_record_shape(op, payload, length))
			return false;
		memcpy(&birth, payload, sizeof(birth));
	} else if (payload != NULL || length != 0)
		return false;
	if (op->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE) {
		if ((op->normalized_info != CLOG_ZEROPAGE && op->normalized_info != CLOG_ZEROPAGE_FULL)
			|| TransactionIdIsValid(op->oldest_xid)
			|| OidIsValid(op->oldest_database))
			return false;
	} else if (op->action == CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE) {
		if (op->normalized_info != CLOG_TRUNCATE || !TransactionIdIsNormal(op->oldest_xid)
			|| !OidIsValid(op->oldest_database)
			|| TransactionIdToPage(op->oldest_xid) != op->page_number)
			return false;
	} else
		return false;
	if (!verify)
		clog_relmap_native_mutation(origin);
	ctl = &ClogRecoveryCtl[origin];
	lock = &ClogRecoveryLocks[origin * 2 + 1].lock;
	PG_TRY();
	{
		locked = true;
		LWLockAcquire(lock, LW_EXCLUSIVE);
		ok = op->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE
				 ? clog_recovery_zero(origin, op->page_number, verify,
									  op->normalized_info == CLOG_ZEROPAGE_FULL)
				 : clog_recovery_truncate(origin, op->page_number, verify);
		if (ok && !verify && op->normalized_info == CLOG_ZEROPAGE_FULL) {
			SlruShared shared = ctl->shared;

			/* The original write, sync and independent disk reload succeeded.
			 * Only this apply supplies birth to this origin's clean resident
			 * slot; a verifier or another controller cannot recreate it. */
			LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
			ok = false;
			if (clog_recovery_permitted(origin)) {
				for (int slot = 0; slot < shared->num_slots; slot++) {
					if (shared->page_status[slot] != SLRU_PAGE_VALID
						|| shared->page_number[slot] != birth.pageno
						|| shared->page_dirty[slot])
						continue;
					ok = true;
					for (int i = 0; i < BLCKSZ && ok; i++)
						ok = shared->page_buffer[slot][i] == 0;
					if (ok)
						shared->page_initial_full_xid[slot] = birth.first_full_xid;
					break;
				}
			}
			LWLockRelease(shared->ControlLock);
		}
		/* Verification keeps unrelated slots, but its disk reload supplies
		 * no target birth. Any failure still discards the complete cache. */
		retain_cache = ok && op->normalized_info == CLOG_ZEROPAGE_FULL;
		finished = true;
	}
	PG_FINALLY();
	{
		if (locked && LWLockHeldByMe(lock)) {
			if (!retain_cache) {
				/* ERROR clears holdoffs even for a still-owned native lock. */
				if (!finished && LWLockHeldByMe(ctl->shared->ControlLock))
					HOLD_INTERRUPTS();
				clog_recovery_cache_discard(ctl);
			}
			if (!finished)
				HOLD_INTERRUPTS();
			LWLockRelease(lock);
		}
		if (!verify)
			clog_relmap_native_mutation_end(origin);
	}
	PG_END_TRY();
	return ok;
}

/* Restore the failed origin's native record, never the founder's horizons.
 * The caller retains complete source/base/era and ordered-record obligations. */
bool
cluster_clog_native_recovery_record_apply_v1(int origin,
											 const ClusterSideProjectionOperationV1 *operation)
{
	return clog_recovery_record(origin, operation, false, NULL, 0);
}

bool
cluster_clog_native_recovery_record_verify_v1(int origin,
											  const ClusterSideProjectionOperationV1 *operation)
{
	return clog_recovery_record(origin, operation, true, NULL, 0);
}

bool
cluster_clog_native_recovery_full_record_apply_v1(
	int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length)
{
	return operation != NULL && operation->normalized_info == CLOG_ZEROPAGE_FULL
		&& clog_recovery_record(origin, operation, false, payload, length);
}

bool
cluster_clog_native_recovery_full_record_verify_v1(
	int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length)
{
	return operation != NULL && operation->normalized_info == CLOG_ZEROPAGE_FULL
		&& clog_recovery_record(origin, operation, true, payload, length);
}

static void
clog_recovery_shmem_init(void)
{
	bool found;

	ClogRelmapMutations = ShmemInitStruct("CLOG relmap mutation",
		CLOG_RECOVERY_ORIGINS * sizeof(ClogRelmapMutation), &found);
	if (!found)
		for (int origin = 0; origin < CLOG_RECOVERY_ORIGINS; ++origin) {
			pg_atomic_init_u64(&ClogRelmapMutations[origin].generation, 1);
			pg_atomic_init_u32(&ClogRelmapMutations[origin].writers, 0);
		}
	ClogRecoveryLocks = ShmemInitStruct("CLOG recovery locks",
										2 * CLOG_RECOVERY_ORIGINS * sizeof(LWLockPadded), &found);
	for (int origin = 0; origin < CLOG_RECOVERY_ORIGINS; origin++) {
		SlruCtl ctl = &ClogRecoveryCtl[origin];
		LWLock *lock = &ClogRecoveryLocks[origin * 2].lock;
		char name[64], path[MAXPGPATH];
		int length;

		if (!found) {
			LWLockInitialize(lock, LWTRANCHE_XACT_BUFFER);
			LWLockInitialize(&ClogRecoveryLocks[origin * 2 + 1].lock, LWTRANCHE_XACT_BUFFER);
		}
		if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] != '/')
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("shared CLOG recovery requires an absolute shared root")));
		length = snprintf(path, sizeof(path), "%s/native_side/origin_%d/pg_xact",
						  cluster_shared_data_dir, origin);
		if (length < 0 || length >= sizeof(path) - 16)
			ereport(FATAL, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							errmsg("shared CLOG recovery path is too long")));
		snprintf(name, sizeof(name), "CLOG recovery %d", origin);
		ctl->PagePrecedes = CLOGPagePrecedes;
		SimpleLruInit(ctl, name, CLOG_RECOVERY_BUFFERS, 0, lock, path, LWTRANCHE_XACT_BUFFER,
					  SYNC_HANDLER_NONE);
		ctl->WritePageObserve = clog_relmap_slru_write_observe;
	}
}
#endif

/*
 * Initialization of shared memory for CLOG
 */
Size
CLOGShmemSize(void)
{
	Size size = SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE);

#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config) {
		size = add_size(
			size, mul_size(CLOG_RECOVERY_ORIGINS, SimpleLruShmemSize(CLOG_RECOVERY_BUFFERS, 0)));
		size = add_size(size, mul_size(2 * CLOG_RECOVERY_ORIGINS, sizeof(LWLockPadded)));
		size = add_size(size, mul_size(CLOG_RECOVERY_ORIGINS, sizeof(ClogRelmapMutation)));
	}
#endif
	return size;
}

void
CLOGShmemInit(void)
{
	XactCtl->PagePrecedes = CLOGPagePrecedes;
	SimpleLruInit(XactCtl, "Xact", CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE,
				  XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER,
				  SYNC_HANDLER_CLOG);
	SlruPagePrecedesUnitTests(XactCtl, CLOG_XACTS_PER_PAGE);
#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config) {
		clog_recovery_shmem_init();
		XactCtl->WritePageObserve = clog_relmap_slru_write_observe;
	}
#endif
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
	volatile int slotno;

#ifdef USE_PGRAC_CLUSTER
	clog_relmap_native_mutation(cluster_node_id);
	PG_TRY();
	{
#endif
	slotno = SimpleLruZeroPage(XactCtl, pageno);

	if (writeXlog)
		WriteZeroPageXlogRec(pageno);
#ifdef USE_PGRAC_CLUSTER
	}
	PG_FINALLY();
	{
		clog_relmap_native_mutation_end(cluster_node_id);
	}
	PG_END_TRY();
#endif

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

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCLOG(void)
{
	TransactionId xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	int			pageno = TransactionIdToPage(xid);

#ifdef USE_PGRAC_CLUSTER
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
void
ExtendCLOG(TransactionId newestXact)
{
	int			pageno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCLOGPage(pageno, true);

	LWLockRelease(XactSLRULock);
}


/* A full page-generation birth is emitted only by the original native
 * allocator's explicit entry. The ordinary raw entry above remains legacy;
 * a caller's query or current epoch is never used to upgrade its records. */
static bool
clog_full_birth_valid(const xl_clog_zeropage_full *birth)
{
	TransactionId raw = (TransactionId)birth->first_full_xid;

	return birth->reserved_zero == 0 && birth->pageno >= 0
		&& TransactionIdIsNormal(raw)
		&& (TransactionIdToPgIndex(raw) == 0 || raw == FirstNormalTransactionId)
		&& TransactionIdToPage(raw) == birth->pageno;
}

void
ExtendCLOGFull(FullTransactionId newestXact)
{
	TransactionId raw = XidFromFullTransactionId(newestXact);
	xl_clog_zeropage_full birth = {0};
#ifdef USE_PGRAC_CLUSTER
	int slot;
#endif

	/* This is a production guard, including release builds. The original
	 * caller supplies its actual candidate, not an inferred raw-xid era. */
	if (!LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE) || !TransactionIdIsNormal(raw))
		elog(ERROR, "full CLOG page birth requires the original native issuer");
	if (TransactionIdToPgIndex(raw) != 0 && raw != FirstNormalTransactionId)
		return;
	birth.pageno = TransactionIdToPage(raw);
	birth.first_full_xid = U64FromFullTransactionId(newestXact);

	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
#ifdef USE_PGRAC_CLUSTER
	slot = ZeroCLOGPage(birth.pageno, false);
#else
	ZeroCLOGPage(birth.pageno, false);
#endif
	XLogBeginInsert();
	XLogRegisterData((char *)&birth, sizeof(birth));
	(void)XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE_FULL);
#ifdef USE_PGRAC_CLUSTER
	/* Failed insertion leaves the original dirty page with unknown birth. */
	XactCtl->shared->page_initial_full_xid[slot] = birth.first_full_xid;
#endif
	LWLockRelease(XactSLRULock);
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

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!SlruScanDirectory(XactCtl, SlruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

#ifdef USE_PGRAC_CLUSTER
	clog_relmap_native_mutation(cluster_node_id);
	PG_TRY();
	{
#endif
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
#ifdef USE_PGRAC_CLUSTER
	}
	PG_FINALLY();
	{
		clog_relmap_native_mutation_end(cluster_node_id);
	}
	PG_END_TRY();
#endif
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

	/* Backup blocks are not used in clog records */
	Assert(info == CLOG_ZEROPAGE_FULL || !XLogRecHasAnyBlockRefs(record));

	if (info == CLOG_ZEROPAGE_FULL)
	{
		xl_clog_zeropage_full birth;
		int slotno;

		/* Check every FULL byte/geometry before touching native shared state.
		 * Legacy WAL keeps its original independent branch below. */
		if (XLogRecHasAnyBlockRefs(record) || XLogRecGetData(record) == NULL
			|| XLogRecGetDataLen(record) != sizeof(birth))
			elog(PANIC, "clog_redo: invalid full page birth record");
		memcpy(&birth, XLogRecGetData(record), sizeof(birth));
		if (!clog_full_birth_valid(&birth))
			elog(PANIC, "clog_redo: invalid full page birth generation");
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		slotno = ZeroCLOGPage(birth.pageno, false);
		SimpleLruWritePage(XactCtl, slotno);
		Assert(!XactCtl->shared->page_dirty[slotno]);
#ifdef USE_PGRAC_CLUSTER
		XactCtl->shared->page_initial_full_xid[slotno] = birth.first_full_xid;
#endif
		LWLockRelease(XactSLRULock);
	}
	else if (info == CLOG_ZEROPAGE)
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

#ifdef USE_PGRAC_CLUSTER
		clog_relmap_native_mutation(cluster_node_id);
		PG_TRY();
		{
#endif
		AdvanceOldestClogXid(xlrec.oldestXact);

		SimpleLruTruncate(XactCtl, xlrec.pageno);
#ifdef USE_PGRAC_CLUSTER
		}
		PG_FINALLY();
		{
			clog_relmap_native_mutation_end(cluster_node_id);
		}
		PG_END_TRY();
#endif
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
	return SlruSyncFileTag(XactCtl, ftag, path);
}
