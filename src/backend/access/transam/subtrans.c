/*-------------------------------------------------------------------------
 *
 * subtrans.c
 *		PostgreSQL subtransaction-log manager
 *
 * The pg_subtrans manager is a pg_xact-like manager that stores the parent
 * transaction Id for each transaction.  It is a fundamental part of the
 * nested transactions implementation.  A main transaction has a parent
 * of InvalidTransactionId, and each subtransaction has its immediate parent.
 * The tree can easily be walked from child to parent, but not in the
 * opposite direction.
 *
 * This code is based on xact.c, but the robustness requirements
 * are completely different from pg_xact, because we only need to remember
 * pg_subtrans information for currently-open transactions.  Thus, there is
 * no need to preserve data over a crash and restart.
 *
 * There are no XLOG interactions since we do not care about preserving
 * data across crashes.  During database startup, we simply force the
 * currently-active page of SUBTRANS to zeroes.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/subtrans.c
 *
 * PGRAC: shared-origin clean startup preserves retained parentage instead of
 * applying the single-instance page reset. It does not grant retention or
 * mutation authority. Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_undo_recovery.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "storage/proc.h"
#include "utils/wait_event.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlogutils.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#endif

/*
 * Defines for SubTrans page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * SubTrans page numbering also wraps around at
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE, and segment numbering at
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateSUBTRANS (see SubTransPagePrecedes) and zeroing
 * them in StartupSUBTRANS.
 */

/* We need four bytes per xact */
#define SUBTRANS_XACTS_PER_PAGE (BLCKSZ / sizeof(TransactionId))

#ifdef USE_PGRAC_CLUSTER
StaticAssertDecl(SUBTRANS_XACTS_PER_PAGE == CLUSTER_NATIVE_SUBTRANS_PER_PAGE,
				 "native SUBTRANS inspection geometry");
#endif

#define TransactionIdToPage(xid) ((xid) / (TransactionId) SUBTRANS_XACTS_PER_PAGE)
#define TransactionIdToEntry(xid) ((xid) % (TransactionId) SUBTRANS_XACTS_PER_PAGE)


/*
 * Link to shared-memory data structures for SUBTRANS control
 */
static SlruCtlData SubTransCtlData;

#define SubTransCtl  (&SubTransCtlData)


#ifdef USE_PGRAC_CLUSTER
/* Original-origin recovery never aliases the local live SUBTRANS cache. */
#define SUBTRANS_RECOVERY_ORIGINS 128
#define SUBTRANS_RECOVERY_BUFFERS 2
static SlruCtlData SubtransRecoveryCtl[SUBTRANS_RECOVERY_ORIGINS];
static LWLockPadded *SubtransRecoveryLocks;
#endif

static int	ZeroSUBTRANSPage(int pageno);
static bool SubTransPagePrecedes(int page1, int page2);


/*
 * Record the parent of a subtransaction in the subtrans log.
 */
void
SubTransSetParent(TransactionId xid, TransactionId parent)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToEntry(xid);
	int			slotno;
	TransactionId *ptr;

	Assert(TransactionIdIsValid(parent));
	Assert(TransactionIdFollows(xid, parent));

	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(SubTransCtl, pageno, true, xid);
	ptr = (TransactionId *) SubTransCtl->shared->page_buffer[slotno];
	ptr += entryno;

	/*
	 * It's possible we'll try to set the parent xid multiple times but we
	 * shouldn't ever be changing the xid from one valid xid to another valid
	 * xid, which would corrupt the data structure.
	 */
	if (*ptr != parent)
	{
		Assert(*ptr == InvalidTransactionId);
		*ptr = parent;
		SubTransCtl->shared->page_dirty[slotno] = true;
	}

	LWLockRelease(SubtransSLRULock);
}

/*
 * Interrogate the parent of a transaction in the subtrans log.
 */
TransactionId
SubTransGetParent(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToEntry(xid);
	int			slotno;
	TransactionId *ptr;
	TransactionId parent;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	/* Bootstrap and frozen XIDs have no parent */
	if (!TransactionIdIsNormal(xid))
		return InvalidTransactionId;

	/* lock is acquired by SimpleLruReadPage_ReadOnly */

	slotno = SimpleLruReadPage_ReadOnly(SubTransCtl, pageno, xid);
	ptr = (TransactionId *) SubTransCtl->shared->page_buffer[slotno];
	ptr += entryno;

	parent = *ptr;

	LWLockRelease(SubtransSLRULock);

	return parent;
}

/*
 * SubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 */
TransactionId
SubTransGetTopmostTransaction(TransactionId xid)
{
	TransactionId parentXid = xid,
				previousXid = xid;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	while (TransactionIdIsValid(parentXid))
	{
		previousXid = parentXid;
		if (TransactionIdPrecedes(parentXid, TransactionXmin))
			break;
		parentXid = SubTransGetParent(parentXid);

		/*
		 * By convention the parent xid gets allocated first, so should always
		 * precede the child xid. Anything else points to a corrupted data
		 * structure that could lead to an infinite loop, so exit.
		 */
		if (!TransactionIdPrecedes(parentXid, previousXid))
			elog(ERROR, "pg_subtrans contains invalid entry: xid %u points to parent xid %u",
				 previousXid, parentXid);
	}

	Assert(TransactionIdIsValid(previousXid));

	return previousXid;
}


#ifdef USE_PGRAC_CLUSTER
/* PGRAC: exact native ASSIGNMENT effects only. Complete source/base/era and
 * the cold serial owner remain mandatory; these bytes are not terminal proof.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
subtrans_recovery_permitted(int origin)
{
	return cluster_shared_config && InRecovery && AmStartupProcess() && origin >= 0
		   && origin < SUBTRANS_RECOVERY_ORIGINS && origin != cluster_node_id
		   && SubtransRecoveryLocks != NULL && SubtransRecoveryCtl[origin].shared != NULL
		   && cluster_undo_recovery_cold_origin_authorized_v1(origin);
}

static void
subtrans_recovery_cache_discard(SlruCtl ctl)
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
		shared->page_dirty[slot] = false;
	}
	LWLockRelease(shared->ControlLock);
}

static bool
subtrans_recovery_directory_ready(int origin)
{
	const char *children[3] = { "native_side", NULL, "pg_subtrans" };
	int dirs[4] = { -1, -1, -1, -1 };
	char origin_name[32];
	char expected[MAXPGPATH];
	struct stat st, routed;
	int length;
	bool ok = false;

	if (!subtrans_recovery_permitted(origin) || cluster_shared_data_dir == NULL
		|| cluster_shared_data_dir[0] != '/')
		return false;
	snprintf(origin_name, sizeof(origin_name), "origin_%d", origin);
	children[1] = origin_name;
	length = snprintf(expected, sizeof(expected), "%s/native_side/%s/pg_subtrans",
					  cluster_shared_data_dir, origin_name);
	if (length < 0 || length >= sizeof(expected)
		|| strcmp(expected, SubtransRecoveryCtl[origin].Dir) != 0)
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
	ok = true;
done:
	for (int i = 3; i >= 0; i--)
		if (dirs[i] >= 0 && close(dirs[i]) != 0)
			ok = false;
	/* Revalidation can ERROR; do not keep raw directory FDs across it. */
	return ok && subtrans_recovery_permitted(origin);
}

/* ASSIGNMENT requires an existing complete base page. Missing retained
 * parentage is not a zero page or proof of an absent transaction. */
static bool
subtrans_recovery_page_identity(SlruCtl ctl, int page, struct stat *out)
{
	char path[MAXPGPATH];
	int length = snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, page / SLRU_PAGES_PER_SEGMENT);

	return length >= 0 && length < sizeof(path) && lstat(path, out) == 0 && S_ISREG(out->st_mode)
		   && out->st_nlink == 1 && out->st_uid == geteuid()
		   && (out->st_mode & (S_IWGRP | S_IWOTH)) == 0
		   && out->st_size >= (off_t)(page % SLRU_PAGES_PER_SEGMENT + 1) * BLCKSZ;
}

static bool
subtrans_recovery_same_file(const struct stat *before, const struct stat *after)
{
	return before->st_dev == after->st_dev && before->st_ino == after->st_ino
		   && before->st_size == after->st_size;
}

/* Generic fsync helpers may ignore directory errors or sync-fd close
 * errors. Recovery completion needs every one of these results explicitly. */
static bool
subtrans_recovery_sync(SlruCtl ctl, int page, const struct stat *expected)
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
				|| !subtrans_recovery_same_file(&before, expected))))
		return false;
	PG_TRY();
	{
		fd = OpenTransientFile(path, (directory ? O_RDONLY | O_DIRECTORY : O_RDWR) | O_NOFOLLOW
										 | O_CLOEXEC | PG_BINARY);
		if (fd >= 0 && fstat(fd, &opened) == 0 && subtrans_recovery_same_file(&before, &opened)) {
			pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
			ok = pg_fsync(fd) == 0;
			pgstat_report_wait_end();
			ok = ok && fstat(fd, &after) == 0 && subtrans_recovery_same_file(&before, &after)
				 && lstat(path, &after) == 0 && subtrans_recovery_same_file(&before, &after);
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

typedef enum SubtransRecoveryAccess {
	SUBTRANS_RECOVERY_PREFLIGHT,
	SUBTRANS_RECOVERY_WRITE,
	SUBTRANS_RECOVERY_VERIFY
} SubtransRecoveryAccess;

static bool
subtrans_recovery_page(int origin, int page, TransactionId top, int count,
					   const TransactionId *children, SubtransRecoveryAccess access)
{
	SlruCtl ctl = &SubtransRecoveryCtl[origin];
	struct stat before, after;
	TransactionId *slots;
	int slot;
	bool ok;

	subtrans_recovery_cache_discard(ctl);
	if (!subtrans_recovery_directory_ready(origin)
		|| !subtrans_recovery_page_identity(ctl, page, &before))
		return false;
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	slot = SimpleLruReadPage(ctl, page, true, top);
	slots = (TransactionId *)ctl->shared->page_buffer[slot];
	ok = subtrans_recovery_permitted(origin) && subtrans_recovery_page_identity(ctl, page, &after)
		 && subtrans_recovery_same_file(&before, &after);
	for (int i = 0; ok && i < count; i++) {
		TransactionId current = slots[TransactionIdToEntry(children[i])];

		/* A retained different parent cannot be relabelled as the top. */
		ok = current == top
			 || (access != SUBTRANS_RECOVERY_VERIFY && current == InvalidTransactionId);
	}
	if (ok && access == SUBTRANS_RECOVERY_WRITE) {
		ok = subtrans_recovery_permitted(origin)
			 && subtrans_recovery_page_identity(ctl, page, &after)
			 && subtrans_recovery_same_file(&before, &after);
		if (ok) {
			for (int i = 0; i < count; i++)
				slots[TransactionIdToEntry(children[i])] = top;
			ctl->shared->page_dirty[slot] = true;
			SimpleLruWritePage(ctl, slot);
		}
	}
	LWLockRelease(ctl->shared->ControlLock);
	if (!ok)
		return false;
	if (access == SUBTRANS_RECOVERY_WRITE
		&& (!subtrans_recovery_permitted(origin) || !subtrans_recovery_sync(ctl, page, &before)))
		return false;
	return subtrans_recovery_directory_ready(origin)
		   && subtrans_recovery_page_identity(ctl, page, &after)
		   && subtrans_recovery_same_file(&before, &after);
}

static bool
subtrans_recovery_children(int origin, TransactionId top, int count, const TransactionId *children,
						   SubtransRecoveryAccess access)
{
	for (int first = 0; first < count;) {
		int last = first + 1;
		int page = TransactionIdToPage(children[first]);

		while (last < count && TransactionIdToPage(children[last]) == page)
			last++;
		if (!subtrans_recovery_page(origin, page, top, last - first, children + first, access))
			return false;
		first = last;
	}
	return true;
}

static bool
subtrans_recovery_access(int origin, TransactionId top, int count, const TransactionId *children,
						 SubtransRecoveryAccess access)
{
	volatile bool locked = false;
	volatile bool ok = false;
	LWLock *lock;
	TransactionId previous = top;

	/* Same bounded list emitted by AssignTransactionId; native wrap ordering
	 * is preserved. No parent-chain or transaction-status inference occurs. */
	if (!subtrans_recovery_permitted(origin) || !TransactionIdIsNormal(top) || count <= 0
		|| count > PGPROC_MAX_CACHED_SUBXIDS || children == NULL)
		return false;
	for (int i = 0; i < count; i++) {
		if (!TransactionIdIsNormal(children[i]) || !TransactionIdPrecedes(previous, children[i])
			|| !TransactionIdPrecedes(top, children[i])
			|| (uint32)(children[i] - top) >= UINT32_C(0x80000000))
			return false;
		previous = children[i];
	}
	if (access == SUBTRANS_RECOVERY_WRITE && !cluster_undo_recovery_cold_source_synced_v1(origin))
		return false;
	lock = &SubtransRecoveryLocks[origin * 2 + 1].lock;
	PG_TRY();
	{
		LWLockAcquire(lock, LW_EXCLUSIVE);
		locked = true;
		subtrans_recovery_cache_discard(&SubtransRecoveryCtl[origin]);
		ok = subtrans_recovery_directory_ready(origin)
			 && subtrans_recovery_children(
				 origin, top, count, children,
				 access == SUBTRANS_RECOVERY_WRITE ? SUBTRANS_RECOVERY_PREFLIGHT : access);
		/* All pages and slots must pass before the first native write. */
		if (ok && access == SUBTRANS_RECOVERY_WRITE) {
			ok = subtrans_recovery_children(origin, top, count, children, access);
			if (ok) {
				ok = subtrans_recovery_sync(&SubtransRecoveryCtl[origin], -1, NULL)
					 && subtrans_recovery_directory_ready(origin)
					 && subtrans_recovery_children(origin, top, count, children,
												   SUBTRANS_RECOVERY_VERIFY);
			}
		}
	}
	PG_FINALLY();
	{
		if (locked) {
			subtrans_recovery_cache_discard(&SubtransRecoveryCtl[origin]);
			LWLockRelease(lock);
		}
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_subtrans_native_recovery_preflight_v1(int origin, TransactionId top, int count,
											  const TransactionId *children)
{
	return subtrans_recovery_access(origin, top, count, children, SUBTRANS_RECOVERY_PREFLIGHT);
}

bool
cluster_subtrans_native_recovery_apply_v1(int origin, TransactionId top, int count,
										  const TransactionId *children)
{
	return subtrans_recovery_access(origin, top, count, children, SUBTRANS_RECOVERY_WRITE);
}

bool
cluster_subtrans_native_recovery_verify_v1(int origin, TransactionId top, int count,
										   const TransactionId *children)
{
	return subtrans_recovery_access(origin, top, count, children, SUBTRANS_RECOVERY_VERIFY);
}

static void
subtrans_recovery_shmem_init(void)
{
	bool found;

	SubtransRecoveryLocks = ShmemInitStruct(
		"SUBTRANS recovery locks", 2 * SUBTRANS_RECOVERY_ORIGINS * sizeof(LWLockPadded), &found);
	for (int origin = 0; origin < SUBTRANS_RECOVERY_ORIGINS; origin++) {
		SlruCtl ctl = &SubtransRecoveryCtl[origin];
		LWLock *lock = &SubtransRecoveryLocks[origin * 2].lock;
		char name[64], path[MAXPGPATH];
		int length;

		if (!found) {
			LWLockInitialize(lock, LWTRANCHE_SUBTRANS_BUFFER);
			LWLockInitialize(&SubtransRecoveryLocks[origin * 2 + 1].lock,
							 LWTRANCHE_SUBTRANS_BUFFER);
		}
		if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] != '/')
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("shared SUBTRANS recovery requires an absolute shared root")));
		length = snprintf(path, sizeof(path), "%s/native_side/origin_%d/pg_subtrans",
						  cluster_shared_data_dir, origin);
		if (length < 0 || length >= sizeof(path) - 16)
			ereport(FATAL, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							errmsg("shared SUBTRANS recovery path is too long")));
		snprintf(name, sizeof(name), "SUBTRANS recovery %d", origin);
		ctl->PagePrecedes = SubTransPagePrecedes;
		SimpleLruInit(ctl, name, SUBTRANS_RECOVERY_BUFFERS, 0, lock, path,
					  LWTRANCHE_SUBTRANS_BUFFER, SYNC_HANDLER_NONE);
	}
}
#endif

/*
 * Initialization of shared memory for SUBTRANS
 */
Size
SUBTRANSShmemSize(void)
{
	Size size = SimpleLruShmemSize(NUM_SUBTRANS_BUFFERS, 0);

#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config) {
		size = add_size(size, mul_size(SUBTRANS_RECOVERY_ORIGINS,
									 SimpleLruShmemSize(SUBTRANS_RECOVERY_BUFFERS, 0)));
		size = add_size(size, mul_size(2 * SUBTRANS_RECOVERY_ORIGINS, sizeof(LWLockPadded)));
	}
#endif
	return size;
}

void
SUBTRANSShmemInit(void)
{
	SyncRequestHandler handler = SYNC_HANDLER_NONE;

#ifdef USE_PGRAC_CLUSTER
	/* Retained parentage is durable input, unlike native ephemeral SUBTRANS.
	 * Use the same queued-fsync/fallback mechanism as the other SLRUs. */
	if (cluster_shared_config)
		handler = SYNC_HANDLER_CLUSTER_SUBTRANS;
#endif
	SubTransCtl->PagePrecedes = SubTransPagePrecedes;
	SimpleLruInit(SubTransCtl, "Subtrans", NUM_SUBTRANS_BUFFERS, 0, SubtransSLRULock, "pg_subtrans",
				  LWTRANCHE_SUBTRANS_BUFFER, handler);
	SlruPagePrecedesUnitTests(SubTransCtl, SUBTRANS_XACTS_PER_PAGE);
#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config)
		subtrans_recovery_shmem_init();
#endif
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial SUBTRANS segment.  (The SUBTRANS directory is assumed to
 * have been created by the initdb shell script, and SUBTRANSShmemInit
 * must have been called already.)
 *
 * Note: it's not really necessary to create the initial segment now,
 * since slru.c would create it on first write anyway.  But we may as well
 * do it to be sure the directory is set up correctly.
 */
void
BootStrapSUBTRANS(void)
{
	int			slotno;

	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);

	/* Create and zero the first page of the subtrans log */
	slotno = ZeroSUBTRANSPage(0);

	/* Make sure it's written out */
	SimpleLruWritePage(SubTransCtl, slotno);
	Assert(!SubTransCtl->shared->page_dirty[slotno]);

	LWLockRelease(SubtransSLRULock);
}

/*
 * Initialize (or reinitialize) a page of SUBTRANS to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroSUBTRANSPage(int pageno)
{
	return SimpleLruZeroPage(SubTransCtl, pageno);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupSUBTRANS(TransactionId oldestActiveXID)
{
	FullTransactionId nextXid;
	int			startPage;
	int			endPage;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config) {
		TransactionId next = XidFromFullTransactionId(ShmemVariableCache->nextXid);

		/* Failed-origin replay must finish before this clean-successor cut.
		 * Prepared transactions and hot standby cannot borrow it. InRecovery
		 * must also be false so a missing SLRU page is not read as all zeroes. */
		if (MyBackendType != B_STARTUP || InRecovery || !TransactionIdIsNormal(next)
			|| !TransactionIdEquals(oldestActiveXID, next))
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("shared SUBTRANS startup requires a completed recovery input")));
		LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);
		endPage = TransactionIdToPage(next);
		SubTransCtl->shared->latest_page_number = endPage;
		if (TransactionIdToEntry(next) != 0) {
			int slotno = SimpleLruReadPage(SubTransCtl, endPage, false, next);
			if (!cluster_native_subtrans_suffix_unused(SubTransCtl->shared->page_buffer[slotno],
													   next)) {
				LWLockRelease(SubtransSLRULock);
				ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("shared SUBTRANS startup has parentage beyond nextXid")));
			}
		}
		/* At a page boundary the allocator will perform the first allocation.
		 * Startup does not create or overwrite even an existing boundary page. */
		LWLockRelease(SubtransSLRULock);
		return;
	}
#endif

	/*
	 * Since we don't expect pg_subtrans to be valid across crashes, we
	 * initialize the currently-active page(s) to zeroes during startup.
	 * Whenever we advance into a new page, ExtendSUBTRANS will likewise zero
	 * the new page without regard to whatever was previously on disk.
	 */
	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);

	startPage = TransactionIdToPage(oldestActiveXID);
	nextXid = ShmemVariableCache->nextXid;
	endPage = TransactionIdToPage(XidFromFullTransactionId(nextXid));

	while (startPage != endPage)
	{
		(void) ZeroSUBTRANSPage(startPage);
		startPage++;
		/* must account for wraparound */
		if (startPage > TransactionIdToPage(MaxTransactionId))
			startPage = 0;
	}
	(void) ZeroSUBTRANSPage(startPage);

	LWLockRelease(SubtransSLRULock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointSUBTRANS(void)
{
	/*
	 * Write dirty SUBTRANS pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 * PGRAC shared origins are the exception: their registered sync handler
	 * makes these writes part of the retained-input checkpoint obligation.
	 */
	TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_START(true);
	SimpleLruWriteAll(SubTransCtl, true);
	TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_DONE(true);
}

#ifdef USE_PGRAC_CLUSTER
/* PGRAC: exact own-origin route is fixed by startup qualification. The native
 * sync consumer propagates I/O failure; no successful checkpoint on error. */
int
subtranssyncfiletag(const FileTag *ftag, char *path)
{
	return SlruSyncFileTag(SubTransCtl, ftag, path);
}
#endif


/*
 * Make sure that SUBTRANS has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty subtrans page to make room
 * in shared memory.
 */
void
ExtendSUBTRANS(TransactionId newestXact)
{
	int			pageno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToEntry(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);

	/* Zero the page */
	ZeroSUBTRANSPage(pageno);

	LWLockRelease(SubtransSLRULock);
}


/*
 * Remove all SUBTRANS segments before the one holding the passed transaction ID
 *
 * oldestXact is the oldest TransactionXmin of any running transaction.  This
 * is called only during checkpoint.
 */
void
TruncateSUBTRANS(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.  We step
	 * back one transaction to avoid passing a cutoff page that hasn't been
	 * created yet in the rare case that oldestXact would be the first item on
	 * a page and oldestXact == next XID.  In that case, if we didn't subtract
	 * one, we'd trigger SimpleLruTruncate's wraparound detection.
	 */
	TransactionIdRetreat(oldestXact);
	cutoffPage = TransactionIdToPage(oldestXact);

	SimpleLruTruncate(SubTransCtl, cutoffPage);
}


/*
 * Decide whether a SUBTRANS page number is "older" for truncation purposes.
 * Analogous to CLOGPagePrecedes().
 */
static bool
SubTransPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * SUBTRANS_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId + 1;
	xid2 = ((TransactionId) page2) * SUBTRANS_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId + 1;

	return (TransactionIdPrecedes(xid1, xid2) &&
			TransactionIdPrecedes(xid1, xid2 + SUBTRANS_XACTS_PER_PAGE - 1));
}
