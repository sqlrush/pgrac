/*-------------------------------------------------------------------------
 *
 * slru.c
 *		Simple LRU buffering for transaction status logfiles
 *
 * We use a simple least-recently-used scheme to manage a pool of page
 * buffers.  Under ordinary circumstances we expect that write
 * traffic will occur mostly to the latest page (and to the just-prior
 * page, soon after a page transition).  Read traffic will probably touch
 * a larger span of pages, but in any case a fairly small number of page
 * buffers should be sufficient.  So, we just search the buffers using plain
 * linear search; there's no need for a hashtable or anything fancy.
 * The management algorithm is straight LRU except that we will never swap
 * out the latest page (since we know it's going to be hit again eventually).
 *
 * We use a control LWLock to protect the shared data structures, plus
 * per-buffer LWLocks that synchronize I/O for each buffer.  The control lock
 * must be held to examine or modify any shared state.  A process that is
 * reading in or writing out a page buffer does not hold the control lock,
 * only the per-buffer lock for the buffer it is working on.
 *
 * "Holding the control lock" means exclusive lock in all cases except for
 * SimpleLruReadPage_ReadOnly(); see comments for SlruRecentlyUsed() for
 * the implications of that.
 *
 * When initiating I/O on a buffer, we acquire the per-buffer lock exclusively
 * before releasing the control lock.  The per-buffer lock is released after
 * completing the I/O, re-acquiring the control lock, and updating the shared
 * state.  (Deadlock is not possible here, because we never try to initiate
 * I/O when someone else is already doing I/O on the same buffer.)
 * To wait for I/O to complete, release the control lock, acquire the
 * per-buffer lock in shared mode, immediately release the per-buffer lock,
 * reacquire the control lock, and then recheck state (since arbitrary things
 * could have happened while we didn't have the lock).
 *
 * As with the regular buffer manager, it is possible for another process
 * to re-dirty a page that is currently being written out.  This is handled
 * by re-setting the page's page_dirty flag.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/slru.c
 *
 * PGRAC MODIFICATIONS
 *   Modified by: SqlRush <sqlrush@gmail.com>
 *   Absorb queued shared SUBTRANS writes before forgetting a deleted segment.
 *   spec-3.5-subtrans-cross-node-visibility.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "access/transam.h"
#include "access/clog_storage.h"
#include "access/clog_wal.h"
#include "access/xloginsert.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#ifdef USE_PGRAC_CLUSTER
#include "postmaster/bgwriter.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"
#include "storage/proc.h"
#endif
#include "storage/fd.h"
#include "storage/shmem.h"

#define SlruFileName(ctl, path, seg) \
	snprintf(path, MAXPGPATH, "%s/%04X", (ctl)->Dir, seg)

/*
 * During SimpleLruWriteAll(), we will usually not need to write more than one
 * or two physical files, but we may need to write several pages per file.  We
 * can consolidate the I/O requests by leaving files open until control returns
 * to SimpleLruWriteAll().  This data structure remembers which files are open.
 */
#define MAX_WRITEALL_BUFFERS	16

typedef struct SlruWriteAllData
{
	int			num_files;		/* # files actually open */
	int			fd[MAX_WRITEALL_BUFFERS];	/* their FD's */
	int			segno[MAX_WRITEALL_BUFFERS];	/* their log seg#s */
} SlruWriteAllData;

typedef struct SlruWriteAllData *SlruWriteAll;

/* CLOG page bytes and their WAL dependency captured under ControlLock. */
typedef struct SlruWriteSnapshot
{
	PGAlignedBlock page;
	XLogRecPtr	flush_lsn;
	ClusterClogPageIdentityV2 clog_page;
} SlruWriteSnapshot;

/*
 * Populate a file tag describing a segment file.  We only use the segment
 * number, since we can derive everything else we need by having separate
 * sync handler functions for clog, multixact etc.
 */
#define INIT_SLRUFILETAG(a,xx_handler,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = (xx_handler), \
	(a).segno = (xx_segno) \
)

/*
 * Macro to mark a buffer slot "most recently used".  Note multiple evaluation
 * of arguments!
 *
 * The reason for the if-test is that there are often many consecutive
 * accesses to the same page (particularly the latest page).  By suppressing
 * useless increments of cur_lru_count, we reduce the probability that old
 * pages' counts will "wrap around" and make them appear recently used.
 *
 * We allow this code to be executed concurrently by multiple processes within
 * SimpleLruReadPage_ReadOnly().  As long as int reads and writes are atomic,
 * this should not cause any completely-bogus values to enter the computation.
 * However, it is possible for either cur_lru_count or individual
 * page_lru_count entries to be "reset" to lower values than they should have,
 * in case a process is delayed while it executes this macro.  With care in
 * SlruSelectLRUPage(), this does little harm, and in any case the absolute
 * worst possible consequence is a nonoptimal choice of page to evict.  The
 * gain from allowing concurrent reads of SLRU pages seems worth it.
 */
#define SlruRecentlyUsed(shared, slotno)	\
	do { \
		int		new_lru_count = (shared)->cur_lru_count; \
		if (new_lru_count != (shared)->page_lru_count[slotno]) { \
			(shared)->cur_lru_count = ++new_lru_count; \
			(shared)->page_lru_count[slotno] = new_lru_count; \
		} \
	} while (0)

/* Saved info for SlruReportIOError */
typedef enum
{
	SLRU_OPEN_FAILED,
	SLRU_SEEK_FAILED,
	SLRU_READ_FAILED,
	SLRU_WRITE_FAILED,
	SLRU_FSYNC_FAILED,
	SLRU_CLOSE_FAILED
} SlruErrorCause;

static SlruErrorCause slru_errcause;
static int	slru_errno;


static void SimpleLruZeroLSNs(SlruCtl ctl, int slotno);
static void SimpleLruWaitIO(SlruCtl ctl, int slotno);
static void SlruInternalWritePage(SlruCtl ctl, int slotno, SlruWriteAll fdata);
static bool SlruWriteClogPage(SlruCtl ctl, int slotno, int fd,
							  const ClusterClogFileBindingV2 *expected,
							  bool bootstrap, XLogRecPtr *end_out);
static bool SlruPhysicalReadPage(SlruCtl ctl, int pageno, int slotno);
static bool SlruPhysicalWritePage(SlruCtl ctl, int pageno, int slotno,
								  SlruWriteAll fdata,
								  const SlruWriteSnapshot *snapshot);
static void SlruReportIOError(SlruCtl ctl, int pageno, TransactionId xid);
static int	SlruSelectLRUPage(SlruCtl ctl, int pageno);
#ifdef USE_PGRAC_CLUSTER
static bool SlruNativeClogRequired(SlruCtl ctl);
#endif

static bool SlruScanDirCbDeleteCutoff(SlruCtl ctl, char *filename,
									  int segpage, void *data);
static void SlruInternalDeleteSegment(SlruCtl ctl, int segno);

/*
 * Initialization of shared memory
 */

Size
SimpleLruShmemSize(int nslots, int nlsns)
{
	Size		sz;

	/* we assume nslots isn't so large as to risk overflow */
	sz = MAXALIGN(sizeof(SlruSharedData));
	sz += MAXALIGN(nslots * sizeof(char *));	/* page_buffer[] */
	sz += MAXALIGN(nslots * sizeof(SlruPageStatus));	/* page_status[] */
	sz += MAXALIGN(nslots * sizeof(bool));	/* page_dirty[] */
	sz += MAXALIGN(nslots * sizeof(int));	/* page_number[] */
#ifdef USE_PGRAC_CLUSTER
	sz += MAXALIGN(nslots * sizeof(uint64)); /* page_initial_full_xid[] */
#endif
	sz += MAXALIGN(nslots * sizeof(int));	/* page_lru_count[] */
	sz += MAXALIGN(nslots * sizeof(LWLockPadded));	/* buffer_locks[] */

	if (nlsns > 0)
	{
		sz += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));	/* group_lsn[] */
		sz += MAXALIGN(nslots * sizeof(ClusterClogPageIdentityV2));
		sz += MAXALIGN(nslots * sizeof(ClusterClogFileBindingV2));
	}

	return BUFFERALIGN(sz) + BLCKSZ * nslots;
}

/*
 * Initialize, or attach to, a simple LRU cache in shared memory.
 *
 * ctl: address of local (unshared) control structure.
 * name: name of SLRU.  (This is user-visible, pick with care!)
 * nslots: number of page slots to use.
 * nlsns: number of LSN groups per page (set to zero if not relevant).
 * ctllock: LWLock to use to control access to the shared control structure.
 * subdir: PGDATA-relative subdirectory that will contain the files.
 * tranche_id: LWLock tranche ID to use for the SLRU's per-buffer LWLocks.
 * sync_handler: which set of functions to use to handle sync requests
 */
void
SimpleLruInit(SlruCtl ctl, const char *name, int nslots, int nlsns,
			  LWLock *ctllock, const char *subdir, int tranche_id,
			  SyncRequestHandler sync_handler)
{
	SlruShared	shared;
	bool		found;

	/* PGRAC: never silently route a shared SLRU through a truncated path.
	 * Reserve the segment-name suffix used by SlruFileName(). */
	if (subdir == NULL || strlen(subdir) >= sizeof(ctl->Dir) - 16)
		ereport(ERROR, (errmsg("SLRU directory path is too long")));

	shared = (SlruShared) ShmemInitStruct(name,
										  SimpleLruShmemSize(nslots, nlsns),
										  &found);

	if (!IsUnderPostmaster)
	{
		/* Initialize locks and shared memory area */
		char	   *ptr;
		Size		offset;
		int			slotno;

		Assert(!found);

		memset(shared, 0, sizeof(SlruSharedData));

		shared->ControlLock = ctllock;

		shared->num_slots = nslots;
		shared->lsn_groups_per_page = nlsns;

		shared->cur_lru_count = 0;

		/* shared->latest_page_number will be set later */

		shared->slru_stats_idx = pgstat_get_slru_index(name);

		ptr = (char *) shared;
		offset = MAXALIGN(sizeof(SlruSharedData));
		shared->page_buffer = (char **) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(char *));
		shared->page_status = (SlruPageStatus *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(SlruPageStatus));
		shared->page_dirty = (bool *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(bool));
		shared->page_number = (int *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(int));
#ifdef USE_PGRAC_CLUSTER
		shared->page_initial_full_xid = (uint64 *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(uint64));
#endif
		shared->page_lru_count = (int *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(int));

		/* Initialize LWLocks */
		shared->buffer_locks = (LWLockPadded *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(LWLockPadded));

		if (nlsns > 0)
		{
			shared->group_lsn = (XLogRecPtr *) (ptr + offset);
			offset += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));
			shared->clog_page_identity = (ClusterClogPageIdentityV2 *) (ptr + offset);
			offset += MAXALIGN(nslots * sizeof(ClusterClogPageIdentityV2));
			memset(shared->clog_page_identity, 0,
				   nslots * sizeof(ClusterClogPageIdentityV2));
			shared->clog_file_binding = (ClusterClogFileBindingV2 *) (ptr + offset);
			offset += MAXALIGN(nslots * sizeof(ClusterClogFileBindingV2));
			memset(shared->clog_file_binding, 0,
				   nslots * sizeof(ClusterClogFileBindingV2));
		}

		ptr += BUFFERALIGN(offset);
		for (slotno = 0; slotno < nslots; slotno++)
		{
			LWLockInitialize(&shared->buffer_locks[slotno].lock,
							 tranche_id);

			shared->page_buffer[slotno] = ptr;
			shared->page_status[slotno] = SLRU_PAGE_EMPTY;
			shared->page_dirty[slotno] = false;
#ifdef USE_PGRAC_CLUSTER
			shared->page_initial_full_xid[slotno] = 0;
#endif
			shared->page_lru_count[slotno] = 0;
			ptr += BLCKSZ;
		}

		/* Should fit to estimated shmem size */
		Assert(ptr - (char *) shared <= SimpleLruShmemSize(nslots, nlsns));
	}
	else
		Assert(found);

	/*
	 * Initialize the unshared control struct, including directory path. We
	 * assume caller set PagePrecedes.
	 */
	ctl->shared = shared;
	ctl->sync_handler = sync_handler;
#ifdef USE_PGRAC_CLUSTER
	ctl->WritePageObserve = NULL;
#endif
	strlcpy(ctl->Dir, subdir, sizeof(ctl->Dir));
}

/* A managed cache entry cannot be consumed or overwritten through a raw
 * path. The complete binding comes from a managed physical read or the
 * original fresh-birth writer; legacy slots retain their original behavior. */
static void
SlruRequireRawSlot(SlruCtl ctl, int slotno)
{
	SlruShared shared = ctl->shared;

	if (shared->clog_file_binding != NULL &&
		shared->clog_file_binding[slotno].identity.format_id != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("managed CLOG cache page requires selected file I/O")));
}

static bool
SlruClogBindingMatches(const ClusterClogFileBindingV2 *left,
					   const ClusterClogFileBindingV2 *right)
{
	uint8 reference[CLUSTER_CLOG_FILE_REFERENCE_BYTES];

	return left->segment == right->segment &&
		cluster_recovery_file_identity_encode_v2(&left->identity, reference, sizeof(reference)) &&
		cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), &right->identity);
}

#ifdef USE_PGRAC_CLUSTER
static bool
SlruNativeClogRequired(SlruCtl ctl)
{
	return ctl->sync_handler == SYNC_HANDLER_CLOG &&
		ctl->clog_native_required != NULL && ctl->clog_native_required();
}

static void
SlruNativeClogRefuse(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("native CLOG requires a current selected file and original I/O custody")));
}

/* Drop only ControlLock while opening the original selected path. The same
 * address-bound table borrow survives this gap; no file creation or CF lookup.
 * The family lock is restored even when the FD owner raises ERROR. */
static int
SlruNativeClogOpen(SlruCtl ctl, int pageno, int flags)
{
	volatile int fd = -1;
	char path[MAXPGPATH];

	SlruFileName(ctl, path, pageno / SLRU_PAGES_PER_SEGMENT);
	LWLockRelease(ctl->shared->ControlLock);
	PG_TRY();
	{
		fd = OpenTransientFile(path, flags | PG_BINARY | O_NOFOLLOW);
	}
	PG_CATCH();
	{
		LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	return fd;
}

static void
SlruNativeClogAcquire(SlruCtl ctl, int pageno,
					  ClusterControlNativeFileBorrowV2 *borrow)
{
	if (!LWLockHeldByMeInMode(ctl->shared->ControlLock, LW_EXCLUSIVE) ||
		ctl->clog_native_acquire == NULL || ctl->clog_native_current == NULL ||
		ctl->clog_native_release == NULL ||
		!ctl->clog_native_acquire(pageno, borrow))
		SlruNativeClogRefuse();
}

/* Same original namespace, with no file creation or CF lookup. */
static int
SlruNativeClogOpenDirectory(SlruCtl ctl)
{
	volatile int fd = -1;

	LWLockRelease(ctl->shared->ControlLock);
	PG_TRY();
	{
		fd = OpenTransientFile(ctl->Dir, O_RDONLY | PG_BINARY | O_DIRECTORY | O_NOFOLLOW);
	}
	PG_CATCH();
	{
		LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	return fd;
}

bool
SimpleLruNativeClogSyncFileV2(SlruCtl ctl,
	const ClusterRecoveryFileIdentityV2 *expected, ClusterClogFileSyncV2 *out)
{
	typedef struct SlruClogSyncWork
	{
		ClusterControlNativeFileBorrowV2 borrow;
		int fd, directory_fd;
		bool control, borrowed;
		bool io_locked[FLEXIBLE_ARRAY_MEMBER];
	} SlruClogSyncWork;
	SlruClogSyncWork *work;
	SlruShared shared;
	ClusterClogFileBindingV2 binding;
	ClusterClogFileSyncV2 observed;
	volatile bool ok = false, body_complete = false;
	uintptr_t a = (uintptr_t) expected, b = (uintptr_t) out;

	if (ctl == NULL || expected == NULL || out == NULL ||
		(shared = ctl->shared) == NULL || shared->ControlLock == NULL ||
		shared->num_slots <= 0 || shared->buffer_locks == NULL ||
		!SlruNativeClogRequired(ctl) ||
		LWLockHeldByMe(shared->ControlLock) ||
		expected->kind != CLUSTER_RECOVERY_FILE_CLOG_V2 || expected->object_id >= 4096 ||
		(a <= b ? b - a < sizeof(*expected) : a - b < sizeof(*out)))
		return false;
	binding.identity = *expected;
	binding.segment = (uint32) expected->object_id;
	/* Allocate before acquiring any native lock. Cleanup state must survive
	 * ERROR's longjmp and real zeroed interrupt holdoff count. */
	work = palloc0(sizeof(*work) + (Size) shared->num_slots);
	work->fd = work->directory_fd = -1;
	PG_TRY();
	{
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
		work->control = true;
		SlruNativeClogAcquire(ctl, binding.segment * SLRU_PAGES_PER_SEGMENT, &work->borrow);
		work->borrowed = true;
		do {
			ClusterClogFileBindingV2 selected;

			selected.identity = work->borrow.expected;
			selected.segment = binding.segment;
			if (!SlruClogBindingMatches(&selected, &binding) ||
				!ctl->clog_native_current(&work->borrow)) break;
			work->fd = SlruNativeClogOpen(ctl, binding.segment * SLRU_PAGES_PER_SEGMENT, O_RDONLY);
			if (work->fd < 0 || !ctl->clog_native_current(&work->borrow)) break;
			work->directory_fd = SlruNativeClogOpenDirectory(ctl);
			if (work->directory_fd < 0 || !ctl->clog_native_current(&work->borrow)) break;
restart_sync:
			/* Wait can replace any slot while dropping ControlLock. Restart
			 * the complete file scan; never retain an old slot association. */
			for (int i = 0; i < shared->num_slots; i++)
			{
				if (shared->page_status[i] != SLRU_PAGE_EMPTY &&
					shared->page_number[i] / SLRU_PAGES_PER_SEGMENT == (int) binding.segment &&
					(shared->page_status[i] == SLRU_PAGE_READ_IN_PROGRESS ||
					 shared->page_status[i] == SLRU_PAGE_WRITE_IN_PROGRESS))
				{
					SimpleLruWaitIO(ctl, i);
					goto restart_sync;
				}
			}
			if (!ctl->clog_native_current(&work->borrow)) break;
			for (int i = 0; i < shared->num_slots; i++)
			{
				if (shared->page_status[i] != SLRU_PAGE_EMPTY &&
					shared->page_number[i] / SLRU_PAGES_PER_SEGMENT == (int) binding.segment)
				{
					if (!SlruClogBindingMatches(&shared->clog_file_binding[i], &binding))
						goto sync_done;
					LWLockAcquire(&shared->buffer_locks[i].lock, LW_EXCLUSIVE);
					work->io_locked[i] = true;
				}
			}
			/* Keeping ControlLock excludes every new same-file SLRU I/O,
			 * including files with no resident slot. No WAL flush is needed. */
			ok = cluster_clog_storage_sync_file_v2(work->fd, work->directory_fd, &binding, &observed)
				&& ctl->clog_native_current(&work->borrow);
		} while (0);
sync_done:
		body_complete = true;
	}
	PG_FINALLY();
	{
		if (!body_complete) InterruptHoldoffCount = 0;
		PG_TRY(finish_sync);
		{
			bool finished = true;

			for (int i = shared->num_slots - 1; i >= 0; i--)
			{
				if (work->io_locked[i])
				{
					if (!body_complete) HOLD_INTERRUPTS();
					LWLockRelease(&shared->buffer_locks[i].lock);
				}
			}
			if (work->fd >= 0 && CloseTransientFile(work->fd) != 0) finished = false;
			if (work->directory_fd >= 0 && CloseTransientFile(work->directory_fd) != 0) finished = false;
			if (work->borrowed && !ctl->clog_native_release(&work->borrow)) finished = false;
			/* A refused cleanup cannot discard its address-bound borrow or
			 * FD duty and return to a running checkpoint process. */
			if (!finished)
				ereport(PANIC, (errcode(ERRCODE_IO_ERROR),
								errmsg("could not finish native CLOG checkpoint cleanup")));
			if (work->control)
			{
				if (!body_complete) HOLD_INTERRUPTS();
				LWLockRelease(shared->ControlLock);
			}
			if (!body_complete) InterruptHoldoffCount = 0;
			pfree(work);
		}
		PG_CATCH(finish_sync);
		{
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("error during native CLOG checkpoint cleanup")));
		}
		PG_END_TRY(finish_sync);
	}
	PG_END_TRY();
	if (ok) *out = observed;
	return ok;
}

/* Cleanup does not forget a refused release. A retains that original borrow
 * duty; no success is returned and no alternate table row is acquired. */
static bool
SlruNativeClogFinish(SlruCtl ctl, ClusterControlNativeFileBorrowV2 *borrow, int fd)
{
	bool closed = fd < 0 || CloseTransientFile(fd) == 0;
	bool released = ctl->clog_native_release(borrow);

	return closed && released;
}

/* Birth is supplied by the original issuer, never recovered from empty disk
 * bytes. The original I/O reservation covers both probes and birth WAL. */
int
SimpleLruNativeClogZeroV2(SlruCtl ctl, FullTransactionId birth)
{
	SlruShared shared;
	ClusterControlNativeFileBorrowV2 borrow = {0};
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 page = {0};
	uint8 record[CLUSTER_CLOG_PAGE_WAL_BYTES_V2];
	uint8 zero[BLCKSZ] = {0};
	PGPROC *proc = MyProc;
	TransactionId xid = XidFromFullTransactionId(birth);
	int pageno = xid / 32768;
	volatile int fd = -1;
	volatile int slotno = -1;
	volatile bool io_locked = false;
	volatile bool control_dropped = false;
	volatile bool pending = false;
	volatile bool own_delay = false;
	volatile bool body_complete = false;

	if (ctl == NULL || (shared = ctl->shared) == NULL ||
		shared->ControlLock == NULL || shared->clog_page_identity == NULL ||
		shared->clog_file_binding == NULL || shared->group_lsn == NULL ||
		shared->lsn_groups_per_page <= 0 || shared->num_slots <= 0 ||
		!SlruNativeClogRequired(ctl) || !FullTransactionIdIsValid(birth) ||
		!TransactionIdIsNormal(xid) || U64FromFullTransactionId(birth) == PG_UINT64_MAX ||
		(xid % 32768 != 0 && xid != FirstNormalTransactionId) ||
		!LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE) ||
		!LWLockHeldByMeInMode(shared->ControlLock, LW_EXCLUSIVE) ||
		proc == NULL || !enableFsync || InRecovery || RecoveryInProgress() ||
		IsBootstrapProcessingMode())
		SlruNativeClogRefuse();
	SlruNativeClogAcquire(ctl, pageno, &borrow);
	binding.identity = borrow.expected;
	binding.segment = (uint32) pageno / SLRU_PAGES_PER_SEGMENT;
	PG_TRY();
	{
		page.identity.logical_page = pageno;
		page.identity.page_generation = U64FromFullTransactionId(birth);
		page.identity.birth_full_xid = birth;
		page.identity.birth_observed = true;
		if (binding.identity.origin != (cluster_enabled ? cluster_node_id : 0) ||
			binding.identity.system_identifier != GetSystemIdentifier() ||
			binding.identity.redo_timeline != GetWALInsertionTimeLine() ||
			!cluster_recovery_file_identity_encode_v2(&binding.identity, record, 128) ||
			!cluster_clog_file_page_encode_v2(&binding, &page, zero, BLCKSZ,
											 record + 128, CLUSTER_CLOG_FRAME_BYTES))
			SlruNativeClogRefuse();
		for (;;)
		{
			if (!ctl->clog_native_current(&borrow))
				SlruNativeClogRefuse();
			slotno = SlruSelectLRUPage(ctl, pageno);
			if (shared->page_number[slotno] == pageno &&
				shared->page_status[slotno] != SLRU_PAGE_EMPTY)
			{
				ClusterClogPageIdentityV2 *current;

				if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
					shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
				{
					SimpleLruWaitIO(ctl, slotno);
					continue;
				}
				current = &shared->clog_page_identity[slotno];
				if (shared->page_status[slotno] != SLRU_PAGE_VALID ||
					!SlruClogBindingMatches(&shared->clog_file_binding[slotno], &binding) ||
					!current->identity.birth_observed ||
					current->identity.logical_page != (uint64) pageno ||
					current->identity.page_generation != U64FromFullTransactionId(birth) ||
					!FullTransactionIdEquals(current->identity.birth_full_xid, birth) ||
					current->identity.page_lsn == PG_UINT64_MAX ||
					current->image_lsn == PG_UINT64_MAX)
					SlruNativeClogRefuse();
				/* A retry must preserve every already-published status bit. */
				SlruRecentlyUsed(shared, slotno);
				pgstat_count_slru_page_hit(shared->slru_stats_idx);
				break;
			}
			if (fd < 0)
			{
				fd = SlruNativeClogOpen(ctl, pageno, O_RDONLY);
				if (fd < 0)
					SlruNativeClogRefuse();
				continue; /* Open dropped ControlLock: reselect the original LRU. */
			}
			if (shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
				(shared->page_status[slotno] != SLRU_PAGE_VALID || shared->page_dirty[slotno]))
				SlruNativeClogRefuse();
			shared->page_number[slotno] = pageno;
			shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
			shared->page_dirty[slotno] = false;
			memset(&shared->clog_page_identity[slotno], 0, sizeof(page));
			shared->clog_file_binding[slotno] = binding;
			SimpleLruZeroLSNs(ctl, slotno);
			pending = true;
			LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);
			io_locked = true;
			own_delay = (proc->delayChkptFlags & DELAY_CHKPT_START) == 0;
			proc->delayChkptFlags |= DELAY_CHKPT_START;
			LWLockRelease(shared->ControlLock);
			control_dropped = true;
			if (cluster_clog_storage_page_unwritten_v2(fd, &binding, pageno))
			{
				XLogRecPtr end;

				XLogBeginInsert();
				XLogRegisterData((char *) record, sizeof(record));
				end = XLogInsert(RM_CLOG_ID, CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2);
				if (XLogRecPtrIsInvalid(end) || end == PG_UINT64_MAX)
					SlruNativeClogRefuse();
				XLogFlush(end);
				if (enableFsync && cluster_clog_storage_page_unwritten_v2(fd, &binding, pageno))
					page.identity.page_lsn = end;
			}
			LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
			control_dropped = false;
			if (XLogRecPtrIsInvalid(page.identity.page_lsn) ||
				!ctl->clog_native_current(&borrow) ||
				shared->page_number[slotno] != pageno ||
				shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS ||
				shared->page_dirty[slotno] ||
				!SlruClogBindingMatches(&shared->clog_file_binding[slotno], &binding))
				SlruNativeClogRefuse();
			memcpy(shared->page_buffer[slotno], zero, BLCKSZ);
			shared->clog_page_identity[slotno] = page;
			shared->page_status[slotno] = SLRU_PAGE_VALID;
			shared->page_dirty[slotno] = true;
			pending = false;
			SlruRecentlyUsed(shared, slotno);
			shared->latest_page_number = pageno;
			pgstat_count_slru_page_zeroed(shared->slru_stats_idx);
			break;
		}
		body_complete = true;
	}
	PG_FINALLY();
	{
		/* ERROR resets holdoffs but leaves locks held. Do not restore caller
		 * holds that abort cleanup will release through LWLockReleaseAll. */
		if (!body_complete)
			InterruptHoldoffCount = 0;
		PG_TRY(cleanup);
		{
			if (control_dropped)
				LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
			if (io_locked)
			{
				if (pending && shared->page_number[slotno] == pageno &&
					shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS &&
					SlruClogBindingMatches(&shared->clog_file_binding[slotno], &binding))
				{
					memset(&shared->clog_page_identity[slotno], 0, sizeof(page));
					memset(&shared->clog_file_binding[slotno], 0, sizeof(binding));
					shared->page_status[slotno] = SLRU_PAGE_EMPTY;
					shared->page_dirty[slotno] = false;
				}
				if (!body_complete)
					HOLD_INTERRUPTS(); /* match this old lock's retail release */
				LWLockRelease(&shared->buffer_locks[slotno].lock);
			}
			if (own_delay)
				proc->delayChkptFlags &= ~DELAY_CHKPT_START;
			if (!SlruNativeClogFinish(ctl, &borrow, fd))
				ereport(PANIC, (errcode(ERRCODE_IO_ERROR),
								errmsg("could not finish native CLOG birth cleanup")));
		}
		PG_CATCH(cleanup);
		{
			/* A second ERROR must not discard an unreleased identity/FD or
			 * reservation. Crash recovery retains the original WAL duties. */
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("error during native CLOG birth cleanup")));
		}
		PG_END_TRY(cleanup);
		if (!body_complete)
			InterruptHoldoffCount = 0; /* neutralize cleanup's ControlLock reacquire */
	}
	PG_END_TRY();
	return slotno;
}

/* Original Startup rmgr only. Independent AVAILABLE custody selects this local
 * file; record bytes provide an address and image, never file authority. This
 * entry owns both family and per-buffer I/O locks through fsync/readback and
 * cache publication. It never creates a segment or inserts recovery WAL. */
static void
SlruNativeClogReplay(SlruCtl ctl, uint8 info, const void *record, Size length,
					XLogRecPtr record_end)
{
	SlruShared shared = ctl->shared;
	ClusterClogWalObservationV2 observed;
	ClusterControlNativeFileBorrowV2 borrow = {0};
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 captured, checked;
	uint8 current[CLUSTER_CLOG_FRAME_BYTES];
	PGAlignedBlock payload;
	XLogRecPtr applied = InvalidXLogRecPtr;
	volatile int fd = -1, slotno = -1;
	volatile bool borrowed = false, io_locked = false, control_dropped = false;
	volatile bool resident = false, old_dirty = false, pending = false;
	volatile bool body_complete = false;
	volatile int saved_number = 0;
	volatile SlruPageStatus saved_status = SLRU_PAGE_EMPTY;
	volatile ClusterClogFileBindingV2 saved_binding = {0};
	volatile ClusterClogPageIdentityV2 saved_page = {0};
	bool segment = info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2;
	int pageno;

	if (!InRecovery || MyBackendType != B_STARTUP || !enableFsync ||
		!SlruNativeClogRequired(ctl) ||
		(info != CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2 && !segment) ||
		!cluster_clog_wal_decode_v2(info, record, length, &observed))
		SlruNativeClogRefuse();
	pageno = segment ? observed.binding.segment * SLRU_PAGES_PER_SEGMENT :
		observed.page.identity.logical_page;
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
	PG_TRY();
	{
		SlruNativeClogAcquire(ctl, pageno, &borrow);
		borrowed = true;
		binding.identity = borrow.expected;
		binding.segment = (uint32) pageno / SLRU_PAGES_PER_SEGMENT;
		if (!SlruClogBindingMatches(&binding, &observed.binding) ||
			!ctl->clog_native_current(&borrow))
			SlruNativeClogRefuse();
		fd = SlruNativeClogOpen(ctl, pageno, O_RDWR);
		if (fd < 0 || !ctl->clog_native_current(&borrow))
			SlruNativeClogRefuse();
		for (;;)
		{
			if (segment)
			{
				/* Reuse an existing slot in this segment without flushing its
				 * unflushed replay effects. A header supplies no page birth. */
				pageno = observed.binding.segment * SLRU_PAGES_PER_SEGMENT;
				for (int i = 0; i < shared->num_slots; i++)
					if (shared->page_status[i] != SLRU_PAGE_EMPTY &&
						shared->page_number[i] / SLRU_PAGES_PER_SEGMENT == observed.binding.segment)
					{
						pageno = shared->page_number[i];
						break;
					}
			}
			slotno = SlruSelectLRUPage(ctl, pageno);
			if (!ctl->clog_native_current(&borrow))
				SlruNativeClogRefuse();
			if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
				shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
			{
				SimpleLruWaitIO(ctl, slotno);
				continue;
			}
			break;
		}
		resident = shared->page_status[slotno] == SLRU_PAGE_VALID &&
			shared->page_number[slotno] == pageno;
		old_dirty = shared->page_dirty[slotno];
		if (segment)
		{
			saved_number = shared->page_number[slotno];
			saved_status = shared->page_status[slotno];
			saved_binding = shared->clog_file_binding[slotno];
			saved_page = shared->clog_page_identity[slotno];
			if (resident && !SlruClogBindingMatches(&binding, &shared->clog_file_binding[slotno]))
				SlruNativeClogRefuse();
		}
		if (resident && !segment)
		{
			captured = shared->clog_page_identity[slotno];
			for (int i = 0; i < shared->lsn_groups_per_page; i++)
				captured.identity.page_lsn = Max(captured.identity.page_lsn,
					shared->group_lsn[slotno * shared->lsn_groups_per_page + i]);
			if (!SlruClogBindingMatches(&binding, &shared->clog_file_binding[slotno]) ||
				captured.identity.logical_page != (uint64) pageno ||
				!cluster_clog_file_page_encode_v2(&binding, &captured,
					shared->page_buffer[slotno], BLCKSZ, current, sizeof(current)))
				SlruNativeClogRefuse();
		}
		LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);
		io_locked = true;
		shared->page_number[slotno] = pageno;
		shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
		shared->clog_file_binding[slotno] = binding;
		pending = true;
		LWLockRelease(shared->ControlLock);
		control_dropped = true;
		pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
		if (!cluster_clog_storage_replay_v2(fd, &binding, info, record, length, record_end,
				resident && !segment ? current : NULL,
				resident && !segment ? sizeof(current) : 0, &applied) ||
			applied != record_end ||
			(!segment && !cluster_clog_storage_read_page_v2(fd, &binding, pageno, &checked,
														 payload.data, BLCKSZ)))
			SlruNativeClogRefuse();
		pgstat_report_wait_end();
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
		control_dropped = false;
		if (!ctl->clog_native_current(&borrow) ||
			shared->page_number[slotno] != pageno ||
			shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS ||
			!SlruClogBindingMatches(&binding, &shared->clog_file_binding[slotno]))
			SlruNativeClogRefuse();
		if (segment)
		{
			shared->page_number[slotno] = saved_number;
			shared->page_status[slotno] = saved_status;
			shared->page_dirty[slotno] = old_dirty;
			shared->clog_file_binding[slotno] = saved_binding;
			shared->clog_page_identity[slotno] = saved_page;
		}
		else
		{
			memcpy(shared->page_buffer[slotno], payload.data, BLCKSZ);
			shared->clog_page_identity[slotno] = checked;
			shared->page_dirty[slotno] = false;
			shared->page_status[slotno] = SLRU_PAGE_VALID;
			SimpleLruZeroLSNs(ctl, slotno);
			SlruRecentlyUsed(shared, slotno);
		}
		pending = false;
		body_complete = true;
	}
	PG_FINALLY();
	{
		if (!body_complete)
			InterruptHoldoffCount = 0;
		PG_TRY(cleanup);
		{
			pgstat_report_wait_end();
			if (control_dropped)
				LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
			if (pending)
			{
				if (segment)
				{
					shared->page_number[slotno] = saved_number;
					shared->page_status[slotno] = saved_status;
					shared->page_dirty[slotno] = old_dirty;
					shared->clog_file_binding[slotno] = saved_binding;
					shared->clog_page_identity[slotno] = saved_page;
				}
				else
				{
					shared->page_status[slotno] = resident ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY;
					shared->page_dirty[slotno] = resident && old_dirty;
					if (!resident)
					{
						memset(&shared->clog_file_binding[slotno], 0, sizeof(binding));
						memset(&shared->clog_page_identity[slotno], 0, sizeof(checked));
					}
				}
			}
			if (io_locked)
			{
				if (!body_complete)
					HOLD_INTERRUPTS();
				LWLockRelease(&shared->buffer_locks[slotno].lock);
			}
			if (borrowed && !SlruNativeClogFinish(ctl, &borrow, fd))
				ereport(PANIC, (errcode(ERRCODE_IO_ERROR),
								errmsg("could not finish native CLOG replay cleanup")));
			if (!body_complete)
			{
				InterruptHoldoffCount = 0;
				HOLD_INTERRUPTS();
			}
			LWLockRelease(shared->ControlLock);
		}
		PG_CATCH(cleanup);
		{
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("error during native CLOG replay cleanup")));
		}
		PG_END_TRY(cleanup);
	}
	PG_END_TRY();
}

void
SimpleLruNativeClogReplayV2(SlruCtl ctl, const void *record, Size length,
						  XLogRecPtr record_end)
{
	SlruNativeClogReplay(ctl, CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2,
						record, length, record_end);
}

void
SimpleLruNativeClogSegmentReplayV2(SlruCtl ctl, const void *record, Size length,
								 XLogRecPtr record_end)
{
	SlruNativeClogReplay(ctl, CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2,
						record, length, record_end);
}

int
SimpleLruNativeClogReadV2(SlruCtl ctl, int pageno, bool write_ok)
{
	SlruShared shared = ctl->shared;
	ClusterControlNativeFileBorrowV2 borrow = {0};
	ClusterClogFileBindingV2 binding;
	volatile int fd = -1;
	volatile int slotno = -1;

	SlruNativeClogAcquire(ctl, pageno, &borrow);
	binding.identity = borrow.expected;
	binding.segment = (uint32) pageno / SLRU_PAGES_PER_SEGMENT;
	PG_TRY();
	{
		for (;;)
		{
			if (!ctl->clog_native_current(&borrow))
				SlruNativeClogRefuse();
			slotno = SlruSelectLRUPage(ctl, pageno);
			if (shared->page_number[slotno] == pageno &&
				shared->page_status[slotno] != SLRU_PAGE_EMPTY)
			{
				ClusterClogPageIdentityV2 *page;

				if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
					(shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS && !write_ok))
				{
					SimpleLruWaitIO(ctl, slotno);
					continue;
				}
				if (shared->clog_file_binding == NULL || shared->clog_page_identity == NULL ||
					!SlruClogBindingMatches(&shared->clog_file_binding[slotno], &binding))
					SlruNativeClogRefuse();
				page = &shared->clog_page_identity[slotno];
				if (!page->identity.birth_observed ||
					page->identity.logical_page != (uint64) pageno ||
					page->identity.page_generation == 0 ||
					page->identity.page_generation == PG_UINT64_MAX ||
					U64FromFullTransactionId(page->identity.birth_full_xid) == PG_UINT64_MAX ||
					!TransactionIdIsNormal(XidFromFullTransactionId(page->identity.birth_full_xid)) ||
					XidFromFullTransactionId(page->identity.birth_full_xid) / 32768 != pageno ||
					page->identity.page_lsn == PG_UINT64_MAX || page->image_lsn == PG_UINT64_MAX)
					SlruNativeClogRefuse();
				SlruRecentlyUsed(shared, slotno);
				pgstat_count_slru_page_hit(shared->slru_stats_idx);
				break;
			}
			if (fd < 0)
			{
				fd = SlruNativeClogOpen(ctl, pageno, O_RDONLY);
				if (fd < 0)
					SlruNativeClogRefuse();
				/* Opening dropped ControlLock: reselect, including victim write. */
				continue;
			}
			if (!SimpleLruReadClogPageV2(ctl, slotno, pageno, fd, &binding))
				SlruNativeClogRefuse();
			break;
		}
		if (!ctl->clog_native_current(&borrow))
			SlruNativeClogRefuse();
	}
	PG_FINALLY();
	{
		if (!SlruNativeClogFinish(ctl, &borrow, fd))
			SlruNativeClogRefuse();
	}
	PG_END_TRY();
	/* Caller consumes the slot before releasing the SAME family lock. The
	 * original control transition must acquire/drain that lock and its slots. */
	return slotno;
}

void
SimpleLruNativeClogWriteV2(SlruCtl ctl, int slotno, bool checkpoint)
{
	SlruShared shared = ctl->shared;
	int pageno = shared->page_number[slotno];
	ClusterControlNativeFileBorrowV2 borrow = {0};
	ClusterClogFileBindingV2 binding;
	volatile int fd = -1;

	SlruNativeClogAcquire(ctl, pageno, &borrow);
	binding.identity = borrow.expected;
	binding.segment = (uint32) pageno / SLRU_PAGES_PER_SEGMENT;
	PG_TRY();
	{
		fd = SlruNativeClogOpen(ctl, pageno, O_RDWR);
		if (fd < 0 || !ctl->clog_native_current(&borrow))
			SlruNativeClogRefuse();
		/* Open released ControlLock. Another original writer may now own IO
		 * with dirty temporarily cleared. Preserve the original completion
		 * wait before deciding whether this attempt has anything left to do. */
		while (shared->page_number[slotno] == pageno &&
			   shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
			SimpleLruWaitIO(ctl, slotno);
		if (!ctl->clog_native_current(&borrow))
			SlruNativeClogRefuse();
		/* A concurrent original writer may finish while open dropped the lock.
		 * Never apply the old expected to a replacement slot. */
		if (shared->page_number[slotno] == pageno &&
			shared->page_status[slotno] == SLRU_PAGE_VALID && shared->page_dirty[slotno])
		{
			XLogRecPtr end = InvalidXLogRecPtr;
			bool written = SlruWriteClogPage(ctl, slotno, fd, &binding,
											 IsBootstrapProcessingMode(), &end);

			if (!written || !ctl->clog_native_current(&borrow))
			{
				if (shared->page_number[slotno] == pageno)
					shared->page_dirty[slotno] = true;
				SlruNativeClogRefuse();
			}
			if (checkpoint)
				CheckpointStats.ckpt_bufs_written++;
		}
	}
	PG_FINALLY();
	{
		if (!SlruNativeClogFinish(ctl, &borrow, fd))
		{
			if (shared->page_number[slotno] == pageno)
				shared->page_dirty[slotno] = true;
			SlruNativeClogRefuse();
		}
	}
	PG_END_TRY();
}
#endif

/*
 * Initialize (or reinitialize) a page to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
SimpleLruZeroPage(SlruCtl ctl, int pageno)
{
	SlruShared	shared = ctl->shared;
	int			slotno;

#ifdef USE_PGRAC_CLUSTER
	/* Birth must consume the original allocator's complete pre-registered
	 * range and full-image WAL. Raw callers cannot zero a
	 * selected-format page or emit a legacy ZERO through an empty raw slot. */
	if (SlruNativeClogRequired(ctl))
		SlruNativeClogRefuse();
#endif
	/* Find a suitable buffer slot for the page */
	slotno = SlruSelectLRUPage(ctl, pageno);
	Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
		   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno]) ||
		   shared->page_number[slotno] == pageno);
	SlruRequireRawSlot(ctl, slotno);

	/* Mark the slot as containing this page */
#ifdef USE_PGRAC_CLUSTER
	shared->page_initial_full_xid[slotno] = 0;
#endif
	shared->page_number[slotno] = pageno;
	shared->page_status[slotno] = SLRU_PAGE_VALID;
	shared->page_dirty[slotno] = true;
	SlruRecentlyUsed(shared, slotno);

	/* A raw zero or reuse never inherits a previous page birth. */
	if (shared->clog_page_identity != NULL)
		memset(&shared->clog_page_identity[slotno], 0,
			   sizeof(ClusterClogPageIdentityV2));
	if (shared->clog_file_binding != NULL)
		memset(&shared->clog_file_binding[slotno], 0,
			   sizeof(ClusterClogFileBindingV2));

	/* Set the buffer to zeroes */
	MemSet(shared->page_buffer[slotno], 0, BLCKSZ);

	/* Set the LSNs for this new page to zero */
	SimpleLruZeroLSNs(ctl, slotno);

	/* Assume this page is now the latest active page */
	shared->latest_page_number = pageno;

	/* update the stats counter of zeroed pages */
	pgstat_count_slru_page_zeroed(shared->slru_stats_idx);

	return slotno;
}

/*
 * Zero all the LSNs we store for this slru page.
 *
 * This should be called each time we create a new page, and each time we read
 * in a page from disk into an existing buffer.  (Such an old page cannot
 * have any interesting LSNs, since we'd have flushed them before writing
 * the page in the first place.)
 *
 * This assumes that InvalidXLogRecPtr is bitwise-all-0.
 */
static void
SimpleLruZeroLSNs(SlruCtl ctl, int slotno)
{
	SlruShared	shared = ctl->shared;

	if (shared->lsn_groups_per_page > 0)
		MemSet(&shared->group_lsn[slotno * shared->lsn_groups_per_page], 0,
			   shared->lsn_groups_per_page * sizeof(XLogRecPtr));
}

/*
 * Wait for any active I/O on a page slot to finish.  (This does not
 * guarantee that new I/O hasn't been started before we return, though.
 * In fact the slot might not even contain the same page anymore.)
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void
SimpleLruWaitIO(SlruCtl ctl, int slotno)
{
	SlruShared	shared = ctl->shared;

	/* See notes at top of file */
	LWLockRelease(shared->ControlLock);
	LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED);
	LWLockRelease(&shared->buffer_locks[slotno].lock);
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	/*
	 * If the slot is still in an io-in-progress state, then either someone
	 * already started a new I/O on the slot, or a previous I/O failed and
	 * neglected to reset the page state.  That shouldn't happen, really, but
	 * it seems worth a few extra cycles to check and recover from it. We can
	 * cheaply test for failure by seeing if the buffer lock is still held (we
	 * assume that transaction abort would release the lock).
	 */
	if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
		shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
	{
		if (LWLockConditionalAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED))
		{
			/* indeed, the I/O must have failed */
			if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS)
			{
				shared->page_status[slotno] = SLRU_PAGE_EMPTY;
#ifdef USE_PGRAC_CLUSTER
				shared->page_initial_full_xid[slotno] = 0;
#endif
			}
			else				/* write_in_progress */
			{
				shared->page_status[slotno] = SLRU_PAGE_VALID;
				shared->page_dirty[slotno] = true;
			}
			LWLockRelease(&shared->buffer_locks[slotno].lock);
		}
	}
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 *
 * If write_ok is true then it is OK to return a page that is in
 * WRITE_IN_PROGRESS state; it is the caller's responsibility to be sure
 * that modification of the page is safe.  If write_ok is false then we
 * will not return the page until it is not undergoing active I/O.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok,
				  TransactionId xid)
{
	SlruShared	shared = ctl->shared;

#ifdef USE_PGRAC_CLUSTER
	if (SlruNativeClogRequired(ctl))
	{
		if (ctl->clog_native_read == NULL)
			SlruNativeClogRefuse();
		return ctl->clog_native_read(ctl, pageno, write_ok);
	}
#endif

	/* Outer loop handles restart if we must wait for someone else's I/O */
	for (;;)
	{
		int			slotno;
		bool		ok;

		/* See if page already is in memory; if not, pick victim slot */
		slotno = SlruSelectLRUPage(ctl, pageno);
		SlruRequireRawSlot(ctl, slotno);

		/* Did we find the page in memory? */
		if (shared->page_number[slotno] == pageno &&
			shared->page_status[slotno] != SLRU_PAGE_EMPTY)
		{
			/*
			 * If page is still being read in, we must wait for I/O.  Likewise
			 * if the page is being written and the caller said that's not OK.
			 */
			if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
				(shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
				 !write_ok))
			{
				SimpleLruWaitIO(ctl, slotno);
				/* Now we must recheck state from the top */
				continue;
			}
			/* Otherwise, it's ready to use */
			SlruRecentlyUsed(shared, slotno);

			/* update the stats counter of pages found in the SLRU */
			pgstat_count_slru_page_hit(shared->slru_stats_idx);

			return slotno;
		}

		/* We found no match; assert we selected a freeable slot */
		Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
			   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
				!shared->page_dirty[slotno]));

		/* Mark the slot read-busy */
#ifdef USE_PGRAC_CLUSTER
		shared->page_initial_full_xid[slotno] = 0;
#endif
		shared->page_number[slotno] = pageno;
		shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
		shared->page_dirty[slotno] = false;
		/* Legacy payload reads have no persisted FullXid provenance. Clear
		 * before dropping ControlLock, including read-error paths. */
		if (shared->clog_page_identity != NULL)
			memset(&shared->clog_page_identity[slotno], 0,
				   sizeof(ClusterClogPageIdentityV2));
		if (shared->clog_file_binding != NULL)
			memset(&shared->clog_file_binding[slotno], 0,
				   sizeof(ClusterClogFileBindingV2));

		/* Acquire per-buffer lock (cannot deadlock, see notes at top) */
		LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);

		/* Release control lock while doing I/O */
		LWLockRelease(shared->ControlLock);

		/* Do the read */
		ok = SlruPhysicalReadPage(ctl, pageno, slotno);

		/* Set the LSNs for this newly read-in page to zero */
		SimpleLruZeroLSNs(ctl, slotno);

		/* Re-acquire control lock and update page state */
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

		Assert(shared->page_number[slotno] == pageno &&
			   shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS &&
			   !shared->page_dirty[slotno]);

		shared->page_status[slotno] = ok ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY;

		LWLockRelease(&shared->buffer_locks[slotno].lock);

		/* Now it's okay to ereport if we failed */
		if (!ok)
			SlruReportIOError(ctl, pageno, xid);

		SlruRecentlyUsed(shared, slotno);

		/* update the stats counter of pages not found in SLRU */
		pgstat_count_slru_page_read(shared->slru_stats_idx);

		return slotno;
	}
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 * The caller must intend only read-only access to the page.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must NOT be held at entry, but will be held at exit.
 * It is unspecified whether the lock will be shared or exclusive.
 */
int
SimpleLruReadPage_ReadOnly(SlruCtl ctl, int pageno, TransactionId xid)
{
	SlruShared	shared = ctl->shared;
	int			slotno;

#ifdef USE_PGRAC_CLUSTER
	if (SlruNativeClogRequired(ctl))
	{
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
		if (ctl->clog_native_read == NULL)
			SlruNativeClogRefuse();
		return ctl->clog_native_read(ctl, pageno, true);
	}
#endif

	/* Try to find the page while holding only shared lock */
	LWLockAcquire(shared->ControlLock, LW_SHARED);

	/* See if page is already in a buffer */
	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		if (shared->page_number[slotno] == pageno &&
			shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
			shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS)
		{
			/* See comments for SlruRecentlyUsed macro */
			SlruRequireRawSlot(ctl, slotno);
			SlruRecentlyUsed(shared, slotno);

			/* update the stats counter of pages found in the SLRU */
			pgstat_count_slru_page_hit(shared->slru_stats_idx);

			return slotno;
		}
	}

	/* No luck, so switch to normal exclusive lock and do regular read */
	LWLockRelease(shared->ControlLock);
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	return SimpleLruReadPage(ctl, pageno, true, xid);
}

/* The caller holds ControlLock across this complete, by-value observation. */
static void
SlruCaptureWriteSnapshot(SlruCtl ctl, int slotno, SlruWriteSnapshot *snapshot)
{
	SlruShared	shared = ctl->shared;
	int			lsnindex = slotno * shared->lsn_groups_per_page;

	Assert(LWLockHeldByMe(shared->ControlLock));
	memcpy(snapshot->page.data, shared->page_buffer[slotno], BLCKSZ);
	memset(&snapshot->clog_page, 0, sizeof(snapshot->clog_page));
	if (shared->clog_page_identity != NULL)
		snapshot->clog_page = shared->clog_page_identity[slotno];
	snapshot->flush_lsn = InvalidXLogRecPtr;
	if (shared->group_lsn != NULL)
	{
		for (int i = 0; i < shared->lsn_groups_per_page; i++)
			snapshot->flush_lsn = Max(snapshot->flush_lsn,
									 shared->group_lsn[lsnindex + i]);
	}
	snapshot->clog_page.identity.page_lsn =
		Max(snapshot->clog_page.identity.page_lsn, snapshot->flush_lsn);
}


/*
 * Write a page from a shared buffer, if necessary.
 * Does nothing if the specified slot is not dirty.
 *
 * NOTE: only one write attempt is made here.  Hence, it is possible that
 * the page is still dirty at exit (if someone else re-dirtied it during
 * the write).  However, we *do* attempt a fresh write even if the page
 * is already being written; this is for checkpoints.
 *
 * Control lock must be held at entry, and will be held at exit.
 */

static void
SlruInternalWritePage(SlruCtl ctl, int slotno, SlruWriteAll fdata)
{
	SlruShared	shared = ctl->shared;
	int			pageno = shared->page_number[slotno];
	bool		ok;
	SlruWriteSnapshot snapshot;
	const SlruWriteSnapshot *write_snapshot = NULL;

	/* If a write is in progress, wait for it to finish */
	while (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
		   shared->page_number[slotno] == pageno)
	{
		SimpleLruWaitIO(ctl, slotno);
	}

	/*
	 * Do nothing if page is not dirty, or if buffer no longer contains the
	 * same page we were called for.
	 */
	if (!shared->page_dirty[slotno] ||
		shared->page_status[slotno] != SLRU_PAGE_VALID ||
		/* SimpleLruWaitIO releases the lock; another backend can replace this page. */
		// cppcheck-suppress knownConditionTrueFalse
		shared->page_number[slotno] != pageno)
		return;

#ifdef USE_PGRAC_CLUSTER
	if (SlruNativeClogRequired(ctl))
	{
		if (ctl->clog_native_write == NULL)
			SlruNativeClogRefuse();
		ctl->clog_native_write(ctl, slotno, fdata != NULL);
		return;
	}
#endif
	SlruRequireRawSlot(ctl, slotno);

	/*
	 * CLOG updaters can modify the shared page and its async-commit LSNs as
	 * soon as we release ControlLock.  Capture both now, so WAL flush and
	 * physical data write refer to the same bytes.  A subsequent update still
	 * sets page_dirty and must be written by a later attempt.  Other SLRUs
	 * retain their existing shared-buffer write path.
	 */
	if (ctl->sync_handler == SYNC_HANDLER_CLOG)
	{
		SlruCaptureWriteSnapshot(ctl, slotno, &snapshot);
		write_snapshot = &snapshot;
	}

	/*
	 * Mark the slot write-busy, and clear the dirtybit.  After this point, a
	 * transaction status update on this page will mark it dirty again.
	 */
	shared->page_status[slotno] = SLRU_PAGE_WRITE_IN_PROGRESS;
	shared->page_dirty[slotno] = false;

	/* Acquire per-buffer lock (cannot deadlock, see notes at top) */
	LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);

#ifdef USE_PGRAC_CLUSTER
	if (ctl->WritePageObserve != NULL)
		ctl->WritePageObserve(ctl, true);
#endif

	/* Release control lock while doing I/O */
	LWLockRelease(shared->ControlLock);

	/* Do the write */
	ok = SlruPhysicalWritePage(ctl, pageno, slotno, fdata, write_snapshot);

	/* If we failed, and we're in a flush, better close the files */
	if (!ok && fdata)
	{
		int			i;

		for (i = 0; i < fdata->num_files; i++)
			CloseTransientFile(fdata->fd[i]);
	}

	/* Re-acquire control lock and update page state */
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	Assert(shared->page_number[slotno] == pageno &&
		   shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS);

	/* If we failed to write, mark the page dirty again */
	if (!ok)
		shared->page_dirty[slotno] = true;

	shared->page_status[slotno] = SLRU_PAGE_VALID;

	LWLockRelease(&shared->buffer_locks[slotno].lock);

#ifdef USE_PGRAC_CLUSTER
	if (ctl->WritePageObserve != NULL)
		ctl->WritePageObserve(ctl, false);
#endif

	/* Now it's okay to ereport if we failed */
	if (!ok)
		SlruReportIOError(ctl, pageno, InvalidTransactionId);

	/* If part of a checkpoint, count this as a buffer written. */
	if (fdata)
		CheckpointStats.ckpt_bufs_written++;
}

/*
 * Wrapper of SlruInternalWritePage, for external callers.
 * fdata is always passed a NULL here.
 */
void
SimpleLruWritePage(SlruCtl ctl, int slotno)
{
	SlruInternalWritePage(ctl, slotno, NULL);
}

bool
SimpleLruReadClogPageV2(SlruCtl ctl, int slotno, int pageno, int fd,
						 const ClusterClogFileBindingV2 *expected)
{
	SlruShared shared;
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 observed;
	PGAlignedBlock payload;
	bool ok = false;

	if (ctl == NULL || expected == NULL || fd < 0 || pageno < 0 || pageno >= 131072 ||
		ctl->sync_handler != SYNC_HANDLER_CLOG || (shared = ctl->shared) == NULL ||
		slotno < 0 || slotno >= shared->num_slots || shared->ControlLock == NULL ||
		shared->page_buffer == NULL || shared->page_buffer[slotno] == NULL ||
		shared->page_status == NULL || shared->page_dirty == NULL ||
		shared->page_number == NULL || shared->page_lru_count == NULL ||
		shared->buffer_locks == NULL || shared->clog_page_identity == NULL ||
		shared->clog_file_binding == NULL || shared->group_lsn == NULL ||
		shared->lsn_groups_per_page <= 0 ||
		!LWLockHeldByMeInMode(shared->ControlLock, LW_EXCLUSIVE))
		return false;
	if (shared->page_dirty[slotno] ||
		(shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
		 shared->page_status[slotno] != SLRU_PAGE_VALID))
		return false;
	/* This caller-reserved slot must not create a second cache owner. */
	for (int i = 0; i < shared->num_slots; i++)
		if (i != slotno && shared->page_status[i] != SLRU_PAGE_EMPTY &&
			shared->page_number[i] == pageno)
			return false;
	binding = *expected;
	if ((uint32) pageno / CLUSTER_CLOG_PAGES_PER_FILE != binding.segment ||
		!cluster_clog_file_header_encode_v2(&binding, payload.data, BLCKSZ))
		return false;

	shared->page_number[slotno] = pageno;
	shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
	memset(&shared->clog_page_identity[slotno], 0, sizeof(ClusterClogPageIdentityV2));
	/* Retain the selected binding as a reservation throughout the I/O gap.
	 * It blocks raw same-page zero/read/write before payload is installed. */
	shared->clog_file_binding[slotno] = binding;
	SimpleLruZeroLSNs(ctl, slotno);
	LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);
	LWLockRelease(shared->ControlLock);
	PG_TRY();
	{
		pgstat_report_wait_start(WAIT_EVENT_SLRU_READ);
		ok = cluster_clog_storage_read_page_v2(fd, &binding, pageno, &observed,
												payload.data, BLCKSZ);
		pgstat_report_wait_end();
	}
	PG_CATCH();
	{
		pgstat_report_wait_end();
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
		memset(&shared->clog_file_binding[slotno], 0, sizeof(ClusterClogFileBindingV2));
		shared->page_status[slotno] = SLRU_PAGE_EMPTY;
		LWLockRelease(&shared->buffer_locks[slotno].lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
	if (shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS ||
		shared->page_number[slotno] != pageno || shared->page_dirty[slotno])
		ok = false;
	if (ok)
	{
		memcpy(shared->page_buffer[slotno], payload.data, BLCKSZ);
		shared->clog_page_identity[slotno] = observed;
		shared->clog_file_binding[slotno] = binding;
		SlruRecentlyUsed(shared, slotno);
	}
	if (!ok)
		memset(&shared->clog_file_binding[slotno], 0, sizeof(ClusterClogFileBindingV2));
	shared->page_status[slotno] = ok ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY;
	LWLockRelease(&shared->buffer_locks[slotno].lock);
	if (ok)
		pgstat_count_slru_page_read(shared->slru_stats_idx);
	return ok;
}

/* A managed write uses the same slot/ControlLock/per-buffer I/O protocol, but
 * its full-image WAL can raise ERROR and its durable file is generation-bound.
 * Keep it separate from the raw writer and its segno-only sync queue. */
static bool
SlruWriteClogPage(SlruCtl ctl, int slotno, int fd,
						  const ClusterClogFileBindingV2 *expected,
				  bool bootstrap, XLogRecPtr *end_out)
{
	SlruShared shared;
	SlruWriteSnapshot snapshot;
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 *current;
	uint8 unknown_binding[sizeof(ClusterClogFileBindingV2)] = {0};
	uint8 checked_frame[CLUSTER_CLOG_FRAME_BYTES];
	XLogRecPtr end = InvalidXLogRecPtr;
	bool ok = false;
	bool fresh_binding;
	int pageno;

	if (ctl == NULL || expected == NULL || fd < 0 ||
		ctl->sync_handler != SYNC_HANDLER_CLOG || (shared = ctl->shared) == NULL ||
		slotno < 0 || slotno >= shared->num_slots || shared->ControlLock == NULL ||
		shared->page_buffer == NULL || shared->page_status == NULL ||
		shared->page_dirty == NULL || shared->page_number == NULL ||
		shared->buffer_locks == NULL || shared->clog_page_identity == NULL ||
		shared->clog_file_binding == NULL ||
		shared->group_lsn == NULL || shared->lsn_groups_per_page <= 0 ||
		!LWLockHeldByMeInMode(shared->ControlLock, LW_EXCLUSIVE))
		return false;
	pageno = shared->page_number[slotno];
	while (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
		   shared->page_number[slotno] == pageno)
		SimpleLruWaitIO(ctl, slotno);
	if (!shared->page_dirty[slotno] || shared->page_status[slotno] != SLRU_PAGE_VALID ||
		shared->page_number[slotno] != pageno || pageno < 0 ||
		shared->page_buffer[slotno] == NULL)
		return false;

	/* Validate the complete by-value snapshot before changing any slot state.
	 * In particular, a raw page number or an LSN cannot supply missing birth. */
	binding = *expected;
	SlruCaptureWriteSnapshot(ctl, slotno, &snapshot);
	fresh_binding = snapshot.clog_page.image_lsn == InvalidXLogRecPtr &&
		memcmp(&shared->clog_file_binding[slotno], unknown_binding,
			   sizeof(unknown_binding)) == 0;
	if ((!fresh_binding && !SlruClogBindingMatches(&shared->clog_file_binding[slotno], &binding)) ||
		snapshot.clog_page.identity.logical_page != (uint64) pageno ||
		!cluster_clog_file_page_encode_v2(&binding, &snapshot.clog_page,
										 snapshot.page.data, BLCKSZ,
										 checked_frame, sizeof(checked_frame)))
		return false;

	/* Only the original unwritten birth may first bind the independent
	 * expected file. Keep that responsibility even when the first I/O fails. */
	if (fresh_binding)
		shared->clog_file_binding[slotno] = binding;
	shared->page_status[slotno] = SLRU_PAGE_WRITE_IN_PROGRESS;
	shared->page_dirty[slotno] = false;
	LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);
	LWLockRelease(shared->ControlLock);

	PG_TRY();
	{
		pgstat_count_slru_page_written(shared->slru_stats_idx);
		pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
		if (bootstrap)
			ok = cluster_clog_storage_bootstrap_write_page_v2(fd, &binding,
					&snapshot.clog_page, snapshot.page.data, BLCKSZ);
		else
			ok = cluster_clog_storage_write_page_v2(fd, &binding, &snapshot.clog_page,
												  snapshot.page.data, BLCKSZ, &end);
		pgstat_report_wait_end();
	}
	PG_CATCH();
	{
		pgstat_report_wait_end();
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
		shared->page_dirty[slotno] = true;
		shared->page_status[slotno] = SLRU_PAGE_VALID;
		LWLockRelease(&shared->buffer_locks[slotno].lock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
	current = &shared->clog_page_identity[slotno];
	if (!SlruClogBindingMatches(&shared->clog_file_binding[slotno], &binding) ||
		shared->page_number[slotno] != pageno ||
		shared->page_status[slotno] != SLRU_PAGE_WRITE_IN_PROGRESS ||
		current->identity.logical_page != snapshot.clog_page.identity.logical_page ||
		current->identity.page_generation != snapshot.clog_page.identity.page_generation ||
		!current->identity.birth_observed ||
		!FullTransactionIdEquals(current->identity.birth_full_xid,
							 snapshot.clog_page.identity.birth_full_xid) ||
		current->image_lsn != snapshot.clog_page.image_lsn)
		ok = false;
	if (ok)
	{
		current->image_lsn = end;
		current->identity.page_lsn = Max(current->identity.page_lsn,
									 snapshot.clog_page.identity.page_lsn);
		/* A live page may already differ from the captured on-disk image. */
		current->identity.payload_crc = current->identity.metadata_crc = 0;
	}
	else
		shared->page_dirty[slotno] = true;
	shared->page_status[slotno] = SLRU_PAGE_VALID;
	LWLockRelease(&shared->buffer_locks[slotno].lock);
	if (ok)
		*end_out = end;
	return ok;
}

/* The external WAL writer retains its original nonzero-record-end contract.
 * Only the managed native caller may select authenticated BOOT writeback. */
XLogRecPtr
SimpleLruWriteClogPageV2(SlruCtl ctl, int slotno, int fd,
					   const ClusterClogFileBindingV2 *expected)
{
	XLogRecPtr end = InvalidXLogRecPtr;

	return SlruWriteClogPage(ctl, slotno, fd, expected, false, &end)
		? end : InvalidXLogRecPtr;
}

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int pageno)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;
	bool		result;
	off_t		endpos;

	/* update the stats counter of checked pages */
	pgstat_count_slru_page_exists(ctl->shared->slru_stats_idx);

	SlruFileName(ctl, path, segno);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		/* expected: file doesn't exist */
		if (errno == ENOENT)
			return false;

		/* report error normally */
		slru_errcause = SLRU_OPEN_FAILED;
		slru_errno = errno;
		SlruReportIOError(ctl, pageno, 0);
	}

	if ((endpos = lseek(fd, 0, SEEK_END)) < 0)
	{
		slru_errcause = SLRU_SEEK_FAILED;
		slru_errno = errno;
		SlruReportIOError(ctl, pageno, 0);
	}

	result = endpos >= (off_t) (offset + BLCKSZ);

	if (CloseTransientFile(fd) != 0)
	{
		slru_errcause = SLRU_CLOSE_FAILED;
		slru_errno = errno;
		return false;
	}

	return result;
}

/*
 * Physical read of a (previously existing) page into a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * read/write operations.  We could cache one virtual file pointer ...
 */
static bool
SlruPhysicalReadPage(SlruCtl ctl, int pageno, int slotno)
{
	SlruShared	shared = ctl->shared;
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	off_t		offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;

	SlruFileName(ctl, path, segno);

	/*
	 * In a crash-and-restart situation, it's possible for us to receive
	 * commands to set the commit status of transactions whose bits are in
	 * already-truncated segments of the commit log (see notes in
	 * SlruPhysicalWritePage).  Hence, if we are InRecovery, allow the case
	 * where the file doesn't exist, and return zeroes instead.
	 */
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno != ENOENT || !InRecovery)
		{
			slru_errcause = SLRU_OPEN_FAILED;
			slru_errno = errno;
			return false;
		}

		ereport(LOG,
				(errmsg("file \"%s\" doesn't exist, reading as zeroes",
						path)));
		MemSet(shared->page_buffer[slotno], 0, BLCKSZ);
		return true;
	}

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_SLRU_READ);
	if (pg_pread(fd, shared->page_buffer[slotno], BLCKSZ, offset) != BLCKSZ)
	{
		pgstat_report_wait_end();
		slru_errcause = SLRU_READ_FAILED;
		slru_errno = errno;
		CloseTransientFile(fd);
		return false;
	}
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
	{
		slru_errcause = SLRU_CLOSE_FAILED;
		slru_errno = errno;
		return false;
	}

	return true;
}

/*
 * Physical write of a page from a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * independent read/write operations.  We do batch operations during
 * SimpleLruWriteAll, though.
 *
 * fdata is NULL for a standalone write, pointer to open-file info during
 * SimpleLruWriteAll.
 */
static bool
SlruPhysicalWritePage(SlruCtl ctl, int pageno, int slotno, SlruWriteAll fdata,
					  const SlruWriteSnapshot *snapshot)
{
	SlruShared	shared = ctl->shared;
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	off_t		offset = rpageno * BLCKSZ;
	int			fd = -1;

	/* update the stats counter of written pages */
	pgstat_count_slru_page_written(shared->slru_stats_idx);

	/*
	 * Honor the write-WAL-before-data rule, if appropriate, so that we do not
	 * write out data before associated WAL records.  This is the same action
	 * performed during FlushBuffer() in the main buffer manager.
	 */
	if (snapshot != NULL || shared->group_lsn != NULL)
	{
		/*
		 * We must determine the largest async-commit LSN for the page. This
		 * is a bit tedious, but since this entire function is a slow path
		 * anyway, it seems better to do this here than to maintain a per-page
		 * LSN variable (which'd need an extra comparison in the
		 * transaction-commit path).
		 */
		XLogRecPtr	max_lsn;
		int			lsnindex,
					lsnoff;

		if (snapshot != NULL)
			max_lsn = snapshot->flush_lsn;
		else
		{
			lsnindex = slotno * shared->lsn_groups_per_page;
			max_lsn = shared->group_lsn[lsnindex++];
			for (lsnoff = 1; lsnoff < shared->lsn_groups_per_page; lsnoff++)
			{
				XLogRecPtr	this_lsn = shared->group_lsn[lsnindex++];

				if (max_lsn < this_lsn)
					max_lsn = this_lsn;
			}
		}

		if (!XLogRecPtrIsInvalid(max_lsn))
		{
			/*
			 * As noted above, elog(ERROR) is not acceptable here, so if
			 * XLogFlush were to fail, we must PANIC.  This isn't much of a
			 * restriction because XLogFlush is just about all critical
			 * section anyway, but let's make sure.
			 */
			START_CRIT_SECTION();
			XLogFlush(max_lsn);
			END_CRIT_SECTION();
		}
	}

	/*
	 * During a SimpleLruWriteAll, we may already have the desired file open.
	 */
	if (fdata)
	{
		int			i;

		for (i = 0; i < fdata->num_files; i++)
		{
			if (fdata->segno[i] == segno)
			{
				fd = fdata->fd[i];
				break;
			}
		}
	}

	if (fd < 0)
	{
		char path[MAXPGPATH];
		/*
		 * If the file doesn't already exist, we should create it.  It is
		 * possible for this to need to happen when writing a page that's not
		 * first in its segment; we assume the OS can cope with that. (Note:
		 * it might seem that it'd be okay to create files only when
		 * SimpleLruZeroPage is called for the first page of a segment.
		 * However, if after a crash and restart the REDO logic elects to
		 * replay the log from a checkpoint before the latest one, then it's
		 * possible that we will get commands to set transaction status of
		 * transactions that have already been truncated from the commit log.
		 * Easiest way to deal with that is to accept references to
		 * nonexistent files here and in SlruPhysicalReadPage.)
		 *
		 * Note: it is possible for more than one backend to be executing this
		 * code simultaneously for different pages of the same file. Hence,
		 * don't use O_EXCL or O_TRUNC or anything like that.
		 */
		SlruFileName(ctl, path, segno);
		fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
		if (fd < 0)
		{
			slru_errcause = SLRU_OPEN_FAILED;
			slru_errno = errno;
			return false;
		}

		if (fdata)
		{
			if (fdata->num_files < MAX_WRITEALL_BUFFERS)
			{
				fdata->fd[fdata->num_files] = fd;
				fdata->segno[fdata->num_files] = segno;
				fdata->num_files++;
			}
			else
			{
				/*
				 * In the unlikely event that we exceed MAX_WRITEALL_BUFFERS,
				 * fall back to treating it as a standalone write.
				 */
				fdata = NULL;
			}
		}
	}

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
	if (pg_pwrite(fd, snapshot != NULL ? snapshot->page.data : shared->page_buffer[slotno],
				  BLCKSZ, offset) != BLCKSZ)
	{
		pgstat_report_wait_end();
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		slru_errcause = SLRU_WRITE_FAILED;
		slru_errno = errno;
		if (!fdata)
			CloseTransientFile(fd);
		return false;
	}
	pgstat_report_wait_end();

	/* Queue up a sync request for the checkpointer. */
	if (ctl->sync_handler != SYNC_HANDLER_NONE)
	{
		FileTag		tag;

		INIT_SLRUFILETAG(tag, ctl->sync_handler, segno);
		if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false))
		{
			/* No space to enqueue sync request.  Do it synchronously. */
			pgstat_report_wait_start(WAIT_EVENT_SLRU_SYNC);
			if (pg_fsync(fd) != 0)
			{
				pgstat_report_wait_end();
				slru_errcause = SLRU_FSYNC_FAILED;
				slru_errno = errno;
				CloseTransientFile(fd);
				return false;
			}
			pgstat_report_wait_end();
		}
	}

	/* Close file, unless part of flush request. */
	if (!fdata)
	{
		if (CloseTransientFile(fd) != 0)
		{
			slru_errcause = SLRU_CLOSE_FAILED;
			slru_errno = errno;
			return false;
		}
	}

	return true;
}

/*
 * Issue the error message after failure of SlruPhysicalReadPage or
 * SlruPhysicalWritePage.  Call this after cleaning up shared-memory state.
 */
static void
SlruReportIOError(SlruCtl ctl, int pageno, TransactionId xid)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];

	SlruFileName(ctl, path, segno);
	errno = slru_errno;
	switch (slru_errcause)
	{
		case SLRU_OPEN_FAILED:
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not open file \"%s\": %m.", path)));
			break;
		case SLRU_SEEK_FAILED:
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not seek in file \"%s\" to offset %d: %m.",
							   path, offset)));
			break;
		case SLRU_READ_FAILED:
			if (errno)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not read from file \"%s\" at offset %d: %m.",
								   path, offset)));
			else
				ereport(ERROR,
						(errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not read from file \"%s\" at offset %d: read too few bytes.", path, offset)));
			break;
		case SLRU_WRITE_FAILED:
			if (errno)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not write to file \"%s\" at offset %d: %m.",
								   path, offset)));
			else
				ereport(ERROR,
						(errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not write to file \"%s\" at offset %d: wrote too few bytes.",
								   path, offset)));
			break;
		case SLRU_FSYNC_FAILED:
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not fsync file \"%s\": %m.",
							   path)));
			break;
		case SLRU_CLOSE_FAILED:
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not close file \"%s\": %m.",
							   path)));
			break;
		default:
			/* can't get here, we trust */
			elog(ERROR, "unrecognized SimpleLru error cause: %d",
				 (int) slru_errcause);
			break;
	}
}

/*
 * Select the slot to re-use when we need a free slot.
 *
 * The target page number is passed because we need to consider the
 * possibility that some other process reads in the target page while
 * we are doing I/O to free a slot.  Hence, check or recheck to see if
 * any slot already holds the target page, and return that slot if so.
 * Thus, the returned slot is *either* a slot already holding the pageno
 * (could be any state except EMPTY), *or* a freeable slot (state EMPTY
 * or CLEAN).
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
SlruSelectLRUPage(SlruCtl ctl, int pageno)
{
	SlruShared	shared = ctl->shared;

	/* Outer loop handles restart after I/O */
	for (;;)
	{
		int			slotno;
		int			cur_count;
		int			bestvalidslot = 0;	/* keep compiler quiet */
		int			best_valid_delta = -1;
		int			best_valid_page_number = 0; /* keep compiler quiet */
		int			bestinvalidslot = 0;	/* keep compiler quiet */
		int			best_invalid_delta = -1;
		int			best_invalid_page_number = 0;	/* keep compiler quiet */

		/* See if page already has a buffer assigned */
		for (slotno = 0; slotno < shared->num_slots; slotno++)
		{
			if (shared->page_number[slotno] == pageno &&
				shared->page_status[slotno] != SLRU_PAGE_EMPTY)
				return slotno;
		}

		/*
		 * If we find any EMPTY slot, just select that one. Else choose a
		 * victim page to replace.  We normally take the least recently used
		 * valid page, but we will never take the slot containing
		 * latest_page_number, even if it appears least recently used.  We
		 * will select a slot that is already I/O busy only if there is no
		 * other choice: a read-busy slot will not be least recently used once
		 * the read finishes, and waiting for an I/O on a write-busy slot is
		 * inferior to just picking some other slot.  Testing shows the slot
		 * we pick instead will often be clean, allowing us to begin a read at
		 * once.
		 *
		 * Normally the page_lru_count values will all be different and so
		 * there will be a well-defined LRU page.  But since we allow
		 * concurrent execution of SlruRecentlyUsed() within
		 * SimpleLruReadPage_ReadOnly(), it is possible that multiple pages
		 * acquire the same lru_count values.  In that case we break ties by
		 * choosing the furthest-back page.
		 *
		 * Notice that this next line forcibly advances cur_lru_count to a
		 * value that is certainly beyond any value that will be in the
		 * page_lru_count array after the loop finishes.  This ensures that
		 * the next execution of SlruRecentlyUsed will mark the page newly
		 * used, even if it's for a page that has the current counter value.
		 * That gets us back on the path to having good data when there are
		 * multiple pages with the same lru_count.
		 */
		cur_count = (shared->cur_lru_count)++;
		for (slotno = 0; slotno < shared->num_slots; slotno++)
		{
			int			this_delta;
			int			this_page_number;

			if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
				return slotno;
			this_delta = cur_count - shared->page_lru_count[slotno];
			if (this_delta < 0)
			{
				/*
				 * Clean up in case shared updates have caused cur_count
				 * increments to get "lost".  We back off the page counts,
				 * rather than trying to increase cur_count, to avoid any
				 * question of infinite loops or failure in the presence of
				 * wrapped-around counts.
				 */
				shared->page_lru_count[slotno] = cur_count;
				this_delta = 0;
			}
			this_page_number = shared->page_number[slotno];
			if (this_page_number == shared->latest_page_number)
				continue;
			if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			{
				if (this_delta > best_valid_delta ||
					(this_delta == best_valid_delta &&
					 ctl->PagePrecedes(this_page_number,
									   best_valid_page_number)))
				{
					bestvalidslot = slotno;
					best_valid_delta = this_delta;
					best_valid_page_number = this_page_number;
				}
			}
			else
			{
				if (this_delta > best_invalid_delta ||
					(this_delta == best_invalid_delta &&
					 ctl->PagePrecedes(this_page_number,
									   best_invalid_page_number)))
				{
					bestinvalidslot = slotno;
					best_invalid_delta = this_delta;
					best_invalid_page_number = this_page_number;
				}
			}
		}

		/*
		 * If all pages (except possibly the latest one) are I/O busy, we'll
		 * have to wait for an I/O to complete and then retry.  In that
		 * unhappy case, we choose to wait for the I/O on the least recently
		 * used slot, on the assumption that it was likely initiated first of
		 * all the I/Os in progress and may therefore finish first.
		 */
		if (best_valid_delta < 0)
		{
			SimpleLruWaitIO(ctl, bestinvalidslot);
			continue;
		}

		/*
		 * If the selected page is clean, we're set.
		 */
		if (!shared->page_dirty[bestvalidslot])
			return bestvalidslot;

		/*
		 * Write the page.
		 */
		SlruInternalWritePage(ctl, bestvalidslot, NULL);

		/*
		 * Now loop back and try again.  This is the easiest way of dealing
		 * with corner cases such as the victim page being re-dirtied while we
		 * wrote it.
		 */
	}
}

/*
 * Write dirty pages to disk during checkpoint or database shutdown.  Flushing
 * is deferred until the next call to ProcessSyncRequests(), though we do fsync
 * the containing directory here to make sure that newly created directory
 * entries are on disk.
 */
void
SimpleLruWriteAll(SlruCtl ctl, bool allow_redirtied)
{
	SlruShared	shared = ctl->shared;
	SlruWriteAllData fdata;
	int			slotno;
	int			pageno = 0;
	int			i;
	bool		ok;

	/* update the stats counter of flushes */
	pgstat_count_slru_flush(shared->slru_stats_idx);

	/*
	 * Find and write dirty pages
	 */
	fdata.num_files = 0;

	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		SlruInternalWritePage(ctl, slotno, &fdata);

		/*
		 * In some places (e.g. checkpoints), we cannot assert that the slot
		 * is clean now, since another process might have re-dirtied it
		 * already.  That's okay.
		 */
		Assert(allow_redirtied ||
			   shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
			   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
				!shared->page_dirty[slotno]));
	}

	LWLockRelease(shared->ControlLock);

	/*
	 * Now close any files that were open
	 */
	ok = true;
	for (i = 0; i < fdata.num_files; i++)
	{
		if (CloseTransientFile(fdata.fd[i]) != 0)
		{
			slru_errcause = SLRU_CLOSE_FAILED;
			slru_errno = errno;
			pageno = fdata.segno[i] * SLRU_PAGES_PER_SEGMENT;
			ok = false;
		}
	}
	if (!ok)
		SlruReportIOError(ctl, pageno, InvalidTransactionId);

	/* Ensure that directory entries for new files are on disk. */
	if (ctl->sync_handler != SYNC_HANDLER_NONE)
		fsync_fname(ctl->Dir, true);
}

/*
 * Remove all segments before the one holding the passed page number
 *
 * All SLRUs prevent concurrent calls to this function, either with an LWLock
 * or by calling it only as part of a checkpoint.  Mutual exclusion must begin
 * before computing cutoffPage.  Mutual exclusion must end after any limit
 * update that would permit other backends to write fresh data into the
 * segment immediately preceding the one containing cutoffPage.  Otherwise,
 * when the SLRU is quite full, SimpleLruTruncate() might delete that segment
 * after it has accrued freshly-written data.
 */
void
SimpleLruTruncate(SlruCtl ctl, int cutoffPage)
{
	SlruShared	shared = ctl->shared;
	int			slotno;

	/* update the stats counter of truncates */
	pgstat_count_slru_truncate(shared->slru_stats_idx);

	/*
	 * Scan shared memory and remove any pages preceding the cutoff page, to
	 * ensure we won't rewrite them later.  (Since this is normally called in
	 * or just after a checkpoint, any dirty pages should have been flushed
	 * already ... we're just being extra careful here.)
	 */
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

restart:

	/*
	 * While we are holding the lock, make an important safety check: the
	 * current endpoint page must not be eligible for removal.
	 */
	if (ctl->PagePrecedes(shared->latest_page_number, cutoffPage))
	{
		LWLockRelease(shared->ControlLock);
		ereport(LOG,
				(errmsg("could not truncate directory \"%s\": apparent wraparound",
						ctl->Dir)));
		return;
	}

	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
			continue;
		if (!ctl->PagePrecedes(shared->page_number[slotno], cutoffPage))
			continue;

		/*
		 * If page is clean, just change state to EMPTY (expected case).
		 */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno])
		{
			shared->page_status[slotno] = SLRU_PAGE_EMPTY;
#ifdef USE_PGRAC_CLUSTER
			shared->page_initial_full_xid[slotno] = 0;
#endif
			continue;
		}

		/*
		 * Hmm, we have (or may have) I/O operations acting on the page, so
		 * we've got to wait for them to finish and then start again. This is
		 * the same logic as in SlruSelectLRUPage.  (XXX if page is dirty,
		 * wouldn't it be OK to just discard it without writing it?
		 * SlruMayDeleteSegment() uses a stricter qualification, so we might
		 * not delete this page in the end; even if we don't delete it, we
		 * won't have cause to read its data again.  For now, keep the logic
		 * the same as it was.)
		 */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			SlruInternalWritePage(ctl, slotno, NULL);
		else
			SimpleLruWaitIO(ctl, slotno);
		goto restart;
	}

	LWLockRelease(shared->ControlLock);

	/* Now we can remove the old segment(s) */
	(void) SlruScanDirectory(ctl, SlruScanDirCbDeleteCutoff, &cutoffPage);
}

/*
 * Delete an individual SLRU segment.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 *
 * PGRAC modifications by SqlRush <sqlrush@gmail.com>:
 * What changed: drain queued shared SUBTRANS writes before the local forget.
 * Why: a completed old write must not reinstate a deleted file's sync request.
 */
static void
SlruInternalDeleteSegment(SlruCtl ctl, int segno)
{
	char		path[MAXPGPATH];

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: old queued writes must precede the local forget request. */
	if (ctl->sync_handler == SYNC_HANDLER_CLUSTER_SUBTRANS &&
		AmCheckpointerProcess())
		AbsorbSyncRequests();
#endif

	/* Forget any fsync requests queued for this segment. */
	if (ctl->sync_handler != SYNC_HANDLER_NONE)
	{
		FileTag		tag;

		INIT_SLRUFILETAG(tag, ctl->sync_handler, segno);
		RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);
	}

	/* Unlink the file. */
	SlruFileName(ctl, path, segno);
	ereport(DEBUG2, (errmsg_internal("removing file \"%s\"", path)));
	unlink(path);
}

/*
 * Delete an individual SLRU segment, identified by the segment number.
 */
void
SlruDeleteSegment(SlruCtl ctl, int segno)
{
	SlruShared	shared = ctl->shared;
	int			slotno;
	bool		did_write;

	/* Clean out any possibly existing references to the segment. */
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
restart:
	did_write = false;
	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		int			pagesegno = shared->page_number[slotno] / SLRU_PAGES_PER_SEGMENT;

		if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
			continue;

		/* not the segment we're looking for */
		if (pagesegno != segno)
			continue;

		/* If page is clean, just change state to EMPTY (expected case). */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno])
		{
			shared->page_status[slotno] = SLRU_PAGE_EMPTY;
#ifdef USE_PGRAC_CLUSTER
			shared->page_initial_full_xid[slotno] = 0;
#endif
			continue;
		}

		/* Same logic as SimpleLruTruncate() */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			SlruInternalWritePage(ctl, slotno, NULL);
		else
			SimpleLruWaitIO(ctl, slotno);

		did_write = true;
	}

	/*
	 * Be extra careful and re-check. The IO functions release the control
	 * lock, so new pages could have been read in.
	 */
	if (did_write)
		goto restart;

	SlruInternalDeleteSegment(ctl, segno);

	LWLockRelease(shared->ControlLock);
}

/*
 * Determine whether a segment is okay to delete.
 *
 * segpage is the first page of the segment, and cutoffPage is the oldest (in
 * PagePrecedes order) page in the SLRU containing still-useful data.  Since
 * every core PagePrecedes callback implements "wrap around", check the
 * segment's first and last pages:
 *
 * first<cutoff  && last<cutoff:  yes
 * first<cutoff  && last>=cutoff: no; cutoff falls inside this segment
 * first>=cutoff && last<cutoff:  no; wrap point falls inside this segment
 * first>=cutoff && last>=cutoff: no; every page of this segment is too young
 */
static bool
SlruMayDeleteSegment(SlruCtl ctl, int segpage, int cutoffPage)
{
	int			seg_last_page = segpage + SLRU_PAGES_PER_SEGMENT - 1;

	Assert(segpage % SLRU_PAGES_PER_SEGMENT == 0);

	return (ctl->PagePrecedes(segpage, cutoffPage) &&
			ctl->PagePrecedes(seg_last_page, cutoffPage));
}

#ifdef USE_ASSERT_CHECKING
static void
SlruPagePrecedesTestOffset(SlruCtl ctl, int per_page, uint32 offset)
{
	TransactionId lhs,
				rhs;
	int			newestPage,
				oldestPage;
	TransactionId newestXact,
				oldestXact;

	/*
	 * Compare an XID pair having undefined order (see RFC 1982), a pair at
	 * "opposite ends" of the XID space.  TransactionIdPrecedes() treats each
	 * as preceding the other.  If RHS is oldestXact, LHS is the first XID we
	 * must not assign.
	 */
	lhs = per_page + offset;	/* skip first page to avoid non-normal XIDs */
	rhs = lhs + (1U << 31);
	Assert(TransactionIdPrecedes(lhs, rhs));
	Assert(TransactionIdPrecedes(rhs, lhs));
	Assert(!TransactionIdPrecedes(lhs - 1, rhs));
	Assert(TransactionIdPrecedes(rhs, lhs - 1));
	Assert(TransactionIdPrecedes(lhs + 1, rhs));
	Assert(!TransactionIdPrecedes(rhs, lhs + 1));
	Assert(!TransactionIdFollowsOrEquals(lhs, rhs));
	Assert(!TransactionIdFollowsOrEquals(rhs, lhs));
	Assert(!ctl->PagePrecedes(lhs / per_page, lhs / per_page));
	Assert(!ctl->PagePrecedes(lhs / per_page, rhs / per_page));
	Assert(!ctl->PagePrecedes(rhs / per_page, lhs / per_page));
	Assert(!ctl->PagePrecedes((lhs - per_page) / per_page, rhs / per_page));
	Assert(ctl->PagePrecedes(rhs / per_page, (lhs - 3 * per_page) / per_page));
	Assert(ctl->PagePrecedes(rhs / per_page, (lhs - 2 * per_page) / per_page));
	Assert(ctl->PagePrecedes(rhs / per_page, (lhs - 1 * per_page) / per_page)
		   || (1U << 31) % per_page != 0);	/* See CommitTsPagePrecedes() */
	Assert(ctl->PagePrecedes((lhs + 1 * per_page) / per_page, rhs / per_page)
		   || (1U << 31) % per_page != 0);
	Assert(ctl->PagePrecedes((lhs + 2 * per_page) / per_page, rhs / per_page));
	Assert(ctl->PagePrecedes((lhs + 3 * per_page) / per_page, rhs / per_page));
	Assert(!ctl->PagePrecedes(rhs / per_page, (lhs + per_page) / per_page));

	/*
	 * GetNewTransactionId() has assigned the last XID it can safely use, and
	 * that XID is in the *LAST* page of the second segment.  We must not
	 * delete that segment.
	 */
	newestPage = 2 * SLRU_PAGES_PER_SEGMENT - 1;
	newestXact = newestPage * per_page + offset;
	Assert(newestXact / per_page == newestPage);
	oldestXact = newestXact + 1;
	oldestXact -= 1U << 31;
	oldestPage = oldestXact / per_page;
	Assert(!SlruMayDeleteSegment(ctl,
								 (newestPage -
								  newestPage % SLRU_PAGES_PER_SEGMENT),
								 oldestPage));

	/*
	 * GetNewTransactionId() has assigned the last XID it can safely use, and
	 * that XID is in the *FIRST* page of the second segment.  We must not
	 * delete that segment.
	 */
	newestPage = SLRU_PAGES_PER_SEGMENT;
	newestXact = newestPage * per_page + offset;
	Assert(newestXact / per_page == newestPage);
	oldestXact = newestXact + 1;
	oldestXact -= 1U << 31;
	oldestPage = oldestXact / per_page;
	Assert(!SlruMayDeleteSegment(ctl,
								 (newestPage -
								  newestPage % SLRU_PAGES_PER_SEGMENT),
								 oldestPage));
}

/*
 * Unit-test a PagePrecedes function.
 *
 * This assumes every uint32 >= FirstNormalTransactionId is a valid key.  It
 * assumes each value occupies a contiguous, fixed-size region of SLRU bytes.
 * (MultiXactMemberCtl separates flags from XIDs.  NotifyCtl has
 * variable-length entries, no keys, and no random access.  These unit tests
 * do not apply to them.)
 */
void
SlruPagePrecedesUnitTests(SlruCtl ctl, int per_page)
{
	/* Test first, middle and last entries of a page. */
	SlruPagePrecedesTestOffset(ctl, per_page, 0);
	SlruPagePrecedesTestOffset(ctl, per_page, per_page / 2);
	SlruPagePrecedesTestOffset(ctl, per_page, per_page - 1);
}
#endif

/*
 * SlruScanDirectory callback
 *		This callback reports true if there's any segment wholly prior to the
 *		one containing the page passed as "data".
 */
bool
SlruScanDirCbReportPresence(SlruCtl ctl, char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	if (SlruMayDeleteSegment(ctl, segpage, cutoffPage))
		return true;			/* found one; don't iterate any more */

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
static bool
SlruScanDirCbDeleteCutoff(SlruCtl ctl, char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	if (SlruMayDeleteSegment(ctl, segpage, cutoffPage))
		SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT);

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes all segments.
 */
bool
SlruScanDirCbDeleteAll(SlruCtl ctl, char *filename, int segpage, void *data)
{
	SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT);

	return false;				/* keep going */
}

/*
 * Scan the SimpleLru directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the SlruCtl struct for the
 * slru being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool
SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *data)
{
	bool		retval = false;
	DIR		   *cldir;
	struct dirent *clde;
	int			segno;
	int			segpage;

	cldir = AllocateDir(ctl->Dir);
	while ((clde = ReadDir(cldir, ctl->Dir)) != NULL)
	{
		size_t		len;

		len = strlen(clde->d_name);

		if ((len == 4 || len == 5 || len == 6) &&
			strspn(clde->d_name, "0123456789ABCDEF") == len)
		{
			segno = (int) strtol(clde->d_name, NULL, 16);
			segpage = segno * SLRU_PAGES_PER_SEGMENT;

			elog(DEBUG2, "SlruScanDirectory invoking callback on %s/%s",
				 ctl->Dir, clde->d_name);
			retval = callback(ctl, clde->d_name, segpage, data);
			if (retval)
				break;
		}
	}
	FreeDir(cldir);

	return retval;
}

/*
 * Individual SLRUs (clog, ...) have to provide a sync.c handler function so
 * that they can provide the correct "SlruCtl" (otherwise we don't know how to
 * build the path), but they just forward to this common implementation that
 * performs the fsync.
 */
int
SlruSyncFileTag(SlruCtl ctl, const FileTag *ftag, char *path)
{
	int			fd;
	int			save_errno;
	int			result;

	SlruFileName(ctl, path, ftag->segno);

	fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		return -1;

	pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
	result = pg_fsync(fd);
	pgstat_report_wait_end();
	save_errno = errno;

	CloseTransientFile(fd);

	errno = save_errno;
	return result;
}
