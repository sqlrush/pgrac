/*-------------------------------------------------------------------------
 *
 * bufmgr.h
 *	  POSTGRES buffer manager definitions.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/bufmgr.h
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *	  - Adds the barrier-aware SHARE lock entry point used by typed heap
 *		index fetches.
 *		Spec: spec-8.2-share-barrier-aware-unwind-requalify.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFMGR_H
#define BUFMGR_H

#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

typedef void *Block;

/*
 * Possible arguments for GetAccessStrategy().
 *
 * If adding a new BufferAccessStrategyType, also add a new IOContext so
 * IO statistics using this strategy are tracked.
 */
typedef enum BufferAccessStrategyType
{
	BAS_NORMAL,					/* Normal random access */
	BAS_BULKREAD,				/* Large read-only scan (hint bit updates are
								 * ok) */
	BAS_BULKWRITE,				/* Large multi-block write (e.g. COPY IN) */
	BAS_VACUUM					/* VACUUM */
} BufferAccessStrategyType;

/* Possible modes for ReadBufferExtended() */
typedef enum
{
	RBM_NORMAL,					/* Normal read */
	RBM_ZERO_AND_LOCK,			/* Don't read from disk, caller will
								 * initialize. Also locks the page. */
	RBM_ZERO_AND_CLEANUP_LOCK,	/* Like RBM_ZERO_AND_LOCK, but locks the page
								 * in "cleanup" mode */
	RBM_ZERO_ON_ERROR,			/* Read, but return an all-zeros page on error */
	RBM_NORMAL_NO_LOG			/* Don't log page as invalid during WAL
								 * replay; otherwise same as RBM_NORMAL */
} ReadBufferMode;

/*
 * Type returned by PrefetchBuffer().
 */
typedef struct PrefetchBufferResult
{
	Buffer		recent_buffer;	/* If valid, a hit (recheck needed!) */
	bool		initiated_io;	/* If true, a miss resulting in async I/O */
} PrefetchBufferResult;

/*
 * Flags influencing the behaviour of ExtendBufferedRel*
 */
typedef enum ExtendBufferedFlags
{
	/*
	 * Don't acquire extension lock. This is safe only if the relation isn't
	 * shared, an access exclusive lock is held or if this is the startup
	 * process.
	 */
	EB_SKIP_EXTENSION_LOCK = (1 << 0),

	/* Is this extension part of recovery? */
	EB_PERFORMING_RECOVERY = (1 << 1),

	/*
	 * Should the fork be created if it does not currently exist? This likely
	 * only ever makes sense for relation forks.
	 */
	EB_CREATE_FORK_IF_NEEDED = (1 << 2),

	/* Should the first (possibly only) return buffer be returned locked? */
	EB_LOCK_FIRST = (1 << 3),

	/* Should the smgr size cache be cleared? */
	EB_CLEAR_SIZE_CACHE = (1 << 4),

	/* internal flags follow */
	EB_LOCK_TARGET = (1 << 5),
}			ExtendBufferedFlags;

/*
 * Some functions identify relations either by relation or smgr +
 * relpersistence.  Used via the BMR_REL()/BMR_SMGR() macros below.  This
 * allows us to use the same function for both recovery and normal operation.
 */
typedef struct BufferManagerRelation
{
	Relation	rel;
	struct SMgrRelationData *smgr;
	char		relpersistence;
} BufferManagerRelation;

#define BMR_REL(p_rel) ((BufferManagerRelation){.rel = p_rel})
#define BMR_SMGR(p_smgr, p_relpersistence) ((BufferManagerRelation){.smgr = p_smgr, .relpersistence = p_relpersistence})


/* forward declared, to avoid having to expose buf_internals.h here */
struct WritebackContext;

/* forward declared, to avoid including smgr.h here */
struct SMgrRelationData;

/* in globals.c ... this duplicates miscadmin.h */
extern PGDLLIMPORT int NBuffers;

/* in bufmgr.c */
extern PGDLLIMPORT bool zero_damaged_pages;
extern PGDLLIMPORT int bgwriter_lru_maxpages;
extern PGDLLIMPORT double bgwriter_lru_multiplier;
extern PGDLLIMPORT bool track_io_timing;

/* only applicable when prefetching is available */
#ifdef USE_PREFETCH
#define DEFAULT_EFFECTIVE_IO_CONCURRENCY 1
#define DEFAULT_MAINTENANCE_IO_CONCURRENCY 10
#else
#define DEFAULT_EFFECTIVE_IO_CONCURRENCY 0
#define DEFAULT_MAINTENANCE_IO_CONCURRENCY 0
#endif
extern PGDLLIMPORT int effective_io_concurrency;
extern PGDLLIMPORT int maintenance_io_concurrency;

extern PGDLLIMPORT int checkpoint_flush_after;
extern PGDLLIMPORT int backend_flush_after;
extern PGDLLIMPORT int bgwriter_flush_after;

/* in buf_init.c */
extern PGDLLIMPORT char *BufferBlocks;

/* in localbuf.c */
extern PGDLLIMPORT int NLocBuffer;
#ifdef USE_PGRAC_CLUSTER
extern bool cluster_buffer_backend_has_pins(void);
#endif
extern PGDLLIMPORT Block *LocalBufferBlockPointers;
extern PGDLLIMPORT int32 *LocalRefCount;

/* upper limit for effective_io_concurrency */
#define MAX_IO_CONCURRENCY 1000

/* special block number for ReadBuffer() */
#define P_NEW	InvalidBlockNumber	/* grow the file to get a new page */

/*
 * Buffer content lock modes (mode argument for LockBuffer())
 */
#define BUFFER_LOCK_UNLOCK		0
#define BUFFER_LOCK_SHARE		1
#define BUFFER_LOCK_EXCLUSIVE	2


/*
 * prototypes for functions in bufmgr.c
 */
extern PrefetchBufferResult PrefetchSharedBuffer(struct SMgrRelationData *smgr_reln,
												 ForkNumber forkNum,
												 BlockNumber blockNum);
extern PrefetchBufferResult PrefetchBuffer(Relation reln, ForkNumber forkNum,
										   BlockNumber blockNum);
extern bool ReadRecentBuffer(RelFileLocator rlocator, ForkNumber forkNum,
							 BlockNumber blockNum, Buffer recent_buffer);
extern Buffer ReadBuffer(Relation reln, BlockNumber blockNum);
extern Buffer ReadBufferExtended(Relation reln, ForkNumber forkNum,
								 BlockNumber blockNum, ReadBufferMode mode,
								 BufferAccessStrategy strategy);
extern Buffer ReadBufferWithoutRelcache(RelFileLocator rlocator,
										ForkNumber forkNum, BlockNumber blockNum,
										ReadBufferMode mode, BufferAccessStrategy strategy,
										bool permanent);
extern void ReleaseBuffer(Buffer buffer);
extern void UnlockReleaseBuffer(Buffer buffer);
extern void MarkBufferDirty(Buffer buffer);
extern void IncrBufferRefCount(Buffer buffer);
extern void CheckBufferIsPinnedOnce(Buffer buffer);
extern Buffer ReleaseAndReadBuffer(Buffer buffer, Relation relation,
								   BlockNumber blockNum);

extern Buffer ExtendBufferedRel(BufferManagerRelation bmr,
								ForkNumber forkNum,
								BufferAccessStrategy strategy,
								uint32 flags);
extern BlockNumber ExtendBufferedRelBy(BufferManagerRelation bmr,
									   ForkNumber fork,
									   BufferAccessStrategy strategy,
									   uint32 flags,
									   uint32 extend_by,
									   Buffer *buffers,
									   uint32 *extended_by);
extern Buffer ExtendBufferedRelTo(BufferManagerRelation bmr,
								  ForkNumber fork,
								  BufferAccessStrategy strategy,
								  uint32 flags,
								  BlockNumber extend_to,
								  ReadBufferMode mode);

extern void InitBufferPoolAccess(void);
extern void AtEOXact_Buffers(bool isCommit);
#ifdef USE_ASSERT_CHECKING
extern void AssertBufferLocksPermitCatalogRead(void);
#endif
extern void PrintBufferLeakWarning(Buffer buffer);
extern void CheckPointBuffers(int flags);
extern BlockNumber BufferGetBlockNumber(Buffer buffer);
extern BlockNumber RelationGetNumberOfBlocksInFork(Relation relation,
												   ForkNumber forkNum);
extern void FlushOneBuffer(Buffer buffer);
extern void FlushRelationBuffers(Relation rel);
extern void FlushRelationsAllBuffers(struct SMgrRelationData **smgrs, int nrels);
extern void CreateAndCopyRelationData(RelFileLocator src_rlocator,
									  RelFileLocator dst_rlocator,
									  bool permanent);
extern void FlushDatabaseBuffers(Oid dbid);
extern void DropRelationBuffers(struct SMgrRelationData *smgr_reln,
								ForkNumber *forkNum,
								int nforks, BlockNumber *firstDelBlock);
extern void DropRelationsAllBuffers(struct SMgrRelationData **smgr_reln,
									int nlocators);
extern void DropDatabaseBuffers(Oid dbid);

#define RelationGetNumberOfBlocks(reln) \
	RelationGetNumberOfBlocksInFork(reln, MAIN_FORKNUM)

extern bool BufferIsPermanent(Buffer buffer);
extern XLogRecPtr BufferGetLSNAtomic(Buffer buffer);

#ifdef NOT_USED
extern void PrintPinnedBufs(void);
#endif
extern void BufferGetTag(Buffer buffer, RelFileLocator *rlocator,
						 ForkNumber *forknum, BlockNumber *blknum);

extern void MarkBufferDirtyHint(Buffer buffer, bool buffer_std);

extern void UnlockBuffers(void);
extern void LockBuffer(Buffer buffer, int mode);

/*
 * PGRAC Stage-8 R2 D11: evidence-only identities for the seven real
 * EXCLUSIVE barrier-refusal callers.  These values select no behavior; they
 * are copied to one passive trace probe after the corresponding fact has
 * already been decided by the existing lock/cleanup/re-entry path.
 */
typedef enum ClusterBufferBarrierSiteId
{
	CLUSTER_BUFFER_BARRIER_SITE_HEAP_DELETE_VM = 1,
	CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PRETOAST_VM = 2,
	CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PAIR_NEW_FIRST = 3,
	CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PAIR_OLD_SECOND = 4,
	CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_OLD = 5,
	CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_NEW = 6,
	CLUSTER_BUFFER_BARRIER_SITE_HIO_PAIR_SECOND = 7
} ClusterBufferBarrierSiteId;

typedef enum ClusterBufferBarrierPhase
{
	CLUSTER_BUFFER_BARRIER_PHASE_LOWER_REFUSED = 1,
	CLUSTER_BUFFER_BARRIER_PHASE_COMMON_EMPTY = 2,
	CLUSTER_BUFFER_BARRIER_PHASE_CALLER_POST = 3,
	CLUSTER_BUFFER_BARRIER_PHASE_REENTRY = 4
} ClusterBufferBarrierPhase;

typedef enum ClusterBufferBarrierOutcome
{
	CLUSTER_BUFFER_BARRIER_OUTCOME_BARRIER_CLOSED = 1,
	CLUSTER_BUFFER_BARRIER_OUTCOME_EMPTY = 2,
	CLUSTER_BUFFER_BARRIER_OUTCOME_POSTCONDITION_OK = 3,
	CLUSTER_BUFFER_BARRIER_OUTCOME_REQUALIFIED = 4,
	CLUSTER_BUFFER_BARRIER_OUTCOME_HIO_RETRY = 5
} ClusterBufferBarrierOutcome;

#define CLUSTER_BUFFER_BARRIER_PROOF_LOWER_REFUSED \
	UINT64CONST(0x0000000000000001)
#define CLUSTER_BUFFER_BARRIER_PROOF_COMMON_EMPTY \
	UINT64CONST(0x000000000000001e)
#define CLUSTER_BUFFER_BARRIER_PROOF_CALLER_POST \
	UINT64CONST(0x00000000000000e0)
#define CLUSTER_BUFFER_BARRIER_PROOF_REENTRY \
	UINT64CONST(0x0000000000000100)

extern void ClusterObserveBufferBarrierReceipt(ClusterBufferBarrierSiteId site_id,
											ClusterBufferBarrierPhase phase,
											RelFileLocator rlocator,
											ForkNumber forknum,
											BlockNumber blocknum,
											ClusterBufferBarrierOutcome outcome,
											uint64 proof_mask);
/* PGRAC: EXCLUSIVE lock that hands a nested-guard BARRIER_CLOSED refusal or
 * pre-mutation Resource-X drift back to the caller (false, nothing held)
 * instead of raising an ERROR.  pin_replaced reports that a deliberate
 * VM/FSM pin handoff let the old numeric BufferDesc be reused. */
extern bool ClusterLockBufferExclusiveBarrierAware(Buffer buffer,
											 ClusterBufferBarrierSiteId site_id,
											 bool *pin_replaced);
/* PGRAC: EXCLUSIVE lock for pass-based auxiliary work.  False is a clean
 * pre-content-X Resource-X retry; the caller retains only its original pin. */
extern bool ClusterLockBufferExclusiveRetryAware(Buffer buffer);
/* PGRAC: SHARE counterpart with the same clean-refusal contract. */
extern bool ClusterLockBufferShareBarrierAware(Buffer buffer);
/* PGRAC: operation-scoped PCM-X direct-init entrances for zero VM/FSM pages. */
/* Returns InvalidBuffer when the caller's pin was deliberately released for
 * a Resource-X wait and the old descriptor was reused.  The relation-level
 * VM/FSM reader must obtain a fresh pin and restart its exact init path. */
extern Buffer LockBufferForVisibilityMapPageInit(Buffer buffer);
extern Buffer LockBufferForFreeSpaceMapPageInit(Buffer buffer);
extern bool ConditionalLockBuffer(Buffer buffer);
extern void LockBufferForCleanup(Buffer buffer);
extern bool ConditionalLockBufferForCleanup(Buffer buffer);
extern bool IsBufferCleanupOK(Buffer buffer);
extern bool HoldingBufferPinThatDelaysRecovery(void);

extern void AbortBufferIO(Buffer buffer);

extern bool BgBufferSync(struct WritebackContext *wb_context);

extern void TestForOldSnapshot_impl(Snapshot snapshot, Relation relation);

/* in buf_init.c */
extern void InitBufferPool(void);
extern Size BufferShmemSize(void);

/* in localbuf.c */
extern void AtProcExit_LocalBuffers(void);

/* in freelist.c */

extern BufferAccessStrategy GetAccessStrategy(BufferAccessStrategyType btype);
extern BufferAccessStrategy GetAccessStrategyWithSize(BufferAccessStrategyType btype,
													  int ring_size_kb);
extern int	GetAccessStrategyBufferCount(BufferAccessStrategy strategy);

extern void FreeAccessStrategy(BufferAccessStrategy strategy);


/* inline functions */

/*
 * Although this header file is nominally backend-only, certain frontend
 * programs like pg_waldump include it.  For compilers that emit static
 * inline functions even when they're unused, that leads to unsatisfied
 * external references; hence hide these with #ifndef FRONTEND.
 */

#ifndef FRONTEND

/*
 * BufferIsValid
 *		True iff the given buffer number is valid (either as a shared
 *		or local buffer).
 *
 * Note: For a long time this was defined the same as BufferIsPinned,
 * that is it would say False if you didn't hold a pin on the buffer.
 * I believe this was bogus and served only to mask logic errors.
 * Code should always know whether it has a buffer reference,
 * independently of the pin state.
 *
 * Note: For a further long time this was not quite the inverse of the
 * BufferIsInvalid() macro, in that it also did sanity checks to verify
 * that the buffer number was in range.  Most likely, this macro was
 * originally intended only to be used in assertions, but its use has
 * since expanded quite a bit, and the overhead of making those checks
 * even in non-assert-enabled builds can be significant.  Thus, we've
 * now demoted the range checks to assertions within the macro itself.
 */
static inline bool
BufferIsValid(Buffer bufnum)
{
	Assert(bufnum <= NBuffers);
	Assert(bufnum >= -NLocBuffer);

	return bufnum != InvalidBuffer;
}

/*
 * BufferGetBlock
 *		Returns a reference to a disk page image associated with a buffer.
 *
 * Note:
 *		Assumes buffer is valid.
 */
static inline Block
BufferGetBlock(Buffer buffer)
{
	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
		return LocalBufferBlockPointers[-buffer - 1];
	else
		return (Block) (BufferBlocks + ((Size) (buffer - 1)) * BLCKSZ);
}

/*
 * BufferGetPageSize
 *		Returns the page size within a buffer.
 *
 * Notes:
 *		Assumes buffer is valid.
 *
 *		The buffer can be a raw disk block and need not contain a valid
 *		(formatted) disk page.
 */
/* XXX should dig out of buffer descriptor */
static inline Size
BufferGetPageSize(Buffer buffer)
{
	AssertMacro(BufferIsValid(buffer));
	return (Size) BLCKSZ;
}

/*
 * BufferGetPage
 *		Returns the page associated with a buffer.
 *
 * When this is called as part of a scan, there may be a need for a nearby
 * call to TestForOldSnapshot().  See the definition of that for details.
 */
static inline Page
BufferGetPage(Buffer buffer)
{
	return (Page) BufferGetBlock(buffer);
}

/*
 * Check whether the given snapshot is too old to have safely read the given
 * page from the given table.  If so, throw a "snapshot too old" error.
 *
 * This test generally needs to be performed after every BufferGetPage() call
 * that is executed as part of a scan.  It is not needed for calls made for
 * modifying the page (for example, to position to the right place to insert a
 * new index tuple or for vacuuming).  It may also be omitted where calls to
 * lower-level functions will have already performed the test.
 *
 * Note that a NULL snapshot argument is allowed and causes a fast return
 * without error; this is to support call sites which can be called from
 * either scans or index modification areas.
 *
 * For best performance, keep the tests that are fastest and/or most likely to
 * exclude a page from old snapshot testing near the front.
 */
static inline void
TestForOldSnapshot(Snapshot snapshot, Relation relation, Page page)
{
	Assert(relation != NULL);

	if (old_snapshot_threshold >= 0
		&& (snapshot) != NULL
		&& ((snapshot)->snapshot_type == SNAPSHOT_MVCC
			|| (snapshot)->snapshot_type == SNAPSHOT_TOAST)
		&& !XLogRecPtrIsInvalid((snapshot)->lsn)
		&& PageGetLSN(page) > (snapshot)->lsn)
		TestForOldSnapshot_impl(snapshot, relation);
}

#endif							/* FRONTEND */

#endif							/* BUFMGR_H */
