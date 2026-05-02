/*-------------------------------------------------------------------------
 *
 * buf_internals.h
 *	  Internal definitions for buffer manager and the buffer replacement
 *	  strategy.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf_internals.h
 *
 * PGRAC MODIFICATIONS (12th + 13th, in BufferDesc struct + BUFFERDESC_PAD_TO_SIZE)
 * -----------------------------------------------------------------------------
 * Modified by: SqlRush <sqlrush@gmail.com>
 *
 * What changed (USE_PGRAC_CLUSTER guarded):
 *   1. BufferDesc struct: append 12B hot cluster tail in cache line 1
 *      ([52, 64)) + 64B cold cluster fields in cache line 2 ([64, 128))
 *      after PG-original 52B (PG 16.13: BufferTag 20B + buf_id 4B +
 *      state 4B + wait_backend 4B + freeNext 4B + content_lock 16B).
 *      block_scn lives in cache line 1 (Stage 2-3 visibility hot path);
 *      cr_chain_head in cache line 2 (cold; CR construction path only).
 *      See spec-1.6 PIVOT B (2026-05-02 user approve).
 *   2. BUFFERDESC_PAD_TO_SIZE bumped 64 -> 128 (cluster fields require
 *      2 cache lines per slot).
 *   3. ClusterInitBufferDescFields: static inline helper writing the 17
 *      placeholder values at InitBufferPool / InitLocalBuffers time.
 *   4. 5 StaticAssertDecl layout invariants -- catch field reorder /
 *      size drift at compile time using semantic constraints (block_scn
 *      stays in cache line 1; cr_chain_head starts cache line 2;
 *      cluster fields follow PG-original content_lock so bufmgr.c:3275
 *      AssertNotCatalogBufferLock reverse-deref stays correct).
 *
 * Why:
 *   pgrac needs PCM lock state machine + CR chain + PI chain + Cache
 *   Fusion + GRD master cache fields per buffer (Stage 2-3 真值激活).
 *   Stage 1.6 places the structural foundation; runtime semantics land
 *   later.  Hot path performance preserved by keeping all hot fields
 *   in cache line 1 (PG 16.13: 52B + cluster 12B tail = 64B).
 *
 * Spec: spec-1.6-buffer-descriptor.md (frozen 2026-05-02)
 * Design: docs/buffer-pool-design.md v1.2 §4.3 + §12 + §15.4
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFMGR_INTERNALS_H
#define BUFMGR_INTERNALS_H

#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/relcache.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlogdefs.h"		 /* PGRAC: XLogRecPtr, InvalidXLogRecPtr */
#include "cluster/cluster_buffer_desc.h" /* PGRAC: BufferType / PcmState / CacheFusionState / BufferFlags / INVALID_BUFFER_ID / INVALID_NODE_ID */
#include "cluster/cluster_scn.h"	 /* PGRAC: SCN, InvalidScn */
#include "datatype/timestamp.h"		 /* PGRAC: TimestampTz */
#endif

/*
 * Buffer state is a single 32-bit variable where following data is combined.
 *
 * - 18 bits refcount
 * - 4 bits usage count
 * - 10 bits of flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 */
#define BUF_REFCOUNT_ONE 1
#define BUF_REFCOUNT_MASK ((1U << 18) - 1)
#define BUF_USAGECOUNT_MASK 0x003C0000U
#define BUF_USAGECOUNT_ONE (1U << 18)
#define BUF_USAGECOUNT_SHIFT 18
#define BUF_FLAG_MASK 0xFFC00000U

/* Get refcount and usagecount from buffer state */
#define BUF_STATE_GET_REFCOUNT(state) ((state) & BUF_REFCOUNT_MASK)
#define BUF_STATE_GET_USAGECOUNT(state) (((state) & BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT)

/*
 * Flags for buffer descriptors
 *
 * Note: BM_TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
 */
#define BM_LOCKED				(1U << 22)	/* buffer header is locked */
#define BM_DIRTY				(1U << 23)	/* data needs writing */
#define BM_VALID				(1U << 24)	/* data is valid */
#define BM_TAG_VALID			(1U << 25)	/* tag is assigned */
#define BM_IO_IN_PROGRESS		(1U << 26)	/* read or write in progress */
#define BM_IO_ERROR				(1U << 27)	/* previous I/O failed */
#define BM_JUST_DIRTIED			(1U << 28)	/* dirtied since write started */
#define BM_PIN_COUNT_WAITER		(1U << 29)	/* have waiter for sole pin */
#define BM_CHECKPOINT_NEEDED	(1U << 30)	/* must write for checkpoint */
#define BM_PERMANENT			(1U << 31)	/* permanent buffer (not unlogged,
											 * or init fork) */
/*
 * The maximum allowed value of usage_count represents a tradeoff between
 * accuracy and speed of the clock-sweep buffer management algorithm.  A
 * large value (comparable to NBuffers) would approximate LRU semantics.
 * But it can take as many as BM_MAX_USAGE_COUNT+1 complete cycles of
 * clock sweeps to find a free buffer, so in practice we don't want the
 * value to be very large.
 */
#define BM_MAX_USAGE_COUNT	5

/*
 * Buffer tag identifies which disk block the buffer contains.
 *
 * Note: the BufferTag data must be sufficient to determine where to write the
 * block, without reference to pg_class or pg_tablespace entries.  It's
 * possible that the backend flushing the buffer doesn't even believe the
 * relation is visible yet (its xact may have started before the xact that
 * created the rel).  The storage manager must be able to cope anyway.
 *
 * Note: if there's any pad bytes in the struct, InitBufferTag will have
 * to be fixed to zero them, since this struct is used as a hash key.
 */
typedef struct buftag
{
	Oid			spcOid;			/* tablespace oid */
	Oid			dbOid;			/* database oid */
	RelFileNumber relNumber;	/* relation file number */
	ForkNumber	forkNum;		/* fork number */
	BlockNumber blockNum;		/* blknum relative to begin of reln */
} BufferTag;

static inline RelFileNumber
BufTagGetRelNumber(const BufferTag *tag)
{
	return tag->relNumber;
}

static inline ForkNumber
BufTagGetForkNum(const BufferTag *tag)
{
	return tag->forkNum;
}

static inline void
BufTagSetRelForkDetails(BufferTag *tag, RelFileNumber relnumber,
						ForkNumber forknum)
{
	tag->relNumber = relnumber;
	tag->forkNum = forknum;
}

static inline RelFileLocator
BufTagGetRelFileLocator(const BufferTag *tag)
{
	RelFileLocator rlocator;

	rlocator.spcOid = tag->spcOid;
	rlocator.dbOid = tag->dbOid;
	rlocator.relNumber = BufTagGetRelNumber(tag);

	return rlocator;
}

static inline void
ClearBufferTag(BufferTag *tag)
{
	tag->spcOid = InvalidOid;
	tag->dbOid = InvalidOid;
	BufTagSetRelForkDetails(tag, InvalidRelFileNumber, InvalidForkNumber);
	tag->blockNum = InvalidBlockNumber;
}

static inline void
InitBufferTag(BufferTag *tag, const RelFileLocator *rlocator,
			  ForkNumber forkNum, BlockNumber blockNum)
{
	tag->spcOid = rlocator->spcOid;
	tag->dbOid = rlocator->dbOid;
	BufTagSetRelForkDetails(tag, rlocator->relNumber, forkNum);
	tag->blockNum = blockNum;
}

static inline bool
BufferTagsEqual(const BufferTag *tag1, const BufferTag *tag2)
{
	return (tag1->spcOid == tag2->spcOid) &&
		(tag1->dbOid == tag2->dbOid) &&
		(tag1->relNumber == tag2->relNumber) &&
		(tag1->blockNum == tag2->blockNum) &&
		(tag1->forkNum == tag2->forkNum);
}

static inline bool
BufTagMatchesRelFileLocator(const BufferTag *tag,
							const RelFileLocator *rlocator)
{
	return (tag->spcOid == rlocator->spcOid) &&
		(tag->dbOid == rlocator->dbOid) &&
		(BufTagGetRelNumber(tag) == rlocator->relNumber);
}


/*
 * The shared buffer mapping table is partitioned to reduce contention.
 * To determine which partition lock a given tag requires, compute the tag's
 * hash code with BufTableHashCode(), then apply BufMappingPartitionLock().
 * NB: NUM_BUFFER_PARTITIONS must be a power of 2!
 */
static inline uint32
BufTableHashPartition(uint32 hashcode)
{
	return hashcode % NUM_BUFFER_PARTITIONS;
}

static inline LWLock *
BufMappingPartitionLock(uint32 hashcode)
{
	return &MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET +
							BufTableHashPartition(hashcode)].lock;
}

static inline LWLock *
BufMappingPartitionLockByIndex(uint32 index)
{
	return &MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET + index].lock;
}

/*
 *	BufferDesc -- shared descriptor/state data for a single shared buffer.
 *
 * Note: Buffer header lock (BM_LOCKED flag) must be held to examine or change
 * tag, state or wait_backend_pgprocno fields.  In general, buffer header lock
 * is a spinlock which is combined with flags, refcount and usagecount into
 * single atomic variable.  This layout allow us to do some operations in a
 * single atomic operation, without actually acquiring and releasing spinlock;
 * for instance, increase or decrease refcount.  buf_id field never changes
 * after initialization, so does not need locking.  freeNext is protected by
 * the buffer_strategy_lock not buffer header lock.  The LWLock can take care
 * of itself.  The buffer header lock is *not* used to control access to the
 * data in the buffer!
 *
 * It's assumed that nobody changes the state field while buffer header lock
 * is held.  Thus buffer header lock holder can do complex updates of the
 * state variable in single write, simultaneously with lock release (cleaning
 * BM_LOCKED flag).  On the other hand, updating of state without holding
 * buffer header lock is restricted to CAS, which ensures that BM_LOCKED flag
 * is not set.  Atomic increment/decrement, OR/AND etc. are not allowed.
 *
 * An exception is that if we have the buffer pinned, its tag can't change
 * underneath us, so we can examine the tag without locking the buffer header.
 * Also, in places we do one-time reads of the flags without bothering to
 * lock the buffer header; this is generally for situations where we don't
 * expect the flag bit being tested to be changing.
 *
 * We can't physically remove items from a disk page if another backend has
 * the buffer pinned.  Hence, a backend may need to wait for all other pins
 * to go away.  This is signaled by storing its own pgprocno into
 * wait_backend_pgprocno and setting flag bit BM_PIN_COUNT_WAITER.  At present,
 * there can be only one such waiter per buffer.
 *
 * We use this same struct for local buffer headers, but the locks are not
 * used and not all of the flag bits are useful either. To avoid unnecessary
 * overhead, manipulations of the state field should be done without actual
 * atomic operations (i.e. only pg_atomic_read_u32() and
 * pg_atomic_unlocked_write_u32()).
 *
 * Be careful to avoid increasing the size of the struct when adding or
 * reordering members.  Keeping it below 64 bytes (the most common CPU
 * cache line size) is fairly important for performance.
 *
 * Per-buffer I/O condition variables are currently kept outside this struct in
 * a separate array.  They could be moved in here and still fit within that
 * limit on common systems, but for now that is not done.
 */
typedef struct BufferDesc
{
	BufferTag	tag;			/* ID of page contained in buffer */
	int			buf_id;			/* buffer's index number (from 0) */

	/* state of the tag, containing flags, refcount and usagecount */
	pg_atomic_uint32 state;

	int			wait_backend_pgprocno;	/* backend of pin-count waiter */
	int			freeNext;		/* link in freelist chain */
	LWLock		content_lock;	/* to lock access to buffer contents */

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: cluster fields (stage 1.6, ~76B + alignment).
	 *
	 * On PG 16.13 the original BufferDesc fields above end at offset 52
	 * (BufferTag 20B + buf_id 4B + state 4B + wait_backend_pgprocno 4B
	 * + freeNext 4B + content_lock 16B = 52B).  This leaves only 12B of
	 * cache line 1 for cluster fields ([52, 64)) -- the "12B hot
	 * cluster tail".  Cache line 2 ([64, 128)) holds the 64B cold
	 * cluster fields accessed only on cluster-specific paths.
	 *
	 * PIVOT B (2026-05-02 user approve): block_scn must stay in cache
	 * line 1 because it's read on every visibility check (Stage 2-3
	 * hot path).  cr_chain_head moves to cache line 2 because CR
	 * construction is a cold path.  See spec-1.6 §1.4 example #5 and
	 * docs/buffer-pool-design.md v1.2 §4.3.
	 *
	 * All values are placeholders at stage 1.6 --
	 * ClusterInitBufferDescFields (defined below) writes them explicitly
	 * at InitBufferPool / InitLocalBuffers time.  Stage 2-3 真值激活 spec
	 * brings the actual PCM state machine / CR chain / PI / Cache
	 * Fusion / GRD master cache semantics.
	 *
	 * Spec: spec-1.6-buffer-descriptor.md
	 * Design: docs/buffer-pool-design.md v1.2 §4.3
	 */

	/* === Cache line 1 hot cluster tail ([52, 64), 12B usable) === */
	uint8		buffer_type;	/* offset 52; BufferType enum; BUF_TYPE_CURRENT at stage 1.6 */
	uint8		pcm_state;		/* offset 53; PcmState enum; PCM_STATE_N at stage 1.6 */
	uint8		pi_flags;		/* offset 54; BufferFlags bitfield; 0 at stage 1.6 (no PI) */
	uint8		cluster_padding_1;	/* offset 55; 1B padding for 8B alignment of block_scn; 0 at stage 1.6 */
	SCN			block_scn;		/* offset 56; page-level SCN (spec-1.4 typedef); InvalidScn at stage 1.6 */
	/* end of cache line 1 at offset 64 */

	/* === Cache line 2 cold cluster fields ([64, 128), 64B) === */
	int			cr_chain_head;	/* offset 64; CR chain head buf_id; INVALID_BUFFER_ID at stage 1.6 */
	int			cr_chain_next;	/* offset 68; next CR in chain; INVALID_BUFFER_ID at stage 1.6 */
	SCN			cr_scn;			/* offset 72; CR buffer's read SCN; InvalidScn at stage 1.6 */
	int			pi_buf_id;		/* offset 80; PI buffer's buf_id; INVALID_BUFFER_ID at stage 1.6 */
	/* offset 84..87: 4B implicit padding for pi_lsn 8-byte alignment */
	XLogRecPtr	pi_lsn;			/* offset 88; PI buffer's let-go LSN; InvalidXLogRecPtr at stage 1.6 */
	uint16		grd_master_node;	/* offset 96; GRD master node id; INVALID_NODE_ID at stage 1.6 */
	uint16		grd_master_seq; /* offset 98; GRD master seq; 0 at stage 1.6 */
	uint8		cf_state;		/* offset 100; CacheFusionState enum; CF_STATE_NONE at stage 1.6 */
	uint8		cf_owner_node;	/* offset 101; CF transfer owner node; 0 at stage 1.6 */
	uint16		cf_request_count;	/* offset 102; CF transfer request count; 0 at stage 1.6 */
	LWLock		pcm_lock;		/* offset 104; PCM lock; LWLockInitialize'd at stage 1.6 (not held) */
	TimestampTz pi_created_at;	/* offset 120; PI creation timestamp; 0 at stage 1.6 */
	/* end of cache line 2 at offset 128 */
#endif							/* USE_PGRAC_CLUSTER */
} BufferDesc;

/*
 * Concurrent access to buffer headers has proven to be more efficient if
 * they're cache line aligned. So we force the start of the BufferDescriptors
 * array to be on a cache line boundary and force the elements to be cache
 * line sized.
 *
 * XXX: As this is primarily matters in highly concurrent workloads which
 * probably all are 64bit these days, and the space wastage would be a bit
 * more noticeable on 32bit systems, we don't force the stride to be cache
 * line sized on those. If somebody does actual performance testing, we can
 * reevaluate.
 *
 * Note that local buffer descriptors aren't forced to be aligned - as there's
 * no concurrent access to those it's unlikely to be beneficial.
 *
 * We use a 64-byte cache line size here, because that's the most common
 * size. Making it bigger would be a waste of memory. Even if running on a
 * platform with either 32 or 128 byte line sizes, it's good to align to
 * boundaries and avoid false sharing.
 */
#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC: BufferDesc grew from PG-vanilla ~52B (padded to 64) to ~120B
 * (padded to 128) on PG 16.13.  12B hot cluster tail shares cache line 1
 * with PG-original 52B; 64B cold cluster fields occupy cache line 2 and
 * are accessed only on cluster-specific paths (Stage 2-3 真值激活).
 *
 * Hot path cache behaviour preserved: ReadBuffer / BufferAlloc /
 * UnpinBuffer / IncrBufferRefCount continue to read only cache line 1
 * (offsets 0..63), exactly the same cache miss profile as PG vanilla.
 *
 * Spec: spec-1.6-buffer-descriptor.md  Design: docs/buffer-pool-design.md v1.2 §4.3
 */
#define BUFFERDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 128 : 1)
#else
#define BUFFERDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)
#endif

typedef union BufferDescPadded
{
	BufferDesc	bufferdesc;
	char		pad[BUFFERDESC_PAD_TO_SIZE];
} BufferDescPadded;

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC: compile-time layout invariants for cluster BufferDesc fields.
 *
 *	These StaticAssertDecls catch any field reorder / size drift at
 *	build time, before any runtime test runs.
 *
 *	PIVOT B (2026-05-02): use semantic constraints instead of magic
 *	offset numbers.  PG 16.13 实测 sizeof(BufferTag) == 20 (not 16),
 *	pushing PG-original fields to offset 52 (not 48).  Hard-coded
 *	"offset 32 / 48 / 64" assertions break on any future PG version
 *	that grows BufferTag again; semantic constraints survive.
 *
 *	Critical: PG bufmgr.c:AssertNotCatalogBufferLock reverse-derefs
 *	BufferDesc from an LWLock pointer using
 *	  bufHdr = (BufferDesc *) ((char *) lock - offsetof(BufferDesc, content_lock))
 *	which uses the offsetof() macro and therefore adapts to whatever
 *	the actual content_lock offset is on the current PG version
 *	(36 on PG 16.13; 32 on earlier PG with smaller BufferTag).
 *	The semantic invariant we lock below is "cluster fields appear
 *	*after* content_lock"; any future reorder breaking that semantic
 *	fails to compile.
 *
 *	Spec: spec-1.6-buffer-descriptor.md §1.2 Deliverable 3.6 + §8 Q2.
 */
StaticAssertDecl(BUFFERDESC_PAD_TO_SIZE == 128,
				 "PGRAC: BUFFERDESC_PAD_TO_SIZE must be 128 in USE_PGRAC_CLUSTER mode");
StaticAssertDecl(sizeof(BufferDesc) <= BUFFERDESC_PAD_TO_SIZE,
				 "PGRAC: BufferDesc with cluster fields exceeds padded size");
StaticAssertDecl(offsetof(BufferDesc, block_scn) + sizeof(SCN) <= 64,
				 "PGRAC: block_scn must stay in BufferDesc cache line 1 (Stage 2-3 visibility hot path)");
StaticAssertDecl(offsetof(BufferDesc, cr_chain_head) >= 64,
				 "PGRAC: cr_chain_head (cold field) must start at or after cache line 2 boundary");
StaticAssertDecl(offsetof(BufferDesc, content_lock) <
				 offsetof(BufferDesc, buffer_type),
				 "PGRAC: cluster fields must follow PG-original content_lock so bufmgr.c:3275 reverse-deref stays correct");

/*
 * PGRAC: ClusterInitBufferDescFields -- write placeholder values to all
 *	17 cluster fields of a BufferDesc.
 *
 *	Called from both InitBufferPool() (shared buffer; PGRAC MODIFICATIONS
 *	14th in buf_init.c) and InitLocalBuffers() (local / temp buffer per
 *	backend; PGRAC MODIFICATIONS 16th in localbuf.c).  Any future
 *	BufferDesc init path (e.g. Stage 2 dynamic buffer pool resizing)
 *	MUST also call this helper to keep cluster fields in valid
 *	placeholder state.
 *
 *	Stage 1.6 stub; Stage 2-3 真值激活时调用方负责状态转换 (PCM 锁 /
 *	CR chain / PI chain / Cache Fusion / GRD).
 *
 *	Critical: cr_chain_head / cr_chain_next / pi_buf_id MUST be set to
 *	INVALID_BUFFER_ID (-1) explicitly -- calloc zero-fill (used by
 *	localbuf.c LocalBufferDescriptors = calloc(...)) leaves them as 0,
 *	which is a *valid* buffer_id and would mislead Stage 3 CR-path
 *	reads of local buffers.  pcm_lock also MUST be LWLockInitialize'd
 *	explicitly -- calloc 0 is not a valid LWLock state.  This is the
 *	entire reason this helper exists.
 *
 *	Spec: spec-1.6-buffer-descriptor.md §1.2 Deliverable 3.5 + §8 Q1.
 */
static inline void
ClusterInitBufferDescFields(BufferDesc *buf)
{
	/* hot cluster fields */
	buf->buffer_type = BUF_TYPE_CURRENT;
	buf->pcm_state = PCM_STATE_N;
	buf->pi_flags = 0;
	buf->cluster_padding_1 = 0;
	buf->cr_chain_head = INVALID_BUFFER_ID;
	buf->block_scn = InvalidScn;

	/* cold cluster fields */
	buf->cr_scn = InvalidScn;
	buf->cr_chain_next = INVALID_BUFFER_ID;
	buf->pi_buf_id = INVALID_BUFFER_ID;
	buf->pi_lsn = InvalidXLogRecPtr;
	buf->grd_master_node = INVALID_NODE_ID;
	buf->grd_master_seq = 0;
	buf->cf_state = CF_STATE_NONE;
	buf->cf_owner_node = 0;
	buf->cf_request_count = 0;

	/*
	 * pcm_lock: stage 1.6 reuses LWTRANCHE_BUFFER_CONTENT tranche (pcm
	 * lock is never acquired at stage 1.6; Stage 2 PCM真值激活 spec will
	 * register a dedicated LWTRANCHE_BUFFER_PCM_LOCK and migrate).
	 */
	LWLockInitialize(&buf->pcm_lock, LWTRANCHE_BUFFER_CONTENT);

	buf->pi_created_at = 0;
}
#endif							/* USE_PGRAC_CLUSTER */

/*
 * The PendingWriteback & WritebackContext structure are used to keep
 * information about pending flush requests to be issued to the OS.
 */
typedef struct PendingWriteback
{
	/* could store different types of pending flushes here */
	BufferTag	tag;
} PendingWriteback;

/* struct forward declared in bufmgr.h */
typedef struct WritebackContext
{
	/* pointer to the max number of writeback requests to coalesce */
	int		   *max_pending;

	/* current number of pending writeback requests */
	int			nr_pending;

	/* pending requests */
	PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;

/* in buf_init.c */
extern PGDLLIMPORT BufferDescPadded *BufferDescriptors;
extern PGDLLIMPORT ConditionVariableMinimallyPadded *BufferIOCVArray;
extern PGDLLIMPORT WritebackContext BackendWritebackContext;

/* in localbuf.c */
extern PGDLLIMPORT BufferDesc *LocalBufferDescriptors;


static inline BufferDesc *
GetBufferDescriptor(uint32 id)
{
	return &(BufferDescriptors[id]).bufferdesc;
}

static inline BufferDesc *
GetLocalBufferDescriptor(uint32 id)
{
	return &LocalBufferDescriptors[id];
}

static inline Buffer
BufferDescriptorGetBuffer(const BufferDesc *bdesc)
{
	return (Buffer) (bdesc->buf_id + 1);
}

static inline ConditionVariable *
BufferDescriptorGetIOCV(const BufferDesc *bdesc)
{
	return &(BufferIOCVArray[bdesc->buf_id]).cv;
}

static inline LWLock *
BufferDescriptorGetContentLock(const BufferDesc *bdesc)
{
	return (LWLock *) (&bdesc->content_lock);
}

/*
 * The freeNext field is either the index of the next freelist entry,
 * or one of these special values:
 */
#define FREENEXT_END_OF_LIST	(-1)
#define FREENEXT_NOT_IN_LIST	(-2)

/*
 * Functions for acquiring/releasing a shared buffer header's spinlock.  Do
 * not apply these to local buffers!
 */
extern uint32 LockBufHdr(BufferDesc *desc);

static inline void
UnlockBufHdr(BufferDesc *desc, uint32 buf_state)
{
	pg_write_barrier();
	pg_atomic_write_u32(&desc->state, buf_state & (~BM_LOCKED));
}

/* in bufmgr.c */

/*
 * Structure to sort buffers per file on checkpoints.
 *
 * This structure is allocated per buffer in shared memory, so it should be
 * kept as small as possible.
 */
typedef struct CkptSortItem
{
	Oid			tsId;
	RelFileNumber relNumber;
	ForkNumber	forkNum;
	BlockNumber blockNum;
	int			buf_id;
} CkptSortItem;

extern PGDLLIMPORT CkptSortItem *CkptBufferIds;

/*
 * Internal buffer management routines
 */
/* bufmgr.c */
extern void WritebackContextInit(WritebackContext *context, int *max_pending);
extern void IssuePendingWritebacks(WritebackContext *wb_context, IOContext io_context);
extern void ScheduleBufferTagForWriteback(WritebackContext *wb_context,
										  IOContext io_context, BufferTag *tag);

/* freelist.c */
extern IOContext IOContextForStrategy(BufferAccessStrategy strategy);
extern BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy,
									 uint32 *buf_state, bool *from_ring);
extern void StrategyFreeBuffer(BufferDesc *buf);
extern bool StrategyRejectBuffer(BufferAccessStrategy strategy,
								 BufferDesc *buf, bool from_ring);

extern int	StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc);
extern void StrategyNotifyBgWriter(int bgwprocno);

extern Size StrategyShmemSize(void);
extern void StrategyInitialize(bool init);
extern bool have_free_buffer(void);

/* buf_table.c */
extern Size BufTableShmemSize(int size);
extern void InitBufTable(int size);
extern uint32 BufTableHashCode(BufferTag *tagPtr);
extern int	BufTableLookup(BufferTag *tagPtr, uint32 hashcode);
extern int	BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id);
extern void BufTableDelete(BufferTag *tagPtr, uint32 hashcode);

/* localbuf.c */
extern bool PinLocalBuffer(BufferDesc *buf_hdr, bool adjust_usagecount);
extern void UnpinLocalBuffer(Buffer buffer);
extern PrefetchBufferResult PrefetchLocalBuffer(SMgrRelation smgr,
												ForkNumber forkNum,
												BlockNumber blockNum);
extern BufferDesc *LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum,
									BlockNumber blockNum, bool *foundPtr);
extern BlockNumber ExtendBufferedRelLocal(BufferManagerRelation bmr,
										  ForkNumber fork,
										  uint32 flags,
										  uint32 extend_by,
										  BlockNumber extend_upto,
										  Buffer *buffers,
										  uint32 *extended_by);
extern void MarkLocalBufferDirty(Buffer buffer);
extern void DropRelationLocalBuffers(RelFileLocator rlocator,
									 ForkNumber forkNum,
									 BlockNumber firstDelBlock);
extern void DropRelationAllLocalBuffers(RelFileLocator rlocator);
extern void AtEOXact_LocalBuffers(bool isCommit);

#endif							/* BUFMGR_INTERNALS_H */
