/*-------------------------------------------------------------------------
 *
 * test_cluster_buffer_cr.c
 *    Native invalidation and clock-victim handling for read-only versions.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_buffer_cr.c
 *
 * NOTES
 *    Compiles the production buffer owners. Shared allocation, locks and
 *    ownership sidecar access are explicit fixtures; mapping is native code.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"

#include <setjmp.h>
#include <stdlib.h>

#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_ic.h"
#include "cluster/storage/cluster_smgr.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "pgstat.h"
#include "catalog/pg_class.h"
#include "executor/instrument.h"
#include "storage/smgr.h"
#include "storage/buf_internals.h"
#include "storage/shmem.h"
#include "utils/memdebug.h"
#include "utils/resowner_private.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#include "test_cluster_buffer_mapping_fixture.h"

static ClusterKoDiscardDetailV1 discarded_detail;

void
cluster_ko_origin_discard_detail_v1(const ClusterKoDiscardDetailV1 *detail)
{
	if (detail != NULL)
		discarded_detail = *detail;
}

BufferDescPadded *BufferDescriptors;
LWLockPadded *MainLWLockArray;
bool cluster_shared_config = true;
bool cluster_shared_catalog = true;
bool cluster_enabled;
bool cluster_read_scache = true;
bool cluster_recmerge_window_active;
int cluster_pcm_grd_max_entries;
ClusterConf *ClusterConfShmem;
static ClusterConf pcm_conf;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static BufferDescPadded descriptors[64];
static LWLockPadded mapping_locks[BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS];
static uint64 owner_generation[64];
static uint32 owner_flags[64];
static uint64 owner_writer[64], owner_activation[64], owner_delivery[64];
static unsigned bumps;
static unsigned frees;
static unsigned wal_resets;
static unsigned lock_depth;
static unsigned lock_acquisitions;
static unsigned invalidations;
static int private_pin[64];
static PGIOAlignedBlock pages[64];
char *BufferBlocks = (char *)pages;
ResourceOwner CurrentResourceOwner;
BufferUsage pgBufferUsage;
BufferDesc *LocalBufferDescriptors;
int NLocBuffer = 0;
static unsigned read_io_calls;
static unsigned object_waits;
static bool close_at_mapping;
static bool collision_at_mapping;
static unsigned ko_samples, local_pins;
static BufferDesc local_descriptor;
static int32 local_refcounts[1];
int32 *LocalRefCount = local_refcounts;
static BufferTag waiting_tag;

bool
PinLocalBuffer(BufferDesc *buf, bool adjust pg_attribute_unused())
{
	UT_ASSERT(buf == &local_descriptor);
	local_pins++;
	return true;
}

static bool
StartBufferIO(BufferDesc *buf, bool for_input)
{
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT(for_input);
	UT_ASSERT(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)) > 0);
	UT_ASSERT((pg_atomic_read_u32(&buf->state) & BM_LOCKED) == 0);
	read_io_calls++;
	return true;
}

static bool
cluster_bufmgr_resource_x_wait_retry(LWLock *content pg_attribute_unused(),
									 int32 id pg_attribute_unused(),
									 uint32 index pg_attribute_unused(),
									 bool *refused pg_attribute_unused())
{
	abort();
}

static unsigned remembered_pins;
static unsigned pin_waiter_signals;
static bool copy_error;
static bool copy_observed_pin;
static void *copy_destination;
static unsigned reserve_calls;
static uint32 clock_hand;
static unsigned physical_writes;
static unsigned drain_mapping_race;
static bool drain_content_busy;
static bool drain_flush_deferred;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
static bool ko_closed;
static bool ko_cleaner_allowed;
static BufferTag ko_cleaner_target;
static uint64 ko_revision = 1;
static unsigned ko_close_at_header;
static unsigned header_calls;
static int storage_route = 1;
static RelFileLocator ko_locator;

static bool ko_retired;
static bool ko_origin_discard;
bool
cluster_ko_directory_drop_retired_v3(RelFileLocator locator)
{
	UT_ASSERT_EQ(lock_depth, 0);
	for (int i = 0; i < NBuffers; i++)
		UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(i)->state) & BM_LOCKED) == 0);
	return ko_retired && RelFileLocatorEquals(locator, ko_locator);
}
bool
cluster_ko_local_drop_discard_current_v4(RelFileLocator locator)
{
	return cluster_ko_directory_drop_retired_v3(locator)
		|| (ko_origin_discard && RelFileLocatorEquals(locator, ko_locator));
}
extern ClusterBufferDrainResult cluster_bufmgr_discard_relation_v4(RelFileLocator locator,
																 uint32 *budget, uint32 *cursor);

/* Waiting is an explicit fixture boundary. The real caller must arrive
 * here with neither a target reference nor any buffer/mapping lock retained. */
static void
cluster_bufmgr_ko_read_wait(void)
{
	int id = BufTableLookup(&waiting_tag, BufTableHashCode(&waiting_tag));
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(remembered_pins, 0);
	UT_ASSERT_EQ(read_io_calls, 0);
	for (int i = 0; i < NBuffers; i++)
		UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(i)->state) & BM_LOCKED) == 0);
	if (id >= 0)
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&GetBufferDescriptor(id)->state)),
					 0);
	if (++object_waits > 2)
		abort();
	ko_closed = false;
	ko_revision++;
}

int
cluster_smgr_which_for(RelFileLocator locator pg_attribute_unused(),
					   BackendId backend pg_attribute_unused())
{
	return storage_route;
}

bool
cluster_ko_peer_admission_begin_v3(RelFileLocator locator, ClusterKoPeerAdmissionV3 *out)
{
	ko_samples++;
	UT_ASSERT_EQ(lock_depth, 0);
	for (int i = 0; i < NBuffers; i++)
		UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(i)->state) & BM_LOCKED) == 0);
	memset(out, 0, sizeof(*out));
	if (ko_closed && RelFileLocatorEquals(locator, ko_locator))
		return false;
	out->locator = locator;
	out->revision = ko_revision;
	return true;
}

bool
cluster_ko_peer_admission_current_v3(const ClusterKoPeerAdmissionV3 *sample, RelFileLocator locator)
{
	bool header_locked = false;
	for (int i = 0; i < NBuffers; i++)
		header_locked |= (pg_atomic_read_u32(&GetBufferDescriptor(i)->state) & BM_LOCKED) != 0;
	UT_ASSERT(header_locked);
	return sample != NULL && sample->cleaner.serial == 0 && sample->revision != 0
		   && sample->revision == ko_revision && RelFileLocatorEquals(locator, sample->locator);
}

bool
cluster_ko_peer_read_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber block,
							  ClusterKoPeerAdmissionV3 *out)
{
	if (cluster_ko_peer_admission_begin_v3(locator, out))
		return true;
	if (!ko_cleaner_allowed
		|| !RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&ko_cleaner_target))
		|| fork != ko_cleaner_target.forkNum || block != ko_cleaner_target.blockNum)
		return false;
	out->locator = locator;
	out->revision = ko_revision;
	out->fork = fork;
	out->block = block;
	out->cleaner.serial = 1;
	return true;
}

bool
cluster_ko_peer_read_current_v3(const ClusterKoPeerAdmissionV3 *sample, RelFileLocator locator,
								ForkNumber fork, BlockNumber block)
{
	ClusterKoPeerAdmissionV3 ordinary = *sample;
	ordinary.cleaner.serial = 0;
	return cluster_ko_peer_admission_current_v3(&ordinary, locator)
		   && (sample->cleaner.serial == 0
			   || (ko_cleaner_allowed && sample->fork == fork && sample->block == block));
}

bool
cluster_ko_peer_current_valid_v3(const ClusterKoCurrentAdmissionV3 *sample, RelFileLocator locator,
								 ForkNumber fork, BlockNumber block)
{
	return sample != NULL && sample->target.fork == fork && sample->target.block == block
		   && cluster_ko_peer_read_current_v3(&sample->target, locator, fork, block);
}

WritebackContext BackendWritebackContext;

typedef struct PrivateRefCountEntry PrivateRefCountEntry;
static PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool do_move);
static PrivateRefCountEntry *NewPrivateRefCountEntry(Buffer buffer);
static void ForgetPrivateRefCountEntry(PrivateRefCountEntry *entry);
static void ReservePrivateRefCountEntry(void);
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy);
static void PinBuffer_Locked(BufferDesc *buf);
static inline bool cluster_bufmgr_should_pcm_track(BufferDesc *buf);
static void cluster_bufmgr_ko_remember_pin(BufferDesc *buf, const ClusterKoPeerAdmissionV3 *sample);
static uint32 WaitBufHdrUnlocked(BufferDesc *buf);
static Buffer GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context);
#define BufHdrGetBlock(buf) ((Block)(BufferBlocks + (Size)(buf)->buf_id * BLCKSZ))
#define BufferGetLSN(buf) PageGetLSN((Page)BufHdrGetBlock(buf))
#define BUF_WRITTEN 0x01
#define BUF_REUSABLE 0x02

static void InvalidateBufferCommitTailLocked(BufferDesc *, BufferTag *, uint32, LWLock *, uint32,
											 uint8, bool);
static void InvalidateBuffer(BufferDesc *buf);
static bool cluster_bufmgr_in_gcs_drop;
#define CLUSTER_INJECTION_POINT(name) ((void)0)

bool
LWLockHeldByMe(LWLock *lock pg_attribute_unused())
{
	return false;
}

void
ResourceOwnerEnlargeBuffers(ResourceOwner owner pg_attribute_unused())
{
	UT_ASSERT_EQ(lock_depth, 0);
}

void
ResourceOwnerRememberBuffer(ResourceOwner owner pg_attribute_unused(), Buffer buffer)
{
	private_pin[buffer - 1]++;
	remembered_pins++;
}

void
ResourceOwnerForgetBuffer(ResourceOwner owner pg_attribute_unused(), Buffer buffer)
{
	UT_ASSERT(private_pin[buffer - 1] > 0);
	private_pin[buffer - 1]--;
	remembered_pins--;
}

void
ProcSendSignal(int proc_number pg_attribute_unused())
{
	pin_waiter_signals++;
}

bool
LWLockHeldByMeInMode(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

#define cluster_pcm_own_flags_get(id) owner_flags[id]
#define cluster_pcm_own_writer_activation_token_get(id) owner_writer[id]
#define cluster_pcm_own_resource_x_activation_generation_get(id) owner_activation[id]
#define cluster_pcm_own_delivery_attempt_get(id) owner_delivery[id]

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	if (drain_mapping_race != 0 && mode == LW_EXCLUSIVE) {
		BufferDesc *buf = GetBufferDescriptor(2);
		if (drain_mapping_race == 1)
			pg_atomic_fetch_or_u32(&buf->state, BM_DIRTY | BM_JUST_DIRTIED);
		else if (drain_mapping_race == 2)
			pg_atomic_fetch_add_u32(&buf->state, BUF_REFCOUNT_ONE);
		else
			owner_flags[2] = PCM_OWN_FLAG_REVOKING;
		drain_mapping_race = 0;
	}
	if (++lock_acquisitions > 128)
		longjmp(error_jump, 1);
	if (collision_at_mapping && mode == LW_EXCLUSIVE) {
		BufferDesc *other = GetBufferDescriptor(0);
		other->tag = waiting_tag;
		pg_atomic_write_u32(&other->state, BM_TAG_VALID | BM_VALID);
		UT_ASSERT_EQ(BufTableInsert(&waiting_tag, BufTableHashCode(&waiting_tag), 0), -1);
		ko_closed = true;
		ko_revision++;
		collision_at_mapping = false;
	}
	if (close_at_mapping) {
		ko_closed = true;
		ko_revision++;
		close_at_mapping = false;
	}
	lock_depth++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	UT_ASSERT(lock_depth > 0);
	lock_depth--;
}

uint32
LockBufHdr(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	if (++header_calls == ko_close_at_header) {
		ko_closed = true;
		ko_revision++;
	}
	UT_ASSERT((state & BM_LOCKED) == 0);
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state | BM_LOCKED;
}

void
StrategyFreeBuffer(BufferDesc *buf)
{
	/* Native BufferAlloc also frees an unused collision victim while it
	 * still holds the new mapping lock; the descriptor header is unlocked. */
	UT_ASSERT((pg_atomic_read_u32(&buf->state) & BM_LOCKED) == 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 0);
	frees++;
}

#define BufferIsPinned(buffer)                                                                     \
	(!BufferIsValid(buffer)	 ? false                                                               \
	 : BufferIsLocal(buffer) ? LocalRefCount[-(buffer) - 1] > 0                                    \
							 : GetPrivateRefCount(buffer) > 0)

static int32
GetPrivateRefCount(Buffer buffer)
{
	return private_pin[buffer - 1];
}

static void
cluster_pcm_own_eviction_capture_locked(BufferDesc *buf, ClusterPcmOwnEvictionCapture *out)
{
	memset(out, 0, sizeof(*out));
	out->tag = buf->tag;
	out->generation = owner_generation[buf->buf_id];
	out->writer_activation_token = owner_writer[buf->buf_id];
	out->resource_x_activation_generation = owner_activation[buf->buf_id];
	out->flags = owner_flags[buf->buf_id];
	out->pcm_state = buf->pcm_state;
	out->buffer_type = buf->buffer_type;
}

static bool
cluster_pcm_own_fence_matches_locked(BufferDesc *buf, const ClusterPcmOwnSnapshot *before)
{
	return owner_generation[buf->buf_id] == before->generation
		   && owner_flags[buf->buf_id] == before->flags
		   && owner_writer[buf->buf_id] == before->writer_activation_token
		   && owner_activation[buf->buf_id] == before->resource_x_activation_generation
		   && buf->buffer_type == before->buffer_type
		   && buf->pcm_state == before->pcm_state && BufferTagsEqual(&buf->tag, &before->tag);
}

static ClusterPcmOwnResult
cluster_pcm_own_bump_locked(BufferDesc *buf, uint32 set pg_attribute_unused(),
							uint32 clear pg_attribute_unused(), uint64 *generation, uint32 *flags)
{
	UT_ASSERT(lock_depth > 0);
	UT_ASSERT(pg_atomic_read_u32(&buf->state) & BM_LOCKED);
	bumps++;
	*generation = ++owner_generation[buf->buf_id];
	*flags = owner_flags[buf->buf_id];
	return CLUSTER_PCM_OWN_OK;
}

static bool
cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked(BufferDesc *buf pg_attribute_unused(),
														 uint32 state pg_attribute_unused())
{
	return false;
}

ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 *generation)
{
	*generation = 1;
	return RESOURCE_X_WRITER_TARGET;
}

void
cluster_page_wal_reset_reuse_locked(BufferDesc *buf pg_attribute_unused())
{
	wal_resets++;
}

void
cluster_pcm_lock_release_saved_tag_for_eviction(BufferTag tag pg_attribute_unused(),
												PcmLockMode mode pg_attribute_unused())
{
	UT_ASSERT(false); /* All test descriptors own N, never a current S/X grant. */
}

static void
cluster_pcm_own_report_bump_failure(BufferDesc *buf pg_attribute_unused(),
									ClusterPcmOwnResult result pg_attribute_unused(),
									uint64 generation pg_attribute_unused(),
									uint32 flags pg_attribute_unused(),
									const char *site pg_attribute_unused())
{
	longjmp(error_jump, 1);
}

static void
cluster_bufmgr_resource_x_writer_report_failure(ResourceXApplyResult result pg_attribute_unused(),
												BufferDesc *buf pg_attribute_unused(),
												const char *site pg_attribute_unused())
{
	abort();
}

static bool
cluster_bufmgr_resource_x_target_evict_locked(
	BufferDesc *buf pg_attribute_unused(), BufferTag *tag pg_attribute_unused(),
	uint32 hash pg_attribute_unused(), LWLock *lock pg_attribute_unused(),
	uint32 state pg_attribute_unused(),
	const ClusterPcmOwnEvictionCapture *capture pg_attribute_unused(),
	uint64 generation pg_attribute_unused(), uint32 pins pg_attribute_unused(),
	bool release pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	longjmp(error_jump, 1);
}

#include "test_cluster_buffer_cr_pins.inc"

#define REFCOUNT_ARRAY_ENTRIES 8
static PrivateRefCountEntry PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES];
static PrivateRefCountEntry private_hash[64];
static bool private_hash_used[64];
static HTAB *PrivateRefCountHash = (HTAB *)private_hash;
static PrivateRefCountEntry *ReservedRefCountEntry;
static int32 PrivateRefCountOverflowed;
static uint32 PrivateRefCountClock;

/* Storage only: refcount spill, restore, reservation and retirement execute
 * the original private-reference implementation below. */
void *
hash_search(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	Buffer buffer = *(const Buffer *)key;
	unsigned i = buffer - 1;
	UT_ASSERT(table == PrivateRefCountHash && buffer > 0 && i < lengthof(private_hash));
	if (found != NULL)
		*found = private_hash_used[i];
	if (!private_hash_used[i] && action != HASH_ENTER)
		return NULL;
	if (!private_hash_used[i]) {
		memset(&private_hash[i], 0, sizeof(private_hash[i]));
		private_hash[i].buffer = buffer;
	}
	private_hash_used[i] = action != HASH_REMOVE;
	return &private_hash[i];
}

#include "test_cluster_buffer_cr_refs.inc"

static uint32
WaitBufHdrUnlocked(BufferDesc *buf pg_attribute_unused())
{
	abort();
}

/* Execute the native clock loop with only its shared hand and optional
 * strategy-ring storage replaced. No replacement victim-selection model. */
#define ClockSweepTick() (clock_hand++ % NBuffers)
#define AddBufferToRing(strategy, buf) ((void)0)
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	uint32 local_buf_state;
	int trycounter;

	UT_ASSERT_EQ(lock_depth, 0);
	reserve_calls++;
	*from_ring = false;
#include "test_cluster_pcm_clock_sweep.inc"
}
#undef ClockSweepTick
#undef AddBufferToRing

void
CheckBufferIsPinnedOnce(Buffer buffer)
{
	UT_ASSERT_EQ(private_pin[buffer - 1], 1);
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT(mode == BUFFER_LOCK_UNLOCK);
	LWLockRelease(BufferDescriptorGetContentLock(GetBufferDescriptor(buffer - 1)));
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return !drain_content_busy && LWLockAcquire(lock, mode);
}

bool
XLogNeedsFlush(XLogRecPtr lsn pg_attribute_unused())
{
	return false;
}

bool
StrategyRejectBuffer(BufferAccessStrategy strategy pg_attribute_unused(),
					 BufferDesc *buf pg_attribute_unused(), bool from_ring pg_attribute_unused())
{
	return false;
}

static void
FlushBuffer(BufferDesc *buf, SMgrRelation reln pg_attribute_unused(),
			IOObject object pg_attribute_unused(), IOContext context pg_attribute_unused())
{
	UT_ASSERT_NE(buf->buffer_type, BUF_TYPE_CR);
	if (drain_flush_deferred)
		return;
	physical_writes++;
	pg_atomic_fetch_and_u32(&buf->state, ~(BM_DIRTY | BM_JUST_DIRTIED));
}

static void
FlushBufferWithAttempt(BufferDesc *buf, SMgrRelation reln, IOObject object, IOContext context,
					   volatile bool *attempted)
{
	unsigned before = physical_writes;

	FlushBuffer(buf, reln, object, context);
	*attempted = physical_writes != before;
}

void
ScheduleBufferTagForWriteback(WritebackContext *wb pg_attribute_unused(),
							  IOContext context pg_attribute_unused(),
							  BufferTag *tag pg_attribute_unused())
{
	UT_ASSERT_EQ(lock_depth, 0);
}

void
pgstat_count_io_op(IOObject object pg_attribute_unused(), IOContext context pg_attribute_unused(),
				   IOOp op pg_attribute_unused())
{}

static void *
copy_bytes(void *dst, const void *src, size_t size)
{
	if (dst == copy_destination) {
		UT_ASSERT_EQ(lock_depth, 0);
		UT_ASSERT(remembered_pins > 0);
		copy_observed_pin = true;
		if (copy_error)
			pg_re_throw();
	}
	return memcpy(dst, src, size);
}

#define memcpy copy_bytes
#include "test_cluster_buffer_cr_owner.inc"
#undef memcpy

/* The relation AEL guarantees that no new version can enter while DROP scans.
 * The fixture preserves the production header-to-mapping lock order. */
static void
InvalidateBuffer(BufferDesc *buf)
{
	BufferTag tag = buf->tag;
	uint32 hash = BufTableHashCode(&tag);
	LWLock *lock = BufMappingPartitionLock(hash);
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT(++invalidations <= 64);
	if (invalidations > 64)
		longjmp(error_jump, 1);
	UnlockBufHdr(buf, state);
	LWLockAcquire(lock, LW_EXCLUSIVE);
	state = LockBufHdr(buf);
	UT_ASSERT(InvalidateBufferCommitLocked(buf, &tag, hash, lock, state));
}

static BufferTag
reset_buffers(void)
{
	BufferTag tag = reset_mapping();
	int i;
	BufferDescriptors = descriptors;
	MainLWLockArray = mapping_locks;
	memset(descriptors, 0, sizeof(descriptors));
	memset(owner_flags, 0, sizeof(owner_flags));
	memset(owner_writer, 0, sizeof(owner_writer));
	memset(owner_activation, 0, sizeof(owner_activation));
	memset(owner_delivery, 0, sizeof(owner_delivery));
	memset(private_pin, 0, sizeof(private_pin));
	memset(PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));
	memset(private_hash, 0, sizeof(private_hash));
	memset(private_hash_used, 0, sizeof(private_hash_used));
	ReservedRefCountEntry = NULL;
	PrivateRefCountOverflowed = PrivateRefCountClock = 0;
	memset(local_refcounts, 0, sizeof(local_refcounts));
	memset(pages, 0, sizeof(pages));
	remembered_pins = pin_waiter_signals = reserve_calls = 0;
	physical_writes = drain_mapping_race = 0;
	drain_content_busy = drain_flush_deferred = false;
	clock_hand = 6;
	cluster_enabled = false;
	ClusterConfShmem = NULL;
	cluster_pcm_grd_max_entries = 0;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	storage_route = 1;
	ko_cleaner_allowed = false;
	ko_revision = 1;
	ko_closed = ko_retired = ko_origin_discard = false;
	ko_close_at_header = header_calls = 0;
	read_io_calls = object_waits = 0;
	close_at_mapping = collision_at_mapping = false;
	ko_samples = local_pins = 0;
	NLocBuffer = 0;
	memset(&local_descriptor, 0, sizeof(local_descriptor));
	LocalBufferDescriptors = &local_descriptor;
	ko_locator = BufTagGetRelFileLocator(&tag);
	copy_error = copy_observed_pin = false;
	copy_destination = NULL;
	PG_exception_stack = NULL;
	bumps = frees = wal_resets = lock_depth = invalidations = 0;
	lock_acquisitions = 0;
	for (i = 0; i < NBuffers; i++) {
		GetBufferDescriptor(i)->buf_id = i;
		owner_generation[i] = 1;
	}
	return tag;
}

static void
install_current(BufferTag *tag, int id)
{
	BufferDesc *buf = GetBufferDescriptor(id);
	buf->tag = *tag;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_VALID);
	UT_ASSERT_EQ(BufTableInsert(tag, BufTableHashCode(tag), id), -1);
}

static void
install_cr(BufferTag *tag, int id)
{
	BufferDesc *buf = GetBufferDescriptor(id);
	int head = -1;
	uint64 generation = 0;
	UT_ASSERT(BufTableCRInsert(tag, BufTableHashCode(tag), id, &head, &generation));
	buf->tag = *tag;
	buf->buffer_type = BUF_TYPE_CR;
	buf->cr.prev_id = -1;
	buf->cr.next_id = head;
	buf->cr.read_scn = 100;
	buf->cr.read_epoch = 1;
	buf->cr.snapshot_identity = 200;
	buf->cr.scan_identity = 300 + id;
	buf->cr_anchor_generation = generation;
	if (head >= 0)
		GetBufferDescriptor(head)->cr.prev_id = id;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_VALID);
}

static bool
evict(int id, unsigned pins)
{
	BufferDesc *buf = GetBufferDescriptor(id);
	uint32 state = pg_atomic_read_u32(&buf->state);
	private_pin[id] = 1;
	pg_atomic_write_u32(&buf->state, (state & ~BUF_REFCOUNT_MASK) + pins * BUF_REFCOUNT_ONE);
	expect_error = true;
	if (setjmp(error_jump))
		return false;
	return InvalidateVictimBuffer(buf);
}

UT_TEST(test_native_current_victim_keeps_original_behavior)
{
	BufferTag tag = reset_buffers();
	install_current(&tag, 2);
	UT_ASSERT(evict(2, 1));
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), -1);
	UT_ASSERT_EQ(bumps, 1);
	UT_ASSERT_EQ(frees, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_cr_victim_preserves_current_and_removes_middle_head_tail)
{
	BufferTag tag = reset_buffers();
	uint32 hash = BufTableHashCode(&tag);
	int head;
	uint64 generation;
	install_current(&tag, 0);
	install_cr(&tag, 1);
	install_cr(&tag, 2);
	install_cr(&tag, 3);
	UT_ASSERT(evict(2, 1));
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), 0);
	UT_ASSERT_EQ(GetBufferDescriptor(3)->cr.next_id, 1);
	UT_ASSERT_EQ(GetBufferDescriptor(1)->cr.prev_id, 3);
	UT_ASSERT(evict(3, 1));
	UT_ASSERT(BufTableCRLookup(&tag, hash, &head, &generation));
	UT_ASSERT_EQ(head, 1);
	UT_ASSERT_EQ(GetBufferDescriptor(1)->cr.prev_id, -1);
	UT_ASSERT(evict(1, 1));
	UT_ASSERT(!BufTableCRLookup(&tag, hash, &head, &generation));
	UT_ASSERT_EQ(BufTableLookup(&tag, hash), 0);
	UT_ASSERT_EQ(bumps, 3);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_last_cr_only_victim_removes_anchor_without_current)
{
	BufferTag tag = reset_buffers();
	int head;
	uint64 generation;
	install_cr(&tag, 4);
	UT_ASSERT(evict(4, 1));
	UT_ASSERT(!BufTableCRLookup(&tag, BufTableHashCode(&tag), &head, &generation));
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&GetBufferDescriptor(4)->state)), 1);
	UT_ASSERT_EQ(bumps, 1);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_foreign_pin_keeps_cr_mapped_without_bump)
{
	BufferTag tag = reset_buffers();
	install_current(&tag, 0);
	install_cr(&tag, 1);
	UT_ASSERT(!evict(1, 2));
	UT_ASSERT_EQ(bumps, 0);
	UT_ASSERT_EQ(GetBufferDescriptor(1)->buffer_type, BUF_TYPE_CR);
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_cr_ownership_reservation_refuses_reuse_without_mutation)
{
	BufferTag tag = reset_buffers();
	BufferCrMetadata saved;
	install_current(&tag, 0);
	install_cr(&tag, 1);
	saved = GetBufferDescriptor(1)->cr;
	owner_flags[1] = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!evict(1, 1));
	UT_ASSERT_EQ(bumps, 0);
	UT_ASSERT_EQ(memcmp(&saved, &GetBufferDescriptor(1)->cr, sizeof(saved)), 0);
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_broken_cr_chain_is_rejected_before_ownership_or_mapping_mutation)
{
	int kind;
	for (kind = 0; kind < 5; kind++) {
		BufferTag tag = reset_buffers();
		BufferDesc *other;
		install_current(&tag, 0);
		install_cr(&tag, 1);
		install_cr(&tag, 2);
		other = GetBufferDescriptor(2);
		switch (kind) {
		case 0:
			other->cr_anchor_generation++;
			break;
		case 1:
			other->cr.next_id = NBuffers;
			break;
		case 2:
			other->tag.blockNum++;
			break;
		case 3:
			GetBufferDescriptor(1)->cr.prev_id = -1;
			break;
		case 4:
			GetBufferDescriptor(1)->cr.next_id = 2;
			break;
		}
		UT_ASSERT(!evict(1, 1));
		UT_ASSERT_EQ(bumps, 0);
		UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 0);
		UT_ASSERT_EQ(GetBufferDescriptor(1)->buffer_type, BUF_TYPE_CR);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_cr_reuse_clears_overlay_and_preserves_lwlock)
{
	BufferTag tag = reset_buffers();
	BufferDesc *buf = GetBufferDescriptor(1);
	unsigned char lock_bytes[sizeof(LWLock)];
	install_current(&tag, 0);
	install_cr(&tag, 1);
	memset(&buf->pcm_lock, 0xa5, sizeof(LWLock));
	memcpy(lock_bytes, &buf->pcm_lock, sizeof(LWLock));
	UT_ASSERT(evict(1, 1));
	UT_ASSERT_EQ(buf->buffer_type, BUF_TYPE_CURRENT);
	UT_ASSERT_EQ(buf->pi_buf_id, INVALID_BUFFER_ID);
	UT_ASSERT_EQ(buf->pi_lsn, InvalidXLogRecPtr);
	UT_ASSERT_EQ(buf->pi_created_at, 0);
	UT_ASSERT_EQ(buf->cr_chain_head, INVALID_BUFFER_ID);
	UT_ASSERT_EQ(buf->cr_chain_next, INVALID_BUFFER_ID);
	UT_ASSERT_EQ(buf->cf_request_count, 0);
	UT_ASSERT_EQ(memcmp(lock_bytes, &buf->pcm_lock, sizeof(LWLock)), 0);
}

UT_TEST(test_fast_truncate_removes_all_versions_including_cr_only_anchors)
{
	BufferTag tag = reset_buffers();
	RelFileLocator locator = { tag.spcOid, tag.dbOid, tag.relNumber };
	BufferTag lower = tag;
	BufferTag higher = tag;
	int head;
	uint64 generation;
	lower.blockNum--;
	higher.blockNum++;
	install_cr(&lower, 1);
	install_current(&tag, 2);
	install_cr(&tag, 3);
	install_cr(&tag, 4);
	install_cr(&higher, 5);
	expect_error = true;
	if (setjmp(error_jump)) {
		UT_ASSERT(false);
		return;
	}
	FindAndDropRelationBuffers(locator, MAIN_FORKNUM, higher.blockNum + 1, tag.blockNum);
	UT_ASSERT_EQ(frees, 4);
	UT_ASSERT_EQ(bumps, 4);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT(!BufTableCRLookup(&tag, BufTableHashCode(&tag), &head, &generation));
	UT_ASSERT(!BufTableCRLookup(&higher, BufTableHashCode(&higher), &head, &generation));
	UT_ASSERT(BufTableCRLookup(&lower, BufTableHashCode(&lower), &head, &generation));
	UT_ASSERT_EQ(head, 1);
}

UT_TEST(test_fast_drop_rejects_a_still_mapped_wrong_tag_without_spinning)
{
	BufferTag tag = reset_buffers();
	RelFileLocator locator = { tag.spcOid, tag.dbOid, tag.relNumber };

	install_cr(&tag, 1);
	GetBufferDescriptor(1)->tag.blockNum++;
	expect_error = true;
	if (setjmp(error_jump) == 0) {
		FindAndDropRelationBuffers(locator, MAIN_FORKNUM, tag.blockNum + 1, tag.blockNum);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(lock_acquisitions, 1);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(bumps, 0);
	UT_ASSERT_EQ(frees, 0);
}

UT_TEST(test_cr_has_no_mutation_authority_even_without_pcm)
{
	BufferTag tag = reset_buffers();
	BufferDesc *current = GetBufferDescriptor(0);
	BufferDesc *cr = GetBufferDescriptor(1);

	install_current(&tag, 0);
	install_cr(&tag, 1);
	cluster_enabled = false;
	UT_ASSERT(cluster_bufmgr_pcm_x_content_write_permitted(current));
	UT_ASSERT(cluster_bufmgr_pcm_x_content_holder_write_permitted(current));
	UT_ASSERT(cluster_bufmgr_pcm_x_ordinary_content_write_permitted(current));
	UT_ASSERT(cluster_bufmgr_block_write_permitted(1));
	UT_ASSERT(!cluster_bufmgr_pcm_x_content_write_permitted(cr));
	UT_ASSERT(!cluster_bufmgr_pcm_x_content_holder_write_permitted(cr));
	UT_ASSERT(!cluster_bufmgr_pcm_x_ordinary_content_write_permitted(cr));
	UT_ASSERT(!cluster_bufmgr_block_write_permitted(2));
}

static BufferCrKey
scoped_key(BufferTag tag)
{
	BufferCrKey key = { 0 };
	key.tag = tag;
	key.scan_identity = 81;
	key.snapshot_identity = 82;
	key.read_scn = 100;
	key.read_epoch = 1;
	return key;
}

UT_TEST(test_native_reservation_publishes_clean_cr_and_copies_with_original_pin)
{
	BufferTag tag = reset_buffers();
	BufferCrKey key = scoped_key(tag);
	PGIOAlignedBlock page;
	PGIOAlignedBlock output;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	BufferDesc *buf;

	UT_ASSERT(buffer > 0);
	if (buffer <= 0)
		return;
	buf = GetBufferDescriptor(buffer - 1);
	install_current(&tag, 0);
	memset(page.data, 0x4a, BLCKSZ);
	UT_ASSERT_EQ(reserve_calls, 1);
	UT_ASSERT_EQ(remembered_pins, 1);
	UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UT_ASSERT(
		BufferCrMatches(buf, pg_atomic_read_u32(&buf->state), &key, buf->cr_anchor_generation));
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 0);
	UnpinBuffer(buf);
	copy_destination = output.data;
	UT_ASSERT(cluster_bufmgr_cr_copy_v1(&key, output.data));
	UT_ASSERT(copy_observed_pin);
	UT_ASSERT_EQ(memcmp(page.data, output.data, BLCKSZ), 0);
	UT_ASSERT_EQ(remembered_pins, 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 0);
}

UT_TEST(test_duplicate_publish_preserves_first_image_and_losing_reservation)
{
	BufferCrKey key = scoped_key(reset_buffers());
	PGIOAlignedBlock first, second, output;
	Buffer a = cluster_bufmgr_cr_reserve_v1();
	Buffer b;
	int head;
	uint64 generation;

	UT_ASSERT(a > 0);
	if (a <= 0)
		return;
	memset(first.data, 0x34, BLCKSZ);
	memset(second.data, 0x56, BLCKSZ);
	UT_ASSERT(cluster_bufmgr_cr_publish_v1(a, &key, first.data));
	b = cluster_bufmgr_cr_reserve_v1();
	UT_ASSERT(!cluster_bufmgr_cr_publish_v1(b, &key, second.data));
	UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(b - 1)->state) & BM_TAG_VALID) == 0);
	UT_ASSERT(BufTableCRLookup(&key.tag, BufTableHashCode(&key.tag), &head, &generation));
	UT_ASSERT_EQ(head, a - 1);
	UT_ASSERT_EQ(GetBufferDescriptor(a - 1)->cr.next_id, -1);
	UnpinBuffer(GetBufferDescriptor(b - 1));
	UnpinBuffer(GetBufferDescriptor(a - 1));
	UT_ASSERT(cluster_bufmgr_cr_copy_v1(&key, output.data));
	UT_ASSERT_EQ(memcmp(first.data, output.data, BLCKSZ), 0);
	UT_ASSERT_EQ(remembered_pins, 0);
}

UT_TEST(test_wrong_scope_snapshot_scn_epoch_or_tag_misses_without_touching_output)
{
	BufferCrKey key = scoped_key(reset_buffers());
	PGIOAlignedBlock page, output, sentinel;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	int variant;

	UT_ASSERT(buffer > 0);
	if (buffer <= 0)
		return;
	memset(page.data, 0x37, BLCKSZ);
	memset(sentinel.data, 0xa1, BLCKSZ);
	UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UnpinBuffer(GetBufferDescriptor(buffer - 1));
	for (variant = 0; variant < 7; variant++) {
		BufferCrKey changed = key;
		switch (variant) {
		case 0:
			changed.scan_identity++;
			break;
		case 1:
			changed.snapshot_identity++;
			break;
		case 2:
			changed.read_scn++;
			break;
		case 3:
			changed.read_epoch++;
			break;
		case 4:
			changed.tag.relNumber++;
			break;
		case 5:
			changed.tag.blockNum++;
			break;
		case 6:
			changed.reserved_zero = 1;
			break;
		}
		memcpy(output.data, sentinel.data, BLCKSZ);
		UT_ASSERT(!cluster_bufmgr_cr_copy_v1(&changed, output.data));
		UT_ASSERT_EQ(memcmp(output.data, sentinel.data, BLCKSZ), 0);
		UT_ASSERT_EQ(remembered_pins, 0);
	}
}

UT_TEST(test_publish_refuses_mapped_or_multiply_pinned_or_reserved_descriptor)
{
	BufferTag tag = reset_buffers();
	BufferCrKey key = scoped_key(tag);
	PGIOAlignedBlock page;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	BufferDesc *buf;

	UT_ASSERT(buffer > 0);
	if (buffer <= 0)
		return;
	buf = GetBufferDescriptor(buffer - 1);
	memset(page.data, 0x77, BLCKSZ);
	install_current(&tag, 0);
	(void)PinBuffer(GetBufferDescriptor(0), NULL);
	UT_ASSERT(!cluster_bufmgr_cr_publish_v1(1, &key, page.data));
	UnpinBuffer(GetBufferDescriptor(0));
	(void)PinBuffer(buf, NULL);
	UT_ASSERT(!cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UnpinBuffer(buf);
	owner_flags[buffer - 1] = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	owner_flags[buffer - 1] = 0;
	UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UnpinBuffer(buf);
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 0);
	UT_ASSERT_EQ(remembered_pins, 0);
}

UT_TEST(test_hash_capacity_refusal_leaves_reservation_unmapped_and_releasable)
{
	BufferCrKey key = scoped_key(reset_buffers());
	PGIOAlignedBlock page;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	BufferDesc *buf;

	UT_ASSERT(buffer > 0);
	if (buffer <= 0)
		return;
	buf = GetBufferDescriptor(buffer - 1);
	memset(page.data, 0, BLCKSZ);
	deny_new_entry = true;
	UT_ASSERT(!cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UT_ASSERT_EQ(buf->buffer_type, BUF_TYPE_CURRENT);
	UT_ASSERT((pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) == 0);
	UnpinBuffer(buf);
	UT_ASSERT_EQ(remembered_pins, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_copy_error_releases_original_pin_and_keeps_immutable_entry)
{
	BufferCrKey key = scoped_key(reset_buffers());
	PGIOAlignedBlock page, output;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	BufferDesc *buf;

	UT_ASSERT(buffer > 0);
	if (buffer <= 0)
		return;
	buf = GetBufferDescriptor(buffer - 1);
	memset(page.data, 0x36, BLCKSZ);
	UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UnpinBuffer(buf);
	copy_destination = output.data;
	copy_error = true;
	if (setjmp(error_jump) == 0) {
		(void)cluster_bufmgr_cr_copy_v1(&key, output.data);
		UT_ASSERT(false);
	}
	UT_ASSERT(copy_observed_pin);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(remembered_pins, 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 0);
	copy_error = false;
	UT_ASSERT(cluster_bufmgr_cr_copy_v1(&key, output.data));
	UT_ASSERT_EQ(memcmp(page.data, output.data, BLCKSZ), 0);
}

UT_TEST(test_copy_unpin_notifies_original_pin_count_waiter)
{
	BufferCrKey key = scoped_key(reset_buffers());
	PGIOAlignedBlock page, output;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	BufferDesc *buf;

	UT_ASSERT(buffer > 0);
	if (buffer <= 0)
		return;
	buf = GetBufferDescriptor(buffer - 1);
	memset(page.data, 0x2b, BLCKSZ);
	UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
	UnpinBuffer(buf);
	pg_atomic_fetch_add_u32(&buf->state, BUF_REFCOUNT_ONE);
	pg_atomic_fetch_or_u32(&buf->state, BM_PIN_COUNT_WAITER);
	UT_ASSERT(cluster_bufmgr_cr_copy_v1(&key, output.data));
	UT_ASSERT_EQ(pin_waiter_signals, 1);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 1);
	UT_ASSERT_EQ(remembered_pins, 0);
}

UT_TEST(test_native_clock_recycles_clean_cr_and_keeps_current)
{
	BufferTag tag = reset_buffers();
	Buffer victim;
	int head;
	uint64 generation;

	install_current(&tag, 0);
	install_cr(&tag, 6);
	victim = cluster_bufmgr_cr_reserve_v1();
	UT_ASSERT_EQ(victim, 7);
	UT_ASSERT_EQ(bumps, 1);
	UT_ASSERT_EQ(physical_writes, 0);
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 0);
	UT_ASSERT(!BufTableCRLookup(&tag, BufTableHashCode(&tag), &head, &generation));
	UT_ASSERT_EQ(GetBufferDescriptor(6)->buffer_type, BUF_TYPE_CURRENT);
	UnpinBuffer(GetBufferDescriptor(6));
}

UT_TEST(test_native_clock_skips_foreign_pinned_cr)
{
	BufferTag tag = reset_buffers();
	Buffer victim;

	install_current(&tag, 0);
	install_cr(&tag, 6);
	pg_atomic_fetch_add_u32(&GetBufferDescriptor(6)->state, BUF_REFCOUNT_ONE);
	victim = cluster_bufmgr_cr_reserve_v1();
	UT_ASSERT_EQ(victim, 8);
	UT_ASSERT_EQ(bumps, 0);
	UT_ASSERT_EQ(physical_writes, 0);
	UT_ASSERT_EQ(GetBufferDescriptor(6)->buffer_type, BUF_TYPE_CR);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&GetBufferDescriptor(6)->state)), 1);
	UnpinBuffer(GetBufferDescriptor(7));
}

UT_TEST(test_native_clock_refuses_dirty_cr_before_any_io_or_pin)
{
	BufferTag tag = reset_buffers();

	install_cr(&tag, 6);
	pg_atomic_fetch_or_u32(&GetBufferDescriptor(6)->state, BM_DIRTY);
	expect_error = true;
	if (setjmp(error_jump) == 0) {
		(void)cluster_bufmgr_cr_reserve_v1();
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(bumps + physical_writes + remembered_pins + lock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(6)->state) & BM_LOCKED) == 0);
	UT_ASSERT_EQ(GetBufferDescriptor(6)->buffer_type, BUF_TYPE_CR);
}

UT_TEST(test_checkpoint_skips_clean_cr_and_refuses_dirty_cr)
{
	BufferTag tag = reset_buffers();
	BufferDesc *buf = GetBufferDescriptor(6);

	install_cr(&tag, 6);
	UT_ASSERT_EQ(SyncOneBuffer(6, false, &BackendWritebackContext), BUF_REUSABLE);
	UT_ASSERT_EQ(physical_writes + remembered_pins, 0);
	pg_atomic_fetch_or_u32(&buf->state, BM_CHECKPOINT_NEEDED);
	expect_error = true;
	if (setjmp(error_jump) == 0) {
		(void)SyncOneBuffer(6, false, &BackendWritebackContext);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(physical_writes + remembered_pins + lock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&buf->state) & BM_LOCKED) == 0);
}

UT_TEST(test_publish_copy_error_leaves_only_the_original_reservation_pin)
{
	BufferCrKey key = scoped_key(reset_buffers());
	PGIOAlignedBlock page;
	Buffer buffer = cluster_bufmgr_cr_reserve_v1();
	BufferDesc *buf = GetBufferDescriptor(buffer - 1);
	int head;
	uint64 generation;

	memset(page.data, 0x68, BLCKSZ);
	copy_destination = BufHdrGetBlock(buf);
	copy_error = true;
	if (setjmp(error_jump) == 0) {
		(void)cluster_bufmgr_cr_publish_v1(buffer, &key, page.data);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(remembered_pins, 1);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT((pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) == 0);
	UT_ASSERT(!BufTableCRLookup(&key.tag, BufTableHashCode(&key.tag), &head, &generation));
	/* The original caller/ResourceOwner still owns and releases the pin. */
	UnpinBuffer(buf);
	UT_ASSERT_EQ(remembered_pins, 0);
}

/* KO registry and lock scheduling are fixture boundaries. These cases
 * execute the actual CR publication/copy bodies and native pin accounting. */
UT_TEST(test_object_fence_refuses_cr_publication_before_and_at_header)
{
	for (unsigned race = 0; race < 2; race++) {
		BufferCrKey key = scoped_key(reset_buffers());
		PGIOAlignedBlock page;
		Buffer buffer = cluster_bufmgr_cr_reserve_v1();
		BufferDesc *buf = GetBufferDescriptor(buffer - 1);
		int head;
		uint64 generation;
		memset(page.data, 0x4d, BLCKSZ);
		cluster_enabled = true;
		ko_closed = race == 0;
		header_calls = 0;
		ko_close_at_header = race == 1 ? 2 : 0;
		UT_ASSERT(!cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
		UT_ASSERT(!BufTableCRLookup(&key.tag, BufTableHashCode(&key.tag), &head, &generation));
		UT_ASSERT((pg_atomic_read_u32(&buf->state) & (BM_TAG_VALID | BM_VALID)) == 0);
		UT_ASSERT_EQ(remembered_pins, 1);
		UT_ASSERT_EQ(lock_depth, 0);
		UnpinBuffer(buf);
	}
}

UT_TEST(test_object_fence_refuses_new_cr_pin_without_touching_output)
{
	for (unsigned race = 0; race < 2; race++) {
		BufferCrKey key = scoped_key(reset_buffers());
		PGIOAlignedBlock page, output;
		Buffer buffer = cluster_bufmgr_cr_reserve_v1();
		BufferDesc *buf = GetBufferDescriptor(buffer - 1);
		bool untouched = true;
		memset(page.data, 0x4d, BLCKSZ);
		memset(output.data, 0x71, BLCKSZ);
		UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
		UnpinBuffer(buf);
		cluster_enabled = true;
		ko_closed = race == 0;
		header_calls = 0;
		ko_close_at_header = race == 1 ? 1 : 0;
		UT_ASSERT(!cluster_bufmgr_cr_copy_v1(&key, output.data));
		for (unsigned i = 0; i < BLCKSZ; i++)
			untouched &= (unsigned char)output.data[i] == 0x71;
		UT_ASSERT(untouched);
		UT_ASSERT_EQ(remembered_pins, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 0);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_object_fence_preserves_finite_cr_pin_and_unrelated_storage)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		BufferCrKey key = scoped_key(reset_buffers());
		PGIOAlignedBlock page, output;
		Buffer buffer = cluster_bufmgr_cr_reserve_v1();
		BufferDesc *buf = GetBufferDescriptor(buffer - 1);
		memset(page.data, 0x4d, BLCKSZ);
		UT_ASSERT(cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
		if (kind != 0)
			UnpinBuffer(buf);
		cluster_enabled = true;
		ko_closed = true;
		if (kind == 1)
			ko_locator.relNumber++;
		if (kind == 2)
			storage_route = 0;
		if (kind == 3)
			cluster_enabled = false;
		UT_ASSERT(cluster_bufmgr_cr_copy_v1(&key, output.data));
		UT_ASSERT_EQ(memcmp(page.data, output.data, BLCKSZ), 0);
		UT_ASSERT_EQ(remembered_pins, kind == 0 ? 1 : 0);
		if (kind == 0)
			UnpinBuffer(buf);
	}
}

UT_TEST(test_object_fence_closes_recent_buffer_before_first_pin)
{
	for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		for (unsigned race = 0; race < 2; race++) {
			BufferTag tag = reset_buffers();
			tag.forkNum = fork;
			install_current(&tag, 0);
			cluster_enabled = true;
			ko_closed = race == 0;
			header_calls = 0;
			ko_close_at_header = race == 1 ? 1 : 0;
			UT_ASSERT(!ReadRecentBuffer(BufTagGetRelFileLocator(&tag), fork, tag.blockNum, 1));
			UT_ASSERT_EQ(remembered_pins, 0);
			UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&GetBufferDescriptor(0)->state)),
						 0);
		}
	}
}

UT_TEST(test_object_fence_preserves_recent_existing_pin_and_private_route)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		BufferTag tag = reset_buffers();
		BufferDesc *buf = GetBufferDescriptor(0);
		install_current(&tag, 0);
		if (kind == 0)
			(void)PinBuffer(buf, NULL);
		cluster_enabled = kind != 2;
		storage_route = kind == 1 ? 0 : 1;
		ko_closed = true;
		UT_ASSERT(ReadRecentBuffer(BufTagGetRelFileLocator(&tag), tag.forkNum, tag.blockNum, 1));
		UT_ASSERT_EQ(remembered_pins, kind == 0 ? 2 : 1);
		UnpinBuffer(buf);
		if (kind == 0)
			UnpinBuffer(buf);
	}
}

UT_TEST(test_object_fence_buffer_alloc_waits_off_locks_without_publication)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		bool hit = kind == 1 || kind == 2;
		BufferTag tag = reset_buffers();
		SMgrRelationData smgr;
		BufferDesc *buf;
		bool found;
		memset(&smgr, 0, sizeof(smgr));
		smgr.smgr_rlocator.locator = BufTagGetRelFileLocator(&tag);
		smgr.smgr_rlocator.backend = InvalidBackendId;
		if (kind == 1)
			install_current(&tag, 0);
		cluster_enabled = true;
		waiting_tag = tag;
		close_at_mapping = kind < 2;
		collision_at_mapping = kind == 2;
		ko_closed = kind == 3;
		buf = BufferAlloc(&smgr, RELPERSISTENCE_PERMANENT, tag.forkNum, tag.blockNum, NULL, &found,
						  IOCONTEXT_NORMAL);
		UT_ASSERT_EQ(object_waits, 1);
		UT_ASSERT_EQ(read_io_calls, hit ? 0 : 1);
		UT_ASSERT_EQ(found, hit != 0);
		UT_ASSERT_EQ(remembered_pins, 1);
		UT_ASSERT(BufferTagsEqual(&buf->tag, &tag));
		UnpinBuffer(buf);
	}
}

UT_TEST(test_object_fence_does_not_enter_local_buffer_path)
{
	BufferTag tag = reset_buffers();
	local_descriptor.tag = tag;
	pg_atomic_write_u32(&local_descriptor.state, BM_TAG_VALID | BM_VALID);
	NLocBuffer = 1;
	cluster_enabled = true;
	ko_closed = true;
	UT_ASSERT(ReadRecentBuffer(BufTagGetRelFileLocator(&tag), tag.forkNum, tag.blockNum, -1));
	UT_ASSERT_EQ(local_pins, 1);
	UT_ASSERT_EQ(ko_samples, 0);
	UT_ASSERT_EQ(header_calls, 0);
}

UT_TEST(test_exact_cleaner_read_reaches_native_pin_but_never_cr_publication)
{
	for (unsigned kind = 0; kind < 6; kind++) {
		BufferTag tag = reset_buffers();
		SMgrRelationData smgr = { 0 };
		BufferDesc *buf;
		bool found;
		if (kind >= 3) {
			tag.forkNum = SPACE_FORKNUM;
			tag.blockNum = 0;
		}
		ko_cleaner_target = tag;
		ko_cleaner_allowed = true;
		if (kind % 3 != 2)
			install_current(&tag, 0);
		cluster_enabled = true;
		ko_closed = true;
		waiting_tag = tag;
		if (kind % 3 == 0) {
			UT_ASSERT(
				ReadRecentBuffer(BufTagGetRelFileLocator(&tag), tag.forkNum, tag.blockNum, 1));
			buf = GetBufferDescriptor(0);
			if (remembered_pins != 0)
				UnpinBuffer(buf);
		} else {
			smgr.smgr_rlocator.locator = BufTagGetRelFileLocator(&tag);
			smgr.smgr_rlocator.backend = InvalidBackendId;
			buf = BufferAlloc(&smgr, RELPERSISTENCE_PERMANENT, tag.forkNum, tag.blockNum, NULL,
							  &found, IOCONTEXT_NORMAL);
			UT_ASSERT_EQ(found, kind % 3 == 1);
			UT_ASSERT(BufferTagsEqual(&buf->tag, &tag));
			UnpinBuffer(buf);
		}
		UT_ASSERT_EQ(object_waits, 0);
		UT_ASSERT_EQ(remembered_pins, 0);
	}
	{
		BufferCrKey key = scoped_key(reset_buffers());
		PGIOAlignedBlock page;
		Buffer buffer = cluster_bufmgr_cr_reserve_v1();
		ko_cleaner_target = key.tag;
		ko_cleaner_allowed = true;
		cluster_enabled = true;
		ko_closed = true;
		memset(page.data, 0x31, BLCKSZ);
		UT_ASSERT(!cluster_bufmgr_cr_publish_v1(buffer, &key, page.data));
		UnpinBuffer(GetBufferDescriptor(buffer - 1));
	}
}

static BufferDesc *
conditional_read_pin(unsigned path, BufferTag *tag)
{
	BufferDesc *buf;
	SMgrRelationData smgr = { 0 };
	bool found;
	cluster_enabled = true;
	if (path != 2)
		install_current(tag, 0);
	if (path == 0) {
		UT_ASSERT(ReadRecentBuffer(BufTagGetRelFileLocator(tag), tag->forkNum, tag->blockNum, 1));
		buf = GetBufferDescriptor(0);
	} else {
		smgr.smgr_rlocator.locator = BufTagGetRelFileLocator(tag);
		smgr.smgr_rlocator.backend = InvalidBackendId;
		buf = BufferAlloc(&smgr, RELPERSISTENCE_PERMANENT, tag->forkNum, tag->blockNum, NULL,
						  &found, IOCONTEXT_NORMAL);
		UT_ASSERT_EQ(found, path == 1);
		pg_atomic_fetch_or_u32(&buf->state, BM_VALID);
	}
	buf->pcm_state = (uint8)PCM_STATE_X;
	return buf;
}

UT_TEST(test_conditional_writer_revalidates_first_pin_admission)
{
	for (unsigned path = 0; path < 3; path++) {
		for (unsigned race = 0; race < 2; race++) {
			BufferTag tag = reset_buffers();
			BufferDesc *buf = conditional_read_pin(path, &tag);
			Buffer buffer = BufferDescriptorGetBuffer(buf);
			unsigned samples;
			bool granted;
			UT_ASSERT(ConditionalLockBuffer(buffer));
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (race == 0) {
				ko_closed = true;
				ko_revision++;
			} else
				ko_close_at_header = header_calls + 1;
			samples = ko_samples;
			/* Simulate an outer content lock: no KO sampling is legal here. */
			lock_depth = 1;
			granted = ConditionalLockBuffer(buffer);
			UT_ASSERT(!granted);
			if (granted)
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			UT_ASSERT_EQ(lock_depth, 1);
			UT_ASSERT_EQ(ko_samples, samples);
			UT_ASSERT_EQ(remembered_pins, 1);
			UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 1);
			lock_depth = 0;
			UnpinBuffer(buf);
		}
	}
}

UT_TEST(test_conditional_finite_owner_must_remain_exact_and_live)
{
	for (unsigned path = 0; path < 3; path++) {
		BufferTag tag = reset_buffers();
		BufferDesc *buf;
		Buffer buffer;
		bool granted;
		ko_closed = true;
		ko_cleaner_allowed = true;
		ko_cleaner_target = tag;
		buf = conditional_read_pin(path, &tag);
		buffer = BufferDescriptorGetBuffer(buf);
		UT_ASSERT(ConditionalLockBuffer(buffer));
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ko_cleaner_allowed = false;
		granted = ConditionalLockBuffer(buffer);
		UT_ASSERT(!granted);
		if (granted)
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		UT_ASSERT_EQ(remembered_pins, 1);
		UnpinBuffer(buf);
	}
}

UT_TEST(test_conditional_reused_pin_has_no_previous_admission)
{
	BufferTag tag = reset_buffers();
	BufferDesc *buf = conditional_read_pin(0, &tag);
	bool granted;
	UT_ASSERT(ConditionalLockBuffer(1));
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	UnpinBuffer(buf);
	(void)PinBuffer(buf, NULL); /* original non-reader flush pin, no use permission */
	granted = ConditionalLockBuffer(1);
	UT_ASSERT(!granted);
	if (granted)
		LockBuffer(1, BUFFER_LOCK_UNLOCK);
	UnpinBuffer(buf);
}

UT_TEST(test_conditional_admission_survives_private_pin_spill_and_restore)
{
	BufferTag tag = reset_buffers();
	BufferDesc *buf = conditional_read_pin(0, &tag);
	bool granted;
	for (int i = 1; i <= REFCOUNT_ARRAY_ENTRIES; i++) {
		BufferTag other = tag;
		other.blockNum += i;
		install_current(&other, i);
		UT_ASSERT(ReadRecentBuffer(BufTagGetRelFileLocator(&other), other.forkNum, other.blockNum,
								   i + 1));
	}
	UT_ASSERT_EQ(PrivateRefCountOverflowed, 1);
	UT_ASSERT(ConditionalLockBuffer(1)); /* sample lives in native overflow storage */
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	(void)GetPrivateRefCountEntry(1, true); /* original move back into hot array */
	UT_ASSERT(ConditionalLockBuffer(1));
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	ko_closed = true;
	ko_revision++;
	granted = ConditionalLockBuffer(1);
	UT_ASSERT(!granted);
	if (granted)
		LockBuffer(1, BUFFER_LOCK_UNLOCK);
	UnpinBuffer(buf);
	for (int i = 1; i <= REFCOUNT_ARRAY_ENTRIES; i++)
		UnpinBuffer(GetBufferDescriptor(i));
	UT_ASSERT_EQ(remembered_pins, 0);
	UT_ASSERT_EQ(PrivateRefCountOverflowed, 0);
}

UT_TEST(test_cleanup_lock_keeps_original_pin_when_object_is_fenced)
{
	BufferTag tag = reset_buffers();
	BufferDesc *buf = conditional_read_pin(0, &tag);
	bool granted;
	UT_ASSERT(ConditionalLockBufferForCleanup(1));
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	ko_closed = true;
	ko_revision++;
	granted = ConditionalLockBufferForCleanup(1);
	UT_ASSERT(!granted);
	if (granted)
		LockBuffer(1, BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(remembered_pins, 1);
	UT_ASSERT_EQ(lock_depth, 0);
	UnpinBuffer(buf);
}

UT_TEST(test_conditional_object_sample_never_replaces_pcm_proof)
{
	for (unsigned refusal = 0; refusal < 5; refusal++) {
		BufferTag tag = reset_buffers();
		BufferDesc *buf = conditional_read_pin(0, &tag);
		bool granted;
		memset(&pcm_conf, 0, sizeof(pcm_conf));
		pcm_conf.node_count = 2;
		ClusterConfShmem = &pcm_conf;
		cluster_node_id = 0;
		cluster_pcm_grd_max_entries = 64;
		UT_ASSERT(cluster_pcm_is_active());
		UT_ASSERT(cluster_bufmgr_should_pcm_track(buf));
		if (refusal == 0)
			buf->pcm_state = (uint8)PCM_STATE_N;
		else if (refusal == 1)
			buf->pcm_state = (uint8)PCM_STATE_S;
		else if (refusal == 2)
			buf->buffer_type = BUF_TYPE_PI;
		else if (refusal == 3)
			owner_flags[0] = PCM_OWN_FLAG_REVOKING;
		else
			buf->buffer_type = BUF_TYPE_CR;
		granted = ConditionalLockBuffer(1);
		UT_ASSERT(!granted);
		if (granted)
			LockBuffer(1, BUFFER_LOCK_UNLOCK);
		UT_ASSERT_EQ(lock_depth, 0);
		UnpinBuffer(buf);
	}
}

UT_TEST(test_conditional_nonshared_paths_keep_native_behavior)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		BufferTag tag = reset_buffers();
		BufferDesc *buf = GetBufferDescriptor(0);
		install_current(&tag, 0);
		buf->pcm_state = (uint8)PCM_STATE_X;
		(void)PinBuffer(buf, NULL);
		cluster_enabled = kind != 0;
		if (kind == 1)
			storage_route = 0;
		if (kind == 2)
			cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		if (kind == 3)
			cluster_interconnect_tier = CLUSTER_IC_TIER_3;
		ko_closed = true;
		UT_ASSERT(ConditionalLockBuffer(1));
		LockBuffer(1, BUFFER_LOCK_UNLOCK);
		UT_ASSERT_EQ(ko_samples, 0);
		UnpinBuffer(buf);
	}
	reset_buffers();
	NLocBuffer = 1;
	local_refcounts[0] = 1;
	UT_ASSERT(ConditionalLockBuffer(-1));
	UT_ASSERT(ConditionalLockBufferForCleanup(-1));
	UT_ASSERT_EQ(lock_depth, 0);
}

static SMgrRelationData drain_relation;

static ClusterBufferDrainResult
drain_step(BufferTag tag, bool flush, uint32 *budget, uint32 *cursor)
{
	memset(&drain_relation, 0, sizeof(drain_relation));
	drain_relation.smgr_rlocator.locator = BufTagGetRelFileLocator(&tag);
	drain_relation.smgr_rlocator.backend = InvalidBackendId;
	return cluster_bufmgr_drain_relation_v3(&drain_relation, flush, budget, cursor);
}

UT_TEST(test_object_drain_bounds_scan_and_preserves_foreign_pin)
{
	BufferTag tag = reset_buffers();
	uint32 cursor = 0, budget = 2;
	install_current(&tag, 2);
	pg_atomic_fetch_add_u32(&GetBufferDescriptor(2)->state, BUF_REFCOUNT_ONE);
	UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_MORE);
	UT_ASSERT_EQ(cursor, 2);
	UT_ASSERT_EQ(budget, 0);
	budget = 128;
	UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_WAIT);
	UT_ASSERT_EQ(cursor, 2);
	UT_ASSERT_EQ(frees + bumps, 0);
	UT_ASSERT_EQ(lock_depth, 0);
	pg_atomic_fetch_sub_u32(&GetBufferDescriptor(2)->state, BUF_REFCOUNT_ONE);
	budget = 128;
	UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_EMPTY);
	UT_ASSERT_EQ(cursor, NBuffers);
	UT_ASSERT_EQ(frees, 1);
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), -1);
}

UT_TEST(test_object_drain_never_discards_dirty_or_io_work)
{
	uint32 flags[]
		= { BM_DIRTY, BM_JUST_DIRTIED, BM_CHECKPOINT_NEEDED, BM_IO_ERROR, BM_IO_IN_PROGRESS };
	for (unsigned i = 0; i < lengthof(flags); i++) {
		BufferTag tag = reset_buffers();
		uint32 cursor = 2, budget = 128;
		install_current(&tag, 2);
		pg_atomic_fetch_or_u32(&GetBufferDescriptor(2)->state, flags[i]);
		UT_ASSERT_NE(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_EMPTY);
		UT_ASSERT_EQ(cursor, 2);
		UT_ASSERT_EQ(frees + bumps + physical_writes, 0);
		UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(2)->state) & flags[i]) != 0);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_object_drain_flushes_all_forks_and_preserves_cr)
{
	for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		BufferTag tag = reset_buffers();
		uint32 cursor = 0, budget = 128;
		tag.forkNum = fork;
		install_current(&tag, 2);
		if (fork == MAIN_FORKNUM)
			install_cr(&tag, 3);
		pg_atomic_fetch_or_u32(&GetBufferDescriptor(2)->state, BM_DIRTY | BM_JUST_DIRTIED);
		UT_ASSERT_EQ(drain_step(tag, true, &budget, &cursor), CLUSTER_BUFFER_DRAIN_EMPTY);
		UT_ASSERT_EQ(physical_writes, 1);
		UT_ASSERT_EQ(frees + bumps + remembered_pins, 0);
		cursor = 0;
		budget = 128;
		UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_EMPTY);
		UT_ASSERT_EQ(frees, fork == MAIN_FORKNUM ? 2 : 1);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_object_drain_rechecks_dirty_pin_and_owner_under_mapping_lock)
{
	for (unsigned race = 1; race <= 3; race++) {
		BufferTag tag = reset_buffers();
		uint32 cursor = 2, budget = 128;
		install_current(&tag, 2);
		drain_mapping_race = race;
		if (race == 3) {
			/* A bare REVOKING bit without its exact reservation is corrupt.
			 * The original owner reports ERROR rather than classifying it idle. */
			expect_error = true;
			if (setjmp(error_jump) == 0) {
				(void)drain_step(tag, false, &budget, &cursor);
				UT_ASSERT(false);
			}
		} else
			UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_WAIT);
		UT_ASSERT_EQ(cursor, 2);
		UT_ASSERT_EQ(frees + bumps + lock_depth, 0);
		UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 2);
	}
}

UT_TEST(test_object_drain_refused_flush_is_pending_without_pin_leak)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		BufferTag tag = reset_buffers();
		uint32 cursor = 2, budget = 128;
		install_current(&tag, 2);
		pg_atomic_fetch_or_u32(&GetBufferDescriptor(2)->state, BM_DIRTY);
		drain_content_busy = kind == 0;
		drain_flush_deferred = kind == 1;
		UT_ASSERT_EQ(drain_step(tag, true, &budget, &cursor), CLUSTER_BUFFER_DRAIN_WAIT);
		UT_ASSERT_EQ(cursor, 2);
		UT_ASSERT_EQ(physical_writes + frees + bumps + lock_depth + remembered_pins, 0);
		UT_ASSERT((pg_atomic_read_u32(&GetBufferDescriptor(2)->state) & BM_DIRTY) != 0);
		drain_content_busy = drain_flush_deferred = false;
		budget = 128;
		UT_ASSERT_EQ(drain_step(tag, true, &budget, &cursor), CLUSTER_BUFFER_DRAIN_EMPTY);
		UT_ASSERT_EQ(physical_writes, 1);
	}
}

UT_TEST(test_object_drain_invalid_input_never_advances_or_touches_buffers)
{
	BufferTag tag = reset_buffers();
	uint32 cursor = 0, budget = 129;
	install_current(&tag, 2);
	UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_INVALID);
	UT_ASSERT_EQ(cursor, 0);
	budget = 128;
	cursor = NBuffers + 1;
	UT_ASSERT_EQ(drain_step(tag, false, &budget, &cursor), CLUSTER_BUFFER_DRAIN_INVALID);
	UT_ASSERT_EQ(cursor, NBuffers + 1);
	UT_ASSERT_EQ(frees + bumps + physical_writes + lock_depth, 0);
}

UT_TEST(test_committed_discard_requires_retired_identity_before_any_buffer_touch)
{
	BufferTag tag = reset_buffers();
	uint32 budget = 128, cursor = 0;
	install_current(&tag, 2);
	pg_atomic_fetch_or_u32(&GetBufferDescriptor(2)->state, BM_DIRTY);
	UT_ASSERT_EQ(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor),
		CLUSTER_BUFFER_DRAIN_INVALID);
	UT_ASSERT_EQ(cursor, 0);
	UT_ASSERT_EQ(budget, 128);
	UT_ASSERT_EQ(frees + bumps + header_calls + physical_writes, 0);
}

UT_TEST(test_committed_discard_removes_dirty_current_and_pi_all_forks_without_io)
{
	for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		for (unsigned kind = 0; kind < 3; kind++) {
			BufferTag tag = reset_buffers();
			BufferDesc *buf = GetBufferDescriptor(2);
			uint32 budget = 128, cursor = 0;
			tag.forkNum = fork;
			ko_retired = true;
			install_current(&tag, 2);
			buf->pcm_state = kind == 0 ? PCM_STATE_X : kind == 1 ? PCM_STATE_S : PCM_STATE_N;
			if (kind == 2) buf->buffer_type = BUF_TYPE_PI;
			pg_atomic_fetch_or_u32(&buf->state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
			UT_ASSERT_EQ(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor),
				CLUSTER_BUFFER_DRAIN_EMPTY);
			UT_ASSERT_EQ(cursor, NBuffers);
			UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), -1);
			UT_ASSERT_EQ(frees, 1);
			UT_ASSERT_EQ(bumps, 1);
			UT_ASSERT_EQ(wal_resets, 1);
			UT_ASSERT_EQ(physical_writes + lock_depth + remembered_pins, 0);
		}
	}
}

UT_TEST(test_origin_discard_scope_removes_dead_bytes_without_directory_retirement)
{
	BufferTag tag = reset_buffers();
	uint32 budget = 128, cursor = 0;

	ko_origin_discard = true; /* Exact native owner is the separate KO fixture. */
	install_current(&tag, 2);
	pg_atomic_fetch_or_u32(&GetBufferDescriptor(2)->state, BM_DIRTY);
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(ko_locator));
	UT_ASSERT_EQ(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor),
		CLUSTER_BUFFER_DRAIN_EMPTY);
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), -1);
	UT_ASSERT_EQ(frees, 1);
	UT_ASSERT_EQ(physical_writes + lock_depth + remembered_pins, 0);
	UT_ASSERT(!cluster_ko_directory_drop_retired_v3(ko_locator));
}

UT_TEST(test_committed_discard_keeps_pins_io_and_ownership_reservations)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		BufferTag tag = reset_buffers();
		BufferDesc *buf = GetBufferDescriptor(2);
		uint32 budget = 128, cursor = 0, state;
		ko_retired = true;
		install_current(&tag, 2);
		pg_atomic_fetch_or_u32(&buf->state, BM_DIRTY);
		switch (fault) {
		case 0: pg_atomic_fetch_add_u32(&buf->state, BUF_REFCOUNT_ONE); break;
		case 1: pg_atomic_fetch_or_u32(&buf->state, BM_IO_IN_PROGRESS); break;
		case 2: pg_atomic_fetch_or_u32(&buf->state, BM_IO_ERROR); break;
		case 3: owner_flags[2] = PCM_OWN_FLAG_REVOKING; break;
		case 4: owner_generation[2] = UINT64_MAX; break;
		case 5: buf->buffer_type = UINT8_MAX; break;
		case 6: owner_generation[2] = 0; break;
		case 7: owner_writer[2] = 7; break;
		case 8: owner_activation[2] = 7; break;
		case 9: owner_delivery[2] = 7; break;
		case 10: buf->pcm_state = UINT8_MAX; break;
		}
		memset(&discarded_detail, 0, sizeof(discarded_detail));
		state = pg_atomic_read_u32(&buf->state);
		UT_ASSERT(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor)
			!= CLUSTER_BUFFER_DRAIN_EMPTY);
		UT_ASSERT(discarded_detail.valid);
		UT_ASSERT_EQ(discarded_detail.source, CLUSTER_KO_DISCARD_BUFFER);
		UT_ASSERT(RelFileLocatorEquals(discarded_detail.locator, ko_locator));
		UT_ASSERT_EQ(discarded_detail.forknum, tag.forkNum);
		UT_ASSERT_EQ(discarded_detail.blocknum, tag.blockNum);
		UT_ASSERT_EQ(discarded_detail.slot, 2);
		if (fault == 0) {
			UT_ASSERT_EQ(discarded_detail.refs, 1);
			UT_ASSERT_EQ(discarded_detail.predicate_mask, 2);
		}
		if (fault == 1 || fault == 2)
			UT_ASSERT_EQ(discarded_detail.predicate_mask, 4);
		UT_ASSERT_EQ(cursor, 2);
		UT_ASSERT_EQ(pg_atomic_read_u32(&buf->state), state);
		UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), 2);
		UT_ASSERT_EQ(bumps + frees + physical_writes + lock_depth, 0);
	}
}

UT_TEST(test_committed_discard_budget_and_mapping_race_keep_original_owner)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		BufferTag tag = reset_buffers();
		uint32 cursor = 0, budget = 2;
		ko_retired = true;
		install_current(&tag, 2);
		UT_ASSERT_EQ(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor),
			CLUSTER_BUFFER_DRAIN_MORE);
		UT_ASSERT_EQ(cursor, 2);
		UT_ASSERT_EQ(budget, 0);
		budget = 128;
		drain_mapping_race = fault == 0 ? 2 : fault == 1 ? 3 : 1;
		UT_ASSERT_EQ(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor),
			fault == 2 ? CLUSTER_BUFFER_DRAIN_EMPTY : CLUSTER_BUFFER_DRAIN_WAIT);
		UT_ASSERT_EQ(frees, fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(physical_writes + lock_depth, 0);
	}
}

UT_TEST(test_committed_discard_removes_clean_cr_with_original_mapping_tail)
{
	BufferTag tag = reset_buffers();
	uint32 cursor = 0, budget = 128;
	int head;
	uint64 generation;
	ko_retired = true;
	install_current(&tag, 0);
	install_cr(&tag, 1);
	install_cr(&tag, 2);
	UT_ASSERT_EQ(cluster_bufmgr_discard_relation_v4(ko_locator, &budget, &cursor),
		CLUSTER_BUFFER_DRAIN_EMPTY);
	UT_ASSERT_EQ(frees, 3);
	UT_ASSERT(!BufTableCRLookup(&tag, BufTableHashCode(&tag), &head, &generation));
	UT_ASSERT_EQ(BufTableLookup(&tag, BufTableHashCode(&tag)), -1);
	UT_ASSERT_EQ(physical_writes + lock_depth, 0);
}

int
main(void)
{
	UT_PLAN(49);
	UT_RUN(test_origin_discard_scope_removes_dead_bytes_without_directory_retirement);
	printf("# private pin entry: %zu bytes; current admission: %zu bytes\n",
		   sizeof(PrivateRefCountEntry), sizeof(ClusterKoCurrentAdmissionV3));
	UT_RUN(test_object_drain_bounds_scan_and_preserves_foreign_pin);
	UT_RUN(test_object_drain_never_discards_dirty_or_io_work);
	UT_RUN(test_object_drain_flushes_all_forks_and_preserves_cr);
	UT_RUN(test_object_drain_invalid_input_never_advances_or_touches_buffers);
	UT_RUN(test_object_drain_rechecks_dirty_pin_and_owner_under_mapping_lock);
	UT_RUN(test_object_drain_refused_flush_is_pending_without_pin_leak);
	UT_RUN(test_native_current_victim_keeps_original_behavior);
	UT_RUN(test_cr_victim_preserves_current_and_removes_middle_head_tail);
	UT_RUN(test_last_cr_only_victim_removes_anchor_without_current);
	UT_RUN(test_foreign_pin_keeps_cr_mapped_without_bump);
	UT_RUN(test_cr_ownership_reservation_refuses_reuse_without_mutation);
	UT_RUN(test_broken_cr_chain_is_rejected_before_ownership_or_mapping_mutation);
	UT_RUN(test_cr_reuse_clears_overlay_and_preserves_lwlock);
	UT_RUN(test_fast_truncate_removes_all_versions_including_cr_only_anchors);
	UT_RUN(test_fast_drop_rejects_a_still_mapped_wrong_tag_without_spinning);
	UT_RUN(test_cr_has_no_mutation_authority_even_without_pcm);
	UT_RUN(test_native_reservation_publishes_clean_cr_and_copies_with_original_pin);
	UT_RUN(test_duplicate_publish_preserves_first_image_and_losing_reservation);
	UT_RUN(test_wrong_scope_snapshot_scn_epoch_or_tag_misses_without_touching_output);
	UT_RUN(test_publish_refuses_mapped_or_multiply_pinned_or_reserved_descriptor);
	UT_RUN(test_hash_capacity_refusal_leaves_reservation_unmapped_and_releasable);
	UT_RUN(test_copy_error_releases_original_pin_and_keeps_immutable_entry);
	UT_RUN(test_copy_unpin_notifies_original_pin_count_waiter);
	UT_RUN(test_native_clock_recycles_clean_cr_and_keeps_current);
	UT_RUN(test_native_clock_skips_foreign_pinned_cr);
	UT_RUN(test_native_clock_refuses_dirty_cr_before_any_io_or_pin);
	UT_RUN(test_checkpoint_skips_clean_cr_and_refuses_dirty_cr);
	UT_RUN(test_publish_copy_error_leaves_only_the_original_reservation_pin);
	UT_RUN(test_object_fence_refuses_cr_publication_before_and_at_header);
	UT_RUN(test_object_fence_refuses_new_cr_pin_without_touching_output);
	UT_RUN(test_object_fence_preserves_finite_cr_pin_and_unrelated_storage);
	UT_RUN(test_object_fence_closes_recent_buffer_before_first_pin);
	UT_RUN(test_object_fence_preserves_recent_existing_pin_and_private_route);
	UT_RUN(test_object_fence_buffer_alloc_waits_off_locks_without_publication);
	UT_RUN(test_object_fence_does_not_enter_local_buffer_path);
	UT_RUN(test_exact_cleaner_read_reaches_native_pin_but_never_cr_publication);
	UT_RUN(test_conditional_writer_revalidates_first_pin_admission);
	UT_RUN(test_conditional_finite_owner_must_remain_exact_and_live);
	UT_RUN(test_conditional_reused_pin_has_no_previous_admission);
	UT_RUN(test_conditional_admission_survives_private_pin_spill_and_restore);
	UT_RUN(test_cleanup_lock_keeps_original_pin_when_object_is_fenced);
	UT_RUN(test_conditional_object_sample_never_replaces_pcm_proof);
	UT_RUN(test_conditional_nonshared_paths_keep_native_behavior);
	UT_RUN(test_committed_discard_requires_retired_identity_before_any_buffer_touch);
	UT_RUN(test_committed_discard_removes_dirty_current_and_pi_all_forks_without_io);
	UT_RUN(test_committed_discard_keeps_pins_io_and_ownership_reservations);
	UT_RUN(test_committed_discard_budget_and_mapping_race_keep_original_owner);
	UT_RUN(test_committed_discard_removes_clean_cr_with_original_mapping_tail);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
