/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual buffer stop observer, ownership producer and PI/IO completion tests.
 * Native buffer header/mapping locks, ResourceOwner, checkpoint I/O and the
 * final InvalidateBuffer mapping removal are explicit fixture boundaries.
 * No live disk durability, native concurrency or full shutdown is claimed. */
#include "postgres.h"

#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_pi_shadow.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_lever.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/shmem.h"
#include "utils/resowner_private.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int NBuffers = 4;
bool cluster_enabled = true;
bool IsUnderPostmaster = true;
bool cluster_past_image = true;
static BufferDescPadded descriptors[4];
BufferDescPadded *BufferDescriptors = descriptors;
static ConditionVariableMinimallyPadded io_cvs[4];
ConditionVariableMinimallyPadded *BufferIOCVArray = io_cvs;
static SCN shadow[4];
SCN *ClusterPiShadow = shadow;
static ClusterPcmOwnEntry own_storage[4];
ResourceOwner CurrentResourceOwner;
static LWLock mapping_lock;
static bool mapping_held;
static bool caller_lock_held;
static int header_reads, discarded, io_forgotten, io_wakes;
static int pi_kept, pi_ineligible;

void
ExceptionalCondition(const char *condition, const char *filename, int line)
{
	fprintf(stderr, "assertion failed: %s at %s:%d\n", condition, filename, line);
	abort();
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	(void)name;
	UT_ASSERT_EQ(size, sizeof(own_storage));
	*found = false;
	return own_storage;
}

Size
mul_size(Size a, Size b)
{
	return a * b;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	(void)region;
}

uint32
LockBufHdr(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT((state & BM_LOCKED) == 0);
	header_reads++;
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state | BM_LOCKED;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	if (caller_lock_held || mapping_held)
		callback(&mapping_lock, LW_SHARED, context);
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &mapping_lock);
	UT_ASSERT_EQ(mode, LW_SHARED);
	UT_ASSERT(!mapping_held);
	mapping_held = true;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &mapping_lock);
	UT_ASSERT(mapping_held);
	mapping_held = false;
}

uint32
BufTableHashCode(BufferTag *tag)
{
	return tag->blockNum;
}

int
BufTableLookup(BufferTag *tag, uint32 hashcode)
{
	int i;
	(void)hashcode;
	UT_ASSERT(mapping_held);
	for (i = 0; i < NBuffers; i++) {
		BufferDesc *buf = GetBufferDescriptor(i);
		if ((pg_atomic_read_u32(&buf->state) & BM_TAG_VALID) != 0
			&& BufferTagsEqual(tag, &buf->tag))
			return i;
	}
	return -1;
}

#undef BufMappingPartitionLock
#define BufMappingPartitionLock(hashcode) (&mapping_lock)

/* Exact native boundary invoked only by the original discard consumer.
 * It is not the stop observer and is not a disk durability proof. */
static void
InvalidateBuffer(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT(state & BM_LOCKED);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(state), 0);
	UT_ASSERT_EQ(buf->pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(cluster_pi_shadow_read(buf->buf_id), InvalidScn);
	ClearBufferTag(&buf->tag);
	buf->buffer_type = BUF_TYPE_CURRENT;
	UnlockBufHdr(buf, state & ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK));
	discarded++;
}

SCN
cluster_scn_current(void)
{
	return scn_encode(0, 123);
}

void
cluster_lever_h_note_pi_kept(void)
{
	pi_kept++;
}

void
cluster_lever_h_note_pi_ineligible(void)
{
	pi_ineligible++;
}

void
ResourceOwnerForgetBufferIO(ResourceOwner owner, Buffer buffer)
{
	(void)owner;
	UT_ASSERT(buffer > 0 && buffer <= NBuffers);
	io_forgotten++;
}

void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	UT_ASSERT(cv != NULL);
	io_wakes++;
}

#include "test_cluster_pcm_snapshot_owner.inc"
#include "test_cluster_bufmgr_stop_owner.inc"

static void
reset_fixture(void)
{
	int i;
	NBuffers = 4;
	BufferDescriptors = descriptors;
	ClusterPiShadow = shadow;
	cluster_enabled = IsUnderPostmaster = cluster_past_image = true;
	memset(descriptors, 0, sizeof(descriptors));
	memset(shadow, 0, sizeof(shadow));
	cluster_pcm_own_shmem_init();
	for (i = 0; i < NBuffers; i++) {
		BufferDesc *buf = GetBufferDescriptor(i);
		buf->buf_id = i;
		pg_atomic_init_u32(&buf->state, 0);
	}
	mapping_held = caller_lock_held = false;
	header_reads = discarded = io_forgotten = io_wakes = pi_kept = pi_ineligible = 0;
}

static BufferDesc *
resident(int index)
{
	BufferDesc *buf = GetBufferDescriptor(index);
	RelFileLocator locator = { 1663, 5, 16386 };
	InitBufferTag(&buf->tag, &locator, MAIN_FORKNUM, 17003 + index);
	buf->buffer_type = BUF_TYPE_XCUR;
	buf->pcm_state = PCM_STATE_X;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_VALID | BM_PERMANENT);
	return buf;
}

static ClusterNormalStopPollResult
poll_stop(bool post)
{
	return cluster_bufmgr_normal_stop_poll(post, NULL, NULL, NULL);
}

static void
test_required_init_and_lock_boundary(void)
{
	ClusterPcmOwnEntry *saved;
	const char *reason;
	reset_fixture();
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_READY);
	saved = ClusterPcmOwnArray;
	ClusterPcmOwnArray = NULL;
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
	ClusterPcmOwnArray = saved;
	ClusterPiShadow = NULL;
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
	ClusterPiShadow = shadow;
	BufferDescriptors = NULL;
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
	BufferDescriptors = descriptors;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	header_reads = 0;
	caller_lock_held = true;
	UT_ASSERT_EQ(cluster_bufmgr_normal_stop_poll(false, NULL, NULL, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "BUFMGR_CALLER_LOCK_HELD");
	UT_ASSERT_EQ(header_reads, 0);
}

static void
test_original_reservation_activation_delivery_completion(void)
{
	BufferDesc *buf;
	uint32 state;
	uint64 token, generation;
	reset_fixture();
	buf = resident(2);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(2, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(2, 0, token, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(2, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_writer_grant_commit_exact(2, 0, token, &generation),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(2, generation, token, 44),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(2, generation, token, 44),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_READY);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_begin_exact(2, generation, 93), CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(2, generation, 93),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(2), token);
}

static void
test_original_pi_convert_preserve_discard(void)
{
	BufferDesc *buf;
	BufferTag tag;
	BufferDescPadded before[4];
	ClusterPcmOwnEntry before_own[4];
	SCN before_shadow[4];
	uint32 state;
	reset_fixture();
	buf = resident(1);
	tag = buf->tag;
	state = LockBufHdr(buf);
	buf->pcm_state = PCM_STATE_N; /* Original caller's downgrade boundary. */
	UT_ASSERT(cluster_bufmgr_convert_to_pi_locked(buf, state));
	UT_ASSERT_EQ(pi_kept, 1);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_READY);
	memcpy(before, descriptors, sizeof(before));
	memcpy(before_own, ClusterPcmOwnArray, sizeof(before_own));
	memcpy(before_shadow, shadow, sizeof(before_shadow));
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(memcmp(before, descriptors, sizeof(before)) == 0);
	UT_ASSERT(memcmp(before_own, ClusterPcmOwnArray, sizeof(before_own)) == 0);
	UT_ASSERT(memcmp(before_shadow, shadow, sizeof(before_shadow)) == 0);
	/* Raw-pin boundary: the original discard must refuse it. */
	pg_atomic_fetch_add_u32(&buf->state, 1);
	UT_ASSERT(!cluster_bufmgr_discard_pi_block(tag));
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_fetch_sub_u32(&buf->state, 1);
	UT_ASSERT(cluster_bufmgr_discard_pi_block(tag));
	UT_ASSERT_EQ(discarded, 1);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
}

static void
test_io_original_completion_and_failure(void)
{
	BufferDesc *buf;
	uint32 state;
	reset_fixture();
	buf = resident(0);
	state = pg_atomic_read_u32(&buf->state);
	pg_atomic_write_u32(&buf->state, state | BM_DIRTY | BM_CHECKPOINT_NEEDED);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_PENDING);
	/* Native IO start/pin is the boundary, completion is the real body. */
	pg_atomic_fetch_or_u32(&buf->state, BM_IO_IN_PROGRESS);
	pg_atomic_fetch_add_u32(&buf->state, 1);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	TerminateBufferIO(buf, true, 0);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	pg_atomic_fetch_sub_u32(&buf->state, 1);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(io_forgotten, 1);
	UT_ASSERT_EQ(io_wakes, 1);
	pg_atomic_fetch_or_u32(&buf->state, BM_IO_IN_PROGRESS);
	TerminateBufferIO(buf, false, BM_IO_ERROR);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
}

static void
test_retained_cache_is_not_live_pi(void)
{
	BufferDesc *buf;
	uint64 token;
	uint32 state;
	reset_fixture();
	buf = resident(0);
	buf->buffer_type = BUF_TYPE_PI;
	buf->pcm_state = PCM_STATE_N;
	/* A stale stamp on a non-D-h1 shape is not a responsibility. */
	shadow[0] = scn_encode(0, 121);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	state = LockBufHdr(buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, token, PCM_OWN_FLAG_REVOKING),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
}

static void
test_late_invalid_and_read_image_owner(void)
{
	BufferDesc *first, *last;
	BufferTag observed;
	const char *reason;
	int id;
	reset_fixture();
	first = resident(0);
	last = resident(3);
	pg_atomic_fetch_add_u32(&first->state, 1);
	last->buffer_type = 99;
	UT_ASSERT_EQ(cluster_bufmgr_normal_stop_poll(false, &observed, &id, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(id, 3);
	UT_ASSERT(BufferTagsEqual(&observed, &last->tag));
	UT_ASSERT_STR_EQ(reason, "BUFMGR_SHAPE_INVALID");
	last->buffer_type = BUF_TYPE_XCUR;
	pg_atomic_fetch_sub_u32(&first->state, 1);
	last->pcm_state = PCM_STATE_READ_IMAGE;
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
	last->pcm_state = PCM_STATE_N; /* Real unlock consumer separately tested. */
	UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
	pg_atomic_write_u32(&ClusterPcmOwnArray[3].flags, 3);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
	pg_atomic_write_u32(&ClusterPcmOwnArray[3].flags, 0);
	pg_atomic_write_u64(&ClusterPcmOwnArray[3].writer_activation_token, 17);
	UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_INVALID);
}

static void
test_invalid_uninitialized_residency_is_not_cached_authority(void)
{
	int mode;
	for (mode = PCM_STATE_S; mode <= PCM_STATE_X; mode++) {
		BufferDesc *first;
		BufferDesc *last;
		BufferTag observed;
		int id;
		reset_fixture();
		first = resident(0);
		last = resident(3);
		pg_atomic_fetch_add_u32(&first->state, 1);
		last->pcm_state = mode;
		pg_atomic_fetch_and_u32(&last->state, ~BM_VALID);
		UT_ASSERT_EQ(cluster_bufmgr_normal_stop_poll(false, &observed, &id, NULL),
					 CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(id, 3);
		UT_ASSERT(BufferTagsEqual(&observed, &last->tag));
		UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_INVALID);
		/* Direct-init before bytes are published is still owned by real IO. */
		pg_atomic_fetch_or_u32(&last->state, BM_IO_IN_PROGRESS);
		UT_ASSERT_EQ(poll_stop(false), CLUSTER_NORMAL_STOP_PENDING);
		TerminateBufferIO(last, false, BM_VALID);
		pg_atomic_fetch_sub_u32(&first->state, 1);
		UT_ASSERT_EQ(poll_stop(true), CLUSTER_NORMAL_STOP_READY);
	}
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_required_init_and_lock_boundary);
	UT_RUN(test_original_reservation_activation_delivery_completion);
	UT_RUN(test_original_pi_convert_preserve_discard);
	UT_RUN(test_io_original_completion_and_failure);
	UT_RUN(test_retained_cache_is_not_live_pi);
	UT_RUN(test_late_invalid_and_read_image_owner);
	UT_RUN(test_invalid_uninitialized_residency_is_not_cached_authority);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
