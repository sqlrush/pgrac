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
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "storage/buf_internals.h"
#include "storage/shmem.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#include "test_cluster_buffer_mapping_fixture.h"

BufferDescPadded *BufferDescriptors;
LWLockPadded *MainLWLockArray;
bool cluster_shared_config = true;
bool cluster_shared_catalog = true;
bool cluster_enabled;
bool cluster_recmerge_window_active;
int cluster_pcm_grd_max_entries;
ClusterConf *ClusterConfShmem;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static BufferDescPadded descriptors[64];
static LWLockPadded mapping_locks[BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS];
static uint64 owner_generation[64];
static uint32 owner_flags[64];
static unsigned bumps;
static unsigned frees;
static unsigned wal_resets;
static unsigned lock_depth;
static unsigned lock_acquisitions;
static unsigned invalidations;
static int private_pin[64];

static void InvalidateBufferCommitTailLocked(BufferDesc *, BufferTag *, uint32, LWLock *, uint32,
											 uint8, bool);
static void InvalidateBuffer(BufferDesc *buf);

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	if (++lock_acquisitions > 128)
		longjmp(error_jump, 1);
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
	UT_ASSERT((state & BM_LOCKED) == 0);
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state | BM_LOCKED;
}

void
StrategyFreeBuffer(BufferDesc *buf)
{
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf->state)), 0);
	frees++;
}

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
	out->flags = owner_flags[buf->buf_id];
	out->pcm_state = buf->pcm_state;
	out->buffer_type = buf->buffer_type;
}

static bool
cluster_pcm_own_fence_matches_locked(BufferDesc *buf, const ClusterPcmOwnSnapshot *before)
{
	return owner_generation[buf->buf_id] == before->generation
		   && owner_flags[buf->buf_id] == before->flags && buf->buffer_type == before->buffer_type
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
	longjmp(error_jump, 1);
}

#include "test_cluster_buffer_cr_owner.inc"

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
	memset(private_pin, 0, sizeof(private_pin));
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

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_native_current_victim_keeps_original_behavior);
	UT_RUN(test_cr_victim_preserves_current_and_removes_middle_head_tail);
	UT_RUN(test_last_cr_only_victim_removes_anchor_without_current);
	UT_RUN(test_foreign_pin_keeps_cr_mapped_without_bump);
	UT_RUN(test_cr_ownership_reservation_refuses_reuse_without_mutation);
	UT_RUN(test_broken_cr_chain_is_rejected_before_ownership_or_mapping_mutation);
	UT_RUN(test_cr_reuse_clears_overlay_and_preserves_lwlock);
	UT_RUN(test_fast_truncate_removes_all_versions_including_cr_only_anchors);
	UT_RUN(test_fast_drop_rejects_a_still_mapped_wrong_tag_without_spinning);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
