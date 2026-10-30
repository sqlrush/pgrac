/*-------------------------------------------------------------------------
 * test_cluster_vm_clear_local_read.c
 *    AF=0 decisions below heap X may only borrow existing local authority.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Actual buffer brackets, raw/pinned decoders and heap decision are extracted
 * verbatim. Native lock scheduling, ownership snapshots and KO validation are
 * controlled leaves; no transport, native ResourceOwner or SQL is simulated.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)true)
#undef AssertMacro
#define AssertMacro(condition) ((void)true)
#undef PG_USED_FOR_ASSERTS_ONLY
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif

#include "access/visibilitymap.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/storage/cluster_smgr.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock image, before;
static BufferDesc descriptor;
static RelationData relation;
static FormData_pg_class relform;
static LWLock content_lock;
static ClusterPcmOwnSnapshot own;
static bool active, tracked, pinned, held, contended, ko_valid;
static bool decoder_error, cache_off, absent_ref;
static int service_error;
static unsigned conditional_calls, snapshots, ko_checks, releases;
static unsigned acquiring_entries, generic_unlocks, remote_unlocks, page_reads;
static ClusterPcmOwnResult snapshot_result;

int NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static int GetPrivateRefCount(Buffer buffer) pg_attribute_unused();
static bool cluster_bufmgr_should_pcm_track(BufferDesc *buf) pg_attribute_unused();
static Buffer vm_readbuf(Relation rel, BlockNumber block, bool extend);

#undef GetBufferDescriptor
#define GetBufferDescriptor(id) ((void)(id), &descriptor)
#undef BufferDescriptorGetContentLock
#define BufferDescriptorGetContentLock(buf) ((void)(buf), &content_lock)
#define cluster_pcm_is_active() active

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition; (void)file; (void)line;
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

static int
GetPrivateRefCount(Buffer buffer)
{
	return buffer == 1 && pinned ? 1 : 0;
}

static bool
cluster_bufmgr_should_pcm_track(BufferDesc *buf)
{
	UT_ASSERT(buf == &descriptor);
	return tracked;
}

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == &descriptor && held);
	if (service_error == 1)
		siglongjmp(*PG_exception_stack, 1);
	snapshots++;
	*out = own;
	return snapshot_result;
}

bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &content_lock && mode == LW_SHARED && !held);
	conditional_calls++;
	if (contended)
		return false;
	held = true;
	return true;
}

bool
LWLockHeldByMe(LWLock *lock)
{
	UT_ASSERT(lock == &content_lock);
	return held;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == &content_lock && held);
	held = false;
	releases++;
}

bool
cluster_ko_peer_current_valid_v3(const ClusterKoCurrentAdmissionV3 *sample,
	RelFileLocator locator, ForkNumber forknum, BlockNumber block)
{
	UT_ASSERT(sample != NULL && held);
	if (service_error == 2)
		siglongjmp(*PG_exception_stack, 1);
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT(forknum == VISIBILITYMAP_FORKNUM && block == 0);
	ko_checks++;
	return ko_valid;
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT(backend == InvalidBackendId);
	return 1;
}

bool
cluster_heap_maintenance_restricted(Relation rel)
{
	return rel == &relation;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1);
	if (decoder_error && held)
		siglongjmp(*PG_exception_stack, 1);
	*locator = BufTagGetRelFileLocator(&descriptor.tag);
	*forknum = descriptor.tag.forkNum;
	*block = descriptor.tag.blockNum;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return descriptor.tag.blockNum;
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && pinned && !held);
	pinned = false;
}

/* The original entry is allowed to acquire or wait. Merely entering it
 * violates this nested caller's cache-only requirement, even on a hot hit. */
bool
ClusterLockBufferShareBarrierAware(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && pinned && !held);
	acquiring_entries++;
	held = true;
	return true;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT(buffer == 1 && held && mode == BUFFER_LOCK_UNLOCK);
	generic_unlocks++;
	remote_unlocks += cache_off;
	held = false;
}

static Buffer
vm_readbuf(Relation rel, BlockNumber block, bool extend)
{
	(void)rel; (void)block; (void)extend;
	page_reads++;
	abort(); /* Exact borrowed pins must never be refreshed beneath heap X. */
}

#include "test_cluster_vm_clear_local_read.inc"

static PrivateRefCountEntry pin_ref;

static PrivateRefCountEntry *
GetPrivateRefCountEntry(Buffer buffer, bool do_move)
{
	UT_ASSERT(buffer == 1 && !do_move);
	return absent_ref ? NULL : &pin_ref;
}

static void
reset(void)
{
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){1663, 5, 900};
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&descriptor, 0, sizeof(descriptor));
	InitBufferTag(&descriptor.tag, &relation.rd_locator, VISIBILITYMAP_FORKNUM, 0);
	memset(&own, 0, sizeof(own));
	own.tag = descriptor.tag;
	own.semantic_buf_state = BM_TAG_VALID | BM_VALID;
	own.pcm_state = PCM_STATE_S;
	own.buffer_type = BUF_TYPE_CURRENT;
	own.generation = 11;
	own.reservation_token = 7;
	snapshot_result = CLUSTER_PCM_OWN_OK;
	memset(&pin_ref, 0, sizeof(pin_ref));
	pin_ref.buffer = 1;
	pin_ref.refcount = 1;
	BufferBlocks = image.data;
	PageInit(image.data, BLCKSZ, 0);
	((PageHeader)image.data)->pd_block_scn = 30;
	((uint8 *)PageGetContents(image.data))[0] = VISIBILITYMAP_ALL_VISIBLE | (VISIBILITYMAP_VALID_BITS << 2);
	memcpy(before.data, image.data, BLCKSZ);
	active = tracked = pinned = ko_valid = true;
	held = contended = decoder_error = absent_ref = false;
	cache_off = false;
	service_error = 0;
	conditional_calls = snapshots = ko_checks = releases = 0;
	acquiring_entries = generic_unlocks = remote_unlocks = page_reads = 0;
}

static void
check_borrowed_pin(void)
{
	UT_ASSERT(pinned && !held);
	UT_ASSERT_EQ(acquiring_entries, 0);
	UT_ASSERT_EQ(generic_unlocks, 0);
	UT_ASSERT_EQ(remote_unlocks, 0);
	UT_ASSERT_EQ(page_reads, 0);
	UT_ASSERT(memcmp(before.data, image.data, BLCKSZ) == 0);
}

UT_TEST(cached_s_and_x_observe_af_zero_without_an_acquiring_entry)
{
	for (int mode = 0; mode < 2; mode++) {
		reset();
		own.pcm_state = mode ? PCM_STATE_X : PCM_STATE_S;
		UT_ASSERT(!cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
		check_borrowed_pin();
	}
}

UT_TEST(cold_or_one_shot_image_returns_to_the_existing_writer_retry)
{
	for (int mode = 0; mode < 3; mode++) {
		reset();
		own.pcm_state = mode == 0 ? PCM_STATE_N : mode == 1 ? PCM_STATE_READ_IMAGE : UINT8_MAX;
		UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
		check_borrowed_pin();
	}
}

UT_TEST(contention_does_not_wait_below_the_heap_lock)
{
	reset();
	contended = true;
	UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
	check_borrowed_pin();
	UT_ASSERT_EQ(snapshots, 0);
}

UT_TEST(raw_af_one_only_requests_the_original_clear)
{
	reset();
	((uint8 *)PageGetContents(image.data))[0] |= VISIBILITYMAP_ALL_FROZEN;
	memcpy(before.data, image.data, BLCKSZ);
	UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
	check_borrowed_pin();
	UT_ASSERT_EQ(conditional_calls, 0);
}

UT_TEST(transitions_and_missing_activation_never_prove_af_zero)
{
	for (int fault = 0; fault < 9; fault++) {
		reset();
		if (fault == 0) own.flags = PCM_OWN_FLAG_REVOKING;
		if (fault == 1) own.flags = PCM_OWN_FLAG_GRANT_PENDING;
		if (fault == 2) own.flags = UINT32_MAX;
		if (fault == 3) own.generation = 0;
		if (fault == 4) own.generation = UINT64_MAX;
		if (fault == 5) own.reservation_token = UINT64_MAX;
		if (fault == 6) own.writer_activation_token = 1;
		if (fault == 7) own.resource_x_activation_generation = 1;
		if (fault == 8) snapshot_result = CLUSTER_PCM_OWN_NOT_READY;
		UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
		check_borrowed_pin();
	}
}

UT_TEST(owned_bytes_must_be_valid_current_bytes)
{
	for (int fault = 0; fault < 7; fault++) {
		reset();
		if (fault == 0) own.buffer_type = BUF_TYPE_CR;
		if (fault == 1) own.buffer_type = BUF_TYPE_PI;
		if (fault == 2) own.semantic_buf_state &= ~BM_VALID;
		if (fault == 3) own.semantic_buf_state &= ~BM_TAG_VALID;
		if (fault == 4) own.semantic_buf_state |= BM_IO_IN_PROGRESS;
		if (fault == 5) own.semantic_buf_state |= BM_IO_ERROR;
		if (fault == 6) own.pcm_state = UINT8_MAX;
		UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
		check_borrowed_pin();
	}
}

UT_TEST(original_pin_and_object_cut_are_not_renewed)
{
	for (int fault = 0; fault < 5; fault++) {
		reset();
		if (fault == 0) active = false;
		if (fault == 1) tracked = false;
		if (fault == 2) absent_ref = true;
		if (fault == 3) pin_ref.refcount = 0;
		if (fault == 4) ko_valid = false;
		UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
		check_borrowed_pin();
	}
}

UT_TEST(malformed_vm_still_requests_clear_after_local_read)
{
	reset();
	((PageHeader)image.data)->pd_block_scn = 0;
	memcpy(before.data, image.data, BLCKSZ);
	UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
	check_borrowed_pin();
}

UT_TEST(wrong_target_does_not_fetch_or_lock)
{
	reset();
	descriptor.tag.relNumber++;
	UT_ASSERT(cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
	check_borrowed_pin();
	UT_ASSERT_EQ(conditional_calls, 0);
}

UT_TEST(cache_off_read_unlock_is_local_and_keeps_the_borrowed_pin)
{
	reset();
	cache_off = true;
	UT_ASSERT(!cluster_heap_lock_vm_needs_clear(&relation, 0, 1));
	check_borrowed_pin();
}

UT_TEST(decoder_error_retires_only_this_local_read_bracket)
{
	volatile bool caught = false;
	reset();
	decoder_error = true;
	PG_TRY();
	{
		(void)cluster_heap_lock_vm_needs_clear(&relation, 0, 1);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_borrowed_pin();
}

#ifdef FIXTURE_HAS_LOCAL_SHARE
UT_TEST(incoming_content_scope_is_preserved_without_recursive_locking)
{
	reset();
	held = true;
	UT_ASSERT(!ClusterConditionalLockBufferShare(1));
	UT_ASSERT(held && pinned);
	UT_ASSERT_EQ(conditional_calls, 0);
	UT_ASSERT_EQ(releases, 0);
	held = false;
}

UT_TEST(local_probe_error_releases_its_provisional_content_lock)
{
	for (int fault = 1; fault <= 2; fault++) {
		volatile bool caught = false;
		reset();
		service_error = fault;
		PG_TRY();
		{
			(void)cluster_heap_lock_vm_needs_clear(&relation, 0, 1);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_borrowed_pin();
	}
}

UT_TEST(local_reader_refuses_missing_or_invalid_borrowed_pins)
{
	reset();
	pinned = false;
	UT_ASSERT(!ClusterConditionalLockBufferShare(1));
	UT_ASSERT(!ClusterConditionalLockBufferShare(InvalidBuffer));
	UT_ASSERT(!ClusterConditionalLockBufferShare(-1));
	UT_ASSERT(!ClusterConditionalLockBufferShare(NBuffers + 1));
	UT_ASSERT_EQ(conditional_calls, 0);
}

UT_TEST(admission_close_does_not_add_remote_cleanup_or_recheck)
{
	reset();
	UT_ASSERT(ClusterConditionalLockBufferShare(1));
	active = false;
	ko_valid = false;
	cache_off = true;
	ClusterUnlockBufferAfterConditionalRead(1);
	check_borrowed_pin();
	UT_ASSERT_EQ(ko_checks, 1);
}

UT_TEST(existing_read_release_wrapper_retires_local_lock_and_original_pin)
{
	reset();
	cache_off = true;
	UT_ASSERT(ClusterConditionalLockBufferShare(1));
	ClusterUnlockReleaseBufferAfterConditionalRead(1);
	UT_ASSERT(!pinned && !held);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(generic_unlocks, 0);
}
#endif

int
main(void)
{
#ifdef FIXTURE_HAS_LOCAL_SHARE
	UT_PLAN(16);
#else
	UT_PLAN(11);
#endif
	UT_RUN(cached_s_and_x_observe_af_zero_without_an_acquiring_entry);
	UT_RUN(cold_or_one_shot_image_returns_to_the_existing_writer_retry);
	UT_RUN(contention_does_not_wait_below_the_heap_lock);
	UT_RUN(raw_af_one_only_requests_the_original_clear);
	UT_RUN(transitions_and_missing_activation_never_prove_af_zero);
	UT_RUN(owned_bytes_must_be_valid_current_bytes);
	UT_RUN(original_pin_and_object_cut_are_not_renewed);
	UT_RUN(malformed_vm_still_requests_clear_after_local_read);
	UT_RUN(wrong_target_does_not_fetch_or_lock);
	UT_RUN(cache_off_read_unlock_is_local_and_keeps_the_borrowed_pin);
	UT_RUN(decoder_error_retires_only_this_local_read_bracket);
#ifdef FIXTURE_HAS_LOCAL_SHARE
	UT_RUN(incoming_content_scope_is_preserved_without_recursive_locking);
	UT_RUN(local_probe_error_releases_its_provisional_content_lock);
	UT_RUN(local_reader_refuses_missing_or_invalid_borrowed_pins);
	UT_RUN(admission_close_does_not_add_remote_cleanup_or_recheck);
	UT_RUN(existing_read_release_wrapper_retires_local_lock_and_original_pin);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
