/*-------------------------------------------------------------------------
 *
 * test_cluster_l1b_fsm_hints.c
 *     Check current page allocation across stale free-space suggestions.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_l1b_fsm_hints.c
 *
 * NOTES
 *     This pgrac-original fixture executes HIO and real page space checks.
 *     Transport, FSM, VM and extension boundaries are controlled leaves.
 *     It does not certify live PCM/GCS or ResourceOwner error cleanup.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/hio.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_space_storage.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true;
bool cluster_shared_config = true;
int cluster_node_id = 0;
int NBuffers = 128, NLocBuffer;
static PGAlignedBlock pages[3], incoming, saved[3];
static RelationData relation_data;
static FormData_pg_class relation_class;
static int pins[3];
static bool locked[3], identity_available;
static BlockNumber target, candidates[4], install_block;
static unsigned candidate_count, candidate_next, reads, acquires, space_reads;
static unsigned feedbacks, extensions, dirties, identity_reads;
static BlockNumber feedback_block[4];
static Size feedback_space[4], requested_space;
static int fail_acquire, fail_code, error_code;
static sigjmp_buf error_jump;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion %s %s:%d\n", condition, file, line);
	abort();
}
bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (level >= ERROR)
		error_code = ERRCODE_INTERNAL_ERROR;
	return level >= ERROR;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}
int
errmsg_internal(const char *format, ...)
{
	(void)format;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	(void)file; (void)line; (void)function;
	siglongjmp(error_jump, 1);
}

static void
raise_error(int code)
{
	error_code = code;
	siglongjmp(error_jump, 1);
}

static unsigned
page_number(Page page)
{
	for (unsigned i = 0; i < lengthof(pages); i++)
		if (page == pages[i].data)
			return i;
	abort();
}

static void
assert_unlocked(void)
{
	for (unsigned i = 0; i < lengthof(pages); i++) {
		UT_ASSERT(!locked[i]);
		UT_ASSERT_EQ(pins[i], 0);
	}
}

static Size
current_space(Page page)
{
	unsigned i = page_number(page);

	UT_ASSERT(locked[i] && pins[i] == 1);
	space_reads++;
	return PageGetHeapFreeSpace(page);
}

static Buffer
read_candidate(Relation relation, BlockNumber block)
{
	UT_ASSERT(relation == &relation_data);
	assert_unlocked();
	UT_ASSERT(block < lengthof(pages));
	if (block >= lengthof(pages))
		abort();
	reads++;
	pins[block]++;
	return block + 1;
}

static Buffer
read_candidate_bi(Relation relation, BlockNumber block, ReadBufferMode mode, BulkInsertState state)
{
	UT_ASSERT_EQ(mode, RBM_NORMAL);
	UT_ASSERT(state == NULL);
	return read_candidate(relation, block);
}

static void
lock_candidate(Buffer buffer, int mode)
{
	unsigned i = buffer - 1;

	UT_ASSERT(i < lengthof(pages) && pins[i] == 1);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(locked[i]);
		locked[i] = false;
		return;
	}
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT(!locked[i]);
	acquires++;
	if (fail_acquire == (int)acquires)
		raise_error(fail_code);
	/* Models successful current acquisition installing authoritative bytes.
	 * The actual PCM/GCS implementation is verified by separate consumers. */
	if (i == install_block)
		pages[i] = incoming;
	locked[i] = true;
}

static void
release_candidate(Buffer buffer)
{
	unsigned i = buffer - 1;

	UT_ASSERT(i < lengthof(pages) && !locked[i] && pins[i] == 1);
	pins[i]--;
}

static void
unlock_release(Buffer buffer)
{
	lock_candidate(buffer, BUFFER_LOCK_UNLOCK);
	release_candidate(buffer);
}

static BlockNumber
fsm_hint(Relation relation, Size needed)
{
	UT_ASSERT(relation == &relation_data);
	assert_unlocked();
	requested_space = needed;
	if (candidate_next == candidate_count)
		return InvalidBlockNumber;
	return candidates[candidate_next++];
}

static BlockNumber
feedback_hint(Relation relation, BlockNumber block, Size actual, Size needed)
{
	assert_unlocked();
	UT_ASSERT(block < lengthof(pages));
	UT_ASSERT_EQ(actual, PageGetHeapFreeSpace(pages[block].data));
	UT_ASSERT(feedbacks < lengthof(feedback_block));
	if (feedbacks >= lengthof(feedback_block))
		abort();
	feedback_block[feedbacks] = block;
	feedback_space[feedbacks++] = actual;
	return fsm_hint(relation, needed);
}

static bool
space_identity(Relation relation, ClusterSpaceIdentity *out)
{
	UT_ASSERT(relation == &relation_data);
	assert_unlocked();
	identity_reads++;
	memset(out, 0, sizeof(*out));
	out->key.locator = relation->rd_locator;
	return identity_available;
}

static bool
vm_boundary(Relation relation, Buffer *first, Buffer *second, BlockNumber block,
	BlockNumber other, Buffer *vm, Buffer *other_vm, BlockNumber frozen)
{
	(void)relation; (void)block; (void)other; (void)other_vm;
	UT_ASSERT_EQ(frozen, InvalidBlockNumber);
	UT_ASSERT(vm == NULL || *vm == InvalidBuffer);
	if (BufferIsValid(*first)) {
		UT_ASSERT(locked[*first - 1]);
		UT_ASSERT(!PageIsAllVisible(pages[*first - 1].data));
	}
	if (BufferIsValid(*second)) {
		UT_ASSERT(locked[*second - 1]);
		UT_ASSERT(!PageIsAllVisible(pages[*second - 1].data));
	}
	return false; /* No VM I/O, positive publication or unlock is modeled. */
}

static Buffer
extend_boundary(Relation relation, BulkInsertState state, int count, bool use_fsm,
	bool *unlocked, const ClusterSpaceIdentity *identity)
{
	(void)state; (void)use_fsm;
	UT_ASSERT(relation == &relation_data && count >= 1 && identity != NULL);
	assert_unlocked();
	extensions++;
	pins[2]++;
	lock_candidate(3, BUFFER_LOCK_EXCLUSIVE);
	*unlocked = false;
	return 3;
}

/* Unsupported boundaries fail rather than pretend to grant authority. */
#undef BufferGetPage
#define BufferGetPage(buffer) ((Page)pages[(buffer) - 1].data)
#define BufferGetBlockNumber(buffer) ((BlockNumber)((buffer) - 1))
#define BufferGetPageSize(buffer) BLCKSZ
#define ReadBuffer read_candidate
#define ReadBufferBI read_candidate_bi
#define LockBuffer lock_candidate
#define ReleaseBuffer release_candidate
#define UnlockReleaseBuffer unlock_release
#define GetPageWithFreeSpace fsm_hint
#define RecordAndGetPageWithFreeSpace feedback_hint
#define RecordPageWithFreeSpace(relation, block, space) abort()
#undef RelationGetTargetBlock
#define RelationGetTargetBlock(relation) target
#undef RelationSetTargetBlock
#define RelationSetTargetBlock(relation, block) (target = (block))
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks(relation) lengthof(pages)
#define RelationAddBlocks extend_boundary
#define cluster_smgr_which_for(locator, backend) 1
#define cluster_space_relation_get_identity space_identity
#undef RelationNeedsWAL
#define RelationNeedsWAL(relation) true
#define cluster_hw_lease_active() false
#define cluster_hio_lease_target_block(relation) InvalidBlockNumber
#define cluster_space_init_heap_buffer_wal(identity, buffer) (abort(), false)
#define PageInitHeapPage(page, size, special) abort()
#define MarkBufferDirty(buffer) (dirties++, abort())
#define GetVisibilityMapPins vm_boundary
#define visibilitymap_pin(relation, block, buffer) abort()
#define visibilitymap_pin_ok(block, buffer) (abort(), false)
#define cluster_hio_lock_buffer_pair(relation, first, second) abort()
#define cluster_hio_refresh_buffer_pair(relation, first, second, backoff) abort()
#define ConditionalLockBufferWithPinCheck(buffer, invalid) (abort(), false)
#define PageGetHeapFreeSpace current_space
#include <test_cluster_l1b_fsm_hints.inc>
#undef PageGetHeapFreeSpace

static void
reset_fixture(void)
{
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relation_class, 0, sizeof(relation_class));
	relation_data.rd_rel = &relation_class;
	relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	/* Do not inherit the native-only weak node identity from port stubs. */
	UT_ASSERT(cluster_hio_vm_repin_enabled(&relation_data));
	for (unsigned i = 0; i < lengthof(pages); i++) {
		char tuple[64] = { 0 };

		PageInit(pages[i].data, BLCKSZ, 0);
		tuple[63] = 40 + i;
		UT_ASSERT_EQ(PageAddItemExtended(pages[i].data, tuple, sizeof(tuple), 1, PAI_IS_HEAP), 1);
	}
	memset(pins, 0, sizeof(pins));
	memset(locked, 0, sizeof(locked));
	identity_available = true;
	target = install_block = InvalidBlockNumber;
	candidate_count = candidate_next = reads = acquires = space_reads = 0;
	feedbacks = extensions = dirties = identity_reads = 0;
	requested_space = 0;
	fail_acquire = -1;
	fail_code = ERRCODE_QUERY_CANCELED;
	error_code = 0;
}

static void
limit_space(unsigned page, Size available)
{
	PageHeader header = (PageHeader)pages[page].data;

	/* Preserve the tuple's bytes outside the free interval. */
	header->pd_upper = header->pd_lower + available + sizeof(ItemIdData);
	UT_ASSERT_EQ(PageGetHeapFreeSpace(pages[page].data), available);
}

static bool
invoke(int options, Buffer *result)
{
	Buffer vm = InvalidBuffer;

	if (sigsetjmp(error_jump, 0) != 0)
		return false;
	*result = RelationGetBufferForTuple(&relation_data, 77, NULL, options,
									  NULL, &vm, NULL, 1);
	UT_ASSERT_EQ(vm, InvalidBuffer);
	return true;
}

static void
finish(Buffer result)
{
	UT_ASSERT_EQ(dirties, 0);
	UT_ASSERT(locked[result - 1] && pins[result - 1] == 1);
	unlock_release(result);
	assert_unlocked();
}

UT_TEST(stale_high_hints_retry_with_real_current_space_feedback)
{
	Buffer result = InvalidBuffer;
	reset_fixture();
	limit_space(0, 0);
	limit_space(1, 16);
	memcpy(saved, pages, sizeof(pages));
	candidates[0] = 0; candidates[1] = 1; candidates[2] = 2;
	candidate_count = 3;
	UT_ASSERT(invoke(0, &result));
	UT_ASSERT_EQ(result, 3);
	UT_ASSERT_EQ(reads, 3);
	UT_ASSERT_EQ(acquires, 3);
	UT_ASSERT_EQ(space_reads, 3);
	UT_ASSERT_EQ(feedbacks, 2);
	UT_ASSERT_EQ(feedback_block[0], 0);
	UT_ASSERT_EQ(feedback_space[0], 0);
	UT_ASSERT_EQ(feedback_block[1], 1);
	UT_ASSERT_EQ(feedback_space[1], 16);
	UT_ASSERT_EQ(extensions, 0);
	UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
	finish(result);
}

UT_TEST(cached_and_fsm_candidates_recheck_space_after_current_install)
{
	for (unsigned cached = 0; cached < 2; cached++) {
		Buffer result = InvalidBuffer;
		reset_fixture();
		limit_space(0, 0);
		incoming = pages[0];
		PageInit(pages[0].data, BLCKSZ, 0);
		install_block = 0;
		if (cached)
			target = 0;
		else
			candidates[candidate_count++] = 0;
		candidates[candidate_count++] = 1;
		UT_ASSERT(invoke(0, &result));
		UT_ASSERT_EQ(result, 2);
		UT_ASSERT_EQ(acquires, 2);
		UT_ASSERT_EQ(feedbacks, 1);
		UT_ASSERT_EQ(feedback_space[0], 0);
		UT_ASSERT_EQ(memcmp(pages[0].data, incoming.data, BLCKSZ), 0);
		finish(result);
	}
}

UT_TEST(current_install_can_replace_an_old_full_image)
{
	Buffer result = InvalidBuffer;
	reset_fixture();
	incoming = pages[0];
	limit_space(0, 0);
	install_block = 0;
	candidates[candidate_count++] = 0;
	UT_ASSERT(invoke(0, &result));
	UT_ASSERT_EQ(result, 1);
	UT_ASSERT_EQ(feedbacks + extensions, 0);
	UT_ASSERT_EQ(memcmp(pages[0].data, incoming.data, BLCKSZ), 0);
	finish(result);
}

UT_TEST(authority_failure_or_cancel_never_uses_the_hint_as_permission)
{
	static const int codes[] = { ERRCODE_QUERY_CANCELED, MAKE_SQLSTATE('5','3','R','6','0') };
	for (unsigned code = 0; code < lengthof(codes); code++)
		for (unsigned position = 1; position <= 2; position++) {
			Buffer result = InvalidBuffer;
			reset_fixture();
			limit_space(0, 0);
			memcpy(saved, pages, sizeof(pages));
			candidates[0] = 0; candidates[1] = 1;
			candidate_count = 2;
			fail_acquire = position;
			fail_code = codes[code];
			UT_ASSERT(!invoke(0, &result));
			UT_ASSERT_EQ(error_code, codes[code]);
			UT_ASSERT_EQ(acquires, position);
			UT_ASSERT_EQ(space_reads, position - 1);
			UT_ASSERT_EQ(feedbacks, position - 1);
			UT_ASSERT_EQ(dirties + extensions, 0);
			UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
			/* The caller's ResourceOwner cleanup is outside this fixture. */
			for (unsigned i = 0; i < lengthof(pages); i++) {
				UT_ASSERT(!locked[i]);
				if (pins[i])
					release_candidate(i + 1);
			}
			assert_unlocked();
		}
}

UT_TEST(aligned_size_threshold_uses_real_page_space)
{
	Buffer result = InvalidBuffer;
	reset_fixture();
	limit_space(0, 79);
	limit_space(1, 80);
	candidates[0] = 0; candidates[1] = 1;
	candidate_count = 2;
	UT_ASSERT(invoke(0, &result));
	UT_ASSERT_EQ(result, 2);
	UT_ASSERT_EQ(requested_space, 80);
	UT_ASSERT_EQ(feedback_space[0], 79);
	finish(result);
}

UT_TEST(lp_dead_exhaustion_is_not_reusable_line_pointer_space)
{
	for (unsigned unused = 0; unused < 2; unused++) {
		Buffer result = InvalidBuffer;
		PageHeader header;
		reset_fixture();
		PageInit(pages[0].data, BLCKSZ, 0);
		header = (PageHeader)pages[0].data;
		header->pd_lower += MaxHeapTuplesPerPage * sizeof(ItemIdData);
		for (unsigned off = 1; off <= MaxHeapTuplesPerPage; off++)
			ItemIdSetDead(PageGetItemId(pages[0].data, off));
		if (unused) {
			ItemIdSetUnused(PageGetItemId(pages[0].data, 1));
			PageSetHasFreeLinePointers(pages[0].data);
		}
		memcpy(saved, pages, sizeof(pages));
		candidates[0] = 0; candidates[1] = 1;
		candidate_count = 2;
		UT_ASSERT(invoke(0, &result));
		UT_ASSERT_EQ(result, unused ? 1 : 2);
		UT_ASSERT_EQ(feedbacks, unused ? 0 : 1);
		UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
		finish(result);
	}
}

UT_TEST(missing_space_identity_fails_before_hint_or_page_access)
{
	Buffer result = InvalidBuffer;
	reset_fixture();
	identity_available = false;
	candidates[candidate_count++] = 0;
	memcpy(saved, pages, sizeof(pages));
	UT_ASSERT(!invoke(0, &result));
	UT_ASSERT_EQ(identity_reads, 1);
	UT_ASSERT_EQ(candidate_next + reads + acquires + space_reads + extensions + dirties, 0);
	UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
	assert_unlocked();
}

UT_TEST(fsm_miss_or_skip_still_acquires_and_checks_real_target)
{
	for (unsigned skip = 0; skip < 2; skip++) {
		Buffer result = InvalidBuffer;
		reset_fixture();
		if (skip) {
			target = 0;
			limit_space(0, 0);
		}
		UT_ASSERT(invoke(skip ? HEAP_INSERT_SKIP_FSM : 0, &result));
		UT_ASSERT_EQ(result, 3);
		UT_ASSERT_EQ(acquires, skip ? 2 : 1);
		UT_ASSERT_EQ(space_reads, skip ? 2 : 1);
		UT_ASSERT_EQ(extensions, skip);
		UT_ASSERT_EQ(candidate_next + feedbacks, 0);
		finish(result);
	}
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(stale_high_hints_retry_with_real_current_space_feedback);
	UT_RUN(cached_and_fsm_candidates_recheck_space_after_current_install);
	UT_RUN(current_install_can_replace_an_old_full_image);
	UT_RUN(authority_failure_or_cancel_never_uses_the_hint_as_permission);
	UT_RUN(aligned_size_threshold_uses_real_page_space);
	UT_RUN(lp_dead_exhaustion_is_not_reusable_line_pointer_space);
	UT_RUN(missing_space_identity_fails_before_hint_or_page_access);
	UT_RUN(fsm_miss_or_skip_still_acquires_and_checks_real_target);
	UT_DONE();
	return ut_failed_count != 0;
}
