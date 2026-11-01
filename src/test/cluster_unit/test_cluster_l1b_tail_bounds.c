/*-------------------------------------------------------------------------
 *
 * test_cluster_l1b_tail_bounds.c
 *     Exercise the original VACUUM tail rescan and truncation boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_l1b_tail_bounds.c
 *
 * NOTES
 *     This pgrac-original test uses native VACUUM decisions and real pages.
 *     Relation locks, buffer transport, scheduling and structural storage are
 *     controlled boundaries. No file is truncated or shared authority granted.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_space_storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "utils/backend_progress.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool VacuumFailsafeActive;
int old_snapshot_threshold = -1;
int NBuffers = 128, NLocBuffer;
static PGAlignedBlock pages[64], saved[64], incoming;
static RelationData relation_data;
static FormData_pg_class relation_class;
static BlockNumber actual_blocks, install_block;
/* Track only the additional AccessExclusiveLock, not VACUUM's caller lock. */
static unsigned reads, prefetches, buffer_locks, pins, relation_locks;
static unsigned lock_attempts, denied_attempts, waits, resets, waiter_checks;
static unsigned waiter_at, truncates, progress_calls, checks;
static BlockNumber truncate_sizes[4];
static int cancel_wait, cancel_read, cancel_lock, error_code, injected_code;
static bool install_on_relock, defer_relock_once, install_during_wait;
static int64 clock_ns;
static Latch fixture_latch;
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
	(void) domain;
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
	(void) format;
	return 0;
}
int
errmsg_internal(const char *format, ...)
{
	(void) format;
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	(void) file; (void) line; (void) function;
	siglongjmp(error_jump, 1);
}

static void
raise_error(void)
{
	error_code = injected_code;
	siglongjmp(error_jump, 1);
}

static void
no_buffer_resources(void)
{
	UT_ASSERT_EQ(buffer_locks + pins, 0);
}

static void
put_item(Page page, unsigned state)
{
	char tuple[64] = { 0 };
	ItemId item;

	PageInit(page, BLCKSZ, 0);
	tuple[63] = 42;
	UT_ASSERT_EQ(PageAddItemExtended(page, tuple, sizeof(tuple), 1, PAI_IS_HEAP), 1);
	item = PageGetItemId(page, 1);
	if (state == LP_DEAD)
		ItemIdSetDead(item);
	else if (state == LP_REDIRECT)
		/* An isolated marker: this tail scan never interprets HOT targets. */
		ItemIdSetRedirect(item, 1);
	else if (state == LP_UNUSED)
		ItemIdSetUnused(item);
	else
		UT_ASSERT_EQ(state, LP_NORMAL);
}

static bool
relation_try_lock(Relation relation, LOCKMODE mode)
{
	UT_ASSERT(relation == &relation_data && mode == AccessExclusiveLock);
	no_buffer_resources();
	UT_ASSERT_EQ(relation_locks, 0);
	lock_attempts++;
	if (lock_attempts <= denied_attempts)
		return false;
	if (defer_relock_once && truncates == 1) {
		defer_relock_once = false;
		return false;
	}
	if (install_on_relock && truncates == 1)
		pages[31] = incoming;
	relation_locks = 1;
	return true;
}

static void
relation_unlock(Relation relation, LOCKMODE mode)
{
	UT_ASSERT(relation == &relation_data && mode == AccessExclusiveLock);
	no_buffer_resources();
	UT_ASSERT_EQ(relation_locks, 1);
	relation_locks = 0;
}

static BlockNumber
relation_size(Relation relation)
{
	UT_ASSERT(relation == &relation_data);
	UT_ASSERT_EQ(relation_locks, 1);
	no_buffer_resources();
	return actual_blocks;
}

static bool
relation_waiters(Relation relation, LOCKMODE mode)
{
	UT_ASSERT(relation == &relation_data && mode == AccessExclusiveLock);
	UT_ASSERT_EQ(relation_locks, 1);
	no_buffer_resources();
	waiter_checks++;
	return waiter_checks == waiter_at;
}

static instr_time
read_clock(void)
{
	instr_time value;

	value.ticks = clock_ns;
	clock_ns += 25 * NS_PER_MS;
	return value;
}

static int
wait_latch(Latch *latch, int events, long timeout, uint32 wait_event)
{
	UT_ASSERT(latch == &fixture_latch);
	UT_ASSERT_EQ(events, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH);
	UT_ASSERT_EQ(timeout, 50);
	UT_ASSERT_EQ(wait_event, WAIT_EVENT_VACUUM_TRUNCATE);
	UT_ASSERT_EQ(relation_locks, 0);
	no_buffer_resources();
	waits++;
	if (install_during_wait)
		pages[7] = incoming;
	if ((int) waits == cancel_wait)
		raise_error();
	return WL_TIMEOUT;
}

static void
reset_latch(Latch *latch)
{
	UT_ASSERT(latch == &fixture_latch);
	UT_ASSERT_EQ(relation_locks, 0);
	no_buffer_resources();
	resets++;
}

static void
check_interrupts(void)
{
	/* Cancellation is injected at the actual wait/read/acquire leaves. */
	checks++;
}

static void
prefetch_page(Relation relation, ForkNumber fork, BlockNumber block)
{
	UT_ASSERT(relation == &relation_data && fork == MAIN_FORKNUM);
	UT_ASSERT(block < actual_blocks);
	UT_ASSERT_EQ(relation_locks, 1);
	no_buffer_resources();
	prefetches++;
}

static Buffer
read_page(Relation relation, ForkNumber fork, BlockNumber block,
		  ReadBufferMode mode, BufferAccessStrategy strategy)
{
	UT_ASSERT(relation == &relation_data && fork == MAIN_FORKNUM);
	UT_ASSERT(mode == RBM_NORMAL && strategy == NULL);
	UT_ASSERT_EQ(relation_locks, 1);
	no_buffer_resources();
	UT_ASSERT(block < actual_blocks && block < lengthof(pages));
	if (block >= actual_blocks || block >= lengthof(pages))
		abort();
	reads++;
	if ((int) reads == cancel_read)
		raise_error();
	pins++;
	return block + 1;
}

static void
lock_page(Buffer buffer, int mode)
{
	UT_ASSERT(buffer > 0 && buffer <= (Buffer) lengthof(pages));
	UT_ASSERT_EQ(mode, BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(relation_locks, 1);
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(buffer_locks, 0);
	if ((int) reads == cancel_lock)
		raise_error();
	if ((BlockNumber) (buffer - 1) == install_block)
		pages[buffer - 1] = incoming;
	buffer_locks++;
}

static void
unlock_release_page(Buffer buffer)
{
	UT_ASSERT(buffer > 0 && buffer <= (Buffer) lengthof(pages));
	UT_ASSERT_EQ(relation_locks, 1);
	UT_ASSERT_EQ(buffer_locks, 1);
	UT_ASSERT_EQ(pins, 1);
	buffer_locks--;
	pins--;
}

static void
truncate_boundary(Relation relation, BlockNumber size)
{
	UT_ASSERT(relation == &relation_data);
	UT_ASSERT_EQ(relation_locks, 1);
	no_buffer_resources();
	UT_ASSERT(size <= actual_blocks && actual_blocks <= lengthof(pages));
	if (size > actual_blocks || actual_blocks > lengthof(pages) ||
		truncates >= lengthof(truncate_sizes))
		abort();
	/* An independent check of proposed suffix contents, not SPACE authority. */
	for (BlockNumber block = size; block < actual_blocks; block++) {
		Page page = pages[block].data;

		if (PageIsNew(page))
			continue;
		for (OffsetNumber off = FirstOffsetNumber;
			 off <= PageGetMaxOffsetNumber(page); off++)
			UT_ASSERT(!ItemIdIsUsed(PageGetItemId(page, off)));
	}
	truncate_sizes[truncates++] = size;
	actual_blocks = size;
}

static void
progress_update(int index, int64 value)
{
	UT_ASSERT(index == PROGRESS_VACUUM_PHASE &&
			  value == PROGRESS_VACUUM_PHASE_TRUNCATE);
	progress_calls++;
}

#undef BufferGetPage
#define BufferGetPage(buffer) ((Page) pages[(buffer) - 1].data)
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks relation_size
#define ConditionalLockRelation relation_try_lock
#define UnlockRelation relation_unlock
#define LockHasWaitersRelation relation_waiters
#define RelationTruncate truncate_boundary
#define ReadBufferExtended read_page
#define PrefetchBuffer prefetch_page
#define LockBuffer lock_page
#define UnlockReleaseBuffer unlock_release_page
#define WaitLatch wait_latch
#define ResetLatch reset_latch
#define MyLatch (&fixture_latch)
#define pgstat_progress_update_param progress_update
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() check_interrupts()
#undef INSTR_TIME_SET_CURRENT
#define INSTR_TIME_SET_CURRENT(t) ((t) = read_clock())
#include <test_cluster_l1b_tail_bounds.inc>

static LVRelState state;

static void
reset_fixture(BlockNumber blocks, BlockNumber floor)
{
	UT_ASSERT(blocks <= lengthof(pages) && floor <= blocks);
	if (blocks > lengthof(pages) || floor > blocks)
		abort();
	memset(&state, 0, sizeof(state));
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relation_class, 0, sizeof(relation_class));
	relation_data.rd_rel = &relation_class;
	relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	for (unsigned i = 0; i < lengthof(pages); i++)
		PageInit(pages[i].data, BLCKSZ, 0);
	state.rel = &relation_data;
	state.relname = "tail_fixture";
	state.rel_pages = actual_blocks = blocks;
	state.nonempty_pages = floor;
	state.do_rel_truncate = true;
	VacuumFailsafeActive = false;
	old_snapshot_threshold = -1;
	reads = prefetches = buffer_locks = pins = relation_locks = 0;
	lock_attempts = denied_attempts = waits = resets = waiter_checks = 0;
	waiter_at = truncates = progress_calls = checks = 0;
	memset(truncate_sizes, 0, sizeof(truncate_sizes));
	cancel_wait = cancel_read = cancel_lock = -1;
	error_code = 0;
	injected_code = ERRCODE_QUERY_CANCELED;
	install_block = InvalidBlockNumber;
	install_on_relock = defer_relock_once = install_during_wait = false;
	clock_ns = 0;
}

static bool
invoke(void)
{
	if (sigsetjmp(error_jump, 0) != 0)
		return false;
	lazy_truncate_heap(&state);
	UT_ASSERT_EQ(progress_calls, 1);
	UT_ASSERT_EQ(state.phase, VACUUM_ERRCB_PHASE_TRUNCATE);
	UT_ASSERT_EQ(relation_locks, 0);
	no_buffer_resources();
	return true;
}

UT_TEST(all_nonunused_line_pointer_states_keep_the_tail)
{
	static const unsigned kinds[] = { LP_NORMAL, LP_REDIRECT, LP_DEAD };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		reset_fixture(8, 1);
		put_item(pages[7].data, kinds[i]);
		memcpy(saved, pages, sizeof(pages));
		UT_ASSERT(invoke());
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(truncates + state.removed_pages, 0);
		UT_ASSERT_EQ(state.rel_pages, 8);
		UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
	}
}

UT_TEST(interior_lp_dead_keeps_its_page_and_only_empty_suffix_is_proposed)
{
	reset_fixture(8, 1);
	put_item(pages[4].data, LP_DEAD);
	memcpy(saved, pages, sizeof(pages));
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(reads, 4);
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(truncate_sizes[0], 5);
	UT_ASSERT_EQ(state.rel_pages, 5);
	UT_ASSERT_EQ(state.removed_pages, 3);
	UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
}

UT_TEST(lock_installed_repopulated_tail_is_not_truncated)
{
	reset_fixture(8, 1);
	put_item(incoming.data, LP_NORMAL);
	install_block = 7;
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(truncates + state.removed_pages, 0);
	UT_ASSERT_EQ(state.rel_pages, 8);
	UT_ASSERT_EQ(memcmp(pages[7].data, incoming.data, BLCKSZ), 0);
}

UT_TEST(length_drift_after_exclusive_lock_abandons_the_old_scan)
{
	static const BlockNumber lengths[] = { 7, 9 };

	for (unsigned i = 0; i < lengthof(lengths); i++) {
		reset_fixture(8, 1);
		actual_blocks = lengths[i];
		UT_ASSERT(invoke());
		UT_ASSERT_EQ(reads + prefetches + truncates, 0);
		UT_ASSERT_EQ(state.rel_pages, 8);
		UT_ASSERT_EQ(state.removed_pages, 0);
	}
}

UT_TEST(exclusive_conflicts_wait_without_held_resources_and_preserve_cancel)
{
	reset_fixture(8, 1);
	denied_attempts = 1000;
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(lock_attempts, 101);
	UT_ASSERT_EQ(waits, 100);
	UT_ASSERT_EQ(resets, 100);
	UT_ASSERT_EQ(reads + truncates + error_code, 0);
	UT_ASSERT_EQ(state.rel_pages, 8);

	reset_fixture(8, 1);
	denied_attempts = 1;
	cancel_wait = 1;
	UT_ASSERT(!invoke());
	UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(relation_locks + buffer_locks + pins + reads + truncates, 0);

	reset_fixture(8, 1);
	denied_attempts = 2;
	put_item(incoming.data, LP_DEAD);
	install_during_wait = true;
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(lock_attempts, 3);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(truncates, 0);
	UT_ASSERT_EQ(state.rel_pages, 8);
}

UT_TEST(waiter_yields_only_scanned_suffix_then_rechecks_after_relock)
{
	reset_fixture(64, 1);
	waiter_at = 2;
	put_item(incoming.data, LP_DEAD);
	install_on_relock = defer_relock_once = true;
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(lock_attempts, 3);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(truncate_sizes[0], 32);
	UT_ASSERT_EQ(reads, 33);
	UT_ASSERT_EQ(state.rel_pages, 32);
	UT_ASSERT_EQ(state.removed_pages, 32);
	UT_ASSERT_EQ(memcmp(pages[31].data, incoming.data, BLCKSZ), 0);
}

UT_TEST(new_empty_and_unused_pages_stop_at_known_nonempty_floor)
{
	reset_fixture(8, 3);
	memset(pages[7].data, 0, BLCKSZ);
	put_item(pages[5].data, LP_UNUSED);
	put_item(pages[2].data, LP_DEAD);
	memcpy(saved, pages, sizeof(pages));
	UT_ASSERT(invoke());
	UT_ASSERT_EQ(reads, 5);
	UT_ASSERT_EQ(truncate_sizes[0], 3);
	UT_ASSERT_EQ(state.rel_pages, 3);
	UT_ASSERT_EQ(state.removed_pages, 5);
	UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
}

UT_TEST(read_or_share_lock_error_never_proposes_a_partial_suffix)
{
	static const int codes[] = { ERRCODE_QUERY_CANCELED, MAKE_SQLSTATE('5','3','R','6','0') };

	for (unsigned code = 0; code < lengthof(codes); code++)
		for (unsigned phase = 0; phase < 2; phase++) {
			reset_fixture(8, 1);
			injected_code = codes[code];
			if (phase == 0)
				cancel_read = 2;
			else
				cancel_lock = 2;
			memcpy(saved, pages, sizeof(pages));
			UT_ASSERT(!invoke());
			UT_ASSERT_EQ(error_code, codes[code]);
			UT_ASSERT_EQ(truncates + state.removed_pages, 0);
			UT_ASSERT_EQ(state.rel_pages, 8);
			UT_ASSERT_EQ(memcmp(saved, pages, sizeof(pages)), 0);
			UT_ASSERT_EQ(relation_locks, 1);
			UT_ASSERT_EQ(buffer_locks, 0);
			UT_ASSERT_EQ(pins, phase);
			/* Actual ResourceOwner cleanup is outside this controlled fixture. */
			pins = 0;
			relation_unlock(&relation_data, AccessExclusiveLock);
		}
}

UT_TEST(original_eligibility_restrictions_and_size_boundaries_remain)
{
	static const struct { BlockNumber pages, floor; bool allowed; } cases[] = {
		{ 100, 95, false }, { 100, 94, true },
		{ 32000, 31001, false }, { 32000, 31000, true }
	};

	reset_fixture(8, 1);
	state.do_rel_truncate = false;
	UT_ASSERT(!should_attempt_truncation(&state));
	state.do_rel_truncate = true;
	VacuumFailsafeActive = true;
	UT_ASSERT(!should_attempt_truncation(&state));
	VacuumFailsafeActive = false;
	old_snapshot_threshold = 0;
	UT_ASSERT(!should_attempt_truncation(&state));
	old_snapshot_threshold = -1;
	for (unsigned i = 0; i < lengthof(cases); i++) {
		state.rel_pages = cases[i].pages;
		state.nonempty_pages = cases[i].floor;
		UT_ASSERT_EQ(should_attempt_truncation(&state), cases[i].allowed);
	}
	UT_ASSERT_EQ(lock_attempts + waits + reads + truncates, 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(all_nonunused_line_pointer_states_keep_the_tail);
	UT_RUN(interior_lp_dead_keeps_its_page_and_only_empty_suffix_is_proposed);
	UT_RUN(lock_installed_repopulated_tail_is_not_truncated);
	UT_RUN(length_drift_after_exclusive_lock_abandons_the_old_scan);
	UT_RUN(exclusive_conflicts_wait_without_held_resources_and_preserve_cancel);
	UT_RUN(waiter_yields_only_scanned_suffix_then_rechecks_after_relock);
	UT_RUN(new_empty_and_unused_pages_stop_at_known_nonempty_floor);
	UT_RUN(read_or_share_lock_error_never_proposes_a_partial_suffix);
	UT_RUN(original_eligibility_restrictions_and_size_boundaries_remain);
	UT_DONE();
	return ut_failed_count != 0;
}
