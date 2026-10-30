/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_vacuum_fsm_order.c
 *    Keep one-pass VACUUM's FSM acquisition outside heap/VM content locks.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_vacuum_fsm_order.c
 *
 * NOTES
 *    Executes the unchanged extracted caller branch and heap-acquire helper.
 *    Cleanup, publication, proof collection and FSM I/O are controlled leaves;
 *    the separate empty-page tests exercise the real cleanup and publisher.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/visibilitymap.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int NBuffers = 3, NLocBuffer;

typedef struct LVRelState {
	Relation rel;
	bool versioned;
	bool shared_maintenance_restricted;
	int nindexes;
	BufferAccessStrategy bstrategy;
	VacDeadItems *dead_items;
} LVRelState;

static bool heap_pin, vm_pin, heap_lock, vm_lock, writer;
static bool cleanup_available, cancel_fsm, cancel_proof, replace_vm;
static unsigned reads, conditional_locks, ordinary_locks, cleanup_locks;
static unsigned repins, misses, pin_reads, fsm_calls, record_calls, violations;
static BlockNumber range_start, range_end;
static sigjmp_buf cancel_jump;
static char events[32];
static unsigned event_count;

static void
note(char event)
{
	UT_ASSERT(event_count + 1 < sizeof(events));
	events[event_count++] = event;
	events[event_count] = '\0';
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

#ifdef FIXTURE_NO_ASSERTS
#undef Assert
#define Assert(condition) ((void)0)
#endif

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && vm_pin && !vm_lock);
	vm_pin = false;
}

Buffer
ReadBufferExtended(Relation relation, ForkNumber fork, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	(void)relation;
	(void)block;
	(void)strategy;
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	UT_ASSERT_EQ(mode, RBM_NORMAL);
	UT_ASSERT(!vm_pin && !heap_pin && !heap_lock && !vm_lock);
	heap_pin = true;
	reads++;
	return 1;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(heap_pin && !vm_lock);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(heap_lock);
		heap_lock = writer = false;
		return;
	}
	UT_ASSERT(!vm_pin && !heap_lock);
	heap_lock = true;
	writer = mode == BUFFER_LOCK_EXCLUSIVE;
	ordinary_locks++;
}

bool
ConditionalLockBufferForCleanup(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && heap_pin && !vm_pin && !heap_lock);
	conditional_locks++;
	if (!cleanup_available)
		return false;
	heap_lock = true;
	/* A successful local conditional lock does not acquire a PCM writer. */
	writer = false;
	return true;
}

bool
IsBufferCleanupOK(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && heap_pin && heap_lock && writer && !vm_pin);
	return cleanup_available;
}

void
LockBufferForCleanup(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	cleanup_locks++;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && heap_pin && heap_lock && !vm_lock);
	heap_pin = heap_lock = writer = false;
	note('U');
}

bool
visibilitymap_pin_recent(Relation relation, BlockNumber block, Buffer recent, Buffer *vm)
{
	(void)relation;
	(void)block;
	UT_ASSERT(recent == 2 && !vm_pin && heap_pin && heap_lock);
	UT_ASSERT_EQ(*vm, InvalidBuffer);
	repins++;
	if (misses > 0) {
		misses--;
		return false;
	}
	vm_pin = true;
	*vm = 2;
	return true;
}

void
visibilitymap_pin(Relation relation, BlockNumber block, Buffer *vm)
{
	(void)relation;
	(void)block;
	UT_ASSERT(!heap_pin && !heap_lock && !vm_lock && !vm_pin);
	vm_pin = true;
	*vm = 2;
	pin_reads++;
}

#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#include "test_cluster_vm_vacuum_acquire.inc"

static void
lazy_vacuum_heap_page(LVRelState *state, BlockNumber block, Buffer buffer, int index, Buffer *vm,
					  bool cleanup)
{
	(void)block;
	UT_ASSERT(state->nindexes == 0 && buffer == 1 && index == 0 && cleanup);
	UT_ASSERT(heap_pin && heap_lock && writer && vm_pin && !vm_lock);
	UT_ASSERT(*vm == 2 && state->dead_items->num_items > 0);
	/* Model only the caller contract: publication ends with heap cleanup
	 * restored and VM unlocked, possibly replacing the caller's VM pin. */
	if (replace_vm)
		*vm = 3;
	note('C');
}

static void
lazy_validate_shared_freeze(LVRelState *state, Page page, BlockNumber block, Buffer buffer)
{
	(void)state;
	(void)page;
	(void)block;
	UT_ASSERT(buffer == 1 && heap_pin && heap_lock && !vm_lock);
	note('V');
}

static void
lazy_prepare_shared_freeze(LVRelState *state, Buffer recent, Buffer *vm)
{
	(void)state;
	UT_ASSERT(recent == 1 && vm_pin && *vm == (replace_vm ? 3 : 2));
	UT_ASSERT(!heap_pin && !heap_lock && !vm_lock);
	note('P');
	if (cancel_proof)
		siglongjmp(cancel_jump, 1);
}

static Size
fixture_free_space(Page page)
{
	UT_ASSERT(page == (Page)events && heap_pin && heap_lock);
	return 1234;
}

void
FreeSpaceMapVacuumRange(Relation relation, BlockNumber start, BlockNumber end)
{
	(void)relation;
	/* A real FSM miss may now fetch a different block. Record the illegal
	 * nesting instead of aborting, so the parent produces a useful RED. */
	if (heap_pin || heap_lock || vm_lock)
		violations++;
	range_start = start;
	range_end = end;
	fsm_calls++;
	note('F');
	if (cancel_fsm)
		siglongjmp(cancel_jump, 1);
}

void
RecordPageWithFreeSpace(Relation relation, BlockNumber block, Size available)
{
	(void)relation;
	(void)block;
	UT_ASSERT(!heap_pin && !heap_lock && !vm_lock);
	UT_ASSERT_EQ(available, 1234);
	record_calls++;
	note('S');
}

/* Keep the whole original one-pass branch, including continue and no-op
 * gates. The one-iteration loop stands for its enclosing heap scan only. */
static void
scan_onepass(LVRelState *vacrel, BlockNumber blkno, bool has_dead, BlockNumber *next_fsm)
{
	VacDeadItems *dead_items = vacrel->dead_items;
	Buffer buf = 1, vmbuffer = 2;
	Page page = (Page)events;
	BlockNumber next_fsm_block_to_vacuum = *next_fsm;
	struct {
		bool has_lpdead_items;
	} prunestate = { has_dead };

#define PageGetHeapFreeSpace fixture_free_space
	for (unsigned once = 0; once < 1; once++) {
#include "test_cluster_vm_vacuum_onepass.inc"
	}
#undef PageGetHeapFreeSpace
	*next_fsm = next_fsm_block_to_vacuum;
}

static LVRelState
reset(VacDeadItems *dead)
{
	LVRelState state = { 0 };

	heap_pin = heap_lock = writer = true;
	vm_pin = true;
	vm_lock = false;
	cleanup_available = true;
	cancel_fsm = cancel_proof = replace_vm = false;
	reads = conditional_locks = ordinary_locks = cleanup_locks = 0;
	repins = misses = pin_reads = fsm_calls = record_calls = violations = 0;
	range_start = range_end = InvalidBlockNumber;
	event_count = 0;
	memset(events, 0, sizeof(events));
	state.versioned = state.shared_maintenance_restricted = true;
	state.dead_items = dead;
	return state;
}

UT_TEST(test_fsm_range_follows_heap_and_vm_release)
{
	for (unsigned replace = 0; replace < 2; replace++)
		for (unsigned interval = 0; interval < 3; interval++) {
			VacDeadItems dead = { 0 };
			LVRelState state = reset(&dead);
			BlockNumber next = 17;
			BlockNumber block = next + VACUUM_FSM_EVERY_PAGES + interval - 1;

			replace_vm = replace;
			dead.num_items = 7;
			scan_onepass(&state, block, true, &next);
			UT_ASSERT_EQ(violations, 0);
			UT_ASSERT(strcmp(events, interval == 0 ? "CVUPS" : "CVUPFS") == 0);
			UT_ASSERT_EQ(fsm_calls, interval != 0);
			UT_ASSERT_EQ(record_calls, 1);
			UT_ASSERT_EQ(dead.num_items, 0);
			UT_ASSERT_EQ(next, interval == 0 ? 17 : block);
			if (interval != 0) {
				UT_ASSERT_EQ(range_start, 17);
				UT_ASSERT_EQ(range_end, block);
			}
			UT_ASSERT(!heap_pin && !heap_lock && vm_pin && !vm_lock);
		}
}

UT_TEST(test_indexed_and_no_dead_paths_do_not_run_onepass_work)
{
	for (unsigned indexed = 0; indexed < 2; indexed++) {
		VacDeadItems dead = { 0 };
		LVRelState state = reset(&dead);
		BlockNumber next = 0;

		state.nindexes = indexed;
		dead.num_items = indexed ? 2 : 0;
		scan_onepass(&state, VACUUM_FSM_EVERY_PAGES, indexed, &next);
		UT_ASSERT_EQ(event_count + fsm_calls + record_calls + violations, 0);
		UT_ASSERT_EQ(next, 0);
		UT_ASSERT_EQ(dead.num_items, indexed ? 2 : 0);
		UT_ASSERT(heap_pin && heap_lock && writer && vm_pin && !vm_lock);
	}
}

UT_TEST(test_cancel_before_or_in_fsm_preserves_completed_cleanup)
{
	static VacDeadItems dead;

	for (unsigned phase = 0; phase < 2; phase++) {
		LVRelState state = reset(&dead);
		BlockNumber next = 0;

		dead.num_items = 3;
		cancel_proof = phase == 0;
		cancel_fsm = phase == 1;
		if (sigsetjmp(cancel_jump, 1) == 0) {
			scan_onepass(&state, VACUUM_FSM_EVERY_PAGES, true, &next);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(violations, 0);
		UT_ASSERT(!heap_pin && !heap_lock && vm_pin && !vm_lock);
		UT_ASSERT_EQ(dead.num_items, 0);
		UT_ASSERT_EQ(record_calls, 0);
		UT_ASSERT_EQ(fsm_calls, phase);
		UT_ASSERT_EQ(next, 0);
	}
}

UT_TEST(test_shared_acquisition_uses_original_writer_without_vm_pin)
{
	for (unsigned race = 0; race < 3; race++) {
		LVRelState state = reset(NULL);
		Buffer vm = 2;
		bool cleanup = false;

		heap_pin = heap_lock = writer = false;
		misses = race;
		UT_ASSERT_EQ(lazy_read_shared_heap(&state, 0, &vm, false, &cleanup), 1);
		UT_ASSERT(cleanup && heap_lock && heap_pin && writer && vm_pin && !vm_lock);
		UT_ASSERT_EQ(conditional_locks, 0);
		UT_ASSERT_EQ(ordinary_locks, race + 1);
		UT_ASSERT_EQ(reads, race + 1);
		UT_ASSERT_EQ(pin_reads, race);
		UT_ASSERT_EQ(repins, race + 1);
	}
}

UT_TEST(test_busy_cleanup_retains_share_then_force_path)
{
	for (unsigned force = 0; force < 2; force++) {
		LVRelState state = reset(NULL);
		Buffer vm = 2;
		bool cleanup = false;

		heap_pin = heap_lock = writer = false;
		cleanup_available = false;
		UT_ASSERT_EQ(lazy_read_shared_heap(&state, 0, &vm, force, &cleanup), 1);
		UT_ASSERT_EQ(cleanup, force);
		UT_ASSERT_EQ(writer, force);
		UT_ASSERT(heap_lock && heap_pin && vm_pin && !vm_lock);
		UT_ASSERT_EQ(conditional_locks, 0);
		UT_ASSERT_EQ(ordinary_locks, force ? 1 : 2);
		UT_ASSERT_EQ(cleanup_locks, force);
	}
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_fsm_range_follows_heap_and_vm_release);
	UT_RUN(test_indexed_and_no_dead_paths_do_not_run_onepass_work);
	UT_RUN(test_cancel_before_or_in_fsm_preserves_completed_cleanup);
	UT_RUN(test_shared_acquisition_uses_original_writer_without_vm_pin);
	UT_RUN(test_busy_cleanup_retains_share_then_force_path);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
