/*-------------------------------------------------------------------------
 * test_cluster_undo_extent_claim.c
 *    Cold and warm record cursors through the actual extent claim owner.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_undo_extent_claim.c
 *
 * The record state and complete claim function are extracted verbatim.
 * Segment I/O is the boundary: no replacement of the cursor decisions,
 * extent arithmetic, lock ordering or publication checks under test.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/storage/cluster_undo_inventory.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_undo_extent.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#include "test_cluster_undo_extent_types.inc"
static ClusterUndoRecordShared state;
static ClusterUndoRecordShared *UndoRecordShared = &state;
static ClusterUndoSegmentExtendPlan plan;
static bool cursor_locked, lifecycle_locked;
static bool select_ok, mutation_ok, generation_mismatch;
static bool drift_on_provision, drift_on_seal, drift_on_range;
static unsigned selections, provisions, reuses, seals, activations, tail_inits, ranges;
static uint32 first_free, candidate_generation, range_segment, range_start, range_count;
static uint8 candidate_state;
int cluster_undo_extent_blocks = 1;
bool cluster_undo_record_segment_commit_on_rollover = true;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (mode != LW_EXCLUSIVE)
		abort();
	if (lock == &state.cursor_lock.lock && !cursor_locked && !lifecycle_locked)
		cursor_locked = true;
	else if (lock == &state.lifecycle_lock.lock && cursor_locked && !lifecycle_locked)
		lifecycle_locked = true;
	else
		abort();
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == &state.lifecycle_lock.lock && lifecycle_locked && cursor_locked)
		lifecycle_locked = false;
	else if (lock == &state.cursor_lock.lock && cursor_locked && !lifecycle_locked)
		cursor_locked = false;
	else
		abort();
}

void
pg_re_throw(void)
{
	abort();
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static void
check_io(uint8 owner)
{
	if (owner != 1 || !cursor_locked || lifecycle_locked)
		abort();
}

uint32
cluster_undo_segment_first_free_block(uint32 seg, uint8 owner)
{
	if (owner != 1 || !cursor_locked || !lifecycle_locked)
		abort();
	return seg == 1 ? first_free : 7;
}

bool
cluster_undo_segment_extend_or_create(uint8 owner, ClusterUndoSegmentExtendPlan *out)
{
	if (owner != 1 || !cursor_locked || !lifecycle_locked || ++selections > 4)
		abort();
	*out = plan;
	return select_ok;
}

void
cluster_undo_segment_allocate(uint32 seg, uint8 owner)
{
	check_io(owner);
	if (seg != 2)
		abort();
	provisions++;
	candidate_state = SEGMENT_ALLOCATED;
	candidate_generation = plan.generation + (generation_mismatch ? 1 : 0);
	plan.needs_provision = false;
	if (drift_on_provision)
		state.active_segment_id = 3;
}

uint32
cluster_undo_segment_reuse_in_place(uint32 seg, uint8 owner, uint32 generation)
{
	check_io(owner);
	if (seg != 2 || generation != plan.generation)
		abort();
	reuses++;
	candidate_state = SEGMENT_ALLOCATED;
	plan.needs_reuse = false;
	return mutation_ok ? seg : 0;
}

uint8
cluster_undo_segment_read_state(uint32 seg, uint8 owner)
{
	if (owner != 1)
		abort();
	return seg == 2 ? candidate_state : SEGMENT_ACTIVE;
}

uint32
cluster_undo_segment_generation(uint32 seg, uint8 owner)
{
	if (owner != 1 || seg != 2)
		abort();
	return candidate_generation;
}

static void
cluster_undo_record_observation_apply_locked(uint8 owner)
{
	if (owner != 1 || !cursor_locked || !lifecycle_locked)
		abort();
}

bool
cluster_undo_segment_mark_full(uint32 seg, uint8 owner)
{
	check_io(owner);
	if (seg != 1)
		abort();
	seals++;
	if (drift_on_seal)
		state.active_segment_id = 3;
	return true;
}

static void
cluster_undo_try_mark_record_segment_committed_owned(uint32 seg, uint8 owner, SCN scn)
{
	check_io(owner);
	if (seg != 1 || scn != 100)
		abort();
}

SCN
cluster_scn_current(void)
{
	return 100;
}

bool
cluster_undo_segment_mark_active(uint32 seg, uint8 owner)
{
	check_io(owner);
	activations++;
	if (seg == 2 && mutation_ok)
		candidate_state = SEGMENT_ACTIVE;
	return mutation_ok;
}

bool
cluster_undo_segment_tail_block_init(uint32 seg, uint8 owner, uint32 block)
{
	check_io(owner);
	if (seg != 2 || block != 1)
		abort();
	tail_inits++;
	return true;
}

bool
cluster_undo_segment_mark_block_range_used(uint32 seg, uint8 owner, uint32 block, uint32 n)
{
	check_io(owner);
	ranges++;
	range_segment = seg;
	range_start = block;
	range_count = n;
	if (drift_on_range)
		state.next_extent_block = 99;
	return mutation_ok;
}

#include "test_cluster_undo_extent_claim.inc"

static void
reset_fixture(bool cold)
{
	memset(&state, 0, sizeof(state));
	memset(&plan, 0, sizeof(plan));
	state.active_segment_id = cold ? 0 : 1;
	plan.segment_id = 2;
	plan.generation = candidate_generation = 4;
	candidate_state = SEGMENT_ALLOCATED;
	select_ok = mutation_ok = true;
	generation_mismatch = drift_on_provision = drift_on_seal = drift_on_range = false;
	cursor_locked = lifecycle_locked = false;
	selections = provisions = reuses = seals = activations = tail_inits = ranges = 0;
	first_free = range_segment = range_start = range_count = wait_event = 0;
}

static void
assert_claimed(const ClusterUndoExtent *ext, uint32 seg, uint32 first)
{
	UT_ASSERT_EQ(ext->segment_id, seg);
	UT_ASSERT_EQ(ext->first_block, first);
	UT_ASSERT_EQ(ext->nblocks, 1);
	UT_ASSERT_EQ(range_segment, seg);
	UT_ASSERT_EQ(range_start, first);
	UT_ASSERT_EQ(range_count, 1);
	UT_ASSERT_EQ(state.active_segment_id, seg);
	UT_ASSERT_EQ(state.next_extent_block, first + 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.extent_claim_count), 1);
	UT_ASSERT(!cursor_locked && !lifecycle_locked);
	UT_ASSERT_EQ(wait_event, 0);
}

UT_TEST(test_cold_full_segment_claims_existing_successor)
{
	ClusterUndoExtent ext = { 0 };
	reset_fixture(true);
	UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_OK);
	assert_claimed(&ext, 2, 1);
	UT_ASSERT_EQ(selections, 1);
	UT_ASSERT_EQ(seals, 1);
	UT_ASSERT_EQ(tail_inits, 1);
}

UT_TEST(test_cold_full_segment_provisions_once)
{
	ClusterUndoExtent ext = { 0 };
	reset_fixture(true);
	plan.needs_provision = true;
	UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_OK);
	assert_claimed(&ext, 2, 1);
	UT_ASSERT_EQ(selections, 1);
	UT_ASSERT_EQ(provisions, 1);
}

UT_TEST(test_cold_full_segment_reuses_exact_candidate_once)
{
	ClusterUndoExtent ext = { 0 };
	reset_fixture(true);
	plan.needs_reuse = true;
	UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_OK);
	assert_claimed(&ext, 2, 1);
	UT_ASSERT_EQ(selections, 1);
	UT_ASSERT_EQ(reuses, 1);
}

UT_TEST(test_warm_full_segment_still_rolls)
{
	ClusterUndoExtent ext = { 0 };
	reset_fixture(false);
	UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_OK);
	assert_claimed(&ext, 2, 1);
}

UT_TEST(test_cold_partial_segment_resumes_bitmap_without_resetting_tail)
{
	ClusterUndoExtent ext = { 0 };
	reset_fixture(true);
	first_free = 5;
	UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_OK);
	assert_claimed(&ext, 1, 5);
	UT_ASSERT_EQ(selections, 0);
	UT_ASSERT_EQ(tail_inits, 0);
}

UT_TEST(test_changed_cursor_after_provision_recomputes_from_winner)
{
	ClusterUndoExtent ext = { 0 };
	reset_fixture(true);
	plan.needs_provision = drift_on_provision = true;
	UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_OK);
	assert_claimed(&ext, 3, 7);
	UT_ASSERT_EQ(seals, 0);
	UT_ASSERT_EQ(activations, 0);
}

UT_TEST(test_changed_cursor_during_seal_never_publishes_successor)
{
	for (int cold = 0; cold <= 1; cold++) {
		ClusterUndoExtent ext = { 0 };
		reset_fixture(cold);
		drift_on_seal = true;
		UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), CLAIM_IO_FAIL);
		UT_ASSERT_EQ(state.active_segment_id, 3);
		UT_ASSERT_EQ(ranges, 0);
		UT_ASSERT_EQ(activations, 0);
		UT_ASSERT(!cursor_locked && !lifecycle_locked);
	}
}

UT_TEST(test_generation_or_io_failure_remains_closed)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterUndoExtent ext = { 0 };
		reset_fixture(true);
		plan.needs_provision = (fault == 0);
		generation_mismatch = (fault == 0);
		plan.needs_reuse = (fault == 1);
		mutation_ok = (fault == 0 || fault == 3);
		drift_on_range = (fault == 3);
		UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), fault < 2 ? CLAIM_FS_FAIL : CLAIM_IO_FAIL);
		UT_ASSERT_EQ(state.active_segment_id, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&state.extent_claim_count), 0);
		UT_ASSERT(!cursor_locked && !lifecycle_locked);
	}
}

UT_TEST(test_failed_selection_does_not_bind_cursor)
{
	for (int hard_cap = 0; hard_cap <= 1; hard_cap++) {
		ClusterUndoExtent ext = { 0 };
		reset_fixture(true);
		select_ok = false;
		plan.at_hard_cap = hard_cap;
		UT_ASSERT_EQ(claim_undo_extent(&ext, 1, 1, 100), hard_cap ? CLAIM_HARD_CAP : CLAIM_FS_FAIL);
		UT_ASSERT_EQ(state.active_segment_id, 0);
		UT_ASSERT_EQ(ranges, 0);
		UT_ASSERT(!cursor_locked && !lifecycle_locked);
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_cold_full_segment_claims_existing_successor);
	UT_RUN(test_cold_full_segment_provisions_once);
	UT_RUN(test_cold_full_segment_reuses_exact_candidate_once);
	UT_RUN(test_warm_full_segment_still_rolls);
	UT_RUN(test_cold_partial_segment_resumes_bitmap_without_resetting_tail);
	UT_RUN(test_changed_cursor_after_provision_recomputes_from_winner);
	UT_RUN(test_changed_cursor_during_seal_never_publishes_successor);
	UT_RUN(test_generation_or_io_failure_remains_closed);
	UT_RUN(test_failed_selection_does_not_bind_cursor);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
