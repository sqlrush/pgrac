/*-------------------------------------------------------------------------
 * test_cluster_ko_writeback_io.c
 *    Native advisory writeback batching versus the original DROP marker.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Real KO entry/marker, native scheduling/coalescing and smgr callback.
 * GES/READY prerequisites, storage/cache/provider and statistics are explicit
 * fixtures. These hints prove neither durable DATA nor physical PI ACK.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main
#include "common/relpath.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "storage/buf_internals.h"
#include "storage/fd.h"
#include "cluster/storage/cluster_shared_fs.h"

int io_direct_flags;
typedef struct ClusterSmgrRelationState { int unused; } ClusterSmgrRelationState;
static ClusterSmgrRelationState hint_state;
static SMgrRelationData hint_relation;
static ClusterKoSharedMessageV3 hint_request;
static WritebackContext hint_context;
static int hint_limit;
static unsigned hint_steps[5], hint_markers, hint_accepted, hint_gates;
static unsigned hint_stats_calls, hint_stats_count;
static int hint_error, hint_gate_error;
static bool hint_probe;
static struct {
	RelFileLocator locator;
	ForkNumber fork;
	BlockNumber block, count;
} hint_ranges[16];
static unsigned hint_range_count;

static bool
hint_commit(void)
{
	return cluster_ko_peer_commit_v3(hint_request.base.origin_node,
									 hint_request.base.epoch, hint_request.base.origin_boot,
									 hint_request.base.peer_boot, hint_request.base.batch_id);
}

static void
hint_step(unsigned step)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	hint_steps[step]++;
	if (hint_probe) {
		hint_markers++;
		if (hint_commit())
			hint_accepted++;
	}
	if ((int)step == hint_error)
		pg_re_throw();
}

static SMgrRelation
hint_open(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT_EQ(backend, InvalidBackendId);
	hint_step(1);
	hint_relation.smgr_rlocator.locator = locator;
	hint_relation.smgr_rlocator.backend = backend;
	hint_gates = 0;
	return &hint_relation;
}

static void
hint_fence(const char *op)
{
	UT_ASSERT_STR_EQ(op, "writeback");
	UT_ASSERT_EQ(hint_gates++, 0);
	if (hint_gate_error == 1)
		pg_re_throw();
}

static void
hint_standby(const char *op)
{
	UT_ASSERT_STR_EQ(op, "writeback");
	UT_ASSERT_EQ(hint_gates++, 1);
	if (hint_gate_error == 2)
		pg_re_throw();
}

static ClusterSmgrRelationState *
cluster_smgr_state_lookup(SMgrRelation reln, bool create)
{
	UT_ASSERT(reln == &hint_relation && create);
	UT_ASSERT_EQ(hint_gates, 2);
	hint_step(2);
	return &hint_state;
}

static ClusterSharedFsHandle *
cluster_smgr_ensure_handle(ClusterSmgrRelationState *state, ForkNumber fork)
{
	UT_ASSERT(state == &hint_state);
	UT_ASSERT(fork >= MAIN_FORKNUM && fork <= MAX_FORKNUM);
	hint_step(3);
	return (ClusterSharedFsHandle *)1;
}

static void
hint_provider(ClusterSharedFsHandle *handle, BlockNumber block, BlockNumber count)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	hint_step(4);
	UT_ASSERT(hint_range_count < lengthof(hint_ranges));
	if (hint_range_count < lengthof(hint_ranges)) {
		hint_ranges[hint_range_count].locator = hint_relation.smgr_rlocator.locator;
		hint_ranges[hint_range_count].block = block;
		hint_ranges[hint_range_count].count = count;
		hint_range_count++;
	}
}

#define cluster_write_fence_reject_if_fenced hint_fence
#define cluster_mrp_standby_shared_write_gate hint_standby
#define cluster_shared_fs_writeback hint_provider
#include "test_cluster_ko_writeback_smgr.inc"
#undef cluster_write_fence_reject_if_fenced
#undef cluster_mrp_standby_shared_write_gate
#undef cluster_shared_fs_writeback

static void
hint_writeback(SMgrRelation reln, ForkNumber fork, BlockNumber block, BlockNumber count)
{
	unsigned n = hint_range_count;
	cluster_smgr_writeback(reln, fork, block, count);
	if (n < hint_range_count)
		hint_ranges[n].fork = fork;
}

static void
hint_count(IOObject object, IOContext context, IOOp op, uint32 count)
{
	UT_ASSERT_EQ(object, IOOBJECT_RELATION);
	UT_ASSERT_EQ(context, IOCONTEXT_NORMAL);
	UT_ASSERT_EQ(op, IOOP_WRITEBACK);
	hint_stats_calls++;
	hint_stats_count += count;
}

#define smgropen hint_open
#define smgrwriteback hint_writeback
#define pgstat_prepare_io_time() ((instr_time){0})
#define pgstat_count_io_op_time(o, c, op, start, count) ((void)(start), hint_count(o, c, op, count))
#include "test_cluster_ko_writeback_io.inc"
#undef smgropen
#undef smgrwriteback
#undef pgstat_prepare_io_time
#undef pgstat_count_io_op_time

static void
hint_fixture(unsigned n)
{
	ClusterKoPeerFenceHandleV3 handle;
	ClusterKoPeerFenceStateV3 state;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	hint_request = peer_fence_fixture(65000 + n);
	local_scan_rel_number = hint_request.base.key.locator.relNumber;
	UT_ASSERT(cluster_ko_peer_fence_begin_v3(&hint_request, &handle, &state));
	UT_ASSERT(fixture_completed_fence_ready(&hint_request, &handle));
	fixture_peer_reply_accepted(&storage.peer_fences[handle.slot]);
	hint_limit = 16;
	WritebackContextInit(&hint_context, &hint_limit);
	memset(hint_steps, 0, sizeof(hint_steps));
	memset(hint_ranges, 0, sizeof(hint_ranges));
	hint_markers = hint_accepted = hint_gates = 0;
	hint_stats_calls = hint_stats_count = hint_range_count = 0;
	hint_error = hint_gate_error = io_direct_flags = 0;
	hint_probe = false;
}

static void
hint_schedule(unsigned other, ForkNumber fork, BlockNumber block)
{
	BufferTag tag;
	RelFileLocator locator = hint_request.base.key.locator;
	locator.relNumber += other;
	InitBufferTag(&tag, &locator, fork, block);
	ScheduleBufferTagForWriteback(&hint_context, IOCONTEXT_NORMAL, &tag);
}

static unsigned
hint_call_count(void)
{
	unsigned count = 0;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		count += storage.directory_io[i].serial != 0;
	return count;
}

UT_TEST(hint_marker_waits_through_native_open_and_provider)
{
	hint_fixture(1);
	hint_schedule(0, MAIN_FORKNUM, 4);
	hint_probe = true;
	IssuePendingWritebacks(&hint_context, IOCONTEXT_NORMAL);
	UT_ASSERT_EQ(hint_markers, 4);
	UT_ASSERT_EQ(hint_accepted, 0);
	UT_ASSERT_EQ(hint_range_count, 1);
	UT_ASSERT_EQ(hint_stats_count, 1);
	UT_ASSERT_EQ(hint_call_count(), 0);
	UT_ASSERT_EQ(hint_context.nr_pending, 0);
	UT_ASSERT(hint_commit());
}

UT_TEST(retired_hints_skip_io_and_metrics_without_losing_other_ranges)
{
	hint_fixture(2);
	for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++)
		hint_schedule(0, fork, 4);
	hint_schedule(1, MAIN_FORKNUM, 3);
	hint_schedule(1, MAIN_FORKNUM, 2);
	hint_schedule(1, MAIN_FORKNUM, 2);
	hint_schedule(1, FSM_FORKNUM, 9);
	UT_ASSERT(hint_commit());
	IssuePendingWritebacks(&hint_context, IOCONTEXT_NORMAL);
	UT_ASSERT_EQ(hint_range_count, 2);
	UT_ASSERT_EQ(hint_ranges[0].locator.relNumber, hint_request.base.key.locator.relNumber + 1);
	UT_ASSERT_EQ(hint_ranges[0].block, 2);
	UT_ASSERT_EQ(hint_ranges[0].count, 2);
	UT_ASSERT_EQ(hint_ranges[1].fork, FSM_FORKNUM);
	UT_ASSERT_EQ(hint_ranges[1].count, 1);
	UT_ASSERT_EQ(hint_stats_count, 4); /* Native counts queued tags, including duplicates. */
	UT_ASSERT_EQ(hint_context.nr_pending, 0);
}

UT_TEST(closing_between_ranges_does_not_enter_later_hint)
{
	hint_fixture(3);
	hint_schedule(0, MAIN_FORKNUM, 10);
	hint_schedule(0, MAIN_FORKNUM, 1);
	hint_schedule(0, MAIN_FORKNUM, 3);
	hint_probe = true;
	IssuePendingWritebacks(&hint_context, IOCONTEXT_NORMAL);
	UT_ASSERT_EQ(hint_range_count, 1);
	UT_ASSERT_EQ(hint_ranges[0].block, 1);
	UT_ASSERT_EQ(hint_stats_count, 1);
	UT_ASSERT_EQ(hint_accepted, 0);
	UT_ASSERT_EQ(hint_context.nr_pending, 0);
	UT_ASSERT(hint_commit());
}

UT_TEST(hint_error_keeps_original_pending_and_releases_only_call)
{
	for (int fault = 1; fault <= 4; fault++) {
		volatile bool caught = false;
		hint_fixture(10 + fault);
		hint_schedule(0, MAIN_FORKNUM, 4);
		hint_error = fault;
		hint_probe = true;
		PG_TRY();
		{
			IssuePendingWritebacks(&hint_context, IOCONTEXT_NORMAL);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(hint_accepted, 0);
		UT_ASSERT_EQ(hint_call_count(), 0);
		UT_ASSERT_EQ(hint_context.nr_pending, 1);
		UT_ASSERT_EQ(hint_stats_calls, 0);
		UT_ASSERT(hint_commit());
	}
}

UT_TEST(unknown_advisory_refusal_is_not_an_object_terminal)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		hint_fixture(20 + fault);
		hint_schedule(0, MAIN_FORKNUM, 4);
		if (fault == 0) ko_state = NULL;
		if (fault == 1) fixture_recovering = true;
		if (fault == 2) storage.directory_io_serial = UINT64_MAX;
		if (fault == 3)
			for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
				ClusterKoDirectoryIoV3 call;
				UT_ASSERT(cluster_ko_directory_io_begin_v3(hint_request.base.key.locator, &call));
			}
		IssuePendingWritebacks(&hint_context, IOCONTEXT_NORMAL);
		UT_ASSERT_EQ(hint_range_count + hint_steps[1], 0);
		UT_ASSERT_EQ(hint_stats_calls, 0);
		UT_ASSERT_EQ(hint_context.nr_pending, 0);
		UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_READY);
		UT_ASSERT_EQ(hint_call_count(), fault == 3 ? CLUSTER_KO_SHARED_CAPACITY : 0);
	}
}

UT_TEST(original_provider_gates_keep_error_and_pending_ownership)
{
	for (int gate = 1; gate <= 2; gate++) {
		volatile bool caught = false;
		hint_fixture(30 + gate);
		hint_schedule(0, MAIN_FORKNUM, 4);
		hint_gate_error = gate;
		PG_TRY();
		{
			IssuePendingWritebacks(&hint_context, IOCONTEXT_NORMAL);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(hint_steps[2] + hint_steps[3] + hint_steps[4], 0);
		UT_ASSERT_EQ(hint_context.nr_pending, 1);
		UT_ASSERT_EQ(hint_call_count(), 0);
		UT_ASSERT_EQ(hint_stats_calls, 0);
	}
}

UT_TEST(native_disabled_direct_io_and_threshold_semantics_remain)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		hint_fixture(40 + kind);
		if (kind == 0) cluster_enabled = false;
		if (kind == 1) cluster_shared_config = false;
		if (kind == 2) cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		hint_limit = 3;
		hint_schedule(0, MAIN_FORKNUM, 4);
		hint_schedule(0, MAIN_FORKNUM, 2);
		hint_schedule(0, MAIN_FORKNUM, 3);
		UT_ASSERT_EQ(hint_range_count, 1);
		UT_ASSERT_EQ(hint_ranges[0].block, 2);
		UT_ASSERT_EQ(hint_ranges[0].count, 3);
		UT_ASSERT_EQ(hint_stats_count, 3);
		UT_ASSERT_EQ(hint_context.nr_pending, 0);
	}
	hint_fixture(44);
	hint_limit = 0;
	hint_schedule(0, MAIN_FORKNUM, 4);
	UT_ASSERT_EQ(hint_steps[1], 0);
	hint_limit = 1;
	io_direct_flags = IO_DIRECT_DATA;
	hint_schedule(0, MAIN_FORKNUM, 4);
	UT_ASSERT_EQ(hint_context.nr_pending, 0);
	UT_ASSERT_EQ(hint_steps[1] + hint_stats_calls, 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(hint_marker_waits_through_native_open_and_provider);
	UT_RUN(retired_hints_skip_io_and_metrics_without_losing_other_ranges);
	UT_RUN(closing_between_ranges_does_not_enter_later_hint);
	UT_RUN(hint_error_keeps_original_pending_and_releases_only_call);
	UT_RUN(unknown_advisory_refusal_is_not_an_object_terminal);
	UT_RUN(original_provider_gates_keep_error_and_pending_ownership);
	UT_RUN(native_disabled_direct_io_and_threshold_semantics_remain);
	UT_DONE();
	return ut_failed_count != 0;
}
