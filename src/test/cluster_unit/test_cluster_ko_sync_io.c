/*-------------------------------------------------------------------------
 * test_cluster_ko_sync_io.c
 *    Queued shared-file sync versus the actual DROP marker and call owner.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * KO entry/COMMIT/GC and the smgr callback are real. GES, physical READY,
 * filesystem and write-authority predicates are explicit fixtures. Native
 * pendingOps result consumption is tested in test_cluster_subtrans_order.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main
#include "cluster/storage/cluster_shared_fs.h"

static ClusterKoSharedMessageV3 sync_request;
static FileTag sync_tag;
static unsigned sync_steps[5], sync_markers, sync_accepted, sync_gates;
static int sync_fault, sync_gate_fault;
static bool sync_probe, sync_exists;

static bool
sync_commit(void)
{
	return cluster_ko_peer_commit_v3(sync_request.base.origin_node,
									 sync_request.base.epoch, sync_request.base.origin_boot,
									 sync_request.base.peer_boot, sync_request.base.batch_id);
}

static void
sync_step(unsigned step)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	UT_ASSERT_EQ(sync_gates, 2);
	sync_steps[step]++;
	if (sync_probe) {
		sync_markers++;
		if (sync_commit())
			sync_accepted++;
	}
	if (sync_fault == (int)step)
		pg_re_throw();
}

static void
sync_fence_gate(const char *operation)
{
	UT_ASSERT_STR_EQ(operation, "syncfiletag");
	UT_ASSERT_EQ(sync_gates++, 0);
	if (sync_gate_fault == 1)
		pg_re_throw();
}

static void
sync_standby_gate(const char *operation)
{
	UT_ASSERT_STR_EQ(operation, "syncfiletag");
	UT_ASSERT_EQ(sync_gates++, 1);
	if (sync_gate_fault == 2)
		pg_re_throw();
}

static bool
sync_fs_exists(RelFileLocator locator, ForkNumber fork)
{
	UT_ASSERT(RelFileLocatorEquals(locator, sync_tag.rlocator));
	UT_ASSERT_EQ(fork, sync_tag.forknum);
	sync_step(1);
	return sync_exists;
}

static void
sync_fs_open(RelFileLocator locator, ForkNumber fork, ClusterSharedFsHandle **out)
{
	UT_ASSERT(RelFileLocatorEquals(locator, sync_tag.rlocator));
	UT_ASSERT_EQ(fork, sync_tag.forknum);
	sync_step(2);
	*out = (ClusterSharedFsHandle *)1;
}

static int
sync_fs_barrier(ClusterSharedFsHandle *handle)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	sync_step(3);
	return 0;
}

static void
sync_fs_close(ClusterSharedFsHandle *handle)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	sync_step(4);
}

#define cluster_write_fence_reject_if_fenced sync_fence_gate
#define cluster_mrp_standby_shared_write_gate sync_standby_gate
#define cluster_shared_fs_exists sync_fs_exists
#define cluster_shared_fs_open_existing sync_fs_open
#define cluster_shared_fs_barrier_sync sync_fs_barrier
#define cluster_shared_fs_close sync_fs_close
#include "test_cluster_ko_sync_io.inc"
#undef cluster_write_fence_reject_if_fenced
#undef cluster_mrp_standby_shared_write_gate
#undef cluster_shared_fs_exists
#undef cluster_shared_fs_open_existing
#undef cluster_shared_fs_barrier_sync
#undef cluster_shared_fs_close

static void
sync_fixture_operation(unsigned n, uint16 operation)
{
	ClusterKoPeerFenceHandleV3 handle = {0};
	ClusterKoPeerFenceStateV3 state;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	sync_request = peer_fence_fixture(60000 + n);
	sync_request.operation = operation;
	local_scan_rel_number = sync_request.base.key.locator.relNumber;
	UT_ASSERT(cluster_ko_peer_fence_begin_v3(&sync_request, &handle, &state));
	UT_ASSERT(fixture_completed_fence_ready(&sync_request, &handle));
	fixture_peer_reply_accepted(&storage.peer_fences[handle.slot]);
	memset(&sync_tag, 0, sizeof(sync_tag));
	sync_tag.handler = SYNC_HANDLER_CLUSTER_SHARED;
	sync_tag.rlocator = sync_request.base.key.locator;
	sync_tag.forknum = MAIN_FORKNUM;
	memset(sync_steps, 0, sizeof(sync_steps));
	sync_markers = sync_accepted = sync_gates = 0;
	sync_fault = sync_gate_fault = 0;
	sync_probe = false;
	sync_exists = true;
}

static void
sync_fixture(unsigned n)
{
	sync_fixture_operation(n, CLUSTER_KO_SHARED_V3_OPERATION_DROP);
}

static int
sync_call(void)
{
	char path[MAXPGPATH];
	sync_gates = 0;
	return cluster_smgr_syncfiletag(&sync_tag, path);
}

static unsigned
sync_call_count(void)
{
	unsigned calls = 0;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		calls += storage.directory_io[i].serial != 0;
	return calls;
}

UT_TEST(entered_sync_blocks_marker_through_close_and_error)
{
	for (int fault = 0; fault <= 4; fault++) {
		volatile bool caught = false;
		sync_fixture(fault);
		sync_probe = true;
		sync_fault = fault;
		PG_TRY();
		{
			UT_ASSERT_EQ(sync_call(), 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault != 0);
		UT_ASSERT_EQ(sync_markers, fault == 0 ? 4 : fault);
		UT_ASSERT_EQ(sync_accepted, 0);
		UT_ASSERT_EQ(sync_call_count(), 0);
		UT_ASSERT(sync_commit());
	}
}

UT_TEST(committed_drop_cancels_without_io_after_active_gc)
{
	ClusterKoSharedMessageV3 first;
	sync_fixture(10);
	first = sync_request;
	UT_ASSERT(sync_commit());
	/* The original collector only reclaims under pressure. Populate its
	 * real active slots; do not clear the tested fence directly. */
	for (unsigned i = 1; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceHandleV3 handle;
		ClusterKoPeerFenceStateV3 state;
		sync_request = first;
		sync_request.base.batch_id += i;
		sync_request.base.key.locator.relNumber += i;
		local_scan_rel_number = sync_request.base.key.locator.relNumber;
		UT_ASSERT(cluster_ko_peer_fence_begin_v3(&sync_request, &handle, &state));
		UT_ASSERT(fixture_completed_fence_ready(&sync_request, &handle));
		fixture_peer_reply_accepted(&storage.peer_fences[handle.slot]);
		UT_ASSERT(sync_commit());
	}
	sync_request = first;
	UT_ASSERT(cluster_ko_peer_fence_gc_tick_v3());
	UT_ASSERT_EQ(storage.peer_fences[0].state, CLUSTER_KO_PEER_FENCE_EMPTY);
	for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		sync_tag.forknum = fork;
		/* The local callback contract distinguishes canceled (1) from fsync (0). */
		UT_ASSERT_EQ(sync_call(), 1);
	}
	UT_ASSERT_EQ(sync_steps[1], 0);
	UT_ASSERT_EQ(sync_call_count(), 0);
}

UT_TEST(closing_cancels_new_sync_without_releasing_entered_call)
{
	ClusterKoDirectoryIoV3 call;
	sync_fixture(11);
	UT_ASSERT(cluster_ko_directory_io_begin_v3(sync_tag.rlocator, &call));
	UT_ASSERT(!sync_commit());
	UT_ASSERT_EQ(sync_call(), 1);
	UT_ASSERT_EQ(sync_steps[1], 0);
	UT_ASSERT_EQ(sync_call_count(), 1);
	UT_ASSERT(!sync_commit());
	UT_ASSERT(cluster_ko_directory_io_end_v3(&call));
	UT_ASSERT(sync_commit());
}

UT_TEST(unknown_refusal_never_becomes_canceled_or_io)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		sync_fixture(20 + fault);
		switch (fault) {
		case 0: ko_state = NULL; break;
		case 1: fixture_recovering = true; break;
		case 2: storage.directory_io_serial = UINT64_MAX; break;
		case 3:
			storage.drop_buckets[ko_drop_bucket(sync_tag.rlocator)] = KO_DROP_IDENTITY_CAPACITY + 1;
			break;
		case 4:
			storage.drop_identities[0].state = (KoDropIdentityState)99;
			break;
		case 5:
			UT_ASSERT(sync_commit());
			storage.drop_identities[0].next = 1;
			break;
		case 6:
			UT_ASSERT(sync_commit());
			storage.drop_identities[0].request.operation = CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE;
			break;
		}
		errno = 0;
		UT_ASSERT_EQ(sync_call(), -1);
		UT_ASSERT_EQ(errno, EAGAIN);
		UT_ASSERT_EQ(sync_steps[1], 0);
	}
}

UT_TEST(reservation_other_locator_and_native_scope_are_not_retirement)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		sync_fixture_operation(30 + kind, kind == 4 ? CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE
												  : CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		if (kind != 0)
			UT_ASSERT(sync_commit());
		if (kind == 1)
			sync_tag.rlocator.relNumber++;
		if (kind == 2)
			cluster_shared_config = false;
		if (kind == 3)
			cluster_interconnect_tier = CLUSTER_IC_TIER_2;
		UT_ASSERT_EQ(sync_call(), 0);
		UT_ASSERT_EQ(sync_steps[1] + sync_steps[2] + sync_steps[3] + sync_steps[4], 4);
		UT_ASSERT_EQ(sync_call_count(), 0);
	}
}

UT_TEST(original_gates_and_missing_file_keep_their_polarity)
{
	for (int gate = 1; gate <= 2; gate++) {
		volatile bool caught = false;
		sync_fixture(40 + gate);
		UT_ASSERT(sync_commit());
		sync_gate_fault = gate;
		PG_TRY();
		{
			(void)sync_call();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(sync_steps[1], 0);
		UT_ASSERT_EQ(sync_call_count(), 0);
	}
	sync_fixture(44);
	sync_exists = false;
	sync_probe = true;
	UT_ASSERT_EQ(sync_call(), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT_EQ(sync_steps[1], 1);
	UT_ASSERT_EQ(sync_steps[2], 0);
	UT_ASSERT_EQ(sync_accepted, 0);
	UT_ASSERT_EQ(sync_call_count(), 0);
	UT_ASSERT(sync_commit());
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(entered_sync_blocks_marker_through_close_and_error);
	UT_RUN(committed_drop_cancels_without_io_after_active_gc);
	UT_RUN(closing_cancels_new_sync_without_releasing_entered_call);
	UT_RUN(unknown_refusal_never_becomes_canceled_or_io);
	UT_RUN(reservation_other_locator_and_native_scope_are_not_retirement);
	UT_RUN(original_gates_and_missing_file_keep_their_polarity);
	UT_DONE();
	return ut_failed_count != 0;
}
