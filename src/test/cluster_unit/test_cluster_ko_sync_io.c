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

/* The required provider calls also reach storage with a cached handle. Run
 * their real lookup/open/dirty-registration bodies against the same real KO
 * table and marker; only HTAB, filesystem and authority are fixtures. */
typedef struct ClusterSmgrRelationState {
	RelFileLocatorBackend rlocator;
	ClusterSharedFsHandle *fork_handles[MAX_FORKNUM + 1];
} ClusterSmgrRelationState;
static HTAB *cluster_smgr_relations;
static ClusterSmgrRelationState provider_state;
static SMgrRelationData provider_relation;
static PGAlignedBlock provider_page;
static unsigned provider_steps[8], provider_gates, provider_accepted, provider_entries;
static int provider_op, provider_fault, provider_gate_fault;
static bool provider_probe, provider_queue, provider_skip;
static const char *const provider_names[] = {
	"extend", "zero-extend", "write", "truncate", "immedsync"
};

static void
provider_step(unsigned step)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	UT_ASSERT_EQ(provider_gates, 2);
	provider_steps[step]++;
	provider_entries += sync_call_count() != 0;
	if (provider_probe && sync_commit())
		provider_accepted++;
	if ((int)step == provider_fault)
		pg_re_throw();
}

static void
provider_fence(const char *op)
{
	UT_ASSERT_STR_EQ(op, provider_names[provider_op]);
	UT_ASSERT_EQ(provider_gates++, 0);
	if (provider_gate_fault == 1)
		pg_re_throw();
}

static void
provider_standby(const char *op)
{
	UT_ASSERT_STR_EQ(op, provider_names[provider_op]);
	UT_ASSERT_EQ(provider_gates++, 1);
	if (provider_gate_fault == 2)
		pg_re_throw();
}

static void *
provider_hash(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	const RelFileLocatorBackend *locator = key;
	UT_ASSERT(table == cluster_smgr_relations && action == HASH_ENTER);
	UT_ASSERT(RelFileLocatorEquals(locator->locator, provider_state.rlocator.locator));
	UT_ASSERT_EQ(locator->backend, InvalidBackendId);
	provider_step(1);
	*found = true;
	return &provider_state;
}

static void
provider_open(RelFileLocator locator, ForkNumber fork, ClusterSharedFsHandle **out)
{
	UT_ASSERT(RelFileLocatorEquals(locator, sync_tag.rlocator));
	UT_ASSERT_EQ(fork, sync_tag.forknum);
	provider_step(2);
	*out = (ClusterSharedFsHandle *)1;
}

static void
provider_extend(ClusterSharedFsHandle *handle, BlockNumber block)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	UT_ASSERT(block == 4 || (provider_op == 1 && block == 5));
	provider_step(3);
}

static int
provider_write(ClusterSharedFsHandle *handle, BlockNumber block, const char *bytes)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	UT_ASSERT(block == 4 || (provider_op == 1 && block == 5));
	if (provider_op == 1) {
		for (unsigned i = 0; i < BLCKSZ; i++)
			UT_ASSERT_EQ(bytes[i], 0);
	} else
		UT_ASSERT(bytes == provider_page.data);
	provider_step(4);
	return 0;
}

static void
provider_truncate(ClusterSharedFsHandle *handle, BlockNumber blocks)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1 && blocks == 4);
	provider_step(3);
}

static void
provider_sync(ClusterSharedFsHandle *handle)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	provider_step(7);
}

static bool
provider_register(const FileTag *tag, SyncRequestType type, bool retry)
{
	UT_ASSERT_EQ(tag->handler, SYNC_HANDLER_CLUSTER_SHARED);
	UT_ASSERT(RelFileLocatorEquals(tag->rlocator, sync_tag.rlocator));
	UT_ASSERT_EQ(tag->forknum, sync_tag.forknum);
	UT_ASSERT(type == SYNC_REQUEST && !retry);
	provider_step(5);
	return provider_queue;
}

static int
provider_barrier(ClusterSharedFsHandle *handle)
{
	UT_ASSERT(handle == (ClusterSharedFsHandle *)1);
	provider_step(6);
	return 0;
}

#define hash_search provider_hash
#define cluster_write_fence_reject_if_fenced provider_fence
#define cluster_mrp_standby_shared_write_gate provider_standby
#define cluster_shared_fs_open_existing provider_open
#define cluster_shared_fs_extend provider_extend
#define cluster_shared_fs_write provider_write
#define cluster_shared_fs_truncate provider_truncate
#define cluster_shared_fs_immedsync provider_sync
#define cluster_shared_fs_barrier_sync provider_barrier
#define RegisterSyncRequest provider_register
#include "test_cluster_ko_provider_io.inc"
#undef hash_search
#undef cluster_write_fence_reject_if_fenced
#undef cluster_mrp_standby_shared_write_gate
#undef cluster_shared_fs_open_existing
#undef cluster_shared_fs_extend
#undef cluster_shared_fs_write
#undef cluster_shared_fs_truncate
#undef cluster_shared_fs_immedsync
#undef cluster_shared_fs_barrier_sync
#undef RegisterSyncRequest

static void
provider_fixture(int op, ForkNumber fork, bool cached)
{
	sync_fixture(300 + op);
	sync_tag.forknum = fork;
	provider_op = op;
	provider_probe = provider_skip = false;
	provider_queue = true;
	provider_fault = provider_gate_fault = 0;
	provider_gates = provider_accepted = provider_entries = 0;
	memset(provider_steps, 0, sizeof(provider_steps));
	memset(&provider_state, 0, sizeof(provider_state));
	memset(&provider_relation, 0, sizeof(provider_relation));
	provider_relation.smgr_rlocator.locator = sync_tag.rlocator;
	provider_relation.smgr_rlocator.backend = InvalidBackendId;
	provider_state.rlocator = provider_relation.smgr_rlocator;
	if (cached)
		provider_state.fork_handles[fork] = (ClusterSharedFsHandle *)1;
	cluster_smgr_relations = (HTAB *)1;
}

static bool
provider_call(void)
{
	volatile bool returned = false;
	provider_gates = 0;
	expecting_error = true;
	PG_TRY();
	{
		switch (provider_op) {
		case 0: cluster_smgr_extend(&provider_relation, sync_tag.forknum, 4, provider_page.data, provider_skip); break;
		case 1: cluster_smgr_zeroextend(&provider_relation, sync_tag.forknum, 4, 2, provider_skip); break;
		case 2: cluster_smgr_write(&provider_relation, sync_tag.forknum, 4, provider_page.data, provider_skip); break;
		case 3: cluster_smgr_truncate(&provider_relation, sync_tag.forknum, 8, 4); break;
		case 4: cluster_smgr_immedsync(&provider_relation, sync_tag.forknum); break;
		}
		returned = true;
	}
	PG_CATCH(); {}
	PG_END_TRY();
	expecting_error = false;
	return returned;
}

UT_TEST(provider_scope_blocks_marker_through_required_io_and_queue)
{
	for (int op = 0; op < 5; op++)
		for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++)
			for (int cached = 0; cached <= 1; cached++) {
				provider_fixture(op, fork, cached);
				provider_probe = true;
				provider_queue = false;
				UT_ASSERT(provider_call());
				UT_ASSERT_EQ(provider_accepted, 0);
				UT_ASSERT_EQ(provider_steps[1], 1);
				UT_ASSERT_EQ(provider_steps[2], !cached);
				UT_ASSERT_EQ(provider_steps[3], op == 1 ? 2 : op == 0 || op == 3);
				UT_ASSERT_EQ(provider_steps[4], op == 1 ? 2 : op == 0 || op == 2);
				UT_ASSERT_EQ(provider_steps[5], op < 3);
				UT_ASSERT_EQ(provider_steps[6], op < 3);
				UT_ASSERT_EQ(provider_steps[7], op == 4);
				UT_ASSERT_EQ(sync_call_count(), 0);
				UT_ASSERT(sync_commit());
			}
}

UT_TEST(retired_required_io_never_opens_or_uses_cached_handle)
{
	for (int op = 0; op < 5; op++)
		for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++)
			for (int cached = 0; cached <= 1; cached++) {
				provider_fixture(op, fork, cached);
				UT_ASSERT(sync_commit());
				UT_ASSERT(!provider_call());
				UT_ASSERT_EQ(reported_sqlstate, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
				UT_ASSERT_EQ(provider_steps[1] + provider_steps[2] + provider_steps[3]
					+ provider_steps[4] + provider_steps[5] + provider_steps[6] + provider_steps[7], 0);
				UT_ASSERT_EQ(sync_call_count(), 0);
			}
}

UT_TEST(provider_error_releases_only_its_call_and_never_returns_success)
{
	for (int op = 0; op < 5; op++)
		for (int fault = 1; fault <= 7; fault++) {
			bool reached = fault <= 2 || (fault == 3 && (op == 0 || op == 1 || op == 3))
				|| (fault >= 4 && fault <= 6 && op < 3) || (fault == 7 && op == 4);
			ClusterKoDirectoryIoV3 outer;
			provider_fixture(op, MAIN_FORKNUM, false);
			UT_ASSERT(cluster_ko_directory_io_begin_v3(sync_tag.rlocator, &outer));
			provider_queue = false;
			provider_probe = true;
			provider_fault = fault;
			UT_ASSERT_EQ(provider_call(), !reached);
			UT_ASSERT_EQ(provider_accepted, 0);
			UT_ASSERT_EQ(sync_call_count(), 1);
			UT_ASSERT(!sync_commit());
			UT_ASSERT(cluster_ko_directory_io_end_v3(&outer));
			UT_ASSERT(sync_commit());
		}
}

UT_TEST(closing_unknown_and_full_table_refuse_before_provider_access)
{
	for (int op = 0; op < 5; op++)
		for (int fault = 0; fault < 4; fault++) {
			ClusterKoDirectoryIoV3 occupied[CLUSTER_KO_SHARED_CAPACITY];
			unsigned count = fault == 3 ? CLUSTER_KO_SHARED_CAPACITY : fault == 0 ? 1 : 0;
			provider_fixture(op, MAIN_FORKNUM, true);
			for (unsigned i = 0; i < count; i++)
				UT_ASSERT(cluster_ko_directory_io_begin_v3(sync_tag.rlocator, &occupied[i]));
			if (fault == 0) UT_ASSERT(!sync_commit());
			if (fault == 1) ko_state = NULL;
			if (fault == 2) storage.directory_io_serial = UINT64_MAX;
			UT_ASSERT(!provider_call());
			UT_ASSERT_EQ(provider_steps[1], 0);
			UT_ASSERT_EQ(sync_call_count(), count);
			ko_state = &storage;
			for (unsigned i = 0; i < count; i++)
				UT_ASSERT(cluster_ko_directory_io_end_v3(&occupied[i]));
		}
}

UT_TEST(provider_original_gates_recovery_and_skip_fsync_remain_distinct)
{
	for (int op = 0; op < 5; op++) {
		for (int gate = 1; gate <= 2; gate++) {
			provider_fixture(op, MAIN_FORKNUM, true);
			fixture_recovering = true;
			provider_gate_fault = gate;
			UT_ASSERT(!provider_call());
			UT_ASSERT_EQ(provider_steps[1], 0);
			UT_ASSERT_EQ(sync_call_count(), 0);
		}
		provider_fixture(op, MAIN_FORKNUM, false);
		fixture_recovering = true;
		ko_state = NULL; /* No runtime fence is an authority for recovery. */
		UT_ASSERT(provider_call());
		UT_ASSERT_EQ(provider_entries, 0);
		ko_state = &storage;
		provider_fixture(op, MAIN_FORKNUM, false);
		provider_skip = true;
		UT_ASSERT(provider_call());
		UT_ASSERT_EQ(provider_steps[5] + provider_steps[6], 0);
		UT_ASSERT(provider_entries != 0);
		UT_ASSERT_EQ(sync_call_count(), 0);
	}
}

UT_TEST(required_writes_keep_the_negative_identity_after_active_gc)
{
	ClusterKoSharedMessageV3 first;
	provider_fixture(0, MAIN_FORKNUM, true);
	first = sync_request;
	UT_ASSERT(sync_commit());
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
	for (provider_op = 0; provider_op < 5; provider_op++) {
		UT_ASSERT(!provider_call());
		UT_ASSERT_EQ(provider_steps[1], 0);
		UT_ASSERT_EQ(sync_call_count(), 0);
	}
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(entered_sync_blocks_marker_through_close_and_error);
	UT_RUN(committed_drop_cancels_without_io_after_active_gc);
	UT_RUN(closing_cancels_new_sync_without_releasing_entered_call);
	UT_RUN(unknown_refusal_never_becomes_canceled_or_io);
	UT_RUN(reservation_other_locator_and_native_scope_are_not_retirement);
	UT_RUN(original_gates_and_missing_file_keep_their_polarity);
	UT_RUN(provider_scope_blocks_marker_through_required_io_and_queue);
	UT_RUN(retired_required_io_never_opens_or_uses_cached_handle);
	UT_RUN(provider_error_releases_only_its_call_and_never_returns_success);
	UT_RUN(closing_unknown_and_full_table_refuse_before_provider_access);
	UT_RUN(provider_original_gates_recovery_and_skip_fsync_remain_distinct);
	UT_RUN(required_writes_keep_the_negative_identity_after_active_gc);
	UT_DONE();
	return ut_failed_count != 0;
}
