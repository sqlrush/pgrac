/* Eight cached readers delay a real remote writer, then reacquire its image.
 * Original requester/master/holder bodies keep state across both transfers.
 * Actor scheduling, authentication and storage remain controlled host leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
static bool hc_seed_eight(int master, int mode, int node);
static void hc_release_readers(void);
#include "test_cluster_native_hot_cycle_successor.inc"
#define pfree(pointer) free(pointer)
#include "test_cluster_native_hot_block_owner.inc"
#undef pfree
#if defined(NATIVE_HOT_CYCLE_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "hot-cycle assertion-OFF requires an assertion-disabled configuration"
#endif

enum { HC_READERS = 8 };
static LWLock *hc_locks[HC_READERS];
static PGPROC hc_procs[HC_READERS];
static ResourceOwner hc_owners[HC_READERS];
static PGPROC *hc_seed_proc;
static ResourceOwner hc_seed_owner;
static int hc_seed_pid;
static BackendId hc_seed_backend;
static uint64 hc_seed_generation, hc_seed_E;
static unsigned hc_releases, hc_errors, hc_drains, hc_seed_calls, hc_wakes;
static int hc_order;
static bool hc_inject_error;

static void hc_seed_context(void)
{
	MyProc = hc_seed_proc; MyProcPid = hc_seed_pid;
	MyBackendId = hc_seed_backend; CurrentResourceOwner = hc_seed_owner;
	request_pins = 1;
}
static void hc_actor_context(unsigned actor)
{
	MyProc = &hc_procs[actor]; MyProcPid = MyProc->pid;
	MyBackendId = MyProc->backendId; CurrentResourceOwner = hc_owners[actor];
	request_pins = 1;
}
static unsigned hc_held(void)
{
	unsigned held = 0;
	for (unsigned n = 0; n < HC_READERS; n++) held += hc_locks[n] != NULL;
	return held;
}
static bool hc_seed_eight(int master, int mode, int node)
{
	NM_REQUIRE(nm_parent_as(master, mode, node));
	UT_ASSERT_EQ(hc_seed_calls, 0); hc_seed_calls++;
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	hc_seed_proc = MyProc; hc_seed_pid = MyProcPid;
	hc_seed_backend = MyBackendId; hc_seed_owner = CurrentResourceOwner;
	UT_ASSERT(hc_seed_owner->firstchild == NULL);
	hc_seed_generation = cluster_pcm_own_gen_get(0);
	hc_seed_E = request_entry()->resource_x_bootstrap_round.highest_attempt_floor;
	hc_wakes = rl_service_wakes;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	rl_barrier = rl_cancel_wait = false;
	rl_on_wait = rl_on_share = rl_after_deliver = NULL;
	pg_atomic_fetch_add_u32(&request_buf.state, HC_READERS * BUF_REFCOUNT_ONE);
	for (unsigned n = 0; n < HC_READERS; n++) {
		hc_procs[n] = *hc_seed_proc;
		hc_procs[n].pid += 100 + n;
		hc_procs[n].backendId += 1 + n;
		hc_procs[n].pgprocno += 1 + n;
		hc_procs[n].cluster_grd_generation += 1 + n;
		hc_owners[n] = ResourceOwnerCreate(hc_seed_owner, "hot remote reader");
		hc_actor_context(n); NM_REQUIRE(rl_run(NULL));
		UT_ASSERT_EQ(fake_lwlock_depth, 1);
		UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_SHARED));
		/* Park the scheduler context without releasing this actor's hold. */
		hc_locks[n] = fake_lwlock_stack[0];
		fake_lwlock_stack[0] = NULL; fake_lwlock_depth = 0; fake_lwlock_held = NULL;
		hc_seed_context();
	}
	UT_ASSERT_EQ(hc_held(), HC_READERS); UT_ASSERT_EQ(ordinary_sends, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), hc_seed_generation);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.highest_attempt_floor, hc_seed_E);
	return true;
}
static void hc_release_one(unsigned actor)
{
	ClusterPcmOwnEntry before = request_own;
	volatile bool caught = false;
	UT_ASSERT_EQ(fake_lwlock_depth, 0); UT_ASSERT(hc_locks[actor] != NULL);
	hc_actor_context(actor);
	fake_lwlock_stack[0] = hc_locks[actor]; fake_lwlock_mode_stack[0] = LW_SHARED;
	fake_lwlock_depth = 1; fake_lwlock_held = hc_locks[actor]; fake_lwlock_mode = LW_SHARED;
	PG_TRY(); {
		PG_TRY(); {
			if (hc_inject_error && hc_releases == 3) pg_re_throw();
		} PG_FINALLY(); {
			rl_actual_unlock(&request_buf);
		} PG_END_TRY();
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT_EQ(caught, hc_inject_error && hc_releases == 3);
	if (caught) hc_errors++;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(memcmp(&before, &request_own, sizeof(before)) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_fetch_sub_u32(&request_buf.state,
		BUF_REFCOUNT_ONE)), HC_READERS + 1 - hc_releases);
	hc_locks[actor] = NULL; hc_releases++;
	hc_seed_context(); ResourceOwnerDelete(hc_owners[actor]); hc_owners[actor] = NULL;
}
static void hc_release_readers(void)
{
	BackendType service = MyBackendType;
	/* The peer writer later serves X->S in its own process with no parked
	 * local readers. It uses the original content-release event as well. */
	if (hc_seed_calls == 0) return;
	UT_ASSERT_EQ(hc_drains, 0); hc_drains++;
	UT_ASSERT_EQ(hc_held(), HC_READERS); UT_ASSERT(holder_content_busy);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	for (unsigned n = 0; n < HC_READERS; n++) {
		unsigned actor = hc_order ? HC_READERS - n - 1 : n;
		UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), hc_seed_generation);
		UT_ASSERT_EQ(ordinary_sends, 1);
		MyBackendType = B_BACKEND; hc_release_one(actor); MyBackendType = service;
		UT_ASSERT_EQ(hc_held(), HC_READERS - n - 1);
	}
	UT_ASSERT_EQ(rl_service_wakes - hc_wakes, HC_READERS);
	UT_ASSERT(hc_seed_owner->firstchild == NULL);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&request_buf.state)), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS, 0);
	printf("# hot-cycle source readers=%u releases=%u errors=%u remaining=%u gen=" UINT64_FORMAT "\n",
		(unsigned)HC_READERS, hc_releases, hc_errors, hc_held(), cluster_pcm_own_gen_get(0));
}
static bool hc_master(int reader, int writer, int mode)
{
	NM_REQUIRE(ss_master(reader, writer, mode));
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	return true;
}
static bool hc_reader(int master, int writer, int mode, int source, uint64 boot)
{
	NM_REQUIRE(ss_reader_role(master, writer, mode, source, boot));
	UT_ASSERT_EQ(hc_seed_calls, 1); UT_ASSERT_EQ(hc_drains, 1);
	UT_ASSERT_EQ(hc_releases, HC_READERS); UT_ASSERT_EQ(hc_held(), 0);
	UT_ASSERT_EQ(hc_errors, hc_inject_error ? 1 : 0);
	for (unsigned n = 0; n < HC_READERS; n++) UT_ASSERT(hc_owners[n] == NULL);
	UT_ASSERT(hc_seed_owner->firstchild == NULL);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), hc_seed_generation + 2);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	printf("# hot-cycle reader order=%d errors=%u requests=%u extra_reads=%u native=%u flags=%u gen=" UINT64_FORMAT "\n",
		hc_order, hc_errors, ordinary_sends, disk_reads, request_buf.pcm_state,
		cluster_pcm_own_flags_get(0), cluster_pcm_own_gen_get(0));
	return true;
}
static bool hc_writer(int master, int reader, int mode)
{
	NM_REQUIRE(ss_writer_role(master, reader, mode));
	UT_ASSERT_EQ(hc_seed_calls, 0); UT_ASSERT_EQ(hc_held(), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&request_buf) == NULL);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_DIRTY | BM_IO_IN_PROGRESS), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	printf("# hot-cycle writer writes=%u IO-wakes=%u native=%u flags=%u\n",
		holder_writes, holder_output_wakes, request_buf.pcm_state, cluster_pcm_own_flags_get(0));
	return true;
}
static void hc_run(int order, bool error)
{
	hc_order = order; hc_inject_error = error;
	hc_releases = hc_errors = hc_drains = hc_seed_calls = 0;
	memset(hc_locks, 0, sizeof(hc_locks)); memset(hc_owners, 0, sizeof(hc_owners));
	nt_master_role = hc_master; nt_holder_role = hc_writer; nt_requester = hc_reader;
	nt_run(5);
}
UT_TEST(test_hot_cycle_forward_remote_writer_and_writeback) { hc_run(0, false); }
UT_TEST(test_hot_cycle_reverse_remote_writer_and_writeback) { hc_run(1, false); }
UT_TEST(test_hot_cycle_forward_reader_ERROR_keeps_other_progress) { hc_run(0, true); }
UT_TEST(test_hot_cycle_reverse_reader_ERROR_keeps_other_progress) { hc_run(1, true); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# hot-cycle USE_ASSERT_CHECKING=1\n");
#else
	printf("# hot-cycle USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(4);
	UT_RUN(test_hot_cycle_forward_remote_writer_and_writeback);
	UT_RUN(test_hot_cycle_reverse_remote_writer_and_writeback);
	UT_RUN(test_hot_cycle_forward_reader_ERROR_keeps_other_progress);
	UT_RUN(test_hot_cycle_reverse_reader_ERROR_keeps_other_progress);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
