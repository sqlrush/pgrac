/* Original exit registration and dispatcher around the actual TX owner. */
#define main tx_enqueue_original_main
int tx_enqueue_original_main(void);
#include "test_cluster_r4_tx_enqueue.c"
#undef main

#include "storage/dsm.h"
#include "storage/ipc.h"

static void atexit_callback(void);
void cluster_grd_cleanup_on_backend_exit_callback(int code, Datum arg);
bool IsUnderPostmaster = true;
bool PostmasterLWLockWaitForbidden;

#include "test_cluster_tx_exit_dispatch.inc"
#ifndef CLUSTER_TX_EXIT_DISPATCH_PRESENT
#error "Original IPC callback tables and dispatcher are required"
#endif

static char exit_events[32];
static unsigned exit_event_count, exit_grd_calls, exit_dsm_calls, exit_proc_calls;
static unsigned exit_error_calls;
static int exit_expected_code;
static ClusterTxwWaitSlot exit_peer_slot;
static PGPROC exit_peer_proc;

static void
exit_event(char event)
{
	UT_ASSERT(exit_event_count + 1 < sizeof(exit_events));
	if (exit_event_count + 1 < sizeof(exit_events)) {
		exit_events[exit_event_count++] = event;
		exit_events[exit_event_count] = '\0';
	}
}

static void
exit_assert_peer(void)
{
	UT_ASSERT_EQ(memcmp(&ClusterTxw->slots[1], &exit_peer_slot, sizeof(exit_peer_slot)), 0);
	UT_ASSERT_EQ(memcmp(&test_procs[1], &exit_peer_proc, sizeof(exit_peer_proc)), 0);
}

static void
exit_assert_owner_retired(void)
{
	ClusterLmdWaitStateSnapshot snapshot;

	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, CLUSTER_TXW_SLOT_FREE);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(test_real_wait_state_read_exact(&test_procs[0].cluster_lmd_wait, &snapshot),
				 CLUSTER_LMD_WAIT_STATE_READ_INACTIVE);
	exit_assert_peer();
}

/* OS atexit was installed before this caller boundary. Never execute it. */
static void
atexit_callback(void)
{
	abort();
}

void
LWLockReleaseAll(void)
{
	UT_ASSERT(shmem_exit_inprogress);
	exit_event('L');
}

void
reset_on_dsm_detach(void)
{}

void
dsm_backend_shutdown(void)
{
	UT_ASSERT(shmem_exit_inprogress);
	UT_ASSERT_EQ(exit_grd_calls, exit_proc_calls + (MyProc != NULL));
	exit_assert_owner_retired();
	exit_event('D');
	exit_dsm_calls++;
}

void
cluster_grd_cleanup_on_backend_exit_callback(int code, Datum arg)
{
	UT_ASSERT(shmem_exit_inprogress && MyProc == &test_procs[0]);
	UT_ASSERT_EQ(code, exit_expected_code);
	UT_ASSERT_EQ(arg, 0);
	exit_assert_owner_retired();
	exit_event('G');
	exit_grd_calls++;
}

static void
exit_proc_teardown(int code, Datum arg)
{
	UT_ASSERT(shmem_exit_inprogress && MyProc == &test_procs[0]);
	UT_ASSERT_EQ(code, exit_expected_code);
	UT_ASSERT_EQ(arg, 19);
	UT_ASSERT_EQ(exit_grd_calls, exit_proc_calls + 1);
	UT_ASSERT(exit_dsm_calls > exit_proc_calls);
	exit_assert_owner_retired();
	exit_event('P');
	exit_proc_calls++;
	MyProc = NULL;
}

static void
exit_register_original_hooks(void)
{
#include "test_cluster_tx_exit_registration.inc"
}

static void
exit_publish_owner(uint32 kind, bool published)
{
	ClusterTxLocator locator = test_locator();
	ClusterTTStatusKey source = test_source_key();
	ClusterLmdVertex blocker = { 0 };
	uint64 sequence;

	if (kind == CLUSTER_TXW_SLOT_TARGET)
		UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	else
		UT_ASSERT(txw_slot_set(0, &source, kind == CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX));
	if (!published)
		return;
	sequence = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
											  TEST_EPOCH, test_local_xid);
	txw_exact_waiter_vertex(&test_wfg_waiter, 0, TEST_EPOCH, test_local_xid, sequence);
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&test_wfg_waiter, &blocker, 0));
}

static void
exit_fixture(uint32 kind, bool published, int code)
{
	ClusterTxLocator peer = test_locator();

	on_exit_reset();
	reset_fixture();
	atexit_callback_setup = true;
	shmem_exit_inprogress = false;
	exit_event_count = exit_grd_calls = exit_dsm_calls = exit_proc_calls = exit_error_calls = 0;
	exit_events[0] = '\0';
	exit_expected_code = code;
	peer.xid++;
	peer.tt_wrap++;
	UT_ASSERT(txw_target_slot_set_if_free(1, &peer));
	test_real_wait_state_init(&test_procs[1].cluster_lmd_wait);
	test_real_wait_state_publish(&test_procs[1].cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
								 TEST_EPOCH + 1, peer.xid);
	exit_peer_slot = ClusterTxw->slots[1];
	exit_peer_proc = test_procs[1];
	exit_publish_owner(kind, published);
	exit_register_original_hooks();
	on_shmem_exit(exit_proc_teardown, (Datum)19);
}

static bool
exit_dispatch_stops(int code)
{
	if (sigsetjmp(test_fail_stop_stack, 1) == 0) {
		test_fail_stop_armed = true;
		shmem_exit(code);
		test_fail_stop_armed = false;
		return false;
	}
	test_fail_stop_armed = false;
	return true;
}

UT_TEST(tx_exit_dispatch_retires_all_three_owner_kinds_before_lower_cleanup)
{
	const uint32 kinds[]
		= { CLUSTER_TXW_SLOT_SOURCE, CLUSTER_TXW_SLOT_TARGET, CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX };

	for (unsigned i = 0; i < lengthof(kinds); i++)
		for (int code = 0; code < 2; code++) {
			exit_fixture(kinds[i], true, code);
			shmem_exit(code);
			UT_ASSERT_STR_EQ(exit_events, "LGDP");
			UT_ASSERT(!shmem_exit_inprogress && MyProc == NULL);
			UT_ASSERT_EQ(before_shmem_exit_index | on_shmem_exit_index, 0);
			UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
			UT_ASSERT_EQ(test_wait_clear_calls, 1);
			UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
			exit_assert_owner_retired();
		}
}

UT_TEST(tx_exit_dispatch_retires_unpublished_slots_without_canceling_peer)
{
	const uint32 kinds[]
		= { CLUSTER_TXW_SLOT_SOURCE, CLUSTER_TXW_SLOT_TARGET, CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		exit_fixture(kinds[i], false, 1);
		shmem_exit(1);
		UT_ASSERT_STR_EQ(exit_events, "LGDP");
		UT_ASSERT_EQ(test_wfg_exact_cancel_calls | test_wait_clear_calls, 0);
		exit_assert_owner_retired();
	}
}

static void
exit_earlier_error(int code, Datum arg)
{
	UT_ASSERT_EQ(code, exit_expected_code);
	UT_ASSERT_EQ(arg, 29);
	exit_event('E');
	exit_error_calls++;
	ereport(ERROR, (errmsg_internal("fixture earlier exit callback error")));
}

UT_TEST(tx_exit_dispatch_error_removes_itself_before_original_cleanup_reentry)
{
	for (unsigned i = 0; i < 2; i++) {
		TestD9AuthoritySnapshot before;

		exit_fixture(i ? CLUSTER_TXW_SLOT_SOURCE : CLUSTER_TXW_SLOT_TARGET, true, 1);
		before = snapshot_d9_authorities();
		before_shmem_exit(exit_earlier_error, (Datum)29);
		UT_ASSERT(exit_dispatch_stops(1));
		UT_ASSERT_EQ(test_last_elevel, ERROR);
		UT_ASSERT_STR_EQ(exit_events, "LE");
		UT_ASSERT_EQ(before_shmem_exit_index, 2);
		UT_ASSERT_EQ(exit_grd_calls | exit_dsm_calls | exit_proc_calls, 0);
		assert_d9_authorities_equal(&before);
		exit_assert_peer();
		shmem_exit(1);
		UT_ASSERT_STR_EQ(exit_events, "LELGDP");
		UT_ASSERT_EQ(exit_error_calls, 1);
		UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
		UT_ASSERT_EQ(test_wait_clear_calls, 1);
		exit_assert_owner_retired();
	}
}

UT_TEST(tx_exit_dispatch_invalid_wait_stops_before_lower_cleanup)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		TestD9AuthoritySnapshot before;
		bool stopped;

		exit_fixture(CLUSTER_TXW_SLOT_TARGET, true, 1);
		test_wait_read_override = true;
		test_wait_read_result
			= fault == 2 ? CLUSTER_LMD_WAIT_STATE_READ_BUSY : CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
		if (fault == 0)
			test_wait_snapshot.request_id = 99;
		else if (fault == 1)
			test_wait_snapshot.wait_seq = 0;
		before = snapshot_d9_authorities();
		stopped = exit_dispatch_stops(1);
		UT_ASSERT(stopped);
		if (!stopped)
			continue;
		UT_ASSERT_EQ(test_last_elevel, PANIC);
		UT_ASSERT(!test_exceptional_condition_hit);
		UT_ASSERT_STR_EQ(exit_events, "L");
		UT_ASSERT_EQ(before_shmem_exit_index, 1);
		UT_ASSERT_EQ(on_shmem_exit_index, 1);
		UT_ASSERT_EQ(exit_grd_calls | exit_dsm_calls | exit_proc_calls, 0);
		UT_ASSERT_EQ(test_wfg_exact_cancel_calls | test_wait_clear_calls, 0);
		assert_d9_authorities_equal(&before);
		exit_assert_peer();
		/* The next fixture resets private storage, never retries this PANIC. */
	}
}

UT_TEST(tx_exit_dispatch_is_repeatable_and_reused_slot_gets_new_wait_identity)
{
	uint64 sequence;

	exit_fixture(CLUSTER_TXW_SLOT_TARGET, true, 0);
	sequence = test_wfg_waiter.wait_seq;
	shmem_exit(0);
	shmem_exit(0);
	UT_ASSERT_STR_EQ(exit_events, "LGDPLD");
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	exit_assert_owner_retired();

	MyProc = &test_procs[0];
	exit_expected_code = 1;
	exit_publish_owner(CLUSTER_TXW_SLOT_TARGET, true);
	UT_ASSERT(test_wfg_waiter.wait_seq > sequence);
	exit_register_original_hooks();
	on_shmem_exit(exit_proc_teardown, (Datum)19);
	shmem_exit(1);
	UT_ASSERT_STR_EQ(exit_events, "LGDPLDLGDP");
	UT_ASSERT_EQ(test_wfg_exact_cancel_calls, 2);
	UT_ASSERT_EQ(test_wait_clear_calls, 2);
	UT_ASSERT_EQ(exit_grd_calls, 2);
	UT_ASSERT_EQ(exit_proc_calls, 2);
	exit_assert_owner_retired();
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(tx_exit_dispatch_retires_all_three_owner_kinds_before_lower_cleanup);
	UT_RUN(tx_exit_dispatch_retires_unpublished_slots_without_canceling_peer);
	UT_RUN(tx_exit_dispatch_error_removes_itself_before_original_cleanup_reentry);
	UT_RUN(tx_exit_dispatch_invalid_wait_stops_before_lower_cleanup);
	UT_RUN(tx_exit_dispatch_is_repeatable_and_reused_slot_gets_new_wait_identity);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
