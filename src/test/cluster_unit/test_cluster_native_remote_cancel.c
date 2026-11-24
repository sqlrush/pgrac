/* Actual ordinary ERROR/exit cleanup and independent M/H source retirement.
 * Authentication, OS locks/storage and error injection are controlled leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_remote_x_fixture.inc"
#ifndef REMOTE_CANCEL_WRITE
#define NATIVE_RESIDENT_LOCK_REUSE_FIXTURE
#include "test_cluster_native_resident_lock_fixture.inc"
#undef NATIVE_RESIDENT_LOCK_REUSE_FIXTURE
#endif

static unsigned rc_cut, rc_native;
static bool rc_late_callback;
static bool rc_exiting;
static sigjmp_buf rc_exit_jump;

static void rc_interrupt(void)
{
	if (!rc_exiting) pg_re_throw();
	/* Native ipc.c removes each registration before calling it. FATAL/exit
	 * abandons the interrupted PG_TRY frames instead of running PG_CATCH.
	 * shmem_exit releases LWLocks first; late ProcKill cancels CV sleep. */
	error_context_stack = NULL;
	while (fake_lwlock_depth != 0)
		LWLockRelease(fake_lwlock_stack[fake_lwlock_depth - 1]);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(disk_exit_count > 0);
	while (disk_exit_count != 0) {
		unsigned n = --disk_exit_count;
		disk_exit_callbacks[n](1, disk_exit_arguments[n]);
	}
	ConditionVariableCancelSleep();
	siglongjmp(rc_exit_jump, 1);
}

static void rc_deliver(void)
{
	nc_deliver();
	input_observation = ordinary_active->observation;
	if (rc_cut == 0) rc_interrupt();
}
static void rc_after_copy(void)
{
	if (request_entry()->resource_x_bootstrap_round.delivery_executor_purpose
		== RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING
		&& memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0) {
		stage_after_content = NULL;
		rc_interrupt();
	}
}
static void rc_io_boundary(bool completed)
{
	if ((rc_cut == 2 && !completed) || (rc_cut == 3 && completed)) {
		complete_io_boundary = NULL;
		rc_interrupt();
	}
}
static bool rc_probe(ResourceXIntentSlot *slot)
{
	uint8 bytes[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES];
	ResourceXAcquisitionRef ref; uint32 examined;
	fake_pcm_clock_us += 1000000;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult r = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, slot, bytes, sizeof(bytes), &examined, &ref);
		NM_REQUIRE(r != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (r == RESOURCE_X_INTENT_PROBE_FOUND) return true;
		if (r == RESOURCE_X_INTENT_PROBE_IDLE) break;
	}
	return false;
}
static void rc_completed(void)
{
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_broadcasts, rc_cut >= 2 ? 1 : 0);
	UT_ASSERT_EQ(complete_io_calls, rc_cut == 2 ? 2 : rc_cut == 3 ? 1 : 0);
#ifdef REMOTE_CANCEL_WRITE
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(wc_legacy, 0);
#else
	UT_ASSERT_EQ(complete_commits, rc_cut >= 2 ? 1 : 0);
#endif
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + (rc_cut >= 2));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(!fake_cv_prepared);
}
static bool rc_requester(int master, int holder, int mode, int source, uint64 boot)
{
#ifndef REMOTE_CANCEL_WRITE
	ClusterBufmgrFirstFill read;
#endif
	ClusterPcmResourceXMasterState *state = NULL;
	ResourceXIntentSlot observed, staged;
	ResourceXDecodedFrame done, consumed;
	ResourceXRequesterJoinSnapshot joined;
	ClusterICEnvelope env = {0};
	PGIOAlignedBlock before;
#ifdef REMOTE_CANCEL_WRITE
	rx_writer_start(rc_native, 4);
#else
	if (rc_native == 0) ordinary_start(&read, 2);
	else rl_start(rc_native, 2);
#endif
	fake_gcs_master_node = 0;
	nc_fd = master; nc_image_fd = holder; nc_mode = mode;
	nc_source_node = source; nc_source_boot = boot;
	auth_observation_hook = nc_auth_peer;
	ordinary_delivery_transport = rc_deliver;
	memcpy(before.data, disk_shared.data, BLCKSZ);
	if (rc_cut == 1) stage_after_content = rc_after_copy;
	if (rc_cut >= 2) complete_io_boundary = rc_io_boundary;
	if (rc_exiting) {
		sigjmp_buf *saved = PG_exception_stack;
		ErrorContextCallback *context = error_context_stack;
		if (sigsetjmp(rc_exit_jump, 0) == 0) {
#ifdef REMOTE_CANCEL_WRITE
			(void)rx_prepare(NULL);
#else
			if (rc_native == 0) (void)ordinary_run(&read); else (void)rl_run(NULL);
#endif
			UT_ASSERT(false);
		}
		PG_exception_stack = saved; error_context_stack = context;
		UT_ASSERT_EQ(disk_exit_count, 0); UT_ASSERT_EQ(disk_exit_lifo_errors, 0);
	} else {
#ifdef REMOTE_CANCEL_WRITE
	NM_REQUIRE(rx_prepare(NULL) == NULL);
#else
	NM_REQUIRE(rc_native == 0 ? !ordinary_run(&read) : !rl_run(NULL));
#endif
	}
	rc_completed();
	UT_ASSERT_EQ(ordinary_leaves, 1);
	UT_ASSERT_EQ(prepared_episode.path, rc_native == 0
		? RESOURCE_X_ACQUIRE_FIRST_FILL : RESOURCE_X_ACQUIRE_RESIDENT);
	UT_ASSERT_EQ(prepared_episode.base_generation, request_base.generation);
	UT_ASSERT_EQ(prepared_episode.reservation_token, request_token);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS),
		rc_cut >= 2 || (rc_cut == 0 && rc_native != 0) ? BM_VALID : BM_IO_ERROR);
	if (rc_cut == 0) UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
	else UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	if (rc_cut >= 2) {
		UT_ASSERT_EQ(request_buf.pcm_state, nt_target_mode);
		UT_ASSERT_EQ(request_buf.buffer_type, nt_target_mode == PCM_STATE_X ? BUF_TYPE_XCUR : BUF_TYPE_SCUR);
	}
	/* The original background probe creates/offers the shared receipt. */
	MyBackendType = B_LMS;
	NM_REQUIRE(rc_probe(&observed)); NM_REQUIRE(nc_requester_state(&state));
	NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
		&observed, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
	NM_REQUIRE(nc_decode(state->requester_settlement_intent.payload,
		RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES, &done));
	UT_ASSERT_EQ(done.common.outcome, rc_cut >= 2 ? RESOURCE_X_OUTCOME_OK : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE);
	UT_ASSERT_EQ(done.body.acquire_completion.native_generation, request_base.generation + (rc_cut >= 2));
	UT_ASSERT_EQ(done.common.target_mode, nt_target_mode);
	if (!rc_late_callback) NM_REQUIRE(nc_callback(&staged));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &done));
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &consumed));
	UT_ASSERT_EQ(consumed.common.outcome, rc_cut >= 2
		? RESOURCE_X_OUTCOME_DUPLICATE : RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED);
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0; env.dest_node_id = 1;
	env.epoch = 17; env.payload_length = consumed.payload_bytes; auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &consumed, 61, &joined) == RESOURCE_X_APPLY_APPLIED);
	if (rc_late_callback) {
		UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		if (mode == 3) NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(
			&env, &consumed, 61, &joined) == RESOURCE_X_APPLY_DUPLICATE);
		NM_REQUIRE(nc_callback(&staged));
	}
	if (mode == 3) {
		ClusterPcmResourceXBootstrapRound before_round = request_entry()->resource_x_bootstrap_round;
		ClusterPcmOwnEntry before_own = request_own;
		ClusterPcmResourceXReadIntent before_intent = state->requester_settlement_intent;
		uint32 before_state = pg_atomic_read_u32(&request_buf.state);
		ResourceXApplyResult replay = gcs_block_resource_x_requester_join_ingress(
			&env, &consumed, 61, &joined);
		/* A quiet receipt is gone after both owners retire it. Its late replay
		 * must be stale, while an installed retained cover remains replayable. */
		UT_ASSERT_EQ(replay, rc_cut < 2 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT(memcmp(&before_round, &request_entry()->resource_x_bootstrap_round, sizeof(before_round)) == 0);
		UT_ASSERT(memcmp(&before_own, &request_own, sizeof(before_own)) == 0);
		UT_ASSERT(memcmp(&before_intent, &state->requester_settlement_intent, sizeof(before_intent)) == 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), before_state);
	}
	UT_ASSERT_EQ(state->requester_settlement_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	if (rc_cut < 2) {
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 0);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor, done.common.assertion_sequence);
	} else UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	rc_completed();
#ifdef REMOTE_CANCEL_WRITE
	cluster_bufmgr_pcm_x_writer_exception_cleanup_all();
#endif
	if (LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)))
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(fake_lwlock_depth, 0); complete_idle(); return true;
}
static void rc_run(unsigned cut, int mode, bool late)
{
	rc_cut = cut; rc_late_callback = late;
	nt_requester = rc_requester;
#ifdef REMOTE_CANCEL_WRITE
	nt_target_mode = PCM_STATE_X;
#else
	nt_target_mode = PCM_STATE_S;
#endif
	for (unsigned source = 0; source < 2; source++) {
		nt_source_mode = source == 0 ? PCM_STATE_S : PCM_STATE_X;
		nt_seed_holder = source == 0 ? nm_parent_as : xt_seed;
		nt_source_finished = source == 0 ? NULL : xt_source_finished;
		nt_keep_seed_bytes = source != 0;
#ifdef REMOTE_CANCEL_WRITE
		rc_native = 1;
#else
		rc_native = 0;
#endif
		for (; rc_native <= 2; rc_native++) {
			printf("# Remote cleanup: source=%s target=%s native=%u cut=%u exit=%u order=%d late=%u\n",
				source == 0 ? "S" : "dirty-X", nt_target_mode == PCM_STATE_S ? "S" : "X",
				rc_native, rc_cut, rc_exiting, mode, rc_late_callback);
			nt_run(mode);
		}
	}
}
UT_TEST(test_remote_cancel_before_copy) { rc_run(0, 0, false); }
UT_TEST(test_remote_cancel_after_copy_before_C) { rc_run(1, 0, false); }
UT_TEST(test_remote_ERROR_after_C_before_IO_end) { rc_run(2, 0, false); }
UT_TEST(test_remote_ERROR_after_IO) { rc_run(3, 0, false); }
UT_TEST(test_remote_quiet_before_holder_ACK) { rc_run(1, 2, false); }
UT_TEST(test_remote_quiet_waits_holder_image_callback) { rc_run(1, 4, false); }
UT_TEST(test_remote_quiet_ACK_before_requester_callback) { rc_run(1, 0, true); }
UT_TEST(test_remote_installed_ACK_before_requester_callback) { rc_run(3, 0, true); }
UT_TEST(test_remote_duplicate_image_and_retired_notice) { rc_run(1, 3, true); }
UT_TEST(test_remote_exit_before_copy) { rc_exiting = true; rc_run(0, 0, false); rc_exiting = false; }
UT_TEST(test_remote_exit_after_copy_before_C) { rc_exiting = true; rc_run(1, 0, false); rc_exiting = false; }
UT_TEST(test_remote_exit_after_C_before_IO_end) { rc_exiting = true; rc_run(2, 0, false); rc_exiting = false; }
UT_TEST(test_remote_exit_after_IO) { rc_exiting = true; rc_run(3, 0, false); rc_exiting = false; }
UT_TEST(test_remote_exit_quiet_before_holder_ACK) { rc_exiting = true; rc_run(1, 2, false); rc_exiting = false; }
UT_TEST(test_remote_exit_waits_holder_image_callback) { rc_exiting = true; rc_run(1, 4, false); rc_exiting = false; }
UT_TEST(test_remote_exit_quiet_ACK_before_requester_callback) { rc_exiting = true; rc_run(1, 0, true); rc_exiting = false; }
UT_TEST(test_remote_exit_installed_ACK_before_requester_callback) { rc_exiting = true; rc_run(3, 0, true); rc_exiting = false; }
UT_TEST(test_remote_exit_duplicate_image_and_retired_notice) { rc_exiting = true; rc_run(1, 3, true); rc_exiting = false; }
int main(void)
{
	UT_PLAN(18);
	UT_RUN(test_remote_cancel_before_copy);
	UT_RUN(test_remote_cancel_after_copy_before_C);
	UT_RUN(test_remote_ERROR_after_C_before_IO_end);
	UT_RUN(test_remote_ERROR_after_IO);
	UT_RUN(test_remote_quiet_before_holder_ACK);
	UT_RUN(test_remote_quiet_waits_holder_image_callback);
	UT_RUN(test_remote_quiet_ACK_before_requester_callback);
	UT_RUN(test_remote_installed_ACK_before_requester_callback);
	UT_RUN(test_remote_duplicate_image_and_retired_notice);
	UT_RUN(test_remote_exit_before_copy);
	UT_RUN(test_remote_exit_after_copy_before_C);
	UT_RUN(test_remote_exit_after_C_before_IO_end);
	UT_RUN(test_remote_exit_after_IO);
	UT_RUN(test_remote_exit_quiet_before_holder_ACK);
	UT_RUN(test_remote_exit_waits_holder_image_callback);
	UT_RUN(test_remote_exit_quiet_ACK_before_requester_callback);
	UT_RUN(test_remote_exit_installed_ACK_before_requester_callback);
	UT_RUN(test_remote_exit_duplicate_image_and_retired_notice);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
