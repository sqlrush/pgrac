/* Actual CF request construction and owner/registry. Clock, transport and
 * ACK delivery are leaves; no alternate request or retry implementation.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
cf_budget_release(void)
{
	bool saved = owner_ack_on_wait;

	claim_teardown = owner_ack_on_wait = true;
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ShareLock), CLUSTER_CF_RELEASE_CONFIRMED);
	claim_teardown = false;
	owner_ack_on_wait = saved;
}

UT_TEST(startup_cf_request_uses_remaining_original_budget)
{
	unsigned before;
	int configured = cluster_cf_enqueue_timeout_ms;

	entry_fixture(false);
	cf_budget_clock_fail = false;
	set_cf_clock_ns = NS_PER_S;
	cluster_cf_enqueue_timeout_ms = 1000;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	before = g_seven_count;
	UT_ASSERT(cluster_cf_lock_deadline_v1(ShareLock, set_cf_clock_ns + 137 * NS_PER_MS));
	UT_ASSERT_EQ(g_seven_count, before + 1);
	UT_ASSERT_EQ(g_last_timeout_ms, 137);
	UT_ASSERT_EQ(g_last_wait_event, WAIT_EVENT_CLUSTER_CF_ENQUEUE);
	cf_budget_release();
	cluster_cf_enqueue_timeout_ms = configured;
}

UT_TEST(startup_cf_expired_budget_does_not_publish_a_request)
{
	ClusterCfAttemptObservation attempt;
	unsigned before;
	uint64 sequence;

	entry_fixture(false);
	cf_budget_clock_fail = false;
	set_cf_clock_ns = NS_PER_S;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	before = g_seven_count;
	sequence = cluster_cf_attempt_sequence();
	UT_ASSERT(!cluster_cf_lock_deadline_v1(ShareLock, set_cf_clock_ns));
	UT_ASSERT_EQ(g_seven_count, before);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT(cluster_cf_last_attempt_since(sequence, &attempt));
	UT_ASSERT(!cluster_cf_attempt_wait_slice(&attempt));
	if (cluster_cf_held(ShareLock))
		cf_budget_release();
}

UT_TEST(startup_bounded_source_returns_cf_slice_to_its_caller)
{
	for (unsigned zero_mode = 0; zero_mode < 2; ++zero_mode) {
		ClusterControlRootResult result;
		ClusterWalSourceRef source, zero = { 0 };
		XLogRecPtr redo = 99;
		ClusterCfAttemptObservation attempt;
		uint64 before;

		recovery_fixture(0);
		set_cf_clock_ns = NS_PER_S;
		memset(&source, 0xa5, sizeof(source));
		before = cluster_cf_attempt_sequence();
		result = cluster_control_root_recovery_source_deadline_v1(
			&recovery_expected, &recovery_token, &source, &redo,
			zero_mode ? 0 : set_cf_clock_ns + 137 * NS_PER_MS);
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(recovery_reads, 0);
		UT_ASSERT_EQ(memcmp(&source, &zero, sizeof(source)), 0);
		UT_ASSERT_EQ(redo, InvalidXLogRecPtr);
		UT_ASSERT(cluster_cf_last_attempt_since(before, &attempt));
		UT_ASSERT(cluster_cf_attempt_wait_slice(&attempt));
		UT_ASSERT_EQ(g_last_timeout_ms, zero_mode ? 1000 : 137);
		if (cluster_cf_held(ShareLock))
			entry_cleanup(ShareLock, cluster_cf_owner_cookie(ShareLock));
	}
}

UT_TEST(startup_three_projection_phases_pass_the_original_cf_deadline)
{
	for (unsigned phase = 0; phase < 3; ++phase) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		ClusterCfAttemptObservation retry;
		bool committed = false;

		if (phase == 2)
			commit_cf_fixture();
		else {
			pre_ir_fixture(phase == 1, false);
			retention_phase = 0;
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		}
		cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns + 137 * NS_PER_MS;
		if (phase == 2) {
			UT_ASSERT(commit_cf_call(&retry, &committed));
			UT_ASSERT(committed);
		} else {
			UT_ASSERT(pre_ir_call(&engage, replay, start));
			UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
		}
		UT_ASSERT_EQ(g_last_timeout_ms, 137);
		UT_ASSERT_EQ(entry_waits, 0);
		if (phase == 2)
			commit_cf_cleanup();
	}
}

UT_TEST(startup_cf_bounded_acquire_keeps_the_original_cleanup_budget)
{
	for (unsigned failure = 0; failure < 3; ++failure) {
		volatile bool returned = false;

		entry_fixture(false);
		set_cf_clock_ns = NS_PER_S;
		g_seven_result
			= failure == 1 ? CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		throw_during_acquire = failure == 2;
		PG_TRY();
		{
			UT_ASSERT_EQ(cluster_cf_lock_deadline_v1(ShareLock, set_cf_clock_ns + 137 * NS_PER_MS),
						 failure == 0);
			returned = true;
		}
		PG_CATCH();
		{
			FlushErrorState();
		}
		PG_END_TRY();
		throw_during_acquire = false;
		UT_ASSERT_EQ(returned, failure != 2);
		UT_ASSERT_EQ(g_last_timeout_ms, 137);
		UT_ASSERT(cluster_cf_held(ShareLock));
		UT_ASSERT_EQ(cf_slot(ShareLock)->owner.request.timeout_ms, 1000);
		if (failure == 0)
			cf_budget_release();
		else
			entry_cleanup(ShareLock, cluster_cf_owner_cookie(ShareLock));
	}
}

UT_TEST(startup_cf_budget_shrinks_without_renewing)
{
	uint64 deadline;
	unsigned before;

	entry_fixture(false);
	set_cf_clock_ns = NS_PER_S;
	deadline = set_cf_clock_ns + 137 * NS_PER_MS;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock_deadline_v1(ShareLock, deadline));
	UT_ASSERT_EQ(g_last_timeout_ms, 137);
	cf_budget_release();
	set_cf_clock_ns += 53 * NS_PER_MS;
	++g_next_request_id;
	UT_ASSERT(cluster_cf_lock_deadline_v1(ShareLock, deadline));
	UT_ASSERT_EQ(g_last_timeout_ms, 84);
	cf_budget_release();
	set_cf_clock_ns = deadline - 1;
	before = g_seven_count;
	UT_ASSERT(!cluster_cf_lock_deadline_v1(ShareLock, deadline));
	UT_ASSERT_EQ(g_seven_count, before);
	UT_ASSERT(!cluster_cf_held(ShareLock));
}

UT_TEST(startup_cf_zero_and_ordinary_keep_the_configured_attempt)
{
	entry_fixture(false);
	set_cf_clock_ns = NS_PER_S;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	for (unsigned mode = 0; mode < 3; ++mode) {
		++g_next_request_id;
		cf_budget_clock_fail = mode != 0;
		UT_ASSERT(mode == 2 ? cluster_cf_lock(ShareLock)
							: cluster_cf_lock_deadline_v1(
								  ShareLock, mode == 1 ? 0 : set_cf_clock_ns + 2000 * NS_PER_MS));
		UT_ASSERT_EQ(g_last_timeout_ms, 1000);
		cf_budget_release();
	}
	cf_budget_clock_fail = false;
}

UT_TEST(startup_cf_clock_failure_cannot_reuse_a_prior_wait)
{
	ClusterCfAttemptObservation previous, current;
	uint64 sequence;
	unsigned before;

	entry_fixture(false);
	set_cf_clock_ns = NS_PER_S;
	sequence = cluster_cf_attempt_sequence();
	UT_ASSERT(!cluster_cf_lock(ShareLock));
	UT_ASSERT(cluster_cf_last_attempt_since(sequence, &previous));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&previous));
	entry_cleanup(ShareLock, previous.control_owner_id);
	sequence = cluster_cf_attempt_sequence();
	before = g_seven_count;
	cf_budget_clock_fail = true;
	UT_ASSERT(!cluster_cf_lock_deadline_v1(ShareLock, set_cf_clock_ns + 137 * NS_PER_MS));
	cf_budget_clock_fail = false;
	UT_ASSERT_EQ(g_seven_count, before);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT(cluster_cf_last_attempt_since(sequence, &current));
	UT_ASSERT(!cluster_cf_attempt_wait_slice(&current));
	UT_ASSERT(!cluster_cf_attempt_wait_slice(&previous));
}

static void
cf_budget_expire_during_cleanup(void)
{
	set_cf_clock_ns += 200 * NS_PER_MS;
	UT_ASSERT(acknowledge_retirements());
}

UT_TEST(startup_cf_expiry_after_old_cleanup_never_starts_another_request)
{
	uint64 deadline, cookie;
	ClusterCfAttemptObservation attempt;
	uint64 sequence;
	unsigned before;

	cold_fixture();
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(ShareLock));
	cookie = cluster_cf_owner_cookie(ShareLock);
	deadline = set_cf_clock_ns + 137 * NS_PER_MS;
	before = g_seven_count;
	sequence = cluster_cf_attempt_sequence();
	entry_on_release_wait = cf_budget_expire_during_cleanup;
	UT_ASSERT(!cluster_cf_lock_deadline_v1(ShareLock, deadline));
	entry_on_release_wait = NULL;
	UT_ASSERT_EQ(g_seven_count, before);
	UT_ASSERT_EQ(cold_release_waits, 1);
	UT_ASSERT(!cluster_cf_held(ShareLock));
	UT_ASSERT(cluster_cf_release_completed(ShareLock, cookie));
	UT_ASSERT(cluster_cf_last_attempt_since(sequence, &attempt));
	UT_ASSERT_EQ(attempt.owner_state_at_return, CLUSTER_LOCK_OWNER_EMPTY);
	UT_ASSERT_EQ(attempt.control_owner_id | attempt.request_id, 0);
	UT_ASSERT(cluster_control_request_empty());
	cold_consumer = false;
}

static uint64 cf_budget_metadata_step;
static void
cf_budget_metadata_elapsed(void)
{
	set_cf_clock_ns += cf_budget_metadata_step;
}

UT_TEST(startup_projection_recomputes_the_cf_budget_after_metadata)
{
	for (unsigned expires = 0; expires < 2; ++expires) {
		ClusterMergeEngage engage = CLUSTER_MERGE_NO_NO_PLAN;
		uint64 replay[2] = { 0 };
		XLogRecPtr start[CLUSTER_WAL_STATE_SLOT_COUNT + 1] = { 0 };
		uint64 deadline;
		unsigned before;

		pre_ir_fixture(false, false);
		retention_phase = 0;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		deadline = cold_deadline_plan.merge_deadline_ns = set_cf_clock_ns + 137 * NS_PER_MS;
		cf_budget_metadata_step = (expires ? 200 : 30) * NS_PER_MS;
		project_after_source = cf_budget_metadata_elapsed;
		before = g_seven_count;
		UT_ASSERT_EQ(pre_ir_call(&engage, replay, start), !expires);
		UT_ASSERT_EQ(cold_deadline_plan.merge_deadline_ns, deadline);
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT_EQ(g_seven_count - before, expires ? 1 : 2);
		if (expires) {
			UT_ASSERT_EQ(entry_code, ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
			UT_ASSERT_EQ(project_sources, 1);
		} else {
			UT_ASSERT_EQ(engage, CLUSTER_MERGE_ENGAGE);
			UT_ASSERT_EQ(g_last_timeout_ms, 107);
		}
		UT_ASSERT(!cluster_cf_held(ShareLock));
		project_after_source = NULL;
	}
}

UT_TEST(startup_bounded_source_preserves_exact_proof_and_native_refusal)
{
	for (unsigned mode = 0; mode < 3; ++mode) {
		ClusterWalSourceRef source, zero = { 0 };
		XLogRecPtr redo = 99;
		ClusterControlRootResult result;

		recovery_fixture(0);
		set_cf_clock_ns = NS_PER_S;
		recovery_acquire_result
			= mode == 2 ? CLUSTER_LOCK_ACQUIRE_OK_NATIVE : CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		if (mode == 2)
			fail_after_publication = false; /* S1 native never publishes an owner. */
		if (mode == 1)
			++recovery_token.root_publish_seq;
		memset(&source, 0xa5, sizeof(source));
		claim_teardown = owner_ack_on_wait = true;
		result = cluster_control_root_recovery_source_deadline_v1(
			&recovery_expected, &recovery_token, &source, &redo, set_cf_clock_ns + 137 * NS_PER_MS);
		claim_teardown = owner_ack_on_wait = false;
		UT_ASSERT_EQ(result, mode == 0	 ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
							 : mode == 1 ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
										 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		if (mode == 0) {
			UT_ASSERT_EQ(redo, 80);
			UT_ASSERT_EQ(source.claim.database_incarnation, 7);
		} else {
			UT_ASSERT_EQ(redo, InvalidXLogRecPtr);
			UT_ASSERT_EQ(memcmp(&source, &zero, sizeof(source)), 0);
		}
		UT_ASSERT_EQ(entry_waits, 0);
		UT_ASSERT(!cluster_cf_held(ShareLock));
	}
}

UT_TEST(startup_actual_bounded_source_never_waits_under_held_ir)
{
	ClusterWalSourceRef source, zero = { 0 };
	XLogRecPtr redo = 99;
	ClusterControlRootResult result;
	ClusterCfAttemptObservation attempt;
	uint64 before;

	ir_cf_fixture(CLUSTER_RECOVERY_SERIAL_COLD_FORMED, 0);
	recovery_fixture(0);
	retention_image.header.file_txn_seq = 7;
	retention_image.records[0].identity.authority_uuid[6] = 0x40;
	retention_image.records[0].identity.authority_uuid[8] = 0x80;
	retention_image.records[0].root_flags = COLD_REQUIRED_ROOT_FLAGS;
	recovery_expected = retention_image.records[0];
	make_read_token(&retention_image, 1, CONTROL_ROOT_SOURCE_PRIMARY, &recovery_token);
	ir_cf_requests[0].duty = recovery_expected.identity;
	ir_cf_requests[0].expected_root_token = recovery_token;
	entry_on_release_wait = ir_cf_release_ack;
	UT_ASSERT_EQ(cluster_recovery_serial_acquire(&ir_cf_requests[0], &ir_cf_guard),
				 CLUSTER_RECOVERY_SERIAL_GRANTED);
	UT_ASSERT_EQ(ir_cf_held_count(), 1);
	set_cf_clock_ns = NS_PER_S;
	before = cluster_cf_attempt_sequence();
	memset(&source, 0xa5, sizeof(source));
	result = cluster_control_root_recovery_source_deadline_v1(
		&recovery_expected, &recovery_token, &source, &redo, set_cf_clock_ns + 137 * NS_PER_MS);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(ir_cf_held_count(), 1);
	UT_ASSERT(ir_cf_guard.held);
	UT_ASSERT_EQ(entry_waits | recovery_reads, 0);
	UT_ASSERT_EQ(memcmp(&source, &zero, sizeof(source)), 0);
	UT_ASSERT_EQ(redo, InvalidXLogRecPtr);
	UT_ASSERT(cluster_cf_last_attempt_since(before, &attempt));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&attempt));
	UT_ASSERT_EQ(cluster_recovery_serial_release(&ir_cf_guard),
				 CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED);
	UT_ASSERT_EQ(ir_cf_held_count(), 0);
	entry_cleanup(ShareLock, attempt.control_owner_id);
	ir_cf_cleanup();
}
