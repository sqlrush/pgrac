/*-------------------------------------------------------------------------
 * test_cluster_relmap_abort.inc
 *    Original dormant abort receipts remain protected through global unlock.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_abort.inc
 *
 * NOTES
 *    Native abort outcome, physical I/O and error contexts are controlled.
 *    The original abort callback can precede TRANS_ABORT. Its explicit test
 *    flag extends only the discard leaf's context assertion, not provenance.
 *    Simulated PANIC is not native process death or crash recovery evidence.
 *-------------------------------------------------------------------------
 */
static unsigned abort_mask;
static bool abort_unlock_error;

static void
abort_unlock(void)
{
	UT_ASSERT(authority_held && !local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(cluster_relmap_v2_finish_entered);
	UT_ASSERT_EQ(terminal_calls, abort_mask == 3 ? 2 : 1);
	for (unsigned slot = 0; slot < 2; slot++)
	{
		UT_ASSERT_EQ(cluster_relmap_v2_arms[slot].armed, (abort_mask & (1U << slot)) != 0);
		if (abort_mask & (1U << slot))
			UT_ASSERT(terminal_disk_cleared[slot]);
	}
	UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 0);
	UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], 0);
	unlock_calls++;
	record_publish_event(UNLOCK);
	if (abort_unlock_error)
		ereport(ERROR, (errmsg("controlled original abort unlock error")));
	authority_held = false;
}

static void
abort_test_reset(void)
{
	postcommit_test_reset();
	v2_abort_callback_context = false;
}

static ClusterRelmapRetentionDirectory *
abort_fixture(bool next_era)
{
	ClusterRelmapRetentionDirectory *directory;

	abort_test_reset();
	directory = terminal_fixture(next_era);
	terminal_publish = false;
	abort_mask = 3;
	abort_unlock_error = false;
	postcommit_reports = 0;
	v2_abort_callback_context = true;
	v2_postcommit_error_stack = true;
	v2_unlock_observer = abort_unlock;
	return directory;
}

static bool
abort_panic(void)
{
	bool caught = false;

	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		cluster_relmap_producer_abort_v2();
	else
	{
		caught = true;
		UT_ASSERT_EQ(error_level, PANIC);
	}
	error_expected = false;
	/* A real PANIC exits; only the fixture tears down exception frames. */
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	return caught;
}

static void
abort_keeps_pins(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionPin *before)
{
	UT_ASSERT(memcmp(before, directory->pins, 3 * sizeof(*before)) == 0);
	UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 0);
	UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], 0);
}

static void
abort_no_retry(unsigned calls, unsigned unlocks)
{
	UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
	UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
	UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
	UT_ASSERT(abort_panic());
	UT_ASSERT(postcommit_panic());
	UT_ASSERT_EQ(terminal_calls, calls);
	UT_ASSERT_EQ(unlock_calls, unlocks);
}

UT_TEST(v2_abort_discards_all_receipts_before_unlock)
{
	for (unsigned which = 0; which < 12; which++)
	{
		ClusterRelmapRetentionDirectory *directory = abort_fixture((which / 3) % 2 != 0);
		ClusterRelmapRetentionPin before[3];
		RelMapFile shared_before, local_before;
		bool armed;

		abort_mask = which % 3 + 1;
		if (abort_mask == 3)
			armed = terminal_arm_both((which & 1) != 0);
		else
			armed = cluster_relmap_producer_arm_v2(abort_mask == 1, &updates);
		UT_ASSERT(armed);
		if (!armed)
			continue;
		memcpy(before, directory->pins, sizeof(before));
		shared_before = shared_map;
		local_before = local_map;
		/* Original early abort is still active; later abort is not. Neither
		 * context is used as a terminal classification or commit proof. */
		v2_terminal_context = which >= 6;
		ClusterConfShmem = which & 1 ? &peer_conf : NULL;
		cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(terminal_calls, abort_mask == 3 ? 2 : 1);
		UT_ASSERT_EQ(terminal_trace[0], abort_mask == 2 ? 1 : 0);
		if (abort_mask == 3)
			UT_ASSERT_EQ(terminal_trace[1], 1);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT_EQ(publish_events, 1);
		UT_ASSERT_EQ(publish_trace[0], UNLOCK);
		UT_ASSERT(!authority_held && !cluster_relmap_v2_finish_entered);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		abort_keeps_pins(directory, before);
		UT_ASSERT(memcmp(&shared_before, &shared_map, sizeof(shared_before)) == 0);
		UT_ASSERT(memcmp(&local_before, &local_map, sizeof(local_before)) == 0);
		UT_ASSERT_EQ(full_requests, abort_mask == 3 ? 2 : 1);
		UT_ASSERT_EQ(postcommit_reports, 0);
		cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(unlock_calls, 1);
	}
	abort_test_reset();
}

UT_TEST(v2_abort_refusals_keep_original_custody)
{
	for (unsigned which = 0; which < 15; which++)
	{
		ClusterRelmapRetentionDirectory *directory = abort_fixture(false);
		ClusterRelmapRetentionPin before[3];
		bool panicked;

		if (!terminal_arm_both((which & 1) != 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		v2_terminal_context = (which & 1) != 0;
		switch (which)
		{
			case 0: local_held = true; break;
			case 1: v2_xidgen_held = true; break;
			case 2: v2_retention_held = true; break;
			case 3: authority_held = false; break;
			case 4: change_original_owner(); break;
			case 5: MyDatabaseId++; break;
			case 6: cluster_relmap_publish_arms[0].armed = true; break;
			case 7: cluster_relmap_publish_arms[1].armed = true; break;
			case 8: cluster_relmap_v2_finish_entered = true; break;
			case 9: MyProcPid++; break;
			case 10: boot_incarnation++; break;
			case 11: writer_epoch++; break;
			case 12: ShutdownRequestPending = true; break;
			case 13: MyBackendType = B_CHECKPOINTER; break;
			case 14: assigned_xid = InvalidFullTransactionId; break;
		}
		panicked = abort_panic();
		UT_ASSERT(panicked);
		if (!panicked)
			continue;
		UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
		UT_ASSERT(cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		abort_keeps_pins(directory, before);
		abort_no_retry(0, 0);
	}
	abort_test_reset();
}

UT_TEST(v2_abort_partial_cleanup_is_one_shot)
{
	for (unsigned which = 0; which < 16; which++)
	{
		ClusterRelmapRetentionDirectory *directory = abort_fixture(false);
		ClusterRelmapRetentionPin before[3];
		bool panicked;

		if (!terminal_arm_both((which & 1) != 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		v2_terminal_context = which >= 8;
		terminal_fault = (which / 2) % 4 + 1;
		terminal_fail_at = which % 2 + 1;
		panicked = abort_panic();
		UT_ASSERT(panicked);
		if (!panicked)
			continue;
		UT_ASSERT_EQ(terminal_calls, terminal_fail_at);
		UT_ASSERT_EQ(postcommit_reports, terminal_fault == 2 ? 1 : 0);
		UT_ASSERT_EQ(unlock_calls + publish_events, 0);
		UT_ASSERT(cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(authority_held && !local_held && !v2_retention_held);
		abort_keeps_pins(directory, before);
		abort_no_retry(terminal_fail_at, 0);
	}
	abort_test_reset();
}

UT_TEST(v2_abort_unlock_error_and_empty_debt_isolation)
{
	for (unsigned which = 0; which < 4; which++)
	{
		ClusterRelmapRetentionDirectory *directory = abort_fixture((which & 1) != 0);
		ClusterRelmapRetentionPin before[3];
		bool panicked;

		if (!terminal_arm_both((which & 1) != 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		v2_terminal_context = which >= 2;
		abort_unlock_error = true;
		panicked = abort_panic();
		UT_ASSERT(panicked);
		if (!panicked)
			continue;
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT(terminal_disk_cleared[0] && terminal_disk_cleared[1]);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT_EQ(postcommit_reports, 1);
		UT_ASSERT(authority_held && cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		abort_keeps_pins(directory, before);
		abort_no_retry(2, 1);
	}
	for (unsigned partial_prepare = 0; partial_prepare < 2; partial_prepare++)
	{
		ClusterRelmapRetentionDirectory *directory = abort_fixture(false);
		ClusterRelmapRetentionPin before[3];

		if (partial_prepare)
		{
			terminal_prepare_fail = true;
			UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
			UT_ASSERT(v2_stage_writes != 0 && directory->pins[1].occupied);
		}
		else
		{
			cluster_relmap_publish_arms[0].armed = true;
			authority_pending[0] = 19;
		}
		memcpy(before, directory->pins, sizeof(before));
		cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(terminal_calls + publish_events + unlock_calls, 0);
		UT_ASSERT(authority_held && !cluster_relmap_v2_finish_entered);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		if (!partial_prepare)
			UT_ASSERT(cluster_relmap_publish_arms[0].armed && authority_pending[0] == 19);
		abort_keeps_pins(directory, before);
	}
	abort_test_reset();
}
