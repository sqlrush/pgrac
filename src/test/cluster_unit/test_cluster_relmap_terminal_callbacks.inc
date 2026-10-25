/* Original native relmap callbacks consume their saved producer custody.
 * Native transaction/WAL, lower GES and ERROR/PANIC handling are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */

static void
original_terminal_callback(bool publish)
{
	if (publish)
		AtEOXact_ClusterRelmapPublish();
	else
	{
		AtAbort_ClusterRelmapPending();
		/* Same original singleton cleanup immediately follows in xact.c. */
		test_relmap_native_abort_release();
	}
}

static bool
original_callback_panic(bool publish)
{
	bool caught = false;

	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		original_terminal_callback(publish);
	else
	{
		caught = true;
		UT_ASSERT_EQ(error_level, PANIC);
	}
	error_expected = false;
	/* Only the fixture models a surviving process after PANIC. */
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	return caught;
}

UT_TEST(v2_original_callbacks_finish_physical_pairs_before_release)
{
	for (unsigned which = 0; which < 12; which++)
	{
		unsigned mask = which % 3 + 1, count = mask == 3 ? 2 : 1;
		bool publish = which >= 6, cleared;
		ClusterRelmapRetentionDirectory *directory = physical_fixture((which / 3) % 2 != 0,
			mask, publish);
		ClusterRelmapRetentionDirectory before;
		TestRelmapPhysicalStats stats;
		TestRelmapNativeLockStats lock_stats;

		cluster_relmap_producer_precommit_v2(false);
		before = *directory;
		physical_finishing = true;
		v2_terminal_context = publish;
		original_terminal_callback(publish);
		cleared = !cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed;
		UT_ASSERT(cleared);
		if (!cleared)
		{
			physical_cleanup();
			continue;
		}
		physical_clean_pairs();
		stats = test_relmap_physical_stats();
		UT_ASSERT_EQ(stats.writes, 4 * count);
		UT_ASSERT_EQ(stats.file_syncs, 4 * count);
		UT_ASSERT_EQ(stats.directory_syncs, 4 * count);
		UT_ASSERT_EQ(stats.renames, 4 * count);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(terminal_calls, count);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], publish ? count : 0);
		UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], publish ? count : 0);
		UT_ASSERT_EQ(publish_calls[0] + publish_calls[1] + discard_calls[0] + discard_calls[1], 0);
		test_relmap_native_lock_stats(&lock_stats);
		UT_ASSERT_EQ(lock_stats.releases, 1);
		UT_ASSERT(lock_stats.exact_request && lock_stats.held_at_release);
		UT_ASSERT(!cluster_relmap_authority_x_held() && !cluster_relmap_v2_finish_entered);
		/* Both original callbacks are empty after the successful owner retires. */
		original_terminal_callback(publish);
		test_relmap_native_lock_stats(&lock_stats);
		UT_ASSERT_EQ(lock_stats.releases, 1);
		UT_ASSERT_EQ(terminal_calls, count);
		physical_cleanup();
	}
}

UT_TEST(v2_original_abort_callback_keeps_pre_effect_lock_cleanup)
{
	for (unsigned which = 0; which < 6; which++)
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		RelMapFile queues[4];

		config.before_release = NULL;
		directory = native_lock_fixture(which >= 3, 3, false, &config);
		if (which % 3 == 0)
			writer_available = false;
		else if (which % 3 == 1)
			directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		else
			fail_assignment = true;
		before = *directory;
		precommit_save_queues(queues);
		UT_ASSERT(precommit_error(false));
		UT_ASSERT(!cluster_relmap_v2_prepare_unfinished);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(test_relmap_native_lock_held());
		original_terminal_callback(false);
		precommit_same_queues(queues);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(inserts + flushes + v2_stage_writes + terminal_calls, 0);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1] + ack_attempts[0] + ack_attempts[1], 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release);
		UT_ASSERT(!test_relmap_native_lock_held());
	}
	native_lock_test_reset();
}

UT_TEST(v2_original_callback_failure_blocks_legacy_abort_release)
{
	for (unsigned which = 0; which < 16; which++)
	{
		unsigned kind = which / 4;
		bool publish = (which & 1) != 0;
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 saved[2];
		RelMapFile queues[4];
		unsigned calls;

		config.before_release = NULL;
		directory = native_lock_fixture((which & 2) != 0, 3, publish, &config);
		if (kind == 3)
		{
			precommit_failed_slot = 1;
			precommit_fault = UNARMED_SECOND_READ_FALSE;
			v2_pair_observer = precommit_pair;
			v2_prepared_pair_observer = unarmed_check;
			v2_preserve_observer = unarmed_preserve;
			UT_ASSERT(precommit_error(false));
			UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_prepare_unfinished);
		}
		else
			cluster_relmap_producer_precommit_v2(false);
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		precommit_save_queues(queues);
		v2_terminal_context = publish;
		if (kind < 2)
		{
			terminal_fail_at = 2;
			terminal_fault = kind == 0 ? 1 : 2;
		}
		else if (kind == 2)
			change_original_owner();
		UT_ASSERT(original_callback_panic(publish));
		UT_ASSERT(test_relmap_native_lock_held());
		UT_ASSERT(memcmp(saved, cluster_relmap_v2_arms, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		precommit_same_queues(queues);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1] + ack_attempts[0] + ack_attempts[1], 0);
		UT_ASSERT_EQ(publish_calls[0] + publish_calls[1] + discard_calls[0] + discard_calls[1], 0);
		calls = terminal_calls;
		UT_ASSERT(original_callback_panic(false));
		UT_ASSERT(original_callback_panic(true));
		UT_ASSERT_EQ(terminal_calls, calls);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT(stats.exact_request && test_relmap_native_lock_held());
	}
	native_lock_test_reset();
}

UT_TEST(v2_original_abort_callback_rejects_commit_transition)
{
	for (unsigned which = 0; which < 4; which++)
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 saved[2];

		config.before_release = NULL;
		directory = native_lock_fixture(which >= 2, 3, false, &config);
		if (which & 1)
		{
			precommit_failed_slot = 1;
			precommit_fault = UNARMED_SECOND_READ_FALSE;
			v2_pair_observer = precommit_pair;
			v2_prepared_pair_observer = unarmed_check;
			v2_preserve_observer = unarmed_preserve;
			UT_ASSERT(precommit_error(false));
		}
		else
			cluster_relmap_producer_precommit_v2(false);
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		/* Native abort can be entered after an ERROR in TRANS_COMMIT.
		 * Its later RecordTransactionAbort guard is too late for relmap IO. */
		v2_terminal_context = true;
		UT_ASSERT(original_callback_panic(false));
		UT_ASSERT_EQ(terminal_calls, 0);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1] + ack_attempts[0] + ack_attempts[1], 0);
		UT_ASSERT(memcmp(saved, cluster_relmap_v2_arms, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT(test_relmap_native_lock_held());
	}
	native_lock_test_reset();
}
