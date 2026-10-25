/*-------------------------------------------------------------------------
 * test_cluster_relmap_native_lock.inc
 *    Original producer custody through the original singleton lock module.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * NOTES
 *    Lower GES, physical I/O, native transaction outcome and notification
 *    transport are controlled. No native death/recovery qualification.
 *-------------------------------------------------------------------------
 */
static bool native_lock_publish;

static void
native_lock_before_release(void)
{
	UT_ASSERT(test_relmap_native_lock_held());
	postcommit_custody_before_notification();
	UT_ASSERT_EQ(active_shared_updates.num_mappings + active_local_updates.num_mappings, 0);
	for (unsigned slot = 0; slot < 2; slot++)
	{
		bool requested = (postcommit_mask & (1U << slot)) != 0;

		UT_ASSERT_EQ(local_invalidations[slot], native_lock_publish && requested ? 1 : 0);
		UT_ASSERT_EQ(ack_attempts[slot], native_lock_publish && requested ? 1 : 0);
	}
	unlock_calls++;
	record_publish_event(UNLOCK);
}

static TestRelmapNativeLockConfig
native_lock_configuration(void)
{
	TestRelmapNativeLockConfig config = {
		.acquire_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK,
		.promote_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED,
		.release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED,
		.before_release = native_lock_before_release
	};

	return config;
}

static ClusterRelmapRetentionDirectory *
native_lock_fixture(bool next_era, unsigned mask, bool publish,
	const TestRelmapNativeLockConfig *config)
{
	ClusterRelmapRetentionDirectory *directory;

	/* Old providers remain the default, including while resetting fixtures. */
	v2_native_lock_active = false;
	directory = precommit_fixture(next_era, mask, publish);
	native_lock_publish = publish;
	test_relmap_native_lock_reset(config);
	authority_held = false;
	v2_native_lock_active = true;
	return directory;
}

static void
native_lock_test_reset(void)
{
	/* An isolated next test process, never a production recovery operation. */
	v2_native_lock_active = false;
	precommit_reset_queues();
	abort_test_reset();
}

static void
native_lock_keeps_receipts(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionDirectory *before, const ClusterRelmapProducerArmV2 saved[2])
{
	UT_ASSERT(memcmp(saved, cluster_relmap_v2_arms, sizeof(cluster_relmap_v2_arms)) == 0);
	UT_ASSERT(memcmp(before->pins, directory->pins, sizeof(before->pins)) == 0);
	UT_ASSERT(cluster_relmap_v2_finish_entered);
	UT_ASSERT(cluster_relmap_authority_x_held() && test_relmap_native_lock_held());
}

UT_TEST(v2_native_lock_batch_finishes_before_original_release)
{
	static const int granted[] = {
		CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK,
		CLUSTER_LOCK_ACQUIRE_OK_GRANTED, CLUSTER_LOCK_ACQUIRE_OK_CONVERTED
	};

	for (unsigned which = 0; which < 12; which++)
	{
		unsigned mask = which % 3 + 1, count = mask == 3 ? 2 : 1;
		bool publish = which >= 6;
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;

		config.acquire_result = granted[which % 3];
		directory = native_lock_fixture((which / 3) % 2 != 0, mask, publish, &config);
		UT_ASSERT(!cluster_relmap_authority_x_held());
		UT_ASSERT_EQ(assignments + inserts + stages, 0);
		if (!cluster_relmap_authority_x_lock())
		{
			UT_ASSERT(false);
			continue;
		}
		UT_ASSERT(cluster_relmap_authority_x_held());
		cluster_relmap_producer_precommit_v2(false);
		before = *directory;
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(inserts, count);
		UT_ASSERT_EQ(flushes, count);
		UT_ASSERT_EQ(xid_under_lock + xid_in_crit, 0);
		v2_terminal_context = publish;
		if (publish)
			cluster_relmap_producer_postcommit_v2();
		else
			cluster_relmap_producer_abort_v2();
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.promotions, 1);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release && stats.request_id != 0);
		UT_ASSERT_EQ(terminal_calls, count);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(!cluster_relmap_authority_x_held() && !test_relmap_native_lock_held());
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(!cluster_relmap_v2_finish_entered);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
	}
	native_lock_test_reset();
}

UT_TEST(v2_native_lock_release_refusal_retains_original_custody)
{
	for (unsigned which = 0; which < 8; which++)
	{
		bool publish = (which & 1) != 0;
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 saved[2];
		unsigned calls, sends, acks;

		config.release_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		config.release_error = which >= 4;
		directory = native_lock_fixture((which / 2) % 2 != 0, 3, publish, &config);
		UT_ASSERT(cluster_relmap_authority_x_lock());
		cluster_relmap_producer_precommit_v2(false);
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		v2_terminal_context = publish;
		UT_ASSERT(publish ? postcommit_panic() : abort_panic());
		native_lock_keeps_receipts(directory, &before, saved);
		UT_ASSERT(terminal_disk_cleared[0] && terminal_disk_cleared[1]);
		UT_ASSERT_EQ(postcommit_reports, config.release_error ? 1 : 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release);
		calls = terminal_calls;
		sends = local_invalidations[0] + local_invalidations[1];
		acks = ack_attempts[0] + ack_attempts[1];
		/* Neither terminal polarity may enter storage, notify or release again. */
		UT_ASSERT(postcommit_panic());
		UT_ASSERT(abort_panic());
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT_EQ(terminal_calls, calls);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], sends);
		UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], acks);
		native_lock_keeps_receipts(directory, &before, saved);
	}
	native_lock_test_reset();
}

UT_TEST(v2_native_lock_refusals_do_not_enter_release)
{
	for (unsigned which = 0; which < 4; which++)
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		RelMapFile queues[4];

		if (which & 1)
			config.promote_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		else
			config.acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		directory = native_lock_fixture(which >= 2, 3, false, &config);
		before = *directory;
		precommit_save_queues(queues);
		UT_ASSERT(precommit_error(false));
		precommit_same_queues(queues);
		UT_ASSERT_EQ(assignments + inserts + flushes + v2_stage_writes, 0);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.promotions, which & 1 ? 1 : 0);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT(!test_relmap_native_lock_held());
	}
	for (unsigned which = 0; which < 14; which++)
	{
		bool publish = which >= 12 || (which & 1) != 0;
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 saved[2];

		directory = native_lock_fixture((which / 2) % 2 != 0, 3, publish, &config);
		UT_ASSERT(cluster_relmap_authority_x_lock());
		cluster_relmap_producer_precommit_v2(false);
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		v2_terminal_context = publish;
		if (which < 8)
		{
			terminal_fail_at = 2;
			terminal_fault = which < 4 ? 1 : 2;
		}
		else if (which < 12)
			change_original_owner();
		else
		{
			postcommit_failed_slot = which % 2;
			postcommit_fault = POSTCOMMIT_ACK_EXHAUSTED;
		}
		UT_ASSERT(publish ? postcommit_panic() : abort_panic());
		native_lock_keeps_receipts(directory, &before, saved);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.promotions, 1);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT_EQ(unlock_calls, 0);
	}
	native_lock_test_reset();
}
