/* Original queued producer acquires or reuses the original singleton.
 * Native transaction/WAL, pair I/O, lower GES and notifications are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */

UT_TEST(v2_precommit_acquires_or_reuses_original_authority)
{
	static const int granted[] = {
		CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK,
		CLUSTER_LOCK_ACQUIRE_OK_GRANTED, CLUSTER_LOCK_ACQUIRE_OK_CONVERTED
	};

	for (unsigned which = 0; which < 24; which++)
	{
		unsigned mask = which % 3 + 1, count = mask == 3 ? 2 : 1;
		bool publish = which % 12 >= 6, preheld = which >= 12;
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		bool refused;

		config.acquire_result = granted[which % 3];
		directory = native_lock_fixture((which / 3) % 2 != 0, mask, publish, &config);
		if (preheld)
			UT_ASSERT(cluster_relmap_authority_x_lock());
		UT_ASSERT_EQ(assignments + inserts + flushes + v2_stage_writes, 0);
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (refused)
			continue;
		UT_ASSERT(cluster_relmap_authority_x_held());
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(inserts, count);
		UT_ASSERT_EQ(flushes, count);
		UT_ASSERT_EQ(xid_under_lock + xid_in_crit, 0);
		UT_ASSERT_EQ(active_shared_updates.num_mappings + active_local_updates.num_mappings, 0);
		for (unsigned slot = 0; slot < 2; slot++)
		{
			UT_ASSERT_EQ(cluster_relmap_v2_arms[slot].armed, (mask & (1U << slot)) != 0);
			if (cluster_relmap_v2_arms[slot].armed)
				UT_ASSERT_EQ(cluster_relmap_v2_arms[slot].sample.owner.top_full_xid,
					U64FromFullTransactionId(assigned_xid));
		}
		before = *directory;
		v2_terminal_context = publish;
		if (publish)
			cluster_relmap_producer_postcommit_v2();
		else
			cluster_relmap_producer_abort_v2();
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.promotions, 1);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release);
		UT_ASSERT(!test_relmap_native_lock_held());
		UT_ASSERT_EQ(terminal_calls, count);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
	}
	native_lock_test_reset();
}

UT_TEST(v2_precommit_entry_refuses_before_original_acquisition)
{
	for (unsigned which = 0; which < 20; which++)
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory = native_lock_fixture(false, 3, false, &config);
		ClusterRelmapRetentionDirectory before = *directory;
		RelMapFile saved[4];

		switch (which)
		{
			case 0: local_held = true; break;
			case 1: v2_xidgen_held = true; break;
			case 2: CritSectionCount = 1; break;
			case 3: ShutdownRequestPending = true; break;
			case 4: MyBackendType = B_CHECKPOINTER; break;
			case 5: MyProcPid = 0; break;
			case 6: v2_terminal_context = true; break;
			case 7: cluster_enabled = false; break;
			case 8: cluster_shared_config = false; break;
			case 9: cluster_node_id = -1; break;
			case 10: cluster_node_id = CLUSTER_MAX_NODES; break;
			case 11: pending_shared_updates = updates; break;
			case 12: pending_local_updates = updates; break;
			case 13: active_shared_updates.num_mappings = -1; break;
			case 14: active_local_updates.num_mappings = MAX_MAPPINGS + 1; break;
			case 15: break; /* native parallel-worker parameter */
			case 16: cluster_relmap_publish_arms[0].armed = true; break;
			case 17: cluster_relmap_v2_finish_entered = true; break;
			case 18: cluster_relmap_v2_prepare_unfinished = true; break;
			case 19: MyDatabaseTableSpace = InvalidOid; break;
		}
		precommit_save_queues(saved);
		UT_ASSERT(precommit_error(which == 15));
		precommit_same_queues(saved);
		UT_ASSERT_EQ(assignments + full_requests + inserts + flushes + v2_stage_writes, 0);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires + stats.promotions + stats.releases, 0);
		UT_ASSERT(!test_relmap_native_lock_held());
		/* Restore only this isolated fixture's deliberately changed node. */
		cluster_node_id = 3;
	}
	/* Even a rejected execution context with no queued work stays a no-op. */
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;

		(void) native_lock_fixture(false, 0, false, &config);
		cluster_enabled = false;
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT(!precommit_error(true));
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires + stats.promotions + stats.releases, 0);
	}
	native_lock_test_reset();
}

UT_TEST(v2_precommit_pre_effect_error_keeps_original_abort_release)
{
	for (unsigned which = 0; which < 4; which++)
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionDirectory before;
		RelMapFile saved[4];

		config.before_release = NULL;
		directory = native_lock_fixture(which >= 2, 3, false, &config);
		if (which & 1)
			directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		else
			writer_available = false;
		before = *directory;
		precommit_save_queues(saved);
		UT_ASSERT(precommit_error(false));
		precommit_same_queues(saved);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(!cluster_relmap_v2_prepare_unfinished && !cluster_relmap_v2_finish_entered);
		UT_ASSERT_EQ(inserts + flushes + v2_stage_writes + preserves + sync_commits, 0);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		UT_ASSERT(test_relmap_native_lock_held());
		/* The empty v2 callback cannot consume this separate original duty. */
		cluster_relmap_producer_abort_v2();
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 0);
		if (test_relmap_native_lock_held())
			test_relmap_native_abort_release();
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.promotions, 1);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release);
		UT_ASSERT(!test_relmap_native_lock_held());
		UT_ASSERT_EQ(terminal_calls + publish_events, 0);
	}
	native_lock_test_reset();
}

UT_TEST(v2_precommit_entered_error_keeps_acquired_authority)
{
	static const enum UnarmedPrepareFault faults[] = {
		UNARMED_SECOND_READ_FALSE, UNARMED_WRITE_ERROR
	};

	for (unsigned which = 0; which < 4; which++)
	{
		TestRelmapNativeLockConfig config = native_lock_configuration();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory *directory = native_lock_fixture(which >= 2, 3, false, &config);
		ClusterRelmapRetentionDirectory before;
		RelMapFile saved[4];
		bool entered;

		precommit_failed_slot = 1;
		precommit_fault = faults[which % 2];
		v2_pair_observer = precommit_pair;
		v2_prepared_pair_observer = unarmed_check;
		v2_preserve_observer = unarmed_preserve;
		precommit_save_queues(saved);
		UT_ASSERT(precommit_error(false));
		precommit_same_queues(saved);
		entered = cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_prepare_unfinished;
		UT_ASSERT(entered);
		if (!entered)
			continue;
		before = *directory;
		UT_ASSERT(abort_panic());
		UT_ASSERT(postcommit_panic());
		UT_ASSERT_EQ(terminal_calls + local_invalidations[0] + local_invalidations[1]
			+ ack_attempts[0] + ack_attempts[1], 0);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1);
		UT_ASSERT_EQ(stats.promotions, 1);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT(stats.exact_request && test_relmap_native_lock_held());
	}
	native_lock_test_reset();
}
