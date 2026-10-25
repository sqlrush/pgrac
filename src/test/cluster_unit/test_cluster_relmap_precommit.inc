/*-------------------------------------------------------------------------
 * test_cluster_relmap_precommit.inc
 *    Original command-activated queues enter one dormant prepare batch.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_precommit.inc
 *
 * NOTES
 *    Original CCI/producer/owner/runtime/retention execute. Queue inputs,
 *    physical leaves and transaction/ERROR/PANIC context are controlled;
 *    no native commit/abort, live activation or recovery qualification.
 *-------------------------------------------------------------------------
 */
static unsigned precommit_failed_slot;
static enum UnarmedPrepareFault precommit_fault;

static void
precommit_reset_queues(void)
{
	memset(&active_shared_updates, 0, sizeof(active_shared_updates));
	memset(&active_local_updates, 0, sizeof(active_local_updates));
	memset(&pending_shared_updates, 0, sizeof(pending_shared_updates));
	memset(&pending_local_updates, 0, sizeof(pending_local_updates));
}

static void
precommit_save_queues(RelMapFile saved[4])
{
	saved[0] = active_shared_updates;
	saved[1] = active_local_updates;
	saved[2] = pending_shared_updates;
	saved[3] = pending_local_updates;
}

static void
precommit_same_queues(const RelMapFile saved[4])
{
	RelMapFile actual[4];

	precommit_save_queues(actual);
	UT_ASSERT(memcmp(saved, actual, sizeof(actual)) == 0);
}

static ClusterRelmapRetentionDirectory *
precommit_fixture(bool next_era, unsigned mask, bool publish)
{
	ClusterRelmapRetentionDirectory *directory;

	precommit_reset_queues();
	directory = publish ? postcommit_fixture(next_era, true) : abort_fixture(next_era);
	abort_mask = postcommit_mask = mask;
	if (mask & 1)
		pending_shared_updates = updates;
	if (mask & 2)
	{
		pending_local_updates = updates;
		pending_local_updates.mappings[0].mapfilenumber = 503;
	}
	/* Consume actual original command-counter activation, not a new queue. */
	AtCCI_RelationMap();
	UT_ASSERT_EQ(pending_shared_updates.num_mappings + pending_local_updates.num_mappings, 0);
	return directory;
}

static bool
precommit_error(bool parallel_worker)
{
	volatile bool caught = false;

	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		cluster_relmap_producer_precommit_v2(parallel_worker);
	else
	{
		caught = true;
		UT_ASSERT_EQ(error_level, ERROR);
	}
	error_expected = false;
	if (local_held)
		LWLockRelease(RelationMappingLock);
	return caught;
}

static ClusterRelmapV2PairResult
precommit_pair(const ClusterRelmapV2Namespace *identity, ClusterRelmapV2View *out)
{
	unarmed_set_fault((identity->shared ? 0 : 1) == precommit_failed_slot
		? precommit_fault : UNARMED_OK);
	return unarmed_pair(identity, out);
}

UT_TEST(v2_precommit_consumes_original_cci_batch_before_terminal)
{
	for (unsigned which = 0; which < 12; which++)
	{
		unsigned mask = which % 3 + 1, count = mask == 3 ? 2 : 1;
		bool publish = which >= 6;
		ClusterRelmapRetentionDirectory *directory = precommit_fixture((which / 3) % 2 != 0,
			mask, publish);
		ClusterRelmapRetentionDirectory before;
		RelMapFile saved[4];
		bool armed;

		precommit_save_queues(saved);
		cluster_relmap_producer_precommit_v2(false);
		armed = cluster_relmap_v2_arms[0].armed == ((mask & 1) != 0)
			&& cluster_relmap_v2_arms[1].armed == ((mask & 2) != 0);
		UT_ASSERT(armed);
		if (!armed)
			continue;
		saved[0].num_mappings = saved[1].num_mappings = 0;
		precommit_same_queues(saved);
		UT_ASSERT_EQ(inserts, count);
		UT_ASSERT_EQ(flushes, count);
		UT_ASSERT_EQ(sync_commits, count);
		UT_ASSERT_EQ(preserves, count * 2);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(xid_under_lock + xid_in_crit, 0);
		UT_ASSERT_EQ(full_requests, count);
		UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
		if (mask & 1)
			UT_ASSERT_EQ(shared_map.mappings[0].mapfilenumber, 501);
		if (mask & 2)
			UT_ASSERT_EQ(local_map.mappings[0].mapfilenumber, 503);
		before = *directory;
		if (publish)
		{
			v2_terminal_context = true;
			cluster_relmap_producer_postcommit_v2();
			UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], count);
			UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], count);
		}
		else
			cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(terminal_calls, count);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(!authority_held);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
	}
	precommit_reset_queues();
	abort_test_reset();
}

UT_TEST(v2_precommit_preflight_refusals_keep_original_queues)
{
	for (unsigned which = 0; which < 20; which++)
	{
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(false, 3, false);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 arms[2];
		RelMapFile saved[4];
		unsigned writes, flush_before, preserve_before;
		bool caught;

		switch (which)
		{
			case 0: pending_shared_updates = updates; break;
			case 1: pending_local_updates = updates; break;
			case 2: active_shared_updates.num_mappings = -1; break;
			case 3: active_shared_updates.num_mappings = MAX_MAPPINGS + 1; break;
			case 4: active_local_updates.num_mappings = -1; break;
			case 5: active_local_updates.num_mappings = MAX_MAPPINGS + 1; break;
			case 6: break; /* native parallel-worker parameter */
			case 7: cluster_relmap_publish_arms[0].armed = true; break;
			case 8: cluster_relmap_publish_arms[1].armed = true; break;
			case 9: UT_ASSERT(cluster_relmap_producer_arm_v2(true, &updates)); break;
			case 10: cluster_relmap_v2_finish_entered = true; break;
			case 11:
				terminal_prepare_fail = true;
				UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
				terminal_prepare_fail = false;
				break;
			case 12: authority_held = false; lock_available = false; break;
			case 13: local_held = true; break;
			case 14: v2_xidgen_held = true; break;
			case 15: ShutdownRequestPending = true; break;
			case 16: MyBackendType = B_CHECKPOINTER; break;
			case 17: v2_terminal_context = true; break;
			case 18: writer_available = false; break;
			case 19: MyDatabaseTableSpace = InvalidOid; break;
		}
		before = *directory;
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		precommit_save_queues(saved);
		writes = inserts;
		flush_before = flushes;
		preserve_before = preserves;
		caught = precommit_error(which == 6);
		UT_ASSERT(caught);
		precommit_same_queues(saved);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		UT_ASSERT(memcmp(arms, cluster_relmap_v2_arms, sizeof(arms)) == 0);
		UT_ASSERT_EQ(inserts, writes);
		UT_ASSERT_EQ(flushes, flush_before);
		UT_ASSERT_EQ(preserves, preserve_before);
		UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
	}
	precommit_reset_queues();
	abort_test_reset();
}

UT_TEST(v2_precommit_partial_batch_error_retains_all_input_and_debt)
{
	static const enum UnarmedPrepareFault faults[] = {
		UNARMED_SECOND_READ_FALSE, UNARMED_WRITE_FALSE, UNARMED_LAST_CHECK_FALSE,
		UNARMED_WRITE_ERROR, UNARMED_PRESERVE_ERROR
	};

	for (unsigned which = 0; which < 20; which++)
	{
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(which >= 10, 3, false);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 arms[2];
		RelMapFile saved[4];
		unsigned requests, writes;
		bool caught;

		precommit_failed_slot = which % 2;
		precommit_fault = faults[(which / 2) % 5];
		v2_pair_observer = precommit_pair;
		v2_prepared_pair_observer = unarmed_check;
		v2_preserve_observer = unarmed_preserve;
		precommit_save_queues(saved);
		caught = precommit_error(false);
		UT_ASSERT(caught);
		if (!caught)
			continue;
		precommit_same_queues(saved);
		UT_ASSERT_EQ(cluster_relmap_v2_arms[0].armed, precommit_failed_slot == 1);
		UT_ASSERT(!cluster_relmap_v2_arms[1].armed);
		before = *directory;
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		requests = full_requests;
		writes = inserts;
		UT_ASSERT(abort_panic());
		UT_ASSERT(precommit_error(false));
		UT_ASSERT_EQ(full_requests, requests);
		UT_ASSERT_EQ(inserts, writes);
		UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
		UT_ASSERT(authority_held);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		UT_ASSERT(memcmp(arms, cluster_relmap_v2_arms, sizeof(arms)) == 0);
		precommit_same_queues(saved);
	}
	precommit_reset_queues();
	abort_test_reset();
}

UT_TEST(v2_precommit_empty_reentry_and_capacity_keep_original_custody)
{
	for (unsigned which = 0; which < 4; which++)
	{
		(void) precommit_fixture(false, 0, false);
		cluster_relmap_publish_arms[which % 2].armed = true;
		authority_held = (which & 1) != 0;
		v2_terminal_context = true;
		cluster_relmap_producer_precommit_v2(which >= 2);
		UT_ASSERT_EQ(full_requests + inserts + terminal_calls + unlock_calls + publish_events, 0);
		UT_ASSERT_EQ(authority_held, (which & 1) != 0);
		UT_ASSERT(cluster_relmap_publish_arms[which % 2].armed);
	}
	for (unsigned which = 0; which < 4; which++)
	{
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(which >= 2, 3, false);
		ClusterRelmapRetentionDirectory before;
		RelMapFile saved[4];
		bool armed;

		cluster_relmap_producer_precommit_v2(false);
		armed = cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed;
		UT_ASSERT(armed);
		if (!armed)
			continue;
		cluster_relmap_producer_precommit_v2(false); /* empty, already prepared */
		if (which & 1) active_local_updates = updates;
		else active_shared_updates = updates;
		precommit_save_queues(saved);
		before = *directory;
		UT_ASSERT(precommit_error(false));
		UT_ASSERT_EQ(inserts, 2);
		precommit_same_queues(saved);
		cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
	}
	for (unsigned era = 0; era < 2; era++)
	{
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(era != 0, 3, false);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapRetentionKey key = directory->pins[0].key;
		RelMapFile saved[4];
		bool caught;

		for (unsigned slot = 1; slot < CLUSTER_RELMAP_RETENTION_CAPACITY - 1; slot++)
		{
			key.identity.dbid = 50000 + slot;
			UT_ASSERT_EQ(cluster_relmap_retention_begin(directory, &key), CLUSTER_RELMAP_RETENTION_OK);
		}
		precommit_save_queues(saved);
		caught = precommit_error(false);
		UT_ASSERT(caught);
		if (!caught)
			continue;
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(directory->pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].occupied);
		precommit_same_queues(saved);
		before = *directory;
		abort_mask = 1;
		cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(terminal_calls, 1);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(!authority_held);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
	}
	precommit_reset_queues();
	abort_test_reset();
}
