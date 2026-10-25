/*-------------------------------------------------------------------------
 * test_cluster_relmap_unarmed.inc
 *    Incomplete original preparation must retain terminal protection.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_unarmed.inc
 *
 * NOTES
 *    Original producer/runtime/owner/retention algorithms execute. Native
 *    outcome, locks, physical leaves and ERROR/PANIC unwind are controlled.
 *    Simulated PANIC is not process death, restart or recovery qualification.
 *-------------------------------------------------------------------------
 */
enum UnarmedPrepareFault
{
	UNARMED_OK, UNARMED_SECOND_READ_FALSE, UNARMED_WRITE_FALSE,
	UNARMED_FIRST_CHECK_FALSE, UNARMED_LAST_CHECK_FALSE,
	UNARMED_SECOND_READ_ERROR, UNARMED_FLUSH_ERROR, UNARMED_WRITE_ERROR,
	UNARMED_CHECK_ERROR, UNARMED_PRESERVE_ERROR, UNARMED_FIRST_READ_FALSE
};
static enum UnarmedPrepareFault unarmed_fault;

static ClusterRelmapV2PairResult
unarmed_pair(const ClusterRelmapV2Namespace *identity, ClusterRelmapV2View *out)
{
	unsigned slot = identity->shared ? 0 : 1;
	ClusterRelmapV2PairResult result = terminal_pair(identity, out);

	if (terminal_reads[slot] == 2 && unarmed_fault == UNARMED_SECOND_READ_ERROR)
		ereport(ERROR, (errmsg("controlled incomplete prepare reread error")));
	if ((terminal_reads[slot] == 2 && unarmed_fault == UNARMED_SECOND_READ_FALSE)
		|| (terminal_reads[slot] == 1 && unarmed_fault == UNARMED_FIRST_READ_FALSE))
		return CLUSTER_RELMAP_V2_PAIR_DEGRADED;
	return result;
}

static bool
unarmed_check(const ClusterRelmapV2Image *image)
{
	unsigned slot = image->identity.shared ? 0 : 1;
	bool result = terminal_pair_check(image);

	if (unarmed_fault == UNARMED_CHECK_ERROR)
		ereport(ERROR, (errmsg("controlled incomplete prepare stage check error")));
	if ((terminal_checks[slot] == 1 && unarmed_fault == UNARMED_FIRST_CHECK_FALSE)
		|| (terminal_checks[slot] == 2 && unarmed_fault == UNARMED_LAST_CHECK_FALSE))
		return false;
	return result;
}

static void
unarmed_preserve(RelFileLocator locator, bool at_commit)
{
	terminal_preserve(locator, at_commit);
	if (unarmed_fault == UNARMED_PRESERVE_ERROR)
		ereport(ERROR, (errmsg("controlled incomplete prepare preservation error")));
}

static ClusterRelmapV2PairResult
unarmed_retry_pair(const ClusterRelmapV2Namespace *identity, ClusterRelmapV2View *out pg_attribute_unused())
{
	UT_ASSERT(authority_held && !local_held && !v2_retention_held);
	terminal_reads[identity->shared ? 0 : 1]++;
	return CLUSTER_RELMAP_V2_PAIR_DEGRADED;
}

static ClusterRelmapRetentionDirectory *
unarmed_fixture(bool next_era, bool prior, bool shared)
{
	ClusterRelmapRetentionDirectory *directory = abort_fixture(next_era);

	abort_mask = prior ? (shared ? 2 : 1) : 0;
	if (prior)
		UT_ASSERT(cluster_relmap_producer_arm_v2(!shared, &updates));
	unarmed_fault = UNARMED_OK;
	v2_pair_observer = unarmed_pair;
	v2_prepared_pair_observer = unarmed_check;
	v2_preserve_observer = unarmed_preserve;
	return directory;
}

static void
unarmed_set_fault(enum UnarmedPrepareFault fault)
{
	unarmed_fault = fault;
	terminal_prepare_fail = fault == UNARMED_WRITE_FALSE;
	terminal_prepare_error = fault == UNARMED_WRITE_ERROR;
	v2_error_flush = fault == UNARMED_FLUSH_ERROR;
}

static bool
unarmed_arm_error(bool shared)
{
	volatile bool caught = false;

	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		UT_ASSERT(!cluster_relmap_producer_arm_v2(shared, &updates));
	else
	{
		caught = true;
		UT_ASSERT_EQ(error_level, ERROR);
	}
	error_expected = false;
	/* Original abort releases local LWLocks before its relmap callback. */
	if (local_held)
		LWLockRelease(RelationMappingLock);
	return caught;
}

static void
unarmed_protection_unchanged(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionDirectory *before, const ClusterRelmapProducerArmV2 *arms)
{
	UT_ASSERT(memcmp(directory->pins, before->pins, sizeof(before->pins)) == 0);
	UT_ASSERT(memcmp(cluster_relmap_v2_arms, arms, sizeof(cluster_relmap_v2_arms)) == 0);
	UT_ASSERT(authority_held && !local_held && !v2_retention_held);
	UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
	UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 0);
	UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], 0);
}

UT_TEST(v2_unarmed_prepare_false_blocks_terminal_cleanup)
{
	for (unsigned which = 0; which < 32; which++)
	{
		bool shared = (which & 1) != 0;
		ClusterRelmapRetentionDirectory *directory = unarmed_fixture((which & 4) != 0,
			(which & 2) != 0, shared);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 arms[2];
		bool panicked;

		unarmed_set_fault((enum UnarmedPrepareFault) (UNARMED_SECOND_READ_FALSE + which / 8));
		UT_ASSERT(!cluster_relmap_producer_arm_v2(shared, &updates));
		UT_ASSERT(!cluster_relmap_v2_arms[shared ? 0 : 1].armed);
		UT_ASSERT(directory->pins[(which & 2) ? 2 : 1].occupied);
		before = *directory;
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		panicked = abort_panic();
		UT_ASSERT(panicked);
		if (!panicked)
			continue;
		unarmed_protection_unchanged(directory, &before, arms);
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		UT_ASSERT(postcommit_panic());
		unarmed_protection_unchanged(directory, &before, arms);
	}
	abort_test_reset();
}

UT_TEST(v2_unarmed_prepare_error_keeps_protection)
{
	for (unsigned which = 0; which < 40; which++)
	{
		bool shared = (which & 1) != 0;
		ClusterRelmapRetentionDirectory *directory = unarmed_fixture((which & 4) != 0,
			(which & 2) != 0, shared);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 arms[2];
		bool panicked;

		unarmed_set_fault((enum UnarmedPrepareFault) (UNARMED_SECOND_READ_ERROR + which / 8));
		UT_ASSERT(unarmed_arm_error(shared));
		UT_ASSERT(!cluster_relmap_v2_arms[shared ? 0 : 1].armed);
		UT_ASSERT(directory->pins[(which & 2) ? 2 : 1].occupied);
		before = *directory;
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		panicked = abort_panic();
		UT_ASSERT(panicked);
		if (!panicked)
			continue;
		UT_ASSERT(postcommit_panic());
		unarmed_protection_unchanged(directory, &before, arms);
	}
	abort_test_reset();
}

UT_TEST(v2_unarmed_prepare_cannot_retry_or_change_polarity)
{
	for (unsigned which = 0; which < 8; which++)
	{
		bool shared = (which & 1) != 0;
		ClusterRelmapRetentionDirectory *directory = unarmed_fixture((which & 4) != 0,
			(which & 2) != 0, shared);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapProducerArmV2 arms[2];
		unsigned full_before, wal_before, flush_before, tt_before, locks_before, reads_before;

		unarmed_set_fault(UNARMED_WRITE_FALSE);
		UT_ASSERT(!cluster_relmap_producer_arm_v2(shared, &updates));
		unarmed_set_fault(UNARMED_OK);
		v2_pair_observer = unarmed_retry_pair;
		before = *directory;
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		full_before = full_requests;
		wal_before = inserts;
		flush_before = flushes;
		tt_before = v2_tt_reads;
		locks_before = local_acquires;
		reads_before = terminal_reads[0] + terminal_reads[1];
		UT_ASSERT(!cluster_relmap_producer_arm_v2(shared, &updates));
		UT_ASSERT_EQ(full_requests, full_before);
		/* Keep the exact-parent control from subsequent cleanup after its
		 * first already-proven unsafe retry has queried the native source. */
		if (full_requests != full_before)
			continue;
		UT_ASSERT(!cluster_relmap_producer_arm_v2(!shared, &updates));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		UT_ASSERT(abort_panic());
		UT_ASSERT(postcommit_panic());
		UT_ASSERT_EQ(full_requests, full_before);
		UT_ASSERT_EQ(inserts, wal_before);
		UT_ASSERT_EQ(flushes, flush_before);
		UT_ASSERT_EQ(v2_tt_reads, tt_before);
		UT_ASSERT_EQ(local_acquires, locks_before);
		UT_ASSERT_EQ(terminal_reads[0] + terminal_reads[1], reads_before);
		unarmed_protection_unchanged(directory, &before, arms);
	}
	abort_test_reset();
}

UT_TEST(v2_pre_effect_refusals_preserve_normal_abort)
{
	for (unsigned which = 0; which < 12; which++)
	{
		ClusterRelmapRetentionDirectory *directory = unarmed_fixture(which >= 6, true, false);
		ClusterRelmapRetentionDirectory before;
		RelMapFile refused_updates = updates;
		const RelMapFile *input = &refused_updates;
		unsigned wal_before = inserts, preserve_before = preserves;

		switch (which % 6)
		{
			case 0: input = NULL; break;
			case 1: refused_updates.num_mappings = -1; break;
			case 2: refused_updates.num_mappings = MAX_MAPPINGS + 1; break;
			case 3: MyDatabaseTableSpace = InvalidOid; break;
			case 4: unarmed_set_fault(UNARMED_FIRST_READ_FALSE); break;
			case 5:
			{
				ClusterRelmapRetentionKey key = directory->pins[1].key;

				key.identity.shared = false;
				for (unsigned slot = 2; slot < CLUSTER_RELMAP_RETENTION_CAPACITY; slot++)
				{
					key.identity.dbid = 50000 + slot;
					UT_ASSERT_EQ(cluster_relmap_retention_begin(directory, &key),
						CLUSTER_RELMAP_RETENTION_OK);
				}
				UT_ASSERT(directory->pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].occupied);
				break;
			}
		}
		before = *directory;
		UT_ASSERT(!cluster_relmap_producer_arm_v2(false, input));
		MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(inserts, wal_before);
		UT_ASSERT_EQ(preserves, preserve_before);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(terminal_calls, 1);
		UT_ASSERT_EQ(terminal_trace[0], 0);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(!authority_held && !cluster_relmap_v2_finish_entered);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(memcmp(directory->pins, before.pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1] + ack_attempts[0] + ack_attempts[1], 0);
	}
	abort_test_reset();
}
