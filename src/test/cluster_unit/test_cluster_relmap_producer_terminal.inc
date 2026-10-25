/*-------------------------------------------------------------------------
 * test_cluster_relmap_producer_terminal.inc
 *    Original prepare receipts retained through one terminal cleanup attempt.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_producer_terminal.inc
 *
 * NOTES
 *    Native owner/runtime/producer code is original; physical I/O and native
 *    transaction leaves are controlled. No live or terminal qualification.
 *-------------------------------------------------------------------------
 */
static ClusterRelmapV2Image terminal_images[2];
static unsigned terminal_reads[2], terminal_checks[2], terminal_calls;
static unsigned terminal_trace[2], terminal_fault, terminal_fail_at;
static bool terminal_publish, terminal_prepare_fail, terminal_prepare_error;
static bool terminal_disk_cleared[2];
static int terminal_preparing_slot;

static ClusterRelmapV2PairResult
terminal_pair(const ClusterRelmapV2Namespace *identity, ClusterRelmapV2View *out)
{
	unsigned slot = identity->shared ? 0 : 1;

	UT_ASSERT(authority_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT_EQ(local_held, terminal_reads[slot] != 0);
	terminal_reads[slot]++;
	terminal_preparing_slot = slot;
	memset(out, 0, sizeof(*out));
	out->committed = committed;
	out->committed_generation = 7;
	out->pending_complete = true;
	return CLUSTER_RELMAP_V2_PAIR_VALID;
}

static bool
terminal_write(const ClusterRelmapV2Namespace *identity, uint64 generation,
	const ClusterRelmapV2Owner *owner, const RelMapFile *pending pg_attribute_unused(),
	ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	ClusterRelmapRetentionKey key = { 0 };

	UT_ASSERT(authority_held && local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(lock == v2_retention_lock);
	UT_ASSERT_EQ(flushes, inserts);
	UT_ASSERT_EQ(owner->wal_end, 1000 + inserts);
	v2_stage_calls++;
	key.identity = *identity;
	key.generation = generation;
	key.origin = owner->node;
	key.top_full_xid = owner->top_full_xid;
	key.membership_epoch = owner->membership_epoch;
	key.admitted_incarnation = owner->admitted_incarnation;
	key.boot_incarnation = owner->boot_incarnation;
	LWLockAcquire(lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(directory, &key, owner->wal_end),
		CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(lock);
	v2_stage_writes++;
	if (terminal_prepare_error)
		ereport(ERROR, (errmsg("controlled prepare write error")));
	if (terminal_prepare_fail)
		return false;
	v2_stage_writes++;
	return true;
}

static bool
terminal_pair_check(const ClusterRelmapV2Image *image)
{
	unsigned slot = image->identity.shared ? 0 : 1;
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];

	UT_ASSERT(authority_held && local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(image, wire, sizeof(wire)), CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT_EQ(image->pending_owner.wal_end, 1000 + inserts);
	terminal_images[slot] = *image;
	terminal_checks[slot]++;
	return true;
}

static void
terminal_preserve(RelFileLocator locator, bool at_commit)
{
	const ClusterRelmapV2Image *image = &terminal_images[terminal_preparing_slot];

	UT_ASSERT(authority_held && local_held && !v2_retention_held && !at_commit);
	UT_ASSERT_EQ(locator.spcOid, image->identity.shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
	UT_ASSERT_EQ(locator.dbOid, image->identity.dbid);
	UT_ASSERT_EQ(locator.relNumber, image->pending.mappings[preserves % 2].mapfilenumber);
	preserves++;
}

static ClusterRelmapV2ClearResult
terminal_clear(const ClusterRelmapV2Image *image, bool publish)
{
	unsigned slot = image->identity.shared ? 0 : 1;
	uint8 actual[CLUSTER_RELMAP_V2_FILE_SIZE], expected[CLUSTER_RELMAP_V2_FILE_SIZE];

	UT_ASSERT(authority_held && !local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(v2_terminal_context && terminal_calls < 2);
	UT_ASSERT_EQ(publish, terminal_publish);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(image, actual, sizeof(actual)), CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&terminal_images[slot], expected, sizeof(expected)),
		CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
	terminal_trace[terminal_calls++] = slot;
	if (terminal_calls == terminal_fail_at)
	{
		if (terminal_fault == 1)
			return CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
		if (terminal_fault == 2)
			ereport(ERROR, (errmsg("controlled terminal pair error")));
		if (terminal_fault == 3)
			change_original_owner();
		if (terminal_fault == 4)
			cluster_relmap_retention_invalidate(&relmap_runtime_shmem->directories[cluster_node_id]);
	}
	terminal_disk_cleared[slot] = true;
	return CLUSTER_RELMAP_V2_CLEARED_POSTREAD;
}

ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_publish(const ClusterRelmapV2Image *image)
{
	return terminal_clear(image, true);
}

ClusterRelmapV2ClearResult
cluster_relmap_authority_v2_discard_pending(const ClusterRelmapV2Image *image)
{
	return terminal_clear(image, false);
}

/* Test caller alone simulates a new process after a sticky failed attempt. */
static void
terminal_test_reset(void)
{
	memset(cluster_relmap_v2_arms, 0, sizeof(cluster_relmap_v2_arms));
	cluster_relmap_v2_finish_entered = false;
	v2_terminal_context = false;
	v2_pair_observer = NULL;
	v2_write_observer = NULL;
	v2_prepared_pair_observer = NULL;
	v2_preserve_observer = NULL;
	MyBackendType = B_BACKEND;
	MyProcPid = 79;
	MyDatabaseId = 12345;
	MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
	cluster_enabled = cluster_shared_config = true;
	ShutdownRequestPending = false;
	CritSectionCount = 0;
	local_held = v2_xidgen_held = v2_retention_held = false;
}

static ClusterRelmapRetentionDirectory *
terminal_fixture(bool next_era)
{
	ClusterRelmapRetentionDirectory *directory;

	terminal_test_reset();
	directory = prepare_fixture(false, next_era);
	memset(terminal_images, 0, sizeof(terminal_images));
	memset(terminal_reads, 0, sizeof(terminal_reads));
	memset(terminal_checks, 0, sizeof(terminal_checks));
	memset(terminal_disk_cleared, 0, sizeof(terminal_disk_cleared));
	terminal_calls = terminal_fault = terminal_fail_at = 0;
	terminal_prepare_fail = terminal_prepare_error = false;
	v2_pair_observer = terminal_pair;
	v2_write_observer = terminal_write;
	v2_prepared_pair_observer = terminal_pair_check;
	v2_preserve_observer = terminal_preserve;
	return directory;
}

static bool
terminal_arm_both(bool first_shared)
{
	bool first = cluster_relmap_producer_arm_v2(first_shared, &updates);
	bool second = first && cluster_relmap_producer_arm_v2(!first_shared, &updates);

	UT_ASSERT(first && second);
	return first && second;
}

static void
terminal_original_duties_untouched(void)
{
	UT_ASSERT_EQ(publish_calls[0] + publish_calls[1] + discard_calls[0] + discard_calls[1], 0);
	UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 0);
	UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1] + unlock_calls, 0);
}

UT_TEST(v2_terminal_custody_uses_original_prepare_for_both_flavours)
{
	for (unsigned which = 0; which < 8; which++)
	{
		ClusterRelmapRetentionDirectory *directory = terminal_fixture((which & 4) != 0);
		ClusterRelmapRetentionPin before[3];

		terminal_publish = (which & 1) != 0;
		if (!terminal_arm_both((which & 2) != 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		UT_ASSERT(directory->pins[0].occupied && directory->pins[1].occupied
			&& directory->pins[2].occupied && !directory->pins[3].occupied);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(full_requests, 2);
		UT_ASSERT_EQ(inserts, 2);
		UT_ASSERT_EQ(flushes, 2);
		UT_ASSERT_EQ(sync_commits, 2);
		UT_ASSERT_EQ(preserves, 4);
		UT_ASSERT_EQ(terminal_checks[0] + terminal_checks[1], 4);
		v2_terminal_context = true;
		UT_ASSERT(cluster_relmap_producer_finish_v2(terminal_publish));
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT_EQ(terminal_trace[0], 0);
		UT_ASSERT_EQ(terminal_trace[1], 1);
		UT_ASSERT(terminal_disk_cleared[0] && terminal_disk_cleared[1]);
		UT_ASSERT(memcmp(before, directory->pins, sizeof(before)) == 0);
		UT_ASSERT(directory->pins[0].occupied && directory->pins[1].occupied
			&& directory->pins[2].occupied && !directory->pins[3].occupied);
		UT_ASSERT(!cluster_relmap_producer_finish_v2(!terminal_publish));
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT_EQ(full_requests, 2);
		UT_ASSERT(authority_held && !local_held && !v2_retention_held);
		terminal_original_duties_untouched();
	}
	terminal_test_reset();
}

UT_TEST(v2_terminal_arm_refuses_without_replacing_original_custody)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = terminal_fixture(false);
		unsigned calls;

		if (which < 2)
			cluster_relmap_publish_arms[which].armed = true;
		if (which == 2 || which == 3)
		{
			bool armed = cluster_relmap_producer_arm_v2(true, &updates);
			UT_ASSERT(armed);
			if (!armed)
				continue;
		}
		if (which == 3)
			change_original_owner();
		if (which == 4)
			terminal_prepare_fail = true;
		if (which == 5)
			terminal_prepare_error = true;
		calls = inserts;
		error_expected = which == 5;
		if (sigsetjmp(error_target, 1) == 0)
		{
			UT_ASSERT(!cluster_relmap_producer_arm_v2(which != 3, &updates));
			UT_ASSERT(which != 5);
		}
		else
		{
			UT_ASSERT_EQ(which, 5);
			UT_ASSERT_EQ(error_level, ERROR);
			UT_ASSERT(local_held && directory->pins[1].may_be_staged);
		}
		error_expected = false;
		UT_ASSERT_EQ(inserts, calls + (which >= 4 ? 1 : 0));
		UT_ASSERT_EQ(terminal_calls, 0);
		UT_ASSERT_EQ(directory->pins[0].wal_end, 900);
		terminal_original_duties_untouched();
	}
	terminal_test_reset();
}

UT_TEST(v2_terminal_attempt_context_refusals_are_sticky)
{
	for (unsigned which = 0; which < 16; which++)
	{
		ClusterRelmapRetentionDirectory *directory = terminal_fixture(false);
		ClusterRelmapRetentionPin before[3];

		if (!terminal_arm_both(true))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		v2_terminal_context = true;
		switch (which)
		{
			case 0: local_held = true; break;
			case 1: v2_xidgen_held = true; break;
			case 2: v2_retention_held = true; break;
			case 3: CritSectionCount = 1; break;
			case 4: ShutdownRequestPending = true; break;
			case 5: authority_held = false; break;
			case 6: MyProcPid++; break;
			case 7: drift_kind = 4; change_original_owner(); break;
			case 8: writer_epoch++; break;
			case 9: boot_incarnation++; break;
			case 10: admitted_incarnation++; break;
			case 11: writer_ref.claim.claim_sha256[0]++; break;
			case 12: insert_timeline++; break;
			case 13: cluster_enabled = false; break;
			case 14: cluster_relmap_publish_arms[0].armed = true; break;
			case 15: MyDatabaseId++; break;
		}
		UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
		UT_ASSERT_EQ(terminal_calls, 0);
		UT_ASSERT(memcmp(before, directory->pins, sizeof(before)) == 0);
		UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
		local_held = v2_xidgen_held = v2_retention_held = false;
		CritSectionCount = 0;
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT_EQ(terminal_calls, 0);
		terminal_original_duties_untouched();
	}
	terminal_test_reset();
}

UT_TEST(v2_terminal_partial_unknown_and_errors_keep_all_custody)
{
	for (unsigned which = 0; which < 8; which++)
	{
		ClusterRelmapRetentionDirectory *directory = terminal_fixture(false);
		ClusterRelmapRetentionPin before[3];
		bool threw = false;

		if (!terminal_arm_both((which & 1) != 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		terminal_publish = (which & 1) != 0;
		terminal_fault = which / 2 + 1;
		terminal_fail_at = which % 2 + 1;
		v2_terminal_context = true;
		error_expected = terminal_fault == 2;
		if (sigsetjmp(error_target, 1) == 0)
			UT_ASSERT(!cluster_relmap_producer_finish_v2(terminal_publish));
		else
		{
			threw = true;
			UT_ASSERT_EQ(error_level, ERROR);
		}
		error_expected = false;
		UT_ASSERT_EQ(threw, terminal_fault == 2);
		UT_ASSERT_EQ(terminal_calls, terminal_fail_at);
		UT_ASSERT(memcmp(before, directory->pins, sizeof(before)) == 0);
		UT_ASSERT(directory->pins[0].occupied && directory->pins[1].occupied
			&& directory->pins[2].occupied && !directory->pins[3].occupied);
		if (terminal_fail_at == 2)
			UT_ASSERT(terminal_disk_cleared[0]);
		UT_ASSERT(!cluster_relmap_producer_finish_v2(!terminal_publish));
		UT_ASSERT_EQ(terminal_calls, terminal_fail_at);
		UT_ASSERT(!cluster_relmap_producer_arm_v2(false, &updates));
		UT_ASSERT_EQ(full_requests, 2);
		UT_ASSERT(authority_held && !local_held && !v2_retention_held);
		terminal_original_duties_untouched();
	}
	terminal_test_reset();
}
