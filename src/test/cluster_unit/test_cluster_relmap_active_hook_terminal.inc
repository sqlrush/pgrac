/* Automatic late ACTIVE binding through the original terminal owners.
 * Native TT I/O, outcome/WAL/admission and PANIC survival remain controlled.
 * Relmap dual-copy temporary-file I/O uses the original physical bridge.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned late_terminal_hook_calls;

static bool
late_terminal_active_binding(TransactionId xid)
{
	late_terminal_hook_calls++;
	UT_ASSERT_EQ(xid, XidFromFullTransactionId(assigned_xid));
	return cluster_relmap_retention_runtime_after_active_v2(xid);
}

static ClusterRelmapRetentionDirectory *
late_terminal_fixture(unsigned which, unsigned mask, bool commit)
{
	ClusterRelmapRetentionDirectory *directory;
	bool refused;

	cluster_node_id = 0;
	directory = physical_fixture(which >= 3, mask, commit);
	/* A fresh original backend per case; never production reattachment. */
	memset(&relmap_runtime_active_owner, 0, sizeof(relmap_runtime_active_owner));
	test_relmap_native_tt_reset();
	v2_published_binding_observer = test_relmap_native_tt_lookup;
	late_terminal_hook_calls = 0;
	test_relmap_native_tt_observe_active(late_terminal_active_binding);
	/* Pins and both prepared copies precede the original ACTIVE publisher. */
	refused = precommit_error(false);
	UT_ASSERT(!refused);
	return refused ? NULL : directory;
}

static bool
late_terminal_publish(const ClusterRelmapRetentionDirectory *directory,
	unsigned mask, ClusterCanonicalTxnBinding *receipt)
{
	ClusterRelmapRetentionPin unrelated = directory->pins[0];
	bool found;

	if (!original_tt_publish())
		return false;
	UT_ASSERT_EQ(late_terminal_hook_calls, 1);
	original_tt_pins(directory, &unrelated, mask, true);
	found = test_relmap_native_tt_lookup(XidFromFullTransactionId(assigned_xid), receipt);
	UT_ASSERT(found);
	return found;
}

UT_TEST(v2_late_active_tt_commit_then_relmap_postcommit)
{
	for (unsigned which = 0; which < 6; which++)
	{
		unsigned mask = which % 3 + 1;
		ClusterRelmapRetentionDirectory *directory = late_terminal_fixture(which, mask, true), before;
		ClusterCanonicalTxnBinding receipt;
		TestRelmapNativeTTTerminal result;

		if (directory == NULL || !late_terminal_publish(directory, mask, &receipt))
		{ original_tt_done(); continue; }
		before = *directory;
		/* Original commit retires the local binding before relmap publication. */
		UT_ASSERT(test_relmap_native_tt_terminal(&receipt, true, false, &result));
		terminal_tt_success(&receipt, &result, true);
		terminal_tt_retained(directory, &before, &receipt);
		physical_finishing = v2_terminal_context = true;
		original_terminal_callback(true);
		physical_clean_pairs();
		UT_ASSERT_EQ(terminal_calls, mask == 3 ? 2 : 1);
		UT_ASSERT(!cluster_relmap_authority_x_held());
		terminal_tt_retained(directory, &before, &receipt);
		original_tt_done();
	}
}

UT_TEST(v2_late_active_relmap_abort_then_tt_abort)
{
	for (unsigned which = 0; which < 6; which++)
	{
		unsigned mask = which % 3 + 1;
		ClusterRelmapRetentionDirectory *directory = late_terminal_fixture(which, mask, false), before;
		ClusterCanonicalTxnBinding receipt, after;
		TestRelmapNativeTTTerminal result;

		if (directory == NULL || !late_terminal_publish(directory, mask, &receipt))
		{ original_tt_done(); continue; }
		before = *directory;
		/* Original abort discards relmap before its TT terminal stamp. */
		physical_finishing = true;
		original_terminal_callback(false);
		physical_clean_pairs();
		UT_ASSERT_EQ(terminal_calls, mask == 3 ? 2 : 1);
		UT_ASSERT(!cluster_relmap_authority_x_held());
		UT_ASSERT(test_relmap_native_tt_lookup(receipt.xid, &after));
		UT_ASSERT(memcmp(&receipt, &after, sizeof(receipt)) == 0);
		terminal_tt_retained(directory, &before, &receipt);
		UT_ASSERT(test_relmap_native_tt_terminal(&receipt, false, false, &result));
		terminal_tt_success(&receipt, &result, false);
		terminal_tt_retained(directory, &before, &receipt);
		original_tt_done();
	}
}

UT_TEST(v2_late_active_committed_tt_keeps_partial_relmap_failure)
{
	for (unsigned which = 0; which < 4; which++)
	{
		ClusterRelmapRetentionDirectory *directory = late_terminal_fixture(which >= 2 ? 3 : 0, 3, true), before;
		ClusterCanonicalTxnBinding receipt;
		ClusterRelmapProducerArmV2 saved[2];
		ClusterRelmapV2Image clean;
		ClusterRelmapV2View remaining;
		TestRelmapNativeTTTerminal result;
		TestRelmapNativeLockStats stats;

		if (directory == NULL || !late_terminal_publish(directory, 3, &receipt))
		{ original_tt_done(); continue; }
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		UT_ASSERT(test_relmap_native_tt_terminal(&receipt, true, false, &result));
		terminal_tt_success(&receipt, &result, true);
		terminal_tt_retained(directory, &before, &receipt);
		test_relmap_physical_reset(physical_root, physical_observe);
		test_relmap_physical_fail_sync((which & 1) != 0, 3);
		physical_finishing = v2_terminal_context = true;
		UT_ASSERT(original_callback_panic(true));
		native_lock_keeps_receipts(directory, &before, saved);
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT(terminal_disk_cleared[0] && !terminal_disk_cleared[1]);
		clean = physical_initial[0];
		clean.committed_generation = 8;
		clean.committed = saved[0].staged.pending;
		physical_pair_equals(0, &clean);
		UT_ASSERT_EQ(test_relmap_physical_v2_read(&physical_initial[1].identity, &remaining),
			CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(remaining.pending_complete && remaining.has_pending);
		UT_ASSERT_EQ(remaining.pending_generation, 8);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1]
			+ ack_attempts[0] + ack_attempts[1] + unlock_calls, 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 0);
		terminal_tt_retained(directory, &before, &receipt);
		original_tt_done();
	}
}

UT_TEST(v2_late_active_refusal_blocks_abort_cleanup)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = late_terminal_fixture(which, 3, false), before;
		ClusterRelmapProducerArmV2 saved[2];
		ClusterCanonicalTxnBinding receipt, after;
		TestRelmapNativeTTStats tt_before, tt_after;
		TestRelmapNativeLockStats stats;

		if (directory == NULL) { original_tt_done(); continue; }
		if (which % 3 == 0) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which % 3 == 1) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which % 3 == 2) writer_available = false;
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		UT_ASSERT(!test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)));
		UT_ASSERT_EQ(late_terminal_hook_calls, 1);
		UT_ASSERT(test_relmap_native_tt_lookup(100, &receipt));
		UT_ASSERT_EQ(receipt.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
		UT_ASSERT(receipt.active_lsn != InvalidXLogRecPtr);
		tt_before = test_relmap_native_tt_stats();
		physical_finishing = true;
		/* Failed relmap abort stops before native RecordTransactionAbort. */
		UT_ASSERT(original_callback_panic(false));
		native_lock_keeps_receipts(directory, &before, saved);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		UT_ASSERT_EQ(terminal_calls + local_invalidations[0] + local_invalidations[1]
			+ ack_attempts[0] + ack_attempts[1] + unlock_calls, 0);
		for (unsigned slot = 0; slot < 2; slot++)
			physical_pair_equals(slot, &terminal_images[slot]);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 0);
		UT_ASSERT(test_relmap_native_tt_lookup(100, &after));
		UT_ASSERT(memcmp(&receipt, &after, sizeof(receipt)) == 0);
		tt_after = test_relmap_native_tt_stats();
		UT_ASSERT(memcmp(&tt_before, &tt_after, sizeof(tt_before)) == 0);
		UT_ASSERT_EQ(tt_after.bindings, 1);
		UT_ASSERT(tt_after.balanced && !tt_after.assertion_failed);
		original_tt_release_check(directory, &receipt, &receipt,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		original_tt_done();
	}
}
