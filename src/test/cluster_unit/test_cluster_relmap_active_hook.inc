/* Original ACTIVE publisher must enroll earlier relmap pins before return.
 * Native TT leaves and initial directory qualification remain controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned active_hook_calls;

static bool
original_active_binding(TransactionId xid)
{
	active_hook_calls++;
	UT_ASSERT_EQ(xid, XidFromFullTransactionId(assigned_xid));
	return cluster_relmap_retention_runtime_after_active_v2(xid);
}

static ClusterRelmapRetentionDirectory *
active_hook_fixture(unsigned which, unsigned mask)
{
	ClusterRelmapRetentionDirectory *directory;

	cluster_node_id = 0;
	directory = physical_fixture(which >= 3, mask, false);
	/* Each case represents a fresh original backend, not shmem reattachment. */
	memset(&relmap_runtime_active_owner, 0, sizeof(relmap_runtime_active_owner));
	test_relmap_native_tt_reset();
	v2_published_binding_observer = test_relmap_native_tt_lookup;
	active_hook_calls = 0;
	return directory;
}

UT_TEST(v2_active_hook_has_no_duty_without_original_begin)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which, 3), before;
		ClusterRelmapOwnerSampleV2 sample;
		unsigned reads, tt_reads;

		(void)GetTopFullTransactionId();
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		directory->state = which % 2 ? CLUSTER_RELMAP_RETENTION_UNKNOWN
			: CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which % 2)
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample,
				&physical_initial[0].identity, 8), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		writer_available = false;
		reads = writer_reads;
		tt_reads = v2_tt_reads;
		before = *directory;
		test_relmap_native_tt_observe_active(original_active_binding);
		UT_ASSERT(test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)));
		UT_ASSERT_EQ(active_hook_calls, 1);
		UT_ASSERT_EQ(writer_reads, reads);
		UT_ASSERT_EQ(v2_tt_reads, tt_reads);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		original_tt_done();
	}
}

UT_TEST(v2_active_hook_rejects_a_changed_original_owner)
{
	for (unsigned which = 0; which < 8; which++)
	{
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which >= 4 ? 3 : 0, 3), before;
		FullTransactionId original;
		TransactionId raw;
		int pid = MyProcPid;

		UT_ASSERT(!precommit_error(false));
		original = assigned_xid;
		raw = XidFromFullTransactionId(original);
		before = *directory;
		if (which % 4 == 0) raw++;
		if (which % 4 == 1) assigned_xid = InvalidFullTransactionId;
		if (which % 4 == 2) cluster_node_id++;
		if (which % 4 == 3) MyProcPid++;
		UT_ASSERT(!cluster_relmap_retention_runtime_after_active_v2(raw));
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		UT_ASSERT_EQ(test_relmap_native_tt_stats().bind_records, 0);
		MyProcPid = pid;
		cluster_node_id = 0;
		assigned_xid = original;
		original_tt_done();
	}
}

UT_TEST(v2_active_hook_does_not_reconstruct_a_new_full_xid_era)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which, 3), before;
		FullTransactionId original;

		UT_ASSERT(!precommit_error(false));
		original = assigned_xid;
		assigned_xid = FullTransactionIdFromU64(U64FromFullTransactionId(original)
			+ (UINT64CONST(1) << 32));
		UT_ASSERT_EQ(XidFromFullTransactionId(assigned_xid), XidFromFullTransactionId(original));
		directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		writer_available = false;
		before = *directory;
		test_relmap_native_tt_observe_active(original_active_binding);
		UT_ASSERT(test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)));
		UT_ASSERT_EQ(active_hook_calls, 1);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		original_tt_done();
	}
}

UT_TEST(v2_active_hook_forgets_only_all_cancelled_zero_effect_prepares)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which, 3), before;
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identities[2];
		bool cancel_both = which % 2 == 0;
		ClusterCanonicalTxnBinding receipt;

		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		for (unsigned slot = 0; slot < 2; slot++)
		{
			identities[slot] = physical_initial[slot].identity;
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identities[slot], 8),
				CLUSTER_RELMAP_RETENTION_OK);
		}
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identities[0], 8),
			CLUSTER_RELMAP_RETENTION_OK);
		if (cancel_both)
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identities[1], 8),
				CLUSTER_RELMAP_RETENTION_OK);
		directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		before = *directory;
		test_relmap_native_tt_observe_active(original_active_binding);
		UT_ASSERT_EQ(test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)), cancel_both);
		UT_ASSERT_EQ(active_hook_calls, 1);
		UT_ASSERT(test_relmap_native_tt_lookup(XidFromFullTransactionId(assigned_xid), &receipt));
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		original_tt_done();
	}
}

UT_TEST(v2_active_hook_binds_all_prior_staged_pins)
{
	for (unsigned which = 0; which < 6; which++)
	{
		unsigned mask = which % 3 + 1;
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which, mask);
		ClusterRelmapRetentionPin unrelated = directory->pins[0];

		UT_ASSERT(!precommit_error(false));
		test_relmap_native_tt_observe_active(original_active_binding);
		UT_ASSERT(test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)));
		UT_ASSERT_EQ(active_hook_calls, 1);
		original_tt_pins(directory, &unrelated, mask, true);
		UT_ASSERT(test_relmap_native_tt_stats().balanced);
		original_tt_done();
	}
}

UT_TEST(v2_active_hook_refusal_preserves_published_active_and_pins)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which, 3), before;
		ClusterCanonicalTxnBinding receipt;
		ClusterRelmapProducerArmV2 saved[2];
		TestRelmapNativeTTStats stats;

		UT_ASSERT(!precommit_error(false));
		if (which % 3 == 0) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which % 3 == 1) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which % 3 == 2) writer_available = false;
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		test_relmap_native_tt_observe_active(original_active_binding);
		UT_ASSERT(!test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)));
		UT_ASSERT_EQ(active_hook_calls, 1);
		UT_ASSERT(test_relmap_native_tt_lookup(XidFromFullTransactionId(assigned_xid), &receipt));
		UT_ASSERT_EQ(receipt.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
		UT_ASSERT(receipt.active_lsn != InvalidXLogRecPtr);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		UT_ASSERT(memcmp(saved, cluster_relmap_v2_arms, sizeof(saved)) == 0);
		stats = test_relmap_native_tt_stats();
		UT_ASSERT(stats.balanced && !stats.assertion_failed);
		UT_ASSERT_EQ(stats.bind_records, 1);
		UT_ASSERT_EQ(stats.ctrc_opens, 1);
		original_tt_done();
	}
}

UT_TEST(v2_active_hook_rechecks_already_published_receipt)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory = active_hook_fixture(which, 3), before;
		ClusterCanonicalTxnBinding receipt, after;

		(void)GetTopFullTransactionId();
		UT_ASSERT(original_tt_publish());
		UT_ASSERT(!precommit_error(false));
		UT_ASSERT(test_relmap_native_tt_lookup(XidFromFullTransactionId(assigned_xid), &receipt));
		if (which % 3 == 0) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which % 3 == 1) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which % 3 == 2) writer_available = false;
		before = *directory;
		test_relmap_native_tt_observe_active(original_active_binding);
		UT_ASSERT(!test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid)));
		UT_ASSERT_EQ(active_hook_calls, 1);
		UT_ASSERT(test_relmap_native_tt_lookup(XidFromFullTransactionId(assigned_xid), &after));
		UT_ASSERT(memcmp(&receipt, &after, sizeof(receipt)) == 0);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		UT_ASSERT_EQ(test_relmap_native_tt_stats().bind_records, 1);
		UT_ASSERT(test_relmap_native_tt_stats().balanced);
		original_tt_done();
	}
}
