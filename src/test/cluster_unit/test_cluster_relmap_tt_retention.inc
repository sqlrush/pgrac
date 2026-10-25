/* Original ACTIVE receipt and relmap TT release conjunct beside real pairs.
 * Native TT outcome/admission/storage and transaction context are controlled.
 * The read-only conjunct never authorizes recycling or retires a pin.
 * Author: SqlRush <sqlrush@gmail.com> */

static void
original_tt_release_check(const ClusterRelmapRetentionDirectory *directory,
	const ClusterCanonicalTxnBinding *original, const ClusterCanonicalTxnBinding *query,
	ClusterRelmapRetentionResult expected)
{
	ClusterRelmapRetentionDirectory before = *directory;
	ClusterCanonicalTxnBinding after;
	TestRelmapPhysicalStats io_before = test_relmap_physical_stats(), io_after;
	TestRelmapNativeTTStats tt_before = test_relmap_native_tt_stats(), tt_after;
	unsigned owner_reads = writer_reads, full_reads = full_requests, raw_reads = raw_requests;
	unsigned assigned = assignments, binding_reads = v2_tt_reads;

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_tt_release_check_v2(
		directory->system_identifier, directory->database_incarnation, query), expected);
	UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
	io_after = test_relmap_physical_stats();
	UT_ASSERT(memcmp(&io_before, &io_after, sizeof(io_before)) == 0);
	UT_ASSERT_EQ(writer_reads, owner_reads);
	UT_ASSERT_EQ(full_requests, full_reads);
	UT_ASSERT_EQ(raw_requests, raw_reads);
	UT_ASSERT_EQ(assignments, assigned);
	UT_ASSERT_EQ(v2_tt_reads, binding_reads);
	UT_ASSERT(!v2_retention_held && !local_held && CritSectionCount == 0);
	UT_ASSERT(test_relmap_native_tt_lookup(original->xid, &after));
	UT_ASSERT(memcmp(original, &after, sizeof(after)) == 0);
	tt_after = test_relmap_native_tt_stats();
	UT_ASSERT_EQ(tt_after.bindings, tt_before.bindings);
	UT_ASSERT_EQ(tt_after.bind_records, tt_before.bind_records);
	UT_ASSERT_EQ(tt_after.ctrc_opens, tt_before.ctrc_opens);
	UT_ASSERT(tt_after.balanced && !tt_after.assertion_failed);
}

UT_TEST(v2_tt_retention_outlives_original_terminal_cleanup)
{
	for (unsigned which = 0; which < 24; which++)
	{
		unsigned mask = which % 3 + 1, count = mask == 3 ? 2 : 1;
		bool publish = (which / 6) % 2 != 0, before_pin = which < 12;
		ClusterRelmapRetentionDirectory *directory, before;
		ClusterCanonicalTxnBinding receipt;
		TestRelmapNativeLockStats stats;
		bool refused;

		cluster_node_id = 0;
		directory = physical_fixture((which / 3) % 2 != 0, mask, publish);
		test_relmap_native_tt_reset();
		v2_published_binding_observer = test_relmap_native_tt_lookup;
		if (before_pin)
		{
			(void)GetTopFullTransactionId();
			if (!original_tt_publish())
			{
				original_tt_done();
				continue;
			}
		}
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (refused || (!before_pin && !original_tt_publish()))
		{
			original_tt_done();
			continue;
		}
		if (!before_pin)
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT(test_relmap_native_tt_lookup(100, &receipt));
		before = *directory;
		original_tt_release_check(directory, &receipt, &receipt, CLUSTER_RELMAP_RETENTION_RETAINED);
		physical_finishing = true;
		v2_terminal_context = publish;
		original_terminal_callback(publish);
		physical_clean_pairs();
		UT_ASSERT_EQ(terminal_calls, count);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(!cluster_relmap_v2_finish_entered && !cluster_relmap_authority_x_held());
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		/* Physical cleanup and global-X release leave the original TT debt. */
		original_tt_release_check(directory, &receipt, &receipt, CLUSTER_RELMAP_RETENTION_RETAINED);
		original_tt_done();
	}
}

UT_TEST(v2_tt_retention_keeps_partial_terminal_failure)
{
	for (unsigned which = 0; which < 8; which++)
	{
		bool publish = (which & 1) != 0;
		ClusterRelmapRetentionDirectory *directory, before;
		ClusterCanonicalTxnBinding receipt;
		ClusterRelmapProducerArmV2 saved[2];
		ClusterRelmapV2Image clean;
		ClusterRelmapV2View remaining;
		TestRelmapNativeLockStats stats;
		bool refused;

		cluster_node_id = 0;
		directory = physical_fixture(which >= 4, 3, publish);
		test_relmap_native_tt_reset();
		v2_published_binding_observer = test_relmap_native_tt_lookup;
		(void)GetTopFullTransactionId();
		if (!original_tt_publish())
		{
			original_tt_done();
			continue;
		}
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (refused)
		{
			original_tt_done();
			continue;
		}
		UT_ASSERT(test_relmap_native_tt_lookup(100, &receipt));
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		original_tt_release_check(directory, &receipt, &receipt, CLUSTER_RELMAP_RETENTION_RETAINED);
		test_relmap_physical_reset(physical_root, physical_observe);
		test_relmap_physical_fail_sync((which & 2) != 0, 3);
		physical_finishing = true;
		v2_terminal_context = publish;
		UT_ASSERT(original_callback_panic(publish));
		native_lock_keeps_receipts(directory, &before, saved);
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT(terminal_disk_cleared[0] && !terminal_disk_cleared[1]);
		clean = physical_initial[0];
		if (publish)
		{
			clean.committed_generation = 8;
			clean.committed = saved[0].staged.pending;
		}
		physical_pair_equals(0, &clean);
		UT_ASSERT_EQ(test_relmap_physical_v2_read(&physical_initial[1].identity, &remaining),
			CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(remaining.pending_complete && remaining.has_pending);
		UT_ASSERT_EQ(remaining.pending_generation, 8);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1]
			+ ack_attempts[0] + ack_attempts[1] + unlock_calls, 0);
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 0);
		original_tt_release_check(directory, &receipt, &receipt, CLUSTER_RELMAP_RETENTION_RETAINED);
		original_tt_done();
	}
}

UT_TEST(v2_tt_retention_routes_original_receipt_and_exact_physical_key)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory, before;
		ClusterCanonicalTxnBinding receipt, query;
		FullTransactionId top;
		bool refused;

		cluster_node_id = 0;
		directory = physical_fixture(which >= 3, which % 3 + 1, false);
		test_relmap_native_tt_reset();
		v2_published_binding_observer = test_relmap_native_tt_lookup;
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (refused || !original_tt_publish())
		{
			original_tt_done();
			continue;
		}
		UT_ASSERT(test_relmap_native_tt_lookup(100, &receipt));
		original_tt_release_check(directory, &receipt, &receipt, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		before = *directory;
		top = assigned_xid;
		cluster_node_id = 7;
		assigned_xid = InvalidFullTransactionId;
		writer_available = false;
		original_tt_release_check(directory, &receipt, &receipt, CLUSTER_RELMAP_RETENTION_RETAINED);
		query = receipt;
		query.active_lsn++;
		original_tt_release_check(directory, &receipt, &query, CLUSTER_RELMAP_RETENTION_RETAINED);
		/* These are read-only structural query inputs, not published receipts
		 * or terminal/recycle authority. Every original pin stays in place. */
		for (unsigned field = 0; field < 5; field++)
		{
			query = receipt;
			if (field == 0) query.segment_id++;
			if (field == 1) query.segment_generation++;
			if (field == 2) query.slot_offset++;
			if (field == 3) query.slot_wrap++;
			if (field == 4) query.xid++;
			original_tt_release_check(directory, &receipt, &query, CLUSTER_RELMAP_RETENTION_OK);
		}
		query = receipt;
		query.origin_instance = 2;
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[1].state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		original_tt_release_check(directory, &receipt, &query, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		cluster_node_id = 0;
		assigned_xid = top;
		writer_available = true;
		original_tt_done();
	}
}
