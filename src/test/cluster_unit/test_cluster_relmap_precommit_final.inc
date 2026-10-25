/* Original queued precommit must retain its batch after late source loss.
 * Scheduling, writer failure and native transaction/WAL leaves are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */

static unsigned final_prepare_count, final_owner_reads;
static ClusterRelmapRetentionDirectory *final_directory;
static ClusterRelmapRetentionDirectory final_directory_before;
static RelMapFile final_queues[4];
static ClusterRelmapProducerArmV2 final_receipts[2];
static ClusterWalSourceRef final_writer_ref;
static uint64 final_top_xid;

static void
final_sync_drift(void)
{
	if (sync_commits != final_prepare_count)
		return;
	/* Model scheduling after the last preparation's nofail sync request,
	 * not a native ForceSyncCommit implementation that changes the source. */
	final_directory_before = *final_directory;
	final_writer_ref = writer_ref;
	final_top_xid = U64FromFullTransactionId(assigned_xid);
	precommit_save_queues(final_queues);
	change_original_owner();
}

static void
final_writer_error(void)
{
	if (sync_commits != final_prepare_count)
		return;
	UT_ASSERT(!local_held && !v2_xidgen_held && !v2_retention_held);
	final_owner_reads++;
	final_directory_before = *final_directory;
	memcpy(final_receipts, cluster_relmap_v2_arms, sizeof(final_receipts));
	precommit_save_queues(final_queues);
	ereport(ERROR, (errmsg("fixture final original writer recheck failed")));
}

static void
final_batch_keeps_protection(unsigned mask)
{
	precommit_same_queues(final_queues);
	UT_ASSERT(memcmp(final_directory, &final_directory_before,
		sizeof(final_directory_before)) == 0);
	UT_ASSERT_EQ(cluster_relmap_v2_arms[0].armed, (mask & 1) != 0);
	UT_ASSERT_EQ(cluster_relmap_v2_arms[1].armed, (mask & 2) != 0);
	UT_ASSERT(!cluster_relmap_v2_prepare_unfinished && !cluster_relmap_v2_finish_entered);
	UT_ASSERT_EQ(inserts, final_prepare_count);
	UT_ASSERT_EQ(flushes, final_prepare_count);
	UT_ASSERT_EQ(sync_commits, final_prepare_count);
	UT_ASSERT_EQ(terminal_calls + unlock_calls + publish_events, 0);
	UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1] + ack_attempts[0] + ack_attempts[1], 0);
	UT_ASSERT(!local_held && !v2_xidgen_held && !v2_retention_held);
}

UT_TEST(v2_precommit_final_owner_drift_keeps_complete_batch)
{
	for (unsigned which = 0; which < 48; which++)
	{
		unsigned mask = which % 3 + 1;

		final_directory = precommit_fixture(which >= 24, mask, false);
		final_prepare_count = mask == 3 ? 2 : 1;
		drift_kind = (which / 3) % 8;
		v2_final_sync_observer = final_sync_drift;
		UT_ASSERT(precommit_error(false));
		v2_final_sync_observer = NULL;
		final_batch_keeps_protection(mask);
		for (unsigned slot = 0; slot < 2; slot++)
		{
			const ClusterRelmapProducerArmV2 *arm = &cluster_relmap_v2_arms[slot];

			if (!arm->armed)
				continue;
			UT_ASSERT_EQ(arm->sample.owner.top_full_xid, final_top_xid);
			UT_ASSERT(memcmp(&arm->sample.writer.ref, &final_writer_ref,
				sizeof(final_writer_ref)) == 0);
		}
	}
	precommit_reset_queues();
	abort_test_reset();
}

UT_TEST(v2_precommit_final_owner_error_keeps_complete_batch)
{
	for (unsigned which = 0; which < 6; which++)
	{
		unsigned mask = which % 3 + 1;

		final_directory = precommit_fixture(which >= 3, mask, false);
		final_prepare_count = mask == 3 ? 2 : 1;
		final_owner_reads = 0;
		/* Parent never visits this source again after the last prepare. */
		final_directory_before = *final_directory;
		precommit_save_queues(final_queues);
		v2_final_writer_observer = final_writer_error;
		UT_ASSERT(precommit_error(false));
		v2_final_writer_observer = NULL;
		UT_ASSERT_EQ(final_owner_reads, 1);
		if (final_owner_reads != 0)
		{
			final_batch_keeps_protection(mask);
			UT_ASSERT(memcmp(final_receipts, cluster_relmap_v2_arms,
				sizeof(final_receipts)) == 0);
			UT_ASSERT(authority_held);
		}
	}
	precommit_reset_queues();
	abort_test_reset();
}
