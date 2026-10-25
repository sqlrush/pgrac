/*-------------------------------------------------------------------------
 * test_cluster_relmap_postcommit.inc
 *    Original dormant publication, invalidation and postcommit failure order.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_postcommit.inc
 *
 * NOTES
 *    Physical I/O, native transaction outcome and notification transport are
 *    controlled. Simulated PANIC is a test boundary, not native crash recovery.
 *-------------------------------------------------------------------------
 */
enum PostcommitFault
{
	POSTCOMMIT_OK, POSTCOMMIT_SEND_ERROR, POSTCOMMIT_ACK_ERROR,
	POSTCOMMIT_ACK_EXHAUSTED, POSTCOMMIT_SEND_DRIFT, POSTCOMMIT_ACK_DRIFT,
	POSTCOMMIT_UNLOCK_ERROR
};
static enum PostcommitFault postcommit_fault;
static unsigned postcommit_mask, postcommit_failed_slot, postcommit_reports;

int errdetail_internal(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errhidecontext(bool hide pg_attribute_unused()) { return 0; }
int errhidestmt(bool hide pg_attribute_unused()) { return 0; }

void
EmitErrorReport(void)
{
	UT_ASSERT(v2_postcommit_error_stack && error_context_stack == NULL);
	UT_ASSERT(CurrentMemoryContext == ErrorContext);
	postcommit_reports++;
}

static void
postcommit_custody_before_notification(void)
{
	UT_ASSERT(authority_held && !local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(cluster_relmap_v2_finish_entered);
	UT_ASSERT_EQ(terminal_calls, (postcommit_mask == 3 ? 2 : 1));
	for (unsigned slot = 0; slot < 2; slot++)
	{
		UT_ASSERT_EQ(cluster_relmap_v2_arms[slot].armed, (postcommit_mask & (1U << slot)) != 0);
		if (postcommit_mask & (1U << slot))
			UT_ASSERT(terminal_disk_cleared[slot]);
	}
}

static void
postcommit_send(const SharedInvalidationMessage *msgs, int n)
{
	unsigned slot = msgs[0].rm.dbId == InvalidOid ? 0 : 1;

	postcommit_custody_before_notification();
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, terminal_images[slot].identity.dbid);
	local_invalidations[slot]++;
	record_publish_event(slot == 0 ? INVALIDATE_SHARED : INVALIDATE_DATABASE);
	if (slot == postcommit_failed_slot)
	{
		if (postcommit_fault == POSTCOMMIT_SEND_ERROR)
			ereport(ERROR, (errmsg("controlled postcommit local invalidation error")));
		if (postcommit_fault == POSTCOMMIT_SEND_DRIFT)
			change_original_owner();
	}
}

static ClusterSinvalAckResult
postcommit_ack(const SharedInvalidationMessage *msgs, int n)
{
	unsigned slot = msgs[0].rm.dbId == InvalidOid ? 0 : 1;

	postcommit_custody_before_notification();
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, terminal_images[slot].identity.dbid);
	UT_ASSERT_EQ(local_invalidations[slot], 1);
	ack_attempts[slot]++;
	if (slot == postcommit_failed_slot)
	{
		if (postcommit_fault == POSTCOMMIT_ACK_ERROR)
			ereport(ERROR, (errmsg("controlled postcommit peer ACK error")));
		if (postcommit_fault == POSTCOMMIT_ACK_EXHAUSTED)
			return ack_attempts[slot] % 2 ? CLUSTER_SINVAL_ACK_TIMEOUT : CLUSTER_SINVAL_ACK_ENQUEUE_FAILED;
		if (postcommit_fault == POSTCOMMIT_ACK_DRIFT)
			change_original_owner();
	}
	if (ack_attempts[slot] <= ack_failures[slot])
		return ack_attempts[slot] % 2 ? CLUSTER_SINVAL_ACK_TIMEOUT : CLUSTER_SINVAL_ACK_ENQUEUE_FAILED;
	record_publish_event(slot == 0 ? ACK_SHARED : ACK_DATABASE);
	return CLUSTER_SINVAL_ACK_DONE;
}

static void
postcommit_unlock(void)
{
	postcommit_custody_before_notification();
	for (unsigned slot = 0; slot < 2; slot++)
	{
		if (!(postcommit_mask & (1U << slot)))
			continue;
		UT_ASSERT_EQ(local_invalidations[slot], 1);
		if (ClusterConfShmem)
			UT_ASSERT_EQ(ack_attempts[slot], ack_failures[slot] + 1);
	}
	unlock_calls++;
	record_publish_event(UNLOCK);
	if (postcommit_fault == POSTCOMMIT_UNLOCK_ERROR)
		ereport(ERROR, (errmsg("controlled original relmap unlock error")));
	authority_held = false;
}

static void
postcommit_test_reset(void)
{
	terminal_test_reset();
	v2_send_observer = NULL;
	v2_ack_observer = NULL;
	v2_unlock_observer = NULL;
	v2_postcommit_error_stack = false;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
}

static ClusterRelmapRetentionDirectory *
postcommit_fixture(bool next_era, bool peers)
{
	ClusterRelmapRetentionDirectory *directory;

	postcommit_test_reset();
	directory = terminal_fixture(next_era);
	ClusterConfShmem = peers ? &peer_conf : NULL;
	postcommit_fault = POSTCOMMIT_OK;
	postcommit_mask = 3;
	postcommit_failed_slot = postcommit_reports = 0;
	terminal_publish = true;
	v2_send_observer = postcommit_send;
	v2_ack_observer = postcommit_ack;
	v2_unlock_observer = postcommit_unlock;
	v2_postcommit_error_stack = true;
	return directory;
}

static bool
postcommit_panic(void)
{
	bool caught = false;

	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		cluster_relmap_producer_postcommit_v2();
	else
	{
		caught = true;
		UT_ASSERT_EQ(error_level, PANIC);
	}
	error_expected = false;
	/* A real PANIC exits; only the fixture tears down its exception frames. */
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	return caught;
}

UT_TEST(v2_postcommit_publishes_then_invalidates_before_unlock)
{
	for (unsigned which = 0; which < 12; which++)
	{
		ClusterRelmapRetentionDirectory *directory = postcommit_fixture((which / 3) % 2 != 0, which >= 6);
		ClusterRelmapRetentionPin before[3];
		enum PublishEvent expected[5];
		unsigned count = 0;
		bool armed;

		postcommit_mask = which % 3 + 1;
		if (postcommit_mask == 3)
			armed = terminal_arm_both((which & 1) != 0);
		else
			armed = cluster_relmap_producer_arm_v2(postcommit_mask == 1, &updates);
		UT_ASSERT(armed);
		if (!armed)
			continue;
		memcpy(before, directory->pins, sizeof(before));
		v2_terminal_context = true;
		cluster_relmap_producer_postcommit_v2();
		for (unsigned slot = 0; slot < 2; slot++)
		{
			if (!(postcommit_mask & (1U << slot)))
				continue;
			expected[count++] = slot == 0 ? INVALIDATE_SHARED : INVALIDATE_DATABASE;
			if (which >= 6)
				expected[count++] = slot == 0 ? ACK_SHARED : ACK_DATABASE;
		}
		expected[count++] = UNLOCK;
		check_publish_trace(expected, count);
		UT_ASSERT_EQ(terminal_calls, postcommit_mask == 3 ? 2 : 1);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(!authority_held && !cluster_relmap_v2_finish_entered);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(memcmp(before, directory->pins, sizeof(before)) == 0);
		UT_ASSERT_EQ(full_requests, postcommit_mask == 3 ? 2 : 1);
		UT_ASSERT_EQ(postcommit_reports, 0);
		cluster_relmap_producer_postcommit_v2();
		UT_ASSERT_EQ(unlock_calls, 1);
	}
	postcommit_test_reset();
}

UT_TEST(v2_postcommit_ack_retries_keep_storage_one_shot)
{
	for (unsigned slot = 0; slot < 2; slot++)
	{
		ClusterRelmapRetentionDirectory *directory = postcommit_fixture(false, true);
		ClusterRelmapRetentionPin before[3];

		if (!terminal_arm_both(slot == 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		ack_failures[slot] = 3;
		v2_terminal_context = true;
		cluster_relmap_producer_postcommit_v2();
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT_EQ(ack_attempts[slot], 4);
		UT_ASSERT_EQ(ack_attempts[!slot], 1);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 2);
		UT_ASSERT_EQ(unlock_calls, 1);
		UT_ASSERT(memcmp(before, directory->pins, sizeof(before)) == 0);
		UT_ASSERT(!cluster_relmap_v2_finish_entered && !authority_held);
	}
	postcommit_test_reset();
}

UT_TEST(v2_postcommit_errors_and_source_loss_fail_stop)
{
	for (unsigned which = 0; which < 11; which++)
	{
		ClusterRelmapRetentionDirectory *directory = postcommit_fixture(false, true);
		ClusterRelmapRetentionPin before[3];
		unsigned calls, sends, acks, unlocks;
		bool panicked;

		if (!terminal_arm_both((which & 1) != 0))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		postcommit_fault = (enum PostcommitFault) (which / 2 + 1);
		postcommit_failed_slot = which % 2;
		v2_terminal_context = true;
		panicked = postcommit_panic();
		UT_ASSERT(panicked);
		if (!panicked)
			continue;
		UT_ASSERT_EQ(terminal_calls, 2);
		UT_ASSERT(terminal_disk_cleared[0] && terminal_disk_cleared[1]);
		UT_ASSERT(cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(authority_held && !local_held && !v2_retention_held);
		UT_ASSERT(memcmp(before, directory->pins, sizeof(before)) == 0);
		UT_ASSERT_EQ(postcommit_reports, postcommit_fault == POSTCOMMIT_SEND_ERROR
			|| postcommit_fault == POSTCOMMIT_ACK_ERROR || postcommit_fault == POSTCOMMIT_UNLOCK_ERROR);
		if (postcommit_fault == POSTCOMMIT_ACK_EXHAUSTED)
			UT_ASSERT_EQ(ack_attempts[postcommit_failed_slot], CLUSTER_RELMAP_PUBLISH_RETRIES);
		UT_ASSERT_EQ(unlock_calls, postcommit_fault == POSTCOMMIT_UNLOCK_ERROR ? 1 : 0);
		calls = terminal_calls;
		sends = local_invalidations[0] + local_invalidations[1];
		acks = ack_attempts[0] + ack_attempts[1];
		unlocks = unlock_calls;
		UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
		UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &updates));
		UT_ASSERT(postcommit_panic());
		UT_ASSERT_EQ(terminal_calls, calls);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], sends);
		UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], acks);
		UT_ASSERT_EQ(unlock_calls, unlocks);
	}
	postcommit_test_reset();
}

UT_TEST(v2_postcommit_preflight_storage_failures_and_empty_isolation)
{
	for (unsigned which = 0; which < 8; which++)
	{
		ClusterRelmapRetentionDirectory *directory = postcommit_fixture(false, true);
		ClusterRelmapRetentionPin before[3];

		if (!terminal_arm_both(true))
			continue;
		memcpy(before, directory->pins, sizeof(before));
		v2_terminal_context = which != 0;
		if (which == 1)
			change_original_owner();
		if (which == 2)
			cluster_relmap_v2_finish_entered = true;
		if (which == 3)
			cluster_relmap_publish_arms[0].armed = true;
		if (which >= 4)
		{
			terminal_fault = which < 6 ? 1 : 2;
			terminal_fail_at = which % 2 + 1;
		}
		UT_ASSERT(postcommit_panic());
		UT_ASSERT_EQ(terminal_calls, which >= 4 ? terminal_fail_at : 0);
		UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 0);
		UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1] + unlock_calls, 0);
		UT_ASSERT_EQ(postcommit_reports, which >= 6 ? 1 : 0);
		UT_ASSERT(cluster_relmap_v2_finish_entered);
		UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(authority_held && memcmp(before, directory->pins, sizeof(before)) == 0);
	}
	postcommit_fixture(false, true);
	cluster_relmap_publish_arms[0].armed = true;
	authority_pending[0] = 19;
	cluster_relmap_producer_postcommit_v2();
	UT_ASSERT_EQ(terminal_calls + publish_events + unlock_calls, 0);
	UT_ASSERT_EQ(authority_pending[0], 19);
	UT_ASSERT(cluster_relmap_publish_arms[0].armed && authority_held);
	UT_ASSERT(!cluster_relmap_v2_finish_entered);
	postcommit_test_reset();
}
