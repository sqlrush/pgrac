/* Capacity CV -> original preflight/BIND -> native transaction dispatch.
 * Scheduling, admission, WAL storage and CLOG remain explicit fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#define UNDO_CAPACITY_BIND_FIXTURE_MAIN capacity_xact_bind_fixture_main
#define EMPTY_UNDO_XACT_FIXTURE_MAIN capacity_xact_native_fixture_main
#define EMPTY_UNDO_XACT_PREFLIGHT_SOURCE "test_cluster_undo_capacity_bind.c"
#include "test_cluster_empty_undo_xact.c"

static void
capacity_xact_reset(bool rowmark, bool active)
{
	native_reset(rowmark);
	bind_reset(rowmark, active);
	cluster_shared_config = true;
	g_after_bind_emit_hook = native_bind_emitted;
}

static bool
capacity_xact_preflight(ClusterCanonicalTxnBinding *binding)
{
	if (!empty_preflight(binding))
		return false;
	empty_assert_active(binding);
	UT_ASSERT_EQ(binding->segment_id, bind_same_segment ? 1 : 2);
	UT_ASSERT_EQ(binding->slot_offset, 0);
	UT_ASSERT_EQ(slot_wait_calls, bind_same_segment ? 1 : 0);
	bind_assert_counts(3, 3, 3, 0, 0);
	UT_ASSERT_EQ(XactLastRecEnd, g_bind_emit_lsn);
	native_binding = *binding;
	return true;
}

UT_TEST(test_capacity_wait_native_completion_keeps_one_binding_and_original_counts)
{
	for (unsigned active = 0; active < 2; active++)
		for (unsigned rowmark = 0; rowmark < 2; rowmark++)
			for (unsigned finish = 0; finish < 3; finish++) {
				ClusterCanonicalTxnBinding binding, repeated;
				bool commit = finish != 2;
				void *bindings;

				capacity_xact_reset(rowmark != 0, active != 0);
				synchronous_commit = finish == 0 ? SYNCHRONOUS_COMMIT_OFF : SYNCHRONOUS_COMMIT_ON;
				if (!capacity_xact_preflight(&binding))
					return;
				bindings = cluster_tt_local_bindings;
				if (!empty_preflight(&repeated))
					return;
				UT_ASSERT_EQ(memcmp(&binding, &repeated, sizeof(binding)), 0);
				if (!native_finish_transaction(commit))
					return;
				empty_assert_finished(&binding, commit);
				UT_ASSERT_EQ(native_commits, commit);
				UT_ASSERT_EQ(native_aborts, !commit);
				UT_ASSERT_EQ(native_clogs, 1);
				UT_ASSERT_EQ(native_flushes, commit);
				UT_ASSERT_EQ(native_undo_flushes, commit);
				UT_ASSERT_EQ(native_sync_waits, commit);
				UT_ASSERT_EQ(XactLastCommitEnd, commit ? native_commit_lsn : 0);
				UT_ASSERT_EQ(g_bind_emit_calls, 1);
				UT_ASSERT_EQ(g_abort_exact_emit_calls, !commit);
				UT_ASSERT(!g_write_before_flush);
				bind_assert_counts(3, 3, 3, 0, 0);
				if (cluster_tt_local_bindings != bindings)
					free(bindings);
			}
}

UT_TEST(test_capacity_wait_native_precommit_error_preserves_binding_until_abort)
{
	static const int errors[] = { ERRCODE_IO_ERROR, ERRCODE_QUERY_CANCELED };

	for (unsigned active = 0; active < 2; active++)
		for (unsigned rowmark = 0; rowmark < 2; rowmark++)
			for (unsigned fault = 0; fault < lengthof(errors); fault++) {
				ClusterCanonicalTxnBinding binding, observed;
				ClusterSpaceDropState *drop = NULL;
				volatile bool caught = false;
				void *bindings;

				capacity_xact_reset(rowmark != 0, active != 0);
				if (!capacity_xact_preflight(&binding))
					return;
				bindings = cluster_tt_local_bindings;
				native_flush_error = errors[fault];
				PG_TRY();
				{
					(void)RecordTransactionCommit(&drop);
				}
				PG_CATCH();
				{
					caught = true;
				}
				PG_END_TRY();
				UT_ASSERT(caught);
				UT_ASSERT_EQ(last_ereport_errcode, errors[fault]);
				UT_ASSERT_EQ(native_pending_aborts, 1);
				UT_ASSERT_EQ(native_commits + native_clogs + native_flushes, 0);
				UT_ASSERT(cluster_tt_local_get_published_binding(100, &observed));
				UT_ASSERT_EQ(memcmp(&binding, &observed, sizeof(binding)), 0);
				empty_assert_active(&binding);
				/* This error occurs after the capacity wait has ended. */
				bind_assert_counts(3, 3, 3, 0, 0);
				if (!native_finish_transaction(false))
					return;
				empty_assert_finished(&binding, false);
				UT_ASSERT_EQ(native_aborts, 1);
				UT_ASSERT_EQ(native_clogs, 1);
				UT_ASSERT_EQ(g_bind_emit_calls, 1);
				UT_ASSERT_EQ(g_abort_exact_emit_calls, 1);
				UT_ASSERT(!g_write_before_flush);
				bind_assert_counts(3, 3, 3, 0, 0);
				if (cluster_tt_local_bindings != bindings)
					free(bindings);
			}
}

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(test_capacity_wait_native_completion_keeps_one_binding_and_original_counts);
	UT_RUN(test_capacity_wait_native_precommit_error_preserves_binding_until_abort);
	UT_DONE();
	return ut_failed_count != 0;
}
