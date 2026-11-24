/* Actual GCS/PCM admission; inspect only the bounded diagnostic copy. */
#include "test_cluster_resource_x_acquire_fixture.inc"

static void
stale_reset(void)
{
	acquire_reset();
	memset(&gcs_acquire_stale_trace, 0, sizeof(gcs_acquire_stale_trace));
	cluster_trace_tx_identity = true;
}

static ResourceXDecodedFrame
stale_frame(void)
{
	ResourceXDecodedFrame frame = acquire_frame(1, RESOURCE_X_ACQUIRE_FIRST_FILL, PCM_STATE_N, PCM_STATE_S);
	frame.common.logical_assertion.resource.spcOid = 1664;
	frame.common.logical_assertion.resource.dbOid = 0;
	frame.common.logical_assertion.resource.relNumber = 1260;
	frame.common.logical_assertion.resource.forkNum = MAIN_FORKNUM;
	frame.common.logical_assertion.resource.blockNum = 0;
	return frame;
}

static void
stale_pair(const char *name, uint64 left, uint64 right)
{
	for (uint32 i = 0; i < gcs_acquire_stale_trace.count; i++)
		if (strcmp(gcs_acquire_stale_trace.fields[i].name, name) == 0) {
			UT_ASSERT_EQ(gcs_acquire_stale_trace.fields[i].left, left);
			UT_ASSERT_EQ(gcs_acquire_stale_trace.fields[i].right, right);
			return;
		}
	UT_ASSERT(false);
}

UT_TEST(test_stale_off_preserves_original_refusal)
{
	ResourceXDecodedFrame f;
	stale_reset(); cluster_trace_tx_identity = false; f = stale_frame();
	f.body.acquire_intent.r4_generation++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(!gcs_acquire_stale_trace.logged);
	UT_ASSERT_EQ(gcs_acquire_stale_trace.count, 0);
}
UT_TEST(test_stale_scope_is_only_first_authid_block)
{
	for (int v = 0; v < 4; v++) {
		ResourceXDecodedFrame f;
		stale_reset(); f = stale_frame();
		if (v == 0) f.common.logical_assertion.resource.blockNum++;
		if (v == 1) f.common.logical_assertion.resource.relNumber++;
		if (v == 2) f.common.logical_assertion.resource.dbOid++;
		if (v == 3) f.common.logical_assertion.resource.spcOid++;
		f.body.acquire_intent.r4_generation++;
		UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(!gcs_acquire_stale_trace.logged);
	}
}
UT_TEST(test_stale_reports_original_r4_comparison)
{
	ResourceXDecodedFrame f;
	stale_reset(); f = stale_frame(); f.body.acquire_intent.r4_generation++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(gcs_acquire_stale_trace.logged);
	UT_ASSERT(!gcs_acquire_stale_trace.active);
	UT_ASSERT(gcs_acquire_stale_trace.reason != NULL && strcmp(gcs_acquire_stale_trace.reason, "intent.r4") == 0);
	stale_pair("intent.r4", 78, 77);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}
UT_TEST(test_stale_reports_original_requester_incarnation)
{
	ResourceXDecodedFrame f;
	stale_reset(); f = stale_frame(); auth_peer_boot++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(gcs_acquire_stale_trace.reason != NULL && strcmp(gcs_acquire_stale_trace.reason, "requester.incarnation") == 0);
	stale_pair("requester.incarnation", 101, 102);
}
UT_TEST(test_stale_reports_original_master_session)
{
	ResourceXDecodedFrame f;
	stale_reset(); f = stale_frame(); f.common.master_session_incarnation++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(gcs_acquire_stale_trace.reason != NULL && strcmp(gcs_acquire_stale_trace.reason, "master.session") == 0);
	stale_pair("master.session", 32, 31);
}
UT_TEST(test_stale_entry_rejection_retains_locked_original_state)
{
	ResourceXDecodedFrame f;
	stale_reset(); f = stale_frame();
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(!gcs_acquire_stale_trace.logged);
	f.body.acquire_intent.reservation_token++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(gcs_acquire_stale_trace.reason != NULL && strcmp(gcs_acquire_stale_trace.reason, "entry.priority_attempt") == 0);
	stale_pair("entry.priority_attempt", f.common.assertion_sequence, f.common.assertion_sequence);
	UT_ASSERT(gcs_acquire_stale_trace.entry_known);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT_EQ(master_durable_admission_depth, 0);
}
UT_TEST(test_stale_once_never_changes_later_decisions)
{
	ResourceXDecodedFrame f; uint32 count;
	stale_reset(); f = stale_frame(); f.body.acquire_intent.r4_generation++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	count = gcs_acquire_stale_trace.count;
	f.body.acquire_intent.r4_generation++;
	UT_ASSERT_EQ(acquire_deliver(&f), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(gcs_acquire_stale_trace.count, count);
	stale_pair("intent.r4", 78, 77);
}
UT_TEST(test_stale_capacity_is_bounded_and_does_not_replace_first_cause)
{
	ResourceXDecodedFrame f;
	stale_reset(); f = stale_frame(); gcs_block_resource_x_acquire_stale_begin(&f, 61);
	for (int i = 0; i < 200; i++)
		cluster_gcs_block_acquire_stale_observe("bounded", i, i + 1, i == 0);
	UT_ASSERT_EQ(gcs_acquire_stale_trace.count, GCS_ACQUIRE_STALE_FIELDS);
	UT_ASSERT(gcs_acquire_stale_trace.omitted > 0);
	UT_ASSERT(gcs_acquire_stale_trace.reason != NULL && strcmp(gcs_acquire_stale_trace.reason, "bounded") == 0);
	gcs_acquire_stale_trace.active = false;
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_stale_off_preserves_original_refusal);
	UT_RUN(test_stale_scope_is_only_first_authid_block);
	UT_RUN(test_stale_reports_original_r4_comparison);
	UT_RUN(test_stale_reports_original_requester_incarnation);
	UT_RUN(test_stale_reports_original_master_session);
	UT_RUN(test_stale_entry_rejection_retains_locked_original_state);
	UT_RUN(test_stale_once_never_changes_later_decisions);
	UT_RUN(test_stale_capacity_is_bounded_and_does_not_replace_first_cause);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
