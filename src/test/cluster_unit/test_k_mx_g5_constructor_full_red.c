/* Hand-selected full constructor acceptance RED. The original native bodies
 * execute with the existing preflight fixture's controlled backend ERROR,
 * WAL-workspace and allocator/WAL/SLRU tail leaves. No live reservation,
 * descriptor durability, receipt publication or heap reachability is proved. */
int k_constructor_preflight_component_main(void);
#define main k_constructor_preflight_component_main
#include "test_cluster_multixact_preflight.c"
#undef main

/* Full-domain acceptance binds separately from the legacy preflight controls.
 * The currently available native entry is the initial binding. */
#define K_FULL_CURRENT_CONSTRUCTOR MultiXactIdCreateLocalCurrentMembers

/* Inputs and observed outputs remain valid across the backend ERROR longjmp. */
static MultiXactMember *full_input;
static MultiXactId full_created;
static uint32 full_nmembers;

static void
full_begin(uint32 count)
{
	reset_fixture();
	full_created = InvalidMultiXactId;
	full_nmembers = count;
	full_input = calloc(count, sizeof(*full_input));
	if (full_input == NULL)
		abort();
	for (uint32 i = 0; i < count; i++) {
		full_input[i].xid = 100 + i;
		full_input[i].status = MultiXactStatusForShare;
	}
}

static bool
full_create(void)
{
	catch_error = true;
	if (setjmp(error_boundary) == 0) {
		full_created = K_FULL_CURRENT_CONSTRUCTOR((int)full_nmembers, full_input);
		catch_error = false;
		return false;
	}
	catch_error = false;
	return true;
}

static void
full_finish(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	printf("# N=%u error=%d workspace=%d materialize=%d created=%u\n",
		full_nmembers, error_code, reserve_calls, materialize_calls, full_created);
	free(full_input);
	full_input = NULL;
}

UT_TEST(bounded_and_full_two_256_member_controls)
{
	const uint32 counts[] = {2, 256};
	two_and_256_members_reserve_before_materialization();
	for (unsigned i = 0; i < lengthof(counts); i++) {
		full_begin(counts[i]);
		UT_ASSERT(!full_create());
		UT_ASSERT_EQ(full_created, 901);
		UT_ASSERT_EQ(reserve_calls, 1);
		UT_ASSERT_EQ(materialize_calls, 1);
		full_finish();
	}
}

UT_TEST(full_257_and_65537_reach_the_current_constructor_tail)
{
	const uint32 counts[] = {257, 65537};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		full_begin(counts[i]);
		UT_ASSERT(!full_create());
		UT_ASSERT_EQ(full_created, 901);
		UT_ASSERT_EQ(reserve_calls, 1);
		UT_ASSERT_EQ(materialize_calls, 1);
		full_finish();
	}
}

UT_TEST(full_member_65536_is_validated_before_wal_workspace)
{
	for (unsigned kind = 0; kind < 2; kind++) {
		full_begin(65537);
		if (kind == 0)
			full_input[65536].xid = InvalidTransactionId;
		else
			full_input[65536].status = MaxMultiXactStatus + 1;
		UT_ASSERT(full_create());
		/* A feature-limit rejection is not evidence of tail validation. */
		UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(full_created, InvalidMultiXactId);
		UT_ASSERT_EQ(reserve_calls, 0);
		UT_ASSERT_EQ(materialize_calls, 0);
		full_finish();
	}
}

UT_TEST(full_second_updater_across_member_boundaries_is_rejected)
{
	const uint32 counts[] = {257, 65537};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		full_begin(counts[i]);
		full_input[i == 0 ? 0 : 256].status = MultiXactStatusUpdate;
		full_input[counts[i] - 1].status = MultiXactStatusNoKeyUpdate;
		UT_ASSERT(full_create());
		UT_ASSERT_EQ(error_code, ERRCODE_DATA_CORRUPTED);
		UT_ASSERT_EQ(full_created, InvalidMultiXactId);
		UT_ASSERT_EQ(reserve_calls, 0);
		UT_ASSERT_EQ(materialize_calls, 0);
		full_finish();
	}
}

UT_TEST(full_wal_workspace_failure_cannot_reach_materialization)
{
	const uint32 counts[] = {257, 65537};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		full_begin(counts[i]);
		fail_reservation = true;
		UT_ASSERT(full_create());
		UT_ASSERT_EQ(error_code, ERRCODE_OUT_OF_MEMORY);
		UT_ASSERT_EQ(full_created, InvalidMultiXactId);
		UT_ASSERT_EQ(reserve_calls, 1);
		UT_ASSERT_EQ(materialize_calls, 0);
		full_finish();
	}
}

UT_TEST(full_65537_size_and_record_edge_need_no_member_read)
{
	Size member_bytes = 99;
	uint32 record_bytes = 99;

	reset_fixture();
	UT_ASSERT(MultiXactIdCurrentMembersSize(65537, &member_bytes, &record_bytes));
	UT_ASSERT_EQ(member_bytes, 524296);
	UT_ASSERT_EQ(record_bytes, 524350);
	/* Reuse the independent literal last-fit/first-overflow record vectors. */
	native_size_checks_full_record_before_truncation_or_allocation();
	UT_ASSERT_EQ(reserve_calls, 0);
	UT_ASSERT_EQ(materialize_calls, 0);
}

UT_TEST(invalid_constructor_arguments_have_no_effect)
{
	const int counts[] = {-1, 0, 2, 257, 65537};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		reset_fixture();
		UT_ASSERT(construction_errors(counts[i], NULL));
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT_EQ(reserve_calls, 0);
		UT_ASSERT_EQ(materialize_calls, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(bounded_and_full_two_256_member_controls);
	UT_RUN(native_wal_overflow_is_refused_before_reading_members);
	UT_RUN(wal_workspace_failure_cannot_reach_materialization);
	UT_RUN(invalid_members_and_multiple_updaters_never_reserve);
	UT_RUN(full_257_and_65537_reach_the_current_constructor_tail);
	UT_RUN(full_member_65536_is_validated_before_wal_workspace);
	UT_RUN(full_second_updater_across_member_boundaries_is_rejected);
	UT_RUN(full_wal_workspace_failure_cannot_reach_materialization);
	UT_RUN(full_65537_size_and_record_edge_need_no_member_read);
	UT_RUN(invalid_constructor_arguments_have_no_effect);
	UT_DONE();
	return ut_failed_count != 0;
}
