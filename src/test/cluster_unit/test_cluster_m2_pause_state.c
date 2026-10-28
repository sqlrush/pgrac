#include "postgres.h"
#include "../modules/test_pgrac_shared_config/test_pgrac_m2_pause.h"
#undef printf
#include "unit_test.h"
#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

UT_TEST(test_default_off_and_foreign_identity_never_consume)
{
	TestM2PauseState state;
	uint64 lease = 0;
	test_m2_pause_state_init(&state);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 0, 1, 4195408, &lease), 0);
	UT_ASSERT_EQ(lease, 0);
	UT_ASSERT(test_m2_pause_arm(&state, 0, 1, 4195408, 12000000) != 0);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 1, 1, 4195408, &lease), 0);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 0, 2, 4195408, &lease), 0);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 0, 1, 4195424, &lease), 0);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 0, 1, 4195408, &lease), 12000000);
}

UT_TEST(test_exact_claim_is_one_shot_until_explicit_disarm)
{
	TestM2PauseState state;
	uint64 claimed = 0, owner;
	bool consumed = false;
	test_m2_pause_state_init(&state);
	owner = test_m2_pause_arm(&state, 0, 2, 4195408, 12000000);
	UT_ASSERT(owner != 0);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 0, 2, 4195408, &claimed), 12000000);
	UT_ASSERT_EQ(claimed, owner);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 0, 2, 4195408, &claimed), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 2, 4195408, 1), 0);
	UT_ASSERT(test_m2_pause_clear(&state, owner, &consumed));
	UT_ASSERT(consumed);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.lease), 0);
}

UT_TEST(test_exit_cleanup_token_cannot_clear_later_same_key_arm)
{
	TestM2PauseState state;
	uint64 old, current;
	bool consumed = true;
	test_m2_pause_state_init(&state);
	old = test_m2_pause_arm(&state, 0, 1, 4195408, 1);
	UT_ASSERT(old != 0);
	UT_ASSERT(test_m2_pause_clear(&state, old, &consumed));
	UT_ASSERT(!consumed);
	current = test_m2_pause_arm(&state, 0, 1, 4195408, 1);
	UT_ASSERT(current != 0 && current != old);
	UT_ASSERT(!test_m2_pause_clear(&state, old, &consumed));
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.lease), current);
	UT_ASSERT(test_m2_pause_clear(&state, current, &consumed));
}

UT_TEST(test_invalid_or_busy_arm_does_not_replace_target)
{
	TestM2PauseState state;
	uint64 lease;
	test_m2_pause_state_init(&state);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, -1, 1, 4195408, 1), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 128, 1, 4195408, 1), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 0, 4195408, 1), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 3, 4195408, 1), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 1, 0, 1), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 1, 4195408, 0), 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 1, 4195408, 60000001), 0);
	lease = test_m2_pause_arm(&state, 127, 2, 4195408, 60000000);
	UT_ASSERT(lease != 0);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 1, 4195424, 1), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.lease), lease);
	UT_ASSERT_EQ(test_m2_pause_claim(&state, 127, 2, 4195408, &lease), 60000000);
}

UT_TEST(test_generation_exhaustion_never_reuses_an_exit_token)
{
	TestM2PauseState state;
	test_m2_pause_state_init(&state);
	pg_atomic_write_u64(&state.generation, UINT64_MAX >> 2);
	UT_ASSERT_EQ(test_m2_pause_arm(&state, 0, 1, 4195408, 1), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.lease), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.generation), UINT64_MAX >> 2);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_default_off_and_foreign_identity_never_consume);
	UT_RUN(test_exact_claim_is_one_shot_until_explicit_disarm);
	UT_RUN(test_exit_cleanup_token_cannot_clear_later_same_key_arm);
	UT_RUN(test_invalid_or_busy_arm_does_not_replace_target);
	UT_RUN(test_generation_exhaustion_never_reuses_an_exit_token);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
#else
int
main(void)
{
	printf("1..0 # SKIP test-module pause requires a cluster cassert build\n");
	return 0;
}
#endif
