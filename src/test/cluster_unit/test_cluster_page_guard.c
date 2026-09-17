/*-------------------------------------------------------------------------
 *
 * test_cluster_page_guard.c
 *    STOP-06 no-wait PAGE target protection.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_guard.h")
#include "cluster/cluster_page_guard.h"
#define TEST_HAVE_CLUSTER_PAGE_GUARD 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#ifndef TEST_HAVE_CLUSTER_PAGE_GUARD

UT_TEST(test_page_guard_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-7-PAGE-GUARD-NOWAIT\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_page_guard_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

static RfPageIdentityV1
identity(uint32 blockno)
{
	RfPageIdentityV1 value;

	memset(&value, 0, sizeof(value));
	value.system_identifier = 99;
	memset(value.storage_uuid, 3, sizeof(value.storage_uuid));
	value.locator.spcOid = 1;
	value.locator.dbOid = 2;
	value.locator.relNumber = 3;
	value.forknum = MAIN_FORKNUM;
	value.blockno = blockno;
	return value;
}

UT_TEST(test_invalid_identity_never_preflights)
{
	RfPageIdentityV1 page = identity(1);
	RfPageGuardPreflightV1 preflight;

	page.system_identifier = 0;
	memset(&preflight, 0x5a, sizeof(preflight));
	UT_ASSERT(!rf_page_guard_preflight_v1(&page, &preflight));
	UT_ASSERT_EQ(preflight.state, RF_PAGE_GUARD_EMPTY);
}

UT_TEST(test_same_identity_maps_to_same_partition)
{
	RfPageIdentityV1 page = identity(7);
	RfPageGuardPreflightV1 first;
	RfPageGuardPreflightV1 second;

	UT_ASSERT(rf_page_guard_preflight_v1(&page, &first));
	UT_ASSERT(rf_page_guard_preflight_v1(&page, &second));
	UT_ASSERT_EQ(first.partition, second.partition);
	UT_ASSERT(first.partition < RF_PAGE_GUARD_PARTITIONS);
}

UT_TEST(test_promote_is_conditional_and_exact)
{
	RfPageIdentityV1 page = identity(9);
	RfPageIdentityV1 other = identity(10);
	RfPageGuardPreflightV1 preflight;
	RfPageGuardV1 guard;

	UT_ASSERT(rf_page_guard_preflight_v1(&page, &preflight));
	memset(&guard, 0, sizeof(guard));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &guard));
	UT_ASSERT(rf_page_guard_revalidate_nowait_v1(&guard, &page));
	UT_ASSERT(!rf_page_guard_revalidate_nowait_v1(&guard, &other));
	UT_ASSERT(!rf_page_guard_promote_nowait_v1(&preflight, &guard));
	rf_page_guard_release_v1(&guard);
}

UT_TEST(test_partition_conflict_fails_without_wait)
{
	RfPageIdentityV1 first_page = identity(20);
	RfPageIdentityV1 second_page = identity(21);
	RfPageGuardPreflightV1 first;
	RfPageGuardPreflightV1 second;
	RfPageGuardV1 first_guard;
	RfPageGuardV1 second_guard;
	uint32 blockno;

	UT_ASSERT(rf_page_guard_preflight_v1(&first_page, &first));
	for (blockno = 21; blockno < 100000; blockno++) {
		second_page.blockno = blockno;
		UT_ASSERT(rf_page_guard_preflight_v1(&second_page, &second));
		if (second.partition == first.partition)
			break;
	}
	UT_ASSERT(blockno < 100000);
	memset(&first_guard, 0, sizeof(first_guard));
	memset(&second_guard, 0, sizeof(second_guard));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&first, &first_guard));
	UT_ASSERT(!rf_page_guard_promote_nowait_v1(&second, &second_guard));
	rf_page_guard_release_v1(&first_guard);
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&second, &second_guard));
	rf_page_guard_release_v1(&second_guard);
}

UT_TEST(test_preflight_identity_drift_is_rejected)
{
	RfPageIdentityV1 page = identity(30);
	RfPageGuardPreflightV1 preflight;
	RfPageGuardV1 guard;

	UT_ASSERT(rf_page_guard_preflight_v1(&page, &preflight));
	preflight.page_identity.blockno++;
	memset(&guard, 0, sizeof(guard));
	UT_ASSERT(!rf_page_guard_promote_nowait_v1(&preflight, &guard));
}

UT_TEST(test_release_is_null_and_double_idempotent)
{
	RfPageIdentityV1 page = identity(40);
	RfPageGuardPreflightV1 preflight;
	RfPageGuardV1 guard;

	UT_ASSERT(rf_page_guard_preflight_v1(&page, &preflight));
	memset(&guard, 0, sizeof(guard));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &guard));
	rf_page_guard_release_v1(&guard);
	rf_page_guard_release_v1(&guard);
	rf_page_guard_release_v1(NULL);
	UT_ASSERT_EQ(guard.state, RF_PAGE_GUARD_RELEASED);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_invalid_identity_never_preflights);
	UT_RUN(test_same_identity_maps_to_same_partition);
	UT_RUN(test_promote_is_conditional_and_exact);
	UT_RUN(test_partition_conflict_fails_without_wait);
	UT_RUN(test_preflight_identity_drift_is_rejected);
	UT_RUN(test_release_is_null_and_double_idempotent);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
