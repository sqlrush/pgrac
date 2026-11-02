/* Original empty publisher and IOS with the assigned product catalog floor.
 * Storage/transport and snapshot leaves remain controlled by the base fixture.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define FIXTURE_ASSIGNED_CATALOG_ONLY 1
#define VM_QUALIFIED_IOS_MAIN k_vm_qualified_catalog_inherited_main
int k_vm_qualified_catalog_inherited_main(void);
#include "test_cluster_vm_qualified_ios.c"

UT_TEST(assigned_floor_keeps_the_current_catalog_unqualified)
{
	Buffer buffer = 2;

	ios_reset(true);
	control.catalog_version_no = CATALOG_VERSION_NO;
	UT_ASSERT(PGRAC_VM_QUALIFIED_CATALOG_VERSION > CATALOG_VERSION_NO);
	UT_ASSERT(!ios_run(&buffer, false));
	UT_ASSERT_EQ(ios_fetches, 1);
	UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters, 0);
	UT_ASSERT_EQ(buffer, InvalidBuffer);
}

UT_TEST(assigned_floor_is_loaded_from_control_not_the_test_catalog)
{
	Buffer buffer = InvalidBuffer;

	ios_reset(true);
	UT_ASSERT(PGRAC_VM_QUALIFIED_CATALOG_VERSION != 0);
	UT_ASSERT_EQ(control.catalog_version_no, PGRAC_VM_QUALIFIED_CATALOG_VERSION);
	UT_ASSERT(ios_run(&buffer, false));
	UT_ASSERT_EQ(ios_fetches, 0);
	UT_ASSERT_EQ(ios_grants, 1);
	UT_ASSERT_EQ(ios_copies, 1);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(assigned_floor_keeps_the_current_catalog_unqualified);
	UT_RUN(assigned_floor_is_loaded_from_control_not_the_test_catalog);
	UT_RUN(empty_native_publication_reaches_ios_without_heap_fetch);
	UT_RUN(old_or_unknown_catalog_does_not_consult_a_positive_vm);
	UT_RUN(disk_positive_current_clear_preserves_exact_heap_visibility);
	UT_RUN(reused_vm_pin_must_obtain_current_s_for_every_tid);
	UT_RUN(space_membership_snapshot_cid_or_target_change_cannot_grant);
	UT_RUN(ios_fallback_unlock_error_does_not_unlock_twice);
	UT_DONE();
	return ut_failed_count != 0;
}
