/*-------------------------------------------------------------------------
 * test_cluster_control_debt_readback.c
 *     Independent whole transition readback, without publication or transfer.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static void readback_late_hook(void);
#define CONTROL_DEBT_RESTART_READ_HOOK readback_late_hook
#define CONTROL_DEBT_PUBLISH_TEST_MAIN debt_readback_old_publisher_main
int debt_readback_old_publisher_main(void);
#define cluster_control_root_debt_transition_readback_v2 debt_readback_fixture_v2
#include "test_cluster_control_debt_publish.c"
#undef cluster_control_root_debt_transition_readback_v2
#undef CONTROL_DEBT_PUBLISH_TEST_MAIN

static unsigned readback_late_kind;
static void readback_late_hook(void)
{
	unsigned fault = readback_late_kind;
	readback_late_kind = 0;
	if (fault == 1) CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
	else if (fault == 2) restart_cf_sequence++;
	else if (fault == 3) cf_held = false;
}

static bool readback(ClusterControlDebtTransitionV2 *t, Size budget)
{
	return debt_readback_fixture_v2(t, store_global, budget);
}

UT_TEST(readback_requires_real_published_whole_pair_not_candidate)
{
	ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
	UT_ASSERT(t != NULL);
	UT_ASSERT(!readback(t, RESTART_BUDGET));
	UT_ASSERT(publisher_run(t));
	reset_faults();
	UT_ASSERT(readback(t, RESTART_BUDGET));
	UT_ASSERT(reads > 0 && writes == 0 && syncs == 0);
	UT_ASSERT(publisher_pair_matches(restart_after));
	UT_ASSERT(fcntl(store_global, F_GETFD) >= 0);
	publisher_end(&t);
}
UT_TEST(readback_keeps_all_batches_and_more_than_sixty_four_pages)
{
	for (unsigned shape = 0; shape < 2; shape++) {
		uint8 before[32], after[32];
		restart_distinct_bases = shape == 0;
		ClusterControlDebtTransitionV2 *t = publisher_begin(shape ? 4097 : 130, shape ? 1 : 2);
		UT_ASSERT(t && publisher_run(t));
		uint64 files = restart_tree_digest(before);
		reset_faults();
		UT_ASSERT(readback(t, RESTART_BUDGET));
		UT_ASSERT(writes == 0 && syncs == 0);
		UT_ASSERT_EQ(restart_tree_digest(after), files);
		UT_ASSERT(!memcmp(before, after, 32));
		publisher_end(&t);
	}
}
UT_TEST(readback_missing_last_page_or_basis_or_registry_never_falls_back)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		uint8 before[32], after[32]; char name[96];
		restart_distinct_bases = true;
		ClusterControlDebtTransitionV2 *t = publisher_begin(130, 2);
		UT_ASSERT(t && publisher_run(t));
		int dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		UT_ASSERT(dir >= 0);
		if (fault == 0) {
			uint8 hash[32]; debt_sha(image->pages + image->page_bytes - 33024, 33024, hash);
			debt_store_name(hash, false, name);
		} else if (fault == 1) debt_store_basis_name(restart_older_ref, name);
		else debt_store_name(image->reference + 64, true, name);
		UT_ASSERT(unlinkat(dir, name, 0) == 0 && close(dir) == 0);
		uint64 files = restart_tree_digest(before);
		reset_faults(); UT_ASSERT(!readback(t, RESTART_BUDGET));
		UT_ASSERT(writes == 0 && syncs == 0);
		UT_ASSERT_EQ(restart_tree_digest(after), files);
		UT_ASSERT(!memcmp(before, after, 32));
		UT_ASSERT(fcntl(store_global, F_GETFD) >= 0);
		publisher_end(&t);
	}
}
UT_TEST(readback_compares_backup_and_complete_record_body)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		UT_ASSERT(t && publisher_run(t));
		int fd = openat(store_global, fault ? "pgrac_control_root" : "pgrac_control_root.bak", O_RDWR);
		uint8 byte = 0;
		UT_ASSERT(fd >= 0 && pread(fd, &byte, 1, 1400) == 1);
		byte ^= 1; UT_ASSERT(pwrite(fd, &byte, 1, 1400) == 1 && close(fd) == 0);
		reset_faults(); UT_ASSERT(!readback(t, RESTART_BUDGET));
		UT_ASSERT(writes == 0 && syncs == 0);
		publisher_end(&t);
	}
}
UT_TEST(readback_foreign_owner_cf_critical_and_lock_refuse_before_io)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		UT_ASSERT(t && publisher_run(t)); reset_faults();
		if (fault == 0) CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
		if (fault == 1) cf_held = false;
		if (fault == 2) restart_cf_sequence++;
		if (fault == 3) publisher_lwlock = true;
		if (fault == 4) CritSectionCount = 1;
		if (fault == 5) MyProcPid++;
		UT_ASSERT(!readback(t, RESTART_BUDGET)); UT_ASSERT_EQ(reads, 0);
		UT_ASSERT(writes == 0 && syncs == 0);
		CurrentResourceOwner = (ResourceOwner)(uintptr_t)1; MyProcPid = (int)getpid();
		publisher_end(&t);
	}
}
UT_TEST(readback_low_budget_bad_fd_and_copy_keep_borrowed_custody)
{
	ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1), copy;
	UT_ASSERT(t && publisher_run(t)); copy = *t; reset_faults();
	UT_ASSERT(!readback(t, 1024)); UT_ASSERT_EQ(reads, 0);
	UT_ASSERT(!readback(&copy, RESTART_BUDGET)); UT_ASSERT_EQ(reads, 0);
	UT_ASSERT(!debt_readback_fixture_v2(t, -1, RESTART_BUDGET));
	UT_ASSERT(fcntl(store_global, F_GETFD) >= 0);
	UT_ASSERT(readback(t, RESTART_BUDGET));
	publisher_end(&t);
}
UT_TEST(readback_bad_last_page_and_read_error_never_mutate_or_close)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(130, 2);
		UT_ASSERT(t && publisher_run(t)); reset_faults();
		if (fault == 0) {
			int fd = restart_open_page(store_global, 3, O_RDWR);
			UT_ASSERT(fd >= 0 && ftruncate(fd, 33023) == 0 && close(fd) == 0);
		} else fail_read = 1;
		UT_ASSERT(!readback(t, RESTART_BUDGET));
		UT_ASSERT(writes == 0 && syncs == 0);
		UT_ASSERT(fcntl(store_global, F_GETFD) >= 0);
		publisher_end(&t);
	}
}

UT_TEST(readback_owner_or_cf_change_during_io_refuses_without_repair)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		uint8 before[32], after[32];
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		uint64 files;

		UT_ASSERT(t && publisher_run(t));
		files = restart_tree_digest(before);
		reset_faults(); readback_late_kind = fault;
		UT_ASSERT(!readback(t, RESTART_BUDGET));
		UT_ASSERT(readback_late_kind == 0 && writes == 0 && syncs == 0);
		CurrentResourceOwner = (ResourceOwner)(uintptr_t)1; cf_held = true;
		UT_ASSERT_EQ(restart_tree_digest(after), files);
		UT_ASSERT(!memcmp(before, after, 32));
		UT_ASSERT(fcntl(store_global, F_GETFD) >= 0);
		publisher_end(&t);
	}
}

int main(void)
{
	UT_PLAN(8);
	UT_RUN(readback_requires_real_published_whole_pair_not_candidate);
	UT_RUN(readback_keeps_all_batches_and_more_than_sixty_four_pages);
	UT_RUN(readback_missing_last_page_or_basis_or_registry_never_falls_back);
	UT_RUN(readback_compares_backup_and_complete_record_body);
	UT_RUN(readback_foreign_owner_cf_critical_and_lock_refuse_before_io);
	UT_RUN(readback_low_budget_bad_fd_and_copy_keep_borrowed_custody);
	UT_RUN(readback_bad_last_page_and_read_error_never_mutate_or_close);
	UT_RUN(readback_owner_or_cf_change_during_io_refuses_without_repair);
	UT_DONE(); return ut_failed_count != 0;
}
