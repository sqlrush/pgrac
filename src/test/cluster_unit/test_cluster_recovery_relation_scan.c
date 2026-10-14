/* Actual observed files through the original SPACE codecs and fork reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define RECOVERY_ROOT_SET_FIXTURE_ONLY
#include "test_cluster_recovery_root_set.c"
#include "cluster/cluster_recovery_relation_scan.h"
#include "../../backend/cluster/cluster_recovery_relation_scan.c"

static ClusterSpaceReservation
relation_state(void)
{
	ClusterSpaceReservation state = { 0 };

	state.identity.key.system_identifier = 817;
	state.identity.key.database_incarnation = 91;
	memcpy(state.identity.key.storage_uuid, root_uuid, 16);
	state.identity.key.locator = (RelFileLocator){ 1663, 5, 16385 };
	memset(state.identity.incarnation, 7, 16);
	state.identity.sequence = 2;
	state.identity.operation = 9;
	state.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	state.next_block = 3;
	return state;
}

static void
relation_space(int fd, ClusterSpaceReservation state, unsigned fault)
{
	PGAlignedBlock pages[2];
	ClusterSpaceIdentity first = state.identity;

	if (fault == 1)
		first.key.database_incarnation++;
	UT_ASSERT(cluster_space_identity_page_encode(&first, 7, pages[0].data, BLCKSZ));
	if (fault == 2)
		state.identity.sequence++;
	UT_ASSERT(cluster_space_reservation_page_encode(&state, 11, pages[1].data, BLCKSZ));
	if (fault == 3)
		pages[0].data[SizeOfPageHeaderData + 17] ^= 1;
	root_file(fd, "base/5/16385_space", pages, sizeof(pages));
}

static int
relation_files(unsigned fault, bool tombstone)
{
	int fd = new_root();
	ClusterSpaceReservation state = relation_state();
	PGAlignedBlock pages[3];

	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	for (unsigned i = 0; i < 3; i++)
		memset(pages[i].data, i + 1, BLCKSZ);
	root_file(fd, "base/5/16385", pages, tombstone ? 0 : sizeof(pages));
	root_file(fd, "base/5/16385_vm", pages, BLCKSZ);
	root_file(fd, "base/5/16385_fsm", pages, BLCKSZ);
	if (tombstone) {
		state.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		state.next_block = 0;
	}
	if (fault != 4)
		relation_space(fd, state, fault);
	return fd;
}

static uint32
relation_index(ClusterRecoveryRootSet *roots, ForkNumber fork)
{
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		if (entry != NULL && entry->file.kind == CLUSTER_RECOVERY_FILE_RELATION
			&& entry->file.locator.relNumber == 16385 && entry->file.forknum == fork)
			return i;
	}
	return UINT32_MAX;
}

static ClusterRecoveryRelationResult
relation_finish(ClusterRecoveryRelationScan *scan, unsigned *pages)
{
	ClusterRecoveryRelationPage out;
	ClusterRecoveryRelationResult result = CLUSTER_RECOVERY_RELATION_MORE;

	for (unsigned i = 0; scan != NULL && i < 100; i++) {
		result = cluster_recovery_relation_next(scan, &out);
		if (result == CLUSTER_RECOVERY_RELATION_PAGE) {
			(*pages)++;
			UT_ASSERT_EQ(out.reservation.identity.key.locator.relNumber, 16385);
			UT_ASSERT_EQ(out.identity_token, 7);
			UT_ASSERT_EQ(out.reservation_token, 11);
			if (out.forknum == MAIN_FORKNUM)
				UT_ASSERT_EQ((unsigned char)out.page[BLCKSZ - 1], out.block + 1);
		} else if (result != CLUSTER_RECOVERY_RELATION_MORE)
			return result;
	}
	return result;
}

UT_TEST(test_untouched_main_and_all_auxiliary_forks_are_enumerated)
{
	int fd = relation_files(0, false);
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	const ForkNumber forks[] = { MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, SPACE_FORKNUM };
	const unsigned counts[] = { 3, 1, 1, 2 };

	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (unsigned i = 0; i < lengthof(forks); i++) {
		ClusterRecoveryRelationScan *scan = NULL;
		unsigned pages = 0;

		UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, relation_index(roots, forks[i]),
													 64 * 1024, &scan),
					 CLUSTER_RECOVERY_RELATION_MORE);
		if (scan != NULL) {
			UT_ASSERT_EQ(relation_finish(scan, &pages), CLUSTER_RECOVERY_RELATION_DRAINED);
			UT_ASSERT_EQ(pages, counts[i]);
			UT_ASSERT_EQ(cluster_recovery_relation_identity(scan)->next_block, 3);
		}
		cluster_recovery_relation_destroy(scan);
	}
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_missing_space_or_wal_target_requires_structural_recovery)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		int fd = relation_files(mode == 0 ? 4 : 0, false);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryRelationScan *scan = NULL;
		RfPageIdentityV1 page = target(16385, mode == 1 ? VISIBILITYMAP_FORKNUM : MAIN_FORKNUM, 99);

		if (mode == 1) {
			/* Recreate the observation after removing the physical WAL target. */
			cluster_recovery_root_set_destroy(roots);
			UT_ASSERT_EQ(unlinkat(fd, "base/5/16385_vm", 0), 0);
			roots = begin_roots(fd, 1024 * 1024);
		}
		if (mode != 0)
			UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page),
						 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, relation_index(roots, page.forknum),
													 64 * 1024, &scan),
					 CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED);
		UT_ASSERT(scan == NULL);
		cluster_recovery_relation_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_foreign_corrupt_and_partial_space_never_exports_relation_pages)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		int fd = relation_files(fault, false);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryRelationScan *scan = NULL;
		unsigned pages = 0;

		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, relation_index(roots, MAIN_FORKNUM),
													 64 * 1024, &scan),
					 CLUSTER_RECOVERY_RELATION_MORE);
		if (scan != NULL) {
			UT_ASSERT_EQ(relation_finish(scan, &pages),
						 fault == 2 ? CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED
									: CLUSTER_RECOVERY_RELATION_CORRUPT);
			UT_ASSERT_EQ(pages, 0);
			UT_ASSERT(cluster_recovery_relation_identity(scan) == NULL);
		}
		cluster_recovery_relation_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_zero_length_main_keeps_tombstone_and_unfinished_drop_obligation)
{
	int fd = relation_files(0, true);
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	ClusterRecoveryRelationScan *scan = NULL;
	unsigned pages = 0;

	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, relation_index(roots, MAIN_FORKNUM),
												 64 * 1024, &scan),
				 CLUSTER_RECOVERY_RELATION_MORE);
	if (scan != NULL) {
		UT_ASSERT_EQ(relation_finish(scan, &pages), CLUSTER_RECOVERY_RELATION_DRAINED);
		UT_ASSERT_EQ(pages, 0);
		UT_ASSERT_EQ(cluster_recovery_relation_identity(scan)->identity.state,
					 CLUSTER_SPACE_IDENTITY_TOMBSTONED);
	}
	cluster_recovery_relation_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_io_and_final_space_change_invalidate_the_fork_inventory)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		int fd = relation_files(0, false);
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryRelationScan *scan = NULL;
		ClusterRecoveryRelationPage out;
		unsigned pages = 0;

		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, relation_index(roots, MAIN_FORKNUM),
													 64 * 1024, &scan),
					 CLUSTER_RECOVERY_RELATION_MORE);
		if (scan != NULL) {
			if (mode < 2)
				inject_pread_fault = mode + 1;
			else {
				ClusterSpaceReservation changed = relation_state();
				UT_ASSERT_EQ(cluster_recovery_relation_next(scan, &out),
							 CLUSTER_RECOVERY_RELATION_MORE);
				UT_ASSERT_EQ(cluster_recovery_relation_next(scan, &out),
							 CLUSTER_RECOVERY_RELATION_MORE);
				changed.identity.operation++;
				relation_space(fd, changed, 0);
			}
			UT_ASSERT_EQ(relation_finish(scan, &pages), mode < 2
															? CLUSTER_RECOVERY_RELATION_IO
															: CLUSTER_RECOVERY_RELATION_CHANGED);
			inject_pread_fault = 0;
			UT_ASSERT(cluster_recovery_relation_identity(scan) == NULL);
		}
		cluster_recovery_relation_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_global_catalog_namespace_and_bounded_allocation)
{
	int fd = relation_files(0, false);
	ClusterSpaceReservation state = relation_state();
	PGAlignedBlock pages[2];
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryRelationScan *scan = NULL;
	ClusterRecoveryRelationPage page;
	ClusterRecoveryRelationResult result;
	uint32 global_index = UINT32_MAX;
	unsigned emitted = 0;

	state.identity.key.locator = (RelFileLocator){ GLOBALTABLESPACE_OID, InvalidOid, 16385 };
	UT_ASSERT_EQ(mkdirat(fd, "global", 0700), 0);
	UT_ASSERT(cluster_space_identity_page_encode(&state.identity, 7, pages[0].data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_encode(&state, 11, pages[1].data, BLCKSZ));
	root_file(fd, "global/16385_space", pages, sizeof(pages));
	memset(pages[0].data, 99, BLCKSZ);
	root_file(fd, "global/16385", pages, BLCKSZ);
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		if (entry->file.locator.spcOid == GLOBALTABLESPACE_OID
			&& entry->file.forknum == MAIN_FORKNUM)
			global_index = i;
	}
	UT_ASSERT(global_index != UINT32_MAX);
	UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, global_index, 1, &scan),
				 CLUSTER_RECOVERY_RELATION_CAPACITY);
	UT_ASSERT(scan == NULL);
	UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, global_index, 64 * 1024, &scan),
				 CLUSTER_RECOVERY_RELATION_MORE);
	if (scan != NULL) {
		for (unsigned i = 0; i < 20; i++) {
			result = cluster_recovery_relation_next(scan, &page);
			if (result == CLUSTER_RECOVERY_RELATION_PAGE) {
				emitted++;
				UT_ASSERT_EQ(page.reservation.identity.key.locator.dbOid, InvalidOid);
				UT_ASSERT_EQ(page.reservation.identity.key.locator.spcOid, GLOBALTABLESPACE_OID);
				UT_ASSERT_EQ((unsigned char)page.page[0], 99);
			} else if (result != CLUSTER_RECOVERY_RELATION_MORE)
				break;
		}
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELATION_DRAINED);
		UT_ASSERT_EQ(emitted, 1);
	}
	cluster_recovery_relation_destroy(scan);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_partial_fork_and_late_invalid_root_cannot_finish)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		int fd = relation_files(0, false);
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRelationScan *scan = NULL;
		ClusterRecoveryRelationPage page;
		unsigned emitted = 0;

		if (mode == 0)
			root_file(fd, "base/5/16385", "x", 1);
		roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_relation_begin(roots, relation_index(roots, MAIN_FORKNUM),
													 64 * 1024, &scan),
					 mode == 0 ? CLUSTER_RECOVERY_RELATION_CORRUPT
							   : CLUSTER_RECOVERY_RELATION_MORE);
		if (mode == 1 && scan != NULL) {
			RfPageIdentityV1 late = target(16385, MAIN_FORKNUM, 99);
			UT_ASSERT_EQ(relation_finish(scan, &emitted), CLUSTER_RECOVERY_RELATION_DRAINED);
			UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &late),
						 CLUSTER_RECOVERY_ROOTS_INVALID);
			UT_ASSERT_EQ(cluster_recovery_relation_next(scan, &page),
						 CLUSTER_RECOVERY_RELATION_CHANGED);
			UT_ASSERT(cluster_recovery_relation_identity(scan) == NULL);
		}
		cluster_recovery_relation_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_untouched_main_and_all_auxiliary_forks_are_enumerated);
	UT_RUN(test_missing_space_or_wal_target_requires_structural_recovery);
	UT_RUN(test_foreign_corrupt_and_partial_space_never_exports_relation_pages);
	UT_RUN(test_zero_length_main_keeps_tombstone_and_unfinished_drop_obligation);
	UT_RUN(test_io_and_final_space_change_invalidate_the_fork_inventory);
	UT_RUN(test_global_catalog_namespace_and_bounded_allocation);
	UT_RUN(test_partial_fork_and_late_invalid_root_cannot_finish);
	UT_DONE();
	return ut_failed_count != 0;
}
