/* Every retained mapping and map-copy path belongs to the original ROOT union.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_recovery_relmap_fixture.h"

static void
relmap_target_fixture(bool shared, int count)
{
	wal_fixture(RM_RELMAP_ID, XLOG_RELMAP_UPDATE, TEST_RELMAP_LENGTH, 0);
	test_relmap_payload(wal_payload.bytes, shared, count);
}

UT_TEST(test_relmap_targets_include_all_copies_and_referenced_forks)
{
	for (unsigned shared = 0; shared < 2; ++shared) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		RfPageIdentityV1 page = target(16385, FSM_FORKNUM, 9);
		const char *names[] = { "pg_filenode.map", "pg_filenode.map.bak", "pgrac_relmap_authority",
								"pgrac_relmap_authority.bak" };
		const ClusterRecoveryFileKind kinds[]
			= { CLUSTER_RECOVERY_FILE_RELMAP, CLUSTER_RECOVERY_FILE_RELMAP_BACKUP,
				CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY,
				CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP };
		unsigned maps = 0, relations = 0;
		if (shared)
			page.locator = (RelFileLocator){ GLOBALTABLESPACE_OID, 0, 16385 };
		UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		relmap_target_fixture(shared, 2);
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(diag.native_effects, 3);
		UT_ASSERT_EQ(diag.unowned_records, 1);
		UT_ASSERT(collect(roots, &diag)); /* duplicates do not shrink or multiply */
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		for (unsigned i = 0; i < 4; ++i) {
			char path[MAXPGPATH];
			const ClusterRecoveryRootEntry *entry;
			snprintf(path, sizeof(path), "%s/%s", shared ? "global" : "base/5", names[i]);
			entry = mx_target_entry(roots, path);
			UT_ASSERT(entry != NULL);
			if (entry == NULL)
				continue;
			UT_ASSERT_EQ(entry->file.kind, kinds[i]);
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->required_blocks, 0);
			maps++;
		}
		for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i) {
			const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
			if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELATION)
				continue;
			UT_ASSERT_EQ(entry->file.locator.spcOid, shared ? 1664 : 1663);
			UT_ASSERT_EQ(entry->file.locator.dbOid, shared ? 0 : 5);
			UT_ASSERT(entry->file.locator.relNumber == 16385
					  || entry->file.locator.relNumber == 16386);
			UT_ASSERT_EQ(entry->required_blocks, entry->file.locator.relNumber == 16385
														 && entry->file.forknum == FSM_FORKNUM
													 ? 10
													 : 0);
			relations++;
		}
		UT_ASSERT_EQ(maps, 4);
		UT_ASSERT_EQ(relations, 8);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_relmap_empty_keeps_missing_short_and_untouched_copies)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryWalTargetsDiag diag;
	const ClusterRecoveryRootEntry *entry;
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	root_file(fd, "base/5/pg_filenode.map", "short", 5);
	root_file(fd, "base/5/pgrac_relmap_authority.bak", "", 0);
	root_file(fd, "base/5/99999", "untouched", 9);
	roots = begin_roots(fd, 1024 * 1024);
	relmap_target_fixture(false, 0);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(diag.native_effects, 1);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	entry = mx_target_entry(roots, "base/5/pg_filenode.map");
	UT_ASSERT(entry
			  && entry->origins == (CLUSTER_RECOVERY_ROOT_FROM_WAL | CLUSTER_RECOVERY_ROOT_PHYSICAL)
			  && entry->file.size == 5);
	entry = mx_target_entry(roots, "base/5/pg_filenode.map.bak");
	UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_FROM_WAL);
	entry = mx_target_entry(roots, "base/5/99999");
	UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_PHYSICAL);
	root_file(fd, "base/5/pg_filenode.map", "changed", 7);
	UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_relmap_target_failures_revoke_entire_union)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		RelFileNumber zero = 0;
		relmap_target_fixture(false, 2);
		if (fault == 0)
			wal_payload.bytes[TEST_RELMAP_LENGTH - 1] ^= 1;
		if (fault == 1)
			native_source.claim.database_incarnation++;
		if (fault == 2) {
			memcpy(wal_payload.bytes + MinSizeOfRelmapUpdate + offsetof(RelMapFile, mappings)
					   + sizeof(RelMapping) + offsetof(RelMapping, mapfilenumber),
				   &zero, sizeof(zero));
			test_relmap_crc(wal_payload.bytes);
		}
		if (fault == 3)
			roots->capacity = 6; /* partial append must not escape */
		if (fault == 4)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		if (fault == 5)
			native_cut.scan_end_exclusive--;
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); ++i)
			UT_ASSERT(cluster_recovery_root_set_entry(roots, i) == NULL);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_relmap_mapping_requires_exact_original_array_and_ordinal)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeRecord record;
		RelMapping copy;
		relmap_target_fixture(true, 2);
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
														65536, &scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) {
			cluster_recovery_root_set_destroy(roots);
			remove_root(fd);
			continue;
		}
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_EFFECT);
		record = *effect.record;
		effect.record = &record;
		memcpy(&copy, effect.bytes, sizeof(copy));
		if (fault == 0)
			effect.bytes = &copy;
		if (fault == 1)
			effect.ordinal++;
		if (fault == 2)
			record.mappings_offset = UINT32_MAX;
		if (fault == 3)
			record.nmappings = UINT32_MAX;
		if (fault == 4)
			record.tablespace = 1663;
		UT_ASSERT(cluster_recovery_root_set_add_native(roots, &effect)
				  > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}
