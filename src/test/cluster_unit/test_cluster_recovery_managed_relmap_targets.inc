/*-------------------------------------------------------------------------
 * test_cluster_recovery_managed_relmap_targets.inc
 *    Managed WAL images contribute every original ROOT target.
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_managed_relmap_targets.inc
 * NOTES
 *    Uses original WAL scanner, codec and physical ROOT; no mutation grant.
 *-------------------------------------------------------------------------
 */
#include "common/cluster_relmap_file.h"

static void
managed_target_fixture(bool shared, int count)
{
	ClusterRecoveryFileIdentityV2 file = { 0 };
	ClusterRelmapV2Image image = { 0 };
	char native[TEST_RELMAP_LENGTH];

	wal_fixture(RM_RELMAP_ID, XLOG_RELMAP_UPDATE_V2, CLUSTER_RELMAP_FILE_SIZE_V2, 0);
	file.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	file.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	file.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	file.system_identifier = 817;
	file.database_incarnation = 91;
	memcpy(file.storage_uuid, root_uuid, sizeof(root_uuid));
	file.redo_branch_uuid[0] = 31;
	file.redo_branch_generation = 7;
	file.redo_timeline = 7;
	file.database_oid = shared ? InvalidOid : 5;
	file.tablespace_oid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	file.origin = 0; /* source writer is 1, not creation origin */
	file.object_id = 55;
	file.creation_generation = 17;
	file.registration_generation = 19;
	image.identity = (ClusterRelmapV2Namespace){ 817, 91, file.database_oid, shared };
	image.committed_generation = 3;
	image.pending_generation = 4;
	image.pending_owner = (ClusterRelmapV2Owner){ 1, 4195136, 13, 71, 0, 33 };
	test_relmap_payload(native, shared, count);
	memcpy(&image.committed, native + MinSizeOfRelmapUpdate, sizeof(RelMapFile));
	image.pending = image.committed;
	for (int i = 0; i < count; i++)
		image.pending.mappings[i].mapfilenumber += 100;
	INIT_CRC32C(image.pending.crc);
	COMP_CRC32C(image.pending.crc, &image.pending, offsetof(RelMapFile, crc));
	FIN_CRC32C(image.pending.crc);
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&file, &image, wal_payload.bytes,
												  CLUSTER_RELMAP_FILE_SIZE_V2));
	native_reader.record->header.xl_tot_len = SizeOfXLogRecord + CLUSTER_RELMAP_FILE_SIZE_V2;
	native_reader.EndRecPtr = native_reader.record->next_lsn = native_cut.scan_end_exclusive
		= native_reader.ReadRecPtr + MAXALIGN(native_reader.record->header.xl_tot_len);
}

UT_TEST(test_managed_relmap_targets_keep_both_images_and_only_authority_copies)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned count = 0; count <= MAX_MAPPINGS; count += MAX_MAPPINGS) {
			int fd = new_root();
			ClusterRecoveryRootSet *roots = begin_roots(fd, 4 * 1024 * 1024);
			ClusterRecoveryWalTargetsDiag diag;
			unsigned maps = 0, relations = 0;
			managed_target_fixture(shared, count);
			UT_ASSERT(collect(roots, &diag));
			UT_ASSERT_EQ(diag.native_effects, 1 + 2 * count);
			UT_ASSERT_EQ(diag.unowned_records, 1);
			UT_ASSERT(collect(roots, &diag));
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
				const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
				UT_ASSERT(entry != NULL);
				if (entry == NULL)
					continue;
				UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
				UT_ASSERT_EQ(entry->required_blocks, 0);
				UT_ASSERT_EQ(entry->file.locator.spcOid, shared ? 1664 : 1663);
				UT_ASSERT_EQ(entry->file.locator.dbOid, shared ? 0 : 5);
				if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELATION) {
					RelFileNumber n = entry->file.locator.relNumber;
					UT_ASSERT((n >= 16385 && n < 16385 + count)
							  || (n >= 16485 && n < 16485 + count));
					relations++;
				} else {
					char path[MAXPGPATH];
					bool backup = entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP;
					UT_ASSERT(backup || entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY);
					snprintf(path, sizeof(path), "%s/pgrac_relmap_authority%s",
							 shared ? "global" : "base/5", backup ? ".bak" : "");
					UT_ASSERT(strcmp(entry->file.path, path) == 0);
					maps++;
				}
			}
			UT_ASSERT_EQ(maps, 2);
			UT_ASSERT_EQ(relations, 8 * count);
			cluster_recovery_root_set_destroy(roots);
			remove_root(fd);
		}
}

UT_TEST(test_managed_relmap_target_entries_require_original_region_and_full_metadata)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeRecord record;
		RelMapping copy;
		managed_target_fixture(false, 2);
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source, &native_cut,
														65536, &scan),
					 CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) {
			cluster_recovery_root_set_destroy(roots);
			remove_root(fd);
			continue;
		}
		/* Select the first pending entry after primary and two committed entries. */
		for (unsigned i = 0; i < 4; i++) {
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			if (i < 3)
				UT_ASSERT_EQ(cluster_recovery_root_set_add_native(roots, &effect),
							 CLUSTER_RECOVERY_ROOTS_MORE);
		}
		record = *effect.record;
		effect.record = &record;
		memcpy(&copy, effect.bytes, sizeof(copy));
		if (fault == 0)
			effect.bytes = &copy;
		if (fault == 1)
			effect.ordinal = 0;
		if (fault == 2)
			record.mappings_offset = UINT32_MAX;
		if (fault == 3)
			record.nmappings = UINT32_MAX;
		if (fault == 4)
			record.database++;
		if (fault == 5)
			record.source.claim.identity.origin_node_id++;
		if (fault == 6)
			record.source.timeline++;
		if (fault == 7)
			effect.ordinal = record.nmappings;
		UT_ASSERT(cluster_recovery_root_set_add_native(roots, &effect)
				  > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_managed_relmap_target_failure_revokes_whole_union)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		managed_target_fixture(true, 2);
		if (fault == 0)
			wal_payload.bytes[CLUSTER_RELMAP_FILE_SIZE_V2 - 1] ^= 1;
		if (fault == 1)
			native_cut.scan_end_exclusive--;
		if (fault == 2)
			roots->capacity = 6;
		if (fault == 3)
			UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(!collect(roots, &diag));
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}
