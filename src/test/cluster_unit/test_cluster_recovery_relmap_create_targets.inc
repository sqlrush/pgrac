/*-------------------------------------------------------------------------
 * test_cluster_recovery_relmap_create_targets.inc
 *    RMC1 observations enumerate original physical targets without creation.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_relmap_create_targets.inc
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "test_cluster_recovery_relmap_create_fixture.h"

static void
creation_target_fixture(int count)
{
	wal_fixture(RM_RELMAP_ID, CLUSTER_RELMAP_CREATE_WAL_INFO_V1,
		CLUSTER_RELMAP_CREATE_WAL_SIZE_V1, 0);
	test_relmap_creation_bytes(wal_payload.bytes, &native_source, count);
	wal_record_storage.decoded.header.xl_tot_len = SizeOfXLogRecord + CLUSTER_RELMAP_CREATE_WAL_SIZE_V1;
	native_reader.EndRecPtr = wal_record_storage.decoded.next_lsn
		= native_reader.ReadRecPtr + MAXALIGN(wal_record_storage.decoded.header.xl_tot_len);
	native_cut.scan_end_exclusive = native_reader.EndRecPtr;
}

UT_TEST(test_creation_targets_keep_both_managed_copies_and_all_mapping_forks)
{
	for (int count = 0; count <= MAX_MAPPINGS; count += MAX_MAPPINGS) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryWalTargetsDiag diag;
		struct stat st;
		creation_target_fixture(count);
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(diag.native_effects, 1);
		UT_ASSERT_EQ(diag.structural_effects, 1);
		UT_ASSERT_EQ(diag.unowned_records, 1);
		UT_ASSERT(collect(roots, &diag));
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 2 + 4 * count);
		for (unsigned copy = 0; copy < 2; ++copy) {
			const char *path = copy ? "base/5/pgrac_relmap_authority.bak" : "base/5/pgrac_relmap_authority";
			const ClusterRecoveryRootEntry *entry = mx_target_entry(roots, path);
			UT_ASSERT(entry != NULL);
			if (entry == NULL) continue;
			UT_ASSERT_EQ(entry->file.kind, copy ? CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP
				: CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY);
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->file.locator.dbOid, 5);
			UT_ASSERT_EQ(entry->required_blocks, 0);
			UT_ASSERT(fstatat(fd, path, &st, AT_SYMLINK_NOFOLLOW) != 0 && errno == ENOENT);
		}
		UT_ASSERT(mx_target_entry(roots, "base/5/pg_filenode.map") == NULL);
		for (int m = 0; m < count; ++m)
			UT_ASSERT_EQ(wal_forks(roots, 16385 + m, 0),
				(1U << MAIN_FORKNUM) | (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM) | (1U << SPACE_FORKNUM));
		cluster_recovery_root_set_destroy(roots); remove_root(fd);
	}
}

UT_TEST(test_creation_targets_preserve_short_missing_and_unrelated_physical_files)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryWalTargetsDiag diag;
	const ClusterRecoveryRootEntry *entry;
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	root_file(fd, "base/5/pgrac_relmap_authority", "short", 5);
	root_file(fd, "base/5/99999", "untouched", 9);
	roots = begin_roots(fd, 1024 * 1024);
	creation_target_fixture(0);
	UT_ASSERT(collect(roots, &diag));
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	entry = mx_target_entry(roots, "base/5/pgrac_relmap_authority");
	UT_ASSERT(entry && entry->file.size == 5
		&& entry->origins == (CLUSTER_RECOVERY_ROOT_FROM_WAL | CLUSTER_RECOVERY_ROOT_PHYSICAL));
	entry = mx_target_entry(roots, "base/5/pgrac_relmap_authority.bak");
	UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_FROM_WAL);
	entry = mx_target_entry(roots, "base/5/99999");
	UT_ASSERT(entry && entry->origins == CLUSTER_RECOVERY_ROOT_PHYSICAL);
	root_file(fd, "base/5/pgrac_relmap_authority", "changed", 7);
	UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT(finish_roots(roots, NULL) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	cluster_recovery_root_set_destroy(roots); remove_root(fd);
}

UT_TEST(test_creation_targets_refuse_forged_carriers_and_partial_union)
{
	for (unsigned fault = 0; fault < 8; ++fault) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		ClusterRecoveryNativeRecord record;
		ClusterRelmapCreateWalV1 parsed;
		char copy[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
		creation_target_fixture(2);
		UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source,
			&native_cut, 65536, &scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) { cluster_recovery_root_set_destroy(roots); remove_root(fd); continue; }
		UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
		record = *effect.record; effect.record = &record;
		if (fault == 0) { memcpy(copy, effect.bytes, sizeof(copy)); effect.bytes = copy; }
		if (fault == 1) effect.ordinal++;
		if (fault == 2) record.database++;
		if (fault == 3) record.nmappings++;
		if (fault == 4) { record.payload_length--; effect.length--; }
		if (fault == 5) {
			UT_ASSERT(cluster_relmap_create_decode_v1(record.payload, record.payload_length, &parsed));
			parsed.identity.tablespace_oid = 9999;
			UT_ASSERT(cluster_relmap_create_encode_v1(&parsed.identity, parsed.batch_uuid,
				&parsed.committed, copy, sizeof(copy)));
			record.payload = copy; effect.bytes = copy; record.tablespace = 9999;
		}
		if (fault == 6) record.source.claim.database_incarnation++;
		if (fault == 7) roots->capacity = 2;
		UT_ASSERT(cluster_recovery_root_set_add_native(roots, &effect) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
		UT_ASSERT(cluster_recovery_root_set_recheck_begin(roots) > CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_native_scan_destroy(scan);
		cluster_recovery_root_set_destroy(roots); remove_root(fd);
	}
}


#include "cluster/cluster_recovery_native_inventory.h"
UT_TEST(test_creation_effect_envelope_keeps_both_unresolved_owner_duties)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryNativeRecordObservationV1 view = {0};
	uint16 owners;
	uint8 raw[SizeOfXLogRecord + CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	creation_target_fixture(1);
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&native_reader, &native_source,
		&native_cut, 65536, &scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
	owners = cluster_recovery_native_effect_owner_families_v1(&effect);
	memcpy(raw, &effect.record->header, SizeOfXLogRecord);
	memcpy(raw + SizeOfXLogRecord, effect.record->payload, effect.record->payload_length);
	view.primary = effect; view.count = 1; view.effects = &effect; view.effect_count = 1;
	view.owner_families = &owners; view.raw_record = raw; view.raw_length = sizeof(raw);
	UT_ASSERT(cluster_recovery_native_raw_sha256_v1(raw, sizeof(raw), view.raw_record_sha256));
	UT_ASSERT(cluster_recovery_native_record_effects_valid_v1(&view, 1));
	owners = CLUSTER_RECOVERY_OWNER_RELMAP;
	UT_ASSERT(!cluster_recovery_native_record_effects_valid_v1(&view, 1));
	owners = CLUSTER_RECOVERY_OWNER_CREATE;
	UT_ASSERT(!cluster_recovery_native_record_effects_valid_v1(&view, 1));
	owners = CLUSTER_RECOVERY_OWNER_RELMAP | CLUSTER_RECOVERY_OWNER_CREATE;
	UT_ASSERT(!cluster_recovery_native_record_effects_valid_v1(&view, 0));
	raw[sizeof(raw) - 1] ^= 1;
	UT_ASSERT(!cluster_recovery_native_record_effects_valid_v1(&view, 1));
	cluster_recovery_native_scan_destroy(scan);
}
