/*-------------------------------------------------------------------------
 * test_cluster_recovery_relmap_create_effects.inc
 *    Complete creation records remain owned, unresolved native observations.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_relmap_create_effects.inc
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "test_cluster_recovery_relmap_create_fixture.h"

static void
creation_scan_fixture(int count)
{
	char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	fixture(RM_RELMAP_ID, CLUSTER_RELMAP_CREATE_WAL_INFO_V1);
	test_relmap_creation_bytes(bytes, &source, count);
	append(bytes, sizeof(bytes));
}

UT_TEST(test_creation_scan_owns_whole_identity_batch_and_image)
{
	for (int count = 0; count <= MAX_MAPPINGS; count += MAX_MAPPINGS) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		ClusterRelmapCreateWalV1 parsed;
		char saved[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
		creation_scan_fixture(count);
		memcpy(saved, payload.bytes, sizeof(saved));
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL) continue;
		memset(&reader, 0, sizeof(reader)); memset(&source, 0, sizeof(source));
		memset(&payload, 0, sizeof(payload));
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_EFFECT);
		UT_ASSERT(effect.kind != CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE);
		UT_ASSERT_EQ(cluster_recovery_native_effect_owner_families_v1(&effect),
			CLUSTER_RECOVERY_OWNER_RELMAP | CLUSTER_RECOVERY_OWNER_CREATE);
		UT_ASSERT(effect.bytes == effect.record->payload);
		UT_ASSERT_EQ(effect.length, sizeof(saved));
		UT_ASSERT(memcmp(effect.bytes, saved, sizeof(saved)) == 0);
		UT_ASSERT(cluster_relmap_create_decode_v1(effect.bytes, effect.length, &parsed));
		UT_ASSERT_EQ(parsed.identity.creation_generation, 101);
		UT_ASSERT_EQ(parsed.identity.registration_generation, 211);
		UT_ASSERT_EQ(parsed.batch_uuid[0], 7);
		UT_ASSERT_EQ(parsed.committed.num_mappings, count);
		UT_ASSERT_EQ(effect.record->database, 5);
		UT_ASSERT_EQ(effect.record->tablespace, DEFAULTTABLESPACE_OID);
		UT_ASSERT_EQ(effect.record->nmappings, count);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect), CLUSTER_RECOVERY_NATIVE_END);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_creation_scan_refuses_malformed_and_foreign_complete_records)
{
	for (unsigned fault = 0; fault < 12; ++fault) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRelmapCreateWalV1 parsed;
		creation_scan_fixture(2);
		UT_ASSERT(cluster_relmap_create_decode_v1(payload.bytes, decoded.main_data_len, &parsed));
		if (fault < 4) {
			if (fault == 0) ++parsed.identity.system_identifier;
			if (fault == 1) ++parsed.identity.database_incarnation;
			if (fault == 2) ++parsed.identity.storage_uuid[15];
			if (fault == 3) parsed.identity.tablespace_oid = GLOBALTABLESPACE_OID;
			if (fault < 3)
				UT_ASSERT(cluster_relmap_create_encode_v1(&parsed.identity, parsed.batch_uuid,
					&parsed.committed, payload.bytes, decoded.main_data_len));
			else payload.bytes[8] ^= 1;
		} else if (fault == 4) decoded.main_data_len--;
		else if (fault == 5) decoded.main_data_len++;
		else if (fault == 6) payload.bytes[40] = 1;
		else if (fault == 7) payload.bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1 - 1] ^= 1;
		else if (fault == 8) decoded.max_block_id = 0;
		else if (fault == 9) decoded.has_page_version_edge = true;
		else if (fault == 10) cut.failed_thread++;
		else cut.scan_end_exclusive--;
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_creation_scan_budget_and_valid_byte_change_are_sticky)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	ClusterRelmapCreateWalV1 changed;
	char saved[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	size_t bytes;
	creation_scan_fixture(1);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	bytes = cluster_recovery_native_scan_bytes(scan);
	cluster_recovery_native_scan_destroy(scan); scan = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_scan_begin(&reader, &source, &cut, bytes - 1, &scan),
		CLUSTER_RECOVERY_NATIVE_CAPACITY);
	UT_ASSERT(scan == NULL);
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL) return;
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
		CLUSTER_RECOVERY_NATIVE_UNCHANGED);
	memcpy(saved, payload.bytes, sizeof(saved));
	UT_ASSERT(cluster_relmap_create_decode_v1(saved, sizeof(saved), &changed));
	changed.identity.creation_generation++;
	changed.batch_uuid[15]++;
	UT_ASSERT(cluster_relmap_create_encode_v1(&changed.identity, changed.batch_uuid,
		&changed.committed, payload.bytes, sizeof(saved)));
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
		CLUSTER_RECOVERY_NATIVE_CHANGED);
	memcpy(payload.bytes, saved, sizeof(saved));
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
		CLUSTER_RECOVERY_NATIVE_CHANGED);
	UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect), CLUSTER_RECOVERY_NATIVE_CHANGED);
	UT_ASSERT(effect.bytes == NULL);
	cluster_recovery_native_scan_destroy(scan);
}
