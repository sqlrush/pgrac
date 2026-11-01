/*-------------------------------------------------------------------------
 * test_cluster_recovery_managed_relmap_effects.inc
 *    Managed WAL intents remain complete native/ROOT input obligations.
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_managed_relmap_effects.inc
 * NOTES
 *    Source/cut fixtures are controlled; original codecs and scanner execute.
 *-------------------------------------------------------------------------
 */
#include "common/cluster_relmap_file.h"

static ClusterRecoveryFileIdentityV2 managed_scan_file;
static ClusterRelmapV2Image managed_scan_image;

static void
managed_scan_fixture(bool shared, int count)
{
	char native[TEST_RELMAP_LENGTH];
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	fixture(RM_RELMAP_ID, XLOG_RELMAP_UPDATE_V2);
	memset(&managed_scan_file, 0, sizeof(managed_scan_file));
	managed_scan_file.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	managed_scan_file.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	managed_scan_file.format_version = 2;
	managed_scan_file.system_identifier = 817;
	managed_scan_file.database_incarnation = 91;
	managed_scan_file.storage_uuid[0] = 5;
	managed_scan_file.redo_branch_uuid[0] = 31;
	managed_scan_file.redo_branch_generation = 7;
	managed_scan_file.redo_timeline = 7;
	managed_scan_file.database_oid = shared ? 0 : 5;
	managed_scan_file.tablespace_oid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	managed_scan_file.origin = 0; /* creation origin is independent of writer 1 */
	managed_scan_file.object_id = 55;
	managed_scan_file.creation_generation = 17;
	managed_scan_file.registration_generation = 19;
	memset(&managed_scan_image, 0, sizeof(managed_scan_image));
	managed_scan_image.identity = (ClusterRelmapV2Namespace){ 817, 91, shared ? 0 : 5, shared };
	managed_scan_image.committed_generation = 3;
	managed_scan_image.pending_generation = 4;
	managed_scan_image.pending_owner = (ClusterRelmapV2Owner){ 1, 4195136, 13, 71, 0, 33 };
	test_relmap_payload(native, shared, count);
	memcpy(&managed_scan_image.committed, native + MinSizeOfRelmapUpdate, sizeof(RelMapFile));
	managed_scan_image.pending = managed_scan_image.committed;
	for (int i = 0; i < count; i++)
		managed_scan_image.pending.mappings[i].mapfilenumber += 100;
	INIT_CRC32C(managed_scan_image.pending.crc);
	COMP_CRC32C(managed_scan_image.pending.crc, &managed_scan_image.pending,
				offsetof(RelMapFile, crc));
	FIN_CRC32C(managed_scan_image.pending.crc);
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&managed_scan_file, &managed_scan_image, bytes,
												  sizeof(bytes)));
	append(bytes, sizeof(bytes));
}

UT_TEST(test_managed_relmap_scan_keeps_full_intent_and_both_mapping_sets)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned count = 0; count <= MAX_MAPPINGS; count += MAX_MAPPINGS) {
			ClusterRecoveryNativeScan *scan = NULL;
			ClusterRecoveryNativeEffect effect;
			unsigned ordinal = 0;
			managed_scan_fixture(shared, count);
			UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
			if (!scan)
				continue;
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE);
			UT_ASSERT_EQ(effect.length, CLUSTER_RELMAP_FILE_SIZE_V2);
			UT_ASSERT_EQ(effect.record->database, shared ? 0 : 5);
			UT_ASSERT_EQ(effect.record->nmappings, 2 * count);
			UT_ASSERT(memcmp(effect.bytes, payload.bytes,
							 sizeof(unsigned char) * CLUSTER_RELMAP_FILE_SIZE_V2)
					  == 0);
			while (cluster_recovery_native_scan_next(scan, &effect)
				   == CLUSTER_RECOVERY_NATIVE_EFFECT) {
				RelMapping mapping;
				UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_RELMAP_ENTRY);
				UT_ASSERT_EQ(effect.ordinal, ordinal);
				memcpy(&mapping, effect.bytes, sizeof(mapping));
				UT_ASSERT_EQ(mapping.mapfilenumber,
							 16385 + ordinal % (count ? count : 1) + (ordinal >= count ? 100 : 0));
				ordinal++;
			}
			UT_ASSERT_EQ(ordinal, 2 * count);
			UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
						 CLUSTER_RECOVERY_NATIVE_UNCHANGED);
			cluster_recovery_native_scan_destroy(scan);
		}
}

UT_TEST(test_managed_relmap_scan_refuses_shape_source_and_file_states)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterRecoveryNativeScan *scan = NULL;
		managed_scan_fixture(false, 2);
		if (fault == 0)
			decoded.main_data_len--;
		if (fault == 1)
			decoded.main_data_len++;
		if (fault == 2)
			payload.bytes[CLUSTER_RELMAP_FILE_SIZE_V2 - 1] ^= 1;
		if (fault == 3) {
			managed_scan_image.pending_owner.wal_end = 0x4000;
			UT_ASSERT(cluster_relmap_file_encode_v2(&managed_scan_file, &managed_scan_image,
													payload.bytes, CLUSTER_RELMAP_FILE_SIZE_V2));
		}
		if (fault >= 4 && fault <= 8) {
			if (fault == 4)
				managed_scan_file.redo_timeline++;
			if (fault == 5)
				managed_scan_file.storage_uuid[1]++;
			if (fault == 6)
				managed_scan_image.pending_owner.node++;
			if (fault == 7) {
				managed_scan_file.system_identifier++;
				managed_scan_image.identity.system_identifier++;
			}
			if (fault == 8) {
				managed_scan_file.database_incarnation++;
				managed_scan_image.identity.database_incarnation++;
			}
			UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&managed_scan_file, &managed_scan_image,
														  payload.bytes,
														  CLUSTER_RELMAP_FILE_SIZE_V2));
		}
		if (fault == 9)
			cut.scan_end_exclusive--;
		if (fault == 10)
			decoded.max_block_id = 0;
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_managed_relmap_scan_rechecks_complete_identity_and_inactive_bytes)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoveryNativeScan *scan = NULL;
		ClusterRecoveryNativeEffect effect;
		managed_scan_fixture(false, 2);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		while (cluster_recovery_native_scan_next(scan, &effect) == CLUSTER_RECOVERY_NATIVE_EFFECT) {
		}
		if (fault == 0)
			payload.bytes[CLUSTER_RELMAP_FILE_SIZE_V2 - 1] ^= 1;
		if (fault == 1) {
			managed_scan_file.creation_generation++;
			UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&managed_scan_file, &managed_scan_image,
														  payload.bytes,
														  CLUSTER_RELMAP_FILE_SIZE_V2));
		}
		if (fault == 2)
			source.claim.identity.root_lineage_seq++;
		UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
					 CLUSTER_RECOVERY_NATIVE_CHANGED);
		cluster_recovery_native_scan_destroy(scan);
	}
}

UT_TEST(test_managed_relmap_scan_keeps_unequal_counts_and_atomic_effect_budget)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effects[5];
	size_t count = 0;
	managed_scan_fixture(false, 0);
	managed_scan_image.pending.num_mappings = 2;
	for (int i = 0; i < 2; i++)
		managed_scan_image.pending.mappings[i] = (RelMapping){ 1259 + i, 16585 + i };
	INIT_CRC32C(managed_scan_image.pending.crc);
	COMP_CRC32C(managed_scan_image.pending.crc, &managed_scan_image.pending,
				offsetof(RelMapFile, crc));
	FIN_CRC32C(managed_scan_image.pending.crc);
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&managed_scan_file, &managed_scan_image,
												  payload.bytes, CLUSTER_RELMAP_FILE_SIZE_V2));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	memset(effects, 0x59, sizeof(effects));
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, effects, 2, &count),
				 CLUSTER_RECOVERY_NATIVE_CAPACITY);
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT_EQ(((unsigned char *)effects)[0], 0x59);
	UT_ASSERT_EQ(cluster_recovery_native_scan_effects_v1(scan, effects, 5, &count),
				 CLUSTER_RECOVERY_NATIVE_EFFECT);
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT_EQ(effects[1].ordinal, 0);
	UT_ASSERT_EQ(effects[2].ordinal, 1);
	cluster_recovery_native_scan_destroy(scan);
}
