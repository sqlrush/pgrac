/* Full native relmap images and bounded original mapping entries.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_recovery_relmap_fixture.h"

static void
native_relmap_fixture(bool shared, int count)
{
	char bytes[TEST_RELMAP_LENGTH];
	fixture(RM_RELMAP_ID, XLOG_RELMAP_UPDATE);
	test_relmap_payload(bytes, shared, count);
	append(bytes, sizeof(bytes));
}

UT_TEST(test_relmap_owns_empty_full_image_and_every_mapping)
{
	for (unsigned shared = 0; shared < 2; ++shared)
		for (unsigned count = 0; count <= MAX_MAPPINGS; count += MAX_MAPPINGS) {
			ClusterRecoveryNativeScan *scan = NULL;
			ClusterRecoveryNativeEffect effect;
			ClusterWalSourceRef expected;
			char saved[TEST_RELMAP_LENGTH];
			native_relmap_fixture(shared, count);
			expected = source;
			memcpy(saved, payload.bytes, sizeof(saved));
			UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
			if (scan == NULL)
				continue;
			memset(&payload, 0, sizeof(payload));
			memset(&reader, 0, sizeof(reader));
			memset(&source, 0, sizeof(source));
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_EFFECT);
			UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE);
			UT_ASSERT_EQ(effect.length, sizeof(saved));
			UT_ASSERT(memcmp(effect.bytes, saved, sizeof(saved)) == 0);
			UT_ASSERT(memcmp(&effect.record->source, &expected, sizeof(expected)) == 0);
			UT_ASSERT_EQ(effect.record->database, shared ? 0 : 5);
			UT_ASSERT_EQ(effect.record->tablespace, shared ? 1664 : 1663);
			UT_ASSERT_EQ(effect.record->nmappings, count);
			UT_ASSERT_EQ(effect.record->terminal_scn, 0);
			for (unsigned i = 0; i < count; ++i) {
				RelMapping mapping;
				UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
							 CLUSTER_RECOVERY_NATIVE_EFFECT);
				UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_RELMAP_ENTRY);
				UT_ASSERT_EQ(effect.ordinal, i);
				UT_ASSERT_EQ(effect.length, sizeof(mapping));
				UT_ASSERT(effect.bytes
						  == effect.record->payload + effect.record->mappings_offset
								 + i * sizeof(mapping));
				memcpy(&mapping, effect.bytes, sizeof(mapping));
				UT_ASSERT_EQ(mapping.mapoid, 1247 + i);
				UT_ASSERT_EQ(mapping.mapfilenumber, 16385 + i);
			}
			UT_ASSERT_EQ(cluster_recovery_native_scan_next(scan, &effect),
						 CLUSTER_RECOVERY_NATIVE_END);
			UT_ASSERT(effect.record == NULL);
			cluster_recovery_native_scan_destroy(scan);
		}
}

UT_TEST(test_relmap_rejects_every_truncation_and_bad_image)
{
	for (unsigned length = 0; length < TEST_RELMAP_LENGTH; ++length) {
		ClusterRecoveryNativeScan *scan = NULL;
		native_relmap_fixture(false, 1);
		decoded.main_data_len = length;
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
	for (unsigned fault = 0; fault < 11; ++fault) {
		ClusterRecoveryNativeScan *scan = NULL;
		int32 value;
		native_relmap_fixture(false, 1);
		switch (fault) {
		case 0:
			decoded.main_data_len++;
			break;
		case 1:
			decoded.header.xl_info = 0x10;
			break;
		case 2:
			value = -1;
			memcpy(payload.bytes + offsetof(xl_relmap_update, nbytes), &value, 4);
			break;
		case 3:
			value = sizeof(RelMapFile) - 1;
			memcpy(payload.bytes + offsetof(xl_relmap_update, nbytes), &value, 4);
			break;
		case 4:
			value = 0;
			memcpy(payload.bytes + MinSizeOfRelmapUpdate, &value, 4);
			test_relmap_crc(payload.bytes);
			break;
		case 5:
		case 6:
			value = fault == 5 ? -1 : MAX_MAPPINGS + 1;
			memcpy(payload.bytes + MinSizeOfRelmapUpdate + offsetof(RelMapFile, num_mappings),
				   &value, 4);
			test_relmap_crc(payload.bytes);
			break;
		case 7:
			payload.bytes[TEST_RELMAP_LENGTH - 1] ^= 1;
			break;
		case 8:
			value = 0;
			memcpy(payload.bytes, &value, 4);
			break;
		case 9:
			value = GLOBALTABLESPACE_OID;
			memcpy(payload.bytes + 4, &value, 4);
			break;
		case 10:
			value = 9999;
			memcpy(payload.bytes + 4, &value, 4);
			break;
		}
		UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_relmap_preserves_original_scope_budget_and_block_gates)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		ClusterRecoveryNativeScan *scan = NULL;
		size_t size;
		native_relmap_fixture(true, 2);
		UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
		if (scan == NULL)
			continue;
		size = cluster_recovery_native_scan_bytes(scan);
		cluster_recovery_native_scan_destroy(scan);
		scan = NULL;
		if (fault == 0)
			UT_ASSERT_EQ(
				cluster_recovery_native_scan_begin(&reader, &source, &cut, size - 1, &scan),
				CLUSTER_RECOVERY_NATIVE_CAPACITY);
		else {
			if (fault == 1)
				cut.failed_thread++;
			if (fault == 2)
				cut.scan_end_exclusive--;
			if (fault == 3)
				decoded.max_block_id = 0;
			if (fault == 4)
				decoded.has_page_version_edge = true;
			if (fault == 5)
				source.claim.identity.system_identifier = 0;
			UT_ASSERT(begin(&scan) != CLUSTER_RECOVERY_NATIVE_MORE);
		}
		UT_ASSERT(scan == NULL);
	}
}

UT_TEST(test_relmap_recheck_retains_inactive_bytes_and_is_sticky)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	char saved[TEST_RELMAP_LENGTH];
	native_relmap_fixture(false, 0);
	memcpy(saved, payload.bytes, sizeof(saved));
	UT_ASSERT_EQ(begin(&scan), CLUSTER_RECOVERY_NATIVE_MORE);
	if (scan == NULL)
		return;
	while (cluster_recovery_native_scan_next(scan, &effect) == CLUSTER_RECOVERY_NATIVE_EFFECT) {}
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
				 CLUSTER_RECOVERY_NATIVE_UNCHANGED);
	payload.bytes[MinSizeOfRelmapUpdate + offsetof(RelMapFile, mappings) + 13] ^= 1;
	test_relmap_crc(payload.bytes);
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
				 CLUSTER_RECOVERY_NATIVE_CHANGED);
	memcpy(payload.bytes, saved, sizeof(saved));
	UT_ASSERT_EQ(cluster_recovery_native_scan_recheck(scan, &reader, &source, &cut),
				 CLUSTER_RECOVERY_NATIVE_CHANGED);
	UT_ASSERT_EQ(cluster_recovery_native_scan_primary(scan, &effect),
				 CLUSTER_RECOVERY_NATIVE_CHANGED);
	UT_ASSERT(effect.record == NULL && effect.bytes == NULL);
	cluster_recovery_native_scan_destroy(scan);
}
