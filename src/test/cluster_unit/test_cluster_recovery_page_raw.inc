/* Complete original native PAGE records, not reconstructed decoded images.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_page_raw_keeps_full_original_file_bytes_after_both_passes)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterWalInputsV1 *inputs;
	unsigned calls;
	inventory_extra = 17; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	calls = inventory_census_calls; UT_ASSERT_EQ(calls, 2);
	for (uint32 i = 0; i < 2; i++) {
		ClusterRecoveryPageWalV1 out, again;
		uint8 bytes[8192], digest[32]; char path[MAXPGPATH]; int fd;
		XLogReaderState *reader; char *error = NULL;
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, i, &out));
		UT_ASSERT(out.raw_record != NULL);
		if (out.raw_record == NULL || out.raw_length > sizeof(bytes)) continue;
		UT_ASSERT_EQ(out.raw_length, out.reader->record->header.xl_tot_len);
		v2_checkpoint_wal_path(&out.source.claim.identity, out.reader->ReadRecPtr, out.source.timeline, path);
		fd = open(path, O_RDONLY); UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, bytes, out.raw_length, out.reader->ReadRecPtr % wal_segment_size), out.raw_length);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT(memcmp(out.raw_record, bytes, out.raw_length) == 0);
		UT_ASSERT(memcmp(bytes, &out.reader->record->header, SizeOfXLogRecord) == 0);
		UT_ASSERT(cluster_recovery_native_raw_sha256_v1(bytes, out.raw_length, digest));
		UT_ASSERT(memcmp(digest, out.raw_record_sha256, 32) == 0);
		reader = raw_test_reader(&out.source, out.reader->ReadRecPtr); UT_ASSERT(reader != NULL);
		UT_ASSERT(XLogReadRecord(reader, &error) != NULL);
		UT_ASSERT(memcmp(XLogReaderCurrentRawRecord(reader), out.raw_record, out.raw_length) == 0);
		XLogReaderFree(reader);
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, i, &again));
		UT_ASSERT(memcmp(&out, &again, sizeof(out)) == 0);
	}
	UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect), CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	UT_ASSERT(inputs_pin_held);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs); inventory_extra = 0;
}

UT_TEST(test_page_raw_borrowed_byte_change_revokes_whole_inventory_without_rebase)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryPageWalV1 out, zero = {0};
		ClusterWalInputsV1 *inputs; uint8 *raw; uint8 saved; uint32 count = 99;
		inventory_extra = 17; inputs = inventory_fixture(false, false);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, 1, &out));
		UT_ASSERT(out.raw_record != NULL);
		if (out.raw_record != NULL) {
			raw = (uint8 *)out.raw_record + (fault ? out.raw_length - 1 : 0); saved = *raw; *raw ^= 1;
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(!cluster_recovery_native_inventory_page_at_v1(inventory, 0, &out));
			UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
			UT_ASSERT(inputs_pin_held); *raw = saved;
			UT_ASSERT(!cluster_recovery_native_inventory_page_count_v1(inventory, &count));
			UT_ASSERT_EQ(count, 0);
		}
		cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs); inventory_extra = 0;
	}
}

UT_TEST(test_page_raw_complete_copy_is_charged_before_any_prefix_publication)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterWalInputsV1 *inputs; size_t budget, raw_bytes = 0;
	inventory_extra = 17; inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	budget = diag.owned_bytes;
	for (uint32 i = 0; i < 2; i++) {
		ClusterRecoveryPageWalV1 out;
		UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, i, &out));
		raw_bytes += MAXALIGN(out.raw_length);
	}
	UT_ASSERT(raw_bytes >= 2 * SizeOfXLogRecord);
	cluster_recovery_native_inventory_destroy(inventory); inventory = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, budget - raw_bytes, &inventory, &diag),
		CLUSTER_RECOVERY_NATIVE_INVENTORY_CAPACITY);
	UT_ASSERT_NULL(inventory); cold_census_done(&inputs);
	inputs = inventory_fixture(false, false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, budget, &inventory, &diag), 0);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs); inventory_extra = 0;
}

UT_TEST(test_page_raw_original_census_keeps_all_sources_and_rejects_changed_whole_view)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH]; uint32 count = 0;
	ClusterRecoveryCensus *census = page_census_fixture(&inputs, &fd, path, true);
	ClusterRecoveryCensusManifest before, after;
	ClusterRecoveryPageWalV1 out, zero = {0};
	unsigned calls; uint8 *raw = NULL;
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(cluster_recovery_census_manifest(census, &before)); calls = inventory_census_calls;
	UT_ASSERT(cluster_recovery_census_page_wal_count_v1(census, &count)); UT_ASSERT_EQ(count, 2);
	for (uint32 i = 0; i < count; i++) {
		uint8 bytes[8192]; char walpath[MAXPGPATH]; int wal;
		UT_ASSERT(cluster_recovery_census_page_wal_at_v1(census, i, &out));
		UT_ASSERT(out.raw_record != NULL);
		if (out.raw_record == NULL || out.raw_length > sizeof(bytes)) continue;
		v2_checkpoint_wal_path(&out.source.claim.identity, out.reader->ReadRecPtr, out.source.timeline, walpath);
		wal = open(walpath, O_RDONLY); UT_ASSERT(wal >= 0);
		UT_ASSERT_EQ(pread(wal, bytes, out.raw_length, out.reader->ReadRecPtr % wal_segment_size), out.raw_length);
		UT_ASSERT_EQ(close(wal), 0);
		UT_ASSERT(memcmp(bytes, out.raw_record, out.raw_length) == 0);
		raw = (uint8 *)out.raw_record + out.raw_length - 1;
	}
	UT_ASSERT(cluster_recovery_census_manifest(census, &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(inventory_census_calls, calls);
	if (raw != NULL) {
		*raw ^= 1; memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_recovery_census_page_wal_at_v1(census, 0, &out));
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		/* census_fail owns and destroys its invalid inventory; do not reuse raw. */
		UT_ASSERT(inputs_pin_held && fcntl(fd, F_GETFD) >= 0);
		UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(!cluster_recovery_census_manifest(census, &after));
	}
	page_census_done(census, &inputs, fd, path);
}
