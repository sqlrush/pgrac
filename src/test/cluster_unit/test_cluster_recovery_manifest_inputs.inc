/* Complete cold observation identities, never semantic recovery authority.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */

static bool
manifest_zero(const ClusterRecoveryCensusManifest *value)
{
	ClusterRecoveryCensusManifest zero = { 0 };
	return memcmp(value, &zero, sizeof(zero)) == 0;
}

UT_TEST(test_census_manifest_requires_original_final_reads)
{
	ClusterWalInputsV1 *inputs;
	char path[MAXPGPATH];
	int fd;
	ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
	ClusterRecoveryCensusManifest first, second;
	ClusterRecoveryCensusDiag diag;
	uint8 zero[32] = { 0 };
	bool available;

	memset(&first, 0x55, sizeof(first));
	UT_ASSERT(!cluster_recovery_census_manifest(census, &first));
	UT_ASSERT(manifest_zero(&first));
	UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_WAL),
				 CLUSTER_RECOVERY_CENSUS_MORE);
	UT_ASSERT(!cluster_recovery_census_manifest(census, &first));
	UT_ASSERT(manifest_zero(&first));
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	available = cluster_recovery_census_manifest(census, &first);
	UT_ASSERT(available);
	if (available) {
		UT_ASSERT(cluster_recovery_census_diag(census, &diag));
		UT_ASSERT_EQ(first.encoding_version, 1);
		UT_ASSERT_EQ(first.source_count, 2);
		UT_ASSERT_EQ(first.root_count, cluster_recovery_root_set_count(census->roots));
		UT_ASSERT_EQ(first.tx_record_count, 2);
		UT_ASSERT_EQ(first.wal_record_count, diag.inventory.records);
		UT_ASSERT_EQ(first.native_effect_count, diag.effects);
		UT_ASSERT_EQ(first.other_wal_record_count, diag.inventory.other_records);
		UT_ASSERT_EQ(first.unowned_target_record_count, diag.inventory.unowned_target_records);
		UT_ASSERT(first.other_wal_record_count != 0); /* still not semantic completion */
		UT_ASSERT(first.unowned_target_record_count != 0);
		UT_ASSERT(memcmp(first.inputs_sha256, zero, 32) != 0);
		UT_ASSERT(memcmp(first.roots_sha256, zero, 32) != 0);
		UT_ASSERT(memcmp(first.observation_sha256, zero, 32) != 0);
		UT_ASSERT(cluster_recovery_census_manifest(census, &second));
		UT_ASSERT_EQ(memcmp(&first, &second, sizeof(first)), 0);
		UT_ASSERT_EQ(inventory_census_calls, 3); /* export adds no new WAL scan */
	}
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_inventory_digest_binds_every_source_cut_tail_and_transcript)
{
	ClusterWalInputsV1 *inputs = inventory_fixture(false, false);
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	NativeInventorySource saved, swap;
	uint8 baseline[32], changed[32];
	bool available;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag),
				 CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	available = cluster_recovery_native_inventory_digest(inventory, baseline);
	UT_ASSERT(available);
	if (available) {
		saved = inventory->sources[0];
#define MANIFEST_CHANGE(field_)                                                                    \
	do {                                                                                           \
		inventory->sources[0].field_++;                                                            \
		UT_ASSERT(cluster_recovery_native_inventory_digest(inventory, changed));                   \
		UT_ASSERT(memcmp(baseline, changed, 32) != 0);                                             \
		inventory->sources[0] = saved;                                                             \
	} while (0)
		MANIFEST_CHANGE(source.claim.identity.system_identifier);
		MANIFEST_CHANGE(source.claim.identity.storage_uuid[15]);
		MANIFEST_CHANGE(source.claim.identity.authority_uuid[15]);
		MANIFEST_CHANGE(source.claim.identity.origin_thread_id);
		MANIFEST_CHANGE(source.claim.identity.reserved42);
		MANIFEST_CHANGE(source.claim.identity.origin_node_id);
		MANIFEST_CHANGE(source.claim.identity.thread_claim_created_at);
		MANIFEST_CHANGE(source.claim.identity.thread_claim_crc32c);
		MANIFEST_CHANGE(source.claim.identity.reserved60);
		MANIFEST_CHANGE(source.claim.identity.origin_owner_incarnation);
		MANIFEST_CHANGE(source.claim.identity.root_lineage_seq);
		MANIFEST_CHANGE(source.claim.database_incarnation);
		MANIFEST_CHANGE(source.claim.max_config_generation);
		MANIFEST_CHANGE(source.claim.claim_sha256[31]);
		MANIFEST_CHANGE(source.timeline);
		MANIFEST_CHANGE(cut.failed_thread);
		MANIFEST_CHANGE(cut.flags);
		MANIFEST_CHANGE(cut.timeline_id);
		MANIFEST_CHANGE(cut.scan_begin_inclusive);
		MANIFEST_CHANGE(cut.scan_end_exclusive);
		MANIFEST_CHANGE(cut.contributor_count);
		MANIFEST_CHANGE(cut.component_count);
		MANIFEST_CHANGE(cut.origin_owner_incarnation);
		MANIFEST_CHANGE(observed.complete_end);
		MANIFEST_CHANGE(observed.last_record_start);
		MANIFEST_CHANGE(observed.last_record_crc);
		MANIFEST_CHANGE(observed.records);
		MANIFEST_CHANGE(observed.database_incarnation);
		MANIFEST_CHANGE(transcript[0]);
		MANIFEST_CHANGE(transcript[31]);
#undef MANIFEST_CHANGE
		swap = inventory->sources[1];
		inventory->sources[0] = swap;
		inventory->sources[1] = saved;
		UT_ASSERT(cluster_recovery_native_inventory_digest(inventory, changed));
		UT_ASSERT(memcmp(baseline, changed, 32) != 0);
		inventory->sources[0] = saved;
		inventory->sources[1] = swap;
		UT_ASSERT(cluster_recovery_native_inventory_digest(inventory, changed));
		UT_ASSERT_EQ(memcmp(baseline, changed, 32), 0);
		/* The digest does not drain or rewind original native effects. */
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
		UT_ASSERT(cluster_recovery_native_inventory_digest(inventory, changed));
		UT_ASSERT_EQ(memcmp(baseline, changed, 32), 0);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
		UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION);
		inventory_fault = 0;
		inventory_drift();
		UT_ASSERT_EQ(cluster_recovery_native_inventory_recheck(inputs, inventory, &diag),
					 CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED);
		memset(changed, 0x55, sizeof(changed));
		UT_ASSERT(!cluster_recovery_native_inventory_digest(inventory, changed));
		for (unsigned i = 0; i < 32; ++i)
			UT_ASSERT_EQ(changed[i], 0);
	}
	cluster_recovery_native_inventory_destroy(inventory);
	cold_census_done(&inputs);
}

UT_TEST(test_census_manifest_revokes_after_scope_or_dependency_loss)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		ClusterRecoveryCensusManifest manifest;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		UT_ASSERT(cluster_recovery_census_manifest(census, &manifest));
		if (mode == 0)
			inputs_pin_current = false;
		else if (mode == 1)
			cluster_recovery_root_set_invalidate(census->roots);
		else
			cluster_recovery_reference_graph_invalidate(census->graph);
		memset(&manifest, 0x44, sizeof(manifest));
		UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
		UT_ASSERT(manifest_zero(&manifest));
		inputs_pin_current = true;
		UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
		UT_ASSERT(manifest_zero(&manifest));
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_census_manifest_final_changes_never_export_partial)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterWalInputsV1 *inputs;
		char path[MAXPGPATH];
		int fd;
		ClusterRecoveryCensus *census = owned_census_fixture(&inputs, &fd, path, false);
		ClusterRecoveryCensusManifest manifest;
		UT_ASSERT_EQ(owned_census_until(census, CLUSTER_RECOVERY_CENSUS_FINAL_TT),
					 CLUSTER_RECOVERY_CENSUS_MORE);
		if (mode == 0) {
			int file = openat(fd, "pg_undo/instance_0/seg_1.dat", O_WRONLY);
			uint32 value = 12345;
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(pwrite(file, &value, sizeof(value),
								offsetof(UndoSegmentHeaderData, tt_slots) + offsetof(TTSlot, xid)),
						 sizeof(value));
			UT_ASSERT_EQ(close(file), 0);
		} else if (mode == 1) {
			int file = openat(fd, "unowned", O_WRONLY);
			UT_ASSERT(file >= 0);
			UT_ASSERT_EQ(pwrite(file, "changed", 7, 0), 7);
			UT_ASSERT_EQ(close(file), 0);
		} else {
			inventory_fault = 0;
			inventory_drift();
		}
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_REFUSED);
		memset(&manifest, 0x11, sizeof(manifest));
		UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
		UT_ASSERT(manifest_zero(&manifest));
		owned_census_done(census, &inputs, fd, path);
	}
}

/* Encoding-only vector; it supplies no cold identity or completion proof. */
UT_TEST(test_inventory_digest_canonical_sha256_and_failed_output)
{
	static const uint8 expected[32]
		= { 0x9e, 0x30, 0x7a, 0xca, 0xce, 0x14, 0x35, 0x36, 0x37, 0x5b, 0x6c,
			0xba, 0x14, 0x3d, 0x37, 0x2d, 0xcf, 0xa4, 0x8f, 0xcc, 0x50, 0xe5,
			0x84, 0x4f, 0xe0, 0xd0, 0xec, 0xe1, 0xa8, 0x9f, 0x1b, 0x08 };
	NativeInventorySource source = { 0 };
	ClusterRecoveryNativeInventory inventory = { 0 };
	uint8 digest[32], second[32], zero[32] = { 0 };
	inventory.sources = &source;
	inventory.source_count = 1;
	inventory.complete = true;
	inventory.status = CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED;
	UT_ASSERT(cluster_recovery_native_inventory_digest(&inventory, digest));
	UT_ASSERT_EQ(memcmp(digest, expected, 32), 0);
	/* The source ref's trailing compiler padding is not encoded. */
	memset(
		(char *)&source.source + offsetof(ClusterWalSourceRef, timeline) + sizeof(TimeLineID), 0x9a,
		sizeof(ClusterWalSourceRef) - offsetof(ClusterWalSourceRef, timeline) - sizeof(TimeLineID));
	UT_ASSERT(cluster_recovery_native_inventory_digest(&inventory, second));
	UT_ASSERT_EQ(memcmp(digest, second, 32), 0);
	source.cut.flags = RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	UT_ASSERT(cluster_recovery_native_inventory_digest(&inventory, second));
	UT_ASSERT(memcmp(digest, second, 32) != 0);
	inventory.complete = false;
	memset(second, 0x88, sizeof(second));
	UT_ASSERT(!cluster_recovery_native_inventory_digest(&inventory, second));
	UT_ASSERT_EQ(memcmp(second, zero, 32), 0);
	inventory.complete = true;
	inventory.status = CLUSTER_RECOVERY_NATIVE_INVENTORY_CHANGED;
	memset(second, 0x88, sizeof(second));
	UT_ASSERT(!cluster_recovery_native_inventory_digest(&inventory, second));
	UT_ASSERT_EQ(memcmp(second, zero, 32), 0);
	UT_ASSERT(!cluster_recovery_native_inventory_digest(NULL, second));
	UT_ASSERT(!cluster_recovery_native_inventory_digest(&inventory, NULL));
	UT_ASSERT(!cluster_recovery_census_manifest(NULL, NULL));
}
