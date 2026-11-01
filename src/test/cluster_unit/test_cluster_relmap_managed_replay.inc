/*-------------------------------------------------------------------------
 * test_cluster_relmap_managed_replay.inc
 *    Bound WAL_INTENT through original recovery custody and real pair I/O.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_managed_replay.inc
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_relmap_recovery_internal.h"
#include "cluster/cluster_wal_source.h"
#include "utils/relmapper.h"

static ClusterRelmapRecoveryPins replay_pins;
static ClusterRelmapV2Image replay_image;
static ClusterWalSourceRef replay_source;
static XLogReaderState replay_reader;
static DecodedXLogRecord replay_record;
static unsigned char replay_intent[CLUSTER_RELMAP_FILE_SIZE_V2];

static void
managed_replay_setup(ClusterRecoveryFileIdentityV2 *id)
{
	ClusterRelmapV2Image intent;
	managed_setup(id); managed_locks();
	replay_image = managed_staged();
	replay_image.pending_owner.wal_end = 8192;
	intent = replay_image; intent.pending_owner.wal_end = 0;
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(id, &intent,
		replay_intent, sizeof(replay_intent)));
	memset(&replay_source, 0, sizeof(replay_source));
	replay_source.claim.identity.system_identifier = id->system_identifier;
	memcpy(replay_source.claim.identity.storage_uuid, id->storage_uuid, 16);
	replay_source.claim.identity.authority_uuid[0] = 7;
	replay_source.claim.identity.origin_node_id = owner.node;
	replay_source.claim.identity.origin_thread_id = owner.node + 1;
	replay_source.claim.identity.origin_owner_incarnation = owner.admitted_incarnation;
	replay_source.claim.identity.thread_claim_created_at = 17;
	replay_source.claim.identity.root_lineage_seq = 1;
	replay_source.claim.database_incarnation = id->database_incarnation;
	replay_source.claim.max_config_generation = 1;
	replay_source.claim.claim_sha256[0] = 5;
	replay_source.timeline = 4;
	memset(&replay_record, 0, sizeof(replay_record));
	replay_record.lsn = 4096; replay_record.next_lsn = 8192;
	replay_record.header.xl_tot_len = SizeOfXLogRecord + sizeof(replay_intent) + 5;
	replay_record.header.xl_rmid = RM_RELMAP_ID;
	replay_record.header.xl_info = XLOG_RELMAP_UPDATE_V2;
	replay_record.header.xl_xid = (uint32)owner.top_full_xid;
	replay_record.header.xl_crc = 91;
	replay_record.max_block_id = -1;
	replay_record.main_data = (char *)replay_intent;
	replay_record.main_data_len = sizeof(replay_intent);
	memset(&replay_reader, 0, sizeof(replay_reader));
	replay_reader.record = &replay_record;
	replay_reader.ReadRecPtr = replay_record.lsn;
	replay_reader.EndRecPtr = replay_record.next_lsn;
	replay_reader.system_identifier = id->system_identifier;
	replay_reader.cluster_expected_thread_id = owner.node + 1;
	replay_reader.seg.ws_tli = replay_source.timeline;
	memset(&replay_pins, 0, sizeof(replay_pins));
	replay_pins.state = CLUSTER_RELMAP_RETENTION_BUILDING;
	replay_pins.expected = replay_pins.count = 1;
	replay_pins.restored = true;
	replay_pins.images[0] = replay_image;
}

static bool
managed_replay(const ClusterRecoveryFileIdentityV2 *id)
{
	return cluster_relmap_authority_managed_replay_v2(id, &replay_image,
		&replay_source, &replay_reader, &replay_pins);
}

UT_TEST(managed_replay_rebuilds_actual_end_and_stages_exact_pair_once)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2];
	managed_replay_setup(&id);
	/* The file creator is NOT the current WAL writer. */
	UT_ASSERT_NE(id.origin, replay_image.pending_owner.node);
	UT_ASSERT(managed_replay(&id));
	UT_ASSERT(replay_pins.attempted[0] && replay_pins.staged[0]);
	UT_ASSERT(!replay_pins.transferred && replay_pins.restored);
	UT_ASSERT(cluster_relmap_file_encode_v2(&id, &replay_image, expected, sizeof(expected)));
	UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
	UT_ASSERT(memcmp(pair.bytes[0], expected, sizeof(expected)) == 0);
	UT_ASSERT(memcmp(pair.bytes[1], expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(pair.view.committed_generation, 7);
	UT_ASSERT_EQ(pair.view.pending_owner.wal_end, replay_reader.EndRecPtr);
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
	reset_io(); managed_locks();
	UT_ASSERT(!managed_replay(&id));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(reads, 0);
	cleanup();
}

UT_TEST(managed_replay_refuses_old_wal_and_malformed_native_frame)
{
	ClusterRecoveryFileIdentityV2 id;
	unsigned char legacy[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
	xl_relmap_update old_header = {InvalidOid, GLOBALTABLESPACE_OID, sizeof(RelMapFile)};
	RelMapFile old_map = map_image(40001);
	memcpy(legacy, &old_header, MinSizeOfRelmapUpdate);
	memcpy(legacy + MinSizeOfRelmapUpdate, &old_map, sizeof(old_map));
	for (int fault = 0; fault < 15; ++fault) {
		managed_replay_setup(&id);
		switch (fault) {
			case 0:
				replay_record.header.xl_info = XLOG_RELMAP_UPDATE;
				replay_record.main_data = (char *)legacy;
				replay_record.main_data_len = sizeof(legacy);
				replay_record.header.xl_tot_len = SizeOfXLogRecord + sizeof(legacy) + 5;
				break;
			case 1: replay_record.header.xl_rmid = RM_XLOG_ID; break;
			case 2: replay_record.main_data_len--; break;
			case 3: replay_reader.EndRecPtr++; break;
			case 4: replay_reader.EndRecPtr = 0; break;
			case 5: replay_record.next_lsn++; break;
			case 6: replay_record.lsn++; break;
			case 7: replay_record.header.xl_tot_len = 1; break;
			case 8: replay_record.header.xl_xid++; break;
			case 9: replay_record.toplevel_xid = replay_record.header.xl_xid + 1; break;
			case 10: replay_record.record_origin = 1; break;
			case 11: replay_record.max_block_id = 0; break;
			case 12: replay_record.has_page_version_edge = true; break;
			case 13: replay_intent[200] ^= 1; break;
			case 14: replay_record.main_data = NULL; break;
		}
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT(!replay_pins.staged[0] && !replay_pins.transferred);
		cleanup();
	}
}

UT_TEST(managed_replay_requires_independent_binding_source_and_exact_custody)
{
	ClusterRecoveryFileIdentityV2 id;
	for (int fault = 0; fault < 12; ++fault) {
		managed_replay_setup(&id);
		switch (fault) {
			case 0: id.creation_generation++; break;
			case 1: id.registration_generation++; break;
			case 2: id.origin++; break;
			case 3: replay_source.claim.identity.origin_owner_incarnation++; break;
			case 4: replay_reader.cluster_expected_thread_id++; break;
			case 5: replay_reader.seg.ws_tli++; break;
			case 6: replay_source.claim.database_incarnation++; break;
			case 7: replay_source.claim.identity.storage_uuid[0]++; break;
			case 8: replay_pins.images[0].pending = map_image(40002); break;
			case 9: replay_pins.restored = false; break;
			case 10: replay_pins.transferred = true; break;
			case 11: replay_pins.count = 0; break;
		}
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(reads, 0);
		cleanup();
	}
}

UT_TEST(managed_replay_never_uses_current_pair_to_supply_wal_history)
{
	ClusterRecoveryFileIdentityV2 id;
	unsigned char file[CLUSTER_RELMAP_FILE_SIZE_V2];
	for (int fault = 0; fault < 3; ++fault) {
		managed_replay_setup(&id);
		if (fault == 0) {
			UT_ASSERT(cluster_relmap_file_encode_v2(&id, &replay_image, file, sizeof(file)));
			memcpy(replay_intent, file, sizeof(file)); /* FILE_PENDING is not WAL_INTENT. */
		} else if (fault == 1) {
			replay_image.committed = map_image(40002);
			replay_pins.images[0] = replay_image;
		} else {
			ClusterRelmapV2Image base = replay_image;
			base.pending_generation = 0;
			memset(&base.pending_owner, 0, sizeof(base.pending_owner));
			memset(&base.pending, 0, sizeof(base.pending));
			base.committed = map_image(40003);
			UT_ASSERT(cluster_relmap_file_encode_v2(&id, &base, file, sizeof(file)));
			write_copy(0, file, sizeof(file)); write_copy(1, file, sizeof(file));
			reset_io(); managed_locks();
		}
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(!replay_pins.staged[0] && replay_pins.count == 1);
		cleanup();
	}
}

UT_TEST(managed_replay_requires_original_serialization_and_sync)
{
	ClusterRecoveryFileIdentityV2 id;
	for (int fault = 0; fault < 3; ++fault) {
		managed_replay_setup(&id);
		if (fault == 0) mapping_held = false;
		if (fault == 1) relmap_x_held = false;
		if (fault == 2) enableFsync = false;
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(reads, 0);
		UT_ASSERT(!replay_pins.staged[0]);
		cleanup();
	}
}

UT_TEST(managed_replay_partial_failures_keep_debt_and_refuse_retry)
{
	ClusterRecoveryFileIdentityV2 id;
	for (int fault = 0; fault < 7; ++fault) {
		managed_replay_setup(&id); managed_fault(fault);
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT(replay_pins.attempted[0] && !replay_pins.staged[0]);
		UT_ASSERT(replay_pins.restored && !replay_pins.transferred && replay_pins.count == 1);
		reset_io(); managed_locks();
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(reads, 0);
		cleanup();
	}
}

static int replay_late_fault;
static void
managed_replay_drift_after_sync(bool is_directory)
{
	if (is_directory) return;
	switch (replay_late_fault) {
		case 0: replay_source.claim.max_config_generation++; break;
		case 1: replay_intent[200] ^= 1; break;
		case 2: replay_reader.EndRecPtr++; break;
		case 3: replay_pins.images[0].pending_generation++; break;
		case 4: mapping_held = false; break;
		case 5: relmap_x_held = false; break;
		case 7: replay_record.header.xl_crc++; break;
	}
}

UT_TEST(managed_replay_rechecks_whole_original_input_before_each_install)
{
	ClusterRecoveryFileIdentityV2 id;
	for (replay_late_fault = 0; replay_late_fault < 8; ++replay_late_fault) {
		managed_replay_setup(&id);
		if (replay_late_fault == 6) {
			changing_registration = &id;
			post_sync_hook = change_registration_after_file_sync;
		} else post_sync_hook = managed_replay_drift_after_sync;
		UT_ASSERT(!managed_replay(&id));
		UT_ASSERT_EQ(renames, 0);
		UT_ASSERT(replay_pins.attempted[0] && !replay_pins.staged[0]);
		cleanup();
	}
}
