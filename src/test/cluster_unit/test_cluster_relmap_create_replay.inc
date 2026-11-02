/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_replay.inc
 *    Creation record, real replay files and original control receipts.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_replay.inc
 * NOTES
 *    Uses the original controlled creator; no recovery authorization grant.
 *-------------------------------------------------------------------------
 */

#include "common/cryptohash.h"

static bool
creation_replay_record(ClusterControlNativeRelmapCreateV2 *c,
	XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	ClusterRelmapFilePairV2 source;
	bool ok;
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	ok = copy_original_read(&completed[1], &source);
	LWLockRelease(RelationMappingLock);
	if (!ok || !cluster_relmap_create_encode_v1(&c->expected, c->batch_uuid,
		&source.view.committed, wal_bytes, sizeof(wal_bytes)))
		return false;
	creation_native_record(reader, decoded);
	return true;
}

/* Derive both receipts from actual whole copies and ALL original selected
 * source intervals, not an unrelated inventory's saved image digest. The
 * two-source bound here describes only this original fixture. */
static bool
creation_replay_receipts(const ClusterControlNativeRelmapCreateV2 *c,
	const ClusterRelmapFilePairV2 *pair, ClusterRecoveryCheckpointFileV2 receipts[2],
	ClusterRecoveryCheckpointFileSourceV2 vector[2])
{
	ClusterControlRelmapSelectionV2 *selection = NULL;
	ClusterControlRelmapReferenceV2 reference;
	ClusterRecoveryCheckpointSourceV2 source;
	pg_cryptohash_ctx *hash = NULL;
	bool ok = false;
	if (cluster_control_root_relmap_selection_read_locked_v4(&c->control.config,
		CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &selection) != 0)
		return false;
	do {
		if (!cluster_control_root_relmap_selection_reference_v4(selection, &reference)
			|| memcmp(&reference, &c->control, sizeof(reference))
			|| cluster_control_root_relmap_selection_source_count_v4(selection) != 2)
			break;
		for (unsigned i = 0; i < 2; ++i) {
			if (!cluster_control_root_relmap_selection_source_at_v4(selection, i, &source))
				goto done;
			vector[i] = (ClusterRecoveryCheckpointFileSourceV2){i, source.redo_start, source.replay_end};
		}
		memset(receipts, 0, 2 * sizeof(*receipts));
		for (unsigned i = 0; i < 2; ++i) {
			receipts[i].identity = c->expected;
			receipts[i].synced_bytes = sizeof(pair->bytes[i]);
			receipts[i].page_count = 1;
			receipts[i].copy_index = i;
			receipts[i].copy_count = 2;
			receipts[i].source_count = 2;
			receipts[i].sources = vector;
			hash = pg_cryptohash_create(PG_SHA256);
			if (!hash || pg_cryptohash_init(hash) < 0
				|| pg_cryptohash_update(hash, pair->bytes[i], sizeof(pair->bytes[i])) < 0
				|| pg_cryptohash_final(hash, receipts[i].image_digest,
					sizeof(receipts[i].image_digest)) < 0)
				goto done;
			pg_cryptohash_free(hash); hash = NULL;
		}
		ok = cluster_control_root_relmap_selection_recheck_locked_v4(selection) == 0;
	} while (0);
done:
	if (hash) pg_cryptohash_free(hash);
	cluster_control_root_relmap_selection_destroy_v4(&selection);
	return ok;
}

UT_TEST(creation_record_replays_missing_pair_before_original_control_confirmation)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterControlNativeFileBorrowV2 borrow = {0};
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterRecoveryCheckpointFileSourceV2 vector[2];
	ClusterRelmapCreateReplayV1 replay;
	ClusterRelmapFilePairV2 pair, disk;
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	UT_ASSERT(creation_replay_record(c, &reader, &decoded));
	UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(&c->control.config,
		CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, c->startup_generation,
		c->batch_uuid, &c->expected, &borrow), 0);
	UT_ASSERT(cluster_relmap_control_create_redo_input_v1(&reader, &borrow.expected, c->batch_uuid, &replay));
	UT_ASSERT_EQ(replay.actual_end, UINT64_C(0x70000A8));
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	UT_ASSERT(cluster_control_native_file_current_v2(&borrow));
	UT_ASSERT(cluster_relmap_authority_managed_replay_create_v2(&borrow.expected,
		&replay.record.committed, &pair));
	d = target_dir();
	UT_ASSERT(d >= 0 && cluster_relmap_file_pair_read_v2(d, &c->expected, &disk));
	UT_ASSERT(!memcmp(pair.bytes, disk.bytes, sizeof(pair.bytes)));
	UT_ASSERT(!memcmp(pair.bytes[0], wal_bytes + 64, sizeof(pair.bytes[0])));
	UT_ASSERT(!memcmp(pair.bytes[1], wal_bytes + 64, sizeof(pair.bytes[1])));
	UT_ASSERT_EQ(disk.view.committed.mappings[0].mapfilenumber, 40001);
	UT_ASSERT(close(d) == 0 && cluster_control_native_file_current_v2(&borrow));
	UT_ASSERT(cluster_control_native_file_release_v2(&borrow));
	LWLockRelease(RelationMappingLock);
	UT_ASSERT(!target_available() && root_renames == 2 && !c->private_attempted);
	UT_ASSERT(creation_replay_receipts(c, &pair, receipts, vector));
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(c, receipts, 2), 0);
	UT_ASSERT(target_available() && root_renames == 4);
	UT_ASSERT(!wal_begins && !wal_inserts && !wal_flushes && cf_held && copy_x);
	copy_end(&c);
}

UT_TEST(creation_record_exact_pair_replay_preserves_both_inodes_before_confirmation)
{
	ClusterControlNativeRelmapCreateV2 *c = NULL;
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterRecoveryCheckpointFileSourceV2 vector[2];
	ClusterRelmapCreateReplayV1 replay;
	ClusterRelmapFilePairV2 pair;
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	struct stat before[2], after;
	const char *names[2] = {"pgrac_relmap_authority", "pgrac_relmap_authority.bak"};
	int d;
	copy_begin(); UT_ASSERT(reserve_copy(&c));
	UT_ASSERT(creation_replay_record(c, &reader, &decoded));
	UT_ASSERT(cluster_relmap_control_create_redo_input_v1(&reader, &c->expected, c->batch_uuid, &replay));
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	UT_ASSERT(cluster_relmap_authority_managed_replay_create_v2(&c->expected, &replay.record.committed, &pair));
	d = target_dir(); UT_ASSERT(d >= 0);
	for (unsigned i = 0; i < 2; ++i) UT_ASSERT(fstatat(d, names[i], &before[i], 0) == 0);
	for (unsigned again = 0; again < 2; ++again)
		UT_ASSERT(cluster_relmap_authority_managed_replay_create_v2(&c->expected, &replay.record.committed, &pair));
	for (unsigned i = 0; i < 2; ++i) {
		UT_ASSERT(fstatat(d, names[i], &after, 0) == 0);
		UT_ASSERT(after.st_dev == before[i].st_dev && after.st_ino == before[i].st_ino);
		UT_ASSERT(!memcmp(pair.bytes[i], wal_bytes + 64, sizeof(pair.bytes[i])));
	}
	UT_ASSERT(close(d) == 0);
	LWLockRelease(RelationMappingLock);
	UT_ASSERT(!target_available() && root_renames == 2);
	UT_ASSERT(creation_replay_receipts(c, &pair, receipts, vector));
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(c, receipts, 2), 0);
	UT_ASSERT(target_available() && !wal_inserts && !InterruptHoldoffCount);
	copy_end(&c);
}

UT_TEST(creation_record_wrong_binding_or_end_never_enters_file_replay)
{
	for (unsigned which = 0; which < 4; ++which) {
		ClusterControlNativeRelmapCreateV2 *c = NULL;
		ClusterRelmapCreateReplayV1 replay, saved;
		ClusterRelmapFilePairV2 pair;
		ClusterRecoveryFileIdentityV2 expected;
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		uint8 batch[16];
		bool parsed;
		int d;
		copy_begin(); UT_ASSERT(reserve_copy(&c));
		UT_ASSERT(creation_replay_record(c, &reader, &decoded));
		expected = c->expected; memcpy(batch, c->batch_uuid, sizeof(batch));
		if (which == 0) ++expected.creation_generation;
		if (which == 1) batch[15] ^= 1;
		if (which == 2) ++reader.EndRecPtr;
		if (which == 3) wal_bytes[56] ^= 1;
		memset(&replay, 0xa5, sizeof(replay)); saved = replay;
		parsed = cluster_relmap_control_create_redo_input_v1(&reader, &expected, batch, &replay);
		if (parsed) {
			LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
			(void)cluster_relmap_authority_managed_replay_create_v2(&c->expected, &replay.record.committed, &pair);
			LWLockRelease(RelationMappingLock);
		}
		UT_ASSERT(!parsed && !memcmp(&replay, &saved, sizeof(saved)));
		d = target_dir();
		UT_ASSERT(d >= 0 && faccessat(d, "pgrac_relmap_authority", F_OK, 0) != 0
			&& faccessat(d, "pgrac_relmap_authority.bak", F_OK, 0) != 0 && close(d) == 0);
		UT_ASSERT(!copy_syncs && !target_available() && root_renames == 2 && !c->private_attempted);
		copy_end(&c);
	}
}

UT_TEST(creation_replay_partial_sync_and_late_control_failure_keep_original_debt)
{
	for (unsigned which = 0; which < 3; ++which) {
		ClusterControlNativeRelmapCreateV2 *c = NULL;
		ClusterRecoveryCheckpointFileV2 receipts[2];
		ClusterRecoveryCheckpointFileSourceV2 vector[2];
		ClusterRelmapCreateReplayV1 replay;
		ClusterRelmapFilePairV2 pair, saved;
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		bool ok;
		int d, fd;
		copy_begin(); UT_ASSERT(reserve_copy(&c));
		UT_ASSERT(creation_replay_record(c, &reader, &decoded));
		UT_ASSERT(cluster_relmap_control_create_redo_input_v1(&reader, &c->expected, c->batch_uuid, &replay));
		d = target_dir(); UT_ASSERT(d >= 0);
		if (which == 0) {
			fd = openat(d, "pgrac_relmap_authority.bak", O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0 && write(fd, "X", 1) == 1 && close(fd) == 0);
		}
		if (which == 1) fail_sync = copy_syncs + 2;
		if (which == 2) drift_sync = copy_syncs + 1;
		memset(&pair, 0xa5, sizeof(pair)); saved = pair;
		LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
		ok = cluster_relmap_authority_managed_replay_create_v2(&c->expected, &replay.record.committed, &pair);
		LWLockRelease(RelationMappingLock);
		if (which < 2) UT_ASSERT(!ok && !memcmp(&pair, &saved, sizeof(saved)));
		else {
			UT_ASSERT(ok && creation_replay_receipts(c, &pair, receipts, vector));
			UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(c, receipts, 2) != 0);
			UT_ASSERT(c->private_attempted);
		}
		if (which == 0) {
			char byte = 0;
			UT_ASSERT(faccessat(d, "pgrac_relmap_authority", F_OK, 0) != 0);
			fd = openat(d, "pgrac_relmap_authority.bak", O_RDONLY);
			UT_ASSERT(fd >= 0 && read(fd, &byte, 1) == 1 && byte == 'X' && close(fd) == 0);
		}
		else UT_ASSERT(faccessat(d, "pgrac_relmap_authority", F_OK, 0) == 0
			&& faccessat(d, "pgrac_relmap_authority.bak", F_OK, 0) == 0);
		UT_ASSERT(close(d) == 0 && root_renames == 2 && !target_available());
		UT_ASSERT(cf_held && copy_x && !InterruptHoldoffCount && !wal_inserts);
		copy_end(&c);
	}
}
