/* Original reader/file bytes, never a reconstructed parsed-record hash.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(test_census_raw_records_preserve_full_file_bytes_after_both_passes)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	unsigned calls; uint64 records = 0;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	calls = inventory_census_calls;
	for (uint64 i = 0;; i++) {
		uint8 bytes[8192], digest[32];
		ClusterRecoveryNativeRecordObservationV1 out = cluster_recovery_census_native_record_v1(cursor, i);
		const ClusterRecoveryNativeRecord *record = out.primary.record;
		pg_cryptohash_ctx *hash; char walpath[MAXPGPATH]; int wal;
		UT_ASSERT(out.observed); if (!out.observed) break;
		if (i == out.count) {
			uint8 zero[32] = {0};
			UT_ASSERT(out.raw_record == NULL && out.raw_length == 0);
			UT_ASSERT(memcmp(out.raw_record_sha256, zero, 32) == 0); break;
		}
		UT_ASSERT(out.raw_record != NULL);
		UT_ASSERT_EQ(out.raw_length, record->header.xl_tot_len);
		if (out.raw_record == NULL || out.raw_length > sizeof(bytes)) continue;
		UT_ASSERT(record->read_lsn % XLOG_BLCKSZ + out.raw_length <= XLOG_BLCKSZ);
		v2_checkpoint_wal_path(&record->source.claim.identity, record->read_lsn,
			record->source.timeline, walpath);
		wal = open(walpath, O_RDONLY); UT_ASSERT(wal >= 0);
		UT_ASSERT_EQ(pread(wal, bytes, out.raw_length, record->read_lsn % wal_segment_size), out.raw_length);
		UT_ASSERT_EQ(close(wal), 0);
		UT_ASSERT(memcmp(out.raw_record, bytes, out.raw_length) == 0);
		UT_ASSERT(memcmp(bytes, &record->header, SizeOfXLogRecord) == 0);
		hash = pg_cryptohash_create(PG_SHA256); UT_ASSERT(hash != NULL);
		UT_ASSERT_EQ(pg_cryptohash_init(hash), 0);
		UT_ASSERT_EQ(pg_cryptohash_update(hash, bytes, out.raw_length), 0);
		UT_ASSERT_EQ(pg_cryptohash_final(hash, digest, sizeof(digest)), 0);
		pg_cryptohash_free(hash);
		UT_ASSERT(memcmp(digest, out.raw_record_sha256, 32) == 0); records++;
	}
	UT_ASSERT(records > 2); UT_ASSERT_EQ(inventory_census_calls, calls);
	UT_ASSERT(cursor->inventory->cursor == NULL && inputs_pin_held);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_census_raw_records_missing_length_bytes_hash_and_end_refuse_whole_view)
{
	for (unsigned fault = 41; fault <= 48; fault++) {
		ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
		ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
		ClusterRecoveryNativeRecordObservationV1 out, zero = {0};
		UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		census_native_record_fault = fault;
		out = cluster_recovery_census_native_record_v1(cursor, fault == 48
			? cursor->diag.inventory.native_records + cursor->diag.inventory.undo_records : 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		UT_ASSERT_EQ(cursor->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
		UT_ASSERT(inputs_pin_held && inputs != NULL);
		census_native_record_fault = 0;
		out = cluster_recovery_census_native_record_v1(cursor, 0);
		UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
		census_tx_done(cursor, &inputs, fd, path);
	}
}

/* Direct file callback only. The original XLogReader performs all record,
 * page, continuation and CRC validation; this is not a cold-authority test. */
static int
raw_test_page_read(XLogReaderState *reader, XLogRecPtr page, int req,
	XLogRecPtr target, char *buffer)
{
	const ClusterWalSourceRef *source = reader->private_data;
	char path[MAXPGPATH]; int fd; ssize_t n;
	(void)req; (void)target;
	v2_checkpoint_wal_path(&source->claim.identity, page, source->timeline, path);
	fd = open(path, O_RDONLY); if (fd < 0) return -1;
	n = pread(fd, buffer, XLOG_BLCKSZ, page % wal_segment_size);
	if (close(fd) != 0 || n != XLOG_BLCKSZ) return -1;
	return n;
}

static XLogReaderState *
raw_test_reader(const ClusterWalSourceRef *source, XLogRecPtr start)
{
	XLogReaderState *reader = XLogReaderAllocate(wal_segment_size, NULL,
		XL_ROUTINE(.page_read = raw_test_page_read), (void *)source);
	if (reader != NULL) {
		reader->system_identifier = source->claim.identity.system_identifier;
		reader->seg.ws_tli = source->timeline;
		reader->cluster_expected_thread_id = source->claim.identity.origin_thread_id;
		XLogBeginRead(reader, start);
	}
	return reader;
}

UT_TEST(test_reader_raw_record_readahead_release_seek_and_crc_failure_revoke)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterRecoveryNativeRecordObservationV1 out;
	XLogReaderState *reader; char *error = NULL;
	char walpath[MAXPGPATH]; uint8 byte; int wal;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	out = cluster_recovery_census_native_record_v1(cursor, 0);
	reader = raw_test_reader(&out.primary.record->source, out.primary.record->read_lsn);
	UT_ASSERT(reader != NULL);
	UT_ASSERT(XLogReadRecord(reader, &error) != NULL);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) != NULL);
	UT_ASSERT(memcmp(XLogReaderCurrentRawRecord(reader), out.raw_record, out.raw_length) == 0);
	UT_ASSERT(XLogReadAhead(reader, false) != NULL);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) == NULL);
	UT_ASSERT(XLogNextRecord(reader, &error) != NULL);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) != NULL);
	XLogReleasePreviousRecord(reader);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) == NULL);
	XLogBeginRead(reader, out.primary.record->read_lsn);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) == NULL);
	UT_ASSERT(XLogReadRecord(reader, &error) != NULL);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) != NULL);
	v2_checkpoint_wal_path(&out.primary.record->source.claim.identity, out.primary.record->read_lsn,
		out.primary.record->source.timeline, walpath);
	wal = open(walpath, O_RDWR); UT_ASSERT(wal >= 0);
	UT_ASSERT_EQ(pread(wal, &byte, 1, out.primary.record->read_lsn % wal_segment_size + out.raw_length - 1), 1);
	byte ^= 1;
	UT_ASSERT_EQ(pwrite(wal, &byte, 1, out.primary.record->read_lsn % wal_segment_size + out.raw_length - 1), 1);
	UT_ASSERT_EQ(close(wal), 0);
	/* Expire only this fixture's page cache; the native reader must reread
	 * the actually corrupted file after the original reposition. */
	XLogBeginRead(reader, out.primary.record->read_lsn);
	reader->readLen = 0;
	UT_ASSERT(XLogReadRecord(reader, &error) == NULL);
	UT_ASSERT(error != NULL && reader->cluster_record_crc_failed);
	UT_ASSERT(XLogReaderCurrentRawRecord(reader) == NULL);
	XLogReaderFree(reader);
	census_tx_done(cursor, &inputs, fd, path);
}

UT_TEST(test_reader_raw_record_page_and_segment_continuations_exclude_page_headers)
{
	ClusterWalInputsV1 *inputs; char path[MAXPGPATH]; int fd;
	ClusterRecoveryCensus *cursor = census_tx_setup(&inputs, &fd, path, false);
	ClusterWalSourceRef source;
	UT_ASSERT_EQ(owned_census_until(cursor, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	source = cluster_recovery_census_native_record_v1(cursor, 0).primary.record->source;
	/* Only build disposable WAL bytes outside the selected cut. Do not call
	 * the checkpoint-publication fixture or alter its original ROOT state. */
	for (unsigned segment = 0; segment < 2; segment++) {
		uint8 bytes[SizeOfXLogRecord + 2 + sizeof(CheckPoint)];
		ControlFileData candidate = {0};
		XLogReaderState *reader; const XLogRecord *raw; char *error = NULL;
		XLogRecPtr position; size_t used = 0;
		candidate.state = DB_SHUTDOWNED;
		candidate.checkPointCopy.ThisTimeLineID = source.timeline;
		candidate.checkPoint = segment ? 100 * (XLogRecPtr)wal_segment_size - 48
			: 98 * (XLogRecPtr)wal_segment_size + 2 * XLOG_BLCKSZ - 48;
		candidate.checkPointCopy.redo = candidate.checkPoint - 128;
		v2_checkpoint_wal_record(&source.claim.identity, &candidate, 0);
		reader = raw_test_reader(&source, candidate.checkPoint);
		UT_ASSERT(reader != NULL);
		if (XLogReadRecord(reader, &error) == NULL) {
			UT_ASSERT(false); XLogReaderFree(reader); continue;
		}
		raw = XLogReaderCurrentRawRecord(reader); UT_ASSERT(raw != NULL);
		UT_ASSERT_EQ(reader->ReadRecPtr, candidate.checkPoint);
		UT_ASSERT_EQ(reader->EndRecPtr, test_checkpoint_end);
		UT_ASSERT(reader->EndRecPtr != reader->ReadRecPtr + sizeof(bytes));
		position = candidate.checkPoint;
		while (used < sizeof(bytes)) {
			char walpath[MAXPGPATH]; size_t n = Min(sizeof(bytes) - used, XLOG_BLCKSZ - position % XLOG_BLCKSZ);
			int wal;
			v2_checkpoint_wal_path(&source.claim.identity, position, source.timeline, walpath);
			wal = open(walpath, O_RDONLY); UT_ASSERT(wal >= 0);
			UT_ASSERT_EQ(pread(wal, bytes + used, n, position % wal_segment_size), n);
			UT_ASSERT_EQ(close(wal), 0); used += n; position += n;
			if (used < sizeof(bytes)) position += position % wal_segment_size == 0
				? SizeOfXLogLongPHD : SizeOfXLogShortPHD;
		}
		UT_ASSERT(raw != NULL && raw->xl_tot_len == sizeof(bytes));
		UT_ASSERT(raw != NULL && memcmp(raw, bytes, sizeof(bytes)) == 0);
		UT_ASSERT_EQ(raw->xl_crc, test_checkpoint_crc);
		UT_ASSERT_EQ(bytes[SizeOfXLogRecord], XLR_BLOCK_ID_DATA_SHORT);
		UT_ASSERT(memcmp(bytes + SizeOfXLogRecord + 2, &candidate.checkPointCopy, sizeof(CheckPoint)) == 0);
		XLogReaderFree(reader);
		for (unsigned file = 0; file < (segment ? 2U : 1U); file++) {
			char walpath[MAXPGPATH];
			v2_checkpoint_wal_path(&source.claim.identity,
				candidate.checkPoint + file * (XLogRecPtr)wal_segment_size, source.timeline, walpath);
			UT_ASSERT_EQ(unlink(walpath), 0);
		}
	}
	census_tx_done(cursor, &inputs, fd, path);
}
