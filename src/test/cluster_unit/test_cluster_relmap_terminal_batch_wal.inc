/*-------------------------------------------------------------------------
 * test_cluster_relmap_terminal_batch_wal.inc
 *     Actual multi-page WAL for complete pending namespace batches.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_relmap_terminal_batch_wal.inc
 * NOTES
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
/* Selected only by the terminal test translation unit. Ordinary retained
 * fixtures keep their original single-record builder and byte sequence. */
static uint32 terminal_namespace_count;
static ClusterRelmapV2Image *terminal_namespace_images;
static WalTestRecord *terminal_namespace_records;

static XLogRecPtr
terminal_namespace_start(XLogRecPtr at)
{
	if (at % XLOG_BLCKSZ == 0)
		return at + SizeOfXLogShortPHD;
	if (XLOG_BLCKSZ - at % XLOG_BLCKSZ < SizeOfXLogRecord)
		return at + XLOG_BLCKSZ - at % XLOG_BLCKSZ + SizeOfXLogShortPHD;
	return at;
}

static XLogRecPtr
terminal_namespace_end(XLogRecPtr at, uint32 bytes)
{
	at = terminal_namespace_start(at);
	while (bytes != 0) {
		uint32 n = Min(bytes, XLOG_BLCKSZ - at % XLOG_BLCKSZ);
		at += n; bytes -= n;
		if (bytes != 0) at += SizeOfXLogShortPHD;
	}
	return MAXALIGN(at);
}

/* Same native framing/CRC as terminal_test_record, extended only to write
 * real continuation headers. The production reader still decodes every byte. */
static WalTestRecord
terminal_namespace_record(const ClusterWalSourceRef *source, XLogRecPtr at,
	XLogRecPtr prev, RmgrId rmid, uint8 info, TransactionId xid,
	const void *data, uint32 len)
{
	uint8 bytes[2048] = {0};
	XLogRecord record = {0};
	uint32 framing = len <= UINT8_MAX ? 2 : 5, used = 0;
	XLogRecPtr start = terminal_namespace_start(at), position = start;
	char path[MAXPGPATH];
	int fd;

	record.xl_tot_len = SizeOfXLogRecord + framing + len;
	UT_ASSERT(record.xl_tot_len <= sizeof(bytes));
	record.xl_prev = prev; record.xl_rmid = rmid; record.xl_info = info; record.xl_xid = xid;
	bytes[SizeOfXLogRecord] = len <= UINT8_MAX ? XLR_BLOCK_ID_DATA_SHORT : XLR_BLOCK_ID_DATA_LONG;
	if (len <= UINT8_MAX) bytes[SizeOfXLogRecord + 1] = len;
	else memcpy(bytes + SizeOfXLogRecord + 1, &len, sizeof(len));
	memcpy(bytes + SizeOfXLogRecord + framing, data, len);
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, framing + len);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	v2_checkpoint_wal_path(&source->claim.identity, start, source->timeline, path);
	fd = open(path, O_RDWR); UT_ASSERT(fd >= 0);
	while (used < record.xl_tot_len) {
		XLogRecPtr page = position - position % XLOG_BLCKSZ;
		uint32 offset = position % XLOG_BLCKSZ;
		uint32 n = Min(record.xl_tot_len - used, XLOG_BLCKSZ - offset);
		if (offset == SizeOfXLogShortPHD) {
			XLogPageHeaderData h = {0};
			h.xlp_magic = XLOG_PAGE_MAGIC; h.xlp_tli = source->timeline;
			h.xlp_thread_id = source->claim.identity.origin_thread_id;
			h.xlp_pageaddr = page;
			if (used != 0) { h.xlp_info = XLP_FIRST_IS_CONTRECORD; h.xlp_rem_len = record.xl_tot_len - used; }
			UT_ASSERT_EQ(pwrite(fd, &h, SizeOfXLogShortPHD, page % wal_segment_size), SizeOfXLogShortPHD);
		}
		UT_ASSERT_EQ(pwrite(fd, bytes + used, n, position % wal_segment_size), n);
		position += n; used += n;
		if (used < record.xl_tot_len) position += SizeOfXLogShortPHD;
	}
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(MAXALIGN(position), terminal_namespace_end(at, record.xl_tot_len));
	return (WalTestRecord){1, MAXALIGN(position), start, record.xl_crc};
}

static XLogRecPtr
terminal_namespace_checkpoint(XLogRecPtr lower, uint32 terminal_bytes)
{
	XLogRecPtr at = lower;
	for (uint32 i = 0; i < terminal_namespace_count; ++i)
		at = terminal_namespace_end(at, SizeOfXLogRecord + 5 + MinSizeOfRelmapUpdate + sizeof(RelMapFile));
	at = terminal_namespace_end(at, SizeOfXLogRecord + 2 + terminal_bytes);
	return terminal_namespace_start(at);
}

static WalTestRecord
terminal_namespace_pending(const ClusterWalSourceRef *source, WalTestRecord first)
{
	WalTestRecord previous = first;
	terminal_namespace_images[0] = custody_image;
	terminal_namespace_records[0] = first;
	for (uint32 i = 1; i < terminal_namespace_count; ++i) {
		uint8 payload[MinSizeOfRelmapUpdate + sizeof(RelMapFile)];
		xl_relmap_update update = {0};
		ClusterRelmapV2Image *image = &terminal_namespace_images[i];
		*image = custody_image;
		image->identity.dbid += i;
		image->pending.mappings[0].mapfilenumber += i;
		relmap_test_crc(&image->pending);
		update.dbid = image->identity.dbid; update.tsid = DEFAULTTABLESPACE_OID;
		update.nbytes = sizeof(RelMapFile);
		memcpy(payload, &update, MinSizeOfRelmapUpdate);
		memcpy(payload + MinSizeOfRelmapUpdate, &image->pending, sizeof(RelMapFile));
		previous = terminal_namespace_record(source, previous.exclusive_end, previous.record_start,
			RM_RELMAP_ID, XLOG_RELMAP_UPDATE, 99, payload, sizeof(payload));
		image->pending_owner.wal_end = previous.exclusive_end;
		terminal_namespace_records[i] = previous;
	}
	return previous;
}
