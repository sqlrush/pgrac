/*-------------------------------------------------------------------------
 * test_cluster_clog_native_truncate.inc
 *     Original local CLOG truncation, horizon publication and held input.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_native_truncate.inc
 * NOTES
 *     Native truncation, redo, horizon and SLRU file bodies execute. WAL,
 *     locks, VariableCache and episode authority are controlled fixtures.
 *     These observations do not certify an era or a physical lifetime.
 *-------------------------------------------------------------------------
 */
static TransactionId native_truncate_oldest;
static unsigned native_truncate_deleted_before;

static void
native_truncate_before_delete(XLogRecPtr lsn)
{
	xl_clog_truncate record;
	UT_ASSERT_EQ(lsn, UINT64_C(0x100000));
	UT_ASSERT_EQ(birth_wal.begins, 1);
	UT_ASSERT_EQ(birth_wal.inserts, 1);
	UT_ASSERT_EQ(birth_wal.info, CLOG_TRUNCATE);
	UT_ASSERT_EQ(birth_wal.length, sizeof(record));
	memcpy(&record, &birth_wal.bytes, sizeof(record));
	UT_ASSERT_EQ(record.pageno, 32);
	UT_ASSERT_EQ(record.oldestXact, native_truncate_oldest);
	UT_ASSERT_EQ(record.oldestXactDb, 79);
	UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid, native_truncate_oldest);
	UT_ASSERT(!LWLockHeldByMe(XactTruncationLock));
	UT_ASSERT(!LWLockHeldByMe(XactSLRULock));
	UT_ASSERT_EQ(deleted, native_truncate_deleted_before);
	UT_ASSERT(fixture_exists(0) && fixture_exists(32));
}

static bool
native_truncate_start(bool old_segment, TransactionId prior, uint64 query,
	ClusterRelmapClogBaseV1 *base, ClusterRelmapClogBaseHandleV1 **held)
{
	relmap_base_reset(0);
	clog_recovery_cache_discard(XactCtl);
	fixture_variable_cache.oldestClogXid = prior;
	native_truncate_oldest = 32 * CLOG_XACTS_PER_PAGE + 17;
	fixture_zero(32);
	if (old_segment) fixture_zero(0);
	else UT_ASSERT(!fixture_exists(0));
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	XactCtl->shared->latest_page_number = 32;
	LWLockRelease(XactSLRULock);
	memset(&birth_wal, 0, sizeof(birth_wal));
	fixture_native_truncate = true;
	fixture_native_truncate_flushes = 0;
	fixture_native_truncate_flush_error = false;
	fixture_native_truncate_flush_hook = native_truncate_before_delete;
	native_truncate_deleted_before = deleted;
	return cluster_clog_relmap_base_read_v1(&relmap_base_source, query, base, held);
}

static void
native_truncate_end(void)
{
	char path[MAXPGPATH];
	fixture_idle();
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[0].writers), 0);
	for (int page = 0; page <= 32; page += 32) {
		fixture_path(page, path);
		if (fixture_exists(page)) UT_ASSERT_EQ(unlink(path), 0);
	}
	clog_recovery_cache_discard(XactCtl);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	XactCtl->shared->latest_page_number = 0;
	LWLockRelease(XactSLRULock);
	fixture_variable_cache.oldestClogXid = InvalidTransactionId;
	fixture_native_truncate = fixture_native_truncate_flush_error = false;
	fixture_native_truncate_flush_hook = NULL;
	relmap_base_interrupts = false;
	fixture_origin = c0_origin = 1;
	InRecovery = authority = true;
	c0_authority = false;
}

UT_TEST(native_clog_truncate_advances_before_wal_flush_and_delete)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation, after;
	uint64 keep = (UINT64_C(7) << 32) | (32 * CLOG_XACTS_PER_PAGE + 100);
	UT_ASSERT(native_truncate_start(true, 3, relmap_base_query(), &base, &held));
	if (held == NULL) return;
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	TruncateCLOG(native_truncate_oldest, 79);
	UT_ASSERT_EQ(fixture_native_truncate_flushes, 1);
	UT_ASSERT(!fixture_exists(0) && fixture_exists(32));
	UT_ASSERT_EQ(deleted, native_truncate_deleted_before + 1);
	UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid, native_truncate_oldest);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT(after > generation);
	UT_ASSERT_EQ(transient_fds, 1);
	UT_ASSERT_EQ(held->base.oldest_clog_xid, 3);
	relmap_mutation_refuses(&base, &held);
	/* A fresh observation sees the surviving file and actual raw horizon;
	 * it does not restore the failed handle or confer FullXid authority. */
	relmap_base_reset(0);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source, keep, &base, &held));
	UT_ASSERT(base.truncation_horizon_observed);
	UT_ASSERT_EQ(base.oldest_clog_xid, native_truncate_oldest);
	UT_ASSERT_EQ(base.page_number, 32);
	cluster_clog_relmap_base_release_v1(&held);
	native_truncate_end();
}

UT_TEST(native_clog_truncate_without_old_segment_preserves_observation)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation, after;
	uint64 keep = (UINT64_C(7) << 32) | (32 * CLOG_XACTS_PER_PAGE + 100);
	UT_ASSERT(native_truncate_start(false, 3, keep, &base, &held));
	if (held == NULL) return;
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	TruncateCLOG(native_truncate_oldest, 79);
	UT_ASSERT_EQ(birth_wal.begins | birth_wal.inserts | fixture_native_truncate_flushes, 0);
	UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid, 3);
	UT_ASSERT_EQ(deleted, native_truncate_deleted_before);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT_EQ(after, generation);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source, keep, &base, &held));
	UT_ASSERT_EQ(transient_fds, 1);
	UT_ASSERT_EQ(base.oldest_clog_xid, 3);
	cluster_clog_relmap_base_release_v1(&held);
	native_truncate_end();
}

UT_TEST(native_clog_truncate_redo_uses_original_horizon_and_delete)
{
	const TransactionId prior[] = {3, 32 * CLOG_XACTS_PER_PAGE + 29, UINT32_MAX - 1};
	for (unsigned i = 0; i < lengthof(prior); ++i) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		XLogReaderState reader = {0};
		DecodedXLogRecord decoded = {0};
		xl_clog_truncate record;
		uint64 generation, after;
		UT_ASSERT(native_truncate_start(true, prior[i], relmap_base_query(), &base, &held));
		if (held == NULL) return;
		record.pageno = 32; record.oldestXact = native_truncate_oldest; record.oldestXactDb = 79;
		reader.record = &decoded;
		decoded.main_data = (char *)&record;
		decoded.main_data_len = sizeof(record);
		decoded.max_block_id = -1;
		decoded.header.xl_info = CLOG_TRUNCATE;
		UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
		InRecovery = true;
		clog_redo(&reader);
		UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid,
			i == 1 ? prior[i] : native_truncate_oldest);
		UT_ASSERT_EQ(birth_wal.begins | birth_wal.inserts | fixture_native_truncate_flushes, 0);
		UT_ASSERT(!fixture_exists(0) && fixture_exists(32));
		UT_ASSERT_EQ(deleted, native_truncate_deleted_before + 1);
		UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
		UT_ASSERT(after > generation);
		relmap_mutation_refuses(&base, &held);
		native_truncate_end();
	}
}

UT_TEST(native_clog_truncate_flush_error_retains_files_and_custody)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation, after;
	volatile bool caught = false;
	UT_ASSERT(native_truncate_start(true, 3, relmap_base_query(), &base, &held));
	if (held == NULL) return;
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	fixture_native_truncate_flush_error = expect_error = true;
	PG_TRY(); {
		TruncateCLOG(native_truncate_oldest, 79);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	fixture_native_truncate_flush_error = expect_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fixture_native_truncate_flushes, 1);
	UT_ASSERT_EQ(fixture_variable_cache.oldestClogXid, native_truncate_oldest);
	UT_ASSERT(fixture_exists(0) && fixture_exists(32));
	UT_ASSERT_EQ(deleted, native_truncate_deleted_before);
	UT_ASSERT_EQ(held->base.oldest_clog_xid, 3);
	UT_ASSERT_EQ(transient_fds, 1);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT(after > generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[0].writers), 0);
	relmap_mutation_refuses(&base, &held);
	native_truncate_end();
}
