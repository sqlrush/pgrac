/*-------------------------------------------------------------------------
 * test_cluster_clog_page_birth.inc
 *     Original native page initialization and explicit full-generation WAL.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_page_birth.inc
 * NOTES
 *     Native CLOG/SLRU bodies execute; locks and WAL insertion are fixtures.
 *     No issued transaction, durability, native lifetime or completion grant.
 *-------------------------------------------------------------------------
 */
static struct {
	xl_clog_zeropage_full bytes;
	uint32 length;
	uint8 info;
	unsigned begins, inserts;
	LWLockMode mode;
	bool fail;
} birth_wal;

/* Only ZERO redo is exercised here. The unrelated native TRUNCATE branch
 * must never mutate a fixture horizon to make these tests pass. */
void
AdvanceOldestClogXid(TransactionId oldest pg_attribute_unused())
{
	abort();
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return LWLockHeldByMe(lock) && mode == birth_wal.mode;
}
void
XLogBeginInsert(void)
{
	birth_wal.begins++;
	birth_wal.length = 0;
	memset(&birth_wal.bytes, 0, sizeof(birth_wal.bytes));
}
void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT(length <= sizeof(birth_wal.bytes));
	if (length <= sizeof(birth_wal.bytes)) memcpy(&birth_wal.bytes, data, length);
	birth_wal.length = length;
}
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(rmid, RM_CLOG_ID);
	UT_ASSERT(LWLockHeldByMe(XidGenLock));
	UT_ASSERT(LWLockHeldByMe(XactSLRULock));
	birth_wal.info = info;
	birth_wal.inserts++;
	if (birth_wal.fail) elog(ERROR, "full birth insertion fixture");
	return UINT64_C(0x100000);
}

static bool
birth_call_caught(FullTransactionId full)
{
	volatile bool caught = false;
	expect_error = true;
	PG_TRY();
	{
		ExtendCLOGFull(full);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expect_error = false;
	return caught;
}
static void
birth_reset(void)
{
	/* These producer tests leave ERROR lock release to the original caller,
	 * unlike the opaque base reader's private catch/holdoff fixture above. */
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	relmap_base_interrupts = false;
	fixture_own_reset();
	memset(&birth_wal, 0, sizeof(birth_wal));
	birth_wal.mode = LW_EXCLUSIVE;
}

UT_TEST(clog_full_birth_uses_actual_native_zero_and_full_wal)
{
	const uint64 full[] = {3, (UINT64_C(1) << 32) | 3,
		(UINT64_C(3) << 32) | (31 * CLOG_XACTS_PER_PAGE)};

	for (unsigned i = 0; i < lengthof(full); ++i) {
		int page = (TransactionId)full[i] / CLOG_XACTS_PER_PAGE;
		int slot;
		birth_reset();
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		UT_ASSERT(!birth_call_caught(FullTransactionIdFromU64(full[i])));
		UT_ASSERT(LWLockHeldByMe(XidGenLock));
		LWLockRelease(XidGenLock);
		UT_ASSERT_EQ(birth_wal.begins, 1);
		UT_ASSERT_EQ(birth_wal.inserts, 1);
		UT_ASSERT_EQ(birth_wal.info, CLOG_ZEROPAGE_FULL);
		UT_ASSERT_EQ(birth_wal.length, sizeof(xl_clog_zeropage_full));
		UT_ASSERT_EQ(birth_wal.bytes.pageno, page);
		UT_ASSERT_EQ(birth_wal.bytes.reserved_zero, 0);
		UT_ASSERT_EQ(birth_wal.bytes.first_full_xid, full[i]);
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		slot = SimpleLruReadPage(XactCtl, page, true, (TransactionId)full[i]);
		UT_ASSERT(XactCtl->shared->page_dirty[slot]);
		for (unsigned b = 0; b < BLCKSZ; ++b)
			UT_ASSERT_EQ(XactCtl->shared->page_buffer[slot][b], 0);
		LWLockRelease(XactSLRULock);
		fixture_idle();
	}
}

UT_TEST(clog_full_birth_requires_exclusive_native_issuer)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		FullTransactionId full = FullTransactionIdFromU64((UINT64_C(5) << 32) | 3);
		birth_reset();
		if (fault != 0) LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		if (fault == 1) birth_wal.mode = LW_SHARED;
		if (fault == 2) full = FullTransactionIdFromU64(UINT64_C(5) << 32);
		if (fault == 3) full = FullTransactionIdFromU64((UINT64_C(5) << 32) | 1);
		UT_ASSERT(birth_call_caught(full));
		UT_ASSERT_EQ(birth_wal.begins | birth_wal.inserts, 0);
		UT_ASSERT(!LWLockHeldByMe(XactSLRULock));
		if (fault != 0) LWLockRelease(XidGenLock);
		fixture_idle();
	}
	birth_reset();
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	UT_ASSERT(!birth_call_caught(FullTransactionIdFromU64((UINT64_C(5) << 32) | 4)));
	UT_ASSERT_EQ(birth_wal.begins | birth_wal.inserts, 0);
	LWLockRelease(XidGenLock);
	fixture_idle();
}

UT_TEST(clog_full_birth_redo_checks_complete_native_image)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		XLogReaderState reader = {0};
		DecodedXLogRecord decoded = {0};
		xl_clog_zeropage_full image = {1, 0, (UINT64_C(7) << 32) | CLOG_XACTS_PER_PAGE};
		volatile bool caught = false;
		unsigned before;
		birth_reset();
		reader.record = &decoded;
		decoded.main_data = (char *)&image;
		decoded.main_data_len = sizeof(image);
		decoded.max_block_id = -1;
		decoded.header.xl_info = CLOG_ZEROPAGE_FULL;
		if (fault == 1) image.reserved_zero = 1;
		if (fault == 2) image.pageno = -1;
		if (fault == 3) image.pageno = 2;
		if (fault == 4) image.first_full_xid++;
		if (fault == 5) image.first_full_xid = (UINT64_C(7) << 32) | 1;
		if (fault == 6) decoded.main_data_len--;
		if (fault == 7) decoded.main_data_len++;
		if (fault == 8) decoded.max_block_id = 0;
		before = writes;
		expect_error = true;
		PG_TRY();
		{
			clog_redo(&reader);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expect_error = false;
		UT_ASSERT_EQ(caught, fault != 0);
		UT_ASSERT_EQ(writes > before, fault == 0);
		UT_ASSERT_EQ(birth_wal.begins | birth_wal.inserts, 0);
		fixture_idle();
	}
}

UT_TEST(clog_full_birth_error_keeps_native_dirty_state)
{
	bool dirty = false;
	birth_reset();
	birth_wal.fail = true;
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	UT_ASSERT(birth_call_caught(FullTransactionIdFromU64((UINT64_C(3) << 32) | 3)));
	UT_ASSERT_EQ(birth_wal.inserts, 1);
	UT_ASSERT(LWLockHeldByMe(XidGenLock));
	UT_ASSERT(LWLockHeldByMe(XactSLRULock));
	for (int i = 0; i < XactCtl->shared->num_slots; ++i)
		dirty |= XactCtl->shared->page_dirty[i];
	UT_ASSERT(dirty);
	/* Mirror only the native ERROR release-all responsibility. No second
	 * initialization, insertion, rollback or successful retry is attempted. */
	LWLockRelease(XactSLRULock);
	LWLockRelease(XidGenLock);
	birth_wal.fail = false;
	fixture_idle();
}
