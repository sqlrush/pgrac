/*-------------------------------------------------------------------------
 * test_cluster_clog_native_full_sync.inc
 *     Original founder FULL redo, synchronous file completion and readback.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_native_full_sync.inc
 * NOTES
 *     CLOG/SLRU and file I/O execute; source authority, locks and WAL transport
 *     remain fixtures. This is not continuous lifetime or completion proof.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_cold_recovery.h"

/* Only the original callback's three fields are used. Its merge/touched/WAL
 * dispatch leaves are controlled; CLOG dispatch executes original redo. */
typedef struct ClusterColdTypedReplay {
	bool apply_foreign;
	TimeLineID replayTLI;
	ClusterColdTouchedV1 touched;
} ClusterColdTypedReplay;
static unsigned full_sync_applies, full_sync_touches, full_sync_returns;
static bool full_sync_foreign, full_sync_other, full_sync_corrupt_after_redo;
static unsigned full_sync_drift_kind;
static DecodedXLogRecord *full_sync_drift_record;

static void
full_sync_drift(void)
{
	fixture_native_sync_hook = NULL;
	if (full_sync_drift_kind == 1) {
		clog_relmap_native_mutation(1);
		clog_relmap_native_mutation_end(1);
	} else if (full_sync_drift_kind == 2)
		full_sync_drift_record->header.xl_scn++;
	else
		((xl_clog_zeropage_full *)full_sync_drift_record->main_data)->reserved_zero++;
}

static void full_sync_scn(uint64 scn) { UT_ASSERT_EQ(scn, 43); }
static void full_sync_mode(bool foreign) { full_sync_foreign = foreign; }
static void full_sync_touch(ClusterColdTouchedV1 *touched, XLogReaderState *record)
{
	UT_ASSERT(touched != NULL && record != NULL);
	full_sync_touches++;
}
static void
full_sync_apply(XLogReaderState *record, XLogRecord *header, TimeLineID timeline)
{
	UT_ASSERT(header == &record->record->header && timeline == 1);
	full_sync_applies++;
	if (!full_sync_foreign && !full_sync_other) {
		clog_redo(record);
		if (full_sync_corrupt_after_redo) {
			xl_clog_zeropage_full birth;

			memcpy(&birth, XLogRecGetData(record), sizeof(birth));
			fixture_bit((TransactionId)birth.first_full_xid + 9, TRANSACTION_STATUS_ABORTED);
		}
		if (full_sync_drift_kind != 0) {
			full_sync_drift_record = record->record;
			fixture_native_sync_hook = full_sync_drift;
		}
	}
}

#define cluster_recovery_merge_set_scn full_sync_scn
#define cluster_recovery_merge_set_apply_foreign full_sync_mode
#define cluster_cold_touched_add_record_v1 full_sync_touch
#define ApplyWalRecord full_sync_apply
#include "test_cluster_clog_native_callback.inc"
#undef ApplyWalRecord
#undef cluster_cold_touched_add_record_v1
#undef cluster_recovery_merge_set_apply_foreign
#undef cluster_recovery_merge_set_scn

static void
full_sync_reader(XLogReaderState *reader, DecodedXLogRecord *decoded,
	xl_clog_zeropage_full *birth, int page)
{
	memset(reader, 0, sizeof(*reader)); memset(decoded, 0, sizeof(*decoded));
	*birth = (xl_clog_zeropage_full){page, 0, (UINT64_C(9) << 32) | ((uint64)page * CLOG_XACTS_PER_PAGE)};
	reader->record = decoded; reader->ReadRecPtr = 0x1000; reader->EndRecPtr = 0x1100;
	decoded->main_data = (char *)birth; decoded->main_data_len = sizeof(*birth);
	decoded->max_block_id = -1; decoded->header.xl_rmid = RM_CLOG_ID;
	decoded->header.xl_info = CLOG_ZEROPAGE_FULL; decoded->header.xl_scn = 43;
	full_sync_applies = full_sync_touches = full_sync_returns = 0;
	full_sync_foreign = full_sync_other = full_sync_corrupt_after_redo = false;
	full_sync_drift_kind = 0; full_sync_drift_record = NULL;
}

static bool
full_sync_call(XLogReaderState *reader, bool foreign)
{
	ClusterColdTypedReplay replay = { .apply_foreign = foreign, .replayTLI = 1 };
	volatile bool caught = false;

	expect_error = true;
	PG_TRY(); {
		cluster_typed_replay_apply_native(reader, &replay);
		full_sync_returns++;
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	expect_error = false;
	UT_ASSERT_EQ(transient_fds, 0);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	return caught;
}

UT_TEST(native_full_sync_reads_the_original_redo_page_without_reinitializing)
{
	for (unsigned which = 0; which < 2; which++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;
		unsigned before_writes, before_syncs, before_dirsyncs;
		int other;

		own_full_begin();
		full_sync_reader(&reader, &decoded, &birth, which ? 192 : 128);
		fixture_zero(1);
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		other = SimpleLruReadPage(XactCtl, 1, true, CLOG_XACTS_PER_PAGE + 17);
		UT_ASSERT(clog_recovery_status(XactCtl, other, CLOG_XACTS_PER_PAGE + 17,
			TRANSACTION_STATUS_ABORTED, CLOG_RECOVERY_WRITE));
		XactCtl->shared->group_lsn[other * CLOG_LSNS_PER_PAGE] = 0x1234;
		LWLockRelease(XactSLRULock);
		before_writes = writes; before_syncs = syncs; before_dirsyncs = dirsyncs;
		UT_ASSERT(!full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_applies, 1); UT_ASSERT_EQ(full_sync_touches, 1);
		UT_ASSERT_EQ(full_sync_returns, 1);
		UT_ASSERT_EQ(writes, before_writes + 1); /* Only original redo writes. */
		UT_ASSERT(syncs > before_syncs && dirsyncs > before_dirsyncs);
		UT_ASSERT(XactCtl->shared->page_dirty[other]);
		UT_ASSERT_EQ(XactCtl->shared->group_lsn[other * CLOG_LSNS_PER_PAGE], 0x1234);
		UT_ASSERT_EQ(fixture_status(CLOG_XACTS_PER_PAGE + 17), TRANSACTION_STATUS_IN_PROGRESS);
		UT_ASSERT_EQ(fixture_status((TransactionId)birth.first_full_xid), TRANSACTION_STATUS_IN_PROGRESS);
		UT_ASSERT(!full_sync_foreign);
		own_full_end();
	}
}

UT_TEST(native_full_sync_rejects_shape_and_context_before_io)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;
		unsigned before_reads, before_writes, before_syncs;

		own_full_begin(); full_sync_reader(&reader, &decoded, &birth, 128);
		if (fault == 0) birth.reserved_zero = 1;
		if (fault == 1) decoded.main_data_len--;
		if (fault == 2) decoded.max_block_id = 0;
		if (fault == 3) decoded.header.xl_rmid = RM_XACT_ID;
		if (fault == 4) decoded.header.xl_info = CLOG_ZEROPAGE;
		if (fault == 5) authority = false;
		if (fault == 6) MyAuxProcType = CheckpointerProcess;
		if (fault == 7) InRecovery = false;
		if (fault == 8) CritSectionCount = 1;
		if (fault == 9) LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		if (fault == 10) LWLockAcquire(XactTruncationLock, LW_SHARED);
		if (fault == 11) LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		before_reads = reads; before_writes = writes; before_syncs = syncs + dirsyncs;
		UT_ASSERT(!cluster_clog_native_recovery_full_sync_v1(1, &reader));
		UT_ASSERT_EQ(reads, before_reads); UT_ASSERT_EQ(writes, before_writes);
		UT_ASSERT_EQ(syncs + dirsyncs, before_syncs);
		if (fault == 9) LWLockRelease(XactSLRULock);
		if (fault == 10) LWLockRelease(XactTruncationLock);
		if (fault == 11) LWLockRelease(XidGenLock);
		CritSectionCount = 0;
		own_full_end();
	}
}

UT_TEST(native_full_sync_io_failures_and_error_keep_native_cleanup)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;

		own_full_begin(); full_sync_reader(&reader, &decoded, &birth, 128);
		if (fault == 0) fail_sync = true;
		if (fault == 1) fail_dirsync = true;
		if (fault == 2) fail_sync_close = true;
		if (fault == 3) fail_read = true;
		if (fault == 4) directory_open_error = EIO;
		UT_ASSERT(full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_returns, 0); UT_ASSERT_EQ(full_sync_applies, 1);
		fail_sync = fail_dirsync = fail_sync_close = fail_read = false;
		directory_open_error = 0;
		fixture_idle();
		own_full_end();
	}
}

UT_TEST(native_full_sync_rejects_owner_mutation_and_changed_disk)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;

		own_full_begin(); full_sync_reader(&reader, &decoded, &birth, 128);
		if (fault == 0) revoke_after_sync = true;
		if (fault == 1) full_sync_corrupt_after_redo = true;
		if (fault == 2) pg_atomic_write_u32(&ClogRelmapMutations[1].writers, 1);
		if (fault >= 3) full_sync_drift_kind = fault - 2;
		UT_ASSERT(full_sync_call(&reader, false));
		UT_ASSERT_EQ(full_sync_returns, 0);
		revoke_after_sync = false;
		fixture_native_sync_hook = NULL;
		if (fault == 2) pg_atomic_write_u32(&ClogRelmapMutations[1].writers, 0);
		own_full_end();
	}
}

UT_TEST(native_full_sync_hook_preserves_legacy_and_foreign_routes)
{
	for (unsigned which = 0; which < 3; which++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		xl_clog_zeropage_full birth;
		unsigned before_syncs;

		own_full_begin(); full_sync_reader(&reader, &decoded, &birth, 128);
		if (which == 0) {
			decoded.header.xl_info = CLOG_ZEROPAGE;
			decoded.main_data_len = sizeof(int);
		}
		if (which == 2) { decoded.header.xl_rmid = RM_XLOG_ID; full_sync_other = true; }
		before_syncs = syncs + dirsyncs;
		UT_ASSERT(!full_sync_call(&reader, which == 1));
		UT_ASSERT_EQ(full_sync_applies, 1); UT_ASSERT_EQ(full_sync_returns, 1);
		UT_ASSERT_EQ(syncs + dirsyncs, before_syncs);
		own_full_end();
	}
}
