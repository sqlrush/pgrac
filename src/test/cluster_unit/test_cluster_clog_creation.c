/* Original native CLOG creation-child trim/status/checkpoint chain. Selected
 * table, process authentication and WAL/lock scheduling are finite providers;
 * framed file bytes and full-page writeback are real. */
#define CLOG_CHECKPOINT_MAIN creation_checkpoint_original_main
int creation_checkpoint_original_main(void);
#include "test_cluster_clog_checkpoint.c"
#undef CLOG_CHECKPOINT_MAIN

static VariableCacheData creation_variables;
VariableCache ShmemVariableCache = &creation_variables;
bool cluster_shared_config;

static void
creation_reset(ProcessingMode processing)
{
	uint8 frame[8256];

	checkpoint_reset(1);
	CLOGConfigureManagedIO(&ctl);
	*XactCtl = ctl;
	creation_support_select(&binding.identity, native_generation, 1);
	native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	Mode = processing;
	cluster_shared_config = false;
	creation_variables.nextXid = FullTransactionIdFromU64(4);
	numbers[0] = shared.latest_page_number = 0;
	identities[0].identity.logical_page = 0;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64(3);
	memset(page.data, 0, BLCKSZ);
	page.data[0] = TRANSACTION_STATUS_COMMITTED << 6;
	if (!cluster_clog_file_page_encode_v2(&binding, identities, (uint8 *) page.data,
		BLCKSZ, frame, sizeof(frame)) || ftruncate(borrowed, 8192 + 8256)
		|| pwrite(borrowed, frame, sizeof(frame), 8192) != sizeof(frame)) abort();
	dirty[0] = false;
}

UT_TEST(creation_trim_status_checkpoint_keep_original_identity_and_wal)
{
	for (unsigned bootstrap = 0; bootstrap < 2; bootstrap++) {
		volatile bool caught = false;
		PGAlignedBlock before;
		ClusterClogPageIdentityV2 observed;
		uint8 payload[8192];
		XLogRecPtr status_lsn;

		creation_reset(bootstrap ? BootstrapProcessing : InitProcessing);
		before = page;
		PG_TRY(); { TrimCLOG(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(!caught);
		UT_ASSERT(!memcmp(&before, &page, sizeof(page)));
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT(!control_held && !io_held && !dirty[0]);
		Mode = NormalProcessing;
		PG_TRY();
		{
			UT_ASSERT_EQ(TransactionIdGetStatus(3, &status_lsn), TRANSACTION_STATUS_COMMITTED);
			LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
			TransactionIdSetPageStatusInternal(4, 0, NULL, TRANSACTION_STATUS_COMMITTED,
				InvalidXLogRecPtr, 0);
			LWLockRelease(XactSLRULock);
			CheckPointCLOG();
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(!caught);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(written_offset, 8192);
		UT_ASSERT_EQ(written_length, 8256);
		UT_ASSERT_EQ(checkpoint_captures + checkpoint_destroys + checkpoint_syncs, 0);
		UT_ASSERT_EQ(checkpoint_dir_syncs, 1);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(cluster_node_id, -1);
		UT_ASSERT(!cluster_enabled && !dirty[0]);
		control_held = false; io_held = true;
		UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 0,
			&observed, payload, sizeof(payload)));
		io_held = false;
		UT_ASSERT_EQ(payload[1] & 3, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(U64FromFullTransactionId(observed.identity.birth_full_xid), 3);
		UT_ASSERT_EQ(observed.image_lsn, 1000);
		checkpoint_close();
	}
}

UT_TEST(creation_checkpoint_rejects_unrelated_batch_before_wal_or_io)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		creation_reset(NormalProcessing);
		dirty[0] = true;
		if (fault == 0) creation_has_request = false;
		if (fault == 1) creation_info.creation.creator_pid++;
		if (fault == 2) creation_drift_at = 2;
		if (fault == 3) native_borrow_fault = 1;
		UT_ASSERT(checkpoint_raises());
		UT_ASSERT_EQ(inserts + writes + native_opens, 0);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT(dirty[0]);
		/* Original WriteAll ERROR leaves its family lock to the caller's
		 * global ERROR cleanup, unlike the new file borrow's own finally. */
		UT_ASSERT(control_held && !io_held);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		HOLD_INTERRUPTS();
		LWLockRelease(XactSLRULock);
		checkpoint_close();
	}
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-creation-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(2);
	UT_RUN(creation_trim_status_checkpoint_keep_original_identity_and_wal);
	UT_RUN(creation_checkpoint_rejects_unrelated_batch_before_wal_or_io);
	rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
