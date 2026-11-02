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
creation_reset_origin(ProcessingMode processing, uint16 origin)
{
	uint8 header[8192];
	uint8 frame[8256];

	checkpoint_reset(1);
	CLOGConfigureManagedIO(&ctl);
	*XactCtl = ctl;
	binding.identity.origin = origin;
	cached_bindings[0] = binding;
	creation_support_select(&binding.identity, native_generation, 1);
	cluster_enabled = true;
	native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	Mode = processing;
	cluster_shared_config = false;
	creation_variables.nextXid = FullTransactionIdFromU64(4);
	numbers[0] = shared.latest_page_number = 0;
	identities[0].identity.logical_page = 0;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64(3);
	memset(page.data, 0, BLCKSZ);
	page.data[0] = TRANSACTION_STATUS_COMMITTED << 6;
	if (!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)) ||
		pwrite(borrowed, header, sizeof(header), 0) != sizeof(header) ||
		!cluster_clog_file_page_encode_v2(&binding, identities, (uint8 *) page.data,
		BLCKSZ, frame, sizeof(frame)) || ftruncate(borrowed, 8192 + 8256)
		|| pwrite(borrowed, frame, sizeof(frame), 8192) != sizeof(frame)) abort();
	dirty[0] = false;
}

static void
creation_reset(ProcessingMode processing)
{
	creation_reset_origin(processing, 0);
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
		UT_ASSERT(cluster_enabled && !dirty[0]);
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

/* The real creator inherits cluster.enabled from the authenticated cohort
 * configuration while remaining a standalone child with node_id = -1. */
UT_TEST(creation_enabled_configuration_keeps_original_child_custody)
{
	volatile bool caught = false;
	XLogRecPtr status_lsn;
	ClusterControlNativeFileBorrowV2 value = {0};
	bool acquired;

	creation_reset(BootstrapProcessing);
	cluster_enabled = true;
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	acquired = CLOGNativeFileAcquire(0, &value);
	if (acquired) UT_ASSERT(cluster_control_native_file_release_v2(&value));
	LWLockRelease(XactSLRULock);
	UT_ASSERT(acquired);
	if (!acquired) { checkpoint_close(); return; }
	PG_TRY();
	{
		TrimCLOG();
		Mode = NormalProcessing;
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
	UT_ASSERT(cluster_enabled && cluster_node_id == -1);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(native_acquires, native_releases);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(!control_held && !io_held && !dirty[0]);
	checkpoint_close();
}

UT_TEST(creation_frame_wal_uses_only_the_authenticated_batch_origin)
{
	for (unsigned sample = 0; sample < 60; sample++) {
		unsigned fault = sample % 15;
		uint8 before[16448], after[16448];
		ClusterClogWalObservationV2 record = {0};
		XLogRecPtr end = 777;
		bool ok;

		creation_reset_origin(NormalProcessing, (sample / 15 % 2) * 127);
		cluster_enabled = sample >= 30;
		if (fault == 1) creation_has_request = false;
		if (fault == 2) creation_has_table = false;
		if (fault == 3) MyBackendType = B_BACKEND;
		if (fault == 4) IsUnderPostmaster = true;
		if (fault == 5) IsPostmasterEnvironment = true;
		if (fault == 6) creation_info.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
		if (fault == 7) creation_context.creator_pid = 0;
		if (fault == 8) creation_info.startup_generation++;
		if (fault == 9) creation_info.table_generation = 0;
		if (fault == 10) {
			creation_context.boot_file_count = 0;
			creation_info.creation = creation_context;
		}
		if (fault == 11) {
			creation_context.boot_files[0].kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
			creation_info.creation = creation_context;
		}
		if (fault == 12) creation_info.creation.creator_pid++;
		if (fault == 13) binding.identity.registration_generation++;
		if (fault == 14) {
			creation_context.boot_files[0].origin = 128;
			creation_info.creation = creation_context;
		}
		UT_ASSERT_EQ(pread(borrowed, before, sizeof(before), 0), sizeof(before));
		io_held = true;
		ok = cluster_clog_storage_write_page_v2(borrowed, &binding, identities,
			page.data, BLCKSZ, &end);
		io_held = false;
		UT_ASSERT_EQ(ok, fault == 0);
		if (fault == 0) {
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(end, 1000);
			UT_ASSERT(cluster_clog_wal_decode_v2(0x50, native_record,
				sizeof(native_record), &record));
			UT_ASSERT_EQ(record.binding.identity.origin, sample / 15 % 2 * 127);
		} else {
			UT_ASSERT_EQ(inserts + writes + reads, 0);
			UT_ASSERT_EQ(end, 777);
			UT_ASSERT_EQ(pread(borrowed, after, sizeof(after), 0), sizeof(after));
			UT_ASSERT(!memcmp(before, after, sizeof(before)));
		}
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		UT_ASSERT_EQ(cluster_node_id, -1);
		UT_ASSERT_EQ(cluster_enabled, sample >= 30);
		checkpoint_close();
	}
}

UT_TEST(creation_bootstrap_commit_keeps_the_original_page_and_checkpoint)
{
	for (unsigned sample = 0; sample < 4; sample++) {
		volatile bool caught = false;
		PGAlignedBlock expected;
		ClusterClogPageIdentityV2 observed;
		uint8 payload[8192];

		creation_reset_origin(BootstrapProcessing, (sample % 2) * 127);
		cluster_enabled = sample >= 2;
		proc.xid = BootstrapTransactionId;
		expected = page;
		/* Bootstrap's permanent xid is 1; the original normal xid 3 stays
		 * committed. Only bits 2..3 of byte zero may change (0x40 -> 0x44). */
		expected.data[0] = 0x44;
		control_held = false;
		PG_TRY();
		{
			LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
			TransactionIdSetPageStatusInternal(BootstrapTransactionId, 0, NULL,
				TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr, 0);
			LWLockRelease(XactSLRULock);
			UT_ASSERT(dirty[0]);
			UT_ASSERT(!memcmp(&expected, &page, sizeof(page)));
			/* The exact original commit is idempotent. */
			LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
			TransactionIdSetPageStatusInternal(BootstrapTransactionId, 0, NULL,
				TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr, 0);
			LWLockRelease(XactSLRULock);
			/* BOOT itself has no local CLOG frame-WAL writer. Exercise
			 * the later native writer without relaxing that original gate. */
			Mode = NormalProcessing;
			CheckPointCLOG();
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(!caught);
		UT_ASSERT(!memcmp(&expected, &page, sizeof(page)));
		UT_ASSERT(!dirty[0] && !control_held && !io_held);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_acquires, native_releases);
		control_held = false; io_held = true;
		UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 0,
			&observed, payload, sizeof(payload)));
		io_held = false;
		UT_ASSERT(!memcmp(expected.data, payload, sizeof(payload)));
		UT_ASSERT_EQ(observed.image_lsn, 1000);
		UT_ASSERT_EQ(U64FromFullTransactionId(observed.identity.birth_full_xid), 3);
		UT_ASSERT_EQ(cluster_node_id, -1);
		checkpoint_close();
	}
}

UT_TEST(creation_bootstrap_status_refuses_other_contexts_before_page_effects)
{
	for (unsigned fault = 0; fault < 23; fault++) {
		volatile bool caught = false;
		PGAlignedBlock before;
		ClusterClogPageIdentityV2 identity;
		TransactionId xid = BootstrapTransactionId, child = 4;
		XidStatus status = TRANSACTION_STATUS_COMMITTED;
		XLogRecPtr lsn = InvalidXLogRecPtr;
		int children = 0, pageno = 0;

		creation_reset(BootstrapProcessing);
		proc.xid = BootstrapTransactionId;
		switch (fault) {
			case 0: Mode = NormalProcessing; break;
			case 1: Mode = InitProcessing; break;
			case 2: InRecovery = true; break;
			case 3: native_mode = CLUSTER_NATIVE_FILES_MANAGED_V2; break;
			case 4: native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; break;
			case 5: creation_has_request = false; break;
			case 6: creation_has_table = false; break;
			case 7: creation_info.creation.creator_pid++; break;
			case 8: creation_info.startup_generation++; break;
			case 9: creation_info.table_generation = 0; break;
			case 10: MyBackendType = B_BACKEND; break;
			case 11: IsUnderPostmaster = true; break;
			case 12: IsPostmasterEnvironment = true; break;
			case 13: cluster_node_id = 0; break;
			case 14: proc.xid = 4; break;
			case 15: MyProc = NULL; break;
			case 16: xid = FrozenTransactionId; break;
			case 17: status = TRANSACTION_STATUS_ABORTED; break;
			case 18: status = TRANSACTION_STATUS_SUB_COMMITTED; break;
			case 19: lsn = 10; break;
			case 20: children = 1; break;
			case 21: pageno = 1; break;
			case 22: creation_context.boot_files[0].object_id = 1;
				creation_info.creation = creation_context; break;
		}
		before = page; identity = identities[0];
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		PG_TRY();
		{
			TransactionIdSetPageStatusInternal(xid, children, &child, status, lsn, pageno);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(!memcmp(&before, &page, sizeof(page)));
		UT_ASSERT(!memcmp(&identity, identities, sizeof(identity)));
		UT_ASSERT(!dirty[0] && control_held && !io_held);
		UT_ASSERT_EQ(native_borrows + native_opens + inserts + writes + reads, 0);
		/* ERROR reset the interrupt holdoff; retire the original caller lock. */
		HOLD_INTERRUPTS();
		LWLockRelease(XactSLRULock);
		checkpoint_close();
	}
}

UT_TEST(creation_bootstrap_commit_preserves_conflicting_terminal_bits)
{
	volatile bool caught = false;
	PGAlignedBlock before;

	creation_reset(BootstrapProcessing);
	proc.xid = BootstrapTransactionId;
	page.data[0] = 0x48; /* xid 1 ABORTED; xid 3 COMMITTED. */
	before = page;
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	PG_TRY();
	{
		TransactionIdSetPageStatusInternal(BootstrapTransactionId, 0, NULL,
			TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr, 0);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!memcmp(&before, &page, sizeof(page)) && !dirty[0]);
	UT_ASSERT_EQ(native_borrows + native_opens + inserts + writes, 0);
	HOLD_INTERRUPTS();
	LWLockRelease(XactSLRULock);
	checkpoint_close();
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-creation-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(7);
	UT_RUN(creation_trim_status_checkpoint_keep_original_identity_and_wal);
	UT_RUN(creation_checkpoint_rejects_unrelated_batch_before_wal_or_io);
	UT_RUN(creation_enabled_configuration_keeps_original_child_custody);
	UT_RUN(creation_frame_wal_uses_only_the_authenticated_batch_origin);
	UT_RUN(creation_bootstrap_commit_keeps_the_original_page_and_checkpoint);
	UT_RUN(creation_bootstrap_status_refuses_other_contexts_before_page_effects);
	UT_RUN(creation_bootstrap_commit_preserves_conflicting_terminal_bits);
	rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
