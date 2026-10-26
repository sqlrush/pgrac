/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_insert.inc
 *     Native CTRC custody through the original Generic/F4 WAL leg.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_insert.inc
 *
 * NOTES
 *     Real native owners, PAGE, Generic registration/edge and stamp FSM.
 *     WAL transport and the unit PAGE primitive's LWLock enumeration are
 *     explicit fixture boundaries, not native durability/capacity proof.
 *
 *-------------------------------------------------------------------------
 */
#include "storage/lwlock.h"

static bool native_insert_active, native_insert_caller_lock;
static CtrcReadNativeFixture *native_insert_live;
static GenericXLogRecoveryImageV1 native_insert_image;
static PGAlignedBlock native_insert_registered;
static RfPageVersionEdgeEntryV1 native_insert_edge;
static RfPageGuardV1 native_insert_blocker;
static unsigned native_insert_fault, native_insert_hashes, native_insert_capacity;
static unsigned native_insert_writer_checks, native_insert_begins, native_insert_blocks;
static unsigned native_insert_installed_calls;
static unsigned native_insert_edges, native_insert_records, native_insert_flushes;
XLogRecPtr ProcLastRecPtr;
pg_cryptohash_ctx *recovery_fpi_hash_create(pg_cryptohash_type type);
ClusterControlRootResult cluster_wal_writer_check(const ClusterWalWriterToken *writer);
void XLogFlush(XLogRecPtr end);
bool ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work);

bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	/* A full caller-held list leaves no slot for original owner snapshots.
	 * Model that callee ERROR; this entry must refuse before reaching it. */
	if (native_insert_active && native_insert_caller_lock) pg_re_throw();
	return native_insert_original_work_current(start, work);
}

static bool
native_insert_page_held(void)
{
	RfPageGuardPreflightV1 preflight;
	RfPageGuardV1 probe = {0};
	bool free;

	if (native_insert_live == NULL) return false;
	UT_ASSERT(rf_page_guard_preflight_v1(&native_insert_live->page.target.page_identity, &preflight));
	free = rf_page_guard_promote_nowait_v1(&preflight, &probe);
	if (free) rf_page_guard_release_v1(&probe);
	return !free;
}

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	/* The linked unit PAGE primitive uses a bitmap, not native LWLocks. */
	if (native_insert_caller_lock || native_insert_page_held())
		callback(NULL, LW_EXCLUSIVE, context);
}

pg_cryptohash_ctx *
recovery_fpi_hash_create(pg_cryptohash_type type)
{
	if (native_insert_active) {
		UT_ASSERT(!native_insert_page_held() && !native_insert_caller_lock);
		UT_ASSERT_EQ(CritSectionCount, 0); native_insert_hashes++;
		if (native_insert_fault == 1) ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++;
		if (native_insert_fault == 2) native_insert_image.image.data[0]++;
		if (native_insert_fault == 3) native_stamp_source.ir_request_id++;
	}
	return pg_cryptohash_create(type);
}

void
XLogEnsureRecordSpace(int max_block_id, int ndatas)
{
	RfPageGuardPreflightV1 preflight;

	UT_ASSERT(native_insert_active && !native_insert_page_held() && !native_insert_caller_lock);
	UT_ASSERT_EQ(CritSectionCount, 0); UT_ASSERT_EQ(max_block_id, 0); UT_ASSERT_EQ(ndatas, 0);
	native_insert_capacity++;
	if (native_insert_fault == 4) ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++;
	if (native_insert_fault == 5) pg_re_throw();
	if (native_insert_fault == 6) {
		UT_ASSERT(rf_page_guard_preflight_v1(&native_insert_live->page.target.page_identity, &preflight));
		UT_ASSERT(rf_page_guard_promote_nowait_v1(&preflight, &native_insert_blocker));
	}
}

bool
ClusterStartupInstalledRecoveryWriterV1(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation)
{
	bool installed = native_insert_original_installed_writer(writer, operation_uuid, generation);

	if (native_insert_active && native_insert_fault == 7)
		ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++;
	if (native_insert_active) {
		native_insert_installed_calls++;
		if (native_insert_installed_calls == 3) {
			if (native_insert_fault == 18) (*generation)++;
			if (native_insert_fault == 22) operation_uuid[15]++;
			if (native_insert_fault == 23) writer->epoch++;
		}
	}
	return installed;
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *writer)
{
	if (!native_insert_active) return native_insert_original_writer_check(writer);
	UT_ASSERT(native_insert_page_held()); UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(memcmp(writer, &ctrc_inputs_owner.writer, sizeof(*writer)) == 0);
	native_insert_writer_checks++;
	if (native_insert_fault == 8 || (native_insert_fault == 14 && native_insert_writer_checks == 2))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (native_insert_fault == 9) native_insert_image.image.data[0]++;
	if (native_insert_fault == 10) ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++;
	if (native_insert_fault == 11) cluster_wal_inputs_release_v1(&native_insert_live->page.replay.inputs);
	if (native_insert_fault == 12) pg_re_throw();
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
XLogBeginInsert(void)
{
	UT_ASSERT(native_insert_active && native_insert_page_held());
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(native_fpi_region.thread_replay[native_stamp_source.selected.source.claim.identity.origin_thread_id].stamp.phase,
		CLUSTER_RECOVERY_STAMP_INSERTING);
	native_insert_begins++;
}

void
XLogRegisterBlock(uint8 id, RelFileLocator *locator, ForkNumber fork,
	BlockNumber block, char *page, uint8 flags)
{
	UT_ASSERT_EQ(CritSectionCount, 1); UT_ASSERT_EQ(native_insert_begins, 1);
	UT_ASSERT_EQ(id, 0); UT_ASSERT_EQ(fork, MAIN_FORKNUM); UT_ASSERT_EQ(block, 7);
	UT_ASSERT(RelFileLocatorEquals(*locator, native_insert_image.identity.key.locator));
	UT_ASSERT_EQ(flags, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
	memcpy(native_insert_registered.data, page, BLCKSZ); native_insert_blocks++;
	if (native_insert_fault == 19) pg_re_throw();
}

void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entry, uint8 count)
{
	UT_ASSERT_EQ(CritSectionCount, 1); UT_ASSERT_EQ(native_insert_blocks, 1);
	UT_ASSERT_EQ(token, 100001); UT_ASSERT_EQ(count, 1);
	native_insert_edge = *entry; native_insert_edges++;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(CritSectionCount, 1); UT_ASSERT_EQ(native_insert_blocks, 1);
	UT_ASSERT_EQ(native_insert_edges, 1); UT_ASSERT_EQ(rmid, RM_GENERIC_ID); UT_ASSERT_EQ(info, 0);
	native_insert_records++;
	if (native_insert_fault == 20) pg_re_throw();
	ProcLastRecPtr = 0x2000040;
	return native_insert_fault == 21 ? ProcLastRecPtr : 0x2000240;
}

void
XLogFlush(XLogRecPtr end)
{
	ClusterRecoveryStampSlotV1 *slot = &native_fpi_region.thread_replay[
		native_stamp_source.selected.source.claim.identity.origin_thread_id].stamp;

	UT_ASSERT_EQ(CritSectionCount, 0); UT_ASSERT_EQ(native_insert_records, 1);
	UT_ASSERT_EQ(end, 0x2000240); UT_ASSERT_EQ(slot->phase, CLUSTER_RECOVERY_STAMP_INSERTED);
	UT_ASSERT_EQ(slot->record_start, 0x2000040); UT_ASSERT_EQ(slot->record_end, end);
	UT_ASSERT(SpinLockFree(&slot->lock)); native_insert_flushes++;
	if (native_insert_fault == 13) pg_re_throw();
	if (native_insert_fault == 15) native_insert_image.image.data[0]++;
	if (native_insert_fault == 16) ctrc_inputs_work.entries[1].slot.first_undo_block.raw[0]++;
	if (native_insert_fault == 17) cluster_wal_inputs_release_v1(&native_insert_live->page.replay.inputs);
	if (native_insert_fault == 24) native_stamp_source.root.file_txn_seq++;
}

static void
native_insert_fixture(CtrcReadNativeFixture *f, uint32 index)
{
	RfPageCtrcFpiV1 prepared;

	native_insert_active = false; native_insert_live = NULL;
	native_fpi_complete_writer = true; native_fpi_fixture(f, 8, true); native_fpi_complete_writer = false;
	prepared = native_fpi_prepare(f, 2, 1024 * 1024);
	UT_ASSERT(prepared.ready && prepared.rewritten); native_insert_image = prepared.frame;
	rf_page_authority_ctrc_replays_destroy_v1(&f->page.page.authority);
	f->page.page = ctrc_page_build(&f->page); UT_ASSERT_EQ(f->page.page.verdict, 0);
	native_stamp_source = cluster_wal_inputs_ctrc_replays_source_v1(f->page.replay.inputs, index);
	native_stamp_installed = true; native_stamp_fault = 0; native_stamp_live = f;
	native_stamp_allocs = native_stamp_frees = 0; native_stamp_alloc_error = false;
	native_insert_hashes = native_insert_capacity = native_insert_writer_checks = 0;
	native_insert_installed_calls = 0;
	native_insert_begins = native_insert_blocks = native_insert_edges = native_insert_records = native_insert_flushes = 0;
	memset(&native_insert_blocker, 0, sizeof(native_insert_blocker));
	native_insert_fault = 0; native_insert_caller_lock = false;
	native_insert_live = f; native_insert_active = true;
}

static void
native_insert_finish(CtrcReadNativeFixture *f)
{
	native_insert_active = false; native_insert_caller_lock = false;
	if (native_insert_blocker.state != RF_PAGE_GUARD_EMPTY) rf_page_guard_release_v1(&native_insert_blocker);
	native_insert_live = NULL; native_fpi_finish(f);
}

static RfPageCtrcFpiInsertV1
native_insert(CtrcReadNativeFixture *f, Size count, Size budget)
{
	return rf_page_recovery_ctrc_replays_fpi_insert_v1(tx_inputs_start, &ctrc_inputs_work,
		&native_stamp_source, f->page.page.authority, &native_insert_image, count, budget);
}

UT_TEST(test_native_insert_registers_exact_image_edge_and_original_window)
{
	for (uint32 index = 0; index < 2; index++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 result; PGAlignedBlock before, after;
		GenericXLogRecoveryImageV1 fixed; ClusterRecoveryStampFpiV1 envelope; ClusterRecoveryStampCaptureV1 capture;
		unsigned releases, grants;

		native_insert_fixture(&f, index); fixed = native_insert_image;
		UT_ASSERT_EQ(pread(f.main_fd, before.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		releases = test_tx_ir_releases; grants = test_tx_ir_grants;
		result = native_insert(&f, 2, 1024 * 1024);
		UT_ASSERT(result.wal_flushed); UT_ASSERT_EQ(result.record_start, 0x2000040); UT_ASSERT_EQ(result.record_end, 0x2000240);
		UT_ASSERT_EQ(result.window.key.target_thread, index == 0 ? 128 : 1);
		UT_ASSERT(memcmp(&result.image, &fixed, sizeof(fixed)) == 0);
		UT_ASSERT(memcmp(native_insert_registered.data, fixed.image.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(&native_insert_edge, &fixed.version, sizeof(fixed.version)) == 0);
		memcpy(&envelope, result.window.payload, sizeof(envelope));
		UT_ASSERT_EQ(envelope.image.result_token, 100001);
		UT_ASSERT_EQ(envelope.protection.source_binding_cookie, native_stamp_source.ir_request_id);
		UT_ASSERT(cluster_recovery_stamp_fpi_inserted_current_v1(&result.window, result.record_start, result.record_end));
		UT_ASSERT(cluster_recovery_stamp_capture_v1(&capture));
		UT_ASSERT_EQ(cluster_recovery_stamp_poll_v1(&capture), CLUSTER_RECOVERY_STAMP_PENDING);
		UT_ASSERT_EQ(native_insert_hashes, 1); UT_ASSERT_EQ(native_insert_capacity, 1);
		UT_ASSERT_EQ(native_insert_records, 1); UT_ASSERT_EQ(native_insert_flushes, 1);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2 && native_insert_page_held());
		UT_ASSERT_EQ(pread(f.main_fd, after.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0); native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_refuses_held_lwlocks_before_hash_or_capacity)
{
	for (unsigned page = 0; page < 2; page++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 result = {0}; volatile bool caught = false;
		native_insert_fixture(&f, 0);
		if (page) UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.page.authority), 0);
		else native_insert_caller_lock = true;
		PG_TRY(); { result = native_insert(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(!caught);
		UT_ASSERT(!result.wal_flushed); UT_ASSERT_EQ(result.window.key.window_seq, 0);
		UT_ASSERT_EQ(native_insert_hashes, 0); UT_ASSERT_EQ(native_insert_capacity, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_page_collision_keeps_all_original_owners)
{
	CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 result;
	native_insert_fixture(&f, 0); native_insert_fault = 6;
	result = native_insert(&f, 2, 1024 * 1024);
	UT_ASSERT(!result.wal_flushed); UT_ASSERT_EQ(result.window.key.window_seq, 0);
	UT_ASSERT_EQ(native_insert_hashes, 1); UT_ASSERT_EQ(native_insert_capacity, 1);
	UT_ASSERT_EQ(native_insert_begins, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
	native_insert_finish(&f);
}

UT_TEST(test_native_insert_rejects_bad_actor_count_budget_and_frame)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 result; Size count = 2, budget = 1024 * 1024;
		native_insert_fixture(&f, 0);
		switch (fault) {
		case 0: MyBackendType = B_CHECKPOINTER; break;
		case 1: ShutdownRequestPending = true; break;
		case 2: inputs_native_recovery = true; break;
		case 3: count = 1; break;
		case 4: count = UINT32_MAX; break;
		case 5: budget = 1; break;
		case 6: native_insert_image.result_token = 0; break;
		case 7: native_stamp_source.ir_request_id++; break;
		case 8: native_fpi_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 9: budget = 2 * (offsetof(ClusterColdCtrcWorkV1, entries)
			+ 2 * sizeof(ClusterColdTtSlotInputV1)) - 1; break;
		}
		result = native_insert(&f, count, budget);
		UT_ASSERT(!result.wal_flushed); UT_ASSERT_EQ(result.window.key.window_seq, 0);
		UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		MyBackendType = B_STARTUP; ShutdownRequestPending = false; inputs_native_recovery = false;
		native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_rechecks_original_bytes_across_preparation)
{
	const unsigned faults[] = {1, 2, 3, 4, 7};
	for (unsigned i = 0; i < lengthof(faults); i++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 result;
		native_insert_fixture(&f, 0); native_insert_fault = faults[i]; result = native_insert(&f, 2, 1024 * 1024);
		UT_ASSERT(!result.wal_flushed); UT_ASSERT_EQ(result.window.key.window_seq, 0);
		UT_ASSERT_EQ(native_insert_records, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_preinsert_refusal_cancels_only_its_window)
{
	for (unsigned fault = 8; fault <= 11; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 result;
		native_insert_fixture(&f, 0); native_insert_fault = fault; result = native_insert(&f, 2, 1024 * 1024);
		UT_ASSERT(!result.wal_flushed); UT_ASSERT_EQ(result.window.key.window_seq, 0);
		UT_ASSERT_EQ(native_insert_records, 0);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_EMPTY);
		if (fault != 11) UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT(native_insert_page_held()); native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_errors_free_private_memory_and_keep_debt)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CtrcReadNativeFixture f; volatile bool caught = false;
		native_insert_fixture(&f, 0);
		if (!fault) native_fpi_alloc_error = true;
		if (fault == 1) native_insert_fault = 5;
		if (fault == 2) native_stamp_alloc_error = true;
		if (fault == 3) native_insert_fault = 12;
		PG_TRY(); { (void)native_insert(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT_EQ(native_insert_records, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		if (fault == 3) {
			UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
			UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_PRE_INSERT);
		}
		native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_registration_and_insert_failure_keep_failed_window)
{
	for (unsigned fault = 19; fault <= 21; fault++) {
		CtrcReadNativeFixture f; volatile bool caught = false;
		native_insert_fixture(&f, 0); native_insert_fault = fault;
		test_capture_error_level = true; test_last_error_level = 0;
		PG_TRY(); { (void)native_insert(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT(native_fpi_region.thread_replay[128].stamp.failed);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[128].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTING);
		UT_ASSERT_EQ(native_insert_flushes, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		if (fault == 21) UT_ASSERT_EQ(test_last_error_level, PANIC);
		test_capture_error_level = false;
		CritSectionCount = 0; native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_flush_failure_and_drift_keep_inserted_debt)
{
	const unsigned faults[] = {13, 14, 15, 16, 17, 18, 22, 23, 24};
	for (unsigned i = 0; i < lengthof(faults); i++) {
		unsigned fault = faults[i];
		CtrcReadNativeFixture f; volatile bool caught = false; RfPageCtrcFpiInsertV1 result = {0};
		native_insert_fixture(&f, 1); native_insert_fault = fault;
		PG_TRY(); { result = native_insert(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 13); UT_ASSERT(!result.wal_flushed);
		UT_ASSERT_EQ(native_insert_flushes, 1); UT_ASSERT(native_fpi_region.thread_replay[1].stamp.failed);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[1].stamp.phase, CLUSTER_RECOVERY_STAMP_INSERTED);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[1].stamp.record_start, 0x2000040);
		UT_ASSERT_EQ(native_fpi_region.thread_replay[1].stamp.record_end, 0x2000240);
		if (fault != 17) UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT(native_insert_page_held()); native_insert_finish(&f);
	}
}

UT_TEST(test_native_insert_pending_window_cannot_start_another_record)
{
	CtrcReadNativeFixture f; RfPageCtrcFpiInsertV1 first, second;
	native_insert_fixture(&f, 0); first = native_insert(&f, 2, 1024 * 1024); UT_ASSERT(first.wal_flushed);
	rf_page_authority_ctrc_replays_destroy_v1(&f.page.page.authority);
	f.page.page = ctrc_page_build(&f.page); UT_ASSERT_EQ(f.page.page.verdict, 0);
	second = native_insert(&f, 2, 1024 * 1024);
	UT_ASSERT(!second.wal_flushed); UT_ASSERT_EQ(second.window.key.window_seq, 0);
	UT_ASSERT_EQ(native_insert_records, 1);
	UT_ASSERT(cluster_recovery_stamp_fpi_inserted_current_v1(&first.window, first.record_start, first.record_end));
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_insert_finish(&f);
}
