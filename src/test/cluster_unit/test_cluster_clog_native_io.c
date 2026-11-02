/* Author: SqlRush <sqlrush@gmail.com>
 * Native CLOG status/SLRU routing over real framed temporary files.
 * Scheduling, selected-control custody and native WAL sinks are finite
 * providers. No native recovery or deployment qualification is implied. */
/* The original providers require per-buffer IO for physical I/O. Native FD
 * open is instead protected by the original table borrow across ControlLock's
 * gap; keep that distinction observable without changing the old fixture. */
#define SLRU_FIXTURE_OPEN managed_io_open
#define CLOG_MANAGED_MAIN managed_fixture_main
int managed_fixture_main(void);
int managed_io_open(const char *path, int flags);
#include "test_cluster_slru_clog_managed_write.c"
#undef CLOG_MANAGED_MAIN
#undef SLRU_FIXTURE_OPEN

/* The original CLOG lock is the same finite lock exercised by the real SLRU. */
#undef XactSLRULock
#define XactSLRULock (&control)
#ifdef CLOG_NATIVE_IO_PRODUCT
#include CLOG_NATIVE_IO_PRODUCT
#else
#include "../../backend/access/transam/clog.c"
#endif
#include "test_cluster_clog_creation_support.h"

static unsigned native_borrows, native_acquires, native_releases, native_opens;
static bool native_open_throw, native_open_drift;
static bool native_open_busy, native_other_dirty;
static unsigned native_waits;
static bool native_current = true, native_release_ok = true, native_missing;
static ClusterControlNativeModeV2 native_mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
static uint64 native_generation = 41;
static unsigned native_borrow_fault;

ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return native_mode; }
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *out)
{
	if (!control_held || io_held || key->kind != CLUSTER_RECOVERY_FILE_CLOG_V2 ||
		key->origin != binding.identity.origin || key->database_oid != 0 || key->tablespace_oid != 0 ||
		key->object_id != 0 || out->private_self != 0) abort();
	if (native_missing) return false;
	out->expected = binding.identity;
	out->table_generation = native_generation;
	out->startup_generation = 1;
	out->private_self = (uintptr_t) out;
	out->private_pid = 1;
	if (native_borrow_fault == 1) out->expected.creation_generation++;
	if (native_borrow_fault == 2) out->table_generation++;
	if (native_borrow_fault == 3) out->startup_generation++;
	native_borrows++; native_acquires++;
	return true;
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	if (!control_held || borrow->private_self != (uintptr_t) borrow || native_borrows == 0) abort();
	return native_current && borrow->table_generation == native_generation;
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	if (!control_held || io_held || borrow->private_self != (uintptr_t) borrow || native_borrows == 0) abort();
	if (!native_release_ok) return false;
	native_releases++; native_borrows--;
	memset(borrow, 0, sizeof(*borrow));
	return true;
}

int OpenTransientFile(const char *path, int flags)
{
	if (native_borrows == 0 || control_held || io_held ||
		(flags & (O_CREAT | O_TRUNC | O_EXCL)) != 0) abort();
	native_opens++;
	if (native_open_throw) siglongjmp(*PG_exception_stack, 1);
	if (native_open_drift) native_current = false;
	if (native_open_busy) {
		native_open_busy = false;
		/* Another original writer starts under ControlLock in this gap,
		 * owns IO, and has not completed when our open returns. */
		states[0] = SLRU_PAGE_WRITE_IN_PROGRESS; dirty[0] = false;
	}
	return open(path, flags, 0600);
}

static void native_reset(void)
{
	managed_reset();
	creation_support_reset(); native_borrow_fault = 0;
	CLOGConfigureManagedIO(&ctl);
	*XactCtl = ctl;
	native_borrows = native_acquires = native_releases = 0;
	native_opens = 0; native_open_throw = native_open_drift = false;
	native_open_busy = native_other_dirty = false; native_waits = 0;
	native_current = native_release_ok = true; native_missing = false;
	native_mode = CLUSTER_NATIVE_FILES_MANAGED_V2; native_generation = 41;
}

static bool
native_status_raises(bool readonly)
{
	volatile bool caught = false;
	XLogRecPtr status_lsn = 999;

	XactCtl->shared = &shared;
	PG_TRY();
	{
		if (readonly)
		{
			control_held = false;
			UT_ASSERT_EQ(TransactionIdGetStatus(98304, &status_lsn),
						 TRANSACTION_STATUS_COMMITTED);
		}
		else
			TransactionIdSetPageStatusInternal(98304, 0, NULL,
				TRANSACTION_STATUS_ABORTED, InvalidXLogRecPtr, 3);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

UT_TEST(native_status_reads_selected_managed_cache)
{
	native_reset(); dirty[0] = false;
	UT_ASSERT(managed_read());
	UT_ASSERT(!native_status_raises(true));
	UT_ASSERT(!control_held && !io_held);
	UT_ASSERT_EQ((uint8) page.data[0], 0x31);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	close(borrowed);
}

UT_TEST(native_status_update_keeps_managed_birth_and_wal_writer)
{
	native_reset(); dirty[0] = false;
	UT_ASSERT(managed_read());
	/* Make this particular native transaction IN_PROGRESS first. */
	page.data[0] &= ~3;
	UT_ASSERT(!native_status_raises(false));
	UT_ASSERT(control_held && !io_held && dirty[0]);
	UT_ASSERT_EQ((uint8) page.data[0] & 3, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT_EQ(identities[0].identity.page_generation, 9);
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(written_offset, 8192 + 3 * 8256);
	UT_ASSERT_EQ(written_length, 8256);
	UT_ASSERT_EQ(queues, 0);
	close(borrowed);
}

UT_TEST(native_status_miss_loads_complete_frame_not_file_header)
{
	native_reset(); dirty[0] = false; states[0] = SLRU_PAGE_EMPTY;
	memset(identities, 0, sizeof(identities));
	memset(cached_bindings, 0, sizeof(cached_bindings));
	memset(page.data, 0x7f, BLCKSZ);
	UT_ASSERT(!native_status_raises(true));
	UT_ASSERT_EQ((uint8) page.data[0], 0x31);
	UT_ASSERT_EQ(identities[0].identity.page_generation, 9);
	UT_ASSERT_EQ(identities[0].image_lsn, 100);
	UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 3);
	UT_ASSERT(!control_held && !io_held && !dirty[0]);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	close(borrowed);
}

UT_TEST(native_unknown_missing_and_stale_refuse_before_status_effects)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		native_reset(); dirty[0] = false;
		native_mode = fault == 0 ? CLUSTER_NATIVE_FILES_UNKNOWN_V2 : CLUSTER_NATIVE_FILES_MANAGED_V2;
		native_missing = fault == 1; native_current = fault != 2;
		if (fault == 3) cached_bindings[0].identity.creation_generation++;
		UT_ASSERT(native_status_raises(false));
		UT_ASSERT(control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ((uint8) page.data[0], 0x41);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT_EQ(native_opens + inserts + writes, 0);
		close(borrowed);
	}
}

UT_TEST(native_cache_birth_is_complete_before_returning_status)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		native_reset(); dirty[0] = false;
		switch (fault) {
			case 0: identities[0].identity.birth_observed = false; break;
			case 1: identities[0].identity.page_generation = PG_UINT64_MAX; break;
			case 2: identities[0].identity.birth_full_xid = FullTransactionIdFromU64(PG_UINT64_MAX); break;
			case 3: identities[0].identity.birth_full_xid = FullTransactionIdFromU64(4 * 32768); break;
			case 4: identities[0].identity.page_lsn = PG_UINT64_MAX; break;
			case 5: identities[0].image_lsn = PG_UINT64_MAX; break;
		}
		UT_ASSERT(native_status_raises(true));
		UT_ASSERT(control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_opens + inserts + writes, 0);
		close(borrowed);
	}
}

UT_TEST(native_read_failure_releases_only_its_fd_and_identity)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		native_reset(); dirty[0] = false; states[0] = SLRU_PAGE_EMPTY;
		native_open_throw = fault == 0; native_open_drift = fault == 1;
		read_throw = fault == 2;
		if (fault == 3) {
			/* InRecovery must not fabricate an absent managed frame. */
			InRecovery = true;
			UT_ASSERT_EQ(ftruncate(borrowed, 8192), 0);
		}
		UT_ASSERT(native_status_raises(true));
		UT_ASSERT(control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT_EQ(closes, fault == 0 ? 0 : 1);
		UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
		UT_ASSERT_EQ(inserts + writes + queues, 0);
		close(borrowed);
	}
}

UT_TEST(native_write_failures_keep_original_dirty_and_source)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterClogPageIdentityV2 before;
		native_reset(); before = identities[0];
		native_open_throw = fault == 0; native_open_drift = fault == 1;
		wal_throw = fault == 2 ? 1 : fault == 3 ? 2 : 0;
		fail_write = fault == 4; fail_sync = fault == 5;
		UT_ASSERT(write_raises(NULL));
		UT_ASSERT(control_held && !io_held && dirty[0]);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
		UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT_EQ(closes, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		UT_ASSERT_EQ(queues, 0);
		UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
		close(borrowed);
	}
}

UT_TEST(native_redirty_stays_dirty_after_fullimage_wal)
{
	native_reset(); inject_update = advance_source = true;
	SimpleLruWritePage(&ctl, 0);
	UT_ASSERT(control_held && !io_held && dirty[0]);
	UT_ASSERT_EQ((uint8) page.data[0], 0x42);
	UT_ASSERT_EQ(identities[0].identity.page_lsn, 50);
	UT_ASSERT_EQ(identities[0].image_lsn, 1000);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(native_acquires, native_releases);
	UT_ASSERT_EQ(lsns[1], 40);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(queues, 0);
	close(borrowed);
}

UT_TEST(native_creation_uses_selected_files_with_cluster_not_serving)
{
	native_reset(); dirty[0] = false;
	creation_support_select(&binding.identity, native_generation, 1);
	native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	UT_ASSERT(!native_status_raises(true));
	UT_ASSERT_EQ(native_acquires, 1);
	UT_ASSERT_EQ(native_releases, 1);
	UT_ASSERT_EQ(native_borrows, 0);
	close(borrowed);
}

UT_TEST(native_creation_key_uses_batch_origin_not_online_node)
{
	for (unsigned origin = 0; origin < 128; origin += 127) {
		ClusterControlNativeFileBorrowV2 value = {0};
		native_reset(); binding.identity.origin = origin;
		creation_support_select(&binding.identity, native_generation, 1);
		native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
		UT_ASSERT(CLOGNativeFileAcquire(3, &value));
		UT_ASSERT_EQ(value.expected.origin, origin);
		UT_ASSERT_EQ(cluster_node_id, -1);
		UT_ASSERT(!cluster_enabled);
		if (value.private_self)
			UT_ASSERT(cluster_control_native_file_release_v2(&value));
		UT_ASSERT_EQ(native_opens + inserts + writes, 0);
		close(borrowed);
	}
}

UT_TEST(native_creation_refuses_unrelated_context_before_borrow)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterControlNativeFileBorrowV2 value = {0};
		native_reset();
		creation_support_select(&binding.identity, native_generation, 1);
		native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
		switch (fault) {
			case 0: creation_has_request = false; break;
			case 1: creation_has_table = false; break;
			case 2: MyBackendType = B_BACKEND; break;
			case 3: IsUnderPostmaster = true; break;
			case 4: IsPostmasterEnvironment = true; break;
			case 5: cluster_node_id = 0; break;
			case 6: creation_info.creation.creator_pid++; break;
			case 7: creation_info.startup_generation++; break;
			case 8: creation_info.table_generation = 0; break;
			case 9: creation_context.boot_file_count = 0;
				creation_info.creation = creation_context; break;
			case 10: creation_info.mode = CLUSTER_NATIVE_FILES_MANAGED_V2; break;
			case 11: native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; break;
			case 12: native_mode = CLUSTER_NATIVE_FILES_MANAGED_V2; break;
			case 13: creation_context.creator_pid = 0;
				creation_info.startup_generation = 0;
				creation_info.creation = creation_context; break;
		}
		UT_ASSERT(!CLOGNativeFileAcquire(3, &value));
		UT_ASSERT_EQ(native_acquires + native_opens + inserts + writes, 0);
		UT_ASSERT_EQ(value.private_self, 0);
		if (value.private_self) (void) cluster_control_native_file_release_v2(&value);
		close(borrowed);
	}
}

UT_TEST(native_creation_borrow_rechecks_full_expected_and_table)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterControlNativeFileBorrowV2 value = {0};
		native_reset();
		creation_support_select(&binding.identity, native_generation, 1);
		native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
		if (fault < 3) native_borrow_fault = fault + 1;
		if (fault == 3) creation_drift_at = 2;
		if (fault == 4) native_current = false;
		if (fault == 5) {
			creation_context.boot_files[0].registration_generation++;
			creation_info.creation = creation_context;
		}
		UT_ASSERT(!CLOGNativeFileAcquire(3, &value));
		UT_ASSERT_EQ(native_acquires, 1);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT_EQ(native_borrows + native_opens + inserts + writes, 0);
		UT_ASSERT_EQ(value.private_self, 0);
		close(borrowed);
	}
}

UT_TEST(native_creation_unregistered_segment_never_reaches_io)
{
	ClusterControlNativeFileBorrowV2 value = {0};
	native_reset();
	creation_support_select(&binding.identity, native_generation, 1);
	native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	UT_ASSERT(!CLOGNativeFileAcquire(32, &value));
	UT_ASSERT(!CLOGNativeFileAcquire(-1, &value));
	UT_ASSERT(!CLOGNativeFileAcquire(131072, &value));
	UT_ASSERT_EQ(native_acquires + native_opens + inserts + writes, 0);
	close(borrowed);
}

UT_TEST(native_creation_status_write_keeps_original_fullimage_wal)
{
	native_reset(); dirty[0] = false;
	creation_support_select(&binding.identity, native_generation, 1);
	native_mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	UT_ASSERT(managed_read());
	page.data[0] &= ~3;
	UT_ASSERT(!native_status_raises(false));
	UT_ASSERT(!write_raises(NULL));
	UT_ASSERT(!dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(native_acquires, native_releases);
	close(borrowed);
}

UT_TEST(native_refused_release_does_not_report_success_or_clear_duty)
{
	native_reset(); native_release_ok = false;
	UT_ASSERT(write_raises(NULL));
	UT_ASSERT(control_held && !io_held && dirty[0]);
	UT_ASSERT_EQ(native_acquires, 1);
	UT_ASSERT_EQ(native_releases, 0);
	UT_ASSERT_EQ(native_borrows, 1);
	UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(inserts, 1);
	close(borrowed);
}

UT_TEST(native_raw_birth_refuses_before_slot_mutation)
{
	volatile bool caught = false;
	PGAlignedBlock before;
	native_reset(); dirty[0] = false; states[0] = SLRU_PAGE_EMPTY;
	memset(cached_bindings, 0, sizeof(cached_bindings));
	before = page;
	PG_TRY(); { (void) SimpleLruZeroPage(&ctl, 4); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY);
	UT_ASSERT(!dirty[0] && control_held && !io_held);
	UT_ASSERT(memcmp(&before, &page, sizeof(before)) == 0);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	close(borrowed);
}

UT_TEST(native_dirty_victim_writes_selected_frame_before_reuse)
{
	ClusterClogPageIdentityV2 next;
	uint8 frame[8256], payload[8192];
	native_reset(); next = identities[0];
	next.identity.logical_page = 4;
	next.identity.page_generation = 10;
	next.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + 4 * 32768);
	next.image_lsn = 101;
	memset(payload, 0x21, sizeof(payload));
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &next, payload, sizeof(payload), frame, sizeof(frame)));
	UT_ASSERT_EQ(pwrite(borrowed, frame, sizeof(frame), 8192 + 4 * 8256), sizeof(frame));
	shared.latest_page_number = 99;
	UT_ASSERT_EQ(SimpleLruReadPage(&ctl, 4, true, 4 * 32768), 0);
	UT_ASSERT(control_held && !io_held && !dirty[0]);
	UT_ASSERT_EQ(numbers[0], 4);
	UT_ASSERT_EQ(identities[0].identity.page_generation, 10);
	UT_ASSERT_EQ((uint8) page.data[0], 0x21);
	UT_ASSERT_EQ(native_acquires, 2);
	UT_ASSERT_EQ(native_releases, 2);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(native_opens, 2);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(written_offset, 8192 + 3 * 8256);
	UT_ASSERT_EQ(written_length, 8256);
	UT_ASSERT_EQ(queues, 0);
	close(borrowed);
}

UT_TEST(native_segno_only_sync_refuses_without_opening_a_file)
{
	FileTag tag = {0};
	char path[MAXPGPATH];
	native_reset(); tag.handler = SYNC_HANDLER_CLOG; tag.segno = 0;
	UT_ASSERT_EQ(clogsyncfiletag(&tag, path), -1);
	UT_ASSERT_EQ(errno, ESTALE);
	UT_ASSERT(strcmp(path, filename) == 0);
	UT_ASSERT_EQ(native_opens + native_borrows + inserts + writes, 0);
	close(borrowed);
}

static void native_other_writer_completes(void)
{
	UT_ASSERT(!control_held && !io_held && native_borrows == 1);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_WRITE_IN_PROGRESS);
	UT_ASSERT(!dirty[0]);
	states[0] = SLRU_PAGE_VALID;
	dirty[0] = native_other_dirty;
	native_waits++;
}

UT_TEST(native_open_gap_waits_for_original_writer_success_or_failure)
{
	for (unsigned failed = 0; failed < 2; failed++) {
		native_reset(); native_open_busy = true; native_other_dirty = failed;
		fixture_io_wait = native_other_writer_completes;
		SimpleLruWritePage(&ctl, 0);
		UT_ASSERT_EQ(native_waits, 1);
		UT_ASSERT(control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(inserts, failed ? 1 : 0);
		UT_ASSERT_EQ(closes, 1);
		close(borrowed);
	}
}

#ifndef NATIVE_CLOG_IO_MAIN
#define NATIVE_CLOG_IO_MAIN main
#endif
int NATIVE_CLOG_IO_MAIN(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-native-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(19);
	UT_RUN(native_status_reads_selected_managed_cache);
	UT_RUN(native_status_update_keeps_managed_birth_and_wal_writer);
	UT_RUN(native_status_miss_loads_complete_frame_not_file_header);
	UT_RUN(native_unknown_missing_and_stale_refuse_before_status_effects);
	UT_RUN(native_cache_birth_is_complete_before_returning_status);
	UT_RUN(native_read_failure_releases_only_its_fd_and_identity);
	UT_RUN(native_write_failures_keep_original_dirty_and_source);
	UT_RUN(native_redirty_stays_dirty_after_fullimage_wal);
	UT_RUN(native_creation_uses_selected_files_with_cluster_not_serving);
	UT_RUN(native_creation_key_uses_batch_origin_not_online_node);
	UT_RUN(native_creation_refuses_unrelated_context_before_borrow);
	UT_RUN(native_creation_borrow_rechecks_full_expected_and_table);
	UT_RUN(native_creation_unregistered_segment_never_reaches_io);
	UT_RUN(native_creation_status_write_keeps_original_fullimage_wal);
	UT_RUN(native_refused_release_does_not_report_success_or_clear_duty);
	UT_RUN(native_raw_birth_refuses_before_slot_mutation);
	UT_RUN(native_dirty_victim_writes_selected_frame_before_reuse);
	UT_RUN(native_segno_only_sync_refuses_without_opening_a_file);
	UT_RUN(native_open_gap_waits_for_original_writer_success_or_failure);
	unlink(filename); rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
