/* Original rmgr + full native SLRU/storage replay, real selected temporary file.
 * Startup/table/lock/error scheduling are controlled leaves, not a native run. */
#define SLRU_FIXTURE_OPEN redo_unused_open
#define SLRU_FIXTURE_COUNT_HOLDOFF() true
#define SLRU_FIXTURE_ERROR_LOCKS() true
#define SLRU_FIXTURE_ERROR_RESET() (InterruptHoldoffCount = 0)
#define CLOG_MANAGED_MAIN redo_managed_main
int redo_managed_main(void);
int redo_unused_open(const char *, int);
#include "test_cluster_slru_clog_managed_write.c"
#undef CLOG_MANAGED_MAIN
#undef SLRU_FIXTURE_OPEN
#undef XactSLRULock
#define XactSLRULock (&control)
#ifdef NATIVE_CLOG_REDO_SOURCE
#include NATIVE_CLOG_REDO_SOURCE
#else
#include "../../backend/access/transam/clog.c"
#endif
BackendType MyBackendType;
static bool redo_throw_read, redo_open_error, redo_missing, redo_drift;
static unsigned redo_borrows, redo_acquires, redo_releases, redo_checks;
static ssize_t redo_pread(int fd, void *bytes, size_t size, off_t offset);
#undef pg_pread
#define pg_pread redo_pread
#include "../../backend/access/transam/clog_replay.c"

ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return CLUSTER_NATIVE_FILES_MANAGED_V2; }
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *out)
{
	if (!control_held || io_held || key->kind != CLUSTER_RECOVERY_FILE_CLOG_V2 ||
		key->origin != 0 || key->object_id || key->database_oid || key->tablespace_oid || out->private_self) abort();
	if (redo_missing) return false;
	out->expected = binding.identity; out->table_generation = 41; out->startup_generation = 1;
	out->private_self = (uintptr_t)out; redo_borrows++; redo_acquires++; return true;
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{
	if (!control_held || !redo_borrows || borrow->private_self != (uintptr_t)borrow) abort();
	redo_checks++;
	return !(redo_drift && redo_checks >= 4);
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{
	if (!control_held || io_held || !redo_borrows || borrow->private_self != (uintptr_t)borrow) abort();
	redo_borrows--; redo_releases++; memset(borrow, 0, sizeof(*borrow)); return true;
}
int OpenTransientFile(const char *path, int flags)
{
	if (control_held || io_held || !redo_borrows || (flags & (O_CREAT|O_EXCL|O_TRUNC))) abort();
	if (redo_open_error) ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("open injection")));
	return open(path, flags, 0600);
}
static ssize_t redo_pread(int fd, void *bytes, size_t size, off_t offset)
{
	if (control_held || !io_held || !redo_borrows) abort();
	if (redo_throw_read) ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("read injection")));
	return managed_fixture_pread(fd, bytes, size, offset);
}
static XLogReaderState redo_reader;
static DecodedXLogRecord redo_decoded;
static uint8 redo_record[8384], redo_initial[8192];

static void redo_reset(bool resident)
{
	uint8 frame[8256];
	managed_reset(); CLOGConfigureManagedIO(&ctl); *XactCtl = ctl;
	MyBackendType = B_STARTUP; InRecovery = true;
	redo_throw_read = redo_open_error = redo_missing = redo_drift = false;
	redo_borrows = redo_acquires = redo_releases = redo_checks = 0;
	memset(redo_initial, 0, sizeof(redo_initial)); redo_initial[0] = 1;
	identities[0].identity.page_lsn = 10; identities[0].image_lsn = 100;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, identities, redo_initial, 8192, frame, 8256));
	UT_ASSERT_EQ(pwrite(borrowed, frame, 8256, 8192 + 3 * 8256), 8256);
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&binding.identity, redo_record, 128));
	/* Captured WAL image adds COMMITTED xid1, resident prior redo adds ABORTED xid2. */
	redo_initial[0] |= 1 << 2;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, identities, redo_initial, 8192, redo_record + 128, 8256));
	redo_initial[0] = 1 | (2 << 4); memcpy(page.data, redo_initial, 8192);
	identities[0].identity.page_lsn = 50; lsns[0] = 60; lsns[1] = 70;
	states[0] = resident ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY; dirty[0] = resident;
	if (!resident) { memset(identities, 0, sizeof(identities)); memset(cached_bindings, 0, sizeof(cached_bindings)); }
	memset(&redo_reader, 0, sizeof(redo_reader)); memset(&redo_decoded, 0, sizeof(redo_decoded));
	redo_reader.record = &redo_decoded; redo_reader.ReadRecPtr = 150; redo_reader.EndRecPtr = 200;
	redo_decoded.header.xl_rmid = RM_CLOG_ID; redo_decoded.header.xl_info = 0x50;
	redo_decoded.max_block_id = -1; redo_decoded.main_data = (char *)redo_record; redo_decoded.main_data_len = 8384;
	control_held = false; InterruptHoldoffCount = 3;
}
static bool redo_run(void)
{
	volatile bool caught = false;
	PG_TRY(); { clog_redo(&redo_reader); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return !caught;
}
static void redo_done(bool success)
{
	UT_ASSERT(!control_held && !io_held); UT_ASSERT_EQ(redo_borrows, 0);
	UT_ASSERT_EQ(redo_acquires, redo_releases); UT_ASSERT_EQ(InterruptHoldoffCount, success ? 3 : 0);
	UT_ASSERT_EQ(inserts + flushes + queues, 0); UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
	close(borrowed);
}
UT_TEST(original_rmgr_merges_dirty_resident_only_after_durable_same_fd_replay)
{
	redo_reset(true); UT_ASSERT(redo_run()); UT_ASSERT_EQ((uint8)page.data[0], 1 | (1 << 2) | (2 << 4));
	UT_ASSERT_EQ(identities[0].image_lsn, 200); UT_ASSERT_EQ(identities[0].identity.page_lsn, 70);
	UT_ASSERT(!dirty[0]); UT_ASSERT_EQ(lsns[0] + lsns[1], 0); UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
	redo_done(true);
}
UT_TEST(original_rmgr_miss_reconstructs_complete_selected_frame)
{
	redo_reset(false); UT_ASSERT(redo_run()); UT_ASSERT_EQ((uint8)page.data[0], 1 | (1 << 2));
	UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID); UT_ASSERT(!dirty[0]); UT_ASSERT_EQ(identities[0].image_lsn, 200);
	redo_done(true);
}
UT_TEST(replay_is_idempotent_and_retains_newer_resident_source)
{
	redo_reset(true); identities[0].image_lsn = 300;
	UT_ASSERT(redo_run()); UT_ASSERT(redo_run());
	UT_ASSERT_EQ(identities[0].identity.page_lsn, 70); UT_ASSERT_EQ(identities[0].image_lsn, 300);
	UT_ASSERT_EQ((uint8)page.data[0], 37); redo_done(true);
}
UT_TEST(unknown_legacy_wrong_identity_or_end_refuse_before_disk_mutation)
{
	for (unsigned f = 0; f < 6; f++) {
		redo_reset(true);
		if (f == 0) redo_missing = true;
		if (f == 1) redo_decoded.header.xl_info = CLOG_ZEROPAGE;
		if (f == 2) {
			ClusterClogFileBindingV2 other = binding;
			uint8 payload[8192] = {1};
			other.identity.creation_generation++;
			UT_ASSERT(cluster_recovery_file_identity_encode_v2(&other.identity, redo_record, 128));
			UT_ASSERT(cluster_clog_file_page_encode_v2(&other, identities, payload, 8192, redo_record + 128, 8256));
		}
		if (f == 3) redo_decoded.main_data_len--;
		if (f == 4) redo_reader.EndRecPtr = redo_reader.ReadRecPtr;
		if (f == 5) InRecovery = false;
		UT_ASSERT(!redo_run()); UT_ASSERT(memcmp(page.data, redo_initial, 8192) == 0);
		UT_ASSERT_EQ(writes, 0); UT_ASSERT(dirty[0]); redo_done(false);
	}
}
UT_TEST(conflicting_terminal_or_cache_birth_never_overwrites)
{
	for (unsigned f = 0; f < 2; f++) {
		redo_reset(true);
		if (f == 0) page.data[0] = (page.data[0] & ~3) | 2;
		else identities[0].identity.page_generation++;
		UT_ASSERT(!redo_run()); UT_ASSERT_EQ(writes, 0); UT_ASSERT(dirty[0]); redo_done(false);
	}
}
UT_TEST(fsync_and_real_error_count_failures_keep_original_cache_and_duties)
{
	for (unsigned f = 0; f < 6; f++) {
		redo_reset(true); fail_sync = f == 0; redo_throw_read = f == 1; redo_open_error = f == 2; redo_drift = f == 3;
		short_write = f == 4; fail_write = f == 5;
		UT_ASSERT(!redo_run()); UT_ASSERT(memcmp(page.data, redo_initial, 8192) == 0);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID); UT_ASSERT(dirty[0]);
		UT_ASSERT_EQ(identities[0].image_lsn, 100); redo_done(false);
	}
}
int main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-redo-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(6);
	UT_RUN(original_rmgr_merges_dirty_resident_only_after_durable_same_fd_replay);
	UT_RUN(original_rmgr_miss_reconstructs_complete_selected_frame);
	UT_RUN(replay_is_idempotent_and_retains_newer_resident_source);
	UT_RUN(unknown_legacy_wrong_identity_or_end_refuse_before_disk_mutation);
	UT_RUN(conflicting_terminal_or_cache_birth_never_overwrites);
	UT_RUN(fsync_and_real_error_count_failures_keep_original_cache_and_duties);
	unlink(filename); rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
