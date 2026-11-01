/* Selected native WAL and real FD recovery; context and I/O faults are finite. */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/clog.h"
#include "access/clog_storage.h"
#include "access/clog_wal.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/fd.h"

static ssize_t replay_test_read(int fd, void *bytes, size_t length, off_t offset);
static ssize_t replay_test_write(int fd, const void *bytes, size_t length, off_t offset);
static int replay_test_sync(int fd);
extern bool cluster_clog_storage_replay_v2(int fd, const ClusterClogFileBindingV2 *expected,
	uint8 info, const void *record, Size length, XLogRecPtr end,
	const void *current, Size current_length, XLogRecPtr *out);
#undef pg_pread
#undef pg_pwrite
#define pg_pread replay_test_read
#define pg_pwrite replay_test_write
#define pg_fsync replay_test_sync
#include "../../backend/access/transam/clog_storage.c"
#ifdef CLOG_REPLAY_TEST_SOURCE
#include CLOG_REPLAY_TEST_SOURCE
#else
#include "../../backend/access/transam/clog_replay.c"
#endif
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool InRecovery, enableFsync;
BackendType MyBackendType;
ProcessingMode Mode;
static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 page, memory_page, observed;
static uint8 header_record[8320], page_record[8384], payload[8192], memory_payload[8192];
static uint8 disk_frame[8256], memory_frame[8256], bytes[8192];
static char path[96];
static int file;
static unsigned reads, writes, syncs;
static bool fail_write, short_write, fail_sync, fail_after_sync, header_drift;
static XLogRecPtr applied;

void
ExceptionalCondition(const char *condition, const char *source, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, source, line);
	abort();
}

static ssize_t
replay_test_read(int fd, void *out, size_t length, off_t offset)
{
	ssize_t result;
	reads++;
	if (fail_after_sync && syncs) { errno = EIO; return -1; }
	result = pread(fd, out, length, offset);
	if (header_drift && reads == 1) {
		ClusterClogFileBindingV2 changed = binding;
		uint8 header[8192];
		changed.identity.registration_generation++;
		if (!cluster_clog_file_header_encode_v2(&changed, header, sizeof(header)) ||
			pwrite(fd, header, sizeof(header), 0) != sizeof(header)) abort();
	}
	return result;
}
static ssize_t
replay_test_write(int fd, const void *in, size_t length, off_t offset)
{
	writes++;
	if (fail_write) { errno = ENOSPC; return -1; }
	return pwrite(fd, in, short_write ? length - 1 : length, offset);
}
static int
replay_test_sync(int fd)
{
	syncs++;
	if (fail_sync) { errno = EIO; return -1; }
	return fsync(fd);
}

static void
seal(void)
{
	if (!cluster_recovery_file_identity_encode_v2(&binding.identity, header_record, 128) ||
		!cluster_clog_file_header_encode_v2(&binding, header_record + 128, 8192) ||
		!cluster_recovery_file_identity_encode_v2(&binding.identity, page_record, 128) ||
		!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192, page_record + 128, 8256) ||
		!cluster_clog_file_page_encode_v2(&binding, &memory_page, memory_payload, 8192, memory_frame, 8256))
		abort();
}
static void
reset(void)
{
	memset(&binding, 0, sizeof(binding));
	binding.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	binding.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	binding.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	binding.identity.system_identifier = 123;
	binding.identity.database_incarnation = 456;
	binding.identity.storage_uuid[0] = 1;
	binding.identity.redo_branch_uuid[0] = 2;
	binding.identity.redo_branch_generation = 2;
	binding.identity.redo_timeline = 1;
	binding.identity.creation_generation = 3;
	binding.identity.registration_generation = 4;
	binding.identity.origin = 2;
	binding.identity.object_id = binding.segment = 2;
	memset(&page, 0, sizeof(page));
	page.identity.logical_page = 65;
	page.identity.page_generation = 9;
	page.identity.birth_observed = true;
	page.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + 65 * 32768);
	page.identity.page_lsn = 80; page.image_lsn = 100;
	memory_page = page;
	memset(payload, 0, sizeof(payload)); memset(memory_payload, 0, sizeof(memory_payload));
	seal();
	strlcpy(path, "/tmp/pgrac-clog-replay-XXXXXX", sizeof(path));
	file = mkstemp(path);
	if (file < 0) abort();
	reads = writes = syncs = 0;
	fail_write = short_write = fail_sync = fail_after_sync = header_drift = false;
	InRecovery = true; enableFsync = true; MyBackendType = B_STARTUP; Mode = NormalProcessing;
	applied = 77;
}
static void
finish(void)
{
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
	close(file); unlink(path);
}
static bool
replay_header(void)
{
	return cluster_clog_storage_replay_v2(file, &binding, 0x40,
		header_record, sizeof(header_record), 500, NULL, 0, &applied);
}
static bool
replay_page(bool memory)
{
	return cluster_clog_storage_replay_v2(file, &binding, 0x50,
		page_record, sizeof(page_record), 1000,
		memory ? memory_frame : NULL, memory ? sizeof(memory_frame) : 0, &applied);
}
static void
put_header(void)
{
	if (pwrite(file, header_record + 128, 8192, 0) != 8192) abort();
}
static void
put_disk(void)
{
	if (!cluster_clog_file_page_encode_v2(&binding, &memory_page, memory_payload, 8192, disk_frame, 8256) ||
		pwrite(file, disk_frame, 8256, 8192 + 8256) != 8256) abort();
}
static void
read_page(void)
{
	uint8 frame[8256] = {0};
	memset(&observed, 0, sizeof(observed)); memset(bytes, 0, sizeof(bytes));
	UT_ASSERT_EQ(pread(file, frame, sizeof(frame), 8192 + 8256), sizeof(frame));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 65, frame, sizeof(frame), &observed, bytes, sizeof(bytes)));
}

UT_TEST(torn_header_is_rebuilt_from_selected_native_record)
{
	uint8 header[8192] = {0};
	reset();
	if (pwrite(file, header_record + 128, 177, 0) != 177) abort();
	UT_ASSERT(replay_header()); UT_ASSERT_EQ(applied, 500);
	UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(pread(file, header, sizeof(header), 0), sizeof(header));
	UT_ASSERT(cluster_clog_file_header_matches_v2(header, sizeof(header), &binding));
	UT_ASSERT(replay_header()); /* Exact replay is idempotent. */
	finish();
}
UT_TEST(torn_frame_rebuild_preserves_unrelated_page_and_actual_end)
{
	uint8 other[8256], old[8256], torn[97];
	reset(); put_header();
	memset(other, 0x4d, sizeof(other)); memset(torn, 0xa5, sizeof(torn));
	if (pwrite(file, other, sizeof(other), 8192) != sizeof(other) ||
		pwrite(file, torn, sizeof(torn), 8192 + 8256) != sizeof(torn)) abort();
	payload[0] = TRANSACTION_STATUS_COMMITTED; seal();
	UT_ASSERT(replay_page(false)); UT_ASSERT_EQ(applied, 1000);
	read_page(); UT_ASSERT_EQ(bytes[0], TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(observed.image_lsn, 1000); UT_ASSERT_EQ(observed.identity.page_lsn, 80);
	UT_ASSERT_EQ(pread(file, old, sizeof(old), 8192), sizeof(old));
	UT_ASSERT(memcmp(other, old, sizeof(old)) == 0);
	finish();
}
UT_TEST(valid_disk_terminal_survives_late_image_and_duplicate)
{
	reset(); put_header();
	payload[0] = TRANSACTION_STATUS_SUB_COMMITTED;
	memory_payload[0] = TRANSACTION_STATUS_COMMITTED;
	memory_page.identity.page_lsn = 120; memory_page.image_lsn = 1500;
	seal(); put_disk();
	UT_ASSERT(replay_page(false)); read_page();
	UT_ASSERT_EQ(bytes[0], TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(observed.image_lsn, 1500); UT_ASSERT_EQ(observed.identity.page_lsn, 120);
	UT_ASSERT(replay_page(false)); read_page();
	UT_ASSERT_EQ(bytes[0], TRANSACTION_STATUS_COMMITTED);
	finish();
}
UT_TEST(original_memory_state_must_cover_all_valid_disk_effects)
{
	reset(); put_header();
	memory_payload[0] = TRANSACTION_STATUS_COMMITTED; put_disk();
	memset(memory_payload, 0, sizeof(memory_payload)); seal();
	UT_ASSERT(!replay_page(true)); UT_ASSERT_EQ(applied, 77); UT_ASSERT_EQ(writes, 0);
	memory_payload[0] = TRANSACTION_STATUS_COMMITTED | (TRANSACTION_STATUS_ABORTED << 2);
	memory_page.identity.page_lsn = 90; seal();
	UT_ASSERT(replay_page(true)); read_page();
	UT_ASSERT_EQ(bytes[0], memory_payload[0]); UT_ASSERT_EQ(observed.identity.page_lsn, 90);
	finish();
}
UT_TEST(conflicting_terminals_refuse_without_physical_mutation)
{
	reset(); put_header();
	payload[0] = TRANSACTION_STATUS_COMMITTED;
	memory_payload[0] = TRANSACTION_STATUS_ABORTED;
	seal(); put_disk();
	UT_ASSERT(!replay_page(false)); UT_ASSERT_EQ(applied, 77); UT_ASSERT_EQ(writes + syncs, 0);
	read_page(); UT_ASSERT_EQ(bytes[0], TRANSACTION_STATUS_ABORTED);
	finish();
}
UT_TEST(different_registered_header_or_page_birth_is_not_repaired)
{
	uint8 other[8192];
	ClusterClogFileBindingV2 changed;
	reset(); changed = binding; changed.identity.creation_generation++;
	if (!cluster_clog_file_header_encode_v2(&changed, other, sizeof(other)) ||
		pwrite(file, other, sizeof(other), 0) != sizeof(other)) abort();
	UT_ASSERT(!replay_header()); UT_ASSERT_EQ(writes + syncs, 0);
	put_header(); memory_page.identity.birth_full_xid =
		FullTransactionIdFromU64(U64FromFullTransactionId(page.identity.birth_full_xid) + (UINT64CONST(1) << 32));
	put_disk();
	UT_ASSERT(!replay_page(false)); UT_ASSERT_EQ(writes + syncs, 0);
	finish();
}
UT_TEST(invalid_source_record_end_or_framing_refuses_before_io)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterClogFileBindingV2 expected;
		XLogRecPtr end = 1000;
		Size length = sizeof(page_record);
		uint8 info = 0x50;
		reset(); put_header(); expected = binding;
		if (fault == 0) expected.identity.database_incarnation++;
		if (fault == 1) expected.identity.registration_generation++;
		if (fault == 2) expected.identity.redo_branch_uuid[1]++;
		if (fault == 3) page_record[128 + 64] ^= 1;
		if (fault == 4) end = 0;
		if (fault == 5) end = UINT64_MAX;
		if (fault == 6) end = page.image_lsn;
		if (fault == 7) length--;
		if (fault == 8) info = 0x60;
		UT_ASSERT(!cluster_clog_storage_replay_v2(file, &expected, info, page_record, length, end, NULL, 0, &applied));
		UT_ASSERT_EQ(reads + writes + syncs, 0); UT_ASSERT_EQ(applied, 77);
		finish();
	}
}
UT_TEST(recovery_context_and_fsync_are_required_without_new_wal)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		reset();
		if (fault == 0) InRecovery = false;
		if (fault == 1) MyBackendType = B_BACKEND;
		if (fault == 2) enableFsync = false;
		if (fault == 3) Mode = BootstrapProcessing;
		UT_ASSERT(!replay_header()); UT_ASSERT_EQ(reads + writes + syncs, 0);
		UT_ASSERT_EQ(applied, 77); finish();
	}
}
UT_TEST(readonly_and_append_borrowed_fds_are_refused)
{
	int readonly_fd, original;
	reset(); original = file;
	readonly_fd = open(path, O_RDONLY);
	if (readonly_fd < 0) abort();
	file = readonly_fd; UT_ASSERT(!replay_header()); close(readonly_fd);
	file = original;
	if (fcntl(file, F_SETFL, O_APPEND) != 0) abort();
	UT_ASSERT(!replay_header());
	if (fcntl(file, F_SETFL, 0) != 0) abort();
	UT_ASSERT_EQ(reads + writes + syncs, 0); UT_ASSERT_EQ(applied, 77);
	finish();
}
UT_TEST(write_sync_and_readback_failure_retain_fd_output_and_partial_material)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		struct stat st;
		reset(); put_header();
		fail_write = fault == 0; short_write = fault == 1;
		fail_sync = fault == 2; fail_after_sync = fault == 3;
		UT_ASSERT(!replay_page(false)); UT_ASSERT_EQ(applied, 77);
		UT_ASSERT_EQ(writes, 1); UT_ASSERT_EQ(fstat(file, &st), 0);
		UT_ASSERT(st.st_size >= 8192);
		/* The original recovery owner can retry its same retained record. */
		fail_write = short_write = fail_sync = fail_after_sync = false;
		UT_ASSERT(replay_page(false)); read_page();
		UT_ASSERT_EQ(observed.image_lsn, 1000); finish();
	}
}
UT_TEST(header_drift_before_page_write_does_not_overwrite)
{
	reset(); put_header(); header_drift = true;
	UT_ASSERT(!replay_page(false)); UT_ASSERT_EQ(applied, 77);
	UT_ASSERT_EQ(writes + syncs, 0); finish();
}
UT_TEST(missing_corrupt_header_and_legacy_raw_page_get_no_zero_fallback)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		reset();
		if (fault == 1) {
			uint8 bad[8192] = {0};
			if (pwrite(file, bad, sizeof(bad), 0) != sizeof(bad)) abort();
		}
		if (fault == 2) {
			put_header();
			if (pwrite(file, "x", 1, 0) != 1) abort();
		}
		UT_ASSERT(!replay_page(false)); UT_ASSERT_EQ(applied, 77);
		UT_ASSERT_EQ(writes + syncs, 0); finish();
	}
}
UT_TEST(bad_arguments_and_output_aliases_preserve_all_inputs)
{
	uint8 saved[8384];
	reset(); memcpy(saved, page_record, sizeof(saved));
	UT_ASSERT(!cluster_clog_storage_replay_v2(-1, &binding, 0x50, page_record, sizeof(page_record), 1000, NULL, 0, &applied));
	UT_ASSERT(!cluster_clog_storage_replay_v2(file, NULL, 0x50, page_record, sizeof(page_record), 1000, NULL, 0, &applied));
	UT_ASSERT(!cluster_clog_storage_replay_v2(file, &binding, 0x50, page_record, sizeof(page_record), 1000, NULL, 1, &applied));
	UT_ASSERT(!cluster_clog_storage_replay_v2(file, &binding, 0x50, page_record, sizeof(page_record), 1000, memory_frame, 1, &applied));
	UT_ASSERT(!cluster_clog_storage_replay_v2(file, &binding, 0x50, page_record, sizeof(page_record), 1000, NULL, 0, (XLogRecPtr *)page_record));
	UT_ASSERT_EQ(reads + writes + syncs, 0); UT_ASSERT_EQ(applied, 77);
	UT_ASSERT(memcmp(saved, page_record, sizeof(saved)) == 0); finish();
}
int
main(void)
{
	(void)replay_test_read; (void)replay_test_write; (void)replay_test_sync;
	UT_PLAN(13);
	UT_RUN(torn_header_is_rebuilt_from_selected_native_record);
	UT_RUN(torn_frame_rebuild_preserves_unrelated_page_and_actual_end);
	UT_RUN(valid_disk_terminal_survives_late_image_and_duplicate);
	UT_RUN(original_memory_state_must_cover_all_valid_disk_effects);
	UT_RUN(conflicting_terminals_refuse_without_physical_mutation);
	UT_RUN(different_registered_header_or_page_birth_is_not_repaired);
	UT_RUN(invalid_source_record_end_or_framing_refuses_before_io);
	UT_RUN(recovery_context_and_fsync_are_required_without_new_wal);
	UT_RUN(readonly_and_append_borrowed_fds_are_refused);
	UT_RUN(write_sync_and_readback_failure_retain_fd_output_and_partial_material);
	UT_RUN(header_drift_before_page_write_does_not_overwrite);
	UT_RUN(missing_corrupt_header_and_legacy_raw_page_get_no_zero_fallback);
	UT_RUN(bad_arguments_and_output_aliases_preserve_all_inputs);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
