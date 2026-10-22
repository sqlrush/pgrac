/*-------------------------------------------------------------------------
 * test_cluster_recovery_tt_io.c
 *    Held-descriptor C0 storage tests against real temporary undo files.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <fcntl.h>
#include <setjmp.h>
#include <sys/stat.h>
#include <unistd.h>
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/cluster_undo_record_api.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/resowner.h"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static char test_dir[MAXPGPATH];
static char test_file[MAXPGPATH];
static int owner_storage[2];
ResourceOwner CurrentResourceOwner;
static ResourceReleaseCallback release_callback;
static void *release_arg;
static pg_on_exit_callback exit_callback;
static Datum exit_arg;
static int external_fds, opens, resolves, reads, writes, syncs, closes;
static bool scope_current = true, capacity = true, open_error;
static bool read_short, write_short, sync_error, close_error, stat_error;
static bool corrupt_postread;
static int write_fd = -1;
static ClusterRecoveryTTAbortV1 record;
static PGAlignedBlock initial;
static bool header_locked;
static char tx_scope_storage;
static bool tx_scope_current;
static int tx_resolves;
static jmp_buf report_jump;
static bool report_armed, sync_retry;
static int reported_level, sync_base_level;

int
data_sync_elevel(int elevel)
{
	sync_base_level = elevel;
	return sync_retry ? elevel : PANIC;
}

static void
test_report(int elevel)
{
	reported_level = elevel;
	if (!report_armed)
		abort();
	longjmp(report_jump, 1);
}

bool
cluster_wal_inputs_tx_abort_path_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *candidate, char *out, size_t size)
{
	tx_resolves++;
	if (!tx_scope_current || inputs != (ClusterWalInputsV1 *)&tx_scope_storage
		|| memcmp(candidate, &record, sizeof(record)) != 0 || strlen(test_file) >= size)
		return false;
	memcpy(out, test_file, strlen(test_file) + 1);
	return true;
}

bool
cluster_undo_block0_header_write_lock(uint32 segment, uint8 owner, uint32 *acquired)
{
	UT_ASSERT_EQ(segment, 257);
	UT_ASSERT_EQ(owner, 2);
	UT_ASSERT(!header_locked);
	header_locked = true;
	*acquired = 1;
	return true;
}
void
cluster_undo_block0_header_write_unlock(uint32 acquired)
{
	if (acquired == UINT32_MAX)
		return;
	UT_ASSERT(header_locked);
	header_locked = false;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s %s:%d\n", condition, file, line);
	abort();
}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	release_callback = callback;
	release_arg = arg;
}
void
before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	exit_callback = callback;
	exit_arg = arg;
}
bool
AcquireExternalFD(void)
{
	if (!capacity)
		return false;
	external_fds++;
	return true;
}
void
ReleaseExternalFD(void)
{
	UT_ASSERT(external_fds > 0);
	external_fds--;
}
int
BasicOpenFile(const char *path, int flags)
{
	opens++;
	if (open_error) {
		errno = EACCES;
		return -1;
	}
	return open(path, flags, 0600);
}
int
cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 instance, uint32 segment, char *out,
						  size_t size)
{
	resolves++;
	if (!scope_current || intent != CLUSTER_UNDO_PATH_RECOVERY_SHARED || instance != 2
		|| segment != 257 || strlen(test_file) >= size)
		return -1;
	memcpy(out, test_file, strlen(test_file) + 1);
	return 0;
}
void
cluster_undo_record_note_smgr_open(void)
{}
void
cluster_undo_record_note_smgr_close(void)
{}
void
cluster_undo_record_note_smgr_pread(void)
{}
void
cluster_undo_record_note_smgr_pwrite(void)
{}
int
pg_fsync(int fd)
{
	syncs++;
	if (sync_error) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
static ssize_t
io_pread(int fd, void *buffer, size_t size, off_t offset)
{
	ssize_t result;
	reads++;
	result = pread(fd, buffer, read_short && size > 0 ? size - 1 : size, offset);
	if (corrupt_postread && writes != 0 && result == (ssize_t)size && size > 0)
		((char *)buffer)[size - 1] ^= 1;
	return result;
}
static ssize_t
io_pwrite(int fd, const void *buffer, size_t size, off_t offset)
{
	writes++;
	write_fd = fd;
	UT_ASSERT(header_locked);
	UT_ASSERT_EQ(size, BLCKSZ);
	UT_ASSERT_EQ(offset, 0);
	return pwrite(fd, buffer, write_short && size > 0 ? size - 1 : size, offset);
}
static int
io_fstat(int fd, struct stat *st)
{
	if (stat_error) {
		errno = EIO;
		return -1;
	}
	return fstat(fd, st);
}
static int
io_close(int fd)
{
	int result = close(fd);
	closes++;
	if (close_error) {
		errno = EIO;
		return -1;
	}
	return result;
}
#undef pg_pread
#undef pg_pwrite
#define pg_pread io_pread
#define pg_pwrite io_pwrite
#define fstat io_fstat
#define close io_close
#undef ereport
#define ereport(elevel, rest) test_report(elevel)
#include "../../backend/cluster/storage/cluster_undo_smgr.c"
#undef ereport
#undef close
#undef fstat
#undef pg_pwrite
#undef pg_pread

static void
reset_file(bool already)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)initial.data;
	int fd;

	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	if (release_callback != NULL)
		release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT(!header_locked);
	opens = resolves = reads = writes = syncs = closes = 0;
	tx_resolves = 0;
	tx_scope_current = true;
	scope_current = capacity = true;
	open_error = read_short = write_short = sync_error = close_error = stat_error = false;
	corrupt_postread = false;
	write_fd = -1;
	memset(&record, 0, sizeof(record));
	record.version = CLUSTER_RECOVERY_TT_ABORT_VERSION;
	record.length = CLUSTER_RECOVERY_TT_ABORT_BYTES;
	record.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	record.target_thread = 2;
	record.writer_thread = 1;
	record.target_incarnation = 8;
	record.writer_incarnation = 9;
	record.recovery_serial = 10;
	record.target_epoch = 11;
	record.target_full_xid = record.top_full_xid = UINT64CONST(0x100000064);
	record.target_claim_sha256[0] = 0x89;
	record.exact.segment_id = 257;
	record.exact.segment_generation = 0;
	record.exact.xid = 100;
	record.exact.slot_offset = 3;
	record.exact.wrap = 2;
	record.exact.instance = 2;
	record.exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	record.before.xid = 100;
	record.before.wrap = 2;
	record.before.status = TT_SLOT_ACTIVE;
	record.before.first_undo_block.raw[0] = UINT64CONST(0x100000101);
	record.before.first_undo_block.raw[1] = UINT64CONST(0x40003);
	cluster_undo_segment_make_header_bytes(257, 2, initial.data);
	header->pd_flags = PD_UNDO_SEG_HEADER;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->segment_id = 257;
	header->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header->segment_state = SEGMENT_ACTIVE;
	header->owner_instance = 2;
	header->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	header->wrap_count = 0;
	header->tt_slots[3] = record.before;
	if (already)
		header->tt_slots[3].status = TT_SLOT_ABORTED;
	initial.data[BLCKSZ - 1] = 0x6a;
	UT_ASSERT(cluster_undo_segment_header_seal(initial.data));
	(void)unlink(test_file);
	fd = open(test_file, O_RDWR | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, initial.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}
static void
change_byte(off_t offset, char value)
{
	int fd = open(test_file, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pwrite(fd, &value, 1, offset), 1);
		UT_ASSERT_EQ(close(fd), 0);
	}
}
static bool
prepare(ClusterUndoSmgrRecoveryStampV1 *stamp)
{
	bool ok = cluster_undo_smgr_recovery_abort_prepare_v1(&record, stamp);
	UT_ASSERT(ok);
	UT_ASSERT(stamp->token != 0);
	return ok;
}
UT_TEST(test_exact_durable_slot_preserves_whole_header_and_head)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	PGAlignedBlock actual, expected;
	int fd;
	reset_file(false);
	if (!prepare(&stamp))
		return;
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(external_fds, 1);
	/* Apply cannot revisit scope/path or delegate to checkpoint. */
	scope_current = false;
	opens = resolves = 0;
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT_EQ(opens, 0);
	UT_ASSERT_EQ(resolves, 0);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	memcpy(expected.data, initial.data, BLCKSZ);
	((UndoSegmentHeaderData *)expected.data)->tt_slots[3].status = TT_SLOT_ABORTED;
	UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
	fd = open(test_file, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pread(fd, actual.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(memcmp(actual.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT(fcntl(write_fd, F_GETFD) < 0 && errno == EBADF);
}

UT_TEST(test_cold_prepare_uses_exact_tx_scope_without_redo_path_or_writes)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	PGAlignedBlock actual;
	int fd;
	reset_file(false);
	scope_current = false; /* No pre-EOR redo scope exists at this boundary. */
	UT_ASSERT(cluster_undo_smgr_recovery_abort_prepare_tx_v1(
		(ClusterWalInputsV1 *)&tx_scope_storage, &record, &stamp));
	UT_ASSERT(stamp.token != 0);
	UT_ASSERT_EQ(tx_resolves, 1);
	UT_ASSERT_EQ(resolves, 0);
	UT_ASSERT_EQ(external_fds, 1);
	UT_ASSERT_EQ(writes + syncs, 0);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	UT_ASSERT_EQ(external_fds, 0);
	fd = open(test_file, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pread(fd, actual.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, initial.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

UT_TEST(test_cold_prepare_refuses_bad_scope_crc_before_image_and_releases_owner)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 99 };
		ClusterWalInputsV1 *inputs = (ClusterWalInputsV1 *)&tx_scope_storage;
		bool ok;
		reset_file(false);
		switch (fault) {
		case 0: tx_scope_current = false; break;
		case 1: inputs = NULL; break;
		case 2: change_byte(BLCKSZ - 1, 0x6b); break;
		case 3: read_short = true; break;
		case 4: capacity = false; break;
		case 5: record.before.flags ^= 1; break;
		case 6: break;
		}
		ok = cluster_undo_smgr_recovery_abort_prepare_tx_v1(inputs, &record, &stamp);
		UT_ASSERT_EQ(ok, fault == 6);
		if (ok) {
			release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
			UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		} else UT_ASSERT_EQ(stamp.token, 0);
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT_EQ(writes + syncs + resolves, 0);
	}
}
UT_TEST(test_idempotent_still_syncs_and_reads_without_rewriting)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	int before_reads;
	reset_file(true);
	if (!prepare(&stamp))
		return;
	before_reads = reads;
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT(reads >= before_reads + 2);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 2);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
}
UT_TEST(test_prepare_refusals_do_not_leak_or_write)
{
	for (int mode = 0; mode < 10; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 999 };
		reset_file(false);
		switch (mode) {
		case 0:
			record.before.xid++;
			break;
		case 1:
			scope_current = false;
			break;
		case 2:
			capacity = false;
			break;
		case 3:
			open_error = true;
			break;
		case 4:
			read_short = true;
			break;
		case 5:
			stat_error = true;
			break;
		case 6:
			change_byte(offsetof(UndoSegmentHeaderData, wrap_count), 1);
			break;
		case 7:
			change_byte(offsetof(UndoSegmentHeaderData, tt_slots) + 3 * sizeof(TTSlot)
							+ offsetof(TTSlot, status),
						TT_SLOT_COMMITTED);
			break;
		case 8:
			UT_ASSERT_EQ(truncate(test_file, BLCKSZ), 0);
			break;
		case 9:
			CurrentResourceOwner = NULL;
			break;
		}
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_prepare_v1(&record, &stamp));
		UT_ASSERT_EQ(stamp.token, 0);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(external_fds, 0);
	}
}
UT_TEST(test_changed_header_or_unlinked_file_is_sticky_refusal)
{
	for (int mode = 0; mode < 4; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		reset_file(false);
		if (!prepare(&stamp))
			continue;
		if (mode == 0)
			change_byte(BLCKSZ - 1, 0x6b);
		if (mode == 1)
			change_byte(offsetof(UndoSegmentHeaderData, wrap_count), 1);
		if (mode == 2)
			UT_ASSERT_EQ(unlink(test_file), 0);
		if (mode == 3)
			UT_ASSERT_EQ(truncate(test_file, BLCKSZ), 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	}
}
UT_TEST(test_io_refusal_never_becomes_durable_on_same_handle)
{
	for (int mode = 0; mode < 4; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		reset_file(false);
		if (!prepare(&stamp))
			continue;
		if (mode == 0)
			read_short = true;
		if (mode == 1)
			write_short = true;
		if (mode == 2)
			corrupt_postread = true;
		if (mode == 3)
			stat_error = true;
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		read_short = write_short = sync_error = corrupt_postread = stat_error = false;
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}
UT_TEST(test_sync_failure_uses_native_severity_after_unlock_and_stays_failed)
{
	for (volatile int mode = 0; mode < 2; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		reset_file(false);
		if (!prepare(&stamp))
			continue;
		sync_retry = mode != 0;
		sync_base_level = reported_level = 0;
		sync_error = report_armed = true;
		if (setjmp(report_jump) == 0) {
			(void)cluster_undo_smgr_recovery_abort_write_v1(stamp);
			report_armed = false;
			UT_ASSERT(false);
		} else
			report_armed = false;
		UT_ASSERT_EQ(sync_base_level, ERROR);
		UT_ASSERT_EQ(reported_level, sync_retry ? ERROR : PANIC);
		UT_ASSERT(!header_locked);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		sync_error = false;
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}
UT_TEST(test_owner_token_capacity_and_release)
{
	ClusterUndoSmgrRecoveryStampV1 stamp, other;
	reset_file(false);
	if (!prepare(&stamp))
		return;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_prepare_v1(&record, &other));
	UT_ASSERT_EQ(other.token, 0);
	UT_ASSERT_EQ(external_fds, 1);
	other.token = stamp.token + 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(other));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_close_v1(stamp));
	release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 1);
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	release_callback(RESOURCE_RELEASE_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 1);
	release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	if (!prepare(&other))
		return;
	UT_ASSERT(other.token != stamp.token);
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(other));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_close_v1(other));
}
UT_TEST(test_close_error_and_exit_release_do_not_publish_completion)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	reset_file(false);
	if (!prepare(&stamp))
		return;
	close_error = true;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_close_v1(stamp));
	UT_ASSERT_EQ(external_fds, 0);
	close_error = false;
	if (!prepare(&stamp))
		return;
	UT_ASSERT_NOT_NULL(exit_callback);
	if (exit_callback != NULL)
		exit_callback(0, exit_arg);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
}
int
main(void)
{
	char template[] = "/tmp/pgrac-c0-io-XXXXXX";
	char *dir = mkdtemp(template);
	if (dir == NULL)
		return 2;
	snprintf(test_dir, sizeof(test_dir), "%s", dir);
	snprintf(test_file, sizeof(test_file), "%s/seg_257.dat", dir);
	UT_PLAN(10);
	UT_RUN(test_exact_durable_slot_preserves_whole_header_and_head);
	UT_RUN(test_cold_prepare_uses_exact_tx_scope_without_redo_path_or_writes);
	UT_RUN(test_cold_prepare_refuses_bad_scope_crc_before_image_and_releases_owner);
	UT_RUN(test_idempotent_still_syncs_and_reads_without_rewriting);
	UT_RUN(test_prepare_refusals_do_not_leak_or_write);
	UT_RUN(test_changed_header_or_unlinked_file_is_sticky_refusal);
	UT_RUN(test_io_refusal_never_becomes_durable_on_same_handle);
	UT_RUN(test_sync_failure_uses_native_severity_after_unlock_and_stays_failed);
	UT_RUN(test_owner_token_capacity_and_release);
	UT_RUN(test_close_error_and_exit_release_do_not_publish_completion);
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	if (release_callback != NULL)
		release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	(void)unlink(test_file);
	(void)rmdir(test_dir);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
