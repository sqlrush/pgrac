/*-------------------------------------------------------------------------
 * test_cluster_recovery_tt_io.c
 *    Held-descriptor C0/D0 storage tests against real temporary undo files.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "miscadmin.h"
#include <fcntl.h>
#include <setjmp.h>
#include "access/rmgr.h"
#include "utils/elog.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include <sys/stat.h>
#include <unistd.h>
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_recovery_ctrc_record.h"
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
#include "test_cluster_d0_native_reader_fixture.h"

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
static bool tx_loss_on_lock, tx_loss_on_stat, tx_loss_on_read;
static int tx_resolves;
static uint8 d0_payload[CLUSTER_RECOVERY_CTRC_BYTES];
static const uint64 d0_system = UINT64_C(0x1020304050607080);
static jmp_buf sync_failure_jump;
static bool catch_sync_failure;
static int sync_level, report_level;
static jmp_buf report_jump;
static bool report_armed, sync_retry;
static int reported_level, sync_base_level;
volatile uint32 CritSectionCount;

int
data_sync_elevel(int elevel)
{
	sync_base_level = sync_level = elevel;
	return sync_retry ? elevel : PANIC;
}

static void
test_report(int elevel)
{
	reported_level = report_level = elevel;
	if (report_armed)
		longjmp(report_jump, 1);
	if (catch_sync_failure)
		longjmp(sync_failure_jump, 1);
	abort();
}

bool
cluster_wal_inputs_tx_abort_authorized_v1(ClusterWalInputsV1 *inputs,
	const ClusterRecoveryTTAbortV1 *candidate)
{
	return tx_scope_current && inputs == (ClusterWalInputsV1 *)&tx_scope_storage
		&& memcmp(candidate, &record, sizeof(record)) == 0;
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
errstart(int elevel, const char *domain)
{
	report_level = elevel;
	if (catch_sync_failure)
		longjmp(sync_failure_jump, 1);
	abort();
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}


bool
cluster_undo_block0_header_write_lock(uint32 segment, uint8 owner, uint32 *acquired)
{
	UT_ASSERT_EQ(segment, 257);
	UT_ASSERT_EQ(owner, 2);
	UT_ASSERT(!header_locked);
	header_locked = true;
	*acquired = 1;
	if (tx_loss_on_lock)
		tx_scope_current = false;
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
	if (header_locked && tx_loss_on_read)
		tx_scope_current = false;
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
	if (header_locked && tx_loss_on_stat)
		tx_scope_current = false;
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
	tx_loss_on_lock = tx_loss_on_stat = tx_loss_on_read = false;
	scope_current = capacity = true;
	open_error = read_short = write_short = sync_error = close_error = stat_error = false;
	corrupt_postread = false;
	CritSectionCount = 0;
	report_armed = catch_sync_failure = sync_retry = false;
	sync_base_level = reported_level = sync_level = report_level = 0;
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
UT_TEST(test_durable_fact_requires_exact_completed_live_stamp)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	ClusterRecoveryTTAbortV1 changed;
	reset_file(false);
	if (!prepare(&stamp))
		return;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(
		(ClusterUndoSmgrRecoveryStampV1){stamp.token + 1}, &record));
	changed = record;
	changed.writer_incarnation++;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &changed));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, NULL));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));

	reset_file(false);
	if (!prepare(&stamp))
		return;
	write_short = true;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	write_short = false;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
}

/* Actual original D0 codec/target adapter, sharing C0's real temporary FD. */
static bool
prepare_d0_with_c0_record(ClusterUndoSmgrRecoveryStampV1 *stamp, uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES])
{
	ClusterRecoveryCTRCReleaseV2 release = { 0 };
	const UndoSegmentHeaderData *header;
	bool ok;
	reset_file(true);
	CritSectionCount = 0;
	header = (const UndoSegmentHeaderData *)initial.data;
	release.version = CLUSTER_RECOVERY_CTRC_VERSION;
	release.length = CLUSTER_RECOVERY_CTRC_BYTES;
	release.flags = CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN;
	release.root_id = 1;
	release.root_generation = 2;
	release.formation_epoch = 7;
	release.recovery_serial = record.recovery_serial;
	release.database_incarnation = 8;
	release.writer_boot = record.writer_incarnation;
	release.cohort_bitmap = 3;
	release.writer_thread = record.writer_thread;
	release.target_owner = record.target_thread;
	release.origin_node_id = record.target_thread - 1;
	release.segment_id = record.exact.segment_id;
	release.segment_generation = header->wrap_count;
	release.slot_offset = record.exact.slot_offset;
	release.wrap = record.exact.wrap;
	release.xid = record.exact.xid;
	release.terminal_outcome = TT_SLOT_ABORTED;
	release.before = header->tt_slots[release.slot_offset];
	release.after = release.before;
	release.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	memset(release.graph_sha256, 3, sizeof(release.graph_sha256));
	memset(release.input_sha256, 4, sizeof(release.input_sha256));
	ok = cluster_recovery_ctrc_encode_v2(&release, 99, payload, CLUSTER_RECOVERY_CTRC_BYTES);
	UT_ASSERT(ok);
	if (!ok)
		return false;
	ok = cluster_undo_smgr_recovery_ctrc_prepare_v2(payload, CLUSTER_RECOVERY_CTRC_BYTES, 99, stamp);
	UT_ASSERT(ok);
	return ok;
}

UT_TEST(test_c0_tx_record_and_d0_share_stage_without_cross_kind_authority)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	ClusterRecoveryTTAbortV1 changed;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES], changed_payload[CLUSTER_RECOVERY_CTRC_BYTES];
	int before_io;
	reset_file(false);
	scope_current = false;
	UT_ASSERT(cluster_undo_smgr_recovery_abort_prepare_tx_v1(
		(ClusterWalInputsV1 *)&tx_scope_storage, &record, &stamp));
	UT_ASSERT_EQ(tx_resolves, 1);
	UT_ASSERT_EQ(resolves, 0);
	before_io = writes + reads + syncs;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	UT_ASSERT_EQ(writes + reads + syncs, before_io);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	changed = record;
	changed.recovery_serial++;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &changed));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	if (!prepare_d0_with_c0_record(&stamp, payload))
		return;
	before_io = writes + reads + syncs;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_close_v1(stamp));
	UT_ASSERT_EQ(writes + reads + syncs, before_io);
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, payload, sizeof(payload)));
	memcpy(changed_payload, payload, sizeof(payload));
	changed_payload[319] ^= 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, changed_payload, sizeof(payload)));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	/* A D0 stage has no C0 record; zero bytes must not manufacture a C0 fact. */
	memset(&changed, 0, sizeof(changed));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &changed));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	UT_ASSERT_EQ(external_fds, 0);
}

UT_TEST(test_d0_sync_failure_keeps_native_error_unlock_and_sticky_debt)
{
	for (unsigned retry = 0; retry < 2; retry++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
		if (!prepare_d0_with_c0_record(&stamp, payload))
			continue;
		sync_retry = retry != 0;
		sync_base_level = reported_level = 0;
		sync_error = report_armed = true;
		if (setjmp(report_jump) == 0) {
			(void)cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload));
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
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}


/* Real canonical D0 and real native checksummed page; authority is a fixture. */
static void
reset_d0(bool committed)
{
	ClusterRecoveryCTRCReleaseV2 d = { 0 };
	UndoSegmentHeaderData *header;
	int fd;

	reset_file(false);
	d.version = CLUSTER_RECOVERY_CTRC_VERSION;
	d.length = sizeof(d0_payload);
	d.flags = CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN;
	d.root_id = 1;
	d.root_generation = 2;
	d.formation_epoch = 3;
	d.recovery_serial = 4;
	d.database_incarnation = 5;
	d.writer_boot = 6;
	d.cohort_bitmap = 3;
	d.origin_node_id = 1;
	d.writer_thread = 1;
	d.target_owner = 2;
	d.slot_offset = 3;
	d.wrap = 2;
	d.segment_id = 257;
	d.xid = 100;
	d.terminal_outcome = committed ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
	d.before = record.before;
	d.before.status = d.terminal_outcome;
	d.before.commit_scn = committed ? 1234 : InvalidScn;
	d.after = d.before;
	d.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	d.graph_sha256[0] = 1;
	d.input_sha256[0] = 2;
	UT_ASSERT(cluster_recovery_ctrc_encode_v2(&d, d0_system, d0_payload, sizeof(d0_payload)));
	header = (UndoSegmentHeaderData *)initial.data;
	header->tt_slots[3] = d.before;
	UT_ASSERT(cluster_undo_segment_header_seal(initial.data));
	fd = open(test_file, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pwrite(fd, initial.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
	}
}
static bool
prepare_d0(ClusterUndoSmgrRecoveryStampV1 *stamp)
{
	bool ok = cluster_undo_smgr_recovery_ctrc_prepare_v2(d0_payload, sizeof(d0_payload), d0_system,
														 stamp);
	UT_ASSERT(ok);
	UT_ASSERT(stamp->token != 0);
	return ok;
}
static void
replace_d0_page(void)
{
	int fd = open(test_file, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(pwrite(fd, initial.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
	}
}
UT_TEST(test_d0_terminal_pages_sync_exactly_on_held_fd)
{
	for (int committed = 0; committed < 2; committed++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		PGAlignedBlock expected, actual;
		int fd;
		reset_d0(committed);
		if (!prepare_d0(&stamp))
			continue;
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(external_fds, 1);
		scope_current = false;
		opens = resolves = 0;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT_EQ(opens, 0);
		UT_ASSERT_EQ(resolves, 0);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		memcpy(expected.data, initial.data, BLCKSZ);
		((UndoSegmentHeaderData *)expected.data)->tt_slots[3].flags
			= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
		fd = open(test_file, O_RDONLY);
		UT_ASSERT(fd >= 0);
		if (fd >= 0) {
			UT_ASSERT_EQ(pread(fd, actual.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(memcmp(expected.data, actual.data, BLCKSZ), 0);
			UT_ASSERT_EQ(close(fd), 0);
		}
		/* Same successful held attempt rechecks and syncs without rewriting. */
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 2);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT(fcntl(write_fd, F_GETFD) < 0 && errno == EBADF);
	}
}
UT_TEST(test_d0_prepare_requires_canonical_exact_before_outside_window)
{
	for (int mode = 0; mode < 15; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 99 };
		UndoSegmentHeaderData *header;
		uint64 system = d0_system;
		size_t length = sizeof(d0_payload);
		reset_d0(true);
		header = (UndoSegmentHeaderData *)initial.data;
		switch (mode) {
		case 0:
			d0_payload[20] ^= 1;
			break;
		case 1:
			system++;
			break;
		case 2:
			length--;
			break;
		case 3:
			scope_current = false;
			break;
		case 4:
			capacity = false;
			break;
		case 5:
			open_error = true;
			break;
		case 6:
			read_short = true;
			break;
		case 7:
			stat_error = true;
			break;
		case 8:
			CurrentResourceOwner = NULL;
			break;
		case 9:
			CritSectionCount = 1;
			break;
		case 10:
			UT_ASSERT_EQ(truncate(test_file, BLCKSZ), 0);
			break;
		case 11:
			header->tt_slots[3].flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
			break;
		case 12:
			header->wrap_count++;
			break;
		case 13:
			header->tt_slots[3].commit_scn++;
			break;
		case 14:
			initial.data[BLCKSZ - 1] ^= 1;
			break;
		}
		if (mode >= 11 && mode <= 13)
			UT_ASSERT(cluster_undo_segment_header_seal(initial.data));
		if (mode >= 11)
			replace_d0_page();
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_v2(d0_payload, length, system, &stamp));
		UT_ASSERT_EQ(stamp.token, 0);
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(syncs, 0);
	}
	reset_d0(true);
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_v2(d0_payload, sizeof(d0_payload), d0_system,
														  NULL));
	UT_ASSERT_EQ(opens, 0);
}
UT_TEST(test_d0_and_c0_share_owner_stage_but_not_token_kind)
{
	ClusterUndoSmgrRecoveryStampV1 stamp, other;
	reset_file(false);
	if (!prepare(&stamp))
		return;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_v2(d0_payload, sizeof(d0_payload), d0_system,
														  &other));
	UT_ASSERT_EQ(other.token, 0);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	reset_d0(true);
	if (!prepare_d0(&stamp))
		return;
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_close_v1(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_abort_prepare_v1(&record, &other));
	other.token = stamp.token + 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(other));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 1);
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
}
UT_TEST(test_d0_changed_target_refuses_before_writing_and_stays_failed)
{
	for (int mode = 0; mode < 3; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		reset_d0(true);
		if (!prepare_d0(&stamp))
			continue;
		if (mode == 0)
			change_byte(BLCKSZ - 1, 0x6b);
		if (mode == 1)
			UT_ASSERT_EQ(unlink(test_file), 0);
		if (mode == 2)
			UT_ASSERT_EQ(truncate(test_file, BLCKSZ), 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	}
}
UT_TEST(test_d0_failed_io_is_sticky_and_sync_uses_native_severity)
{
	for (int mode = 0; mode < 6; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		reset_d0(false);
		if (!prepare_d0(&stamp))
			continue;
		if (mode == 0)
			read_short = true;
		if (mode == 1)
			write_short = true;
		if (mode == 2 || mode == 3)
			sync_error = true;
		if (mode == 4)
			corrupt_postread = true;
		if (mode == 5)
			stat_error = true;
		if (sync_error) {
			sync_retry = mode == 3;
			catch_sync_failure = true;
			if (setjmp(sync_failure_jump) == 0) {
				(void)cluster_undo_smgr_recovery_ctrc_write_v2(stamp);
				UT_ASSERT(false);
			}
			catch_sync_failure = false;
			UT_ASSERT_EQ(sync_level, ERROR);
			UT_ASSERT_EQ(report_level, sync_retry ? ERROR : PANIC);
		} else
			UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT(!header_locked);
		read_short = write_short = sync_error = corrupt_postread = stat_error = false;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}
UT_TEST(test_d0_owner_and_exit_cleanup_only_release_descriptor)
{
	ClusterUndoSmgrRecoveryStampV1 stamp, fresh;
	reset_d0(true);
	if (!prepare_d0(&stamp))
		return;
	release_callback(RESOURCE_RELEASE_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 1);
	release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
	if (!prepare_d0(&fresh))
		return;
	UT_ASSERT(fresh.token != stamp.token);
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
	exit_callback(0, exit_arg);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(fresh));
	if (!prepare_d0(&fresh))
		return;
	close_error = true;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_close_v2(fresh));
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 0);
}

UT_TEST(test_d0_bound_write_exact_canonical_payload_on_held_fd)
{
	for (int committed = 0; committed < 2; committed++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		uint8 bound[320];
		reset_d0(committed);
		if (!prepare_d0(&stamp))
			continue;
		memcpy(bound, d0_payload, sizeof(bound));
		d0_payload[0] ^= 1; /* stage owns a full copy of prepared canonical bytes */
		scope_current = false;
		opens = resolves = 0;
		CritSectionCount = 1;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, bound, sizeof(bound)));
		UT_ASSERT_EQ(opens + resolves, 0);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, bound, sizeof(bound)));
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 2);
		CritSectionCount = 0;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	}
}
UT_TEST(test_d0_bound_same_page_different_graph_or_identity_refuses_before_io)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		ClusterRecoveryCTRCReleaseV2 other;
		PGAlignedBlock before, after;
		uint8 changed[320];
		unsigned before_reads;
		reset_d0(true);
		if (!prepare_d0(&stamp))
			continue;
		UT_ASSERT(cluster_recovery_ctrc_decode_v2(d0_payload, 320, d0_system, &other));
		switch (mode) {
		case 0:
			other.graph_sha256[0]++;
			break;
		case 1:
			other.input_sha256[0]++;
			break;
		case 2:
			other.recovery_serial++;
			break;
		case 3:
			other.writer_boot++;
			break;
		}
		UT_ASSERT(cluster_recovery_ctrc_encode_v2(&other, d0_system, changed, sizeof(changed)));
		/* Both records really derive exactly the same native before/after
		 * target; target-page equality alone cannot bind their authority. */
		UT_ASSERT(cluster_recovery_ctrc_target_image_v2(changed, sizeof(changed), d0_system,
														initial.data, BLCKSZ, before.data, BLCKSZ));
		UT_ASSERT(cluster_recovery_ctrc_target_image_v2(d0_payload, 320, d0_system, initial.data,
														BLCKSZ, after.data, BLCKSZ));
		UT_ASSERT(memcmp(before.data, after.data, BLCKSZ) == 0);
		before_reads = reads;
		CritSectionCount = 1;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, changed, sizeof(changed)));
		UT_ASSERT_EQ(reads, before_reads);
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, d0_payload, 320));
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		CritSectionCount = 0;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	}
}
UT_TEST(test_d0_bound_token_owner_kind_and_length_isolation)
{
	ClusterUndoSmgrRecoveryStampV1 stamp, wrong;
	reset_d0(false);
	if (!prepare_d0(&stamp))
		return;
	wrong.token = stamp.token + 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(wrong, NULL, 0));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, NULL, 0));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, d0_payload, 320));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	reset_file(false);
	if (!prepare(&stamp))
		return;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, NULL, 0));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
	for (unsigned mode = 0; mode < 2; mode++) {
		unsigned before_reads;
		reset_d0(false);
		if (!prepare_d0(&stamp))
			continue;
		before_reads = reads;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(
			stamp, mode == 0 ? NULL : d0_payload, mode == 0 ? 320 : 319));
		UT_ASSERT_EQ(reads, before_reads);
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_v2(stamp));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	}
}

UT_TEST(test_d0_matches_is_readonly_and_compares_all_payload_bytes)
{
	for (int committed = 0; committed < 2; committed++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		RecoveryHeaderStamp saved;
		uint8 bytes[322];
		int old_opens, old_resolves, old_reads, old_writes, old_syncs, old_closes;

		reset_d0(committed);
		if (!prepare_d0(&stamp))
			continue;
		memcpy(bytes + 1, d0_payload, 320);
		saved = recovery_header_stamp;
		old_opens = opens; old_resolves = resolves; old_reads = reads;
		old_writes = writes; old_syncs = syncs; old_closes = closes;
		CritSectionCount = 1;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, bytes + 1, 320));
		for (size_t i = 0; i < 320; i++) {
			bytes[i + 1] ^= 1;
			UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, bytes + 1, 320));
			bytes[i + 1] ^= 1;
		}
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, bytes + 1, 320));
		UT_ASSERT(memcmp(&saved, &recovery_header_stamp, sizeof(saved)) == 0);
		UT_ASSERT_EQ(opens, old_opens); UT_ASSERT_EQ(resolves, old_resolves);
		UT_ASSERT_EQ(reads, old_reads); UT_ASSERT_EQ(writes, old_writes);
		UT_ASSERT_EQ(syncs, old_syncs); UT_ASSERT_EQ(closes, old_closes);
		UT_ASSERT_EQ(memcmp(bytes + 1, d0_payload, 320), 0);
		CritSectionCount = 0;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	}
}

UT_TEST(test_d0_matches_token_owner_kind_and_sticky_failure_isolation)
{
	ClusterUndoSmgrRecoveryStampV1 stamp, wrong;
	RecoveryHeaderStamp saved;
	uint8 bytes[320];
	int io;

	reset_d0(false);
	if (!prepare_d0(&stamp))
		return;
	saved = recovery_header_stamp;
	io = opens + resolves + reads + writes + syncs + closes;
	wrong.token = 0;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(wrong, d0_payload, 320));
	wrong.token = stamp.token + 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(wrong, d0_payload, 320));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
	CurrentResourceOwner = NULL;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, NULL, 320));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 319));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 321));
	UT_ASSERT(memcmp(&saved, &recovery_header_stamp, sizeof(saved)) == 0);
	UT_ASSERT_EQ(io, opens + resolves + reads + writes + syncs + closes);
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
	memcpy(bytes, d0_payload, 320); bytes[224] ^= 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, bytes, 320));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));

	reset_file(false);
	if (!prepare(&stamp))
		return;
	saved = recovery_header_stamp;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
	UT_ASSERT(memcmp(&saved, &recovery_header_stamp, sizeof(saved)) == 0);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
}

#define D0_NATIVE_RAW_BYTES (SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong + 320)

/* Completed decoded state is controlled; the original read/reference codec
 * consumes it unchanged. This does not run XLogReadRecord or certify source. */
static void
seal_native_d0(uint8 raw[D0_NATIVE_RAW_BYTES], DecodedXLogRecord *decoded)
{
	XLogRecord header;
	pg_crc32c crc;
	memcpy(&header, raw, SizeOfXLogRecord);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, raw + SizeOfXLogRecord, D0_NATIVE_RAW_BYTES - SizeOfXLogRecord);
	COMP_CRC32C(crc, &header, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	header.xl_crc = crc;
	memcpy(raw, &header, SizeOfXLogRecord);
	decoded->header = header;
}

static void
native_d0_fixture(XLogReaderState *reader, DecodedXLogRecord *decoded,
	uint8 raw[D0_NATIVE_RAW_BYTES])
{
	ClusterRecoveryCTRCReleaseV2 d0;
	uint32 length = 320;
	UT_ASSERT(cluster_recovery_ctrc_decode_v2(d0_payload, sizeof(d0_payload), d0_system, &d0));
	memset(reader, 0, sizeof(*reader));
	memset(decoded, 0, sizeof(*decoded));
	memset(raw, 0, D0_NATIVE_RAW_BYTES);
	reader->system_identifier = d0_system;
	reader->cluster_expected_thread_id = d0.writer_thread;
	reader->ReadRecPtr = UINT64_C(0x1000000);
	reader->EndRecPtr = reader->ReadRecPtr + 4096;
	reader->record = decoded;
	decoded->size = sizeof(*decoded);
	decoded->lsn = reader->ReadRecPtr;
	decoded->next_lsn = reader->EndRecPtr;
	decoded->max_block_id = -1;
	decoded->main_data = (char *)d0_payload;
	decoded->main_data_len = sizeof(d0_payload);
	decoded->header.xl_tot_len = D0_NATIVE_RAW_BYTES;
	decoded->header.xl_rmid = RM_CLUSTER_UNDO_ID;
	decoded->header.xl_info = CLUSTER_RECOVERY_CTRC_WAL_INFO;
	memcpy(raw, &decoded->header, SizeOfXLogRecord);
	raw[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_LONG;
	memcpy(raw + SizeOfXLogRecord + 1, &length, sizeof(length));
	memcpy(raw + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong, d0_payload, 320);
	seal_native_d0(raw, decoded);
}

UT_TEST(test_d0_record_prepare_binds_native_reference_before_held_target)
{
	for (unsigned terminal = 0; terminal < 2; terminal++) {
		XLogReaderState reader, reader_before;
		DecodedXLogRecord decoded, decoded_before;
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 91 };
		ClusterRecoveryCTRCReferenceV2 reference, expected;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES];
		reset_d0(terminal == 0);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		reader_before = reader;
		decoded_before = decoded;
		UT_ASSERT(cluster_recovery_ctrc_reference_v2(&reader, 17, source, raw, sizeof(raw), &expected));
		memset(&reference, 0xA5, sizeof(reference));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), &stamp, &reference));
		UT_ASSERT(stamp.token != 0 && stamp.token != 91);
		UT_ASSERT(memcmp(&reference, &expected, sizeof(reference)) == 0);
		UT_ASSERT(reference.end != reference.start + reference.raw_length);
		UT_ASSERT_EQ(external_fds, 1);
		UT_ASSERT_EQ(opens, 1);
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT(memcmp(&reader, &reader_before, sizeof(reader)) == 0);
		UT_ASSERT(memcmp(&decoded, &decoded_before, sizeof(decoded)) == 0);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, d0_payload, 320));
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}

UT_TEST(test_d0_record_prepare_native_refusal_precedes_any_target_io)
{
	for (unsigned mode = 0; mode < 13; mode++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 91 };
		ClusterRecoveryCTRCReferenceV2 reference, before;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES];
		const XLogReaderState *input = &reader;
		const uint8 *identity = source, *snapshot = raw;
		size_t length = sizeof(raw);
		reset_d0(true);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		memset(&reference, 0xA5, sizeof(reference));
		before = reference;
		switch (mode) {
		case 0: input = NULL; break;
		case 1: identity = NULL; break;
		case 2: snapshot = NULL; break;
		case 3: length--; break;
		case 4: reader.record = NULL; break;
		case 5: reader.EndRecPtr++; break;
		case 6: reader.cluster_expected_thread_id++; break;
		case 7: memset(source, 0, sizeof(source)); break;
		case 8: raw[sizeof(raw) - 1] ^= 1; break;
		case 9:
			raw[sizeof(raw) - 1] ^= 1;
			seal_native_d0(raw, &decoded); /* valid CRC/header, different actual body */
			break;
		case 10: decoded.toplevel_xid = 100; break;
		case 11: CritSectionCount = 1; break;
		case 12: CurrentResourceOwner = NULL; break;
		}
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_record_v2(input, 17,
			identity, snapshot, length, &stamp, &reference));
		UT_ASSERT_EQ(opens + resolves + reads + writes + syncs, 0);
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT_EQ(stamp.token, 91);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
	}
	CritSectionCount = 0;
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
}

UT_TEST(test_d0_record_prepare_output_aliases_preserve_reader_and_stage)
{
	for (unsigned mode = 0; mode < 10; mode++) {
		XLogReaderState reader, reader_before;
		DecodedXLogRecord decoded, decoded_before;
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 91 }, *token_out = &stamp;
		ClusterRecoveryCTRCReferenceV2 reference, before, *ref_out = &reference;
		RecoveryHeaderStamp stage_before;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES], raw_before[D0_NATIVE_RAW_BYTES];
		reset_d0(true);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		memset(&reference, 0xA5, sizeof(reference));
		before = reference;
		reader_before = reader;
		decoded_before = decoded;
		stage_before = recovery_header_stamp;
		memcpy(raw_before, raw, sizeof(raw));
		switch (mode) {
		case 0: token_out = NULL; break;
		case 1: ref_out = NULL; break;
		case 2: ref_out = (void *)&stamp; break;
		case 3: token_out = (void *)&reader; break;
		case 4: ref_out = (void *)&decoded; break;
		case 5: token_out = (void *)d0_payload; break;
		case 6: ref_out = (void *)raw; break;
		case 7: token_out = (void *)source; break;
		case 8: ref_out = (void *)&recovery_header_stamp; break;
		case 9: token_out = (void *)&decoded; break;
		}
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), token_out, ref_out));
		UT_ASSERT_EQ(opens + resolves + reads + writes + syncs, 0);
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT_EQ(stamp.token, 91);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
		UT_ASSERT(memcmp(&reader, &reader_before, sizeof(reader)) == 0);
		UT_ASSERT(memcmp(&decoded, &decoded_before, sizeof(decoded)) == 0);
		UT_ASSERT(memcmp(raw, raw_before, sizeof(raw)) == 0);
		UT_ASSERT(memcmp(&recovery_header_stamp, &stage_before, sizeof(stage_before)) == 0);
	}
}

UT_TEST(test_d0_record_prepare_target_refusal_never_publishes_reference_or_token)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 91 }, existing;
		ClusterRecoveryCTRCReferenceV2 reference, before;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES];
		reset_d0(true);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		memset(&reference, 0xA5, sizeof(reference));
		before = reference;
		if (mode == 0) capacity = false;
		if (mode == 1) open_error = true;
		if (mode == 2) read_short = true;
		if (mode == 3) change_byte(BLCKSZ - 1, 0x6b);
		if (mode == 4) UT_ASSERT(prepare_d0(&existing));
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), &stamp, &reference));
		UT_ASSERT_EQ(stamp.token, 91);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT_EQ(external_fds, mode == 4);
		if (mode == 4) {
			UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(existing, d0_payload, 320));
			UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(existing));
		}
	}
}


UT_TEST(test_d0_record_finish_requires_write_readback_and_close)
{
	for (unsigned terminal = 0; terminal < 2; terminal++) {
		XLogReaderState reader, reader_before;
		DecodedXLogRecord decoded;
		ClusterUndoSmgrRecoveryStampV1 stamp;
		ClusterRecoveryCTRCReferenceV2 reference, before;
		PGAlignedBlock expected, observed;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES], unaligned[sizeof(reference) + 1];
		uint8 supplied_before[sizeof(reference)];
		int fd, before_reads, before_io;
		reset_d0(terminal == 0);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), &stamp, &reference));
		expected = recovery_header_stamp.after;
		reader_before = reader; before = reference;
		/* R is a logical C structure: unrelated padding may differ. */
		memset(unaligned, 0xA5, sizeof(unaligned));
		memcpy(unaligned + 1 + offsetof(ClusterRecoveryCTRCReferenceV2, source_index),
			&reference.source_index, sizeof(reference.source_index));
		memcpy(unaligned + 1 + offsetof(ClusterRecoveryCTRCReferenceV2, start),
			&reference.start, sizeof(reference.start));
		memcpy(unaligned + 1 + offsetof(ClusterRecoveryCTRCReferenceV2, end),
			&reference.end, sizeof(reference.end));
		memcpy(unaligned + 1 + offsetof(ClusterRecoveryCTRCReferenceV2, raw_length),
			&reference.raw_length, sizeof(reference.raw_length));
		memcpy(unaligned + 1 + offsetof(ClusterRecoveryCTRCReferenceV2, raw_record_sha256),
			&reference.raw_record_sha256, sizeof(reference.raw_record_sha256));
		memcpy(supplied_before, unaligned + 1, sizeof(supplied_before));
		before_reads = reads;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, &reader,
			17, source, raw, sizeof(raw), (const void *)(unaligned + 1)));
		UT_ASSERT_EQ(opens, 1);
		UT_ASSERT_EQ(reads, before_reads + 2);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(closes, 1);
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT(!header_locked);
		UT_ASSERT(memcmp(&reader, &reader_before, sizeof(reader)) == 0);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
		UT_ASSERT(memcmp(unaligned + 1, supplied_before, sizeof(supplied_before)) == 0);
		fd = open(test_file, O_RDONLY);
		UT_ASSERT(fd >= 0);
		if (fd >= 0) {
			UT_ASSERT_EQ(pread(fd, observed.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT(memcmp(observed.data, expected.data, BLCKSZ) == 0);
			close(fd);
		}
		before_io = reads + writes + syncs + closes;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, &reader,
			17, source, raw, sizeof(raw), &reference));
		UT_ASSERT_EQ(reads + writes + syncs + closes, before_io);
	}
}

UT_TEST(test_d0_record_finish_rebinding_refusal_preserves_owned_stage)
{
	for (unsigned mode = 0; mode < 14; mode++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterUndoSmgrRecoveryStampV1 stamp, input_stamp;
		ClusterRecoveryCTRCReferenceV2 reference, supplied;
		const ClusterRecoveryCTRCReferenceV2 *expected;
		RecoveryHeaderStamp stage_before;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES];
		size_t length = sizeof(raw);
		uint32 index = 17;
		int before_io;
		reset_d0(true);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), &stamp, &reference));
		input_stamp = stamp; supplied = reference; expected = &supplied;
		stage_before = recovery_header_stamp;
		switch (mode) {
		case 0: expected = NULL; break;
		case 1: ((uint8 *)&supplied)[0] ^= 1; break;
		case 2: source[0] ^= 1; break;
		case 3:
			reader.ReadRecPtr += 8192; reader.EndRecPtr += 8192;
			decoded.lsn = reader.ReadRecPtr; decoded.next_lsn = reader.EndRecPtr;
			break;
		case 4: raw[sizeof(raw) - 1] ^= 1; break;
		case 5:
			raw[sizeof(raw) - 1] ^= 1;
			seal_native_d0(raw, &decoded);
			break;
		case 6:
			decoded.header.xl_prev = 1;
			memcpy(raw, &decoded.header, SizeOfXLogRecord);
			seal_native_d0(raw, &decoded);
			break;
		case 7: supplied.raw_record_sha256[31] ^= 1; break;
		case 8: length--; break;
		case 9: CritSectionCount = 1; break;
		case 10: CurrentResourceOwner = (ResourceOwner)&owner_storage[1]; break;
		case 11: input_stamp.token++; break;
		case 12: input_stamp.token = 0; break;
		case 13: index++; break;
		}
		before_io = reads + writes + syncs + closes;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(input_stamp,
			&reader, index, source, raw, length, expected));
		UT_ASSERT_EQ(reads + writes + syncs + closes, before_io);
		UT_ASSERT_EQ(external_fds, 1);
		UT_ASSERT(memcmp(&recovery_header_stamp, &stage_before, sizeof(stage_before)) == 0);
		CritSectionCount = 0;
		CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	}
}

UT_TEST(test_d0_record_finish_target_failures_stay_sticky_without_closing)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterUndoSmgrRecoveryStampV1 stamp;
		ClusterRecoveryCTRCReferenceV2 reference, before;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES];
		int before_io;
		reset_d0(false);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), &stamp, &reference));
		before = reference;
		if (mode == 0) read_short = true;
		if (mode == 1) write_short = true;
		if (mode == 2) corrupt_postread = true;
		if (mode == 3) stat_error = true;
		if (mode == 4) change_byte(BLCKSZ - 1, 0x6b);
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, &reader,
			17, source, raw, sizeof(raw), &reference));
		UT_ASSERT(recovery_header_stamp.failed);
		UT_ASSERT_EQ(external_fds, 1);
		UT_ASSERT_EQ(closes, 0);
		UT_ASSERT(!header_locked);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
		read_short = write_short = corrupt_postread = stat_error = false;
		before_io = reads + writes + syncs + closes;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, &reader,
			17, source, raw, sizeof(raw), &reference));
		UT_ASSERT_EQ(reads + writes + syncs + closes, before_io);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}

UT_TEST(test_d0_record_finish_preserves_native_sync_error_and_close_refusal)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterUndoSmgrRecoveryStampV1 stamp;
		ClusterRecoveryCTRCReferenceV2 reference, before;
		uint8 source[32], raw[D0_NATIVE_RAW_BYTES];
		reset_d0(true);
		native_d0_fixture(&reader, &decoded, raw);
		memset(source, 6, sizeof(source));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_prepare_record_v2(&reader, 17,
			source, raw, sizeof(raw), &stamp, &reference));
		before = reference;
		if (mode < 2) {
			sync_error = true; sync_retry = mode == 1; catch_sync_failure = true;
			if (setjmp(sync_failure_jump) == 0) {
				(void)cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, &reader,
					17, source, raw, sizeof(raw), &reference);
				UT_ASSERT(false);
			}
			catch_sync_failure = false;
			UT_ASSERT_EQ(sync_level, ERROR);
			UT_ASSERT_EQ(report_level, sync_retry ? ERROR : PANIC);
			UT_ASSERT(recovery_header_stamp.failed);
			UT_ASSERT_EQ(external_fds, 1);
			UT_ASSERT_EQ(closes, 0);
			sync_error = false;
			UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		} else {
			close_error = true;
			UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, &reader,
				17, source, raw, sizeof(raw), &reference));
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(closes, 1);
			close_error = false;
			UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		}
		UT_ASSERT(!header_locked);
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
	}
}


/* Real original WAL reader, with the existing target fixture's canonical D0. */
static bool
open_native_target_fixture(NativeFixture *f)
{
	ClusterRecoveryCTRCReleaseV2 value;
	const size_t first_part = 128;

	memset(f, 0, sizeof(*f)); f->fd = -1;
	if (!cluster_recovery_ctrc_decode_v2(d0_payload, sizeof(d0_payload), d0_system, &value))
		return false;
	value.writer_thread = 3;
	if (!cluster_recovery_ctrc_encode_v2(&value, d0_system, d0_payload, sizeof(d0_payload))
		|| !open_fixture(f, CLUSTER_RECOVERY_CTRC_WAL_INFO, 0, true))
		return false;
	for (unsigned i = 0; i < 2; i++) {
		memcpy(f->raw[i] + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong,
			d0_payload, sizeof(d0_payload));
		seal_raw(f->raw[i]);
	}
	if (pwrite(f->fd, f->raw[0], first_part, XLOG_BLCKSZ - first_part) != first_part
		|| pwrite(f->fd, f->raw[0] + first_part, RAW_BYTES - first_part,
			XLOG_BLCKSZ + SizeOfXLogShortPHD) != RAW_BYTES - first_part
		|| pwrite(f->fd, f->raw[1], RAW_BYTES, f->second - SEG_START) != RAW_BYTES) {
		close_fixture(f);
		return false;
	}
	return true;
}

UT_TEST(test_d0_native_custody_prepares_and_finishes_original_target)
{
	for (unsigned terminal = 0; terminal < 2; terminal++) {
		NativeFixture f;
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 91 };
		ClusterRecoveryCTRCReferenceV2 reference, expected_reference;
		PGAlignedBlock expected, observed;
		uint8 source[32]; char *error = NULL; int fd;
		bool prepared;

		reset_d0(terminal == 0);
		UT_ASSERT(open_native_target_fixture(&f));
		if (f.reader == NULL) continue;
		UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
		memset(source, 6, sizeof(source));
		UT_ASSERT(cluster_recovery_ctrc_reference_v2(f.reader, 17, source,
			f.raw[0], RAW_BYTES, &expected_reference));
		prepared = cluster_undo_smgr_recovery_ctrc_prepare_record_v2(f.reader, 17,
			source, NULL, 0, &stamp, &reference);
		UT_ASSERT(prepared);
		if (!prepared) { close_fixture(&f); continue; }
		UT_ASSERT(reference.start == expected_reference.start);
		UT_ASSERT(reference.end == expected_reference.end);
		UT_ASSERT(reference.raw_length == expected_reference.raw_length);
		UT_ASSERT(memcmp(reference.raw_record_sha256,
			expected_reference.raw_record_sha256, 32) == 0);
		UT_ASSERT(reference.end > reference.start + reference.raw_length);
		expected = recovery_header_stamp.after;
		UT_ASSERT(XLogReadAhead(f.reader, false) != NULL);
		memset(f.reader->readRecordBuf, 0xEE, RAW_BYTES);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, f.reader,
			17, source, NULL, 0, &reference));
		UT_ASSERT_EQ(opens, 1); UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(closes, 1);
		UT_ASSERT_EQ(external_fds, 0);
		fd = open(test_file, O_RDONLY); UT_ASSERT(fd >= 0);
		if (fd >= 0) {
			UT_ASSERT_EQ(pread(fd, observed.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT(memcmp(observed.data, expected.data, BLCKSZ) == 0);
			close(fd);
		}
		close_fixture(&f);
	}
}

UT_TEST(test_d0_native_custody_refuses_before_io_or_output_publication)
{
	for (unsigned mode = 0; mode < 11; mode++) {
		NativeFixture f;
		ClusterUndoSmgrRecoveryStampV1 stamp = { .token = 91 };
		ClusterRecoveryCTRCReferenceV2 reference, before;
		ClusterRecoveryCTRCReferenceV2 *out = &reference;
		XLogReaderState *reader;
		const uint8 *raw = NULL; size_t length = 0;
		uint8 source[32]; char *error = NULL;

		reset_d0(true);
		UT_ASSERT(open_native_target_fixture(&f));
		if (f.reader == NULL) continue;
		UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
		reader = f.reader;
		memset(source, 6, sizeof(source)); memset(&reference, 0xA5, sizeof(reference));
		before = reference;
		switch (mode) {
		case 0: reader = NULL; break;
		case 1: length = RAW_BYTES; break;
		case 2: raw = f.raw[0]; break;
		case 3: memset(source, 0, sizeof(source)); break;
		case 4: reader->EndRecPtr++; break;
		case 5: XLogBeginRead(reader, f.first); break;
		case 6: CritSectionCount = 1; break;
		case 7: CurrentResourceOwner = NULL; break;
		case 8: raw = f.raw[1]; length = RAW_BYTES; break;
		case 9: out = (void *)(XLogReaderGetClusterD0RawRecord(reader) + 1); break;
		case 10: reader->cluster_expected_thread_id = 4; break;
		}
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_prepare_record_v2(reader, 17,
			source, raw, length, &stamp, out));
		UT_ASSERT_EQ(stamp.token, 91);
		UT_ASSERT(memcmp(&reference, &before, sizeof(reference)) == 0);
		UT_ASSERT_EQ(opens + reads + writes + syncs + closes, 0);
		UT_ASSERT_EQ(external_fds, 0);
		CritSectionCount = 0;
		CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
		close_fixture(&f);
	}
}

UT_TEST(test_d0_native_custody_finish_rebinds_current_record_before_io)
{
	NativeFixture f;
	ClusterUndoSmgrRecoveryStampV1 stamp;
	ClusterRecoveryCTRCReferenceV2 reference;
	uint8 source[32]; char *error = NULL; int before_io;
	bool prepared;

	reset_d0(false);
	UT_ASSERT(open_native_target_fixture(&f));
	if (f.reader == NULL) return;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	memset(source, 6, sizeof(source));
	prepared = cluster_undo_smgr_recovery_ctrc_prepare_record_v2(f.reader, 17,
		source, NULL, 0, &stamp, &reference);
	UT_ASSERT(prepared);
	if (!prepared) { close_fixture(&f); return; }
	before_io = reads + writes + syncs + closes;
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, f.reader,
		17, source, NULL, 0, &reference));
	UT_ASSERT_EQ(reads + writes + syncs + closes, before_io);
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_matches_v2(stamp, d0_payload, 320));
	UT_ASSERT_EQ(external_fds, 1);
	XLogBeginRead(f.reader, f.first);
	UT_ASSERT(XLogReadRecord(f.reader, &error) != NULL);
	source[31] ^= 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, f.reader,
		17, source, NULL, 0, &reference));
	UT_ASSERT_EQ(reads + writes + syncs + closes, before_io);
	source[31] ^= 1;
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_finish_record_v2(stamp, f.reader,
		17, source, NULL, 0, &reference));
	UT_ASSERT_EQ(external_fds, 0);
	close_fixture(&f);
}

UT_TEST(test_tx_held_descriptor_refuses_lost_ir_before_target_write)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		PGAlignedBlock actual;
		int fd;

		reset_file(false);
		UT_ASSERT(cluster_undo_smgr_recovery_abort_prepare_tx_v1(
			(ClusterWalInputsV1 *)&tx_scope_storage, &record, &stamp));
		if (fault == 0) tx_scope_current = false;
		if (fault == 1) tx_loss_on_lock = true;
		if (fault == 2) tx_loss_on_stat = true;
		if (fault == 3) tx_loss_on_read = true;
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_write_v1(stamp));
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT(!cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
		UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT(!header_locked);
		fd = open(test_file, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, actual.data, BLCKSZ, 0), BLCKSZ);
		UT_ASSERT(memcmp(actual.data, initial.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
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
	UT_PLAN(36);
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
	UT_RUN(test_durable_fact_requires_exact_completed_live_stamp);
	UT_RUN(test_c0_tx_record_and_d0_share_stage_without_cross_kind_authority);
	UT_RUN(test_d0_sync_failure_keeps_native_error_unlock_and_sticky_debt);
	UT_RUN(test_d0_terminal_pages_sync_exactly_on_held_fd);
	UT_RUN(test_d0_prepare_requires_canonical_exact_before_outside_window);
	UT_RUN(test_d0_and_c0_share_owner_stage_but_not_token_kind);
	UT_RUN(test_d0_changed_target_refuses_before_writing_and_stays_failed);
	UT_RUN(test_d0_failed_io_is_sticky_and_sync_uses_native_severity);
	UT_RUN(test_d0_owner_and_exit_cleanup_only_release_descriptor);
	UT_RUN(test_d0_bound_write_exact_canonical_payload_on_held_fd);
	UT_RUN(test_d0_bound_same_page_different_graph_or_identity_refuses_before_io);
	UT_RUN(test_d0_bound_token_owner_kind_and_length_isolation);
	UT_RUN(test_d0_matches_is_readonly_and_compares_all_payload_bytes);
	UT_RUN(test_d0_matches_token_owner_kind_and_sticky_failure_isolation);
	UT_RUN(test_d0_record_prepare_binds_native_reference_before_held_target);
	UT_RUN(test_d0_record_prepare_native_refusal_precedes_any_target_io);
	UT_RUN(test_d0_record_prepare_output_aliases_preserve_reader_and_stage);
	UT_RUN(test_d0_record_prepare_target_refusal_never_publishes_reference_or_token);
	UT_RUN(test_d0_record_finish_requires_write_readback_and_close);
	UT_RUN(test_d0_record_finish_rebinding_refusal_preserves_owned_stage);
	UT_RUN(test_d0_record_finish_target_failures_stay_sticky_without_closing);
	UT_RUN(test_d0_record_finish_preserves_native_sync_error_and_close_refusal);
	UT_RUN(test_d0_native_custody_prepares_and_finishes_original_target);
	UT_RUN(test_d0_native_custody_refuses_before_io_or_output_publication);
	UT_RUN(test_d0_native_custody_finish_rebinds_current_record_before_io);
	UT_RUN(test_tx_held_descriptor_refuses_lost_ir_before_target_write);
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	if (release_callback != NULL)
		release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	(void)unlink(test_file);
	(void)rmdir(test_dir);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
