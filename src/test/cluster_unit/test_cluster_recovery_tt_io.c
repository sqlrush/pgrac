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
#include "miscadmin.h"
#include <fcntl.h>
#include <setjmp.h>
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
static jmp_buf report_jump;
static bool report_armed, sync_retry;
static int reported_level, sync_base_level;
volatile uint32 CritSectionCount;
static bool held_read_error, held_lock_refused, held_borrowed_lock, held_error_clearing;
static uint8 *held_payload_change;
static bool held_owner_change;
volatile uint32 InterruptHoldoffCount;
static int held_stat_calls, held_stat_fail_at;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void pg_re_throw(void)
{
	if (PG_exception_stack == NULL) abort();
	siglongjmp(*PG_exception_stack, 1);
}


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
cluster_undo_block0_header_write_lock(uint32 segment, uint8 owner, uint32 *acquired)
{
	UT_ASSERT_EQ(segment, 257);
	UT_ASSERT_EQ(owner, 2);
	if (header_locked) {
		UT_ASSERT(held_borrowed_lock);
		*acquired = UINT32_MAX;
		return true;
	}
	if (held_lock_refused) { *acquired = UINT32_MAX; return false; }
	header_locked = true;
	InterruptHoldoffCount++;
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
	UT_ASSERT(InterruptHoldoffCount > 0);
	if (held_error_clearing) UT_ASSERT_EQ(InterruptHoldoffCount, 1);
	header_locked = false;
	InterruptHoldoffCount--;
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
	if (held_read_error) {
		/* errfinish(ERROR) resets this before its first PG_RE_THROW. */
		InterruptHoldoffCount = 0;
		held_error_clearing = true;
		pg_re_throw();
	}
	if (held_payload_change != NULL) {
		held_payload_change[CLUSTER_RECOVERY_CTRC_BYTES - 1] ^= 1;
		held_payload_change = NULL;
	}
	if (held_owner_change) CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
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
	held_stat_calls++;
	if (held_stat_fail_at != 0 && held_stat_calls == held_stat_fail_at) {
		errno = EIO;
		return -1;
	}
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
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	opens = resolves = reads = writes = syncs = closes = 0;
	tx_resolves = 0;
	tx_scope_current = true;
	tx_loss_on_lock = tx_loss_on_stat = tx_loss_on_read = false;
	scope_current = capacity = true;
	open_error = read_short = write_short = sync_error = close_error = stat_error = false;
	corrupt_postread = held_read_error = held_lock_refused = false;
	held_borrowed_lock = held_error_clearing = held_owner_change = false;
	held_payload_change = NULL;
	held_stat_calls = held_stat_fail_at = 0;
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
prepare_d0(ClusterUndoSmgrRecoveryStampV1 *stamp, uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES])
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
	if (!prepare_d0(&stamp, payload))
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
		if (!prepare_d0(&stamp, payload))
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

UT_TEST(test_d0_final_readback_requires_actual_durable_stage_without_rewrite)
{
	ClusterUndoSmgrRecoveryStampV1 stamp;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
	int saved_reads, saved_stats, saved_opens, saved_syncs, saved_writes;

	if (!prepare_d0(&stamp, payload)) return;
	saved_reads = reads;
	saved_stats = held_stat_calls;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT_EQ(reads, saved_reads);
	UT_ASSERT_EQ(held_stat_calls, saved_stats);
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
	saved_reads = reads;
	saved_stats = held_stat_calls;
	saved_opens = opens;
	saved_syncs = syncs;
	saved_writes = writes;
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT_EQ(reads, saved_reads + 2);
	UT_ASSERT_EQ(held_stat_calls, saved_stats + 4);
	UT_ASSERT_EQ(opens, saved_opens);
	UT_ASSERT_EQ(syncs, saved_syncs);
	UT_ASSERT_EQ(writes, saved_writes);
	UT_ASSERT_EQ(external_fds, 1);
	UT_ASSERT(!header_locked);
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT_EQ(external_fds, 0);
}

UT_TEST(test_d0_final_readback_foreign_inputs_do_not_poison_original_stage)
{
	ClusterUndoSmgrRecoveryStampV1 stamp, foreign;
	uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES], changed[CLUSTER_RECOVERY_CTRC_BYTES];
	int saved_reads, saved_stats;

	if (!prepare_d0(&stamp, payload)) return;
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
	saved_reads = reads;
	saved_stats = held_stat_calls;
	foreign = stamp;
	foreign.token++;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(foreign, payload, sizeof(payload)));
	memcpy(changed, payload, sizeof(changed));
	changed[319] ^= 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, changed, sizeof(changed)));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, NULL, sizeof(payload)));
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload) - 1));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[1];
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	CritSectionCount = 1;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	CritSectionCount = 0;
	UT_ASSERT_EQ(reads, saved_reads);
	UT_ASSERT_EQ(held_stat_calls, saved_stats);
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
	reset_file(false);
	if (!prepare(&stamp)) return;
	UT_ASSERT(cluster_undo_smgr_recovery_abort_write_v1(stamp));
	saved_reads = reads;
	saved_stats = held_stat_calls;
	UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
	UT_ASSERT_EQ(reads, saved_reads);
	UT_ASSERT_EQ(held_stat_calls, saved_stats);
	UT_ASSERT(cluster_undo_smgr_recovery_abort_durable_v1(stamp, &record));
	UT_ASSERT(cluster_undo_smgr_recovery_abort_close_v1(stamp));
}

UT_TEST(test_d0_final_readback_full_page_and_file_refusals_are_sticky)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
		int saved_writes, saved_syncs, saved_reads;

		if (!prepare_d0(&stamp, payload)) continue;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
		saved_writes = writes;
		saved_syncs = syncs;
		if (fault == 0) change_byte(BLCKSZ - 1, 0x6b);
		if (fault == 1) read_short = true;
		if (fault == 2) held_stat_fail_at = held_stat_calls + 1;
		if (fault == 3) held_stat_fail_at = held_stat_calls + 2;
		if (fault == 4) UT_ASSERT_EQ(unlink(test_file), 0);
		if (fault == 5) held_lock_refused = true;
		if (fault == 6) held_payload_change = payload;
		if (fault == 7) held_owner_change = true;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
		UT_ASSERT(!header_locked);
		UT_ASSERT_EQ(writes, saved_writes);
		UT_ASSERT_EQ(syncs, saved_syncs);
		UT_ASSERT_EQ(external_fds, 1);
		if (fault == 0) change_byte(BLCKSZ - 1, 0x6a);
		read_short = held_lock_refused = held_owner_change = false;
		CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
		held_stat_fail_at = 0;
		saved_reads = reads;
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
		UT_ASSERT_EQ(reads, saved_reads);
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
	}
}

UT_TEST(test_d0_final_readback_error_unlocks_only_local_lock_and_retains_fd)
{
	for (unsigned borrowed = 0; borrowed < 2; borrowed++) {
		ClusterUndoSmgrRecoveryStampV1 stamp;
		uint8 payload[CLUSTER_RECOVERY_CTRC_BYTES];
		volatile bool caught = false;
		int saved_writes, saved_syncs;

		if (!prepare_d0(&stamp, payload)) continue;
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
		saved_writes = writes;
		saved_syncs = syncs;
		held_borrowed_lock = borrowed != 0;
		if (held_borrowed_lock) { header_locked = true; InterruptHoldoffCount++; }
		held_read_error = true;
		PG_TRY();
		{
			(void)cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		held_read_error = held_error_clearing = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(header_locked, borrowed != 0);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		if (held_borrowed_lock) {
			/* The caller owns this separate error-cleanup responsibility. */
			HOLD_INTERRUPTS();
			cluster_undo_block0_header_write_unlock(1);
			held_borrowed_lock = false;
		}
		UT_ASSERT_EQ(external_fds, 1);
		UT_ASSERT_EQ(writes, saved_writes);
		UT_ASSERT_EQ(syncs, saved_syncs);
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_readback_v2(stamp, payload, sizeof(payload)));
		UT_ASSERT(!cluster_undo_smgr_recovery_ctrc_write_bound_v2(stamp, payload, sizeof(payload)));
		UT_ASSERT(cluster_undo_smgr_recovery_ctrc_close_v2(stamp));
		UT_ASSERT_EQ(external_fds, 0);
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
	UT_PLAN(18);
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
	UT_RUN(test_tx_held_descriptor_refuses_lost_ir_before_target_write);
	UT_RUN(test_d0_final_readback_requires_actual_durable_stage_without_rewrite);
	UT_RUN(test_d0_final_readback_foreign_inputs_do_not_poison_original_stage);
	UT_RUN(test_d0_final_readback_full_page_and_file_refusals_are_sticky);
	UT_RUN(test_d0_final_readback_error_unlocks_only_local_lock_and_retains_fd);
	CurrentResourceOwner = (ResourceOwner)&owner_storage[0];
	if (release_callback != NULL)
		release_callback(RESOURCE_RELEASE_AFTER_LOCKS, false, true, release_arg);
	(void)unlink(test_file);
	(void)rmdir(test_dir);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
