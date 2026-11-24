/* Real frame I/O; native WAL/error scheduling is a finite test boundary. */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/clog_storage.h"
#include "access/clog_wal.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/proc.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#endif

static ssize_t fixture_pwrite(int fd, const void *bytes, size_t length, off_t offset);
static int fixture_fsync(int fd);
#undef pg_pwrite
#define pg_pwrite fixture_pwrite
#define pg_fsync fixture_fsync
#ifdef CLOG_WRITE_TEST_SOURCE
#include CLOG_WRITE_TEST_SOURCE
#else
#include "../../backend/access/transam/clog_write.c"
#endif
#undef pg_pwrite
#undef pg_fsync
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

PGPROC *MyProc;
static PGPROC proc;
ProcessingMode Mode;
bool InRecovery;
bool enableFsync;
static bool recovery;
#ifdef USE_PGRAC_CLUSTER
bool cluster_enabled;
int cluster_node_id;
#endif
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 captured, observed;
static uint8 initial[272384], payload[8192], result[8192], wal[8384];
static int file;
static unsigned writes, syncs, inserts, flushes;
static bool short_write, fail_write, fail_sync, corrupt_readback, change_header;
static bool redirty_input;
static bool occupy_target, grow_other_page;
static uint64 target_offset;
static unsigned throw_at;
static uint32 registered_len;
static const char *registered_bytes;
static uint64 system_id;
static TimeLineID timeline;
static XLogRecPtr insert_end, returned_end;
static char events[16];
static unsigned nevents;

static void materialize_page(uint64 page, uint8 fill);

void
ExceptionalCondition(const char *condition, const char *source, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, source, line);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL) abort();
	siglongjmp(*PG_exception_stack, 1);
}
bool RecoveryInProgress(void) { return recovery; }
uint64 GetSystemIdentifier(void) { return system_id; }
TimeLineID GetWALInsertionTimeLine(void) { return timeline; }
void XLogBeginInsert(void) { registered_len = 0; }
void
XLogRegisterData(char *bytes, uint32 length)
{
	if (registered_len != 0 || length != 8384) abort();
	registered_bytes = bytes;
	registered_len = length;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	if (rmgr != RM_CLOG_ID || info != 0x50 || registered_len != 8384 ||
		(MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	memcpy(wal, registered_bytes, sizeof(wal));
	inserts++;
	events[nevents++] = 'I';
	if (throw_at == 1) siglongjmp(*PG_exception_stack, 1);
	return insert_end;
}
void
XLogFlush(XLogRecPtr lsn)
{
	if (lsn != insert_end || (MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	flushes++;
	events[nevents++] = 'F';
	if (throw_at == 2) siglongjmp(*PG_exception_stack, 1);
	if (redirty_input) {
		memset(payload, 0x59, sizeof(payload));
		captured.identity.page_lsn = 1900;
		captured.image_lsn = 1700;
	}
	if (occupy_target) materialize_page(captured.identity.logical_page, 0x55);
	if (grow_other_page) materialize_page(31, 0x66);
	if (change_header) {
		ClusterClogFileBindingV2 changed = binding;
		uint8 header[8192];
		changed.identity.creation_generation++;
		if (!cluster_clog_file_header_encode_v2(&changed, header, sizeof(header)) ||
			pwrite(file, header, sizeof(header), 0) != sizeof(header)) abort();
	}
}
static ssize_t
fixture_pwrite(int fd, const void *bytes, size_t length, off_t offset)
{
	if (flushes != 1 || length != 8256 || offset != (off_t)target_offset ||
		(MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	writes++;
	events[nevents++] = 'W';
	if (fail_write) { errno = EIO; return -1; }
	return pwrite(fd, bytes, short_write ? length / 2 : length, offset);
}
static int
fixture_fsync(int fd)
{
	if (writes != 1 || (MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	syncs++;
	events[nevents++] = 'S';
	if (fail_sync) { errno = EIO; return -1; }
	if (corrupt_readback) {
		uint8 bad = 0xff;
		if (pwrite(fd, &bad, 1, target_offset + 64) != 1) abort();
	}
	return fsync(fd);
}

static void
reset(void)
{
	char path[] = "/tmp/pgrac-clog-write-XXXXXX";
	memset(&binding, 0, sizeof(binding));
	binding.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	binding.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	binding.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	binding.identity.system_identifier = system_id = 123;
	binding.identity.database_incarnation = 456;
	binding.identity.storage_uuid[0] = 1;
	binding.identity.redo_branch_uuid[0] = 2;
	binding.identity.redo_branch_generation = 2;
	binding.identity.redo_timeline = timeline = 1;
	binding.identity.creation_generation = 3;
	binding.identity.registration_generation = 4;
	memset(&captured, 0, sizeof(captured));
	captured.identity.page_generation = 9;
	captured.identity.birth_observed = true;
	captured.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + 3);
	captured.identity.page_lsn = 80;
	captured.image_lsn = 100;
	memset(initial, 0, sizeof(initial));
	memset(payload, 0x31, sizeof(payload));
	if (!cluster_clog_file_header_encode_v2(&binding, initial, 8192) ||
		!cluster_clog_file_page_encode_v2(&binding, &captured, payload, 8192,
										 initial + 8192, 8256)) abort();
	file = mkstemp(path);
	if (file < 0 || unlink(path) != 0 ||
		pwrite(file, initial, 16448, 0) != 16448) abort();
	memset(payload, 0x42, sizeof(payload));
	captured.identity.page_lsn = 90;
	memset(&proc, 0, sizeof(proc));
	MyProc = &proc;
	Mode = NormalProcessing;
	InRecovery = recovery = false;
	enableFsync = true;
#ifdef USE_PGRAC_CLUSTER
	cluster_enabled = true;
	cluster_node_id = 0;
#endif
	writes = syncs = inserts = flushes = registered_len = nevents = throw_at = 0;
	short_write = fail_write = fail_sync = corrupt_readback = change_header = false;
	redirty_input = false;
	occupy_target = grow_other_page = false;
	target_offset = 8192;
	insert_end = 1000;
	returned_end = 777;
	memset(events, 0, sizeof(events));
}
static bool
write_page(void)
{
	return cluster_clog_storage_write_page_v2(file, &binding, &captured,
		payload, sizeof(payload), &returned_end);
}
static void
unchanged_file(void)
{
	uint8 bytes[16448];
	UT_ASSERT_EQ(pread(file, bytes, sizeof(bytes), 0), sizeof(bytes));
	UT_ASSERT(memcmp(bytes, initial, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(returned_end, 777);
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
}

UT_TEST(full_native_wal_flush_precedes_frame_sync_readback)
{
	ClusterClogWalObservationV2 record = {0};
	reset();
	UT_ASSERT(write_page());
	UT_ASSERT(memcmp(events, "IFWS", 4) == 0);
	UT_ASSERT_EQ(returned_end, 1000);
	UT_ASSERT_EQ(proc.delayChkptFlags, 0);
	UT_ASSERT(cluster_clog_wal_decode_v2(0x50, wal, sizeof(wal), &record));
	UT_ASSERT_EQ(record.page.image_lsn, 100);
	UT_ASSERT_EQ(record.page.identity.page_lsn, 90);
	UT_ASSERT(record.image != NULL);
	if (record.image != NULL) UT_ASSERT_EQ(record.image[64], 0x42);
	UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, 0, &observed, result, 8192));
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(observed.identity.page_lsn, 90);
	UT_ASSERT(memcmp(result, payload, 8192) == 0);
	UT_ASSERT_EQ(captured.image_lsn, 100);
	UT_ASSERT_EQ(lseek(file, 0, SEEK_CUR), 0);
	close(file);
}
UT_TEST(preowned_checkpoint_delay_is_borrowed)
{
	reset();
	proc.delayChkptFlags = DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;
	UT_ASSERT(write_page());
	UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);
	close(file);
}
UT_TEST(nonlocal_recovery_bootstrap_and_missing_proc_refuse_before_wal)
{
	for (unsigned i = 0; i < 7; i++) {
		reset();
		if (i == 0) system_id++;
		if (i == 1) timeline++;
		if (i == 2) binding.identity.origin = 1;
		if (i == 3) InRecovery = true;
		if (i == 4) recovery = true;
		if (i == 5) Mode = BootstrapProcessing;
		if (i == 6) MyProc = NULL;
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(inserts + writes, 0);
		unchanged_file();
		close(file);
	}
}
UT_TEST(bad_arguments_and_append_fd_cannot_insert_or_write)
{
	reset();
	UT_ASSERT(!cluster_clog_storage_write_page_v2(-1, &binding, &captured, payload, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_write_page_v2(file, NULL, &captured, payload, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_write_page_v2(file, &binding, NULL, payload, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_write_page_v2(file, &binding, &captured, payload, 8191, &returned_end));
	UT_ASSERT(!cluster_clog_storage_write_page_v2(file, &binding, &captured, payload, 8192, NULL));
	UT_ASSERT(!cluster_clog_storage_write_page_v2(file, &binding, &captured, payload, 8192, &captured.image_lsn));
	UT_ASSERT_EQ(fcntl(file, F_SETFL, O_APPEND), 0);
	UT_ASSERT(!write_page());
	UT_ASSERT_EQ(inserts + writes, 0);
	unchanged_file();
	close(file);
}
UT_TEST(original_disk_birth_and_watermarks_must_match)
{
	for (unsigned i = 0; i < 5; i++) {
		reset();
		if (i == 0) captured.identity.page_generation++;
		if (i == 1) captured.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(8) << 32) + 3);
		if (i == 2) captured.image_lsn++;
		if (i == 3) captured.identity.page_lsn = 79;
		if (i == 4) { uint8 bad = 0xff; if (pwrite(file, &bad, 1, 8192 + 64) != 1) abort(); }
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(returned_end, 777);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		close(file);
	}
}
UT_TEST(wal_error_restores_only_our_delay_and_keeps_fd)
{
	for (unsigned stage = 1; stage <= 2; stage++) {
		volatile bool raised = false;
		reset();
		proc.delayChkptFlags = DELAY_CHKPT_COMPLETE;
		throw_at = stage;
		PG_TRY(); { (void)write_page(); }
		PG_CATCH(); { raised = true; }
		PG_END_TRY();
		UT_ASSERT(raised);
		UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_COMPLETE);
		UT_ASSERT_EQ(writes + syncs, 0);
		unchanged_file();
		close(file);
	}
}
UT_TEST(short_or_failed_writes_keep_material_and_output)
{
	for (unsigned i = 0; i < 2; i++) {
		reset();
		short_write = i == 0;
		fail_write = i == 1;
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(returned_end, 777);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		UT_ASSERT(fcntl(file, F_GETFD) >= 0);
		if (i == 0) {
			uint8 first, last;
			UT_ASSERT_EQ(pread(file, &first, 1, 8192 + 64), 1);
			UT_ASSERT_EQ(pread(file, &last, 1, 16447), 1);
			UT_ASSERT_EQ(first, 0x42);
			UT_ASSERT_EQ(last, 0x31);
		}
		if (i == 1) unchanged_file();
		close(file);
	}
}
UT_TEST(sync_or_readback_failure_never_claims_success)
{
	for (unsigned i = 0; i < 2; i++) {
		reset();
		fail_sync = i == 0;
		corrupt_readback = i == 1;
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(returned_end, 777);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		UT_ASSERT(fcntl(file, F_GETFD) >= 0);
		close(file);
	}
}
UT_TEST(header_drift_after_flush_blocks_the_physical_write)
{
	reset();
	change_header = true;
	UT_ASSERT(!write_page());
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(writes + syncs, 0);
	UT_ASSERT_EQ(returned_end, 777);
	UT_ASSERT_EQ(proc.delayChkptFlags, 0);
	close(file);
}
UT_TEST(every_overwrite_gets_an_image_and_stale_snapshot_cannot_retry)
{
	reset();
	UT_ASSERT(write_page());
	UT_ASSERT(!write_page());
	UT_ASSERT_EQ(inserts, 1);
	captured.image_lsn = returned_end;
	insert_end = 2000;
	writes = flushes = syncs = nevents = 0;
	UT_ASSERT(write_page());
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(returned_end, 2000);
	close(file);
}
UT_TEST(private_wal_and_disk_bytes_survive_later_input_redirty)
{
	ClusterClogWalObservationV2 record = {0};
	reset();
	redirty_input = true;
	UT_ASSERT(write_page());
	UT_ASSERT(cluster_clog_wal_decode_v2(0x50, wal, sizeof(wal), &record));
	UT_ASSERT_EQ(record.page.identity.page_lsn, 90);
	UT_ASSERT_EQ(record.page.image_lsn, 100);
	UT_ASSERT(record.image != NULL);
	if (record.image != NULL) UT_ASSERT_EQ(record.image[64], 0x42);
	UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, 0, &observed, result, 8192));
	UT_ASSERT_EQ(observed.identity.page_lsn, 90);
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(result[0], 0x42);
	UT_ASSERT_EQ(result[8191], 0x42);
	UT_ASSERT_EQ(payload[0], 0x59);
	UT_ASSERT_EQ(captured.identity.page_lsn, 1900);
	close(file);
}
UT_TEST(invalid_actual_wal_end_never_flushes_or_writes)
{
	const XLogRecPtr invalid[] = {0, UINT64_MAX, 80, 90, 100};
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		reset();
		insert_end = invalid[i];
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes + writes + syncs, 0);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		unchanged_file();
		close(file);
	}
}
UT_TEST(disabled_fsync_is_not_a_durable_write)
{
	reset();
	enableFsync = false;
	UT_ASSERT(!write_page());
	UT_ASSERT_EQ(inserts + flushes + writes + syncs, 0);
	unchanged_file();
	UT_ASSERT_EQ(proc.delayChkptFlags, 0);
	close(file);
}

/* The original SLRU can first flush a later page, leaving complete sparse slots.
 * Real file bytes below are setup, never substitutes for production IO checks. */
static void
new_birth(uint64 page)
{
	uint32 raw = page == 0 ? 3 : page * 32768;
	captured.identity.logical_page = page;
	captured.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + raw);
	captured.identity.page_generation = U64FromFullTransactionId(captured.identity.birth_full_xid);
	captured.image_lsn = 0;
	if (!cluster_clog_file_offset_v2(page % 32, &target_offset)) abort();
}
static void
materialize_page(uint64 page, uint8 fill)
{
	ClusterClogPageIdentityV2 metadata = captured;
	uint8 data[8192], frame[8256];
	uint64 offset;
	uint32 raw = page == 0 ? 3 : page * 32768;

	metadata.identity.logical_page = page;
	metadata.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + raw);
	metadata.identity.page_generation = U64FromFullTransactionId(metadata.identity.birth_full_xid);
	metadata.image_lsn = 500;
	memset(data, fill, sizeof(data));
	if (!cluster_clog_file_page_encode_v2(&binding, &metadata, data, sizeof(data), frame, sizeof(frame)) ||
		!cluster_clog_file_offset_v2(page % 32, &offset) ||
		pwrite(file, frame, sizeof(frame), offset) != sizeof(frame)) abort();
}
UT_TEST(observed_birth_materializes_tail_and_complete_sparse_slots)
{
	for (unsigned hole = 0; hole < 2; hole++) {
		uint8 original[16448], later[8256], check[8256];
		ClusterClogWalObservationV2 record = {0};
		reset();
		UT_ASSERT_EQ(pread(file, original, sizeof(original), 0), sizeof(original));
		if (hole) {
			materialize_page(31, 0x66);
			UT_ASSERT_EQ(pread(file, later, sizeof(later), 264128), sizeof(later));
		}
		new_birth(1);
		UT_ASSERT(write_page());
		UT_ASSERT(memcmp(events, "IFWS", 4) == 0);
		UT_ASSERT_EQ(returned_end, 1000);
		UT_ASSERT_EQ(captured.image_lsn, 0);
		UT_ASSERT(cluster_clog_wal_decode_v2(0x50, wal, sizeof(wal), &record));
		UT_ASSERT_EQ(record.page.identity.logical_page, 1);
		UT_ASSERT_EQ(record.page.image_lsn, 0);
		UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, 1, &observed, result, sizeof(result)));
		UT_ASSERT_EQ(observed.image_lsn, 1000);
		UT_ASSERT_EQ(observed.identity.page_lsn, 90);
		UT_ASSERT_EQ(U64FromFullTransactionId(observed.identity.birth_full_xid), (UINT64CONST(7) << 32) + 32768);
		UT_ASSERT(memcmp(result, payload, sizeof(result)) == 0);
		UT_ASSERT_EQ(pread(file, initial, sizeof(original), 0), sizeof(original));
		UT_ASSERT(memcmp(initial, original, sizeof(original)) == 0);
		if (hole) {
			UT_ASSERT_EQ(pread(file, check, sizeof(check), 264128), sizeof(check));
			UT_ASSERT(memcmp(check, later, sizeof(check)) == 0);
		}
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		close(file);
	}
}
UT_TEST(partial_nonzero_corrupt_or_unknown_slots_do_not_become_new_births)
{
	for (unsigned shape = 0; shape < 8; shape++) {
		struct stat before, after;
		uint8 byte = 1;
		reset();
		new_birth(1);
		if (shape == 0) { if (ftruncate(file, 16449) != 0) abort(); }
		if (shape == 1) { if (ftruncate(file, 24703) != 0) abort(); }
		if (shape == 2) { if (pwrite(file, &byte, 1, 24703) != 1) abort(); }
		if (shape == 3) { materialize_page(1, 0x55); if (pwrite(file, &byte, 1, 16448 + 64) != 1) abort(); }
		if (shape == 4) captured.identity.birth_observed = false;
		if (shape == 5) captured.image_lsn = 99;
		if (shape == 6) captured.identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + 3);
		if (shape == 7) { if (ftruncate(file, 272385) != 0) abort(); }
		UT_ASSERT_EQ(fstat(file, &before), 0);
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(writes + inserts, 0);
		UT_ASSERT_EQ(returned_end, 777);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		UT_ASSERT_EQ(fstat(file, &after), 0);
		UT_ASSERT_EQ(after.st_size, before.st_size);
		close(file);
	}
}
UT_TEST(post_flush_target_occupation_refuses_without_overwriting_new_material)
{
	for (unsigned drift = 0; drift < 2; drift++) {
		reset();
		new_birth(1);
		occupy_target = drift == 0;
		change_header = drift == 1;
		UT_ASSERT(!write_page());
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT_EQ(returned_end, 777);
		if (drift == 0) {
			UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, 1, &observed, result, sizeof(result)));
			UT_ASSERT_EQ(observed.image_lsn, 500);
			UT_ASSERT_EQ(result[0], 0x55);
		} else {
			uint8 bytes[8192];
			ClusterClogFileBindingV2 changed = binding;
			struct stat st;
			changed.identity.creation_generation++;
			UT_ASSERT_EQ(pread(file, bytes, sizeof(bytes), 0), sizeof(bytes));
			UT_ASSERT(cluster_clog_file_header_matches_v2(bytes, sizeof(bytes), &changed));
			UT_ASSERT_EQ(fstat(file, &st), 0);
			UT_ASSERT_EQ(st.st_size, 16448);
		}
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		close(file);
	}
}
UT_TEST(unrelated_page_growth_during_flush_preserves_empty_target_and_other_page)
{
	reset();
	new_birth(1);
	grow_other_page = true;
	UT_ASSERT(write_page());
	UT_ASSERT_EQ(returned_end, 1000);
	UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, 31, &observed, result, sizeof(result)));
	UT_ASSERT_EQ(observed.image_lsn, 500);
	UT_ASSERT_EQ(result[0], 0x66);
	UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, 1, &observed, result, sizeof(result)));
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(result[0], 0x42);
	close(file);
}
UT_TEST(new_page_wal_errors_keep_old_file_and_checkpoint_flags)
{
	for (unsigned stage = 1; stage <= 2; stage++) {
		volatile bool raised = false;
		reset();
		new_birth(1);
		proc.delayChkptFlags = DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;
		throw_at = stage;
		PG_TRY(); { (void)write_page(); }
		PG_CATCH(); { raised = true; }
		PG_END_TRY();
		UT_ASSERT(raised);
		UT_ASSERT_EQ(writes + syncs, 0);
		UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);
		unchanged_file();
		close(file);
	}
}
UT_TEST(partial_new_frame_is_retained_and_cannot_retry_as_empty)
{
	struct stat st;
	reset();
	new_birth(1);
	short_write = true;
	UT_ASSERT(!write_page());
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(fstat(file, &st), 0);
	UT_ASSERT_EQ(st.st_size, 20576);
	UT_ASSERT_EQ(returned_end, 777);
	UT_ASSERT_EQ(proc.delayChkptFlags, 0);
	short_write = false;
	UT_ASSERT(!write_page());
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(fstat(file, &st), 0);
	UT_ASSERT_EQ(st.st_size, 20576);
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
	close(file);
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(full_native_wal_flush_precedes_frame_sync_readback);
	UT_RUN(preowned_checkpoint_delay_is_borrowed);
	UT_RUN(nonlocal_recovery_bootstrap_and_missing_proc_refuse_before_wal);
	UT_RUN(bad_arguments_and_append_fd_cannot_insert_or_write);
	UT_RUN(original_disk_birth_and_watermarks_must_match);
	UT_RUN(wal_error_restores_only_our_delay_and_keeps_fd);
	UT_RUN(short_or_failed_writes_keep_material_and_output);
	UT_RUN(sync_or_readback_failure_never_claims_success);
	UT_RUN(header_drift_after_flush_blocks_the_physical_write);
	UT_RUN(every_overwrite_gets_an_image_and_stale_snapshot_cannot_retry);
	UT_RUN(private_wal_and_disk_bytes_survive_later_input_redirty);
	UT_RUN(invalid_actual_wal_end_never_flushes_or_writes);
	UT_RUN(disabled_fsync_is_not_a_durable_write);
	UT_RUN(observed_birth_materializes_tail_and_complete_sparse_slots);
	UT_RUN(partial_nonzero_corrupt_or_unknown_slots_do_not_become_new_births);
	UT_RUN(post_flush_target_occupation_refuses_without_overwriting_new_material);
	UT_RUN(unrelated_page_growth_during_flush_preserves_empty_target_and_other_page);
	UT_RUN(new_page_wal_errors_keep_old_file_and_checkpoint_flags);
	UT_RUN(partial_new_frame_is_retained_and_cannot_retry_as_empty);
	UT_DONE();
	return ut_failed_count != 0;
}
