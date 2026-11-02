/* Real segment birth I/O; native WAL/error scheduling is a finite boundary. */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>
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
#ifdef CLOG_INITIALIZE_TEST_SOURCE
#include CLOG_INITIALIZE_TEST_SOURCE
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
#include "test_cluster_clog_no_creation.h"
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 captured, observed;
static uint8 payload[8192], result[8192], header_wal[8320], page_wal[8384];
static int file;
static unsigned writes, syncs, inserts, flushes, throw_at, fail_write_at, short_at;
static bool fail_sync, corrupt_readback, redirty_input, change_file, disable_sync;
static uint32 registered_len;
static const char *registered_bytes;
static uint64 system_id, page_offset;
static TimeLineID timeline;
static XLogRecPtr header_end, page_end, returned_end;
static char events[16];
static unsigned nevents;

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
	if (registered_len != 0 || (length != 8320 && length != 8384)) abort();
	registered_bytes = bytes;
	registered_len = length;
}
XLogRecPtr
XLogInsert(RmgrId rmgr, uint8 info)
{
	if (rmgr != RM_CLOG_ID || inserts > 1 || writes != 0 ||
		(MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	if (inserts == 0) {
		if (info != 0x40 || registered_len != sizeof(header_wal)) abort();
		memcpy(header_wal, registered_bytes, sizeof(header_wal));
		events[nevents++] = 'H';
	} else {
		if (info != 0x50 || registered_len != sizeof(page_wal)) abort();
		memcpy(page_wal, registered_bytes, sizeof(page_wal));
		events[nevents++] = 'P';
	}
	inserts++;
	if (throw_at == inserts) siglongjmp(*PG_exception_stack, 1);
	return inserts == 1 ? header_end : page_end;
}
void
XLogFlush(XLogRecPtr lsn)
{
	if (lsn != page_end || inserts != 2 || writes != 0 ||
		(MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	flushes++;
	events[nevents++] = 'F';
	if (throw_at == 3) siglongjmp(*PG_exception_stack, 1);
	if (redirty_input) {
		memset(payload, 0x59, sizeof(payload));
		captured.identity.page_lsn = 1900;
		captured.identity.page_generation++;
		binding.identity.creation_generation++;
	}
	if (change_file && pwrite(file, "other", 5, 0) != 5) abort();
	if (disable_sync) enableFsync = false;
}
static ssize_t
fixture_pwrite(int fd, const void *bytes, size_t length, off_t offset)
{
	if (flushes != 1 || writes > 1 ||
		(MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	if (writes == 0) {
		if (length != 8192 || offset != 0) abort();
		events[nevents++] = 'h';
	} else {
		if (length != 8256 || offset != (off_t)page_offset) abort();
		events[nevents++] = 'p';
	}
	writes++;
	if (fail_write_at == writes) { errno = EIO; return -1; }
	return pwrite(fd, bytes, short_at == writes ? length / 2 : length, offset);
}
static int
fixture_fsync(int fd)
{
	if (writes != 2 || (MyProc->delayChkptFlags & DELAY_CHKPT_START) == 0) abort();
	syncs++;
	events[nevents++] = 'S';
	if (fail_sync) { errno = EIO; return -1; }
	if (corrupt_readback) {
		uint8 bad = 0xff;
		if (pwrite(fd, &bad, 1, page_offset + 64) != 1) abort();
	}
	return fsync(fd);
}
static void
reset(uint64 logical_page)
{
	char path[] = "/tmp/pgrac-clog-initialize-XXXXXX";
	uint32 raw = logical_page == 0 ? 3 : logical_page * 32768;

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
	binding.segment = binding.identity.object_id = logical_page / 32;
	memset(&captured, 0, sizeof(captured));
	captured.identity.logical_page = logical_page;
	captured.identity.page_generation = (UINT64CONST(7) << 32) + raw;
	captured.identity.birth_observed = true;
	captured.identity.birth_full_xid = FullTransactionIdFromU64(captured.identity.page_generation);
	captured.identity.page_lsn = 90;
	memset(payload, 0x42, sizeof(payload));
	memset(header_wal, 0, sizeof(header_wal));
	memset(page_wal, 0, sizeof(page_wal));
	file = mkstemp(path);
	if (file < 0 || unlink(path) != 0 ||
		!cluster_clog_file_offset_v2(logical_page % 32, &page_offset)) abort();
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
	fail_write_at = short_at = 0;
	fail_sync = corrupt_readback = redirty_input = change_file = disable_sync = false;
	header_end = 700;
	page_end = 1000;
	returned_end = 777;
	memset(events, 0, sizeof(events));
}
static bool
initialize(void)
{
	return cluster_clog_storage_initialize_v2(file, &binding, &captured,
		payload, sizeof(payload), &returned_end);
}
static off_t
file_size(void)
{
	struct stat st;
	if (fstat(file, &st) != 0) abort();
	return st.st_size;
}
static void
failed_output_and_fd(void)
{
	UT_ASSERT_EQ(returned_end, 777);
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
	UT_ASSERT_EQ(proc.delayChkptFlags, 0);
}

UT_TEST(header_and_first_page_wal_are_flushed_before_file_materialization)
{
	const uint64 pages[] = {0, 1, 31, 32, 131071};
	for (unsigned i = 0; i < lengthof(pages); i++) {
		ClusterClogWalObservationV2 h = {0}, p = {0};
		reset(pages[i]);
		UT_ASSERT(initialize());
		UT_ASSERT(memcmp(events, "HPFhpS", 6) == 0);
		UT_ASSERT_EQ(inserts, 2);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(returned_end, 1000);
		UT_ASSERT_EQ(file_size(), page_offset + 8256);
		UT_ASSERT(cluster_clog_wal_decode_v2(0x40, header_wal, sizeof(header_wal), &h));
		UT_ASSERT(cluster_clog_wal_decode_v2(0x50, page_wal, sizeof(page_wal), &p));
		UT_ASSERT_EQ(h.binding.segment, binding.segment);
		UT_ASSERT_EQ(p.page.image_lsn, 0);
		UT_ASSERT_EQ(p.page.identity.logical_page, pages[i]);
		UT_ASSERT_EQ(p.page.identity.page_lsn, 90);
		UT_ASSERT(cluster_clog_storage_read_page_v2(file, &binding, pages[i], &observed, result, 8192));
		UT_ASSERT_EQ(observed.image_lsn, 1000);
		UT_ASSERT_EQ(observed.identity.page_lsn, 90);
		UT_ASSERT(memcmp(result, payload, 8192) == 0);
		UT_ASSERT_EQ(captured.image_lsn, 0);
		UT_ASSERT_EQ(lseek(file, 0, SEEK_CUR), 0);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		if (pages[i] % 32 != 0)
			UT_ASSERT(!cluster_clog_storage_read_page_v2(file, &binding, pages[i] - 1, &observed, result, 8192));
		close(file);
	}
}
UT_TEST(existing_partial_legacy_or_framed_material_is_never_overwritten)
{
	const off_t sizes[] = {1, 8192, 8193, 16448, 272384};
	for (unsigned i = 0; i < lengthof(sizes); i++) {
		char bytes[4] = {0};
		reset(0);
		UT_ASSERT_EQ(pwrite(file, "old!", 4, 0), 4);
		UT_ASSERT_EQ(ftruncate(file, sizes[i]), 0);
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(writes + inserts, 0);
		UT_ASSERT_EQ(file_size(), sizes[i]);
		UT_ASSERT_EQ(pread(file, bytes, 1, 0), 1);
		UT_ASSERT_EQ(bytes[0], 'o');
		failed_output_and_fd();
		close(file);
	}
}
UT_TEST(nonlocal_recovery_bootstrap_and_missing_proc_refuse_before_wal)
{
	for (unsigned i = 0; i < 8; i++) {
		reset(0);
		if (i == 0) system_id++;
		if (i == 1) timeline++;
		if (i == 2) binding.identity.origin = 1;
		if (i == 3) InRecovery = true;
		if (i == 4) recovery = true;
		if (i == 5) Mode = BootstrapProcessing;
		if (i == 6) MyProc = NULL;
		if (i == 7) enableFsync = false;
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(file_size(), 0);
		failed_output_and_fd();
		close(file);
	}
}
UT_TEST(invalid_or_previously_materialized_page_identity_cannot_create)
{
	for (unsigned i = 0; i < 7; i++) {
		reset(0);
		if (i == 0) captured.image_lsn = 10;
		if (i == 1) captured.identity.birth_observed = false;
		if (i == 2) captured.identity.birth_full_xid = FullTransactionIdFromU64(UINT64_MAX);
		if (i == 3) captured.identity.logical_page = 32;
		if (i == 4) binding.identity.registration_generation = 0;
		if (i == 5) captured.identity.page_lsn = UINT64_MAX;
		if (i == 6) captured.identity.page_generation = 0;
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(file_size(), 0);
		failed_output_and_fd();
		close(file);
	}
}
UT_TEST(invalid_arguments_fd_modes_and_nonregular_files_refuse)
{
	int pipes[2], sockets[2], read_only;
	reset(0);
	UT_ASSERT(!cluster_clog_storage_initialize_v2(-1, &binding, &captured, payload, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, NULL, &captured, payload, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, NULL, payload, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, &captured, NULL, 8192, &returned_end));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, &captured, payload, 8191, &returned_end));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, &captured, payload, 8192, NULL));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, &captured, payload, 8192, &captured.image_lsn));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, &captured, payload, 8192, (XLogRecPtr *)payload));
	UT_ASSERT(!cluster_clog_storage_initialize_v2(file, &binding, &captured, payload, 8192,
		(XLogRecPtr *)&binding.identity.system_identifier));
	UT_ASSERT_EQ(fcntl(file, F_SETFL, O_APPEND), 0);
	UT_ASSERT(!initialize());
	UT_ASSERT_EQ(fcntl(file, F_SETFL, 0), 0);
	UT_ASSERT_EQ(pipe(pipes), 0);
	UT_ASSERT(!cluster_clog_storage_initialize_v2(pipes[1], &binding, &captured, payload, 8192, &returned_end));
	close(pipes[0]); close(pipes[1]);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT(!cluster_clog_storage_initialize_v2(sockets[0], &binding, &captured, payload, 8192, &returned_end));
	close(sockets[0]); close(sockets[1]);
	read_only = open("/dev/null", O_RDONLY);
	UT_ASSERT(read_only >= 0);
	UT_ASSERT(!cluster_clog_storage_initialize_v2(read_only, &binding, &captured, payload, 8192, &returned_end));
	close(read_only);
	UT_ASSERT_EQ(inserts + writes, 0);
	failed_output_and_fd();
	close(file);
}
UT_TEST(wal_error_at_either_insert_or_flush_keeps_empty_file_and_owned_flags)
{
	for (unsigned stage = 1; stage <= 3; stage++) {
		for (unsigned preowned = 0; preowned < 2; preowned++) {
			bool caught = false;
			reset(0);
			proc.delayChkptFlags = DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0);
			throw_at = stage;
			PG_TRY(); { (void)initialize(); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT(caught);
			UT_ASSERT_EQ(writes + syncs, 0);
			UT_ASSERT_EQ(file_size(), 0);
			UT_ASSERT_EQ(returned_end, 777);
			UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0));
			UT_ASSERT(fcntl(file, F_GETFD) >= 0);
			close(file);
		}
	}
}
UT_TEST(invalid_actual_wal_ends_never_reach_data_io)
{
	const XLogRecPtr bad[] = {0, UINT64_MAX, 90, 700};
	for (unsigned i = 0; i < lengthof(bad); i++) {
		reset(0);
		page_end = bad[i];
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(flushes + writes, 0);
		UT_ASSERT_EQ(file_size(), 0);
		failed_output_and_fd();
		close(file);
	}
	for (unsigned i = 0; i < 3; i++) {
		reset(0);
		header_end = i == 0 ? 0 : (i == 1 ? UINT64_MAX : 90);
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes + writes, 0);
		failed_output_and_fd();
		close(file);
	}
}
UT_TEST(file_or_sync_state_drift_after_flush_blocks_first_physical_write)
{
	for (unsigned i = 0; i < 2; i++) {
		reset(0);
		change_file = i == 0;
		disable_sync = i == 1;
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(inserts, 2);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(file_size(), change_file ? 5 : 0);
		failed_output_and_fd();
		close(file);
	}
}
UT_TEST(partial_or_failed_header_and_page_writes_preserve_failure_material)
{
	for (unsigned stage = 1; stage <= 2; stage++) {
		for (unsigned short_case = 0; short_case < 2; short_case++) {
			off_t retained_size;
			reset(0);
			if (short_case) short_at = stage; else fail_write_at = stage;
			UT_ASSERT(!initialize());
			UT_ASSERT_EQ(inserts, 2);
			UT_ASSERT_EQ(flushes, 1);
			UT_ASSERT_EQ(writes, stage);
			UT_ASSERT_EQ(syncs, 0);
			retained_size = file_size();
			UT_ASSERT_EQ(retained_size, stage == 1 ? (short_case ? 4096 : 0) : (short_case ? 12320 : 8192));
			failed_output_and_fd();
			if (retained_size != 0) {
				unsigned prior_inserts = inserts;
				UT_ASSERT(!initialize());
				UT_ASSERT_EQ(inserts, prior_inserts);
				UT_ASSERT_EQ(file_size(), retained_size);
			}
			close(file);
		}
	}
}
UT_TEST(sync_or_complete_readback_failure_cannot_publish_success)
{
	for (unsigned i = 0; i < 2; i++) {
		reset(0);
		fail_sync = i == 0;
		corrupt_readback = i == 1;
		UT_ASSERT(!initialize());
		UT_ASSERT_EQ(writes, 2);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(file_size(), 16448);
		failed_output_and_fd();
		close(file);
	}
}
UT_TEST(private_header_page_and_payload_survive_input_redirty_after_wal)
{
	ClusterClogFileBindingV2 saved;
	uint64 generation;
	reset(0);
	saved = binding;
	generation = captured.identity.page_generation;
	redirty_input = true;
	UT_ASSERT(initialize());
	UT_ASSERT(cluster_clog_storage_read_page_v2(file, &saved, 0, &observed, result, 8192));
	UT_ASSERT_EQ(observed.identity.page_generation, generation);
	UT_ASSERT_EQ(observed.identity.page_lsn, 90);
	UT_ASSERT_EQ(result[0], 0x42);
	UT_ASSERT_EQ(payload[0], 0x59);
	UT_ASSERT_EQ(captured.identity.page_lsn, 1900);
	UT_ASSERT_EQ(returned_end, 1000);
	close(file);
}
UT_TEST(success_retains_preowned_checkpoint_flags_and_borrowed_fd)
{
	reset(0);
	proc.delayChkptFlags = DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;
	UT_ASSERT(initialize());
	UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
	close(file);
}
int
main(void)
{
	UT_PLAN(12);
	UT_RUN(header_and_first_page_wal_are_flushed_before_file_materialization);
	UT_RUN(existing_partial_legacy_or_framed_material_is_never_overwritten);
	UT_RUN(nonlocal_recovery_bootstrap_and_missing_proc_refuse_before_wal);
	UT_RUN(invalid_or_previously_materialized_page_identity_cannot_create);
	UT_RUN(invalid_arguments_fd_modes_and_nonregular_files_refuse);
	UT_RUN(wal_error_at_either_insert_or_flush_keeps_empty_file_and_owned_flags);
	UT_RUN(invalid_actual_wal_ends_never_reach_data_io);
	UT_RUN(file_or_sync_state_drift_after_flush_blocks_first_physical_write);
	UT_RUN(partial_or_failed_header_and_page_writes_preserve_failure_material);
	UT_RUN(sync_or_complete_readback_failure_cannot_publish_success);
	UT_RUN(private_header_page_and_payload_survive_input_redirty_after_wal);
	UT_RUN(success_retains_preowned_checkpoint_flags_and_borrowed_fd);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
