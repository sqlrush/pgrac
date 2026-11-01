/* Actual SLRU state, native full-frame writer and temporary file bytes.
 * Lock scheduling and native WAL insertion/error injection are finite leaves. */
#define main snapshot_fixture_main
#define XLogFlush snapshot_fixture_flush
int snapshot_fixture_main(void);
#include "test_cluster_clog_write_snapshot.c"
#undef main
#undef XLogFlush

#include "access/clog_storage.h"
#include "access/clog_wal.h"
#include "access/xloginsert.h"
#include "storage/proc.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#endif
void XLogFlush(XLogRecPtr lsn);
#include "../../backend/access/transam/clog_write.c"

#ifdef CLOG_SLRU_REFUSING_SCAFFOLD
#include CLOG_SLRU_REFUSING_SCAFFOLD
#endif

PGPROC *MyProc;
ProcessingMode Mode;
bool enableFsync;
#ifdef USE_PGRAC_CLUSTER
bool cluster_enabled;
int cluster_node_id;
#endif
static PGPROC proc;
static ClusterClogFileBindingV2 binding;
static uint8 native_record[8384];
static const char *registered;
static unsigned inserts, wal_throw;
static bool change_birth, change_image, advance_source;
static XLogRecPtr actual_end;
static int borrowed;

void XLogFlush(XLogRecPtr lsn);
bool RecoveryInProgress(void) { return InRecovery; }
uint64 GetSystemIdentifier(void) { return 123; }
TimeLineID GetWALInsertionTimeLine(void) { return 1; }
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return mode == LW_EXCLUSIVE && LWLockHeldByMe(lock);
}
void pg_re_throw(void)
{
	if (PG_exception_stack == NULL) abort();
	siglongjmp(*PG_exception_stack, 1);
}
void XLogBeginInsert(void) { registered = NULL; }
void XLogRegisterData(char *bytes, uint32 length)
{
	if (length != sizeof(native_record) || registered != NULL) abort();
	registered = bytes;
}
XLogRecPtr XLogInsert(RmgrId rmgr, uint8 info)
{
	if (rmgr != RM_CLOG_ID || info != 0x50 || registered == NULL ||
		control_held || !io_held || CritSectionCount != 0 ||
		!(proc.delayChkptFlags & DELAY_CHKPT_START)) abort();
	memcpy(native_record, registered, sizeof(native_record));
	inserts++;
	record_event('I');
	if (wal_throw == 1) siglongjmp(*PG_exception_stack, 1);
	return actual_end;
}
void XLogFlush(XLogRecPtr lsn)
{
	if (lsn != actual_end || control_held || !io_held || CritSectionCount != 0 ||
		!(proc.delayChkptFlags & DELAY_CHKPT_START)) abort();
	flushed = lsn;
	flushes++;
	record_event('F');
	if (wal_throw == 2) siglongjmp(*PG_exception_stack, 1);
	/* Model an original control-lock updater while this writer retains I/O. */
	if (advance_source) identities[0].identity.page_lsn = 50;
	if (change_birth) identities[0].identity.page_generation++;
	if (change_image) identities[0].image_lsn++;
}

static void
managed_reset(void)
{
	uint8 header[8192], frame[8256], old[8192];

	reset(SYNC_HANDLER_CLOG);
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
	identities[0].identity.logical_page = 3;
	identities[0].identity.page_generation = 9;
	identities[0].identity.birth_full_xid = FullTransactionIdFromU64((UINT64CONST(7) << 32) + 3 * 32768);
	identities[0].identity.birth_observed = true;
	identities[0].identity.page_lsn = 10;
	identities[0].image_lsn = 100;
	memset(old, 0x31, sizeof(old));
	if (!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)) ||
		!cluster_clog_file_page_encode_v2(&binding, identities, old, sizeof(old), frame, sizeof(frame))) abort();
	borrowed = open(filename, O_CREAT | O_EXCL | O_RDWR, 0600);
	if (borrowed < 0 || pwrite(borrowed, header, sizeof(header), 0) != sizeof(header) ||
		pwrite(borrowed, frame, sizeof(frame), 8192 + 3 * 8256) != sizeof(frame)) abort();
	identities[0].identity.page_lsn = 15;
	identities[0].identity.payload_crc = identities[0].identity.metadata_crc = 123;
	memset(&proc, 0, sizeof(proc)); MyProc = &proc;
	Mode = NormalProcessing; InRecovery = false; enableFsync = true;
#ifdef USE_PGRAC_CLUSTER
	cluster_enabled = true; cluster_node_id = 0;
#endif
	inserts = wal_throw = 0; actual_end = 1000;
	change_birth = change_image = advance_source = false;
}
static XLogRecPtr
managed_write(void)
{
	return SimpleLruWriteClogPageV2(&ctl, 0, borrowed, &binding);
}
static void
restored(bool is_dirty)
{
	UT_ASSERT(control_held && !io_held);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
	UT_ASSERT_EQ(dirty[0], is_dirty);
	UT_ASSERT_EQ(wait_event, 0);
	UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
	UT_ASSERT_EQ(queues + closes, 0);
}

UT_TEST(snapshot_redirty_and_actual_end_survive_original_io_lifecycle)
{
	ClusterClogPageIdentityV2 observed;
	ClusterClogWalObservationV2 record = {0};
	uint8 payload[8192];
	managed_reset(); inject_update = advance_source = true;
	UT_ASSERT_EQ(managed_write(), 1000);
	restored(true);
	UT_ASSERT(memcmp(events, "IFWS", 4) == 0);
	UT_ASSERT_EQ(stats_writes, 1);
	UT_ASSERT_EQ(identities[0].image_lsn, 1000);
	UT_ASSERT_EQ(identities[0].identity.page_lsn, 50);
	UT_ASSERT_EQ(identities[0].identity.payload_crc, 0);
	UT_ASSERT_EQ(identities[0].identity.metadata_crc, 0);
	UT_ASSERT_EQ(lsns[1], 40);
	UT_ASSERT(cluster_clog_wal_decode_v2(0x50, native_record, sizeof(native_record), &record));
	UT_ASSERT_EQ(record.page.identity.page_lsn, 20);
	UT_ASSERT(record.image != NULL && record.image[64] == 0x41);
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, payload, sizeof(payload)));
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(observed.identity.page_lsn, 20);
	UT_ASSERT_EQ(payload[0], 0x41);
	UT_ASSERT_EQ((uint8)page.data[0], 0x42);
	actual_end = 2000; advance_source = false;
	UT_ASSERT_EQ(managed_write(), 2000);
	restored(false);
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, payload, sizeof(payload)));
	UT_ASSERT_EQ(observed.image_lsn, 2000);
	UT_ASSERT_EQ(observed.identity.page_lsn, 50);
	UT_ASSERT_EQ(payload[0], 0x42);
	close(borrowed);
}
UT_TEST(io_refusal_retains_dirty_identity_and_borrowed_fd)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterClogPageIdentityV2 before;
		managed_reset(); before = identities[0];
		fail_write = fault == 0; short_write = fault == 1; fail_sync = fault == 2;
		UT_ASSERT_EQ(managed_write(), 0);
		restored(true);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
		UT_ASSERT_EQ(proc.delayChkptFlags, 0);
		close(borrowed);
	}
}
UT_TEST(native_wal_error_restores_slot_locks_and_only_owned_delay)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		for (unsigned preowned = 0; preowned < 2; preowned++) {
			volatile bool caught = false;
			managed_reset(); wal_throw = fault;
			proc.delayChkptFlags = DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0);
			PG_TRY(); { (void) managed_write(); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT(caught); restored(true);
			UT_ASSERT_EQ(writes, 0);
			UT_ASSERT_EQ(identities[0].image_lsn, 100);
			UT_ASSERT_EQ(proc.delayChkptFlags, DELAY_CHKPT_COMPLETE | (preowned ? DELAY_CHKPT_START : 0));
			close(borrowed);
		}
	}
}
UT_TEST(invalid_input_or_unknown_birth_never_marks_slot_busy)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterClogPageIdentityV2 before;
		managed_reset();
		if (fault == 0) identities[0].identity.birth_observed = false;
		if (fault == 1) identities[0].identity.logical_page = 4;
		if (fault == 2) ctl.sync_handler = SYNC_HANDLER_NONE;
		if (fault == 3) shared.clog_page_identity = NULL;
		if (fault == 4) shared.group_lsn = NULL;
		if (fault == 5) shared.lsn_groups_per_page = 0;
		if (fault == 6) control_held = false;
		before = identities[0];
		UT_ASSERT_EQ(SimpleLruWriteClogPageV2(&ctl, fault == 7 ? -1 : fault == 8 ? 1 : 0,
			borrowed, fault == 9 ? NULL : &binding), 0);
		UT_ASSERT_EQ(inserts + writes + stats_writes, 0);
		UT_ASSERT(!io_held && dirty[0]);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_VALID);
		UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
		close(borrowed);
	}
}
UT_TEST(refused_file_identity_and_empty_file_never_get_legacy_fallback)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		managed_reset();
		if (fault == 0) binding.identity.creation_generation++;
		if (fault == 1) binding.identity.origin++;
		if (fault == 2) InRecovery = true;
		if (fault == 3 && ftruncate(borrowed, 0) != 0) abort();
		UT_ASSERT_EQ(managed_write(), 0); restored(true);
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(identities[0].image_lsn, 100);
		close(borrowed);
	}
}
UT_TEST(completion_never_attaches_image_to_changed_birth_or_prior_image)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		managed_reset(); change_birth = fault == 0; change_image = fault == 1;
		UT_ASSERT_EQ(managed_write(), 0); restored(true);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(identities[0].image_lsn, fault == 1 ? 101 : 100);
		UT_ASSERT_EQ(identities[0].identity.page_generation, fault == 0 ? 10 : 9);
		close(borrowed);
	}
}
UT_TEST(clean_slot_and_bad_controller_have_no_io)
{
	managed_reset(); dirty[0] = false;
	UT_ASSERT_EQ(managed_write(), 0); restored(false);
	UT_ASSERT_EQ(SimpleLruWriteClogPageV2(NULL, 0, borrowed, &binding), 0);
	UT_ASSERT_EQ(inserts + writes + stats_writes, 0);
	close(borrowed);
}

int main(void)
{
	strlcpy(directory, "/tmp/pgrac-slru-managed-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(7);
	UT_RUN(snapshot_redirty_and_actual_end_survive_original_io_lifecycle);
	UT_RUN(io_refusal_retains_dirty_identity_and_borrowed_fd);
	UT_RUN(native_wal_error_restores_slot_locks_and_only_owned_delay);
	UT_RUN(invalid_input_or_unknown_birth_never_marks_slot_busy);
	UT_RUN(refused_file_identity_and_empty_file_never_get_legacy_fallback);
	UT_RUN(completion_never_attaches_image_to_changed_birth_or_prior_image);
	UT_RUN(clean_slot_and_bad_controller_have_no_io);
	unlink(filename); rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
