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
static ssize_t managed_fixture_pread(int fd, void *bytes, size_t length, off_t offset);
#undef pg_pread
#define pg_pread managed_fixture_pread
#include "../../backend/access/transam/clog_storage.c"
void XLogFlush(XLogRecPtr lsn);
#ifdef CLOG_WRITE_TEST_SOURCE
#include CLOG_WRITE_TEST_SOURCE
#else
#include "../../backend/access/transam/clog_write.c"
#endif

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
#define CLOG_FIXTURE_HAVE_UNDER_POSTMASTER
#include "test_cluster_clog_no_creation.h"
#undef CLOG_FIXTURE_HAVE_UNDER_POSTMASTER
static PGPROC proc;
static ClusterClogFileBindingV2 binding;
static ClusterClogFileBindingV2 cached_bindings[1];
static uint8 native_record[8384];
static const char *registered;
static unsigned inserts, wal_throw;
static bool change_birth, change_image, advance_source;
static bool change_binding, read_throw;
static unsigned reads;
static bool read_gap_zero, read_gap_caught;
static XLogRecPtr actual_end;
static int borrowed;

void XLogFlush(XLogRecPtr lsn);
bool RecoveryInProgress(void) { return InRecovery; }
uint64 GetSystemIdentifier(void) { return 123; }
TimeLineID GetWALInsertionTimeLine(void) { return 1; }
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
#ifdef SLRU_FIXTURE_ISSUER_HELD
	if (lock == XidGenLock) return mode == LW_EXCLUSIVE && SLRU_FIXTURE_ISSUER_HELD();
#endif
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
#ifdef SLRU_FIXTURE_BIRTH_INSERT
	if (SLRU_FIXTURE_BIRTH_PENDING()) return SLRU_FIXTURE_BIRTH_INSERT(rmgr, info);
#endif
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
#ifdef SLRU_FIXTURE_BIRTH_FLUSH
	if (SLRU_FIXTURE_BIRTH_PENDING()) { SLRU_FIXTURE_BIRTH_FLUSH(lsn); return; }
#endif
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
	if (change_binding) cached_bindings[0].identity.creation_generation++;
}
static ssize_t
managed_fixture_pread(int fd, void *bytes, size_t length, off_t offset)
{
	UT_ASSERT(!control_held && io_held);
	reads++;
	if (read_gap_zero) {
		/* One raw backend enters under the original ControlLock while this
		 * managed reader still owns the per-buffer I/O lock. */
		read_gap_zero = false;
		io_held = false; /* The other backend does not own the reader's lock. */
		control_held = true;
		PG_TRY(); { (void) SimpleLruZeroPage(&ctl, 3); }
		PG_CATCH(); { read_gap_caught = true; }
		PG_END_TRY();
		control_held = false;
		io_held = true; /* Resume the managed reader's original ownership. */
	}
	if (read_throw) siglongjmp(*PG_exception_stack, 1);
	return pread(fd, bytes, length, offset);
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
	cached_bindings[0] = binding;
	shared.clog_file_binding = cached_bindings;
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
	change_binding = read_throw = read_gap_zero = read_gap_caught = false; reads = 0;
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
	control_held = false; io_held = true;
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, payload, sizeof(payload)));
	control_held = true; io_held = false;
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(observed.identity.page_lsn, 20);
	UT_ASSERT_EQ(payload[0], 0x41);
	UT_ASSERT_EQ((uint8)page.data[0], 0x42);
	actual_end = 2000; advance_source = false;
	UT_ASSERT_EQ(managed_write(), 2000);
	restored(false);
	UT_ASSERT_EQ(inserts, 2);
	control_held = false; io_held = true;
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, payload, sizeof(payload)));
	control_held = true; io_held = false;
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

static bool
managed_read(void)
{
	return SimpleLruReadClogPageV2(&ctl, 0, 3, borrowed, &binding);
}

UT_TEST(physical_reload_installs_binding_and_roundtrips_through_managed_writer)
{
	ClusterClogPageIdentityV2 observed;
	uint8 bytes[8192];
	unsigned prior_reads;
	managed_reset(); dirty[0] = false;
	UT_ASSERT(managed_read()); restored(false);
	UT_ASSERT_EQ((uint8) page.data[0], 0x31);
	UT_ASSERT_EQ(identities[0].identity.page_lsn, 10);
	UT_ASSERT_EQ(identities[0].image_lsn, 100);
	UT_ASSERT_EQ(lsns[0] + lsns[1], 0);
	UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 3);
	prior_reads = reads;
	UT_ASSERT(managed_read());
	UT_ASSERT(reads > prior_reads); /* Same page never bypasses physical identity. */
	memset(page.data, 0x52, BLCKSZ); dirty[0] = true; lsns[0] = 80;
	UT_ASSERT_EQ(managed_write(), 1000); restored(false);
	/* Direct reader also runs under this fixture's finite original IO guard. */
	control_held = false; io_held = true;
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, bytes, sizeof(bytes)));
	control_held = true; io_held = false;
	UT_ASSERT_EQ(bytes[0], 0x52);
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(observed.identity.page_lsn, 80);
	close(borrowed);
}

UT_TEST(read_refusal_discards_provenance_without_installing_partial_bytes)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterClogFileBindingV2 empty = {0};
		ClusterClogPageIdentityV2 unknown = {0};
		uint8 bad = 0xff;
		managed_reset(); dirty[0] = false; InRecovery = true;
		if (fault == 0 && ftruncate(borrowed, 0) != 0) abort();
		if (fault == 1 && ftruncate(borrowed, 4 * BLCKSZ) != 0) abort();
		if (fault == 2 && pwrite(borrowed, &bad, 1, 8192 + 3 * 8256 + 64) != 1) abort();
		if (fault == 3) binding.identity.creation_generation++;
		if (fault == 4 && pwrite(borrowed, &bad, 1, 0) != 1) abort();
		UT_ASSERT(!managed_read());
		UT_ASSERT(control_held && !io_held && !dirty[0]);
		UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY);
		UT_ASSERT_EQ((uint8)page.data[0], 0x41);
		UT_ASSERT(memcmp(&empty, cached_bindings, sizeof(empty)) == 0);
		UT_ASSERT(memcmp(&unknown, identities, sizeof(unknown)) == 0);
		UT_ASSERT_EQ(lsns[0] + lsns[1] + queues + writes, 0);
		UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
		close(borrowed);
	}
}

UT_TEST(read_reservation_does_not_evict_dirty_or_duplicate_page)
{
	SlruPageStatus statuses[2] = {SLRU_PAGE_VALID, SLRU_PAGE_VALID};
	int pages[2] = {3, 3};
	ClusterClogPageIdentityV2 before;
	managed_reset(); before = identities[0];
	UT_ASSERT(!managed_read()); restored(true);
	UT_ASSERT_EQ(reads + writes + inserts, 0);
	UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
	dirty[0] = false;
	shared.num_slots = 2; shared.page_status = statuses; shared.page_number = pages;
	UT_ASSERT(!managed_read());
	UT_ASSERT_EQ(reads + writes + inserts, 0);
	UT_ASSERT(control_held && !io_held);
	UT_ASSERT_EQ(statuses[0], SLRU_PAGE_VALID);
	shared.num_slots = 1; shared.page_status = states; shared.page_number = numbers;
	UT_ASSERT(!SimpleLruReadClogPageV2(&ctl, -1, 3, borrowed, &binding));
	UT_ASSERT(!SimpleLruReadClogPageV2(&ctl, 1, 3, borrowed, &binding));
	UT_ASSERT(!SimpleLruReadClogPageV2(&ctl, 0, -1, borrowed, &binding));
	UT_ASSERT(!SimpleLruReadClogPageV2(&ctl, 0, 131072, borrowed, &binding));
	close(borrowed);
}

UT_TEST(raw_entrypoints_refuse_a_managed_cache_page_before_mutation)
{
	for (unsigned call = 0; call < 4; call++) {
		volatile bool caught = false;
		ClusterClogPageIdentityV2 before;
		managed_reset(); dirty[0] = false; ctl.PagePrecedes = page_precedes;
		UT_ASSERT(managed_read()); before = identities[0];
		dirty[0] = true;
		if (call == 3) control_held = false;
		PG_TRY(); {
			if (call == 0) SimpleLruWritePage(&ctl, 0);
			if (call == 1) (void) SimpleLruReadPage(&ctl, 3, true, 0);
			if (call == 2) (void) SimpleLruZeroPage(&ctl, 3);
			if (call == 3) (void) SimpleLruReadPage_ReadOnly(&ctl, 3, 0);
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(caught); restored(true);
		UT_ASSERT_EQ(inserts + writes + queues, 0);
		UT_ASSERT(memcmp(&before, identities, sizeof(before)) == 0);
		close(borrowed);
	}
}

UT_TEST(cached_full_file_identity_cannot_be_borrowed_by_another_carrier)
{
	for (unsigned field = 0; field < 6; field++) {
		uint8 header[8192], frame[8256], bytes[8192];
		managed_reset(); dirty[0] = false;
		UT_ASSERT(managed_read());
		if (field == 0) binding.identity.creation_generation++;
		if (field == 1) binding.identity.registration_generation++;
		if (field == 2) binding.identity.database_incarnation++;
		if (field == 3) binding.identity.storage_uuid[1]++;
		if (field == 4) binding.identity.redo_branch_uuid[1]++;
		if (field == 5) binding.identity.redo_branch_generation++;
		/* Independent file setup: same page birth/LSNs in a different carrier. */
		memset(bytes, 0x33, sizeof(bytes));
		if (!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)) ||
			!cluster_clog_file_page_encode_v2(&binding, identities, bytes, sizeof(bytes), frame, sizeof(frame)) ||
			pwrite(borrowed, header, sizeof(header), 0) != sizeof(header) ||
			pwrite(borrowed, frame, sizeof(frame), 8192 + 3 * 8256) != sizeof(frame)) abort();
		memset(page.data, 0x52, BLCKSZ); dirty[0] = true;
		UT_ASSERT_EQ(managed_write(), 0); restored(true);
		UT_ASSERT_EQ(inserts + writes, 0);
		UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 3);
		close(borrowed);
	}
}

UT_TEST(completion_rechecks_full_cached_binding_after_the_io_gap)
{
	managed_reset(); change_binding = true;
	UT_ASSERT_EQ(managed_write(), 0); restored(true);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(identities[0].image_lsn, 100);
	UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 4);
	close(borrowed);
}

UT_TEST(read_error_restores_empty_slot_and_retains_original_fd)
{
	volatile bool caught = false;
	ClusterClogFileBindingV2 empty = {0};
	managed_reset(); dirty[0] = false; read_throw = true;
	PG_TRY(); { (void) managed_read(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(control_held && !io_held && !dirty[0]);
	UT_ASSERT_EQ(states[0], SLRU_PAGE_EMPTY);
	UT_ASSERT_EQ(identities[0].identity.page_generation, 0);
	UT_ASSERT(memcmp(&empty, cached_bindings, sizeof(empty)) == 0);
	UT_ASSERT_EQ(wait_event, 0);
	UT_ASSERT(fcntl(borrowed, F_GETFD) >= 0);
	close(borrowed);
}

UT_TEST(full_binding_array_is_bounded_initialized_and_preserved_on_attach)
{
	SlruCtlData initialized = {0}, attached = {0};
	for (unsigned groups = 0; groups <= 2; groups += 2) {
		IsUnderPostmaster = false;
		SimpleLruInit(&initialized, "test", 3, groups, &control, directory, 0,
			groups ? SYNC_HANDLER_CLOG : SYNC_HANDLER_NONE);
		UT_ASSERT_EQ(allocated_size, SimpleLruShmemSize(3, groups));
		UT_ASSERT((initialized.shared->clog_file_binding != NULL) == (groups != 0));
		if (groups && initialized.shared->clog_file_binding != NULL) {
			ClusterClogFileBindingV2 empty = {0};
			for (unsigned i = 0; i < 3; i++)
				UT_ASSERT(memcmp(&empty, &initialized.shared->clog_file_binding[i], sizeof(empty)) == 0);
			UT_ASSERT((char *)(initialized.shared->clog_file_binding + 3) <= initialized.shared->page_buffer[0]);
			initialized.shared->clog_file_binding[2] = binding;
		}
		IsUnderPostmaster = true;
		SimpleLruInit(&attached, "test", 3, groups, &control, directory, 0,
			groups ? SYNC_HANDLER_CLOG : SYNC_HANDLER_NONE);
		UT_ASSERT(attached.shared == initialized.shared);
		if (groups && attached.shared->clog_file_binding != NULL)
			UT_ASSERT_EQ(attached.shared->clog_file_binding[2].identity.creation_generation, binding.identity.creation_generation);
		free(allocated_shared); allocated_shared = NULL;
	}
	IsUnderPostmaster = false;
	printf("# cache file binding bytes per CLOG slot: %zu\n", sizeof(ClusterClogFileBindingV2));
}

UT_TEST(original_unwritten_birth_binds_expected_file_before_its_first_io)
{
	ClusterClogPageIdentityV2 observed = {0};
	uint8 bytes[8192] = {0};
	managed_reset();
	/* The original issuer's independently observed birth has no prior image.
	 * The complete file header exists, but this page has never been written. */
	memset(cached_bindings, 0, sizeof(cached_bindings));
	identities[0].image_lsn = 0;
	if (ftruncate(borrowed, 8192) != 0) abort();
	UT_ASSERT_EQ(managed_write(), 1000); restored(false);
	UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 3);
	control_held = false; io_held = true;
	UT_ASSERT(cluster_clog_storage_read_page_v2(borrowed, &binding, 3, &observed, bytes, sizeof(bytes)));
	control_held = true; io_held = false;
	UT_ASSERT_EQ(observed.image_lsn, 1000);
	UT_ASSERT_EQ(bytes[0], 0x41);
	close(borrowed);
	/* An unbound reload (nonzero prior image) is never a new birth. */
	managed_reset(); memset(cached_bindings, 0, sizeof(cached_bindings));
	UT_ASSERT_EQ(managed_write(), 0); restored(true);
	UT_ASSERT_EQ(inserts + writes, 0);
	close(borrowed);
}

UT_TEST(inflight_managed_read_keeps_raw_zero_out_of_its_reserved_slot)
{
	managed_reset(); dirty[0] = false; read_gap_zero = true;
	UT_ASSERT(managed_read()); restored(false);
	UT_ASSERT(read_gap_caught);
	UT_ASSERT_EQ((uint8)page.data[0], 0x31);
	UT_ASSERT_EQ(cached_bindings[0].identity.creation_generation, 3);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	close(borrowed);
}

#ifndef CLOG_MANAGED_MAIN
#define CLOG_MANAGED_MAIN main
#endif
int CLOG_MANAGED_MAIN(void)
{
	strlcpy(directory, "/tmp/pgrac-slru-managed-XXXXXX", sizeof(directory));
	if (mkdtemp(directory) == NULL) abort();
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(17);
	UT_RUN(snapshot_redirty_and_actual_end_survive_original_io_lifecycle);
	UT_RUN(io_refusal_retains_dirty_identity_and_borrowed_fd);
	UT_RUN(native_wal_error_restores_slot_locks_and_only_owned_delay);
	UT_RUN(invalid_input_or_unknown_birth_never_marks_slot_busy);
	UT_RUN(refused_file_identity_and_empty_file_never_get_legacy_fallback);
	UT_RUN(completion_never_attaches_image_to_changed_birth_or_prior_image);
	UT_RUN(clean_slot_and_bad_controller_have_no_io);
	UT_RUN(physical_reload_installs_binding_and_roundtrips_through_managed_writer);
	UT_RUN(read_refusal_discards_provenance_without_installing_partial_bytes);
	UT_RUN(read_reservation_does_not_evict_dirty_or_duplicate_page);
	UT_RUN(raw_entrypoints_refuse_a_managed_cache_page_before_mutation);
	UT_RUN(cached_full_file_identity_cannot_be_borrowed_by_another_carrier);
	UT_RUN(completion_rechecks_full_cached_binding_after_the_io_gap);
	UT_RUN(read_error_restores_empty_slot_and_retains_original_fd);
	UT_RUN(full_binding_array_is_bounded_initialized_and_preserved_on_attach);
	UT_RUN(original_unwritten_birth_binds_expected_file_before_its_first_io);
	UT_RUN(inflight_managed_read_keeps_raw_zero_out_of_its_reserved_slot);
	unlink(filename); rmdir(directory);
	UT_DONE(); return ut_failed_count != 0;
}
