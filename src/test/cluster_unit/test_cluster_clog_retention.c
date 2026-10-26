/* PGRAC: original native CLOG truncation with the original retention runtime.
 * Authority, COMPLETE publication, WAL transport and lock scheduling are
 * controlled; native CLOG/SLRU bodies and disposable file operations execute.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#define main clog_recovery_original_main
int clog_recovery_original_main(void);
#include "test_cluster_clog_recovery.c"
#undef main
#include "../../backend/cluster/cluster_relmap_retention.c"
#include "../../backend/cluster/cluster_relmap_retention_runtime.c"
#include "test_cluster_clog_retention_floor.inc"

bool cluster_enabled = true;
ProcessingMode Mode = NormalProcessing;
static VariableCacheData truncate_variables;
VariableCache ShmemVariableCache = &truncate_variables;
static xl_clog_truncate truncate_wal;
static uint64 truncate_requested;
static unsigned truncate_fault, truncate_inserts;
static bool truncate_interleave;

bool
RecoveryInProgress(void)
{
	return InRecovery;
}

/* All acquisitions in the inherited lock fixture are exclusive. */
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return mode == LW_EXCLUSIVE && LWLockHeldByMe(lock);
}

static ClusterRelmapRetentionDirectory *
truncate_directory(void)
{
	return &relmap_runtime_shmem->directories[cluster_node_id];
}

static ClusterRelmapRetentionKey
truncate_key(uint64 full, bool shared)
{
	ClusterRelmapRetentionKey key = { 0 };

	key.identity.system_identifier = 11;
	key.identity.database_incarnation = 12;
	key.identity.shared = shared;
	key.identity.dbid = shared ? InvalidOid : 42;
	key.generation = 8;
	key.origin = cluster_node_id;
	key.top_full_xid = full;
	key.admitted_incarnation = key.boot_incarnation = 1;
	return key;
}

void
XLogBeginInsert(void)
{
	ClusterRelmapRetentionDirectory *directory = truncate_directory();

	UT_ASSERT(!LWLockHeldByMe(&relmap_runtime_shmem->lock));
	UT_ASSERT(!LWLockHeldByMe(XidGenLock));
	UT_ASSERT(!LWLockHeldByMe(XactSLRULock));
	UT_ASSERT(!LWLockHeldByMe(XactTruncationLock));
	if (truncate_interleave) {
		ClusterRelmapRetentionRuntimeTruncateV2 nested, before;
		ClusterRelmapRetentionKey below = truncate_key(directory->inflight.cutoff - 1, false);
		ClusterRelmapRetentionKey at = truncate_key(directory->inflight.cutoff, false);
		ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];

		memset(&nested, 0x5a, sizeof(nested));
		before = nested;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(11, 12,
			cluster_node_id, truncate_requested, &nested), CLUSTER_RELMAP_RETENTION_BUSY);
		UT_ASSERT_EQ(memcmp(&nested, &before, sizeof(nested)), 0);
		LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
		memcpy(pins, directory->pins, sizeof(pins));
		UT_ASSERT_EQ(cluster_relmap_retention_begin(directory, &below), CLUSTER_RELMAP_RETENTION_NATIVE_GONE);
		UT_ASSERT_EQ(memcmp(pins, directory->pins, sizeof(pins)), 0);
		UT_ASSERT_EQ(cluster_relmap_retention_begin(directory, &at), CLUSTER_RELMAP_RETENTION_OK);
		LWLockRelease(&relmap_runtime_shmem->lock);
	}
	if (truncate_fault == 1)
		ereport(ERROR, (errmsg("controlled native truncate WAL failure")));
	if (truncate_fault == 4) {
		LWLockAcquire(&relmap_runtime_shmem->lock, LW_EXCLUSIVE);
		cluster_relmap_retention_invalidate(directory);
		LWLockRelease(&relmap_runtime_shmem->lock);
	}
}

void
XLogRegisterData(char *data, uint32 length)
{
	UT_ASSERT_EQ(length, sizeof(truncate_wal));
	if (length == sizeof(truncate_wal))
		memcpy(&truncate_wal, data, length);
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(rmid, RM_CLOG_ID);
	UT_ASSERT_EQ(info, CLOG_TRUNCATE);
	UT_ASSERT_EQ(truncate_variables.oldestClogXid, truncate_wal.oldestXact);
	UT_ASSERT_EQ(native_wal_flushes, 0);
	UT_ASSERT_EQ(deleted, 0);
	++truncate_inserts;
	return 100;
}

static void
truncate_file(unsigned segment, bool create)
{
	char path[64];
	PGAlignedBlock bytes;
	int fd;

	snprintf(path, sizeof(path), "pg_xact/%04X", segment);
	memset(bytes.data, (segment & 0x7f) + 1, BLCKSZ);
	fd = open(path, create ? O_RDWR | O_CREAT | O_EXCL : O_RDONLY, 0600);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		if (create)
			UT_ASSERT_EQ(write(fd, bytes.data, BLCKSZ), BLCKSZ);
		else {
			PGAlignedBlock actual;
			struct stat st;

			UT_ASSERT_EQ(fstat(fd, &st), 0);
			UT_ASSERT_EQ(st.st_size, BLCKSZ);
			UT_ASSERT_EQ(read(fd, actual.data, BLCKSZ), BLCKSZ);
			UT_ASSERT_EQ(memcmp(actual.data, bytes.data, BLCKSZ), 0);
		}
		UT_ASSERT_EQ(close(fd), 0);
	}
}

static void
truncate_reset(uint64 floor, uint64 issued, unsigned origin)
{
	DIR *dir = opendir("pg_xact");
	struct dirent *entry;
	ClusterRelmapRetentionDirectory *directory;

	UT_ASSERT(dir != NULL);
	while ((entry = readdir(dir)) != NULL) {
		char path[64];

		if (entry->d_name[0] == '.') continue;
		snprintf(path, sizeof(path), "pg_xact/%s", entry->d_name);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	memset(relmap_runtime_shmem->directories, 0, sizeof(relmap_runtime_shmem->directories));
	cluster_node_id = origin;
	directory = truncate_directory();
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(directory, 11, 12, origin, 7,
		floor, issued), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(directory), CLUSTER_RELMAP_RETENTION_OK);
	truncate_variables.oldestClogXid = (TransactionId)floor;
	XactCtl->shared->latest_page_number = TransactionIdToPage((TransactionId)issued);
	for (int i = 0; i < XactCtl->shared->num_slots; i++) {
		XactCtl->shared->page_status[i] = SLRU_PAGE_EMPTY;
		XactCtl->shared->page_dirty[i] = false;
	}
	cluster_enabled = cluster_shared_config = enableFsync = true;
	Mode = NormalProcessing;
	InRecovery = expect_error = fail_open = fail_unlink = false;
	CritSectionCount = InterruptHoldoffCount = 0;
	truncate_fault = truncate_inserts = native_wal_flushes = deleted = 0;
	truncate_interleave = false;
	memset(&truncate_wal, 0, sizeof(truncate_wal));
}

static bool
truncate_call(uint64 requested)
{
	bool result;
	bool acquired = !LWLockHeldByMe(WrapLimitsVacuumLock);

	truncate_requested = requested;
	if (acquired) LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);
	result = cluster_relmap_retention_truncate_clog_v2(FullTransactionIdFromU64(requested),
		42, 11, 12, cluster_node_id);
	if (acquired) LWLockRelease(WrapLimitsVacuumLock);
	return result;
}

UT_TEST(original_truncate_uses_all_namespace_minimum_for_files_floor_and_wal)
{
	const unsigned origins[] = { 0, CLUSTER_MAX_NODES - 1 };
	const unsigned epochs[] = { 0, 7 };

	for (unsigned e = 0; e < lengthof(epochs); e++)
		for (unsigned o = 0; o < lengthof(origins); o++) {
			uint64 base = (uint64)epochs[e] << 32;
			ClusterRelmapRetentionKey first, second;
			ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];

			truncate_reset(base + 3, base + 4194404, origins[o]);
			first = truncate_key(base + 1048676, e == 0);
			second = truncate_key(base + 2097252, e != 0);
			UT_ASSERT_EQ(cluster_relmap_retention_begin(truncate_directory(), &first), CLUSTER_RELMAP_RETENTION_OK);
			UT_ASSERT_EQ(cluster_relmap_retention_begin(truncate_directory(), &second), CLUSTER_RELMAP_RETENTION_OK);
			memcpy(pins, truncate_directory()->pins, sizeof(pins));
			for (unsigned i = 0; i < 4; i++) truncate_file(i, true);
			UT_ASSERT(truncate_call(base + 3145828));
			UT_ASSERT(access("pg_xact/0000", F_OK) != 0 && errno == ENOENT);
			for (unsigned i = 1; i < 4; i++) truncate_file(i, false);
			UT_ASSERT_EQ(truncate_wal.pageno, 32);
			UT_ASSERT_EQ(truncate_wal.oldestXact, 1048676);
			UT_ASSERT_EQ(truncate_wal.oldestXactDb, 42);
			UT_ASSERT_EQ(truncate_variables.oldestClogXid, 1048676);
			UT_ASSERT_EQ(native_wal_flushes, 1);
			UT_ASSERT_EQ(truncate_inserts, 1);
			UT_ASSERT_EQ(deleted, 1);
			UT_ASSERT_EQ(truncate_directory()->native_floor, base + 1048676);
			UT_ASSERT_EQ(truncate_directory()->inflight.serial, 0);
			UT_ASSERT_EQ(memcmp(pins, truncate_directory()->pins, sizeof(pins)), 0);
			fixture_idle();
		}
}

UT_TEST(reservation_excludes_late_older_begin_and_concurrent_truncate)
{
	ClusterRelmapRetentionKey pin;

	truncate_reset(3, 4194404, 0);
	pin = truncate_key(1048676, true);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(truncate_directory(), &pin), CLUSTER_RELMAP_RETENTION_OK);
	truncate_file(0, true);
	truncate_file(1, true);
	truncate_interleave = true;
	UT_ASSERT(truncate_call(3145828));
	UT_ASSERT_EQ(truncate_directory()->native_floor, 1048676);
	UT_ASSERT_EQ(truncate_directory()->inflight.serial, 0);
	UT_ASSERT_EQ(truncate_directory()->pins[1].key.top_full_xid, 1048676);
	truncate_file(1, false);
	fixture_idle();
}

UT_TEST(normal_return_with_undeleted_files_retains_unknown_reservation)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRelmapRetentionKey pin;
		ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];
		ClusterRelmapRetentionRuntimeTruncateV2 receipt;

		truncate_reset(3, 4194404, 0);
		pin = truncate_key(3145828, true);
		UT_ASSERT_EQ(cluster_relmap_retention_begin(truncate_directory(), &pin), CLUSTER_RELMAP_RETENTION_OK);
		truncate_file(0, true);
		truncate_file(3, true);
		memcpy(pins, truncate_directory()->pins, sizeof(pins));
		fail_unlink = fault == 0;
		if (fault == 1) XactCtl->shared->latest_page_number = 0;
		UT_ASSERT(!truncate_call(3145828));
		UT_ASSERT_EQ(truncate_directory()->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		UT_ASSERT_EQ(truncate_directory()->inflight.cutoff, 3145828);
		UT_ASSERT(truncate_directory()->inflight.serial != 0);
		UT_ASSERT_EQ(truncate_directory()->native_floor, 3);
		UT_ASSERT_EQ(memcmp(pins, truncate_directory()->pins, sizeof(pins)), 0);
		truncate_file(0, false);
		truncate_file(3, false);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(11, 12, 0, 3145828,
			&receipt), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(native_wal_flushes, 1);
		fail_unlink = false;
		fixture_idle();
	}
}

UT_TEST(native_error_or_late_invalidation_preserves_pins_and_no_retry)
{
	const unsigned faults[] = { 1, 3, 4 };

	for (unsigned i = 0; i < lengthof(faults); i++) {
		volatile bool caught = false, result = false;
		ClusterRelmapRetentionKey pin;
		ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];

		truncate_reset(3, 4194404, 0);
		pin = truncate_key(2097252, true);
		UT_ASSERT_EQ(cluster_relmap_retention_begin(truncate_directory(), &pin), CLUSTER_RELMAP_RETENTION_OK);
		memcpy(pins, truncate_directory()->pins, sizeof(pins));
		truncate_file(0, true);
		truncate_file(2, true);
		truncate_fault = faults[i];
		expect_error = truncate_fault != 4;
		if (truncate_fault == 3) {
			LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);
			XactCtl->shared->page_status[0] = SLRU_PAGE_VALID;
			XactCtl->shared->page_number[0] = 0;
			XactCtl->shared->page_dirty[0] = true;
			memset(XactCtl->shared->group_lsn, 0,
				XactCtl->shared->num_slots * CLOG_LSNS_PER_PAGE * sizeof(XLogRecPtr));
			fail_open = true;
		}
		PG_TRY();
		{
			result = truncate_call(3145828);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, truncate_fault != 4);
		UT_ASSERT(!result);
		UT_ASSERT_EQ(truncate_directory()->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
		UT_ASSERT_EQ(truncate_directory()->inflight.cutoff, 2097252);
		UT_ASSERT(truncate_directory()->inflight.serial != 0);
		UT_ASSERT_EQ(truncate_directory()->native_floor, 3);
		UT_ASSERT_EQ(memcmp(pins, truncate_directory()->pins, sizeof(pins)), 0);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		if (caught) {
			UT_ASSERT(LWLockHeldByMe(WrapLimitsVacuumLock));
			HOLD_INTERRUPTS(); /* Original ERROR reset; only the fixture releases its caller lock. */
			LWLockRelease(WrapLimitsVacuumLock);
		}
		UT_ASSERT(!truncate_call(3145828));
		fail_open = expect_error = false;
		fixture_idle();
	}
}

UT_TEST(invalid_native_context_refuses_before_reservation_or_io)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterRelmapRetentionDirectory before;
		LWLock *caller_lock = NULL;

		truncate_reset(3, 4194404, 0);
		truncate_file(0, true);
		if (fault == 0) truncate_directory()->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (fault == 1) truncate_directory()->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (fault == 2) cluster_enabled = false;
		if (fault == 3) cluster_shared_config = false;
		if (fault == 4) Mode = InitProcessing;
		if (fault == 5) InRecovery = true;
		if (fault == 6) CritSectionCount = 1;
		if (fault == 7) caller_lock = XidGenLock;
		if (fault == 8) caller_lock = XactSLRULock;
		if (fault == 9) caller_lock = XactTruncationLock;
		if (fault == 10) caller_lock = &relmap_runtime_shmem->lock;
		if (fault == 13) enableFsync = false;
		LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);
		if (caller_lock) LWLockAcquire(caller_lock, LW_EXCLUSIVE);
		before = *truncate_directory();
		UT_ASSERT(!cluster_relmap_retention_truncate_clog_v2(FullTransactionIdFromU64(3145828),
			42, fault == 11 ? 99 : 11, 12, fault == 12 ? 1 : 0));
		UT_ASSERT_EQ(memcmp(&before, truncate_directory(), sizeof(before)), 0);
		UT_ASSERT_EQ(truncate_variables.oldestClogXid, 3);
		UT_ASSERT_EQ(native_wal_flushes, 0);
		UT_ASSERT_EQ(truncate_inserts, 0);
		UT_ASSERT_EQ(deleted, 0);
		truncate_file(0, false);
		if (caller_lock) {
			UT_ASSERT(LWLockHeldByMe(caller_lock));
			LWLockRelease(caller_lock);
		}
		UT_ASSERT(LWLockHeldByMe(WrapLimitsVacuumLock));
		LWLockRelease(WrapLimitsVacuumLock);
		CritSectionCount = 0;
		fixture_idle();
	}
}

UT_TEST(original_raw_wrap_comparison_keeps_full_xid_pin_segment)
{
	ClusterRelmapRetentionKey pin;
	uint64 base = UINT64CONST(7) << 32;

	truncate_reset(base + UINT64CONST(0xffc00003), base + UINT64CONST(0x1000003e8), 0);
	pin = truncate_key(base + UINT64CONST(0xffe00064), true);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(truncate_directory(), &pin), CLUSTER_RELMAP_RETENTION_OK);
	for (unsigned i = 0xffc; i <= 0xfff; i++) truncate_file(i, true);
	truncate_file(0, true);
	UT_ASSERT(truncate_call(base + UINT64CONST(0x100000064)));
	UT_ASSERT(access("pg_xact/0FFC", F_OK) != 0 && errno == ENOENT);
	UT_ASSERT(access("pg_xact/0FFD", F_OK) != 0 && errno == ENOENT);
	truncate_file(0xffe, false);
	truncate_file(0xfff, false);
	truncate_file(0, false);
	UT_ASSERT_EQ(truncate_wal.oldestXact, 0xffe00064U);
	UT_ASSERT_EQ(truncate_directory()->native_floor, base + UINT64CONST(0xffe00064));
	UT_ASSERT_EQ(deleted, 2);
	fixture_idle();
}

UT_TEST(clean_noop_and_unpinned_cut_complete_without_pin_release)
{
	truncate_reset(3, 4194404, 0);
	UT_ASSERT(truncate_call(3145828));
	UT_ASSERT_EQ(truncate_inserts, 0);
	UT_ASSERT_EQ(native_wal_flushes, 0);
	UT_ASSERT_EQ(truncate_variables.oldestClogXid, 3);
	UT_ASSERT_EQ(truncate_directory()->native_floor, 3145828);
	UT_ASSERT_EQ(truncate_directory()->inflight.serial, 0);
	UT_ASSERT_EQ(truncate_directory()->state, CLUSTER_RELMAP_RETENTION_COMPLETE);
	truncate_reset(3, 4194404, 0);
	for (unsigned i = 0; i < 4; i++) truncate_file(i, true);
	UT_ASSERT(truncate_call(3145828));
	UT_ASSERT_EQ(deleted, 3);
	truncate_file(3, false);
	UT_ASSERT_EQ(truncate_wal.oldestXact, 3145828);
	UT_ASSERT_EQ(truncate_directory()->inflight.serial, 0);
	fixture_idle();
}

UT_TEST(missing_original_vacuum_exclusion_refuses_without_effect)
{
	ClusterRelmapRetentionDirectory before;

	truncate_reset(3, 4194404, 0);
	truncate_file(0, true);
	before = *truncate_directory();
	UT_ASSERT(!cluster_relmap_retention_truncate_clog_v2(FullTransactionIdFromU64(3145828),
		42, 11, 12, 0));
	UT_ASSERT_EQ(memcmp(&before, truncate_directory(), sizeof(before)), 0);
	UT_ASSERT_EQ(truncate_variables.oldestClogXid, 3);
	UT_ASSERT_EQ(truncate_inserts, 0);
	truncate_file(0, false);
	fixture_idle();
}

int
main(void)
{
	char temporary[] = "/tmp/pgrac-clog-retention.XXXXXX";
	int original_cwd = open(".", O_RDONLY | O_DIRECTORY);

	if (original_cwd < 0 || !mkdtemp(temporary) || chdir(temporary) != 0 || mkdir("pg_xact", 0700) != 0)
		return 2;
	native_fixture = true;
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(WrapLimitsVacuumLock, LWTRANCHE_XACT_BUFFER);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
	SimpleLruInit(XactCtl, "Original retained CLOG", 2, CLOG_LSNS_PER_PAGE, XactSLRULock,
		"pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_CLOG);
	relmap_runtime_shmem = calloc(1, sizeof(*relmap_runtime_shmem));
	if (relmap_runtime_shmem == NULL) return 2;
	LWLockInitialize(&relmap_runtime_shmem->lock, 1);
#ifdef CLUSTER_CLOG_RETENTION_CUT_ONLY
	UT_PLAN(1);
#else
	UT_PLAN(8);
#endif
	UT_RUN(original_truncate_uses_all_namespace_minimum_for_files_floor_and_wal);
#ifdef CLUSTER_CLOG_RETENTION_CUT_ONLY
	if (false) {
#else
	{
#endif
	UT_RUN(reservation_excludes_late_older_begin_and_concurrent_truncate);
	UT_RUN(normal_return_with_undeleted_files_retains_unknown_reservation);
	UT_RUN(native_error_or_late_invalidation_preserves_pins_and_no_retry);
	UT_RUN(invalid_native_context_refuses_before_reservation_or_io);
	UT_RUN(original_raw_wrap_comparison_keeps_full_xid_pin_segment);
	UT_RUN(clean_noop_and_unpinned_cut_complete_without_pin_release);
	UT_RUN(missing_original_vacuum_exclusion_refuses_without_effect);
	}
	truncate_reset(3, 4194404, 0);
	UT_ASSERT_EQ(rmdir("pg_xact"), 0);
	UT_ASSERT_EQ(fchdir(original_cwd), 0);
	UT_ASSERT_EQ(close(original_cwd), 0);
	UT_ASSERT_EQ(rmdir(temporary), 0);
	free(relmap_runtime_shmem);
	for (unsigned i = 0; i < allocation_count; i++) free(allocations[i]);
	UT_DONE();
	return ut_failed_count != 0;
}
