/*-------------------------------------------------------------------------
 * test_cluster_multixact_limits_io.c
 *    Local MultiXact retention through native SLRU files and WAL replay.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_multixact_limits_io.c
 * NOTES
 *    PGRAC test; only shmem, locks, original authority and fault delivery are
 *    fixtures. Native SLRU lookup, read, write, delete and replay bodies execute.
 *    This is not a cluster fencing or native startup qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "postmaster/bgwriter.h"
#include "postmaster/checkpointer.h"
#include <dirent.h>
#include <sys/wait.h>
#ifdef MULTIXACT_TEST_SOURCE
#include MULTIXACT_TEST_SOURCE
#else
#include "../../backend/access/transam/multixact.c"
#endif
#include "../../backend/cluster/cluster_mxid_stripe.c"
#include "../../backend/cluster/cluster_xid_authority.c"
#include "../../backend/access/transam/slru.c"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true;
int cluster_node_id = 0;
char *cluster_shared_data_dir;
bool InRecovery;
bool cluster_enabled = true;
bool cluster_xid_striping = true;
int autovacuum_multixact_freeze_max_age = 400000000;
static LWLockPadded local_locks[128];
LWLockPadded *MainLWLockArray = local_locks;
static MultiXactStateData local_state;
static PGPROC local_proc;
PGPROC *MyProc = &local_proc;
static xl_multixact_truncate captured_wal;
static unsigned wal_records, wal_flushes;
static struct {
	uint8 info;
	uint32 length;
	char data[SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember)];
} allocation_wal[16];
static char insert_data[SizeOfMultiXactTruncate + 2 * sizeof(MultiXactMember)];
static uint32 insert_length;
static bool capture_allocation;
static TransactionId advanced_xid;
static int error_level;
static bool empty_input;
AuxProcType MyAuxProcType = StartupProcess;
bool IsUnderPostmaster;
BackendType MyBackendType;
volatile uint32 CritSectionCount;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
int pg_file_create_mode = 0600;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
CheckpointStatsData CheckpointStats;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static char fixture_root[MAXPGPATH];
static bool expect_error, fail_open, fail_sync, fail_dirsync;
static int transient_fds;
static unsigned writes, syncs, dirsyncs, reads;
static struct {
	LWLock *lock;
	bool held;
} fixture_locks[6000];
static unsigned fixture_lock_count;
static void *allocations[300];
static unsigned allocation_count;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}
bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	error_level = level;
	if (expect_error && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	abort();
}
void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int code pg_attribute_unused())
{
	return 0;
}
int
errmsg_plural(const char *s pg_attribute_unused(), const char *p pg_attribute_unused(),
			  unsigned long n pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail_plural(const char *s pg_attribute_unused(), const char *p pg_attribute_unused(),
				 unsigned long n pg_attribute_unused(), ...)
{
	return 0;
}
bool
cluster_cr_injection_armed(const char *name pg_attribute_unused(),
						   uint64 *param pg_attribute_unused())
{
	return false;
}
ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op pg_attribute_unused(),
								  const ClusterMultiXactSourceRequest *r pg_attribute_unused(),
								  ClusterMultiXactSourceResult *out pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
data_sync_elevel(int level)
{
	return level;
}
void
AbsorbSyncRequests(void)
{
	abort();
}
void
ProcessInterrupts(void)
{
	abort();
}

void
cluster_mxid_stripe_lazy_latch(void)
{}
bool ClusterXLogStartupNativeEmptyMultiXactInput(void);
bool
ClusterXLogStartupNativeEmptyMultiXactInput(void)
{
	return empty_input;
}
void
cluster_xid_stripe_observe(ClusterXidStripeObs *out)
{
	memset(out, 0, sizeof(*out));
	out->disk_state = CLUSTER_XID_STRIPE_DISK_ABSENT;
	out->mxid_disk_state = CLUSTER_MXID_DISK_ABSENT;
}

bool
RecoveryInProgress(void)
{
	return InRecovery;
}
bool
IsTransactionState(void)
{
	return false;
}
char *
get_database_name(Oid id pg_attribute_unused())
{
	return NULL;
}
void
SendPostmasterSignal(PMSignalReason reason pg_attribute_unused())
{}
void
AdvanceNextFullTransactionIdPastXid(TransactionId xid)
{
	advanced_xid = xid;
}
bool
TransactionIdPrecedes(TransactionId a, TransactionId b)
{
	return (int32)(a - b) < 0;
}
bool
TransactionIdFollowsOrEquals(TransactionId a, TransactionId b)
{
	return (int32)(a - b) >= 0;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	void *p = calloc(1, size);
	if (p == NULL || allocation_count >= lengthof(allocations))
		abort();
	allocations[allocation_count++] = p;
	*found = false;
	return p;
}
void
LWLockInitialize(LWLock *lock, int tranche pg_attribute_unused())
{
	if (fixture_lock_count >= lengthof(fixture_locks))
		abort();
	fixture_locks[fixture_lock_count++].lock = lock;
}
static unsigned
fixture_lock_index(LWLock *lock)
{
	for (unsigned i = 0; i < fixture_lock_count; i++)
		if (fixture_locks[i].lock == lock)
			return i;
	abort();
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	unsigned i = fixture_lock_index(lock);
	UT_ASSERT(!fixture_locks[i].held);
	fixture_locks[i].held = true;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	unsigned i = fixture_lock_index(lock);
	UT_ASSERT(fixture_locks[i].held);
	fixture_locks[i].held = false;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return fixture_locks[fixture_lock_index(lock)].held;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return LWLockAcquire(lock, mode);
}

int
pgstat_get_slru_index(const char *name pg_attribute_unused())
{
	return 0;
}
void
pgstat_count_slru_page_zeroed(int i pg_attribute_unused())
{}
void
pgstat_count_slru_page_hit(int i pg_attribute_unused())
{}
void
pgstat_count_slru_page_read(int i pg_attribute_unused())
{
	reads++;
}
void
pgstat_count_slru_page_exists(int i pg_attribute_unused())
{}
DIR *
AllocateDir(const char *path)
{
	return opendir(path);
}
struct dirent *
ReadDir(DIR *dir, const char *path pg_attribute_unused())
{
	return readdir(dir);
}
int
FreeDir(DIR *dir)
{
	return closedir(dir);
}
void
pgstat_count_slru_page_written(int i pg_attribute_unused())
{
	writes++;
}
void
pgstat_count_slru_flush(int i pg_attribute_unused())
{}
void
pgstat_count_slru_truncate(int i pg_attribute_unused())
{}
void
XLogFlush(XLogRecPtr lsn pg_attribute_unused())
{
	wal_flushes++;
}
void
XLogBeginInsert(void)
{
	insert_length = 0;
}
void
XLogRegisterData(char *data, uint32 len)
{
	UT_ASSERT(insert_length + len <= sizeof(insert_data));
	if (insert_length + len <= sizeof(insert_data)) {
		memcpy(insert_data + insert_length, data, len);
		insert_length += len;
	}
}
XLogRecPtr
XLogInsert(RmgrId id, uint8 info)
{
	UT_ASSERT_EQ(id, RM_MULTIXACT_ID);
	if (capture_allocation) {
		UT_ASSERT(wal_records < lengthof(allocation_wal));
		UT_ASSERT(insert_length <= sizeof(allocation_wal[0].data));
		if (wal_records < lengthof(allocation_wal)
			&& insert_length <= sizeof(allocation_wal[0].data)) {
			allocation_wal[wal_records].info = info;
			allocation_wal[wal_records].length = insert_length;
			memcpy(allocation_wal[wal_records].data, insert_data, insert_length);
		}
	} else {
		UT_ASSERT_EQ(info, XLOG_MULTIXACT_TRUNCATE_ID);
		UT_ASSERT_EQ(insert_length, SizeOfMultiXactTruncate);
		memcpy(&captured_wal, insert_data, SizeOfMultiXactTruncate);
	}
	wal_records++;
	return 100;
}
bool
RegisterSyncRequest(const FileTag *tag pg_attribute_unused(), SyncRequestType type,
					bool retry pg_attribute_unused())
{
	UT_ASSERT(type == SYNC_REQUEST || type == SYNC_FORGET_REQUEST);
	return true;
}

int
OpenTransientFile(const char *path, int flags)
{
	int fd;
	if (fail_open && (flags & O_RDWR)) {
		errno = EIO;
		return -1;
	}
	fd = open(path, flags, pg_file_create_mode);
	if (fd >= 0)
		transient_fds++;
	return fd;
}
int
CloseTransientFile(int fd)
{
	transient_fds--;
	return close(fd);
}
int
pg_fsync(int fd)
{
	syncs++;
	if (fail_sync) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
void
fsync_fname(const char *path, bool isdir)
{
	int fd, rc;
	UT_ASSERT(isdir);
	dirsyncs++;
	if (fail_dirsync) {
		errno = EIO;
		(void)errstart(ERROR, NULL);
	}
	fd = open(path, O_RDONLY | O_DIRECTORY);
	if (fd < 0)
		abort();
	rc = fsync(fd);
	if (close(fd) != 0)
		rc = -1;
	UT_ASSERT_EQ(rc, 0);
}

static void
local_discard_cache(void)
{
	for (int family = 0; family < 2; family++) {
		SlruShared s = (family ? MultiXactMemberCtl : MultiXactOffsetCtl)->shared;
		for (int i = 0; i < s->num_slots; i++) {
			s->page_status[i] = SLRU_PAGE_EMPTY;
			s->page_dirty[i] = false;
		}
	}
}
static void
local_reset(void)
{
	char path[MAXPGPATH];

	empty_input = false;
	for (int family = 0; family < 2; family++) {
		SlruCtl ctl = family ? MultiXactMemberCtl : MultiXactOffsetCtl;
		DIR *dir = opendir(ctl->Dir);
		struct dirent *entry;
		UT_ASSERT(dir != NULL);
		while ((entry = readdir(dir)) != NULL) {
			if (entry->d_name[0] == '.')
				continue;
			snprintf(path, sizeof(path), "%s/%s", ctl->Dir, entry->d_name);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		UT_ASSERT_EQ(closedir(dir), 0);
	}
	local_discard_cache();
	memset(&local_state, 0, sizeof(local_state));
	memset(&local_proc, 0, sizeof(local_proc));
	MultiXactState = &local_state;
	local_state.oldestMultiXactId = 33;
	local_state.nextMXact = 100;
	local_state.nextOffset = 900000;
	local_state.finishedStartup = true;
	local_state.oldestOffsetKnown = true;
	local_state.oldestOffset = 99;
	local_state.offsetStopLimit = UINT32_MAX - 1000000;
	local_state.oldestMultiXactDB = 5;
	cluster_node_id = 0;
	cluster_mxid_stripe_latch_runtime(true, 0, 32);
	for (unsigned i = 0; i < fixture_lock_count; i++)
		fixture_locks[i].held = false;
	CritSectionCount = 0;
	InRecovery = false;
	expect_error = fail_open = fail_sync = fail_dirsync = false;
	writes = reads = syncs = dirsyncs = wal_records = wal_flushes = 0;
	capture_allocation = false;
	advanced_xid = InvalidTransactionId;
}
static void
local_page(SlruCtl ctl, int page, const char *data, size_t length)
{
	char path[MAXPGPATH];
	int fd;
	SlruFileName(ctl, path, page / SLRU_PAGES_PER_SEGMENT);
	fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, data, length, (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ), length);
	UT_ASSERT_EQ(close(fd), 0);
}
static void
local_offsets(MultiXactOffset from, MultiXactOffset to)
{
	PGAlignedBlock page = { 0 };
	((MultiXactOffset *)page.data)[48] = from;
	((MultiXactOffset *)page.data)[80] = to;
	local_page(MultiXactOffsetCtl, 0, page.data, BLCKSZ);
}
static bool
local_limit_refuses(bool startup)
{
	bool refused = false;
	expect_error = true;
	PG_TRY();
	{
		(void)SetOffsetVacuumLimit(startup);
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	expect_error = false;
	return refused;
}
static bool
local_segment_exists(SlruCtl ctl, int segno)
{
	char path[MAXPGPATH];
	SlruFileName(ctl, path, segno);
	return access(path, F_OK) == 0;
}

/* Native allocation/recording execute; CREATE WAL below is a layout fixture,
 * not a claim that MultiXactIdCreateFromMembers or WAL durability executed. */
static bool
local_allocate_and_record(MultiXactId *id, MultiXactMember *members)
{
	MultiXactOffset offset = 0;
	bool refused = false;
	expect_error = true;
	PG_TRY();
	{
		xl_multixact_create create;
		*id = GetNewMultiXactId(2, &offset);
		RecordNewMultiXact(*id, offset, 2, members);
		END_CRIT_SECTION();
		create.mid = *id;
		create.moff = offset;
		create.nmembers = 2;
		XLogBeginInsert();
		XLogRegisterData((char *)&create, SizeOfMultiXactCreate);
		XLogRegisterData((char *)members, 2 * sizeof(*members));
		(void)XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID);
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	expect_error = false;
	return !refused;
}

UT_TEST(physical_catchup_keeps_old_pages_and_replays_native_offsets_and_members)
{
	MultiXactMember members[2]
		= {{100, MultiXactStatusForKeyShare}, {116, MultiXactStatusForShare}};
	for (int wrap = 0; wrap < 2; wrap++) {
		PGAlignedBlock old_page, member_page = {0}, expected_offsets, expected_members;
		MultiXactId id = 0;
		int old_pageno = wrap ? MultiXactIdToOffsetPage(MaxMultiXactId) : 0;
		int new_pageno = wrap ? 0 : 128;
		int slot;
		unsigned saved_count;
		local_reset();
		local_state.nextMXact = wrap ? UINT32_MAX - 31 : 33;
		local_state.oldestMultiXactId = wrap ? 17 : 262145;
		local_state.nextOffset = 500;
		local_state.multiVacLimit = local_state.oldestMultiXactId + 1000;
		local_state.multiWarnLimit = local_state.oldestMultiXactId + 2000;
		cluster_mxid_stripe_latch_runtime(true, 0, wrap ? UINT32_MAX - 63 : 32);
		memset(old_page.data, 0x72, BLCKSZ);
		local_page(MultiXactOffsetCtl, old_pageno, old_page.data, BLCKSZ);
		local_page(MultiXactMemberCtl, 0, member_page.data, BLCKSZ);
		capture_allocation = true;
		if (!local_allocate_and_record(&id, members)) {
			UT_ASSERT(false);
			continue;
		}
		UT_ASSERT_EQ(id, wrap ? 32 : 262160);
		UT_ASSERT_EQ(wal_records, 2); /* actual ZERO then fixture CREATE */
		UT_ASSERT_EQ(allocation_wal[0].info, XLOG_MULTIXACT_ZERO_OFF_PAGE);
		{
			int zero_page;
			memcpy(&zero_page, allocation_wal[0].data, sizeof(zero_page));
			UT_ASSERT_EQ(zero_page, new_pageno);
		}
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, wrap ? 48 : 262176);
		UT_ASSERT_EQ(wal_records, 3); /* no second ZERO of occupied page */
		SimpleLruWriteAll(MultiXactOffsetCtl, true);
		SimpleLruWriteAll(MultiXactMemberCtl, true);
		local_discard_cache();
		slot = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl, old_pageno, 0);
		UT_ASSERT_EQ(memcmp(old_page.data, MultiXactOffsetCtl->shared->page_buffer[slot], BLCKSZ), 0);
		LWLockRelease(MultiXactOffsetSLRULock);
		slot = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl, new_pageno, 0);
		memcpy(expected_offsets.data, MultiXactOffsetCtl->shared->page_buffer[slot], BLCKSZ);
		UT_ASSERT_EQ(((MultiXactOffset *)expected_offsets.data)[wrap ? 32 : 16], 500);
		UT_ASSERT_EQ(((MultiXactOffset *)expected_offsets.data)[wrap ? 48 : 32], 502);
		LWLockRelease(MultiXactOffsetSLRULock);
		slot = SimpleLruReadPage_ReadOnly(MultiXactMemberCtl, 0, 0);
		memcpy(expected_members.data, MultiXactMemberCtl->shared->page_buffer[slot], BLCKSZ);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(500)), 100);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(501)), 116);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(502)), 100);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(503)), 116);
		LWLockRelease(MultiXactMemberSLRULock);
		if (!wrap)
			for (int page = 1; page < 4; page++)
				UT_ASSERT(!SimpleLruDoesPhysicalPageExist(MultiXactOffsetCtl, page));
		UT_ASSERT(wal_records <= lengthof(allocation_wal));
		if (wal_records > lengthof(allocation_wal))
			continue;
		saved_count = wal_records;
		/* Restore the pre-allocation pages and discard cache before real redo. */
		local_page(MultiXactOffsetCtl, new_pageno, member_page.data, BLCKSZ);
		local_page(MultiXactMemberCtl, 0, member_page.data, BLCKSZ);
		local_discard_cache();
		InRecovery = true;
		for (unsigned i = 0; i < saved_count; i++) {
			DecodedXLogRecord decoded = {0};
			XLogReaderState reader = {0};
			decoded.header.xl_info = allocation_wal[i].info;
			decoded.max_block_id = -1;
			decoded.main_data = allocation_wal[i].data;
			decoded.main_data_len = allocation_wal[i].length;
			reader.record = &decoded;
			multixact_redo(&reader);
		}
		SimpleLruWriteAll(MultiXactOffsetCtl, true);
		SimpleLruWriteAll(MultiXactMemberCtl, true);
		local_discard_cache();
		slot = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl, new_pageno, 0);
		UT_ASSERT_EQ(memcmp(expected_offsets.data, MultiXactOffsetCtl->shared->page_buffer[slot], BLCKSZ), 0);
		LWLockRelease(MultiXactOffsetSLRULock);
		slot = SimpleLruReadPage_ReadOnly(MultiXactMemberCtl, 0, 0);
		UT_ASSERT_EQ(memcmp(expected_members.data, MultiXactMemberCtl->shared->page_buffer[slot], BLCKSZ), 0);
		LWLockRelease(MultiXactMemberSLRULock);
		slot = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl, old_pageno, 0);
		UT_ASSERT_EQ(memcmp(old_page.data, MultiXactOffsetCtl->shared->page_buffer[slot], BLCKSZ), 0);
		LWLockRelease(MultiXactOffsetSLRULock);
		UT_ASSERT_EQ(advanced_xid, 116);
		UT_ASSERT_EQ(transient_fds, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(physical_exact_page_missing_short_and_zero_keep_original_refusal)
{
	for (int fault = 0; fault < 3; fault++) {
		PGAlignedBlock page = { 0 };
		local_reset();
		/* A later nonzero entry cannot stand in for the exact local one. */
		((MultiXactOffset *)page.data)[64] = 300;
		if (fault != 0)
			local_page(MultiXactOffsetCtl, 0, page.data, fault == 1 ? 73 : BLCKSZ);
		UT_ASSERT(local_limit_refuses(true));
		UT_ASSERT_EQ(local_state.oldestOffset, 99);
		UT_ASSERT_EQ(writes + wal_records, 0);
		UT_ASSERT_EQ(transient_fds, 0);
	}
}

UT_TEST(physical_runtime_limit_preserves_live_member_page)
{
	PGAlignedBlock page, after;
	local_reset();
	memset(page.data, 0x61, BLCKSZ);
	local_page(MultiXactMemberCtl, 1, page.data, BLCKSZ);
	local_offsets(MULTIXACT_MEMBERS_PER_PAGE + 5, 9000);
	SetMultiXactIdLimit(37, 5, false);
	UT_ASSERT_EQ(local_state.oldestMultiXactId, 37);
	UT_ASSERT_EQ(local_state.oldestOffset, MULTIXACT_MEMBERS_PER_PAGE + 5);
	/* Read back the retained physical page, independent of the SLRU cache. */
	{
		char path[MAXPGPATH];
		int fd;
		SlruFileName(MultiXactMemberCtl, path, 0);
		fd = open(path, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, after.data, BLCKSZ, BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
	}
	UT_ASSERT_EQ(memcmp(page.data, after.data, BLCKSZ), 0);
	UT_ASSERT_EQ(wal_records, 0);
}

UT_TEST(physical_member_deletion_and_actual_redo_preserve_live_segments)
{
	const uint32 span = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	const int last = MXOffsetToMemberSegment(MaxMultiXactOffset);
	const int segments[] = { 0, 1, 2, 3, 4, last };
	PGAlignedBlock page;
	for (int wrap = 0; wrap < 2; wrap++) {
		xl_multixact_truncate saved;
		for (int replay = 0; replay < 2; replay++) {
			local_reset();
			memset(page.data, 0x72, BLCKSZ);
			for (unsigned i = 0; i < lengthof(segments); i++)
				local_page(MultiXactMemberCtl, segments[i] * SLRU_PAGES_PER_SEGMENT, page.data,
						   BLCKSZ);
			local_offsets(wrap ? UINT32_MAX - 100 : span, span * 3 + 5);
			if (!replay) {
				TruncateMultiXact(65, 5);
				UT_ASSERT_EQ(wal_records, 1);
				if (wal_records != 1)
					return;
				UT_ASSERT_EQ(wal_flushes, 1);
				UT_ASSERT_EQ(captured_wal.startTruncOff, 33);
				UT_ASSERT_EQ(captured_wal.endTruncOff, 65);
				UT_ASSERT_EQ(captured_wal.startTruncMemb, wrap ? UINT32_MAX - 100 : span);
				UT_ASSERT_EQ(captured_wal.endTruncMemb, span * 3 + 5);
				saved = captured_wal;
			} else {
				DecodedXLogRecord decoded = { 0 };
				XLogReaderState reader = { 0 };
				decoded.header.xl_info = XLOG_MULTIXACT_TRUNCATE_ID;
				decoded.max_block_id = -1;
				decoded.main_data = (char *)&saved;
				decoded.main_data_len = SizeOfMultiXactTruncate;
				reader.record = &decoded;
				InRecovery = true;
				local_state.finishedStartup = false;
				multixact_redo(&reader);
				UT_ASSERT_EQ(wal_records, 0);
			}
			for (unsigned i = 0; i < lengthof(segments); i++) {
				bool removed = segments[i] == 1 || segments[i] == 2
							   || (wrap && (segments[i] == 0 || segments[i] == last));
				UT_ASSERT_EQ(local_segment_exists(MultiXactMemberCtl, segments[i]), !removed);
			}
			UT_ASSERT_EQ(local_state.oldestMultiXactId, 65);
			UT_ASSERT_EQ(local_proc.delayChkptFlags, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			UT_ASSERT_EQ(transient_fds, 0);
		}
	}
}

UT_TEST(physical_unknown_old_endpoint_never_deletes_live_members)
{
	PGAlignedBlock page;
	const uint32 span = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	local_reset();
	local_state.oldestMultiXactId = 48;
	memset(page.data, 0x52, BLCKSZ);
	local_page(MultiXactMemberCtl, 0, page.data, BLCKSZ);
	local_page(MultiXactMemberCtl, SLRU_PAGES_PER_SEGMENT, page.data, BLCKSZ);
	local_offsets(0, span * 3);
	TruncateMultiXact(80, 5);
	UT_ASSERT_EQ(wal_records, 0);
	UT_ASSERT_EQ(local_state.oldestMultiXactId, 48);
	UT_ASSERT(local_segment_exists(MultiXactMemberCtl, 0));
	UT_ASSERT(local_segment_exists(MultiXactMemberCtl, 1));
}

UT_TEST(original_authority_reader_qualifies_native_prefix_without_writes)
{
	char dir[MAXPGPATH], path[MAXPGPATH];
	snprintf(dir, sizeof(dir), "%s/global", fixture_root);
	snprintf(path, sizeof(path), "%s/%s", fixture_root, CLUSTER_XID_AUTHORITY_REL_PATH);
	UT_ASSERT_EQ(mkdir(dir, 0700), 0);
	for (int fault = 0; fault < 7; fault++) {
		ClusterXidAuthorityHeader header = { 0 }, after;
		PGAlignedBlock page = { 0 };
		int fd;
		size_t length = fault == 6 ? 17 : sizeof(header);
		local_reset();
		local_state.oldestMultiXactId = FirstMultiXactId;
		header.magic = CLUSTER_XID_AUTHORITY_MAGIC;
		header.version = CLUSTER_XID_AUTHORITY_VERSION;
		header.flags = CLUSTER_XID_AUTHORITY_FLAG_SEALED;
		header.next_multi = FirstMultiXactId;
		if (fault == 1)
			header.next_multi = 2;
		else if (fault == 2)
			header.flags = 0;
		else if (fault == 3)
			header.version++;
		else if (fault == 4)
			header.magic = 0;
		INIT_CRC32C(header.crc);
		COMP_CRC32C(header.crc, &header, offsetof(ClusterXidAuthorityHeader, crc));
		FIN_CRC32C(header.crc);
		if (fault == 5)
			header.crc ^= 1;
		fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(write(fd, &header, length), length);
		UT_ASSERT_EQ(close(fd), 0);
		((MultiXactOffset *)page.data)[32] = 17;
		local_page(MultiXactOffsetCtl, 0, page.data, BLCKSZ);
		UT_ASSERT_EQ(local_limit_refuses(true), fault != 0);
		UT_ASSERT_EQ(local_state.oldestOffset, fault ? 99 : 17);
		UT_ASSERT_EQ(writes + wal_records, 0);
		fd = open(path, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(read(fd, &after, sizeof(after)), length);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(memcmp(&header, &after, length), 0);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(rmdir(dir), 0);
}

UT_TEST(native_empty_boot_reads_real_authority_and_original_backup)
{
	char dir[MAXPGPATH], path[MAXPGPATH], backup[MAXPGPATH];
	snprintf(dir, sizeof(dir), "%s/global", fixture_root);
	snprintf(path, sizeof(path), "%s/%s", fixture_root, CLUSTER_XID_AUTHORITY_REL_PATH);
	snprintf(backup, sizeof(backup), "%s.bak", path);
	UT_ASSERT_EQ(mkdir(dir, 0700), 0);
	for (int fault = 0; fault < 11; fault++) {
		ClusterXidAuthorityHeader header = { 0 }, good, after;
		int fd;
		size_t length = fault == 2 ? 17 : sizeof(header);
		local_reset();
		empty_input = true;
		cluster_mxid_stripe_latch_runtime(false, 0, 0);
		local_state.oldestMultiXactId = local_state.nextMXact = FirstMultiXactId;
		local_state.nextOffset = 0;
		local_state.oldestOffsetKnown = false;
		header.magic = CLUSTER_XID_AUTHORITY_MAGIC;
		header.version = CLUSTER_XID_AUTHORITY_VERSION;
		header.flags = CLUSTER_XID_AUTHORITY_FLAG_SEALED | CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
		header.next_multi = FirstMultiXactId;
		INIT_CRC32C(header.crc);
		COMP_CRC32C(header.crc, &header, offsetof(ClusterXidAuthorityHeader, crc));
		FIN_CRC32C(header.crc);
		good = header;
		if (fault == 3)
			header.version++;
		if (fault == 4)
			header.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_SEALED;
		if (fault == 5)
			header.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
		if (fault == 6)
			header.reserved++;
		if (fault == 7)
			header.magic = CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED;
		if (fault == 8)
			header.next_multi++;
		INIT_CRC32C(header.crc);
		COMP_CRC32C(header.crc, &header, offsetof(ClusterXidAuthorityHeader, crc));
		FIN_CRC32C(header.crc);
		if (fault == 1 || fault >= 9)
			header.crc ^= 1;
		fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(write(fd, &header, length), length);
		UT_ASSERT_EQ(close(fd), 0);
		if (fault >= 9) {
			if (fault == 10)
				good.crc ^= 1;
			fd = open(backup, O_RDWR | O_CREAT | O_TRUNC, 0600);
			UT_ASSERT(fd >= 0);
			UT_ASSERT_EQ(write(fd, &good, sizeof(good)), sizeof(good));
			UT_ASSERT_EQ(close(fd), 0);
		}
		UT_ASSERT_EQ(local_limit_refuses(true), fault != 0 && fault != 9);
		UT_ASSERT_EQ(local_state.oldestOffsetKnown, fault == 0 || fault == 9);
		if (local_state.oldestOffsetKnown) {
			UT_ASSERT_EQ(local_state.oldestOffset, 0);
			UT_ASSERT_EQ(
				local_state.offsetStopLimit,
				(MultiXactOffset)(0 - MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT));
		}
		UT_ASSERT_EQ(writes + syncs + wal_records, 0);
		fd = open(path, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(read(fd, &after, sizeof(after)), length);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(memcmp(&header, &after, length), 0);
		UT_ASSERT_EQ(unlink(path), 0);
		if (fault >= 9)
			UT_ASSERT_EQ(unlink(backup), 0);
	}
	UT_ASSERT_EQ(rmdir(dir), 0);
}

int
main(void)
{
	char path[MAXPGPATH];
	strlcpy(fixture_root, "/tmp/pgrac-mx-limits.XXXXXX", sizeof(fixture_root));
	if (!mkdtemp(fixture_root))
		return 2;
	cluster_shared_data_dir = fixture_root;
	LWLockInitialize(MultiXactGenLock, 0);
	LWLockInitialize(MultiXactTruncationLock, 0);
	for (int family = 0; family < 2; family++) {
		SlruCtl ctl = family ? MultiXactMemberCtl : MultiXactOffsetCtl;
		LWLock *lock = family ? MultiXactMemberSLRULock : MultiXactOffsetSLRULock;
		snprintf(path, sizeof(path), "%s/%s", fixture_root, family ? "members" : "offsets");
		if (mkdir(path, 0700) != 0)
			return 2;
		LWLockInitialize(lock, 0);
		ctl->PagePrecedes = family ? MultiXactMemberPagePrecedes : MultiXactOffsetPagePrecedes;
		SimpleLruInit(ctl, family ? "members" : "offsets", 4, 0, lock, path,
					  LWTRANCHE_MULTIXACTOFFSET_BUFFER, SYNC_HANDLER_NONE);
	}
	UT_PLAN(7);
	UT_RUN(native_empty_boot_reads_real_authority_and_original_backup);
	UT_RUN(physical_exact_page_missing_short_and_zero_keep_original_refusal);
	UT_RUN(physical_runtime_limit_preserves_live_member_page);
	UT_RUN(physical_member_deletion_and_actual_redo_preserve_live_segments);
	UT_RUN(physical_unknown_old_endpoint_never_deletes_live_members);
	UT_RUN(original_authority_reader_qualifies_native_prefix_without_writes);
	UT_RUN(physical_catchup_keeps_old_pages_and_replays_native_offsets_and_members);
	local_reset();
	UT_ASSERT_EQ(rmdir(MultiXactOffsetCtl->Dir), 0);
	UT_ASSERT_EQ(rmdir(MultiXactMemberCtl->Dir), 0);
	UT_ASSERT_EQ(rmdir(fixture_root), 0);
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	UT_DONE();
	return ut_failed_count != 0;
}
