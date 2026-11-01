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
 *    fixtures. Native creation, cache, member readback, SLRU and redo execute;
 *    checkpoint cursors, Startup/Trim and continuation execute too; catalog,
 *    pg_control, process lifetime and WAL transport remain fixture boundaries.
 *    This is not a cluster fencing or native startup qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "postmaster/bgwriter.h"
#include "postmaster/checkpointer.h"
#include <dirent.h>
#include <sys/wait.h>
#ifdef USE_CLUSTER_UNIT
#error "This fixture requires the native MultiXact persistence body"
#endif
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
int MaxBackends = 1;
int max_prepared_xacts = 0;
BackendId MyBackendId = 1;
static MultiXactId local_oldest_member[2], local_oldest_visible[2];
static ClusterConf local_topology;
ClusterConf *ClusterConfShmem = &local_topology;
static char context_marker;
MemoryContext TopTransactionContext = (MemoryContext)&context_marker;
static void *transaction_allocations[64];
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
bool IsBinaryUpgrade;
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
static void (*warning_unlock_hook)(void);
static MultiXactId warning_next, warning_lower;
static bool warning_known, warning_member_stop;
static unsigned warning_unlocks, warning_writes, warning_wal;
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

/* Only allocation/context lifetime is doubled; native cache lookup, insertion,
 * sorting and AtEOXact reset still run. Unexpected allocation scope fails. */
void *
palloc(Size size)
{
	for (unsigned i = 0; i < lengthof(transaction_allocations); i++) {
		if (transaction_allocations[i] == NULL) {
			void *pointer = malloc(size);
			if (pointer == NULL)
				abort();
			transaction_allocations[i] = pointer;
			return pointer;
		}
	}
	abort();
}
void
pfree(void *pointer)
{
	if (pointer == NULL)
		abort();
	for (unsigned i = 0; i < lengthof(transaction_allocations); i++) {
		if (transaction_allocations[i] == pointer) {
			free(pointer);
			transaction_allocations[i] = NULL;
			return;
		}
	}
	abort();
}
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	if (context != TopTransactionContext)
		abort();
	return palloc(size);
}
MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name pg_attribute_unused(),
							  Size minimum pg_attribute_unused(), Size initial pg_attribute_unused(),
							  Size maximum pg_attribute_unused())
{
	if (parent != TopTransactionContext)
		abort();
	return parent;
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
	if (lock == MultiXactGenLock && warning_unlock_hook != NULL) {
		void (*hook)(void) = warning_unlock_hook;

		warning_unlock_hook = NULL;
		hook();
	}
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
		if (info == XLOG_MULTIXACT_CREATE_ID)
			UT_ASSERT(CritSectionCount > 0);
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

	warning_unlock_hook = NULL;
	warning_unlocks = 0;
	if (MyBackendId < 1 || MyBackendId >= lengthof(local_oldest_member)
		|| MaxOldestSlot >= lengthof(local_oldest_member))
		abort();
	OldestMemberMXactId = local_oldest_member;
	OldestVisibleMXactId = local_oldest_visible;
	AtEOXact_MultiXact();
	for (unsigned i = 0; i < lengthof(transaction_allocations); i++) {
		free(transaction_allocations[i]);
		transaction_allocations[i] = NULL;
	}
	memset(local_oldest_member, 0, sizeof(local_oldest_member));
	memset(local_oldest_visible, 0, sizeof(local_oldest_visible));
	memset(&local_topology, 0, sizeof(local_topology));
	local_topology.node_count = 2;
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
	MyBackendType = B_BACKEND;
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

/* No USE_CLUSTER_UNIT materializer: the original local-current constructor
 * produces CREATE WAL and records members in its original critical section.
 * This is the persistence tail, not heap/CTRC publication or WAL durability. */
static bool
local_allocate_and_record(MultiXactId *id, MultiXactMember *members)
{
	bool refused = false;
	expect_error = true;
	PG_TRY();
	{
		*id = MultiXactIdCreateLocalCurrentMembers(2, members);
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	expect_error = false;
	return !refused;
}

static void
local_check_members(MultiXactId id, MultiXactOffset offset, TransactionId first,
					TransactionId second)
{
	MultiXactMember *members = NULL;
	MultiXactOffset observed_offset = 0;
	int count;

	/* This API must read native pages even while the backend cache is warm. */
	count = GetMultiXactIdMembersWithOffset(id, &members, false, false, &observed_offset);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(observed_offset, offset);
	UT_ASSERT(members != NULL);
	if (members != NULL && count == 2) {
		UT_ASSERT_EQ(members[0].xid, first);
		UT_ASSERT_EQ(members[0].status, MultiXactStatusForKeyShare);
		UT_ASSERT_EQ(members[1].xid, second);
		UT_ASSERT_EQ(members[1].status, MultiXactStatusForShare);
	}
	if (members != NULL)
		pfree(members);
}

UT_TEST(physical_catchup_keeps_old_pages_and_replays_native_offsets_and_members)
{
	static const struct {
		int slot;
		bool wrap;
		MultiXactId first, second, third;
	} cases[] = {{0, false, 262160, 262176, 262192},
				 {1, false, 262145, 262161, 262177},
				 {0, true, 32, 48, 64}, {1, true, 17, 33, 49}};
	for (unsigned c = 0; c < lengthof(cases); c++) {
		MultiXactMember members[2]
			= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
		PGAlignedBlock old_page, member_page = {0}, expected_offsets, expected_members;
		MultiXactId id = 0;
		bool wrap = cases[c].wrap;
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
		cluster_node_id = cases[c].slot;
		cluster_mxid_stripe_latch_runtime(true, cases[c].slot, wrap ? UINT32_MAX - 63 : 32);
		memset(old_page.data, 0x72, BLCKSZ);
		local_page(MultiXactOffsetCtl, old_pageno, old_page.data, BLCKSZ);
		local_page(MultiXactMemberCtl, 0, member_page.data, BLCKSZ);
		capture_allocation = true;
		if (!local_allocate_and_record(&id, members)) {
			UT_ASSERT(false);
			continue;
		}
		UT_ASSERT_EQ(id, cases[c].first);
		UT_ASSERT_EQ(wal_records, 2); /* actual ZERO then actual CREATE */
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(allocation_wal[0].info, XLOG_MULTIXACT_ZERO_OFF_PAGE);
		{
			int zero_page;
			memcpy(&zero_page, allocation_wal[0].data, sizeof(zero_page));
			UT_ASSERT_EQ(zero_page, new_pageno);
		}
		/* Identical members hit the real native cache; a distinct set below
		 * must allocate again, so this shortcut cannot hide the gap path. */
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, cases[c].first);
		UT_ASSERT_EQ(wal_records, 2);
		UT_ASSERT_EQ(local_state.nextMXact, cases[c].first + 1);
		UT_ASSERT_EQ(local_state.nextOffset, 502);
		members[0].xid = 200;
		members[1].xid = 216;
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, cases[c].second);
		UT_ASSERT_EQ(wal_records, 3); /* no second ZERO of occupied page */
		SimpleLruWriteAll(MultiXactOffsetCtl, true);
		SimpleLruWriteAll(MultiXactMemberCtl, true);
		local_discard_cache();
		slot = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl, old_pageno, 0);
		UT_ASSERT_EQ(memcmp(old_page.data, MultiXactOffsetCtl->shared->page_buffer[slot], BLCKSZ), 0);
		LWLockRelease(MultiXactOffsetSLRULock);
		slot = SimpleLruReadPage_ReadOnly(MultiXactOffsetCtl, new_pageno, 0);
		memcpy(expected_offsets.data, MultiXactOffsetCtl->shared->page_buffer[slot], BLCKSZ);
		UT_ASSERT_EQ(((MultiXactOffset *)expected_offsets.data)[MultiXactIdToOffsetEntry(cases[c].first)], 500);
		UT_ASSERT_EQ(((MultiXactOffset *)expected_offsets.data)[MultiXactIdToOffsetEntry(cases[c].second)], 502);
		LWLockRelease(MultiXactOffsetSLRULock);
		slot = SimpleLruReadPage_ReadOnly(MultiXactMemberCtl, 0, 0);
		memcpy(expected_members.data, MultiXactMemberCtl->shared->page_buffer[slot], BLCKSZ);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(500)), 100);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(501)), 116);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(502)), 200);
		UT_ASSERT_EQ(*(TransactionId *)(expected_members.data + MXOffsetToMemberOffset(503)), 216);
		LWLockRelease(MultiXactMemberSLRULock);
		local_check_members(cases[c].first, 500, 100, 116);
		local_check_members(cases[c].second, 502, 200, 216);
		UT_ASSERT(!local_limit_refuses(false));
		UT_ASSERT_EQ(local_state.oldestOffset, 500);
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
		/* Restore only fixture cursors, not a claim of checkpoint startup. */
		local_state.nextMXact = wrap ? UINT32_MAX - 31 : 33;
		local_state.nextOffset = 500;
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
		UT_ASSERT_EQ(local_state.nextMXact, cases[c].second + 1);
		UT_ASSERT_EQ(local_state.nextOffset, 504);
		local_check_members(cases[c].first, 500, 100, 116);
		local_check_members(cases[c].second, 502, 200, 216);
		UT_ASSERT_EQ(advanced_xid, 216);
		InRecovery = false;
		members[0].xid = 300;
		members[1].xid = 316;
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, cases[c].third);
		UT_ASSERT_EQ(wal_records, 4);
		UT_ASSERT_EQ(local_state.nextOffset, 506);
		local_check_members(cases[c].first, 500, 100, 116);
		local_check_members(cases[c].second, 502, 200, 216);
		local_check_members(cases[c].third, 504, 300, 316);
		UT_ASSERT_EQ(transient_fds, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

typedef struct LocalMultiCheckpoint {
	MultiXactId next, oldest;
	MultiXactOffset offset;
	Oid database;
} LocalMultiCheckpoint;

static void
local_read_page(SlruCtl ctl, int page, char *data)
{
	int slot = SimpleLruReadPage_ReadOnly(ctl, page, 0);

	memcpy(data, ctl->shared->page_buffer[slot], BLCKSZ);
	LWLockRelease(ctl->shared->ControlLock);
}

static void
local_restart_cursors(const LocalMultiCheckpoint *checkpoint)
{
	/* This is the checkpoint/catalog input boundary, not a postmaster restart.
	 * Discard both caches and derived limits so the original startup consumer
	 * must requalify the retained physical offsets instead of reusing them. */
	AtEOXact_MultiXact();
	local_discard_cache();
	memset(&local_state, 0, sizeof(local_state));
	local_state.oldestMultiXactId = checkpoint->oldest;
	local_state.oldestMultiXactDB = checkpoint->database;
	MultiXactSetNextMXact(checkpoint->next, checkpoint->offset);
	MyBackendType = B_STARTUP;
	StartupMultiXact();
}

static bool
local_trim_refuses(void)
{
	bool refused = false;

	error_level = 0;
	expect_error = true;
	PG_TRY();
	{
		TrimMultiXact();
	}
	PG_CATCH();
	{
		refused = true;
	}
	PG_END_TRY();
	expect_error = false;
	return refused;
}

UT_TEST(physical_checkpoint_startup_preserves_retained_bytes_and_continues)
{
	static const struct {
		int slot;
		bool wrap;
		MultiXactId first, second, third;
	} cases[] = {{0, false, 262160, 262176, 262192},
				 {1, false, 262145, 262161, 262177},
				 {0, true, 32, 48, 64}, {1, true, 17, 33, 49}};

	for (unsigned c = 0; c < lengthof(cases); c++) {
		MultiXactMember members[2]
			= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
		LocalMultiCheckpoint checkpoint;
		PGAlignedBlock old_page, member_page = {0}, offsets, retained_members, after;
		MultiXactId id = 0;
		bool wrap = cases[c].wrap;
		int old_pageno = wrap ? MultiXactIdToOffsetPage(MaxMultiXactId) : 0;
		int new_pageno = wrap ? 0 : 128;
		int slot;
		unsigned saved_writes, saved_wal;

		local_reset();
		local_state.nextMXact = wrap ? UINT32_MAX - 31 : 33;
		local_state.oldestMultiXactId = wrap ? 17 : 262145;
		local_state.nextOffset = 500;
		local_state.multiVacLimit = local_state.oldestMultiXactId + 1000;
		local_state.multiWarnLimit = local_state.oldestMultiXactId + 2000;
		cluster_node_id = cases[c].slot;
		cluster_mxid_stripe_latch_runtime(true, cases[c].slot, wrap ? UINT32_MAX - 63 : 32);
		memset(old_page.data, 0x72, BLCKSZ);
		member_page.data[BLCKSZ - 1] = 0x6b;
		local_page(MultiXactOffsetCtl, old_pageno, old_page.data, BLCKSZ);
		local_page(MultiXactMemberCtl, 0, member_page.data, BLCKSZ);
		capture_allocation = true;
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, cases[c].first);
		members[0].xid = 200;
		members[1].xid = 216;
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, cases[c].second);
		/* Opaque retained suffix bytes must survive startup. These markers
		 * are not issued IDs, members, or authority to read a foreign ID. */
		LWLockAcquire(MultiXactOffsetSLRULock, LW_EXCLUSIVE);
		slot = SimpleLruReadPage(MultiXactOffsetCtl, new_pageno, true, 0);
		((MultiXactOffset *)MultiXactOffsetCtl->shared->page_buffer[slot])
			[MULTIXACT_OFFSETS_PER_PAGE - 1] = 771;
		MultiXactOffsetCtl->shared->page_dirty[slot] = true;
		LWLockRelease(MultiXactOffsetSLRULock);
		MultiXactGetCheckptMulti(true, &checkpoint.next, &checkpoint.offset,
								&checkpoint.oldest, &checkpoint.database);
		UT_ASSERT_EQ(checkpoint.next, cases[c].second + 1);
		UT_ASSERT_EQ(checkpoint.offset, 504);
		CheckPointMultiXact();
		local_discard_cache();
		local_read_page(MultiXactOffsetCtl, new_pageno, offsets.data);
		local_read_page(MultiXactMemberCtl, 0, retained_members.data);
		saved_writes = writes;
		saved_wal = wal_records;
		local_restart_cursors(&checkpoint);
		UT_ASSERT(!local_state.finishedStartup && !local_state.oldestOffsetKnown);
		UT_ASSERT_EQ(MultiXactOffsetCtl->shared->latest_page_number, new_pageno);
		UT_ASSERT_EQ(MultiXactMemberCtl->shared->latest_page_number, 0);
		UT_ASSERT(!local_trim_refuses());
		UT_ASSERT(local_state.finishedStartup && local_state.oldestOffsetKnown);
		UT_ASSERT_EQ(local_state.oldestOffset, 500);
		UT_ASSERT_EQ(local_state.nextMXact, checkpoint.next);
		UT_ASSERT_EQ(local_state.nextOffset, checkpoint.offset);
		/* A flush before discard exposes even accidental dirty-page trims. */
		CheckPointMultiXact();
		UT_ASSERT_EQ(writes, saved_writes);
		UT_ASSERT_EQ(wal_records, saved_wal);
		local_discard_cache();
		local_read_page(MultiXactOffsetCtl, new_pageno, after.data);
		UT_ASSERT_EQ(memcmp(offsets.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactMemberCtl, 0, after.data);
		UT_ASSERT_EQ(memcmp(retained_members.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactOffsetCtl, old_pageno, after.data);
		UT_ASSERT_EQ(memcmp(old_page.data, after.data, BLCKSZ), 0);
		local_check_members(cases[c].first, 500, 100, 116);
		local_check_members(cases[c].second, 502, 200, 216);
		MyBackendType = B_BACKEND;
		members[0].xid = 300;
		members[1].xid = 316;
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, cases[c].third);
		UT_ASSERT_EQ(local_state.nextOffset, 506);
		UT_ASSERT_EQ(wal_records, saved_wal + 1);
		CheckPointMultiXact();
		local_discard_cache();
		local_check_members(cases[c].first, 500, 100, 116);
		local_check_members(cases[c].second, 502, 200, 216);
		local_check_members(cases[c].third, 504, 300, 316);
		UT_ASSERT_EQ(transient_fds, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(physical_checkpoint_startup_refuses_contradiction_before_mutation)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		MultiXactMember members[2]
			= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
		LocalMultiCheckpoint checkpoint;
		PGAlignedBlock page = {0}, offsets, retained_members, after;
		MultiXactId id = 0;
		unsigned saved_writes, saved_wal;
		int slot;

		local_reset();
		local_state.nextMXact = local_state.oldestMultiXactId = 33;
		local_state.nextOffset = 500;
		local_state.multiVacLimit = 1033;
		local_state.multiWarnLimit = 2033;
		local_page(MultiXactOffsetCtl, 0, page.data, BLCKSZ);
		local_page(MultiXactMemberCtl, 0, page.data, BLCKSZ);
		capture_allocation = true;
		UT_ASSERT(local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(id, 48);
		MultiXactGetCheckptMulti(true, &checkpoint.next, &checkpoint.offset,
								&checkpoint.oldest, &checkpoint.database);
		UT_ASSERT_EQ(checkpoint.next, 49);
		UT_ASSERT_EQ(checkpoint.offset, 502);
		if (fault < 2) {
			LWLockAcquire(MultiXactOffsetSLRULock, LW_EXCLUSIVE);
			slot = SimpleLruReadPage(MultiXactOffsetCtl, 0, true, 0);
			((MultiXactOffset *)MultiXactOffsetCtl->shared->page_buffer[slot])[49]
				= fault == 0 ? 0 : 503;
			MultiXactOffsetCtl->shared->page_dirty[slot] = true;
			LWLockRelease(MultiXactOffsetSLRULock);
		}
		CheckPointMultiXact();
		local_discard_cache();
		local_read_page(MultiXactOffsetCtl, 0, offsets.data);
		local_read_page(MultiXactMemberCtl, 0, retained_members.data);
		local_restart_cursors(&checkpoint);
		if (fault == 2)
			MyBackendType = B_BACKEND;
		else if (fault == 3)
			InRecovery = true;
		else if (fault == 4)
			local_state.oldestMultiXactId = InvalidMultiXactId;
		saved_writes = writes;
		saved_wal = wal_records;
		/* FATAL is caught for inspection only; production startup exits. */
		UT_ASSERT(local_trim_refuses());
		UT_ASSERT_EQ(error_level, FATAL);
		UT_ASSERT(!local_state.finishedStartup && !local_state.oldestOffsetKnown);
		UT_ASSERT_EQ(local_state.nextMXact, checkpoint.next);
		UT_ASSERT_EQ(local_state.nextOffset, checkpoint.offset);
		CheckPointMultiXact();
		UT_ASSERT_EQ(writes, saved_writes);
		UT_ASSERT_EQ(wal_records, saved_wal);
		local_discard_cache();
		local_read_page(MultiXactOffsetCtl, 0, after.data);
		UT_ASSERT_EQ(memcmp(offsets.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactMemberCtl, 0, after.data);
		UT_ASSERT_EQ(memcmp(retained_members.data, after.data, BLCKSZ), 0);
		UT_ASSERT_EQ(transient_fds, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

/* Only the scheduling boundary is simulated. The opaque prepared-page bytes
 * are preservation markers, not issued IDs or authority to read old members. */
static void
local_publish_warning_window(void)
{
	UT_ASSERT(!LWLockHeldByMe(MultiXactGenLock));
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(writes, warning_writes);
	UT_ASSERT_EQ(wal_records, warning_wal);
	warning_unlocks++;
	local_state.nextMXact = warning_next;
	local_state.oldestMultiXactId = warning_lower;
	local_state.nextOffset = local_state.oldestOffset = 502;
	local_state.oldestOffsetKnown = warning_known;
	if (warning_member_stop)
		local_state.offsetStopLimit = 503;
}

static void
local_prepare_warning_window(bool wrap, int origin, PGAlignedBlock *old,
							 PGAlignedBlock *offsets, PGAlignedBlock *members)
{
	int old_page = wrap ? MultiXactIdToOffsetPage(MaxMultiXactId) : 0;
	int next_page = wrap ? 0 : 128;

	local_reset();
	local_state.nextMXact = wrap ? UINT32_MAX - 31 : 33;
	local_state.oldestMultiXactId = local_state.multiVacLimit = wrap ? UINT32_MAX - 63 : 32;
	local_state.nextOffset = local_state.oldestOffset = 500;
	local_state.multiWarnLimit = local_state.multiVacLimit + 1000;
	cluster_node_id = origin;
	cluster_mxid_stripe_latch_runtime(true, origin, wrap ? UINT32_MAX - 63 : 32);
	memset(old->data, 0, BLCKSZ);
	old->data[BLCKSZ - 1] = 0x72;
	memset(offsets->data, 0, BLCKSZ);
	memset(members->data, 0, BLCKSZ);
	((MultiXactOffset *)offsets->data)[2] = 887;
	((MultiXactOffset *)offsets->data)[MULTIXACT_OFFSETS_PER_PAGE - 1] = 771;
	members->data[BLCKSZ - 1] = 0x6b;
	local_page(MultiXactOffsetCtl, old_page, old->data, BLCKSZ);
	local_page(MultiXactOffsetCtl, next_page, offsets->data, BLCKSZ);
	local_page(MultiXactMemberCtl, 0, members->data, BLCKSZ);
	warning_next = wrap ? 17 : 262145;
	warning_lower = wrap ? 33 : 262161;
	warning_known = true;
	warning_member_stop = false;
	warning_writes = writes;
	warning_wal = wal_records;
	capture_allocation = true;
	warning_unlock_hook = local_publish_warning_window;
}

UT_TEST(physical_warning_recheck_preserves_prepared_page_and_records_final_candidate)
{
	static const struct {
		int origin;
		bool wrap;
		MultiXactId expected;
	} cases[] = {{0, false, 262176}, {1, false, 262161},
				 {0, true, 48}, {1, true, 33}};

	for (unsigned c = 0; c < lengthof(cases); c++) {
		MultiXactMember members[2]
			= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
		PGAlignedBlock old, offsets, retained_members, expected, after;
		MultiXactId id = 0;
		int old_page = cases[c].wrap ? MultiXactIdToOffsetPage(MaxMultiXactId) : 0;
		int next_page = cases[c].wrap ? 0 : 128;
		bool allocated;

		local_prepare_warning_window(cases[c].wrap, cases[c].origin,
								 &old, &offsets, &retained_members);
		allocated = local_allocate_and_record(&id, members);
		UT_ASSERT(allocated);
		UT_ASSERT_EQ(warning_unlocks, 1);
		UT_ASSERT_EQ(id, cases[c].expected);
		if (!allocated || warning_unlocks != 1 || id != cases[c].expected)
			continue;
		UT_ASSERT(warning_unlock_hook == NULL);
		UT_ASSERT_EQ(local_state.nextMXact, id + 1);
		UT_ASSERT_EQ(local_state.nextOffset, 504);
		UT_ASSERT_EQ(wal_records, 1);
		UT_ASSERT_EQ(allocation_wal[0].info, XLOG_MULTIXACT_CREATE_ID);
		expected = offsets;
		((MultiXactOffset *)expected.data)[MultiXactIdToOffsetEntry(id)] = 502;
		((MultiXactOffset *)expected.data)[MultiXactIdToOffsetEntry(id + 1)] = 504;
		CheckPointMultiXact();
		local_discard_cache();
		local_read_page(MultiXactOffsetCtl, next_page, after.data);
		UT_ASSERT_EQ(memcmp(expected.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactOffsetCtl, old_page, after.data);
		UT_ASSERT_EQ(memcmp(old.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactMemberCtl, 0, after.data);
		UT_ASSERT_EQ(after.data[BLCKSZ - 1], retained_members.data[BLCKSZ - 1]);
		local_check_members(id, 502, 100, 116);
		UT_ASSERT_EQ(transient_fds, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(!LWLockHeldByMe(MultiXactGenLock));
	}
}

UT_TEST(physical_warning_recheck_refuses_changed_member_limits_before_writes)
{
	for (int fault = 0; fault < 2; fault++) {
		MultiXactMember members[2]
			= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
		PGAlignedBlock old, offsets, retained_members, after;
		MultiXactId id = 0;

		local_prepare_warning_window(false, 0, &old, &offsets, &retained_members);
		warning_known = fault != 0;
		warning_member_stop = fault == 1;
		UT_ASSERT(!local_allocate_and_record(&id, members));
		UT_ASSERT_EQ(warning_unlocks, 1);
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT_EQ(id, 0);
		if (warning_unlocks != 1)
			continue;
		UT_ASSERT_EQ(local_state.nextMXact, warning_next);
		UT_ASSERT_EQ(local_state.oldestMultiXactId, warning_lower);
		UT_ASSERT_EQ(local_state.nextOffset, 502);
		UT_ASSERT_EQ(wal_records, warning_wal);
		CheckPointMultiXact();
		UT_ASSERT_EQ(writes, warning_writes);
		local_discard_cache();
		local_read_page(MultiXactOffsetCtl, 128, after.data);
		UT_ASSERT_EQ(memcmp(offsets.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactOffsetCtl, 0, after.data);
		UT_ASSERT_EQ(memcmp(old.data, after.data, BLCKSZ), 0);
		local_read_page(MultiXactMemberCtl, 0, after.data);
		UT_ASSERT_EQ(memcmp(retained_members.data, after.data, BLCKSZ), 0);
		UT_ASSERT_EQ(transient_fds, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(!LWLockHeldByMe(MultiXactGenLock));
	}
}

static void
local_prepare_member_wrap(int origin, MultiXactOffset start,
						  PGAlignedBlock *old_offsets, PGAlignedBlock *old_members)
{
	PGAlignedBlock stale_members;

	local_reset();
	local_state.nextMXact = 33;
	local_state.oldestMultiXactId = 262145;
	local_state.multiVacLimit = 263145;
	local_state.multiWarnLimit = 264145;
	local_state.nextOffset = start;
	local_state.oldestOffset = start;
	local_state.offsetStopLimit = 1000000;
	cluster_node_id = origin;
	cluster_mxid_stripe_latch_runtime(true, origin, 32);
	memset(old_offsets->data, 0x72, BLCKSZ);
	memset(old_members->data, 0, BLCKSZ);
	old_members->data[0] = 0x6b;
	memset(stale_members.data, 0x5c, BLCKSZ);
	local_page(MultiXactOffsetCtl, 0, old_offsets->data, BLCKSZ);
	local_page(MultiXactMemberCtl, MXOffsetToMemberPage(MaxMultiXactOffset),
			   old_members->data, BLCKSZ);
	local_page(MultiXactMemberCtl, 0, stale_members.data, BLCKSZ);
	capture_allocation = true;
}

UT_TEST(physical_member_wrap_preserves_retained_pages_and_skips_only_unused_zero)
{
	for (int origin = 0; origin < 2; origin++)
		for (unsigned straddle = 0; straddle < 2; straddle++) {
			MultiXactOffset start = straddle ? MaxMultiXactOffset : MaxMultiXactOffset - 1;
			MultiXactId first = origin ? 262145 : 262160;
			MultiXactMember members[2]
				= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
			PGAlignedBlock old_offsets, old_members, after;
			MultiXactId id = 0;
			unsigned member_zeros = 0;

			local_prepare_member_wrap(origin, start, &old_offsets, &old_members);
			if (!local_allocate_and_record(&id, members)) {
				UT_ASSERT(false);
				continue;
			}
			UT_ASSERT_EQ(id, first);
			UT_ASSERT_EQ(local_state.nextOffset, straddle ? 1 : 0);
			UT_ASSERT_EQ(wal_records, straddle ? 3 : 2);
			members[0].xid = 200;
			members[1].xid = 216;
			UT_ASSERT(local_allocate_and_record(&id, members));
			UT_ASSERT_EQ(id, first + 16);
			UT_ASSERT_EQ(local_state.nextOffset, 3);
			UT_ASSERT_EQ(wal_records, 4);
			for (unsigned i = 0; i < wal_records; i++)
				if (allocation_wal[i].info == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
					int page;
					memcpy(&page, allocation_wal[i].data, sizeof(page));
					UT_ASSERT_EQ(page, 0);
					member_zeros++;
				}
			UT_ASSERT_EQ(member_zeros, 1);
			SimpleLruWriteAll(MultiXactOffsetCtl, true);
			SimpleLruWriteAll(MultiXactMemberCtl, true);
			local_discard_cache();
			local_read_page(MultiXactOffsetCtl, 0, after.data);
			UT_ASSERT_EQ(memcmp(old_offsets.data, after.data, BLCKSZ), 0);
			local_read_page(MultiXactMemberCtl, MXOffsetToMemberPage(MaxMultiXactOffset),
							after.data);
			/* The low prefix precedes both reserved tail members. */
			UT_ASSERT(MXOffsetToFlagsOffset(start) >= 256);
			UT_ASSERT_EQ(memcmp(old_members.data, after.data, 256), 0);
			local_read_page(MultiXactMemberCtl, 0, after.data);
			UT_ASSERT_EQ(*(TransactionId *)(after.data + MXOffsetToMemberOffset(0)),
						 straddle ? 116 : InvalidTransactionId);
			UT_ASSERT_EQ(after.data[BLCKSZ - 1], 0);
			local_read_page(MultiXactOffsetCtl, 128, after.data);
			UT_ASSERT_EQ(((MultiXactOffset *)after.data)[MultiXactIdToOffsetEntry(first)], start);
			UT_ASSERT_EQ(((MultiXactOffset *)after.data)[MultiXactIdToOffsetEntry(first + 1)], 1);
			UT_ASSERT_EQ(((MultiXactOffset *)after.data)[MultiXactIdToOffsetEntry(first + 16)], 1);
			/* A broken sentinel must fail here without asking the fixture's
			 * uncaught-error delivery to abort the remaining test groups. */
			if (((MultiXactOffset *)after.data)[MultiXactIdToOffsetEntry(first + 1)] != 1)
				continue;
			local_check_members(first, start, 100, 116);
			local_check_members(first + 16, 1, 200, 216);
			members[0].xid = 300;
			members[1].xid = 316;
			UT_ASSERT(local_allocate_and_record(&id, members));
			UT_ASSERT_EQ(id, first + 32);
			UT_ASSERT_EQ(local_state.nextOffset, 5);
			UT_ASSERT_EQ(wal_records, 5);
			SimpleLruWriteAll(MultiXactOffsetCtl, true);
			SimpleLruWriteAll(MultiXactMemberCtl, true);
			local_discard_cache();
			local_check_members(first, start, 100, 116);
			local_check_members(first + 16, 1, 200, 216);
			local_check_members(first + 32, 3, 300, 316);
			UT_ASSERT_EQ(transient_fds, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
		}
}

UT_TEST(physical_member_wrap_stop_refuses_before_gap_extension)
{
	for (int origin = 0; origin < 2; origin++)
		for (unsigned straddle = 0; straddle < 2; straddle++) {
			MultiXactOffset start = straddle ? MaxMultiXactOffset : MaxMultiXactOffset - 1;
			MultiXactMember members[2]
				= {{116, MultiXactStatusForShare}, {100, MultiXactStatusForKeyShare}};
			PGAlignedBlock old_offsets, old_members, after, stale_members;
			MultiXactId id = 0;

			local_prepare_member_wrap(origin, start, &old_offsets, &old_members);
			memset(stale_members.data, 0x5c, BLCKSZ);
			local_state.offsetStopLimit = straddle ? 2 : 1;
			UT_ASSERT(!local_allocate_and_record(&id, members));
			UT_ASSERT_EQ(error_level, ERROR);
			UT_ASSERT_EQ(id, 0);
			UT_ASSERT_EQ(local_state.nextMXact, 33);
			UT_ASSERT_EQ(local_state.nextOffset, start);
			UT_ASSERT_EQ(wal_records + writes + syncs + dirsyncs, 0);
			UT_ASSERT(!local_segment_exists(MultiXactOffsetCtl, 4));
			UT_ASSERT(!LWLockHeldByMe(MultiXactGenLock));
			local_read_page(MultiXactOffsetCtl, 0, after.data);
			UT_ASSERT_EQ(memcmp(old_offsets.data, after.data, BLCKSZ), 0);
			local_read_page(MultiXactMemberCtl, MXOffsetToMemberPage(MaxMultiXactOffset),
							after.data);
			UT_ASSERT_EQ(memcmp(old_members.data, after.data, BLCKSZ), 0);
			local_read_page(MultiXactMemberCtl, 0, after.data);
			UT_ASSERT_EQ(memcmp(stale_members.data, after.data, BLCKSZ), 0);
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
	UT_PLAN(13);
	UT_RUN(native_empty_boot_reads_real_authority_and_original_backup);
	UT_RUN(physical_exact_page_missing_short_and_zero_keep_original_refusal);
	UT_RUN(physical_runtime_limit_preserves_live_member_page);
	UT_RUN(physical_member_deletion_and_actual_redo_preserve_live_segments);
	UT_RUN(physical_unknown_old_endpoint_never_deletes_live_members);
	UT_RUN(original_authority_reader_qualifies_native_prefix_without_writes);
	UT_RUN(physical_catchup_keeps_old_pages_and_replays_native_offsets_and_members);
	UT_RUN(physical_checkpoint_startup_preserves_retained_bytes_and_continues);
	UT_RUN(physical_checkpoint_startup_refuses_contradiction_before_mutation);
	UT_RUN(physical_warning_recheck_preserves_prepared_page_and_records_final_candidate);
	UT_RUN(physical_warning_recheck_refuses_changed_member_limits_before_writes);
	UT_RUN(physical_member_wrap_preserves_retained_pages_and_skips_only_unused_zero);
	UT_RUN(physical_member_wrap_stop_refuses_before_gap_extension);
	local_reset();
	UT_ASSERT_EQ(rmdir(MultiXactOffsetCtl->Dir), 0);
	UT_ASSERT_EQ(rmdir(MultiXactMemberCtl->Dir), 0);
	UT_ASSERT_EQ(rmdir(fixture_root), 0);
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	UT_DONE();
	return ut_failed_count != 0;
}
