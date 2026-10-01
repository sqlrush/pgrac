/*-------------------------------------------------------------------------
 * test_cluster_multixact_recovery.c
 *    Original-origin recovery through native MultiXact and SLRU file I/O.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_multixact_recovery.c
 * NOTES
 *    PGRAC test; only shmem, locks, original authority and fault delivery are
 *    fixtures. Native SLRU lookup, read, write, delete and sync bodies execute.
 *    This is not a cluster fencing or native startup qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dirent.h>
#include <sys/wait.h>
#include "../../backend/access/transam/multixact.c"
#include "../../backend/access/transam/slru.c"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true;
int cluster_node_id = 0;
char *cluster_shared_data_dir;
bool InRecovery = true;
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
static bool authority = true, revoke_on_lock, expect_error, fail_open, fail_sync, fail_dirsync;
static bool fixture_retired;
static int transient_fds;
static unsigned writes, syncs, dirsyncs, reads;
static struct { LWLock *lock; bool held; } fixture_locks[6000];
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
	if (expect_error && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
void errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
	const char *fn pg_attribute_unused()) { abort(); }
void pg_re_throw(void) { siglongjmp(*PG_exception_stack, 1); }
int errmsg(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errmsg_internal(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errdetail(const char *fmt pg_attribute_unused(), ...) { return 0; }
int errcode(int code pg_attribute_unused()) { return 0; }
int errcode_for_file_access(void) { return 0; }
int data_sync_elevel(int level) { return level; }
void ProcessInterrupts(void) { abort(); }

bool cluster_undo_recovery_origin_authorized_v1(int origin) { return authority && origin == 1; }
bool
cluster_undo_recovery_multixact_page_retired_v1(int origin, XLogRecPtr source_lsn,
	XLogRecPtr source_end_lsn, bool members pg_attribute_unused(), uint32 page)
{
	return authority && fixture_retired && origin == 1 && source_lsn == 100
		&& source_end_lsn == 200 && page == 0;
}
int cluster_mxid_origin_slot(MultiXactId id pg_attribute_unused()) { return 1; }
bool TransactionIdPrecedes(TransactionId a, TransactionId b) { return (int32)(a - b) < 0; }
bool TransactionIdFollowsOrEquals(TransactionId a, TransactionId b) { return (int32)(a - b) >= 0; }

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
	if (revoke_on_lock && lock == &MultiXactRecoveryLocks[5].lock)
		authority = false;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	unsigned i = fixture_lock_index(lock);
	UT_ASSERT(fixture_locks[i].held);
	fixture_locks[i].held = false;
}
bool LWLockHeldByMe(LWLock *lock) { return fixture_locks[fixture_lock_index(lock)].held; }
bool LWLockConditionalAcquire(LWLock *lock, LWLockMode mode) { return LWLockAcquire(lock, mode); }

int pgstat_get_slru_index(const char *name pg_attribute_unused()) { return 0; }
void pgstat_count_slru_page_zeroed(int i pg_attribute_unused()) {}
void pgstat_count_slru_page_hit(int i pg_attribute_unused()) {}
void pgstat_count_slru_page_read(int i pg_attribute_unused()) { reads++; }
void pgstat_count_slru_page_written(int i pg_attribute_unused()) { writes++; }
void pgstat_count_slru_flush(int i pg_attribute_unused()) {}
void pgstat_count_slru_truncate(int i pg_attribute_unused()) {}
void XLogFlush(XLogRecPtr lsn pg_attribute_unused()) { abort(); }
bool RegisterSyncRequest(const FileTag *tag pg_attribute_unused(),
	SyncRequestType type pg_attribute_unused(), bool retry pg_attribute_unused()) { abort(); }

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
int CloseTransientFile(int fd) { transient_fds--; return close(fd); }
int
pg_fsync(int fd)
{
	syncs++;
	if (fail_sync) { errno = EIO; return -1; }
	return fsync(fd);
}
void
fsync_fname(const char *path, bool isdir)
{
	int fd, rc;
	UT_ASSERT(isdir);
	dirsyncs++;
	if (fail_dirsync) { errno = EIO; (void)errstart(ERROR, NULL); }
	fd = open(path, O_RDONLY | O_DIRECTORY);
	if (fd < 0)
		abort();
	rc = fsync(fd);
	if (close(fd) != 0)
		rc = -1;
	UT_ASSERT_EQ(rc, 0);
}

static void
fixture_idle(void)
{
	UT_ASSERT_EQ(transient_fds, 0);
	for (unsigned i = 0; i < fixture_lock_count; i++)
		UT_ASSERT(!fixture_locks[i].held);
	for (int family = 0; family < 2; family++) {
		SlruShared s = family ? MultiXactRecoveryMemberCtl[1].shared
			: MultiXactRecoveryOffsetCtl[1].shared;
		for (int i = 0; i < s->num_slots; i++) {
			UT_ASSERT_EQ(s->page_status[i], SLRU_PAGE_EMPTY);
			UT_ASSERT(!s->page_dirty[i]);
		}
	}
}
static ClusterSideProjectionOperationV1
fixture_create(MultiXactId multi, MultiXactOffset offset, int count)
{
	ClusterSideProjectionOperationV1 op = {0};
	op.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_CREATE;
	op.normalized_info = XLOG_MULTIXACT_CREATE_ID;
	op.multixact_id = multi;
	op.member_offset = offset;
	op.member_count = count;
	return op;
}
static void
fixture_zero(bool members, int page)
{
	ClusterSideProjectionOperationV1 op = {0};
	op.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	op.normalized_info = members ? XLOG_MULTIXACT_ZERO_MEM_PAGE : XLOG_MULTIXACT_ZERO_OFF_PAGE;
	op.page_number = page;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, NULL, 0, 100, 200));
	fixture_idle();
}
static void
fixture_read(SlruCtl ctl, int page, char *data)
{
	char path[MAXPGPATH];
	int fd;
	snprintf(path, sizeof(path), "%s/%04X", ctl->Dir, page / SLRU_PAGES_PER_SEGMENT);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(pread(fd, data, BLCKSZ, (off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(shared_controls_are_distinct_and_use_original_namespace)
{
	UT_ASSERT(MultiXactRecoveryOffsetCtl[1].shared != MultiXactOffsetCtl->shared);
	UT_ASSERT(MultiXactRecoveryOffsetCtl[1].shared != MultiXactRecoveryOffsetCtl[2].shared);
	UT_ASSERT(strstr(MultiXactRecoveryOffsetCtl[1].Dir, "/native_side/origin_1/pg_multixact/offsets"));
	UT_ASSERT_EQ(MultiXactRecoveryOffsetCtl[1].sync_handler, SYNC_HANDLER_NONE);
	fixture_idle();
}

UT_TEST(authority_failure_and_local_origin_never_write)
{
	ClusterSideProjectionOperationV1 op = fixture_create(17, 1, 1);
	MultiXactMember member = {100, MultiXactStatusForShare};
	unsigned before = writes;

	authority = false;
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	authority = true;
	UT_ASSERT(!cluster_multixact_native_recovery_apply(0, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT(!cluster_multixact_native_recovery_apply(128, &op, (uint8 *)&member, sizeof(member), 100, 200));
	revoke_on_lock = true;
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	revoke_on_lock = false;
	authority = true;
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(actual_native_create_crosses_offset_and_member_pages)
{
	ClusterSideProjectionOperationV1 op = fixture_create(MULTIXACT_OFFSETS_PER_PAGE - 1,
		MULTIXACT_MEMBERS_PER_PAGE - 1, 2);
	MultiXactMember members[2] = {{100, MultiXactStatusForKeyShare}, {116, MultiXactStatusUpdate}};
	PGAlignedBlock page;
	unsigned before;

	fixture_zero(false, 0);
	fixture_zero(false, 1);
	fixture_zero(true, 0);
	fixture_zero(true, 1);
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, (uint8 *)members, sizeof(members), 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, (uint8 *)members, sizeof(members), 100, 200));
	fixture_read(&MultiXactRecoveryOffsetCtl[1], 0, page.data);
	UT_ASSERT_EQ(((MultiXactOffset *)page.data)[MULTIXACT_OFFSETS_PER_PAGE - 1], op.member_offset);
	fixture_read(&MultiXactRecoveryOffsetCtl[1], 1, page.data);
	UT_ASSERT_EQ(((MultiXactOffset *)page.data)[0], op.member_offset + 2);
	fixture_read(&MultiXactRecoveryMemberCtl[1], 1, page.data);
	UT_ASSERT_EQ(*(TransactionId *)(page.data + MXOffsetToMemberOffset(op.member_offset + 1)), 116);
	UT_ASSERT_EQ((*(uint32 *)(page.data + MXOffsetToFlagsOffset(op.member_offset + 1))
		>> MXOffsetToFlagsBitShift(op.member_offset + 1)) & MXACT_MEMBER_XACT_BITMASK, MultiXactStatusUpdate);
	before = syncs;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, (uint8 *)members, sizeof(members), 100, 200));
	UT_ASSERT(syncs >= before + 2);
	members[1].xid++;
	UT_ASSERT(!cluster_multixact_native_recovery_verify(1, &op, (uint8 *)members, sizeof(members), 100, 200));
	fixture_idle();
}

UT_TEST(missing_and_short_native_pages_do_not_become_zero_bases)
{
	ClusterSideProjectionOperationV1 op = fixture_create(4 * MULTIXACT_OFFSETS_PER_PAGE, 1, 1);
	MultiXactMember member = {100, MultiXactStatusForShare};
	unsigned before = writes;

	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	op = fixture_create(123, 3 * MULTIXACT_MEMBERS_PER_PAGE, 1);
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	op.member_count = UINT32_MAX;
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(native_io_error_and_fsync_failure_leave_no_dirty_owner)
{
	ClusterSideProjectionOperationV1 op = fixture_create(125, 211, 1);
	MultiXactMember member = {200, MultiXactStatusForShare};
	volatile bool caught = false;

	fail_open = expect_error = true;
	PG_TRY();
	{
		(void)cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fail_open = expect_error = false;
	UT_ASSERT(caught);
	fixture_idle();
	fail_sync = true;
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	fail_sync = false;
	fixture_idle();
	fail_dirsync = expect_error = true;
	caught = false;
	PG_TRY();
	{
		(void)cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fail_dirsync = expect_error = false;
	UT_ASSERT(caught);
	fixture_idle();
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	fixture_idle();
}

UT_TEST(wraparound_sentinel_and_members_keep_native_geometry)
{
	ClusterSideProjectionOperationV1 op = fixture_create(MaxMultiXactId, MaxMultiXactOffset, 2);
	MultiXactMember members[2] = {{300, MultiXactStatusForShare}, {316, MultiXactStatusForUpdate}};
	PGAlignedBlock page;

	fixture_zero(false, MultiXactIdToOffsetPage(MaxMultiXactId));
	fixture_zero(true, MXOffsetToMemberPage(MaxMultiXactOffset));
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, (uint8 *)members, sizeof(members), 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, (uint8 *)members, sizeof(members), 100, 200));
	fixture_read(&MultiXactRecoveryOffsetCtl[1], 0, page.data);
	UT_ASSERT_EQ(((MultiXactOffset *)page.data)[FirstMultiXactId], 1);
	fixture_read(&MultiXactRecoveryMemberCtl[1], 0, page.data);
	UT_ASSERT_EQ(*(TransactionId *)(page.data + MXOffsetToMemberOffset(0)), 316);
	fixture_idle();
}

UT_TEST(truncate_is_durable_and_retains_final_partial_segments)
{
	ClusterSideProjectionOperationV1 op = {0};
	char path[MAXPGPATH];

	fixture_zero(false, SLRU_PAGES_PER_SEGMENT * 2);
	fixture_zero(true, SLRU_PAGES_PER_SEGMENT * 2);
	op.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
	op.normalized_info = XLOG_MULTIXACT_TRUNCATE_ID;
	op.oldest_database = 1;
	op.truncate_start_multixact = 1;
	op.truncate_end_multixact = MULTIXACT_OFFSETS_PER_PAGE * SLRU_PAGES_PER_SEGMENT * 2 + 1;
	op.truncate_start_member = 0;
	op.truncate_end_member = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT * 2 + 1;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, NULL, 0, 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 100, 200));
	snprintf(path, sizeof(path), "%s/0000", MultiXactRecoveryOffsetCtl[1].Dir);
	UT_ASSERT(access(path, F_OK) != 0 && errno == ENOENT);
	snprintf(path, sizeof(path), "%s/0002", MultiXactRecoveryOffsetCtl[1].Dir);
	UT_ASSERT_EQ(access(path, F_OK), 0);
	snprintf(path, sizeof(path), "%s/0002", MultiXactRecoveryMemberCtl[1].Dir);
	UT_ASSERT_EQ(access(path, F_OK), 0);
	fixture_idle();
}

UT_TEST(writer_exit_and_different_working_directory_preserve_native_bytes)
{
	ClusterSideProjectionOperationV1 op = fixture_create(17, 71, 1);
	MultiXactMember member = {400, MultiXactStatusForShare};
	pid_t child;
	int status;
	char original[MAXPGPATH];

	fixture_zero(false, 0);
	fixture_zero(true, 0);
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0)
		_exit(cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200) ? 0 : 1);
	if (child < 0)
		return;
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	UT_ASSERT(getcwd(original, sizeof(original)) != NULL);
	UT_ASSERT_EQ(chdir("/"), 0);
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT_EQ(chdir(original), 0);
	fixture_idle();
}

UT_TEST(directory_and_segment_aliases_are_refused_before_writes)
{
	ClusterSideProjectionOperationV1 op = fixture_create(17, 71, 1);
	MultiXactMember member = {400, MultiXactStatusForShare};
	SlruCtl ctl = &MultiXactRecoveryOffsetCtl[1];
	char moved[MAXPGPATH], segment[MAXPGPATH], alias[MAXPGPATH];
	unsigned before = writes;

	snprintf(moved, sizeof(moved), "%s.saved", ctl->Dir);
	UT_ASSERT_EQ(rename(ctl->Dir, moved), 0);
	UT_ASSERT_EQ(symlink(moved, ctl->Dir), 0);
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT_EQ(unlink(ctl->Dir), 0);
	UT_ASSERT_EQ(rename(moved, ctl->Dir), 0);
	snprintf(segment, sizeof(segment), "%s/0000", ctl->Dir);
	snprintf(alias, sizeof(alias), "%s/linked-segment", fixture_root);
	UT_ASSERT_EQ(link(segment, alias), 0);
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT_EQ(unlink(alias), 0);
	UT_ASSERT_EQ(writes, before);
	fixture_idle();
}

UT_TEST(postread_observes_real_disk_corruption_and_replay_repairs_members)
{
	ClusterSideProjectionOperationV1 op = fixture_create(17, 71, 1);
	MultiXactMember member = {400, MultiXactStatusForShare};
	char path[MAXPGPATH];
	TransactionId bad = 777;
	int fd;
	unsigned before = reads;

	snprintf(path, sizeof(path), "%s/0000", MultiXactRecoveryMemberCtl[1].Dir);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(pwrite(fd, &bad, sizeof(bad), MXOffsetToMemberOffset(71)), sizeof(bad));
	UT_ASSERT_EQ(fsync(fd), 0);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(!cluster_multixact_native_recovery_verify(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT(reads > before);
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, (uint8 *)&member, sizeof(member), 100, 200));
	fixture_idle();
}

UT_TEST(repeated_create_after_proven_later_truncate_reconstructs_only_retired_pages)
{
	ClusterSideProjectionOperationV1 op = {0};
	ClusterSideProjectionOperationV1 create = fixture_create(17, 71, 1);
	MultiXactMember member = {400, MultiXactStatusForShare};

	op.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
	op.normalized_info = XLOG_MULTIXACT_TRUNCATE_ID;
	op.oldest_database = 1;
	op.truncate_start_multixact = 1;
	op.truncate_end_multixact = MULTIXACT_OFFSETS_PER_PAGE * SLRU_PAGES_PER_SEGMENT + 1;
	op.truncate_end_member = MULTIXACT_MEMBERS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 200, 300));
	UT_ASSERT(!cluster_multixact_native_recovery_apply(1, &create,
		(uint8 *)&member, sizeof(member), 100, 200));
	fixture_retired = true;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &create,
		(uint8 *)&member, sizeof(member), 100, 200));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &create,
		(uint8 *)&member, sizeof(member), 100, 200));
	fixture_retired = false;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 200, 300));
	UT_ASSERT(cluster_multixact_native_recovery_verify(1, &op, NULL, 0, 200, 300));
	fixture_idle();
}

UT_TEST(next_offsets_truncate_retires_previous_boundary_segment)
{
	ClusterSideProjectionOperationV1 op = {0};
	char path[MAXPGPATH];

	fixture_zero(false, 0);
	op.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
	op.normalized_info = XLOG_MULTIXACT_TRUNCATE_ID;
	op.oldest_database = 1;
	op.truncate_start_multixact = 1;
	op.truncate_end_multixact = MULTIXACT_OFFSETS_PER_PAGE * SLRU_PAGES_PER_SEGMENT;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 100, 200));
	snprintf(path, sizeof(path), "%s/0000", MultiXactRecoveryOffsetCtl[1].Dir);
	UT_ASSERT_EQ(access(path, F_OK), 0);
	op.truncate_start_multixact = op.truncate_end_multixact;
	op.truncate_end_multixact++;
	UT_ASSERT(cluster_multixact_native_recovery_apply(1, &op, NULL, 0, 200, 300));
	UT_ASSERT(access(path, F_OK) != 0 && errno == ENOENT);
	fixture_idle();
}

static void
fixture_cleanup(void)
{
	char path[MAXPGPATH];
	const char *parents[] = {"/native_side/origin_1/pg_multixact", "/native_side/origin_1", "/native_side", ""};

	for (int family = 0; family < 2; family++) {
		SlruCtl ctl = family ? &MultiXactRecoveryMemberCtl[1] : &MultiXactRecoveryOffsetCtl[1];
		DIR *dir = opendir(ctl->Dir);
		struct dirent *entry;
		while ((entry = readdir(dir)) != NULL) {
			if (entry->d_name[0] == '.')
				continue;
			snprintf(path, sizeof(path), "%s/%s", ctl->Dir, entry->d_name);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		UT_ASSERT_EQ(closedir(dir), 0);
		UT_ASSERT_EQ(rmdir(ctl->Dir), 0);
	}
	for (unsigned i = 0; i < lengthof(parents); i++) {
		snprintf(path, sizeof(path), "%s%s", fixture_root, parents[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
}

int
main(void)
{
	char path[MAXPGPATH];
	const char *dirs[] = {"native_side", "native_side/origin_1", "native_side/origin_1/pg_multixact",
		"native_side/origin_1/pg_multixact/offsets", "native_side/origin_1/pg_multixact/members"};

	strlcpy(fixture_root, "/tmp/pgrac-mx-recovery.XXXXXX", sizeof(fixture_root));
	if (!mkdtemp(fixture_root))
		return 2;
	cluster_shared_data_dir = fixture_root;
	for (unsigned i = 0; i < lengthof(dirs); i++) {
		snprintf(path, sizeof(path), "%s/%s", fixture_root, dirs[i]);
		if (mkdir(path, 0700) != 0)
			return 2;
	}
	multi_recovery_shmem_init();
	UT_PLAN(12);
	UT_RUN(shared_controls_are_distinct_and_use_original_namespace);
	UT_RUN(authority_failure_and_local_origin_never_write);
	UT_RUN(actual_native_create_crosses_offset_and_member_pages);
	UT_RUN(missing_and_short_native_pages_do_not_become_zero_bases);
	UT_RUN(native_io_error_and_fsync_failure_leave_no_dirty_owner);
	UT_RUN(wraparound_sentinel_and_members_keep_native_geometry);
	UT_RUN(truncate_is_durable_and_retains_final_partial_segments);
	UT_RUN(writer_exit_and_different_working_directory_preserve_native_bytes);
	UT_RUN(directory_and_segment_aliases_are_refused_before_writes);
	UT_RUN(postread_observes_real_disk_corruption_and_replay_repairs_members);
	UT_RUN(repeated_create_after_proven_later_truncate_reconstructs_only_retired_pages);
	UT_RUN(next_offsets_truncate_retires_previous_boundary_segment);
	fixture_cleanup();
	UT_DONE();
	return ut_failed_count != 0;
}
