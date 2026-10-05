/* Original KO work and storage consumer over real files.
 * Faults are confined to the syscall boundary; no completion is fabricated.
 * Author: SqlRush <sqlrush@gmail.com> */
int drop_owner_fixture_main(void);
#define main drop_owner_fixture_main
#define cluster_smgr_which_for drop_fixture_smgr_which_for
#include "test_cluster_ko_stop.c"
#undef cluster_smgr_which_for
#undef main

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "cluster/storage/cluster_shared_fs.h"
#include "storage/fd.h"

bool cluster_shared_catalog = true;
bool IsBinaryUpgrade = false;
bool enableFsync = true;
char *cluster_shared_data_dir;
int io_direct_flags;
volatile uint32 QueryCancelHoldoffCount;

static ClusterSharedFsOps drop_ops = { .id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS };
const ClusterSharedFsOps *
cluster_shared_fs_get_active_ops(void)
{
	return &drop_ops;
}
/* The native caller must return at the retained-work boundary. Falling into
 * its old synchronous ERROR recovery is a fixture failure, never success. */
ErrorData *
CopyErrorData(void)
{
	abort();
}
void
FlushErrorState(void)
{
	abort();
}
void
ThrowErrorData(ErrorData *error pg_attribute_unused())
{
	abort();
}
void
FreeErrorData(ErrorData *error pg_attribute_unused())
{
	abort();
}
int
errcode_for_file_access(void)
{
	return 0;
}

char *
pstrdup(const char *source)
{
	char *result = MemoryContextAllocZero(TopMemoryContext, strlen(source) + 1);
	strcpy(result, source);
	return result;
}

char *
psprintf(const char *format, ...)
{
	char buffer[4096];
	va_list args;
	int length;

	va_start(args, format);
	length = vsnprintf(buffer, sizeof(buffer), format, args);
	va_end(args);
	if (length < 0 || length >= sizeof(buffer))
		abort();
	return pstrdup(buffer);
}

char *
GetRelationPath(Oid db, Oid spc, RelFileNumber number, int backend, ForkNumber fork)
{
	const char *suffix[] = { "", "_fsm", "_vm", "_init", "_space" };
	if (spc != 1663 || backend != InvalidBackendId || fork < 0 || fork > SPACE_FORKNUM)
		abort();
	return psprintf("base/%u/%u%s", db, number, suffix[fork]);
}

static unsigned forgets;
static unsigned external_fds;
static bool work_fds[1024];
bool
AcquireExternalFD(void)
{
	external_fds++;
	return true;
}
void
ReleaseExternalFD(void)
{
	if (external_fds == 0)
		abort();
	external_fds--;
}
bool
RegisterSyncRequest(const FileTag *tag, SyncRequestType type, bool retry)
{
	UT_ASSERT_EQ(type, SYNC_FORGET_REQUEST);
	UT_ASSERT_EQ(tag->handler, SYNC_HANDLER_CLUSTER_SHARED);
	UT_ASSERT(tag->forknum > MAIN_FORKNUM && tag->forknum <= MAX_FORKNUM);
	UT_ASSERT(retry);
	forgets++;
	return true;
}

enum WorkFault {
	WORK_OK,
	WORK_TRUNCATE,
	WORK_MAIN_SYNC,
	WORK_AUX,
	WORK_PARTIAL,
	WORK_DIR_SYNC,
	WORK_UNLINK_UNKNOWN,
	WORK_CLOSE,
	WORK_FINISH,
	WORK_CUT_AFTER_TRUNCATE,
	WORK_CUT_AFTER_SYNC,
	WORK_CUT_AFTER_UNLINK
};
static enum WorkFault work_fault;
static unsigned truncates, main_syncs, dir_syncs, unlinks, closes, opens;
static unsigned per_fork_unlinks[MAX_FORKNUM + 1];
static char work_root[MAXPGPATH], work_parent[MAXPGPATH];
static char work_paths[MAX_FORKNUM + 1][MAXPGPATH];
static uint32 work_slot;

static int
work_open(const char *path, int flags, ...)
{
	int fd = open(path, flags, 0600);
	opens++;
	if (fd >= 0) {
		if (fd >= lengthof(work_fds))
			abort();
		work_fds[fd] = true;
	}
	return fd;
}

static int
work_openat(int dir, const char *name, int flags)
{
	int fd = openat(dir, name, flags);
	opens++;
	if (fd >= 0) {
		if (fd >= lengthof(work_fds))
			abort();
		work_fds[fd] = true;
	}
	return fd;
}

static int
work_truncate(int fd, off_t size)
{
	int result;
	truncates++;
	if (work_fault == WORK_TRUNCATE) {
		errno = EIO;
		return -1;
	}
	result = ftruncate(fd, size);
	if (work_fault == WORK_CUT_AFTER_TRUNCATE)
		current_epoch++;
	return result;
}

static int
work_sync(int fd)
{
	struct stat st;
	if (fstat(fd, &st) != 0)
		abort();
	if (S_ISDIR(st.st_mode)) {
		dir_syncs++;
		if (work_fault == WORK_DIR_SYNC) {
			errno = EIO;
			return -1;
		}
	} else {
		main_syncs++;
		UT_ASSERT_EQ(st.st_size, 0);
		UT_ASSERT_EQ(unlinks, 0);
		if (work_fault == WORK_MAIN_SYNC) {
			errno = EIO;
			return -1;
		}
		if (work_fault == WORK_CUT_AFTER_SYNC)
			current_epoch++;
	}
	return fsync(fd);
}

static int
work_unlink(int dir, const char *name, int flags)
{
	ForkNumber f = strstr(name, "_fsm") != NULL	   ? FSM_FORKNUM
				   : strstr(name, "_vm") != NULL   ? VISIBILITYMAP_FORKNUM
				   : strstr(name, "_init") != NULL ? INIT_FORKNUM
												   : SPACE_FORKNUM;
	int result;
	unlinks++;
	per_fork_unlinks[f]++;
	UT_ASSERT(main_syncs > 0);
	if (work_fault == WORK_AUX || (work_fault == WORK_PARTIAL && f == VISIBILITYMAP_FORKNUM)) {
		errno = EIO;
		return -1;
	}
	result = unlinkat(dir, name, flags);
	if (work_fault == WORK_UNLINK_UNKNOWN) {
		errno = EIO;
		return -1;
	}
	if (work_fault == WORK_CUT_AFTER_UNLINK)
		current_epoch++;
	return result;
}

static int
work_close(int fd)
{
	int result;
	struct stat st;
	if (fstat(fd, &st) != 0)
		abort();
	closes++;
	result = close(fd);
	work_fds[fd] = false;
	if (work_fault == WORK_FINISH && S_ISDIR(st.st_mode))
		generation_race = true;
	if (work_fault == WORK_CLOSE) {
		errno = EIO;
		return -1;
	}
	return result;
}

#define ftruncate work_truncate
#define pg_fsync work_sync
#define unlinkat work_unlink
#define open(...) work_open(__VA_ARGS__)
#define openat(dir, name, flags) work_openat(dir, name, flags)
#define close(fd) work_close(fd)
#include "../../backend/cluster/storage/cluster_shared_fs_sharedfs.c"
#undef close
#undef openat
#undef open
#undef unlinkat
#undef pg_fsync
#undef ftruncate
int cluster_smgr_which_for(RelFileLocator locator, BackendId backend);
#include "../../backend/cluster/storage/cluster_smgr.c"

static int
descriptor_count(void)
{
	int n = 0;
	for (int fd = 0; fd < 1024; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			n++;
	return n;
}

static void
write_file(const char *path, const char *bytes)
{
	int fd = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
	if (fd < 0 || write(fd, bytes, BLCKSZ) != BLCKSZ || close(fd) != 0)
		abort();
}

static void
prepare_files(const ClusterSpaceStructureChange *change)
{
	PGIOAlignedBlock page;
	const char *suffix[] = { "", "_fsm", "_vm", "_init", "_space" };
	if (!cluster_space_identity_page_encode(&change->identity.result, change->identity.result_token,
											page.data, BLCKSZ))
		abort();
	for (ForkNumber f = 0; f <= MAX_FORKNUM; f++) {
		snprintf(work_paths[f], sizeof(work_paths[f]), "%s/%u%s", work_parent,
				 change->identity.result.key.locator.relNumber, suffix[f]);
		write_file(work_paths[f], page.data);
	}
}

static bool
work_setup_impl(const char *name, bool native, bool stale)
{
	ClusterPageWalBindingV1 binding;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterSpaceStructureChange change;
	char base[MAXPGPATH];
	bool (*volatile entry)(uint32 *, bool *) = cluster_smgr_drop_work_poll;
	UT_ASSERT(entry != NULL);
	if (entry == NULL)
		return false;
	/* Isolate test cases, including deliberately unfinishable obligations. */
	for (int fd = 0; fd < lengthof(work_fds); fd++) {
		if (work_fds[fd]) {
			close(fd);
			work_fds[fd] = false;
		}
	}
	if (exit_callback != NULL)
		exit_callback(0, (Datum)0);
	external_fds = 0;
	if (native) {
		prepare_postcommit(true);
		binding = postcommit_binding;
		memcpy(wal, postcommit_wal, sizeof(wal));
	} else
		work_slot = prepare_promoted_drop(&binding, wal);
	if (!cluster_space_structure_wal_decode(wal, sizeof(wal), &change))
		abort();
	snprintf(work_root, sizeof(work_root), "/tmp/pgrac_drop_work_%d_%s", (int)getpid(), name);
	snprintf(base, sizeof(base), "%s/base", work_root);
	snprintf(work_parent, sizeof(work_parent), "%s/%u", base,
			 change.identity.result.key.locator.dbOid);
	if (mkdir(work_root, 0700) != 0 || mkdir(base, 0700) != 0 || mkdir(work_parent, 0700) != 0)
		abort();
	cluster_shared_data_dir = work_root;
	prepare_files(&change);
	work_fault = WORK_OK;
	forgets = truncates = main_syncs = dir_syncs = unlinks = closes = opens = 0;
	memset(per_fork_unlinks, 0, sizeof(per_fork_unlinks));
	enableFsync = true;
	if (native) {
		native_commit_active = true;
		postcommit_scope_changed = stale;
		postcommit_storage_consumer = cluster_smgr_unlink_committed_drop;
		run_native_postcommit();
		postcommit_storage_consumer = NULL;
		UT_ASSERT_EQ(opens + truncates + main_syncs + dir_syncs + unlinks + closes, 0);
		UT_ASSERT_EQ(storage.native_waiting, 1);
		UT_ASSERT_EQ(completion_allocations, 0);
		MyBackendType = B_CHECKPOINTER;
		if (stale) {
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
			UT_ASSERT_EQ(storage.native_waiting, 1);
			current_epoch--;
		}
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
		work_slot = 0;
	}
	return true;
}

static bool
work_setup(const char *name)
{
	return work_setup_impl(name, false, false);
}

static bool
poll_work(void)
{
	uint32 cursor = work_slot;
	bool completed = false;
	UT_ASSERT(cluster_smgr_drop_work_poll(&cursor, &completed));
	UT_ASSERT_EQ(cursor, work_slot + 1);
	return completed;
}

static void
assert_finished(void)
{
	struct stat st;
	UT_ASSERT_EQ(stat(work_paths[MAIN_FORKNUM], &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);
	for (ForkNumber f = 1; f <= MAX_FORKNUM; f++)
		UT_ASSERT(access(work_paths[f], F_OK) < 0 && errno == ENOENT);
	UT_ASSERT(!storage.contexts[work_slot].structure_drop_pending);
	UT_ASSERT(storage.contexts[work_slot].structure_owned && storage.contexts[work_slot].used);
	UT_ASSERT_EQ(forgets, MAX_FORKNUM);
	UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
	UT_ASSERT_EQ(external_fds, 0);
}

UT_TEST(test_work_success_keeps_main_and_structure_obligation)
{
	int fds;
	if (!work_setup("success"))
		return;
	fds = descriptor_count();
	UT_ASSERT(poll_work());
	assert_finished();
	UT_ASSERT_EQ(descriptor_count(), fds);
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(main_syncs, 1);
	UT_ASSERT_EQ(dir_syncs, 1);
}

UT_TEST(test_native_commit_and_real_smgr_defer_io_to_original_work)
{
	for (unsigned stale = 0; stale < 2; stale++) {
		if (!work_setup_impl(stale ? "native_stale" : "native_commit", true, stale))
			return;
		work_fault = WORK_DIR_SYNC;
		UT_ASSERT(!poll_work());
		UT_ASSERT_EQ(unlinks, MAX_FORKNUM);
		UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
		/* The original backend handle is gone; only retained work may retry. */
		UT_ASSERT(ko_completions == NULL);
		work_fault = WORK_OK;
		UT_ASSERT(poll_work());
		assert_finished();
		UT_ASSERT_EQ(truncates, 1);
		UT_ASSERT_EQ(unlinks, MAX_FORKNUM);
		UT_ASSERT_EQ(dir_syncs, 2);
	}
}

UT_TEST(test_native_checkpointer_poll_has_bounded_visible_failure_and_retry)
{
	uint64 failures;
	if (!work_setup_impl("native_poll", true, false))
		return;
	ko_drop_poll_boundary = cluster_smgr_drop_work_poll;
	MyBackendType = B_BACKEND;
	UT_ASSERT(!run_native_checkpointer());
	MyBackendType = B_CHECKPOINTER;
	native_commit_active = false;
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(opens + truncates + main_syncs + dir_syncs + unlinks + closes, 0);
	native_commit_active = true;
	work_fault = WORK_DIR_SYNC;
	failures = cluster_ko_failclosed_count();
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(dir_syncs, 1);
	UT_ASSERT_EQ(cluster_ko_failclosed_count(), failures + 1);
	UT_ASSERT_EQ(fixture_log_events, 1);
	/* A failed item advances the cursor; there is no retry loop this tick. */
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(dir_syncs, 1);
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(dir_syncs, 2);
	UT_ASSERT_EQ(cluster_ko_failclosed_count(), failures + 2);
	UT_ASSERT_EQ(fixture_log_events, 1);
	work_fault = WORK_OK;
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT(run_native_checkpointer());
	assert_finished();
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(unlinks, MAX_FORKNUM);
	UT_ASSERT_EQ(dir_syncs, 3);
	ko_drop_poll_boundary = NULL;
}

UT_TEST(test_work_absent_auxiliary_forks_are_not_unlink_completions)
{
	int fds;
	struct stat st;
	if (!work_setup("absent_aux"))
		return;
	for (ForkNumber f = FSM_FORKNUM; f <= INIT_FORKNUM; f++)
		UT_ASSERT_EQ(unlink(work_paths[f]), 0);
	fds = descriptor_count();
	UT_ASSERT(poll_work());
	UT_ASSERT_EQ(stat(work_paths[MAIN_FORKNUM], &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);
	UT_ASSERT_EQ(unlinks, 1);
	UT_ASSERT_EQ(per_fork_unlinks[SPACE_FORKNUM], 1);
	UT_ASSERT_EQ(closes, 3);
	UT_ASSERT_EQ(descriptor_count(), fds);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT(!storage.contexts[work_slot].structure_drop_pending);
	UT_ASSERT(storage.contexts[work_slot].structure_owned);
}

static void
retry_fault(enum WorkFault fault, const char *name)
{
	int fds;
	if (!work_setup(name))
		return;
	fds = descriptor_count();
	work_fault = fault;
	UT_ASSERT(!poll_work());
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	UT_ASSERT_EQ(closes, 0);
	/* Original ResourceOwner cleanup does not dispose raw physical state. */
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	work_fault = WORK_OK;
	UT_ASSERT(poll_work());
	assert_finished();
	UT_ASSERT_EQ(truncates, fault == WORK_TRUNCATE ? 2 : 1);
	UT_ASSERT_EQ(main_syncs, fault == WORK_MAIN_SYNC ? 2 : 1);
	UT_ASSERT_EQ(dir_syncs, fault == WORK_DIR_SYNC ? 2 : 1);
	UT_ASSERT_EQ(per_fork_unlinks[FSM_FORKNUM], fault == WORK_AUX ? 2 : 1);
	UT_ASSERT_EQ(per_fork_unlinks[VISIBILITYMAP_FORKNUM], fault == WORK_PARTIAL ? 2 : 1);
	UT_ASSERT_EQ(descriptor_count(), fds);
}
#define RETRY_TEST(name, fault)                                                                    \
	UT_TEST(name)                                                                                  \
	{                                                                                              \
		retry_fault(fault, #name);                                                                 \
	}
RETRY_TEST(test_work_truncate_retry, WORK_TRUNCATE)
RETRY_TEST(test_work_main_fsync_retry, WORK_MAIN_SYNC)
RETRY_TEST(test_work_aux_unlink_retry, WORK_AUX)
RETRY_TEST(test_work_partial_aux_retry, WORK_PARTIAL)
RETRY_TEST(test_work_directory_sync_after_space_unlink_retry, WORK_DIR_SYNC)

UT_TEST(test_work_permanent_failure_is_bounded_and_keeps_responsibility)
{
	if (!work_setup("permanent"))
		return;
	work_fault = WORK_MAIN_SYNC;
	for (unsigned n = 1; n <= 3; n++) {
		UT_ASSERT(!poll_work());
		UT_ASSERT_EQ(main_syncs, n);
		UT_ASSERT_EQ(truncates, 1);
		UT_ASSERT_EQ(unlinks, 0);
		UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	}
	work_fault = WORK_OK;
	UT_ASSERT(poll_work());
}

UT_TEST(test_work_finish_retry_never_reopens_or_repeats_io)
{
	unsigned old_closes;
	if (!work_setup("finish_retry"))
		return;
	work_fault = WORK_FINISH;
	UT_ASSERT(!poll_work());
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	old_closes = closes;
	generation_race = false;
	work_fault = WORK_OK;
	UT_ASSERT(poll_work());
	assert_finished();
	UT_ASSERT_EQ(closes, old_closes);
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(main_syncs, 1);
	UT_ASSERT_EQ(dir_syncs, 1);
	UT_ASSERT_EQ(unlinks, MAX_FORKNUM);
	UT_ASSERT_EQ(opens, MAX_FORKNUM + 2);
}

UT_TEST(test_work_retry_rejects_replacement_aux_without_unlinking_it)
{
	char moved[MAXPGPATH];
	PGIOAlignedBlock bytes;
	struct stat before, after;
	if (!work_setup("replacement_aux"))
		return;
	work_fault = WORK_PARTIAL;
	UT_ASSERT(!poll_work());
	snprintf(moved, sizeof(moved), "%s.old", work_paths[VISIBILITYMAP_FORKNUM]);
	UT_ASSERT_EQ(rename(work_paths[VISIBILITYMAP_FORKNUM], moved), 0);
	memset(bytes.data, 0x7b, BLCKSZ);
	write_file(work_paths[VISIBILITYMAP_FORKNUM], bytes.data);
	UT_ASSERT_EQ(stat(work_paths[VISIBILITYMAP_FORKNUM], &before), 0);
	work_fault = WORK_OK;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(stat(work_paths[VISIBILITYMAP_FORKNUM], &after), 0);
	UT_ASSERT_EQ(before.st_ino, after.st_ino);
	UT_ASSERT_EQ(after.st_size, BLCKSZ);
	UT_ASSERT_EQ(per_fork_unlinks[FSM_FORKNUM], 1);
	UT_ASSERT_EQ(per_fork_unlinks[VISIBILITYMAP_FORKNUM], 1);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
}

UT_TEST(test_work_short_space_and_fsync_off_never_mutate_main)
{
	int fd;
	struct stat st;
	if (!work_setup("short_space"))
		return;
	enableFsync = false;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(opens, 0);
	enableFsync = true;
	fd = open(work_paths[SPACE_FORKNUM], O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, BLCKSZ - 1), 0);
	close(fd);
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(truncates + main_syncs + unlinks + dir_syncs, 0);
	UT_ASSERT_EQ(stat(work_paths[MAIN_FORKNUM], &st), 0);
	UT_ASSERT_EQ(st.st_size, BLCKSZ);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
}

UT_TEST(test_work_scan_completes_next_item_after_first_io_failure)
{
	ClusterKoCompletionV2 *owner = NULL;
	ClusterSpaceStructureChange change;
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint32 cursor;
	bool completed;
	char first_main[MAXPGPATH];
	struct stat st;
	if (!work_setup("scan"))
		return;
	strcpy(first_main, work_paths[MAIN_FORKNUM]);
	UT_ASSERT(cluster_space_structure_wal_decode(storage.contexts[work_slot].structure, sizeof(wal),
												 &change));
	terminal = storage.contexts[work_slot].terminal;
	MyBackendType = B_BACKEND;
	multiple_barriers = true;
	allocated_batch = last_shared_request.batch_id;
	space_identity.key.locator.relNumber++;
	cluster_ko_flush_and_wait_ack(space_identity.key.locator, RELPERSISTENCE_PERMANENT);
	UT_ASSERT(cluster_ko_shared_claim_v2(&space_identity.key, space_identity.incarnation, &owner));
	change.identity.expected.key.locator = change.identity.result.key.locator
		= space_identity.key.locator;
	change.reservation.before.identity.key.locator = change.reservation.result.identity.key.locator
		= space_identity.key.locator;
	terminal.identity.locator = space_identity.key.locator;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &terminal, wal, sizeof(wal)));
	xact_callback(XACT_EVENT_COMMIT, NULL);
	UT_ASSERT(cluster_ko_shared_native_handoff_v2(&owner));
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
	prepare_files(&change);
	cursor = work_slot;
	work_fault = WORK_TRUNCATE;
	UT_ASSERT(cluster_smgr_drop_work_poll(&cursor, &completed));
	UT_ASSERT(!completed);
	work_fault = WORK_OK;
	UT_ASSERT(cluster_smgr_drop_work_poll(&cursor, &completed));
	UT_ASSERT(completed);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	UT_ASSERT(!storage.contexts[cursor - 1].structure_drop_pending);
	UT_ASSERT_EQ(stat(first_main, &st), 0);
	UT_ASSERT_EQ(st.st_size, BLCKSZ);
	UT_ASSERT_EQ(stat(work_paths[MAIN_FORKNUM], &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);
}

static void
cut_fault(enum WorkFault fault, const char *name)
{
	uint32 cursor;
	bool completed = true;
	unsigned old_unlinks, old_syncs;
	if (!work_setup(name))
		return;
	work_fault = fault;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(main_syncs, fault == WORK_CUT_AFTER_TRUNCATE ? 0 : 1);
	UT_ASSERT_EQ(unlinks, fault == WORK_CUT_AFTER_UNLINK ? 1 : 0);
	old_unlinks = unlinks;
	old_syncs = main_syncs;
	cursor = work_slot;
	UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
	UT_ASSERT(completed);
	UT_ASSERT_EQ(cursor, work_slot);
	UT_ASSERT_EQ(main_syncs, old_syncs);
	UT_ASSERT_EQ(unlinks, old_unlinks);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	/* Test cleanup restores the fixture's read-side refusal, never disk state. */
	current_epoch--;
	work_fault = WORK_OK;
	UT_ASSERT(poll_work());
}
#define CUT_TEST(name, fault)                                                                      \
	UT_TEST(name)                                                                                  \
	{                                                                                              \
		cut_fault(fault, #name);                                                                   \
	}
CUT_TEST(test_work_cut_checked_before_main_sync, WORK_CUT_AFTER_TRUNCATE)
CUT_TEST(test_work_cut_checked_before_aux_unlink, WORK_CUT_AFTER_SYNC)
CUT_TEST(test_work_cut_checked_between_aux_unlinks, WORK_CUT_AFTER_UNLINK)

UT_TEST(test_work_ambiguous_unlink_never_treats_enoent_as_its_success)
{
	if (!work_setup("unknown_unlink"))
		return;
	work_fault = WORK_UNLINK_UNKNOWN;
	UT_ASSERT(!poll_work());
	work_fault = WORK_OK;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(unlinks, 1);
	UT_ASSERT_EQ(dir_syncs, 0);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
}

UT_TEST(test_work_close_error_never_finishes_or_retries_unknown_descriptor)
{
	unsigned old_closes;
	if (!work_setup("unknown_close"))
		return;
	work_fault = WORK_CLOSE;
	UT_ASSERT(!poll_work());
	old_closes = closes;
	work_fault = WORK_OK;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(closes, old_closes);
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
}

UT_TEST(test_work_executor_process_exit_does_not_redo_partial_io)
{
	ClusterKoShared *shared;
	pid_t child;
	int status;
	uint32 cursor;
	bool completed = true;
	if (!work_setup("exit"))
		return;
	shared = mmap(NULL, sizeof(*shared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(shared != MAP_FAILED);
	if (shared == MAP_FAILED)
		return;
	memcpy(shared, &storage, sizeof(storage));
	ko_state = shared;
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		MyProcPid = getpid();
		work_fault = WORK_DIR_SYNC;
		if (poll_work() || dir_syncs != 1 || unlinks != MAX_FORKNUM)
			_exit(1);
		exit_callback(0, (Datum)0);
		fflush(stdout);
		_exit(ut_current_failed ? 1 : 0); /* The OS closes this executor's raw descriptors. */
	}
	if (child > 0) {
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		UT_ASSERT_EQ(shared->contexts[work_slot].drop_executor_pid, child);
		MyProcPid = getpid();
		cursor = work_slot;
		UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
		UT_ASSERT_EQ(cursor, work_slot);
		UT_ASSERT(completed);
		UT_ASSERT(shared->contexts[work_slot].structure_drop_pending);
		UT_ASSERT_EQ(truncates + main_syncs + unlinks + dir_syncs, 0);
		UT_ASSERT(access(work_paths[SPACE_FORKNUM], F_OK) < 0 && errno == ENOENT);
	}
	ko_state = &storage;
	munmap(shared, sizeof(*shared));
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_native_commit_and_real_smgr_defer_io_to_original_work);
	UT_RUN(test_native_checkpointer_poll_has_bounded_visible_failure_and_retry);
	printf("# retained storage state: %zu bytes per original work\n",
		   cluster_shared_fs_sharedfs_drop_work_size());
	UT_RUN(test_work_success_keeps_main_and_structure_obligation);
	UT_RUN(test_work_absent_auxiliary_forks_are_not_unlink_completions);
	UT_RUN(test_work_truncate_retry);
	UT_RUN(test_work_main_fsync_retry);
	UT_RUN(test_work_aux_unlink_retry);
	UT_RUN(test_work_partial_aux_retry);
	UT_RUN(test_work_directory_sync_after_space_unlink_retry);
	UT_RUN(test_work_permanent_failure_is_bounded_and_keeps_responsibility);
	UT_RUN(test_work_finish_retry_never_reopens_or_repeats_io);
	UT_RUN(test_work_retry_rejects_replacement_aux_without_unlinking_it);
	UT_RUN(test_work_short_space_and_fsync_off_never_mutate_main);
	UT_RUN(test_work_scan_completes_next_item_after_first_io_failure);
	UT_RUN(test_work_cut_checked_before_main_sync);
	UT_RUN(test_work_cut_checked_before_aux_unlink);
	UT_RUN(test_work_cut_checked_between_aux_unlinks);
	UT_RUN(test_work_ambiguous_unlink_never_treats_enoent_as_its_success);
	UT_RUN(test_work_close_error_never_finishes_or_retries_unknown_descriptor);
	UT_RUN(test_work_executor_process_exit_does_not_redo_partial_io);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
