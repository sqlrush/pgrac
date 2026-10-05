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
#if defined(__linux__)
#include <sys/vfs.h>
#elif defined(__APPLE__)
#include <sys/mount.h>
#endif
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
static unsigned external_fd_limit = 1024;
static bool work_fds[1024];
bool
AcquireExternalFD(void)
{
	if (external_fds >= external_fd_limit)
		return false;
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
	WORK_UNLINK_RENAMED,
	WORK_NFS,
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
	if (work_fault == WORK_UNLINK_RENAMED) {
		UT_ASSERT_EQ(renameat(dir, name, dir, "still-linked-original"), 0);
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

#if defined(__linux__) || defined(__APPLE__)
static int
work_statfs(int fd, struct statfs *out)
{
	int result = fstatfs(fd, out);
	if (result == 0 && work_fault == WORK_NFS) {
#ifdef __linux__
		out->f_type = 0x6969;
#else
		strcpy(out->f_fstypename, "nfs");
#endif
	}
	return result;
}
#endif

#define fstatfs(fd, out) work_statfs(fd, out)
#define ftruncate work_truncate
#define pg_fsync work_sync
#define unlinkat work_unlink
#define open(...) work_open(__VA_ARGS__)
#define openat(dir, name, flags) work_openat(dir, name, flags)
#define close(fd) work_close(fd)
#include "../../backend/cluster/storage/cluster_shared_fs_sharedfs.c"
#undef fstatfs
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
	external_fd_limit = 1024;
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
			UT_ASSERT_EQ(storage.native_waiting, 0);
			current_epoch--;
			UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_RELEASED);
			work_slot = 0;
			return true;
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
assert_work_not_selected(void)
{
	uint32 cursor = work_slot;
	bool completed = true;

	UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
	UT_ASSERT_EQ(cursor, work_slot);
	UT_ASSERT(completed);
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

static void
assert_recovery_required(void)
{
	const char *reason = NULL;
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(reason != NULL && strcmp(reason, "KO_SHARED_DROP_RECOVERY_REQUIRED") == 0);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	UT_ASSERT(storage.contexts[work_slot].structure_owned);
	UT_ASSERT_EQ(storage.contexts[work_slot].drop_executor_pid, MyProcPid);
}

UT_TEST(test_failed_sync_abandons_original_fds_and_requires_recovery)
{
	for (unsigned directory = 0; directory < 2; directory++) {
		int baseline;
		uint32 cursor;
		bool completed = true;
		unsigned io;
		if (!work_setup(directory ? "abandon_directory" : "abandon_main")) return;
		baseline = descriptor_count();
		work_fault = directory ? WORK_DIR_SYNC : WORK_MAIN_SYNC;
		UT_ASSERT(!poll_work());
		UT_ASSERT_EQ(external_fds, 0);
		UT_ASSERT_EQ(descriptor_count(), baseline);
		UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
		assert_recovery_required();
		io = opens + truncates + main_syncs + dir_syncs + unlinks + closes;
		work_fault = WORK_OK;
		for (unsigned tick = 0; tick < 3; tick++) {
			cursor = work_slot;
			UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
			UT_ASSERT_EQ(cursor, work_slot);
			UT_ASSERT(completed);
			UT_ASSERT_EQ(opens + truncates + main_syncs + dir_syncs + unlinks + closes, io);
		}
	}
}

UT_TEST(test_replaced_cut_cleanup_closes_each_original_fd_once)
{
	uint32 cursor;
	bool completed = true;
	unsigned io;
	int baseline;
	if (!work_setup("abandon_cut")) return;
	baseline = descriptor_count();
	work_fault = WORK_PARTIAL;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(external_fds, MAX_FORKNUM + 2);
	current_epoch++;
	io = opens + truncates + main_syncs + dir_syncs + unlinks;
	for (unsigned tick = 0; tick <= CLUSTER_KO_SHARED_CAPACITY; tick++) {
		cursor = work_slot;
		UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
		UT_ASSERT(completed);
	}
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT_EQ(descriptor_count(), baseline);
	UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
	UT_ASSERT_EQ(opens + truncates + main_syncs + dir_syncs + unlinks, io);
	current_epoch--;
	assert_recovery_required();
}

/* Populate each slot through the original backend handoff and promotion. */
static uint32
append_promoted_work(void)
{
	ClusterKoCompletionV2 *owner = NULL;
	ClusterSpaceStructureChange change;
	ClusterPageWalBindingV1 terminal = storage.contexts[work_slot].terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	UT_ASSERT(cluster_space_structure_wal_decode(storage.contexts[work_slot].structure,
												 sizeof(wal), &change));
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
	for (uint32 slot = 0; slot < CLUSTER_KO_SHARED_CAPACITY; slot++)
		if (storage.contexts[slot].structure_drop_pending
			&& RelFileLocatorEquals(storage.contexts[slot].terminal.identity.locator,
									space_identity.key.locator))
			return slot;
	abort();
}

static void
full_work_table_cleanup(bool healthy)
{
	ClusterKoDropWorkV2 *works[CLUSTER_KO_SHARED_CAPACITY] = {0};
	ClusterKoSharedContext retained[CLUSTER_KO_SHARED_CAPACITY];
	uint32 slots[CLUSTER_KO_SHARED_CAPACITY], cursor;
	unsigned held, completed_count = 0;
	int baseline;
	const unsigned retired = CLUSTER_KO_SHARED_CAPACITY - (healthy ? 1 : 0);

	if (!work_setup(healthy ? "full_healthy" : "full_replaced")) return;
	baseline = descriptor_count();
	slots[0] = work_slot;
	for (unsigned i = 1; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		slots[i] = append_promoted_work();
	/* Model ExternalFD pressure without changing the process/OS fd limit. */
	external_fd_limit = 8 * (MAX_FORKNUM + 2);
	work_fault = WORK_AUX;
	for (unsigned i = 0; i < retired; i++) {
		cursor = slots[i];
		UT_ASSERT(cluster_ko_shared_drop_work_begin_v2(
			&cursor, cluster_shared_fs_sharedfs_drop_work_size(), &works[i]));
		unlinks = main_syncs = 0;
		UT_ASSERT(!cluster_shared_fs_sharedfs_drop_work(works[i]));
		retained[i] = storage.contexts[slots[i]];
	}
	held = external_fds;
	UT_ASSERT_EQ(held, external_fd_limit);
	UT_ASSERT_EQ(descriptor_count(), baseline + held);
	if (healthy) {
		/* The original owner may abandon, but that API does not close fds.
		 * All positive handoffs and the final healthy work remain native. */
		for (unsigned i = 0; i < retired; i++)
			UT_ASSERT(cluster_ko_shared_drop_work_abandon_v2(
				works[i], cluster_shared_fs_sharedfs_drop_work_size()) != NULL);
	} else
		current_epoch++;
	work_fault = WORK_OK;
	opens = truncates = main_syncs = dir_syncs = unlinks = closes = 0;
	cursor = 0;
	for (unsigned tick = 0; tick <= CLUSTER_KO_SHARED_CAPACITY; tick++) {
		bool completed = true;
		unsigned old_closes = closes;
		uint32 old_cursor = cursor;
		if (cluster_smgr_drop_work_poll(&cursor, &completed)) {
			UT_ASSERT(healthy);
			UT_ASSERT_EQ(cursor, healthy ? slots[retired] + 1 : old_cursor);
			completed_count += completed ? 1 : 0;
		} else {
			UT_ASSERT_EQ(cursor, old_cursor);
			UT_ASSERT(completed);
			cursor = 0; /* Original native caller's wrap, not the cleanup cursor. */
		}
		UT_ASSERT(closes - old_closes <= 2 * (MAX_FORKNUM + 2));
	}
	UT_ASSERT_EQ(completed_count, healthy ? 1 : 0);
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT_EQ(descriptor_count(), baseline);
	UT_ASSERT_EQ(closes, held + (healthy ? MAX_FORKNUM + 2 : 0));
	UT_ASSERT_EQ(opens, healthy ? MAX_FORKNUM + 2 : 0);
	UT_ASSERT_EQ(truncates, healthy ? 1 : 0);
	UT_ASSERT_EQ(main_syncs, healthy ? 1 : 0);
	UT_ASSERT_EQ(dir_syncs, healthy ? 1 : 0);
	UT_ASSERT_EQ(unlinks, healthy ? MAX_FORKNUM : 0);
	for (unsigned i = 0; i < retired; i++) {
		retained[i].structure_drop_failed = true;
		UT_ASSERT(memcmp(&retained[i], &storage.contexts[slots[i]], sizeof(retained[i])) == 0);
		UT_ASSERT(!cluster_ko_shared_drop_work_finish_v2(&works[i]));
	}
	if (healthy)
		UT_ASSERT(!storage.contexts[slots[retired]].structure_drop_pending);
	else
		current_epoch--;
	assert_recovery_required();
}

UT_TEST(test_full_replaced_work_table_returns_descriptor_credits)
{
	full_work_table_cleanup(false);
}

UT_TEST(test_abandoned_work_scan_does_not_starve_healthy_work)
{
	full_work_table_cleanup(true);
}

UT_TEST(test_unknown_cut_and_wrong_owner_cannot_dispose_retryable_fds)
{
	uint64 epoch, boot;
	ResourceOwner owner;
	unsigned io;
	if (!work_setup("cleanup_polarity")) return;
	work_fault = WORK_PARTIAL;
	UT_ASSERT(!poll_work());
	epoch = current_epoch;
	boot = formation.membership.last_admitted_incarnation[0];
	owner = CurrentResourceOwner;
	io = opens + truncates + main_syncs + dir_syncs + unlinks;
	for (unsigned unknown = 0; unknown < 4; unknown++) {
		capture_ok = unknown != 0;
		current_epoch = unknown == 1 ? 0 : unknown == 3 ? epoch + 1 : epoch;
		formation.membership.last_admitted_incarnation[0] = unknown == 2 ? 0 : boot;
		CurrentResourceOwner = unknown == 3 ? (ResourceOwner)2 : owner;
		for (unsigned tick = 0; tick <= CLUSTER_KO_SHARED_CAPACITY; tick++) {
			uint32 cursor = work_slot;
			bool completed = true;
			UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
			UT_ASSERT_EQ(cursor, work_slot);
			UT_ASSERT(completed);
		}
		UT_ASSERT_EQ(closes, 0);
		UT_ASSERT_EQ(external_fds, MAX_FORKNUM + 2);
		UT_ASSERT_EQ(opens + truncates + main_syncs + dir_syncs + unlinks, io);
		UT_ASSERT(!storage.contexts[work_slot].structure_drop_failed);
	}
	capture_ok = true;
	current_epoch = epoch;
	formation.membership.last_admitted_incarnation[0] = boot;
	CurrentResourceOwner = owner;
	work_fault = WORK_OK;
	UT_ASSERT(poll_work());
	assert_finished();
}

UT_TEST(test_native_commit_and_real_smgr_defer_io_to_original_work)
{
	for (unsigned stale = 0; stale < 2; stale++)
		for (unsigned failed = 0; failed < 2; failed++) {
			char name[64];
			ClusterKoSharedContext retained;

			snprintf(name, sizeof(name), "native_commit_%u_%u", stale, failed);
			if (!work_setup_impl(name, true, stale))
				return;
			/* The backend has handed off without doing pathname I/O. */
			UT_ASSERT(ko_completions == NULL);
			if (stale) {
				const char *reason;
				bool completed = false;
				uint32 cursor = 0;
				struct stat st;
				UT_ASSERT(!cluster_smgr_drop_work_poll(&cursor, &completed));
				UT_ASSERT(!completed && !storage.contexts[0].used);
				UT_ASSERT_EQ(opens + truncates + main_syncs + dir_syncs + unlinks + closes, 0);
				for (unsigned f = 0; f <= MAX_FORKNUM; f++) {
					UT_ASSERT(stat(work_paths[f], &st) == 0);
					UT_ASSERT_EQ(st.st_size, BLCKSZ);
				}
				UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason), CLUSTER_NORMAL_STOP_INVALID);
				UT_ASSERT_EQ(native_allocated, 1);
				native_fixture_shared_memory_end();
				continue;
			}
			if (!failed) {
				UT_ASSERT(poll_work());
				assert_finished();
			} else {
				work_fault = WORK_DIR_SYNC;
				UT_ASSERT(!poll_work());
				retained = storage.contexts[work_slot];
				resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
				work_fault = WORK_OK;
				/* A later successful fsync cannot replace bytes the OS may
				 * have discarded. Native continuation must retain recovery. */
				for (unsigned retry = 0; retry < 3; retry++) {
					assert_work_not_selected();
					UT_ASSERT(memcmp(&retained, &storage.contexts[work_slot],
									 sizeof(retained)) == 0);
					UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
					UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
					UT_ASSERT_EQ(external_fds, 0);
				}
				assert_recovery_required();
			}
			UT_ASSERT_EQ(truncates, 1);
			UT_ASSERT_EQ(main_syncs, 1);
			UT_ASSERT_EQ(unlinks, MAX_FORKNUM);
			UT_ASSERT_EQ(dir_syncs, 1);
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
	/* A failed unlink which left the same original inode can be retried.
	 * Unlike fsync failure, it does not lose a prior durability guarantee. */
	work_fault = WORK_AUX;
	failures = cluster_ko_failclosed_count();
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(unlinks, 1);
	UT_ASSERT_EQ(cluster_ko_failclosed_count(), failures + 1);
	UT_ASSERT_EQ(fixture_log_events, 1);
	/* A failed item advances the cursor; there is no retry loop this tick. */
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(unlinks, 1);
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT_EQ(unlinks, 2);
	UT_ASSERT_EQ(cluster_ko_failclosed_count(), failures + 2);
	UT_ASSERT_EQ(fixture_log_events, 1);
	work_fault = WORK_OK;
	UT_ASSERT(!run_native_checkpointer());
	UT_ASSERT(run_native_checkpointer());
	assert_finished();
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT_EQ(main_syncs, 1);
	UT_ASSERT_EQ(unlinks, MAX_FORKNUM + 2);
	UT_ASSERT_EQ(dir_syncs, 1);
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
RETRY_TEST(test_work_aux_unlink_retry, WORK_AUX)
RETRY_TEST(test_work_partial_aux_retry, WORK_PARTIAL)

static void
sync_failure_requires_recovery(enum WorkFault fault, const char *name)
{
	unsigned old_unlinks, old_syncs;
	if (!work_setup(name))
		return;
	work_fault = fault;
	UT_ASSERT(!poll_work());
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	old_unlinks = unlinks;
	old_syncs = main_syncs + dir_syncs;
	resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	/* The OS may discard dirty data on the first fsync error. A later
	 * successful fsync cannot authorize completion of this work. */
	work_fault = WORK_OK;
	for (unsigned retry = 0; retry < 3; retry++) {
		assert_work_not_selected();
		UT_ASSERT_EQ(main_syncs + dir_syncs, old_syncs);
		UT_ASSERT_EQ(unlinks, old_unlinks);
		UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
		UT_ASSERT(storage.contexts[work_slot].structure_owned);
		UT_ASSERT_EQ(truncates, 1);
	}
	UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
	UT_ASSERT_EQ(external_fds, 0);
	assert_recovery_required();
}

UT_TEST(test_work_main_fsync_failure_requires_recovery)
{
	sync_failure_requires_recovery(WORK_MAIN_SYNC, "main_sync_failure");
}

UT_TEST(test_work_directory_sync_failure_requires_recovery)
{
	sync_failure_requires_recovery(WORK_DIR_SYNC, "directory_sync_failure");
}

UT_TEST(test_work_permanent_failure_is_bounded_and_keeps_responsibility)
{
	if (!work_setup("permanent"))
		return;
	work_fault = WORK_TRUNCATE;
	for (unsigned n = 1; n <= 3; n++) {
		UT_ASSERT(!poll_work());
		UT_ASSERT_EQ(main_syncs, 0);
		UT_ASSERT_EQ(truncates, n);
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
	UT_ASSERT(native_handoff_after_buffers(&owner));
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
	/* Positive replacement disposes the original descriptors in a bounded
	 * cleanup scan. Even a restored fixture cut cannot authorize fresh I/O. */
	for (unsigned tick = 0; tick <= CLUSTER_KO_SHARED_CAPACITY; tick++)
		assert_work_not_selected();
	UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
	UT_ASSERT_EQ(external_fds, 0);
	current_epoch--;
	work_fault = WORK_OK;
	assert_work_not_selected();
	UT_ASSERT_EQ(main_syncs, old_syncs);
	UT_ASSERT_EQ(unlinks, old_unlinks);
	assert_recovery_required();
}
#define CUT_TEST(name, fault)                                                                      \
	UT_TEST(name)                                                                                  \
	{                                                                                              \
		cut_fault(fault, #name);                                                                   \
	}
CUT_TEST(test_work_cut_checked_before_main_sync, WORK_CUT_AFTER_TRUNCATE)
CUT_TEST(test_work_cut_checked_before_aux_unlink, WORK_CUT_AFTER_SYNC)
CUT_TEST(test_work_cut_checked_between_aux_unlinks, WORK_CUT_AFTER_UNLINK)

UT_TEST(test_work_ambiguous_unlink_uses_original_unlinked_inode)
{
	if (!work_setup("unknown_unlink"))
		return;
	work_fault = WORK_UNLINK_UNKNOWN;
	UT_ASSERT(!poll_work());
	work_fault = WORK_OK;
	UT_ASSERT(poll_work());
	assert_finished();
	UT_ASSERT_EQ(per_fork_unlinks[FSM_FORKNUM], 1);
	UT_ASSERT_EQ(unlinks, MAX_FORKNUM);
	UT_ASSERT_EQ(truncates, 1);
}

UT_TEST(test_work_enoent_with_linked_original_never_finishes)
{
	if (!work_setup("renamed_original"))
		return;
	work_fault = WORK_UNLINK_RENAMED;
	UT_ASSERT(!poll_work());
	work_fault = WORK_OK;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(per_fork_unlinks[FSM_FORKNUM], 1);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
}

UT_TEST(test_work_nfs_refused_before_first_mutation)
{
	struct stat st;
	if (!work_setup("nfs"))
		return;
	work_fault = WORK_NFS;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(errno, ENOTSUP);
	UT_ASSERT_EQ(truncates + main_syncs + unlinks + dir_syncs, 0);
	UT_ASSERT_EQ(stat(work_paths[MAIN_FORKNUM], &st), 0);
	UT_ASSERT_EQ(st.st_size, BLCKSZ);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
}

UT_TEST(test_work_close_error_never_finishes_or_retries_unknown_descriptor)
{
	unsigned old_closes;
	int baseline;
	if (!work_setup("unknown_close"))
		return;
	baseline = descriptor_count();
	work_fault = WORK_CLOSE;
	UT_ASSERT(!poll_work());
	UT_ASSERT_EQ(external_fds, 0);
	UT_ASSERT_EQ(closes, MAX_FORKNUM + 2);
	UT_ASSERT_EQ(descriptor_count(), baseline);
	old_closes = closes;
	work_fault = WORK_OK;
	assert_work_not_selected();
	UT_ASSERT_EQ(closes, old_closes);
	UT_ASSERT_EQ(truncates, 1);
	UT_ASSERT(storage.contexts[work_slot].structure_drop_pending);
	assert_recovery_required();
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
	UT_PLAN(27);
	UT_RUN(test_failed_sync_abandons_original_fds_and_requires_recovery);
	UT_RUN(test_replaced_cut_cleanup_closes_each_original_fd_once);
	UT_RUN(test_full_replaced_work_table_returns_descriptor_credits);
	UT_RUN(test_abandoned_work_scan_does_not_starve_healthy_work);
	UT_RUN(test_unknown_cut_and_wrong_owner_cannot_dispose_retryable_fds);
	UT_RUN(test_native_commit_and_real_smgr_defer_io_to_original_work);
	UT_RUN(test_native_checkpointer_poll_has_bounded_visible_failure_and_retry);
	printf("# retained storage state: %zu bytes per original work\n",
		   cluster_shared_fs_sharedfs_drop_work_size());
	UT_RUN(test_work_success_keeps_main_and_structure_obligation);
	UT_RUN(test_work_absent_auxiliary_forks_are_not_unlink_completions);
	UT_RUN(test_work_truncate_retry);
	UT_RUN(test_work_main_fsync_failure_requires_recovery);
	UT_RUN(test_work_aux_unlink_retry);
	UT_RUN(test_work_partial_aux_retry);
	UT_RUN(test_work_directory_sync_failure_requires_recovery);
	UT_RUN(test_work_permanent_failure_is_bounded_and_keeps_responsibility);
	UT_RUN(test_work_finish_retry_never_reopens_or_repeats_io);
	UT_RUN(test_work_retry_rejects_replacement_aux_without_unlinking_it);
	UT_RUN(test_work_short_space_and_fsync_off_never_mutate_main);
	UT_RUN(test_work_scan_completes_next_item_after_first_io_failure);
	UT_RUN(test_work_cut_checked_before_main_sync);
	UT_RUN(test_work_cut_checked_before_aux_unlink);
	UT_RUN(test_work_cut_checked_between_aux_unlinks);
	UT_RUN(test_work_ambiguous_unlink_uses_original_unlinked_inode);
	UT_RUN(test_work_enoent_with_linked_original_never_finishes);
	UT_RUN(test_work_nfs_refused_before_first_mutation);
	UT_RUN(test_work_close_error_never_finishes_or_retries_unknown_descriptor);
	UT_RUN(test_work_executor_process_exit_does_not_redo_partial_io);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
