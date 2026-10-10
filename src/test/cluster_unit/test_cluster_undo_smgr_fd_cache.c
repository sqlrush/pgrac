/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_smgr_fd_cache.c
 *    Exercise undo descriptor reuse through the real storage consumers.
 *
 *    Files, open, pread, pwrite, close and replacement are real.  The fixture
 *    supplies process role, distinct path namespaces, external-FD accounting
 *    and the process-exit callback boundary.  It does not replace the cache
 *    or interpret transaction status.  This is not a worker scheduling test.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_undo_smgr_fd_cache.c
 *
 * NOTES
 *    Uses private temporary files; no database or cluster is started.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_xnode_profile.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_inventory.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

BackendType MyBackendType = B_BACKEND;
int MyProcPid = 4321;
pg_time_t MyStartTime = 0;
TimestampTz MyStartTimestamp = INT64CONST(0x102030405060);
int cluster_node_id = 0;
int max_safe_fds = 192;
int pg_file_create_mode = 0600;
int pg_dir_create_mode = 0700;
ClusterXpService cluster_xp_current_service = CLXP_SERVICE_NONE;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

typedef struct TestOpenedFd {
	int fd;
	bool live;
} TestOpenedFd;

static char test_dir[MAXPGPATH];
static TestOpenedFd opened[1024];
static int opened_count;
static int open_attempts;
static int successful_opens;
static int close_calls;
static int pread_calls;
static bool force_open_error;
static bool force_open_throw;
static bool recovery_admitted = true;
static int external_limit = 32;
static int external_used;
static int external_high_water;
static int external_acquired;
static int external_released;
static pg_on_exit_callback exit_callback;
static Datum exit_arg;
static int exit_registrations;

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	/* Single-threaded interleaving fixture: a contended spinlock is a bug. */
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

/* An unexpected backend ERROR must not become an accepted I/O miss. */
bool
errstart_cold(int level, const char *domain)
{
	return true;
}

int
errcode(int code)
{
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt, ...)
{
	return 0;
}

void
errfinish(const char *file, int line, const char *function)
{
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion failed: %s at %s:%d\n", condition, file, line);
	abort();
}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	exit_registrations++;
	exit_callback = function;
	exit_arg = arg;
}

bool
AcquireExternalFD(void)
{
	if (external_used >= external_limit)
		return false;
	external_used++;
	external_acquired++;
	if (external_used > external_high_water)
		external_high_water = external_used;
	return true;
}

void
ReleaseExternalFD(void)
{
	UT_ASSERT(external_used > 0);
	if (external_used > 0)
		external_used--;
	external_released++;
}

void
cluster_undo_record_note_smgr_open(void)
{}

void
cluster_undo_record_note_smgr_close(void)
{}

void
cluster_undo_record_note_smgr_pread(void)
{
	pread_calls++;
}

void
cluster_undo_record_note_smgr_pwrite(void)
{}

int
BasicOpenFile(const char *path, int flags)
{
	int fd;
	int saved_errno;

	open_attempts++;
	if (force_open_throw) {
		UT_ASSERT(PG_exception_stack != NULL);
		if (PG_exception_stack == NULL)
			abort();
		siglongjmp(*PG_exception_stack, 1);
	}
	if (force_open_error) {
		errno = EACCES;
		return -1;
	}
	fd = open(path, flags, pg_file_create_mode);
	saved_errno = errno;
	if (fd >= 0) {
		UT_ASSERT(opened_count < lengthof(opened));
		if (opened_count >= lengthof(opened))
			abort();
		opened[opened_count++] = (TestOpenedFd){ fd, true };
		successful_opens++;
	}
	errno = saved_errno;
	return fd;
}

static int
test_close(int fd)
{
	int result = close(fd);
	int saved_errno = errno;

	close_calls++;
	UT_ASSERT_EQ(result, 0);
	if (result == 0) {
		for (int i = opened_count - 1; i >= 0; i--) {
			if (opened[i].live && opened[i].fd == fd) {
				opened[i].live = false;
				break;
			}
		}
	}
	errno = saved_errno;
	return result;
}

DIR *
AllocateDir(const char *path)
{
	return opendir(path);
}

int
FreeDir(DIR *dir)
{
	return closedir(dir);
}

int
pg_fsync(int fd)
{
	return fsync(fd);
}

/* The namespaces model the resolver's distinct roots, not its admission
 * implementation.  Only the existing recovery-hit recheck is injected. */
int
cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 owner, uint32 segment, char *path,
						  size_t size)
{
	int written;

	if (intent == CLUSTER_UNDO_PATH_RECOVERY_SHARED && !recovery_admitted) {
		errno = EACCES;
		return -1;
	}
	written = snprintf(path, size, "%s/i%u-o%u-s%u.dat", test_dir, (unsigned)intent,
					   (unsigned)owner, (unsigned)segment);
	return written < 0 || (size_t)written >= size ? -1 : 0;
}

bool
cluster_undo_segment_header_identity_ok(const char *block, uint32 segment, uint8 owner)
{
	return block != NULL && (unsigned char)block[0] == 0xa5;
}

#define close test_close
#include "../../backend/cluster/storage/cluster_undo_smgr.c"
#undef close

static int
live_fds(void)
{
	int count = 0;

	for (int i = 0; i < opened_count; i++) {
		if (opened[i].live) {
			UT_ASSERT(fcntl(opened[i].fd, F_GETFD) >= 0);
			count++;
		}
	}
	return count;
}

/* Observe the original opens, so closing and reusing the same FD number
 * cannot masquerade as retaining a pool pin.  No product cache is inspected. */
static int
initial_pool_pins_live(int pins)
{
	int count = 0;

	UT_ASSERT(opened_count >= pins);
	for (int i = 0; i < pins && i < opened_count; i++) {
		if (opened[i].live) {
			UT_ASSERT(fcntl(opened[i].fd, F_GETFD) >= 0);
			count++;
		}
	}
	return count;
}

static void
require_all_closed(void)
{
	UT_ASSERT_EQ(live_fds(), 0);
	UT_ASSERT_EQ(external_used, 0);
	UT_ASSERT_EQ(external_acquired, external_released);
	UT_ASSERT_EQ(close_calls, successful_opens);
	/* No test-created descriptors are opened between reset and this check. */
	for (int i = 0; i < opened_count; i++) {
		errno = 0;
		UT_ASSERT_EQ(fcntl(opened[i].fd, F_GETFD), -1);
		UT_ASSERT_EQ(errno, EBADF);
	}
}

static bool
begin_case(BackendType role)
{
	char template[] = "/tmp/pgrac-undo-fd-XXXXXX";
	char *created;

	cluster_undo_smgr_fd_cache_reset();
	memset(opened, 0, sizeof(opened));
	opened_count = open_attempts = successful_opens = close_calls = pread_calls = 0;
	force_open_error = false;
	force_open_throw = false;
	recovery_admitted = true;
	external_limit = 32;
	max_safe_fds = 192;
	external_used = external_high_water = external_acquired = external_released = 0;
	MyBackendType = role;
	cluster_node_id = 0;
	created = mkdtemp(template);
	UT_ASSERT_NOT_NULL(created);
	if (created == NULL)
		return false;
	strlcpy(test_dir, created, sizeof(test_dir));
	return true;
}

static void
finish_case(void)
{
	DIR *dir;
	struct dirent *entry;

	cluster_undo_smgr_fd_cache_reset();
	require_all_closed();
	/* Clean only files in this case's private directory.  A failed product
 * close is reported above before the fixture releases its leaked handle. */
	for (int i = 0; i < opened_count; i++) {
		if (opened[i].live) {
			(void)close(opened[i].fd);
			opened[i].live = false;
		}
	}
	dir = opendir(test_dir);
	UT_ASSERT_NOT_NULL(dir);
	if (dir == NULL)
		return;
	while ((entry = readdir(dir)) != NULL) {
		char path[MAXPGPATH];

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		UT_ASSERT(snprintf(path, sizeof(path), "%s/%s", test_dir, entry->d_name) < sizeof(path));
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(rmdir(test_dir), 0);
	test_dir[0] = '\0';
}

static void
seed_path(const char *path, unsigned char marker)
{
	char page[BLCKSZ];
	int fd;

	memset(page, marker, sizeof(page));
	fd = open(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(pwrite(fd, page, sizeof(page), 0), sizeof(page));
	UT_ASSERT_EQ(close(fd), 0);
}

static void
seed_segment(ClusterUndoPathIntent intent, uint8 owner, uint32 segment, unsigned char marker)
{
	char path[MAXPGPATH];

	UT_ASSERT_EQ(cluster_undo_path_resolve(intent, owner, segment, path, sizeof(path)), 0);
	seed_path(path, marker);
}

static void
expect_read(ClusterUndoPathIntent intent, uint8 owner, uint32 segment, unsigned char marker,
			bool header_bytes)
{
	char actual[BLCKSZ];
	char expected[BLCKSZ];
	size_t size = header_bytes ? 32 : BLCKSZ;
	bool ok;

	memset(actual, 0xee, sizeof(actual));
	memset(expected, marker, sizeof(expected));
	if (header_bytes)
		ok = cluster_undo_smgr_read_header_bytes(intent, segment, owner, 128, actual, (uint32)size);
	else
		ok = cluster_undo_smgr_read_block(intent, segment, owner, 0, actual);
	UT_ASSERT(ok);
	if (ok)
		UT_ASSERT_EQ(memcmp(actual, expected, size), 0);
}

static void
exercise_two_local_segments(BackendType role)
{
	if (!begin_case(role))
		return;
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x31);
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 2, 0x42);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x31, false);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 2, 0x42, true);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x31, true);
	/* The old single-descriptor implementation succeeds on every read but
 * opens three times.  This is the behavioral RED, not a missing API. */
	UT_ASSERT_EQ(successful_opens, 2);
	UT_ASSERT_EQ(open_attempts, 2);
	UT_ASSERT_EQ(pread_calls, 3);
	UT_ASSERT_EQ(live_fds(), 2);
	UT_ASSERT_EQ(external_used, 2);
	UT_ASSERT_EQ(exit_registrations, 1);
	finish_case();
}

UT_TEST(test_lms_own_runtime_reuses_two_descriptors)
{
	exercise_two_local_segments(B_LMS);
}

UT_TEST(test_worker_own_runtime_reuses_two_descriptors)
{
	exercise_two_local_segments(B_LMS_WORKER);
}

static void
exercise_77_existing_segments(BackendType role)
{
	if (!begin_case(role))
		return;
	/* 462 safe FDs allow 154 external reservations: 77 for this pool and
	 * 77 for other users.  All files exist before either real read sweep. */
	max_safe_fds = 462;
	external_limit = 154;
	for (uint32 segment = 1; segment <= 77; segment++)
		seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment);
	for (int sweep = 0; sweep < 2; sweep++) {
		int opens_before = open_attempts;
		int successes_before = successful_opens;

		for (uint32 segment = 1; segment <= 77; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment,
						sweep != 0);
		printf("# fd-pool role=%s segments=77 sweep=%d new_opens=%d live=%d external=%d\n",
			   role == B_LMS ? "lms" : "worker", sweep + 1, open_attempts - opens_before,
			   live_fds(), external_used);
		UT_ASSERT_EQ(open_attempts - opens_before, sweep == 0 ? 77 : 0);
		UT_ASSERT_EQ(successful_opens - successes_before, sweep == 0 ? 77 : 0);
		UT_ASSERT_EQ(pread_calls, (sweep + 1) * 77);
		UT_ASSERT_EQ(live_fds(), 77);
		UT_ASSERT_EQ(external_used, 77);
		UT_ASSERT_EQ(close_calls, 0);
	}
	UT_ASSERT_EQ(external_high_water, 77);
	finish_case();
}

UT_TEST(test_lms_77_existing_segments_second_sweep_never_reopens)
{
	exercise_77_existing_segments(B_LMS);
}

UT_TEST(test_worker_77_existing_segments_second_sweep_never_reopens)
{
	exercise_77_existing_segments(B_LMS_WORKER);
}

static void
exercise_77_segments_with_40_pool_pins(BackendType role)
{
	if (!begin_case(role))
		return;
	/* 40 pool reservations, another 40 for external users, and the original
	 * single-FD path for the 37 files that cannot enter this pool. */
	max_safe_fds = 240;
	external_limit = 80;
	for (uint32 segment = 1; segment <= 77; segment++)
		seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment);
	for (int sweep = 0; sweep < 3; sweep++) {
		int opens_before = open_attempts;
		int successes_before = successful_opens;
		int pool_opens;

		for (uint32 segment = 1; segment <= 40; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment,
						sweep != 0);
		pool_opens = open_attempts - opens_before;
		for (uint32 segment = 41; segment <= 77; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment,
						sweep != 0);
		printf("# fd-pins role=%s sweep=%d pool_opens=%d overflow_opens=%d pins=%d external=%d\n",
			   role == B_LMS ? "lms" : "worker", sweep + 1, pool_opens,
			   open_attempts - opens_before - pool_opens, initial_pool_pins_live(40),
			   external_used);
		UT_ASSERT_EQ(pool_opens, sweep == 0 ? 40 : 0);
		UT_ASSERT_EQ(open_attempts - opens_before - pool_opens, 37);
		UT_ASSERT_EQ(successful_opens - successes_before, sweep == 0 ? 77 : 37);
		UT_ASSERT_EQ(pread_calls, (sweep + 1) * 77);
		UT_ASSERT_EQ(initial_pool_pins_live(40), 40);
		UT_ASSERT_EQ(live_fds(), 41);
		UT_ASSERT_EQ(external_used, 40);
		UT_ASSERT_EQ(external_high_water, 40);
		UT_ASSERT_EQ(external_acquired, 40);
		UT_ASSERT_EQ(external_released, 0);
	}
	finish_case();
}

UT_TEST(test_lms_77_segments_reopen_only_37_overflow_files_per_warm_sweep)
{
	exercise_77_segments_with_40_pool_pins(B_LMS);
}

UT_TEST(test_worker_77_segments_reopen_only_37_overflow_files_per_warm_sweep)
{
	exercise_77_segments_with_40_pool_pins(B_LMS_WORKER);
}

UT_TEST(test_full_local_segment_range_preserves_descriptor_headroom)
{
	const int nodes[] = { 0, UNDO_OWNER_INSTANCE_MAX - 1 };

	for (int n = 0; n < lengthof(nodes); n++) {
		uint32 first = (uint32)nodes[n] * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		uint8 owner = (uint8)(nodes[n] + 1);

		if (!begin_case(n == 0 ? B_LMS : B_LMS_WORKER))
			return;
		cluster_node_id = nodes[n];
		/* At and above the budget needed for all 256 local segments, the
		 * pool retains the whole working set, including the last owner. */
		max_safe_fds = n == 0 ? 1536 : 3072;
		external_limit = n == 0 ? 512 : 1024;
		for (uint32 i = 0; i < CLUSTER_UNDO_SEGS_PER_INSTANCE; i++)
			seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner, first + i, (unsigned char)i);
		for (int sweep = 0; sweep < 2; sweep++) {
			int opens_before = open_attempts;

			for (uint32 i = 0; i < CLUSTER_UNDO_SEGS_PER_INSTANCE; i++)
				expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, owner, first + i, (unsigned char)i,
							sweep != 0);
			printf("# fd-pool owner=%u segments=256 sweep=%d new_opens=%d live=%d external=%d\n",
				   (unsigned)owner, sweep + 1, open_attempts - opens_before, live_fds(),
				   external_used);
			UT_ASSERT_EQ(open_attempts - opens_before, sweep == 0 ? 256 : 0);
			UT_ASSERT_EQ(open_attempts, 256);
			UT_ASSERT_EQ(successful_opens, 256);
			UT_ASSERT_EQ(pread_calls, (sweep + 1) * 256);
			UT_ASSERT_EQ(live_fds(), 256);
			UT_ASSERT_EQ(external_used, 256);
			UT_ASSERT_EQ(close_calls, 0);
		}
		UT_ASSERT_EQ(external_high_water, 256);
		/* The entire other half of the external budget remains reservable. */
		for (int i = 0; i < external_limit / 2; i++)
			UT_ASSERT(AcquireExternalFD());
		for (int i = 0; i < external_limit / 2; i++)
			ReleaseExternalFD();
		finish_case();
	}
}

UT_TEST(test_low_fd_budget_leaves_room_for_wait_event_sets)
{
	static const struct {
		int safe;
		int external;
		int retained;
	} budgets[] = { { 47, 15, 7 }, { 48, 16, 8 } };

	for (int b = 0; b < lengthof(budgets); b++) {
		if (!begin_case(B_LMS))
			return;
		max_safe_fds = budgets[b].safe;
		external_limit = budgets[b].external;
		/* Existing resources consume part of the half reserved for others. */
		for (int i = 0; i < 4; i++)
			UT_ASSERT(AcquireExternalFD());
		for (uint32 segment = 1; segment <= 32; segment++) {
			seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment);
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment,
						false);
		}
		UT_ASSERT_EQ(external_high_water, 4 + budgets[b].retained);
		UT_ASSERT_EQ(live_fds(), budgets[b].retained + 1);
		UT_ASSERT_EQ(initial_pool_pins_live(budgets[b].retained), budgets[b].retained);
		for (uint32 segment = 1; segment <= budgets[b].retained; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, true);
		UT_ASSERT_EQ(open_attempts, 32);
		UT_ASSERT_EQ(external_acquired, 4 + budgets[b].retained);
		UT_ASSERT_EQ(external_released, 0);
		/* Both hand-calculated budgets still admit four wait-event resources. */
		for (int i = 0; i < 4; i++)
			UT_ASSERT(AcquireExternalFD());
		UT_ASSERT(!AcquireExternalFD());
		for (int i = 0; i < 8; i++)
			ReleaseExternalFD();
		finish_case();
	}
}

UT_TEST(test_existing_external_users_limit_pool_without_losing_reads)
{
	if (!begin_case(B_LMS_WORKER))
		return;
	max_safe_fds = 48;
	external_limit = 16;
	for (int i = 0; i < 13; i++)
		UT_ASSERT(AcquireExternalFD());
	for (uint32 segment = 1; segment <= 5; segment++) {
		seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment);
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, false);
	}
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 5, 5, true);
	UT_ASSERT_EQ(open_attempts, 5);
	UT_ASSERT_EQ(pread_calls, 6);
	UT_ASSERT_EQ(close_calls, 1);
	UT_ASSERT_EQ(live_fds(), 4);
	UT_ASSERT_EQ(initial_pool_pins_live(3), 3);
	UT_ASSERT_EQ(external_used, 16);
	UT_ASSERT_EQ(external_high_water, 16);
	UT_ASSERT_EQ(external_acquired, 16);
	UT_ASSERT_EQ(external_released, 0);
	UT_ASSERT(!AcquireExternalFD());
	for (uint32 segment = 1; segment <= 3; segment++)
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, true);
	UT_ASSERT_EQ(open_attempts, 5);
	cluster_undo_smgr_fd_cache_reset();
	UT_ASSERT_EQ(live_fds(), 0);
	UT_ASSERT_EQ(external_used, 13);
	UT_ASSERT_EQ(external_acquired - external_released, 13);
	for (int i = 0; i < 13; i++)
		ReleaseExternalFD();
	finish_case();
}

UT_TEST(test_budget_reduction_or_new_external_users_preserve_pool_pins)
{
	for (int pressure = 0; pressure < 2; pressure++) {
		int occupied = pressure == 0 ? 0 : 120;
		int opens_before;

		if (!begin_case(pressure == 0 ? B_LMS : B_LMS_WORKER))
			return;
		max_safe_fds = pressure == 0 ? 240 : 480;
		external_limit = pressure == 0 ? 80 : 160;
		for (uint32 segment = 1; segment <= 43; segment++)
			seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment);
		for (uint32 segment = 1; segment <= 40; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment,
						false);
		UT_ASSERT_EQ(open_attempts, 40);
		if (pressure == 0) {
			/* New pool allowance is 20, but the 40 existing reservations
			 * still fit the new external limit.  Do not acquire or evict. */
			max_safe_fds = 120;
			external_limit = 40;
		} else {
			/* Other users take the remaining budget after the pool warms;
			 * capacity is still 80, so this exercises AcquireExternalFD refusal. */
			for (int i = 0; i < occupied; i++)
				UT_ASSERT(AcquireExternalFD());
		}
		for (uint32 segment = 41; segment <= 43; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, true);
		UT_ASSERT_EQ(open_attempts, 43);
		UT_ASSERT_EQ(initial_pool_pins_live(40), 40);
		UT_ASSERT_EQ(live_fds(), 41);
		UT_ASSERT_EQ(external_used, 40 + occupied);
		UT_ASSERT_EQ(external_high_water, 40 + occupied);
		UT_ASSERT_EQ(external_acquired, 40 + occupied);
		UT_ASSERT_EQ(external_released, 0);
		opens_before = open_attempts;
		for (uint32 segment = 1; segment <= 40; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, true);
		UT_ASSERT_EQ(open_attempts, opens_before);
		UT_ASSERT_EQ(initial_pool_pins_live(40), 40);
		UT_ASSERT_EQ(external_used, 40 + occupied);
		cluster_undo_smgr_fd_cache_reset();
		UT_ASSERT_EQ(live_fds(), 0);
		UT_ASSERT_EQ(external_used, occupied);
		for (int i = 0; i < occupied; i++)
			ReleaseExternalFD();
		finish_case();
	}
}

UT_TEST(test_other_roles_owners_and_intents_keep_single_descriptor)
{
	static const struct {
		BackendType role;
		ClusterUndoPathIntent intent;
		uint8 owner;
		uint32 first_segment;
	} cases[] = {
		{ B_BACKEND, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1 },
		{ B_LMS, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 257 },
		{ B_LMS_WORKER, CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 1, 1 },
		{ B_LMS, CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0, 2, 257 },
		{ B_LMS_WORKER, CLUSTER_UNDO_PATH_RECOVERY_SHARED, 2, 257 },
	};

	for (int i = 0; i < lengthof(cases); i++) {
		if (!begin_case(cases[i].role))
			return;
		seed_segment(cases[i].intent, cases[i].owner, cases[i].first_segment, 0x51);
		seed_segment(cases[i].intent, cases[i].owner, cases[i].first_segment + 1, 0x62);
		expect_read(cases[i].intent, cases[i].owner, cases[i].first_segment, 0x51, false);
		expect_read(cases[i].intent, cases[i].owner, cases[i].first_segment + 1, 0x62, true);
		expect_read(cases[i].intent, cases[i].owner, cases[i].first_segment, 0x51, false);
		UT_ASSERT_EQ(successful_opens, 3);
		UT_ASSERT_EQ(live_fds(), 1);
		UT_ASSERT_EQ(external_acquired, 0);
		finish_case();
	}
}

UT_TEST(test_owner_and_intent_namespaces_never_share_file_bytes)
{
	if (!begin_case(B_LMS))
		return;
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x11);
	seed_segment(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 1, 1, 0x22);
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 257, 0x33);
	seed_segment(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 2, 257, 0x44);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x11, false);
	expect_read(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 1, 1, 0x22, false);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 257, 0x33, true);
	expect_read(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 2, 257, 0x44, true);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x11, true);
	finish_case();
}

UT_TEST(test_recovery_cached_descriptor_never_retains_admission)
{
	char actual[BLCKSZ];

	if (!begin_case(B_LMS_WORKER))
		return;
	seed_segment(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 2, 257, 0x55);
	expect_read(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 2, 257, 0x55, false);
	recovery_admitted = false;
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 257, 2, 0, actual));
	UT_ASSERT_EQ(pread_calls, 1);
	finish_case();
}

UT_TEST(test_failed_open_is_not_cached_and_later_creation_is_readable)
{
	char actual[BLCKSZ];

	if (!begin_case(B_LMS))
		return;
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x66);
	force_open_error = true;
	for (int i = 0; i < 2; i++)
		UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, actual));
	UT_ASSERT_EQ(open_attempts, 2);
	UT_ASSERT_EQ(successful_opens, 0);
	UT_ASSERT_EQ(external_used, 0);
	force_open_error = false;
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x66, false);
	UT_ASSERT(!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 1, 0, actual));
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 2, 0x77);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 2, 0x77, true);
	UT_ASSERT_EQ(successful_opens, 2);
	finish_case();
}

UT_TEST(test_open_error_rethrows_and_releases_external_fd_reservation)
{
	char actual[BLCKSZ];
	volatile bool caught = false;

	if (!begin_case(B_LMS_WORKER))
		return;
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x69);
	force_open_throw = true;
	PG_TRY();
	{
		(void)cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, actual);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(open_attempts, 1);
	UT_ASSERT_EQ(pread_calls, 0);
	UT_ASSERT_EQ(external_used, 0);
	UT_ASSERT_EQ(external_acquired, external_released);
	force_open_throw = false;
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x69, true);
	finish_case();
}

static void
exercise_overflow_open_failure(bool throw_error)
{
	char actual[BLCKSZ];
	volatile bool caught = false;
	volatile bool read_ok = false;
	volatile int read_errno = 0;

	if (!begin_case(B_LMS_WORKER))
		return;
	max_safe_fds = 48;
	external_limit = 16;
	for (uint32 segment = 1; segment <= 10; segment++)
		seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment);
	for (uint32 segment = 1; segment <= 8; segment++)
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, false);
	/* A previous overflow FD must close before a different overflow open
	 * fails.  None of the eight original pool pins may be affected. */
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 9, 9, true);
	UT_ASSERT_EQ(live_fds(), 9);
	UT_ASSERT_EQ(initial_pool_pins_live(8), 8);
	force_open_throw = throw_error;
	force_open_error = !throw_error;
	PG_TRY();
	{
		read_ok = cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 10, 1, 0, actual);
		read_errno = errno;
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(caught, throw_error);
	UT_ASSERT(!read_ok);
	if (!throw_error)
		UT_ASSERT_EQ(read_errno, EACCES);
	UT_ASSERT_EQ(open_attempts, 10);
	UT_ASSERT_EQ(successful_opens, 9);
	UT_ASSERT_EQ(pread_calls, 9);
	UT_ASSERT_EQ(live_fds(), 8);
	UT_ASSERT_EQ(initial_pool_pins_live(8), 8);
	UT_ASSERT_EQ(external_used, 8);
	UT_ASSERT_EQ(external_acquired, 8);
	UT_ASSERT_EQ(external_released, 0);
	UT_ASSERT_EQ(close_calls, 1);
	force_open_throw = false;
	force_open_error = false;
	for (uint32 segment = 1; segment <= 8; segment++)
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, (unsigned char)segment, true);
	UT_ASSERT_EQ(open_attempts, 10);
	/* Failure is not cached; a successful retry then hits the single FD. */
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 10, 10, true);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 10, 10, false);
	UT_ASSERT_EQ(open_attempts, 11);
	UT_ASSERT_EQ(live_fds(), 9);
	UT_ASSERT_EQ(external_used, 8);
	UT_ASSERT_EQ(external_acquired, 8);
	UT_ASSERT_EQ(external_released, 0);
	UT_ASSERT_EQ(initial_pool_pins_live(8), 8);
	UT_ASSERT(exit_callback != NULL);
	if (exit_callback != NULL)
		exit_callback(0, exit_arg);
	require_all_closed();
	finish_case();
}

UT_TEST(test_overflow_open_error_preserves_pool_reservations)
{
	exercise_overflow_open_failure(true);
}

UT_TEST(test_overflow_open_failure_preserves_pool_reservations)
{
	exercise_overflow_open_failure(false);
}

UT_TEST(test_in_place_recycle_is_visible_through_cached_descriptor)
{
	char path[MAXPGPATH];
	char page[BLCKSZ];
	int fd;

	if (!begin_case(B_LMS_WORKER))
		return;
	seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x21);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x21, false);
	UT_ASSERT_EQ(
		cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, path, sizeof(path)), 0);
	fd = open(path, O_RDWR | PG_BINARY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		memset(page, 0x32, sizeof(page));
		UT_ASSERT_EQ(pwrite(fd, page, sizeof(page), 0), sizeof(page));
		UT_ASSERT_EQ(close(fd), 0);
	}
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x32, false);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x32, true);
	UT_ASSERT_EQ(successful_opens, 1);
	finish_case();
}

UT_TEST(test_reset_closes_all_descriptors_and_reopens_replaced_path)
{
	char path[MAXPGPATH];
	char replacement[MAXPGPATH];
	struct stat before;
	struct stat after;
	int closed;

	if (!begin_case(B_LMS))
		return;
	for (uint32 segment = 1; segment <= 3; segment++) {
		seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, 0x41);
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, 0x41, false);
	}
	UT_ASSERT_EQ(
		cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, path, sizeof(path)), 0);
	UT_ASSERT_EQ(stat(path, &before), 0);
	UT_ASSERT(snprintf(replacement, sizeof(replacement), "%s/replacement", test_dir)
			  < sizeof(replacement));
	seed_path(replacement, 0x52);
	cluster_undo_smgr_fd_cache_reset();
	require_all_closed();
	closed = close_calls;
	cluster_undo_smgr_fd_cache_reset();
	UT_ASSERT_EQ(close_calls, closed);
	/* Published undo is recycled in place.  A path replacement in this
 * fixture is intentionally separated from cached reads by explicit reset. */
	UT_ASSERT_EQ(rename(replacement, path), 0);
	UT_ASSERT_EQ(stat(path, &after), 0);
	UT_ASSERT(before.st_dev != after.st_dev || before.st_ino != after.st_ino);
	expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x52, false);
	UT_ASSERT_EQ(successful_opens, 4);
	finish_case();
}

UT_TEST(test_external_fd_budget_refusal_preserves_reads_and_balances_cleanup)
{
	static const struct {
		int safe;
		int limit;
		int occupied;
		int pool;
	} budgets[] = { { 192, 0, 0, 0 }, { 192, 1, 0, 1 }, { 5, 1, 0, 0 }, { 48, 16, 16, 0 } };

	for (int b = 0; b < lengthof(budgets); b++) {
		if (!begin_case(B_LMS_WORKER))
			return;
		max_safe_fds = budgets[b].safe;
		external_limit = budgets[b].limit;
		for (int i = 0; i < budgets[b].occupied; i++)
			UT_ASSERT(AcquireExternalFD());
		for (uint32 segment = 1; segment <= 3; segment++)
			seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment,
						 (unsigned char)(0x60 + segment));
		for (uint32 segment = 1; segment <= 3; segment++)
			expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment,
						(unsigned char)(0x60 + segment), false);
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x61, true);
		/* With one pool slot, segment 1 stays pooled while 2/3 use the
		 * single FD.  With no pool slots, all reads use the original path. */
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0x61, false);
		UT_ASSERT_EQ(open_attempts, budgets[b].pool == 0 ? 4 : 3);
		UT_ASSERT_EQ(successful_opens, budgets[b].pool == 0 ? 4 : 3);
		UT_ASSERT_EQ(pread_calls, 5);
		UT_ASSERT_EQ(close_calls, budgets[b].pool == 0 ? 3 : 1);
		UT_ASSERT_EQ(live_fds(), 1 + budgets[b].pool);
		UT_ASSERT_EQ(initial_pool_pins_live(budgets[b].pool), budgets[b].pool);
		UT_ASSERT_EQ(external_used, budgets[b].occupied + budgets[b].pool);
		UT_ASSERT_EQ(external_acquired, budgets[b].occupied + budgets[b].pool);
		UT_ASSERT_EQ(external_released, 0);
		UT_ASSERT(external_high_water <= budgets[b].limit);
		cluster_undo_smgr_fd_cache_reset();
		UT_ASSERT_EQ(external_used, budgets[b].occupied);
		for (int i = 0; i < budgets[b].occupied; i++)
			ReleaseExternalFD();
		finish_case();
	}
}

UT_TEST(test_registered_exit_callback_closes_pool_and_single_descriptor)
{
	if (!begin_case(B_LMS))
		return;
	for (uint32 segment = 1; segment <= 3; segment++) {
		seed_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, 0x71);
		expect_read(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, segment, 0x71, false);
	}
	seed_segment(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 2, 257, 0x72);
	expect_read(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 2, 257, 0x72, true);
	UT_ASSERT_EQ(exit_registrations, 1);
	UT_ASSERT(exit_callback != NULL);
	if (exit_callback != NULL)
		exit_callback(0, exit_arg);
	require_all_closed();
	finish_case();
}

UT_TEST(test_inventory_requires_complete_unchanged_baseline_and_preserves_gaps)
{
	ClusterUndoInventory state;
	ClusterUndoInventorySnapshot snapshot;
	uint64 seen[CLUSTER_UNDO_INVENTORY_WORDS] = { UINT64CONST(0x8001), 0, 0, 0 };

	if (!begin_case(B_LMS))
		return;
	cluster_undo_inventory_attach(&state, true);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(snapshot.tracked && !snapshot.usable);
	(void)cluster_undo_inventory_finish(&snapshot, seen, false);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.usable);
	UT_ASSERT(cluster_undo_inventory_finish(&snapshot, seen, true));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(snapshot.usable);
	UT_ASSERT_EQ(snapshot.published[0], UINT64CONST(0x8001));
	/* A later incomplete observation must not remove a known published file. */
	seen[0] = 1;
	UT_ASSERT(!cluster_undo_inventory_finish(&snapshot, seen, true));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT_EQ(snapshot.published[0], UINT64CONST(0x8001));
	UT_ASSERT(!snapshot.usable);
	cluster_undo_inventory_attach(NULL, false);
	finish_case();
}

UT_TEST(test_inventory_publication_is_invisible_until_readable_and_all_writers_finish)
{
	ClusterUndoInventory state;
	ClusterUndoInventorySnapshot before, during, after;
	uint64 seen[CLUSTER_UNDO_INVENTORY_WORDS] = { 1, 0, 0, 0 };

	if (!begin_case(B_LMS))
		return;
	cluster_undo_inventory_attach(&state, true);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &before);
	UT_ASSERT(cluster_undo_inventory_finish(&before, seen, true));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &before);
	UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &during);
	UT_ASSERT(!during.usable && during.tracked && during.publishing);
	UT_ASSERT_EQ(state.published[0], 1);
	cluster_undo_inventory_publish_end(16, true);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &during);
	UT_ASSERT(!during.usable);
	cluster_undo_inventory_publish_end(256, true);
	UT_ASSERT(!cluster_undo_inventory_finish(&before, seen, true));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &after);
	UT_ASSERT(after.usable);
	UT_ASSERT_EQ(after.published[0], UINT64CONST(0x8001));
	UT_ASSERT_EQ(after.published[3], UINT64CONST(1) << 63);
	cluster_undo_inventory_attach(NULL, false);
	finish_case();
}

UT_TEST(test_inventory_failed_publication_recovery_and_wrong_scope_cannot_skip)
{
	ClusterUndoInventory state;
	ClusterUndoInventorySnapshot snapshot;
	uint64 seen[CLUSTER_UNDO_INVENTORY_WORDS] = { 1, 0, 0, 0 };

	if (!begin_case(B_LMS))
		return;
	cluster_undo_inventory_attach(&state, true);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(cluster_undo_inventory_finish(&snapshot, seen, true));
	UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	cluster_undo_inventory_publish_end(16, false);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.usable);
	UT_ASSERT_EQ(state.publishers, 0);
	UT_ASSERT_EQ(state.published[0], 1);
	UT_ASSERT(cluster_undo_inventory_finish(&snapshot, seen, true));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.tracked && !snapshot.usable);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, &snapshot);
	UT_ASSERT(!snapshot.tracked && !snapshot.usable);
	cluster_undo_inventory_disable(1);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.tracked && !snapshot.usable);
	UT_ASSERT(!cluster_undo_inventory_finish(&snapshot, seen, true));
	cluster_undo_inventory_attach(&state, true);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(snapshot.tracked && !snapshot.usable);
	cluster_undo_inventory_attach(NULL, false);
	finish_case();
}

UT_TEST(test_inventory_namespace_change_and_generation_exhaustion_fall_back)
{
	ClusterUndoInventory state;
	ClusterUndoInventorySnapshot snapshot;
	uint64 seen[CLUSTER_UNDO_INVENTORY_WORDS] = { 1, 0, 0, 0 };
	char saved[MAXPGPATH];

	if (!begin_case(B_LMS))
		return;
	cluster_undo_inventory_attach(&state, true);
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(cluster_undo_inventory_finish(&snapshot, seen, true));
	strlcpy(saved, test_dir, sizeof(saved));
	strlcpy(test_dir, "/other-root", sizeof(test_dir));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.usable && !snapshot.tracked);
	strlcpy(test_dir, saved, sizeof(test_dir));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.usable && !snapshot.tracked);
	cluster_undo_inventory_attach(&state, true);
	state.serial = UINT64_MAX;
	UT_ASSERT(!cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
	UT_ASSERT(!snapshot.usable && !snapshot.tracked);
	cluster_undo_inventory_attach(NULL, false);
	finish_case();
}


UT_TEST(test_inventory_disable_counter_counts_each_transition_once)
{
	ClusterUndoInventory state;
	ClusterUndoInventorySnapshot snapshot;
	ClusterUndoInventoryStats stats;
	char saved[MAXPGPATH];

	if (!begin_case(B_LMS))
		return;
	strlcpy(saved, test_dir, sizeof(saved));
	for (int reason = 0; reason < 6; reason++) {
		cluster_undo_inventory_attach(&state, true);
		cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
		UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
		UT_ASSERT_EQ(stats.disable_count, 0);
		/* A foreign recovery does not disable this node's inventory. */
		cluster_undo_inventory_disable(2);
		UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
		UT_ASSERT_EQ(stats.disable_count, 0);
		switch (reason) {
		case 0:
			cluster_undo_inventory_disable(1);
			break;
		case 1:
			strlcpy(test_dir, "/other-root", sizeof(test_dir));
			cluster_undo_inventory_snapshot(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, &snapshot);
			strlcpy(test_dir, saved, sizeof(test_dir));
			break;
		case 2:
			state.serial = UINT64_MAX;
			UT_ASSERT(!cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
			break;
		case 3:
			state.publishers = UINT32_MAX;
			UT_ASSERT(!cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
			break;
		case 4:
			UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
			state.serial = UINT64_MAX;
			/* Both exhausted serial and invalid segment must count only once. */
			cluster_undo_inventory_publish_end(0, true);
			break;
		case 5:
			UT_ASSERT(cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
			cluster_undo_inventory_publish_end(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, true);
			break;
		}
		cluster_undo_inventory_disable(1);
		cluster_undo_inventory_disable(1);
		UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
		UT_ASSERT_EQ(stats.disable_count, 1);
		UT_ASSERT_EQ(stats.bitmap_hit_count, 0);
		UT_ASSERT_EQ(stats.full_scan_count, 0);
		UT_ASSERT(state.disabled);
	}
	cluster_undo_inventory_attach(NULL, false);
	finish_case();
}

UT_TEST(test_inventory_stats_attach_preserves_counts_and_absence_is_explicit)
{
	ClusterUndoInventory state;
	ClusterUndoInventoryStats stats;

	cluster_undo_inventory_attach(NULL, false);
	cluster_undo_inventory_count_scan(true);
	cluster_undo_inventory_count_scan(false);
	UT_ASSERT(!cluster_undo_inventory_read_stats(NULL));
	memset(&stats, 0xff, sizeof(stats));
	UT_ASSERT(!cluster_undo_inventory_read_stats(&stats));
	UT_ASSERT_EQ(stats.bitmap_hit_count, 0);
	UT_ASSERT_EQ(stats.full_scan_count, 0);
	UT_ASSERT_EQ(stats.disable_count, 0);
	cluster_undo_inventory_attach(&state, true);
	cluster_undo_inventory_count_scan(true);
	cluster_undo_inventory_count_scan(false);
	cluster_undo_inventory_count_scan(false);
	cluster_undo_inventory_attach(&state, false);
	for (int read = 0; read < 2; read++) {
		UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
		UT_ASSERT_EQ(stats.bitmap_hit_count, 1);
		UT_ASSERT_EQ(stats.full_scan_count, 2);
		UT_ASSERT_EQ(stats.disable_count, 0);
		UT_ASSERT_EQ(state.serial, 0);
		UT_ASSERT_EQ(state.publishers, 0);
		UT_ASSERT(!state.complete && !state.disabled);
	}
	cluster_undo_inventory_attach(&state, true);
	UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
	UT_ASSERT_EQ(stats.bitmap_hit_count, 0);
	UT_ASSERT_EQ(stats.full_scan_count, 0);
	UT_ASSERT_EQ(stats.disable_count, 0);
	cluster_undo_inventory_attach(NULL, false);
}

int
main(void)
{
	UT_PLAN(27);
	UT_RUN(test_lms_own_runtime_reuses_two_descriptors);
	UT_RUN(test_worker_own_runtime_reuses_two_descriptors);
	UT_RUN(test_lms_77_existing_segments_second_sweep_never_reopens);
	UT_RUN(test_worker_77_existing_segments_second_sweep_never_reopens);
	UT_RUN(test_lms_77_segments_reopen_only_37_overflow_files_per_warm_sweep);
	UT_RUN(test_worker_77_segments_reopen_only_37_overflow_files_per_warm_sweep);
	UT_RUN(test_full_local_segment_range_preserves_descriptor_headroom);
	UT_RUN(test_low_fd_budget_leaves_room_for_wait_event_sets);
	UT_RUN(test_existing_external_users_limit_pool_without_losing_reads);
	UT_RUN(test_budget_reduction_or_new_external_users_preserve_pool_pins);
	UT_RUN(test_other_roles_owners_and_intents_keep_single_descriptor);
	UT_RUN(test_owner_and_intent_namespaces_never_share_file_bytes);
	UT_RUN(test_recovery_cached_descriptor_never_retains_admission);
	UT_RUN(test_failed_open_is_not_cached_and_later_creation_is_readable);
	UT_RUN(test_open_error_rethrows_and_releases_external_fd_reservation);
	UT_RUN(test_overflow_open_error_preserves_pool_reservations);
	UT_RUN(test_overflow_open_failure_preserves_pool_reservations);
	UT_RUN(test_in_place_recycle_is_visible_through_cached_descriptor);
	UT_RUN(test_reset_closes_all_descriptors_and_reopens_replaced_path);
	UT_RUN(test_external_fd_budget_refusal_preserves_reads_and_balances_cleanup);
	UT_RUN(test_registered_exit_callback_closes_pool_and_single_descriptor);
	UT_RUN(test_inventory_requires_complete_unchanged_baseline_and_preserves_gaps);
	UT_RUN(test_inventory_publication_is_invisible_until_readable_and_all_writers_finish);
	UT_RUN(test_inventory_failed_publication_recovery_and_wrong_scope_cannot_skip);
	UT_RUN(test_inventory_namespace_change_and_generation_exhaustion_fall_back);
	UT_RUN(test_inventory_disable_counter_counts_each_transition_once);
	UT_RUN(test_inventory_stats_attach_preserves_counts_and_absence_is_explicit);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
