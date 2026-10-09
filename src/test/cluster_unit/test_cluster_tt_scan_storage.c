/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_scan_storage.c
 *    Durable TT scans through the real undo storage and inventory consumers.
 *
 *    Both TT resolvers are linked from cluster_tt_durable.c.  This file
 *    includes the real SMGR to observe actual opens/preads and inject EIO or
 *    publication between two storage observations.  Header bytes, missing
 *    files and short files are real; neither TT verdicts nor inventory
 *    decisions are mocked.  Process identity, path resolution, existence
 *    probing and external-FD accounting are standalone fixture boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_tt_scan_storage.c
 *
 * NOTES
 *    Private temporary files only.  This does not test cross-process
 *    scheduling, recovery admission, final-file publication durability or
 *    transaction visibility above the two durable TT scan entry points.
 *    Link a sectioned cluster_tt_durable.o with dead stripping; do not link
 *    another cluster_undo_smgr.o or the existing mocked TT fixture.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_xnode_profile.h"
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
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
ClusterXpService cluster_xp_current_service = CLXP_SERVICE_NONE;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

typedef struct ScanFd {
	int fd;
	uint32 segment;
	bool live;
} ScanFd;

typedef enum ScanConsumer { SCAN_RESOLVE, SCAN_LOCATE } ScanConsumer;

typedef struct ScanAnswer {
	int result;
	SCN scn;
	uint16 segment;
	uint16 slot;
	uint16 wrap;
	uint8 status;
} ScanAnswer;

#define SCAN_SENTINEL UINT16_MAX
#define SCAN_XID ((TransactionId)12345)
#define SCAN_WRAP 7
#define SCAN_SLOT 3

static const BackendType scan_roles[] = { B_LMS, B_LMS_WORKER, B_BACKEND };
static char scan_dir[MAXPGPATH];
static ClusterUndoInventory scan_inventory;
static ScanFd scan_fds[CLUSTER_UNDO_SEGS_PER_INSTANCE + 1];
static int scan_open_calls;
static int scan_enoent_calls;
static int scan_pread_calls;
static int scan_eio_calls;
static int scan_external_used;
static int scan_external_acquired;
static int scan_external_released;
static int scan_wait_depth;
static int scan_wait_starts;
static int scan_calls;
static uint32 scan_eio_segment;
static void (*scan_after_read)(uint32 segment);
static int scan_publications;
static pg_on_exit_callback scan_exit_callback;
static Datum scan_exit_arg;

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	/* Deterministic single-thread interleavings must never contend. */
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

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
	scan_exit_callback = function;
	scan_exit_arg = arg;
}

bool
AcquireExternalFD(void)
{
	if (scan_external_used >= 32)
		return false;
	scan_external_used++;
	scan_external_acquired++;
	return true;
}

void
ReleaseExternalFD(void)
{
	UT_ASSERT(scan_external_used > 0);
	scan_external_used--;
	scan_external_released++;
}

void
cluster_undo_record_note_smgr_open(void)
{}

void
cluster_undo_record_note_smgr_close(void)
{}

void
cluster_undo_record_note_smgr_pread(void)
{}

void
cluster_undo_record_note_smgr_pwrite(void)
{}

void
cluster_tt_durable_count_by_xid_scan(void)
{
	scan_calls++;
}

void
cluster_tt_durable_io_wait_start(void)
{
	UT_ASSERT_EQ(scan_wait_depth, 0);
	scan_wait_depth++;
	scan_wait_starts++;
}

void
cluster_tt_durable_io_wait_end(void)
{
	UT_ASSERT_EQ(scan_wait_depth, 1);
	scan_wait_depth--;
}

ClusterUndoPathIntent
cluster_undo_recovery_intent_for_owner(uint8 owner)
{
	/* Runtime scans only; no recovery token is manufactured by this fixture. */
	return cluster_undo_intent_for_owner(owner);
}

int
cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 owner, uint32 segment, char *path,
						  size_t size)
{
	int written = snprintf(path, size, "%s/i%u-o%u-s%u.dat", scan_dir, (unsigned)intent,
						   (unsigned)owner, (unsigned)segment);

	return written < 0 || (size_t)written >= size ? -1 : 0;
}

bool
cluster_undo_segment_file_exists(uint8 owner, uint32 segment)
{
	char path[MAXPGPATH];

	if (owner < 1 || owner > UNDO_OWNER_INSTANCE_MAX
		|| cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(owner), owner, segment,
									 path, sizeof(path))
			   != 0)
		return false;
	/* Same syscall boundary as the allocator, never a canned existence bit. */
	return access(path, F_OK) == 0;
}

bool
cluster_undo_segment_header_identity_ok(const char *block, uint32 segment, uint8 owner)
{
	return block != NULL && UndoSegmentHeader_identity_matches(block, segment, owner);
}

int
BasicOpenFile(const char *path, int flags)
{
	int fd;
	int saved_errno;

	scan_open_calls++;
	fd = open(path, flags, pg_file_create_mode);
	saved_errno = errno;
	if (fd < 0 && saved_errno == ENOENT)
		scan_enoent_calls++;
	if (fd >= 0) {
		const char *name = strrchr(path, '/');
		unsigned intent, owner, segment;
		int i;

		if (name == NULL || sscanf(name + 1, "i%u-o%u-s%u.dat", &intent, &owner, &segment) != 3)
			abort();
		for (i = 0; i < lengthof(scan_fds); i++)
			if (!scan_fds[i].live)
				break;
		if (i == lengthof(scan_fds))
			abort();
		scan_fds[i] = (ScanFd){ fd, segment, true };
	}
	errno = saved_errno;
	return fd;
}

static int
scan_close(int fd)
{
	int result = close(fd);
	int saved_errno = errno;
	bool found = false;

	UT_ASSERT_EQ(result, 0);
	for (int i = 0; i < lengthof(scan_fds); i++) {
		if (scan_fds[i].live && scan_fds[i].fd == fd) {
			scan_fds[i].live = false;
			found = true;
			break;
		}
	}
	UT_ASSERT(found);
	errno = saved_errno;
	return result;
}

static ssize_t
scan_pread(int fd, void *buf, size_t count, off_t offset)
{
	uint32 segment = 0;
	ssize_t result;
	int saved_errno;

	for (int i = 0; i < lengthof(scan_fds); i++)
		if (scan_fds[i].live && scan_fds[i].fd == fd) {
			segment = scan_fds[i].segment;
			break;
		}
	if (segment == 0)
		abort();
	scan_pread_calls++;
	if (segment == scan_eio_segment) {
		scan_eio_calls++;
		errno = EIO;
		return -1;
	}
	result = pread(fd, buf, count, offset);
	saved_errno = errno;
	if (result == BLCKSZ && offset == 0 && scan_after_read != NULL)
		scan_after_read(segment);
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

#undef pg_pread
#define pg_pread scan_pread
#define close scan_close
#include "../../backend/cluster/storage/cluster_undo_smgr.c"
#undef close
#undef pg_pread

static void
scan_reset_counts(void)
{
	UT_ASSERT_EQ(scan_wait_depth, 0);
	scan_open_calls = scan_enoent_calls = scan_pread_calls = scan_eio_calls = 0;
	scan_calls = scan_wait_starts = 0;
}

static bool
scan_begin_case(BackendType role)
{
	char pattern[] = "/tmp/pgrac-tt-scan-XXXXXX";
	char *created;

	cluster_undo_smgr_fd_cache_reset();
	UT_ASSERT_EQ(scan_external_used, 0);
	memset(scan_fds, 0, sizeof(scan_fds));
	scan_external_acquired = scan_external_released = 0;
	scan_eio_segment = 0;
	scan_after_read = NULL;
	scan_publications = 0;
	MyBackendType = role;
	cluster_node_id = 0;
	created = mkdtemp(pattern);
	UT_ASSERT_NOT_NULL(created);
	if (created == NULL)
		return false;
	strlcpy(scan_dir, created, sizeof(scan_dir));
	cluster_undo_inventory_attach(&scan_inventory, true);
	scan_reset_counts();
	return true;
}

static void
scan_finish_case(void)
{
	DIR *dir;
	struct dirent *entry;

	scan_after_read = NULL;
	scan_eio_segment = 0;
	cluster_undo_smgr_fd_cache_reset();
	if (scan_exit_callback != NULL)
		scan_exit_callback(0, scan_exit_arg);
	UT_ASSERT_EQ(scan_external_used, 0);
	UT_ASSERT_EQ(scan_external_acquired, scan_external_released);
	UT_ASSERT_EQ(scan_wait_depth, 0);
	for (int i = 0; i < lengthof(scan_fds); i++) {
		UT_ASSERT(!scan_fds[i].live);
		if (scan_fds[i].live)
			(void)close(scan_fds[i].fd);
	}
	cluster_undo_inventory_attach(NULL, false);
	dir = opendir(scan_dir);
	UT_ASSERT_NOT_NULL(dir);
	if (dir == NULL)
		return;
	while ((entry = readdir(dir)) != NULL) {
		char path[MAXPGPATH];
		int n;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		n = snprintf(path, sizeof(path), "%s/%s", scan_dir, entry->d_name);
		if (n < 0 || n >= sizeof(path))
			abort();
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(rmdir(scan_dir), 0);
	scan_dir[0] = '\0';
}

/* Seed actual bytes outside the observed SMGR calls; setup failure is fatal. */
static void
scan_seed(uint32 segment, TransactionId xid, uint8 status, uint16 wrap, SCN scn, bool short_file)
{
	PGAlignedBlock page = { 0 };
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)page.data;
	uint8 owner = (uint8)((segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
	char path[MAXPGPATH];
	int fd;
	size_t length = short_file ? BLCKSZ - 1 : BLCKSZ;

	header->pd_flags = PD_UNDO_SEG_HEADER;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->segment_id = segment;
	header->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header->owner_instance = owner;
	header->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	header->segment_state = SEGMENT_ACTIVE;
	header->tt_slots[SCAN_SLOT].xid = xid;
	header->tt_slots[SCAN_SLOT].status = status;
	header->tt_slots[SCAN_SLOT].wrap = wrap;
	header->tt_slots[SCAN_SLOT].commit_scn = scn;
	if (cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(owner), owner, segment,
								  path, sizeof(path))
		!= 0)
		abort();
	fd = open(path, O_CREAT | O_TRUNC | O_RDWR | PG_BINARY, pg_file_create_mode);
	if (fd < 0 || pwrite(fd, page.data, length, 0) != (ssize_t)length || close(fd) != 0)
		abort();
}

static void
scan_seed_three(int origin)
{
	uint32 base = (uint32)origin * CLUSTER_UNDO_SEGS_PER_INSTANCE;

	scan_seed(base + 1, SCAN_XID, TT_SLOT_COMMITTED, SCAN_WRAP, scn_encode(1, 77), false);
	scan_seed(base + 64, SCAN_XID + 1, TT_SLOT_ACTIVE, SCAN_WRAP, InvalidScn, false);
	scan_seed(base + 256, SCAN_XID + 2, TT_SLOT_ABORTED, SCAN_WRAP, InvalidScn, false);
}

static ScanAnswer
scan_query(ScanConsumer consumer, int origin, TransactionId xid, uint32 wrap)
{
	ScanAnswer answer
		= { 0, scn_encode(1, 999), SCAN_SENTINEL, SCAN_SENTINEL, SCAN_SENTINEL, TT_SLOT_INVALID };

	if (consumer == SCAN_RESOLVE)
		answer.result = cluster_tt_slot_durable_resolve_by_xid_origin(
			origin, xid, wrap, &answer.scn, &answer.segment, &answer.slot, &answer.wrap);
	else
		answer.result = cluster_tt_slot_durable_locate_any_by_xid_origin(
			origin, xid, &answer.segment, &answer.slot, &answer.wrap, &answer.status);
	UT_ASSERT_EQ(scan_wait_depth, 0);
	return answer;
}

static void
scan_expect(ScanAnswer answer, ScanConsumer consumer, int result, uint16 segment, uint16 wrap,
			uint8 status, SCN scn)
{
	bool found = consumer == SCAN_RESOLVE ? result == CLUSTER_TT_DURABLE_RESOLVED_SCN
										  : result == CLUSTER_TT_DURABLE_LOCATE_FOUND;

	UT_ASSERT_EQ(answer.result, result);
	UT_ASSERT_EQ(answer.segment, found ? segment : SCAN_SENTINEL);
	UT_ASSERT_EQ(answer.slot, found ? SCAN_SLOT : SCAN_SENTINEL);
	UT_ASSERT_EQ(answer.wrap, found ? wrap : SCAN_SENTINEL);
	if (consumer == SCAN_RESOLVE)
		UT_ASSERT_EQ(answer.scn, found ? scn : InvalidScn);
	else
		UT_ASSERT_EQ(answer.status, found ? status : TT_SLOT_INVALID);
}

static void
scan_expect_target(ScanAnswer answer, ScanConsumer consumer, int origin)
{
	scan_expect(answer, consumer,
				consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_RESOLVED_SCN
										 : CLUSTER_TT_DURABLE_LOCATE_FOUND,
				(uint16)(origin * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1), SCAN_WRAP, TT_SLOT_COMMITTED,
				scn_encode(1, 77));
}

static void
scan_role_reuse(BackendType role)
{
	for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
		if (!scan_begin_case(role))
			return;
		scan_seed_three(0);
		scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
		UT_ASSERT_EQ(scan_open_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
		UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 3);
		UT_ASSERT_EQ(scan_pread_calls, 3);
		/* Optimization RED on the original full-scan consumer, not a link RED. */
		scan_reset_counts();
		scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
		UT_ASSERT_EQ(scan_enoent_calls, 0);
		UT_ASSERT_EQ(scan_open_calls, role == B_BACKEND ? 3 : 0);
		UT_ASSERT_EQ(scan_pread_calls, 3);
		UT_ASSERT_EQ(scan_calls, 1);
		UT_ASSERT_EQ(scan_wait_starts, 1);
		scan_finish_case();
	}
}

UT_TEST(test_lms_both_scans_reuse_real_inventory)
{
	scan_role_reuse(B_LMS);
}

UT_TEST(test_worker_both_scans_reuse_real_inventory)
{
	scan_role_reuse(B_LMS_WORKER);
}

UT_TEST(test_backend_both_scans_reuse_real_inventory)
{
	scan_role_reuse(B_BACKEND);
}

UT_TEST(test_inventory_preserves_tt_results_and_output_identity)
{
	const struct {
		TransactionId xid;
		uint32 wanted_wrap;
		int resolve;
		int locate;
		uint16 segment;
		uint16 wrap;
		uint8 status;
		SCN scn;
	} cases[]
		= { { SCAN_XID, SCAN_WRAP, CLUSTER_TT_DURABLE_RESOLVED_SCN, CLUSTER_TT_DURABLE_LOCATE_FOUND,
			  1, SCAN_WRAP, TT_SLOT_COMMITTED, scn_encode(1, 77) },
			{ SCAN_XID + 1, SCAN_WRAP, CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH,
			  CLUSTER_TT_DURABLE_LOCATE_FOUND, 64, SCAN_WRAP, TT_SLOT_ACTIVE, InvalidScn },
			{ SCAN_XID + 2, SCAN_WRAP, CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH,
			  CLUSTER_TT_DURABLE_LOCATE_FOUND, 256, SCAN_WRAP, TT_SLOT_ABORTED, InvalidScn },
			{ SCAN_XID + 3, SCAN_WRAP, CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN,
			  CLUSTER_TT_DURABLE_LOCATE_FOUND, 65, SCAN_WRAP, TT_SLOT_COMMITTED, InvalidScn },
			{ SCAN_XID + 4, CLUSTER_TT_WRAP_ANY, CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP,
			  CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS, 0, 0, TT_SLOT_INVALID, InvalidScn },
			{ SCAN_XID + 4, SCAN_WRAP, CLUSTER_TT_DURABLE_RESOLVED_SCN,
			  CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS, 128, SCAN_WRAP, TT_SLOT_COMMITTED,
			  scn_encode(1, 88) },
			{ SCAN_XID + 99, SCAN_WRAP, CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH,
			  CLUSTER_TT_DURABLE_LOCATE_MISSING, 0, 0, TT_SLOT_INVALID, InvalidScn },
			{ SCAN_XID, SCAN_WRAP + 1, CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH,
			  CLUSTER_TT_DURABLE_LOCATE_FOUND, 1, SCAN_WRAP, TT_SLOT_COMMITTED, InvalidScn } };

	if (!scan_begin_case(B_LMS_WORKER))
		return;
	scan_seed_three(0);
	scan_seed(65, SCAN_XID + 3, TT_SLOT_COMMITTED, SCAN_WRAP, InvalidScn, false);
	scan_seed(128, SCAN_XID + 4, TT_SLOT_COMMITTED, SCAN_WRAP, scn_encode(1, 88), false);
	scan_seed(129, SCAN_XID + 4, TT_SLOT_COMMITTED, SCAN_WRAP + 1, scn_encode(1, 99), false);
	for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
		for (int i = 0; i < lengthof(cases); i++) {
			bool zero_match = consumer == SCAN_RESOLVE
								  ? cases[i].resolve == CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH
								  : cases[i].locate == CLUSTER_TT_DURABLE_LOCATE_MISSING;

			cluster_undo_inventory_attach(&scan_inventory, true);
			for (int pass = 0; pass < 2; pass++) {
				scan_reset_counts();
				scan_expect(scan_query(consumer, 0, cases[i].xid, cases[i].wanted_wrap), consumer,
							consumer == SCAN_RESOLVE ? cases[i].resolve : cases[i].locate,
							cases[i].segment, cases[i].wrap, cases[i].status, cases[i].scn);
				UT_ASSERT_EQ(scan_enoent_calls,
							 pass == 0 || zero_match ? CLUSTER_UNDO_SEGS_PER_INSTANCE - 6 : 0);
				UT_ASSERT_EQ(scan_pread_calls, pass > 0 && zero_match ? 12 : 6);
			}
		}
	}
	scan_finish_case();
}

UT_TEST(test_zero_match_rechecks_whole_range)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_begin_case(scan_roles[role]))
				return;
			scan_seed_three(0);
			scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
			scan_reset_counts();
			scan_expect(scan_query(consumer, 0, SCAN_XID + 99, SCAN_WRAP), consumer,
						consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH
												 : CLUSTER_TT_DURABLE_LOCATE_MISSING,
						0, 0, TT_SLOT_INVALID, InvalidScn);
			UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 3);
			UT_ASSERT_EQ(scan_pread_calls, 6);
			UT_ASSERT_EQ(scan_calls, 1);
			UT_ASSERT_EQ(scan_wait_starts, 2);
			scan_finish_case();
		}
	}
}

UT_TEST(test_zero_match_recheck_observes_untracked_file_and_read_errors)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			for (int failure = 0; failure < 3; failure++) {
				if (!scan_begin_case(scan_roles[role]))
					return;
				scan_seed_three(0);
				scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
				/* Deliberately omit publication to test the zero-match fallback
				 * independently of the inventory's publication completeness. */
				scan_seed(2, SCAN_XID + 99, TT_SLOT_COMMITTED, SCAN_WRAP, scn_encode(1, 123),
						  failure == 2);
				if (failure == 1)
					scan_eio_segment = 2;
				scan_reset_counts();
				scan_expect(scan_query(consumer, 0, SCAN_XID + 99, SCAN_WRAP), consumer,
							consumer == SCAN_RESOLVE
								? (failure ? CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE
										   : CLUSTER_TT_DURABLE_RESOLVED_SCN)
								: (failure ? CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE
										   : CLUSTER_TT_DURABLE_LOCATE_FOUND),
							2, SCAN_WRAP, TT_SLOT_COMMITTED, scn_encode(1, 123));
				UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 4);
				UT_ASSERT_EQ(scan_pread_calls, failure ? 8 : 7);
				UT_ASSERT_EQ(scan_eio_calls, failure == 1 ? 2 : 0);
				UT_ASSERT_EQ(scan_calls, 1);
				UT_ASSERT_EQ(scan_wait_starts, 2);
				scan_finish_case();
			}
		}
	}
}

UT_TEST(test_mixed_missing_present_eio_short_file_never_completes)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_begin_case(scan_roles[role]))
				return;
			scan_seed_three(0);
			scan_seed(256, SCAN_XID + 2, TT_SLOT_ABORTED, SCAN_WRAP, InvalidScn, true);
			scan_eio_segment = 64;
			for (int pass = 0; pass < 2; pass++) {
				scan_reset_counts();
				scan_expect(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer,
							consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE
													 : CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE,
							0, 0, TT_SLOT_INVALID, InvalidScn);
				UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 3);
				UT_ASSERT_EQ(scan_eio_calls, 2);
				UT_ASSERT_EQ(scan_pread_calls, 5);
			}
			scan_eio_segment = 0;
			scan_seed(256, SCAN_XID + 2, TT_SLOT_ABORTED, SCAN_WRAP, InvalidScn, false);
			for (int pass = 0; pass < 2; pass++) {
				scan_reset_counts();
				scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
				UT_ASSERT_EQ(scan_enoent_calls, pass == 0 ? CLUSTER_UNDO_SEGS_PER_INSTANCE - 3 : 0);
				UT_ASSERT_EQ(scan_pread_calls, 3);
			}
			scan_finish_case();
		}
	}
}

UT_TEST(test_known_segment_failure_retries_whole_range_and_refuses)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			for (int short_file = 0; short_file <= 1; short_file++) {
				if (!scan_begin_case(scan_roles[role]))
					return;
				scan_seed_three(0);
				scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
				if (short_file)
					scan_seed(64, SCAN_XID + 1, TT_SLOT_ACTIVE, SCAN_WRAP, InvalidScn, true);
				else
					scan_eio_segment = 64;
				scan_reset_counts();
				scan_expect(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer,
							consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE
													 : CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE,
							0, 0, TT_SLOT_INVALID, InvalidScn);
				UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 3);
				UT_ASSERT_EQ(scan_pread_calls, 8);
				UT_ASSERT_EQ(scan_eio_calls, short_file ? 0 : 4);
				UT_ASSERT_EQ(scan_calls, 1);
				UT_ASSERT_EQ(scan_wait_starts, 2);
				scan_finish_case();
			}
		}
	}
}

static void
scan_publish_duplicate(uint32 segment)
{
	bool tracked;

	if (segment != 1)
		return;
	scan_after_read = NULL;
	tracked = cluster_undo_inventory_publish_begin(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1);
	UT_ASSERT(tracked);
	if (!tracked)
		return;
	/* Only publisher entry/exit are modeled; bytes are real.  The syscall
	 * hook runs after a complete pread, outside the inventory spinlock. */
	scan_seed(2, SCAN_XID, TT_SLOT_COMMITTED, SCAN_WRAP, scn_encode(1, 123), false);
	cluster_undo_inventory_publish_end(2, true);
	scan_publications++;
}

UT_TEST(test_publication_during_limited_scan_cannot_hide_second_match)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_begin_case(scan_roles[role]))
				return;
			scan_seed_three(0);
			scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
			scan_after_read = scan_publish_duplicate;
			scan_reset_counts();
			scan_expect(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer,
						consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP
												 : CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS,
						0, 0, TT_SLOT_INVALID, InvalidScn);
			UT_ASSERT_EQ(scan_publications, 1);
			UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 4);
			UT_ASSERT_EQ(scan_pread_calls, 7);
			UT_ASSERT_EQ(scan_calls, 1);
			UT_ASSERT_EQ(scan_wait_starts, 2);
			scan_finish_case();
		}
	}
}

UT_TEST(test_foreign_namespace_keeps_original_full_scan)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_begin_case(scan_roles[role]))
				return;
			scan_seed_three(1);
			for (int pass = 0; pass < 2; pass++) {
				scan_reset_counts();
				scan_expect_target(scan_query(consumer, 1, SCAN_XID, SCAN_WRAP), consumer, 1);
				UT_ASSERT_EQ(scan_enoent_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE - 3);
				UT_ASSERT_EQ(scan_open_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
				UT_ASSERT_EQ(scan_pread_calls, 3);
			}
			scan_finish_case();
		}
	}
}

static void
scan_expect_inventory_counts(uint64 bitmap, uint64 full, uint64 disabled)
{
	ClusterUndoInventoryStats stats;

	UT_ASSERT(cluster_undo_inventory_read_stats(&stats));
	UT_ASSERT_EQ(stats.bitmap_hit_count, bitmap);
	UT_ASSERT_EQ(stats.full_scan_count, full);
	UT_ASSERT_EQ(stats.disable_count, disabled);
}

UT_TEST(test_inventory_counts_actual_passes_through_both_consumers)
{
	for (int role = 0; role < lengthof(scan_roles); role++) {
		for (int consumer = SCAN_RESOLVE; consumer <= SCAN_LOCATE; consumer++) {
			if (!scan_begin_case(scan_roles[role]))
				return;
			scan_seed_three(0);
			scan_expect_inventory_counts(0, 0, 0);
			scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
			scan_expect_inventory_counts(0, 1, 0);
			scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
			scan_expect_inventory_counts(1, 1, 0);

			/* A zero-match retry is one limited pass and one full pass. */
			scan_expect(scan_query(consumer, 0, SCAN_XID + 100, SCAN_WRAP), consumer,
						consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH
												 : CLUSTER_TT_DURABLE_LOCATE_MISSING,
						0, 0, TT_SLOT_INVALID, InvalidScn);
			scan_expect_inventory_counts(2, 2, 0);

			/* Failed reads count the attempted passes, never a successful verdict. */
			scan_eio_segment = 64;
			scan_expect(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer,
						consumer == SCAN_RESOLVE ? CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE
												 : CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE,
						0, 0, TT_SLOT_INVALID, InvalidScn);
			scan_expect_inventory_counts(3, 3, 0);
			scan_eio_segment = 0;
			cluster_undo_inventory_disable(1);
			cluster_undo_inventory_disable(1);
			scan_expect_inventory_counts(3, 3, 1);
			scan_expect_target(scan_query(consumer, 0, SCAN_XID, SCAN_WRAP), consumer, 0);
			scan_expect_inventory_counts(3, 4, 1);
			scan_finish_case();
		}
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_lms_both_scans_reuse_real_inventory);
	UT_RUN(test_worker_both_scans_reuse_real_inventory);
	UT_RUN(test_backend_both_scans_reuse_real_inventory);
	UT_RUN(test_inventory_preserves_tt_results_and_output_identity);
	UT_RUN(test_zero_match_rechecks_whole_range);
	UT_RUN(test_zero_match_recheck_observes_untracked_file_and_read_errors);
	UT_RUN(test_mixed_missing_present_eio_short_file_never_completes);
	UT_RUN(test_known_segment_failure_retries_whole_range_and_refuses);
	UT_RUN(test_publication_during_limited_scan_cannot_hide_second_match);
	UT_RUN(test_foreign_namespace_keeps_original_full_scan);
	UT_RUN(test_inventory_counts_actual_passes_through_both_consumers);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
