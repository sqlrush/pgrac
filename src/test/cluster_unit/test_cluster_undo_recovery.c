/*-------------------------------------------------------------------------
 * test_cluster_undo_recovery.c
 *    Original recovery authority and canonical UNDO path qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include <setjmp.h>

#include "cluster/cluster_mode.h"
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
bool cluster_enabled = true;
bool cluster_undo_gcs_coherence = true;
char *DataDir = "/recoverer/local";
static ClusterConf conf;
ClusterConf *ClusterConfShmem = &conf;

static bool source_ok, storage_ok;
static uint64 system_id;
static uint8 storage_uuid[16];
static ClusterThreadRecoveryAuthorityResultV1 authority_result;
static uint32 path_calls;
static jmp_buf failure_jump;
static bool expect_failure;
static ClusterRecoveryDutyKey duty;
static ClusterControlRootSnapshot root;
static ClusterControlRootReadToken token;
static ClusterRecoverySerialGuard serial;
static ClusterThreadRecoveryAuthorityV1 authority;
static const RfSideOnlinePlanV1 *plan = (const RfSideOnlinePlanV1 *)(uintptr_t)1;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

uint64 GetSystemIdentifier(void) { return system_id; }

bool
cluster_shared_fs_get_protected_set_identity(ClusterProtectedSetIdentityV1 *out)
{
	memset(out, 0, sizeof(*out));
	out->backend_id = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
	memcpy(out->storage_uuid, storage_uuid, 16);
	return storage_ok;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(const ClusterThreadRecoveryAuthorityV1 *a)
{
	UT_ASSERT(a == &authority);
	return authority_result;
}

bool
rf_side_online_plan_source_matches_v1(const RfSideOnlinePlanV1 *p, uint64 sysid,
	const uint8 uuid[16], const RfContributorStreamCutV1 *cut)
{
	UT_ASSERT(p == plan);
	return source_ok && sysid == 123 && memcmp(uuid, storage_uuid, 16) == 0
		&& cut->failed_thread == 3 && cut->timeline_id == 7
		&& cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE
		&& cut->scan_begin_inclusive == 100 && cut->scan_end_exclusive == 500;
}

int
cluster_shared_fs_undo_path_resolve(uint8 owner, uint32 segment, char *path, size_t size)
{
	int length;

	path_calls++;
	length = snprintf(path, size, "/shared/pg_undo/instance_%u/seg_%u.dat", owner - 1, segment);
	return length < 0 || (size_t)length >= size ? -1 : 0;
}

#undef ereport
#define ereport(level_, rest_) \
	do { if (expect_failure) longjmp(failure_jump, 1); abort(); } while (0)
#include "test_cluster_undo_path_native.inc"

static int cached_fd = -1;
static uint32 cached_fd_segment;
static uint8 cached_fd_owner;
static ClusterUndoPathIntent cached_fd_intent;
static uint32 fd_opens, fd_closes;
static bool opened_shared;
static uint32 shared_syncs, local_syncs;

static int
fixture_open(const char *path, int flags)
{
	opened_shared = strcmp(path, "/shared/pg_undo/instance_2/seg_513.dat") == 0;
	UT_ASSERT(opened_shared || strcmp(path, "/recoverer/local/pg_undo/instance_2/seg_513.dat") == 0);
	fd_opens++;
	return 42;
}

static int
fixture_close(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	fd_closes++;
	return 0;
}

static int
fixture_fsync(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	if (opened_shared) shared_syncs++;
	else local_syncs++;
	return 0;
}

static void fd_cache_close(void);
#define BasicOpenFile fixture_open
#define close fixture_close
#define pg_fsync fixture_fsync
#define cluster_undo_record_note_smgr_close() ((void)0)
#define cluster_undo_record_note_smgr_open() ((void)0)
#define cluster_undo_smgr_ensure_exit_hook() ((void)0)
#include "test_cluster_undo_fd_native.inc"

static uint32 directory_creates;
static int
fixture_mkdir(const char *path, int mode)
{
	UT_ASSERT(strcmp(path, "/shared/pg_undo/instance_2") == 0);
	directory_creates++;
	return 0;
}

int
cluster_shared_fs_undo_instance_dir_resolve(uint8 owner, char *path, size_t size)
{
	UT_ASSERT(false); /* Scoped native creation must use its qualified path. */
	return -1;
}

#define pg_mkdir_p fixture_mkdir
#define mkdir fixture_mkdir
#define pg_dir_create_mode S_IRWXU
#include "test_cluster_undo_dir_native.inc"

static void
reset_authority(void)
{
	memset(&duty, 0, sizeof(duty));
	memset(&root, 0, sizeof(root));
	memset(&token, 0, sizeof(token));
	memset(&serial, 0, sizeof(serial));
	memset(&authority, 0, sizeof(authority));
	source_ok = storage_ok = true;
	conf.node_count = 4;
	cluster_enabled = cluster_undo_gcs_coherence = true;
	system_id = 123;
	memset(storage_uuid, 0x44, sizeof(storage_uuid));
	duty.system_identifier = 123;
	duty.origin_thread_id = 3;
	duty.root_lineage_seq = 8;
	memcpy(duty.storage_uuid, storage_uuid, 16);
	root.identity = duty;
	root.checkpoint_tli = root.tail_tli = 7;
	root.checkpoint_lower_lsn = 100;
	root.validated_tail_lsn_exclusive = 500;
	serial.held = true;
	serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
	authority.duty = &duty;
	authority.root_snapshot = &root;
	authority.root_token = &token;
	authority.serial_guard = &serial;
	authority_result = CLUSTER_THREAD_AUTHORITY_OK;
	path_calls = 0;
	fd_cache_close();
	fd_opens = fd_closes = 0;
	shared_syncs = local_syncs = 0;
	directory_creates = 0;
}

UT_TEST(test_original_resolver_reaches_only_scoped_canonical_origin)
{
	ClusterUndoRecoveryScopeV1 scope = {0};
	char path[MAXPGPATH];

	reset_authority();
	UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(3),
		3, 513, path, sizeof(path)), 0);
	UT_ASSERT(strcmp(path, "/recoverer/local/pg_undo/instance_2/seg_513.dat") == 0);
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(3), CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(3),
		3, 513, path, sizeof(path)), 0);
	UT_ASSERT(strcmp(path, "/shared/pg_undo/instance_2/seg_513.dat") == 0);
	UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(2),
		2, 257, path, sizeof(path)), -1);
	UT_ASSERT_EQ(cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED,
		3, 1, path, sizeof(path)), -1);
	UT_ASSERT_EQ(path_calls, 1);
	cluster_undo_recovery_scope_leave_v1(&scope);
	UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(3), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
	UT_ASSERT_EQ(cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED,
		3, 513, path, sizeof(path)), -1);
}

UT_TEST(test_scope_rejects_missing_or_non_mutation_authority)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterUndoRecoveryScopeV1 scope = {0};

		reset_authority();
		if (fault < 5) authority_result = (ClusterThreadRecoveryAuthorityResultV1)(fault + 1);
		if (fault == 5) serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 6) serial.mode = CLUSTER_RECOVERY_SERIAL_INITIALIZER;
		if (fault == 7) source_ok = false;
		if (fault == 8) storage_ok = false;
		if (fault == 9) system_id++;
		if (fault == 10) ClusterConfShmem->node_count = 1;
		if (fault == 11) cluster_undo_gcs_coherence = false;
		UT_ASSERT(!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
		UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(3), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
		UT_ASSERT_EQ(path_calls, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_stale_scope_never_falls_back_to_local_path)
{
	for (int fault = 0; fault < 8; fault++) {
		ClusterUndoRecoveryScopeV1 scope = {0}, other = {0};
		char path[MAXPGPATH];

		reset_authority();
		if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
			UT_ASSERT(false);
			return;
		}
		UT_ASSERT(!cluster_undo_recovery_scope_enter_v1(&other, &authority, plan));
		cluster_undo_recovery_scope_leave_v1(&other);
		if (fault == 0) authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
		if (fault == 1) duty.root_lineage_seq++;
		if (fault == 2) token.root_lineage_seq++;
		if (fault == 3) storage_uuid[0] ^= 1;
		if (fault == 4) root.validated_tail_lsn_exclusive++;
		if (fault == 5) serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 6) source_ok = false;
		if (fault == 7) cluster_undo_gcs_coherence = false;
		strlcpy(path, "unchanged", sizeof(path));
		UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(3),
			3, 513, path, sizeof(path)), -1);
		UT_ASSERT(strcmp(path, "unchanged") == 0);
		UT_ASSERT_EQ(path_calls, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_cached_native_fd_rechecks_recovery_authority)
{
	ClusterUndoRecoveryScopeV1 scope = {0};

	reset_authority();
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(get_segment_fd(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 513, 3), 42);
	UT_ASSERT_EQ(get_segment_fd(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 513, 3), 42);
	UT_ASSERT_EQ(fd_opens, 1);
	authority_result = CLUSTER_THREAD_AUTHORITY_SERIAL_STALE;
	UT_ASSERT_EQ(get_segment_fd(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 513, 3), -1);
	UT_ASSERT_EQ(fd_closes, 1);
	cluster_undo_recovery_scope_leave_v1(&scope);
	UT_ASSERT_EQ(get_segment_fd(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 513, 3), -1);
	UT_ASSERT_EQ(fd_opens, 1);
}

UT_TEST(test_native_directory_creation_uses_same_qualified_namespace)
{
	ClusterUndoRecoveryScopeV1 scope = {0};

	reset_authority();
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
		UT_ASSERT(false);
		return;
	}
	ensure_undo_instance_subdir(3);
	UT_ASSERT_EQ(directory_creates, 1);
	authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	expect_failure = true;
	if (setjmp(failure_jump) == 0) {
		ensure_undo_instance_subdir(3);
		UT_ASSERT(false);
	}
	expect_failure = false;
	UT_ASSERT_EQ(directory_creates, 1);
	cluster_undo_recovery_scope_leave_v1(&scope);
}

UT_TEST(test_native_fsync_closes_only_the_canonical_write_obligation)
{
	ClusterUndoRecoveryScopeV1 scope = {0};

	reset_authority();
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(get_segment_fd(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 513, 3), 42);
	UT_ASSERT(cluster_undo_smgr_fsync_segment_file(513, 3));
	UT_ASSERT_EQ(shared_syncs, 1);
	UT_ASSERT_EQ(local_syncs, 0);
	UT_ASSERT_EQ(fd_opens, 1);
	authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	UT_ASSERT(!cluster_undo_smgr_fsync_segment_file(513, 3));
	UT_ASSERT_EQ(shared_syncs + local_syncs, 1);
	cluster_undo_recovery_scope_leave_v1(&scope);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_native_fsync_closes_only_the_canonical_write_obligation);
	UT_RUN(test_native_directory_creation_uses_same_qualified_namespace);
	UT_RUN(test_cached_native_fd_rechecks_recovery_authority);
	UT_RUN(test_original_resolver_reaches_only_scoped_canonical_origin);
	UT_RUN(test_scope_rejects_missing_or_non_mutation_authority);
	UT_RUN(test_stale_scope_never_falls_back_to_local_path);
	UT_DONE();
	return ut_failed_count != 0;
}
