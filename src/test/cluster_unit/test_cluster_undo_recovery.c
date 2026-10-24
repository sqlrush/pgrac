/*-------------------------------------------------------------------------
 * test_cluster_undo_recovery.c
 *    Original recovery authority and canonical UNDO path qualification.
 *-------------------------------------------------------------------------
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "access/xlog.h"
#include <setjmp.h>

#include "cluster/cluster_mode.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
bool cluster_enabled = true;
bool cluster_undo_gcs_coherence = true;
char *DataDir = "/recoverer/local";
static ClusterConf conf;
ClusterConf *ClusterConfShmem = &conf;

static bool source_ok, storage_ok, recovery_purpose;
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
static unsigned wal_sync_calls;
static int wal_sync_fault;
static ClusterWalSourceRef cold_source;
static unsigned cold_source_fault, cold_source_reads;
static bool terminal_enabled, terminal_plan_ok, terminal_redo_ok;
static unsigned terminal_plan_calls;
static XLogReaderState terminal_reader;


/* The ordinary scope never enters the separate selected-C0 path. */
bool
cluster_cold_replay_window_active_v1(void)
{
	UT_ASSERT(false);
	return false;
}

bool
rf_side_online_plan_recovery_abort_sources_v1(const RfSideOnlinePlanV1 *p, uint32 index,
											  ClusterWalSourceRef *writer,
											  ClusterWalSourceRef *target)
{
	UT_ASSERT(false);
	return false;
}

bool
rf_side_online_plan_replay_start_matches_v1(const RfSideOnlinePlanV1 *p, uint32 thread,
											uint64 incarnation, uint64 db, XLogRecPtr redo)
{
	UT_ASSERT(terminal_enabled);
	return terminal_enabled && terminal_redo_ok && p == plan && thread == 3
		   && incarnation == duty.origin_owner_incarnation && db == 4 && redo == 120;
}

uint32
rf_side_online_plan_native_terminal_count_v1(const RfSideOnlinePlanV1 *p,
											 const ClusterWalSourceRef *source)
{
	return terminal_enabled && terminal_plan_ok && p == plan
				   && memcmp(source, &cold_source, sizeof(*source)) == 0
			   ? 1
			   : UINT32_MAX;
}

bool
rf_side_online_plan_terminal_matches_v1(const RfSideOnlinePlanV1 *p,
										const ClusterWalSourceRef *source,
										const XLogReaderState *reader)
{
	terminal_plan_calls++;
	return terminal_plan_ok && p == plan && reader == &terminal_reader
		   && memcmp(source, &cold_source, sizeof(*source)) == 0;
}

ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *expected,
										const ClusterControlRootReadToken *selected,
										ClusterWalSourceRef *out, XLogRecPtr *redo)
{
	UT_ASSERT(expected == &root && memcmp(selected, &token, sizeof(token)) == 0);
	cold_source_reads++;
	*out = cold_source;
	*redo = 120;
	if (cold_source_fault == 11)
		authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	if (cold_source_fault == 16)
		root.tail_last_record_crc32c++;
	return cold_source_fault == 10 ? CLUSTER_CONTROL_ROOT_IO_ERROR
								   : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_recovery_sync_v1(const ClusterControlRootSnapshot *expected,
									  const ClusterControlRootReadToken *selected,
									  ClusterWalTailObservation *out)
{
	UT_ASSERT(expected == &root && selected == &token);
	wal_sync_calls++;
	memset(out, 0, sizeof(*out));
	out->complete_end = root.validated_tail_lsn_exclusive;
	out->last_record_start = root.tail_last_record_lsn;
	out->last_record_crc = root.tail_last_record_crc32c;
	if (wal_sync_fault == 2)
		authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	if (wal_sync_fault == 3)
		root.validated_tail_lsn_exclusive++;
	if (wal_sync_fault == 4)
		out->complete_end--;
	if (wal_sync_fault == 5)
		out->last_record_crc++;
	return wal_sync_fault == 1 ? CLUSTER_CONTROL_ROOT_IO_ERROR : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

uint64
GetSystemIdentifier(void)
{
	return system_id;
}

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
		   && cut->origin_owner_incarnation == duty.origin_owner_incarnation
		   && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE && cut->scan_begin_inclusive == 100
		   && cut->scan_end_exclusive == 500;
}

int
cluster_shared_fs_undo_path_resolve(uint8 owner, uint32 segment, char *path, size_t size)
{
	int length;

	path_calls++;
	length = snprintf(path, size, "/shared/pg_undo/instance_%u/seg_%u.dat", owner - 1, segment);
	return length < 0 || (size_t)length >= size ? -1 : 0;
}

bool
rf_side_online_plan_recovery_source_matches_v1(const RfSideOnlinePlanV1 *p, uint64 sysid,
											   const uint8 uuid[16],
											   const RfContributorStreamCutV1 *cut)
{
	return recovery_purpose && rf_side_online_plan_source_matches_v1(p, sysid, uuid, cut);
}

bool
rf_side_online_plan_multixact_page_retired_v1(const RfSideOnlinePlanV1 *p, uint32 origin_thread,
											  XLogRecPtr source_lsn, XLogRecPtr source_end_lsn,
											  bool members, uint32 page)
{
	return p == plan && origin_thread == 3 && source_lsn == 100 && source_end_lsn == 200 && members
		   && page == 0;
}

#undef ereport
#define ereport(level_, rest_)                                                                     \
	do {                                                                                           \
		if (expect_failure)                                                                        \
			longjmp(failure_jump, 1);                                                              \
		abort();                                                                                   \
	} while (0)
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
	UT_ASSERT(opened_shared
			  || strcmp(path, "/recoverer/local/pg_undo/instance_2/seg_513.dat") == 0);
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
	if (opened_shared)
		shared_syncs++;
	else
		local_syncs++;
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
	source_ok = storage_ok = recovery_purpose = true;
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
	wal_sync_calls = 0;
	wal_sync_fault = 0;
}

UT_TEST(test_cold_source_durability_is_bound_to_original_scope_and_lifetime)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };
		reset_authority();
		serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		UT_ASSERT(!cluster_undo_recovery_cold_source_sync_v1(2));
		UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
		UT_ASSERT(!cluster_undo_recovery_cold_source_synced_v1(2));
		wal_sync_fault = fault;
		if (fault == 6)
			serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		if (fault == 7)
			recovery_purpose = false;
		UT_ASSERT_EQ(cluster_undo_recovery_cold_source_sync_v1(fault == 8 ? 1 : 2), fault == 0);
		UT_ASSERT_EQ(wal_sync_calls, fault >= 6 ? 0 : 1);
		UT_ASSERT_EQ(cluster_undo_recovery_cold_source_synced_v1(2), fault == 0);
		if (fault == 0) {
			UT_ASSERT(cluster_undo_recovery_cold_source_sync_v1(2));
			UT_ASSERT_EQ(wal_sync_calls, 1);
			UT_ASSERT(!cluster_undo_recovery_cold_source_synced_v1(1));
			authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
			UT_ASSERT(!cluster_undo_recovery_cold_source_synced_v1(2));
		}
		cluster_undo_recovery_scope_leave_v1(&scope);
		UT_ASSERT(!cluster_undo_recovery_cold_source_synced_v1(2));
	}
}

UT_TEST(test_original_resolver_reaches_only_scoped_canonical_origin)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	char path[MAXPGPATH];

	reset_authority();
	UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(3), 3, 513, path,
										   sizeof(path)),
				 0);
	UT_ASSERT(strcmp(path, "/recoverer/local/pg_undo/instance_2/seg_513.dat") == 0);
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(3), CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(3), 3, 513, path,
										   sizeof(path)),
				 0);
	UT_ASSERT(strcmp(path, "/shared/pg_undo/instance_2/seg_513.dat") == 0);
	UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(2), 2, 257, path,
										   sizeof(path)),
				 -1);
	UT_ASSERT_EQ(
		cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 3, 1, path, sizeof(path)), -1);
	UT_ASSERT_EQ(path_calls, 1);
	cluster_undo_recovery_scope_leave_v1(&scope);
	UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(3), CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
	UT_ASSERT_EQ(
		cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RECOVERY_SHARED, 3, 513, path, sizeof(path)),
		-1);
}

UT_TEST(test_scope_rejects_missing_or_non_mutation_authority)
{
	for (int fault = 0; fault < 12; fault++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };

		reset_authority();
		if (fault < 5)
			authority_result = (ClusterThreadRecoveryAuthorityResultV1)(fault + 1);
		if (fault == 5)
			serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 6)
			serial.mode = CLUSTER_RECOVERY_SERIAL_INITIALIZER;
		if (fault == 7)
			source_ok = false;
		if (fault == 8)
			storage_ok = false;
		if (fault == 9)
			system_id++;
		if (fault == 10)
			ClusterConfShmem->node_count = 1;
		if (fault == 11)
			cluster_undo_gcs_coherence = false;
		UT_ASSERT(!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
		UT_ASSERT_EQ(cluster_undo_recovery_intent_for_owner(3),
					 CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
		UT_ASSERT_EQ(path_calls, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_stale_scope_never_falls_back_to_local_path)
{
	for (int fault = 0; fault < 8; fault++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 }, other = { 0 };
		char path[MAXPGPATH];

		reset_authority();
		if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
			UT_ASSERT(false);
			return;
		}
		UT_ASSERT(!cluster_undo_recovery_scope_enter_v1(&other, &authority, plan));
		UT_ASSERT(cluster_undo_recovery_origin_authorized_v1(2));
		UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(3));
		UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(-1));
		UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(128));
		cluster_undo_recovery_scope_leave_v1(&other);
		if (fault == 0)
			authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
		if (fault == 1)
			duty.root_lineage_seq++;
		if (fault == 2)
			token.root_lineage_seq++;
		if (fault == 3)
			storage_uuid[0] ^= 1;
		if (fault == 4)
			root.validated_tail_lsn_exclusive++;
		if (fault == 5)
			serial.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
		if (fault == 6)
			source_ok = false;
		if (fault == 7)
			cluster_undo_gcs_coherence = false;
		UT_ASSERT(!cluster_undo_recovery_origin_authorized_v1(2));
		strlcpy(path, "unchanged", sizeof(path));
		UT_ASSERT_EQ(cluster_undo_path_resolve(cluster_undo_recovery_intent_for_owner(3), 3, 513,
											   path, sizeof(path)),
					 -1);
		UT_ASSERT(strcmp(path, "unchanged") == 0);
		UT_ASSERT_EQ(path_calls, 0);
		cluster_undo_recovery_scope_leave_v1(&scope);
	}
}

UT_TEST(test_cached_native_fd_rechecks_recovery_authority)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };

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
	ClusterUndoRecoveryScopeV1 scope = { 0 };

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
	ClusterUndoRecoveryScopeV1 scope = { 0 };

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

UT_TEST(test_multixact_retirement_evidence_requires_current_original_scope)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };

	reset_authority();
	UT_ASSERT(!cluster_undo_recovery_multixact_page_retired_v1(2, 100, 200, true, 0));
	if (!cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(cluster_undo_recovery_multixact_page_retired_v1(2, 100, 200, true, 0));
	UT_ASSERT(!cluster_undo_recovery_multixact_page_retired_v1(1, 100, 200, true, 0));
	UT_ASSERT(!cluster_undo_recovery_multixact_page_retired_v1(2, 101, 200, true, 0));
	UT_ASSERT(!cluster_undo_recovery_multixact_page_retired_v1(2, 100, 200, false, 0));
	authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	UT_ASSERT(!cluster_undo_recovery_multixact_page_retired_v1(2, 100, 200, true, 0));
	cluster_undo_recovery_scope_leave_v1(&scope);
}

UT_TEST(test_cold_native_scope_requires_cold_mode_and_executable_plan)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };

	reset_authority();
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(2));
	UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(2));
	serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	UT_ASSERT(cluster_undo_recovery_cold_origin_authorized_v1(2));
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(1));
	recovery_purpose = false;
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(2));
	recovery_purpose = true;
	authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(2));
	authority_result = CLUSTER_THREAD_AUTHORITY_OK;
	root.validated_tail_lsn_exclusive++;
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(2));
	root.validated_tail_lsn_exclusive--;
	cluster_undo_recovery_scope_leave_v1(&scope);
	UT_ASSERT(!cluster_undo_recovery_cold_origin_authorized_v1(2));
}

UT_TEST(test_cold_source_match_binds_original_scope_and_full_selected_input)
{
	for (unsigned fault = 0; fault <= 16; fault++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };
		ClusterControlRootSnapshot selected;
		ClusterWalSourceRef query;
		reset_authority();
		duty.origin_node_id = 2;
		root.identity = duty;
		serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		memset(&cold_source, 0, sizeof(cold_source));
		cold_source.claim.identity = duty;
		cold_source.claim.database_incarnation = 4;
		cold_source.claim.max_config_generation = 8;
		memset(cold_source.claim.claim_sha256, 0x55, 32);
		cold_source.timeline = 7;
		query = cold_source;
		selected = root;
		cold_source_fault = fault;
		cold_source_reads = 0;
		UT_ASSERT(!cluster_undo_recovery_cold_source_matches_v1(&query, &selected, 120));
		UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
		if (fault == 1)
			query.claim.identity.origin_owner_incarnation++;
		if (fault == 2)
			query.claim.claim_sha256[31] ^= 1;
		if (fault == 3)
			query.claim.database_incarnation++;
		if (fault == 4)
			query.claim.max_config_generation++;
		if (fault == 5)
			query.timeline++;
		if (fault == 6)
			selected.tail_last_record_crc32c++;
		if (fault == 8)
			authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
		if (fault == 9)
			recovery_purpose = false;
		if (fault == 12)
			query.claim.identity.origin_node_id = -1;
		if (fault == 13)
			serial.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		UT_ASSERT_EQ(cluster_undo_recovery_cold_source_matches_v1(fault == 14 ? NULL : &query,
																  fault == 15 ? NULL : &selected,
																  fault == 7 ? 119 : 120),
					 fault == 0);
		if (fault == 0)
			UT_ASSERT_EQ(cold_source_reads, 1);
		cluster_undo_recovery_scope_leave_v1(&scope);
		UT_ASSERT(!cluster_undo_recovery_cold_source_matches_v1(&query, &selected, 120));
	}
}

UT_TEST(test_cold_scope_serial_never_revives_same_address)
{
	ClusterUndoRecoveryScopeV1 scope = { 0 };
	uint64 first, second;
	reset_authority();
	duty.origin_node_id = 2;
	root.identity = duty;
	serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	memset(&cold_source, 0, sizeof(cold_source));
	cold_source.claim.identity = duty;
	cold_source.claim.database_incarnation = 4;
	cold_source.claim.max_config_generation = 8;
	memset(cold_source.claim.claim_sha256, 0x55, 32);
	cold_source.timeline = 7;
	cold_source_fault = cold_source_reads = 0;
	UT_ASSERT_EQ(cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120), 0);
	UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
	first = cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120);
	UT_ASSERT(first != 0);
	UT_ASSERT_EQ(cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120), first);
	UT_ASSERT_EQ(cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 119), 0);
	cluster_undo_recovery_scope_leave_v1(&scope);
	UT_ASSERT_EQ(cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120), 0);
	UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
	second = cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120);
	UT_ASSERT(second != 0 && second != first);
	authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
	UT_ASSERT_EQ(cluster_undo_recovery_cold_scope_id_v1(&cold_source, &root, 120), 0);
	cluster_undo_recovery_scope_leave_v1(&scope);
}

UT_TEST(test_cold_terminal_match_uses_only_original_round_and_plan)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterUndoRecoveryScopeV1 scope = { 0 };
		ClusterWalSourceRef query;
		uint64 round;
		reset_authority();
		duty.origin_node_id = 2;
		root.identity = duty;
		serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		memset(&cold_source, 0, sizeof(cold_source));
		cold_source.claim.identity = duty;
		cold_source.claim.database_incarnation = 4;
		cold_source.claim.max_config_generation = 8;
		memset(cold_source.claim.claim_sha256, 0x55, 32);
		cold_source.timeline = 7;
		query = cold_source;
		cold_source_fault = cold_source_reads = 0;
		terminal_enabled = terminal_plan_ok = terminal_redo_ok = true;
		terminal_plan_calls = 0;
		UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
		round = cluster_undo_recovery_cold_scope_id_v1(&query, &root, 120);
		if (fault == 1)
			round++;
		if (fault == 2)
			terminal_plan_ok = false;
		if (fault == 3)
			terminal_redo_ok = false;
		if (fault == 4)
			query.claim.claim_sha256[31] ^= 1;
		if (fault == 5)
			recovery_purpose = false;
		if (fault == 6)
			authority_result = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
		if (fault == 7) {
			cluster_undo_recovery_scope_leave_v1(&scope);
			UT_ASSERT(cluster_undo_recovery_scope_enter_v1(&scope, &authority, plan));
		}
		UT_ASSERT_EQ(cluster_undo_recovery_cold_terminal_matches_v1(
						 &query, &root, 120, round, fault == 8 ? NULL : &terminal_reader),
					 fault == 0);
		if (fault == 0)
			UT_ASSERT_EQ(terminal_plan_calls, 1);
		if (fault == 1 || fault >= 4)
			UT_ASSERT_EQ(terminal_plan_calls, 0);
		UT_ASSERT_EQ(cluster_undo_recovery_cold_terminal_count_v1(&query, &root, 120, round),
					 fault == 0 || fault == 8 ? 1 : UINT32_MAX);
		cluster_undo_recovery_scope_leave_v1(&scope);
		UT_ASSERT(!cluster_undo_recovery_cold_terminal_matches_v1(&query, &root, 120, round,
																  &terminal_reader));
		UT_ASSERT_EQ(cluster_undo_recovery_cold_terminal_count_v1(&query, &root, 120, round),
					 UINT32_MAX);
		terminal_enabled = false;
	}
}

#include "test_cluster_undo_checkpoint_visit.inc"

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_cold_single_source_uses_only_original_round_and_plan);
	UT_RUN(test_checkpoint_visit_uses_original_execution_owner);
	UT_RUN(test_checkpoint_visit_refuses_missing_original_owner);
	UT_RUN(test_checkpoint_visit_refuses_changed_metadata_and_sealed_cut);
	UT_RUN(test_checkpoint_visit_refuses_callback_and_same_address_reentry);
	UT_RUN(test_checkpoint_visit_error_keeps_original_owner);
	UT_RUN(test_cold_terminal_match_uses_only_original_round_and_plan);
	UT_RUN(test_cold_scope_serial_never_revives_same_address);
	UT_RUN(test_cold_source_match_binds_original_scope_and_full_selected_input);
	UT_RUN(test_cold_source_durability_is_bound_to_original_scope_and_lifetime);
	UT_RUN(test_cold_native_scope_requires_cold_mode_and_executable_plan);
	UT_RUN(test_native_fsync_closes_only_the_canonical_write_obligation);
	UT_RUN(test_native_directory_creation_uses_same_qualified_namespace);
	UT_RUN(test_cached_native_fd_rechecks_recovery_authority);
	UT_RUN(test_original_resolver_reaches_only_scoped_canonical_origin);
	UT_RUN(test_scope_rejects_missing_or_non_mutation_authority);
	UT_RUN(test_stale_scope_never_falls_back_to_local_path);
	UT_RUN(test_multixact_retirement_evidence_requires_current_original_scope);
	UT_DONE();
	return ut_failed_count != 0;
}
