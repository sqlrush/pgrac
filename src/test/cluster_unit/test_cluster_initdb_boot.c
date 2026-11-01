/* Original A dispatch; only F's family producers are controlled leaves. */
#include "postgres.h"
#include <setjmp.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "common/pgrac_initdb_recovery.h"
#include "common/cluster_recovery_bootstrap.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_undo_segment.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static PgracInitdbRecoveryContext pgrac_native_recovery, initdb_recovery_context;
static bool initdb_recovery_present;
static bool pgrac_native_recovery_requested, present;
static unsigned raw_calls, managed_calls, undo_calls, fault;
static jmp_buf failure;
static const PgracInitdbRecoveryContext *
cluster_wal_thread_initdb_recovery_context(void)
{
	return present ? &pgrac_native_recovery : NULL;
}
static void
BootStrapCLOG(void)
{
	++raw_calls;
}
static void
result(const ClusterRecoveryFileIdentityV2 *id, ClusterRecoveryBootFileV2 *out)
{
	memset(out, 0, sizeof(*out));
	out->identity = *id;
	out->page.page_generation = id->kind == CLUSTER_RECOVERY_FILE_CLOG_V2 ? 3 : 0;
	out->image_digest[0] = 1;
	out->synced_bytes = id->kind == CLUSTER_RECOVERY_FILE_UNDO_V2 ? UNDO_SEGMENT_SIZE_BYTES : 16448;
	if (fault == 2)
		++out->identity.creation_generation;
	if (fault == 3)
		out->image_digest[0] = 0;
	if (fault == 4)
		++pgrac_native_recovery.boot_files[0].creation_generation;
}
bool
BootStrapCLOGManagedV2(const ClusterRecoveryFileIdentityV2 *id, FullTransactionId xid,
					   ClusterRecoveryBootFileV2 *out)
{
	++managed_calls;
	if (fault == 1)
		return false;
	if (fault == 5)
		longjmp(failure, 1);
	result(id, out);
	out->page.birth_observed = true;
	out->page.birth_full_xid = xid;
	if (fault == 6)
		out->page.birth_full_xid = FullTransactionIdFromU64(4);
	return true;
}
bool
cluster_undo_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
						  ClusterRecoveryBootFileV2 *out)
{
	++undo_calls;
	UT_ASSERT_EQ(fd, 71);
	if (fault == 1)
		return false;
	result(id, out);
	return true;
}
#undef ereport
#define ereport(level, args) longjmp(failure, 1)
#include "test_cluster_initdb_boot_entries.inc"

static CheckPoint checkpoint;
static void
setup(void)
{
	ClusterRecoveryFileIdentityV2 *id;
	memset(&pgrac_native_recovery, 0, sizeof(pgrac_native_recovery));
	pgrac_native_recovery.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	pgrac_native_recovery.version = 2;
	pgrac_native_recovery.creator_pid = 7;
	pgrac_native_recovery.config.fd = 3;
	pgrac_native_recovery.config.bytes = 1;
	pgrac_native_recovery.config.sha256[0] = 1;
	id = &pgrac_native_recovery.first;
	id->kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id->format_version = 2;
	id->system_identifier = 91;
	id->database_incarnation = 27;
	id->storage_uuid[0] = 1;
	id->redo_branch_uuid[0] = 2;
	id->redo_branch_generation = id->creation_generation = id->registration_generation = 1;
	id->redo_timeline = 1;
	id->tablespace_oid = GLOBALTABLESPACE_OID;
	pgrac_native_recovery.boot_file_count = 2;
	for (unsigned i = 0; i < 2; ++i) {
		pgrac_native_recovery.boot_files[i] = *id;
		pgrac_native_recovery.boot_files[i].kind
			= i ? CLUSTER_RECOVERY_FILE_UNDO_V2 : CLUSTER_RECOVERY_FILE_CLOG_V2;
		pgrac_native_recovery.boot_files[i].database_oid = 0;
		pgrac_native_recovery.boot_files[i].tablespace_oid = 0;
		pgrac_native_recovery.boot_files[i].creation_generation = 5 + i;
	}
	memset(&checkpoint, 0, sizeof(checkpoint));
	checkpoint.ThisTimeLineID = 1;
	checkpoint.nextXid = FullTransactionIdFromU64(3);
	present = pgrac_native_recovery_requested = true;
	raw_calls = managed_calls = undo_calls = fault = 0;
}
UT_TEST(original_bootstrap_uses_complete_identity_and_actual_initial_xid)
{
	setup();
	if (!setjmp(failure))
		BootStrapCLOGWithIdentity(&checkpoint, 91);
	else
		UT_ASSERT(false);
	UT_ASSERT_EQ(managed_calls, 1);
	UT_ASSERT_EQ(raw_calls, 0);
}
UT_TEST(legacy_bootstrap_keeps_original_raw_call)
{
	setup();
	present = false;
	BootStrapCLOGWithIdentity(&checkpoint, 91);
	UT_ASSERT_EQ(raw_calls, 1);
	UT_ASSERT_EQ(managed_calls, 0);
}
UT_TEST(wrong_original_identity_refuses_before_producer)
{
	setup();
	if (!setjmp(failure)) {
		BootStrapCLOGWithIdentity(&checkpoint, 92);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(managed_calls, 0);
	UT_ASSERT_EQ(raw_calls, 0);
}
UT_TEST(producer_false_error_wrong_receipt_and_late_input_never_fall_back)
{
	for (unsigned i = 1; i <= 6; ++i) {
		setup();
		fault = i;
		if (!setjmp(failure)) {
			BootStrapCLOGWithIdentity(&checkpoint, 91);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(raw_calls, 0);
		UT_ASSERT_EQ(managed_calls, 1);
	}
}
UT_TEST(original_seed_dispatch_checks_complete_undo_result)
{
	setup();
	UT_ASSERT(pgrac_native_create_boot_undo(71));
	UT_ASSERT_EQ(undo_calls, 1);
	for (unsigned i = 1; i <= 4; ++i) {
		setup();
		fault = i;
		UT_ASSERT(!pgrac_native_create_boot_undo(71));
		UT_ASSERT_EQ(undo_calls, 1);
	}
}
UT_TEST(incomplete_native_request_never_calls_either_family)
{
	setup();
	pgrac_native_recovery.boot_file_count = 1;
	UT_ASSERT(!pgrac_native_create_boot_undo(71));
	if (!setjmp(failure)) {
		BootStrapCLOGWithIdentity(&checkpoint, 91);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(raw_calls + managed_calls + undo_calls, 0);
}
UT_TEST(native_child_consumes_boot_fd_without_shared_base_publication_custody)
{
	char temp[] = "/tmp/pgrac-boot-native-XXXXXX";
	PgracInitdbRecoveryContext request;
	PgracInitdbWalContext ctx = { 0 };
	int root, data, shared, pipefd, status;
	pid_t child;
	setup();
	UT_ASSERT(mkdtemp(temp) != NULL);
	root = open(temp, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(root >= 0);
	UT_ASSERT(!mkdirat(root, "data", 0700) && !mkdirat(root, "shared", 0700));
	data = openat(root, "data", O_RDONLY | O_DIRECTORY);
	shared = openat(root, "shared", O_RDONLY | O_DIRECTORY);
	UT_ASSERT(data >= 0 && shared >= 0);
	UT_ASSERT(pgrac_initdb_recovery_prepare_files_v2(
		&pgrac_native_recovery.first, pgrac_native_recovery.boot_files,
		&pgrac_native_recovery.config, data, shared, &request));
	UT_ASSERT(pgrac_initdb_recovery_pipe_v2(&request, &pipefd));
	ctx.phase = PGRAC_INITDB_WAL_BOOTSTRAP;
	ctx.thread_id = 1;
	ctx.system_identifier = request.first.system_identifier;
	ctx.database_incarnation = request.first.database_incarnation;
	memcpy(ctx.storage_uuid, request.first.storage_uuid, 16);
	ctx.base_fd = shared;
	ctx.recovery_fd = pipefd;
	ctx.config = request.config;
	/* A real inherited configuration FD is separately owned by the native
	 * child; it must not alias the temporary original shared-directory proof. */
	ctx.config.fd = root;
	child = fork();
	UT_ASSERT(child >= 0);
	if (!child) {
		bool ok = fchdir(data) == 0 && initdb_boot_recovery_accept(&ctx);
		ok = ok && ctx.base_fd == 0 && ctx.recovery_fd == 0 && ctx.database_incarnation == 0
			 && ctx.base_device == 0 && ctx.base_inode == 0 && initdb_recovery_present
			 && !memcmp(&initdb_recovery_context, &request, sizeof(request))
			 && fcntl(shared, F_GETFD) < 0 && errno == EBADF && fcntl(root, F_GETFD) >= 0;
		_exit(ok ? 0 : 7);
	}
	UT_ASSERT(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
	UT_ASSERT(!close(pipefd) && !close(data) && !close(shared));
	UT_ASSERT(!unlinkat(root, "data", AT_REMOVEDIR) && !unlinkat(root, "shared", AT_REMOVEDIR));
	UT_ASSERT(!close(root) && !rmdir(temp));
}

UT_TEST(boot_result_keeps_clog_birth_and_unused_tt_facts_separate)
{
	ClusterRecoveryBootFileV2 tt, bad, clog;
	ClusterRecoveryFileIdentityV2 other;
	setup();
	UT_ASSERT(cluster_undo_bootstrap_v2(71, &pgrac_native_recovery.boot_files[1], &tt));
	UT_ASSERT(cluster_recovery_boot_file_matches_v2(&pgrac_native_recovery.boot_files[1], &tt));
	UT_ASSERT(BootStrapCLOGManagedV2(&pgrac_native_recovery.boot_files[0],
									 FirstNormalFullTransactionId, &clog));
	UT_ASSERT(cluster_recovery_boot_file_matches_v2(&pgrac_native_recovery.boot_files[0], &clog));
	for (unsigned i = 0; i < 5; ++i) {
		bad = tt;
		if (i == 0)
			bad.page.page_generation = 1;
		if (i == 1)
			bad.page.birth_observed = true;
		if (i == 2)
			bad.page.birth_full_xid = FirstNormalFullTransactionId;
		if (i == 3)
			bad.page.page_lsn = 1;
		if (i == 4)
			bad.page.metadata_crc = 1;
		UT_ASSERT(
			!cluster_recovery_boot_file_matches_v2(&pgrac_native_recovery.boot_files[1], &bad));
	}
	bad = clog;
	bad.page.birth_observed = false;
	UT_ASSERT(!cluster_recovery_boot_file_matches_v2(&pgrac_native_recovery.boot_files[0], &bad));
	bad = clog;
	++bad.synced_bytes;
	UT_ASSERT(!cluster_recovery_boot_file_matches_v2(&pgrac_native_recovery.boot_files[0], &bad));
	bad = clog;
	bad.page.page_generation = 4;
	UT_ASSERT(!cluster_recovery_boot_file_matches_v2(&pgrac_native_recovery.boot_files[0], &bad));
	other = pgrac_native_recovery.first;
	bad = tt;
	bad.identity = other;
	UT_ASSERT(!cluster_recovery_boot_file_matches_v2(&other, &bad));
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(boot_result_keeps_clog_birth_and_unused_tt_facts_separate);
	UT_RUN(native_child_consumes_boot_fd_without_shared_base_publication_custody);
	UT_RUN(original_bootstrap_uses_complete_identity_and_actual_initial_xid);
	UT_RUN(legacy_bootstrap_keeps_original_raw_call);
	UT_RUN(wrong_original_identity_refuses_before_producer);
	UT_RUN(producer_false_error_wrong_receipt_and_late_input_never_fall_back);
	UT_RUN(original_seed_dispatch_checks_complete_undo_result);
	UT_RUN(incomplete_native_request_never_calls_either_family);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
