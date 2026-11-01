/* Actual request pipe and original managed creator, with real pair files. */
#include "test_cluster_initdb_recovery_fixture.inc"
#include <sys/wait.h>
#include "catalog/pg_tablespace_d.h"
#include "common/pgrac_initdb_recovery.h"

#include "catalog/pg_control.h"
static bool pgrac_native_recovery_requested;
static PgracInitdbRecoveryContext pgrac_native_recovery;
static ClusterRecoveryFileIdentityV2 pgrac_native_relmap_identities[4];
static int pgrac_native_data_fd, pgrac_native_base_fd;
#include "test_cluster_initdb_recovery_dispatch.inc"

static PgracInitdbConfigContext config;
static ClusterRecoveryFileIdentityV2 first;

static void
setup_request(void)
{
	prepare();
	memset(&config, 0, sizeof(config));
	config.device = 7;
	config.inode = 8;
	config.bytes = 9;
	config.fd = 3;
	config.sha256[0] = 10;
	memset(&first, 0, sizeof(first));
	first.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	first.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	first.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	first.system_identifier = 91;
	first.database_incarnation = 27;
	first.storage_uuid[0] = 1;
	first.redo_branch_uuid[0] = 2;
	first.tablespace_oid = GLOBALTABLESPACE_OID;
	first.origin = 0;
	first.redo_timeline = 1;
	first.object_id = 0;
	first.creation_generation = 1;
	first.registration_generation = 1;
	first.redo_branch_generation = 1;
}

static int
child_read(PgracInitdbRecoveryContext *ctx, unsigned change, bool create)
{
	int request, status;
	pid_t child;
	if (!pgrac_initdb_recovery_pipe_v2(ctx, &request))
		return 7;
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		PgracInitdbRecoveryContext actual;
		PgracInitdbConfigContext expected = config;
		uint8 storage[16];
		bool ok;
		ControlFileData control = { 0 };
		memcpy(storage, first.storage_uuid, 16);
		if (change == 1)
			++expected.inode;
		if (change == 2)
			storage[15] ^= 1;
		memset(&actual, 0xa5, sizeof(actual));
		ok = pgrac_initdb_recovery_read_v2(request, &expected,
										   first.system_identifier + (change == 3),
										   first.database_incarnation + (change == 4), storage,
										   change == 5 ? shared_fd : source_fd, shared_fd, &actual);
		if (!ok) {
			const unsigned char *p = (const unsigned char *)&actual;
			for (size_t i = 0; i < sizeof(actual); ++i)
				if (p[i] != 0xa5)
					_exit(3);
			if (fcntl(request, F_GETFD) != -1 || errno != EBADF)
				_exit(4);
			_exit(1);
		}
		pgrac_native_recovery_requested = true;
		pgrac_native_recovery = actual;
		pgrac_native_data_fd = source_fd;
		pgrac_native_base_fd = shared_fd;
		if (!pgrac_initdb_recovery_relmaps_v2(&actual, pgrac_native_relmap_identities))
			_exit(5);
		control.system_identifier = first.system_identifier;
		control.checkPointCopy.ThisTimeLineID = change == 6 ? 2 : 1;
		if (create && !pgrac_native_create_relmaps(&control))
			_exit(6);
		_exit(0);
	}
	UT_ASSERT(close(request) == 0);
	UT_ASSERT(waitpid(child, &status, 0) == child && WIFEXITED(status));
	return WEXITSTATUS(status);
}

UT_TEST(original_creator_pipe_to_complete_managed_pairs)
{
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryFileIdentityV2 ids[4];
	setup_request();
	if (!pgrac_initdb_recovery_prepare_v2(&first, &config, source_fd, shared_fd, &ctx)) {
		UT_ASSERT(false);
		cleanup();
		return;
	}
	UT_ASSERT(pgrac_initdb_recovery_relmaps_v2(&ctx, ids));
	for (unsigned i = 0; i < 4; ++i) {
		UT_ASSERT_EQ(ids[i].database_oid, map_databases[i]);
		UT_ASSERT_EQ(ids[i].object_id, map_databases[i]);
		UT_ASSERT_EQ(ids[i].creation_generation, 1 + i);
		UT_ASSERT_EQ(ids[i].registration_generation, 1);
	}
	UT_ASSERT_EQ(child_read(&ctx, 0, true), 0);
	for (unsigned i = 0; i < 4; ++i) {
		int fd = openat(shared_fd, paths[i], O_RDONLY | O_DIRECTORY);
		ClusterRelmapFilePairV2 pair;
		UT_ASSERT(fd >= 0 && cluster_relmap_file_pair_read_v2(fd, &ids[i], &pair));
		UT_ASSERT(close(fd) == 0);
	}
	UT_ASSERT_EQ(child_read(&ctx, 0, true), 6); /* no overwrite/adoption */
	cleanup();
}

UT_TEST(stale_binding_refuses_before_any_pair_creation)
{
	PgracInitdbRecoveryContext ctx;
	struct stat st;
	setup_request();
	if (!pgrac_initdb_recovery_prepare_v2(&first, &config, source_fd, shared_fd, &ctx)) {
		UT_ASSERT(false);
		cleanup();
		return;
	}
	for (unsigned i = 1; i <= 5; ++i)
		UT_ASSERT_EQ(child_read(&ctx, i, true), 1);
	UT_ASSERT_EQ(child_read(&ctx, 6, true), 6);
	UT_ASSERT(fstatat(shared_fd, "global/pgrac_relmap_authority", &st, AT_SYMLINK_NOFOLLOW) == -1
			  && errno == ENOENT);
	cleanup();
}

UT_TEST(preregistration_rejects_bad_birth_and_alias_without_io)
{
	PgracInitdbRecoveryContext ctx, saved;
	setup_request();
	memset(&ctx, 0x6a, sizeof(ctx));
	saved = ctx;
	for (unsigned i = 0; i < 7; ++i) {
		ClusterRecoveryFileIdentityV2 bad = first;
		if (i == 0)
			bad.origin = 1;
		if (i == 1)
			bad.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
		if (i == 2)
			bad.creation_generation = UINT64_MAX - 2;
		if (i == 3)
			bad.registration_generation = 0;
		if (i == 4)
			bad.redo_timeline = 2;
		if (i == 5)
			bad.database_oid = 1;
		if (i == 6)
			bad.object_id = 1;
		UT_ASSERT(!pgrac_initdb_recovery_prepare_v2(&bad, &config, source_fd, shared_fd, &ctx));
		UT_ASSERT(memcmp(&ctx, &saved, sizeof(ctx)) == 0);
	}
	UT_ASSERT(!pgrac_initdb_recovery_prepare_v2(&ctx.first, &config, -1, -1, &ctx));
	UT_ASSERT(memcmp(&ctx, &saved, sizeof(ctx)) == 0);
	cleanup();
}

UT_TEST(wrong_parent_and_closed_or_malformed_request_refuse)
{
	PgracInitdbRecoveryContext ctx, out;
	setup_request();
	if (!pgrac_initdb_recovery_prepare_v2(&first, &config, source_fd, shared_fd, &ctx)) {
		UT_ASSERT(false);
		cleanup();
		return;
	}
	ctx.creator_pid++;
	UT_ASSERT_EQ(child_read(&ctx, 0, true), 1);
	for (unsigned variant = 0; variant < 3; ++variant) {
		int fds[2];
		size_t length = variant == 0 ? sizeof(ctx) - 1 : sizeof(ctx);
		UT_ASSERT(pipe(fds) == 0);
		UT_ASSERT(write(fds[1], &ctx, length) == length);
		if (variant == 1)
			UT_ASSERT(write(fds[1], "x", 1) == 1);
		UT_ASSERT(close(fds[1]) == 0);
		UT_ASSERT(!pgrac_initdb_recovery_read_v2(fds[0], &config, first.system_identifier,
												 first.database_incarnation, first.storage_uuid,
												 source_fd, shared_fd, &out));
		UT_ASSERT(fcntl(fds[0], F_GETFD) == -1 && errno == EBADF);
	}
	cleanup();
}

int
main(int argc, char **argv)
{
	pg_logging_init(argv[0]);
	UT_PLAN(4);
	UT_RUN(original_creator_pipe_to_complete_managed_pairs);
	UT_RUN(stale_binding_refuses_before_any_pair_creation);
	UT_RUN(preregistration_rejects_bad_birth_and_alias_without_io);
	UT_RUN(wrong_parent_and_closed_or_malformed_request_refuse);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
