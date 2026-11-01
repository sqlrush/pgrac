/* Original initdb binding and child options, with the real closed pipe reader. */
#include "test_cluster_initdb_recovery_fixture.inc"
#include <limits.h>
#include <sys/wait.h>
#include "catalog/pg_tablespace_d.h"
#include "common/pgrac_initdb_recovery.h"

static int pgrac_native_thread;
static bool made_new_pgdata, found_existing_pgdata;
static bool made_new_xlogdir, found_existing_xlogdir;
static char *pgrac_native_data_path, *pgrac_native_wal_path, *pgrac_native_base_path;
static int pgrac_native_data_fd, pgrac_native_wal_fd, pgrac_native_base_fd;
static PgracInitdbConfigContext pgrac_native_config;
static uint64 pgrac_native_sysid, pgrac_native_database_incarnation;
static uint8 pgrac_native_storage_bytes[16];
static bool pgrac_native_recovery_requested;
static PgracInitdbRecoveryContext pgrac_native_recovery;
static ClusterRecoveryFileIdentityV2 pgrac_native_relmap_identities[4];
static bool expect_refusal;
static unsigned config_checks;

static void pgrac_native_check_directories(struct stat *, struct stat *);
static void pgrac_native_config_check(bool);
static void pg_attribute_noreturn() bind_refuse(void);
#undef pg_fatal
#define pg_fatal(...) bind_refuse()
#include "test_cluster_initdb_child_sync_original.inc"
#undef pg_fatal

/* Original directory/config guards are controlled leaves here. Request
 * identity, complete cohort, pipe ownership/close and binding body are real. */
static void
pgrac_native_check_directories(struct stat *data, struct stat *wal)
{
	if (fstat(pgrac_native_data_fd, data) != 0 ||
		fstat(pgrac_native_wal_fd, wal) != 0)
		_exit(20);
}

static void
pgrac_native_config_check(bool after)
{
	if (after)
		_exit(21);
	++config_checks;
}

static bool
legacy_options(void)
{
	return strcmp(boot_options, "-F -c log_checkpoints=false") == 0 &&
		strcmp(backend_options,
			   "--single -F -O -j -c search_path=pg_catalog -c exit_on_error=true -c log_checkpoints=false") == 0;
}

static void
bind_refuse(void)
{
	_exit(expect_refusal && !pgrac_native_recovery_requested &&
		  config_checks == 0 && legacy_options() ? 0 : 22);
}

static int
bound_child(uint16 origin, unsigned check, bool wrong_origin)
{
	PgracInitdbRecoveryContext request;
	ClusterRecoveryFileIdentityV2 first = {0}, clog;
	uint64 cohort[2] = {3, 0};
	char data[MAXPGPATH], shared[MAXPGPATH];
	int pipe_fd, status;
	pid_t child;
	prepare();
	first.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	first.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	first.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	first.system_identifier = 91;
	first.database_incarnation = 27;
	first.storage_uuid[0] = 1;
	first.redo_branch_uuid[0] = 2;
	first.tablespace_oid = GLOBALTABLESPACE_OID;
	first.redo_timeline = 1;
	first.creation_generation = first.registration_generation = 1;
	first.redo_branch_generation = 1;
	pgrac_native_config.device = 7;
	pgrac_native_config.inode = 8;
	pgrac_native_config.bytes = 9;
	pgrac_native_config.fd = 3;
	pgrac_native_config.sha256[0] = 10;
	clog = first;
	clog.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	clog.tablespace_oid = 0;
	clog.origin = origin;
	clog.creation_generation = 5 + origin;
	UT_ASSERT(pgrac_initdb_recovery_prepare_cohort_v2(&first, &clog, cohort, origin,
													  &pgrac_native_config, source_fd, shared_fd, &request));
	UT_ASSERT(pgrac_initdb_recovery_pipe_v2(&request, &pipe_fd));
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0)
	{
		char value[32];
		/* The original founder binds an empty shared target before creation. */
		if (origin == 0)
			remove_contents(shared_fd);
		snprintf(data, sizeof(data), "%s/source", root);
		snprintf(shared, sizeof(shared), "%s/shared", root);
		pgrac_native_data_path = pgrac_native_wal_path = data;
		pgrac_native_base_path = shared;
		pgrac_native_thread = wrong_origin ? 3 : origin + 1;
		made_new_pgdata = made_new_xlogdir = true;
		pgrac_native_sysid = first.system_identifier;
		pgrac_native_database_incarnation = first.database_incarnation;
		memcpy(pgrac_native_storage_bytes, first.storage_uuid, 16);
		expect_refusal = wrong_origin;
		snprintf(value, sizeof(value), "%d", pipe_fd);
		if (setenv(PGRAC_INITDB_RECOVERY_ENV, value, 1) != 0)
			_exit(23);
		pgrac_native_bind_creation();
		if (wrong_origin || !pgrac_native_recovery_requested || config_checks != 1 ||
			getenv(PGRAC_INITDB_RECOVERY_ENV) != NULL ||
			fcntl(pipe_fd, F_GETFD) != -1 || errno != EBADF)
			_exit(24);
		if (check == 0)
			_exit(strcmp(boot_options, "-c fsync=on -c log_checkpoints=false") == 0 ? 0 : 25);
		_exit(strcmp(backend_options,
					 "--single -O -j -c fsync=on -c search_path=pg_catalog -c exit_on_error=true -c log_checkpoints=false") == 0 ? 0 : 26);
	}
	UT_ASSERT(close(pipe_fd) == 0);
	UT_ASSERT(waitpid(child, &status, 0) == child && WIFEXITED(status));
	cleanup();
	return WIFEXITED(status) ? WEXITSTATUS(status) : 27;
}

UT_TEST(ordinary_initdb_retains_original_options)
{
	pgrac_native_thread = -1;
	pgrac_native_bind_creation();
	UT_ASSERT(legacy_options());
	UT_ASSERT(!pgrac_native_recovery_requested && config_checks == 0);
}

UT_TEST(authenticated_founder_and_peer_bootstrap_enable_sync)
{
	UT_ASSERT_EQ(bound_child(0, 0, false), 0);
	UT_ASSERT_EQ(bound_child(1, 0, false), 0);
}

UT_TEST(authenticated_founder_and_peer_single_user_enable_sync)
{
	UT_ASSERT_EQ(bound_child(0, 1, false), 0);
	UT_ASSERT_EQ(bound_child(1, 1, false), 0);
}

UT_TEST(wrong_cohort_origin_never_installs_managed_options)
{
	UT_ASSERT_EQ(bound_child(0, 0, true), 0);
	UT_ASSERT_EQ(bound_child(1, 1, true), 0);
}

int
main(int argc, char **argv)
{
	pg_logging_init(argv[0]);
	UT_PLAN(4);
	UT_RUN(ordinary_initdb_retains_original_options);
	UT_RUN(authenticated_founder_and_peer_bootstrap_enable_sync);
	UT_RUN(authenticated_founder_and_peer_single_user_enable_sync);
	UT_RUN(wrong_cohort_origin_never_installs_managed_options);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
