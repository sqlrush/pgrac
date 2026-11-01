/* Actual request pipe and original managed creator, with real pair files. */
#undef FRONTEND
#include "postgres.h"
#define FRONTEND 1
#include "test_cluster_initdb_recovery_fixture.inc"
#include <sys/wait.h>
#include <setjmp.h>
#include "common/pgrac_initdb_cohort.h"
#include "common/cluster_recovery_bootstrap.h"
#include "common/cluster_recovery_registry.h"
#undef FRONTEND
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_wal_thread.h"
#define FRONTEND 1
#include "../../backend/cluster/cluster_initdb_origin_private.h"
#include "../../backend/cluster/cluster_initdb_tree_private.h"
#include "../../backend/cluster/cluster_cf_contract_private.h"
#include "../../bin/initdb/pgrac_wal.h"
#include "catalog/pg_tablespace_d.h"
#include "common/pgrac_initdb_recovery.h"
#include "cluster/cluster_recovery_checkpoint_coverage.h"

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

static jmp_buf creation_failure;
static bool expect_creation_failure;
static void
pg_attribute_noreturn() refuse(const char *reason) {
	if (expect_creation_failure)
		longjmp(creation_failure, 1);
	fprintf(stderr, "unexpected creator refusal: %s\n", reason);
	abort();
}
#include "test_cluster_initdb_recovery_owner.inc"
static bool
creation_boot_current(InitdbOrigin *o, bool first)
{
	return o->relmap_registration.boot_file_count == 0;
}

static PgracInitdbCohortContext original_request;
static ClusterSharedConfigRef original_configuration;
static unsigned child_calls, control_calls, legacy_calls;
static bool change_registration;
static void
request_current(const PgracInitdbCohortContext *r,
							const ClusterSharedConfigRef *c) {
	UT_ASSERT(r == &original_request && c == &original_configuration);
}
static void
run_child_registered(char **args, uint64 creator,
								 const PgracInitdbRecoveryContext *ctx) {
	UT_ASSERT(args[0] != NULL && creator == (uint64)getpid());
	UT_ASSERT_EQ(child_read((PgracInitdbRecoveryContext *)ctx, 0, true), 0);
	++child_calls;
}
static void
run_child(char **args, uint64 creator) {
	UT_ASSERT(args[0] != NULL && creator == (uint64)getpid());
	++legacy_calls;
}
static void
control_read(InitdbOrigin *o, uint32 thread, uint64 system, bool first_read) {
	UT_ASSERT(thread == 1 && system == first.system_identifier);
	++control_calls;
	if (first_read && change_registration)
		++o->relmap_registration.first.creation_generation;
}
static void
directory_current(const InitdbDirectory *d) {
	UT_ASSERT(d != NULL);
}
static void
creation_side_current(const InitdbDirectory *d, const InitdbOrigin *o, unsigned n) {
	UT_ASSERT(d != NULL && o != NULL && n == 0);
}
static void
creation_tree_recheck(int fd, bool derived, const ClusterInitdbTree *t) {
	UT_ASSERT(fd >= 0 && t != NULL);
}
static void
creation_storage_current(const InitdbOrigin *o) {
	UT_ASSERT(o != NULL);
}

static InitdbOrigin creator_origins[128];
static void
creator_setup(PgracInitdbRecoveryContext *ctx) {
	setup_request();
	UT_ASSERT(pgrac_initdb_recovery_prepare_v2(&first, &config, source_fd, shared_fd, ctx));
	memset(creator_origins, 0, sizeof(creator_origins));
	creator_origins[0].data.fd = source_fd;
	creator_origins[0].wal.fd = source_fd;
	strlcpy(creator_origins[0].data.path, "original-data", MAXPGPATH);
	memset(&original_request, 0, sizeof(original_request));
	original_request.creator_pid = getpid();
	original_request.segment_size = 16 * 1024 * 1024;
	memset(&original_configuration, 0, sizeof(original_configuration));
	original_configuration.identity.system_identifier = first.system_identifier;
	original_configuration.identity.database_incarnation = first.database_incarnation;
	original_configuration.identity.configured[0] = 1;
	memcpy(original_configuration.identity.storage_uuid, first.storage_uuid, 16);
	memcpy(original_configuration.sha256, config.sha256, 32);
	child_calls = control_calls = legacy_calls = 0;
	change_registration = false;
}
static void
creator_run(const PgracInitdbRecoveryContext *ctx) {
	char shared[MAXPGPATH];
	snprintf(shared, sizeof(shared), "%s/shared", root);
	run_origin("original-initdb", &original_request, &original_configuration, shared,
			   &creator_origins[0], 0, ctx);
}
static void
creator_current(void) {
	InitdbDirectory dirs[4] = {0};
	ClusterInitdbTree t = {0};
	for (unsigned i = 0; i < 4; ++i)
		dirs[i].fd = shared_fd;
	creation_sources_current(&original_request, &original_configuration, dirs, creator_origins,
							 &t, &t, true);
}

UT_TEST(original_creator_retains_actual_pairs_through_registry_selection) {
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryRegistryV2 registry = {0};
	creator_setup(&ctx);
	creator_run(&ctx);
	UT_ASSERT_EQ(child_calls, 1);
	UT_ASSERT_EQ(control_calls, 1);
	UT_ASSERT(creator_origins[0].managed_relmap);
	UT_ASSERT(memcmp(&ctx, &creator_origins[0].relmap_registration, sizeof(ctx)) == 0);
	registry.coverage.file_count = 8;
	registry.coverage.files = creator_origins[0].relmap_observations;
	UT_ASSERT(creation_relmap_registry_valid(creator_origins, &registry));
	creator_current();
	UT_ASSERT_EQ(control_calls, 2);
	cleanup();
}

UT_TEST(original_creator_current_refuses_late_pair_change_without_rebase) {
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryCheckpointFileV2 saved[8];
	int fd;
	creator_setup(&ctx);
	creator_run(&ctx);
	memcpy(saved, creator_origins[0].relmap_observations, sizeof(saved));
	fd = openat(shared_fd, "base/4/pgrac_relmap_authority", O_WRONLY);
	UT_ASSERT(fd >= 0 && pwrite(fd, "X", 1, 200) == 1 && close(fd) == 0);
	expect_creation_failure = true;
	if (setjmp(creation_failure) == 0) {
		creator_current();
		UT_ASSERT(false);
	}
	expect_creation_failure = false;
	UT_ASSERT(memcmp(saved, creator_origins[0].relmap_observations, sizeof(saved)) == 0);
	UT_ASSERT(fcntl(source_fd, F_GETFD) >= 0 && fcntl(shared_fd, F_GETFD) >= 0);
	cleanup();
}

UT_TEST(original_creator_rejects_late_registration_and_preserves_legacy_entry) {
	PgracInitdbRecoveryContext ctx;
	creator_setup(&ctx);
	change_registration = true;
	expect_creation_failure = true;
	if (setjmp(creation_failure) == 0) {
		creator_run(&ctx);
		UT_ASSERT(false);
	}
	expect_creation_failure = false;
	UT_ASSERT(!creator_origins[0].managed_relmap);
	UT_ASSERT_EQ(child_calls, 1);
	cleanup();
	creator_setup(&ctx);
	creator_run(NULL);
	UT_ASSERT_EQ(legacy_calls, 1);
	UT_ASSERT_EQ(child_calls, 0);
	UT_ASSERT(!creator_origins[0].managed_relmap);
	cleanup();
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

/* The observation has no sources/coverage until the original cohort supplies
 * its complete native checkpoint vector. Identity always comes from ctx. */
static void
created_request(PgracInitdbRecoveryContext *ctx) {
	setup_request();
	UT_ASSERT(pgrac_initdb_recovery_prepare_v2(&first, &config, source_fd, shared_fd, ctx));
	UT_ASSERT_EQ(child_read(ctx, 0, true), 0);
}

UT_TEST(actual_created_pairs_return_complete_independent_observations) {
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryCheckpointFileV2 observations[8];
	ClusterRecoveryFileIdentityV2 ids[4];
	created_request(&ctx);
	UT_ASSERT(pgrac_initdb_recovery_relmaps_v2(&ctx, ids));
	UT_ASSERT(
		pgrac_initdb_recovery_relmap_observe_v2(&ctx, source_fd, shared_fd, observations));
	for (unsigned i = 0; i < 8; ++i) {
		const ClusterRecoveryCheckpointFileV2 *f = &observations[i];
		uint8 encoded[128];
		UT_ASSERT(
			cluster_recovery_file_identity_encode_v2(&f->identity, encoded, sizeof(encoded)));
		UT_ASSERT(
			cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded), &ids[i / 2]));
		UT_ASSERT_EQ(f->copy_index, i % 2);
		UT_ASSERT_EQ(f->copy_count, 2);
		UT_ASSERT_EQ(f->synced_bytes, CLUSTER_RELMAP_FILE_SIZE_V2);
		UT_ASSERT_EQ(f->first_page, 0);
		UT_ASSERT_EQ(f->page_count, 1);
		UT_ASSERT_EQ(f->source_count, 0);
		UT_ASSERT(f->sources == NULL);
	}
	UT_ASSERT(
		pgrac_initdb_recovery_relmap_current_v2(&ctx, source_fd, shared_fd, observations));
	UT_ASSERT(fcntl(source_fd, F_GETFD) >= 0 && fcntl(shared_fd, F_GETFD) >= 0);
	cleanup();
}

UT_TEST(wrong_preallocated_birth_and_partial_pair_preserve_whole_output) {
	PgracInitdbRecoveryContext ctx, wrong;
	ClusterRecoveryCheckpointFileV2 out[8], saved[8];
	int fd;
	created_request(&ctx);
	memset(out, 0xa5, sizeof(out));
	memcpy(saved, out, sizeof(out));
	wrong = ctx;
	++wrong.first.creation_generation;
	UT_ASSERT(!pgrac_initdb_recovery_relmap_observe_v2(&wrong, source_fd, shared_fd, out));
	UT_ASSERT(memcmp(out, saved, sizeof(out)) == 0);
	fd = openat(shared_fd, "base/5/pgrac_relmap_authority.bak", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, 100) == 0 && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_recovery_relmap_observe_v2(&ctx, source_fd, shared_fd, out));
	UT_ASSERT(memcmp(out, saved, sizeof(out)) == 0);
	UT_ASSERT(fcntl(source_fd, F_GETFD) >= 0 && fcntl(shared_fd, F_GETFD) >= 0);
	cleanup();
}

UT_TEST(late_image_digest_and_original_directory_mismatch_refuse) {
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryCheckpointFileV2 out[8];
	int fd;
	created_request(&ctx);
	UT_ASSERT(pgrac_initdb_recovery_relmap_observe_v2(&ctx, source_fd, shared_fd, out));
	out[7].image_digest[0] ^= 1;
	UT_ASSERT(!pgrac_initdb_recovery_relmap_current_v2(&ctx, source_fd, shared_fd, out));
	out[7].image_digest[0] ^= 1;
	UT_ASSERT(!pgrac_initdb_recovery_relmap_current_v2(&ctx, shared_fd, shared_fd, out));
	fd = openat(shared_fd, "global/pgrac_relmap_authority", O_WRONLY);
	UT_ASSERT(fd >= 0 && pwrite(fd, "X", 1, 200) == 1 && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_recovery_relmap_current_v2(&ctx, source_fd, shared_fd, out));
	cleanup();
}

UT_TEST(unexpected_database_and_symlink_never_form_complete_inventory) {
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryCheckpointFileV2 out[8], saved[8];
	created_request(&ctx);
	memset(out, 0xa5, sizeof(out));
	memcpy(saved, out, sizeof(out));
	UT_ASSERT(mkdirat(shared_fd, "base/99", 0700) == 0);
	UT_ASSERT(!pgrac_initdb_recovery_relmap_observe_v2(&ctx, source_fd, shared_fd, out));
	UT_ASSERT(memcmp(out, saved, sizeof(out)) == 0);
	UT_ASSERT(unlinkat(shared_fd, "base/99", AT_REMOVEDIR) == 0);
	UT_ASSERT(unlinkat(shared_fd, "base/5/pgrac_relmap_authority.bak", 0) == 0);
	UT_ASSERT(symlinkat("pgrac_relmap_authority", shared_fd,
						"base/5/pgrac_relmap_authority.bak") == 0);
	UT_ASSERT(!pgrac_initdb_recovery_relmap_observe_v2(&ctx, source_fd, shared_fd, out));
	UT_ASSERT(memcmp(out, saved, sizeof(out)) == 0);
	cleanup();
}

UT_TEST(observation_alias_and_wrong_creator_refuse_before_output) {
	union {
		PgracInitdbRecoveryContext ctx;
		ClusterRecoveryCheckpointFileV2 out[8];
	} alias, saved;
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryCheckpointFileV2 out[8], original[8];
	created_request(&ctx);
	memset(&alias, 0xa5, sizeof(alias));
	alias.ctx = ctx;
	saved = alias;
	UT_ASSERT(
		!pgrac_initdb_recovery_relmap_observe_v2(&alias.ctx, source_fd, shared_fd, alias.out));
	UT_ASSERT(memcmp(&alias, &saved, sizeof(alias)) == 0);
	memset(out, 0xa5, sizeof(out));
	memcpy(original, out, sizeof(out));
	++ctx.creator_pid;
	UT_ASSERT(!pgrac_initdb_recovery_relmap_observe_v2(&ctx, source_fd, shared_fd, out));
	UT_ASSERT(memcmp(out, original, sizeof(out)) == 0);
	cleanup();
}

static void
boot_inputs(ClusterRecoveryFileIdentityV2 b[2])
{
	b[0] = b[1] = first;
	for (unsigned i = 0; i < 2; ++i) {
		b[i].kind = i ? CLUSTER_RECOVERY_FILE_UNDO_V2 : CLUSTER_RECOVERY_FILE_CLOG_V2;
		b[i].database_oid = b[i].tablespace_oid = 0;
		b[i].object_id = 0;
		b[i].creation_generation = 5 + i;
	}
}
UT_TEST(complete_boot_files_survive_original_closed_pipe)
{
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryFileIdentityV2 boot[2], actual[2];
	setup_request();
	boot_inputs(boot);
	UT_ASSERT(
		pgrac_initdb_recovery_prepare_files_v2(&first, boot, &config, source_fd, shared_fd, &ctx));
	UT_ASSERT_EQ(ctx.boot_file_count, 2);
	UT_ASSERT(pgrac_initdb_recovery_boot_v2(&ctx, actual));
	UT_ASSERT(memcmp(boot, actual, sizeof(boot)) == 0);
	UT_ASSERT_EQ(child_read(&ctx, 0, false), 0);
	cleanup();
}
UT_TEST(boot_identity_set_rejects_wrong_kind_origin_branch_and_birth)
{
	PgracInitdbRecoveryContext ctx, saved;
	ClusterRecoveryFileIdentityV2 boot[2];
	setup_request();
	memset(&ctx, 0xa5, sizeof(ctx));
	saved = ctx;
	for (unsigned i = 0; i < 8; ++i) {
		boot_inputs(boot);
		if (i == 0)
			boot[0].kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
		if (i == 1)
			boot[1].origin++;
		if (i == 2)
			boot[1].redo_branch_uuid[0]++;
		if (i == 3)
			boot[0].creation_generation = 1;
		if (i == 4)
			boot[1].creation_generation = boot[0].creation_generation;
		if (i == 5)
			boot[0].object_id = 1;
		if (i == 6)
			boot[1].database_incarnation++;
		if (i == 7)
			boot[0].registration_generation++;
		UT_ASSERT(!pgrac_initdb_recovery_prepare_files_v2(&first, boot, &config, source_fd,
														  shared_fd, &ctx));
		UT_ASSERT(memcmp(&ctx, &saved, sizeof(ctx)) == 0);
	}
	cleanup();
}
UT_TEST(incomplete_boot_request_and_output_alias_never_publish)
{
	PgracInitdbRecoveryContext ctx, saved;
	ClusterRecoveryFileIdentityV2 boot[2], out[2], original[2];
	int fd = -1;
	setup_request();
	boot_inputs(boot);
	UT_ASSERT(
		pgrac_initdb_recovery_prepare_files_v2(&first, boot, &config, source_fd, shared_fd, &ctx));
	saved = ctx;
	UT_ASSERT(!pgrac_initdb_recovery_prepare_files_v2(&ctx.first, ctx.boot_files, &config,
													  source_fd, shared_fd, &ctx));
	UT_ASSERT(memcmp(&ctx, &saved, sizeof(ctx)) == 0);
	memset(out, 0xa5, sizeof(out));
	memcpy(original, out, sizeof(out));
	ctx.boot_file_count = 1;
	UT_ASSERT(!pgrac_initdb_recovery_boot_v2(&ctx, out));
	UT_ASSERT(memcmp(out, original, sizeof(out)) == 0);
	UT_ASSERT(!pgrac_initdb_recovery_pipe_v2(&ctx, &fd));
	UT_ASSERT_EQ(fd, -1);
	ctx = saved;
	ctx.boot_files[1].system_identifier++;
	UT_ASSERT(!pgrac_initdb_recovery_pipe_v2(&ctx, &fd));
	cleanup();
}

UT_TEST(original_initdb_relays_full_identity_to_native_child)
{
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryFileIdentityV2 boot[2];
	int incoming, status;
	pid_t frontend;
	setup_request();
	boot_inputs(boot);
	UT_ASSERT(
		pgrac_initdb_recovery_prepare_files_v2(&first, boot, &config, source_fd, shared_fd, &ctx));
	UT_ASSERT(pgrac_initdb_recovery_pipe_v2(&ctx, &incoming));
	frontend = fork();
	UT_ASSERT(frontend >= 0);
	if (frontend == 0) {
		PgracInitdbRecoveryContext actual;
		int outgoing, child_status;
		pid_t native;
		if (!pgrac_initdb_recovery_read_v2(incoming, &config, first.system_identifier,
										   first.database_incarnation, first.storage_uuid,
										   source_fd, shared_fd, &actual)
			|| !pgrac_initdb_recovery_relay_v2(&actual, &outgoing))
			_exit(1);
		native = fork();
		if (native == 0) {
			PgracInitdbRecoveryContext child;
			if (!pgrac_initdb_recovery_read_v2(outgoing, &config, first.system_identifier,
											   first.database_incarnation, first.storage_uuid,
											   source_fd, shared_fd, &child)
				|| child.creator_pid != (uint64)getppid()
				|| memcmp(child.boot_files, boot, sizeof(boot)))
				_exit(2);
			_exit(0);
		}
		if (native < 0 || close(outgoing) || waitpid(native, &child_status, 0) != native
			|| !WIFEXITED(child_status))
			_exit(3);
		_exit(WEXITSTATUS(child_status));
	}
	UT_ASSERT(close(incoming) == 0);
	UT_ASSERT(waitpid(frontend, &status, 0) == frontend);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	cleanup();
}
UT_TEST(relay_rejects_nonrecipient_and_incomplete_sets)
{
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryFileIdentityV2 boot[2];
	int fd = -1;
	setup_request();
	boot_inputs(boot);
	UT_ASSERT(
		pgrac_initdb_recovery_prepare_files_v2(&first, boot, &config, source_fd, shared_fd, &ctx));
	UT_ASSERT(!pgrac_initdb_recovery_relay_v2(&ctx, &fd));
	UT_ASSERT_EQ(fd, -1);
	ctx.creator_pid = getppid();
	ctx.boot_file_count = 0;
	UT_ASSERT(!pgrac_initdb_recovery_relay_v2(&ctx, &fd));
	UT_ASSERT_EQ(fd, -1);
	cleanup();
}

int
main(int argc, char **argv)
{
	pg_logging_init(argv[0]);
	UT_PLAN(17);
	UT_RUN(original_creator_pipe_to_complete_managed_pairs);
	UT_RUN(stale_binding_refuses_before_any_pair_creation);
	UT_RUN(preregistration_rejects_bad_birth_and_alias_without_io);
	UT_RUN(wrong_parent_and_closed_or_malformed_request_refuse);
	UT_RUN(actual_created_pairs_return_complete_independent_observations);
	UT_RUN(wrong_preallocated_birth_and_partial_pair_preserve_whole_output);
	UT_RUN(late_image_digest_and_original_directory_mismatch_refuse);
	UT_RUN(unexpected_database_and_symlink_never_form_complete_inventory);
	UT_RUN(observation_alias_and_wrong_creator_refuse_before_output);
	UT_RUN(original_creator_retains_actual_pairs_through_registry_selection);
	UT_RUN(original_creator_current_refuses_late_pair_change_without_rebase);
	UT_RUN(original_creator_rejects_late_registration_and_preserves_legacy_entry);
	UT_RUN(complete_boot_files_survive_original_closed_pipe);
	UT_RUN(boot_identity_set_rejects_wrong_kind_origin_branch_and_birth);
	UT_RUN(incomplete_boot_request_and_output_alias_never_publish);
	UT_RUN(original_initdb_relays_full_identity_to_native_child);
	UT_RUN(relay_rejects_nonrecipient_and_incomplete_sets);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
