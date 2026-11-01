/* Actual creator publication and object I/O. Only the complete native-source
 * revalidation leaf is controlled; no native initdb/coverage grant is claimed. */
#define TEST_CONTROL_FILE_SELECTION 1
#include "test_cluster_control_registry.c"
#include <dirent.h>
#include <setjmp.h>
#include "common/pgrac_initdb_cohort.h"
#include "common/cluster_recovery_bootstrap.h"
#include "cluster/cluster_undo_segment.h"
#include "common/pgrac_initdb_recovery.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_initdb_config.h"
#include "../../backend/cluster/cluster_initdb_origin_private.h"
#include "../../backend/cluster/cluster_initdb_tree_private.h"
#include "../../backend/cluster/cluster_cf_contract_private.h"
#include "../../bin/initdb/pgrac_wal.h"

static jmp_buf fatal;
static bool expected_fatal;
static void
pg_attribute_noreturn() refuse(const char *reason)
{
	if (expected_fatal)
		longjmp(fatal, 1);
	fprintf(stderr, "unexpected creation refusal: %s\n", reason);
	abort();
}
static void *
publication_alloc(Size n)
{
	void *p = malloc(n);
	if (!p)
		abort();
	return p;
}
#define palloc publication_alloc
#define pfree free
#include "test_cluster_initdb_registry_publish.inc"
#undef palloc
#undef pfree

static InitdbDirectory global_dir, startup_dir, directories[4];
static InitdbStartupObject objects[128];
static InitdbOrigin origins[128];
static ClusterSharedConfigRef original_config;
static PgracInitdbCohortContext request;
static ClusterInitdbTree tree;
static uint8 startup_inputs[128][CLUSTER_WAL_STARTUP_BYTES];
static char base[MAXPGPATH];
static unsigned current_calls, fault_at;
static int fault_kind;

static void
creation_sources_current(const PgracInitdbCohortContext *r, const ClusterSharedConfigRef *c,
						 InitdbDirectory d[4], InitdbOrigin *o, const ClusterInitdbTree *a,
						 const ClusterInitdbTree *b, bool derived)
{
	if (r != &request || c != &original_config || d != directories || o != origins || a != &tree
		|| b != &tree || !derived)
		abort();
	current_calls++;
	if (faccessat(global_dir.fd, "pgrac_control_root", F_OK, 0) == 0)
		abort(); /* Every original-source check precedes primary publication. */
	if (current_calls != fault_at)
		return;
	if (fault_kind == 1)
		refuse("controlled original source refusal");
	if (fault_kind == 2)
		origins[1].control.checkPointCopy.nextOid++;
	if (fault_kind == 3)
		files[1].image_digest[0] ^= 1;
	if (fault_kind == 4) {
		int fd = openat(
			global_dir.fd,
			"pgrac_recovery_registry/0000000000000001-0000000000000001-0000000000000001.v2",
			O_WRONLY);
		if (fd < 0 || pwrite(fd, "x", 1, 300) != 1 || close(fd))
			abort();
	}
	if (fault_kind == 5) {
		int fd = openat(global_dir.fd, "pgrac_control_root.bak", O_WRONLY);
		if (fd < 0 || pwrite(fd, "x", 1, 1000) != 1 || close(fd))
			abort();
	}
	if (fault_kind == 6) {
		int fd = openat(objects[0].directory.fd, objects[0].name, O_WRONLY);
		if (fd < 0 || pwrite(fd, "x", 1, 0) != 1 || close(fd))
			abort();
	}
}
static void
bind_dir(InitdbDirectory *d, int parent, const char *path, const char *name)
{
	memset(d, 0, sizeof(*d));
	d->parent = parent;
	strlcpy(d->path, path, sizeof(d->path));
	strlcpy(d->name, name, sizeof(d->name));
	d->fd = openat(parent, name, O_RDONLY | O_DIRECTORY);
	if (d->fd < 0 || fstat(d->fd, &d->identity) || fstat(parent, &d->parent_identity))
		abort();
}
static void
setup(void)
{
	char path[MAXPGPATH], child[MAXPGPATH];
	int parent;
	root_fixture();
	memset(origins, 0, sizeof(origins));
	root.header.file_txn_seq = 1;
	root.header.migration_prepare_generation = 1;
	reg.coverage.control_generation = reg.coverage.registration_generation = 1;
	reg.coverage.checkpoint_generation = reg.coverage.commit_generation = 1;
	reg.redo_branch_generation = 1;
	for (unsigned i = 0; i < 2; i++) {
		sources[i].source.claim.max_config_generation = 1;
		sources[i].native_floor = sources[i].post_flush_issued = FullTransactionIdFromU64(0);
		sources[i].local_bounds_observed = false;
		sources[i].next_full_xid = FullTransactionIdFromU64(0);
		sources[i].oldest_clog_xid = 0;
		sources[i].redo_start = sources[i].checkpoint.redo;
		sources[i].replay_end = sources[i].checkpoint_end;
		/* Separate source intervals; neither is replaced by a cohort MIN/MAX. */
		if (i) {
			sources[i].checkpoint_start = sources[i].checkpoint.redo = sources[i].redo_start = 200;
			sources[i].checkpoint_end = sources[i].replay_end = 240;
		}
		origins[i].control.system_identifier = reg.system_identifier;
		origins[i].control.state = DB_SHUTDOWNED;
		origins[i].control.checkPointCopy = sources[i].checkpoint;
		origins[i].control.checkPoint = sources[i].checkpoint_start;
		origins[i].checkpoint.checkpoint_start = sources[i].checkpoint_start;
		origins[i].checkpoint.checkpoint_end = sources[i].checkpoint_end;
		origins[i].input.snapshot.identity = sources[i].source.claim.identity;
		memcpy(origins[i].input.refs.claim_sha256, sources[i].source.claim.claim_sha256, 32);
		root.records[i].checkpoint_lower_lsn = sources[i].checkpoint_start;
		files[i].identity.registration_generation = 1;
		files[i].identity.redo_branch_generation = 1;
		for (unsigned j = 0; j < 2; j++) {
			intervals[i][j].redo_start = sources[j].redo_start;
			intervals[i][j].replay_end = sources[j].replay_end;
		}
	}
	/* The first pass above precedes the second source update. */
	intervals[0][1].redo_start = 200;
	intervals[0][1].replay_end = 240;
	memset(&original_config, 0, sizeof(original_config));
	original_config.identity.system_identifier = reg.system_identifier;
	original_config.identity.database_incarnation = reg.database_incarnation;
	original_config.identity.generation = 1;
	memcpy(original_config.identity.storage_uuid, reg.storage_uuid, 16);
	memcpy(original_config.identity.authority_uuid, root.header.authority_uuid, 16);
	original_config.identity.configured[0] = 3;
	memcpy(original_config.sha256, root.header.v2.config_sha256, 32);
	strlcpy(base, "/tmp/pgrac-creation-publish-XXXXXX", sizeof(base));
	if (!mkdtemp(base))
		abort();
	parent = open(base, O_RDONLY | O_DIRECTORY);
	snprintf(path, sizeof(path), "%s/global", base);
	if (mkdir(path, 0700))
		abort();
	bind_dir(&global_dir, parent, path, "global");
	snprintf(path, sizeof(path), "%s/global/wal_startup", base);
	if (mkdir(path, 0700))
		abort();
	bind_dir(&startup_dir, global_dir.fd, path, "wal_startup");
	for (unsigned i = 0; i < 2; i++) {
		snprintf(child, sizeof(child), "thread_%u", i + 1);
		snprintf(path, sizeof(path), "%s/global/wal_startup/%s", base, child);
		if (mkdir(path, 0700))
			abort();
		bind_dir(&objects[i].directory, startup_dir.fd, path, child);
		strlcpy(objects[i].name, "original-startup", sizeof(objects[i].name));
		if (!cluster_initdb_object_write_observed(objects[i].directory.fd, objects[i].name,
												  startup_inputs[i], CLUSTER_WAL_STARTUP_BYTES,
												  &objects[i].identity))
			abort();
	}
	current_calls = fault_at = 0;
	fault_kind = 0;
	test_registry_io_fault(0);
}
static void
remove_tree(int fd)
{
	DIR *dir = fdopendir(dup(fd));
	struct dirent *e;
	if (!dir)
		abort();
	while ((e = readdir(dir))) {
		struct stat s;
		if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
			continue;
		if (fstatat(fd, e->d_name, &s, AT_SYMLINK_NOFOLLOW))
			abort();
		if (S_ISDIR(s.st_mode)) {
			int child = openat(fd, e->d_name, O_RDONLY | O_DIRECTORY);
			if (child < 0)
				abort();
			remove_tree(child);
			if (close(child))
				abort();
		}
		if (unlinkat(fd, e->d_name, S_ISDIR(s.st_mode) ? AT_REMOVEDIR : 0))
			abort();
	}
	if (closedir(dir))
		abort();
}
static void
cleanup(void)
{
	int parent = global_dir.parent;
	for (unsigned i = 0; i < 2; i++)
		if (close(objects[i].directory.fd))
			abort();
	if (close(startup_dir.fd) || close(global_dir.fd))
		abort();
	remove_tree(parent);
	if (close(parent) || rmdir(base))
		abort();
}
static bool
publish(bool managed)
{
	expected_fatal = true;
	if (setjmp(fatal)) {
		expected_fatal = false;
		return false;
	}
	creation_root_publish(&request, &original_config, directories, origins, &tree, &tree,
						  &global_dir, &startup_dir, objects, &root, startup_inputs,
						  managed ? &reg : NULL, 4096);
	expected_fatal = false;
	return true;
}
UT_TEST(registered_root_is_last_and_selected_registry_reads_back)
{
	uint8 pair[133120];
	ControlRootImage metadata;
	setup();
	UT_ASSERT(publish(true));
	UT_ASSERT(current_calls >= 3);
	UT_ASSERT(cluster_control_root_registry_capture_v4(global_dir.fd, reg.storage_uuid,
													   reg.system_identifier, pair, &metadata));
	UT_ASSERT(cluster_control_root_registry_matches_v4(pair, 66560, &reg));
	UT_ASSERT(
		cluster_control_root_registry_read_selected_v4(global_dir.fd, pair, 66560, &reg, 4096));
	cleanup();
}
UT_TEST(complete_source_and_native_checkpoint_are_required_before_io)
{
	setup();
	UT_ASSERT(creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	for (unsigned i = 0; i < 2; i++) {
		sources[i].checkpoint.nextOid++;
		UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
		sources[i].checkpoint.nextOid--;
		sources[i].source.claim.claim_sha256[0] ^= 1;
		UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
		sources[i].source.claim.claim_sha256[0] ^= 1;
	}
	reg.coverage.source_count = 1;
	UT_ASSERT(!publish(true));
	UT_ASSERT(faccessat(global_dir.fd, "pgrac_recovery_registry", F_OK, 0) != 0);
	cleanup();
}
UT_TEST(every_native_checkpoint_field_keeps_its_original_source)
{
	setup();
#define REFUSE_CHECKPOINT_CHANGE(field)                                                            \
	do {                                                                                           \
		sources[1].checkpoint.field++;                                                             \
		UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));         \
		sources[1].checkpoint.field--;                                                             \
	} while (0)
	REFUSE_CHECKPOINT_CHANGE(redo);
	REFUSE_CHECKPOINT_CHANGE(ThisTimeLineID);
	REFUSE_CHECKPOINT_CHANGE(PrevTimeLineID);
	sources[1].checkpoint.fullPageWrites = !sources[1].checkpoint.fullPageWrites;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	sources[1].checkpoint.fullPageWrites = !sources[1].checkpoint.fullPageWrites;
	sources[1].checkpoint.nextXid
		= FullTransactionIdFromU64(U64FromFullTransactionId(sources[1].checkpoint.nextXid) + 1);
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	sources[1].checkpoint.nextXid = origins[1].control.checkPointCopy.nextXid;
	REFUSE_CHECKPOINT_CHANGE(nextOid);
	REFUSE_CHECKPOINT_CHANGE(nextMulti);
	REFUSE_CHECKPOINT_CHANGE(nextMultiOffset);
	REFUSE_CHECKPOINT_CHANGE(oldestXid);
	REFUSE_CHECKPOINT_CHANGE(oldestXidDB);
	REFUSE_CHECKPOINT_CHANGE(oldestMulti);
	REFUSE_CHECKPOINT_CHANGE(oldestMultiDB);
	REFUSE_CHECKPOINT_CHANGE(time);
	REFUSE_CHECKPOINT_CHANGE(oldestCommitTsXid);
	REFUSE_CHECKPOINT_CHANGE(newestCommitTsXid);
	REFUSE_CHECKPOINT_CHANGE(oldestActiveXid);
#undef REFUSE_CHECKPOINT_CHANGE
	UT_ASSERT(creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	cleanup();
}
UT_TEST(missing_configured_source_and_partial_file_intervals_refuse)
{
	setup();
	reg.coverage.source_count = 1;
	files[0].source_count = files[1].source_count = 1;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	reg.coverage.source_count = 2;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	files[0].source_count = files[1].source_count = 2;
	intervals[1][1].replay_end--;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	intervals[1][1].replay_end++;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 100));
	UT_ASSERT(creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	cleanup();
}
UT_TEST(initial_registration_cannot_borrow_later_state_or_bounds)
{
	setup();
	reg.coverage.checkpoint_generation++;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	reg.coverage.checkpoint_generation--;
	sources[0].post_flush_issued = FullTransactionIdFromU64(3);
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	sources[0].post_flush_issued = FullTransactionIdFromU64(0);
	sources[1].replay_end++;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	cleanup();
}
UT_TEST(registry_sync_failure_never_publishes_either_root)
{
	for (unsigned i = 1; i <= 3; i++) {
		setup();
		test_registry_io_fault(i);
		UT_ASSERT(!publish(true));
		UT_ASSERT(faccessat(global_dir.fd, "pgrac_control_root.bak", F_OK, 0) != 0);
		UT_ASSERT(faccessat(global_dir.fd, "pgrac_control_root", F_OK, 0) != 0);
		cleanup();
	}
}
UT_TEST(late_source_or_registry_changes_leave_primary_absent)
{
	for (int kind = 1; kind <= 4; kind++) {
		setup();
		fault_at = 2;
		fault_kind = kind;
		UT_ASSERT(!publish(true));
		UT_ASSERT(faccessat(global_dir.fd, "pgrac_control_root", F_OK, 0) != 0);
		cleanup();
	}
}
UT_TEST(backup_and_startup_changes_are_rechecked_before_primary)
{
	for (int kind = 5; kind <= 6; kind++) {
		setup();
		fault_at = 3;
		fault_kind = kind;
		UT_ASSERT(!publish(true));
		UT_ASSERT(faccessat(global_dir.fd, "pgrac_control_root", F_OK, 0) != 0);
		cleanup();
	}
}
UT_TEST(legacy_publisher_remains_legacy_and_managed_child_is_closed)
{
	struct stat s;
	setup();
	UT_ASSERT(publish(false));
	UT_ASSERT(fstatat(global_dir.fd, "pgrac_control_root", &s, 0) == 0);
	UT_ASSERT_EQ(s.st_size, CLUSTER_CONTROL_ROOT_FILE_BYTES);
	UT_ASSERT(faccessat(global_dir.fd, "pgrac_recovery_registry", F_OK, 0) != 0);
	cleanup();
	setup();
	origins[127].managed_relmap = true;
	UT_ASSERT(!publish(false));
	UT_ASSERT_EQ(current_calls, 0);
	UT_ASSERT(faccessat(global_dir.fd, "pgrac_control_root", F_OK, 0) != 0);
	cleanup();
}
UT_TEST(managed_child_requires_its_complete_captured_relmap_inventory) {
	setup();
	origins[0].managed_relmap = true;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 4096));
	cleanup();
}
UT_TEST(managed_registry_matches_each_captured_image_and_preallocated_identity) {
	ClusterRecoveryCheckpointFileV2 complete[10] = { 0 }, saved;
	ClusterRecoveryFileIdentityV2 ids[4];
	PgracInitdbRecoveryContext *ctx;
	setup();
	origins[0].managed_relmap = true;
	ctx = &origins[0].relmap_registration;
	ctx->magic = PGRAC_INITDB_RECOVERY_MAGIC;
	ctx->version = 2;
	ctx->creator_pid = getpid();
	ctx->config.fd = 3;
	ctx->config.bytes = 1;
	ctx->config.sha256[0] = 1;
	ctx->first = files[0].identity;
	ctx->first.database_oid = ctx->first.object_id = 0;
	ctx->first.tablespace_oid = GLOBALTABLESPACE_OID;
	UT_ASSERT(pgrac_initdb_recovery_relmaps_v2(ctx, ids));
	for (unsigned i = 0; i < 8; ++i) {
		ClusterRecoveryCheckpointFileV2 *o = &origins[0].relmap_observations[i];
		memset(o, 0, sizeof(*o));
		o->identity = ids[i / 2];
		o->copy_index = i % 2;
		o->copy_count = 2;
		o->synced_bytes = 2304;
		o->page_count = 1;
		o->image_digest[0] = i + 1;
		complete[i] = *o;
		complete[i].source_count = 2;
		complete[i].sources = intervals[0];
	}
	reg.coverage.files = complete;
	reg.coverage.file_count = 8;
	/* Captured relmaps alone no longer authorize a complete managed BOOT. */
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 16384));
	ctx->boot_file_count = 2;
	for (unsigned i = 0; i < 2; ++i) {
		ClusterRecoveryBootFileV2 *b = &origins[0].boot_observations[i];
		ctx->boot_files[i] = ctx->first;
		ctx->boot_files[i].kind = i ? CLUSTER_RECOVERY_FILE_UNDO_V2 : CLUSTER_RECOVERY_FILE_CLOG_V2;
		ctx->boot_files[i].tablespace_oid = 0;
		ctx->boot_files[i].creation_generation += 4 + i;
		b->identity = ctx->boot_files[i];
		b->page.page_generation = 1;
		b->synced_bytes = i ? UNDO_SEGMENT_SIZE_BYTES : 16448;
		b->image_digest[0] = i + 1;
		complete[8 + i].identity = b->identity;
		complete[8 + i].copy_count = 1;
		complete[8 + i].synced_bytes = b->synced_bytes;
		complete[8 + i].page_count = i ? UNDO_SEGMENT_SIZE_BYTES / BLCKSZ : 1;
		memcpy(complete[8 + i].image_digest, b->image_digest, 32);
		complete[8 + i].source_count = 2;
		complete[8 + i].sources = intervals[0];
	}
	reg.coverage.file_count = 10;
	UT_ASSERT(creation_registry_valid(&original_config, origins, &root, &reg, 16384));
	for (unsigned i = 0; i < 8; ++i) {
		saved = complete[i];
		complete[i].image_digest[0] ^= 1;
		UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 16384));
		complete[i] = saved;
		++complete[i].identity.creation_generation;
		UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 16384));
		complete[i] = saved;
	}
	reg.coverage.file_count = 6;
	UT_ASSERT(!creation_registry_valid(&original_config, origins, &root, &reg, 16384));
	reg.coverage.file_count = 10;
	UT_ASSERT(creation_registry_valid(&original_config, origins, &root, &reg, 16384));
	cleanup();
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_RUN(registered_root_is_last_and_selected_registry_reads_back);
	UT_RUN(complete_source_and_native_checkpoint_are_required_before_io);
	UT_RUN(every_native_checkpoint_field_keeps_its_original_source);
	UT_RUN(missing_configured_source_and_partial_file_intervals_refuse);
	UT_RUN(initial_registration_cannot_borrow_later_state_or_bounds);
	UT_RUN(registry_sync_failure_never_publishes_either_root);
	UT_RUN(late_source_or_registry_changes_leave_primary_absent);
	UT_RUN(backup_and_startup_changes_are_rechecked_before_primary);
	UT_RUN(legacy_publisher_remains_legacy_and_managed_child_is_closed);
	UT_RUN(managed_child_requires_its_complete_captured_relmap_inventory);
	UT_RUN(managed_registry_matches_each_captured_image_and_preallocated_identity);
	UT_DONE();
	return ut_failed_count != 0;
}
