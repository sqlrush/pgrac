/* Real ROOT/registry and production shared table. CF/storage/config are the
 * existing fixture leaves; no server/VM or file-lifetime qualification. */
#define TEST_CONTROL_RELMAP_CONSUMER 1
#include "test_cluster_control_relmap_selection.c"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "storage/shmem.h"
#include "common/pgrac_initdb_recovery.h"
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
static const ClusterShmemRegion *registered;
static void *native_test_region;
static PgracInitdbRecoveryContext *creation;
bool IsUnderPostmaster;
bool IsPostmasterEnvironment;
ProcessingMode Mode = InitProcessing;
int cluster_node_id;
int MyProcPid;
static uint32 local_wait_event;
uint32 *my_wait_event_info = &local_wait_event;
bool
errstart_cold(int level, const char *domain)
{
	(void)level;
	(void)domain;
	return true;
}
int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
	pg_re_throw();
}
const PgracInitdbRecoveryContext *
cluster_wal_thread_initdb_recovery_context(void)
{
	return creation;
}
void
cluster_shmem_register_region(const ClusterShmemRegion *r)
{
	registered = r;
}
void *
ShmemInitStruct(const char *name, Size n, bool *found)
{
	void *p;
	(void)name;
	if (native_test_region) {
		*found = true;
		return native_test_region;
	}
	p = mmap(NULL, n, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
	*found = false;
	if (p == MAP_FAILED)
		abort();
	native_test_region = p;
	return p;
}
#include "../../backend/cluster/cluster_control_native_files.c"
static void
table_reset(void)
{
	if (native_files)
		munmap(native_files, registered->size_fn());
	native_files = NULL;
	native_test_region = NULL;
	creation = NULL;
	MyProcPid = getpid();
	cluster_control_native_files_shmem_register();
	registered->init_fn();
}
static ClusterControlFileKeyV2
key(unsigned i)
{
	ClusterControlFileKeyV2 k = { 0 };
	k.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	k.object_id = i;
	return k;
}
static void
native_inventory(unsigned n)
{
	inventory_begin(1);
	for (unsigned i = 0; i < n; i++) {
		inventory[2 + i] = files[0];
		inventory[2 + i].identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
		inventory[2 + i].identity.origin = 0;
		inventory[2 + i].identity.database_oid = 0;
		inventory[2 + i].identity.tablespace_oid = 0;
		inventory[2 + i].identity.object_id = i;
		inventory[2 + i].copy_index = 0;
		inventory[2 + i].copy_count = 1;
	}
	reg.coverage.file_count = n + 2;
	inventory_write();
}
UT_TEST(unknown_refuses_unchanged_outputs)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, before;
	ClusterControlFileKeyV2 k = key(0);
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_UNKNOWN_V2);
	before = b;
	UT_ASSERT(!cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT(!memcmp(&b, &before, sizeof(b)));
	UT_ASSERT(!cluster_control_native_file_current_v2(&b));
	UT_ASSERT(!cluster_control_native_file_release_v2(&b));
}
UT_TEST(original_control_full_inventory_borrow_and_gap)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, copy;
	ClusterControlNativeTableInfoV2 info;
	ClusterControlFileKeyV2 k = key(0);
	table_reset();
	native_inventory(3);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.count, 4); /* actual pair deduplicated, all CLOG entries */
	UT_ASSERT_EQ(info.startup_generation, 9);
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT(identity_same(&b.expected, &inventory[2].identity));
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	copy = b;
	UT_ASSERT(!cluster_control_native_file_current_v2(&copy));
	UT_ASSERT(!cluster_control_native_file_release_v2(&copy));
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 0, 2), 0);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 0, 3),
				 CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 2, 1),
				 CLUSTER_NATIVE_FILES_REFUSED_V2);
	cf_held = false; /* hot reads and cleanup never reenter CF */
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT(!cluster_control_native_file_release_v2(&b));
	cf_held = true;
	selection_end();
}
UT_TEST(preregistration_requires_complete_persistent_set_and_no_half_publish)
{
	ClusterControlNativeTableInfoV2 before, after;
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(
		cluster_control_native_files_preregister_locked_v2(&expected_config, 65536, 9, 0, 0, 2),
		CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2);
	UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_UNKNOWN_V2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	cf_held = false;
	UT_ASSERT(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9) != 0);
	cf_held = true;
	config_ok = false;
	UT_ASSERT(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9) != 0);
	config_ok = true;
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	selection_end();
}
UT_TEST(addition_waits_for_borrow_and_reidentity_always_refuses)
{
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 info;
	ClusterControlFileKeyV2 k = key(0);
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
	inventory[4] = inventory[3];
	inventory[4].identity.object_id = 2;
	reg.coverage.file_count = 5;
	inventory_write();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9),
				 CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT_EQ(
		cluster_control_native_files_preregister_locked_v2(&expected_config, 65536, 9, 0, 0, 2), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.table_generation, 2);
	inventory[2].identity.creation_generation++;
	inventory_write();
	UT_ASSERT(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9) != 0);
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT_EQ(b.expected.creation_generation + 1, inventory[2].identity.creation_generation);
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	selection_end();
}
UT_TEST(shared_borrow_blocks_parent_publication_and_aliases_refuse)
{
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlFileKeyV2 k = key(0);
	int ready[2], finish[2], status;
	char token = 1;
	pid_t child;
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(!cluster_control_native_file_acquire_v2(&k, (void *)native_files));
	UT_ASSERT(!cluster_control_native_files_info_v2((void *)native_files));
	UT_ASSERT(!pipe(ready) && !pipe(finish));
	child = fork();
	UT_ASSERT(child >= 0);
	if (!child) {
		MyProcPid = getpid();
		if (!cluster_control_native_file_acquire_v2(&k, &b) || write(ready[1], &token, 1) != 1
			|| read(finish[0], &token, 1) != 1 || !cluster_control_native_file_current_v2(&b)
			|| !cluster_control_native_file_release_v2(&b))
			_exit(8);
		_exit(0);
	}
	UT_ASSERT(read(ready[0], &token, 1) == 1);
	inventory[4] = inventory[3];
	inventory[4].identity.object_id = 2;
	reg.coverage.file_count = 5;
	inventory_write();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9),
				 CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT(write(finish[1], &token, 1) == 1);
	UT_ASSERT(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	close(ready[0]);
	close(ready[1]);
	close(finish[0]);
	close(finish[1]);
	selection_end();
}
static PgracInitdbRecoveryContext creator_batch;
UT_TEST(original_creation_batch_selects_six_exact_keys_without_root)
{
	ClusterControlNativeTableInfoV2 info;
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlFileKeyV2 k = key(0);
	ClusterRecoveryFileIdentityV2 relmaps[4];
	table_reset();
	native_inventory(1);
	memset(&creator_batch, 0, sizeof(creator_batch));
	creator_batch.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	creator_batch.version = 2;
	creator_batch.creator_pid = getpid();
	creator_batch.config.fd = 3;
	creator_batch.config.bytes = 1;
	creator_batch.config.sha256[0] = 1;
	creator_batch.first = inventory[0].identity;
	creator_batch.first.object_id = 0;
	creator_batch.boot_file_count = 2;
	for (unsigned i = 0; i < 2; i++) {
		creator_batch.boot_files[i] = creator_batch.first;
		creator_batch.boot_files[i].kind
			= i ? CLUSTER_RECOVERY_FILE_UNDO_V2 : CLUSTER_RECOVERY_FILE_CLOG_V2;
		creator_batch.boot_files[i].database_oid = creator_batch.boot_files[i].tablespace_oid = 0;
		creator_batch.boot_files[i].creation_generation += 4 + i;
	}
	creation = &creator_batch;
	cf_held = false;
	registered->init_fn();
	UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_CREATION_V2);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.count, 6);
	UT_ASSERT(!memcmp(&info.creation, &creator_batch, sizeof(creator_batch)));
	UT_ASSERT(pgrac_initdb_recovery_relmaps_v2(&creator_batch, relmaps));
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT(identity_same(&b.expected, &creator_batch.boot_files[0]));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	for (unsigned i = 0; i < 4; i++) {
		k.kind = relmaps[i].kind;
		k.database_oid = relmaps[i].database_oid;
		k.tablespace_oid = relmaps[i].tablespace_oid;
		k.object_id = relmaps[i].object_id;
		k.origin = relmaps[i].origin;
		UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
		UT_ASSERT(identity_same(&b.expected, &relmaps[i]));
		UT_ASSERT(cluster_control_native_file_release_v2(&b));
	}
	k = key(1);
	UT_ASSERT(!cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 0, 1),
				 CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2);
	creation = NULL;
	cf_held = true;
	selection_end();
}
static void
fail_publishing(void)
{
	if (native_files->publishing)
		pg_re_throw();
	config_hook = fail_publishing;
}
UT_TEST(late_error_releases_only_local_publish_latch_and_keeps_old_table)
{
	ClusterControlNativeTableInfoV2 before, after;
	volatile bool caught = false;
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	config_hook = fail_publishing;
	PG_TRY();
	{
		(void)cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_hook = NULL;
	UT_ASSERT(caught && !native_files->publishing && cf_held);
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	selection_end();
}
UT_TEST(capacity_wrap_and_full_gap_never_truncate)
{
	ClusterControlNativeTableInfoV2 info;
	table_reset();
	native_inventory(1);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(
					 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2 + 1, 9),
				 CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_UNKNOWN_V2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 0, UINT64_MAX),
				 CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	native_files->banks[native_files->active].info.table_generation = UINT64_MAX;
	UT_ASSERT(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9) != 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.table_generation, UINT64_MAX);
	selection_end();
}
static uint64
nanos(void)
{
	struct timespec t;
	if (clock_gettime(CLOCK_MONOTONIC, &t))
		abort();
	return (uint64)t.tv_sec * 1000000000 + t.tv_nsec;
}
UT_TEST(actual_copy_cost)
{
	ClusterControlFileKeyV2 k = key(0);
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 info;
	uint64 begin, elapsed;
	bool (*volatile acquire_fn)(const ClusterControlFileKeyV2 *, ClusterControlNativeFileBorrowV2 *)
		= cluster_control_native_file_acquire_v2;
	bool (*volatile current_fn)(const ClusterControlNativeFileBorrowV2 *)
		= cluster_control_native_file_current_v2;
	bool (*volatile release_fn)(ClusterControlNativeFileBorrowV2 *)
		= cluster_control_native_file_release_v2;
	table_reset();
	native_inventory(64);
	begin = nanos();
	UT_ASSERT_EQ(
		cluster_control_native_files_preregister_locked_v2(&expected_config, 65536, 9, 0, 0, 63),
		0);
	elapsed = nanos() - begin;
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	printf("# actual selection+publish ns=%llu rows=%llu shared_bytes=%zu capacity=%llu\n",
		   (unsigned long long)elapsed, (unsigned long long)info.count, info.shared_bytes,
		   (unsigned long long)info.capacity);
	begin = nanos();
	for (unsigned i = 0; i < 100000; i++) {
		if (!acquire_fn(&k, &b) || !current_fn(&b) || !release_fn(&b))
			abort();
	}
	printf("# acquire+current+release 100000 ns=%llu borrow_bytes=%zu\n",
		   (unsigned long long)(nanos() - begin), sizeof(b));
	begin = nanos();
	UT_ASSERT_EQ(
		cluster_control_native_files_preregister_locked_v2(&expected_config, 65536, 9, 0, 0, 64),
		CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2);
	printf(
		"# missing persistent identity refusal ns=%llu identity_bytes=%zu table_info_bytes=%zu\n",
		(unsigned long long)(nanos() - begin), sizeof(b.expected), sizeof(info));
	selection_end();
}
UT_TEST(address_unique_available_pair_uses_registered_origin_without_cf)
{
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 info;
	table_reset();
	inventory_begin(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	cf_held = false;
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_RELMAP_V2, 1001, DEFAULTTABLESPACE_OID, 0, &b),
		CLUSTER_NATIVE_FILES_OK_V2);
	UT_ASSERT_EQ(b.expected.origin, 1);
	UT_ASSERT_EQ(b.expected.database_oid, 1001);
	UT_ASSERT_EQ(b.expected.tablespace_oid, DEFAULTTABLESPACE_OID);
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.count, 2); /* two physical pairs, two logical identities */
	UT_ASSERT_EQ(info.borrowed, 1);
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	cf_held = true;
	selection_end();
}
UT_TEST(address_multiple_origins_refuse_instead_of_selecting_first)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	ClusterControlNativeTableInfoV2 before, after;
	ClusterControlFileKeyV2 k = key(0);
	table_reset();
	native_inventory(2);
	inventory[3].identity.object_id = 0;
	inventory[3].identity.origin = 127;
	inventory_write();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	cf_held = false;
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, &b), CLUSTER_NATIVE_FILES_AMBIGUOUS_V2);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	/* The original explicit-origin API is unchanged. */
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT_EQ(b.expected.origin, 0);
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	cf_held = true;
	selection_end();
}
UT_TEST(address_missing_and_invalid_outputs_keep_original_duty)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, copy, before;
	ClusterControlNativeTableInfoV2 info;
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, &b), CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
	native_inventory(1);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 1, 0, 0, &b), CLUSTER_NATIVE_FILES_NOT_FOUND_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 1, 0, &b), CLUSTER_NATIVE_FILES_NOT_FOUND_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_UNDO_V2, 0, 0, 0, &b), CLUSTER_NATIVE_FILES_NOT_FOUND_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 1, &b), CLUSTER_NATIVE_FILES_NOT_FOUND_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_UNKNOWN_V2, 0, 0, 0, &b), CLUSTER_NATIVE_FILES_REFUSED_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, NULL), CLUSTER_NATIVE_FILES_REFUSED_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, (void *)native_files), CLUSTER_NATIVE_FILES_REFUSED_V2);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, &b), 0);
	before = copy = b;
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, &b), CLUSTER_NATIVE_FILES_REFUSED_V2);
	UT_ASSERT(!memcmp(&b, &before, sizeof(b)));
	UT_ASSERT(!cluster_control_native_file_current_v2(&copy));
	UT_ASSERT(!cluster_control_native_file_release_v2(&copy));
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.borrowed, 1);
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT(!cluster_control_native_file_release_v2(&b));
	selection_end();
}
UT_TEST(address_full_inventory_borrow_blocks_changed_publication)
{
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 before, after;
	uint64 begin;
	ClusterControlNativeResultV2 (*volatile borrow_fn)(ClusterRecoveryFileKindV2, Oid, Oid,
		uint64, ClusterControlNativeFileBorrowV2 *) = cluster_control_native_file_borrow_by_address_v2;
	table_reset();
	native_inventory(65);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(borrow_fn(CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 64, &b), 0);
	UT_ASSERT_EQ(b.expected.object_id, 64);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	inventory[67] = inventory[66];
	inventory[67].identity.object_id = 65;
	reg.coverage.file_count = 68;
	inventory_write();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9),
		CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	cf_held = false;
	begin = nanos();
	for (unsigned i = 0; i < 10000; i++) {
		if (borrow_fn(CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 64, &b) != 0
			|| !cluster_control_native_file_current_v2(&b) || !cluster_control_native_file_release_v2(&b))
			abort();
	}
	printf("# address borrow+current+release 10000 ns=%llu rows=67\n",
		(unsigned long long)(nanos() - begin));
	cf_held = true;
	selection_end();
}
UT_TEST(address_unpublished_creation_is_not_ordinary_available)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	ClusterControlFileKeyV2 k = key(0);
	table_reset();
	native_inventory(1);
	memset(&creator_batch, 0, sizeof(creator_batch));
	creator_batch.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	creator_batch.version = 2;
	creator_batch.creator_pid = getpid();
	creator_batch.config.fd = 3;
	creator_batch.config.bytes = 1;
	creator_batch.config.sha256[0] = 1;
	creator_batch.first = inventory[0].identity;
	creator_batch.first.object_id = 0;
	creator_batch.boot_file_count = 2;
	for (unsigned i = 0; i < 2; i++) {
		creator_batch.boot_files[i] = creator_batch.first;
		creator_batch.boot_files[i].kind = i ? CLUSTER_RECOVERY_FILE_UNDO_V2 : CLUSTER_RECOVERY_FILE_CLOG_V2;
		creator_batch.boot_files[i].database_oid = creator_batch.boot_files[i].tablespace_oid = 0;
		creator_batch.boot_files[i].creation_generation += 4 + i;
	}
	creation = &creator_batch;
	cf_held = false;
	registered->init_fn();
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
		CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 0, &b), CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	creation = NULL;
	cf_held = true;
	selection_end();
}
UT_TEST(cohort_creation_table_excludes_every_local_undo_seed)
{
	for (unsigned origin = 0; origin < 2; origin++)
	{
		ClusterControlNativeTableInfoV2 info;
		ClusterControlNativeFileBorrowV2 b = {0};
		ClusterControlFileKeyV2 k = key(0);
		table_reset();
		native_inventory(1);
		memset(&creator_batch, 0, sizeof(creator_batch));
		creator_batch.magic = PGRAC_INITDB_RECOVERY_MAGIC;
		creator_batch.version = 2;
		creator_batch.creator_pid = getpid();
		creator_batch.config.fd = 3;
		creator_batch.config.bytes = 1;
		creator_batch.config.sha256[0] = 1;
		creator_batch.first = inventory[0].identity;
		creator_batch.first.object_id = 0;
		creator_batch.first.creation_generation = 1;
		creator_batch.cohort[0] = 3;
		creator_batch.cohort_origin = origin;
		creator_batch.boot_file_count = 1;
		creator_batch.boot_files[0] = creator_batch.first;
		creator_batch.boot_files[0].kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
		creator_batch.boot_files[0].database_oid = creator_batch.boot_files[0].tablespace_oid = 0;
		creator_batch.boot_files[0].origin = origin;
		creator_batch.boot_files[0].creation_generation = 5 + origin;
		creation = &creator_batch;
		cf_held = false;
		registered->init_fn();
		UT_ASSERT(cluster_control_native_files_info_v2(&info));
		UT_ASSERT_EQ(info.count, origin ? 1 : 5);
		k.origin = origin;
		UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
		UT_ASSERT(cluster_control_native_file_release_v2(&b));
		k.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
		UT_ASSERT(!cluster_control_native_file_acquire_v2(&k, &b));
		creation = NULL;
		cf_held = true;
		selection_end();
	}
}
UT_TEST(inventory_snapshot_refuses_unknown_alias_and_nonfresh_output)
{
	ClusterControlNativeInventoryV2 *v = NULL;
	ClusterControlNativeInventoryV2 *sentinel = (void *)1;
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), CLUSTER_NATIVE_FILES_REFUSED_V2);
	UT_ASSERT(v == NULL);
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &sentinel), CLUSTER_NATIVE_FILES_REFUSED_V2);
	UT_ASSERT(sentinel == (void *)1);
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, (void *)native_files), CLUSTER_NATIVE_FILES_REFUSED_V2);
}
UT_TEST(inventory_snapshot_copies_every_available_row_above64_without_cf)
{
	ClusterControlNativeInventoryV2 *v = NULL;
	uint64 started;
	table_reset();
	native_inventory(80);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	cf_held = false;
	started = nanos();
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), 0);
	UT_ASSERT(v != NULL);
	if (v) {
		printf("# inventory snapshot rows=%llu bytes=%zu ns=%llu\n",
			(unsigned long long)v->info.count, v->private_bytes,
			(unsigned long long)(nanos() - started));
		UT_ASSERT_EQ(v->info.count, 81);
		UT_ASSERT_EQ(v->info.reservation_count, 0);
		for (unsigned i = 0; i < 82; i++) {
			bool found = false;
			for (uint64 j = 0; j < v->info.count; j++)
				if (identity_same(&v->files[j], &inventory[i].identity))
					found = true;
			UT_ASSERT(found);
		}
		UT_ASSERT(cluster_control_native_files_inventory_current_v2(v));
		UT_ASSERT(!cf_held);
	}
	cluster_control_native_inventory_destroy_v2(&v);
	UT_ASSERT(v == NULL);
	cf_held = true;
	selection_end();
}
UT_TEST(inventory_snapshot_preserves_complete_reserved_values_without_admission)
{
	ClusterControlNativeInventoryV2 *v = NULL;
	ClusterControlNativeFileBorrowV2 borrow = {0};
	NativeFileBank *bank;
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	bank = &native_files->banks[native_files->active];
	for (unsigned i = 0; i < 2; i++) {
		bank->reservations[i].expected = inventory[2].identity;
		bank->reservations[i].expected.object_id = 100 + i;
		bank->reservations[i].batch_uuid[0] = 7;
		bank->reservations[i].owner_generation = 9;
		bank->reservations[i].state = CLUSTER_RECOVERY_FILE_RESERVED_V1;
	}
	bank->info.reservation_count = 2;
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), 0);
	if (v) {
		UT_ASSERT_EQ(v->info.reservation_count, 2);
		UT_ASSERT(!memcmp(v->reservations, bank->reservations, 2 * sizeof(*v->reservations)));
		UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_CLOG_V2, 0, 0, 100, &borrow),
			CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
		UT_ASSERT(cluster_control_native_files_inventory_current_v2(v));
	}
	cluster_control_native_inventory_destroy_v2(&v);
	UT_ASSERT_EQ(bank->info.reservation_count, 2);
	selection_end();
}
UT_TEST(inventory_snapshot_budget_and_invalid_counts_never_return_a_prefix)
{
	ClusterControlNativeInventoryV2 *v = NULL;
	Size exact;
	table_reset();
	native_inventory(80);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), 0);
	exact = v ? v->private_bytes : 0;
	cluster_control_native_inventory_destroy_v2(&v);
	UT_ASSERT(exact > sizeof(*v));
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(exact - 1, &v), CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	UT_ASSERT(v == NULL);
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(exact, &v), 0);
	cluster_control_native_inventory_destroy_v2(&v);
	native_files->banks[native_files->active].info.count = CLUSTER_NATIVE_FILES_CAPACITY_V2 + 1;
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	UT_ASSERT(v == NULL);
	selection_end();
}
UT_TEST(inventory_snapshot_current_refuses_saved_value_and_address_drift)
{
	ClusterControlNativeInventoryV2 *v = NULL, copied, *copy;
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), 0);
	if (v) {
		copied = *v;
		copy = &copied;
		UT_ASSERT(!cluster_control_native_files_inventory_current_v2(copy));
		cluster_control_native_inventory_destroy_v2(&copy);
		UT_ASSERT(copy == &copied);
		((ClusterRecoveryFileIdentityV2 *)v->files)[0].creation_generation++;
		UT_ASSERT(!cluster_control_native_files_inventory_current_v2(v));
		((ClusterRecoveryFileIdentityV2 *)v->files)[0].creation_generation--;
		UT_ASSERT(cluster_control_native_files_inventory_current_v2(v));
		MyProcPid++;
		UT_ASSERT(!cluster_control_native_files_inventory_current_v2(v));
		MyProcPid--;
	}
	cluster_control_native_inventory_destroy_v2(&v);
	selection_end();
}
UT_TEST(inventory_snapshot_publication_revokes_current_but_destroy_keeps_borrow)
{
	ClusterControlNativeInventoryV2 *v = NULL;
	ClusterControlNativeFileBorrowV2 borrow = {0};
	ClusterControlFileKeyV2 k = key(0);
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), 0);
	native_files->publishing = true;
	UT_ASSERT(!cluster_control_native_files_inventory_current_v2(v));
	native_files->publishing = false;
	native_inventory(3);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(!cluster_control_native_files_inventory_current_v2(v));
	UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &borrow));
	cluster_control_native_inventory_destroy_v2(&v);
	UT_ASSERT(cluster_control_native_file_current_v2(&borrow));
	UT_ASSERT(cluster_control_native_file_release_v2(&borrow));
	selection_end();
}
UT_TEST(inventory_snapshot_inflight_publication_and_forged_extent_refuse)
{
	ClusterControlNativeInventoryV2 *v = NULL;
	table_reset();
	native_inventory(2);
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	native_files->publishing = true;
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT(v == NULL);
	native_files->publishing = false;
	UT_ASSERT_EQ(cluster_control_native_files_inventory_v2(65536, &v), 0);
	if (v) {
		v->private_bytes--;
		UT_ASSERT(!cluster_control_native_files_inventory_current_v2(v));
		v->private_bytes++;
		v->files++;
		UT_ASSERT(!cluster_control_native_files_inventory_current_v2(v));
		v->files--;
		UT_ASSERT(cluster_control_native_files_inventory_current_v2(v));
	}
	cluster_control_native_inventory_destroy_v2(&v);
	selection_end();
}

static void
creation_child_fixture(unsigned origin)
{
	table_reset();
	native_inventory(1);
	memset(&creator_batch, 0, sizeof(creator_batch));
	creator_batch.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	creator_batch.version = 2;
	creator_batch.creator_pid = getpid();
	creator_batch.config.fd = 3;
	creator_batch.config.bytes = 1;
	creator_batch.config.sha256[0] = 1;
	creator_batch.first = inventory[0].identity;
	creator_batch.first.object_id = 0;
	creator_batch.first.creation_generation = 1;
	creator_batch.cohort[0] = 15;
	creator_batch.cohort_origin = origin;
	creator_batch.boot_file_count = 1;
	creator_batch.boot_files[0] = creator_batch.first;
	creator_batch.boot_files[0].kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	creator_batch.boot_files[0].database_oid = creator_batch.boot_files[0].tablespace_oid = 0;
	creator_batch.boot_files[0].origin = origin;
	creator_batch.boot_files[0].creation_generation = 5 + origin;
	creation = &creator_batch;
	IsUnderPostmaster = IsPostmasterEnvironment = false;
	MyBackendType = B_STANDALONE_BACKEND;
	Mode = BootstrapProcessing;
	cluster_node_id = -1;
	cf_held = false;
}
static void
creation_child_cleanup(void)
{
	creation = NULL;
	IsUnderPostmaster = IsPostmasterEnvironment = false;
	MyBackendType = B_INVALID;
	Mode = InitProcessing;
	cluster_node_id = 0;
	cf_held = true;
	selection_end();
}
UT_TEST(creation_shmem_missing_request_keeps_original_table_unknown)
{
	ClusterControlNativeTableInfoV2 info;
	table_reset();
	UT_ASSERT_EQ(cluster_control_native_files_creation_shmem_size_v2(), 0);
	cluster_control_native_files_creation_shmem_init_v2();
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.mode, CLUSTER_NATIVE_FILES_UNKNOWN_V2);
	UT_ASSERT_EQ(info.table_generation, 0);
}
UT_TEST(creation_shmem_original_cohort_child_installs_only_its_exact_batch)
{
	for (unsigned origin = 0; origin < 4; origin++) {
		ClusterControlNativeTableInfoV2 info;
		ClusterControlNativeFileBorrowV2 b = {0};
		ClusterControlFileKeyV2 k = key(0);
		Size reserved;
		creation_child_fixture(origin);
		/* Reproduce the original child before any table-region initializer. */
		munmap(native_files, registered->size_fn());
		native_files = native_test_region = NULL;
		registered = NULL;
		if (origin & 1)
			Mode = InitProcessing;
		reserved = cluster_control_native_files_creation_shmem_size_v2();
		UT_ASSERT(reserved > 100000); /* Cannot use PG's original 100K slop. */
		UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_UNKNOWN_V2);
		cluster_control_native_files_creation_shmem_init_v2();
		UT_ASSERT(registered == NULL); /* No full-cluster registry activation. */
		UT_ASSERT(cluster_control_native_files_info_v2(&info));
		UT_ASSERT_EQ(info.shared_bytes, reserved);
		UT_ASSERT_EQ(info.mode, CLUSTER_NATIVE_FILES_CREATION_V2);
		UT_ASSERT_EQ(info.count, origin ? 1 : 5);
		UT_ASSERT_EQ(info.startup_generation, creator_batch.creator_pid);
		UT_ASSERT(!memcmp(&info.creation, &creator_batch, sizeof(creator_batch)));
		UT_ASSERT_EQ(cluster_node_id, -1);
		k.origin = origin;
		UT_ASSERT(cluster_control_native_file_acquire_v2(&k, &b));
		UT_ASSERT(identity_same(&b.expected, &creator_batch.boot_files[0]));
		UT_ASSERT(cluster_control_native_file_release_v2(&b));
		k.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
		UT_ASSERT(!cluster_control_native_file_acquire_v2(&k, &b));
		printf("# original creation table bytes=%zu origin=%u\n", reserved, origin);
		/* Let the original fixture cleanup own the existing native region. */
		cluster_control_native_files_shmem_register();
		creation_child_cleanup();
	}
}
UT_TEST(creation_shmem_rejects_online_or_changed_original_child_context)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		creation_child_fixture(0);
		if (fault == 0) IsUnderPostmaster = true;
		if (fault == 1) IsPostmasterEnvironment = true;
		if (fault == 2) MyBackendType = B_BACKEND;
		if (fault == 3) Mode = NormalProcessing;
		if (fault == 4) cluster_node_id = 0;
		for (unsigned action = 0; action < 2; action++) {
			volatile bool caught = false;
			PG_TRY();
			{
				if (action == 0)
					(void)cluster_control_native_files_creation_shmem_size_v2();
				else
					cluster_control_native_files_creation_shmem_init_v2();
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT(caught);
			UT_ASSERT_EQ(cluster_control_native_files_mode_v2(), CLUSTER_NATIVE_FILES_UNKNOWN_V2);
		}
		creation_child_cleanup();
	}
}
int
main(void)
{
	UT_PLAN(41);
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_RUN(test_control_selects_complete_original_file_identity);
	UT_RUN(test_control_selection_refuses_missing_custody_and_dependency);
	UT_RUN(test_control_selection_late_root_and_error_preserve_output);
	UT_RUN(test_control_selection_valid_outer_hash_cannot_hide_noncanonical_source);
	UT_RUN(test_control_selection_key_and_named_directory_remain_original);
	UT_RUN(test_control_selection_ambiguous_births_refuse_without_latest_rule);
	UT_RUN(test_complete_decoded_vector_exceeds_active_pin_capacity);
	UT_RUN(test_complete_inventory_origin_lookup_and_sources);
	UT_RUN(test_inventory_requires_original_custody_and_fresh_output);
	UT_RUN(test_inventory_rejects_ambiguous_birth_origin_and_incomplete_copy);
	UT_RUN(test_inventory_requires_complete_configured_source_set_and_shared_address);
	UT_RUN(test_complete_receipt_membership_refuses_partial_extra_and_wrong_identity);
	UT_RUN(test_post_io_root_registry_error_and_cf_reacquire_cannot_revive);
	UT_RUN(test_post_io_named_directory_remains_original);
	UT_RUN(test_actual_pair_sync_readback_then_complete_control_recheck);
	UT_RUN(test_copied_checkpoint_reference_does_not_revive_cf_authority);

	UT_RUN(unknown_refuses_unchanged_outputs);
	UT_RUN(original_control_full_inventory_borrow_and_gap);
	UT_RUN(preregistration_requires_complete_persistent_set_and_no_half_publish);
	UT_RUN(addition_waits_for_borrow_and_reidentity_always_refuses);
	UT_RUN(shared_borrow_blocks_parent_publication_and_aliases_refuse);
	UT_RUN(cohort_creation_table_excludes_every_local_undo_seed);
	UT_RUN(original_creation_batch_selects_six_exact_keys_without_root);
	UT_RUN(late_error_releases_only_local_publish_latch_and_keeps_old_table);
	UT_RUN(capacity_wrap_and_full_gap_never_truncate);
	UT_RUN(actual_copy_cost);
	UT_RUN(address_unique_available_pair_uses_registered_origin_without_cf);
	UT_RUN(address_multiple_origins_refuse_instead_of_selecting_first);
	UT_RUN(address_missing_and_invalid_outputs_keep_original_duty);
	UT_RUN(address_full_inventory_borrow_blocks_changed_publication);
	UT_RUN(address_unpublished_creation_is_not_ordinary_available);
	UT_RUN(inventory_snapshot_refuses_unknown_alias_and_nonfresh_output);
	UT_RUN(inventory_snapshot_copies_every_available_row_above64_without_cf);
	UT_RUN(inventory_snapshot_preserves_complete_reserved_values_without_admission);
	UT_RUN(inventory_snapshot_budget_and_invalid_counts_never_return_a_prefix);
	UT_RUN(inventory_snapshot_current_refuses_saved_value_and_address_drift);
	UT_RUN(inventory_snapshot_publication_revokes_current_but_destroy_keeps_borrow);
	UT_RUN(inventory_snapshot_inflight_publication_and_forged_extent_refuse);
	UT_RUN(creation_shmem_missing_request_keeps_original_table_unknown);
	UT_RUN(creation_shmem_original_cohort_child_installs_only_its_exact_batch);
	UT_RUN(creation_shmem_rejects_online_or_changed_original_child_context);
	UT_DONE();
	return ut_failed_count != 0;
}
