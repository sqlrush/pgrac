/* Original ROOT batch, actual shared table; no live native caller activation. */
#define TEST_CONTROL_NATIVE_RESERVATIONS 1
int original_registry_batch_main(int argc, char **argv);
#define LWLockHeldByMe reservation_fixture_held
#include "test_cluster_control_registry_batches.c"
#undef LWLockHeldByMe
static LWLock *held_family_lock;
bool LWLockHeldByMe(LWLock *lock);
bool
LWLockHeldByMe(LWLock *lock)
{
	return lock == held_family_lock || reservation_fixture_held(lock);
}
#undef main
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "storage/shmem.h"
#include <sys/mman.h>
#include <sys/wait.h>
static const ClusterShmemRegion *registered;
static void *native_test_region;
static PgracInitdbRecoveryContext *creation;
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

UT_TEST(persistent_reserved_range_projects_before_native_locks)
{
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(
		cluster_control_native_files_preregister_locked_v2(&expected_config, 65536, 9, 0, 7, 8), 0);
	UT_ASSERT_EQ(cluster_control_native_files_clog_range_v2(0, 7, 8), 0);
	batch_end();
}
UT_TEST(ordinary_reserved_address_is_explicitly_unavailable)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_CLOG_V2, 0,
																  0, 7, &b),
				 CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	batch_end();
}

static ClusterControlNativeResultV2
reserved_acquire(ClusterControlNativeFileBorrowV2 *out)
{
	return cluster_control_native_file_reserved_acquire_locked_v2(
		&expected_config, 65536, 9, reserved[0].batch_uuid, &reserved[0].expected, out);
}
UT_TEST(exact_creator_borrow_retains_original_protection_without_cf)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, copy;
	ClusterControlNativeTableInfoV2 info;
	unsigned before;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(reserved_acquire(&b), 0);
	UT_ASSERT(identity_same(&b.expected, &reserved[0].expected));
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.reservation_count, 2);
	UT_ASSERT_EQ(info.count, 2);
	UT_ASSERT_EQ(info.borrowed, 1);
	copy = b;
	cf_held = false;
	before = dependencies;
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(!cluster_control_native_file_current_v2(&copy));
	UT_ASSERT(!cluster_control_native_file_release_v2(&copy));
	MyProcPid++;
	UT_ASSERT(!cluster_control_native_file_release_v2(&b));
	MyProcPid--;
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT(!cluster_control_native_file_release_v2(&b));
	UT_ASSERT_EQ(dependencies, before);
	cf_held = true;
	batch_end();
}
UT_TEST(creator_acquire_rejects_wrong_batch_owner_and_full_expected)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	ClusterRecoveryFileIdentityV2 id;
	ClusterControlNativeTableInfoV2 info;
	uint8 batch[16];
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	memcpy(batch, reserved[0].batch_uuid, 16);
	batch[0]++;
	UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
				  &expected_config, 65536, 9, batch, &reserved[0].expected, &b)
			  != 0);
	id = reserved[0].expected;
	id.creation_generation++;
	UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
				  &expected_config, 65536, 9, reserved[0].batch_uuid, &id, &b)
			  != 0);
	owner_ok = false;
	UT_ASSERT(reserved_acquire(&b) != 0);
	owner_ok = true;
	claim_ok = false;
	UT_ASSERT(reserved_acquire(&b) != 0);
	claim_ok = true;
	anchor_ok = false;
	UT_ASSERT(reserved_acquire(&b) != 0);
	anchor_ok = true;
	UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
				  &expected_config, 65536, 10, reserved[0].batch_uuid, &reserved[0].expected, &b)
			  != 0);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.borrowed, 0);
	batch_end();
}
UT_TEST(creator_acquire_precedes_family_locks_and_protects_aliases)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	unsigned reads;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	reads = dependencies;
	batch_exclusive = false;
	UT_ASSERT(reserved_acquire(&b) != 0);
	batch_exclusive = true;
	cf_held = false;
	UT_ASSERT(reserved_acquire(&b) != 0);
	cf_held = true;
	for (unsigned i = 0; i < 4; i++) {
		LWLock *locks[] = { ControlFileLock, RelationMappingLock, XidGenLock, XactSLRULock };
		held_family_lock = locks[i];
		UT_ASSERT(reserved_acquire(&b) != 0);
		held_family_lock = NULL;
	}
	CritSectionCount = 1;
	UT_ASSERT(reserved_acquire(&b) != 0);
	CritSectionCount = 0;
	UT_ASSERT(reserved_acquire((void *)native_files) != 0);
	UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
				  &expected_config, 65536, 9, (void *)&b, &reserved[0].expected, &b)
			  != 0);
	UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
				  &expected_config, 65536, 9, reserved[0].batch_uuid, (void *)&b, &b)
			  != 0);
	UT_ASSERT(cluster_control_native_file_reserved_acquire_locked_v2(
				  (void *)&b, 65536, 9, reserved[0].batch_uuid, &reserved[0].expected, &b)
			  != 0);
	UT_ASSERT_EQ(reads, dependencies);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	UT_ASSERT_EQ(reserved_acquire(&b), 0);
	UT_ASSERT(reserved_acquire(&b) != 0);
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	batch_end();
}
static void
complete_original_reserved(void)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterControlRegistryBatchV4 change = { 0 };
	ClusterRecoveryCheckpointFileV2 receipts[2];
	uint8 ref[128];
	for (unsigned i = 0; i < 2; i++) {
		receipts[i] = inventory[0];
		receipts[i].identity = reserved[i].expected;
		receipts[i].copy_count = 1;
		receipts[i].copy_index = 0;
		receipts[i].synced_bytes = 8192;
	}
	change.operation = CLUSTER_CONTROL_REGISTRY_COMPLETE_V4;
	memcpy(change.batch_uuid, reserved[0].batch_uuid, 16);
	change.receipt_count = 2;
	change.receipts = receipts;
	if (!inventory_read(&s) || cluster_control_root_registry_batch_locked_v4(s, &change, ref) != 0)
		abort();
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(complete_transition_waits_for_original_borrow_then_available)
{
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 before, after;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT_EQ(reserved_acquire(&b), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	complete_original_reserved();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9),
				 CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	UT_ASSERT(cluster_control_native_file_current_v2(&b));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT_EQ(after.reservation_count, 0);
	UT_ASSERT_EQ(after.count, 4);
	UT_ASSERT(reserved_acquire(&b) != 0);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_CLOG_V2, 0,
																  0, 7, &b),
				 0);
	UT_ASSERT(identity_same(&b.expected, &reserved[0].expected));
	UT_ASSERT(cluster_control_native_file_release_v2(&b));
	batch_end();
}
UT_TEST(late_source_error_keeps_zero_borrow_and_original_cf)
{
	ClusterControlNativeFileBorrowV2 b = { 0 }, zero = { 0 };
	ClusterControlNativeTableInfoV2 before, after;
	volatile bool caught = false;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	config_error = true;
	PG_TRY();
	{
		(void)reserved_acquire(&b);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	UT_ASSERT(caught);
	UT_ASSERT(cf_held);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	config_hook = selection_change;
	UT_ASSERT(reserved_acquire(&b) != 0);
	UT_ASSERT(!memcmp(&b, &zero, sizeof(b)));
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	batch_end();
}
UT_TEST(cancelled_reservation_cannot_be_borrowed_from_stale_projection)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterControlRegistryBatchV4 c = { 0 };
	uint8 ref[128];
	ClusterControlNativeFileBorrowV2 b = { 0 };
	ClusterControlNativeTableInfoV2 before, after;
	table_reset();
	owner_begin();
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9), 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&before));
	c.operation = CLUSTER_CONTROL_REGISTRY_CANCEL_V4;
	memcpy(c.batch_uuid, reserved[0].batch_uuid, 16);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, ref), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(cluster_control_native_files_publish_locked_v2(&expected_config, 65536, 9) != 0);
	UT_ASSERT(reserved_acquire(&b) != 0);
	UT_ASSERT(cluster_control_native_files_info_v2(&after));
	UT_ASSERT(!memcmp(&before, &after, sizeof(before)));
	batch_end();
}
UT_TEST(full_reservation_vector_exceeds_pin_array_and_reports_copy_cost)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterControlRegistryBatchV4 c;
	ClusterRecoveryFileIdentityV2 ids[73];
	uint8 ref[128];
	ClusterControlNativeTableInfoV2 info, before;
	struct timespec a, b;
	table_reset();
	owner_begin();
	c = new_batch(ids);
	c.identity_count = 73;
	for (unsigned i = 2; i < 73; i++) {
		ids[i] = ids[0];
		ids[i].object_id += i;
	}
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, ref), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	clock_gettime(CLOCK_MONOTONIC, &a);
	UT_ASSERT_EQ(
		cluster_control_native_files_preregister_locked_v2(&expected_config, 65536, 9, 0, 17, 89),
		0);
	clock_gettime(CLOCK_MONOTONIC, &b);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT_EQ(info.reservation_count, 75);
	UT_ASSERT_EQ(info.count, 2);
	printf(
		"# projection75 ns=%llu shared_bytes=%llu rows=%llu reserved_capacity=%u "
		"network_CF_not_measured\n",
		(unsigned long long)((b.tv_sec - a.tv_sec) * UINT64_C(1000000000) + b.tv_nsec - a.tv_nsec),
		(unsigned long long)info.shared_bytes,
		(unsigned long long)(info.count + info.reservation_count),
		(unsigned)CLUSTER_NATIVE_RESERVATIONS_CAPACITY_V2);
	before = info;
	UT_ASSERT_EQ(cluster_control_native_files_publish_locked_v2(
					 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2 + 1, 9),
				 CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2);
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	UT_ASSERT(!memcmp(&before, &info, sizeof(info)));
	batch_end();
}
int
main(int argc, char **argv)
{
	if (argc > 1 && !strcmp(argv[1], "--original"))
		return original_registry_batch_main(1, argv);
	UT_RUN(persistent_reserved_range_projects_before_native_locks);
	UT_RUN(ordinary_reserved_address_is_explicitly_unavailable);
	UT_RUN(exact_creator_borrow_retains_original_protection_without_cf);
	UT_RUN(creator_acquire_rejects_wrong_batch_owner_and_full_expected);
	UT_RUN(creator_acquire_precedes_family_locks_and_protects_aliases);
	UT_RUN(complete_transition_waits_for_original_borrow_then_available);
	UT_RUN(late_source_error_keeps_zero_borrow_and_original_cf);
	UT_RUN(cancelled_reservation_cannot_be_borrowed_from_stale_projection);
	UT_RUN(full_reservation_vector_exceeds_pin_array_and_reports_copy_cost);
	UT_DONE();
	return ut_failed_count != 0;
}
