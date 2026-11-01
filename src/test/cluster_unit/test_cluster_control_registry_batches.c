/* Actual selected ROOT/registry files; authority and native receipts are leaves. */
#define TEST_CONTROL_RELMAP_CONSUMER 1
#define cluster_cf_held_is_clusterwide batch_fixture_cf_held
#define cluster_cf_control_image_read_locked batch_fixture_common_read
#include "test_cluster_control_relmap_selection.c"
#undef cluster_cf_control_image_read_locked
#undef cluster_cf_held_is_clusterwide
static bool batch_exclusive = true;
bool cluster_cf_held_is_clusterwide(LOCKMODE mode);
bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return batch_fixture_cf_held(mode) && (mode != ExclusiveLock || batch_exclusive);
}
#include "common/cluster_recovery_reservations.h"
#include "cluster/cluster_control_registry_batch.h"
#include "catalog/catversion.h"
#include <dirent.h>
#include <time.h>

static uint32 batch_catalog = CATALOG_VERSION_NO;
ClusterControlRootResult cluster_cf_control_image_read_locked(uint64 generation,
															  const uint8 hash[32], uint64 sys,
															  ControlFileData *out);
ClusterControlRootResult
cluster_cf_control_image_read_locked(uint64 generation, const uint8 hash[32], uint64 sys,
									 ControlFileData *out)
{
	ClusterControlRootResult r = batch_fixture_common_read(generation, hash, sys, out);
	if (r == 0)
		out->catalog_version_no = batch_catalog;
	return r;
}
static ClusterRecoveryRegistryV3 batch_registry;
static ClusterRecoveryFileReservationV1 reserved[4];
static void
batch_begin(void)
{
	uint8 oldref[128], ref[128];
	inventory_begin(2);
	batch_catalog = CATALOG_VERSION_NO;
	batch_exclusive = true;
	memset(reserved, 0, sizeof(reserved));
	for (unsigned i = 0; i < 2; i++) {
		reserved[i].expected = inventory[0].identity;
		reserved[i].expected.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
		reserved[i].expected.database_oid = reserved[i].expected.tablespace_oid = 0;
		reserved[i].expected.object_id = 7 + i;
		memset(reserved[i].batch_uuid, 0x71, 16);
		reserved[i].creation_owner = root.records[0].identity;
		reserved[i].owner_generation = 1;
		reserved[i].state = CLUSTER_RECOVERY_FILE_RESERVED_V1;
	}
	memset(&batch_registry, 0, sizeof(batch_registry));
	batch_registry.available = reg;
	batch_registry.catalog_version = CATALOG_VERSION_NO;
	batch_registry.reservation_count = 2;
	batch_registry.reservations = reserved;
	if (!cluster_recovery_registry_create_v3(selection_dir, &batch_registry, 65536, ref))
		abort();
	{
		/* The original immutable V2 file already exists. Only encode its reference. */
		Size n;
		uint8 *b;
		if (!cluster_recovery_registry_size_v2(&reg, 65536, &n))
			abort();
		b = malloc(n);
		if (!cluster_recovery_registry_encode_v2(&reg, b, n, oldref))
			abort();
		free(b);
	}
	if (!cluster_control_root_registry_encode_v4(&root, &reg, oldref, selection_bytes, 66560))
		abort();
	/* Independent exact frozen ROOT slot/CRC construction, used against parent. */
	memcpy(selection_bytes + 512, ref, 128);
	{
		pg_crc32c c;
		INIT_CRC32C(c);
		COMP_CRC32C(c, selection_bytes, 1020);
		FIN_CRC32C(c);
		for (unsigned i = 0; i < 4; i++)
			selection_bytes[1020 + i] = c >> (8 * i);
	}
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
}
static void
batch_end(void)
{
	int d = openat(selection_dir, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
	DIR *dir = fdopendir(d);
	struct dirent *e;
	if (!dir)
		abort();
	while ((e = readdir(dir)) != NULL)
		if (strstr(e->d_name, ".v3"))
			unlinkat(d, e->d_name, 0);
	closedir(dir);
	selection_end();
}
UT_TEST(test_v3_registry_is_selected_by_exact_root_v4)
{
	ControlRootImage *r = calloc(1, sizeof(*r));
	batch_begin();
	UT_ASSERT(cluster_control_root_registry_decode_v4(selection_bytes, 66560,
													  root.header.storage_uuid, 11, r));
	UT_ASSERT_EQ(r->header.format_version, 4);
	UT_ASSERT_EQ(r->header.file_txn_seq, root.header.file_txn_seq);
	free(r);
}
UT_TEST(test_available_inventory_retains_whole_reservations_privately)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 out[8];
	uint64 count = 0;
	ClusterRecoveryFileReservationV1 observation;
	batch_begin();
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_count_v4(s), 2);
	UT_ASSERT(cluster_control_root_native_files_copy_v4(s, out, 8, &count));
	UT_ASSERT_EQ(count, 4);
	UT_ASSERT(cluster_control_root_registry_reservation_at_v4(s, 1, &observation));
	UT_ASSERT(identity_same(&observation.expected, &reserved[1].expected));
	UT_ASSERT(!cluster_control_root_registry_reservation_at_v4(s, 2, &observation));
	UT_ASSERT(!cluster_control_root_registry_reservation_at_v4(s, 0, (void *)s));
	for (uint64 i = 0; i < count; i++)
		UT_ASSERT(out[i].kind == CLUSTER_RECOVERY_FILE_RELMAP_V2);
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) == 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_reservation_cannot_be_ordinary_file_or_wrong_catalog)
{
	ClusterRecoveryFileIdentityV2 out, old;
	ClusterControlFileKeyV2 key = { 0 };
	ClusterControlRelmapSelectionV2 *s = NULL;
	batch_begin();
	key.kind = reserved[0].expected.kind;
	key.object_id = reserved[0].expected.object_id;
	memset(&out, 0x6c, sizeof(out));
	old = out;
	UT_ASSERT(cluster_control_root_file_identity_read_locked_v4(&expected_config, &key, 65536, &out)
			  != 0);
	UT_ASSERT(!memcmp(&old, &out, sizeof(out)));
	batch_catalog--;
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	batch_catalog++;
	UT_ASSERT(inventory_read(&s));
	cluster_control_root_relmap_selection_destroy_v4(&s);
}

#include "cluster/cluster_control_registry_batch.h"
/* Native owner, claim and anchor leaves only; ROOT/registry/writer I/O is real. */
#include "cluster/cluster_membership.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_claim.h"
#include "../../backend/cluster/cluster_recovery_anchor_private.h"
#include "access/xlog.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_conf.h"
#include "utils/timestamp.h"
#include "storage/lwlock.h"
volatile uint32 CritSectionCount;
static LWLockPadded batch_lwlocks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = batch_lwlocks;
bool
LWLockHeldByMe(LWLock *lock)
{
	return false;
}
bool cluster_shared_config = true, cluster_enabled = true;
bool cluster_controlfile_shared_authority = true;
int cluster_node_id;
char *cluster_wal_threads_dir = "/controlled-native-claims";
static bool owner_ok = true, claim_ok = true, anchor_ok = true;
static unsigned claim_reads, anchor_reads, root_renames;
static unsigned fail_rename;
static void (*rename_hook)(void);
uint64
GetSystemIdentifier(void)
{
	return 11;
}
uint64
cluster_epoch_get_current(void)
{
	return owner_ok ? 1 : 2;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 40;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return CLUSTER_MEMBER_MEMBER;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return 40;
}
bool
cluster_wal_thread_dir_validated(void)
{
	return true;
}
uint16
cluster_wal_thread_id(void)
{
	return 1;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return false;
}
bool
cluster_write_fence_allowed(void)
{
	return owner_ok;
}
bool
cluster_serving_ready_check(bool *pending, const char **failed)
{
	*pending = false;
	return true;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 123456;
}
ClusterControlRootResult
cluster_wal_claim_v2_read(const char *path, const ClusterWalThreadClaimRefV2 *ref,
						  ClusterWalThreadClaimV2 *out)
{
	claim_reads++;
	return claim_ok
				   && cluster_control_root_identity_equal(&ref->identity, &root.records[0].identity)
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}
ClusterControlRootResult
cluster_recovery_anchor_v2_read_locked(const ClusterRecoveryAnchorRefV2 *ref,
									   const ControlFileData *common, ControlFileData *out)
{
	anchor_reads++;
	memset(out, 0, sizeof(*out));
	out->state = DB_IN_PRODUCTION;
	return anchor_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}
ClusterControlRootResult
cluster_recovery_anchor_v2_thread_state(const ClusterRecoveryAnchorRefV2 *ref,
										const ClusterControlRootSnapshot *record,
										ControlFileData *out)
{
	return anchor_ok ? 0 : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}
int
BasicOpenFilePerm(const char *p, int flags, mode_t mode)
{
	return open(p, flags, mode);
}
int
pg_fsync(int fd)
{
	return fsync(fd);
}
int
durable_rename(const char *old, const char *new, int elevel)
{
	root_renames++;
	if (rename_hook) {
		void (*f)(void) = rename_hook;
		rename_hook = NULL;
		f();
	}
	if (fail_rename == root_renames)
		return -1;
	return rename(old, new);
}
static void
owner_begin(void)
{
	batch_begin();
	owner_ok = claim_ok = anchor_ok = true;
	claim_reads = anchor_reads = root_renames = fail_rename = 0;
	rename_hook = NULL;
	root.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_OPEN;
	root.header.v2.serving[0] = 1;
	/* Rebuild same container, exact independent frozen slot. */
	{
		uint8 *p = malloc(65536), ref[128];
		Size n;
		if (!cluster_recovery_registry_size_v3(&batch_registry, 65536, &n)
			|| !cluster_recovery_registry_encode_v3(&batch_registry, p, n, ref)
			|| !cluster_control_root_reservations_encode_v4(&root, &batch_registry, ref,
															selection_bytes, 66560))
			abort();
		free(p);
	}
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
}
static ClusterControlRegistryBatchV4
new_batch(ClusterRecoveryFileIdentityV2 ids[2])
{
	ClusterControlRegistryBatchV4 c = { 0 };
	c.operation = CLUSTER_CONTROL_REGISTRY_RESERVE_V4;
	memset(c.batch_uuid, 0x72, 16);
	c.identity_count = 2;
	c.identities = ids;
	for (unsigned i = 0; i < 2; i++) {
		ids[i] = reserved[i].expected;
		ids[i].object_id += 10;
		ids[i].registration_generation++;
	}
	return c;
}
UT_TEST(test_whole_batch_reserve_borrow_complete_and_cancel)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2], out;
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterControlRegistryBatchV4 c;
	uint8 reference[128];
	struct timespec started, ended;
	ClusterControlRootResult result;
	uint64 bytes = 0;
	owner_begin();
	c = new_batch(ids);
	UT_ASSERT(inventory_read(&s));
	clock_gettime(CLOCK_MONOTONIC, &started);
	result = cluster_control_root_registry_batch_locked_v4(s, &c, reference);
	clock_gettime(CLOCK_MONOTONIC, &ended);
	UT_ASSERT_EQ(result, 0);
	for (unsigned i = 0; i < 8; i++)
		bytes = (bytes << 8) | reference[48 + i];
	printf("# reserve2 fixture ns=%llu registry_bytes=%llu root_write_bytes=133120 "
		   "network_CF_not_measured\n",
		   (unsigned long long)((ended.tv_sec - started.tv_sec) * UINT64_C(1000000000)
								+ ended.tv_nsec - started.tv_nsec),
		   (unsigned long long)bytes);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_reserved_file_locked_v4(s, c.batch_uuid, &ids[0], &out));
	UT_ASSERT(identity_same(&out, &ids[0]));
	for (unsigned i = 0; i < 2; i++) {
		receipts[i] = inventory[0];
		receipts[i].identity = ids[i];
		receipts[i].copy_count = 1;
		receipts[i].copy_index = 0;
		receipts[i].synced_bytes = 16448;
	}
	c.operation = CLUSTER_CONTROL_REGISTRY_COMPLETE_V4;
	c.identities = NULL;
	c.identity_count = 0;
	c.receipts = receipts;
	c.receipt_count = 2;
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, reference), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(!cluster_control_root_reserved_file_locked_v4(s, c.batch_uuid, &ids[0], &out));
	{
		ClusterControlFileKeyV2 k = { 0 };
		k.kind = ids[0].kind;
		k.object_id = ids[0].object_id;
		UT_ASSERT(
			cluster_control_root_file_identity_read_locked_v4(&expected_config, &k, 65536, &out)
			== 0);
		UT_ASSERT(identity_same(&out, &ids[0]));
	}
	c.operation = CLUSTER_CONTROL_REGISTRY_CANCEL_V4;
	memset(c.batch_uuid, 0x71, 16);
	c.receipts = NULL;
	c.receipt_count = 0;
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, reference), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 0);
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_source_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(claim_reads && anchor_reads);
}
UT_TEST(test_partial_or_changed_receipt_cannot_publish_half_batch)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryCheckpointFileV2 receipt = { 0 };
	ClusterControlRegistryBatchV4 c = { 0 };
	uint8 ref[128], old[128];
	owner_begin();
	c.operation = CLUSTER_CONTROL_REGISTRY_COMPLETE_V4;
	memset(c.batch_uuid, 0x71, 16);
	receipt = inventory[0];
	receipt.identity = reserved[0].expected;
	receipt.copy_count = 1;
	receipt.copy_index = 0;
	c.receipts = &receipt;
	c.receipt_count = 1;
	memset(ref, 0xc7, sizeof(ref));
	memcpy(old, ref, sizeof(old));
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, ref) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	UT_ASSERT(!memcmp(old, ref, sizeof(ref)));
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
static void
lose_owner(void)
{
	owner_ok = false;
}
UT_TEST(test_late_owner_loss_keeps_old_root_and_material_no_retry)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128], old[128];
	owner_begin();
	c = new_batch(ids);
	memset(out, 0x41, sizeof(out));
	memcpy(old, out, 128);
	UT_ASSERT(inventory_read(&s));
	rename_hook = lose_owner;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 1);
	UT_ASSERT(!memcmp(out, old, 128));
	owner_ok = true;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 1);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}

static void
corrupt_new_registry(void)
{
	if (root_renames == 1) {
		rename_hook = corrupt_new_registry;
		return;
	}
	{
		int d = openat(selection_dir, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		DIR *dir = fdopendir(d);
		struct dirent *e;
		if (!dir)
			abort();
		while ((e = readdir(dir)) != NULL)
			if (strstr(e->d_name, ".v3") && strstr(e->d_name, "0000000000000010")) {
				int fd = openat(d, e->d_name, O_RDWR);
				uint8 byte = 0xad;
				if (fd < 0 || pwrite(fd, &byte, 1, 257) != 1 || fsync(fd) || close(fd))
					abort();
			}
		closedir(dir);
	}
}
UT_TEST(test_final_selected_registry_is_reread_after_root_publication)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128], old[128];
	owner_begin();
	c = new_batch(ids);
	memset(out, 0x66, 128);
	memcpy(old, out, 128);
	UT_ASSERT(inventory_read(&s));
	rename_hook = corrupt_new_registry;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(!memcmp(out, old, 128));
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_native_claim_anchor_and_creator_identity_remain_required)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2], out;
	ClusterControlRegistryBatchV4 c;
	uint8 ref[128];
	owner_begin();
	c = new_batch(ids);
	UT_ASSERT(inventory_read(&s));
	claim_ok = false;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, ref) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	claim_ok = true;
	UT_ASSERT(inventory_read(&s));
	anchor_ok = false;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, ref) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	anchor_ok = true;
	UT_ASSERT(inventory_read(&s));
	owner_ok = false;
	UT_ASSERT(!cluster_control_root_reserved_file_locked_v4(s, reserved[0].batch_uuid,
															&reserved[0].expected, &out));
	UT_ASSERT_EQ(root_renames, 0);
	owner_ok = true;
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_no_existing_key_replacement_or_duplicate_batch)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128];
	owner_begin();
	c = new_batch(ids);
	ids[0] = inventory[0].identity;
	ids[0].registration_generation++;
	ids[0].creation_generation++;
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	c = new_batch(ids);
	memset(c.batch_uuid, 0x71, 16);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_cf_x_exact_interval_and_output_aliases_are_required)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128];
	owner_begin();
	c = new_batch(ids);
	UT_ASSERT(inventory_read(&s));
	batch_exclusive = false;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out)
			  == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	batch_exclusive = true;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, (void *)s) != 0);
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, (void *)ids) != 0);
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) == 0);
	cf_sequence++;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_backup_and_primary_uncertainty_preserve_output_no_retry)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128], old[128];
	owner_begin();
	c = new_batch(ids);
	memset(out, 0xe1, 128);
	memcpy(old, out, 128);
	UT_ASSERT(inventory_read(&s));
	fail_rename = 2;
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	UT_ASSERT(!memcmp(out, old, 128));
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_entered_error_retains_original_directory_and_cf)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128], old[128];
	volatile bool caught = false;
	owner_begin();
	c = new_batch(ids);
	memset(out, 0xd4, 128);
	memcpy(old, out, 128);
	UT_ASSERT(inventory_read(&s));
	config_error = true;
	PG_TRY();
	{
		(void)cluster_control_root_registry_batch_locked_v4(s, &c, out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	UT_ASSERT(caught && cf_held && fcntl(selection_dir, F_GETFD) >= 0);
	UT_ASSERT(!memcmp(out, old, 128) && root_renames == 0);
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_coverage_only_root_upgrades_without_losing_any_old_entry)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128];
	owner_begin();
	inventory_write(); /* Exact original V2 registry reference. */
	c = new_batch(ids);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 0);
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, out), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 2);
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_count_v4(s), 2);
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_source_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}

UT_TEST(test_relmap_completion_requires_original_pair_geometry)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[2];
	ClusterRecoveryCheckpointFileV2 receipts[4];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128];
	owner_begin();
	c = new_batch(ids);
	for (unsigned i = 0; i < 2; i++) {
		ids[i] = inventory[2].identity;
		ids[i].database_oid = 8000 + i;
		ids[i].registration_generation++;
	}
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, out), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	for (unsigned i = 0; i < 4; i++) {
		receipts[i] = inventory[i % 2];
		receipts[i].identity = ids[i / 2];
		receipts[i].copy_index = i % 2;
		receipts[i].synced_bytes = 2176;
	}
	c.operation = CLUSTER_CONTROL_REGISTRY_COMPLETE_V4;
	c.identity_count = 0;
	c.identities = NULL;
	c.receipt_count = 4;
	c.receipts = receipts;
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	for (unsigned i = 0; i < 4; i++)
		receipts[i].synced_bytes = 2304;
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, out), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_count_v4(s), 4);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
UT_TEST(test_combined_budget_and_more_than_active_pin_capacity)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 ids[73];
	ClusterControlRegistryBatchV4 c;
	uint8 out[128];
	owner_begin();
	c = new_batch(ids);
	c.identity_count = 73;
	for (unsigned i = 2; i < 73; i++) {
		ids[i] = ids[0];
		ids[i].object_id += i;
	}
	UT_ASSERT(cluster_control_root_relmap_selection_read_locked_v4(&expected_config, 4096, &s)
			  == 0);
	UT_ASSERT(cluster_control_root_registry_batch_locked_v4(s, &c, out) != 0);
	UT_ASSERT_EQ(root_renames, 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_batch_locked_v4(s, &c, out), 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_registry_reservation_count_v4(s), 75);
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_count_v4(s), 2);
	cluster_control_root_relmap_selection_destroy_v4(&s);
}
#ifdef TEST_CONTROL_NATIVE_RESERVATIONS
#define main original_registry_batch_main
#endif
int
main(int argc, char **argv)
{
	if (argc > 1 && !strcmp(argv[1], "--original")) {
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

		UT_DONE();
		return ut_failed_count != 0;
	}
	UT_RUN(test_v3_registry_is_selected_by_exact_root_v4);
	batch_end();
	UT_RUN(test_available_inventory_retains_whole_reservations_privately);
	batch_end();
	UT_RUN(test_reservation_cannot_be_ordinary_file_or_wrong_catalog);
	batch_end();
	UT_RUN(test_whole_batch_reserve_borrow_complete_and_cancel);
	batch_end();
	UT_RUN(test_partial_or_changed_receipt_cannot_publish_half_batch);
	batch_end();
	UT_RUN(test_late_owner_loss_keeps_old_root_and_material_no_retry);
	batch_end();
	UT_RUN(test_final_selected_registry_is_reread_after_root_publication);
	batch_end();
	UT_RUN(test_native_claim_anchor_and_creator_identity_remain_required);
	batch_end();
	UT_RUN(test_no_existing_key_replacement_or_duplicate_batch);
	batch_end();
	UT_RUN(test_cf_x_exact_interval_and_output_aliases_are_required);
	batch_end();
	UT_RUN(test_backup_and_primary_uncertainty_preserve_output_no_retry);
	batch_end();
	UT_RUN(test_entered_error_retains_original_directory_and_cf);
	batch_end();
	UT_RUN(test_coverage_only_root_upgrades_without_losing_any_old_entry);
	batch_end();
	UT_RUN(test_relmap_completion_requires_original_pair_geometry);
	batch_end();
	UT_RUN(test_combined_budget_and_more_than_active_pin_capacity);
	batch_end();
	UT_DONE();
	return ut_failed_count != 0;
}
