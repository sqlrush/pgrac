/* PGRAC: actual shared retention birth/attach and original-owner routing.
 * Shmem, LWLock, native authority and TT publication are controlled providers.
 * COMPLETE/floor/issued values below are fixtures, not runtime certification.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_shmem.h"
#include "storage/shmem.h"

static const ClusterShmemRegion *relmap_runtime_registered;
static void *relmap_runtime_allocation;
static Size relmap_runtime_bytes;
static unsigned relmap_runtime_new_tranches, relmap_runtime_lock_inits;
static unsigned relmap_runtime_tranche_registrations;
static unsigned relmap_runtime_stage_calls, relmap_runtime_stage_writes;
static unsigned relmap_runtime_stage_mode;
static ClusterRelmapV2Owner relmap_runtime_stage_owner;
static ClusterRelmapV2Namespace relmap_runtime_stage_identity;
static RelMapFile relmap_runtime_stage_map;

#include "../../backend/cluster/cluster_relmap_retention_runtime.c"

/* Controlled storage boundary only. The real authority/filesystem failure-cut
 * suite is separate; these counters are not durable I/O evidence. */
bool
cluster_relmap_authority_v2_write_pending(const ClusterRelmapV2Namespace *identity,
	uint64 generation, const ClusterRelmapV2Owner *owner, const RelMapFile *pending,
	ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionResult result;

	if (relmap_retention_held || lock != relmap_runtime_test_lock
		|| owner->node >= CLUSTER_MAX_NODES
		|| directory != &relmap_runtime_shmem->directories[owner->node])
		abort();
	relmap_runtime_stage_calls++;
	relmap_runtime_stage_owner = *owner;
	relmap_runtime_stage_identity = *identity;
	relmap_runtime_stage_map = *pending;
	if (relmap_runtime_stage_mode == 1)
		return false; /* Read-only preflight failed before the real stage marker. */
	key.identity = *identity;
	key.generation = generation;
	key.origin = owner->node;
	key.top_full_xid = owner->top_full_xid;
	key.membership_epoch = owner->membership_epoch;
	key.admitted_incarnation = owner->admitted_incarnation;
	key.boot_incarnation = owner->boot_incarnation;
	LWLockAcquire(lock, LW_EXCLUSIVE);
	result = cluster_relmap_retention_stage_start(directory, &key, owner->wal_end);
	LWLockRelease(lock);
	if (result != CLUSTER_RELMAP_RETENTION_OK)
		return false;
	if (relmap_retention_held)
		abort();
	relmap_runtime_stage_writes++;
	if (relmap_runtime_stage_mode == 2)
		return false; /* First possible write failed; marker must remain. */
	relmap_runtime_stage_writes++;
	if (relmap_runtime_stage_mode == 3)
		epoch++;
	if (relmap_runtime_stage_mode == 4)
		relmap_global_x = false;
	return true;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	relmap_runtime_registered = region;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (relmap_retention_held || !relmap_runtime_registered
		|| strcmp(name, relmap_runtime_registered->name) != 0
		|| size != relmap_runtime_registered->size_fn())
		abort();
	*found = relmap_runtime_allocation != NULL;
	if (!*found) {
		relmap_runtime_allocation = malloc(size);
		if (!relmap_runtime_allocation)
			abort();
		relmap_runtime_bytes = size;
		memset(relmap_runtime_allocation, 0xa5, size);
	} else if (size != relmap_runtime_bytes)
		abort();
	return relmap_runtime_allocation;
}

int
LWLockNewTrancheId(void)
{
	relmap_runtime_new_tranches++;
	return 201;
}

void
LWLockInitialize(LWLock *lock, int tranche)
{
	if (tranche != 201 || relmap_runtime_lock_inits != 0)
		abort();
	relmap_runtime_lock_inits++;
	memset(lock, 0, sizeof(*lock));
	lock->tranche = tranche;
	relmap_runtime_test_lock = lock;
}

void
LWLockRegisterTranche(int tranche, const char *name)
{
	if (tranche != 201 || strcmp(name, "ClusterRelmapRetention") != 0)
		abort();
	relmap_runtime_tranche_registrations++;
}

static void
relmap_runtime_fixture(bool attach)
{
	cluster_node_id = 0;
	relmap_owner_fixture();
	free(relmap_runtime_allocation);
	relmap_runtime_allocation = NULL;
	relmap_runtime_registered = NULL;
	relmap_runtime_shmem = NULL;
	relmap_runtime_test_lock = NULL;
	relmap_runtime_bytes = 0;
	relmap_runtime_new_tranches = relmap_runtime_lock_inits = 0;
	relmap_runtime_tranche_registrations = 0;
	relmap_runtime_stage_calls = relmap_runtime_stage_writes = relmap_runtime_stage_mode = 0;
	cluster_relmap_retention_runtime_shmem_register();
	if (attach)
		relmap_runtime_registered->init_fn();
}

static void
relmap_runtime_qualified_fixture(unsigned origin)
{
	ClusterRelmapRetentionDirectory *directory = &relmap_runtime_shmem->directories[origin];
	/* Only test setup supplies a qualified COMPLETE origin; no product API does. */
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(directory,
		ref.claim.identity.system_identifier, ref.claim.database_incarnation, origin, 7,
		relmap_top_full - 100, relmap_top_full + 100), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(directory), CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(relmap_runtime_test_lock);
	relmap_retention_acquires = 0;
}


static void
relmap_runtime_prepare_stage(ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapV2Namespace *identity, unsigned origin, bool shared)
{
	relmap_runtime_fixture(true);
	cluster_node_id = ref.claim.identity.origin_node_id = origin;
	ref.claim.identity.origin_thread_id = origin + 1;
	relmap_runtime_qualified_fixture(origin);
	UT_ASSERT(cluster_relmap_owner_capture_v2(sample));
	*identity = relmap_owner_namespace();
	identity->shared = shared;
	identity->dbid = shared ? InvalidOid : MyDatabaseId;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(sample, identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
}

UT_TEST(test_relmap_runtime_stage_routes_original_owner_namespace_and_lock)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1)
		for (unsigned shared = 0; shared < 2; shared++) {
			ClusterRelmapOwnerSampleV2 sample, before;
			ClusterRelmapV2Namespace identity;
			RelMapFile pending = { 0 };
			ClusterRelmapRetentionPin *pin;

			relmap_runtime_prepare_stage(&sample, &identity, origin, shared);
			before = sample;
			pending.num_mappings = 3;
			UT_ASSERT(cluster_relmap_retention_runtime_write_pending_v2(
				&sample, &identity, 8, 456, &pending));
			UT_ASSERT_EQ(relmap_runtime_stage_calls, 1);
			UT_ASSERT_EQ(relmap_runtime_stage_writes, 2);
			UT_ASSERT_EQ(memcmp(&sample, &before, sizeof(sample)), 0);
			before.owner.wal_end = 456;
			UT_ASSERT_EQ(memcmp(&relmap_runtime_stage_owner, &before.owner, sizeof(before.owner)), 0);
			UT_ASSERT_EQ(memcmp(&relmap_runtime_stage_identity, &identity, sizeof(identity)), 0);
			UT_ASSERT_EQ(memcmp(&relmap_runtime_stage_map, &pending, sizeof(pending)), 0);
			pin = &relmap_runtime_shmem->directories[origin].pins[0];
			UT_ASSERT(pin->occupied && pin->may_be_staged && pin->wal_end == 456);
			UT_ASSERT_EQ(relmap_assign_calls, 1);
			UT_ASSERT_EQ(relmap_binding_reads, 1);
			UT_ASSERT(!relmap_retention_held);
		}
}

UT_TEST(test_relmap_runtime_stage_keeps_original_mapping_lock_order)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	RelMapFile pending = { 0 };

	relmap_runtime_prepare_stage(&sample, &identity, 0, true);
	relmap_local_lock = relmap_stage_lock_window = true;
	UT_ASSERT(cluster_relmap_retention_runtime_write_pending_v2(&sample, &identity, 8, 456, &pending));
	UT_ASSERT(relmap_local_lock && !relmap_retention_held);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	relmap_local_lock = relmap_stage_lock_window = false;
}

UT_TEST(test_relmap_runtime_stage_refuses_unattached_unknown_and_building)
{
	for (unsigned state = 0; state < 3; state++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory before = { 0 };
		RelMapFile pending = { 0 };

		relmap_runtime_fixture(state != 0);
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		if (state == 2)
			relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (state != 0)
			before = relmap_runtime_shmem->directories[0];
		UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(&sample, &identity, 8, 456, &pending));
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(relmap_runtime_stage_writes, 0);
		if (state != 0)
			UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	}
}

UT_TEST(test_relmap_runtime_stage_rejects_wrong_inputs_before_storage)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory before;
		RelMapFile pending = { 0 };
		uint64 generation = 8, wal_end = 456;

		relmap_runtime_prepare_stage(&sample, &identity, 0, true);
		before = relmap_runtime_shmem->directories[0];
		if (fault == 0) sample.producer_pid++;
		if (fault == 1) sample.owner.top_full_xid += UINT64CONST(0x100000000);
		if (fault == 2) sample.owner.node = CLUSTER_MAX_NODES;
		if (fault == 3) sample.owner.wal_end = 456;
		if (fault == 4) sample.owner.boot_incarnation++;
		if (fault == 5) relmap_admission_delta = 1;
		if (fault == 6) epoch++;
		if (fault == 7) native_ctl.InsertTimeLineID++;
		if (fault == 8) identity.system_identifier++;
		if (fault == 9) identity.database_incarnation++;
		if (fault == 10) identity.dbid = MyDatabaseId;
		if (fault == 11) { identity.shared = false; identity.dbid = MyDatabaseId + 1; }
		if (fault == 12) generation = 0;
		if (fault == 13) wal_end = 0;
		UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(
			&sample, &identity, generation, wal_end, &pending));
		if (fault != 13)
			UT_ASSERT(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, generation)
				!= CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(relmap_runtime_stage_calls, 0);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

UT_TEST(test_relmap_runtime_stage_rejects_nulls_and_reentrant_lock)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	RelMapFile pending = { 0 };
	ClusterRelmapRetentionDirectory before;

	relmap_runtime_prepare_stage(&sample, &identity, 0, true);
	before = relmap_runtime_shmem->directories[0];
	UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(NULL, &identity, 8, 456, &pending));
	UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(&sample, NULL, 8, 456, &pending));
	UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(&sample, &identity, 8, 456, NULL));
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(NULL, &identity, 8),
		CLUSTER_RELMAP_RETENTION_INVALID);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(&sample, &identity, 8, 456, &pending));
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT(relmap_retention_held);
	LWLockRelease(relmap_runtime_test_lock);
	UT_ASSERT_EQ(relmap_runtime_stage_calls, 0);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
}

UT_TEST(test_relmap_runtime_stage_needs_matching_directory_and_exact_pin)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory before, *directory;
		RelMapFile pending = { 0 };
		uint64 generation = fault == 4 ? 9 : 8;

		relmap_runtime_prepare_stage(&sample, &identity, 0, true);
		directory = &relmap_runtime_shmem->directories[0];
		if (fault == 0) directory->origin++;
		if (fault == 1) directory->system_identifier++;
		if (fault == 2) directory->database_incarnation++;
		if (fault == 3)
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
				CLUSTER_RELMAP_RETENTION_OK);
		before = *directory;
		UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(
			&sample, &identity, generation, 456, &pending));
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, generation),
			fault < 3 ? CLUSTER_RELMAP_RETENTION_INVALID : CLUSTER_RELMAP_RETENTION_CONFLICT);
		UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_runtime_stage_writes, 0);
		UT_ASSERT_EQ(relmap_runtime_stage_calls, fault < 3 ? 0 : 1);
	}
}

UT_TEST(test_relmap_runtime_stage_failure_preserves_marker_before_cancel)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		RelMapFile pending = { 0 };
		ClusterRelmapRetentionPin *pin;

		relmap_runtime_prepare_stage(&sample, &identity, 0, true);
		relmap_runtime_stage_mode = mode;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_write_pending_v2(
			&sample, &identity, 8, 456, &pending), mode == 0);
		pin = &relmap_runtime_shmem->directories[0].pins[0];
		UT_ASSERT_EQ(pin->may_be_staged, mode != 1);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
			mode == 1 ? CLUSTER_RELMAP_RETENTION_OK : CLUSTER_RELMAP_RETENTION_RETAINED);
		UT_ASSERT_EQ(pin->occupied, mode != 1);
		UT_ASSERT_EQ(relmap_runtime_stage_writes, mode == 1 ? 0 : mode == 2 ? 1 : 2);
	}
}

UT_TEST(test_relmap_runtime_stage_rechecks_owner_after_storage_success)
{
	for (unsigned mode = 3; mode <= 4; mode++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		RelMapFile pending = { 0 };
		ClusterRelmapRetentionDirectory before;

		relmap_runtime_prepare_stage(&sample, &identity, 0, true);
		relmap_runtime_stage_mode = mode;
		UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(&sample, &identity, 8, 456, &pending));
		UT_ASSERT_EQ(relmap_runtime_stage_writes, 2);
		UT_ASSERT(relmap_runtime_shmem->directories[0].pins[0].may_be_staged);
		before = relmap_runtime_shmem->directories[0];
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	}
}

UT_TEST(test_relmap_runtime_cancel_only_exact_unstaged_preparation)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace shared, database;
	ClusterRelmapRetentionDirectory before;

	relmap_runtime_prepare_stage(&sample, &shared, 0, true);
	database = shared;
	database.shared = false;
	database.dbid = MyDatabaseId;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &database, 9), CLUSTER_RELMAP_RETENTION_OK);
	before = relmap_runtime_shmem->directories[0];
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &shared, 9),
		CLUSTER_RELMAP_RETENTION_CONFLICT);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &shared, 8), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(!relmap_runtime_shmem->directories[0].pins[0].occupied);
	UT_ASSERT_EQ(memcmp(&before.pins[1], &relmap_runtime_shmem->directories[0].pins[1], sizeof(before.pins[1])), 0);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &shared, 8), CLUSTER_RELMAP_RETENTION_CONFLICT);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
	UT_ASSERT_EQ(relmap_binding_reads, 2);
}


UT_TEST(test_relmap_runtime_refuses_before_attachment)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	relmap_runtime_fixture(false);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(relmap_binding_reads + relmap_retention_acquires, 0);
	UT_ASSERT_EQ(relmap_assign_calls, 1);
}

UT_TEST(test_relmap_runtime_birth_is_unknown_and_one_lock)
{
	ClusterRelmapRetentionDirectory zero = { 0 };
	relmap_runtime_fixture(true);
	UT_ASSERT_EQ(relmap_runtime_registered->lwlock_count, 1);
	UT_ASSERT_EQ(relmap_runtime_registered->reserved_flags, 0);
	UT_ASSERT_EQ(relmap_runtime_bytes, MAXALIGN(sizeof(*relmap_runtime_shmem)));
	UT_ASSERT_EQ(relmap_runtime_new_tranches, 1);
	UT_ASSERT_EQ(relmap_runtime_lock_inits, 1);
	UT_ASSERT_EQ(relmap_runtime_tranche_registrations, 1);
	UT_ASSERT(relmap_runtime_test_lock == &relmap_runtime_shmem->lock);
	for (unsigned i = 0; i < CLUSTER_MAX_NODES; i++)
		UT_ASSERT_EQ(memcmp(&relmap_runtime_shmem->directories[i], &zero, sizeof(zero)), 0);
}

UT_TEST(test_relmap_runtime_unknown_never_adds_pin)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	ClusterRelmapRetentionDirectory before;
	relmap_runtime_fixture(true);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	before = relmap_runtime_shmem->directories[0];
	relmap_have_binding = true;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT(!relmap_retention_held);
}

UT_TEST(test_relmap_runtime_routes_original_origin_and_inherits_binding)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory *directory;
		relmap_runtime_fixture(true);
		cluster_node_id = ref.claim.identity.origin_node_id = origin;
		ref.claim.identity.origin_thread_id = origin + 1;
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_binding.origin_instance = origin + 1;
		relmap_runtime_qualified_fixture(origin);
		directory = &relmap_runtime_shmem->directories[origin];
		relmap_have_binding = true;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
			CLUSTER_RELMAP_RETENTION_OK);
		relmap_have_binding = false; /* same full's existing original receipt survives */
		identity.shared = true;
		identity.dbid = InvalidOid; /* a distinct namespace, not a competing generation */
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 9),
			CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(directory->pins[0].key.origin, origin);
		UT_ASSERT_EQ(directory->pins[0].key.top_full_xid, sample.owner.top_full_xid);
		UT_ASSERT_EQ(directory->pins[1].key.generation, 9);
		UT_ASSERT(directory->pins[0].has_tt_binding && directory->pins[1].has_tt_binding);
		UT_ASSERT_EQ(memcmp(&directory->pins[1].binding, &relmap_binding, sizeof(relmap_binding)), 0);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[origin == 0 ? 1 : 0].state,
			CLUSTER_RELMAP_RETENTION_UNKNOWN);
		UT_ASSERT_EQ(relmap_retention_acquires, 2);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
		UT_ASSERT_EQ(sync_calls + rename_calls, 0);
	}
}

UT_TEST(test_relmap_runtime_preserves_pins_and_reservation_on_attach)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	ClusterRelmapRetentionTruncate reservation;
	ClusterRelmapRetentionDirectory *directory;
	void *before;
	relmap_runtime_fixture(true);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_runtime_qualified_fixture(0);
	relmap_have_binding = true;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	directory = &relmap_runtime_shmem->directories[0];
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(directory, &directory->pins[0].key, 900),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(directory, relmap_top_full + 10,
		&reservation), CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(relmap_runtime_test_lock);
	before = malloc(relmap_runtime_bytes);
	UT_ASSERT_NOT_NULL(before);
	memcpy(before, relmap_runtime_shmem, relmap_runtime_bytes);
	relmap_runtime_shmem = NULL; /* EXEC_BACKEND-like process-local rebind */
	relmap_runtime_registered->init_fn();
	UT_ASSERT(relmap_runtime_shmem == relmap_runtime_allocation);
	UT_ASSERT_EQ(memcmp(before, relmap_runtime_shmem, relmap_runtime_bytes), 0);
	UT_ASSERT_EQ(relmap_runtime_new_tranches, 1);
	UT_ASSERT_EQ(relmap_runtime_lock_inits, 1);
	UT_ASSERT_EQ(relmap_runtime_tranche_registrations, 2);
	free(before);
}

UT_TEST(test_relmap_runtime_rejects_incompatible_attachment)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		void *before;
		relmap_runtime_fixture(true);
		if (fault == 0) relmap_runtime_shmem->magic++;
		if (fault == 1) relmap_runtime_shmem->version++;
		if (fault == 2) relmap_runtime_shmem->bytes++;
		if (fault == 3) relmap_runtime_shmem->tranche = 0;
		before = malloc(relmap_runtime_bytes);
		UT_ASSERT_NOT_NULL(before);
		memcpy(before, relmap_runtime_shmem, relmap_runtime_bytes);
		expect_panic = true;
		if (sigsetjmp(native_error, 1) == 0) {
			relmap_runtime_registered->init_fn();
			UT_ASSERT(false);
		}
		expect_panic = false;
		UT_ASSERT(relmap_runtime_shmem == NULL);
		UT_ASSERT_EQ(memcmp(before, relmap_runtime_allocation, relmap_runtime_bytes), 0);
		UT_ASSERT_EQ(relmap_runtime_lock_inits, 1);
		UT_ASSERT_EQ(relmap_runtime_tranche_registrations, 1);
		free(before);
	}
}

UT_TEST(test_relmap_runtime_keeps_original_owner_refusals)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace identity;
		ClusterRelmapRetentionDirectory before;
		relmap_runtime_fixture(true);
		UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
		identity = relmap_owner_namespace();
		relmap_runtime_qualified_fixture(0);
		before = relmap_runtime_shmem->directories[0];
		if (fault == 0) sample.owner.node = CLUSTER_MAX_NODES;
		if (fault == 1) sample.owner.node = 1;
		if (fault == 2) identity.database_incarnation++;
		if (fault == 3) relmap_local_lock = true;
		if (fault == 4) relmap_retention_held = true;
		if (fault == 5) epoch++;
		if (fault == 6) relmap_binding_drift = true;
		UT_ASSERT_NE(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
			CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_retention_acquires, 0);
		UT_ASSERT_EQ(relmap_assign_calls, 1);
	}
}

static ClusterRelmapRetentionDirectory *
relmap_runtime_binding_fixture(unsigned origin)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionKey other;

	relmap_runtime_fixture(true);
	cluster_node_id = ref.claim.identity.origin_node_id = origin;
	ref.claim.identity.origin_thread_id = origin + 1;
	relmap_binding.origin_instance = origin + 1;
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	relmap_runtime_qualified_fixture(origin);
	directory = &relmap_runtime_shmem->directories[origin];
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	identity.shared = true;
	identity.dbid = InvalidOid;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 9),
		CLUSTER_RELMAP_RETENTION_OK);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(directory, &directory->pins[0].key, 900),
		CLUSTER_RELMAP_RETENTION_OK);
	other = directory->pins[0].key;
	other.identity.dbid++;
	other.top_full_xid++;
	UT_ASSERT_EQ(cluster_relmap_retention_begin(directory, &other), CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(relmap_runtime_test_lock);
	relmap_global_x = false; /* ACTIVE is an original owner without the relmap X lock. */
	relmap_active_owner = relmap_have_binding = true;
	relmap_retention_acquires = relmap_binding_reads = relmap_assign_calls = 0;
	return directory;
}

/* The query owns no native/TT receipt producer. Preserve every origin, not
 * merely the selected pin, across both positive and refusing observations. */
static void
relmap_runtime_tt_expect(uint64 system_identifier, uint64 database_incarnation,
	const ClusterCanonicalTxnBinding *binding, ClusterRelmapRetentionResult expected)
{
	Size bytes = sizeof(relmap_runtime_shmem->directories);
	void *before = relmap_runtime_shmem == NULL ? NULL : malloc(bytes);
	unsigned reads = relmap_binding_reads, assignments = relmap_assign_calls;
	unsigned stages = relmap_runtime_stage_calls;
	int syncs = sync_calls, renames = rename_calls;
	bool lock_held = relmap_retention_held;

	if (relmap_runtime_shmem != NULL) {
		UT_ASSERT_NOT_NULL(before);
		memcpy(before, relmap_runtime_shmem->directories, bytes);
	}
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_tt_release_check_v2(
		system_identifier, database_incarnation, binding), expected);
	if (before != NULL) {
		UT_ASSERT_EQ(memcmp(before, relmap_runtime_shmem->directories, bytes), 0);
		free(before);
	}
	UT_ASSERT_EQ(relmap_binding_reads, reads);
	UT_ASSERT_EQ(relmap_assign_calls, assignments);
	UT_ASSERT_EQ(relmap_runtime_stage_calls, stages);
	UT_ASSERT_EQ(sync_calls, syncs);
	UT_ASSERT_EQ(rename_calls, renames);
	UT_ASSERT_EQ(relmap_retention_held, lock_held);
}

UT_TEST(test_relmap_runtime_tt_empty_complete_is_only_a_readonly_conjunct)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1) {
		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(origin);
		relmap_binding.origin_instance = origin + 1;
		relmap_global_x = false;
		relmap_active_owner = true; /* Controlled original consumer lock context. */
		relmap_have_binding = have_ref = false; /* Query must not ask either producer. */
		relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
			ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(relmap_retention_acquires, 1);
	}
}

UT_TEST(test_relmap_runtime_tt_keeps_staged_and_unstaged_pins_in_every_namespace)
{
	for (unsigned selected = 0; selected < 2; selected++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		/* Each namespace independently blocks, including before any stage. */
		directory->pins[1 - selected].occupied = false;
		relmap_binding.active_lsn++; /* Receipt address is not the physical TT key. */
		relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
			ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_RETAINED);
		UT_ASSERT_EQ(directory->pins[selected].may_be_staged, selected == 0);
	}
}

UT_TEST(test_relmap_runtime_tt_distinguishes_each_physical_key_component)
{
	for (unsigned field = 0; field < 5; field++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
		ClusterCanonicalTxnBinding other = relmap_binding;

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		directory->pins[2].occupied = false; /* No unrelated unresolved raw alias. */
		if (field == 0) other.segment_id++;
		if (field == 1) other.segment_generation++;
		if (field == 2) other.xid++;
		if (field == 3) other.slot_offset++;
		if (field == 4) other.slot_wrap++;
		relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
			ref.claim.database_incarnation, &other, CLUSTER_RELMAP_RETENTION_OK);
	}
}

UT_TEST(test_relmap_runtime_tt_unbound_raw_alias_never_implies_an_empty_set)
{
	ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
	ClusterCanonicalTxnBinding other = relmap_binding;

	UT_ASSERT(!directory->pins[0].has_tt_binding && !directory->pins[1].has_tt_binding);
	other.segment_id++;
	other.slot_wrap++;
	relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
		ref.claim.database_incarnation, &other, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	other.xid += 2; /* Distinct raw identity does not acquire an invented TT pin. */
	relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
		ref.claim.database_incarnation, &other, CLUSTER_RELMAP_RETENTION_OK);
}

UT_TEST(test_relmap_runtime_tt_routes_binding_origin_instead_of_current_node)
{
	ClusterCanonicalTxnBinding local;

	relmap_runtime_binding_fixture(CLUSTER_MAX_NODES - 1);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	relmap_runtime_qualified_fixture(0);
	cluster_node_id = 0; /* Current origin is qualified and empty, foreign one is pinned. */
	relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
		ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_RETAINED);
	local = relmap_binding;
	local.origin_instance = 1;
	relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
		ref.claim.database_incarnation, &local, CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(relmap_retention_acquires, 2);
}

UT_TEST(test_relmap_runtime_tt_refuses_unattached_unknown_and_building)
{
	for (unsigned state = 0; state < 3; state++) {
		relmap_runtime_fixture(state != 0);
		if (state == 2)
			relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_BUILDING;
		relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
			ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(relmap_retention_acquires, state == 0 ? 0 : 1);
	}
}

UT_TEST(test_relmap_runtime_tt_rejects_missing_identity_and_origin_before_indexing)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		uint64 system_identifier, database_incarnation;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		system_identifier = ref.claim.identity.system_identifier;
		database_incarnation = ref.claim.database_incarnation;
		if (fault == 0) system_identifier = 0;
		if (fault == 1) database_incarnation = 0;
		if (fault == 2) relmap_binding.origin_instance = 0;
		if (fault == 3) relmap_binding.origin_instance = CLUSTER_MAX_NODES + 1;
		if (fault == 4) relmap_binding.origin_instance = UINT8_MAX;
		relmap_runtime_tt_expect(system_identifier, database_incarnation,
			fault == 5 ? NULL : &relmap_binding, CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(relmap_retention_acquires, 0);
	}
}

UT_TEST(test_relmap_runtime_tt_rejects_a_foreign_directory_identity)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapRetentionDirectory *directory;
		uint64 system_identifier, database_incarnation;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		directory = &relmap_runtime_shmem->directories[0];
		system_identifier = ref.claim.identity.system_identifier;
		database_incarnation = ref.claim.database_incarnation;
		if (fault == 0) directory->origin++;
		if (fault == 1) directory->system_identifier++;
		if (fault == 2) directory->database_incarnation++;
		if (fault == 3) system_identifier++;
		if (fault == 4) database_incarnation++;
		relmap_runtime_tt_expect(system_identifier, database_incarnation,
			&relmap_binding, CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(relmap_retention_acquires, 1);
	}
}

UT_TEST(test_relmap_runtime_tt_preserves_original_core_receipt_refusals)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		if (fault == 0) relmap_binding.publish_state = CLUSTER_CANONICAL_TXN_NONE;
		if (fault == 1) relmap_binding.reserved16 = 1;
		if (fault == 2) relmap_binding.segment_id = 0;
		if (fault == 3) relmap_binding.segment_generation = UINT32_MAX;
		if (fault == 4) relmap_binding.slot_offset = TT_SLOTS_PER_SEGMENT;
		if (fault == 5) relmap_binding.slot_wrap = TT_WRAP_INVALID;
		if (fault == 6) relmap_binding.xid = BootstrapTransactionId;
		if (fault == 7) relmap_binding.active_lsn = InvalidXLogRecPtr;
		relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
			ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_INVALID);
	}
}

UT_TEST(test_relmap_runtime_tt_refuses_corrupt_pins_and_recursive_lock)
{
	ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
	unsigned acquisitions;

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	directory->pins[0].binding.xid++; /* An invalid unrelated pin is not silently skipped. */
	relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
		ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	acquisitions = relmap_retention_acquires;
	relmap_runtime_tt_expect(ref.claim.identity.system_identifier,
		ref.claim.database_incarnation, &relmap_binding, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(relmap_retention_acquires, acquisitions);
	LWLockRelease(relmap_runtime_test_lock);
}

UT_TEST(test_relmap_runtime_active_binds_all_existing_namespaces_and_staged_pin)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(origin);
		ClusterRelmapRetentionPin other = directory->pins[2];

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		for (unsigned i = 0; i < 2; i++) {
			UT_ASSERT(directory->pins[i].has_tt_binding);
			UT_ASSERT_EQ(memcmp(&directory->pins[i].binding, &relmap_binding, sizeof(relmap_binding)), 0);
		}
		UT_ASSERT(directory->pins[0].may_be_staged && !directory->pins[1].may_be_staged);
		UT_ASSERT_EQ(directory->pins[0].wal_end, 900);
		UT_ASSERT_EQ(memcmp(&other, &directory->pins[2], sizeof(other)), 0);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[origin == 0 ? 1 : 0].state,
			CLUSTER_RELMAP_RETENTION_UNKNOWN);
		UT_ASSERT_EQ(relmap_binding_reads, 1);
		UT_ASSERT_EQ(relmap_retention_acquires, 1);
		UT_ASSERT_EQ(relmap_assign_calls + sync_calls + rename_calls, 0);
		UT_ASSERT(!relmap_retention_held && !relmap_global_x);
	}
}

UT_TEST(test_relmap_runtime_active_retry_is_idempotent_and_conflict_is_atomic)
{
	ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
	ClusterRelmapRetentionDirectory before;
	ClusterCanonicalTxnBinding receipt;

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	before = *directory;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
	/* A conflict in the later pin must leave the earlier unbound pin untouched. */
	directory->pins[0].has_tt_binding = false;
	memset(&directory->pins[0].binding, 0, sizeof(directory->pins[0].binding));
	before = *directory;
	relmap_binding.slot_offset++;
	receipt = relmap_binding;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_CONFLICT);
	UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
	UT_ASSERT_EQ(memcmp(&receipt, &relmap_binding, sizeof(receipt)), 0);
	UT_ASSERT_EQ(relmap_assign_calls, 0);
}

UT_TEST(test_relmap_runtime_active_empty_set_has_no_new_duty)
{
	ClusterRelmapRetentionDirectory before;
	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	before = relmap_runtime_shmem->directories[0];
	relmap_global_x = false;
	relmap_active_owner = relmap_have_binding = true;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
	UT_ASSERT_EQ(relmap_assign_calls, 0);
}

UT_TEST(test_relmap_runtime_active_refuses_unattached_unknown_and_building)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapRetentionDirectory before;
		relmap_runtime_fixture(fault != 0);
		relmap_global_x = false;
		relmap_active_owner = relmap_have_binding = true;
		if (fault == 2)
			UT_ASSERT_EQ(cluster_relmap_retention_build_start(&relmap_runtime_shmem->directories[0],
				ref.claim.identity.system_identifier, ref.claim.database_incarnation, 0, 7,
				relmap_top_full - 100, relmap_top_full + 100), CLUSTER_RELMAP_RETENTION_OK);
		if (fault != 0) before = relmap_runtime_shmem->directories[0];
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		if (fault != 0)
			UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_binding_reads, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(relmap_retention_acquires, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(relmap_assign_calls, 0);
	}
}

UT_TEST(test_relmap_runtime_active_refuses_unsafe_context_before_receipt_read)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
		ClusterRelmapRetentionDirectory before = *directory;
		if (fault == 0) cluster_enabled = false;
		if (fault == 1) cluster_shared_config = false;
		if (fault == 2) cluster_node_id = -1;
		if (fault == 3) cluster_node_id = CLUSTER_MAX_NODES;
		if (fault == 4) MyBackendType = B_BG_WRITER;
		if (fault == 5) MyProcPid = 0;
		if (fault == 6) CritSectionCount = 1;
		if (fault == 7) relmap_in_transaction = false;
		if (fault == 8) ShutdownRequestPending = true;
		if (fault == 9) flush_probe_recovery = true;
		if (fault == 10) relmap_local_lock = true;
		if (fault == 11) relmap_retention_held = true;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_binding_reads + relmap_retention_acquires + relmap_assign_calls, 0);
	}
}

UT_TEST(test_relmap_runtime_active_refuses_invalid_top_or_missing_and_invalid_receipt)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
		ClusterRelmapRetentionDirectory before = *directory;
		ClusterCanonicalTxnBinding receipt;
		if (fault == 0) relmap_top_full = 0;
		if (fault == 1) relmap_top_full = UINT64CONST(0x400000002);
		if (fault == 2) relmap_have_binding = false;
		if (fault == 3) relmap_binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHING;
		if (fault == 4) relmap_binding.xid++;
		if (fault == 5) relmap_binding.origin_instance++;
		if (fault == 6) relmap_binding.segment_id = 0;
		if (fault == 7) relmap_binding.segment_generation = UINT32_MAX;
		if (fault == 8) relmap_binding.slot_wrap = TT_WRAP_INVALID;
		if (fault == 9) relmap_binding.active_lsn = InvalidXLogRecPtr;
		if (fault == 10) relmap_binding.reserved16 = 1;
		receipt = relmap_binding;
		UT_ASSERT_NE(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
		UT_ASSERT_EQ(memcmp(&receipt, &relmap_binding, sizeof(receipt)), 0);
		UT_ASSERT_EQ(relmap_assign_calls, 0);
		UT_ASSERT_EQ(relmap_binding_reads, fault < 2 ? 0 : 1);
		UT_ASSERT_EQ(relmap_retention_acquires, fault < 3 ? 0 : 1);
	}
}

UT_TEST(test_relmap_runtime_active_rechecks_original_full_and_native_writer_after_receipt)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
		ClusterRelmapRetentionDirectory before = *directory;
		if (fault == 0) relmap_binding_drift = true;
		else relmap_binding_owner_fault = fault;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_binding_reads, 1);
		UT_ASSERT_EQ(relmap_retention_acquires + relmap_assign_calls, 0);
	}
}

UT_TEST(test_relmap_runtime_active_same_raw_in_another_era_never_attaches_old_pins)
{
	ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
	ClusterRelmapRetentionDirectory before = *directory;
	relmap_top_full += UINT64CONST(0x100000000);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
	UT_ASSERT_EQ(relmap_binding_reads, 1);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
	UT_ASSERT_EQ(relmap_assign_calls, 0);
}

UT_TEST(test_relmap_runtime_active_refuses_a_foreign_directory_identity)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
		ClusterRelmapRetentionDirectory before;
		if (fault == 0) directory->system_identifier++;
		if (fault == 1) directory->database_incarnation++;
		if (fault == 2) directory->origin++;
		before = *directory;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
		UT_ASSERT_EQ(relmap_binding_reads, 1);
		UT_ASSERT_EQ(relmap_retention_acquires, 1);
		UT_ASSERT_EQ(relmap_assign_calls, 0);
	}
}
