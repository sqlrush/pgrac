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
	MyAuxProcType = NotAnAuxProcess;
	relmap_xidgen_held = false;
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

static ClusterRelmapRetentionRuntimeTruncateV2
relmap_runtime_truncate_receipt(unsigned origin)
{
	ClusterRelmapRetentionRuntimeTruncateV2 receipt = { 0 };

	receipt.system_identifier = ref.claim.identity.system_identifier;
	receipt.database_incarnation = ref.claim.database_incarnation;
	receipt.origin = origin;
	return receipt;
}

/* Refusals must preserve every origin and the caller's last usable receipt,
 * including a second begin using the original output variable while BUSY. */
static void
relmap_runtime_truncate_refusal(unsigned operation,
	ClusterRelmapRetentionRuntimeTruncateV2 *receipt, uint64 cutoff,
	ClusterRelmapRetentionResult expected)
{
	Size bytes = sizeof(relmap_runtime_shmem->directories);
	void *before = relmap_runtime_shmem == NULL ? NULL : malloc(bytes);
	ClusterRelmapRetentionRuntimeTruncateV2 saved = { 0 };
	ClusterRelmapRetentionResult result;
	unsigned reads = relmap_binding_reads, assignments = relmap_assign_calls;
	unsigned stages = relmap_runtime_stage_calls;
	int syncs = sync_calls, renames = rename_calls;
	bool held = relmap_retention_held;

	if (before != NULL)
		memcpy(before, relmap_runtime_shmem->directories, bytes);
	else
		UT_ASSERT(relmap_runtime_shmem == NULL);
	if (receipt != NULL)
		saved = *receipt;
	if (operation == 0)
		result = cluster_relmap_retention_runtime_truncate_begin_v2(saved.system_identifier,
			saved.database_incarnation, saved.origin, cutoff, receipt);
	else if (operation == 1)
		result = cluster_relmap_retention_runtime_truncate_complete_v2(receipt);
	else
		result = cluster_relmap_retention_runtime_truncate_uncertain_v2(receipt);
	UT_ASSERT_EQ(result, expected);
	if (before != NULL) {
		UT_ASSERT_EQ(memcmp(before, relmap_runtime_shmem->directories, bytes), 0);
		free(before);
	}
	if (receipt != NULL)
		UT_ASSERT_EQ(memcmp(&saved, receipt, sizeof(saved)), 0);
	UT_ASSERT_EQ(relmap_binding_reads, reads);
	UT_ASSERT_EQ(relmap_assign_calls, assignments);
	UT_ASSERT_EQ(relmap_runtime_stage_calls, stages);
	UT_ASSERT_EQ(sync_calls, syncs);
	UT_ASSERT_EQ(rename_calls, renames);
	UT_ASSERT_EQ(relmap_retention_held, held);
}

static void
relmap_runtime_truncate_quiet(void)
{
	UT_ASSERT(!relmap_retention_held);
	UT_ASSERT_EQ(relmap_binding_reads + relmap_assign_calls, 0);
	UT_ASSERT_EQ(relmap_runtime_stage_calls + relmap_runtime_stage_writes, 0);
	UT_ASSERT_EQ(sync_calls + rename_calls, 0);
}

UT_TEST(test_relmap_runtime_truncate_routes_original_origin_and_completes_exact_cut)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1) {
		ClusterRelmapRetentionRuntimeTruncateV2 receipt = { 0 }, saved;
		ClusterRelmapRetentionDirectory *directory, before;
		Size bytes;
		void *others;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(origin);
		directory = &relmap_runtime_shmem->directories[origin];
		before = *directory;
		bytes = sizeof(relmap_runtime_shmem->directories);
		others = malloc(bytes);
		UT_ASSERT_NOT_NULL(others);
		memcpy(others, relmap_runtime_shmem->directories, bytes);
		cluster_node_id = -1; /* Never select an implicit current-node directory. */
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(
			before.system_identifier, before.database_incarnation, origin,
			before.last_issued + 1, &receipt), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(receipt.origin, origin);
		UT_ASSERT_EQ(receipt.system_identifier, before.system_identifier);
		UT_ASSERT_EQ(receipt.database_incarnation, before.database_incarnation);
		UT_ASSERT_EQ(receipt.reservation.cutoff, before.last_issued + 1);
		UT_ASSERT_EQ(receipt.reservation.generation, before.generation);
		UT_ASSERT_EQ(receipt.reservation.serial, 1);
		UT_ASSERT_EQ(directory->native_floor, before.native_floor);
		UT_ASSERT_EQ(relmap_retention_acquires, 1);
		saved = receipt;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_complete_v2(&receipt),
			CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(directory->native_floor, before.last_issued + 1);
		UT_ASSERT_EQ(directory->inflight.serial, 0);
		UT_ASSERT_EQ(directory->state, CLUSTER_RELMAP_RETENTION_COMPLETE);
		UT_ASSERT_EQ(memcmp(directory->pins, before.pins, sizeof(before.pins)), 0);
		UT_ASSERT_EQ(memcmp(&receipt, &saved, sizeof(saved)), 0);
		/* Ignore only the selected origin when checking foreign directories. */
		memcpy((char *) others + origin * sizeof(*directory), directory, sizeof(*directory));
		UT_ASSERT_EQ(memcmp(others, relmap_runtime_shmem->directories, bytes), 0);
		free(others);
		UT_ASSERT_EQ(relmap_retention_acquires, 2);
		relmap_runtime_truncate_quiet();
	}
}

UT_TEST(test_relmap_runtime_truncate_clamps_to_each_staged_or_unstaged_namespace)
{
	for (unsigned selected = 0; selected < 2; selected++) {
		ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(CLUSTER_MAX_NODES - 1);
		ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];
		ClusterRelmapRetentionRuntimeTruncateV2 receipt = relmap_runtime_truncate_receipt(directory->origin);

		directory->pins[1 - selected].occupied = false;
		memcpy(pins, directory->pins, sizeof(pins));
		cluster_node_id = 0; /* An empty local origin cannot replace this one. */
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(receipt.system_identifier,
			receipt.database_incarnation, receipt.origin, relmap_top_full + 20, &receipt),
			CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(receipt.reservation.cutoff, relmap_top_full);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_complete_v2(&receipt),
			CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(directory->native_floor, relmap_top_full);
		UT_ASSERT_EQ(memcmp(pins, directory->pins, sizeof(pins)), 0);
		UT_ASSERT_EQ(relmap_retention_acquires, 2);
		relmap_runtime_truncate_quiet();
	}
}

UT_TEST(test_relmap_runtime_truncate_reserves_against_original_begin_and_busy_retry)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	ClusterRelmapRetentionRuntimeTruncateV2 receipt;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	receipt = relmap_runtime_truncate_receipt(0);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(receipt.system_identifier,
		receipt.database_incarnation, 0, relmap_top_full + 10, &receipt), CLUSTER_RELMAP_RETENTION_OK);
	relmap_runtime_truncate_refusal(0, &receipt, relmap_top_full + 20, CLUSTER_RELMAP_RETENTION_BUSY);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_NATIVE_GONE);
	relmap_top_full += 10; /* Equality with the reserved first-retained cut is safe. */
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_complete_v2(&receipt), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(relmap_runtime_shmem->directories[0].pins[0].occupied);
	UT_ASSERT(!relmap_retention_held);
}

UT_TEST(test_relmap_runtime_truncate_uncertain_preserves_reservation_and_pins_on_attach)
{
	ClusterRelmapRetentionDirectory *directory = relmap_runtime_binding_fixture(0);
	ClusterRelmapRetentionDirectory before;
	ClusterRelmapRetentionRuntimeTruncateV2 receipt = relmap_runtime_truncate_receipt(0);

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(receipt.system_identifier,
		receipt.database_incarnation, 0, relmap_top_full + 20, &receipt), CLUSTER_RELMAP_RETENTION_OK);
	before = *directory;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_uncertain_v2(&receipt), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(directory->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
	before.state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
	relmap_runtime_shmem = NULL;
	relmap_runtime_registered->init_fn();
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	for (unsigned operation = 0; operation < 3; operation++)
		relmap_runtime_truncate_refusal(operation, &receipt, relmap_top_full + 20,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	relmap_runtime_truncate_quiet();
}

UT_TEST(test_relmap_runtime_truncate_rejects_stale_or_changed_receipts)
{
	ClusterRelmapRetentionRuntimeTruncateV2 receipt, stale, changed;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	receipt = relmap_runtime_truncate_receipt(0);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(receipt.system_identifier,
		receipt.database_incarnation, 0, relmap_top_full, &receipt), CLUSTER_RELMAP_RETENTION_OK);
	stale = receipt;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_complete_v2(&receipt), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(receipt.system_identifier,
		receipt.database_incarnation, 0, relmap_top_full, &receipt), CLUSTER_RELMAP_RETENTION_OK);
	for (unsigned operation = 1; operation < 3; operation++) {
		relmap_runtime_truncate_refusal(operation, &stale, 0, CLUSTER_RELMAP_RETENTION_CONFLICT);
		for (unsigned field = 0; field < 4; field++) {
			changed = receipt;
			if (field == 0) changed.reservation.generation++;
			if (field == 1) changed.reservation.serial++;
			if (field == 2) changed.reservation.cutoff++;
			if (field == 3) changed.reservation.serial = 0;
			relmap_runtime_truncate_refusal(operation, &changed, 0, CLUSTER_RELMAP_RETENTION_CONFLICT);
		}
	}
	relmap_runtime_truncate_quiet();
}

UT_TEST(test_relmap_runtime_truncate_refuses_unattached_unknown_and_building)
{
	for (unsigned state = 0; state < 3; state++) {
		ClusterRelmapRetentionRuntimeTruncateV2 receipt;

		relmap_runtime_fixture(state != 0);
		if (state == 2)
			relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_BUILDING;
		receipt = relmap_runtime_truncate_receipt(0);
		for (unsigned operation = 0; operation < 3; operation++)
			relmap_runtime_truncate_refusal(operation, &receipt, relmap_top_full,
				CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(relmap_retention_acquires, state == 0 ? 0 : 3);
	}
}

UT_TEST(test_relmap_runtime_truncate_checks_identity_origin_and_null_before_indexing)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapRetentionRuntimeTruncateV2 receipt;

		relmap_runtime_fixture(true);
		receipt = relmap_runtime_truncate_receipt(0);
		if (fault == 0) receipt.system_identifier = 0;
		if (fault == 1) receipt.database_incarnation = 0;
		if (fault == 2) receipt.origin = CLUSTER_MAX_NODES;
		if (fault == 3) receipt.origin = UINT16_MAX;
		for (unsigned operation = 0; operation < 3; operation++)
			relmap_runtime_truncate_refusal(operation, fault == 4 ? NULL : &receipt,
				relmap_top_full, CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(relmap_retention_acquires, 0);
	}
}

UT_TEST(test_relmap_runtime_truncate_rejects_foreign_directory_or_receipt_identity)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionRuntimeTruncateV2 receipt;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		directory = &relmap_runtime_shmem->directories[0];
		receipt = relmap_runtime_truncate_receipt(0);
		if (fault == 0) directory->origin++;
		if (fault == 1) directory->system_identifier++;
		if (fault == 2) directory->database_incarnation++;
		if (fault == 3) receipt.system_identifier++;
		if (fault == 4) receipt.database_incarnation++;
		for (unsigned operation = 0; operation < 3; operation++)
			relmap_runtime_truncate_refusal(operation, &receipt, relmap_top_full,
				CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(relmap_retention_acquires, 3);
	}
}

UT_TEST(test_relmap_runtime_truncate_preserves_core_cut_and_serial_refusals)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionRuntimeTruncateV2 receipt;
		uint64 cutoff;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		directory = &relmap_runtime_shmem->directories[0];
		receipt = relmap_runtime_truncate_receipt(0);
		cutoff = relmap_top_full;
		if (fault == 0) cutoff = directory->native_floor - 1;
		if (fault == 1) cutoff = directory->last_issued + 2;
		if (fault == 2) cutoff = UINT64CONST(0x500000001); /* Special raw xid in a later era. */
		if (fault == 3) cutoff = PG_UINT64_MAX;
		if (fault == 4) directory->truncate_serial = PG_UINT64_MAX;
		relmap_runtime_truncate_refusal(0, &receipt, cutoff, fault == 4
			? CLUSTER_RELMAP_RETENTION_UNAVAILABLE : CLUSTER_RELMAP_RETENTION_INVALID);
	}
}

UT_TEST(test_relmap_runtime_truncate_never_reacquires_or_releases_callers_lock)
{
	ClusterRelmapRetentionRuntimeTruncateV2 receipt;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	receipt = relmap_runtime_truncate_receipt(0);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	for (unsigned operation = 0; operation < 3; operation++)
		relmap_runtime_truncate_refusal(operation, &receipt, relmap_top_full,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
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

/* Candidate inputs and nofail native publication below are controlled models,
 * not certified origin floors/issuance or concurrent native allocator tests. */
static void
relmap_runtime_allocation_count(void *argument)
{
	unsigned *calls = argument;
	UT_ASSERT(LWLockHeldByMeInMode(XidGenLock, LW_EXCLUSIVE));
	UT_ASSERT(LWLockHeldByMeInMode(relmap_runtime_test_lock, LW_EXCLUSIVE));
	(*calls)++;
}

static ClusterRelmapRetentionResult
relmap_runtime_allocation_call(bool publish, uint64 sysid, uint64 dbinc,
	uint16 origin, uint64 candidate)
{
	unsigned calls = 0;
	ClusterRelmapRetentionResult result = publish
		? cluster_relmap_retention_runtime_allocation_publish_v2(sysid, dbinc, origin,
			candidate, relmap_runtime_allocation_count, &calls)
		: cluster_relmap_retention_runtime_allocation_check_v2(sysid, dbinc, origin, candidate);
	UT_ASSERT_EQ(calls, publish && result == CLUSTER_RELMAP_RETENTION_OK ? 1 : 0);
	return result;
}

/* Stores and fault injection are controlled; the native allocator is not called. */
typedef struct RelmapRuntimePublication {
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	uint64 candidate, native_next;
	TransactionId proc_xid;
	unsigned calls;
	unsigned fault;
	bool probes;
} RelmapRuntimePublication;

static void
relmap_runtime_allocation_stores(void *argument)
{
	RelmapRuntimePublication *publication = argument;
	ClusterRelmapRetentionRuntimeTruncateV2 receipt = { 0 };
	unsigned reads = relmap_binding_reads, acquisitions = relmap_retention_acquires;

	UT_ASSERT(relmap_retention_held && relmap_xidgen_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 2);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, publication->candidate);
	publication->calls++;
	if (publication->fault == 1)
		ereport(ERROR, (errmsg("controlled failure before native stores")));
	if (publication->probes) {
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&publication->sample,
			&publication->identity, 9), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(
			publication->identity.system_identifier, publication->identity.database_incarnation,
			0, publication->candidate, &receipt), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		for (unsigned publish = 0; publish < 2; publish++)
			UT_ASSERT_EQ(relmap_runtime_allocation_call(publish,
				publication->identity.system_identifier, publication->identity.database_incarnation,
				0, publication->candidate + 1), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(relmap_binding_reads, reads);
		UT_ASSERT_EQ(relmap_retention_acquires, acquisitions);
	}
	/* A real caller supplies only these original nofail stores, without probes. */
	publication->native_next = publication->candidate + 1;
	publication->proc_xid = (uint32) publication->candidate;
	if (publication->fault == 2)
		ereport(ERROR, (errmsg("controlled failure after native stores")));
}

static void
relmap_runtime_allocation_unchanged(bool begin, uint64 sysid, uint64 dbinc,
	uint16 origin, uint64 candidate, ClusterRelmapRetentionResult expected)
{
	Size bytes = sizeof(relmap_runtime_shmem->directories);
	void *before = relmap_runtime_shmem == NULL ? NULL : malloc(bytes);
	unsigned reads = relmap_binding_reads, assignments = relmap_assign_calls;
	unsigned stages = relmap_runtime_stage_calls;
	int syncs = sync_calls, renames = rename_calls;
	bool held = relmap_retention_held, xid_held = relmap_xidgen_held;

	UT_ASSERT(before != NULL || relmap_runtime_shmem == NULL);
	if (before != NULL)
		memcpy(before, relmap_runtime_shmem->directories, bytes);
	UT_ASSERT_EQ(relmap_runtime_allocation_call(begin, sysid, dbinc, origin, candidate), expected);
	if (before != NULL) {
		UT_ASSERT_EQ(memcmp(before, relmap_runtime_shmem->directories, bytes), 0);
		free(before);
	}
	UT_ASSERT_EQ(relmap_retention_held, held);
	UT_ASSERT_EQ(relmap_xidgen_held, xid_held);
	UT_ASSERT_EQ(relmap_binding_reads, reads);
	UT_ASSERT_EQ(relmap_assign_calls, assignments);
	UT_ASSERT_EQ(relmap_runtime_stage_calls, stages);
	UT_ASSERT_EQ(sync_calls, syncs);
	UT_ASSERT_EQ(rename_calls, renames);
}

UT_TEST(test_relmap_runtime_allocation_snapshot_routes_original_origin_without_mutation)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1) {
		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(origin);
		cluster_node_id = origin == 0 ? CLUSTER_MAX_NODES - 1 : 0;
		relmap_global_x = false; /* Allocation must not require a relmap owner query. */
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		relmap_runtime_allocation_unchanged(false, ref.claim.identity.system_identifier,
			ref.claim.database_incarnation, origin, relmap_top_full + 101, CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(relmap_retention_acquires, 1);
		LWLockRelease(XidGenLock);
	}
}

UT_TEST(test_relmap_runtime_allocation_holds_final_window_until_original_stores)
{
	RelmapRuntimePublication publication = { 0 };
	ClusterRelmapRetentionDirectory before;
	unsigned reads;

	relmap_runtime_prepare_stage(&publication.sample, &publication.identity, 0, true);
	before = relmap_runtime_shmem->directories[0];
	publication.candidate = before.last_issued + 1;
	publication.probes = true;
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	reads = relmap_binding_reads;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_allocation_publish_v2(
		publication.identity.system_identifier, publication.identity.database_incarnation,
		0, publication.candidate, relmap_runtime_allocation_stores, &publication),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(!relmap_retention_held && relmap_xidgen_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 1);
	before.last_issued = publication.candidate;
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT_EQ(publication.calls, 1);
	UT_ASSERT_EQ(publication.native_next, publication.candidate + 1);
	UT_ASSERT_EQ(publication.proc_xid, (uint32) publication.candidate);
	UT_ASSERT_EQ(relmap_binding_reads, reads);
	relmap_runtime_allocation_unchanged(true, publication.identity.system_identifier,
		publication.identity.database_incarnation, 0, publication.candidate,
		CLUSTER_RELMAP_RETENTION_INVALID);
	LWLockRelease(XidGenLock);
}

UT_TEST(test_relmap_runtime_allocation_checks_each_namespace_and_stage_halfspace)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned staged = 0; staged < 2; staged++) {
			ClusterRelmapOwnerSampleV2 sample;
			ClusterRelmapV2Namespace identity;
			uint64 edge;

			relmap_runtime_prepare_stage(&sample, &identity, 0, shared);
			if (staged)
				UT_ASSERT(cluster_relmap_retention_runtime_write_pending_v2(
					&sample, &identity, 8, 456, &relmap_runtime_stage_map));
			edge = sample.owner.top_full_xid + UINT64CONST(0x80000000);
			LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
			relmap_runtime_allocation_unchanged(false, identity.system_identifier,
				identity.database_incarnation, 0, edge - 1, CLUSTER_RELMAP_RETENTION_OK);
			for (unsigned begin = 0; begin < 2; begin++)
				relmap_runtime_allocation_unchanged(begin, identity.system_identifier,
					identity.database_incarnation, 0, edge, CLUSTER_RELMAP_RETENTION_XID_WINDOW);
			UT_ASSERT_EQ(relmap_runtime_allocation_call(true, identity.system_identifier,
				identity.database_incarnation, 0, edge - 1), CLUSTER_RELMAP_RETENTION_OK);
			LWLockRelease(XidGenLock);
		}
}

UT_TEST(test_relmap_runtime_allocation_rechecks_changed_pin_and_final_candidate)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	uint64 edge;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	UT_ASSERT(cluster_relmap_owner_capture_v2(&sample));
	identity = relmap_owner_namespace();
	edge = sample.owner.top_full_xid + UINT64CONST(0x80000000);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	relmap_runtime_allocation_unchanged(false, identity.system_identifier,
		identity.database_incarnation, 0, edge, CLUSTER_RELMAP_RETENTION_OK);
	/* Deterministic interleaving: an existing old top adds its pin after check. */
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	relmap_runtime_allocation_unchanged(true, identity.system_identifier,
		identity.database_incarnation, 0, edge, CLUSTER_RELMAP_RETENTION_XID_WINDOW);
	/* A new candidate after stripe/herding recomputation also needs final check. */
	relmap_runtime_allocation_unchanged(false, identity.system_identifier,
		identity.database_incarnation, 0, edge - 1, CLUSTER_RELMAP_RETENTION_OK);
	relmap_runtime_allocation_unchanged(true, identity.system_identifier,
		identity.database_incarnation, 0, edge + 1, CLUSTER_RELMAP_RETENTION_XID_WINDOW);
	LWLockRelease(XidGenLock);
}

UT_TEST(test_relmap_runtime_allocation_refuses_unattached_unknown_and_building)
{
	for (unsigned state = 0; state < 3; state++) {
		relmap_runtime_fixture(state != 0);
		if (state == 2)
			relmap_runtime_shmem->directories[0].state = CLUSTER_RELMAP_RETENTION_BUILDING;
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		for (unsigned begin = 0; begin < 2; begin++)
			relmap_runtime_allocation_unchanged(begin, ref.claim.identity.system_identifier,
				ref.claim.database_incarnation, 0, relmap_top_full + 101,
				CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(relmap_retention_acquires, state == 0 ? 0 : 2);
		LWLockRelease(XidGenLock);
	}
}

UT_TEST(test_relmap_runtime_allocation_rejects_identity_and_origin_before_indexing)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		uint64 sysid, dbinc;
		uint16 origin = 0;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		sysid = ref.claim.identity.system_identifier;
		dbinc = ref.claim.database_incarnation;
		if (fault == 0) sysid = 0;
		if (fault == 1) dbinc = 0;
		if (fault == 2) origin = CLUSTER_MAX_NODES;
		if (fault == 3) origin = UINT16_MAX;
		if (fault == 4) relmap_runtime_shmem->directories[0].origin++;
		if (fault == 5) relmap_runtime_shmem->directories[0].system_identifier++;
		if (fault == 6) relmap_runtime_shmem->directories[0].database_incarnation++;
		if (fault == 7) sysid++;
		if (fault == 8) dbinc++;
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		for (unsigned begin = 0; begin < 2; begin++)
			relmap_runtime_allocation_unchanged(begin, sysid, dbinc, origin,
				relmap_top_full + 101, CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(relmap_retention_acquires, fault < 4 ? 0 : 2);
		LWLockRelease(XidGenLock);
	}
}

UT_TEST(test_relmap_runtime_allocation_preserves_core_candidate_refusals)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRelmapRetentionDirectory *directory;
		uint64 candidate;

		relmap_runtime_fixture(true);
		relmap_runtime_qualified_fixture(0);
		directory = &relmap_runtime_shmem->directories[0];
		candidate = directory->last_issued + 1;
		if (fault == 0) candidate = 0;
		if (fault == 1) candidate = UINT64CONST(0x500000002);
		if (fault == 2) candidate = PG_UINT64_MAX;
		if (fault == 3) candidate = directory->last_issued;
		if (fault == 4) candidate = directory->last_issued - 1;
		if (fault == 5) directory->native_floor = candidate + 1;
		if (fault == 6) {
			/* Overflow fixture: never wrap the pin's upper bound to a small era. */
			directory->native_floor = PG_UINT64_MAX - 300;
			directory->last_issued = PG_UINT64_MAX - 200;
			directory->pins[0].occupied = true;
			directory->pins[0].key.top_full_xid = PG_UINT64_MAX - 150;
			candidate = PG_UINT64_MAX - 1;
		}
		if (fault == 7) candidate = UINT64CONST(0x500000000);
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		for (unsigned begin = 0; begin < 2; begin++)
			relmap_runtime_allocation_unchanged(begin, ref.claim.identity.system_identifier,
				ref.claim.database_incarnation, 0, candidate, fault == 6
					? CLUSTER_RELMAP_RETENTION_XID_WINDOW : CLUSTER_RELMAP_RETENTION_INVALID);
		LWLockRelease(XidGenLock);
	}
}

UT_TEST(test_relmap_runtime_allocation_requires_original_exclusive_lock_and_own_window)
{
	uint64 sysid, dbinc, candidate;

	relmap_runtime_fixture(true);
	relmap_runtime_qualified_fixture(0);
	sysid = ref.claim.identity.system_identifier;
	dbinc = ref.claim.database_incarnation;
	candidate = relmap_top_full + 101;
	for (unsigned mode = 0; mode < 2; mode++) {
		if (mode == 1) LWLockAcquire(XidGenLock, LW_SHARED);
		for (unsigned publish = 0; publish < 2; publish++)
			relmap_runtime_allocation_unchanged(publish, sysid, dbinc, 0, candidate,
				CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		if (mode == 1) LWLockRelease(XidGenLock);
	}
	UT_ASSERT_EQ(relmap_retention_acquires, 0);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	for (unsigned publish = 0; publish < 2; publish++)
		relmap_runtime_allocation_unchanged(publish, sysid, dbinc, 0, candidate,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT(relmap_retention_held && relmap_xidgen_held);
	LWLockRelease(relmap_runtime_test_lock);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_allocation_publish_v2(sysid, dbinc,
		0, candidate, NULL, NULL), CLUSTER_RELMAP_RETENTION_INVALID);
	UT_ASSERT(!relmap_retention_held && relmap_xidgen_held);
	UT_ASSERT_EQ(relmap_retention_acquires, 1);
	UT_ASSERT_EQ(relmap_runtime_allocation_call(true, sysid, dbinc, 0, candidate),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(relmap_retention_acquires, 2);
	LWLockRelease(XidGenLock);
}

static void
relmap_runtime_allocation_inject_error(RelmapRuntimePublication *publication)
{
	volatile bool caught = false;
	expect_panic = true;
	PG_TRY();
	{
		(void) cluster_relmap_retention_runtime_allocation_publish_v2(
			publication->identity.system_identifier, publication->identity.database_incarnation,
			0, publication->candidate, relmap_runtime_allocation_stores, publication);
		UT_ASSERT(false);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expect_panic = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT(PG_exception_stack == NULL);
	UT_ASSERT(!relmap_retention_held && relmap_xidgen_held);
	UT_ASSERT_EQ(publication->calls, 1);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, publication->candidate);
}

UT_TEST(test_relmap_runtime_allocation_error_cleanup_never_rewinds_observation)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		RelmapRuntimePublication publication = { 0 };

		relmap_runtime_prepare_stage(&publication.sample, &publication.identity, 0, true);
		publication.candidate = relmap_runtime_shmem->directories[0].last_issued + 1;
		publication.fault = fault;
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		relmap_runtime_allocation_inject_error(&publication);
		UT_ASSERT_EQ(publication.native_next, fault == 1 ? 0 : publication.candidate + 1);
		UT_ASSERT_EQ(publication.proc_xid, fault == 1 ? 0 : (uint32) publication.candidate);
		/* Match native LWLockReleaseAll: ERROR reset the holdoff count. */
		HOLD_INTERRUPTS();
		LWLockRelease(XidGenLock);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		relmap_runtime_allocation_unchanged(true, publication.identity.system_identifier,
			publication.identity.database_incarnation, 0, publication.candidate,
			CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT_EQ(relmap_runtime_allocation_call(true, publication.identity.system_identifier,
			publication.identity.database_incarnation, 0, publication.candidate + 1),
			CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[0].last_issued, publication.candidate + 1);
		LWLockRelease(XidGenLock);
	}
}

UT_TEST(test_relmap_runtime_allocation_error_reacquire_cannot_release_an_unrelated_lock)
{
	RelmapRuntimePublication publication = { 0 };
	ClusterRelmapRetentionDirectory before;

	relmap_runtime_prepare_stage(&publication.sample, &publication.identity, 0, true);
	publication.candidate = relmap_runtime_shmem->directories[0].last_issued + 1;
	publication.fault = 1;
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	relmap_runtime_allocation_inject_error(&publication);
	HOLD_INTERRUPTS(); /* Original LWLockReleaseAll owns the remaining XidGenLock. */
	LWLockRelease(XidGenLock);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	before = relmap_runtime_shmem->directories[0];
	/* No end/marker survives ERROR; neither adapter can release this acquisition. */
	for (unsigned publish = 0; publish < 2; publish++)
		relmap_runtime_allocation_unchanged(publish, publication.identity.system_identifier,
			publication.identity.database_incarnation, 0, publication.candidate + 1,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT(relmap_retention_held && relmap_xidgen_held);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT_EQ(publication.calls, 1);
	LWLockRelease(relmap_runtime_test_lock);
	LWLockRelease(XidGenLock);
}


/* Compare all origins, including retained provenance, staged obligations and
 * reservations. Controlled providers also reject native queries under lock. */
static void
relmap_runtime_recovery_expect(bool accepted)
{
	Size bytes = sizeof(relmap_runtime_shmem->directories);
	ClusterRelmapRetentionDirectory *expected = malloc(bytes);
	unsigned acquires = relmap_retention_acquires;
	unsigned reads = relmap_binding_reads, assignments = relmap_assign_calls;
	unsigned stages = relmap_runtime_stage_calls, writes = relmap_runtime_stage_writes;
	int syncs = sync_calls, renames = rename_calls;
	int holdoff = InterruptHoldoffCount;
	bool held = relmap_retention_held, xidgen = relmap_xidgen_held;

	UT_ASSERT_NOT_NULL(expected);
	memcpy(expected, relmap_runtime_shmem->directories, bytes);
	if (accepted)
		for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin++)
			expected[origin].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_recovery_invalidate_v2(), accepted);
	UT_ASSERT_EQ(memcmp(expected, relmap_runtime_shmem->directories, bytes), 0);
	UT_ASSERT_EQ(relmap_retention_acquires, acquires + (accepted ? 1 : 0));
	UT_ASSERT_EQ(relmap_binding_reads, reads);
	UT_ASSERT_EQ(relmap_assign_calls, assignments);
	UT_ASSERT_EQ(relmap_runtime_stage_calls, stages);
	UT_ASSERT_EQ(relmap_runtime_stage_writes, writes);
	UT_ASSERT_EQ(sync_calls, syncs);
	UT_ASSERT_EQ(rename_calls, renames);
	UT_ASSERT_EQ(relmap_retention_held, held);
	UT_ASSERT_EQ(relmap_xidgen_held, xidgen);
	UT_ASSERT_EQ(InterruptHoldoffCount, holdoff);
	free(expected);
}

UT_TEST(test_relmap_runtime_recovery_invalidates_every_origin_preserving_obligations)
{
	ClusterRelmapRetentionDirectory *directory =
		relmap_runtime_binding_fixture(CLUSTER_MAX_NODES - 1);

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(directory->pins[0].may_be_staged && directory->pins[0].has_tt_binding);
	UT_ASSERT(directory->pins[1].occupied && directory->pins[1].has_tt_binding);
	UT_ASSERT(directory->pins[2].occupied && !directory->pins[2].has_tt_binding);
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES - 1; origin++) {
		relmap_runtime_qualified_fixture(origin);
		/* Existing incomplete origins are revoked too, never treated as empty. */
		relmap_runtime_shmem->directories[origin].state = origin % 3;
	}
	MyAuxProcType = StartupProcess;
	MyBackendType = B_STARTUP;
	cluster_node_id = -1; /* Every origin, without an ordinary backend owner. */
	relmap_global_x = relmap_active_owner = relmap_in_transaction = false;
	have_ref = relmap_have_binding = false;
	relmap_runtime_recovery_expect(true);
}

UT_TEST(test_relmap_runtime_recovery_keeps_inflight_across_retry_and_attach)
{
	ClusterRelmapRetentionDirectory *directory =
		relmap_runtime_binding_fixture(CLUSTER_MAX_NODES - 1);
	ClusterRelmapRetentionRuntimeTruncateV2 receipt =
		relmap_runtime_truncate_receipt(directory->origin);
	ClusterRelmapRetentionDirectory before;

	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_truncate_begin_v2(receipt.system_identifier,
		receipt.database_incarnation, receipt.origin, relmap_top_full + 20, &receipt),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_NE(directory->inflight.serial, 0);
	before = *directory;
	before.state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	MyAuxProcType = StartupProcess;
	relmap_runtime_recovery_expect(true);
	relmap_runtime_recovery_expect(true);
	relmap_runtime_shmem = NULL;
	relmap_runtime_registered->init_fn();
	UT_ASSERT_EQ(memcmp(&before,
		&relmap_runtime_shmem->directories[CLUSTER_MAX_NODES - 1], sizeof(before)), 0);
	UT_ASSERT_EQ(relmap_runtime_lock_inits, 1);
	for (unsigned operation = 0; operation < 3; operation++)
		relmap_runtime_truncate_refusal(operation, &receipt, relmap_top_full + 20,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
}

UT_TEST(test_relmap_runtime_recovery_refuses_before_attachment)
{
	relmap_runtime_fixture(false);
	MyAuxProcType = StartupProcess;
	UT_ASSERT(!cluster_relmap_retention_runtime_recovery_invalidate_v2());
	UT_ASSERT_NULL(relmap_runtime_shmem);
	UT_ASSERT_NULL(relmap_runtime_allocation);
	UT_ASSERT_EQ(relmap_retention_acquires + relmap_runtime_new_tranches, 0);
	/* A fresh attached UNKNOWN registry is safe to revoke, not to certify. */
	relmap_runtime_registered->init_fn();
	relmap_runtime_recovery_expect(true);
}

UT_TEST(test_relmap_runtime_recovery_refuses_other_roles_and_unsafe_locks)
{
	relmap_runtime_binding_fixture(0);
	for (int role = NotAnAuxProcess; role < NUM_AUXPROCTYPES; role++) {
		if (role == StartupProcess)
			continue;
		MyAuxProcType = (AuxProcType) role;
		MyBackendType = B_STARTUP; /* This label alone is not native Startup. */
		relmap_runtime_recovery_expect(false);
	}
	MyBackendType = B_BACKEND;
	MyAuxProcType = StartupProcess;
	CritSectionCount = 1;
	relmap_runtime_recovery_expect(false);
	CritSectionCount = 0;
	ShutdownRequestPending = true;
	relmap_runtime_recovery_expect(false);
	ShutdownRequestPending = false;
	relmap_local_lock = true;
	relmap_runtime_recovery_expect(false);
	UT_ASSERT(relmap_local_lock);
	relmap_local_lock = false;
	for (unsigned mode = 0; mode < 2; mode++) {
		LWLockAcquire(XidGenLock, mode == 0 ? LW_SHARED : LW_EXCLUSIVE);
		relmap_runtime_recovery_expect(false);
		LWLockRelease(XidGenLock);
	}
	LWLockAcquire(relmap_runtime_test_lock, LW_EXCLUSIVE);
	relmap_runtime_recovery_expect(false);
	LWLockRelease(relmap_runtime_test_lock);
}

UT_TEST(test_relmap_runtime_recovery_never_makes_old_consumers_available)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace identity;
	ClusterRelmapRetentionRuntimeTruncateV2 receipt;
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionDirectory before;

	relmap_runtime_prepare_stage(&sample, &identity, 0, true);
	relmap_active_owner = relmap_have_binding = true;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
	directory = &relmap_runtime_shmem->directories[0];
	before = *directory;
	before.state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	receipt = relmap_runtime_truncate_receipt(0);
	MyAuxProcType = StartupProcess;
	relmap_runtime_recovery_expect(true);
	MyAuxProcType = NotAnAuxProcess;
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT(!cluster_relmap_retention_runtime_write_pending_v2(
		&sample, &identity, 8, 456, &relmap_runtime_stage_map));
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &identity, 8),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	relmap_runtime_tt_expect(identity.system_identifier, identity.database_incarnation,
		&relmap_binding, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	relmap_runtime_truncate_refusal(0, &receipt, relmap_top_full + 20,
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	for (unsigned publish = 0; publish < 2; publish++)
		relmap_runtime_allocation_unchanged(publish, identity.system_identifier,
			identity.database_incarnation, 0, before.last_issued + 1,
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	LWLockRelease(XidGenLock);
	UT_ASSERT_EQ(memcmp(&before, directory, sizeof(before)), 0);
	UT_ASSERT_EQ(relmap_runtime_stage_calls + relmap_runtime_stage_writes, 0);
}
