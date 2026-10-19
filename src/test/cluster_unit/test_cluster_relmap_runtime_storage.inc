/* PGRAC: original owner -> shared runtime -> production authority -> real I/O.
 * Native qualification, transaction/TT, Shmem and serialized LWLocks are
 * controlled providers; COMPLETE/floor/issued below are fixtures, not proofs.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-11-pre2-p0-m5-e-relmap-retention.md, section3/R05/R07.
 */
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_shmem.h"
#include "postmaster/interrupt.h"
#include "storage/shmem.h"

bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id;
int MyProcPid = 79;
Oid MyDatabaseId = 42;
BackendType MyBackendType = B_BACKEND;
AuxProcType MyAuxProcType = NotAnAuxProcess;
volatile sig_atomic_t ShutdownRequestPending;
volatile uint32 CritSectionCount;
static LWLockPadded runtime_storage_native_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = runtime_storage_native_locks;

/* No allocation owner runs in this storage composition fixture. */
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return mode == LW_EXCLUSIVE && LWLockHeldByMe(lock);
}

static void *runtime_storage_allocation;
static const ClusterShmemRegion *runtime_storage_region;
static ClusterWalWriterToken runtime_storage_writer;
static uint64 runtime_storage_full_xid;
static unsigned runtime_storage_assignments, runtime_storage_binding_reads;

static void
runtime_storage_owner_read(void)
{
	if (retention_held)
		abort();
}

FullTransactionId
GetTopFullTransactionId(void)
{
	runtime_storage_owner_read();
	if (mapping_held || !relmap_x_held || CritSectionCount != 0)
		abort();
	runtime_storage_assignments++;
	return FullTransactionIdFromU64(runtime_storage_full_xid);
}

FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	runtime_storage_owner_read();
	return FullTransactionIdFromU64(runtime_storage_full_xid);
}

bool
IsTransactionState(void)
{
	runtime_storage_owner_read();
	return true;
}

bool
RecoveryInProgress(void)
{
	runtime_storage_owner_read();
	return false;
}

TimeLineID
GetWALInsertionTimeLine(void)
{
	runtime_storage_owner_read();
	return 1;
}

uint64
cluster_qvotec_get_self_incarnation(void)
{
	runtime_storage_owner_read();
	return owner.boot_incarnation;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 origin)
{
	runtime_storage_owner_read();
	return origin == cluster_node_id ? owner.admitted_incarnation : 0;
}

ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *out)
{
	runtime_storage_owner_read();
	if (timeline != 1)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	*out = runtime_storage_writer;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *sample)
{
	runtime_storage_owner_read();
	return memcmp(sample, &runtime_storage_writer, sizeof(*sample)) == 0
		? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
}

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	runtime_storage_owner_read();
	if (mapping_held || xid != (uint32) runtime_storage_full_xid)
		abort();
	runtime_storage_binding_reads++;
	memset(out, 0, sizeof(*out));
	return false; /* No binding is manufactured by this fixture. */
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	runtime_storage_region = region;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (!runtime_storage_region || strcmp(name, runtime_storage_region->name) != 0
		|| size != runtime_storage_region->size_fn() || retention_held)
		abort();
	*found = runtime_storage_allocation != NULL;
	if (!*found) {
		runtime_storage_allocation = malloc(size);
		if (!runtime_storage_allocation)
			abort();
		memset(runtime_storage_allocation, 0xa5, size);
	}
	return runtime_storage_allocation;
}

int
LWLockNewTrancheId(void)
{
	return 201;
}

void
LWLockInitialize(LWLock *lock, int tranche)
{
	if (tranche != 201 || runtime_test_lock != NULL)
		abort();
	memset(lock, 0, sizeof(*lock));
	lock->tranche = tranche;
	runtime_test_lock = lock;
}

void
LWLockRegisterTranche(int tranche, const char *name)
{
	if (tranche != 201 || strcmp(name, "ClusterRelmapRetention") != 0)
		abort();
}

/* No runtime or owner body copies, alternate stage provider or public writable
 * accessor: private state is visible only because this is one test TU. */
#include "../../backend/cluster/cluster_relmap_owner.c"
#include "../../backend/cluster/cluster_relmap_retention_runtime.c"

static void
runtime_storage_setup(ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapV2Namespace *ns, unsigned origin, bool shared)
{
	*ns = identity;
	ns->shared = shared;
	ns->dbid = shared ? InvalidOid : MyDatabaseId;
	setup(ns);
	free(runtime_storage_allocation);
	runtime_storage_allocation = NULL;
	runtime_test_lock = NULL;
	relmap_runtime_shmem = NULL;
	cluster_node_id = origin;
	runtime_storage_full_xid = owner.top_full_xid;
	runtime_storage_assignments = runtime_storage_binding_reads = 0;
	memset(&runtime_storage_writer, 0, sizeof(runtime_storage_writer));
	runtime_storage_writer.ref.timeline = 1;
	runtime_storage_writer.epoch = owner.membership_epoch;
	runtime_storage_writer.ref.claim.identity.system_identifier = ns->system_identifier;
	runtime_storage_writer.ref.claim.database_incarnation = ns->database_incarnation;
	runtime_storage_writer.ref.claim.identity.origin_node_id = origin;
	runtime_storage_writer.ref.claim.identity.origin_thread_id = origin + 1;
	runtime_storage_writer.ref.claim.identity.origin_owner_incarnation = owner.admitted_incarnation;
	cluster_relmap_retention_runtime_shmem_register();
	runtime_storage_region->init_fn();
	stage_directory = &relmap_runtime_shmem->directories[origin];
	stage_lock = runtime_test_lock;
	/* The test alone supplies COMPLETE; no production publisher is introduced. */
	LWLockAcquire(stage_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(stage_directory,
		ns->system_identifier, ns->database_incarnation, origin, 1,
		runtime_storage_full_xid - 16, runtime_storage_full_xid + 16), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(stage_directory), CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(stage_lock);
	relmap_x_held = true;
	UT_ASSERT(cluster_relmap_owner_capture_v2(sample));
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(sample, ns, 8), CLUSTER_RELMAP_RETENTION_OK);
	writer_key.identity = *ns;
	writer_key.generation = 8;
	writer_key.origin = origin;
	writer_key.top_full_xid = sample->owner.top_full_xid;
	writer_key.membership_epoch = sample->owner.membership_epoch;
	writer_key.admitted_incarnation = sample->owner.admitted_incarnation;
	writer_key.boot_incarnation = sample->owner.boot_incarnation;
}

static bool
runtime_storage_stage(const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *ns)
{
	RelMapFile map = map_image(40001);
	bool result;

	/* Original producer order: after CRIT, with the local mapping lock held.
	 * No assigning XID or TT provider may be queried in this window. */
	mapping_held = observing_stage = true;
	result = cluster_relmap_retention_runtime_write_pending_v2(sample, ns, 8, owner.wal_end, &map);
	mapping_held = observing_stage = false;
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT_EQ(writes_without_pin, 0);
	UT_ASSERT_EQ(retention_acquires, retention_releases);
	UT_ASSERT(!retention_held);
	UT_ASSERT_EQ(runtime_storage_assignments, 1);
	UT_ASSERT_EQ(runtime_storage_binding_reads, 1);
	return result;
}

static void
runtime_storage_assert_pin(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRelmapV2Namespace *ns, bool staged)
{
	const ClusterRelmapRetentionPin *pin = &stage_directory->pins[0];

	UT_ASSERT(pin->occupied && pin->may_be_staged == staged);
	UT_ASSERT_EQ(pin->key.origin, sample->owner.node);
	UT_ASSERT_EQ(pin->key.top_full_xid, sample->owner.top_full_xid);
	UT_ASSERT_EQ(pin->key.identity.dbid, ns->dbid);
	UT_ASSERT_EQ(pin->key.identity.shared, ns->shared);
	UT_ASSERT_EQ(pin->wal_end, staged ? owner.wal_end : 0);
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin++)
		if (origin != sample->owner.node)
			UT_ASSERT_EQ(relmap_runtime_shmem->directories[origin].state,
				CLUSTER_RELMAP_RETENTION_UNKNOWN);
}

UT_TEST(test_runtime_storage_original_namespace_origin_and_committed_reader)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1)
		for (unsigned shared = 0; shared < 2; shared++) {
			ClusterRelmapOwnerSampleV2 sample, before;
			ClusterRelmapV2Namespace ns;
			ClusterRelmapV2Image image;
			RelMapFile committed;
			uint64 generation;
			unsigned char a[2176], b[2176];

			runtime_storage_setup(&sample, &ns, origin, shared);
			before = sample;
			UT_ASSERT(runtime_storage_stage(&sample, &ns));
			UT_ASSERT_EQ(memcmp(&before, &sample, sizeof(sample)), 0);
			runtime_storage_assert_pin(&sample, &ns, true);
			UT_ASSERT_EQ(writes, 2);
			UT_ASSERT_EQ(renames, 2);
			UT_ASSERT_EQ(file_syncs, 2);
			UT_ASSERT_EQ(dir_syncs, 2);
			read_copy(0, a);
			read_copy(1, b);
			UT_ASSERT_EQ(memcmp(a, b, sizeof(a)), 0);
			UT_ASSERT_EQ(cluster_relmap_v2_decode(a, sizeof(a), &ns, &image), CLUSTER_RELMAP_V2_VALID);
			UT_ASSERT_EQ(image.pending_owner.node, origin);
			UT_ASSERT_EQ(image.pending_owner.top_full_xid, sample.owner.top_full_xid);
			UT_ASSERT_EQ(image.pending_owner.wal_end, owner.wal_end);
			UT_ASSERT_EQ(image.committed_generation, 7);
			UT_ASSERT_EQ(image.pending_generation, 8);
			UT_ASSERT_EQ(image.pending.mappings[0].mapfilenumber, 40001);
			UT_ASSERT(cluster_relmap_authority_v2_read_committed(&ns, &committed, &generation));
			UT_ASSERT_EQ(committed.mappings[0].mapfilenumber, 40000);
			UT_ASSERT_EQ(generation, 7);
		}
}

UT_TEST(test_runtime_storage_first_write_failure_keeps_shared_pin)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace ns;
	unsigned char before[2176], after[2176];

	runtime_storage_setup(&sample, &ns, 0, true);
	read_copy(0, before);
	fail_write = 1;
	UT_ASSERT(!runtime_storage_stage(&sample, &ns));
	UT_ASSERT_EQ(writes, 1);
	runtime_storage_assert_pin(&sample, &ns, true);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &ns, 8),
		CLUSTER_RELMAP_RETENTION_RETAINED);
	for (int copy = 0; copy < 2; copy++) {
		read_copy(copy, after);
		UT_ASSERT_EQ(memcmp(before, after, sizeof(before)), 0);
	}
}

UT_TEST(test_runtime_storage_later_io_failures_keep_shared_pin)
{
	for (unsigned cut = 0; cut < 8; cut++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;

		runtime_storage_setup(&sample, &ns, 127, false);
		switch (cut) {
			case 0: fail_write = 2; break;
			case 1: fail_file_sync = 1; break;
			case 2: fail_file_sync = 2; break;
			case 3: fail_rename = 1; break;
			case 4: fail_rename = 2; break;
			case 5: fail_dir_sync = 1; break;
			case 6: fail_dir_sync = 2; break;
			case 7: corrupt_after_rename = true; fault_rename = 2; break;
		}
		UT_ASSERT(!runtime_storage_stage(&sample, &ns));
		UT_ASSERT(writes > 0);
		runtime_storage_assert_pin(&sample, &ns, true);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &ns, 8),
			CLUSTER_RELMAP_RETENTION_RETAINED);
	}
}

UT_TEST(test_runtime_storage_bad_preflight_allows_only_unstaged_cancel)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace ns;
	unsigned char bad[2176], after[2176];

	runtime_storage_setup(&sample, &ns, 0, true);
	read_copy(1, bad);
	bad[0] ^= 1;
	write_copy(1, bad, sizeof(bad));
	UT_ASSERT(!runtime_storage_stage(&sample, &ns));
	UT_ASSERT_EQ(writes, 0);
	runtime_storage_assert_pin(&sample, &ns, false);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &ns, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(!stage_directory->pins[0].occupied);
	read_copy(1, after);
	UT_ASSERT_EQ(memcmp(bad, after, sizeof(bad)), 0);
}

UT_TEST(test_runtime_storage_unknown_building_never_reach_files)
{
	for (unsigned building = 0; building < 2; building++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapRetentionPin pin;

		runtime_storage_setup(&sample, &ns, 127, true);
		pin = stage_directory->pins[0];
		LWLockAcquire(stage_lock, LW_EXCLUSIVE);
		stage_directory->state = building ? CLUSTER_RELMAP_RETENTION_BUILDING : CLUSTER_RELMAP_RETENTION_UNKNOWN;
		LWLockRelease(stage_lock);
		UT_ASSERT(!runtime_storage_stage(&sample, &ns));
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &ns, 8),
			CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
	}
}

UT_TEST(test_runtime_storage_preflight_cancel_and_invalidate)
{
	for (unsigned invalidate = 0; invalidate < 2; invalidate++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;

		runtime_storage_setup(&sample, &ns, 0, false);
		cancel_on_read = !invalidate;
		invalidate_on_read = invalidate;
		UT_ASSERT(!runtime_storage_stage(&sample, &ns));
		UT_ASSERT(reads > 0);
		UT_ASSERT_EQ(writes, 0);
		if (invalidate) {
			UT_ASSERT_EQ(stage_directory->state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
			runtime_storage_assert_pin(&sample, &ns, false);
		} else {
			UT_ASSERT_EQ(cancelled, CLUSTER_RELMAP_RETENTION_OK);
			UT_ASSERT(!stage_directory->pins[0].occupied);
		}
	}
}

static void
runtime_storage_drift_after_last_sync(bool is_directory)
{
	if (is_directory && dir_syncs == 2)
		runtime_storage_writer.epoch++;
}

UT_TEST(test_runtime_storage_final_native_drift_keeps_pending_and_pin)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapV2Namespace ns;
	ClusterRelmapV2View view;

	runtime_storage_setup(&sample, &ns, 0, true);
	post_sync_hook = runtime_storage_drift_after_last_sync;
	UT_ASSERT(!runtime_storage_stage(&sample, &ns));
	UT_ASSERT_EQ(dir_syncs, 2);
	UT_ASSERT_EQ(writes, 2);
	runtime_storage_assert_pin(&sample, &ns, true);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&ns, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.has_pending && view.pending_complete);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &ns, 8),
		CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
}

UT_TEST(test_runtime_storage_clear_pair_keeps_shared_pin)
{
	for (unsigned publish = 0; publish < 2; publish++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = { 0 };
		ClusterRelmapV2View view;

		runtime_storage_setup(&sample, &ns, 127, false);
		UT_ASSERT(runtime_storage_stage(&sample, &ns));
		staged.identity = ns;
		staged.committed_generation = 7;
		staged.pending_generation = 8;
		staged.pending_owner = sample.owner;
		staged.pending_owner.wal_end = owner.wal_end;
		staged.committed = map_image(40000);
		staged.pending = map_image(40001);
		UT_ASSERT_EQ(publish ? cluster_relmap_authority_v2_publish(&staged)
			: cluster_relmap_authority_v2_discard_pending(&staged), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&ns, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(!view.has_pending && view.pending_complete);
		UT_ASSERT_EQ(view.committed_generation, publish ? 8 : 7);
		/* Caller terminal proof is outside this fixture. Even a durable clean
		 * pair alone cannot authorize the separate retention/native/TT release. */
		runtime_storage_assert_pin(&sample, &ns, true);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(&sample, &ns, 8),
			CLUSTER_RELMAP_RETENTION_RETAINED);
	}
}
