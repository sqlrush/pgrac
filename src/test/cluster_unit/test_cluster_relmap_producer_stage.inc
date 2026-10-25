/* Actual original relmapper/owner/native-writer/runtime composition.
 * Transaction assignment, locks, shmem, TT and physical pair I/O are controlled
 * leaves; COMPLETE/floor/issued are fixtures, never live qualification.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "cluster/cluster_relmap_retention_runtime.h"
#include "cluster/cluster_shmem.h"
#include "storage/shmem.h"

static bool v2_error_flush;
static void v2_stage_flush(XLogRecPtr end);
#pragma push_macro("XLogFlush")
#undef XLogFlush
#define XLogFlush v2_stage_flush
#include "../../backend/cluster/cluster_relmap_retention_runtime.c"
#pragma pop_macro("XLogFlush")

static const ClusterShmemRegion *v2_region;
static void *v2_allocation;
static unsigned v2_pair_mode, v2_pair_reads, v2_stage_mode, v2_stage_calls;
static unsigned v2_stage_writes, v2_tt_reads;
static ClusterRelmapV2Owner v2_staged_owner;
static ClusterRelmapV2Namespace v2_staged_identity;
static RelMapFile v2_staged_map;

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

static void
v2_stage_flush(XLogRecPtr end)
{
	XLogFlush(end);
	if (v2_error_flush)
		ereport(ERROR, (errmsg("controlled native flush error")));
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return mode == LW_EXCLUSIVE && LWLockHeldByMe(lock);
}

FullTransactionId
GetCurrentFullTransactionIdIfAny(void)
{
	return assigned_xid;
}

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	UT_ASSERT(!local_held && !v2_retention_held);
	UT_ASSERT_EQ(xid, XidFromFullTransactionId(assigned_xid));
	v2_tt_reads++;
	memset(out, 0, sizeof(*out));
	return false;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	v2_region = region;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	UT_ASSERT(v2_region && strcmp(name, v2_region->name) == 0);
	UT_ASSERT_EQ(size, v2_region->size_fn());
	UT_ASSERT(!local_held && !v2_retention_held && !v2_allocation);
	*found = false;
	v2_allocation = malloc(size);
	if (!v2_allocation)
		abort();
	return v2_allocation;
}

int
LWLockNewTrancheId(void)
{
	return 301;
}

void
LWLockInitialize(LWLock *lock, int tranche)
{
	UT_ASSERT_EQ(tranche, 301);
	memset(lock, 0, sizeof(*lock));
	lock->tranche = tranche;
	v2_retention_lock = lock;
}

void
LWLockRegisterTranche(int tranche, const char *name)
{
	UT_ASSERT_EQ(tranche, 301);
	UT_ASSERT(strcmp(name, "ClusterRelmapRetention") == 0);
}

ClusterRelmapV2PairResult
cluster_relmap_authority_v2_read(const ClusterRelmapV2Namespace *identity,
	ClusterRelmapV2View *out)
{
	if (v2_pair_observer)
		return v2_pair_observer(identity, out);
	UT_ASSERT(authority_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT_EQ(identity->system_identifier, GetSystemIdentifier());
	UT_ASSERT_EQ(identity->database_incarnation, writer_ref.claim.database_incarnation);
	UT_ASSERT_EQ(identity->dbid, identity->shared ? InvalidOid : MyDatabaseId);
	UT_ASSERT_EQ(local_held, v2_pair_reads != 0);
	v2_pair_reads++;
	if ((v2_pair_mode == 12 && v2_pair_reads == 1)
		|| (v2_pair_mode == 13 && v2_pair_reads == 2))
		ereport(ERROR, (errmsg("controlled full pair read error")));
	memset(out, 0, sizeof(*out));
	out->committed = committed;
	out->committed_generation = 7;
	out->pending_complete = true;
	if (v2_pair_mode == 1)
		return CLUSTER_RELMAP_V2_PAIR_DEGRADED;
	if (v2_pair_mode == 2)
		out->pending_complete = false;
	if (v2_pair_mode == 3)
		out->has_pending = true;
	if (v2_pair_mode == 4)
		out->committed_generation = 0;
	if (v2_pair_mode == 5)
		out->committed_generation = UINT64_MAX;
	if (v2_pair_mode == 6)
		return CLUSTER_RELMAP_V2_PAIR_INVALID;
	if (v2_pair_mode == 7)
		return CLUSTER_RELMAP_V2_PAIR_CONFLICT;
	if (v2_pair_reads == 2 && v2_pair_mode == 8)
		out->committed.mappings[0].mapfilenumber++;
	if (v2_pair_reads == 2 && v2_pair_mode == 9)
		out->committed_generation++;
	if ((v2_pair_mode == 10 && v2_pair_reads == 1)
		|| (v2_pair_mode == 11 && v2_pair_reads == 2))
		change_original_owner();
	return CLUSTER_RELMAP_V2_PAIR_VALID;
}

bool
cluster_relmap_authority_v2_write_pending(const ClusterRelmapV2Namespace *identity,
	uint64 generation, const ClusterRelmapV2Owner *owner, const RelMapFile *pending,
	ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionResult result;

	if (v2_write_observer)
		return v2_write_observer(identity, generation, owner, pending, directory, lock);

	UT_ASSERT(authority_held && local_held && !v2_retention_held && CritSectionCount == 0);
	UT_ASSERT(lock == v2_retention_lock);
	UT_ASSERT(directory == &relmap_runtime_shmem->directories[owner->node]);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(owner->wal_end, 1001);
	v2_stage_calls++;
	v2_staged_owner = *owner;
	v2_staged_identity = *identity;
	v2_staged_map = *pending;
	stage_order = ++sequence;
	if (v2_stage_mode == 1)
		return false;
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
	v2_stage_writes++;
	if (v2_stage_mode == 2)
		return false;
	if (v2_stage_mode == 4)
		ereport(ERROR, (errmsg("controlled pending write error")));
	v2_stage_writes++;
	if (v2_stage_mode == 3)
		change_original_owner();
	return true;
}

static ClusterRelmapRetentionDirectory *
v2_fixture(bool preassigned, bool next_era)
{
	ClusterRelmapRetentionDirectory *directory;
	ClusterRelmapRetentionKey old = { 0 };

	reset(preassigned);
	if (next_era)
	{
		next_xid = FullTransactionIdFromEpochAndXid(8, 100);
		assigned_xid = preassigned ? next_xid : InvalidFullTransactionId;
	}
	authority_held = true;
	allowSystemTableMods = false;
	v2_retention_held = v2_xidgen_held = v2_error_flush = false;
	v2_retention_lock = NULL;
	v2_region = NULL;
	relmap_runtime_shmem = NULL;
	free(v2_allocation);
	v2_allocation = NULL;
	v2_pair_mode = v2_pair_reads = v2_stage_mode = v2_stage_calls = 0;
	v2_stage_writes = v2_tt_reads = 0;
	cluster_relmap_retention_runtime_shmem_register();
	v2_region->init_fn();
	directory = &relmap_runtime_shmem->directories[cluster_node_id];
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(directory, GetSystemIdentifier(),
		writer_ref.claim.database_incarnation, cluster_node_id, 1,
		U64FromFullTransactionId(next_xid) - 10, U64FromFullTransactionId(next_xid) + 100),
		CLUSTER_RELMAP_RETENTION_OK);
	old.identity.system_identifier = GetSystemIdentifier();
	old.identity.database_incarnation = writer_ref.claim.database_incarnation;
	old.identity.dbid = 54321;
	old.generation = 4;
	old.origin = cluster_node_id;
	old.top_full_xid = U64FromFullTransactionId(next_xid) - 1;
	old.membership_epoch = writer_epoch;
	old.admitted_incarnation = admitted_incarnation;
	old.boot_incarnation = boot_incarnation;
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(directory, &old, 900), CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(directory), CLUSTER_RELMAP_RETENTION_OK);
	return directory;
}

static void
v2_no_adoption_or_terminal(void)
{
	RelMapFile zero = { 0 };

	UT_ASSERT(authority_held && !local_held && !v2_retention_held);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(preserves + sync_commits, 0);
	UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
	UT_ASSERT(memcmp(&shared_map, &zero, sizeof(zero)) == 0);
	UT_ASSERT(memcmp(&local_map, &zero, sizeof(zero)) == 0);
}

UT_TEST(v2_original_stage_captures_merges_and_retains)
{
	for (unsigned which = 0; which < 8; which++)
	{
		bool shared = (which & 1) != 0;
		bool preassigned = (which & 2) != 0;
		ClusterRelmapRetentionDirectory *directory = v2_fixture(preassigned, (which & 4) != 0);
		ClusterRelmapRetentionPin old = directory->pins[0];
		ClusterRelmapOwnerSampleV2 owner;
		ClusterRelmapV2Image image;
		uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
		bool staged;

		staged = cluster_relmap_producer_stage_v2(shared, &updates, &owner, &image);
		UT_ASSERT(staged);
		if (!staged)
			continue;
		UT_ASSERT_EQ(full_requests, 1);
		UT_ASSERT_EQ(raw_requests, 0);
		UT_ASSERT_EQ(assignments, preassigned ? 0 : 1);
		UT_ASSERT_EQ(xid_under_lock + xid_in_crit, 0);
		UT_ASSERT(xid_order < lock_order && lock_order < wal_order && wal_order < stage_order);
		UT_ASSERT_EQ(v2_pair_reads, 2);
		UT_ASSERT_EQ(v2_tt_reads, 1);
		UT_ASSERT_EQ(begins, 1);
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(flushes, 1);
		UT_ASSERT_EQ(v2_stage_writes, 2);
		UT_ASSERT_EQ(owner.owner.wal_end, 0);
		UT_ASSERT_EQ(owner.owner.top_full_xid, U64FromFullTransactionId(next_xid));
		UT_ASSERT(cluster_relmap_owner_matches_v2(&owner));
		UT_ASSERT_EQ(image.committed_generation, 7);
		UT_ASSERT_EQ(image.pending_generation, 8);
		UT_ASSERT_EQ(image.pending_owner.wal_end, 1001);
		UT_ASSERT_EQ(image.identity.shared, shared);
		UT_ASSERT_EQ(image.identity.dbid, shared ? InvalidOid : MyDatabaseId);
		UT_ASSERT_EQ(wal_header.dbid, image.identity.dbid);
		UT_ASSERT_EQ(wal_header.tsid, shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
		UT_ASSERT(memcmp(&image.committed, &committed, sizeof(committed)) == 0);
		UT_ASSERT_EQ(image.pending.mappings[0].mapfilenumber, 501);
		UT_ASSERT(memcmp(&wal_image, &image.pending, sizeof(wal_image)) == 0);
		UT_ASSERT(memcmp(&v2_staged_map, &image.pending, sizeof(wal_image)) == 0);
		UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, wire, sizeof(wire)), CLUSTER_RELMAP_V2_VALID);
		UT_ASSERT(memcmp(&directory->pins[0], &old, sizeof(old)) == 0);
		UT_ASSERT(directory->pins[1].occupied && directory->pins[1].may_be_staged);
		UT_ASSERT_EQ(directory->pins[1].wal_end, 1001);
		v2_no_adoption_or_terminal();
	}
}

UT_TEST(v2_original_stage_pre_wal_refusals_preserve_outputs)
{
	for (unsigned which = 0; which < 20; which++)
	{
		ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapOwnerSampleV2 owner, old_owner;
		ClusterRelmapV2Image image, old_image;
		const RelMapFile *input = &updates;

		memset(&owner, 0xa5, sizeof(owner)); old_owner = owner;
		memset(&image, 0x5a, sizeof(image)); old_image = image;
		if (which < 7) v2_pair_mode = which + 1;
		if (which == 7) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which == 8) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which == 9) relmap_runtime_shmem = NULL;
		if (which == 10) authority_held = false;
		if (which == 11) local_held = true;
		if (which == 12) v2_xidgen_held = true;
		if (which == 13) ShutdownRequestPending = true;
		if (which == 14) writer_available = false;
		if (which == 15) updates.num_mappings = -1;
		if (which == 16) updates.num_mappings = MAX_MAPPINGS + 1;
		if (which == 17) input = NULL;
		before = *directory;
		UT_ASSERT(!cluster_relmap_producer_stage_v2(true, input,
			which == 18 ? NULL : &owner, which == 19 ? NULL : &image));
		UT_ASSERT(memcmp(directory, &before, sizeof(before)) == 0);
		UT_ASSERT(memcmp(&owner, &old_owner, sizeof(owner)) == 0);
		UT_ASSERT(memcmp(&image, &old_image, sizeof(image)) == 0);
		UT_ASSERT_EQ(begins + inserts + flushes + v2_stage_calls, 0);
		UT_ASSERT_EQ(local_held, which == 11);
		UT_ASSERT_EQ(authority_held, which != 10);
		UT_ASSERT_EQ(v2_xidgen_held, which == 12);
	}
	v2_xidgen_held = false;
}

UT_TEST(v2_original_stage_late_refusals_keep_original_protection)
{
	for (unsigned which = 0; which < 8; which++)
	{
		ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
		ClusterRelmapRetentionPin old = directory->pins[0];
		ClusterRelmapOwnerSampleV2 owner, old_owner;
		ClusterRelmapV2Image image, old_image;

		memset(&owner, 0xa5, sizeof(owner)); old_owner = owner;
		memset(&image, 0x5a, sizeof(image)); old_image = image;
		if (which < 4) v2_pair_mode = which + 8;
		if (which == 4) drift_at = 2;
		if (which >= 5) v2_stage_mode = which - 4;
		UT_ASSERT(!cluster_relmap_producer_stage_v2(false, &updates, &owner, &image));
		UT_ASSERT(memcmp(&owner, &old_owner, sizeof(owner)) == 0);
		UT_ASSERT(memcmp(&image, &old_image, sizeof(image)) == 0);
		UT_ASSERT(memcmp(&directory->pins[0], &old, sizeof(old)) == 0);
		UT_ASSERT_EQ(inserts, which >= 4 ? 1 : 0);
		UT_ASSERT_EQ(flushes, inserts);
		UT_ASSERT_EQ(directory->pins[1].occupied, which != 2);
		if (which != 2)
		{
			UT_ASSERT_EQ(directory->pins[1].key.generation, 8);
			UT_ASSERT_EQ(directory->pins[1].key.top_full_xid, U64FromFullTransactionId(next_xid));
			UT_ASSERT_EQ(directory->pins[1].may_be_staged, which >= 6);
		}
		v2_no_adoption_or_terminal();
	}
}

static bool
v2_stage_raises(ClusterRelmapOwnerSampleV2 *owner, ClusterRelmapV2Image *image)
{
	error_expected = true;
	if (sigsetjmp(error_target, 0) == 0)
	{
		(void) cluster_relmap_producer_stage_v2(true, &updates, owner, image);
		error_expected = false;
		return false;
	}
	error_expected = false;
	return true;
}

UT_TEST(v2_original_stage_errors_leave_original_failure_duties)
{
	for (unsigned which = 0; which < 5; which++)
	{
		ClusterRelmapRetentionDirectory *directory = v2_fixture(false, false);
		ClusterRelmapRetentionPin old = directory->pins[0];
		ClusterRelmapOwnerSampleV2 owner, old_owner;
		ClusterRelmapV2Image image, old_image;

		memset(&owner, 0xa5, sizeof(owner)); old_owner = owner;
		memset(&image, 0x5a, sizeof(image)); old_image = image;
		if (which < 2) v2_pair_mode = which + 12;
		if (which == 2) updates.mappings[0].mapoid = 9999;
		if (which == 3) v2_error_flush = true;
		if (which == 4) v2_stage_mode = 4;
		UT_ASSERT(v2_stage_raises(&owner, &image));
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT(memcmp(&owner, &old_owner, sizeof(owner)) == 0);
		UT_ASSERT(memcmp(&image, &old_image, sizeof(image)) == 0);
		UT_ASSERT(memcmp(&directory->pins[0], &old, sizeof(old)) == 0);
		UT_ASSERT(authority_held && !v2_retention_held);
		UT_ASSERT_EQ(local_held, which != 0);
		UT_ASSERT_EQ(CritSectionCount, which == 3 ? 1 : 0);
		UT_ASSERT_EQ(directory->pins[1].occupied, which != 0);
		UT_ASSERT_EQ(directory->pins[1].may_be_staged, which == 4);
		UT_ASSERT_EQ(inserts, which >= 3 ? 1 : 0);
		/* Only this controlled caller simulates native transaction abort teardown. */
		local_held = false;
		CritSectionCount = 0;
		v2_no_adoption_or_terminal();
	}
}
