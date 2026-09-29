/*-------------------------------------------------------------------------
 *
 * test_cluster_space_cache.c
 *    Execute native DML identity consumers with real SPACE decoding.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/shmem.h"
#include "storage/s_lock.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA;
int NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile sig_atomic_t InterruptPending;

/* Keep PostgreSQL's real dynahash; only its process memory/error boundaries
 * and the invalidation transport are fixtures. */
static MemoryContextData memory_context = { .type = T_AllocSetContext };
MemoryContext TopMemoryContext = &memory_context;
MemoryContext CacheMemoryContext;
static RelcacheCallbackFunction invalidate;
static Datum invalidate_arg;

static PGAlignedBlock page;
static SMgrRelationData smgr;
static RelationData relation_data;
static FormData_pg_class catalog;
static ClusterWalDurablePrefixRef ref;
static ClusterSpaceIdentity disk_identity;
static unsigned exists_calls, size_calls, read_calls;
static bool pinned, locked, recovering, have_ref, have_space, invalidate_on_release;

void
ProcessInterrupts(void)
{
	abort();
}

void
CreateCacheMemoryContext(void)
{
	CacheMemoryContext = &memory_context;
}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size min, Size init, Size max)
{
	if (parent != &memory_context)
		abort();
	return &memory_context;
}

void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	if (context != &memory_context)
		abort();
	return malloc(size);
}

void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	return MemoryContextAlloc(context, size);
}

void
MemoryContextSetIdentifier(MemoryContext context, const char *name)
{
	if (context != &memory_context)
		abort();
}

void
pfree(void *ptr)
{
	free(ptr);
}

Size
add_size(Size a, Size b)
{
	if (a > SIZE_MAX - b)
		abort();
	return a + b;
}

Size
mul_size(Size a, Size b)
{
	if (b != 0 && a > SIZE_MAX / b)
		abort();
	return a * b;
}

int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	/* A local cache must never enter the shared-hash contention boundary. */
	abort();
}

int
errcode(int sqlerrcode)
{
	abort();
}

int
errmsg(const char *fmt, ...)
{
	abort();
}

void
CacheRegisterRelcacheCallback(RelcacheCallbackFunction callback, Datum arg)
{
	if (invalidate != NULL)
		abort();
	invalidate = callback;
	invalidate_arg = arg;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = ref;
	return have_ref;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
{
	abort();
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation_data.rd_locator) || backend != InvalidBackendId)
		abort();
	return 1;
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation_data.rd_locator) || backend != InvalidBackendId)
		abort();
	return &smgr;
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &smgr || forknum != SPACE_FORKNUM)
		abort();
	exists_calls++;
	return have_space;
}

BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &smgr || forknum != SPACE_FORKNUM)
		abort();
	size_calls++;
	return 1;
}

Buffer
ReadBufferWithoutRelcache(RelFileLocator locator, ForkNumber forknum, BlockNumber block,
						  ReadBufferMode mode, BufferAccessStrategy strategy, bool permanent)
{
	if (!RelFileLocatorEquals(locator, relation_data.rd_locator) || forknum != SPACE_FORKNUM
		|| block != 0 || mode != RBM_NORMAL || strategy != NULL || !permanent || pinned)
		abort();
	read_calls++;
	pinned = true;
	return 1;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 1 || !pinned || locked || mode != BUFFER_LOCK_SHARE)
		abort();
	locked = true;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked)
		abort();
	return 0;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked)
		abort();
	pinned = locked = false;
	if (invalidate_on_release) {
		invalidate_on_release = false;
		disk_identity.incarnation[0] = 0x67;
		disk_identity.sequence = 2;
		disk_identity.operation = 20;
		if (!cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ))
			abort();
		invalidate(invalidate_arg, relation_data.rd_id);
	}
}

/* Original DML and allocation prologues; no copied routing policy. */
static ClusterSpaceIdentity
insert_identity(Relation relation)
{
	ClusterSpaceIdentity cluster_page_identity;
	bool cluster_page_versioned = false;

	memset(&cluster_page_identity, 0, sizeof(cluster_page_identity));
#include "test_cluster_space_cache_insert.inc"
	if (!cluster_page_versioned)
		abort();
	return cluster_page_identity;
}

static ClusterSpaceIdentity
delete_identity(Relation relation)
{
	ClusterSpaceIdentity cluster_page_identity;
	bool cluster_page_versioned = false;

	memset(&cluster_page_identity, 0, sizeof(cluster_page_identity));
#include "test_cluster_space_cache_delete.inc"
	if (!cluster_page_versioned)
		abort();
	return cluster_page_identity;
}

static ClusterSpaceIdentity
update_identity(Relation relation)
{
	ClusterSpaceIdentity cluster_page_identity;
	bool cluster_page_versioned = false;

	memset(&cluster_page_identity, 0, sizeof(cluster_page_identity));
#include "test_cluster_space_cache_update.inc"
	if (!cluster_page_versioned)
		abort();
	return cluster_page_identity;
}

static ClusterSpaceIdentity
allocation_identity(Relation relation)
{
	ClusterSpaceIdentity page_identity_storage;
	const struct ClusterSpaceIdentity *page_identity = NULL;

#include "test_cluster_space_cache_allocation.inc"
	if (page_identity == NULL)
		abort();
	return *page_identity;
}

static void
setup(void)
{
	if (invalidate != NULL)
		invalidate(invalidate_arg, InvalidOid);
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&catalog, 0, sizeof(catalog));
	memset(&ref, 0, sizeof(ref));
	memset(&disk_identity, 0, sizeof(disk_identity));
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 16384 };
	relation_data.rd_id = 16384;
	relation_data.rd_isvalid = true;
	relation_data.rd_rel = &catalog;
	catalog.relpersistence = RELPERSISTENCE_PERMANENT;
	ref.claim.identity.system_identifier = 42;
	ref.claim.database_incarnation = 3;
	memset(ref.claim.identity.storage_uuid, 0x23, 16);
	disk_identity.key.system_identifier = 42;
	disk_identity.key.database_incarnation = 3;
	memset(disk_identity.key.storage_uuid, 0x23, 16);
	disk_identity.key.locator = relation_data.rd_locator;
	memset(disk_identity.incarnation, 0x45, 16);
	disk_identity.sequence = 1;
	disk_identity.operation = 19;
	disk_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!cluster_space_identity_page_encode(&disk_identity, 19, page.data, BLCKSZ))
		abort();
	BufferBlocks = page.data;
	exists_calls = size_calls = read_calls = 0;
	pinned = locked = false;
	recovering = invalidate_on_release = false;
	have_ref = have_space = true;
}

UT_TEST(test_insert_allocation_and_delete_share_one_identity_read)
{
	ClusterSpaceIdentity insert, allocation, deletion;
	int i;

	setup();
	insert = insert_identity(&relation_data);
	allocation = allocation_identity(&relation_data);
	deletion = delete_identity(&relation_data);
	UT_ASSERT_EQ(insert.incarnation[0], 0x45);
	UT_ASSERT_EQ(allocation.sequence, 1);
	UT_ASSERT_EQ(deletion.operation, 19);
	for (i = 0; i < 100; i++) {
		ClusterSpaceIdentity repeated
			= i % 2 ? update_identity(&relation_data) : insert_identity(&relation_data);

		UT_ASSERT_EQ(repeated.incarnation[0], 0x45);
	}
	UT_ASSERT_EQ(exists_calls, 1);
	UT_ASSERT_EQ(size_calls, 1);
	UT_ASSERT_EQ(read_calls, 1);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_targeted_invalidation_reloads_new_incarnation)
{
	ClusterSpaceIdentity out;

	setup();
	(void)insert_identity(&relation_data);
	disk_identity.incarnation[0] = 0x66;
	disk_identity.sequence = 2;
	disk_identity.operation = 20;
	UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ));
	invalidate(invalidate_arg, relation_data.rd_id);
	out = delete_identity(&relation_data);
	UT_ASSERT_EQ(out.incarnation[0], 0x66);
	UT_ASSERT_EQ(out.sequence, 2);
	UT_ASSERT_EQ(read_calls, 2);
	(void)allocation_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 2);
}

UT_TEST(test_unrelated_invalidation_preserves_hit_global_reset_drops_it)
{
	setup();
	(void)insert_identity(&relation_data);
	invalidate(invalidate_arg, 20000);
	(void)insert_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 1);
	invalidate(invalidate_arg, InvalidOid);
	(void)insert_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 2);
}

UT_TEST(test_locator_and_namespace_changes_cannot_hit_old_cache)
{
	ClusterSpaceIdentity out;
	int change;

	setup();
	(void)insert_identity(&relation_data);
	for (change = 0; change < 4; change++) {
		switch (change) {
		case 0:
			disk_identity.key.locator.relNumber++;
			relation_data.rd_locator = disk_identity.key.locator;
			break;
		case 1:
			ref.claim.identity.system_identifier = ++disk_identity.key.system_identifier;
			break;
		case 2:
			ref.claim.database_incarnation = ++disk_identity.key.database_incarnation;
			break;
		case 3:
			ref.claim.identity.storage_uuid[0] = ++disk_identity.key.storage_uuid[0];
			break;
		}
		disk_identity.incarnation[0]++;
		UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 19, page.data, BLCKSZ));
		UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
		UT_ASSERT_EQ(out.incarnation[0], 0x46 + change);
		UT_ASSERT_EQ(read_calls, change + 2);
	}
}

UT_TEST(test_invalidation_during_read_reobserves_instead_of_publishing_old_value)
{
	ClusterSpaceIdentity out;

	setup();
	invalidate_on_release = true;
	UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(out.incarnation[0], 0x67);
	UT_ASSERT_EQ(out.sequence, 2);
	UT_ASSERT_EQ(read_calls, 2);
	(void)insert_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 2);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_invalid_relcache_cannot_resurrect_a_stale_entry)
{
	ClusterSpaceIdentity out, saved;

	setup();
	(void)insert_identity(&relation_data);
	memset(&out, 0x55, sizeof(out));
	saved = out;
	relation_data.rd_isvalid = false;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	relation_data.rd_isvalid = true;
	UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 2);
}

UT_TEST(test_unavailable_authority_and_recovery_do_not_reuse_cache)
{
	ClusterSpaceIdentity out, saved;

	setup();
	(void)insert_identity(&relation_data);
	memset(&out, 0x55, sizeof(out));
	saved = out;
	have_ref = false;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	have_ref = true;
	recovering = true;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	recovering = false;
	catalog.relpersistence = RELPERSISTENCE_UNLOGGED;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(read_calls, 1);
}

UT_TEST(test_missing_corrupt_and_tombstoned_identity_never_poison_cache)
{
	ClusterSpaceIdentity out, saved;

	setup();
	memset(&out, 0x55, sizeof(out));
	saved = out;
	have_space = false;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	have_space = true;
	page.data[60] ^= 1;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 1);
	disk_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	disk_identity.sequence = 2;
	disk_identity.operation = 20;
	UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ));
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 2);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	disk_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ));
	UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 3);
	UT_ASSERT(!pinned && !locked);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_insert_allocation_and_delete_share_one_identity_read);
	UT_RUN(test_targeted_invalidation_reloads_new_incarnation);
	UT_RUN(test_unrelated_invalidation_preserves_hit_global_reset_drops_it);
	UT_RUN(test_locator_and_namespace_changes_cannot_hit_old_cache);
	UT_RUN(test_invalidation_during_read_reobserves_instead_of_publishing_old_value);
	UT_RUN(test_invalid_relcache_cannot_resurrect_a_stale_entry);
	UT_RUN(test_unavailable_authority_and_recovery_do_not_reuse_cache);
	UT_RUN(test_missing_corrupt_and_tombstoned_identity_never_poison_cache);
	UT_DONE();
	return ut_failed_count != 0;
}
