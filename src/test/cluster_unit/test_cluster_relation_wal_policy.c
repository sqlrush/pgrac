/*-------------------------------------------------------------------------
 *
 * test_cluster_relation_wal_policy.c
 *    Native permanent-relation WAL and pending-sync policy in shared mode.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relation_wal_policy.c
 *
 * NOTES
 *    Uses the production RelationNeedsWAL macro and extracted storage owners.
 *    Only the hash storage and memory context are fixtures, not WAL policy.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage.h"
#include "cluster/cluster_guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config;
int wal_level;
MemoryContext TopTransactionContext;

/* Actual private storage.c type is included with the owner bodies. */
#include "test_cluster_relation_wal_types.inc"
static HTAB *pendingSyncHash;
static PendingRelSync stored;
static unsigned create_count, insert_count;
static bool occupied;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected assertion %s %s:%d\n", condition, file, line);
	abort();
}

HTAB *
hash_create(const char *name, long nelem, const HASHCTL *info, int flags)
{
	if (strcmp(name, "pending sync hash") != 0 || nelem != 16
		|| info->keysize != sizeof(RelFileLocator) || info->entrysize != sizeof(PendingRelSync)
		|| flags != (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT))
		abort();
	create_count++;
	return (HTAB *)&stored;
}

void *
hash_search(HTAB *hash, const void *key, HASHACTION action, bool *found)
{
	bool present = occupied && memcmp(key, &stored.rlocator, sizeof(RelFileLocator)) == 0;
	if (hash != (HTAB *)&stored || (action != HASH_ENTER && action != HASH_FIND))
		abort();
	if (found != NULL)
		*found = present;
	if (action == HASH_ENTER) {
		if (occupied)
			abort();
		memcpy(&stored.rlocator, key, sizeof(RelFileLocator));
		occupied = true;
		insert_count++;
		return &stored;
	}
	return present ? &stored : NULL;
}

#include "test_cluster_relation_wal_native.inc"

static void
reset(bool shared)
{
	cluster_shared_config = shared;
	pendingSyncHash = NULL;
	create_count = insert_count = 0;
	occupied = false;
	memset(&stored, 0, sizeof(stored));
}

UT_TEST(test_native_predicate_matrix)
{
	RelationData rel;
	Relation relation = &rel;
	FormData_pg_class form;
	const char persistences[]
		= { RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED };
	memset(&rel, 0, sizeof(rel));
	memset(&form, 0, sizeof(form));
	rel.rd_rel = &form;
	for (int shared = 0; shared < 2; shared++) {
		reset(shared);
		for (wal_level = WAL_LEVEL_MINIMAL; wal_level <= WAL_LEVEL_LOGICAL; wal_level++) {
			for (unsigned p = 0; p < lengthof(persistences); p++) {
				form.relpersistence = persistences[p];
				for (unsigned subids = 0; subids < 4; subids++) {
					bool expected
						= p == 0 && (shared || wal_level >= WAL_LEVEL_REPLICA || subids == 0);
					rel.rd_createSubid = (subids & 1) ? 1 : InvalidSubTransactionId;
					rel.rd_firstRelfilelocatorSubid = (subids & 2) ? 2 : InvalidSubTransactionId;
					UT_ASSERT_EQ(RelationNeedsWAL(relation), expected);
				}
			}
		}
	}
}

UT_TEST(test_shared_storage_never_enrolls_pending_sync)
{
	RelFileLocator locator = { 1663, 5, 900 };
	reset(true);
	AddPendingSync(&locator);
	UT_ASSERT_EQ(create_count + insert_count, 0);
	UT_ASSERT(!RelFileLocatorSkippingWAL(locator));
}

UT_TEST(test_shared_worker_restore_does_not_elide_wal)
{
	RelFileLocator entries[] = { { 1663, 5, 900 }, { 0, 0, 0 } };
	reset(true);
	RestorePendingSyncs((char *)entries);
	UT_ASSERT_EQ(create_count + insert_count, 0);
	UT_ASSERT(!RelFileLocatorSkippingWAL(entries[0]));
}

UT_TEST(test_nonshared_pending_sync_remains_native)
{
	RelFileLocator entries[] = { { 1663, 5, 900 }, { 0, 0, 0 } };
	RelFileLocator other = { 1663, 5, 901 };
	for (int worker = 0; worker < 2; worker++) {
		reset(false);
		if (worker)
			RestorePendingSyncs((char *)entries);
		else
			AddPendingSync(entries);
		UT_ASSERT_EQ(create_count, 1);
		UT_ASSERT_EQ(insert_count, 1);
		UT_ASSERT(!stored.is_truncated);
		UT_ASSERT(RelFileLocatorSkippingWAL(entries[0]));
		UT_ASSERT(!RelFileLocatorSkippingWAL(other));
	}
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_native_predicate_matrix);
	UT_RUN(test_shared_storage_never_enrolls_pending_sync);
	UT_RUN(test_shared_worker_restore_does_not_elide_wal);
	UT_RUN(test_nonshared_pending_sync_remains_native);
	UT_DONE();
	return ut_failed_count != 0;
}
