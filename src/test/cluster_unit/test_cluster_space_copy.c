/*-------------------------------------------------------------------------
 *
 * test_cluster_space_copy.c
 *    Native copy owners keep the newly created destination SPACE identity.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static SMgrRelationData source, destination;
static RelationData relation;
static FormData_pg_class form;
static unsigned created, copied, logged, flushes, drops, creates;
static bool registered;
static uint64 destination_identity;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	if (backend != InvalidBackendId)
		abort();
	if (RelFileLocatorEquals(locator, source.smgr_rlocator.locator))
		return &source;
	if (RelFileLocatorEquals(locator, destination.smgr_rlocator.locator))
		return &destination;
	abort();
}

void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	(void)owner;
	(void)rel;
	abort();
}

SMgrRelation
RelationCreateStorage(RelFileLocator locator, char persistence, bool register_delete)
{
	if (!RelFileLocatorEquals(locator, destination.smgr_rlocator.locator)
		|| persistence != form.relpersistence)
		abort();
	creates++;
	registered = register_delete;
	/* The actual SPACE producer is tested separately. Its result must not
	 * be overwritten by any of the original caller's subsequent fork loops. */
	destination_identity = 42;
	return &destination;
}

void
FlushRelationBuffers(Relation rel)
{
	if (rel != &relation)
		abort();
	flushes++;
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &source || forknum < 0 || forknum > MAX_FORKNUM)
		abort();
	return true;
}

void
smgrcreate(SMgrRelation rel, ForkNumber forknum, bool redo)
{
	if (rel != &destination || redo || forknum < 0 || forknum > MAX_FORKNUM)
		abort();
	created |= 1U << forknum;
}

void
log_smgrcreate(const RelFileLocator *locator, ForkNumber forknum)
{
	if (!RelFileLocatorEquals(*locator, destination.smgr_rlocator.locator))
		abort();
	logged |= 1U << forknum;
}

void
RelationCopyStorage(SMgrRelation src, SMgrRelation dst, ForkNumber forknum, char persistence)
{
	if (src != &source || dst != &destination || persistence != form.relpersistence)
		abort();
	copied |= 1U << forknum;
	if (forknum == SPACE_FORKNUM)
		destination_identity = 7;
}

static void
RelationCopyStorageUsingBuffer(RelFileLocator src, RelFileLocator dst, ForkNumber forknum,
							   bool permanent)
{
	if (permanent != (form.relpersistence == RELPERSISTENCE_PERMANENT))
		abort();
	RelationCopyStorage(smgropen(src, InvalidBackendId), smgropen(dst, InvalidBackendId), forknum,
						form.relpersistence);
}

void
RelationDropStorage(Relation rel)
{
	if (rel != &relation)
		abort();
	drops++;
}

void
smgrclose(SMgrRelation rel)
{
	if (rel != &destination)
		abort();
}
void
smgrcloserellocator(RelFileLocatorBackend locator)
{
	(void)smgropen(locator.locator, locator.backend);
}

/* Exact production owner bodies, extracted by name with count guards. */
#include "test_cluster_space_copy_heap.inc"
#include "test_cluster_space_copy_index.inc"
#include "test_cluster_space_copy_database.inc"

static void
reset(char persistence)
{
	memset(&source, 0, sizeof(source));
	memset(&destination, 0, sizeof(destination));
	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	source.smgr_rlocator.locator = (RelFileLocator){ 1663, 5, 16384 };
	destination.smgr_rlocator.locator = (RelFileLocator){ 1663, 5, 16385 };
	source.smgr_rlocator.backend = destination.smgr_rlocator.backend = InvalidBackendId;
	relation.rd_smgr = &source;
	relation.rd_locator = source.smgr_rlocator.locator;
	relation.rd_backend = InvalidBackendId;
	relation.rd_rel = &form;
	form.relpersistence = persistence;
	created = copied = logged = flushes = drops = creates = 0;
	destination_identity = 0;
	registered = false;
}

static void
check_copy(bool buffered)
{
	UT_ASSERT_EQ(creates, 1);
	UT_ASSERT_EQ(created,
				 (1U << FSM_FORKNUM) | (1U << VISIBILITYMAP_FORKNUM) | (1U << INIT_FORKNUM));
	UT_ASSERT_EQ(copied, (1U << MAIN_FORKNUM) | created);
	UT_ASSERT_EQ(logged,
				 form.relpersistence == RELPERSISTENCE_PERMANENT ? created : (1U << INIT_FORKNUM));
	UT_ASSERT_EQ(destination_identity, 42);
	UT_ASSERT_EQ(flushes, buffered ? 0 : 1);
	UT_ASSERT_EQ(drops, buffered ? 0 : 1);
	UT_ASSERT_EQ(registered, !buffered);
}

UT_TEST(test_heap_tablespace_copy_keeps_new_identity)
{
	reset(RELPERSISTENCE_PERMANENT);
	heapam_relation_copy_data(&relation, &destination.smgr_rlocator.locator);
	check_copy(false);
}

UT_TEST(test_index_tablespace_copy_keeps_new_identity)
{
	reset(RELPERSISTENCE_PERMANENT);
	index_copy_data(&relation, destination.smgr_rlocator.locator);
	check_copy(false);
}

UT_TEST(test_database_copy_uses_outer_abort_owner)
{
	reset(RELPERSISTENCE_PERMANENT);
	CreateAndCopyRelationData(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
							  true);
	check_copy(true);
}

UT_TEST(test_unlogged_copy_keeps_native_init_wal)
{
	reset(RELPERSISTENCE_UNLOGGED);
	CreateAndCopyRelationData(source.smgr_rlocator.locator, destination.smgr_rlocator.locator,
							  false);
	check_copy(true);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_heap_tablespace_copy_keeps_new_identity);
	UT_RUN(test_index_tablespace_copy_keeps_new_identity);
	UT_RUN(test_database_copy_uses_outer_abort_owner);
	UT_RUN(test_unlogged_copy_keeps_native_init_wal);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
