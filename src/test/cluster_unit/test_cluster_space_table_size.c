/*-------------------------------------------------------------------------
 *
 * test_cluster_space_table_size.c
 *    Native all-fork sizing includes SPACE without counting INIT twice.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_table_size.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static SMgrRelationData storage;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	(void)locator;
	(void)backend;
	abort();
}

void
smgrsetowner(SMgrRelation *owner, SMgrRelation relation)
{
	(void)owner;
	(void)relation;
	abort();
}

BlockNumber
smgrnblocks(SMgrRelation relation, ForkNumber forknum)
{
	static const BlockNumber sizes[] = { 100, 10, 1, 9000, 2 };

	if (relation != &storage || forknum < 0 || forknum > 4)
		abort();
	return sizes[forknum];
}

UT_TEST(test_all_live_forks_includes_space_not_init)
{
	RelationData relation;

	memset(&relation, 0, sizeof(relation));
	relation.rd_smgr = &storage;
	UT_ASSERT_EQ(table_block_relation_size(&relation, InvalidForkNumber), UINT64_C(113) * BLCKSZ);
	UT_ASSERT_EQ(table_block_relation_size(&relation, SPACE_FORKNUM), UINT64_C(2) * BLCKSZ);
	UT_ASSERT_EQ(table_block_relation_size(&relation, INIT_FORKNUM), UINT64_C(9000) * BLCKSZ);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_all_live_forks_includes_space_not_init);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
