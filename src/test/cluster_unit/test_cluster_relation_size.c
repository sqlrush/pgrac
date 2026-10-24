/* Real dbsize fork calculation with controlled storage/syscall boundaries.
 * Copyright (c) 2026, pgrac contributors.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <setjmp.h>
#include <sys/stat.h>
#include "miscadmin.h"
#include "storage/smgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static SMgrRelationData relation;
static RelFileLocator locator = {1663, 5, 16384};
static BackendId expected_backend;
static ForkNumber expected_fork;
static bool present, fail_size, fail_stat, catching;
static BlockNumber blocks;
static unsigned opens, exists_calls, size_calls, stat_calls, path_calls, interrupts;
static int last_error;
static jmp_buf error_jump;
static int64 segment_sizes[4];
static unsigned segments;

static void
fixture_error(int code)
{
	last_error = code;
	if (!catching)
		abort();
	longjmp(error_jump, 1);
}

SMgrRelation
smgropen(RelFileLocator actual, BackendId backend)
{
	UT_ASSERT_EQ(actual.spcOid, locator.spcOid);
	UT_ASSERT_EQ(actual.dbOid, locator.dbOid);
	UT_ASSERT_EQ(actual.relNumber, locator.relNumber);
	UT_ASSERT_EQ(backend, expected_backend);
	opens++;
	return &relation;
}

bool
smgrexists(SMgrRelation actual, ForkNumber fork)
{
	UT_ASSERT(actual == &relation);
	UT_ASSERT_EQ(fork, expected_fork);
	exists_calls++;
	return present;
}

BlockNumber
smgrnblocks(SMgrRelation actual, ForkNumber fork)
{
	UT_ASSERT(actual == &relation);
	UT_ASSERT_EQ(fork, expected_fork);
	size_calls++;
	if (fail_size)
		fixture_error(EIO);
	return blocks;
}

static char *
fixture_path(RelFileLocator actual, BackendId backend, ForkNumber fork)
{
	static char path[] = "fixture_relation";
	UT_ASSERT_EQ(actual.relNumber, locator.relNumber);
	UT_ASSERT_EQ(backend, expected_backend);
	UT_ASSERT_EQ(fork, expected_fork);
	path_calls++;
	return path;
}

static int
fixture_stat(const char *path, struct stat *result)
{
	unsigned segment = stat_calls++;
	UT_ASSERT(strncmp(path, "fixture_relation", strlen("fixture_relation")) == 0);
	if (fail_stat) {
		errno = EACCES;
		return -1;
	}
	if (segment >= segments) {
		errno = ENOENT;
		return -1;
	}
	memset(result, 0, sizeof(*result));
	result->st_size = segment_sizes[segment];
	return 0;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

#undef relpathbackend
#define relpathbackend(actual, backend, fork) fixture_path(actual, backend, fork)
#define stat(path, result) fixture_stat(path, result)
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)interrupts++)
#undef ereport
#define ereport(level, details) fixture_error(errno)
#include "test_cluster_relation_size.inc"
#undef stat

static void
reset_fixture(void)
{
	memset(&relation, 0, sizeof(relation));
	relation.smgr_which = 1;
	locator.relNumber = 16384;
	expected_backend = InvalidBackendId;
	expected_fork = MAIN_FORKNUM;
	present = true;
	fail_size = fail_stat = catching = false;
	blocks = 17;
	opens = exists_calls = size_calls = stat_calls = path_calls = interrupts = 0;
	last_error = 0;
	segments = 0;
	memset(segment_sizes, 0, sizeof(segment_sizes));
}

UT_TEST(shared_heap_uses_bound_storage)
{
	reset_fixture();
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork),
				 (int64)17 * BLCKSZ);
	UT_ASSERT_EQ(opens, 1);
	UT_ASSERT_EQ(exists_calls, 1);
	UT_ASSERT_EQ(size_calls, 1);
	UT_ASSERT_EQ(stat_calls, 0);
	UT_ASSERT_EQ(path_calls, 0);
}

UT_TEST(shared_index_uses_exact_locator)
{
	reset_fixture();
	locator.relNumber = 16389;
	blocks = 39;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork),
				 (int64)39 * BLCKSZ);
	UT_ASSERT_EQ(stat_calls, 0);
}

UT_TEST(shared_forks_use_same_provider)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		reset_fixture();
		expected_fork = (ForkNumber)fork;
		blocks = fork + 1;
		UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork),
					 (int64)(fork + 1) * BLCKSZ);
		UT_ASSERT_EQ(size_calls, 1);
		UT_ASSERT_EQ(stat_calls, 0);
	}
}

UT_TEST(absent_shared_fork_remains_zero)
{
	reset_fixture();
	expected_fork = INIT_FORKNUM;
	present = false;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork), 0);
	UT_ASSERT_EQ(exists_calls, 1);
	UT_ASSERT_EQ(size_calls, 0);
	UT_ASSERT_EQ(stat_calls, 0);
}

UT_TEST(shared_provider_error_is_not_zero)
{
	volatile bool caught = false;
	reset_fixture();
	fail_size = catching = true;
	if (setjmp(error_jump) == 0)
		(void)calculate_relation_size(&locator, expected_backend, expected_fork);
	else
		caught = true;
	catching = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_error, EIO);
	UT_ASSERT_EQ(stat_calls, 0);
}

UT_TEST(shared_large_fork_does_not_overflow_int32)
{
	reset_fixture();
	blocks = UINT32_C(1) << 30;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork),
				 (INT64_C(1) << 30) * BLCKSZ);
}

UT_TEST(local_segments_preserve_actual_bytes)
{
	reset_fixture();
	relation.smgr_which = 0;
	segments = 3;
	segment_sizes[0] = 2 * BLCKSZ;
	segment_sizes[1] = BLCKSZ;
	segment_sizes[2] = 17;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork),
				 (int64)3 * BLCKSZ + 17);
	UT_ASSERT_EQ(stat_calls, 4);
	UT_ASSERT_EQ(size_calls, 0);
	UT_ASSERT_EQ(exists_calls, 0);
	UT_ASSERT_EQ(interrupts, 4);
}

UT_TEST(temp_relation_preserves_md_path)
{
	reset_fixture();
	relation.smgr_which = 0;
	expected_backend = 7;
	segments = 1;
	segment_sizes[0] = BLCKSZ;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork), BLCKSZ);
	UT_ASSERT_EQ(path_calls, 1);
	UT_ASSERT_EQ(size_calls, 0);
}

UT_TEST(missing_local_fork_remains_zero)
{
	reset_fixture();
	relation.smgr_which = 0;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork), 0);
	UT_ASSERT_EQ(stat_calls, 1);
}

UT_TEST(local_permission_error_is_not_zero)
{
	volatile bool caught = false;
	reset_fixture();
	relation.smgr_which = 0;
	fail_stat = catching = true;
	if (setjmp(error_jump) == 0)
		(void)calculate_relation_size(&locator, expected_backend, expected_fork);
	else
		caught = true;
	catching = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_error, EACCES);
}

UT_TEST(no_storage_locator_does_not_open_smgr)
{
	reset_fixture();
	locator.relNumber = InvalidRelFileNumber;
	UT_ASSERT_EQ(calculate_relation_size(&locator, expected_backend, expected_fork), 0);
	UT_ASSERT_EQ(opens, 0);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(shared_heap_uses_bound_storage);
	UT_RUN(shared_index_uses_exact_locator);
	UT_RUN(shared_forks_use_same_provider);
	UT_RUN(absent_shared_fork_remains_zero);
	UT_RUN(shared_provider_error_is_not_zero);
	UT_RUN(shared_large_fork_does_not_overflow_int32);
	UT_RUN(local_segments_preserve_actual_bytes);
	UT_RUN(temp_relation_preserves_md_path);
	UT_RUN(missing_local_fork_remains_zero);
	UT_RUN(local_permission_error_is_not_zero);
	UT_RUN(no_storage_locator_does_not_open_smgr);
	UT_DONE();
	return ut_failed_count != 0;
}
