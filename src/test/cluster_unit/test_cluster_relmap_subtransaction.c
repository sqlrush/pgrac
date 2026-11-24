/*-------------------------------------------------------------------------
 * test_cluster_relmap_subtransaction.c
 *    Execute the real relmap entry before any child mapping is installed.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>

#include "../../backend/utils/cache/relmapper.c"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

ProcessingMode Mode = NormalProcessing;
bool cluster_shared_catalog;
static int nesting;
static bool parallel;
static int error_level, error_code;
static char error_message[256], error_hint[512];
static sigjmp_buf rejected;
static bool expect_error;
static unsigned errors;

int
GetCurrentTransactionNestLevel(void)
{
	return nesting;
}

bool
IsInParallelMode(void)
{
	return parallel;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (level < ERROR)
		return false;
	if (!expect_error)
		abort();
	error_level = level;
	error_code = ERRCODE_INTERNAL_ERROR;
	return true;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errcode(int code)
{
	error_code = code;
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	vsnprintf(error_message, sizeof(error_message), fmt, args);
	va_end(args);
	return 0;
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	vsnprintf(error_message, sizeof(error_message), fmt, args);
	va_end(args);
	return 0;
}

int
errhint(const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	vsnprintf(error_hint, sizeof(error_hint), fmt, args);
	va_end(args);
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	errors++;
	siglongjmp(rejected, 1);
}

static RelMapFile *maps[] = { &shared_map, &local_map, &active_shared_updates,
	&active_local_updates, &pending_shared_updates, &pending_local_updates };

static void
reset(void)
{
	unsigned i;

	for (i = 0; i < lengthof(maps); i++)
	{
		memset(maps[i], 0, sizeof(*maps[i]));
		maps[i]->magic = RELMAPPER_FILEMAGIC;
		maps[i]->num_mappings = 1;
		maps[i]->mappings[0].mapoid = 1259;
		maps[i]->mappings[0].mapfilenumber = 9000 + i;
	}
	Mode = NormalProcessing;
	nesting = 1;
	parallel = false;
	cluster_shared_catalog = true;
	expect_error = false;
	error_level = error_code = errors = 0;
	memset(error_message, 0, sizeof(error_message));
	memset(error_hint, 0, sizeof(error_hint));
}

static void
check_child(bool catalog, bool shared, bool immediate, bool existing)
{
	RelMapFile before[lengthof(maps)];
	unsigned i;

	reset();
	cluster_shared_catalog = catalog;
	nesting = immediate ? 2 : 3;
	for (i = 0; i < lengthof(maps); i++)
		before[i] = *maps[i];
	expect_error = true;
	if (sigsetjmp(rejected, 1) == 0)
		RelationMapUpdateMap(existing ? 1259 : 1249, 7777, shared, immediate);
	expect_error = false;
	UT_ASSERT_EQ(errors, 1);
	UT_ASSERT_EQ(error_level, ERROR);
	for (i = 0; i < lengthof(maps); i++)
		UT_ASSERT(memcmp(&before[i], maps[i], sizeof(before[i])) == 0);
#ifdef USE_PGRAC_CLUSTER
	if (catalog)
	{
		UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
		UT_ASSERT(strstr(error_hint, "top-level transaction") != NULL);
		UT_ASSERT(strstr(error_hint, "SAVEPOINT") != NULL);
		UT_ASSERT(strstr(error_hint, "PL/pgSQL EXCEPTION") != NULL);
	}
	else
#endif
	{
		UT_ASSERT_EQ(error_code, ERRCODE_INTERNAL_ERROR);
		UT_ASSERT(strcmp(error_message, "cannot change relation mapping within subtransaction") == 0);
		UT_ASSERT(error_hint[0] == '\0');
	}
}

UT_TEST(shared_children_reject_before_any_map_change)
{
	for (unsigned variant = 0; variant < 8; variant++)
		check_child(true, (variant & 1) != 0, (variant & 2) != 0, (variant & 4) != 0);
}

UT_TEST(private_children_keep_native_refusal_without_map_change)
{
	for (unsigned variant = 0; variant < 8; variant++)
		check_child(false, (variant & 1) != 0, (variant & 2) != 0, (variant & 4) != 0);
}

UT_TEST(top_and_bootstrap_updates_still_select_only_the_original_map)
{
	for (unsigned variant = 0; variant < 32; variant++)
	{
		RelMapFile before[lengthof(maps)];
		bool bootstrap = (variant & 1) != 0;
		bool shared = (variant & 2) != 0;
		bool immediate = (variant & 4) != 0;
		bool existing = (variant & 8) != 0;
		unsigned target;

		reset();
		cluster_shared_catalog = (variant & 16) != 0;
		Mode = bootstrap ? BootstrapProcessing : NormalProcessing;
		nesting = bootstrap ? 3 : 1;
		target = bootstrap ? (shared ? 0 : 1) : immediate ? (shared ? 2 : 3) : (shared ? 4 : 5);
		for (unsigned i = 0; i < lengthof(maps); i++)
			before[i] = *maps[i];
		RelationMapUpdateMap(existing ? 1259 : 1249, 7777, shared, immediate);
		if (existing)
			before[target].mappings[0].mapfilenumber = 7777;
		else
		{
			before[target].num_mappings = 2;
			before[target].mappings[1].mapoid = 1249;
			before[target].mappings[1].mapfilenumber = 7777;
		}
		UT_ASSERT_EQ(errors, 0);
		for (unsigned i = 0; i < lengthof(maps); i++)
			UT_ASSERT(memcmp(&before[i], maps[i], sizeof(before[i])) == 0);
	}
}

UT_TEST(parallel_top_still_refuses_before_map_change)
{
	for (unsigned variant = 0; variant < 8; variant++)
	{
		RelMapFile before[lengthof(maps)];

		reset();
		parallel = true;
		cluster_shared_catalog = (variant & 1) != 0;
		for (unsigned i = 0; i < lengthof(maps); i++)
			before[i] = *maps[i];
		expect_error = true;
		if (sigsetjmp(rejected, 1) == 0)
			RelationMapUpdateMap(1259, 7777, (variant & 2) != 0, (variant & 4) != 0);
		expect_error = false;
		UT_ASSERT_EQ(errors, 1);
		UT_ASSERT_EQ(error_code, ERRCODE_INTERNAL_ERROR);
		UT_ASSERT(strcmp(error_message, "cannot change relation mapping in parallel mode") == 0);
		for (unsigned i = 0; i < lengthof(maps); i++)
			UT_ASSERT(memcmp(&before[i], maps[i], sizeof(before[i])) == 0);
	}
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(shared_children_reject_before_any_map_change);
	UT_RUN(private_children_keep_native_refusal_without_map_change);
	UT_RUN(top_and_bootstrap_updates_still_select_only_the_original_map);
	UT_RUN(parallel_top_still_refuses_before_map_change);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
