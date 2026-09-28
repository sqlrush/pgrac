/*-------------------------------------------------------------------------
 * test_cluster_config_delivery_route.c
 *    Execute the production ProcessConfigFile routing body, with parser and
 *    delivery boundaries isolated. Native delivery is exercised by TAP018.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/conffiles.h"
#include "utils/memutils.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shared_config.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config;
bool IsUnderPostmaster;
static unsigned local_reads, deliveries, contexts, deletes;
static bool deliverable;
MemoryContext CurrentMemoryContext;
static char parent_context, child_context;

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size minsize, Size initsize,
							  Size maxsize)
{
	UT_ASSERT(parent == (MemoryContext)&parent_context);
	contexts++;
	return (MemoryContext)&child_context;
}
void
MemoryContextDelete(MemoryContext context)
{
	UT_ASSERT(context == (MemoryContext)&child_context);
	deletes++;
}
static ConfigVariable *
ProcessConfigFileInternal(GucContext context, bool apply, int elevel)
{
	UT_ASSERT(apply);
	local_reads++;
	return NULL;
}
bool
cluster_shared_config_delivery_reload(void)
{
	deliveries++;
	return deliverable;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# %s %s:%d\n", condition, file, line);
	abort();
}
#include "test_cluster_config_delivery_route.inc"

UT_TEST(default_off_keeps_native_parser)
{
	cluster_shared_config = false;
	IsUnderPostmaster = false;
	ProcessConfigFile(PGC_POSTMASTER);
	IsUnderPostmaster = true;
	ProcessConfigFile(PGC_SIGHUP);
	UT_ASSERT_EQ(local_reads, 2);
	UT_ASSERT_EQ(deliveries, 0);
	UT_ASSERT_EQ(contexts, deletes);
}
UT_TEST(shared_startup_still_reads_bootstrap_input)
{
	cluster_shared_config = true;
	IsUnderPostmaster = false;
	ProcessConfigFile(PGC_POSTMASTER);
	UT_ASSERT_EQ(local_reads, 3);
	UT_ASSERT_EQ(deliveries, 0);
}
UT_TEST(shared_parent_reload_cannot_read_local_shadow)
{
	deliverable = true;
	IsUnderPostmaster = false;
	ProcessConfigFile(PGC_SIGHUP);
	UT_ASSERT_EQ(local_reads, 3);
	UT_ASSERT_EQ(deliveries, 1);
}
UT_TEST(shared_child_reload_cannot_read_local_shadow)
{
	IsUnderPostmaster = true;
	ProcessConfigFile(PGC_SIGHUP);
	UT_ASSERT_EQ(local_reads, 3);
	UT_ASSERT_EQ(deliveries, 2);
}
UT_TEST(missing_delivery_keeps_old_state_not_local_fallback)
{
	deliverable = false;
	ProcessConfigFile(PGC_SIGHUP);
	UT_ASSERT_EQ(local_reads, 3);
	UT_ASSERT_EQ(deliveries, 3);
	UT_ASSERT_EQ(contexts, deletes);
	UT_ASSERT(CurrentMemoryContext == (MemoryContext)&parent_context);
}
int
main(void)
{
	CurrentMemoryContext = (MemoryContext)&parent_context;
	UT_PLAN(5);
	UT_RUN(default_off_keeps_native_parser);
	UT_RUN(shared_startup_still_reads_bootstrap_input);
	UT_RUN(shared_parent_reload_cannot_read_local_shadow);
	UT_RUN(shared_child_reload_cannot_read_local_shadow);
	UT_RUN(missing_delivery_keeps_old_state_not_local_fallback);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
