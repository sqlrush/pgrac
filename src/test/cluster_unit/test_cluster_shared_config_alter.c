/*-------------------------------------------------------------------------
 *
 * test_cluster_shared_config_alter.c
 *	  Shared ALTER SYSTEM and the parameters pg_control records.
 *
 *	  Runs the real cluster_shared_config_alter_system with its policy
 *	  table, extracted from cluster_shared_config_guc.c.  The eight
 *	  XLOG_PARAMETER_CHANGE parameters are refused with 55R07, in any case
 *	  and for RESET, before the CF precondition and before any
 *	  publication; other parameters keep the shared publication route.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shared_config_alter.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <stdarg.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_shared_config.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 1;
volatile sig_atomic_t InterruptPending = false;
struct Latch *MyLatch = NULL;

/* Boundaries the extracted code calls. */
static unsigned cf_checks, publications;
static ClusterSharedConfigEntry published;
static char published_name[64];

uint64
cluster_epoch_get_current(void)
{
	return 7;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 3;
}
bool
cluster_cf_held(LOCKMODE mode pg_attribute_unused())
{
	cf_checks++;
	return false;
}
ClusterControlRootResult
cluster_control_root_config_change(const ClusterSharedConfigEntry *change,
								   ClusterSharedConfigPublication *out pg_attribute_unused(),
								   ClusterSharedConfigPolicyReport *report pg_attribute_unused())
{
	publications++;
	published = *change;
	strlcpy(published_name, change->name, sizeof(published_name));
	published.name = published_name;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
char *
pg_server_to_any(const char *s, int len pg_attribute_unused(), int encoding pg_attribute_unused())
{
	return unconstify(char *, s);
}
void
ProcessInterrupts(void)
{}
void
ResetLatch(Latch *latch pg_attribute_unused())
{}
int
WaitLatch(Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}

/* Memory: this file allocates only short names. */
char *
pstrdup(const char *in)
{
	char *out = malloc(strlen(in) + 1);
	strcpy(out, in);
	return out;
}
void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

/* The report: ERROR returns to the test. */
static sigjmp_buf report_target;
static bool report_armed;
static int report_level, report_code;
static char report_message[512], report_detail[512], report_hint[512];

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	UT_ASSERT(report_armed && elevel == ERROR);
	report_level = elevel;
	return true;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
int
errcode(int sqlerrcode)
{
	report_code = sqlerrcode;
	return 0;
}
#define REPORT_TEXT(fn, buffer)                                                                    \
	int fn(const char *fmt, ...)                                                                   \
	{                                                                                              \
		va_list ap;                                                                                \
		va_start(ap, fmt);                                                                         \
		vsnprintf(buffer, sizeof(buffer), fmt, ap);                                                \
		va_end(ap);                                                                                \
		return 0;                                                                                  \
	}
REPORT_TEXT(errmsg, report_message)
REPORT_TEXT(errdetail, report_detail)
REPORT_TEXT(errhint, report_hint)
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	siglongjmp(report_target, 1);
}

#include "test_cluster_shared_config_alter.inc"

static void
reset(void)
{
	cf_checks = publications = 0;
	memset(&published, 0, sizeof(published));
	report_level = report_code = 0;
	report_message[0] = report_detail[0] = report_hint[0] = '\0';
}

/* Returns whether ALTER SYSTEM ended in ERROR. */
static bool
alter(const char *name, const char *value)
{
	report_armed = true;
	if (sigsetjmp(report_target, 1) != 0) {
		report_armed = false;
		return true;
	}
	cluster_shared_config_alter_system(name, value);
	report_armed = false;
	return false;
}

static const char *const recorded[] = { "wal_level",
										"wal_log_hints",
										"max_connections",
										"max_worker_processes",
										"max_wal_senders",
										"max_prepared_transactions",
										"max_locks_per_transaction",
										"track_commit_timestamp" };

UT_TEST(recorded_parameters_are_refused_before_any_publication)
{
	for (size_t i = 0; i < lengthof(recorded); i++) {
		char upper[64], expected[128];
		const char *names[2] = { recorded[i], upper };

		for (size_t c = 0; c <= strlen(recorded[i]); c++)
			upper[c] = (char)pg_toupper((unsigned char)recorded[i][c]);
		snprintf(expected, sizeof(expected), "parameter \"%s\" cannot be changed", recorded[i]);
		for (int n = 0; n < 2; n++) {
			for (int set = 0; set < 2; set++) {
				reset();
				UT_ASSERT(alter(names[n], set ? "300" : NULL));
				UT_ASSERT_EQ(report_code, ERRCODE_CLUSTER_SHARED_PARAMETER_FIXED);
				UT_ASSERT(strstr(report_message, expected) != NULL);
				UT_ASSERT(strstr(report_detail, "PGRAC_REASON=RECORDED_PARAMETER") != NULL);
				UT_ASSERT_EQ(cf_checks, 0);
				UT_ASSERT_EQ(publications, 0);
			}
		}
	}
}

UT_TEST(other_parameters_keep_the_shared_publication)
{
	reset();
	UT_ASSERT(!alter("work_mem", "12MB"));
	UT_ASSERT_EQ(publications, 1);
	UT_ASSERT_EQ(published.node_id, CLUSTER_SHARED_CONFIG_COMMON);
	UT_ASSERT(strcmp(published.name, "work_mem") == 0);
	reset();
	UT_ASSERT(!alter("Shared_Buffers", "64MB"));
	UT_ASSERT_EQ(publications, 1);
	UT_ASSERT_EQ(published.node_id, cluster_node_id);
	UT_ASSERT(strcmp(published.name, "shared_buffers") == 0);
	/* Names that only resemble a recorded parameter are not refused. */
	reset();
	UT_ASSERT(!alter("max_wal_size", "1GB"));
	UT_ASSERT_EQ(publications, 1);
}

UT_TEST(reset_all_is_still_unsupported)
{
	reset();
	UT_ASSERT(alter(NULL, NULL));
	UT_ASSERT_EQ(report_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(publications, 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(recorded_parameters_are_refused_before_any_publication);
	UT_RUN(other_parameters_keep_the_shared_publication);
	UT_RUN(reset_all_is_still_unsupported);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
