/*-------------------------------------------------------------------------
 * test_cluster_service_profile.c
 *   Receiver service timing and attribution cleanup tests.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_service_profile.c
 * NOTES
 *   Extracts the unchanged production timing wrappers. Service work is a
 *   controlled boundary; ERROR uses PostgreSQL's actual PG_TRY/PG_FINALLY.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "portability/instr_time.h"
static int clock_calls;
static int64 now_ns;
#undef INSTR_TIME_SET_CURRENT
#define INSTR_TIME_SET_CURRENT(t) (clock_calls++, errno = ERANGE, (t).ticks = now_ns)
#include "cluster/cluster_xnode_profile.h"
#include "cluster/cluster_cr_server.h"
#include "storage/condition_variable.h"
#include "utils/wait_event.h"
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_xnode_profile_enabled;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl;
ClusterXpService cluster_xp_current_service;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}
void
ExceptionalCondition(const char *c, const char *f, int l)
{
	abort();
}

/* Only these two fields are inspected by the real timing wrapper. */
#define GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE 1
typedef struct GcsBlockR4TxOriginContext {
	int domain;
	bool undo_data_fetch;
} GcsBlockR4TxOriginContext;
static void cr_serve_slot(ClusterLmsCrSlot *slot);
static bool fail_work;
static bool nest_work;
static int work_calls;
static ClusterXpService observed_service;

static void
cr_serve_slot_traced(ClusterLmsCrSlot *slot)
{
	work_calls++;
	observed_service = cluster_xp_current_service;
	now_ns += 500;
	if (fail_work) {
		errno = EIO;
		siglongjmp(*PG_exception_stack, 1);
	}
}

static void
gcs_block_r4_tx_origin_step(GcsBlockR4TxOriginContext *context)
{
	if (nest_work) {
		ClusterLmsCrSlot slot = { 0 };

		slot.req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT;
		cr_serve_slot(&slot);
		UT_ASSERT_EQ(cluster_xp_current_service, CLXP_SERVICE_R4_TX);
	}
	cr_serve_slot_traced(NULL);
}

static int wait_error;
static long seen_timeout;
static uint32 seen_wait_event;
static bool wait_expired;

bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 wait_event)
{
	work_calls++;
	seen_timeout = timeout;
	seen_wait_event = wait_event;
	now_ns += 700;
	if (wait_error != 0)
		siglongjmp(*PG_exception_stack, 1);
	return wait_expired;
}

void
ConditionVariableSleep(ConditionVariable *cv, uint32 wait_event)
{
	(void)ConditionVariableTimedSleep(cv, -1, wait_event);
}

#include "test_cluster_service_profile.inc"

static ClusterXnodeProfileShared profile;
static void
reset_profile(void)
{
	memset(&profile, 0, sizeof(profile));
	ClusterXnodeProfileCtl = &profile;
	cluster_xp_current_service = CLXP_SERVICE_NONE;
	cluster_xnode_profile_enabled = true;
	clock_calls = work_calls = 0;
	now_ns = 100;
	fail_work = nest_work = false;
}

UT_TEST(test_verdict_service_exact_and_off_no_clock)
{
	ClusterLmsCrSlot slot = { 0 };

	reset_profile();
	slot.req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT;
	cr_serve_slot(&slot);
	UT_ASSERT_EQ(work_calls, 1);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_UNDO_VERDICT);
	UT_ASSERT_EQ(cluster_xp_current_service, CLXP_SERVICE_NONE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[CLXP_UNDO_VERDICT_SERVICE].n_events), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[CLXP_UNDO_VERDICT_SERVICE].total_nanos), 500);
	UT_ASSERT_EQ(clock_calls, 2);
	/* Related wire families are separate services, never verdict samples. */
	slot.req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT;
	cr_serve_slot(&slot);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_NONE);
	slot.req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_FETCH;
	cr_serve_slot(&slot);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_NONE);
	UT_ASSERT_EQ(clock_calls, 2);
	slot.req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT;
	work_calls = 1;
	cluster_xnode_profile_enabled = false;
	cr_serve_slot(&slot);
	UT_ASSERT_EQ(work_calls, 2);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_NONE);
	UT_ASSERT_EQ(clock_calls, 2);
}

UT_TEST(test_origin_active_steps_exclude_other_domains_and_restore_nested_service)
{
	GcsBlockR4TxOriginContext context = { .domain = GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE };

	reset_profile();
	nest_work = true;
	gcs_block_r4_tx_origin_step_profiled(&context);
	UT_ASSERT_EQ(work_calls, 2);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_R4_TX);
	UT_ASSERT_EQ(cluster_xp_current_service, CLXP_SERVICE_NONE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[CLXP_R4_TX_SERVICE].n_events), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[CLXP_R4_TX_SERVICE].total_nanos), 1000);
	UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[CLXP_UNDO_VERDICT_SERVICE].n_events), 1);
	nest_work = false;
	context.undo_data_fetch = true;
	gcs_block_r4_tx_origin_step_profiled(&context);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_NONE);
	context.undo_data_fetch = false;
	context.domain = 2;
	gcs_block_r4_tx_origin_step_profiled(&context);
	UT_ASSERT_EQ(observed_service, CLXP_SERVICE_NONE);
	UT_ASSERT_EQ(clock_calls, 4);
}

UT_TEST(test_service_error_propagates_and_releases_only_diagnostic_context)
{
	ClusterLmsCrSlot slot = { .req_kind = CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT };
	GcsBlockR4TxOriginContext context = { .domain = GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE };
	int which;

	for (which = 0; which < 2; which++) {
		volatile bool caught = false;
		ClusterXnodeBucket bucket = which == 0 ? CLXP_UNDO_VERDICT_SERVICE : CLXP_R4_TX_SERVICE;

		reset_profile();
		cluster_xp_current_service = CLXP_SERVICE_UNDO_VERDICT;
		fail_work = true;
		PG_TRY();
		{
			if (which == 0)
				cr_serve_slot(&slot);
			else
				gcs_block_r4_tx_origin_step_profiled(&context);
		}
		PG_CATCH();
		{
			caught = true;
			UT_ASSERT_EQ(errno, EIO);
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(cluster_xp_current_service, CLXP_SERVICE_UNDO_VERDICT);
		UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[bucket].n_events), 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[bucket].total_nanos), 500);
		UT_ASSERT_EQ(clock_calls, 2);
		UT_ASSERT_EQ(work_calls, 1);
	}
}

UT_TEST(test_wait_profiles_preserve_calls_and_observe_timeout_error_cancel)
{
	ConditionVariable cv;
	int outcome;

	{
		for (outcome = 0; outcome < 4; outcome++) {
			ClusterXnodeBucket bucket = CLXP_PCM_EXECUTOR_WAIT;
			volatile bool caught = false;

			reset_profile();
			wait_error = outcome == 2 ? ERRCODE_INTERNAL_ERROR
									  : (outcome == 3 ? ERRCODE_QUERY_CANCELED : 0);
			wait_expired = outcome == 1;
			PG_TRY();
			{
				pcm_profile_compatible_wait(&cv, 123, bucket);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, outcome >= 2);
			UT_ASSERT_EQ(work_calls, 1);
			UT_ASSERT_EQ(seen_timeout, 123);
			UT_ASSERT_EQ(seen_wait_event, WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
			UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[bucket].n_events), 1);
			UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[bucket].total_nanos), 700);
		}
	}
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_verdict_service_exact_and_off_no_clock);
	UT_RUN(test_origin_active_steps_exclude_other_domains_and_restore_nested_service);
	UT_RUN(test_service_error_propagates_and_releases_only_diagnostic_context);
	UT_RUN(test_wait_profiles_preserve_calls_and_observe_timeout_error_cancel);
	UT_DONE();
	return ut_failed_count != 0;
}
