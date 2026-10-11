/*-------------------------------------------------------------------------
 * test_cluster_checkpoint_wait_budget.c
 *   PGRAC: original checkpointer wait accounting with controlled clock and
 *   identity sources. No second implementation of the accounting algorithm.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_checkpoint_wait_budget.c
 * NOTES
 *   PGRAC unit fixture; time and external identity sources are controlled.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <time.h>
#include "postmaster/checkpointer.h"
#include "postmaster/interrupt.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static uint64 now_us, current_epoch, current_nonce;
static bool requested;
static ClusterNormalStopFailure failure;
static ClusterWalSourceRef current_ref;
static ClusterPhase1FullStopPlan plan;
static sigjmp_buf error_boundary;
AuxProcType MyAuxProcType = CheckpointerProcess;
bool cluster_shared_config = true;
volatile sig_atomic_t ShutdownRequestPending = true;
int cluster_clean_leave_drain_timeout_ms = 30000;

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

static int
fixture_clock_gettime(clockid_t clock, struct timespec *time)
{
	UT_ASSERT_EQ(clock, CLOCK_MONOTONIC);
	time->tv_sec = now_us / UINT64_C(1000000);
	time->tv_nsec = (now_us % UINT64_C(1000000)) * 1000;
	return 0;
}

uint64
cluster_epoch_get_current(void)
{
	return current_epoch;
}

bool
cluster_normal_stop_requested(void)
{
	return requested;
}

ClusterNormalStopFailure
cluster_normal_stop_failure(void)
{
	return failure;
}

void
cluster_normal_stop_fail(ClusterNormalStopFailure reason)
{
	if (failure == CLUSTER_NORMAL_STOP_FAILURE_NONE)
		failure = reason;
}

void
cluster_clean_leave_get_state(ClusterLeaveState *state)
{
	memset(state, 0, sizeof(*state));
	/* The normal-stop identity has a separate nonzero epoch; the legacy
	 * single-leave field remains zero throughout a full-cluster stop. */
	state->leave_epoch = 0;
	state->leaving_node_id = -1;
	pg_atomic_init_u32(&state->phase, CLUSTER_LEAVE_IDLE);
	pg_atomic_init_u32(&state->request_in_progress, 1);
	pg_atomic_init_u32(&state->shutdown_driven, 1);
	pg_atomic_init_u64(&state->leave_attempt_nonce, current_nonce);
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *ref)
{
	*ref = current_ref;
	return true;
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	UT_ASSERT(level == ERROR || level == LOG);
	return level == ERROR;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

int
errcode(int code)
{
	UT_ASSERT_EQ(code, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	siglongjmp(error_boundary, 1);
}

#define clock_gettime fixture_clock_gettime
#include "test_cluster_checkpoint_wait_budget.inc"
#undef clock_gettime

static void
reset_fixture(void)
{
	memset(&checkpoint_wait_budget, 0, sizeof(checkpoint_wait_budget));
	memset(&plan, 0, sizeof(plan));
	plan.valid = true;
	plan.epoch = current_epoch = 9;
	plan.attempt_nonce = current_nonce = 81;
	plan.absolute_deadline_us = UINT64_C(123000000);
	plan.own_wal_started_at = 71;
	for (int i = 0; i < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; ++i)
		plan.member_incarnations[i] = i + 101;
	memset(&current_ref, 0, sizeof(current_ref));
	current_ref.claim.identity.system_identifier = 77;
	current_ref.claim.identity.origin_thread_id = 1;
	current_ref.claim.identity.origin_owner_incarnation = 91;
	current_ref.timeline = 1;
	requested = ShutdownRequestPending = cluster_shared_config = true;
	MyAuxProcType = CheckpointerProcess;
	failure = CLUSTER_NORMAL_STOP_FAILURE_NONE;
	now_us = UINT64_C(1000000);
	cluster_clean_leave_drain_timeout_ms = 30000;
}

/* PGRAC: use the actual legacy policy to keep its epoch-zero contract
 * distinct from the current normal-stop plan. */
UT_TEST(shared_stop_budget_accepts_the_original_nonzero_epoch_plan)
{
	reset_fixture();
	UT_ASSERT(!cluster_clean_leave_phase1_full_stop_plan_valid(&plan));
	if (sigsetjmp(error_boundary, 1)) {
		UT_ASSERT(false);
		return;
	}
	cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
	UT_ASSERT(checkpoint_wait_budget.valid);
	UT_ASSERT_EQ(checkpoint_wait_budget.epoch, 9);
	UT_ASSERT_EQ(checkpoint_wait_budget.attempt_nonce, 81);
	cluster_checkpoint_wait_budget_end();
	UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
}

UT_TEST(flush_work_does_not_consume_the_wait_budget)
{
	uint64 started;
	reset_fixture();
	if (sigsetjmp(error_boundary, 1)) {
		UT_ASSERT(false);
		return;
	}
	cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
	started = now_us;
	now_us += UINT64_C(90000000);
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_charge(false, started, &current_ref, 9), 20);
	UT_ASSERT_EQ(checkpoint_wait_budget.accumulated_us, 0);
	now_us += UINT64_C(90000000);
	started = now_us;
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_charge(false, started, &current_ref, 9), 20);
	cluster_checkpoint_wait_budget_end();
	UT_ASSERT(!checkpoint_wait_budget.valid);
	UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
}

UT_TEST(prepare_and_publish_waits_share_one_accumulator)
{
	uint64 started;
	reset_fixture();
	if (sigsetjmp(error_boundary, 1)) {
		UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_DEADLINE);
		UT_ASSERT_EQ(checkpoint_wait_budget.accumulated_us, UINT64_C(30000000));
		return;
	}
	cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
	started = now_us;
	now_us += UINT64_C(5000000);
	(void)cluster_checkpoint_wait_budget_charge(true, started, &current_ref, 9);
	now_us += UINT64_C(8000000);
	(void)cluster_checkpoint_wait_budget_charge(false, now_us, &current_ref, 9);
	UT_ASSERT_EQ(checkpoint_wait_budget.accumulated_us, UINT64_C(13000000));
	UT_ASSERT(!checkpoint_wait_budget.active);
	now_us += UINT64_C(90000000); /* BufferSync is outside WAITING. */
	started = now_us;
	now_us += UINT64_C(17000000);
	(void)cluster_checkpoint_wait_budget_charge(true, started, &current_ref, 9);
	UT_ASSERT(false);
}

UT_TEST(wait_identity_clock_and_saturation_are_fail_closed)
{
	for (unsigned fault = 0; fault < 9; ++fault) {
		ClusterWalSourceRef original;
		uint64 started;
		reset_fixture();
		if (sigsetjmp(error_boundary, 1)) {
			UT_ASSERT(failure != CLUSTER_NORMAL_STOP_FAILURE_NONE);
			continue;
		}
		original = current_ref;
		cluster_checkpoint_wait_budget_begin(&plan, &original);
		started = now_us;
		(void)cluster_checkpoint_wait_budget_charge(true, started, &original, 9);
		switch (fault) {
		case 0:
			current_epoch++;
			break;
		case 1:
			current_nonce++;
			break;
		case 2:
			current_ref.claim.identity.origin_owner_incarnation++;
			break;
		case 3:
			requested = false;
			break;
		case 4:
			now_us = 0;
			break;
		case 5:
			now_us--;
			break;
		case 6:
			checkpoint_wait_budget.accumulated_us = UINT64_MAX;
			now_us++;
			break;
		case 7:
			checkpoint_wait_budget.valid = false;
			break;
		case 8:
			original.timeline++;
			break;
		}
		(void)cluster_checkpoint_wait_budget_charge(true, started, &original, 9);
		UT_ASSERT(false);
	}
}

UT_TEST(same_attempt_cannot_reset_budget_even_after_end)
{
	for (unsigned ended = 0; ended < 2; ++ended) {
		reset_fixture();
		if (sigsetjmp(error_boundary, 1)) {
			UT_ASSERT(failure != CLUSTER_NORMAL_STOP_FAILURE_NONE);
			continue;
		}
		cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
		if (ended)
			cluster_checkpoint_wait_budget_end();
		cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
		UT_ASSERT(false);
	}
}

UT_TEST(wait_timeout_is_clipped_to_the_remaining_balance)
{
	uint64 started;
	reset_fixture();
	if (sigsetjmp(error_boundary, 1)) {
		UT_ASSERT(false);
		return;
	}
	cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
	started = now_us;
	now_us += UINT64_C(29998500);
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_charge(true, started, &current_ref, 9), 2);
	now_us += 1000;
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_charge(false, now_us, &current_ref, 9), 1);
	cluster_checkpoint_wait_budget_end();
	UT_ASSERT_EQ(checkpoint_wait_budget.accumulated_us, UINT64_C(29999500));
}

UT_TEST(wait_balance_diagnostic_is_read_only_and_excludes_flush_work)
{
	ClusterCheckpointWaitBudget saved;
	uint64 started;
	reset_fixture();
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(now_us), UINT64_MAX);
	if (sigsetjmp(error_boundary, 1)) {
		UT_ASSERT(false);
		return;
	}
	cluster_checkpoint_wait_budget_begin(&plan, &current_ref);
	started = now_us;
	now_us += UINT64_C(5000000);
	saved = checkpoint_wait_budget;
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(started), UINT64_C(25000000));
	UT_ASSERT_EQ(memcmp(&saved, &checkpoint_wait_budget, sizeof(saved)), 0);
	(void)cluster_checkpoint_wait_budget_charge(true, started, &current_ref, 9);
	now_us += UINT64_C(8000000);
	saved = checkpoint_wait_budget;
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(now_us), UINT64_C(17000000));
	UT_ASSERT_EQ(memcmp(&saved, &checkpoint_wait_budget, sizeof(saved)), 0);
	(void)cluster_checkpoint_wait_budget_charge(false, now_us, &current_ref, 9);
	now_us += UINT64_C(90000000); /* Actual flush interval is not a wait. */
	started = now_us;
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(started), UINT64_C(17000000));
	now_us += UINT64_C(17000000);
	saved = checkpoint_wait_budget;
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(started), 0);
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(0), UINT64_MAX);
	UT_ASSERT_EQ(cluster_checkpoint_wait_budget_remaining_us(now_us + 1), UINT64_MAX);
	UT_ASSERT_EQ(memcmp(&saved, &checkpoint_wait_budget, sizeof(saved)), 0);
	UT_ASSERT_EQ(failure, CLUSTER_NORMAL_STOP_FAILURE_NONE);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(wait_balance_diagnostic_is_read_only_and_excludes_flush_work);
	UT_RUN(shared_stop_budget_accepts_the_original_nonzero_epoch_plan);
	UT_RUN(flush_work_does_not_consume_the_wait_budget);
	UT_RUN(prepare_and_publish_waits_share_one_accumulator);
	UT_RUN(wait_identity_clock_and_saturation_are_fail_closed);
	UT_RUN(same_attempt_cannot_reset_budget_even_after_end);
	UT_RUN(wait_timeout_is_clipped_to_the_remaining_balance);
	UT_DONE();
	return ut_failed_count != 0;
}
