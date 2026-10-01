/*-------------------------------------------------------------------------
 *
 * test_cluster_hw_remaster_retry.c
 *    Unit tests for spec-4.6a same-episode HW remaster retry decisions.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_hw_remaster_retry.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_hw_remaster.h"
#include "access/xlog_internal.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_hw_snapshot.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_wal_state.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Actual authority selector, rebuild entry, terminal publication and result name. These fixtures
 * deliberately make the legacy snapshot unavailable and forbid any replay,
 * adoption write or shard opening. Shared mode must select the typed owner
 * without allocating or touching the retired snapshot route. */
static bool active = true, recoverable = true;
bool cluster_shared_config;
char *cluster_shared_data_dir = "/fixture/shared";
int cluster_node_id;
static unsigned snapshot_reads, allocations, blocked, done;
static ClusterHwRemasterResult terminal;
static uint64 deadline;
static void *
test_alloc(Size size)
{
	allocations++;
	return malloc(size);
}
#define cluster_conf_node_count() (active ? 4 : 1)
#define cluster_hw_remaster_recoverable() recoverable
#define cluster_hw_bump_failclosed() ((void)0)
#define cluster_wal_thread_id_for(cluster, node) ((uint16)((node) + 1))
#define palloc(size) test_alloc(size)
#define pfree(ptr) free(ptr)
#define cluster_hw_snapshot_read(...) (snapshot_reads++, CLUSTER_HW_SNAPSHOT_INVALID_SHORT)
#define cluster_grd_lookup_master(...) (abort(), 0)
#define cluster_hw_apply_hwm(...) abort()
#define cluster_r4_bit22_cutover_active() false
#define cluster_control_root_read_canonical_dead_origin(...) (abort(), (ClusterControlRootResult)0)
#define cluster_wal_state_read_slot(...) (abort(), CLUSTER_WAL_SLOT_OK)
#define cluster_thread_recovery_validated_end(...) (abort(), CLUSTER_THREADREC_DONE)
#define hw_scan_reserve_tail(...) (abort(), false)
#define cluster_hw_snapshot_adoption_write() abort()
#define cluster_grd_redeclare_episode_epoch() (abort(), UINT64_C(0))
#define cluster_grd_shard_phase(...) (abort(), GRD_SHARD_REBUILDING)
#define cluster_grd_shard_master(...) (abort(), 0)
#define cluster_hw_mark_shard_rebuilt(...) abort()
#define cluster_hw_remaster_set_next_attempt_at(node, value) (deadline = (value))
#define cluster_hw_remaster_set_result(node, value) (terminal = (value))
#define cluster_hw_bump_remaster_done() (done++)
#define cluster_hw_bump_remaster_blocked() (blocked++)
#define cluster_hw_remaster_attempts(node) UINT32_C(0)
#define hw_remaster_next_attempt_deadline(attempts) ((void)(attempts), UINT64_C(7))
#undef ereport
#define ereport(level, rest) ((void)0)
#include "test_cluster_hw_remaster_native.inc"

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static ClusterHwRemasterRelaunchDecision
decide(uint64 launched, uint64 episode, ClusterHwRemasterResult result, uint32 attempts,
	   uint64 next_attempt_at, uint64 now, int max_attempts)
{
	return cluster_hw_remaster_relaunch_decide(launched, episode, result, attempts, next_attempt_at,
											   now, max_attempts);
}

UT_TEST(test_new_episode_initial_launch_resets_state)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(10, 11, CLUSTER_HW_REMASTER_BLOCKED, 7, 1234, 2000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_INITIAL);
	UT_ASSERT_EQ(d.next_attempts, 0);
	UT_ASSERT_EQ(d.next_attempt_at, 0);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_RUNNING);
}

UT_TEST(test_running_done_and_not_applicable_do_not_relaunch)
{
	UT_ASSERT_EQ(decide(11, 11, CLUSTER_HW_REMASTER_RUNNING, 0, 0, 2000, 16).action,
				 CLUSTER_HW_REMASTER_LAUNCH_SKIP);
	UT_ASSERT_EQ(decide(11, 11, CLUSTER_HW_REMASTER_DONE, 0, 0, 2000, 16).action,
				 CLUSTER_HW_REMASTER_LAUNCH_SKIP);
	UT_ASSERT_EQ(decide(11, 11, CLUSTER_HW_REMASTER_NOT_APPLICABLE, 0, 0, 2000, 16).action,
				 CLUSTER_HW_REMASTER_LAUNCH_SKIP);
}

UT_TEST(test_structural_blocked_warns_once_and_never_retries)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL, 0, 0, 2000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_MARK_STRUCTURAL);
	UT_ASSERT_EQ(d.next_attempt_at, CLUSTER_HW_REMASTER_NO_DEADLINE);

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED_STRUCTURAL, 0, CLUSTER_HW_REMASTER_NO_DEADLINE,
			   3000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);
}

UT_TEST(test_blocked_waits_until_backoff_deadline)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 0, 5000, 4999, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 0, 5000, 5000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_RETRY);
	UT_ASSERT_EQ(d.next_attempts, 1);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_RUNNING);
}

UT_TEST(test_cap_exhausts_once_and_sighup_raise_recovers)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 2, 0, 5000, 2);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED);
	UT_ASSERT_EQ(d.next_attempt_at, CLUSTER_HW_REMASTER_NO_DEADLINE);

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 2, CLUSTER_HW_REMASTER_NO_DEADLINE, 6000, 2);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);

	/* Operator raised cluster.hw_remaster_retry_max_attempts via SIGHUP. */
	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 2, CLUSTER_HW_REMASTER_NO_DEADLINE, 7000, 3);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_RETRY);
	UT_ASSERT_EQ(d.next_attempts, 3);
}

UT_TEST(test_zero_max_attempts_disables_retry)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_BLOCKED, 0, 0, 5000, 0);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_MARK_EXHAUSTED);
}

/* spec-4.6a section 2.1 truth-table row 8: latched == episode but the result
 * slot reads NONE (registration failed and was reverted) -> take the
 * first-launch row again; no attempt is charged. */
UT_TEST(test_none_result_registration_failed_falls_back_to_initial)
{
	ClusterHwRemasterRelaunchDecision d;

	d = decide(11, 11, CLUSTER_HW_REMASTER_NONE, 3, 999, 2000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_INITIAL);
	UT_ASSERT_EQ(d.next_attempts, 0);
	UT_ASSERT_EQ(d.next_attempt_at, 0);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_RUNNING);
}

UT_TEST(test_backoff_exponential_cap)
{
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 0), 1000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 1), 1000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 2), 2000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(1000, 7), 60000);
	UT_ASSERT_EQ(cluster_hw_remaster_compute_backoff_ms(100, 20), 60000);
}

UT_TEST(test_shared_rebuild_has_no_legacy_authority_or_io)
{
	active = recoverable = cluster_shared_config = true;
	snapshot_reads = allocations = 0;
	UT_ASSERT_EQ(cluster_hw_remaster_rebuild_origin(1, 12), CLUSTER_HW_REMASTER_NOT_APPLICABLE);
	UT_ASSERT(!cluster_hw_authority_active());
	UT_ASSERT_EQ(snapshot_reads, 0);
	UT_ASSERT_EQ(allocations, 0);
}

UT_TEST(test_canonical_nonparticipant_has_no_completion_or_retry_debt)
{
	ClusterHwRemasterRelaunchDecision d;
	blocked = done = 0;
	terminal = CLUSTER_HW_REMASTER_RUNNING;
	deadline = 0;
	deadline = CLUSTER_HW_REMASTER_NO_DEADLINE;
	hw_remaster_record_terminal(1, CLUSTER_HW_REMASTER_NOT_APPLICABLE);
	UT_ASSERT_EQ(terminal, CLUSTER_HW_REMASTER_NOT_APPLICABLE);
	UT_ASSERT_EQ(deadline, 0);
	UT_ASSERT_EQ(done, 0);
	UT_ASSERT_EQ(blocked, 0);
	d = decide(12, 12, terminal, 0, deadline, 60000, 16);
	UT_ASSERT_EQ(d.action, CLUSTER_HW_REMASTER_LAUNCH_SKIP);
	UT_ASSERT_EQ(d.next_result, CLUSTER_HW_REMASTER_NOT_APPLICABLE);
}

UT_TEST(test_unshared_and_inactive_keep_original_route)
{
	active = recoverable = true;
	cluster_shared_config = false;
	snapshot_reads = allocations = 0;
	UT_ASSERT_EQ(cluster_hw_remaster_rebuild_origin(1, 12), CLUSTER_HW_REMASTER_BLOCKED);
	UT_ASSERT_EQ(snapshot_reads, 1);
	UT_ASSERT_EQ(allocations, 1);
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_hw_remaster_rebuild_origin(cluster_node_id, 12),
				 CLUSTER_HW_REMASTER_NOT_APPLICABLE);
	active = false;
	UT_ASSERT_EQ(cluster_hw_remaster_rebuild_origin(1, 12), CLUSTER_HW_REMASTER_NOT_APPLICABLE);
	active = true;
	UT_ASSERT_EQ(snapshot_reads, 1);
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_new_episode_initial_launch_resets_state);
	UT_RUN(test_running_done_and_not_applicable_do_not_relaunch);
	UT_RUN(test_structural_blocked_warns_once_and_never_retries);
	UT_RUN(test_blocked_waits_until_backoff_deadline);
	UT_RUN(test_cap_exhausts_once_and_sighup_raise_recovers);
	UT_RUN(test_zero_max_attempts_disables_retry);
	UT_RUN(test_none_result_registration_failed_falls_back_to_initial);
	UT_RUN(test_backoff_exponential_cap);
	UT_RUN(test_shared_rebuild_has_no_legacy_authority_or_io);
	UT_RUN(test_canonical_nonparticipant_has_no_completion_or_retry_debt);
	UT_RUN(test_unshared_and_inactive_keep_original_route);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
