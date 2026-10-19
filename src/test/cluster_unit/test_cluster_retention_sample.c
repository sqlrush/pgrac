/*-------------------------------------------------------------------------
 * test_cluster_retention_sample.c
 *    Execute the native retention scan and its scalar compatibility wrapper.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_retention_sample.c
 *
 * Actual ProcArray type and product bodies are extracted verbatim. Shared
 * memory, clock and lock scheduling are explicit boundary fixtures.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_undo_retention.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "test_cluster_pgproc_active_types.inc"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
static ProcArrayStruct *procArray;
static PGPROC entries[8];
static PGPROC *allProcs = entries;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static SCN sample_clock;
static unsigned acquires, releases, clock_reads, lock_errors;
static bool held, change_after_unlock;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (held || lock != ProcArrayLock || mode != LW_EXCLUSIVE)
		lock_errors++;
	held = true;
	acquires++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (!held || lock != ProcArrayLock)
		lock_errors++;
	held = false;
	releases++;
	if (change_after_unlock) {
		sample_clock = 900;
		entries[0].pid = 0;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 50);
	}
}

SCN
cluster_scn_current(void)
{
	if (!held || clock_reads != 0)
		lock_errors++;
	clock_reads++;
	return sample_clock;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return scn_local(a) < scn_local(b) ? -1 : scn_local(a) > scn_local(b) ? 1 : 0;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

#include "test_cluster_retention_sample.inc"

static void
reset_fixture(void)
{
	free(procArray);
	procArray = calloc(1, sizeof(ProcArrayStruct) + lengthof(entries) * sizeof(int));
	if (procArray == NULL)
		abort();
	procArray->maxProcs = lengthof(entries);
	MemSet(entries, 0, sizeof(entries));
	for (unsigned i = 0; i < lengthof(entries); i++) {
		procArray->pgprocnos[i] = i;
		entries[i].pid = 100 + i;
		pg_atomic_init_u64(&entries[i].cluster_read_scn_atomic, InvalidScn);
	}
	cluster_node_id = 0;
	sample_clock = 700;
	acquires = releases = clock_reads = lock_errors = 0;
	held = change_after_unlock = false;
}

static bool
empty_sample(const ClusterUndoRetentionSample *sample)
{
	return sample->horizon_scn == InvalidScn && sample->clock_scn == InvalidScn
		   && sample->contributor_kind == CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN
		   && sample->contributor_token == 0;
}

UT_TEST(test_empty_array_has_one_prescan_clock)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 700);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_all_database_and_vacuum_snapshot_minima_are_included)
{
	for (int oldest = 0; oldest < 3; oldest++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		procArray->numProcs = 3;
		entries[0].databaseId = 999;
		entries[1].statusFlags = PROC_IN_VACUUM;
		for (int i = 0; i < 3; i++)
			pg_atomic_write_u64(&entries[i].cluster_read_scn_atomic, i == oldest ? 400 : 500);
		UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 400);
		UT_ASSERT_EQ(sample.clock_scn, 700);
		UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		UT_ASSERT_EQ(sample.contributor_token, oldest + 1);
		UT_ASSERT_EQ(lock_errors, 0);
	}
}

UT_TEST(test_equal_time_prefers_snapshot_without_changing_scalar_bits)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic,
						(UINT64CONST(4) << SCN_NODE_ID_SHIFT) | sample_clock);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, sample_clock);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
}

UT_TEST(test_prepared_without_read_scn_does_not_invent_a_pin)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	entries[0].pid = 0;
	entries[0].xid = 81;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
}

UT_TEST(test_unexplained_read_scn_is_unknown_and_equal_snapshot_wins)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	entries[0].pid = 0;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	clock_reads = 0;
	procArray->numProcs = 2;
	pg_atomic_write_u64(&entries[1].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 2);
}

UT_TEST(test_missing_read_scn_clears_partial_minimum)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 2;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	entries[1].xmin = 80;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_invalid_clock_never_borrows_a_snapshot_time)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	sample_clock = InvalidScn;
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(releases, 1);
}

UT_TEST(test_unlock_cannot_change_sample_or_scalar_wrapper)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	change_after_unlock = true;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	change_after_unlock = true;
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), 400);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_null_output_and_disabled_cluster_do_not_scan)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	UT_ASSERT(!cluster_undo_retention_horizon_sample(NULL));
	cluster_node_id = -1;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
	UT_ASSERT_EQ(acquires, 0);
	UT_ASSERT_EQ(clock_reads, 0);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_empty_array_has_one_prescan_clock);
	UT_RUN(test_all_database_and_vacuum_snapshot_minima_are_included);
	UT_RUN(test_equal_time_prefers_snapshot_without_changing_scalar_bits);
	UT_RUN(test_prepared_without_read_scn_does_not_invent_a_pin);
	UT_RUN(test_unexplained_read_scn_is_unknown_and_equal_snapshot_wins);
	UT_RUN(test_missing_read_scn_clears_partial_minimum);
	UT_RUN(test_invalid_clock_never_borrows_a_snapshot_time);
	UT_RUN(test_unlock_cannot_change_sample_or_scalar_wrapper);
	UT_RUN(test_null_output_and_disabled_cluster_do_not_scan);
	free(procArray);
	UT_DONE();
	return ut_failed_count != 0;
}
