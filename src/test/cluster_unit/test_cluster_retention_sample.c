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
#include "access/twophase.h"
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
static bool held, change_after_unlock, corrupt_count_after_lock;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (held || lock != ProcArrayLock || mode != LW_EXCLUSIVE)
		lock_errors++;
	held = true;
	acquires++;
	if (corrupt_count_after_lock)
		procArray->numProcs = -1;
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
	if (!held || clock_reads != acquires - 1)
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
	held = change_after_unlock = corrupt_count_after_lock = false;
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

UT_TEST(test_nonfinite_clock_cannot_borrow_a_snapshot_or_empty_array)
{
	for (int count = 0; count <= 1; count++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		sample_clock = (SCN) UINT64_MAX;
		procArray->numProcs = count;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(acquires, 1);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(clock_reads, 1);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_nonfinite_reader_cannot_be_ignored_in_a_finite_clock_cut)
{
	for (int has_xmin = 0; has_xmin <= 1; has_xmin++) {
		ClusterUndoRetentionSample sample;

		reset_fixture();
		procArray->numProcs = 2;
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
		entries[1].xmin = has_xmin ? 80 : InvalidTransactionId;
		pg_atomic_write_u64(&entries[1].cluster_read_scn_atomic, (SCN) UINT64_MAX);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_scalar_compatibility_never_publishes_a_nonfinite_clock)
{
	reset_fixture();
	sample_clock = (SCN) UINT64_MAX;
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(test_original_sample_recovers_after_nonfinite_source_is_replaced)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, (SCN) UINT64_MAX);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
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

UT_TEST(test_replication_slot_pin_without_backend_refuses_horizon)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->replication_slot_xmin = FirstNormalTransactionId + 17;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_catalog_slot_pin_cannot_borrow_backend_read_scn)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = 1;
	entries[0].xmin = FirstNormalTransactionId + 17;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 100);
	procArray->replication_slot_catalog_xmin = entries[0].xmin;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_any_noninvalid_slot_pin_requires_its_own_scn_proof)
{
	const TransactionId pins[] = {BootstrapTransactionId, FrozenTransactionId,
								  FirstNormalTransactionId, MaxTransactionId};
	ClusterUndoRetentionSample sample;

	for (unsigned i = 0; i < lengthof(pins); i++) {
		for (unsigned catalog = 0; catalog < 2; catalog++) {
			reset_fixture();
			if (catalog)
				procArray->replication_slot_catalog_xmin = pins[i];
			else
				procArray->replication_slot_xmin = pins[i];
			MemSet(&sample, 0xa5, sizeof(sample));
			UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
			UT_ASSERT(empty_sample(&sample));
			UT_ASSERT_EQ(acquires, 1);
			UT_ASSERT_EQ(releases, 1);
			UT_ASSERT_EQ(lock_errors, 0);
			UT_ASSERT(!held);
		}
	}
}

UT_TEST(test_horizon_resamples_after_both_slot_pins_are_removed)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->replication_slot_xmin = FirstNormalTransactionId + 17;
	procArray->replication_slot_catalog_xmin = FirstNormalTransactionId + 19;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	procArray->replication_slot_xmin = InvalidTransactionId;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	procArray->replication_slot_catalog_xmin = InvalidTransactionId;
	sample_clock = 800;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 800);
	UT_ASSERT_EQ(sample.clock_scn, 800);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	UT_ASSERT_EQ(acquires, 3);
	UT_ASSERT_EQ(releases, 3);
	UT_ASSERT_EQ(clock_reads, 3);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

/* The allocation contains eight entries even for an invalid logical count.
 * The parent can therefore run this RED without an out-of-bounds fixture. */
UT_TEST(test_invalid_procarray_counts_refuse_the_complete_h_observation)
{
	const int counts[][2] = {{-1, 8}, {2, 1}, {0, -1}};
	ClusterUndoRetentionSample sample;

	for (unsigned i = 0; i < lengthof(counts); i++) {
		reset_fixture();
		procArray->numProcs = counts[i][0];
		procArray->maxProcs = counts[i][1];
		pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 300);
		MemSet(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
		UT_ASSERT(empty_sample(&sample));
		UT_ASSERT_EQ(cluster_undo_retention_horizon(), InvalidScn);
		UT_ASSERT_EQ(acquires, 2);
		UT_ASSERT_EQ(releases, 2);
		UT_ASSERT_EQ(clock_reads, 2);
		UT_ASSERT_EQ(lock_errors, 0);
		UT_ASSERT(!held);
	}
}

UT_TEST(test_procarray_count_is_validated_inside_the_original_lock_cut)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	corrupt_count_after_lock = true;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_full_capacity_census_preserves_its_actual_snapshot_minimum)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = procArray->maxProcs;
	pg_atomic_write_u64(&entries[lengthof(entries) - 1].cluster_read_scn_atomic, 300);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 300);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, lengthof(entries));
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(clock_reads, 1);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
}

UT_TEST(test_valid_census_can_be_sampled_after_a_rejected_count)
{
	ClusterUndoRetentionSample sample;

	reset_fixture();
	procArray->numProcs = -1;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT(empty_sample(&sample));
	procArray->numProcs = 1;
	pg_atomic_write_u64(&entries[0].cluster_read_scn_atomic, 400);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	UT_ASSERT_EQ(acquires, 2);
	UT_ASSERT_EQ(releases, 2);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(lock_errors, 0);
	UT_ASSERT(!held);
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
	UT_PLAN(21);
	UT_RUN(test_empty_array_has_one_prescan_clock);
	UT_RUN(test_all_database_and_vacuum_snapshot_minima_are_included);
	UT_RUN(test_equal_time_prefers_snapshot_without_changing_scalar_bits);
	UT_RUN(test_prepared_without_read_scn_does_not_invent_a_pin);
	UT_RUN(test_unexplained_read_scn_is_unknown_and_equal_snapshot_wins);
	UT_RUN(test_missing_read_scn_clears_partial_minimum);
	UT_RUN(test_invalid_clock_never_borrows_a_snapshot_time);
	UT_RUN(test_nonfinite_clock_cannot_borrow_a_snapshot_or_empty_array);
	UT_RUN(test_nonfinite_reader_cannot_be_ignored_in_a_finite_clock_cut);
	UT_RUN(test_scalar_compatibility_never_publishes_a_nonfinite_clock);
	UT_RUN(test_original_sample_recovers_after_nonfinite_source_is_replaced);
	UT_RUN(test_unlock_cannot_change_sample_or_scalar_wrapper);
	UT_RUN(test_replication_slot_pin_without_backend_refuses_horizon);
	UT_RUN(test_catalog_slot_pin_cannot_borrow_backend_read_scn);
	UT_RUN(test_any_noninvalid_slot_pin_requires_its_own_scn_proof);
	UT_RUN(test_horizon_resamples_after_both_slot_pins_are_removed);
	UT_RUN(test_invalid_procarray_counts_refuse_the_complete_h_observation);
	UT_RUN(test_procarray_count_is_validated_inside_the_original_lock_cut);
	UT_RUN(test_full_capacity_census_preserves_its_actual_snapshot_minimum);
	UT_RUN(test_valid_census_can_be_sampled_after_a_rejected_count);
	UT_RUN(test_null_output_and_disabled_cluster_do_not_scan);
	free(procArray);
	UT_DONE();
	return ut_failed_count != 0;
}
