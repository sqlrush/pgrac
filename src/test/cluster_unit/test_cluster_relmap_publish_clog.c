/* Original runtime publication -> original native observer and CLOG/SLRU.
 * Shmem, locks, owner inputs and WAL flush delivery remain controlled.
 * Only these three groups run; the included original92 do not execute.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
int relmap_publish_clog_original_main(void);
#define main relmap_publish_clog_original_main
#include "test_cluster_relmap_v2_storage.c"
#undef main
#include "test_cluster_relmap_publish_clog.h"

static bool publish_clog_drift, publish_clog_throw;
static sigjmp_buf native_commit_error;

static void
publish_clog_pending_unchanged(const ClusterRelmapV2Image *staged)
{
	ClusterRelmapV2View view;

	UT_ASSERT_EQ(writes + renames + file_syncs + dir_syncs, 0);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&staged->identity, &view),
		CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.has_pending && view.pending_complete);
	UT_ASSERT_EQ(view.pending_generation, staged->pending_generation);
	UT_ASSERT_EQ(view.committed_generation, staged->committed_generation);
	UT_ASSERT_EQ(view.pending_owner.top_full_xid, staged->pending_owner.top_full_xid);
	UT_ASSERT(stage_directory->pins[0].occupied && stage_directory->pins[0].may_be_staged);
}

static bool
publish_clog_owner_current(const ClusterRelmapOwnerSampleV2 *sample)
{
	TestRelmapPublishClogStats stats = test_relmap_publish_clog_stats();

	UT_ASSERT(!retention_held && !mapping_held && relmap_x_held);
	UT_ASSERT_EQ(writes + renames + file_syncs + dir_syncs, 0);
	if (publish_clog_drift && stats.owner_checks == 2) {
		UT_ASSERT(stats.wal_flushes == 1 && stats.file_syncs == 1 && stats.dir_syncs == 1);
		runtime_storage_writer.epoch++;
	}
	return cluster_relmap_owner_terminal_matches_v2(sample);
}

static bool
publish_clog_observe(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	return test_relmap_publish_clog_sync(sample, out, publish_clog_owner_current,
		publish_clog_throw ? &native_commit_error : NULL);
}

static ClusterRelmapV2Image
publish_clog_setup(ClusterRelmapOwnerSampleV2 *sample, ClusterRelmapV2Namespace *ns,
	unsigned origin, bool shared, XidStatus status, unsigned fault)
{
	runtime_storage_setup(sample, ns, origin, shared);
	/* Replace only the fixture's zero-effect preparation, through its original
	 * cancellation/begin APIs, before any possible pending write. */
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_cancel_prepare_v2(sample, ns, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT_EQ(runtime_storage_assignments, 1);
	UT_ASSERT_EQ(runtime_storage_binding_reads, 1);
	runtime_storage_assignments = runtime_storage_binding_reads = 0;
	runtime_storage_writer.epoch = 7;
	UT_ASSERT(cluster_relmap_owner_capture_v2(sample));
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_begin_v2(sample, ns, 8),
		CLUSTER_RELMAP_RETENTION_OK);
	writer_key.membership_epoch = sample->owner.membership_epoch;
	UT_ASSERT(runtime_storage_stage(sample, ns));
	test_relmap_publish_clog_setup(root, sample, status, fault);
	publish_clog_drift = publish_clog_throw = false;
	test_relmap_native_commit_calls = 0;
	test_relmap_native_commit_hook = publish_clog_observe;
	runtime_storage_reset_terminal_io(true);
	return runtime_storage_staged_image(sample, ns);
}

static void
publish_clog_idle(TestRelmapPublishClogStats stats)
{
	UT_ASSERT_EQ(stats.calls, 1);
	UT_ASSERT_EQ(stats.open_fds, 0);
	UT_ASSERT_EQ(stats.holdoff, 0);
	UT_ASSERT(!stats.locks_held && !stats.assertion_failed);
	UT_ASSERT_EQ(test_relmap_native_commit_calls, 1);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT_EQ(retention_acquires, retention_releases);
}

UT_TEST(original_native_commit_sync_precedes_both_pair_writes)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1)
		for (unsigned shared = 0; shared < 2; shared++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = publish_clog_setup(&sample, &ns, origin, shared,
			TRANSACTION_STATUS_COMMITTED, 0);
		ClusterRelmapRetentionPin pin = stage_directory->pins[0];
		ClusterRelmapV2View view;
		TestRelmapPublishClogStats stats;

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_publish_v2(&sample, &staged),
			CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		stats = test_relmap_publish_clog_done();
		publish_clog_idle(stats);
		UT_ASSERT_EQ(stats.owner_checks, 2);
		UT_ASSERT_EQ(stats.wal_flushes, 1);
		UT_ASSERT_EQ(stats.sync_requests, 1);
		UT_ASSERT_EQ(stats.file_syncs, 1);
		UT_ASSERT_EQ(stats.dir_syncs, 1);
		UT_ASSERT_EQ(stats.disk_status, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&ns, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(!view.has_pending && view.committed_generation == staged.pending_generation);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
	}
}

UT_TEST(original_clog_noncommit_and_durability_refusals_keep_pending)
{
	for (unsigned cut = 0; cut < 9; cut++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		XidStatus status = cut == 0 ? TRANSACTION_STATUS_ABORTED
			: cut == 1 ? TRANSACTION_STATUS_IN_PROGRESS
			: cut == 2 ? TRANSACTION_STATUS_SUB_COMMITTED : TRANSACTION_STATUS_COMMITTED;
		ClusterRelmapV2Image staged = publish_clog_setup(&sample, &ns,
			cut % 2 ? 0 : CLUSTER_MAX_NODES - 1, cut % 2, status, cut < 3 ? 0 : cut - 2);
		ClusterRelmapRetentionPin pin = stage_directory->pins[0];
		TestRelmapPublishClogStats stats;

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_publish_v2(&sample, &staged),
			CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		stats = test_relmap_publish_clog_done();
		publish_clog_idle(stats);
		if (cut == 0) {
			UT_ASSERT_EQ(stats.disk_status, TRANSACTION_STATUS_ABORTED);
			UT_ASSERT_EQ(stats.wal_flushes, 1);
		} else if (cut < 3 || cut == 8) {
			UT_ASSERT_EQ(stats.wal_flushes + stats.sync_requests + stats.file_syncs + stats.dir_syncs, 0);
		}
		publish_clog_pending_unchanged(&staged);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
	}
}

UT_TEST(original_slru_error_and_late_owner_change_preserve_debt)
{
	for (unsigned error = 0; error < 2; error++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = publish_clog_setup(&sample, &ns, 0, true,
			TRANSACTION_STATUS_COMMITTED, error ? 7 : 0);
		ClusterRelmapRetentionPin pin = stage_directory->pins[0];
		volatile bool caught = false;
		ClusterRelmapV2ClearResult result = CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
		TestRelmapPublishClogStats stats;

		publish_clog_drift = !error;
		publish_clog_throw = error;
		if (sigsetjmp(native_commit_error, 0) == 0)
			result = cluster_relmap_retention_runtime_publish_v2(&sample, &staged);
		else
			caught = true;
		UT_ASSERT_EQ(caught, error != 0);
		UT_ASSERT_EQ(result, CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		stats = test_relmap_publish_clog_done();
		publish_clog_idle(stats);
		publish_clog_pending_unchanged(&staged);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(original_native_commit_sync_precedes_both_pair_writes);
	UT_RUN(original_clog_noncommit_and_durability_refusals_keep_pending);
	UT_RUN(original_slru_error_and_late_owner_change_preserve_debt);
	cleanup();
	UT_DONE();
	return ut_failed_count != 0;
}
