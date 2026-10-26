/* PGRAC: original runtime native-commit gate beside real dual-copy storage.
 * Native CLOG sync/qualification, COMPLETE and locks remain controlled leaves.
 * Only the three groups below execute; the included original92 do not run.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
int relmap_publish_native_original_main(void);
#define main relmap_publish_native_original_main
#include "test_cluster_relmap_v2_storage.c"
#undef main

static unsigned native_commit_fault;
static bool native_commit_may_throw;
static sigjmp_buf native_commit_error;

static bool
native_commit_observe(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	bool result;

	UT_ASSERT(!retention_held && !mapping_held && relmap_x_held);
	UT_ASSERT_EQ(writes + renames + file_syncs + dir_syncs, 0);
	/* Construct only the controlled native leaf; all original runtime/owner
	 * checks, pair writes/sync/readback and final gates execute unchanged. */
	result = test_relmap_native_commit_default(sample, out);
	switch (native_commit_fault) {
		case 1: return false;
		case 2: out->status = TRANSACTION_STATUS_ABORTED; break;
		case 3: out->status = TRANSACTION_STATUS_IN_PROGRESS; break;
		case 4: out->status = TRANSACTION_STATUS_SUB_COMMITTED; break;
		case 5: out->top_full_xid += UINT64CONST(0x100000000); break;
		case 6: out->bounds.system_identifier++; break;
		case 7: out->bounds.database_incarnation++; break;
		case 8: out->bounds.origin++; break;
		case 9: out->bounds.membership_epoch++; break;
		case 10: out->bounds.admitted_incarnation++; break;
		case 11: out->bounds.boot_incarnation++; break;
		case 12: out->bounds.native_floor = out->top_full_xid + 1; break;
		case 13: out->bounds.last_issued = out->top_full_xid - 1; break;
		case 14: runtime_storage_writer.epoch++; break;
		case 15: stage_directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN; break;
		case 16: stage_directory->pins[0].wal_end++; break;
		case 17:
			if (!native_commit_may_throw) abort();
			siglongjmp(native_commit_error, 1);
	}
	return result;
}

static ClusterRelmapV2Image
native_commit_setup(ClusterRelmapOwnerSampleV2 *sample, ClusterRelmapV2Namespace *ns,
	unsigned origin, bool shared, unsigned fault)
{
	runtime_storage_setup(sample, ns, origin, shared);
	UT_ASSERT(runtime_storage_stage(sample, ns));
	native_commit_fault = fault;
	native_commit_may_throw = false;
	test_relmap_native_commit_calls = 0;
	test_relmap_native_commit_hook = native_commit_observe;
	runtime_storage_reset_terminal_io(true);
	return runtime_storage_staged_image(sample, ns);
}

static void
native_commit_pending_unchanged(const ClusterRelmapV2Image *staged)
{
	ClusterRelmapV2View view;
	unsigned before = writes + renames + file_syncs + dir_syncs;

	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&staged->identity, &view),
		CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.has_pending && view.pending_complete);
	UT_ASSERT_EQ(view.pending_generation, staged->pending_generation);
	UT_ASSERT_EQ(view.committed_generation, staged->committed_generation);
	UT_ASSERT_EQ(view.pending_owner.top_full_xid, staged->pending_owner.top_full_xid);
	UT_ASSERT_EQ(before, 0);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT_EQ(retention_acquires, retention_releases);
	UT_ASSERT(!retention_held);
	UT_ASSERT(stage_directory->pins[0].occupied && stage_directory->pins[0].may_be_staged);
}

UT_TEST(native_commit_precedes_original_pair_publication_without_retiring_pins)
{
	for (unsigned origin = 0; origin < CLUSTER_MAX_NODES; origin += CLUSTER_MAX_NODES - 1)
		for (unsigned shared = 0; shared < 2; shared++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = native_commit_setup(&sample, &ns, origin, shared, 0);
		ClusterRelmapRetentionPin pin = stage_directory->pins[0];
		ClusterRelmapV2View view;

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_publish_v2(&sample, &staged),
			CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT_EQ(test_relmap_native_commit_calls, 1);
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&ns, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(!view.has_pending && view.committed_generation == staged.pending_generation);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
		UT_ASSERT_EQ(io_under_retention_lock, 0);
		UT_ASSERT_EQ(retention_acquires, retention_releases);
	}
	/* Native abort discards pending before RecordTransactionAbort: never add
	 * a terminal-CLOG requirement to that original earlier cleanup interval. */
	{
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = native_commit_setup(&sample, &ns, 0, false, 17);

		runtime_storage_reset_terminal_io(false);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_discard_pending_v2(&sample, &staged),
			CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT_EQ(test_relmap_native_commit_calls, 0);
		UT_ASSERT(stage_directory->pins[0].occupied);
	}
}

UT_TEST(native_commit_refusal_and_wrong_full_identity_leave_both_pending_copies)
{
	for (unsigned fault = 1; fault <= 13; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = native_commit_setup(&sample, &ns, fault % 2 ? 0 : CLUSTER_MAX_NODES - 1,
			fault % 2, fault);

		UT_ASSERT_EQ(cluster_relmap_retention_runtime_publish_v2(&sample, &staged),
			CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(test_relmap_native_commit_calls, 1);
		native_commit_pending_unchanged(&staged);
	}
}

UT_TEST(native_commit_late_drift_and_error_preserve_original_pending_debt)
{
	for (unsigned fault = 14; fault <= 17; fault++) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRelmapV2Namespace ns;
		ClusterRelmapV2Image staged = native_commit_setup(&sample, &ns, 0, true, fault);
		ClusterRelmapV2ClearResult result = CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
		volatile bool caught = false;

		if (sigsetjmp(native_commit_error, 0) == 0) {
			native_commit_may_throw = true;
			result = cluster_relmap_retention_runtime_publish_v2(&sample, &staged);
		} else
			caught = true;
		UT_ASSERT_EQ(caught, fault == 17);
		UT_ASSERT_EQ(result, CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(test_relmap_native_commit_calls, 1);
		native_commit_pending_unchanged(&staged);
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(native_commit_precedes_original_pair_publication_without_retiring_pins);
	UT_RUN(native_commit_refusal_and_wrong_full_identity_leave_both_pending_copies);
	UT_RUN(native_commit_late_drift_and_error_preserve_original_pending_debt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
