/* A real local promise and original READY cohort remain observations until
 * complete original history sources can back an operation-owned handle.
 * Author: SqlRush <sqlrush@gmail.com> */
int k_protection_report_main(void);
#define main k_protection_report_main
#include "test_cluster_undo_horizon.c"
#undef main
#include "utils/resowner.h"

/* A controlled backend ownership boundary, not history-source authority.
 * No resource can be registered on the missing-source refusal path. */
static int k_protection_owner;
ResourceOwner CurrentResourceOwner = (ResourceOwner)&k_protection_owner;

static void
k_protection_cohort(ClusterHeapFreezeCohortObservation *saved,
	UndoHorizonV2MaintenanceObservation *observed)
{
	(void)reset_v2_cohort(false);
	native_history_reset();
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(saved, NULL, observed),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(saved->source_local_horizon_scn, native_history_clock);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, native_history_clock);
}

UT_TEST(k_ready_cohort_cannot_create_protection_without_joint_sources)
{
	ClusterHeapFreezeCohortObservation saved;
	UndoHorizonV2MaintenanceObservation observed;
	ClusterHeapHistoryProtectionV1 *protection = NULL;
	unsigned clocks;
	SCN floor;

	k_protection_cohort(&saved, &observed);
	clocks = native_history_clocks;
	floor = procArray->cluster_read_scn_floor;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&saved),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(cluster_heap_history_protection_acquire_v1(&saved, &protection),
		CLUSTER_HEAP_HISTORY_NOT_READY);
	UT_ASSERT(protection == NULL);
	UT_ASSERT_EQ(native_history_clocks, clocks);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, floor);
	UT_ASSERT(cluster_undo_retention_horizon_covers(floor));
	UT_ASSERT(!native_history_locked && !native_xid_locked);
	native_history_enabled = false;
}

UT_TEST(k_leaving_recovery_does_not_reconstruct_history_source_coverage)
{
	ClusterHeapFreezeCohortObservation saved;
	UndoHorizonV2MaintenanceObservation observed;
	ClusterHeapHistoryProtectionV1 *protection = NULL;
	SCN floor;

	k_protection_cohort(&saved, &observed);
	floor = procArray->cluster_read_scn_floor;
	native_history_recovery = true;
	UT_ASSERT(!cluster_undo_retention_horizon_covers(floor));
	native_history_recovery = false;
	UT_ASSERT(cluster_undo_retention_horizon_covers(floor));
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&saved),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(cluster_heap_history_protection_acquire_v1(&saved, &protection),
		CLUSTER_HEAP_HISTORY_NOT_READY);
	UT_ASSERT(protection == NULL);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, floor);
	native_history_enabled = false;
}

UT_TEST(k_absent_protection_never_passes_prepare_or_wal_final_check)
{
	ClusterHeapHistoryProtectionV1 *protection = NULL;

	UT_ASSERT(!cluster_heap_history_protection_current_v1(protection));
	UT_ASSERT(!cluster_heap_history_protection_prepare_v1(protection));
	UT_ASSERT(!cluster_heap_history_protection_final_v1(protection));
	cluster_heap_history_protection_release_v1(&protection);
	UT_ASSERT(protection == NULL);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(k_ready_cohort_cannot_create_protection_without_joint_sources);
	UT_RUN(k_leaving_recovery_does_not_reconstruct_history_source_coverage);
	UT_RUN(k_absent_protection_never_passes_prepare_or_wal_final_check);
	UT_DONE();
	free(procArray);
	return ut_failed_count != 0;
}
