/* Original Start and installed-writer bodies behind an isolated test bridge.
 * Native binding, claim, fence and ResourceOwner leaves remain controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_original_start_namespace.h"
#define TEST_CLUSTER_STARTUP_RELMAP_TERMINAL 1
int d3_terminal_start_fixture_main(void);
#define main d3_terminal_start_fixture_main
#include "test_cluster_cold_tx_start.c"
#undef main
#include "test_cluster_relmap_original_start.h"

const struct ClusterRecoveryStartV1 *
test_relmap_start_begin(ClusterColdTypedV1 *input, int pid)
{
	ClusterColdTypedV1 *borrowed = input;

	reset();
	MyProcPid = pid;
	/* Original reset installs a distinct, fully identified successor. Old
	 * replay-source tokens do not stand in for that installed writer. */
	input->top_tx_census = &census;
	census.plan = input->plan;
	cluster_recovery_cold_inputs_retain(&borrowed);
	cluster_recovery_cold_inputs_handoff(&info);
	if (!ClusterBeginColdRecoveryTx(&info) || ut_failed_count != 0)
		return NULL;
	return info.cluster_tx_start;
}
bool test_relmap_start_current(const struct ClusterRecoveryStartV1 *start)
{ return ClusterColdRecoveryRelmapCurrent(start); }
void test_relmap_start_fail(const struct ClusterRecoveryStartV1 *start)
{ ClusterColdRecoveryRelmapFail(start); }
const ClusterColdTypedV1 *test_relmap_start_inputs(const struct ClusterRecoveryStartV1 *start)
{ return ClusterColdRecoveryTxInputs(start); }
bool test_relmap_start_owner(const struct ClusterRecoveryStartV1 *start,
	ClusterRecoveryCheckpointOwnerV1 *owner)
{ return ClusterColdRecoveryTxCheckpointOwnerV1(start, owner); }
bool test_relmap_start_writer(ClusterWalWriterToken *writer, uint8 operation[16], uint64 *generation)
{ return ClusterStartupInstalledRecoveryWriterV1(writer, operation, generation); }
bool test_relmap_start_distinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes)
{ return cluster_recovery_plan_output_distinct_v1(start, out, bytes); }
bool test_relmap_start_pending(void)
{
	return window_region.checkpoint_owner.relmap_required
		&& window_region.checkpoint_owner.tx_accepted_generation == 0
		&& window_region.checkpoint_owner.ctrc_accepted_generation == 0
		&& ut_failed_count == 0;
}
void test_relmap_start_fault(unsigned fault)
{
	switch (fault) {
	case 0: clusterStartupWriterInstalled = false; break;
	case 1: observed_writer.epoch++; break;
	case 2: clusterStartupWriter.generation++; break;
	case 3: clusterStartupWriter.operation_uuid[1]++; break;
	case 4: claim_serial++; break;
	default: abort();
	}
}
bool test_relmap_start_done(void)
{
	ClusterColdRecoveryRelmapFail(info.cluster_tx_start);
	ClusterReleaseColdRecoveryInputs(&info);
	return cluster_recovery_cold_start == NULL && cluster_recovery_cold_inputs == NULL
		&& ut_failed_count == 0;
}
