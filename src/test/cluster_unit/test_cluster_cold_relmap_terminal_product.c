/* Original whole participant with isolated declared-plan/Start providers.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_cold_relmap_terminal_namespace.h"
#include "../../backend/cluster/cluster_cold_recovery_relmap.c"

/* Observe the exact receipt handoff for late-error injection. */
bool test_terminal_saved_native(const ClusterColdPlanV1 *plan);
bool test_terminal_saved_native(const ClusterColdPlanV1 *plan)
{
	const ColdRelmapTerminal *receipt = plan->relmap_terminal;
	return receipt != NULL && receipt->count != 0 && receipt->entries[0].native.held != NULL;
}
