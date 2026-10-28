/* Author: SqlRush <sqlrush@gmail.com> */
/* Native cold-service entry guards for normal-start fixtures with no retained
 * cold input, checkpoint registration or merge claim. The guards and their
 * empty owner initializers are extracted from the product, not reimplemented.
 * Retained-cold service is outside this fixture: crossing any guard aborts.
 * No positive cold authority or optimizer dead-code elimination is required. */
#include "postgres.h"

#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_census.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_recovery_plan.h"
#include "utils/resowner.h"

/* These entry guards only inspect the shared pointer, not its layout. */
typedef struct ClusterRecoveryPlanShmem ClusterRecoveryPlanShmem;
typedef struct ClusterRecoveryCheckpointSlotV1 ClusterRecoveryCheckpointSlotV1;

#include "test_cluster_normal_cold_memory.inc"

#include "test_cluster_normal_cold_observe_guard.inc"
	(void)&state;
	(void)&candidate;
	(void)&observed;
	(void)&confirmed;
	(void)memory_budget;
	abort();
}

/* The native short-circuit must reject a foreign start before asking for any
 * retained checkpoint owner. This leaf cannot manufacture such an owner. */
static bool
normal_cold_unavailable_checkpoint_owner(const struct ClusterRecoveryStartV1 *start,
									   ClusterRecoveryCheckpointOwnerV1 *out)
{
	(void)start;
	(void)out;
	abort();
}

#define ClusterColdRecoveryTxCheckpointOwnerV1 normal_cold_unavailable_checkpoint_owner
#include "test_cluster_normal_cold_wait_guard.inc"
	(void)&fixed;
	(void)&slot;
	abort();
}
#undef ClusterColdRecoveryTxCheckpointOwnerV1

#include "test_cluster_normal_cold_claim_guard.inc"
	(void)&custody;
	(void)&cookie;
	abort();
}
