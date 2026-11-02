#ifndef TEST_CLUSTER_STARTUP_CONFIRM_MERGE_H
#define TEST_CLUSTER_STARTUP_CONFIRM_MERGE_H
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_control_relmap_delta.h"
extern ClusterRecoveryFencePlan *confirm_merge_begin(const ClusterControlRootSnapshot *, const ClusterControlRootReadToken *, uint64 *);
extern void confirm_merge_observe(const ClusterControlRootSnapshot *, const ClusterControlRootReadToken *, bool);
extern bool confirm_merge_current(ClusterRecoveryFencePlan *, uint64);
extern bool confirm_merge_held(ClusterRecoveryFencePlan *, uint64);
extern bool confirm_merge_phase(ClusterRecoveryFencePlan *, uint64, const struct ClusterColdTypedV1 *, const struct ClusterControlRelmapRecoveryCreateV2 *);
extern bool confirm_merge_install(ClusterRecoveryFencePlan *, uint64, const struct ClusterColdTypedV1 *, const struct ClusterControlRelmapRecoveryCreateV2 *);
extern bool confirm_merge_resume(ClusterRecoveryFencePlan *, uint64, const ClusterControlRootReadToken *);
extern bool confirm_merge_baseline(ClusterRecoveryFencePlan *, const ClusterControlRootReadToken *);
extern bool confirm_merge_end(ClusterRecoveryFencePlan **);
#endif
