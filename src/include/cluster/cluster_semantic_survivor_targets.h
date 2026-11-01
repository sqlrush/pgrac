/* Bounded original process-table observations, never closure or receipts. */
#ifndef CLUSTER_SEMANTIC_SURVIVOR_TARGETS_H
#define CLUSTER_SEMANTIC_SURVIVOR_TARGETS_H
#include "cluster/cluster_semantic_survivor_local.h"

#define CLUSTER_SURVIVOR_TARGETS_RUN 1
#define CLUSTER_SURVIVOR_TARGETS_WAIT 2
#define CLUSTER_SURVIVOR_TARGETS_DONE 3
#define CLUSTER_SURVIVOR_TARGETS_STALE 4
typedef struct ClusterSurvivorTargetCensus {
	uint64 nonce, round, native_epoch, gate_epoch, record_generation, birth_cut;
	uint32 cursor, limit, phase, unknown, observed, stale, refused, attempts;
	ClusterSurvivorLocalTarget target;
	uint64 query_generation, reserved;
} ClusterSurvivorTargetCensus;
StaticAssertDecl(sizeof(ClusterSurvivorTargetCensus) == 128, "target cursor size");
StaticAssertDecl(offsetof(ClusterSurvivorTargetCensus, target) == 80, "target cursor identity");
StaticAssertDecl(offsetof(ClusterSurvivorTargetCensus, query_generation) == 112, "target cursor generation");
StaticAssertDecl(offsetof(ClusterSurvivorControl, reserved) + CLUSTER_SURVIVOR_LOCAL_BYTES == 62752,
 "target cursor absolute offset");
StaticAssertDecl(CLUSTER_SURVIVOR_LOCAL_BYTES + sizeof(ClusterSurvivorTargetCensus) + 128 + 2528
	== sizeof(((ClusterSurvivorControl *)0)->reserved), "target cursor leaves the close record and zero tail");
/* Original unlocked LMON only; query/reply events advance at most one
 * outstanding owner. Diagnostic counts never qualify a producer closure. */
extern void cluster_semantic_survivor_targets_tick(void);
#endif
