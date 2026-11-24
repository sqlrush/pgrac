/* Independent negative ordinary admission. Never a receipt or service proof.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_SEMANTIC_SURVIVOR_CLOSE_H
#define CLUSTER_SEMANTIC_SURVIVOR_CLOSE_H
#include "cluster/cluster_semantic_survivor_targets.h"

#define CLUSTER_SURVIVOR_CLOSE_IDLE 0
#define CLUSTER_SURVIVOR_CLOSE_CLOSING 1
#define CLUSTER_SURVIVOR_CLOSE_STALE 2
#define CLUSTER_SURVIVOR_CLOSE_OFFSET \
 (CLUSTER_SURVIVOR_LOCAL_BYTES + sizeof(ClusterSurvivorTargetCensus))

typedef struct ClusterSurvivorCloseRecord {
 pg_atomic_uint64 seq;
 uint64 generation, event_id, event_seq, epoch, old_epoch;
 uint64 gate_seq, gate_epoch, record_generation, owner_generation;
 int32 owner_pid, owner_pgprocno;
 uint32 state, kind;
 uint8 dead_bitmap[16];
 uint64 reserved[2];
} ClusterSurvivorCloseRecord;
/* Stack-only sample. blocked=false only means this independent gate has not
 * closed; it does not override any original admission guard. */
typedef struct ClusterSurvivorCloseSnapshot {
 ClusterSurvivorCloseRecord record;
 bool blocked;
} ClusterSurvivorCloseSnapshot;
StaticAssertDecl(sizeof(ClusterSurvivorCloseRecord) == 128, "close record size");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, seq) == 0, "close sequence");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, owner_pid) == 80, "close owner");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, dead_bitmap) == 96, "close failed set");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, reserved) == 112, "close reserved");
StaticAssertDecl(offsetof(ClusterSurvivorControl, reserved) + CLUSTER_SURVIVOR_CLOSE_OFFSET == 62880,
 "close absolute offset");
StaticAssertDecl(CLUSTER_SURVIVOR_CLOSE_OFFSET + sizeof(ClusterSurvivorCloseRecord) + 2528
 == sizeof(((ClusterSurvivorControl *)0)->reserved), "close fits original tail");
StaticAssertDecl(sizeof(ClusterSurvivorControl) == 65536, "close control budget");
/* Only the original unlocked LMON can install/invalidate. No reopen API. */
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_close_begin(void);
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_close_observe(
 ClusterSurvivorCloseSnapshot *out);
extern void cluster_semantic_survivor_close_invalidate(void);
/* Original StartTransaction, before resources; cleanup and old gates unchanged. */
extern void cluster_semantic_survivor_ordinary_wait(void);
#endif
