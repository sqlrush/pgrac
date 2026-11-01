/* Original caller-owned producer observations; no closure or permission.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_SEMANTIC_SURVIVOR_PRODUCER_H
#define CLUSTER_SEMANTIC_SURVIVOR_PRODUCER_H
#include "cluster/cluster_service_observe.h"
#include "cluster/cluster_wal_inputs.h"

typedef enum ClusterSurvivorProducerOwner {
 CLUSTER_SURVIVOR_PRODUCER_BACKEND = 1,
 CLUSTER_SURVIVOR_PRODUCER_NATIVE,
 CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS
} ClusterSurvivorProducerOwner;

/* Caller-supplied scope, never an authenticated episode or receipt.
 * gate_epoch/generation bind only the backend/native token observations.
 * INPUT_JOBS observes its native epoch and private jobs, not the token gate. */
typedef struct ClusterSurvivorProducerScope {
 uint64 native_epoch;
 uint64 gate_epoch;
 uint64 record_generation;
} ClusterSurvivorProducerScope;
typedef struct ClusterSurvivorProducerObservation {
 ClusterSurvivorProducerOwner owner;
 union {
  ClusterServiceBackendCollection backend;
  ClusterServiceProducerCollection native;
  ClusterWalInputsJobsObservationV1 inputs;
 } sample;
 const char *reason;
} ClusterSurvivorProducerObservation;
StaticAssertDecl(sizeof(ClusterSurvivorProducerObservation) <= 1024,
				 "producer poll uses bounded caller storage");

/* Original process only, at an unlocked return point. Selects exactly one
 * existing owner family and preserves that family's narrower coverage.
 * No cross-process read, allocation, poll-to-retire, registration or wakeup.
 * Always PENDING/STALE/REFUSED, including empty or DONE samples. It cannot
 * acknowledge a global/native cut or replace the remaining producer census.
 * Unknown results clear every sample; scope is passed by value. */
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_producer_poll(
 ClusterSurvivorProducerOwner owner, ClusterSurvivorProducerScope scope,
 ClusterSurvivorProducerObservation *out);
#endif
