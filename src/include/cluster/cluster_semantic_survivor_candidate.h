/* Caller-owned, unconfirmed survivor collection. No admission or ACK proof.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_SEMANTIC_SURVIVOR_CANDIDATE_H
#define CLUSTER_SEMANTIC_SURVIVOR_CANDIDATE_H
#include "cluster/cluster_semantic_survivor.h"
#include "cluster/cluster_service_observe.h"
#include "cluster/cluster_semantic_survivor_producer.h"

/* The original LMON exclusively serializes this unconfirmed object (shared storage is supplied by its owner).
 * Begin requires zeroed storage. Invalidation retains original obligations;
 * it does not permit reuse. Inputs are untrusted syntax, including receipts
 * and transport generation: staging never authenticates an ingress owner.
 * Begin/stage recheck actual native epoch and original shared gate scope;
 * these negative fences do not qualify ROOT, membership or producer closure.
 * A confirmed local change invalidates; unknown publication stays PENDING.
 * An exiting original owner refuses; exit during a write invalidates while
 * retaining the original basis, rows and cleanup obligations.
 * None of these operations can return CURRENT or set confirmed table bits.
 * Candidate operations themselves do not allocate, route, wake or close.
 */
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_candidate_begin(
	ClusterSurvivorControl *control, const uint8 *bytes, Size length,
	const uint8 format[32], uint32 self, const char **reason);
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_candidate_stage(
	ClusterSurvivorControl *control, const uint8 *bytes, Size length,
	const uint8 format[32], uint64 transport_generation, const char **reason);
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_candidate_invalidate(
	ClusterSurvivorControl *control, const char **reason);
/* Actual original native owner observation, at its unlocked idle boundary.
 * A CURRENT point stays PENDING until separate original producer closure.
 * out retains the actual observation, never a fabricated completion receipt.
 */
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_candidate_probe(
	ClusterSurvivorControl *control, ClusterServiceProducerCollection *out,
	const char **reason);
/* Internal read-only scope: true is metadata validity, never authority. */
extern bool cluster_semantic_survivor_candidate_scope(ClusterSurvivorControl *control,
 ClusterSurvivorProducerScope *scope, uint8 basis[32]);
#endif
