/*-------------------------------------------------------------------------
 *
 * cluster_service_observe.h
 *    Read-only observations by the original native service owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_service_observe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SERVICE_OBSERVE_H
#define CLUSTER_SERVICE_OBSERVE_H

#include "cluster/cluster_clean_leave.h"

typedef struct ClusterServiceObservation {
	const char *domain;
	const char *reason;
	int slot;
	uint32 position;
	uint64 key;
	unsigned modules;
} ClusterServiceObservation;

/* Process-local observation only; not wire, shared state or a reusable cut. */
typedef struct ClusterServiceActorObservation {
	uint32 actor_bit;
	uint32 bound_actor_bit;
	uint32 active_depth;
	const char *reason;
} ClusterServiceActorObservation;

/* Observe the original native actor's enter/leave bracket even online.
 * Only its actual matching Aux/BackendType may inspect this private copy.
 * Active outer work stays PENDING through nesting; mismatched binding is
 * INVALID and is never repaired here. READY means this bracket is inactive,
 * not that modules, transport, admission or producer closure have completed.
 * The original caller still retains/revalidates identity and each separate
 * owner/cut. No stop storage, request, lock, retirement or failure write. */
extern ClusterNormalStopPollResult
cluster_service_actor_observe(ClusterServiceActorObservation *out);

/* Original LMON/LMS/LMD/SINVAL process only, at its unlocked idle boundary.
 * Uses actual native roles and original module polls, even without shutdown.
 * Does not seal, cancel, publish failure or authorize configuration/DATA use.
 * READY is a point observation, never a retained producer/transport cut. */
extern ClusterNormalStopPollResult cluster_service_observe(ClusterServiceObservation *out);

/* Existing shutdown wrappers retain their stage-specific CLOSE semantics. */
extern ClusterNormalStopPollResult
cluster_service_normal_stop_observe(ClusterServiceObservation *out);

/* Original online LMON, with shared configuration, at its unlocked idle
 * boundary. Also inspect the four original PI/WAL-cut/writeback/KO shared
 * owners used by its shutdown wrapper. Observe every owner, retaining the
 * first highest-priority cause; unknown results are INVALID. This never
 * changes the original online/stop entries, seals a producer or retires an
 * owner. The caller must retain/revalidate its own identity and closure;
 * READY remains only a point observation, not QUIESCED or service proof. */
extern ClusterNormalStopPollResult cluster_lmon_service_observe(ClusterServiceObservation *out);

#endif /* CLUSTER_SERVICE_OBSERVE_H */
