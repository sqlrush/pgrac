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

typedef struct ClusterServiceProducerIdentity {
	int32 pid;
	int32 pgprocno;
	int32 backend_id;
	uint32 actor_bit;
	uint64 proc_generation;
	uint64 epoch;
} ClusterServiceProducerIdentity;

typedef struct ClusterServiceProducerObservation {
	ClusterServiceProducerIdentity identity;
	ClusterServiceActorObservation actor;
	ClusterServiceObservation responsibility;
	const char *reason;
} ClusterServiceProducerObservation;

StaticAssertDecl(sizeof(ClusterServiceProducerIdentity) == 32, "native owner identity size");
StaticAssertDecl(sizeof(ClusterServiceProducerObservation) <= 128, "native observation size");

/* Actual current backend snapshot references, not a native/global cut.
 * Reference flags are private-container presence, never an object count.
 * identity.actor_bit is zero: this is not an auxiliary service actor. */
#define CLUSTER_SERVICE_SNAPSHOT_ACTIVE       (1U << 0)
#define CLUSTER_SERVICE_SNAPSHOT_REGISTERED   (1U << 1)
#define CLUSTER_SERVICE_SNAPSHOT_TRANSACTION  (1U << 2)
#define CLUSTER_SERVICE_SNAPSHOT_CATALOG      (1U << 3)
#define CLUSTER_SERVICE_SNAPSHOT_HISTORIC     (1U << 4)
#define CLUSTER_SERVICE_SNAPSHOT_EXPORTED     (1U << 5)
#define CLUSTER_SERVICE_SNAPSHOT_EVALUATOR    (1U << 6)
#define CLUSTER_SERVICE_SNAPSHOT_XACT_STARTED (1U << 7)
typedef struct ClusterServiceSnapshotObservation {
	ClusterServiceProducerIdentity identity;
	uint64 retained_read_scn;
	uint32 references;
	bool known;
	const char *reason;
} ClusterServiceSnapshotObservation;
StaticAssertDecl(sizeof(ClusterServiceSnapshotObservation) <= 64,
				 "snapshot return point is bounded caller storage");

/* Original ordinary backend only; bounded read-only presence observation.
 * No new registration, invalidation, retirement, signal, lock or wait.
 * PENDING even at an empty point: producer closure is separately unproved.
 * Wrong identity/context refuses; epoch drift is STALE; no usable stale sample.
 */
extern ClusterSemanticAdmissionObservationResult cluster_snapshot_service_observe(
	uint64 expected_epoch, ClusterServiceSnapshotObservation *out);

/* Same original actor, private/module plus shared admission point observations.
 * Caller-owned only: CURRENT does not seal either owner or grant permission.
 * No atomic cross-domain zero proof is implied; producer closure is separate. */
typedef struct ClusterServiceProducerCollection {
	ClusterServiceProducerObservation producer;
	ClusterSemanticAdmissionObservation admission;
	const char *reason;
} ClusterServiceProducerCollection;

StaticAssertDecl(sizeof(ClusterServiceProducerCollection) <= 1024,
				 "native producer collection is bounded caller storage");

extern ClusterSemanticAdmissionObservationResult cluster_service_producer_collect(
	uint64 expected_epoch, uint64 expected_generation, ClusterServiceProducerCollection *out);


/* Original feature-token return domain plus the same current native owner.
 * The gate epoch/generation stays bound to the original OPEN; the native
 * identity uses expected_epoch. Caller-owned, bounded, read-only observation.
 * Always PENDING/STALE/REFUSED: neither an empty feature domain nor an idle
 * native point proves native producer closure or produces a receipt. */
extern ClusterSemanticAdmissionObservationResult cluster_service_producer_collect_return(
	uint64 expected_epoch, uint64 expected_gate_epoch, uint64 expected_generation,
	ClusterServiceProducerCollection *out);

/* One actual native owner's point observation, at an unlocked boundary.
 * Binds the existing actor bracket and original role fanout to its own
 * PGPROC identity and epoch, rechecking after the module calls. An active
 * outer bracket stays PENDING without inspecting private module work.
 * LMON includes its four shared responsibilities. Signed auxiliary backend
 * identity is preserved. No closure/admission/retirement is performed;
 * READY cannot be retained as a producer cut or service permission. */
extern ClusterNormalStopPollResult
cluster_service_producer_observe(uint64 expected_epoch, ClusterServiceProducerObservation *out);

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
