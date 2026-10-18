/* PGRAC: bounded relmap pending retention state core.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_H
#define CLUSTER_RELMAP_RETENTION_H

#include "common/cluster_relmap_v2.h"

#define CLUSTER_RELMAP_RETENTION_CAPACITY 64

/* Volatile coordination only, never a substitute for either durable copy.
 * All access, including checks, requires ONE caller-held retention lock.
 * No function takes another lock, allocates, performs I/O or raises ERROR.
 * Future allocation callers hold XidGenLock before that retention lock and
 * check BOTH their first and recomputed stripe/herding candidates. */
typedef enum ClusterRelmapRetentionState {
	CLUSTER_RELMAP_RETENTION_UNKNOWN = 0,
	CLUSTER_RELMAP_RETENTION_BUILDING,
	CLUSTER_RELMAP_RETENTION_COMPLETE
} ClusterRelmapRetentionState;

typedef enum ClusterRelmapRetentionResult {
	CLUSTER_RELMAP_RETENTION_OK = 0,
	CLUSTER_RELMAP_RETENTION_UNAVAILABLE,
	CLUSTER_RELMAP_RETENTION_INVALID,
	CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED,
	CLUSTER_RELMAP_RETENTION_CONFLICT,
	CLUSTER_RELMAP_RETENTION_NATIVE_GONE,
	CLUSTER_RELMAP_RETENTION_XID_WINDOW,
	CLUSTER_RELMAP_RETENTION_RETAINED,
	CLUSTER_RELMAP_RETENTION_BUSY
} ClusterRelmapRetentionResult;

typedef struct ClusterRelmapRetentionKey {
	ClusterRelmapV2Namespace identity;
	uint64 generation;
	uint16 origin;
	uint64 top_full_xid;
	uint64 membership_epoch;
	uint64 admitted_incarnation;
	uint64 boot_incarnation;
} ClusterRelmapRetentionKey;

typedef struct ClusterRelmapRetentionPin {
	ClusterRelmapRetentionKey key;
	uint64 wal_end;
	bool occupied;
	/* Set BEFORE a pending file write can start; ERROR cannot cancel it. */
	bool may_be_staged;
} ClusterRelmapRetentionPin;

typedef struct ClusterRelmapRetentionTruncate {
	uint64 generation;
	uint64 serial;
	uint64 cutoff;
} ClusterRelmapRetentionTruncate;

/* One original origin, system and DB incarnation per directory. Recovery
 * must use each origin's qualified native/SIDE route, never an empty local
 * directory to authorize foreign reclamation. Storage belongs to the caller;
 * this slice does not register shared memory or activate any live consumer. */
typedef struct ClusterRelmapRetentionDirectory {
	ClusterRelmapRetentionState state;
	uint64 system_identifier;
	uint64 database_incarnation;
	uint16 origin;
	uint64 generation;
	uint64 native_floor;
	uint64 last_issued;
	uint64 truncate_serial;
	ClusterRelmapRetentionTruncate inflight;
	ClusterRelmapRetentionPin pins[CLUSTER_RELMAP_RETENTION_CAPACITY];
} ClusterRelmapRetentionDirectory;

/* Start on zeroed, unpublished storage only; cannot reset an old directory.
 * The original Startup owner must qualify the actual native floor and issued
 * FullXid observations, enumerate ALL namespaces/BOTH copies, restore every
 * pending, then finish. These values/functions do not authenticate a census.
 * Any failed restore poisons BUILDING; ignoring it cannot publish emptiness.
 * A later rebuild uses new storage and must invalidate the old generation
 * before any destructive consumer; no ordinary backend may publish COMPLETE. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_build_start(
	ClusterRelmapRetentionDirectory *directory, uint64 system_identifier,
	uint64 database_incarnation, uint16 origin, uint64 generation, uint64 native_floor,
	uint64 last_issued);
extern ClusterRelmapRetentionResult cluster_relmap_retention_build_add(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	uint64 wal_end);
extern ClusterRelmapRetentionResult
cluster_relmap_retention_build_finish(ClusterRelmapRetentionDirectory *directory);
extern void cluster_relmap_retention_invalidate(ClusterRelmapRetentionDirectory *directory);

/* Original global relmap X holder, AFTER real top assignment/admission and
 * BEFORE RelationMappingLock, relmap WAL and all pending-file I/O. Refusal
 * grants no stage permission. No TT binding is created by this native pin.
 * Caller must separately retain exact existing/later TT and WAL/claim edges. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_begin(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key);
extern ClusterRelmapRetentionResult cluster_relmap_retention_stage_start(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	uint64 wal_end);
extern ClusterRelmapRetentionResult cluster_relmap_retention_cancel_prepare(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key);

/* Register BEFORE any native cut publication/I/O; cutoff is a qualified
 * FullXid boundary supplied by the original truncation owner. Clamp to pins
 * under the same lock as begin; begin below the reservation then refuses.
 * The output reservation must be separate storage, not directory->inflight.
 * Complete only after the original owner's successful I/O. Any uncertain
 * outcome invalidates the directory and retains the reservation and pins. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_truncate_begin(
	ClusterRelmapRetentionDirectory *directory, uint64 requested_cutoff,
	ClusterRelmapRetentionTruncate *reservation);
extern ClusterRelmapRetentionResult cluster_relmap_retention_truncate_complete(
	ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionTruncate *reservation);
extern ClusterRelmapRetentionResult cluster_relmap_retention_truncate_uncertain(
	ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionTruncate *reservation);

/* Check only, then recheck+observe at the actual allocation publication point
 * under XidGenLock -> retention lock. A successful check is NOT a reservation.
 * Observe and publishing nextXid/ProcArray must not admit an intervening begin.
 * Existing wrap barrier/StopLimit checks remain independent requirements. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_allocation_check(
	const ClusterRelmapRetentionDirectory *directory, uint64 candidate);
extern ClusterRelmapRetentionResult cluster_relmap_retention_allocation_observe(
	ClusterRelmapRetentionDirectory *directory, uint64 candidate);

/* Deliberately no staged-pin release: dual clear/readback AND original native
 * terminal/TT/SPACE proofs must be integrated before such authority exists. */
#endif
