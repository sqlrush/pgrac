/* PGRAC: bounded relmap pending retention state core.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_H
#define CLUSTER_RELMAP_RETENTION_H

#include "cluster/cluster_tt_local.h"
#include "common/cluster_relmap_v2.h"

#define CLUSTER_RELMAP_RETENTION_CAPACITY 64

/* Volatile coordination only, never a substitute for either durable copy.
 * All access to published storage, including checks, requires ONE caller-held
 * retention lock. Startup may build exclusively owned, unpublished storage
 * without that lock; it must never do disk I/O under the runtime lock.
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
	/* Original published receipt, volatile only; never fabricate a TT slot. */
	ClusterCanonicalTxnBinding binding;
	bool occupied;
	bool has_tt_binding;
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

/* Read-only custody of an actual uniquely enumerated, unstaged pin. This
 * records original volatile inputs, not a reservation or lifetime authority. */
typedef struct ClusterRelmapRetentionSnapshot {
	ClusterRelmapRetentionPin pin;
	uint64 generation;
	uint64 native_floor;
	uint64 last_issued;
	uint64 truncate_serial;
	ClusterRelmapRetentionTruncate inflight;
} ClusterRelmapRetentionSnapshot;

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
 * grants no stage permission. begin is the no-new-binding wrapper; it inherits
 * an actual binding already enrolled for this same FullXid, if any. The owner
 * must use begin_bound when a real published binding predates this pin.
 * WAL/claim edges and original TT duties remain independent requirements. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_begin(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key);
extern ClusterRelmapRetentionResult cluster_relmap_retention_begin_bound(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	const ClusterCanonicalTxnBinding *binding);

/* The original ACTIVE owner supplies its actual published receipt and original
 * top FullXid, never an era reconstructed from raw xid. Enroll ALL extant pins
 * for that FullXid atomically, including staged pins; a no-pin call adds none.
 * Exact retries are idempotent; a differing receipt never replaces one.
 * Refusal must retain the original published ACTIVE/BIND and follow its ERROR
 * path. These pure checks do not authenticate the supplied receipt or owner. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_bind(
	ClusterRelmapRetentionDirectory *directory, uint64 top_full_xid,
	const ClusterCanonicalTxnBinding *binding);

/* One relmap conjunct only, NOT TT recycle/reuse permission. A matching exact
 * physical key is retained irrespective of the query's ACTIVE LSN. An unbound
 * same-raw obligation is unresolved and refuses, not evidence of no TT edge.
 * Ordinary A0/GC and recovery D0 must also satisfy their original owner gates. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_tt_release_check(
	const ClusterRelmapRetentionDirectory *directory,
	const ClusterCanonicalTxnBinding *binding);
/* Read-only exact unstaged preparation check under the same caller-held lock.
 * Rechecks the original floor/issued window; never creates a pin, assigns a
 * WAL end or grants stage/terminal/release authority after unlocking. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_prepare_check(
	const ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key);
/* Capture and match under the original caller-held retention lock. Enumerate
 * the entire bounded directory, refusing duplicate/conflicting namespace pins.
 * Capture output must not overlap the directory or key. A match allows only
 * the original pin's unstaged -> exact WAL-end staged transition; TT binding,
 * directory generation, floor/issued and truncate metadata must remain exact.
 * Nothing here acquires, updates, releases or qualifies any retained input. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_prepare_capture(
	const ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	ClusterRelmapRetentionSnapshot *snapshot);
extern ClusterRelmapRetentionResult cluster_relmap_retention_prepare_matches(
	const ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	const ClusterRelmapRetentionSnapshot *snapshot, bool staged, uint64 wal_end);
extern ClusterRelmapRetentionResult cluster_relmap_retention_stage_start(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	uint64 wal_end);
/* Read-only exact staged obligation check under the same caller-held lock.
 * Unlike stage_start, this cannot turn an unstaged preparation into a stage.
 * OK grants no terminal classification, cleanup or pin release by itself. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_stage_check(
	const ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key,
	uint64 wal_end);
extern ClusterRelmapRetentionResult cluster_relmap_retention_cancel_prepare(
	ClusterRelmapRetentionDirectory *directory, const ClusterRelmapRetentionKey *key);
/* cancel_prepare removes only the unstaged relmap edge. It neither cancels nor
 * releases the original TT ACTIVE/BIND even when this pin carried a binding. */

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
