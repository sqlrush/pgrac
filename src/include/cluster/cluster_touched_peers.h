/*-------------------------------------------------------------------------
 *
 * cluster_touched_peers.h
 *	  pgrac per-transaction cross-node "touched_peers" tracking
 *	  (spec-5.14 fail-stop reconfiguration).
 *
 *	  A backend-local, per-transaction 128-bit bitmap recording which
 *	  declared peers' VOLATILE state this transaction has consumed at a
 *	  cross-node ingress point (a GES grant, a Cache Fusion / GCS block
 *	  install, a cross-node SCN observe, a cross-node visibility / CR
 *	  remote ref, or a cross-node sinval send).  When a fail-stop
 *	  reconfiguration event fires (a member crashed / partitioned), a
 *	  transaction whose touched_peers intersects the dead set MUST be
 *	  aborted before returning more results or committing (spec-5.14
 *	  INV-TP2), closing the read-side 8.A hole spec-2.29 deferred.
 *
 *	  Design (spec-5.14 Q2 A / Q3 A):
 *	    - backend-local uint8[16] (128 bits), no shmem, no lock; only
 *	      the owning backend reads/writes it
 *	    - CONSERVATIVE over-approximation: missing a stamp = false-
 *	      visible (8.A violation, worst); an extra stamp = extra abort
 *	      (liveness only).  Ambiguity → always stamp.  An unidentifiable
 *	      remote node → poison (unknown_touched), never a silent no-op.
 *	    - intersects() is FAIL-CLOSED: returns true on poison /
 *	      uninitialized / any uncertainty (INV-TP3)
 *	    - parallel workers OR-merge their snapshots to the leader via a
 *	      ParallelContext DSM slot (Q7 / D1b)
 *
 *	  Spec authority: pgrac:specs/spec-5.14-fail-stop-reconfig.md
 *	  (v0.2 FROZEN; INV-TP1..TP5).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_touched_peers.h
 *
 * NOTES
 *	  pgrac-original file.  Backend-only (never included by FRONTEND or
 *	  PG core public headers).  Real bodies compile only in
 *	  --enable-cluster mode (USE_PGRAC_CLUSTER); disable-cluster builds
 *	  get silent no-op stubs in cluster_touched_peers.c so caller code
 *	  paths (xact.c reset, parallel.c DSM, the 5 ingress callsites) stay
 *	  portable.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TOUCHED_PEERS_H
#define CLUSTER_TOUCHED_PEERS_H

#include "c.h"

/*
 * 128-bit bitmap, one bit per declared peer node id.  Sized to match
 * CLUSTER_RECONFIG_DEAD_BITMAP_BYTES (16) so touched ∩ dead compares
 * byte-for-byte; a StaticAssert in cluster_touched_peers.c enforces the
 * equality (U7, L218 anti-drift).
 */
#define CLUSTER_TOUCHED_PEERS_BYTES 16
#define CLUSTER_TOUCHED_PEERS_BITS (CLUSTER_TOUCHED_PEERS_BYTES * 8)

/*
 * 5 cross-node ingress classes.  The class is recorded only for
 * observability (per-kind stamp counters + diag trace); the fail-stop
 * abort decision (cluster_touched_peers_intersects) ignores the class
 * and looks only at the node bits + poison.
 */
typedef enum ClusterTouchKind {
	CLUSTER_TOUCH_GES_LOCK = 0, /* class 1: GES lock master / remote holder */
	CLUSTER_TOUCH_GCS_BLOCK, /* class 2: Cache Fusion / GCS block sender (correctness-critical) */
	CLUSTER_TOUCH_SCN,		 /* class 3: cross-node SCN observe / broadcast */
	CLUSTER_TOUCH_VISIBILITY, /* class 4: cross-node visibility origin / CR remote ref (correctness-critical) */
	CLUSTER_TOUCH_SINVAL, /* class 5: cross-node sinval send (conservative) */
	CLUSTER_TOUCH_KIND_COUNT
} ClusterTouchKind;

/*
 * Snapshot of a backend's touched state.  Used BOTH as the backend-local
 * representation and as the parallel-worker -> leader DSM payload
 * (spec-5.14 review round 2 P1: the poison flag MUST travel with the
 * bitmap, else a poison-only worker loses its poison on merge and the
 * Q7 exit-before-failstop hole reopens).
 */
typedef struct ClusterTouchedPeersSnapshot {
	uint8 bitmap[CLUSTER_TOUCHED_PEERS_BYTES]; /* per-node touched bits */
	bool unknown_touched;					   /* poison: intersects() true vs any fail-stop */
} ClusterTouchedPeersSnapshot;

/*
 * Stamp node_id (with class kind) into the current transaction's
 * touched_peers.  Backend-local, lock-free, idempotent.
 *	- self node id            -> no-op (never depend on our own "remote" state)
 *	- valid remote node id    -> set the bit
 *	- invalid / out-of-range  -> set the unknown_touched poison (NEVER a
 *	  silent no-op: under-approximation = false-visible, INV-TP1/TP3)
 * Hot path: never ereport.  Returns true iff this node (or poison) was
 * not previously set (lets per-kind counters LOG-once on first stamp).
 */
extern bool cluster_touched_peers_stamp(int32 node_id, ClusterTouchKind kind);

/*
 * Explicitly poison the current transaction's touched_peers: a caller
 * that knows it consumed some peer's volatile state but cannot identify
 * the node id (missing / untrustworthy sender field) calls this; every
 * subsequent fail-stop then makes intersects() return true.
 */
extern void cluster_touched_peers_poison(ClusterTouchKind kind);

/*
 * Does the current transaction's touched_peers intersect dead_bitmap
 * (both uint8[16])?  FAIL-CLOSED — returns true if:
 *	(a) the bitmaps share any set bit; or
 *	(b) unknown_touched poison is set; or
 *	(c) dead_bitmap is NULL / state is not safely readable.
 * Never returns false when uncertain (INV-TP3).
 */
extern bool cluster_touched_peers_intersects(const uint8 *dead_bitmap);

/* StartTransaction / Commit / Abort / Cleanup: clear the bitmap + poison. */
extern void cluster_touched_peers_reset(void);

/* Parallel (D1b): a worker exports its own snapshot into its DSM slot;
 * the leader OR-merges a worker snapshot into its own touched state. */
extern void cluster_touched_peers_export_to_dsm(ClusterTouchedPeersSnapshot *out_snap);
extern void cluster_touched_peers_merge_from_dsm(const ClusterTouchedPeersSnapshot *worker_snap);

/* Parallel (D1b): the leader registers/unregisters the per-worker DSM
 * slot array (created in InitializeParallelDSM); merge_active OR-merges
 * every registered worker slot into the leader's touched state and is
 * called from the D4 check before intersects().  Overflow of the small
 * registration table fail-closes to poison. */
extern void cluster_touched_peers_register_parallel_slots(ClusterTouchedPeersSnapshot *slots,
														  int nworkers);
extern void
cluster_touched_peers_unregister_parallel_slots(const ClusterTouchedPeersSnapshot *slots);
extern void cluster_touched_peers_merge_active_parallel_workers(void);

/* Fail-closed poison (no stamp counted): used when a parallel worker was
 * killed before exporting its touches, so the leader cannot know which peers
 * it consumed (D1b / review P1-B). */
extern void cluster_touched_peers_mark_uncertain(void);

/* Render the current backend's touched bitmap low 64 bits as 0x%016X
 * (pg_cluster_state self_touched_hex; nodes 0..63).  buf must hold >= 19
 * bytes ("0x" + 16 hex + NUL). */
extern void cluster_touched_peers_self_hex(char *buf, Size buflen);

#endif /* CLUSTER_TOUCHED_PEERS_H */
