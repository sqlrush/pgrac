/*-------------------------------------------------------------------------
 *
 * cluster_pi_shadow.h
 *	  pgrac spec-6.12h D-h3a -- Past Image ship-SCN shadow table and the
 *	  pure PI recovery-boundary gate.
 *
 *	  A Past Image (BUF_TYPE_PI, D-h1) is only a usable recovery base if
 *	  redo replay can decide, per WAL record, whether the record's
 *	  effects are ALREADY in the PI bytes (its lineage) or landed on the
 *	  block only after it was shipped away (post-ship).  Per-thread WAL
 *	  (spec-4.1) makes raw LSNs from different nodes incomparable, and
 *	  pd_block_scn is not bumped by HOT pruning, so neither can carry
 *	  that boundary.  The provable boundary is the SHIP SCN: the local
 *	  AD-008 Lamport clock sampled at the instant of the PI conversion.
 *
 *	  Boundary proof (U-h; all cites verified in-tree):
 *
 *	  ship_scn = cluster_scn_current() sampled INSIDE the conversion's
 *	  buffer-header-lock hold, after the refcount==0 check
 *	  (cluster_bufmgr_convert_to_pi_locked, bufmgr.c).
 *
 *	  1. Lineage <= ship_scn.  Every record whose effects are in the PI
 *	     bytes was stamped xl_scn = cluster_scn_current() inside its WAL
 *	     insertion (xlog.c, spec-4.5) while its writer held a pin; the
 *	     writer's unpin happens-before the conversion's LockBufHdr sees
 *	     refcount 0, and the clock never decreases, so xl_scn <=
 *	     ship_scn.  Cross-node lineage (an S copy converted by the
 *	     S-INVALIDATE path carries records written by an earlier remote
 *	     X holder) is covered by the envelope discipline: the image
 *	     could only have been installed here after
 *	     cluster_scn_observe()-ing the envelope that carried it
 *	     (cluster_ic_envelope.c), which lifted the local clock to at
 *	     least every stamp in those bytes.
 *
 *	  2. Post-ship > ship_scn (STRICT).  Every conversion site converts
 *	     BEFORE its reply/ACK leaves the node (ordering pinned by the
 *	     D-h3a comments at the four sites in cluster_gcs_block.c), so
 *	     the outbound envelope is stamped scn_current >= ship_scn, and
 *	     every receiver runs cluster_scn_observe = max(local, remote)+1
 *	     (cluster_scn.c; the +1 covers equality).  A writer can touch
 *	     the block only after receiving the shipped image / grant, so
 *	     any record it stamps afterwards is strictly above ship_scn.
 *	     For the S-INVALIDATE PI the chain is two hops -- converter ACK
 *	     -> master observe -> grant reply -> upgrader observe -- and the
 *	     grant is structurally after the ACK collection (the master
 *	     clears the holder's bit only in the ACK handler).
 *
 *	  3. Therefore equality is always the lineage side (SKIP), and the
 *	     comparison uses ONLY the Lamport counter dimension: two nodes
 *	     may share a counter value, but by (2) the post-ship side is
 *	     strictly greater, so node_id must never break ties (a raw
 *	     uint64 SCN compare would be node_id-dominated -- same rule as
 *	     cluster_pcm_pi_discard_covered).
 *
 *	  Fail-closed rows (Rule 8.A): an invalid ship_scn (clock unarmed
 *	  at conversion) or an invalid xl_scn (legacy-thread records carry
 *	  0, xlog.c) makes the boundary unprovable -> the caller must
 *	  abandon the PI and recover from storage + full redo.
 *
 *	  Slot validity contract (consumer side, D-h3b/c): the slot for
 *	  buf_id is trustworthy iff, under that buffer's header lock, the
 *	  buffer is still PI-shaped (BUF_TYPE_PI && BM_TAG_VALID && !BM_VALID
 *	  && tag matches).  The PI shape is reachable ONLY through
 *	  cluster_bufmgr_convert_to_pi_locked -- the retag sites
 *	  (InvalidateBuffer / InvalidateVictimBuffer) and the read-IO start
 *	  (StartBufferIO, forInput) reset buffer_type, and the bytes of a
 *	  !BM_VALID shared buffer are only ever written inside the
 *	  StartBufferIO..TerminateBufferIO window -- so a PI-shaped buffer
 *	  always pairs the CURRENT conversion's stamp with bytes frozen
 *	  since that stamp.
 *
 *	  Each slot is written under its buffer's header spinlock (1:1
 *	  slot<->buffer), and consumed under the same lock, so plain stores
 *	  suffice; no atomics needed.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_pi_shadow.h
 *
 * NOTES
 *	  This is a pgrac-original file.  The gate inline is frontend-safe
 *	  (scn_encode / scn_local / SCN_VALID live above the FRONTEND guard
 *	  in cluster_scn.h).
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PI_SHADOW_H
#define CLUSTER_PI_SHADOW_H

#include "cluster/cluster_scn.h"

/*
 * Per-record verdict of the PI recovery-boundary gate.
 */
typedef enum ClusterPiGateVerdict {
	CLUSTER_PI_GATE_UNUSABLE = 0, /* boundary unprovable: abandon the PI,
								   * recover from storage + full redo */
	CLUSTER_PI_GATE_SKIP = 1,	  /* record already in the PI lineage */
	CLUSTER_PI_GATE_APPLY = 2	  /* post-ship record: redo it onto the PI */
} ClusterPiGateVerdict;

/*
 * cluster_pi_recovery_gate -- the PURE boundary judge (see the file
 * header for the full proof).  apply iff xl_scn's Lamport counter is
 * STRICTLY above the ship stamp; equality is lineage; anything
 * unprovable poisons the whole PI (the caller must not keep replaying
 * onto it after an UNUSABLE verdict).
 */
static inline ClusterPiGateVerdict
cluster_pi_recovery_gate(SCN xl_scn, SCN ship_scn)
{
	if (!SCN_VALID(ship_scn))
		return CLUSTER_PI_GATE_UNUSABLE; /* clock unarmed at conversion */
	if (!SCN_VALID(xl_scn))
		return CLUSTER_PI_GATE_UNUSABLE; /* unstamped (legacy-thread) record */
	/* SCN_CMP_OK: Lamport-counter order via scn_local (raw compare would be
	 * node_id-dominated; node_id never breaks ties -- header proof item 3). */
	return (scn_local(xl_scn) > scn_local(ship_scn)) ? CLUSTER_PI_GATE_APPLY : CLUSTER_PI_GATE_SKIP;
}

/* Set once by shmem init; NULL until the region is attached. */
extern PGDLLIMPORT SCN *ClusterPiShadow;

extern Size cluster_pi_shadow_shmem_size(void);
extern void cluster_pi_shadow_shmem_init(void);
extern void cluster_pi_shadow_shmem_register(void);

/*
 * Slot accessors.  Caller holds the buffer-header spinlock of the buffer
 * whose buf_id indexes the slot (writer AND reader side); the guards make
 * pre-shmem / disabled-build callers a safe no-op (read = InvalidScn ->
 * gate UNUSABLE -> fail-closed).
 */
static inline void
cluster_pi_shadow_stamp(int buf_id, SCN ship_scn)
{
	if (ClusterPiShadow != NULL && buf_id >= 0)
		ClusterPiShadow[buf_id] = ship_scn;
}

static inline SCN
cluster_pi_shadow_read(int buf_id)
{
	if (ClusterPiShadow != NULL && buf_id >= 0)
		return ClusterPiShadow[buf_id];
	return InvalidScn;
}

static inline void
cluster_pi_shadow_clear(int buf_id)
{
	cluster_pi_shadow_stamp(buf_id, InvalidScn);
}

#endif /* CLUSTER_PI_SHADOW_H */
