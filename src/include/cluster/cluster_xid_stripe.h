/*-------------------------------------------------------------------------
 *
 * cluster_xid_stripe.h
 *	  pgrac xid space segmentation -- pure stripe derivation layer.
 *
 *	  With cluster.xid_striping enabled, node slot k only ever issues
 *	  32-bit xids congruent to k (mod CLUSTER_XID_STRIDE) whose full
 *	  form is at or above the cluster activation floor.  Above the
 *	  floor the value space is therefore globally unique and every raw
 *	  xid is self-describing: its origin slot is xid mod STRIDE.
 *
 *	  This header ships the pure derivation layer (no shmem, no locks,
 *	  no ereport -- underivable is reported as -1 / Invalid and callers
 *	  must fail closed) plus two thin runtime wrappers that consult the
 *	  process-local latched stripe state.  The persistent stripe face
 *	  (activation floor / per-slot floors / owner ids) and the boot /
 *	  join code that populates the latch land with a later deliverable
 *	  of the same spec; until latched the wrappers report underivable.
 *
 *	  Widening note: cluster_xid_widen uses a SIGNED relative window
 *	  (next_full - 2^31, next_full + 2^31), unlike the one-sided
 *	  snapshot idiom in xid8funcs.c widen_snapshot_xid.  Consumers of
 *	  this layer see REMOTE xids that may lead the local nextXid by up
 *	  to the herding slack, so the window must extend forward as well;
 *	  interpretation inside any 2^32 window is unique, and the +/- 2^31
 *	  bound is guaranteed forward by counter herding and backward by
 *	  the freeze / xidStopLimit discipline.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xid_stripe.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *	  Frontend-safe: only depends on access/transam.h.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XID_STRIPE_H
#define CLUSTER_XID_STRIPE_H

#include "access/transam.h"

/* Stripe width == declared-node slot count upper bound (AD-012). */
#define CLUSTER_XID_STRIDE 16

/*
 * Widen a raw 32-bit xid to its full form relative to next_full (the
 * local nextXid), using the signed +/- 2^31 window described in the
 * file header.  Returns InvalidFullTransactionId when xid is not a
 * normal xid, when next_full is invalid, or when the behind-window
 * interpretation would precede full xid 0 (fail-closed).
 */
extern FullTransactionId cluster_xid_widen(TransactionId xid, FullTransactionId next_full);

/*
 * Derive the origin slot of fxid against the activation floor.
 * Returns the slot (0 .. CLUSTER_XID_STRIDE-1), or -1 when fxid or
 * floor_full is invalid or fxid precedes the floor (underivable --
 * callers must fail closed, never guess).
 */
extern int cluster_xid_origin_slot_full(FullTransactionId fxid, FullTransactionId floor_full);

/*
 * Smallest FullTransactionId >= next_full whose 32-bit value is
 * congruent to slot (mod CLUSTER_XID_STRIDE).  In-class positions
 * whose 32-bit value would be a special xid (0-2) are skipped to the
 * next in-class position; rounding across 2^32 carries into the next
 * epoch.  Pure candidate arithmetic for the striped allocator and for
 * herding jumps (both run under XidGenLock, which this function does
 * NOT take).
 */
extern FullTransactionId cluster_xid_next_striped_full(FullTransactionId next_full, int slot);

/*
 * Herding predicate: does cluster_max lead local_next by strictly more
 * than threshold values (64-bit full-xid space)?  Used with the slack
 * threshold to decide observe-and-jump and with the hard-limit
 * threshold to decide fail-closed refusal.
 */
extern bool cluster_xid_lag_exceeds(FullTransactionId local_next, FullTransactionId cluster_max,
									uint64 threshold);

/*
 * Runtime wrappers over the process-local latched stripe state.
 * cluster_xid_origin_slot widens xid against ReadNextFullTransactionId()
 * and derives against the latched floor; returns -1 when the stripe
 * runtime is not latched active or derivation fails.
 * cluster_xid_is_mine reports whether xid derives to this node's slot
 * (false whenever underivable).
 */
extern int cluster_xid_origin_slot(TransactionId xid);
extern bool cluster_xid_is_mine(TransactionId xid);

/*
 * Allocation-side slot gate consumed by GetNewTransactionId under
 * XidGenLock: this node's stripe slot when striping is enabled and the
 * declared node identity fits the stripe width, else -1 (allocation
 * stays vanilla).  Boot-time validation in cluster_init_shmem FATALs
 * on striping-on configurations whose node_id cannot stripe, so -1
 * here never silently mixes striped and unstriped allocation in an
 * enabled cluster.
 */
extern int cluster_xid_allocation_slot(void);

/*
 * Latch the process-local stripe runtime state.  Ownership: the stripe
 * activation / join path (later deliverable of spec-6.15) calls this
 * once per process at startup; values are immutable for the life of
 * the process (activation is single-shot and PGC_POSTMASTER-gated).
 * active=true with an out-of-range slot or invalid floor is treated as
 * inactive (fail-closed, no partial activation).
 */
extern void cluster_xid_stripe_latch_runtime(bool active, int my_slot,
											 FullTransactionId floor_full);

/*
 * ----------------------------------------------------------------
 * Durable stripe records (spec-6.15 D5, appendix B.1)
 *
 * Two CRC-checked voting-disk record types, following the spec-5.15
 * ClusterJoinCommitMarker envelope discipline (magic / version /
 * monotonic generation / CRC32C; records are memset-0 before fill so
 * padding is stable under the CRC):
 *
 *  - ClusterXidStripeSlotRecord ("PGXS"), one per stripe slot in
 *    voting-disk region 4.  Sole writer is the OWNING node (LMON tick
 *    refreshes next_xid_hwm_full -- the herding carrier), with one
 *    exception: the spec-5.18 removal coordinator cross-writes the
 *    retired flag (region-3 coordinator-writes-joiner-slot precedent).
 *    The owner durable identity is {node_id, owner_incarnation}: the
 *    stripe slot index equals the declared node_id in v1, and
 *    owner_incarnation is the spec-5.15 admitted incarnation at FIRST
 *    claim, written once and never rewritten (a restart presents a
 *    fresh, higher incarnation, so stability across restarts is by
 *    construction of the stored value, not by re-derivation).
 *
 *  - ClusterXidStripeActivationRecord ("PGXA"), a single cluster-wide
 *    record in voting-disk region 5.  Written once by the activation
 *    coordinator; stride_mode_epoch is monotonic and never rewinds.
 *
 * Readers treat ANY integrity failure (magic / version / CRC / sanity)
 * as record-absent and fail closed (refuse activation / join / latch);
 * they never guess.  The validators below are pure and unit-tested.
 * ----------------------------------------------------------------
 */

#define CLUSTER_PGXS_MAGIC 0x50475853 /* "PGXS" */
#define CLUSTER_PGXS_VERSION 1

typedef struct ClusterXidStripeSlotRecord {
	uint32 magic;	/* CLUSTER_PGXS_MAGIC */
	uint32 version; /* CLUSTER_PGXS_VERSION */
	int32 node_id;	/* owning slot == declared node_id (0..STRIDE-1) */
	uint8 retired;	/* 0|1 -- set once by the 5.18 removal coordinator */
	uint8 _pad[3];
	uint64 owner_incarnation; /* first-claim admitted incarnation (immutable) */
	uint64 floor_full;		  /* per-slot activation floor (full-xid U64) */
	uint64 next_xid_hwm_full; /* herding high watermark (full-xid U64) */
	uint64 stride_mode_epoch; /* activation epoch the slot was claimed under */
	uint64 generation;		  /* monotonic torn-write guard (read newest) */
	uint32 crc32c;			  /* CRC32C over [magic .. generation] */
} ClusterXidStripeSlotRecord;

#define CLUSTER_PGXA_MAGIC 0x50475841 /* "PGXA" */
#define CLUSTER_PGXA_VERSION 1

typedef struct ClusterXidStripeActivationRecord {
	uint32 magic;				 /* CLUSTER_PGXA_MAGIC */
	uint32 version;				 /* CLUSTER_PGXA_VERSION */
	uint64 activated_floor_full; /* cluster activation floor (full-xid U64) */
	uint64 stride_mode_epoch;	 /* >= 1; monotonic, never rewinds */
	uint64 generation;			 /* monotonic torn-write guard (read newest) */
	uint32 crc32c;				 /* CRC32C over [magic .. generation] */
} ClusterXidStripeActivationRecord;

/*
 * Pure record integrity (rule 15) -- compute / validate.  Validation
 * failure means "treat as absent, fail closed"; expected_node is the
 * slot the record was read from (a record that names a different slot
 * is a misplaced write and is rejected).
 */
extern void cluster_xid_stripe_slot_record_compute_crc(ClusterXidStripeSlotRecord *rec);
extern bool cluster_xid_stripe_slot_record_valid(const ClusterXidStripeSlotRecord *rec,
												 int32 expected_node);
extern void cluster_xid_stripe_activation_record_compute_crc(ClusterXidStripeActivationRecord *rec);
extern bool cluster_xid_stripe_activation_record_valid(const ClusterXidStripeActivationRecord *rec);

#endif /* CLUSTER_XID_STRIPE_H */
