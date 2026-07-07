/*-------------------------------------------------------------------------
 *
 * cluster_mxid_stripe.h
 *	  pgrac MultiXactId space segmentation -- pure stripe derivation layer.
 *
 *	  Companion of the spec-6.15 xid stripe (cluster_xid_stripe.h): with
 *	  cluster.xid_striping enabled and the stripe face activated, node
 *	  slot k only ever issues MultiXactIds congruent to k (mod
 *	  CLUSTER_MXID_STRIDE) at or above the durable mxid activation
 *	  floor, so a raw multixact id above the floor is self-describing:
 *	  its origin slot is mxid mod STRIDE.
 *
 *	  Half-space window: PostgreSQL has no FullMultiXactId (no epoch
 *	  counter to widen against, unlike FullTransactionId), so the
 *	  derivation is valid only inside the half-space
 *	  [floor, floor + 2^31) of the 32-bit circular id space, where the
 *	  distance (uint32)(mxid - floor) < 2^31 is unambiguous.  At or
 *	  beyond floor + 2^31 the wraparound interpretation of a value
 *	  becomes ambiguous, so derivation reports underivable and the
 *	  ALLOCATOR refuses to issue past the boundary (fail-closed on both
 *	  sides -- a full wrap that would alias the half-space can never be
 *	  reached).  Multixact ids are consumed only when multiple
 *	  transactions lock/update the same row concurrently, so 2^31 ids
 *	  from activation is an operationally distant boundary; a guardrail
 *	  counter tracks refusals.
 *
 *	  This header ships the pure derivation layer (no shmem, no locks,
 *	  no ereport -- underivable is reported as -1 and callers must fail
 *	  closed) plus thin runtime wrappers over a process-local latched
 *	  runtime, and the durable activation-slot extension record that
 *	  carries the mxid floor (rides the region-6 xid activation slot at
 *	  a fixed byte offset; all-zeros reads back as record-absent, the
 *	  fail-closed empty state).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_mxid_stripe.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-7.1-cross-instance-positive-interread.md
 *	  Frontend-safe: depends only on c.h (MultiXactId) and
 *	  cluster_xid_stripe.h (stride constant, itself frontend-safe).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MXID_STRIPE_H
#define CLUSTER_MXID_STRIPE_H

#include "c.h"

#include "cluster/cluster_xid_stripe.h"

/* One stride shared with the xid stripe (same declared-slot space). */
#define CLUSTER_MXID_STRIDE CLUSTER_XID_STRIDE

/* Half-space width: derivation window size above the activation floor. */
#define CLUSTER_MXID_HALFSPACE ((uint32)0x80000000)

/*
 * Derive the origin slot of mxid against the mxid activation floor.
 * Returns the slot (0 .. CLUSTER_MXID_STRIDE-1), or -1 when mxid or
 * floor_mxid is invalid or mxid falls outside the half-space
 * [floor_mxid, floor_mxid + 2^31) (underivable -- callers must fail
 * closed, never guess).  The distance test uses uint32 modular
 * arithmetic, so it stays exact when the half-space spans the 2^32
 * boundary.
 */
extern int cluster_mxid_origin_slot_floored(MultiXactId mxid, MultiXactId floor_mxid);

/*
 * Smallest MultiXactId >= next whose value is congruent to slot (mod
 * CLUSTER_MXID_STRIDE).  The one in-class position whose value is
 * InvalidMultiXactId (0, reachable for slot 0 right after a 2^32
 * carry) is skipped to the next in-class position.  Pure candidate
 * arithmetic for the striped allocator (runs under MultiXactGenLock,
 * which this function does NOT take).
 */
extern MultiXactId cluster_mxid_next_striped(MultiXactId next, int slot);

/*
 * Guardrail predicate: does candidate fall at or beyond the half-space
 * boundary floor_mxid + 2^31?  True also when floor_mxid is invalid
 * (nothing provable -- fail closed).  The allocator refuses to issue a
 * candidate for which this reports true, so the derivation window can
 * never silently alias.
 */
extern bool cluster_mxid_halfspace_exceeded(MultiXactId candidate, MultiXactId floor_mxid);

/*
 * Runtime wrappers over the process-local latched mxid stripe state.
 * cluster_mxid_origin_slot derives mxid against the latched floor;
 * returns -1 when the mxid stripe runtime is not latched active or
 * derivation fails.  cluster_mxid_is_mine reports whether mxid derives
 * to this node's slot (false whenever underivable).
 */
extern int cluster_mxid_origin_slot(MultiXactId mxid);
extern bool cluster_mxid_is_mine(MultiXactId mxid);

/*
 * Allocation-side slot gate consumed by GetNewMultiXactId under
 * MultiXactGenLock: this node's stripe slot when striping is enabled,
 * the declared node identity fits the stripe width AND the mxid floor
 * has been latched (the mxid face can trail the xid face: a cluster
 * activated before the mxid extension record existed keeps allocating
 * vanilla dense ids, which stay below-floor underivable -- honest
 * degrade, never a misattribution).  -1 = allocation stays vanilla.
 */
extern int cluster_mxid_allocation_slot(void);

/* Latched mxid activation floor (InvalidMultiXactId until latched). */
extern MultiXactId cluster_mxid_stripe_floor(void);

/*
 * Latch the process-local mxid stripe runtime.  Ownership mirrors
 * cluster_xid_stripe_latch_runtime: the boot face calls this once per
 * process on first use; values are immutable for the life of the
 * process.  active=true with an out-of-range slot or invalid floor is
 * treated as inactive (fail-closed, no partial activation).
 */
extern void cluster_mxid_stripe_latch_runtime(bool active, int my_slot, MultiXactId floor_mxid);

/*
 * ----------------------------------------------------------------
 * Durable mxid activation extension record ("PGXM")
 *
 * Rides the region-6 activation slot (512 bytes, sole writer qvotec)
 * at fixed byte offset CLUSTER_PGXM_SLOT_OFFSET, after the "PGXA"
 * ClusterXidStripeActivationRecord at offset 0.  Written in the SAME
 * single 512-byte slot write as the PGXA record by the activation
 * seed service, so the xid floor and the mxid floor land atomically
 * together.  The PGXA validator is deliberately untouched: a slot
 * written by a pre-extension binary reads back all-zeros here, which
 * the validator below rejects as record-absent -- the mxid face then
 * stays inactive (fail-closed) while the xid face works unchanged.
 * ----------------------------------------------------------------
 */

#define CLUSTER_PGXM_MAGIC 0x5047584D /* "PGXM" */
#define CLUSTER_PGXM_VERSION 1

/* Byte offset of the extension inside the region-6 activation slot. */
#define CLUSTER_PGXM_SLOT_OFFSET 64

typedef struct ClusterMxidStripeExtensionRecord {
	uint32 magic;				 /* CLUSTER_PGXM_MAGIC */
	uint32 version;				 /* CLUSTER_PGXM_VERSION */
	uint32 activated_mxid_floor; /* cluster mxid activation floor */
	uint32 _pad0;				 /* zero on write; CRC-covered */
	uint64 generation;			 /* monotonic torn-write guard */
	uint32 crc32c;				 /* CRC32C over [magic .. generation] */
} ClusterMxidStripeExtensionRecord;

StaticAssertDecl(sizeof(ClusterMxidStripeExtensionRecord) == 32,
				 "ClusterMxidStripeExtensionRecord must be 32 bytes slot-stable");
StaticAssertDecl(CLUSTER_PGXM_SLOT_OFFSET >= sizeof(ClusterXidStripeActivationRecord),
				 "PGXM extension must not overlap the PGXA record");
StaticAssertDecl(CLUSTER_PGXM_SLOT_OFFSET + sizeof(ClusterMxidStripeExtensionRecord) <= 512,
				 "PGXM extension must fit the 512-byte voting slot");

/*
 * Pure record integrity (rule 15) -- compute / validate.  Validation
 * failure means "treat as absent, fail closed": the mxid face stays
 * inactive, it never guesses a floor.
 */
extern void cluster_mxid_stripe_extension_record_compute_crc(ClusterMxidStripeExtensionRecord *rec);
extern bool cluster_mxid_stripe_extension_record_valid(const ClusterMxidStripeExtensionRecord *rec);

#endif /* CLUSTER_MXID_STRIPE_H */
