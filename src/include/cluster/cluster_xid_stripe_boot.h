/*-------------------------------------------------------------------------
 *
 * cluster_xid_stripe_boot.h
 *	  pgrac xid stripe activation / boot face (spec-6.15 D5b).
 *
 *	  Bridges the durable stripe records (voting-disk regions 4/5,
 *	  cluster_xid_stripe.h) to the membership join gate and to the
 *	  process-local stripe runtime latch:
 *
 *	    - qvotec (the sole voting-disk writer) SCANS region 5 at startup
 *	      and publishes the outcome to shmem, and services the single-
 *	      producer seed mailbox (LMON stages an activation record when
 *	      the cluster has none and this node is the seed candidate);
 *	    - the reconfig joiner gate (LMON) consults the published state
 *	      before letting this node become MEMBER: striping-on requires
 *	      a published activation record (holding until the seed lands),
 *	      striping-off refuses to join an activated cluster, and a
 *	      corrupt region 5 holds everyone (fail-closed, never guessed)
 *	      -- SQLSTATE 53RB1 is the refusal surface;
 *	    - every process lazily latches the stripe runtime from the
 *	      published state on first derivation use (activation is
 *	      single-shot and monotonic, so a one-way inactive->active
 *	      latch is race-free).
 *
 *	  Activation is IRREVERSIBLE: there is deliberately no API to clear
 *	  or rewrite the activation record (a second activation with a new
 *	  floor would alias the old one -- spec-6.15 §3.6).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xid_stripe_boot.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XID_STRIPE_BOOT_H
#define CLUSTER_XID_STRIPE_BOOT_H

#ifndef FRONTEND

#include "access/transam.h" /* FullTransactionId */
#include "c.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * Published knowledge about the durable activation record (region 5).
 * UNKNOWN until qvotec's first successful scan; ABSENT when at least
 * one disk was readable and no disk holds a record (all zeros / file
 * not yet grown); PUBLISHED once a valid record was adopted; CORRUPT
 * when garbage was found and no valid record exists anywhere -- the
 * fail-closed hold state (operator repair; a corrupt region is NEVER
 * seeded over, the real activation record might be under the damage).
 */
typedef enum ClusterXidStripeDiskState {
	CLUSTER_XID_STRIPE_DISK_UNKNOWN = 0,
	CLUSTER_XID_STRIPE_DISK_ABSENT,
	CLUSTER_XID_STRIPE_DISK_PUBLISHED,
	CLUSTER_XID_STRIPE_DISK_CORRUPT
} ClusterXidStripeDiskState;

/* Joiner-gate verdict for the stripe face (consumed by reconfig). */
typedef enum ClusterXidStripeJoinVerdict {
	CLUSTER_XID_STRIPE_JOIN_PROCEED = 0, /* stripe face resolved; admit */
	CLUSTER_XID_STRIPE_JOIN_HOLD,		 /* not resolved yet; retry next tick */
	CLUSTER_XID_STRIPE_JOIN_REFUSE		 /* config mismatch; fail-closed 53RB1 */
} ClusterXidStripeJoinVerdict;

/*
 * Published knowledge about THIS node's region-4 stripe slot (D5c).
 * MINE = a valid record naming this node, not retired (rejoin resumes
 * it — owner identity is the FIRST-claim admitted incarnation and is
 * never compared against the current boot's incarnation, which is
 * freshly generated every start); RETIRED = the slot was permanently
 * consumed by a spec-5.18 removal (v1 never reuses it — fresh joins
 * of the same node_id refuse with 53RB1); CORRUPT fails closed.
 */
typedef enum ClusterXidStripeSlotState {
	CLUSTER_XID_STRIPE_SLOT_UNKNOWN = 0,
	CLUSTER_XID_STRIPE_SLOT_ABSENT,
	CLUSTER_XID_STRIPE_SLOT_MINE,
	CLUSTER_XID_STRIPE_SLOT_RETIRED,
	CLUSTER_XID_STRIPE_SLOT_CORRUPT
} ClusterXidStripeSlotState;

/* shmem sizing / init (wired from cluster_init_shmem). */
extern Size cluster_xid_stripe_shmem_size(void);
extern void cluster_xid_stripe_shmem_init(void);

/*
 * qvotec face (sole voting-disk writer).  scan_disks reads region 5
 * across the opened voting disks and publishes the verdict; it is
 * called once at qvotec startup and again by the poll loop while the
 * state is still UNKNOWN (a disk may come up late).  service_seed
 * writes a staged activation record to all disks and ACKs the mailbox
 * when a majority accepted it (re-scanning first: if a record appeared
 * meanwhile it is adopted instead of overwritten).
 */
extern void cluster_xid_stripe_scan_disks(const int *fds, int n_disks);
extern void cluster_xid_stripe_service_seed(const int *fds, int n_disks);

/*
 * Joiner-gate policy (LMON / reconfig).  self_may_seed reports whether
 * this node is the deterministic seed candidate (lowest fresh-alive
 * declared node) -- computed by the caller, which owns the alive view.
 * On HOLD with striping enabled and the record ABSENT, the gate stages
 * a seed request (single-shot) for qvotec when self_may_seed.
 */
extern ClusterXidStripeJoinVerdict cluster_xid_stripe_join_gate(bool self_may_seed);

/*
 * One-way lazy latch: populate the process-local stripe runtime from
 * the published activation state (no-op until PUBLISHED).  Called by
 * the runtime wrappers in cluster_xid_stripe.c on first use; safe in
 * any backend / aux process (reads shmem with one lock acquisition,
 * then never again for the life of the process).
 */
extern void cluster_xid_stripe_lazy_latch(void);

/*
 * Companion lazy latch for the mxid stripe face (spec-7.1 D3-a):
 * populates the process-local mxid runtime (cluster_mxid_stripe.c)
 * from the published activation state when the region-6 slot carried a
 * valid "PGXM" extension record.  A published activation WITHOUT the
 * extension (pre-extension cluster) leaves the mxid face unlatched
 * forever -- allocation stays vanilla dense and every foreign multi
 * stays underivable fail-closed.
 */
extern void cluster_mxid_stripe_lazy_latch(void);

/*
 * D5e allocation clamp face: this node's durable per-slot floor (the
 * region-4 claim), or InvalidFullTransactionId when no claim has been
 * published.  Read under XidGenLock by GetNewTransactionId; the value
 * is monotonic (set once at claim, never lowered).
 */
extern FullTransactionId cluster_xid_stripe_my_slot_floor(void);

/* D6 observability snapshot consumed by pg_cluster_state (dump keys). */
typedef struct ClusterXidStripeObs {
	uint32 disk_state; /* ClusterXidStripeDiskState */
	uint32 slot_state; /* ClusterXidStripeSlotState */
	uint64 activated_floor_full;
	uint64 stride_mode_epoch;
	uint64 my_slot_floor_full; /* 0 = unclaimed */
	uint64 my_hwm_on_disk;
	uint64 herding_floor_full;
	uint64 cluster_min_active_hwm;
	uint64 cluster_max_active_hwm;
	uint64 replay_floor_full;
	uint32 replay_active_bitmap;
	uint32 activated_mxid_floor; /* "PGXM" extension floor; 0 = absent (spec-7.1 D3-a) */
} ClusterXidStripeObs;

extern void cluster_xid_stripe_observe(ClusterXidStripeObs *obs);

/* Published-state introspection (observability / tests). */
extern ClusterXidStripeDiskState cluster_xid_stripe_disk_state(void);
extern bool cluster_xid_stripe_get_activation(uint64 *floor_full, uint64 *epoch,
											  uint64 *generation);
extern ClusterXidStripeSlotState cluster_xid_stripe_slot_state(void);

/*
 * spec-5.18 removal hook (D5c): durably mark target_node's stripe slot
 * retired BEFORE the removal point of no return.  Blocking-bounded
 * (mirrors the join-marker submit); returns true when the retired
 * record is majority-durable OR the cluster was never activated
 * (nothing to retire).  false = not durable yet — the removal driver
 * stays in its pre-commit phase and retries next tick (fail-closed:
 * removal never commits ahead of the retired mark).
 * owner_incarnation_hint seeds the tombstone when the slot was never
 * claimed (the removal driver passes the last admitted incarnation).
 */
extern bool cluster_xid_stripe_submit_retire(int32 target_node, uint64 owner_incarnation_hint);

/* qvotec self-incarnation accessor (the canonical durable identity
 * seed this boot presents; consumed by the D5c slot claim). */
extern uint64 cluster_qvotec_self_incarnation_value(void);

/*
 * D5d replay face: WAL redo publishes the replay-learned stripe
 * knowledge (activation floor + active slot set); the hot-standby
 * KnownAssignedXids gap-fill consumes it.  should_fill(xid) is TRUE
 * for pre-striping history (below floor) and active classes; FALSE
 * only for an above-floor value of an inactive class (which, by the
 * per-thread WAL ordering invariant, was never issued).
 */
/*
 * D3 counter herding.  herding_tick runs from the qvotec poll (sole
 * voting-disk writer): sweeps the peers' published region-4 hwm,
 * publishes min/max, and durably advances this node's hwm promise
 * (publish-before-arm) which the allocation clamp consumes as the
 * herding floor.  window_exceeded is the hard-limit predicate behind
 * SQLSTATE 53RB2 (candidate ahead of the slowest active slot by more
 * than slack x 64 -> refuse to issue, fail-closed).
 */
extern void cluster_xid_stripe_herding_tick(const int *fds, int n_disks);
extern FullTransactionId cluster_xid_stripe_herding_floor(void);
extern bool cluster_xid_stripe_window_exceeded(FullTransactionId candidate);

extern void cluster_xid_stripe_replay_note_join(uint64 floor_full, uint64 epoch, int slot);
extern void cluster_xid_stripe_replay_note_retire(int slot);
extern bool cluster_xid_stripe_replay_filter_active(void);
extern bool cluster_xid_stripe_replay_should_fill(TransactionId xid);

#endif /* USE_PGRAC_CLUSTER */

#endif /* FRONTEND */

#endif /* CLUSTER_XID_STRIPE_BOOT_H */
