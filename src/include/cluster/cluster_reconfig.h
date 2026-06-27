/*-------------------------------------------------------------------------
 *
 * cluster_reconfig.h
 *	  pgrac cluster reconfig coordinator — internal-only A scope
 *	  (spec-2.29).
 *
 *	  CSSD DEAD edge → LMON deterministic coordinator (min of survivor
 *	  set under Q2 A'' rule) → epoch++ → PROCSIG_CLUSTER_RECONFIG_START
 *	  broadcast to local backends → ProcessInterrupts re-check
 *	  cluster_qvotec_in_quorum + freeze flag → writable transactions
 *	  ereport 53R60 (or 53R50 if quorum lost).
 *
 *	  Sprint A Step 1 scope (this header):
 *	    - ReconfigEvent struct (8-field event metadata, 128-node
 *	      bitmap via uint8[16], cssd_dead_generation for P1.2 dedup)
 *	    - ClusterReconfigState shmem region (LWLock-guarded last
 *	      applied event + 3 atomic counters)
 *	    - 6 entry-point APIs: shmem_size/init/register/get_last_event
 *	      + broadcast_local_procsig (P1.3 split) +
 *	      apply_epoch_bump_as_coordinator + lmon_tick (Step 2 body)
 *	    - check_pending_in_proc_interrupts (Step 2 D4 ProcessInterrupts)
 *	    - publish_event (internal)
 *	    - CLUSTER_RECONFIG_DEAD_BITMAP_BYTES = 16 (128 nodes)
 *
 *	  Steps 2-7 add: lmon_tick body, ProcessInterrupts integration,
 *	  envelope verify path observe_remote (D20), SRF view 9 cols,
 *	  TAP 099 L1-L10, regress + manuals, catalog surface delta,
 *	  ship gate.
 *
 *	  Spec authority: pgrac:specs/spec-2.29-reconfig-coordinator-
 *	  internal.md (DRAFT v0.3; 21 deliverables / 10 invariants
 *	  I1-I10 / 14 risks).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_reconfig.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER);disable-cluster builds get stub bodies
 *	  in cluster_reconfig.c so caller code paths stay portable.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECONFIG_H
#define CLUSTER_RECONFIG_H

#include "c.h"
#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES (spec-5.13 clean_departed_epoch) */


/*
 * 128-bit bitmap holding declared peers' DEAD state.  Sized for
 * CLUSTER_MAX_NODES = 128 (see cluster_conf.h).  uint8[16] gives
 * natural byte addressing for the siphash2-4 hash input + clean
 * hex serialization in pg_cluster_reconfig_state.dead_bitmap.
 *
 * Per spec-2.29 P2.8 fix: v0.1 wrote uint64 dead_bitmap which only
 * covers 64 nodes;v0.2/v0.3 promoted to uint8[16] for 128 nodes.
 */
#define CLUSTER_RECONFIG_DEAD_BITMAP_BYTES 16

/*
 * spec-5.14 D6 — number of touched_peers ingress classes (mirrors
 * CLUSTER_TOUCH_KIND_COUNT in cluster_touched_peers.h; kept as a plain
 * macro here to avoid a header dependency, with a StaticAssert in
 * cluster_reconfig.c enforcing the two stay equal).
 */
#define CLUSTER_RECONFIG_TOUCH_KIND_COUNT 5


/*
 * Observer role recorded in ReconfigEvent.observer_role:
 *	  0 = none      → never-applied state (event_id = 0)
 *	  1 = coordinator → self computed Q2 A'' rule + applied epoch++
 *	  2 = survivor  → received via envelope piggyback (Steps 2-3)
 */
#define CLUSTER_RECONFIG_OBSERVER_NONE 0
#define CLUSTER_RECONFIG_OBSERVER_COORDINATOR 1
#define CLUSTER_RECONFIG_OBSERVER_SURVIVOR 2


/*
 * spec-5.14 D3 — reconfig kind: fail-stop (a member crashed / partitioned,
 * its volatile state may be lost / torn) vs clean leave (a member is leaving
 * gracefully after flushing).  These have different safety rules; this spec
 * implements only the fail-stop branch.  CLEAN_LEAVE is reserved for
 * spec-5.13 — it currently has no producer and the D4 dispatch treats it
 * defensively (conservative fail-stop handling + a LOG WARNING).
 */
typedef enum ClusterReconfigKind {
	RECONFIG_KIND_NONE = 0,		  /* never-applied / initial */
	RECONFIG_KIND_FAIL_STOP = 1,  /* CSSD DEAD edge — this spec, live */
	RECONFIG_KIND_CLEAN_LEAVE = 2 /* reserved (spec-5.13); no producer yet */
} ClusterReconfigKind;


/*
 * spec-5.14 D4 — the four possible outcomes of the ProcessInterrupts reconfig
 * dispatch, factored out of the ereport mechanism so the decision matrix
 * (§3.2) is a pure, unit-testable function (U8).
 */
typedef enum ClusterReconfigVerdict {
	RECONFIG_VERDICT_ABSORB = 0,	 /* no abort (idle / non-touched read-only) */
	RECONFIG_VERDICT_ABORT_TOUCHED,	 /* touched ∩ dead, in quorum → 40R01 */
	RECONFIG_VERDICT_ABORT_RECONFIG, /* non-touched writable, in quorum → 53R60 */
	RECONFIG_VERDICT_ABORT_QUORUM	 /* lost quorum (touched or writable) → 53R50 */
} ClusterReconfigVerdict;

/*
 * Pure decision matrix (no side effects).  touched = the tx consumed volatile
 * state from a fail-stopped member; has_top_xid = a durable write xid is
 * assigned; in_quorum = this node still holds quorum.
 *   - touched (read OR write)        → ABORT_TOUCHED (or ABORT_QUORUM if lost)
 *   - non-touched read-only          → ABSORB (INV-TP5; even if quorum lost,
 *                                       handled by the spec-2.28 fence path)
 *   - non-touched writable           → ABORT_RECONFIG (or ABORT_QUORUM if lost)
 */
extern ClusterReconfigVerdict cluster_reconfig_classify_verdict(bool touched, bool has_top_xid,
																bool in_quorum);


/*
 * ReconfigEvent — one published reconfig event.
 *
 *	  Field layout natural-aligned (no pg_attribute_packed); total
 *	  size is ~80 bytes including required padding.  StaticAssertDecl
 *	  enforces upper bound in cluster_reconfig.c.
 *
 *	  Per spec-2.29 P2.8 fix: do NOT assume exact 64-byte sizeof;
 *	  shmem region size is computed via sizeof(ClusterReconfigState)
 *	  expression rather than literal byte count.
 */
typedef struct ReconfigEvent {
	/*
	 * event_id — siphash2-4(dead_bitmap[16] || cssd_dead_generation).
	 *
	 * MUST NOT include old_epoch in the hash input (per spec-2.29
	 * P1.2 fix): tick1 bump N→N+1 then tick2 same dead_bitmap with
	 * old_epoch=N+1 would compute a different hash → infinite bump
	 * loop.  Hash uses (dead_bitmap, cssd_dead_generation) so:
	 *   - same dead within one DEAD episode → same dead_gen →
	 *     same event_id → dedup skip
	 *   - rejoin-then-redeath with same dead_bitmap → dead_gen
	 *     advanced → different event_id → re-fire
	 *
	 * 0 means "never applied" (the well-known sentinel value;
	 * siphash output 0 is astronomically rare and treated as
	 * fresh-tick anyway).
	 */
	uint64 event_id;

	/* min(QVOTEC_in_quorum_self ∪ CSSD_alive_set − CSSD_dead_set) */
	int32 coordinator_node_id;
	uint32 _pad0;

	uint64 old_epoch;
	uint64 new_epoch; /* coordinator: old+1;survivor: observed via piggyback */

	uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];

	TimestampTz applied_at;

	int32 observer_role; /* CLUSTER_RECONFIG_OBSERVER_* */
	uint32 _pad1;

	uint64 event_seq;			 /* per-process monotonic apply counter */
	uint64 cssd_dead_generation; /* P1.2 — snapshot at apply time */

	uint8 reconfig_kind; /* spec-5.14 D3: ClusterReconfigKind (shmem-only,
								  * never on the wire; CLEAN_LEAVE reserved) */
	uint8 _pad2[7];
} ReconfigEvent;


/*
 * ClusterReconfigState — shmem region "pgrac cluster reconfig".
 *
 *	  LWLock guards publish path (read of last_applied is lock-shared,
 *	  write is lock-exclusive — see cluster_reconfig_publish_event).
 *	  Atomic counters need no lock.
 */
typedef struct ClusterReconfigState {
	LWLock lock;
	ReconfigEvent last_applied;				  /* event_id=0 → never applied */
	pg_atomic_uint64 apply_counter;			  /* total events observed */
	pg_atomic_uint64 dedup_skip_counter;	  /* duplicate event_id skipped */
	pg_atomic_uint64 procsig_broadcast_count; /* PROCSIG broadcast tally */

	/* spec-5.14 D6 — touched_peers fail-stop observability (Q8 A': these
	 * live in this existing region; the region count is unchanged). */
	pg_atomic_uint64 touched_abort_count; /* tx aborted 40R01 (touched ∩ dead) */
	pg_atomic_uint64 touched_stamp_count; /* total cross-node ingress stamps */
	pg_atomic_uint64 touched_stamp_by_kind[CLUSTER_RECONFIG_TOUCH_KIND_COUNT];
	pg_atomic_uint64 clean_leave_rejected_count;	/* defensive (unexpected) CLEAN_LEAVE hits */
	pg_atomic_uint64 clean_leave_drain_grace_count; /* spec-5.13 S6 CL-I12 drain-grace continues */

	/*
	 * spec-5.13 D3 (CL-I13) — clean-departed suppression.  A node that left
	 * cleanly (a CLEAN_LEAVE reconfig committed naming it) stops heart-beating
	 * once it exits.  Without suppression cluster_reconfig_lmon_tick would treat
	 * that CSSD DEAD as a crash → a SECOND, spurious fail-stop reconfig (and a
	 * spurious 40R01 of any drain-grace survivor tx whose touched_peers still
	 * names it).  These are set when a survivor observes the CLEAN_LEAVE commit,
	 * and rebuilt at startup from the durable §2.5 COMMITTED marker (the epoch
	 * is NOT durable, so the marker is also the epoch-floor recovery source —
	 * P1-V0.7).  effective_dead = cssd_dead & ~clean_departed_bitmap.  Guarded
	 * by `lock` (set EXCLUSIVE, read SHARED in the lmon_tick masking).
	 */
	uint8 clean_departed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 clean_departed_epoch[CLUSTER_MAX_NODES]; /* per-node leave epoch; 0 = not departed */
	pg_atomic_uint64 clean_departed_count; /* lifetime departures recorded (observability) */
} ClusterReconfigState;


/* ============================================================
 * Shmem region management (Step 1 D2).
 * ============================================================
 */

extern Size cluster_reconfig_shmem_size(void);
extern void cluster_reconfig_shmem_init(void);
extern void cluster_reconfig_shmem_register(void);


/* ============================================================
 * Observability accessor (Step 1 D2 partial / Step 3 D5b final).
 * ============================================================
 */

/*
 * Always populates *out (P2.9 always-1-row contract — never returns
 * false / "no data").  Never-applied state surfaces as
 * event_id=0, observer_role=CLUSTER_RECONFIG_OBSERVER_NONE,
 * applied_at=0.  Caller distinguishes via event_id.
 */
extern void cluster_reconfig_get_last_event(ReconfigEvent *out);


/* ============================================================
 * Coordinator path APIs (Step 2 D2 wiring).
 * Skeletons present in Step 1;bodies land in Step 2.
 * ============================================================
 */

/*
 * Step 2 entry: LMON tick calls this every iteration.  Stateless
 * deterministic — re-runs Q2 A'' coordinator computation each tick;
 * dedup via event_id (P1.2).  Step 1 stub: silent no-op (so cluster_lmon.c
 * can be wired in Step 2 D3 without dangling symbol).
 */
extern void cluster_reconfig_lmon_tick(void);

/*
 * Step 2 P1.3 (a): every in_quorum survivor (NOT just coordinator)
 * broadcasts PROCSIG_CLUSTER_RECONFIG_START to its local backends.
 * Step 1 stub: count broadcast call in procsig_broadcast_count atomic;
 * Step 2 body: real ProcArray iteration + SendProcSignal.
 */
extern void cluster_reconfig_broadcast_local_procsig(void);

/*
 * Step 2 P1.3 (b): only the deterministically-chosen coordinator
 * calls this — invokes cluster_epoch_advance_for_reconfig (D18) and
 * publishes event with observer_role=CLUSTER_RECONFIG_OBSERVER_COORDINATOR.
 * Step 1 stub: build minimal ReconfigEvent + publish (no real epoch++);
 * Step 2 body: full D18 call + GetXLogInsertRecPtr + publish.
 */
extern void cluster_reconfig_apply_epoch_bump_as_coordinator(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 coordinator_node_id,
	uint64 cssd_dead_generation);


/* ============================================================
 * ProcessInterrupts integration (Step 2 D4).
 * ============================================================
 */

/*
 * Called from tcop/postgres.c::ProcessInterrupts when the per-backend
 * cluster_reconfig_start_pending sig_atomic_t is set.  Read-clear,
 * commit-critical-section guard (I6), then enumerate fail-closed
 * cause (53R50 quorum lost / 53R60 reconfig in progress).
 * Step 1 stub: no-op (handler body lands in Step 2).
 */
extern void cluster_reconfig_check_pending_in_proc_interrupts(void);


/* ============================================================
 * Internal publish helper (Step 1 D2).
 * ============================================================
 */

extern void cluster_reconfig_publish_event(const ReconfigEvent *evt);


/* ============================================================
 * Pure-function helper exposed for unit test coverage (Step 2 T-reconfig-2).
 *
 *	event_id = hash_bytes_extended(dead_bitmap[16] || cssd_dead_generation, seed=0).
 *	Deterministic, no state.  Per spec-2.29 P1.2 fix the hash MUST NOT
 *	include old_epoch (self-loops on coordinator bump).
 * ============================================================
 */

extern uint64
cluster_reconfig_compute_event_id(const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
								  uint64 cssd_dead_generation);


/* ============================================================
 * Counter accessors (Step 2 + Step 3 SRF + unit test coverage).
 *
 *	apply_counter:           total reconfig events published
 *	dedup_skip_counter:      total events skipped due to event_id ==
 *	                         last_applied.event_id
 *	procsig_broadcast_count: total PROCSIG broadcast tick invocations
 *	                         (always-survivor branch, see I7)
 * ============================================================
 */

extern uint64 cluster_reconfig_get_apply_counter(void);
extern uint64 cluster_reconfig_get_dedup_skip_counter(void);
extern uint64 cluster_reconfig_get_procsig_broadcast_count(void);


/* ============================================================
 * spec-5.14 D6 — touched_peers fail-stop observability counters.
 *
 *	  These live in the existing ClusterReconfigState region (Q8 A':
 *	  region count unchanged).  The note_* mutators are called from the
 *	  hot cross-node ingress path (stamp) and the D4 dispatch (abort /
 *	  clean-leave reject); they are atomic and never ereport (L213).
 *	  The `kind` argument of note_touched_stamp is a ClusterTouchKind
 *	  (passed as int to avoid a header dependency on
 *	  cluster_touched_peers.h).
 * ============================================================
 */
extern void cluster_reconfig_note_touched_stamp(int kind);
extern uint64 cluster_reconfig_note_touched_abort(void); /* returns prior count (LOG-once gate) */
extern void cluster_reconfig_note_clean_leave_rejected(void);
/* spec-5.13 S6 (CL-I12) — a touched survivor tx was allowed to continue under a
 * CLEAN_LEAVE reconfig (drain-grace) instead of being aborted 40R01 (fail-stop).
 * returns the prior count (LOG-once gate). */
extern uint64 cluster_reconfig_note_clean_leave_drain_grace(void);

extern uint64 cluster_reconfig_get_touched_abort_count(void);
extern uint64 cluster_reconfig_get_touched_stamp_count(void);
extern uint64 cluster_reconfig_get_touched_stamp_by_kind(int kind);
extern uint64 cluster_reconfig_get_clean_leave_rejected_count(void);
extern uint64 cluster_reconfig_get_clean_leave_drain_grace_count(void);


/* ============================================================
 * spec-5.13 D3 (CL-I13) — clean-departed suppression + epoch-floor recovery.
 *
 *	record: mark node_id as cleanly departed at leave_epoch (a survivor that
 *	  observed the CLEAN_LEAVE commit, or the startup rebuild from a durable
 *	  COMMITTED §2.5 marker).  Sets the bitmap bit + per-node epoch.  When
 *	  raise_epoch_floor is true (startup rebuild path, P1-V0.7) it also raises
 *	  the local cluster epoch floor to >= leave_epoch — the epoch is not durable,
 *	  so the marker is the only proof the cluster reached that epoch; this path
 *	  is exempt from the IC OBSERVE_MAX_JUMP bound (it is startup recovery, not a
 *	  hostile envelope).
 *	clear: spec-5.15 rejoin (incarnation bump) or operator removes the entry so
 *	  the node can rejoin as a live member.
 * ============================================================ */
extern void cluster_reconfig_record_clean_departed(int32 node_id, uint64 leave_epoch,
												   bool raise_epoch_floor);
extern void cluster_reconfig_clear_clean_departed(int32 node_id);
extern bool cluster_reconfig_is_clean_departed(int32 node_id);
extern uint64 cluster_reconfig_get_clean_departed_epoch(int32 node_id);
extern uint64 cluster_reconfig_get_clean_departed_count(void);

/*
 * spec-5.13 D3 — coordinator publishes the CLEAN_LEAVE reconfig event (the
 * commit point of the §3.1 two-phase commit): bump the membership epoch and
 * publish reconfig_kind=CLEAN_LEAVE with dead_bitmap = {leaving_node_id}, then
 * record the node clean-departed at the new epoch.  Mirrors
 * cluster_reconfig_apply_epoch_bump_as_coordinator but for the cooperative
 * leave kind (no write-fence marker — the leaving node drained voluntarily; the
 * durable record is the §2.5 leave-intent marker, bracketed around this call by
 * the driver).  Returns the new epoch E (the driver stamps it into the COMMITTED
 * marker).  The driver writes the COMMITTING marker BEFORE this call and the
 * COMMITTED marker AFTER it (durable-commit ordering, §2.5).  `baseline_epoch`
 * is the epoch the leave committed against; the bump is a guarded CAS that
 * fails closed (returns 0, no commit) if a concurrent reconfig already moved
 * the epoch off the baseline (CL-I3, safe at any node count).
 */
extern uint64 cluster_reconfig_apply_clean_leave_as_coordinator(int32 leaving_node_id,
																uint64 baseline_epoch);


#endif /* CLUSTER_RECONFIG_H */
