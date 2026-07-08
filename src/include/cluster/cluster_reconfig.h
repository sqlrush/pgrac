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

#include "cluster/cluster_conf.h"		/* CLUSTER_MAX_NODES (spec-5.13 clean_departed_epoch) */
#include "cluster/cluster_marker_async.h"
#include "cluster/cluster_membership.h" /* ClusterMembershipTable (spec-5.15 D2 SSOT) */

struct Latch; /* spec-5.15 D4 — join-marker qvotec mailbox latch (pointer only) */


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
	RECONFIG_KIND_NONE = 0,		   /* never-applied / initial */
	RECONFIG_KIND_FAIL_STOP = 1,   /* CSSD DEAD edge — spec-5.14, live */
	RECONFIG_KIND_CLEAN_LEAVE = 2, /* cooperative leave — spec-5.13 */
	/*
	 * spec-5.15 D3 (INV-J11) — single discriminator, append-only.  Online
	 * declared-node join is two-phase: JOIN_PENDING (Phase 1, joiner JOINING)
	 * then JOIN_COMMITTED (Phase 2, joiner MEMBER).  The affected set for these
	 * is carried in join_bitmap (leave kinds keep dead_bitmap).
	 */
	RECONFIG_KIND_JOIN_PENDING = 3,
	RECONFIG_KIND_JOIN_COMMITTED = 4,
	/*
	 * spec-5.18 D3 — permanent node removal (decommission).  Append-only after
	 * 5.15's JOIN_COMMITTED (spec-5.17 RETIRED -> no LEAVE_DRAINING reservation).
	 * The removed set is carried in ClusterReconfigState.removed_bitmap (a leave-
	 * direction kind; NODE_REMOVED's own event_id folds removed_bitmap +
	 * removal_event_id so a clean-left removal that leaves dead_bitmap unchanged is
	 * NOT deduped away, R14).
	 */
	RECONFIG_KIND_NODE_REMOVED = 5
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

	/*
	 * spec-5.15 D3 — join-edge affected set.  Leave kinds (FAIL_STOP /
	 * CLEAN_LEAVE) carry their set in dead_bitmap and leave this empty; join
	 * kinds (JOIN_PENDING / JOIN_COMMITTED) carry the admitted joiner(s) here
	 * and leave dead_bitmap empty.  Append-only field; bumps sizeof from 88 to
	 * 104 (StaticAssert bound widened to 112 below).
	 */
	uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
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

	/*
	 * spec-5.18 D3 (INV-LF1) — permanently-removed set.  A node permanently
	 * removed (decommissioned) is durable membership_state==REMOVED; this bitmap is
	 * the masking parallel (distinct from clean_departed_bitmap dormant / dead_bitmap
	 * liveness).  cluster_reconfig_lmon_tick masks it out of the effective dead set:
	 *   effective_dead = cssd_dead & ~clean_departed_bitmap & ~removed_bitmap
	 * so a removed node's subsequent CSSD DEAD/ALIVE is ignored (never re-triggers a
	 * reconfig and never passively re-admits it).  qvotec also ORs it into the
	 * steady-state fence baseline (INV-LF10) so a removed node stays fenced forever.
	 * Set EXCLUSIVE at the removal commit / rebuilt at startup from the durable §2.5
	 * removal marker (the epoch is NOT durable, so removed_epoch is the floor source).
	 */
	uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 removed_epoch[CLUSTER_MAX_NODES]; /* per-node removal epoch; 0 = not removed */
	pg_atomic_uint64 removed_count;			 /* lifetime removals recorded (observability) */

	/*
	 * spec-5.15 D2 — online-join membership SSOT.  The decision table (the
	 * last_admitted_incarnation[] monotonic floor + per-node membership_state[])
	 * lives here so every backend shares one membership view; cluster_membership.c
	 * attaches to &this->membership at shmem init and mutates it under `lock`
	 * (INV-J7 floor / INV-J8 decision SSOT).
	 */
	ClusterMembershipTable membership;

	/*
	 * spec-5.15 D4 — pending-join set: declared peers currently in the
	 * JOIN_PENDING window (Phase-1 publish .. Phase-2 commit).  Set/cleared by the
	 * coordinator under `lock`.
	 */
	uint8 pending_join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];

	/*
	 * spec-5.15 D5 (INV-J9) — node-local joiner write gate.  1 = this node may
	 * write (steady-state / admitted member, the default); 0 = this node is a
	 * joiner that has NOT yet been published MEMBER, so every backend (current AND
	 * future) fail-closed 53R60 at the xact/commit write entry.  A joiner sets it
	 * to 0 at the start of its join lifecycle and back to 1 only AFTER the
	 * JOIN_COMMITTED publish reaches it (epoch adopted from the durable marker AND
	 * local membership_state==MEMBER observed) — never right after the marker is
	 * durable (P1-r5 half-publish guard).  Initialized to 1 in shmem_init.
	 */
	uint8 self_join_admitted;

	/*
	 * spec-5.15 D5 — joiner gate failure latch + deadline.  self_join_failed=1
	 * means this node's join was definitively rejected (stale incarnation) or
	 * timed out: writable backends then get 53R61 FATAL (restart with a fresh
	 * incarnation) instead of the transient 53R60.  self_join_deadline_us is the
	 * convergence/commit deadline set when the gate closes (joiner self-tick).
	 */
	uint8 self_join_failed;
	uint64 self_join_deadline_us;

	/*
	 * spec-5.15 D6 — online-join observability counters (lifetime; no lock,
	 * atomic).  join_pending: JOIN_PENDING phases published; join_apply: members
	 * committed (JOIN_COMMITTED); join_reject: candidates the commit-time vet
	 * rejected; join_timeout: joins that failed closed on convergence timeout;
	 * clean_departed_cleared: rejoin clears of a clean_departed suppression.
	 */
	pg_atomic_uint64 join_pending_count;
	pg_atomic_uint64 join_apply_count;
	pg_atomic_uint64 join_reject_count;
	pg_atomic_uint64 join_timeout_count;
	pg_atomic_uint64 clean_departed_cleared_count;
	pg_atomic_uint64 marker_slow_ack_count;
	pg_atomic_uint64 marker_timeout_count;

	/*
	 * spec-5.15 D1 — per declared node, the freshest voting-slot incarnation +
	 * generation qvotec observed across disks (published each qvotec poll via
	 * cluster_reconfig_record_observed_slot; qvotec is the sole voting-disk
	 * reader, hence the natural publisher).  LMON reads these to detect/vet a
	 * join edge from shmem with no disk read in the tick.  pg_atomic so qvotec
	 * publishes lock-free from its process; generation == 0 means no valid slot
	 * was observed (the node is absent / not ready).
	 */
	pg_atomic_uint64 observed_incarnation[CLUSTER_MAX_NODES];
	pg_atomic_uint64 observed_generation[CLUSTER_MAX_NODES];

	/*
	 * spec-5.15 D4 — per declared node, the membership epoch qvotec last observed
	 * in that node's voting slot (published alongside the incarnation).  The
	 * coordinator reads these to test §3.3 convergence: a JOIN_PENDING commits
	 * (Phase-2) only once every existing MEMBER survivor's observed epoch has
	 * caught up to the JOIN_PENDING new_epoch.
	 */
	pg_atomic_uint64 observed_epoch[CLUSTER_MAX_NODES];

	/*
	 * spec-5.15 Hardening v1.3 (INV-J14 stale-slot fail-open) — per declared node,
	 * whether qvotec's decide_quorum_view saw that node FRESH-ALIVE this poll (its
	 * voting-disk heartbeat_ts_us recent, per the P2.1 freshness gate).  This is the
	 * liveness signal the cold-bootstrap proof needs: a generation > 0 slot alone
	 * may be a CRASHED peer's stale leftover at epoch INITIAL — counting it would
	 * fail-open (latch BOOTSTRAP without a live co-boot quorum).  Anchored on the
	 * durable voting-disk heartbeat (NOT live CSSD), so it is robust to IC/tier1
	 * heartbeat churn — the v1.2 race fix is preserved.  1 = fresh-alive, 0 = stale /
	 * absent (default 0 = fail-closed).  pg_atomic — qvotec (writer) and LMON (reader)
	 * are different processes.
	 */
	pg_atomic_uint64 observed_fresh_alive[CLUSTER_MAX_NODES];

	/*
	 * spec-5.16 (Hardening — 3-node join participation) — per declared node, the
	 * COMMITTED join-marker incarnation + admitted epoch qvotec last observed on a
	 * quorum-majority of that node's region-3 slots.  This is the durable signal a
	 * SURVIVOR (not just the coordinator) uses to recognize a rejoined peer as
	 * MEMBER at runtime — the symmetric observer half of the LEAVE detection — so
	 * its GRD FSM joins the JOIN re-declare barrier (without it the coordinator's
	 * barrier waits forever for a non-participating survivor in >=3-node).  0 = no
	 * durable COMMITTED marker observed.  pg_atomic so qvotec publishes lock-free.
	 */
	pg_atomic_uint64 observed_committed_join_incarnation[CLUSTER_MAX_NODES];
	pg_atomic_uint64 observed_committed_join_epoch[CLUSTER_MAX_NODES];

	/*
	 * spec-5.15 D4 — join-commit-marker submit mailbox (§2.6).  The coordinator
	 * stages a marker for the joiner (join_marker_target_node_id), bumps
	 * join_marker_request_seq, wakes qvotec via join_qvotec_latch, and waits
	 * (bounded) for its own qvotec — the sole voting-disk writer — to write the
	 * marker to the joiner's region-3 slot on a quorum-majority of disks.  Mirrors
	 * the spec-4.12 fence / spec-5.13 leave mailbox.  Single producer (coordinator
	 * driver/LMON) + single consumer (qvotec).
	 */
	struct Latch *join_qvotec_latch;
	pg_atomic_uint64 join_marker_request_seq;
	pg_atomic_uint64 join_marker_completion_seq;
	pg_atomic_uint32 join_marker_result; /* ClusterJoinMarkerSubmitResult */
	int32 join_marker_target_node_id;	 /* region-3 slot to write (the joiner N) */
	ClusterJoinCommitMarker join_pending_marker;
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

/*
 * spec-5.15 D1 — compute the join-edge bitmap (declared peers that went from a
 * non-member state to fresh-ALIVE with an incarnation above the admitted floor).
 * Caller holds the reconfig LWLock.  Returns the number of bits set.
 */
extern int
cluster_reconfig_compute_join_bitmap(uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES]);

/*
 * spec-5.15 D3 (INV-J11) — kind-aware event_id.  FAIL_STOP / CLEAN_LEAVE use the
 * legacy hash (byte-compatible with the 2.29 death path + the 5.13 leave marker
 * binding); JOIN_PENDING / JOIN_COMMITTED fold kind || join_bitmap ||
 * joiner_incarnations || cssd_dead_generation; NONE -> 0.
 */
extern uint64 cluster_reconfig_compute_event_id_v2(
	uint8 reconfig_kind, const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES],
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES], uint64 cssd_dead_generation);

/*
 * spec-5.15 D1/D4 — qvotec publishes the freshest observed voting-slot
 * incarnation + generation + membership epoch per node (it is the sole disk
 * reader); LMON reads them to detect/vet a join edge and to test §3.3
 * convergence.  get returns true iff a valid slot (generation > 0) was observed;
 * out-params always written (0 when absent).
 */
extern void cluster_reconfig_record_observed_slot(int32 node_id, uint64 incarnation,
												  uint64 generation, uint64 epoch);
extern bool cluster_reconfig_get_observed_slot(int32 node_id, uint64 *incarnation,
											   uint64 *generation);
extern uint64 cluster_reconfig_get_observed_epoch(int32 node_id);
/* spec-5.16 — qvotec publishes / LMON reads the per-peer durable COMMITTED join
 * marker so a survivor recognizes a rejoined peer as MEMBER (3-node join). */
extern void cluster_reconfig_record_observed_committed_join(int32 node_id, uint64 incarnation,
															uint64 epoch);
extern bool cluster_reconfig_get_observed_committed_join(int32 node_id, uint64 *incarnation,
														 uint64 *epoch);

/*
 * spec-5.15 Hardening v1.3 — publish / read the per-node FRESH-ALIVE liveness
 * qvotec's decide_quorum_view derived from the durable voting-disk heartbeat
 * (the P2.1 freshness gate).  The cold-bootstrap proof counts a peer only when
 * it is fresh-alive AND at epoch INITIAL — never on a generation > 0 slot alone
 * (a crashed peer's stale leftover).  get returns false when absent (fail-closed).
 */
extern void cluster_reconfig_record_observed_fresh_alive(int32 node_id, bool fresh_alive);
extern bool cluster_reconfig_get_observed_fresh_alive(int32 node_id);

/*
 * spec-5.15 Hardening v1.1 (HF-1 / INV-J9): true iff a majority of the current
 * MEMBER survivors have advanced their durable observed epoch to >=
 * admitted_epoch — i.e. the coordinator's JOIN_COMMITTED publish actually
 * propagated to a quorum of the membership.  qvotec gates the joiner's gate-open
 * on this (not on the durable marker alone), closing the half-publish window
 * (P1-1).  Fail-closed: 0 visible MEMBER survivor -> false.
 */
extern bool cluster_reconfig_join_publish_proven(uint64 admitted_epoch);

/*
 * spec-5.15 Hardening v1.1 (HF-2 / INV-J14): positive cold-bootstrap proof — a
 * majority of declared nodes CSSD-alive AND no declared peer past
 * CLUSTER_EPOCH_INITIAL.  joiner_self_tick uses it to decide, fail-closed,
 * whether a freshly-booted online_join node may keep its gate open (bootstrap)
 * vs must seek admission (rejoiner); a slow qvotec stays UNDECIDED (gate closed)
 * rather than mis-deciding bootstrap (P1-2).  Exposed for the unit test.
 */
extern bool cluster_reconfig_bootstrap_quorum_at_initial(void);

/* ============================================================
 * spec-5.15 D4 — two-phase online-join publication + §2.6 marker handshake.
 * ============================================================
 */

/*
 * Phase 1 (coordinator): bump the epoch, publish a JOIN_PENDING event naming the
 * joiners in join_bitmap, set their membership_state to JOINING + the
 * pending_join_bitmap, and write a durable PREPARE marker for each.  joiner_
 * incarnations[N] is the incarnation each joiner presented (folded into the
 * event_id).  Broadcast of PROCSIG is done by the caller (tick), after publish.
 */
extern void cluster_reconfig_apply_join_as_coordinator(
	const uint8 join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 coordinator_node_id,
	const uint64 joiner_incarnations[CLUSTER_MAX_NODES]);

/*
 * Phase 2 (coordinator): strict order (P1-r5) — ① write the COMMITTED marker
 * majority-durable (the commit point) → ② publish (bump JOIN_COMMITTED epoch +
 * set membership_state[node]=MEMBER + record_admitted + clear pending + clear
 * clean_departed[node]).  Returns true iff the COMMITTED marker reached a disk
 * majority and the publish ran; false (no state change) on marker failure (the
 * joiner stays JOINING, the next tick retries / eventually times out).
 */
extern bool cluster_reconfig_commit_member(int32 node_id, uint64 admitted_incarnation);

/*
 * §2.6 join-commit-marker submit (coordinator→qvotec handshake).  Stage a marker
 * for target_node, wake qvotec, and block (bounded) until qvotec has written it
 * to target_node's region-3 slot on a quorum-majority of disks (ACK) or failed /
 * timed out.  qvotec_poll_pending / _complete are the qvotec side (sole writer);
 * publish_join_qvotec_latch lets the coordinator wake qvotec.
 */
extern ClusterJoinMarkerSubmitResult
cluster_reconfig_submit_join_marker(int32 target_node, const ClusterJoinCommitMarker *m);
extern bool cluster_reconfig_submit_join_marker_async(ClusterMarkerAsync *a, int32 target_node,
													  const ClusterJoinCommitMarker *m,
													  ClusterMarkerAsyncKind kind,
													  TimestampTz now);
extern ClusterMarkerPollResult cluster_reconfig_poll_join_marker_async(ClusterMarkerAsync *a,
																	   TimestampTz now,
																	   uint32 *out_result,
																	   uint64 *out_elapsed_us);
extern bool cluster_reconfig_join_qvotec_poll_pending(int32 *out_target_node, void *out_slot512);
extern void cluster_reconfig_join_qvotec_complete(bool acked);
extern void cluster_reconfig_publish_join_qvotec_latch(struct Latch *latch);

extern void cluster_reconfig_note_marker_slow_ack(ClusterMarkerAsyncKind kind, int32 target_node,
												  uint64 elapsed_us);
extern void cluster_reconfig_note_marker_timeout(ClusterMarkerAsyncKind kind, int32 target_node,
												 uint64 elapsed_us);
extern uint64 cluster_reconfig_get_marker_slow_ack_count(void);
extern uint64 cluster_reconfig_get_marker_timeout_count(void);

/* ============================================================
 * spec-5.15 D5 — joiner-side write gate + admission (INV-J9 / §2.4 / §2.7).
 * ============================================================
 */

/*
 * Node-local write-gate verdict, read at the xact/commit write entry (covers
 * current AND future backends — INV-J9): ALLOW (steady / admitted); BLOCK_53R60
 * (this node is a joiner not yet admitted; retry-safe); BLOCK_53R61 (the join was
 * rejected / timed out; FATAL, the node must restart with a fresh incarnation).
 */
typedef enum ClusterJoinGateVerdict {
	CLUSTER_JOIN_GATE_ALLOW = 0,
	CLUSTER_JOIN_GATE_BLOCK_53R60,
	CLUSTER_JOIN_GATE_BLOCK_53R61
} ClusterJoinGateVerdict;

extern ClusterJoinGateVerdict cluster_reconfig_self_join_gate_verdict(void);

/* spec-5.15 D6 — online-join observability counter accessors. */
extern uint64 cluster_reconfig_get_join_pending_count(void);
extern uint64 cluster_reconfig_get_join_apply_count(void);
extern uint64 cluster_reconfig_get_join_reject_count(void);
extern uint64 cluster_reconfig_get_join_timeout_count(void);
extern uint64 cluster_reconfig_get_clean_departed_cleared_count(void);

/*
 * Hardening v1.0.4 (spec-5.13 clean-leave x spec-5.15 online-join serialization):
 * true iff a membership JOIN is currently in its pending window.  The clean-leave
 * request + announce paths consult this to enforce "one membership reconfig at a
 * time"; the join driver checks the mirror predicate cluster_clean_leave_in_progress().
 */
extern bool cluster_reconfig_join_in_progress(void);

/*
 * qvotec calls this when it has read this node's own §2.6 COMMITTED join marker
 * (admitted_incarnation == self's incarnation) on a quorum-majority of disks:
 * adopt the admitted epoch (may jump >16) AND set self MEMBER, THEN open the
 * write gate (gate-open guard = adopt && state==MEMBER — P1-r5 half-publish).
 */
extern void cluster_reconfig_note_self_admitted(uint64 admitted_epoch);


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

/* ============================================================
 * spec-5.18 D3 — permanently-removed set accessors (INV-LF1).
 *
 *	record: set removed_bitmap[node] + removed_epoch[node] + clear the dormant
 *	  clean_departed[node] (a removed node supersedes a dormant one) under `lock`;
 *	  raise_epoch_floor for the startup-rebuild path (mirror clean_departed P1-V0.7).
 *	is_removed / get_removed_epoch: read the durable removal state (decision side).
 * ============================================================ */
extern void cluster_reconfig_record_removed(int32 node_id, uint64 remove_epoch,
											bool raise_epoch_floor);
extern bool cluster_reconfig_is_removed(int32 node_id);
/* HF-2: lock-free durable removed test for the 53R64 self-demote write-gate hot path. */
extern bool cluster_reconfig_is_removed_unlocked(int32 node_id);
extern uint64 cluster_reconfig_get_removed_epoch(int32 node_id);
extern uint64 cluster_reconfig_get_removed_count(void);
extern void
cluster_reconfig_snapshot_removed_bitmap(uint8 *out /* [CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] */);
extern void cluster_reconfig_seed_removed_membership(int32 node_id, uint64 remove_epoch,
													 uint64 removed_incarnation,
													 bool raise_epoch_floor);

/*
 * spec-5.18 D3 (R14) — NODE_REMOVED event identity.  The legacy event_id hashes
 * only (dead_bitmap, cssd_dead_generation); a clean-left removal leaves those
 * unchanged so it would be deduped away and never published.  Fold the kind +
 * removed_bitmap + removal_event_id (NOT old_epoch, per 2.29 P1.2 anti-self-loop)
 * to guarantee a distinct, non-deduped event id for each removal attempt.
 */
extern uint64 cluster_reconfig_compute_removal_event_id(
	const uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], uint64 removal_event_id);

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

/*
 * spec-5.18 D3 — the membership-shrink commit point of permanent removal, run on
 * the survivor coordinator.  Guarded epoch advance (CL-I3-style TOCTOU defense at
 * >=3 nodes): bump baseline+1 ONLY if the epoch is still the baseline the removal
 * bound against; on a lost CAS (a real death intruded) return 0 so the driver
 * ABORTED_ESCALATEs (pre-SHRUNK).  On success: publish a NODE_REMOVED reconfig
 * event (event_id folds removed_bitmap + removal_event_id, R14), record the node
 * removed (removed_bitmap + epoch), and shrink its membership_state to REMOVED.
 * Returns the new epoch (0 on guarded-advance failure).
 */
extern uint64 cluster_reconfig_apply_node_removed_as_coordinator(int32 removed_node_id,
																 uint64 baseline_epoch,
																 uint64 removal_event_id,
																 uint64 last_incarnation,
																 bool *out_contest);


#endif /* CLUSTER_RECONFIG_H */
