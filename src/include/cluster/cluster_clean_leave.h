/*-------------------------------------------------------------------------
 *
 * cluster_clean_leave.h
 *	  pgrac clean leave reconfiguration (spec-5.13) — cooperative
 *	  GES/GCS/PCM drain for a surviving node that leaves on purpose.
 *
 *	  spec-2.29 / 4.6 / 4.7 reconfiguration is all death-driven (a CSSD
 *	  DEAD edge → fence-close / failure-driven remaster).  This module
 *	  handles the dual case: a LIVE, cooperative node that leaves by plan.
 *	  Before it really exits it must actively drain everything it
 *	  holds/masters — GES grants, GRD master shards, PCM X locks, and dirty
 *	  GCS pages — to the surviving nodes, proving zero leftover holder /
 *	  waiter / dirty state / stale master, rather than "looking like a
 *	  crash".  If the drain cannot complete (the leaving node dies mid-drain
 *	  or the deadline expires) the path ESCALATES to fail-stop (spec-5.14):
 *	  clean leave never weakens fail-stop safety (CL-I7).
 *
 *	  soundness命门 (§0.3 / CL-I5): the leaving node holds PCM X on its
 *	  dirty blocks (exclusive → no survivor holds a conflicting current),
 *	  force-WAL-logs + FlushBuffers them to shared storage in its OWN
 *	  backend/checkpointer (never LMON, CL-I9), then releases X.  After that
 *	  no node's memory holds the current image, so shared-storage device
 *	  state is authoritative; survivors invalidate stale cache at leave-epoch
 *	  advance and read the just-flushed current via storage fallback.  This
 *	  sidesteps the Stage-6 cross-instance cache-coherence wall (53R9X)
 *	  WITHOUT shipping a cache image — but storage fallback for a leaving
 *	  block is allowed ONLY after it was flushed + the survivor invalidated
 *	  its cache; before that, fail-closed (CL-I5).
 *
 *	  Layering (mirrors the spec-5.11/5.12 policy split for unit-testability):
 *	    - cluster_clean_leave_policy.c : pure decision layer (phase-FSM
 *	      transition validity, writable-only quiesce gate, version-coherent
 *	      leave check, leave-intent marker structural validation).  No
 *	      PostgreSQL runtime dependency → exercised directly by cluster_unit.
 *	    - cluster_clean_leave.c : runtime driver (shmem state, the phase
 *	      state machine, voting-disk marker I/O, ProcSignal quiesce, IC
 *	      announce/ack, LMON orchestration, GES/GCS drain dispatch).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_clean_leave.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.13-clean-leave-reconfig.md
 *	  Depends on spec-5.14 substrate (reconfig_kind + touched_peers); this
 *	  module is the CLEAN_LEAVE producer + touched_peers drain-grace consumer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CLEAN_LEAVE_H
#define CLUSTER_CLEAN_LEAVE_H

#include <signal.h> /* sig_atomic_t (cluster_clean_leave_quiesce_pending) */

#include "c.h"
#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

#include "cluster/cluster_marker_async.h"

/* 128 nodes, same width as ReconfigEvent.dead_bitmap. */
#define CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES 16
/* Default drain deadline; aligns with the feature-082 30s barrier. */
#define CLUSTER_CLEAN_LEAVE_DRAIN_TIMEOUT_DEFAULT_MS 30000

/*
 * ClusterLeavePhase — leaving-node driver state machine (§2.1).  9 states.
 * IDLE is the only resting state; COMMITTED / ABORTED / ABORTED_ESCALATE are
 * terminal-ish (return to IDLE after cleanup, except COMMITTED on the leaving
 * node which leads to exit).
 */
typedef enum ClusterLeavePhase {
	CLUSTER_LEAVE_IDLE = 0,		   /* no leave in progress */
	CLUSTER_LEAVE_REQUESTED,	   /* intent recorded (voting-disk marker + IC announce) */
	CLUSTER_LEAVE_QUIESCING,	   /* leaving node aborting writable backends (53R62) */
	CLUSTER_LEAVE_GES_DRAINING,	   /* releasing GES grants + remastering shards */
	CLUSTER_LEAVE_GCS_FLUSHING,	   /* force-WAL-log + FlushBuffer dirty/X, release PCM X */
	CLUSTER_LEAVE_BARRIER_WAIT,	   /* waiting all-survivor LEAVE_DRAIN_ACK */
	CLUSTER_LEAVE_COMMITTED,	   /* leave epoch bumped + marker majority-durable; node may exit */
	CLUSTER_LEAVE_ABORTED,		   /* clean abort (mixed-mode NAK / preflight reject): no escalate,
									 * no epoch bump, clears marker, reverts to IDLE; nothing drained */
	CLUSTER_LEAVE_ABORTED_ESCALATE /* real death/deadline mid-drain → fail-stop (5.14) */
} ClusterLeavePhase;

/*
 * ClusterLeaveIntentMarker — durable, CRC-checked voting-disk marker (§2.5).
 * REQUESTED is written by the leaving node before QUIESCING; COMMITTING /
 * COMMITTED are written by the coordinator (P1 two-phase commit).  Only a
 * COMMITTED marker is a clean-departed rebuild basis (it is written only
 * after the real commit point — the clean_leave reconfig event publish).
 * Defined before ClusterLeaveState because the shmem submit mailbox embeds it.
 */
#define CLUSTER_LEAVE_MARKER_MAGIC 0x50474C4D /* "PGLM" */
#define CLUSTER_LEAVE_MARKER_VERSION 1

/* marker.phase values (a 3-value subset bracketing the commit point). */
#define CLUSTER_LEAVE_MARKER_PHASE_REQUESTED 1
#define CLUSTER_LEAVE_MARKER_PHASE_COMMITTING 2 /* before epoch bump; NOT a trust basis */
#define CLUSTER_LEAVE_MARKER_PHASE_COMMITTED 3	/* after epoch bump+publish; the ONLY trust basis */

typedef struct ClusterLeaveIntentMarker {
	uint32 magic;	/* CLUSTER_LEAVE_MARKER_MAGIC — guards against a stale slot */
	uint16 version; /* CLUSTER_LEAVE_MARKER_VERSION — compat evolution */
	uint16 _pad;
	int32 leaving_node_id; /* who is leaving (rebuild must check == declared peer) */
	uint64 leave_epoch;	   /* the clean_leave reconfig new_epoch=E (epoch-floor anchor) */
	uint64 event_id;	   /* the bound clean_leave reconfig event_id (anti stale-trust) */
	uint8 dead_bitmap[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES]; /* the departed set (== event's) */
	uint64 cssd_dead_generation; /* dead_generation snapshot when written */
	TimestampTz written_at;
	uint8 phase; /* REQUESTED / COMMITTING / COMMITTED */
	uint8 _pad2[3];
	uint32 crc; /* CRC32C over [magic..phase] (rule 15 integrity) */
} ClusterLeaveIntentMarker;

/* outcome of cluster_clean_leave_submit_marker (mirrors the fence-marker result). */
typedef enum ClusterLeaveMarkerSubmitResult {
	CLUSTER_LEAVE_MARKER_SUBMIT_ACK = 0, /* marker durable on >= quorum-majority disks */
	CLUSTER_LEAVE_MARKER_SUBMIT_FAILED,	 /* qvotec wrote but did not reach majority */
	CLUSTER_LEAVE_MARKER_SUBMIT_TIMEOUT	 /* qvotec did not complete within the bound */
} ClusterLeaveMarkerSubmitResult;

/*
 * ClusterLeaveAbortReason (Hardening v1.0.3) — why an in-flight leave clean-
 * aborted, recorded in shmem so cluster_clean_leave_request() can map a
 * drive_drain-side preflight-incomplete timeout to rejected:preflight_incomplete
 * (a clean abort lands back in IDLE with no NAK, so the phase alone is ambiguous).
 */
typedef enum ClusterLeaveAbortReason {
	CLUSTER_LEAVE_ABORT_NONE = 0,
	CLUSTER_LEAVE_ABORT_PREFLIGHT_INCOMPLETE = 1 /* not every alive survivor ACKed in time */
} ClusterLeaveAbortReason;

/*
 * ClusterLeaveState — shmem state for the in-progress leave (§2.1).  Single
 * leave at a time (declared-set is static in v1).  Guarded by `lock`; `phase`
 * is also kept as an atomic for the hot serve-gate read.
 */
typedef struct ClusterLeaveState {
	LWLock lock;			/* guard phase transitions + publish */
	pg_atomic_uint32 phase; /* ClusterLeavePhase (atomic read for hot gate) */
	int32 leaving_node_id;	/* -1 if none */
	uint32 _pad0;
	uint64 leave_epoch;				/* epoch this leave is bound to (CL-I3 immutable) */
	uint64 leave_baseline_dead_gen; /* CSSD dead_generation when the leave was bound
									 * (retained for observability; the coherence gate
									 * now uses leave_baseline_others_dead, spec-2.29a
									 * ②b — the scalar counted the leaving node's own
									 * expected DEAD and falsely escalated) */
	uint64 barrier_deadline_us;		/* fail-closed deadline (drain_timeout_ms) */
	uint8 ack_bitmap[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES]; /* survivor acks */
	/* spec-2.29a ②b: CSSD dead set (EXCLUDING the leaving node) snapshotted when
	 * the leave was bound; the coherence gate escalates only if a THIRD-PARTY
	 * death changed this set mid-drain. */
	uint8 leave_baseline_others_dead[CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES];
	pg_atomic_uint64 ges_drained_count; /* shards/grants drained (observability) */
	pg_atomic_uint64 gcs_flushed_count; /* dirty/X pages flushed */
	pg_atomic_uint64 shards_remastered; /* shards moved off leaving node */
	pg_atomic_uint64 escalate_count;	/* lifetime leaves escalated to fail-stop */
	pg_atomic_uint32 nak_received;		/* a survivor refused (mixed-mode); drives clean ABORTED */

	/*
	 * Survivor / coordinator side of someone else's leave (this node is NOT the
	 * leaver: leaving_node_id != cluster_node_id).  survivor_acked: this survivor
	 * has dropped its refs + sent its readiness ACK (set once, idempotent tick).
	 * commit_ready_received: this node is the coordinator and the leaving node
	 * sent LEAVE_COMMIT_READY — the lmon_tick coordinator branch runs the
	 * two-phase commit (not the IC handler, to keep it light).
	 */
	pg_atomic_uint32 survivor_acked;
	pg_atomic_uint32 commit_ready_received;
	/* leaving node: the LMON broadcast the real CLEAN_LEAVE_ANNOUNCE once (IC
	 * sends are LMON-only, so the announce is LMON-driven, not backend-driven). */
	pg_atomic_uint32 announce_sent;
	/* spec-5.13 S6 (CL-I5) — survivor storage-fallbacks withheld during an
	 * uncommitted leave (the leaving node has not yet flushed). */
	pg_atomic_uint64 serve_gate_fail_closed_count;

	/*
	 * Hardening v1.0.1 fields.
	 *   nak_reason (P2): the ClusterLeaveNakReason of the latest survivor NAK, so
	 *     the request can map a DISABLED NAK to rejected:peers_not_all_enabled
	 *     (F6 preflight, D13b) rather than the bare ACCEPTED-then-async-abort.
	 *   commit_point_observed (P1-1, leaving node): the leaving node has observed
	 *     the commit (epoch advanced, dead_gen unchanged); past this point the
	 *     leave can NOT be un-committed, so the barrier deadline must NOT escalate.
	 *   committed_durable_confirmed (P1-1, leaving node): the coordinator has told
	 *     us (LEAVE_COMMITTED) that the COMMITTED marker is majority-durable; only
	 *     then does the leaving node reach COMMITTED ("may exit") — the §2.5 /
	 *     P1-V0.7 exit gate (durable truth-source must exist before departure).
	 *   committed_marker_durable (P1-1, coordinator): this coordinator's COMMITTED
	 *     marker reached a voting-disk majority.  Until set, cl_survivor_tick keeps
	 *     retrying the marker write (never 3-then-give-up) and does NOT release the
	 *     leave-tracking slot.
	 */
	pg_atomic_uint32 nak_reason;
	pg_atomic_uint32 commit_point_observed;
	pg_atomic_uint32 committed_durable_confirmed;
	pg_atomic_uint32 committed_marker_durable;

	/*
	 * Hardening v1.0.2 fields.
	 *   leave_attempt_nonce (P2): the nonce of the current leave attempt — set by
	 *     the leaver at request, and by a survivor from the announce it accepted.
	 *     Every control-message handler checks the payload nonce against it so a
	 *     stale/delayed frame from a prior same-epoch attempt is dropped.
	 *   preflight_pending / preflight_sent (P1, F6 layer-1 true preflight): the
	 *     request backend stages a side-effect-free preflight=true probe; the LMON
	 *     (IC sends are LMON-only) broadcasts it once and sets preflight_sent.  The
	 *     request proceeds to REQUESTED only when every alive survivor preflight-
	 *     ACKs (no NAK), so a mixed-mode request triggers NO survivor side effect.
	 */
	pg_atomic_uint64 leave_attempt_nonce;
	pg_atomic_uint32 preflight_pending;
	pg_atomic_uint32 preflight_sent;

	/*
	 * Hardening v1.0.3 fields.
	 *   request_in_progress (P1 same-node serialization): a CAS-reserved owner
	 *     flag held for the WHOLE request (entry..return, including the multi-
	 *     second preflight wait and the inline drain).  The unlocked phase==IDLE
	 *     test cannot serialize two same-node pg_cluster_clean_leave_request()
	 *     callers — phase stays IDLE until REQUESTED, which is set only AFTER the
	 *     preflight — so both could pass it and double-drain.  A second caller
	 *     whose CAS fails is rejected:leave_in_progress.  Released via PG_CATCH on
	 *     any ereport(ERROR) escape so the reservation can never leak.
	 *   abort_reason (P1 preflight-incomplete): set before a drive_drain-side
	 *     clean-abort caused by a preflight-incomplete timeout (no NAK, but not
	 *     every alive survivor ACKed) so the request maps it to
	 *     rejected:preflight_incomplete instead of the bare ACCEPTED (the request
	 *     keys on this, NOT the post-abort phase, which is back at IDLE).
	 */
	pg_atomic_uint32 request_in_progress;
	pg_atomic_uint32 abort_reason; /* ClusterLeaveAbortReason */

	/*
	 * Voting-disk leave-marker submit mailbox (§2.5 two-phase commit) — a
	 * local single-producer/single-consumer handshake mirroring the spec-4.12
	 * fence-marker mailbox.  Producer = this node's driver/LMON staging a marker;
	 * consumer = this node's qvotec (the sole voting-disk writer) writing it to
	 * this node's own leave-slot on every disk and acking majority-durability.
	 * pending_marker is written before request_seq is bumped (write barrier
	 * between); qvotec sets completion_seq = request_seq + marker_result when done.
	 */
	struct Latch *qvotec_latch;				/* qvotec publishes MyLatch (latch-wake) */
	pg_atomic_uint64 marker_request_seq;	/* driver bumps to submit a marker write */
	pg_atomic_uint64 marker_completion_seq; /* qvotec sets = request_seq when done */
	pg_atomic_uint32 marker_result;			/* ClusterLeaveMarkerSubmitResult */
	uint32 _pad2;
	ClusterLeaveIntentMarker pending_marker; /* staged here before bumping request_seq */
} ClusterLeaveState;


/* ============================================================
 * IC wire payloads (D8) — CLEAN_LEAVE_ANNOUNCE / LEAVE_DRAIN_ACK|NAK.
 *
 *	Each payload carries its own magic/version/CRC (rule 15) on top of the
 *	envelope CRC: the envelope CRC protects transport integrity; the payload
 *	magic+version+CRC additionally guard against a misrouted / version-skewed
 *	message being acted on as a clean-leave command (8.A defense-in-depth).
 * ============================================================ */
#define CLUSTER_CLEAN_LEAVE_IC_MAGIC 0x504C4943 /* "PLIC" — pgrac leave IC */
/* v2 (Hardening v1.0.2): payloads carry leave_nonce; a mixed-version cluster
 * fails closed (version mismatch dropped) rather than misparsing the wider frame. */
#define CLUSTER_CLEAN_LEAVE_IC_VERSION 2

/* LEAVE_DRAIN_NAK reasons (why a survivor refuses the clean leave). */
typedef enum ClusterLeaveNakReason {
	CLUSTER_LEAVE_NAK_NONE = 0,
	CLUSTER_LEAVE_NAK_DISABLED = 1,			/* survivor clean_leave_enabled = off */
	CLUSTER_LEAVE_NAK_NOT_IN_QUORUM = 2,	/* survivor not in quorum (cannot commit) */
	CLUSTER_LEAVE_NAK_LEAVE_IN_PROGRESS = 3 /* survivor already tracking a different leave
											 * (single-leave-at-a-time, Hardening v1.0.1) */
} ClusterLeaveNakReason;

/*
 * CLEAN_LEAVE_ANNOUNCE payload — leaving node -> all survivors.
 *	preflight=1 : "are you enabled for clean leave?" probe (leave_epoch=0)
 *	preflight=0 : real "I am leaving" announce → survivor enters leave-aware
 *	              reconfig (CL-I4 fail-closed-until-drained) and replies ACK/NAK.
 */
typedef struct ClusterLeaveAnnouncePayload {
	uint32 magic;
	uint16 version;
	uint16 _pad0;
	int32 leaving_node_id;
	uint8 preflight; /* 1 = enabled-probe, 0 = real announce */
	uint8 _pad1[3];
	uint64 leave_epoch;			 /* 0 until bound (preflight / pre-commit) */
	uint64 cssd_dead_generation; /* version-coherence cross-check (CL-I3) */
	uint64 leave_nonce;			 /* Hardening v1.0.2 (P2): per-attempt nonce binding every
						 * control message to THIS leave attempt (distinguishes a
						 * same-epoch abort/retry; envelope epoch-drop only blocks
						 * cross-membership-epoch frames). */
	uint32 crc;					 /* CRC32C over [magic..leave_nonce] */
} ClusterLeaveAnnouncePayload;

/*
 * LEAVE_DRAIN_ACK / LEAVE_DRAIN_NAK payload — survivor -> leaving node.
 *	nak=0 (ACK): "dropped all refs to leaving + accepted remaster + ready-to-
 *	             commit" (PRE-epoch readiness; §3.1 F1 non-cycle).
 *	nak=1 (NAK): refuse (nak_reason); leaving node CLUSTER_LEAVE_ABORTED.
 */
typedef struct ClusterLeaveAckPayload {
	uint32 magic;
	uint16 version;
	uint16 _pad0;
	int32 survivor_node_id;
	int32 leaving_node_id;
	uint64 leave_epoch;
	uint64 leave_nonce; /* Hardening v1.0.2 (P2): echoes the announce nonce so the
						 * leaver drops a stale ACK/NAK from a prior attempt. */
	uint8 nak;			/* 0 = ACK, 1 = NAK */
	uint8 nak_reason;	/* ClusterLeaveNakReason when nak=1 */
	uint8 _pad1[2];
	uint32 crc; /* CRC32C over [magic..nak_reason] */
} ClusterLeaveAckPayload;


/* ============================================================
 * Pure decision layer (cluster_clean_leave_policy.c) — no PG runtime.
 * ============================================================ */

/* Is the phase transition from->to a legal driver-FSM edge? (U2) */
extern bool cluster_clean_leave_phase_valid_transition(ClusterLeavePhase from,
													   ClusterLeavePhase to);

/* canonical lowercase name for a phase (SRF / dump). */
extern const char *cluster_clean_leave_phase_str(int phase);

/* writable-only quiesce gate (U5 / CL-I6): abort iff in a transaction that has
 * a real top-level xid (a writable tx); read-only / idle absorb. */
extern bool cluster_clean_leave_should_abort_writable(bool in_transaction, bool has_top_xid);

/* version-coherent leave (U3 / CL-I3 / L235): the leave is still coherent only
 * if neither the cluster epoch nor the CSSD dead_generation moved since the
 * leave was bound; any external membership change (a third-party death, or a
 * third-party fail-stop that already bumped the epoch) => not coherent => the
 * caller must ABORTED_ESCALATE.  spec-2.29a ②b: the dead set compared here
 * EXCLUDES the leaving node itself (others-dead bitmap), so the leaving node's
 * own expected alive→DEAD transition never falsely escalates the leave. */
extern bool cluster_clean_leave_version_coherent(uint64 bound_epoch, uint64 current_epoch,
												 const uint8 *bound_others_dead,
												 const uint8 *current_others_dead, int nbytes);

/* leave-intent marker structural validation (magic/version/CRC/identity).  Pure:
 * computes CRC32C over [magic..phase] and checks magic, version, that the
 * leaving node is the expected declared peer, and the dead_bitmap names only the
 * leaving node.  Does NOT consult epoch (that is a post-recovery sanity check). */
extern void cluster_clean_leave_marker_compute_crc(ClusterLeaveIntentMarker *m);
extern bool cluster_clean_leave_marker_struct_valid(const ClusterLeaveIntentMarker *m,
													int32 expected_leaving_node);
/* Is this marker a clean-departed rebuild basis? (phase==COMMITTED && struct-valid) */
extern bool cluster_clean_leave_marker_is_committed_basis(const ClusterLeaveIntentMarker *m,
														  int32 expected_leaving_node);

/* survivor leave-epoch observe gate (U7 / CL-I10): the survivor runs its
 * leaving-node cache invalidate once it observes the cluster epoch reach the
 * bound leave_epoch via its locally-tracked stable baseline (NOT the leave
 * event's fresh-read old_epoch field, which an IC piggyback can deliver before
 * the local detector and wedge the gate forever).  leave_epoch==0 means no
 * leave is bound.  This is the happens-before boundary that must complete
 * before any post-epoch storage read (CL-I5). */
extern bool cluster_clean_leave_should_invalidate(uint64 observed_epoch, uint64 leave_epoch);

/* CL-I5 storage-fallback serve gate (U8 / §0.3命门): a survivor may serve a
 * leaving-node block from the storage fallback ONLY after that block was
 * flushed by the leaving node AND the survivor invalidated its stale cache.
 * block_from_leaving==false → the normal serve path (always allowed);
 * block_from_leaving==true → allowed iff leave_flushed_invalidated, else fail-
 * closed (freeze_queue / 53R62) — never read stale storage (Rule 8.A). */
extern bool cluster_clean_leave_serve_gate_allows(bool block_from_leaving,
												  bool leave_flushed_invalidated);

/* IC payload integrity (D8 / U9 / rule 15) — pure CRC compute + validate.
 * *_valid checks magic + version + CRC; the announce form also rejects an
 * out-of-range leaving_node_id.  FAIL-CLOSED: any mismatch → false (a
 * misrouted / version-skewed message is never acted on). */
extern void cluster_clean_leave_announce_compute_crc(ClusterLeaveAnnouncePayload *p);
extern bool cluster_clean_leave_announce_payload_valid(const ClusterLeaveAnnouncePayload *p);
extern void cluster_clean_leave_ack_compute_crc(ClusterLeaveAckPayload *p);
extern bool cluster_clean_leave_ack_payload_valid(const ClusterLeaveAckPayload *p);


/* ============================================================
 * Runtime layer (cluster_clean_leave.c).
 * ============================================================ */

/* ProcSignal pending flag for the quiesce request (D7; async-safe set). */
extern PGDLLIMPORT volatile sig_atomic_t cluster_clean_leave_quiesce_pending;

/* shmem region (registered via the cluster shmem registry; init postmaster-once). */
extern Size cluster_clean_leave_shmem_size(void);
extern void cluster_clean_leave_shmem_init(void);
extern void cluster_clean_leave_shmem_register(void);

/* ------------------------------------------------------------------
 * Voting-disk leave-marker two-phase commit (§2.5) — qvotec-mediated.
 *
 *	submit_marker: driver/LMON side.  Stage a marker, wake qvotec, and block
 *	  (bounded) until qvotec has written it to this node's own leave-slot on a
 *	  quorum-majority of disks (ACK) or failed/timed out.  REQUESTED is submitted
 *	  by the leaving node; COMMITTING/COMMITTED by the coordinator (its own slot).
 *	qvotec_poll_pending / _complete: qvotec side of the handshake (sole writer).
 *	  poll packs the staged marker into a 512-byte slot buffer; complete publishes
 *	  the majority-durable verdict back to the waiting submitter.
 *	publish_qvotec_latch: qvotec publishes MyLatch so the submitter can wake it.
 *	rebuild_from_disks: startup recovery (P1-V0.7) — scan every node's leave-slot
 *	  across a quorum-majority of disks; a struct-valid COMMITTED marker rebuilds
 *	  clean_departed + raises the epoch floor (the epoch is not durable).
 * ------------------------------------------------------------------ */
extern ClusterLeaveMarkerSubmitResult
cluster_clean_leave_submit_marker(const ClusterLeaveIntentMarker *m);
extern bool cluster_clean_leave_submit_marker_async(ClusterMarkerAsync *a,
													const ClusterLeaveIntentMarker *m,
													ClusterMarkerAsyncKind kind, int32 target_node,
													TimestampTz now);
extern ClusterMarkerPollResult cluster_clean_leave_poll_marker_async(ClusterMarkerAsync *a,
																	 TimestampTz now,
																	 uint32 *out_result,
																	 uint64 *out_elapsed_us);
extern bool cluster_clean_leave_qvotec_poll_pending(void *out_slot512);
extern void cluster_clean_leave_qvotec_complete(bool acked);
extern void cluster_clean_leave_publish_qvotec_latch(struct Latch *latch);
extern void cluster_clean_leave_rebuild_from_disks(const int *fds, int n_disks);

/*
 * Result of the internal C driver entry — mapped to the operator UDF's text
 * return (D13b behaviour matrix) by cluster_clean_leave_views.c.
 */
typedef enum ClusterLeaveRequestResult {
	CLUSTER_LEAVE_REQ_ACCEPTED = 0,			  /* entered REQUESTED + drove the drain */
	CLUSTER_LEAVE_REQ_REJECTED_DISABLED,	  /* cluster.clean_leave_enabled = off */
	CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM, /* this node is not in quorum */
	CLUSTER_LEAVE_REQ_NOOP_NO_PEER,			  /* single-node / no surviving peer to hand off to */
	CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS,	  /* a leave is already in progress */
	CLUSTER_LEAVE_REQ_REJECTED_PEERS_NOT_ENABLED, /* mixed-mode: a survivor is disabled (preflight) */
	CLUSTER_LEAVE_REQ_REJECTED_NOT_DURABLE, /* REQUESTED marker did not reach a disk majority */
	CLUSTER_LEAVE_REQ_REJECTED_PREFLIGHT_INCOMPLETE /* Hardening v1.0.3 (P1): an alive survivor did
													 * not preflight-ACK before the deadline (silent /
													 * version-skew / IC loss) — fail-closed, NOT the
													 * old fail-open ACCEPTED.  Distinct from
													 * peers_not_all_enabled (a definite disabled NAK):
													 * incomplete = handshake unfinished, retry/diagnose */
} ClusterLeaveRequestResult;

/* leaving-node driver (runs on the LEAVING node). */
extern ClusterLeaveRequestResult
cluster_clean_leave_request(void);				   /* internal C entry; gated by GUC + in_quorum */
extern void cluster_clean_leave_drive_drain(void); /* phase state-machine step, backend ctx */

/* LMON orchestration (both sides): consume announce + advance barrier + escalate. */
extern void cluster_clean_leave_lmon_tick(void);

/* survivor ack of "dropped all refs to leaving node + accepted remaster". */
extern void cluster_clean_leave_survivor_ack(int32 leaving_node_id, uint64 leave_epoch);

/* IC wire (D8): register the 3 clean-leave msg types (called once from the
 * cluster_lmon msg-type registration block, postmaster phase 1). */
extern void cluster_clean_leave_register_ic_msg_types(void);

/* IC send helpers (D8).  broadcast_announce fans the announce/preflight out to
 * every survivor; send_ack is the survivor's per-peer ACK/NAK reply. */
extern void cluster_clean_leave_ic_broadcast_announce(uint64 leave_epoch, uint64 leave_nonce,
													  bool preflight);
extern void cluster_clean_leave_ic_send_ack(int32 dest_node_id, int32 leaving_node_id,
											uint64 leave_epoch, uint64 leave_nonce, bool nak,
											uint8 nak_reason);

/* ProcSignal quiesce integration (D7) — three-step enumerated (L100/L118). */
extern void cluster_clean_leave_handle_quiesce_interrupt(void);
extern void cluster_clean_leave_check_pending_in_proc_interrupts(void);

/* observability — always 1 row (consistent copy of the shmem state). */
extern void cluster_clean_leave_get_state(ClusterLeaveState *out);

/* CL-I2 no-leftover proof (assertion / acceptance helper). */
extern bool cluster_clean_leave_verify_no_leftover(int32 leaving_node_id);

/*
 * CL-I5 block-serve gate (S6) — called from the GCS block-serve path right
 * before it would serve a block from the storage fallback.  Returns true (allow)
 * unless an uncommitted clean leave is in progress on this survivor, in which
 * case the storage image of a leaving-node block may be a pre-flush stale
 * version: returns false (the caller fail-closes 53R62 retry).  Coarse but sound
 * (CL-I4): during the drain ALL storage-fallbacks are withheld; once the leave
 * commits (the node is clean_departed → flushed + this survivor invalidated its
 * cache) it allows again.
 */
extern bool cluster_clean_leave_block_serve_gate_allows(void);

/*
 * §3.1 refuse-new-writes gate (the standing flag the one-shot quiesce cannot
 * provide).  True iff this node has an active clean leave (REQUESTED..COMMITTED);
 * a writable transaction is then fail-closed with 53R62 at xid assignment +
 * the commit boundary so nothing the leave did not flush becomes durable (8.A).
 */
extern bool cluster_clean_leave_node_refuses_writes(void);

/*
 * Hardening v1.0.4 (P1-1/P2): true iff THIS node is participating in any clean
 * leave (leaver / survivor / mid-request).  The spec-5.15 online-join driver
 * consults this to enforce "one membership reconfig at a time" — a join must not
 * start or commit while a clean leave is active (a join's epoch bump is otherwise
 * indistinguishable from the leave's commit on the leaving node, CL barrier hang).
 */
extern bool cluster_clean_leave_in_progress(void);

#endif /* CLUSTER_CLEAN_LEAVE_H */
