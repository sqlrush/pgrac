/*-------------------------------------------------------------------------
 *
 * cluster_node_remove.h
 *	  pgrac online node leave: fence + cluster-wide cleanup (spec-5.18) —
 *	  permanent removal (decommission) of an already-left / non-returning
 *	  declared node.
 *
 *	  spec-5.13 (clean leave) and spec-5.14 (fail-stop) let a node *leave*
 *	  but both preserve its way back (5.13 clean_departed = dormant-rejoinable;
 *	  5.14 dead_bitmap = crashed-but-restartable).  This module handles
 *	  *permanent removal*: it takes a node that already left (5.13 clean-left /
 *	  5.14 fail-stopped) or is declared non-returning, and
 *	    (1) permanently shrinks it out of the effective member set
 *	        (durable membership_state -> REMOVED, NOT 5.13's dormant);
 *	    (2) fences it with the spec-4.12 cooperative write-fence
 *	        (marker_kind = NODE_REMOVED) so a zombie revival can neither write
 *	        shared storage, fool a survivor, nor passively rejoin;
 *	    (3) runs a cluster-wide cleanup_on_exit — permanently remasters the
 *	        GRD shards it mastered, clears its GES/PCM leftover, tombstones its
 *	        voting-disk slot, and proves zero leftover.
 *
 *	  soundness命门 (§0.4 / INV-LF2): fence-before-shrink.  The 4.12 fence
 *	  marker (N in fenced_dead_bitmap) must be majority-durable BEFORE
 *	  membership_state[N]=REMOVED is published (epoch advance), else a window
 *	  exists where a survivor already treats N as a non-member (reassigns its
 *	  shards) while N is not yet fenced and could still write shared storage —
 *	  double-write / lost-write / split-brain.  This mirrors spec-5.16's
 *	  fence-arm-before-open-gate (5.16 join side / 5.18 leave side).
 *
 *	  three-phase durable commit (§2.5 / INV-LF7): membership-shrink-commit and
 *	  cleanup-complete are two distinct milestones.  REMOVING (pre-bump, not a
 *	  trust source) -> SHRUNK (post-bump: N is non-member + fenced, cleanup
 *	  PENDING) -> REMOVED (only after verify_no_leftover + all-survivor ACK; THE
 *	  final trust source).  A crash between SHRUNK and REMOVED resumes cleanup
 *	  from the SHRUNK marker (CLEANUP_BLOCKED is resumable, never escalates).
 *
 *	  Layering (mirrors the spec-5.11/5.12/5.13 policy split for unit-testability):
 *	    - cluster_node_remove_policy.c : pure decision layer (phase-FSM
 *	      transition validity, request-result precheck matrix, version-coherent
 *	      escalate-vs-blocked classify, removal-marker structural validation +
 *	      majority authority decide + carry-forward, INV-LF7 recovery matrix,
 *	      IC payload integrity).  No PostgreSQL runtime dependency -> exercised
 *	      directly by cluster_unit (test_cluster_node_remove).
 *	    - cluster_node_remove.c : runtime driver (shmem state, the 10-phase
 *	      state machine, voting-disk marker I/O, fence-arm, shrink commit, IC
 *	      announce/ack, LMON orchestration, GRD/PCM cleanup dispatch).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_node_remove.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.18-online-node-leave-fence-cleanup.md
 *	  Depends on spec-5.13 (clean_departed), spec-5.14 (reconfig_kind),
 *	  spec-5.15 (membership_state SSOT + incarnation guard), spec-4.12
 *	  (cooperative write-fence) substrate; this module is the leave-subband
 *	  finalizer (permanent removal).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_NODE_REMOVE_H
#define CLUSTER_NODE_REMOVE_H

#include "c.h"
#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/lwlock.h"

#include "cluster/cluster_marker_async.h"

#include "cluster/cluster_write_fence.h" /* CLUSTER_FENCE_MARKER_BYTES (marker offset) */

/* 128 nodes, same width as ReconfigEvent.dead_bitmap. */
#define CLUSTER_NODE_REMOVE_ACK_BITMAP_BYTES 16
/* Default cleanup ACK-barrier deadline; aligns with the feature-082 30s barrier. */
#define CLUSTER_NODE_REMOVE_CLEANUP_TIMEOUT_DEFAULT_MS 30000

/*
 * ClusterRemovePhase — survivor-coordinator removal state machine (§2.1 / §3.1).
 * 10 states.  IDLE is the only resting state.  CLEANUP_BLOCKED is a post-SHRUNK
 * resumable state: removal is already committed (member shrunk + fenced,
 * irreversible) but cleanup is incomplete; it is fail-closed (never reports
 * COMMITTED) and retryable, NOT escalated.  ABORTED is a clean pre-fence abort;
 * ABORTED_ESCALATE is reachable only PRE-SHRUNK (removal not committed -> hand to
 * fail-stop 5.14).
 */
typedef enum ClusterRemovePhase {
	CLUSTER_REMOVE_IDLE = 0,  /* no removal in progress */
	CLUSTER_REMOVE_REQUESTED, /* operator request recorded */
	CLUSTER_REMOVE_PRECHECK,  /* validating drained / quorum / declared / !self / !already */
	CLUSTER_REMOVE_FENCE_ARMING, /* writing 4.12 fence marker (target in fenced set), wait majority-durable */
	CLUSTER_REMOVE_SHRINK_COMMITTING, /* marker REMOVING + epoch bump + publish membership=REMOVED + marker SHRUNK */
	CLUSTER_REMOVE_CLEANUP, /* cluster-wide cleanup_on_exit: GRD remaster + GES/PCM clear + verify */
	CLUSTER_REMOVE_CLEANUP_BLOCKED, /* post-SHRUNK cleanup leftover/deadline: committed+fenced, cleanup pending;
									   * RESUMABLE/retryable; fail-closed (NEVER reports COMMITTED); NOT escalated */
	CLUSTER_REMOVE_COMMITTED, /* removal complete; target fenced + non-member + zero leftover (marker REMOVED) */
	CLUSTER_REMOVE_ABORTED, /* clean abort (precheck reject / fence-arm confirmed-no-majority) — nothing committed */
	CLUSTER_REMOVE_ABORTED_ESCALATE /* PRE-SHRUNK only: real death/contest before membership commit -> fail-stop 5.14.
									   * Post-SHRUNK is irreversible -> never reaches here (uses CLEANUP_BLOCKED). */
} ClusterRemovePhase;

/*
 * ClusterRemoveRequestResult — pg_cluster_remove_node() behaviour matrix (§2.2 /
 * D16).  Mapped to the operator UDF's text return by cluster_node_remove_views.c.
 */
typedef enum ClusterRemoveRequestResult {
	CLUSTER_REMOVE_REQ_ACCEPTED = 0,	   /* "accepted" — entered REQUESTED */
	CLUSTER_REMOVE_REQ_FEATURE_DISABLED,   /* "rejected:feature_disabled" (GUC off) */
	CLUSTER_REMOVE_REQ_CANNOT_REMOVE_SELF, /* "rejected:cannot_remove_self" */
	CLUSTER_REMOVE_REQ_NOT_DECLARED,	   /* "rejected:not_declared" */
	CLUSTER_REMOVE_REQ_NOT_DRAINED,		   /* "rejected:node_not_drained" (INV-LF4) */
	CLUSTER_REMOVE_REQ_NOT_IN_QUORUM,	   /* "rejected:not_in_quorum" */
	CLUSTER_REMOVE_REQ_ALREADY_REMOVED, /* "noop:already_removed" — ONLY when marker==REMOVED (fully complete) */
	CLUSTER_REMOVE_REQ_RESUME, /* "resume:cleanup_pending" — SHRUNK/CLEANUP_BLOCKED-but-not-REMOVED:
											* re-drives cleanup (accepted), NOT a noop (v0.4 P1) */
	CLUSTER_REMOVE_REQ_IN_PROGRESS /* "rejected:removal_in_progress" — active drive in a non-blocked phase */
} ClusterRemoveRequestResult;

/*
 * ClusterRemovalMarker — durable, CRC-checked voting-disk removal marker (§2.5).
 * The removed node's durable truth source (revival defense + startup rebuild).
 * Rides the voting-slot _reserved1 region right AFTER the 4.12 fence marker, so
 * the SAME coordinator self-slot carries both (INV-LF2 requires both durable on
 * the same slot).  Three phases bracket the two milestones (membership-shrink
 * commit vs removal-complete): REMOVING -> SHRUNK -> REMOVED.
 *
 * v0.5 P1 命门: the fixed offset only guarantees non-overlap, NOT survival —
 * qvotec memsets the whole _reserved1 every poll and only repacks/preserves the
 * fence marker, so this marker MUST mirror the 4.12 R13 carry-forward machinery
 * (pack / preserve_per_disk / majority authority_decide), else heartbeat/baseline/
 * fence-submit erases it (D8 / R12).
 */
#define CLUSTER_REMOVAL_MARKER_MAGIC 0x52454d56 /* "REMV" */
#define CLUSTER_REMOVAL_MARKER_VERSION 1

typedef enum ClusterRemovalMarkerPhase {
	CLUSTER_REMOVAL_MARKER_REMOVING
	= 1, /* pre-epoch-bump durable; NOT a trust source (mirror 5.13 COMMITTING) */
	CLUSTER_REMOVAL_MARKER_SHRUNK
	= 2, /* post-epoch-bump durable; membership shrunk + fence armed, cleanup
										  * PENDING.  Trusts only "N is non-member + fenced"; startup MUST
										  * resume/finish cleanup, MUST NOT report COMMITTED. */
	CLUSTER_REMOVAL_MARKER_REMOVED
	= 3 /* written ONLY after verify_no_leftover + all-survivor ACK; THE final
										  * trust source = "removal complete, zero leftover". */
} ClusterRemovalMarkerPhase;

typedef struct ClusterRemovalMarker {
	uint32 magic;		   /* CLUSTER_REMOVAL_MARKER_MAGIC — guards against a stale slot */
	uint16 version;		   /* CLUSTER_REMOVAL_MARKER_VERSION */
	uint16 phase;		   /* ClusterRemovalMarkerPhase */
	int32 removed_node_id; /* who is removed (rebuild must check == declared peer) */
	uint32 _pad0;
	uint64 remove_epoch;		/* epoch this removal is bound to */
	uint64 removed_incarnation; /* target's last admitted incarnation (re-admit must exceed) */
	uint64 removal_event_id;	/* identity (mirror 5.13 event_id; not sorted) */
	uint32 crc32c;				/* CRC32C over [magic..removal_event_id] (rule 15 integrity) */
} ClusterRemovalMarker;			/* 48 bytes */

/*
 * v0.4 P2: FIXED on-slot offset.  The 4.12 fence marker occupies voting-slot
 * _reserved1[0 .. CLUSTER_FENCE_MARKER_BYTES) (= [0,64)).  INV-LF2 requires the
 * fence marker AND the removal marker BOTH durable on the SAME coordinator self-
 * slot, so they MUST NOT overlap.  The removal marker is pinned right after the
 * fence marker.  (The "fits within _reserved1, clear of slot crc32c" StaticAssert
 * needs ClusterVotingSlot and lives in cluster_node_remove_policy.c, mirroring the
 * fence-marker StaticAsserts in cluster_write_fence.c.)
 */
#define CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET                                                    \
	CLUSTER_FENCE_MARKER_BYTES /* 64 -> slot offset 192 */
StaticAssertDecl(CLUSTER_REMOVAL_MARKER_RESERVED1_OFFSET >= CLUSTER_FENCE_MARKER_BYTES,
				 "removal marker must not overlap the 4.12 fence marker in _reserved1");

/* outcome of cluster_node_remove_submit_marker (mirrors the fence-marker result). */
typedef enum ClusterRemovalMarkerSubmitResult {
	CLUSTER_REMOVAL_MARKER_SUBMIT_ACK = 0, /* marker durable on >= quorum-majority disks */
	CLUSTER_REMOVAL_MARKER_SUBMIT_FAILED,  /* qvotec wrote but did not reach majority */
	CLUSTER_REMOVAL_MARKER_SUBMIT_TIMEOUT  /* qvotec did not complete within the bound */
} ClusterRemovalMarkerSubmitResult;

/*
 * ClusterNodeRemoveState — shmem state for the in-progress removal (§2.1).  Single
 * removal at a time (declared-set is static in v1).  Guarded by `lock`; `phase` is
 * also kept as an atomic for the hot serve-gate read.
 */
typedef struct ClusterNodeRemoveState {
	LWLock lock;			   /* guard phase transitions + publish */
	pg_atomic_uint32 phase;	   /* ClusterRemovePhase (atomic read for hot gate) */
	int32 target_node_id;	   /* -1 if none; node being removed */
	int32 coordinator_node_id; /* survivor driving removal (min(member)) */
	uint64 remove_epoch;	   /* epoch this removal is bound to (immutable once set) */
	uint64
		target_last_incarnation; /* target's last admitted incarnation (durable removed record) */
	uint64 removal_event_id;	 /* removal identity (NODE_REMOVED event id; not sorted) */
	uint64 remove_baseline_dead_gen; /* CSSD dead_generation when bound; an unchanged value at the
									  * epoch bump means OUR removal committed (no real death intruded) */
	bool fence_armed;				 /* INV-LF2: true only after 4.12 marker majority-durable */
	bool membership_shrunk;			 /* true after membership_state[target]=REMOVED published */
	bool grd_cleaned;				 /* GRD remaster + cleanup_on_node_dead + verify done */
	bool pcm_cleaned;				 /* PCM clear + verify done */
	uint8 ack_bitmap[CLUSTER_NODE_REMOVE_ACK_BITMAP_BYTES]; /* survivor cleanup ACK set */
	TimestampTz
		cleanup_deadline_us; /* post-SHRUNK: exceed -> CLEANUP_BLOCKED (resumable); pre-SHRUNK n/a */

	/* observability counters */
	pg_atomic_uint64 removal_request_count;
	pg_atomic_uint64 removal_committed_count;
	pg_atomic_uint64 removal_aborted_count;
	pg_atomic_uint64 removal_escalate_count; /* pre-SHRUNK ABORTED_ESCALATE only */
	pg_atomic_uint64
		cleanup_blocked_count; /* post-SHRUNK cleanup blocked->resume cycles (LOG-once) */
	pg_atomic_uint64 leftover_detected_count;	  /* INV-LF3 fail-closed trips */
	pg_atomic_uint64 zombie_write_rejected_count; /* INV-LF8 (mirrors 53R51 trips for removed) */

	/* survivor / coordinator side of someone else's removal */
	pg_atomic_uint32 survivor_acked; /* this survivor dropped its N-refs + sent its cleanup ACK */
	pg_atomic_uint32 announce_sent;	 /* LMON broadcast the real NODE_REMOVE_ANNOUNCE once */

	/*
	 * Voting-disk removal-marker submit mailbox (§2.5 three-phase commit) — a
	 * local single-producer/single-consumer handshake mirroring the spec-4.12
	 * fence-marker mailbox.  Producer = this node's driver/LMON staging a marker;
	 * consumer = this node's qvotec (the sole voting-disk writer) writing it to
	 * this node's own self-slot _reserved1[64..] on every disk and acking
	 * majority-durability.  pending_marker is written before request_seq is bumped
	 * (write barrier between); qvotec sets completion_seq = request_seq when done.
	 */
	struct Latch *qvotec_latch;				/* qvotec publishes MyLatch (latch-wake) */
	pg_atomic_uint64 marker_request_seq;	/* driver bumps to submit a marker write */
	pg_atomic_uint64 marker_completion_seq; /* qvotec sets = request_seq when done */
	pg_atomic_uint32 marker_result;			/* ClusterRemovalMarkerSubmitResult */
	uint32 _pad1;
	ClusterRemovalMarker pending_marker; /* staged here before bumping request_seq */
} ClusterNodeRemoveState;

StaticAssertDecl(sizeof(ClusterNodeRemoveState) <= 512, "ClusterNodeRemoveState region budget");


/* ============================================================
 * IC wire payloads (D10) — NODE_REMOVE_ANNOUNCE / REMOVE_CLEANUP_ACK.
 *
 *	Each payload carries its own magic/version/CRC (rule 15) on top of the
 *	envelope CRC: the envelope CRC protects transport integrity; the payload
 *	magic+version+CRC additionally guard against a misrouted / version-skewed
 *	message being acted on as a removal command (8.A defense-in-depth).
 * ============================================================ */
#define CLUSTER_NODE_REMOVE_IC_MAGIC 0x50524943 /* "PRIC" — pgrac remove IC */
/*
 * Wire version.  v2 (spec-5.18 Hardening v1.1, HF-1): the announce payload
 * carries removed_incarnation so a non-target survivor can seed its own
 * membership_state[N]=REMOVED with the correct incarnation floor when it
 * applies the removal locally (INV-LF11), not just drop its N-refs.
 */
#define CLUSTER_NODE_REMOVE_IC_VERSION 2

/*
 * NODE_REMOVE_ANNOUNCE payload — coordinator -> all survivors (broadcast).
 *	Survivors enter removal-aware reconfig (drop their N-refs, accept the
 *	permanent remaster) and reply REMOVE_CLEANUP_ACK.
 */
typedef struct ClusterNodeRemoveAnnouncePayload {
	uint32 magic;
	uint16 version;
	uint16 _pad0;
	int32 coordinator_node_id;
	int32 target_node_id;		/* who is removed */
	uint64 remove_epoch;		/* the removal reconfig new_epoch */
	uint64 removal_event_id;	/* identity bound to this removal attempt */
	uint64 removed_incarnation; /* HF-1: target's pinned incarnation floor (for
								 * survivor-local membership REMOVED seed, INV-LF11) */
	uint32 crc;					/* CRC32C over [magic..removed_incarnation] */
} ClusterNodeRemoveAnnouncePayload;

/*
 * REMOVE_CLEANUP_ACK payload — survivor -> coordinator (point-to-point).
 *	"I dropped all refs to the removed node + accepted the permanent remaster."
 */
typedef struct ClusterNodeRemoveCleanupAckPayload {
	uint32 magic;
	uint16 version;
	uint16 _pad0;
	int32 survivor_node_id;
	int32 target_node_id;
	uint32 _pad1;
	uint64 remove_epoch;
	uint64 removal_event_id;
	uint32 crc; /* CRC32C over [magic..removal_event_id] */
} ClusterNodeRemoveCleanupAckPayload;


/* ============================================================
 * Pure decision layer (cluster_node_remove_policy.c) — no PG runtime.
 * ============================================================ */

/* Is the phase transition from->to a legal driver-FSM edge? (U2) */
extern bool cluster_node_remove_phase_valid_transition(ClusterRemovePhase from,
													   ClusterRemovePhase to);

/* canonical lowercase name for a phase (SRF / dump). */
extern const char *cluster_node_remove_phase_str(int phase);

/* canonical text for a request result (operator UDF return). */
extern const char *cluster_node_remove_request_result_str(ClusterRemoveRequestResult r);

/*
 * Precheck verdict (U10 / §3.2) — pure mapping of the live facts to a request
 * result.  marker_phase is the on-disk removal marker phase for the target
 * (0 = none).  drive_active = a non-blocked active drive is already running.
 */
extern ClusterRemoveRequestResult cluster_node_remove_precheck(bool feature_enabled, bool is_self,
															   bool is_declared, bool is_drained,
															   bool in_quorum, int marker_phase,
															   bool cleanup_blocked,
															   bool drive_active);

/*
 * version-coherent classify (U4 / L235): given a removal that detected a real
 * external death/contest (epoch or dead_generation bumped since bound), decide
 * the escape phase.  PRE-SHRUNK (membership not committed) -> ABORTED_ESCALATE
 * (hand to fail-stop 5.14).  POST-SHRUNK (committed + fenced, irreversible) ->
 * CLEANUP_BLOCKED (resumable; NEVER escalate, since N is already masked out of
 * effective_dead so fail-stop will not re-clean it).
 */
extern ClusterRemovePhase cluster_node_remove_classify_contest(bool membership_shrunk);

/* is the bound removal still version-coherent (no external epoch / dead_gen bump)? */
extern bool cluster_node_remove_version_coherent(uint64 bound_epoch, uint64 current_epoch,
												 uint64 bound_dead_gen, uint64 current_dead_gen);

/*
 * INV-LF2 ordering gate (U3): the driver may enter SHRINK_COMMITTING ONLY after
 * the 4.12 fence marker is confirmed majority-durable (fence_armed).  Pure mirror
 * of the driver gate so the ordering invariant is unit-exercisable.
 */
extern bool cluster_node_remove_may_enter_shrink(bool fence_armed);

/*
 * fence-arm timeout classify (U3b / P2-1): after a fence submit times out, the
 * marker may already be majority-durable on disk (4.12 markers are monotone and
 * cannot be cleanly retracted).  A clean ABORTED is allowed ONLY when a re-run of
 * cluster_fence_authority_decide confirms there is NO majority.  majority-or-
 * uncertain -> NOT a clean abort (conservatively keep fence_armed and continue).
 */
extern bool cluster_node_remove_fence_timeout_is_clean_abort(bool confirmed_no_majority);

/* removal-marker structural validation (magic/version/CRC/identity). */
extern void cluster_removal_marker_compute_crc(ClusterRemovalMarker *m);
extern bool cluster_removal_marker_struct_valid(const ClusterRemovalMarker *m,
												int32 expected_removed_node);

/*
 * INV-LF7 recovery decision (U11): given the durable (fence-says-N-fenced,
 * removal-marker-phase) pair recovered at startup, what does the coordinator do?
 *   fence=no,  rm=none           -> IDLE (nothing happened)
 *   fence=YES, rm=none/REMOVING  -> re-drive to REMOVED (do not leave fenced-member)
 *   fence=YES, rm=SHRUNK         -> resume CLEANUP -> REMOVED (never report COMMITTED early)
 *   fence=YES, rm=REMOVED        -> done (COMMITTED)
 *   fence=no,  rm=SHRUNK/REMOVED -> IMPOSSIBLE (INV-LF2 fence-first) -> corruption (caller FATAL)
 * Returns the resume phase, or CLUSTER_REMOVE_IDLE for the nothing-happened case;
 * *is_corruption is set true for the impossible fence=no+rm>=SHRUNK combos.
 */
extern ClusterRemovePhase cluster_node_remove_recover_phase(bool fence_says_fenced,
															int marker_phase, bool *is_corruption);

/*
 * Removal-marker carry-forward + majority authority decide (§2.5 / D8 / R12) —
 * mirror the 4.12 fence-marker R13 machinery so the qvotec per-poll _reserved1
 * memset does not erase the removal marker.
 *   pack:               write a marker into reserved1[64..]
 *   unpack:             read a marker from reserved1[64..] (false + zeroed on magic miss)
 *   preserve_per_disk:  carry THIS disk's own prior marker forward (anti-amplification)
 *   authority_decide:   majority over per-disk markers (P0a: identical tuple on >= majority)
 */
extern void cluster_removal_marker_pack(uint8 *reserved1, const ClusterRemovalMarker *m);
extern bool cluster_removal_marker_unpack(const uint8 *reserved1, ClusterRemovalMarker *out);
extern void cluster_removal_marker_preserve_per_disk(uint8 *new_reserved1,
													 const uint8 *prior_reserved1_same_disk);
extern bool cluster_removal_marker_authority_decide(const ClusterRemovalMarker *disk_markers,
													const bool *disk_has_marker, int n_disks,
													ClusterRemovalMarker *out);

/* IC payload integrity (D10 / U9 / rule 15) — pure CRC compute + validate. */
extern void cluster_node_remove_announce_compute_crc(ClusterNodeRemoveAnnouncePayload *p);
extern bool cluster_node_remove_announce_payload_valid(const ClusterNodeRemoveAnnouncePayload *p);
extern void cluster_node_remove_ack_compute_crc(ClusterNodeRemoveCleanupAckPayload *p);
extern bool cluster_node_remove_ack_payload_valid(const ClusterNodeRemoveCleanupAckPayload *p);


/* ============================================================
 * Runtime layer (cluster_node_remove.c).
 * ============================================================ */

/* shmem region (registered via the cluster shmem registry; init postmaster-once). */
extern Size cluster_node_remove_shmem_size(void);
extern void cluster_node_remove_shmem_init(void);
extern void cluster_node_remove_shmem_register(void);

/* ------------------------------------------------------------------
 * Voting-disk removal-marker three-phase commit (§2.5) — qvotec-mediated.
 *	submit_marker: driver/LMON side.  Stage a marker, wake qvotec, and block
 *	  (bounded) until qvotec wrote it to this node's own self-slot _reserved1[64..]
 *	  on a quorum-majority of disks (ACK) or failed/timed out.
 *	qvotec_poll_pending / _complete: qvotec side of the handshake (sole writer).
 *	publish_qvotec_latch: qvotec publishes MyLatch so the submitter can wake it.
 *	rebuild_from_disks: startup recovery (§2.5) — scan every node's self-slot
 *	  removal marker across a quorum-majority of disks; a struct-valid SHRUNK/
 *	  REMOVED marker seeds removed_bitmap + raises the epoch floor + enqueues
 *	  resume-cleanup (SHRUNK) or trusts complete (REMOVED).
 * ------------------------------------------------------------------ */
extern ClusterRemovalMarkerSubmitResult
cluster_node_remove_submit_marker(const ClusterRemovalMarker *m);
extern bool cluster_node_remove_submit_marker_async(ClusterMarkerAsync *a,
													const ClusterRemovalMarker *m,
													ClusterMarkerAsyncKind kind,
													int32 target_node,
													TimestampTz now);
extern ClusterMarkerPollResult cluster_node_remove_poll_marker_async(ClusterMarkerAsync *a,
																	 TimestampTz now,
																	 uint32 *out_result,
																	 uint64 *out_elapsed_us);
extern bool cluster_node_remove_qvotec_poll_pending(ClusterRemovalMarker *out);
extern void cluster_node_remove_qvotec_complete(bool acked);
extern void cluster_node_remove_publish_qvotec_latch(struct Latch *latch);
extern void cluster_node_remove_rebuild_from_disks(const int *fds, int n_disks);

/* operator-facing C driver entry (SQL UDF pg_cluster_remove_node wraps this). */
extern ClusterRemoveRequestResult cluster_node_remove_request(int32 node_id);

/* coordinator: phase state-machine step (backend / LMON ctx). */
extern void cluster_node_remove_drive(void);

/* INV-LF2 fence-arm for the removed node (returns true only after majority-durable). */
extern bool cluster_node_remove_arm_fence(int32 node_id, uint64 remove_epoch);

/* LMON orchestration (both sides): consume announce + advance barrier + resume/escalate. */
extern void cluster_node_remove_lmon_tick(void);

/* survivor ack of "dropped all refs to removed node + accepted permanent remaster". */
extern void cluster_node_remove_survivor_ack(int32 target_node_id, uint64 remove_epoch);

/* INV-LF3 cluster-wide cleanup_on_exit + zero-leftover proof (idempotent). */
extern bool cluster_node_remove_run_cleanup(int32 node_id, uint64 remove_epoch);
extern bool cluster_node_remove_verify_no_leftover(int32 node_id);

/* IC wire (D10): register the 2 removal msg types (called once from the cluster_lmon
 * msg-type registration block, postmaster phase 1). */
extern void cluster_node_remove_register_ic_msg_types(void);
extern void cluster_node_remove_ic_broadcast_announce(int32 target_node_id, uint64 remove_epoch,
													  uint64 removal_event_id,
													  uint64 removed_incarnation);
extern void cluster_node_remove_ic_send_ack(int32 dest_node_id, int32 target_node_id,
											uint64 remove_epoch, uint64 removal_event_id);

/* observability — always 1 row (consistent copy of the shmem state). */
extern void cluster_node_remove_get_state(ClusterNodeRemoveState *out);

/*
 * INV-LF9 self-demote: true iff THIS node observes itself removed+fenced; a new
 * writable transaction is then fail-closed with 53R64 (the node must be re-admitted
 * by an operator un-fence, not auto-rejoin).
 */
extern bool cluster_node_remove_self_is_removed(void);

#endif /* CLUSTER_NODE_REMOVE_H */
