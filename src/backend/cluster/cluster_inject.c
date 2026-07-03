/*-------------------------------------------------------------------------
 *
 * cluster_inject.c
 *	  pgrac cluster error-injection framework (Stage 0.27).
 *
 *	  Implements:
 *	    - The compile-time injection-point registry (six points covering
 *	      cluster_init_shmem / cluster_ic_init / cluster_conf_load).
 *	    - cluster_injection_run -- look up the named entry, dispatch the
 *	      armed fault type, increment the lifetime hit counter.
 *	    - Five fault dispatchers (ERROR / WARNING / SLEEP / CRASH / SKIP);
 *	      NONE is the implicit "counter only" path.
 *	    - Two SRFs (cluster_inject_fault / cluster_get_injection_state)
 *	      backing pg_stat_cluster_injections.
 *	    - The GUC startup-arm path: cluster.injection_points lists names
 *	      that should be auto-armed to fault_type=WARNING at backend
 *	      startup (counter + warn).  Wired via the GUC's assign_hook so
 *	      that postgresql.conf changes take effect on the next reload.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_inject.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The two SRF entry points are unconditionally compiled because
 *	  pg_proc.dat references them in both build modes; their bodies
 *	  are #ifdef USE_PGRAC_CLUSTER guarded.  The internal registry,
 *	  hit counters, and dispatcher are compiled out completely on
 *	  --disable-cluster builds (spec-0.3 contract).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "utils/builtins.h"

#include <sys/time.h> /* gettimeofday (full-duration sleep loop) */

#include "cluster/cluster_inject.h"


/* SRF info-V1 declarations -- always linked because pg_proc.dat
 * references these regardless of build mode. */
PG_FUNCTION_INFO_V1(cluster_inject_fault);
PG_FUNCTION_INFO_V1(cluster_get_injection_state);


#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * Registry (compile-time static array).
 * ============================================================ */

typedef struct ClusterInjectPoint {
	const char *name;			   /* compile-time string literal */
	pg_atomic_uint64 hits;		   /* lifetime hit counter */
	pg_atomic_uint32 armed_type;   /* ClusterInjectFaultType */
	pg_atomic_uint64 armed_param;  /* sleep us / SKIP cookie */
	pg_atomic_uint32 skip_pending; /* set by SKIP dispatch, read+reset by probe */
} ClusterInjectPoint;

/*
 * The 20 cluster injection points.  Order in this array drives the
 * SRF row order; entries kept sorted by name within each stage block.
 * 6 baseline points were established in spec-0.27; 8 more added at
 * spec-0.30 to cover every stage-0.x already-implemented subsystem
 * with a non-signal-handler function-level hook position; 3 added at
 * spec-1.1 for cluster_shared_fs; 3 more at spec-1.2 for cluster_smgr.
 *
 * Two subsystems deliberately excluded (spec-0.30 §1.4 exceptions):
 *   - cluster_elog: CLUSTER_LOG is a header macro that expands inline
 *     at each call site; cluster_elog.c has only a global variable
 *     definition with no function-level position.
 *   - cluster_signal: cluster_signal.c contains only the
 *     cluster_handle_reconfig_start_interrupt signal handler;
 *     docs/error-injection-design.md §4.2 forbids injection points
 *     inside signal handlers (async-signal-safe constraint).
 *
 * See docs/error-injection-design.md §2.4 for the naming convention
 * and §5 for the Stage 1+ roadmap.
 */
static ClusterInjectPoint cluster_injection_points[] = {
	/* Stage 0.27 baseline + 0.30 sweep (14 entries) */
	{ .name = "cluster-conf-load-success" },
	{ .name = "cluster-conf-parse-fail" },
	{ .name = "cluster-conf-shmem-init" },
	{ .name = "cluster-debug-dump-entry" },
	{ .name = "cluster-guc-init-pre-define" },
	{ .name = "cluster-ic-mock-send-pre-enqueue" },
	{ .name = "cluster-ic-tier-selected" },
	{ .name = "cluster-init-post-shmem" },
	{ .name = "cluster-init-pre-shmem" },
	{ .name = "cluster-init-top" },
	{ .name = "cluster-pgstat-mirror-sync" },
	{ .name = "cluster-shmem-request" },
	{ .name = "cluster-shutdown-top" },
	{ .name = "cluster-views-srf-entry" },

	/* Stage 1.1 cluster_shared_fs (3 entries) */
	{ .name = "cluster-shared-fs-backend-register" },
	{ .name = "cluster-shared-fs-init-top" },
	{ .name = "cluster-shared-fs-local-open" },

	/* Stage 1.2 cluster_smgr (3 entries) */
	{ .name = "cluster-smgr-create-top" },
	{ .name = "cluster-smgr-open-top" },
	{ .name = "cluster-smgr-which-decision" },

	/* Stage 1.3 cluster shmem region registry (4 entries) */
	{ .name = "cluster-shmem-region-init-post" },
	{ .name = "cluster-shmem-region-init-pre" },
	{ .name = "cluster-shmem-register-region" },
	{ .name = "cluster-shmem-views-srf-entry" },

	/* Stage 1.7 cluster PCM lock framework (4 entries; Q6 user 修订
	 * 2026-05-02 release-pre instead of release-post for naming honesty
	 * since 1.7 stub never reaches a 'post' point). */
	{ .name = "cluster-pcm-acquire-entry" },
	{ .name = "cluster-pcm-convert-pre" },
	{ .name = "cluster-pcm-downgrade-pre" },
	{ .name = "cluster-pcm-release-pre" },

	/*
	 * Stage 1.10 cluster startup phase machinery (17 entries; spec-1.10
	 * §2.5).  Per phase 0/1/2/3/4 three points: -enter / -exit / -fail
	 * (15 phase-specific) plus driver-top entries cluster-run-startup-
	 * top + cluster-run-shutdown-top (2 driver-level).  Symmetric layout
	 * lets 1.11-1.14 real handlers + Stage 4 recovery + Stage 6 reconfig
	 * arm faults at any granularity without re-amending the injection
	 * registry.  HC4: -enter + sleep fault simulates a stuck phase to
	 * verify cluster.phase{N}_timeout enforcement.
	 */
	{ .name = "cluster-run-shutdown-top" },
	{ .name = "cluster-run-startup-top" },
	{ .name = "cluster-startup-phase-0-enter" },
	{ .name = "cluster-startup-phase-0-exit" },
	{ .name = "cluster-startup-phase-0-fail" },
	{ .name = "cluster-startup-phase-1-enter" },
	{ .name = "cluster-startup-phase-1-exit" },
	{ .name = "cluster-startup-phase-1-fail" },
	{ .name = "cluster-startup-phase-2-enter" },
	{ .name = "cluster-startup-phase-2-exit" },
	{ .name = "cluster-startup-phase-2-fail" },
	{ .name = "cluster-startup-phase-3-enter" },
	{ .name = "cluster-startup-phase-3-exit" },
	{ .name = "cluster-startup-phase-3-fail" },
	{ .name = "cluster-startup-phase-4-enter" },
	{ .name = "cluster-startup-phase-4-exit" },
	{ .name = "cluster-startup-phase-4-fail" },
	/* spec-5.8 Hardening v1.0.1 — FC1 acting-gate test seam: drop one responder
	 * bit in the coordinator probe round to synthesise a partial member set. */
	{ .name = "cluster-lmd-force-partial-round" },
	/* Stage 1.11 Sprint B (spec-1.11 D10) — 6 LMON lifecycle injects. */
	{ .name = "cluster-lmon-pre-spawn" },
	{ .name = "cluster-lmon-post-spawn" },
	{ .name = "cluster-lmon-ready-publish" },
	{ .name = "cluster-lmon-main-loop-iter" },
	{ .name = "cluster-lmon-shutdown-pre" },
	{ .name = "cluster-lmon-shutdown-post" },
	/* Stage 1.12 Sprint B (spec-1.12 D10) — 6 LCK lifecycle injects. */
	{ .name = "cluster-lck-pre-spawn" },
	{ .name = "cluster-lck-post-spawn" },
	{ .name = "cluster-lck-ready-publish" },
	{ .name = "cluster-lck-main-loop-iter" },
	{ .name = "cluster-lck-shutdown-pre" },
	{ .name = "cluster-lck-shutdown-post" },
	/* Stage 1.13 (spec-1.13 D10) — 6 DIAG lifecycle injects. */
	{ .name = "cluster-diag-pre-spawn" },
	{ .name = "cluster-diag-post-spawn" },
	{ .name = "cluster-diag-ready-publish" },
	{ .name = "cluster-diag-main-loop-iter" },
	{ .name = "cluster-diag-shutdown-pre" },
	{ .name = "cluster-diag-shutdown-post" },
	/* Stage 1.14 (spec-1.14 D10) — 6 Cluster Stats lifecycle injects. */
	{ .name = "cluster-stats-pre-spawn" },
	{ .name = "cluster-stats-post-spawn" },
	{ .name = "cluster-stats-ready-publish" },
	{ .name = "cluster-stats-main-loop-iter" },
	{ .name = "cluster-stats-shutdown-pre" },
	{ .name = "cluster-stats-shutdown-post" },
	/* Stage 2.5 (spec-2.5 D11) — 6 CSSD lifecycle injects. */
	{ .name = "cluster-cssd-pre-spawn" },
	{ .name = "cluster-cssd-post-spawn" },
	{ .name = "cluster-cssd-ready-publish" },
	{ .name = "cluster-cssd-main-loop-pre-tick" },
	{ .name = "cluster-cssd-shutdown-pre" },
	{ .name = "cluster-cssd-shutdown-post" },
	/* spec-2.6 Sprint A Step 4 D14 — 5 qvotec / quorum-lite injects. */
	{ .name = "cluster-qvotec-poll-pre" },
	{ .name = "cluster-qvotec-poll-post" },
	{ .name = "cluster-voting-disk-write-fail" },
	{ .name = "cluster-quorum-loss-broadcast" },
	{ .name = "cluster-collision-detect" },
	/*
	 * spec-2.28 Sprint A Step 4 D12:  3 fence-lite injects.
	 *   pre-freeze-broadcast        : LMON before SendProcSignal loop;
	 *                                 098 TAP can pause/skip to test
	 *                                 commit-gate-only path
	 *   pre-self-fence-shutdown     : postmaster_check before kill(SIGINT);
	 *                                 098 TAP injects pause to verify
	 *                                 grace_ms timing then cancel
	 *   post-thaw-broadcast         : after thaw SendProcSignal loop;
	 *                                 098 TAP synchronisation point
	 */
	{ .name = "cluster-fence-pre-freeze-broadcast" },
	{ .name = "cluster-fence-pre-self-fence-shutdown" },
	{ .name = "cluster-fence-post-thaw-broadcast" },
	/* spec-2.29 D10 (5 reconfig inject points):
	 *   cluster-reconfig-tick-entry        : lmon_tick top of body
	 *   cluster-reconfig-decide-coordinator: after Q2 A'' rule computed
	 *   cluster-reconfig-epoch-bump-pre    : right before D18 advance
	 *   cluster-reconfig-broadcast-procsig-pre: before ProcArray SendProcSignal
	 *   cluster-cssd-mark-peer-dead        : unit/inject test bypass for CSSD
	 *                                        deadband_scan (forces peer DEAD
	 *                                        without real heartbeat miss)
	 */
	{ .name = "cluster-reconfig-tick-entry" },
	{ .name = "cluster-reconfig-decide-coordinator" },
	{ .name = "cluster-reconfig-epoch-bump-pre" },
	{ .name = "cluster-reconfig-broadcast-procsig-pre" },
	/*
	 * spec-5.15 Hardening v1.1 (HF-1): pause the join coordinator inside
	 * commit_member after the COMMITTED marker is majority-durable but before
	 * the publish, so t/315 L8 can prove the half-publish window keeps the
	 * joiner's write gate CLOSED (publish-proof, not the marker alone, opens it).
	 */
	{ .name = "cluster-reconfig-join-commit-marker-durable" },
	{ .name = "cluster-cssd-mark-peer-dead" },
	/* Stage 1.15 (spec-1.15 D11 inject) — 4 SCN encoding-layer injects. */
	{ .name = "cluster-scn-advance-pre" },
	{ .name = "cluster-scn-advance-post" },
	{ .name = "cluster-scn-observe-entry" },
	{ .name = "cluster-scn-wraparound-warning" },
	/* Stage 1.16 (spec-1.16 D5) — 5 commit/abort/observe-bump injects. */
	{ .name = "cluster-scn-commit-pre-advance" },
	{ .name = "cluster-scn-commit-post-advance" },
	{ .name = "cluster-scn-abort-pre-advance" },
	{ .name = "cluster-scn-abort-post-advance" },
	{ .name = "cluster-scn-observe-bump-pre" },
	/* Stage 1.17 (spec-1.17 D5) — 2 walwriter BOC sweep injects. */
	{ .name = "cluster-scn-boc-sweep-pre" },
	{ .name = "cluster-scn-boc-sweep-post" },
	/*
	 * Stage 1.18 (spec-1.18 D9) — 2 commit/abort WAL emit + replay observe.
	 *
	 *   cluster-scn-wal-write-pre: fires inside XactLogCommitRecord /
	 *	   XactLogAbortRecord while CritSectionCount > 0.  HC5 -- ereport(
	 *	   ERROR) here is converted to PANIC by PG's critical-section
	 *	   contract; tests that arm :error must expect PANIC + recovery
	 *	   path, NOT ERROR + retry.  Use :sleep / :skip / :crash for
	 *	   non-PANIC fault injection here.
	 *   cluster-scn-replay-observe-pre: fires inside
	 *	   cluster_scn_recovery_replay_observe (xact_redo_commit /
	 *	   xact_redo_abort entry).  Recovery hasn't entered a critical
	 *	   section yet at this point, so :error is ERROR-safe.
	 */
	{ .name = "cluster-scn-wal-write-pre" },
	{ .name = "cluster-scn-replay-observe-pre" },
	/*
	 * Stage 1.19 (spec-1.19 D7) — 1 WAL page-header init point.
	 *
	 *   cluster-wal-page-init-thread-id: fires inside
	 *	   AdvanceXLInsertBuffer (xlog.c:~1925) after the cluster
	 *	   placeholder writes (xlp_thread_id = LEGACY,
	 *	   xlp_cluster_flags = RESERVED) and before the rest of the
	 *	   page header (XLP_BKP_REMOVABLE / XLP_LONG_HEADER setup).
	 *
	 *	   HC5 v0.2 (mixed PANIC-capable / ERROR-safe context;
	 *	   user 反审 #3 落地):
	 *	     1. Caller path xlog.c:1617 = XLogInsertRecord:796
	 *		    START_CRIT_SECTION → :error becomes PANIC + crash
	 *		    recovery (PG critical-section forbids ereport ERROR).
	 *	     2. Caller path xlog.c:2829 = XLogWrite opportunistic
	 *		    AFTER xlog.c:2820 END_CRIT_SECTION → :error is
	 *		    ERROR-safe (rolls back transaction normally).
	 *
	 *	   Tests that arm :error MUST expect EITHER PANIC + crash
	 *	   recovery OR plain ERROR depending on which caller path
	 *	   triggers first under load.  Default fault types are
	 *	   :skip / :warning / :crash (PANIC-safe in either context).
	 *	   See spec-1.19 §3.3 + §4.2 069 TAP L5-L7.
	 *
	 *	   Unlike spec-1.18 cluster-scn-wal-write-pre (always inside
	 *	   XactLogCommitRecord critical section, single PANIC-only
	 *	   context) and unlike cluster-scn-replay-observe-pre (always
	 *	   outside critical section, single ERROR-safe context), this
	 *	   point is genuinely mixed.
	 */
	{ .name = "cluster-wal-page-init-thread-id" },
	/*
	 * spec-2.34 D17 (HC93 + retransmit / dedup / budget TAP support):
	 *
	 *	cluster-gcs-block-drop-reply-before-send:
	 *	  Fires inside cluster_gcs_block.c master-side reply path BEFORE
	 *	  cluster_ic_send_envelope.  Configured as SKIP via
	 *	  cluster_injection_should_skip → master returns without sending
	 *	  the reply for the next N requests, so the sender experiences
	 *	  timeout → retransmit → eventually CACHED_REPLY (dedup hit) or
	 *	  53R90 (budget exhausted).
	 *
	 *	cluster-gcs-block-force-epoch-stale-reply:
	 *	  Fires inside master-side handler before producing the reply.
	 *	  SKIP causes the master to send DENIED_EPOCH_STALE once, exercising
	 *	  the HC94 lazy retry path (sender re-lookup_master + retransmit
	 *	  within budget).
	 */
	{ .name = "cluster-gcs-block-drop-reply-before-send" },
	{ .name = "cluster-gcs-block-force-epoch-stale-reply" },
	/*
	 * spec-2.35 D15 — CF 2-way protocol fault injection.
	 *
	 *	cluster-gcs-block-forward-master-side:
	 *	  Fires inside master-side decision tree just before the forward
	 *	  branch is taken.  SKIP makes the master skip forward — falls
	 *	  through to STORAGE_FALLBACK / MASTER_NOT_HOLDER so TAP can
	 *	  exercise fallback paths under the same topology that would
	 *	  otherwise trigger forward.
	 *
	 *	cluster-gcs-block-evict-holder-before-ship:
	 *	  Fires inside holder-side forward handler before bufmgr copy.
	 *	  SKIP simulates evict race:  holder pretends buffer is not
	 *	  cached → reply DENIED_MASTER_NOT_HOLDER with HC108-authorized
	 *	  forwarding_master_node so sender's HC105 retransmit budget
	 *	  recovery path is exercised.
	 */
	{ .name = "cluster-gcs-block-forward-master-side" },
	{ .name = "cluster-gcs-block-evict-holder-before-ship" },
	/*
	 * spec-6.12a ㉕ — remote-holder downgrade refusal injection.
	 *
	 *	cluster-gcs-block-remote-downgrade:
	 *	  Fires inside the holder-side forward handler just before the
	 *	  quiescent X->S remote downgrade attempt.  SKIP makes the holder
	 *	  refuse the request → falls back to the pre-㉕ one-shot read-image
	 *	  ship, so TAP can pin the refusal path (and the
	 *	  a_remote_downgrade_refused_count observability) under a topology
	 *	  that would otherwise downgrade.
	 */
	{ .name = "cluster-gcs-block-remote-downgrade" },
	/*
	 * spec-5.2a D3 — FAITHFUL clean-page stale-holder injection.
	 *
	 *	cluster-clean-xfer-stale-holder:
	 *	  Fires in local_master_x_transfer_and_wait on a clean-eligible
	 *	  X_GRANTED reply, AFTER the holder has really shipped + (eager-)flushed
	 *	  + dropped its copy to N.  SKIP makes the requester skip the install so
	 *	  the master keeps recording the now-N holder -- the genuine F0-4 stale
	 *	  state -- which the next eligible request recovers via the
	 *	  storage-fallback break (L5).  One-shot (should_skip consumes).
	 */
	{ .name = "cluster-clean-xfer-stale-holder" },
	/*
	 * spec-2.36 D16 — CF 3-way protocol fault injection.
	 *
	 *	cluster-gcs-block-invalidate-drop-broadcast:
	 *	  Fires inside master broadcast loop.  SKIP makes the master drop
	 *	  the INVALIDATE envelope for the current bitmap bit, exercising
	 *	  HC116 retransmit budget + DENIED_INVALIDATE_TIMEOUT (53R91) path.
	 *
	 *	cluster-gcs-block-invalidate-stall-ack:
	 *	  Fires inside holder-side INVALIDATE handler before ack.  SKIP
	 *	  makes the holder never reply, driving master to wait until
	 *	  cluster.gcs_block_invalidate_ack_timeout_ms then surface 53R91.
	 *
	 *	cluster-gcs-block-x-forward-master-side:
	 *	  Fires inside master X-state decision branch.  SKIP makes the
	 *	  master skip the X-state special path, falling through to the
	 *	  original spec-2.33 master grant flow (TAP can verify the X
	 *	  decision tree gate without sourcing a real X holder).
	 *
	 *	cluster-gcs-block-starvation-force-denied:
	 *	  Fires inside master N→S handler.  SKIP forces DENIED_PENDING_X
	 *	  reply unconditionally so TAP can drive reader backoff retry +
	 *	  53R92 ERRCODE_CLUSTER_GCS_BLOCK_STARVATION_EXHAUSTED.
	 */
	{ .name = "cluster-gcs-block-invalidate-drop-broadcast" },
	{ .name = "cluster-gcs-block-invalidate-stall-ack" },
	{ .name = "cluster-gcs-block-x-forward-master-side" },
	{ .name = "cluster-gcs-block-starvation-force-denied" },
	/*
	 * spec-2.41 D5 / P1-C — SCN lost-write detector behavioral trigger.
	 *
	 *	cluster-gcs-block-stale-ship:
	 *	  Fires before gcs_block_lost_write_verdict() on BOTH ship paths —
	 *	  master-direct and holder-forward.  In a real 2-node cluster the
	 *	  master-direct detector is bypassed (self-ship / read-image goto),
	 *	  so the holder-forward path is the reachable twin that validates a
	 *	  cross-node transfer.  SKIP forces the SHIPPED page's pd_block_scn
	 *	  to InvalidScn while the expected pi_watermark_scn is VALID — §2.6
	 *	  branch 2 (a tracked block shipping an unstamped page = ANOMALY) —
	 *	  so the ship fails closed DENIED_LOST_WRITE, the requester
	 *	  ereport(53R93), and lost_write_invalidscn_failclosed_count grows.
	 *	  One-shot (should_skip consumes).  Anticipated by spec-2.37
	 *	  (deferred behavioral trigger), activated here for spec-2.41 §6 D.
	 */
	{ .name = "cluster-gcs-block-stale-ship" },
	/*
	 * spec-2.38 D14 — SI Broadcaster fault injection.
	 *
	 *	cluster-sinval-broadcast-drop-send:
	 *	  Fires inside cluster_sinval_drain_outbound_and_broadcast just
	 *	  before cluster_ic_send_envelope.  SKIP makes the broadcaster
	 *	  silently drop the batch (still consumes it from outbound queue
	 *	  and bumps echo_dropped_count for observability) so TAP can
	 *	  verify peer never sees the batch + validates HC134 fail-closed
	 *	  observability semantics.
	 *
	 *	cluster-sinval-receive-skip-validate:
	 *	  Fires inside cluster_sinval_handle_envelope before checksum /
	 *	  epoch / source_node validation.  SKIP makes the handler bypass
	 *	  validation entirely (still enqueues to inbound) so TAP can
	 *	  reproduce HC135 echo-defense violation scenarios + confirm that
	 *	  HC132 outbound-independence is the 唯一 hard echo 防线.
	 */
	{ .name = "cluster-sinval-broadcast-drop-send" },
	{ .name = "cluster-sinval-receive-skip-validate" },
	/*
	 * spec-2.39 D17 — ACK envelope fault injection (2 NEW points).
	 *
	 *	cluster-sinval-ack-drop-send:
	 *	  Fires inside cluster_sinval_drain_ack_outbound_and_send (LMON
	 *	  context, after dequeue but before cluster_ic_send_envelope).
	 *	  SKIP makes LMON silently drop the ACK envelope so sender path
	 *	  exercises ack_timeout WARN 53R95 + bump ack_timeout_count.
	 *
	 *	cluster-sinval-ack-skip-validate:
	 *	  Fires inside cluster_sinval_handle_ack_envelope.  SKIP bypasses
	 *	  envelope CRC / acker_node bound / cluster_conf membership / status
	 *	  validation entirely and proceeds directly to HTAB match (used
	 *	  to reproduce malformed-ack scenarios for TAP coverage of HC140).
	 */
	{ .name = "cluster-sinval-ack-drop-send" },
	{ .name = "cluster-sinval-ack-skip-validate" },

	/*
	 * spec-3.9 D7 own-instance CR injection points (4 entries).  These are
	 * SKIP-style precondition providers, NOT generic error/sleep dispatch:
	 * the CR code reads the armed state + param via cluster_cr_injection_armed()
	 * and raises its OWN SQLSTATE / runs its own pg_usleep so the taxonomy TAP
	 * asserts the CR code's precise 53R9F/53R9G/data_corrupted, not the
	 * framework's generic XX000 (spec-3.9 v0.4 F8/F9/F10).
	 *
	 *   cr_snapshot_too_old   (param = segment_id)  -> CR raises 53R9F
	 *   cr_corruption         (param 1..4 subkind)  -> CR raises data_corrupted
	 *   cr_cross_instance     (param = origin node) -> CR raises 53R9G
	 *   cr_construct_delay_us (param = delay us)    -> CR pg_usleep under the
	 *                                                  ClusterCRConstruct wait
	 *                                                  event (deterministic L8)
	 */
	{ .name = "cr_snapshot_too_old" },
	{ .name = "cr_corruption" },
	{ .name = "cr_cross_instance" },
	{ .name = "cr_construct_delay_us" },

	/*
	 * spec-5.53 D2b — provably-complete relfilenode-reuse epoch floor (1 entry).
	 * SKIP-style: when armed, smgrdounlinkall skips the CR pool lifecycle epoch
	 * bump for ONE relfilenode unlink, simulating a "missed bump" regression
	 * (cf. the spec-5.51 H1 narrow-gate miss).  The fault-inject test then proves
	 * the bump is the load-bearing floor: with it skipped, a real DROP does NOT
	 * advance cr_pool_epoch (the regression is observable / caught).
	 */
	{ .name = "cluster-cr-skip-epoch-bump" },

	/*
	 * spec-5.55 Hardening v1.1 (finding #2) — shared-resolver-cache memo-path
	 * acceptance fail-closed (1 entry).  When armed, a TRUST-mode memo hit's
	 * Source-3 acceptance verdict is forced to fail closed (gate (3)), driving
	 * the exact production branch (count_acceptance(false) -> INVALID_OR_AMBIGUOUS).
	 * The natural MVCC workload cannot reach it: a resolved deleter is always
	 * recent (scn >= horizon -> never wrap-suspect), and an old entry below an
	 * advanced horizon is never re-probed.  The fault-inject test then proves the
	 * branch routes correctly AND the 8.A balance invariant still holds.
	 */
	{ .name = "cluster-cr-resolver-memo-suspect" },

	/*
	 * spec-4.1 D7 — per-thread WAL routing (2 NEW points).
	 *
	 *	cluster-wal-thread-validate-pre:
	 *	  Observational.  Fires in cluster_wal_thread_init right before
	 *	  the directory-identity check (postmaster startup, only when
	 *	  cluster.wal_threads_dir is set).
	 *
	 *	cluster-wal-thread-claim-create-fail:
	 *	  Decision-style (cluster_injection_should_skip): when armed,
	 *	  claim_create() simulates an O_EXCL/fsync failure so cluster_tap
	 *	  exercises the FATAL 53RA1 first-boot path without real storage
	 *	  faults (t/242 L11).
	 */
	{ .name = "cluster-wal-thread-validate-pre" },
	{ .name = "cluster-wal-thread-claim-create-fail" },

	/*
	 * spec-4.2 D5 — ClusterWalState registry (2 NEW points).
	 *
	 *	cluster-wal-state-ensure-pre:
	 *	  Observational.  Fires in cluster_wal_thread_init right before
	 *	  cluster_wal_state_ensure() (postmaster startup, only when
	 *	  cluster.wal_threads_dir is set).
	 *
	 *	cluster-wal-state-write-fail:
	 *	  Decision-style (cluster_injection_should_skip): when armed,
	 *	  the own-slot publish simulates a write failure so the FATAL
	 *	  53RA2 path is exercisable without real storage faults.
	 */
	{ .name = "cluster-wal-state-ensure-pre" },
	{ .name = "cluster-wal-state-write-fail" },

	/*
	 * spec-4.5a §4.2 L4 (F4) — forced snapshot read_scn (1 NEW point).
	 *
	 *	cr_force_read_scn (param = absolute SCN):
	 *	  Precondition provider in the cr_* mould: when armed, every
	 *	  cluster-source snapshot this node builds takes read_scn = param
	 *	  instead of cluster_scn_current() (ClusterSnapshotRefreshFields).
	 *	  A cold-crash merged-recovery TAP has no natural way to hold a
	 *	  snapshot older than the peer's commit (every session post-dates
	 *	  recovery), so t/248 injects the older read point to drive the
	 *	  CR inverse-apply machinery; the natural consumers of old
	 *	  read_scns are runtime/warm cross-instance reads (4.6/4.7).
	 */
	{ .name = "cr_force_read_scn" },

	/*
	 * spec-4.6 §4.2 L11/L14 (1 NEW point).
	 *
	 *	cluster-grd-redeclare-skip:
	 *	  SKIP fault on the backend-side cooperative rebind walker
	 *	  (cluster_grd_redeclare_all_registered):  the armed backend
	 *	  neither rebinds nor acks, so the LMON rebuild barrier stalls —
	 *	  drives the rebuild_timeout fail-closed leg (shards stay frozen,
	 *	  53R9I on the request path, old holder NOT overwritten) and the
	 *	  post-barrier leak-sweep leg (terminate the armed backend:  its
	 *	  old-epoch release is wire-rejected, the leaked holder is swept
	 *	  at P6 on the master).  Per-backend arm (spec-0.27 §3.6):  arm
	 *	  and hold in the SAME session.
	 */
	{ .name = "cluster-grd-redeclare-skip" },

	/*
	 * spec-4.11 D1 increment 3b-1 (1 NEW point).
	 *
	 *	cluster-thread-recovery-drive:
	 *	  ERROR fault inside the online thread-recovery data driver
	 *	  (cluster_thread_recovery_drive_data), after the dead thread's reader is
	 *	  built but before any segment is opened.  Proves the R13 harness demotes
	 *	  a catchable ERROR to a result-returning BLOCKED (the recovery-apply
	 *	  worker survives), never swallowing a FATAL/PANIC.  Per-backend arm
	 *	  (spec-0.27 §3.6): arm and drive in the SAME session.
	 */
	{ .name = "cluster-thread-recovery-drive" },
	/*
	 * spec-4.8ab D1 (2 NEW points) — undo checkpoint-writeback boundary guards.
	 *
	 *	undo-force-wal-before-data-violation:
	 *	  SKIP fault inside flush_dirty_slot.  When armed, the WAL-before-data
	 *	  guard sees the block's protecting LSN one byte past the durable bound
	 *	  and fail-closes (53R9N) -- proves the guard fires (L3a).
	 *	undo-skip-checkpoint-flush-one:
	 *	  SKIP fault inside cluster_undo_buf_flush_all.  When armed, exactly one
	 *	  dirty block is left unflushed so the checkpoint-coverage re-scan detects
	 *	  a PRE-redo escapee and fail-closes (53R9N) -- proves the coverage guard
	 *	  fires (L3b).  Both one-shot (should_skip consumes the arm).
	 */
	{ .name = "undo-force-wal-before-data-violation" },
	{ .name = "undo-skip-checkpoint-flush-one" },

	/* spec-5.7 D6 (1 NEW point) — KO object-reuse flush barrier fail-closed.
	 *
	 *	ko-peer-skip-ack:
	 *	  SKIP fault inside cluster_ko_drain_inbound_and_apply on the PEER.
	 *	  When armed, the peer still flushes + drops the relfilenode's buffers
	 *	  but does NOT send the KO_FLUSH_ACK, so the dropping node's barrier
	 *	  times out and fails closed (53RAA) -- proves the apply-after-drop
	 *	  fail-closed path deterministically (t/297 L2). */
	{ .name = "cluster-ko-peer-skip-ack" },
	/*
	 * spec-5.13 D12 (6 NEW points) — cooperative clean-leave driver FSM.  These
	 * register the points so they are armable (cluster_inject_fault) + visible in
	 * pg_stat_cluster_injections.  A :sleep on -ges-drained / -gcs-flushed pauses
	 * the leaving node mid-drain (t/308 L8 escalate: SIGKILL while paused, before
	 * COMMIT_READY, so the survivor fail-stops the node instead of clean-departing
	 * it).  -escalate-to-failstop fires in cl_escalate.
	 */
	{ .name = "cluster-clean-leave-request" },
	{ .name = "cluster-clean-leave-quiesce-pre" },
	{ .name = "cluster-clean-leave-ges-drained" },
	{ .name = "cluster-clean-leave-gcs-flushed" },
	{ .name = "cluster-clean-leave-barrier-complete" },
	{ .name = "cluster-clean-leave-escalate-to-failstop" },
	/*
	 * spec-5.13 Hardening v1.0.3 (1 NEW point) — a survivor receives the F6
	 * preflight probe but replies neither ACK nor NAK (armed :skip on the
	 * survivor).  Drives the leaver's fail-closed preflight-incomplete gate
	 * (t/315 P1): the leaver must reject:preflight_incomplete, NOT fail open.
	 */
	{ .name = "cluster-clean-leave-survivor-suppress-preflight-ack" },
	/*
	 * spec-5.18 D14 (7 NEW points) — permanent node-removal state-machine
	 * checkpoints, one per phase transition, so the cluster_tap can drive the
	 * fence-before-shrink ordering (INV-LF2), the three-phase marker recovery
	 * (INV-LF7), and the post-SHRUNK CLEANUP_BLOCKED resume (v0.4 P1).
	 */
	{ .name = "cluster-node-remove-request" },
	{ .name = "cluster-node-remove-precheck" },
	{ .name = "cluster-node-remove-fence-armed" },
	{ .name = "cluster-node-remove-shrink-committing" },
	{ .name = "cluster-node-remove-shrink-committed" },
	{ .name = "cluster-node-remove-cleanup-done" },
	{ .name = "cluster-node-remove-escalate" },
};

#define CLUSTER_INJECTION_COUNT lengthof(cluster_injection_points)


/*
 * Fast-path gate.  Read once per CLUSTER_INJECTION_POINT() expansion.
 * Maintained as a plain int (with atomic load semantics on the gate
 * write side) because all writes happen under the SRF arm/disarm path
 * which is serialised by GUC / superuser context.  Reads are racy by
 * design -- a stale "armed" read worst-case takes the slow path which
 * re-checks per-entry state atomically.
 */
int cluster_injection_armed_count = 0;


/*
 * Static initialiser hook.  pg_atomic_init_u32/u64 needs to be called
 * before first read/write; we lazy-init on the first arm/run.  Since
 * all writes funnel through cluster_injection_arm_internal (under
 * superuser context) and reads through cluster_injection_run (which
 * calls into init if needed), there is no early-fork race.
 */
static bool cluster_injection_initialised = false;

static void
cluster_injection_initialise(void)
{
	if (cluster_injection_initialised)
		return;

	for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
		pg_atomic_init_u64(&cluster_injection_points[i].hits, 0);
		pg_atomic_init_u32(&cluster_injection_points[i].armed_type, CLUSTER_FAULT_NONE);
		pg_atomic_init_u64(&cluster_injection_points[i].armed_param, 0);
		pg_atomic_init_u32(&cluster_injection_points[i].skip_pending, 0);
	}
	cluster_injection_initialised = true;
}


/* ============================================================
 * Lookup, arm/disarm, run, dispatchers.
 * ============================================================ */

static ClusterInjectPoint *
cluster_injection_lookup(const char *name)
{
	if (name == NULL)
		return NULL;

	for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
		if (strcmp(cluster_injection_points[i].name, name) == 0)
			return &cluster_injection_points[i];
	}
	return NULL;
}


static ClusterInjectFaultType
parse_fault_type(const char *s)
{
	if (s == NULL)
		return CLUSTER_FAULT_NONE;

	if (pg_strcasecmp(s, "none") == 0)
		return CLUSTER_FAULT_NONE;
	if (pg_strcasecmp(s, "error") == 0)
		return CLUSTER_FAULT_ERROR;
	if (pg_strcasecmp(s, "warning") == 0)
		return CLUSTER_FAULT_WARNING;
	if (pg_strcasecmp(s, "sleep") == 0)
		return CLUSTER_FAULT_SLEEP;
	if (pg_strcasecmp(s, "crash") == 0)
		return CLUSTER_FAULT_CRASH;
	if (pg_strcasecmp(s, "skip") == 0)
		return CLUSTER_FAULT_SKIP;
	return CLUSTER_FAULT_NONE; /* unknown -> NONE; caller may warn */
}


/*
 * spec-2.1 Hardening v1.0.2 D-I3 (codex review P2 post-Sprint B):
 * shared validator for fault type + param.  Both arm entry points
 * (SQL SRF cluster_inject_fault and GUC parser at line ~588) must
 * call this to enforce identical strict semantics.  v1.0.1 only
 * enforced strict validation in the SQL SRF; the GUC colon-form
 * (cluster.injection_points='name:sleep:-1') bypassed it and went
 * straight to pg_usleep with a wrap-around uint64.  Extracting a
 * single validator + calling from both sites prevents the same
 * bug from resurfacing as new arm entry points are added (Stage
 * 2.X is expected to introduce more inject scaffolding).
 *
 * Returns FAULT_VALIDATE_OK and writes resolved type to *out_type
 * on success.  On failure, writes the resolved (probably NONE)
 * type and returns a tag indicating which validation failed; the
 * caller is responsible for choosing ereport severity (SQL SRF
 * uses ERROR; GUC parser uses WARNING + skip per existing
 * pattern).
 */
typedef enum {
	FAULT_VALIDATE_OK,
	FAULT_VALIDATE_UNKNOWN_TYPE,
	FAULT_VALIDATE_NEGATIVE_SLEEP,
	FAULT_VALIDATE_SLEEP_TOO_LARGE
} ClusterInjectValidateResult;

#define CLUSTER_INJECT_SLEEP_CAP_US (INT64CONST(3600) * 1000 * 1000)

static ClusterInjectValidateResult
validate_fault_param(const char *type_str, int64 param, ClusterInjectFaultType *out_type)
{
	*out_type = parse_fault_type(type_str);

	if (*out_type == CLUSTER_FAULT_NONE
		&& (type_str == NULL || pg_strcasecmp(type_str, "none") != 0))
		return FAULT_VALIDATE_UNKNOWN_TYPE;

	if (*out_type == CLUSTER_FAULT_SLEEP) {
		if (param < 0)
			return FAULT_VALIDATE_NEGATIVE_SLEEP;
		if (param > CLUSTER_INJECT_SLEEP_CAP_US)
			return FAULT_VALIDATE_SLEEP_TOO_LARGE;
	}

	return FAULT_VALIDATE_OK;
}


static const char *
fault_type_name(ClusterInjectFaultType t)
{
	switch (t) {
	case CLUSTER_FAULT_NONE:
		return "none";
	case CLUSTER_FAULT_ERROR:
		return "error";
	case CLUSTER_FAULT_WARNING:
		return "warning";
	case CLUSTER_FAULT_SLEEP:
		return "sleep";
	case CLUSTER_FAULT_CRASH:
		return "crash";
	case CLUSTER_FAULT_SKIP:
		return "skip";
	}
	return "unknown";
}


/*
 * cluster_injection_arm_internal -- atomic arm / disarm core.
 *
 *	Caller must have already validated the name (via lookup) and the
 *	fault type.  Updates armed_type + armed_param atomically and
 *	maintains cluster_injection_armed_count so the fast-path gate
 *	stays accurate.
 */
static void
cluster_injection_arm_internal(ClusterInjectPoint *p, ClusterInjectFaultType new_type, int64 param)
{
	uint32 old_type;

	cluster_injection_initialise();

	old_type = pg_atomic_exchange_u32(&p->armed_type, new_type);
	pg_atomic_write_u64(&p->armed_param, (uint64)param);

	if (old_type == CLUSTER_FAULT_NONE && new_type != CLUSTER_FAULT_NONE)
		cluster_injection_armed_count++;
	else if (old_type != CLUSTER_FAULT_NONE && new_type == CLUSTER_FAULT_NONE)
		cluster_injection_armed_count--;
	/* same-direction transition (e.g. error -> warning): no count change */

	if (cluster_injection_armed_count < 0)
		cluster_injection_armed_count = 0; /* defensive */
}


/*
 * cluster_injection_run -- the slow-path entry called from
 * CLUSTER_INJECTION_POINT(name) when the fast-path gate is non-zero.
 */
void
cluster_injection_run(const char *name)
{
	ClusterInjectPoint *p;
	uint32 armed_type;
	uint64 param;

	cluster_injection_initialise();

	p = cluster_injection_lookup(name);
	if (p == NULL)
		return; /* unknown name; treat as no-op */

	(void)pg_atomic_fetch_add_u64(&p->hits, 1);

	armed_type = pg_atomic_read_u32(&p->armed_type);
	param = pg_atomic_read_u64(&p->armed_param);

	switch ((ClusterInjectFaultType)armed_type) {
	case CLUSTER_FAULT_NONE:
		/* counter-only mode (e.g. GUC startup arm without behaviour) */
		break;

	case CLUSTER_FAULT_ERROR:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster injection point \"%s\" armed with ERROR", name)));
		break;

	case CLUSTER_FAULT_WARNING:
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
						  errmsg("cluster injection point \"%s\" armed with WARNING", name)));
		break;

	case CLUSTER_FAULT_SLEEP: {
		/*
		 * pg_usleep() returns EARLY when interrupted by a signal (latch sets,
		 * procsignals, etc.).  A backend that is armed with a :sleep injection
		 * frequently receives such signals while it is paused -- e.g. a node
		 * mid-clean-leave -- which would cut a :sleep:Nus arm far short of N
		 * (observed in CI: a 30s arm slept ~2s, so a node meant to be held
		 * mid-phase raced past the injection and committed before the test could
		 * act).  A :sleep:Nus injection that sleeps for less than N is a framework
		 * bug: it makes every injection-driven pause non-deterministic.  Loop on
		 * the wall clock until the full requested duration has elapsed so the
		 * injection deterministically holds the phase.  CHECK_FOR_INTERRUPTS()
		 * keeps a genuine query-cancel / backend-terminate honoured (a SIGKILL is
		 * uncatchable and ends the backend immediately regardless); a bare latch
		 * set processes to nothing and the loop simply re-sleeps the remainder.
		 */
		struct timeval start, now;
		int64 elapsed_us;

		gettimeofday(&start, NULL);
		for (;;) {
			int64 remaining;

			gettimeofday(&now, NULL);
			elapsed_us = (int64)(now.tv_sec - start.tv_sec) * INT64CONST(1000000)
						 + (int64)(now.tv_usec - start.tv_usec);
			if (elapsed_us >= (int64)param)
				break;
			remaining = (int64)param - elapsed_us;
			CHECK_FOR_INTERRUPTS();
			pg_usleep((long)remaining);
		}
		break;
	}

	case CLUSTER_FAULT_CRASH:
#ifdef USE_ASSERT_CHECKING
		abort();
#else
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster injection point \"%s\" armed with CRASH "
							   "(degraded to ERROR in non-cassert build)",
							   name)));
#endif
		break;

	case CLUSTER_FAULT_SKIP:
		pg_atomic_write_u32(&p->skip_pending, 1);
		break;
	}
}


/*
 * cluster_injection_should_skip -- caller-side probe for SKIP fault.
 *
 *	Returns true iff a SKIP fault was just dispatched at the named
 *	point.  Auto-resets the pending flag so a single arm produces a
 *	single skip (no loops).  Caller code typically reads this right
 *	after CLUSTER_INJECTION_POINT(name) and short-circuits its
 *	protected logic when true.
 */
bool
cluster_injection_should_skip(const char *name)
{
	ClusterInjectPoint *p;

	cluster_injection_initialise();

	p = cluster_injection_lookup(name);
	if (p == NULL)
		return false;

	return pg_atomic_exchange_u32(&p->skip_pending, 0) != 0;
}


/*
 * cluster_cr_injection_armed -- spec-3.9 D7 CR-specific armed-state peek.
 *
 *	Returns true iff the named CR injection point is armed (any non-NONE
 *	fault type), and sets *out_param to its armed_param.  Unlike
 *	cluster_injection_should_skip, this does NOT consume / reset state: the
 *	point stays armed until disarmed with cluster_inject_fault(name,'none',0),
 *	matching the TAP arm -> trigger -> disarm pattern.  The CR code uses the
 *	param to raise its own precise SQLSTATE / run its own pg_usleep, so the
 *	taxonomy is the CR code's, not the framework's generic dispatch (F8/F9).
 *
 *	Bumps the lifetime hit counter so pg_stat_cluster_injections reflects use.
 */
bool
cluster_cr_injection_armed(const char *name, uint64 *out_param)
{
	ClusterInjectPoint *p;

	cluster_injection_initialise();

	p = cluster_injection_lookup(name);
	if (p == NULL)
		return false;
	if (pg_atomic_read_u32(&p->armed_type) == CLUSTER_FAULT_NONE)
		return false;

	if (out_param != NULL)
		*out_param = pg_atomic_read_u64(&p->armed_param);
	pg_atomic_fetch_add_u64(&p->hits, 1);
	return true;
}


/*
 * cluster_injection_get_count -- registry size accessor (stage 0.29).
 *
 *	Returns the compile-time number of injection points.  Used by
 *	cluster_debug.c for enumerating the registry without exposing the
 *	static array.
 */
int
cluster_injection_get_count(void)
{
	return CLUSTER_INJECTION_COUNT;
}

/*
 * cluster_injection_get_state_at -- read one registry entry (stage 0.29).
 *
 *	Iterator companion to cluster_injection_get_count.  Returns false
 *	if `idx` is out of range; otherwise fills *name_out / *type_out /
 *	*hits_out from the entry at index `idx`.  Pointer outputs are
 *	read atomically from the live atomic_uint32 / atomic_uint64.
 *
 *	`name_out` points into the compile-time registry's string literal;
 *	caller must not free or modify.
 */
bool
cluster_injection_get_state_at(int idx, const char **name_out, ClusterInjectFaultType *type_out,
							   uint64 *hits_out)
{
	ClusterInjectPoint *p;

	if (idx < 0 || idx >= CLUSTER_INJECTION_COUNT)
		return false;

	cluster_injection_initialise();

	p = &cluster_injection_points[idx];

	if (name_out != NULL)
		*name_out = p->name;
	if (type_out != NULL)
		*type_out = (ClusterInjectFaultType)pg_atomic_read_u32(&p->armed_type);
	if (hits_out != NULL)
		*hits_out = pg_atomic_read_u64(&p->hits);

	return true;
}


/*
 * cluster_injection_init_from_guc -- apply the cluster.injection_points
 * GUC value at backend startup.
 *
 *	Wired as the assign_hook for the GUC; runs whenever the value is
 *	loaded (postmaster start, SIGHUP, ALTER SYSTEM).  Re-arms named
 *	points to WARNING (counter + warn).  Names not in the new list
 *	revert to NONE iff they were previously WARNING-armed via this path
 *	(otherwise an explicit SQL arm wins).
 *
 *	For Stage 0.27 we keep the implementation simple: every point not
 *	named in the list is set back to NONE.  Mixed-mode (GUC arm +
 *	manual SRF arm) lives in a Stage 2+ richer GUC; for now,
 *	`cluster.injection_points` is "the WARNING set" and SRF is "the
 *	other behaviours".  See docs/error-injection-design.md §6.3.
 */
/*
 * cluster_injection_assign_hook -- assign_hook for cluster.injection_points.
 *
 *	Public so cluster_guc.c can pass it to DefineCustomStringVariable
 *	without having to know the registry internals.
 */
void
cluster_injection_assign_hook(const char *newval, void *extra)
{
	const char *cursor;
	bool seen[CLUSTER_INJECTION_COUNT];

	(void)extra;
	cluster_injection_initialise();

	memset(seen, 0, sizeof(seen));

	cursor = newval ? newval : "";
	while (*cursor != '\0') {
		const char *start;
		size_t len;
		char buf[128];
		ClusterInjectPoint *p;
		ClusterInjectFaultType arm_type = CLUSTER_FAULT_WARNING;
		int64 arm_param = 0;
		char *colon;
		ClusterInjectFaultType last_type;

		while (*cursor == ' ' || *cursor == ',' || *cursor == '\t')
			cursor++;
		if (*cursor == '\0')
			break;

		start = cursor;
		while (*cursor != '\0' && *cursor != ',' && *cursor != ' ' && *cursor != '\t')
			cursor++;
		len = cursor - start;
		if (len == 0)
			continue;
		if (len >= sizeof(buf))
			len = sizeof(buf) - 1;
		memcpy(buf, start, len);
		buf[len] = '\0';

		/*
		 * Spec-1.10.1 D7 extension: optional ':type[:param]' suffix lets
		 * tests arm a specific fault type from postmaster GUC time
		 * (before any SQL connection exists).  Backward compatible:
		 * bare 'name' still arms to WARNING/0.
		 *
		 *	cluster.injection_points = 'cluster-startup-phase-1-enter:sleep:2'
		 */
		colon = strchr(buf, ':');
		if (colon != NULL) {
			char *type_str = colon + 1;
			char *param_colon = strchr(type_str, ':');
			ClusterInjectValidateResult vr;

			*colon = '\0';
			if (param_colon != NULL) {
				*param_colon = '\0';
				arm_param = strtoll(param_colon + 1, NULL, 10);
			}

			/*
			 * Hardening v1.0.2 D-I3 (codex review P2 post-Sprint B):
			 * use the same validate_fault_param helper as the SQL SRF.
			 * Pre-v1.0.2 this path only checked unknown type and let
			 * negative / huge sleep params pass through to pg_usleep
			 * (uint64 wrap-around).  Per the GUC parser pattern keep
			 * WARNING + skip semantics (existing behaviour for unknown
			 * type) so a single malformed entry does not FATAL the
			 * postmaster startup.
			 */
			vr = validate_fault_param(type_str, arm_param, &arm_type);
			switch (vr) {
			case FAULT_VALIDATE_OK:
				break;
			case FAULT_VALIDATE_UNKNOWN_TYPE:
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unknown cluster injection fault type: \"%s\"", type_str)));
				continue;
			case FAULT_VALIDATE_NEGATIVE_SLEEP:
				ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								  errmsg("cluster injection sleep param must be >= 0 "
										 "microseconds (got %lld)",
										 (long long)arm_param)));
				continue;
			case FAULT_VALIDATE_SLEEP_TOO_LARGE:
				ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								  errmsg("cluster injection sleep param exceeds 1-hour "
										 "cap of 3600000000 us (got %lld)",
										 (long long)arm_param)));
				continue;
			}
		}

		p = cluster_injection_lookup(buf);
		if (p == NULL) {
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown cluster injection point in cluster.injection_points: \"%s\"",
							buf)));
			continue;
		}
		cluster_injection_arm_internal(p, arm_type, arm_param);
		seen[p - cluster_injection_points] = true;
		(void)last_type; /* silence unused under -Wunused-variable on some builds */
	}

	/*
	 * Disarm points that were not in the new list, but only the
	 * WARNING-armed kind.  SQL-armed faults (cluster_inject_fault
	 * with ERROR / SLEEP / CRASH / SKIP) are independent and survive
	 * GUC reloads.  Spec-1.10.1 D7 colon-syntax extensions go through
	 * the same path: a postmaster-time SLEEP arm survives subsequent
	 * cluster.injection_points reloads -- callers must explicitly
	 * disarm via cluster_inject_fault('name', 'none', 0) if needed.
	 */
	for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
		if (seen[i])
			continue;
		if (pg_atomic_read_u32(&cluster_injection_points[i].armed_type) == CLUSTER_FAULT_WARNING) {
			cluster_injection_arm_internal(&cluster_injection_points[i], CLUSTER_FAULT_NONE, 0);
		}
	}
}


/*
 * cluster_injection_init_from_guc -- public entry called by
 * cluster_init_guc to wire the assign_hook.  Cannot register the
 * assign_hook within DefineCustomStringVariable directly because
 * cluster_inject.h must not depend on guc.h.
 */
void
cluster_injection_init_from_guc(void)
{
	cluster_injection_initialise();
}


#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SRF entry points (always linked; bodies guarded by USE_PGRAC_CLUSTER).
 * ============================================================ */

Datum
cluster_inject_fault(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	text *name_text;
	text *type_text;
	int64 param;
	char *name;
	char *type_str;
	ClusterInjectPoint *p;
	ClusterInjectFaultType new_type;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to arm cluster injection points")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("name and fault_type must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	type_text = PG_GETARG_TEXT_PP(1);
	param = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT64(2);

	name = text_to_cstring(name_text);
	type_str = text_to_cstring(type_text);

	p = cluster_injection_lookup(name);
	if (p == NULL) {
		ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("unknown cluster injection point: \"%s\"", name)));
		PG_RETURN_BOOL(false);
	}

	/*
	 * Hardening v1.0.1 / codex review P2-2: validate fault type strictly
	 * (unknown -> ERROR) + sleep param range (0..1h us).
	 *
	 * v1.0.2 D-I3 refactor: shared validate_fault_param helper; same
	 * checks now also run in the GUC parser path (line ~588 below).
	 * Pre-v1.0.2 the SQL SRF was strict but the GUC colon-form
	 * (cluster.injection_points='name:sleep:-1') bypassed validation.
	 */
	{
		ClusterInjectValidateResult vr = validate_fault_param(type_str, param, &new_type);

		switch (vr) {
		case FAULT_VALIDATE_OK:
			break;
		case FAULT_VALIDATE_UNKNOWN_TYPE:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("unknown cluster injection fault type: \"%s\"", type_str),
							errhint("Valid fault types: none, error, warning, "
									"sleep, crash, skip.")));
			break;
		case FAULT_VALIDATE_NEGATIVE_SLEEP:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cluster injection sleep param must be >= 0 "
								   "microseconds (got %lld)",
								   (long long)param)));
			break;
		case FAULT_VALIDATE_SLEEP_TOO_LARGE:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cluster injection sleep param exceeds 1-hour "
								   "cap of 3600000000 us (got %lld)",
								   (long long)param)));
			break;
		}
	}

	cluster_injection_arm_internal(p, new_type, param);

	PG_RETURN_BOOL(true);
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_inject_fault requires --enable-cluster")));
	PG_RETURN_BOOL(false);
#endif
}


Datum
cluster_get_injection_state(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

		cluster_injection_initialise();

		for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
			ClusterInjectPoint *p = &cluster_injection_points[i];
			Datum values[4];
			bool nulls[4] = { false, false, false, false };
			uint32 t;

			t = pg_atomic_read_u32(&p->armed_type);

			values[0] = CStringGetTextDatum(p->name);
			values[1] = CStringGetTextDatum(fault_type_name((ClusterInjectFaultType)t));
			values[2] = Int64GetDatum((int64)pg_atomic_read_u64(&p->armed_param));
			values[3] = Int64GetDatum((int64)pg_atomic_read_u64(&p->hits));

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_stat_cluster_injections requires --enable-cluster")));
#endif

	return (Datum)0;
}
