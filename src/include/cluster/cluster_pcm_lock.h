/*-------------------------------------------------------------------------
 *
 * cluster_pcm_lock.h
 *	  pgrac cluster PCM (Parallel Cache Management) lock state machine.
 *
 *	  spec-1.7 shipped the API typedefs, opaque GrdEntry forward
 *	  declaration, GUC, inject points, and shmem registration surface.
 *	  spec-2.30 activates the local PCM state-machine body:
 *	  acquire / release / upgrade / downgrade now mutate a shmem HTAB
 *	  of opaque GrdEntry records protected by per-entry LWLockPadded.
 *	  Buffer manager callers, GCS wire requests, and convert-queue
 *	  protocol are intentionally deferred to later Cache Fusion specs.
 *
 *	  GrdEntry is intentionally an opaque struct (Q3 user 修订
 *	  2026-05-02): the full struct definition lives in
 *	  src/backend/cluster/cluster_pcm_lock.c (private).  Header
 *	  exposes only the typedef + helper functions
 *	  (cluster_pcm_grd_count / cluster_pcm_grd_shmem_size /
 *	  cluster_pcm_grd_init / cluster_pcm_grd_get_summary plus counter
 *	  accessors).  The opaque boundary lets later specs evolve fields
 *	  such as convert queues without exposing the internal ABI.
 *
 *	  GUC cluster.pcm_grd_max_entries default -1: auto-resolve to
 *	  NBuffers at startup.  Explicit 0 disables the PCM state machine
 *	  and preserves the old scaffolding behavior.
 *
 *	  pg_cluster_state.pcm exposes the activation state, live HTAB
 *	  summary rows, and per-transition counters for DBA diagnostics.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_pcm_lock.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.7-pcm-state-placeholder.md (frozen 2026-05-02 v1.1)
 *	  Design: docs/pcm-lock-protocol-design.md v1.0 §3-§5
 *	  AD-002 (PCM lock state machine N/S/X + PI orthogonal flag)
 *	  + AD-005 (Cache Fusion full; cf_state stub via BufferDesc)
 *	  + AD-006 (CR construction; PCM_TRANS_S_TO_X_CLEANOUT placeholder)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_LOCK_H
#define CLUSTER_PCM_LOCK_H

#include "c.h"
#include "cluster/cluster_buffer_desc.h" /* PcmState (1.6), INVALID_NODE_ID */
#include "cluster/cluster_conf.h"		 /* cluster_conf_has_peers */
#include "cluster/cluster_grd.h"		 /* ClusterGrdHolderId */
#include "cluster/cluster_resource_x_identity.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "cluster/cluster_scn.h"		 /* spec-2.41 D2 — SCN dual watermark */
#include "storage/buf_internals.h"		 /* BufferTag */

#ifdef USE_PGRAC_CLUSTER

/*
 * PcmLockMode -- API-level alias of PcmState (cluster_buffer_desc.h).
 *
 *	BufferDesc field stays named pcm_state (1.6 introduced); PCM lock
 *	API parameters use PcmLockMode for namespace clarity.  Same enum,
 *	same values via typedef alias (avoids value drift).
 *
 *	Constant aliases PCM_LOCK_MODE_N/S/X let API callers write
 *	  cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S)
 *	rather than
 *	  cluster_pcm_lock_acquire(tag, PCM_STATE_S)
 *	while sharing the underlying 0/1/2 values exactly.
 *
 *	Spec: spec-1.7 §1.4 example #4 (Q2 user 修订 2026-05-02 verified).
 */
typedef PcmState PcmLockMode;
#define PCM_LOCK_MODE_N PCM_STATE_N
#define PCM_LOCK_MODE_S PCM_STATE_S
#define PCM_LOCK_MODE_X PCM_STATE_X


/*
 * PcmLockTransition -- 9 legal state-machine transitions.
 *
 *	Defined per docs/pcm-lock-protocol-design.md §4.1 + AD-002.
 *	spec-2.30 uses these values as the executable local PCM
 *	state-machine input and increments one counter per transition.
 *
 *	Transition #9 (S→X cleanout) is reader-triggered ITL cleanout per
 *	AD-006 第四轮; Stage 3 (AD-006 第五轮 ~27000 LOC) wires actual
 *	cleanout calls.
 */
typedef enum PcmLockTransition {
	PCM_TRANS_N_TO_S = 1,			 /* read-first */
	PCM_TRANS_N_TO_X = 2,			 /* write-first */
	PCM_TRANS_S_TO_X_UPGRADE = 3,	 /* self upgrade */
	PCM_TRANS_X_TO_S_DOWNGRADE = 4,	 /* downgrade with PI */
	PCM_TRANS_X_TO_N_DOWNGRADE = 5,	 /* full downgrade with PI */
	PCM_TRANS_X_TO_N_RELEASE = 6,	 /* release without PI */
	PCM_TRANS_S_TO_N_INVALIDATE = 7, /* invalidated by remote X request */
	PCM_TRANS_S_TO_N_RELEASE = 8,	 /* local release */
	PCM_TRANS_S_TO_X_CLEANOUT = 9	 /* AD-006 ITL cleanout */
} PcmLockTransition;
#define PCM_TRANSITION_COUNT 9

/* Exact master-side apply verdict.  PENDING_X is distinct from a structural
 * state incompatibility so the block data plane can return its retryable
 * DENIED_PENDING_X status after the final, entry-lock-serialized admission
 * check closes a preflight-to-apply race. */
typedef enum PcmGcsTransitionApplyResult {
	PCM_GCS_TRANSITION_APPLIED = 0,
	PCM_GCS_TRANSITION_INCOMPATIBLE = 1,
	PCM_GCS_TRANSITION_PENDING_X = 2
} PcmGcsTransitionApplyResult;


/*
 * GrdEntry -- opaque per-block global lock state (master node).
 *
 *	Full struct definition lives in src/backend/cluster/cluster_pcm_lock.c
 *	(private).  Header exposes only the typedef and accessor/mutation APIs.
 *	The opaque boundary lets future Cache Fusion specs evolve fields such as
 *	convert queues without leaking layout into callers.
 */
typedef struct GrdEntry GrdEntry;

/* Stage 8 8.15-PRE-CAP D2: a directory entry may outlive the HTAB lock only
 * through this pinned identity.  The pointer remains opaque; tag, binding
 * generation and registry slot are the validation tuple used at release and
 * by the later retire/reuse batches. */
typedef enum PcmEntryLifecycle {
	PCM_ENTRY_LIVE = 1,
	PCM_ENTRY_QUIESCING,
	PCM_ENTRY_RETIRING
} PcmEntryLifecycle;

typedef enum PcmEntryAcquireResult {
	PCM_ENTRY_ACQUIRE_OK = 0,
	PCM_ENTRY_ACQUIRE_NOT_FOUND,
	PCM_ENTRY_ACQUIRE_RETRY_CURRENT,
	PCM_ENTRY_ACQUIRE_NO_CAPACITY,
	PCM_ENTRY_ACQUIRE_NOT_READY,
	PCM_ENTRY_ACQUIRE_CORRUPT
} PcmEntryAcquireResult;

/* Stage 8 8.15-PRE-CAP D3: the sole closed refusal domain for retiring a
 * PCM GRD binding.  These are object-lifecycle facts only; none grants or
 * advances Resource-X authority. */
typedef enum PcmRetireRefusal {
	PCM_RETIRE_REFUSAL_NONE = 0,
	PCM_RETIRE_REFUSAL_GATE_NOT_OPEN,
	PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH,
	PCM_RETIRE_REFUSAL_LIFECYCLE_NOT_LIVE,
	PCM_RETIRE_REFUSAL_PINNED,
	PCM_RETIRE_REFUSAL_WAITER_PRESENT,
	PCM_RETIRE_REFUSAL_TRANSPORT_PRESENT,
	PCM_RETIRE_REFUSAL_PCM_MODE_NOT_N,
	PCM_RETIRE_REFUSAL_HOLDER_PRESENT,
	PCM_RETIRE_REFUSAL_PI_PRESENT,
	PCM_RETIRE_REFUSAL_WATERMARK_PRESENT,
	PCM_RETIRE_REFUSAL_CONVERT_PENDING,
	PCM_RETIRE_REFUSAL_RESOURCE_X_ACTIVE,
	PCM_RETIRE_REFUSAL_RETAINED_PAIR_PRESENT,
	PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL,
	PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL,
	PCM_RETIRE_REFUSAL_FORMATION_STALE,
	PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY,
	PCM_RETIRE_REFUSAL_N
} PcmRetireRefusal;

typedef enum PcmRetireReason {
	PCM_RETIRE_REASON_HOLDER_RELEASE = 1,
	PCM_RETIRE_REASON_RESOURCE_X_SETTLED,
	PCM_RETIRE_REASON_PI_DISCARDED,
	PCM_RETIRE_REASON_FORMATION_SWEEP,
	PCM_RETIRE_REASON_BOUNDED_RECLAIM
} PcmRetireReason;

#define PCM_GRD_RECLAIM_SOFT_PERCENT 75
#define PCM_GRD_LMON_RECLAIM_BUDGET 256
#define PCM_GRD_SYNC_RECLAIM_BUDGET 64

typedef struct PcmReclaimBatch {
	uint32 examined_count;
	uint32 attempted_count;
	uint32 retired_count;
	uint32 next_cursor;
	uint8 complete_wrap;
	uint8 reserved[7];
} PcmReclaimBatch;

StaticAssertDecl(sizeof(PcmReclaimBatch) == 24,
				 "PcmReclaimBatch layout must remain 24 bytes");

/* Read-only D5 lifecycle observability.  Every value is sourced from the
 * native PCM directory/reclaim owner; none aliases the removed PCM-X ticket
 * runtime or grants Resource-X authority. */
typedef struct PcmGrdLifecycleStats {
	uint64 live_entries;
	uint64 tombstone_slots;
	uint64 binding_generation;
	uint64 reclaim_attempt_count;
	uint64 reclaim_success_count;
	uint64 reclaim_reuse_count;
	uint64 capacity_retry_count;
	uint64 capacity_fail_count;
	uint64 peak_live_entries;
	uint64 reclaim_refused[PCM_RETIRE_REFUSAL_N];
} PcmGrdLifecycleStats;

/* Passive paired maxima: index 0 is caller elapsed/budget, index 1 is
 * head age since semantic progress / no-progress budget. Never authority. */
typedef struct PcmWaitMarginObservation {
	BufferTag tag;
	int32 requester_node;
	uint64 monotonic_us;
	uint64 attempt;
	char phase[48];
	bool valid;
} PcmWaitMarginObservation;

typedef struct PcmWaitMarginStats {
	uint64 elapsed_us[2];
	uint64 budget_us[2];
	uint64 sample_count[2];
	uint64 capture_gap_count[2];
	uint64 metadata_gap_count[2];
	PcmWaitMarginObservation observation[2];
} PcmWaitMarginStats;

extern void cluster_pcm_wait_margin_note(bool head, uint64 elapsed_us, uint64 budget_us);
extern void cluster_pcm_wait_margin_note_exact(bool head, uint64 elapsed_us, uint64 budget_us,
											   const BufferTag *tag, int32 requester_node,
											   const char *phase, uint64 attempt,
											   uint64 monotonic_us);
extern bool cluster_pcm_wait_margin_snapshot(PcmWaitMarginStats *out);

#define PCM_RX_REQUESTER_WAIT_REASONS(X)                                                           \
	X(PREFLIGHT, "preflight")                                                                      \
	X(OBSERVATION, "observation")                                                                  \
	X(RESERVATION, "reservation")                                                                  \
	X(PREUSE, "preuse")                                                                            \
	X(INSTALL, "install")                                                                          \
	X(PREDECESSOR, "predecessor")                                                                  \
	X(DISPATCH, "dispatch")                                                                        \
	X(ROUND, "round")                                                                              \
	X(RETIRED_EMPTY, "retired_empty")                                                              \
	X(RETIRED_SUCCESSOR, "retired_successor")                                                      \
	X(TERMINAL, "terminal")

typedef enum PcmRxRequesterWaitReason {
#define PCM_RX_WAIT_REASON(id, name) PCM_RX_WAIT_##id,
	PCM_RX_REQUESTER_WAIT_REASONS(PCM_RX_WAIT_REASON)
#undef PCM_RX_WAIT_REASON
		PCM_RX_REQUESTER_WAIT_REASON_COUNT
} PcmRxRequesterWaitReason;

#define PCM_RX_WAIT_FIRST_REASON UINT32_C(1)
#define PCM_RX_WAIT_THRESHOLD_1 UINT32_C(2)
#define PCM_RX_WAIT_THRESHOLD_2 UINT32_C(4)
#define PCM_RX_WAIT_THRESHOLD_4 UINT32_C(8)

/* Process-local diagnostics, never copied into a head or used as authority. */
typedef struct PcmRxRequesterWaitState {
	uint64 started_us;
	uint64 budget_us;
	uint64 phase_started_us;
	uint64 last_us;
	uint64 total_age_us;
	uint64 phase_age_us;
	uint32 seen_reasons;
	uint8 threshold_seen[PCM_RX_REQUESTER_WAIT_REASON_COUNT];
	PcmRxRequesterWaitReason reason;
} PcmRxRequesterWaitState;

typedef struct PcmRxRequesterWaitSnapshot {
	uint64 attempt;
	uint64 diagnostic_started_us;
	uint64 last_semantic_progress_us;
	uint64 formation;
	uint64 master_session;
	uint64 r4_generation;
	int32 master_node;
	uint8 round_phase;
	uint8 local_owner_state;
	bool delivery_bound;
	bool delivery_executor_active;
} PcmRxRequesterWaitSnapshot;

extern uint32 cluster_pcm_rx_requester_wait_note(PcmRxRequesterWaitState *state,
												 PcmRxRequesterWaitReason reason, uint64 started_us,
												 uint64 budget_us, uint64 now_us);
extern const char *cluster_pcm_rx_requester_wait_reason_name(PcmRxRequesterWaitReason reason);
extern bool cluster_pcm_rx_requester_wait_snapshot(const BufferTag *tag,
												   PcmRxRequesterWaitSnapshot *out);

#define PCM_RX_METRICS(X)                                                                          \
	X(PCM_RX_HEAD_CREATE, "head_create_count")                                                     \
	X(PCM_RX_HEAD_JOIN, "head_join_count")                                                         \
	X(PCM_RX_FOLLOWER_DEADLINE, "follower_deadline_leave_count")                                   \
	X(PCM_RX_FOLLOWER_IDENTITY_REJECT, "follower_identity_reject_count")                           \
	X(PCM_RX_FOLLOWER_REJECT_MUTATION, "follower_reject_shared_mutation_count")                    \
	X(PCM_RX_CLAIM_BIND, "install_claim_bind_count")                                               \
	X(PCM_RX_CLAIM_CONFLICT, "install_claim_conflict_count")                                       \
	X(PCM_RX_RESERVATION_WAIT, "active_reservation_same_claim_wait_count")                         \
	X(PCM_RX_RESERVATION_CONFLICT, "active_reservation_conflict_count")                            \
	X(PCM_RX_RESERVATION_MALFORMED, "active_reservation_malformed_count")                          \
	X(PCM_RX_DISPATCH, "head_transport_dispatch_count")                                            \
	X(PCM_RX_FOLLOWER_DUPLICATE_ENQUEUE, "follower_duplicate_transport_enqueue_count")             \
	X(PCM_RX_SEMANTIC_PROGRESS, "semantic_progress_count")                                         \
	X(PCM_RX_HEAD_EXPIRE, "head_no_progress_expire_count")                                         \
	X(PCM_RX_GLOBAL_FUSE_TRIGGER, "global_fuse_trigger_count")                                     \
	X(PCM_RX_GLOBAL_FUSE_PERSISTED, "global_fuse_persisted_count")                                 \
	X(PCM_RX_DELIVERY_BIND, "delivery_target_bind_count")                                          \
	X(PCM_RX_DELIVERY_NORMAL, "delivery_normal_claim_count")                                       \
	X(PCM_RX_DELIVERY_CLEANUP, "delivery_cleanup_claim_count")                                     \
	X(PCM_RX_DELIVERY_OWNER_BUSY, "delivery_owner_busy_count")                                     \
	X(PCM_RX_DELIVERY_COMPLETE, "delivery_complete_count")                                         \
	X(PCM_RX_DELIVERY_CLEANUP_COMPLETE, "delivery_cleanup_complete_count")                         \
	X(PCM_RX_DELIVERY_REQUEST_RETRY, "delivery_request_retry_count")                               \
	X(PCM_RX_DELIVERY_SOURCE_REDRIVE, "delivery_source_block_redrive_count")                       \
	X(PCM_RX_DELIVERY_LATE_FRAME, "delivery_late_frame_accepted_count")                            \
	X(PCM_RX_DELIVERY_NAMESPACE_BLOCKED, "delivery_namespace_blocked_count")                       \
	X(PCM_RX_DELIVERY_CALLBACK, "delivery_callback_count")                                         \
	X(PCM_RX_DELIVERY_CALLBACK_MAX_US, "delivery_callback_max_us")                                 \
	X(PCM_RX_DELIVERY_PENDING_MAX_US, "delivery_pending_age_max_us")                               \
	X(PCM_RX_DELIVERY_FAILED_CALLER, "delivery_failed_caller_count")                               \
	X(PCM_RX_REQUESTER_WAIT_AGE_MAX_US, "requester_wait_age_max_us")                               \
	X(PCM_RX_REQUESTER_PHASE_AGE_MAX_US, "requester_wait_phase_age_max_us")                        \
	X(PCM_RX_REQUESTER_WAIT_DIAGNOSTIC_GAP, "requester_wait_diagnostic_gap_count")                 \
	X(PCM_RX_REQUESTER_WAIT_THRESHOLD, "requester_wait_threshold_count")                           \
	X(PCM_RX_REQUESTER_WAIT_PREFLIGHT, "requester_wait_preflight_count")                           \
	X(PCM_RX_REQUESTER_WAIT_OBSERVATION, "requester_wait_observation_count")                       \
	X(PCM_RX_REQUESTER_WAIT_RESERVATION, "requester_wait_reservation_count")                       \
	X(PCM_RX_REQUESTER_WAIT_PREUSE, "requester_wait_preuse_count")                                 \
	X(PCM_RX_REQUESTER_WAIT_INSTALL, "requester_wait_install_count")                               \
	X(PCM_RX_REQUESTER_WAIT_PREDECESSOR, "requester_wait_predecessor_count")                       \
	X(PCM_RX_REQUESTER_WAIT_DISPATCH, "requester_wait_dispatch_count")                             \
	X(PCM_RX_REQUESTER_WAIT_ROUND, "requester_wait_round_count")                                   \
	X(PCM_RX_REQUESTER_WAIT_RETIRED_EMPTY, "requester_reobserve_retired_empty_count")              \
	X(PCM_RX_REQUESTER_WAIT_RETIRED_SUCCESSOR, "requester_reobserve_retired_successor_count")      \
	X(PCM_RX_REQUESTER_WAIT_TERMINAL, "requester_wait_terminal_count")

typedef enum PcmRxMetric {
#define PCM_RX_ENUM(id, key) id,
	PCM_RX_METRICS(PCM_RX_ENUM)
#undef PCM_RX_ENUM
		PCM_RX_METRIC_COUNT
} PcmRxMetric;

typedef struct PcmRxStats {
	uint64 count[PCM_RX_METRIC_COUNT];
} PcmRxStats;

extern void cluster_pcm_rx_metric_note(PcmRxMetric metric);
extern void cluster_pcm_rx_delivery_max_note(PcmRxMetric metric, uint64 elapsed_us);
extern bool cluster_pcm_rx_stats_snapshot(PcmRxStats *out);
extern void cluster_pcm_rx_rejected_follower_note(uint64 before_generation,
												  uint64 after_generation);
extern void cluster_pcm_rx_dispatch_note(bool head_dispatch);
extern uint8 cluster_pcm_rx_last_step_head_failure(void);

/* One immediate wait result, process-local diagnostics only. */
typedef enum PcmRxWaitFailure {
	PCM_RX_WAIT_FAILURE_NONE,
	PCM_RX_WAIT_CALLER_DEADLINE_EXPIRED,
	PCM_RX_WAIT_HEAD_NO_PROGRESS_EXPIRED
} PcmRxWaitFailure;
extern PcmRxWaitFailure cluster_pcm_rx_take_wait_failure(void);

#define PCM_VM_METRICS(X)                                                                          \
	X(PCM_VM_X_REQUEST, "vm_x_request_count")                                                      \
	X(PCM_VM_HEAD_STARTED, "vm_head_started_count")                                                \
	X(PCM_VM_HEAD_JOIN, "vm_head_join_count")                                                      \
	X(PCM_VM_CACHED_HIT, "vm_cached_x_hit_count")                                                  \
	X(PCM_VM_INSTALL_COMPLETE, "vm_x_acquisition_completed_count")                                 \
	X(PCM_VM_REMOTE_X_TRANSFER, "vm_inter_node_transfer_count")                                    \
	X(PCM_VM_REMOTE_S_SOURCE, "vm_remote_shared_source_count")                                     \
	X(PCM_VM_HEAD_FAILED, "vm_head_failed_count")                                                  \
	X(PCM_VM_ALL_VISIBLE_CLEARED, "vm_all_visible_clear_count")                                    \
	X(PCM_VM_REPEAT_CLEAR, "vm_repeated_heap_clear_count")                                         \
	X(PCM_VM_CLEAR_OBSERVATION_GAP, "vm_clear_observation_gap_count")                              \
	X(PCM_VM_LATENCY_GAP, "vm_latency_capture_gap_count")

typedef enum PcmVmMetric {
#define PCM_VM_ENUM(id, key) id,
	PCM_VM_METRICS(PCM_VM_ENUM)
#undef PCM_VM_ENUM
		PCM_VM_METRIC_COUNT
} PcmVmMetric;

#define PCM_VM_HISTOGRAM_BUCKETS 32
#define PCM_VM_HEAP_BLOCKS ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4)
#define PCM_VM_BITMAP_WORDS ((PCM_VM_HEAP_BLOCKS + 63) / 64)
typedef struct PcmVmStats {
	bool tag_valid;
	BufferTag tag;
	uint64 other_tag_observations;
	uint64 clear_bitmap[PCM_VM_BITMAP_WORDS];
	uint64 count[PCM_VM_METRIC_COUNT];
	uint64 latency_count[2];
	uint64 latency_sum_us[2];
	uint64 latency_max_us[2];
	uint64 latency_histogram[2][PCM_VM_HISTOGRAM_BUCKETS];
} PcmVmStats;

extern void cluster_pcm_vm_metric_note(const BufferTag *tag, PcmVmMetric metric);
extern void cluster_pcm_vm_clear_note(const BufferTag *tag, BlockNumber heap_block);
extern void cluster_pcm_vm_latency_note(const BufferTag *tag, bool remote, uint64 elapsed_us);
extern bool cluster_pcm_vm_stats_snapshot(PcmVmStats *out);

StaticAssertDecl(sizeof(PcmGrdLifecycleStats)
				 == (9 + PCM_RETIRE_REFUSAL_N) * sizeof(uint64),
				 "PcmGrdLifecycleStats must remain a fixed native counter cohort");

/* Read-only current gauges for protocol debt which must drain before a
 * lifecycle acceptance point.  These values are derived from the existing
 * entry and Resource-X sidecar state; they are not persistent counters and
 * carry no authority. */
typedef struct PcmGrdProtocolDebtStats {
	uint64 wait_refcount;
	uint64 transport_refcount;
	uint64 retained_entry_count;
	uint64 active_resource_x_entry_count;
	uint64 local_owner_entry_count;
	uint64 evicting_entry_count;
	uint64 invalid_entry_count;
} PcmGrdProtocolDebtStats;

StaticAssertDecl(sizeof(PcmGrdProtocolDebtStats) == 7 * sizeof(uint64),
				 "PcmGrdProtocolDebtStats must remain a fixed read-only gauge cohort");

typedef struct PcmEntryRef {
	GrdEntry *entry;
	BufferTag tag;
	uint64 binding_generation;
	uint32 registry_slot;
	bool pinned;
	uint8 reserved[3];
} PcmEntryRef;

StaticAssertDecl(sizeof(PcmEntryRef) == 48,
				 "PcmEntryRef layout must remain 48 bytes");

/* A staged transport may outlive the lookup pin that created it.  This
 * exact identity keeps the binding non-retirable without granting PCM or
 * Resource-X authority. */
typedef struct PcmEntryTransportRef {
	GrdEntry *entry;
	BufferTag tag;
	uint64 binding_generation;
	uint32 registry_slot;
	bool held;
	uint8 reserved[3];
} PcmEntryTransportRef;

StaticAssertDecl(sizeof(PcmEntryTransportRef) == 48,
				 "PcmEntryTransportRef layout must remain 48 bytes");

extern bool pcm_entry_ref_acquire(const BufferTag *tag, bool create,
	PcmEntryRef *out, PcmEntryAcquireResult *result);
extern void pcm_entry_ref_release(PcmEntryRef *ref);
extern bool pcm_entry_transport_ref_begin(const PcmEntryRef *entry_ref,
	PcmEntryTransportRef *transport_ref);
extern void pcm_entry_transport_ref_end(PcmEntryTransportRef *transport_ref);
extern bool pcm_entry_retire_classify_exact(const BufferTag *tag,
	uint64 expected_generation, PcmRetireRefusal *why);
extern bool pcm_entry_try_retire_exact(const BufferTag *tag,
	uint64 binding_generation, PcmRetireReason reason);
extern bool cluster_pcm_lock_reclaim_bounded(uint32 probe_budget,
	PcmReclaimBatch *out);
extern void cluster_pcm_lock_lmon_reclaim_tick(void);
extern void cluster_pcm_grd_lifecycle_stats_snapshot(
	PcmGrdLifecycleStats *out);
extern void cluster_pcm_grd_protocol_debt_snapshot(
	PcmGrdProtocolDebtStats *out);

/* Stage 8 R9: one logical Resource-X acquisition is the already-frozen D6
 * assertion plus exact formation and acquisition generation.  Transport
 * freshness remains outside logical equality. */
typedef struct ResourceXAcquisitionRef {
	ResourceXAssertion assertion;
	uint64 formation;
	uint64 acquisition_generation;
} ResourceXAcquisitionRef;

StaticAssertDecl(sizeof(ResourceXAcquisitionRef) == 40,
				 "ResourceXAcquisitionRef layout must remain 40 bytes");

/* Process-local proof that one cached X is the exact product of the retained
 * direct-init terminal round and that a different FIFO successor is blocked
 * on that holder's final canonical authority.  This is neither wire nor GRD
 * authority; callers must still preserve the master WAIT_BLOCKERS and
 * BufferDesc REVOKING fences. */
typedef struct ResourceXTerminalXLineage {
	ResourceXAssertion holder_assertion;
	uint64 holder_attempt;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 accepted_base_authority_generation;
	uint64 final_authority_generation;
	uint64 direct_init_ownership_generation;
	uint64 cached_ownership_generation;
	uint64 r4_record_generation;
	uint64 successor_attempt;
	int32 master_node;
	int32 successor_node;
} ResourceXTerminalXLineage;

StaticAssertDecl(sizeof(ResourceXTerminalXLineage) == 104,
				 "ResourceXTerminalXLineage layout must remain 104 bytes");

/* PGRAC adaptation: entry-local, non-authority ownership of the narrow
 * terminal-X recycle/revoke interval.  The handle is never serialized and
 * cannot grant X; it only makes RECYCLING and REVOKING mutually exclusive
 * while preserving the exact retained terminal cover identity. */
typedef struct ResourceXLocalOwnerHandle {
	ResourceXAcquisitionRef ref;
	uint64 master_session_incarnation;
	uint64 r4_record_generation;
	uint64 buffer_ownership_generation;
	uint64 reservation_token;
	uint64 owner_generation;
	uint64 absolute_deadline_us;
	int32 owner_procno;
	uint32 reserved;
} ResourceXLocalOwnerHandle;

StaticAssertDecl(sizeof(ResourceXLocalOwnerHandle) == 96,
				 "ResourceXLocalOwnerHandle layout must remain 96 bytes");

/* Local LMS work, never a wire frame or shared-memory layout. The successful
 * claim transfers one existing raw pin; it does not acquire another pin. */
typedef struct ResourceXSourceFinishClaim {
	ResourceXDecodedFrame block;
	ResourceXDecodedFrame image;
	ResourceXLocalOwnerHandle owner;
	int32 master_node;
	int32 buffer_id;
} ResourceXSourceFinishClaim;

#define RESOURCE_X_PROGRESS_BOUND UINT32_C(0x00000001)
#define RESOURCE_X_PROGRESS_T1 UINT32_C(0x00000002)
#define RESOURCE_X_PROGRESS_T2 UINT32_C(0x00000004)
#define RESOURCE_X_PROGRESS_T3 UINT32_C(0x00000008)
#define RESOURCE_X_PROGRESS_RECOVERY_BLOCKED UINT32_C(0x00000010)
#define RESOURCE_X_PROGRESS_KNOWN_MASK UINT32_C(0x0000001f)

#define RESOURCE_X_GATE_OPEN UINT32_C(0)
#define RESOURCE_X_GATE_FROZEN UINT32_C(1)
#define RESOURCE_X_GATE_RECOVERY_BLOCKED UINT32_C(2)

/* Read-only projection of the native Resource-X admission gate.  This is
 * the source-removed replacement for sampling the legacy PCM-X runtime: it
 * carries no ticket/session authority and is valid only as one exact atomic
 * gate observation. */
typedef struct ResourceXGateSnapshot {
	uint64 formation;
	uint64 freeze_generation;
	uint32 phase;
	uint32 reserved;
} ResourceXGateSnapshot;

StaticAssertDecl(sizeof(ResourceXGateSnapshot) == 24,
				 "ResourceXGateSnapshot layout must remain 24 bytes");

typedef enum ResourceXApplyResult {
	RESOURCE_X_APPLY_APPLIED = 0,
	RESOURCE_X_APPLY_DUPLICATE,
	RESOURCE_X_APPLY_NOT_FOUND,
	RESOURCE_X_APPLY_STALE,
	RESOURCE_X_APPLY_BAD_STATE,
	RESOURCE_X_APPLY_INVALID,
	RESOURCE_X_APPLY_RECOVERY_BLOCKED
} ResourceXApplyResult;

typedef enum ResourceXBootstrapRoundAction {
	RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST = 1,
	RESOURCE_X_BOOTSTRAP_ROUND_WAIT,
	RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT,
	RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL,
	RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED,
	RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE,
	RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT,
	RESOURCE_X_BOOTSTRAP_ROUND_REOBSERVE
} ResourceXBootstrapRoundAction;

/* D1 read-only projection of the existing requester round.  It deliberately
 * carries no new authority and no entry binding generation: D2 introduces
 * that lifetime identity.  A zero binding generation in D1 logs therefore
 * means "not yet present", never a fabricated value. */
typedef struct ResourceXBootstrapRoundFailureSnapshot {
	ResourceXAcquisitionRef ref;
	uint64 base_authority_generation;
	uint64 authority_generation;
	uint64 buffer_ownership_generation;
	uint64 r4_record_generation;
	uint64 master_session_incarnation;
	uint64 absolute_deadline_us;
	uint64 head_change_generation;
	uint64 head_last_semantic_progress_us;
	uint64 head_no_progress_budget_us;
	uint64 requester_base_generation;
	uint64 retired_acquisition_generation;
	uint32 progress_flags;
	uint8 round_phase;
	uint8 terminal;
	uint8 head_failure_reason;
	uint8 reserved;
} ResourceXBootstrapRoundFailureSnapshot;

#define RESOURCE_X_HEAD_FAILURE_NONE UINT8_C(0)
#define RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED UINT8_C(1)
#define RESOURCE_X_HEAD_PROGRESS_OVERFLOW UINT8_C(2)
extern const char *cluster_pcm_lock_resource_x_head_failure_name(uint8 reason);

StaticAssertDecl(sizeof(ResourceXBootstrapRoundFailureSnapshot) == 136,
				 "ResourceXBootstrapRoundFailureSnapshot layout must remain 136 bytes");

/* A24: process-local identity retained by one ordinary TARGET follower while
 * the BufferDesc install and requester-round terminal publication cross
 * separate lock domains.  This object is never shared, serialized, persisted,
 * or interpreted as Resource-X authority. */
typedef enum ResourceXTargetInstallFollowState {
	RESOURCE_X_TARGET_INSTALL_INVALID = 0,
	RESOURCE_X_TARGET_INSTALL_INFLIGHT,
	RESOURCE_X_TARGET_INSTALL_TERMINAL,
	RESOURCE_X_TARGET_INSTALL_STALE,
	RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED,
	RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY,
	/* The BufferDesc projection changed while the entry result was sampled.
	 * This is a zero-authority instruction to retry under the original
	 * caller deadline, not an accepted Resource-X state. */
	RESOURCE_X_TARGET_INSTALL_RESAMPLE
} ResourceXTargetInstallFollowState;

#define RESOURCE_X_TARGET_INSTALL_CAPTURE_REVOKE_TOKEN UINT8_C(0x01)
#define RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT UINT8_C(0x02)
#define RESOURCE_X_INSTALL_CLAIM_NONE UINT8_C(0)
#define RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1 UINT8_C(1)
#define RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT UINT8_C(2)
struct ClusterPcmOwnSnapshot;

/* Called by the exact T1 installer while it still holds content-X on the
 * newly reserved descriptor.  This publishes physical evidence only. */
extern ResourceXApplyResult cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(
	const ResourceXAcquisitionRef *ref, const struct ClusterPcmOwnSnapshot *reserved);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_install_claim_snapshot_exact(
	const ResourceXAcquisitionRef *ref, uint8 *source_out, uint64 *pending_generation_out,
	uint64 *reservation_token_out);

/* Stack-only E observation for B-E-B claim binding.  It is never a grant,
 * persisted state, wire payload, or a wait/authority receipt. */
typedef struct ResourceXInstallClaimJoinObservation {
	ResourceXDecodedCommon request;
	uint64 entry_binding_generation;
	uint64 r4_record_generation;
	int32 master_node;
	uint32 master_ingress_connection_generation;
} ResourceXInstallClaimJoinObservation;
extern ResourceXApplyResult cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	ResourceXInstallClaimJoinObservation *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(
	const ResourceXInstallClaimJoinObservation *observation,
	const struct ClusterPcmOwnSnapshot *before, const struct ClusterPcmOwnSnapshot *after);
#define RESOURCE_X_TARGET_INSTALL_CAPTURE_KNOWN_MASK                                               \
	(RESOURCE_X_TARGET_INSTALL_CAPTURE_REVOKE_TOKEN | RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT)

typedef struct ResourceXTargetInstallContinuation {
	BufferTag resource;
	int32 requester_node;
	int32 master_node;
	uint64 entry_binding_generation;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 r4_record_generation;
	uint64 acquisition_generation;
	uint64 accepted_base_authority_generation;
	uint64 observed_head_change_generation;
	/* Original finite diagnostic threshold; not elapsed-only invalidation. */
	uint64 caller_absolute_deadline_us;
	uint64 requested_sleep_slice_us;
	uint64 pending_ownership_generation;
	uint64 expected_x_ownership_generation;
	uint64 reservation_token;
	uint64 observed_claim_pending_generation;
	uint64 observed_claim_reservation_token;
	uint32 requester_sender_connection_generation;
	uint32 master_ingress_connection_generation;
	uint8 capture_flags;
	bool valid;
} ResourceXTargetInstallContinuation;

typedef enum ResourceXExecutorProbeResult {
	RESOURCE_X_EXECUTOR_READY = 0,
	RESOURCE_X_EXECUTOR_COMPLETE,
	RESOURCE_X_EXECUTOR_BLOCKED,
	RESOURCE_X_EXECUTOR_CHANGED,
	RESOURCE_X_EXECUTOR_RECOVERY_BLOCKED
} ResourceXExecutorProbeResult;

typedef enum ResourceXNoProgressReason {
	RESOURCE_X_NO_PROGRESS_NONE = 0,
	RESOURCE_X_NO_PROGRESS_BUFFER_BUSY,
	RESOURCE_X_NO_PROGRESS_BUFFER_ABSENT,
	RESOURCE_X_NO_PROGRESS_BUFFER_STALE,
	RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT
} ResourceXNoProgressReason;

typedef struct ResourceXExecutorSnapshot {
	ResourceXAcquisitionRef ref;
	uint32 progress_flags;
	uint32 no_progress_reason;
	uint64 no_progress_generation;
	uint64 requester_base_generation;
	uint64 retired_acquisition_generation;
} ResourceXExecutorSnapshot;

typedef struct ResourceXBufferInstallProof {
	uint64 ownership_generation;
	uint64 writer_activation_token;
	uint64 resource_x_activation_generation;
} ResourceXBufferInstallProof;

typedef struct ResourceXBufferActivationProof {
	uint64 ownership_generation;
	uint64 writer_activation_token;
	uint64 resource_x_activation_generation;
} ResourceXBufferActivationProof;

typedef struct ResourceXActivationGateToken {
	uint64 formation;
	uint64 freeze_generation;
	uint64 acquisition_generation;
	uint32 active;
	uint32 reserved;
} ResourceXActivationGateToken;

StaticAssertDecl(sizeof(ResourceXExecutorSnapshot) == 72,
				 "ResourceXExecutorSnapshot layout must remain 72 bytes");
StaticAssertDecl(sizeof(ResourceXBufferInstallProof) == 24,
				 "ResourceXBufferInstallProof layout must remain 24 bytes");
StaticAssertDecl(sizeof(ResourceXBufferActivationProof) == 24,
				 "ResourceXBufferActivationProof layout must remain 24 bytes");
StaticAssertDecl(sizeof(ResourceXActivationGateToken) == 32,
				 "ResourceXActivationGateToken layout must remain 32 bytes");

typedef enum ResourceXReconfigResult {
	RESOURCE_X_RECONFIG_DONE = 0,
	RESOURCE_X_RECONFIG_MORE,
	RESOURCE_X_RECONFIG_RETRY,
	RESOURCE_X_RECONFIG_ORPHAN,
	RESOURCE_X_RECONFIG_CORRUPT
} ResourceXReconfigResult;

/* R10 retained master state.  Intent/reclaim values are wire-independent,
 * closed process-local result domains frozen by spec-8.10 D10-06. */
typedef enum ResourceXIntentResult {
	RESOURCE_X_INTENT_STAGED = 1,
	RESOURCE_X_INTENT_NOT_ADMITTED = 2,
	RESOURCE_X_INTENT_HARD_REARMED = 3,
	RESOURCE_X_INTENT_STALE = 4
} ResourceXIntentResult;

typedef enum ResourceXIntentProbeResult {
	RESOURCE_X_INTENT_PROBE_IDLE = 0,
	RESOURCE_X_INTENT_PROBE_FOUND = 1,
	RESOURCE_X_INTENT_PROBE_MORE = 2,
	RESOURCE_X_INTENT_PROBE_COMPLETE = 3,
	RESOURCE_X_INTENT_PROBE_CORRUPT = 4,
	/* Local work only: never encoded as a wire intent. */
	RESOURCE_X_INTENT_PROBE_DELIVERY = 5,
	RESOURCE_X_INTENT_PROBE_SOURCE_FINISH = 6
} ResourceXIntentProbeResult;

typedef enum ResourceXIntentState {
	RESOURCE_X_INTENT_SLOT_EMPTY = 0,
	RESOURCE_X_INTENT_SLOT_ARMED = 1,
	RESOURCE_X_INTENT_SLOT_STAGED = 2
} ResourceXIntentState;

typedef enum ResourceXIntentOwnerKind {
	RESOURCE_X_INTENT_OWNER_NONE = 0,
	RESOURCE_X_INTENT_OWNER_MASTER_BLOCK = 1,
	RESOURCE_X_INTENT_OWNER_MASTER_GRANT = 2,
	RESOURCE_X_INTENT_OWNER_HOLDER_STATUS = 3,
	RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE = 4,
	RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT = 5,
	RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE = 6
} ResourceXIntentOwnerKind;

/* This handle is meaningful only together with its resource-scoped owner
 * record.  It is carried by the LMS physical descriptor but is never a wire
 * field and never authorizes a global intent lookup. */
typedef struct ResourceXIntentBodyHandle {
	ResourceXAssertion assertion;
	uint64 owner_generation;
	uint32 owner_node;
	uint8 owner_kind;
	uint8 owner_index;
	uint16 reserved;
} ResourceXIntentBodyHandle;

typedef struct ResourceXIntentSlot {
	uint64 logical_generation;
	uint64 authority_generation;
	uint64 first_armed_us;
	uint64 last_attempt_us;
	uint32 destination_node;
	uint16 payload_bytes;
	uint8 kind;
	uint8 state;
	ResourceXIntentBodyHandle body;
} ResourceXIntentSlot;

StaticAssertDecl(sizeof(ResourceXIntentBodyHandle) == 40,
				 "ResourceXIntentBodyHandle layout must remain 40 bytes");
StaticAssertDecl(sizeof(ResourceXIntentSlot) == 80,
				 "ResourceXIntentSlot layout must remain 80 bytes");

typedef enum ResourceXReclaimResult {
	RESOURCE_X_RECLAIM_NONE = 1,
	RESOURCE_X_RECLAIM_NONHEAD = 2,
	RESOURCE_X_RECLAIM_HEAD_SUCCESSOR_STARTED = 3,
	RESOURCE_X_RECLAIM_ORPHAN_BLOCKED = 4
} ResourceXReclaimResult;

typedef enum ResourceXMasterPhase {
	RESOURCE_X_MASTER_NONE = 0,
	RESOURCE_X_MASTER_QUEUED = 1,
	RESOURCE_X_MASTER_WAIT_BLOCKERS = 2,
	RESOURCE_X_MASTER_WAIT_PROOF = 3,
	RESOURCE_X_MASTER_GRANT_COMMITTED = 4,
	RESOURCE_X_MASTER_SETTLED = 5,
	RESOURCE_X_MASTER_RECOVERY_BLOCKED = 6,
	RESOURCE_X_MASTER_RELEASED = 7
} ResourceXMasterPhase;

typedef struct ResourceXMasterSnapshot {
	ResourceXAssertion assertion;
	uint64 base_authority_generation;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 final_authority_generation;
	uint64 source_carrier_generation;
	uint64 requester_target_generation;
	uint32 incompatible_holders_bitmap;
	uint32 blocked_holders_bitmap;
	int32 source_node;
	uint8 phase;
	uint8 proof_kind;
	uint8 source_disposition;
	uint8 is_head;
} ResourceXMasterSnapshot;

StaticAssertDecl(sizeof(ResourceXMasterSnapshot) == 96,
				 "ResourceXMasterSnapshot layout must remain 96 bytes");

#define RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE UINT32_C(0x00000001)
#define RESOURCE_X_REQUESTER_JOIN_HAS_GRANT UINT32_C(0x00000002)
#define RESOURCE_X_REQUESTER_JOIN_READY UINT32_C(0x00000004)
#define RESOURCE_X_REQUESTER_JOIN_TERMINAL UINT32_C(0x00000008)
#define RESOURCE_X_REQUESTER_JOIN_AUTHORITY_WITH_IMAGE UINT32_C(0x00000010)
#define RESOURCE_X_REQUESTER_JOIN_KNOWN_MASK UINT32_C(0x0000001f)

/* Requester-local retained view of the two independent type-15 halves.
 * Connection-generation freshness is validated by ingress but is not part
 * of this logical equality snapshot. */
typedef struct ResourceXRequesterJoinSnapshot {
	ResourceXAssertion assertion;
	uint64 base_authority_generation;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 final_authority_generation;
	uint64 source_carrier_generation;
	uint64 requester_target_generation;
	uint64 t_image_us;
	uint64 t_grant_us;
	uint64 t_install_us;
	/* Actual accepted authority event; t_grant_us stays zero for a delegated
	 * image. grant_source_node names its authority master, not packet source. */
	uint64 t_authority_us;
	int32 grant_source_node;
	int32 image_source_node;
	uint32 flags;
	uint32 reserved;
} ResourceXRequesterJoinSnapshot;

StaticAssertDecl(sizeof(ResourceXRequesterJoinSnapshot) == 128,
				 "ResourceXRequesterJoinSnapshot layout must remain 128 bytes");

/* Requester-local ordinary Resource-X observation cohort.  These are
 * postmaster-incarnation readings, not Recovery Foundation proof. */
typedef struct ResourceXO1Stats {
	uint64 remote_install_observed_count;
	uint64 remote_grant_after_image_count;
	uint64 remote_image_at_or_after_grant_count;
	uint64 remote_episode_excluded_no_install;
	uint64 remote_episode_excluded_missing_grant;
	uint64 remote_episode_excluded_missing_image;
	uint64 last_remote_t_image_us;
	uint64 last_remote_t_grant_us;
	uint64 last_remote_t_install_us;
} ResourceXO1Stats;

StaticAssertDecl(sizeof(ResourceXO1Stats) == 72,
				 "ResourceXO1Stats must remain the exact nine-reading cohort");

/* Bounded, opt-in observation only. No field is an authority input. */
#define RESOURCE_X_TRACE_MAX_EVENTS UINT32_C(1048576)
#define RESOURCE_X_TRACE_IDLE 0
#define RESOURCE_X_TRACE_RECORDING 1
#define RESOURCE_X_TRACE_SEALED 2
typedef enum ResourceXTraceKind {
	RESOURCE_X_TRACE_SEND = 1,
	RESOURCE_X_TRACE_RECEIVE,
	RESOURCE_X_TRACE_APPLY,
	RESOURCE_X_TRACE_HEAD,
	RESOURCE_X_TRACE_RETIRE,
	RESOURCE_X_TRACE_PREUSE,
	RESOURCE_X_TRACE_UNLOCK,
	RESOURCE_X_TRACE_OPERATION_BEGIN,
	RESOURCE_X_TRACE_OPERATION_DONE,
	RESOURCE_X_TRACE_FOLLOWER_JOIN
} ResourceXTraceKind;

/* Local APPLY observations, not new wire/trace kinds. */
typedef enum ResourceXDeliveryTraceDetail {
	RESOURCE_X_DELIVERY_TRACE_CLAIM = 128,
	RESOURCE_X_DELIVERY_TRACE_COMPLETE,
	RESOURCE_X_DELIVERY_TRACE_CALLBACK
} ResourceXDeliveryTraceDetail;

typedef struct ResourceXTraceEvent {
	ResourceXAssertion assertion;
	uint64 time_us;
	uint64 formation;
	uint64 master_session;
	uint64 attempt;
	uint64 base_generation;
	uint64 final_generation;
	uint64 value[4];
	int32 pid;
	int32 node;
	int32 peer;
	uint16 kind;
	uint16 detail;
	uint32 flags;
	uint32 reserved;
} ResourceXTraceEvent;
StaticAssertDecl(sizeof(ResourceXTraceEvent) == 128, "observation record must remain 128 bytes");

typedef struct ResourceXTraceStats {
	BufferTag tag;
	uint32 state;
	uint64 epoch;
	uint64 count;
	uint64 overflow;
	uint64 late_events;
	uint64 exported;
	uint64 started_us;
	uint64 sealed_us;
	uint32 capacity;
	int32 owner_pid;
} ResourceXTraceStats;

extern bool cluster_pcm_lock_resource_x_trace_begin(const BufferTag *tag, uint64 epoch);
extern bool cluster_pcm_lock_resource_x_trace_seal(uint64 epoch);
extern bool cluster_pcm_lock_resource_x_trace_snapshot(ResourceXTraceStats *out);
extern uint32 cluster_pcm_lock_resource_x_trace_read(uint64 epoch, uint64 first,
													  ResourceXTraceEvent *out, uint32 limit);
extern bool cluster_pcm_lock_resource_x_trace_release(uint64 epoch);
extern void cluster_pcm_lock_resource_x_trace_note(const ResourceXTraceEvent *event);
extern void cluster_pcm_lock_resource_x_trace_ref(uint16 kind,
	const ResourceXAcquisitionRef *ref, uint64 value);
extern void cluster_pcm_lock_resource_x_trace_frame(uint16 kind,
	const ResourceXDecodedFrame *frame, int32 peer, int32 result);
extern void cluster_pcm_lock_resource_x_trace_wire(uint8 msg_type, int32 peer,
	const void *payload, uint32 length, int32 result);

/* Exact, process-local D2 result retained for the R8 sweep owner.  This is
 * deliberately not a wire structure and carries both the removed queue
 * identity and the successor selected under the same resource-entry lock. */
typedef struct ResourceXReclaimWitness {
	ResourceXAssertion assertion;
	uint64 base_authority_generation;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 enqueue_order;
	uint64 final_authority_generation;
	uint64 successor_assertion_sequence;
	uint64 successor_enqueue_order;
	int32 successor_node;
	uint32 result;
	uint8 previous_phase;
	uint8 successor_phase;
	uint8 was_head;
	uint8 source_evidence_preserved;
	uint32 reserved;
} ResourceXReclaimWitness;

StaticAssertDecl(sizeof(ResourceXReclaimWitness) == 104,
				 "ResourceXReclaimWitness layout must remain 104 bytes");

/* Master-local durable-storage proof.  It is never a wire layout: the GCS
 * storage producer constructs it only after its real durable-image check. */
typedef struct ResourceXDurableProof {
	ResourceXAssertion assertion;
	uint64 base_authority_generation;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 requester_target_generation;
	uint64 page_scn_lsn;
	uint32 page_checksum;
	uint32 source_proof_crc32c;
} ResourceXDurableProof;

StaticAssertDecl(sizeof(ResourceXDurableProof) == 80,
				 "ResourceXDurableProof layout must remain 80 bytes");

typedef struct ResourceXReconfigToken {
	uint64 old_formation;
	uint64 new_formation;
	uint64 freeze_generation;
	uint32 dead_requester_bitmap;
	uint32 reserved;
} ResourceXReconfigToken;

#define RESOURCE_X_RECONFIG_RECLAIM_WITNESS_MAX \
	(4 * RESOURCE_X_PROTOCOL_NODE_LIMIT)

typedef struct ResourceXReconfigBatch {
	uint32 examined_count;
	uint32 old_detached_count;
	uint32 successor_count;
	uint32 orphan_count;
	uint32 retry_count;
	uint32 sidecar_neutralized_count;
	uint32 complete_wrap;
	uint32 zero_residual;
	uint32 reclaim_count;
	uint32 reclaim_nonhead_count;
	uint32 reclaim_head_count;
	uint32 reclaim_orphan_count;
	uint64 next_state_index;
	uint64 residual_count;
	ResourceXReclaimWitness
		reclaim_witnesses[RESOURCE_X_RECONFIG_RECLAIM_WITNESS_MAX];
} ResourceXReconfigBatch;

typedef struct ResourceXReconfigStats {
	uint64 freeze_count;
	uint64 slot_examined_count;
	uint64 old_detached_count;
	uint64 successor_count;
	uint64 orphan_count;
	uint64 sidecar_neutralized_count;
	uint64 sidecar_stale_count;
	uint64 retry_count;
	uint64 blocked_count;
	uint64 thaw_count;
	uint64 reclaim_nonhead_count;
	uint64 reclaim_head_count;
	uint64 reclaim_orphan_count;
} ResourceXReconfigStats;

#define RESOURCE_X_COMPLETION_FINGERPRINT_BYTES 32

/* R8/R10 same-token cutover proofs are shared-memory records, never wire
 * layouts.  Oracle public material establishes the freeze/claim/unfreeze
 * phase shape; these exact fields are a PGRAC adaptation for proving that a
 * bounded scan crossed the final Resource-X mutation. */
typedef struct ResourceXZeroResidualProof {
	ResourceXReconfigToken token;
	uint64 proof_generation;
	uint64 scan_begin_cursor;
	uint64 scan_end_cursor;
	uint64 scan_capacity;
	uint64 scan_begin_slot_count;
	uint64 scan_end_slot_count;
	uint64 final_mutation_sequence;
	uint64 full_wrap_digest;
	uint64 empty_slot_count;
	uint64 successor_slot_count;
	uint64 terminal_slot_count;
	uint64 retry_revisit_count;
	uint8 creator_product_fingerprint[RESOURCE_X_COMPLETION_FINGERPRINT_BYTES];
	uint32 complete_wrap;
	uint32 zero_residual;
} ResourceXZeroResidualProof;

typedef struct ResourceXCleanCompletionProof {
	ResourceXReconfigToken token;
	uint64 proof_generation;
	uint64 final_mutation_sequence;
	uint64 scan_capacity;
	uint64 logical_slot_count;
	uint64 terminal_request_count;
	uint64 logical_digest;
	uint64 transport_mutation_sequence;
	uint64 transport_staged_count;
	uint8 creator_product_fingerprint[RESOURCE_X_COMPLETION_FINGERPRINT_BYTES];
	uint8 send_c1_manifest_fingerprint[RESOURCE_X_COMPLETION_FINGERPRINT_BYTES];
	uint32 logical_debt_zero;
	uint32 transport_debt_zero;
} ResourceXCleanCompletionProof;

StaticAssertDecl(sizeof(ResourceXReconfigToken) == 32,
				 "ResourceXReconfigToken layout must remain 32 bytes");
StaticAssertDecl(sizeof(ResourceXReconfigBatch) == 13376,
				 "ResourceXReconfigBatch layout must remain 13376 bytes");
StaticAssertDecl(sizeof(ResourceXReconfigStats) == 104,
				 "ResourceXReconfigStats layout must remain 104 bytes");
StaticAssertDecl(sizeof(ResourceXZeroResidualProof) == 168,
				 "ResourceXZeroResidualProof layout must remain 168 bytes");
StaticAssertDecl(sizeof(ResourceXCleanCompletionProof) == 168,
				 "ResourceXCleanCompletionProof layout must remain 168 bytes");

static inline bool
cluster_resource_x_next_freeze_generation(uint64 current, uint64 *out)
{
	if (out == NULL)
		return false;
	*out = 0;
	if (current >= UINT64_MAX - 1)
		return false;
	*out = current + 1;
	return true;
}


/*
 * Coherent, process-local view of one PCM authority entry.
 *
 * Every field is copied while the entry lock is held.  Callers may retain the
 * value as an optimistic token and later require an exact match before an
 * ownership handoff.  This is not a wire structure and adds no GrdEntry
 * fields.
 */
typedef struct PcmAuthoritySnapshot {
	ClusterGrdHolderId master_holder;
	uint64 transition_count;
	uint64 pending_x_since_lsn;
	PcmState state;
	int32 x_holder_node;
	uint32 s_holders_bitmap;
	int32 pending_x_requester_node;
	uint32 reserved[2];
} PcmAuthoritySnapshot;

StaticAssertDecl(sizeof(PcmAuthoritySnapshot) == 64,
				 "PcmAuthoritySnapshot process-local layout must remain 64 bytes");

typedef enum PcmXTransferCommitResult {
	PCM_X_TRANSFER_COMMIT_OK = 0,
	PCM_X_TRANSFER_COMMIT_NOT_FOUND,
	PCM_X_TRANSFER_COMMIT_STALE,
	PCM_X_TRANSFER_COMMIT_BAD_STATE
} PcmXTransferCommitResult;


typedef enum PcmPendingXReserveResult {
	PCM_PENDING_X_RESERVE_INVALID = -2,
	PCM_PENDING_X_RESERVE_NO_CAPACITY = -1,
	PCM_PENDING_X_RESERVE_OCCUPIED = 0,
	PCM_PENDING_X_RESERVE_OK = 1
} PcmPendingXReserveResult;

/* Process-local queue cookie stored in GrdEntry.pending_x_since_lsn.  Keep
 * the encoder shared by the PCM owner and coherent authority snapshots so a
 * same-node legacy mark or successor ticket can never satisfy an active
 * convert round. */
#define PCM_PENDING_X_QUEUE_KIND UINT64_C(0x8000000000000000)
#define PCM_PENDING_X_VALUE_MASK UINT64_C(0x7fffffffffffffff)

static inline bool
PcmPendingXQueueValue(uint64 ticket_id, uint64 *value_out)
{
	if (value_out != NULL)
		*value_out = 0;
	if (ticket_id == 0 || ticket_id > PCM_PENDING_X_VALUE_MASK || value_out == NULL)
		return false;
	*value_out = PCM_PENDING_X_QUEUE_KIND | ticket_id;
	return true;
}


/*
 * GUC cluster.pcm_grd_max_entries -- maximum number of GrdEntry slots
 *	in the cluster_pcm_grd shmem region.
 *
 *	Default -1: auto-resolve to NBuffers at startup.  Explicit 0 disables
 *	the PCM state machine.  Positive values must be >= NBuffers.  Range
 *	[-1, 1048576].  PGC_POSTMASTER (startup-fixed).
 */
extern int cluster_pcm_grd_max_entries;

/*
 * PGRAC: spec-2.31 D2 v0.5 — gated PCM activation predicate.
 *
 *	Forward-declare cluster_enabled / cluster_node_id (defined in
 *	cluster_guc.c) without including cluster_guc.h, to keep that
 *	dependency one-way.  cluster_conf_has_peers() is included because
 *	no-peer single-node mode must not enter PCM acquire/release: there
 *	is no remote holder to coordinate with, and partial acquire/release
 *	paths can otherwise turn a local buffer lock into spurious PCM work.
 *
 *	cluster_pcm_is_active() is the single gate entry point used by
 *	bufmgr LockBuffer / LockBufferForCleanup hot path (HC67).
 *
 *	Gate layers:
 *	  1. compile-time: USE_PGRAC_CLUSTER (this header is empty outside it)
 *	  2. cluster_enabled (Stage 1.11 GUC; PGC_POSTMASTER)
 *	  3. cluster_node_id >= 0 (single-node fallback uses -1 sentinel until
 *	     pgrac.conf loads;  during initdb / non-cluster bootstrap the
 *	     PCM API would FATAL on the -1 range check, so we must skip the
 *	     hook entirely until a node id is assigned)
 *	  4. cluster_conf_has_peers() (single-node fallback pays no PCM tax)
 *	  5. cluster_pcm_grd_max_entries != 0 (spec-1.7 disable path)
 *	  6. NOT inside the cold merged-recovery window (spec-4.5a G6): the
 *	     engage gate proved every peer DEAD (all-cold premise), so no
 *	     remote holder exists to coordinate with, the GRD this startup
 *	     just rebuilt is empty, and a dead peer can never answer a
 *	     master request; the startup process also has no backend
 *	     identity for the GCS data plane (MyBackendId = -1 FATALs).
 *	     WARM crash recovery on a peer-configured node stays on the
 *	     PCM path (and currently fail-closes -- roadmap 4.7 instance
 *	     recovery / CF freeze; see t/243 L4 scope note).
 *
 *	Inline expansion yields 5 global reads + predictable branch (default
 *	cluster_enabled=true + max_entries=-1 → NBuffers + node_id set during
 *	postmaster startup before any LockBuffer is reachable, and the merged
 *	window flag is false outside startup redo, so branch predictor 99%+
 *	taken in the steady state).
 */
extern bool cluster_enabled;
extern int cluster_node_id;
extern bool cluster_recmerge_window_active; /* cluster_recovery_merge.c */

static inline bool
cluster_pcm_is_active(void)
{
	return cluster_enabled && cluster_node_id >= 0 && cluster_conf_has_peers()
		   && cluster_pcm_grd_max_entries != 0 && !cluster_recmerge_window_active;
}


/*
 * PGRAC: spec-2.31 D5 v0.4 — apply PCM ownership fields to BufferDesc.
 *
 *	Defined here (not in cluster_buffer_desc.h) because we depend on
 *	PcmLockMode which is owned by this header;  cluster_buffer_desc.h
 *	already provides BUF_TYPE_SCUR / BUF_TYPE_XCUR / PCM_STATE_* via the
 *	#include above, and this avoids the circular header dependency that
 *	would result from cluster_buffer_desc.h declaring the helper itself.
 *
 *	Called by bufmgr LockBuffer hook on the success path only (HC66):
 *	  - cluster_pcm_lock_acquire succeeded, AND
 *	  - LWLockAcquire(content_lock) succeeded (no ereport)
 *	→ then this helper updates the caller-supplied buffer_type pointer
 *	  (monotone hint of last PCM ownership mode) and pcm_state pointer
 *	  (real-time mirror of PCM master state).
 */
static inline void
cluster_buffer_desc_apply_pcm_ownership_fields(uint8 *out_buffer_type, uint8 *out_pcm_state,
											   PcmLockMode mode)
{
	*out_buffer_type = (mode == PCM_LOCK_MODE_S) ? (uint8)BUF_TYPE_SCUR : (uint8)BUF_TYPE_XCUR;
	*out_pcm_state = (mode == PCM_LOCK_MODE_S) ? (uint8)PCM_STATE_S : (uint8)PCM_STATE_X;
}


/*
 * PCM lock mutation API.
 *
 *	spec-2.30 activates these as C-internal APIs.  They do not have
 *	SQL-callable bindings; pg_proc.dat and system_views.sql are untouched
 *	for this API surface.
 *
 *	Inject points wrap each function entry (Q6 user 修订 2026-05-02
 *	release-pre instead of release-post for naming honesty -- 1.7
 *	stub never reaches a 'post' point because it ereports immediately):
 *	  cluster_pcm_lock_acquire   -> "cluster-pcm-acquire-entry"
 *	  cluster_pcm_lock_release   -> "cluster-pcm-release-pre"
 *	  cluster_pcm_lock_upgrade   -> "cluster-pcm-convert-pre"
 *	  cluster_pcm_lock_downgrade -> "cluster-pcm-downgrade-pre"
 */
extern void cluster_pcm_lock_acquire(BufferTag tag, PcmLockMode mode);

/*
 * PGRAC: spec-2.33 D7 — BufferDesc-aware variant.  Used by bufmgr LockBuffer
 * so that the GCS data-plane sender can install received block bytes into
 * the caller's shared buffer (HC84) without requiring callers to thread
 * BufferDesc through the tag-only path.  Falls back to the same local path
 * as cluster_pcm_lock_acquire when master == self.
 *
 * For S/X with a remote master, this entry point invokes
 * cluster_gcs_send_block_request_and_wait (spec-2.33 D3).  Tag-only
 * cluster_pcm_lock_acquire fails closed in that case with an errhint
 * directing callers here.
 *
 * Returns true if a DURABLE PCM grant was recorded (caller mirrors ownership
 * into buf->pcm_state).  Returns false for a spec-5.2 D2 one-shot READ_IMAGE:
 * bytes were installed for this read only and LockBuffer marks a transient
 * READ_IMAGE content bracket after aborting the exact grant reservation;
 * unlock clears it to N so the next access re-fetches.  A false return with
 * *out_retry_denied set is instead a queue-arbitration retry boundary; the
 * caller must exact-abort and replace its GRANT_PENDING reservation.
 */
extern bool cluster_pcm_lock_acquire_buffer(BufferDesc *buf, PcmLockMode mode,
											bool *out_retry_denied);

/*
 * PGRAC: spec-5.2a D2 — clean-page X-transfer arm (backend-local, one-shot).
 *
 *	cluster_pcm_clean_page_xfer_arm(true) marks the NEXT cluster PCM X acquire
 *	(cluster_pcm_lock_acquire_buffer) as a clean-page transfer.  The acquire
 *	path calls cluster_pcm_clean_page_xfer_consume() exactly once, which
 *	read-and-clears the flag, so the eligibility can NEVER leak into a
 *	subsequent (heap) buffer access (inv ①/⑤, R3).  The caller MUST have
 *	proven the page is clean — no active ITL / MVCC state — before arming
 *	(sequence refill knows this by relkind, spec-5.2a D5); the GCS holder
 *	re-verifies independently (spec-5.2a D4).  is_armed() is a non-destructive
 *	peek used by tests / assertions.
 */
extern void cluster_pcm_clean_page_xfer_arm(bool armed);
extern bool cluster_pcm_clean_page_xfer_is_armed(void);
extern bool cluster_pcm_clean_page_xfer_consume(void);

/*
 * PGRAC: spec-2.35 D5 (HC111 + HC112) — bufmgr release hook bifurcation.
 *
 *	spec-2.31 D7 had a single cluster_pcm_lock_release_buffer() invoked
 *	from bufmgr LockBuffer's UNLOCK path.  That conflated two semantically
 *	distinct events:
 *	  (a) "content lock released" — the in-process content_lock SHARED or
 *	      EXCLUSIVE LWLock just dropped, but the buffer is still resident
 *	      in shared_buffers and may still serve other backends as a SCUR
 *	      cache holder (relevant for CF 2-way read sharing per spec-2.35).
 *	  (b) "cache residency lost" — the buffer is being evicted from
 *	      shared_buffers (InvalidateBuffer / InvalidateVictimBuffer /
 *	      DropRelations*Buffers / DropDatabaseBuffers), so the master's
 *	      s_holders_bitmap bit and master_holder lifecycle must clear.
 *
 *	HC111 redefines s_holders_bitmap as "cache residency" semantics.  HC112
 *	requires bufmgr to call (a) on content-lock unlock and (b) only on
 *	actual eviction.
 *
 *	cluster_pcm_lock_unlock_content_buffer(buf, mode):
 *	  - SCUR: no-op (cache residency preserved; bit stays set so the
 *	    master can still forward GCS_BLOCK_REQUEST to this node)
 *	  - XCUR: delegates to cluster_pcm_lock_release_buffer_for_eviction
 *	    (X is single-holder; releasing the X content lock also drops the
 *	    cache claim, matching spec-2.31 D7 prior semantics for X)
 *	  - N: no-op
 *
 *	cluster_pcm_lock_release_buffer_for_eviction(buf, mode):
 *	  - Performs the bit-clearing + master_holder lifecycle update.
 *	  - mode is taken from BufferDesc.pcm_state at eviction time so the
 *	    caller does not need to remember which mode was last held.
 */
extern void cluster_pcm_lock_unlock_content_buffer(BufferDesc *buf, PcmLockMode mode);
extern void cluster_pcm_lock_release_saved_tag_for_eviction(BufferTag tag, PcmLockMode mode);
extern void cluster_pcm_lock_release_buffer_for_eviction(BufferDesc *buf, PcmLockMode mode);
/* PGRAC: spec-5.2 D11 / P0-26 — commit a local-master X transfer only if the
 * full remote-X authority sampled before the wire round is unchanged.  The
 * exact source identity/generation/session closes late-reply and concurrent
 * queue-handoff races; STALE is a fresh-request retry boundary. */
extern PcmXTransferCommitResult cluster_pcm_lock_master_take_x_after_transfer(
	BufferTag tag, const PcmAuthoritySnapshot *expected, XLogRecPtr page_lsn, SCN page_scn,
	int32 holder_node, uint32 requester_procno, uint64 request_id, uint64 epoch);
/* PGRAC: spec-5.2 D11 path B — master==holder==self ships its X image to a
 * remote requester and records the requester as the new X holder (single-phase
 * writer-transfer-revoke; caller drops self's copy no-wire before calling).
 * S3 forensics step 1b: request_id/epoch ride along for advance provenance. */
extern void cluster_pcm_lock_master_grant_x_to(BufferTag tag, int32 requester_node,
											   XLogRecPtr page_lsn, SCN page_scn, uint64 request_id,
											   uint64 epoch);

/*
 * PGRAC: spec-2.35 D3 (HC110) — master_holder lookup for forward routing.
 *
 *	master-side GCS_BLOCK_REQUEST handler invokes this after
 *	cluster_pcm_lock_query(tag) returns S to decide whether the request
 *	can be forwarded to an authorized holder.  Returns -1 if no GrdEntry
 *	exists for the tag, or if master_holder is in the cleared sentinel
 *	state.  Otherwise returns the holder's cluster.node_id (0..31).
 */
extern int32 cluster_pcm_master_holder_node_by_tag(BufferTag tag);

extern void cluster_pcm_lock_release(BufferTag tag);
extern void cluster_pcm_lock_upgrade(BufferTag tag);
extern void cluster_pcm_lock_downgrade(BufferTag tag, PcmLockMode target_mode, bool keep_pi);


/*
 * Diagnostic / introspection helpers (always-callable).
 *
 *	cluster_pcm_lock_query: returns the live local PCM state for the tag,
 *	  or PCM_LOCK_MODE_N if no GRD entry exists.
 *
 *	cluster_pcm_grd_count: returns the live HTAB entry count.
 *
 *	cluster_pcm_grd_shmem_size: returns 0 if GUC=0, else shmem for the
 *	  header plus the resolved entry capacity.
 *
 *	cluster_pcm_grd_init: shmem registry init_fn callback.  Explicit
 *	  disable path (GUC=0) returns before ShmemInitStruct; otherwise it
 *	  initializes the header, HTAB, HTAB lock, and per-entry locks lazily.
 */
extern PcmLockMode cluster_pcm_lock_query(BufferTag tag);
extern int cluster_pcm_grd_capacity(void);
extern bool cluster_pcm_lock_authority_snapshot(BufferTag tag, PcmAuthoritySnapshot *out);
extern bool cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *authority_out,
										uint64 *master_authority_generation_out,
										SCN *expected_page_scn_out);
extern bool cluster_pcm_lock_authority_matches(BufferTag tag, const PcmAuthoritySnapshot *expected);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_t1_grant_exact(const ResourceXAcquisitionRef *ref);
/* Temporary R10 production adapter handoff.  The caller must already have
 * selected the exact PCM-X master ticket, then pass a complete legacy
 * authority snapshot taken without either domain lock. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_adapter_base_bind_exact(
	const ResourceXAssertion *assertion, uint64 formation,
	uint64 base_authority_generation,
	const PcmAuthoritySnapshot *legacy_authority);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	uint64 ticket_id, uint64 formation,
	uint64 base_authority_generation,
	const PcmAuthoritySnapshot *legacy_authority);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_adapter_successor_base_exact(
	const ResourceXAssertion *assertion, uint64 formation,
	const PcmAuthoritySnapshot *legacy_authority,
	uint64 *base_authority_generation_out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_assert_exact(
	const ResourceXDecodedFrame *assertion, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out);
/* PGRAC adaptation: kind-9 is a non-authority pre-ASSERT receipt.  The
 * caller passes independently authenticated/current connection, R4, and
 * master-session values; the exact ASSERT consumes the receipt under the
 * same resource entry lock that creates the canonical master request. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_request_exact(
	const ResourceXDecodedFrame *request, int32 authenticated_source_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 master_sender_connection_generation,
	ResourceXDecodedFrame *ack_out);
extern bool cluster_pcm_lock_resource_x_s_barrier_active(
	const BufferTag *tag);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
	const ResourceXDecodedFrame *assertion, int32 authenticated_source_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 current_master_sender_connection_generation,
	ResourceXMasterSnapshot *out);
/* Process-local membership in one node head.  This is not authority and is
 * never copied to another caller.  A failed witness cannot be reset by a
 * background installation, shared binding clear, or a later caller's head. */
typedef struct ResourceXCallerWitness {
	ResourceXDecodedCommon joined_request;
	uint64 entry_binding_generation;
	uint64 r4_record_generation;
	uint64 failed_attempt;
	int32 master_node;
	uint32 master_ingress_connection_generation;
	uint64 reobserve_attempt;
	PcmRxRequesterWaitReason reobserve_reason;
} ResourceXCallerWitness;

/* Caller-owned auxiliary handle requalification. This is a local observation
 * cursor, not a retained grant, a pin or a wire identity. Keep it across a
 * failed repin so descriptor replacement cannot erase caller failure history
 * or restart the logical acquisition's diagnostic age. */
typedef struct ResourceXAuxiliaryAcquireContext {
	BufferTag resource;
	ResourceXCallerWitness caller;
	PcmRxRequesterWaitState wait_state;
	uint64 r4_record_generation;
	uint64 absolute_deadline_us;
	uint64 diagnostic_request_sequence;
	uint64 reobserve_count;
	bool active;
	bool reobserve;
} ResourceXAuxiliaryAcquireContext;

/* Local residency, not a wire identity or permission to modify page bytes. */
typedef struct ResourceXDeliveryTarget {
	uint64 generation;
	uint64 reservation_token;
	uint32 buffer_id_plus_one;
	uint32 flags;
} ResourceXDeliveryTarget;

#define RESOURCE_X_DELIVERY_NORMAL UINT8_C(1)
#define RESOURCE_X_DELIVERY_CLEANUP UINT8_C(2)
typedef struct ResourceXDeliveryClaim {
	ResourceXInstallClaimJoinObservation observation;
	ResourceXDeliveryTarget target;
	uint64 executor_sequence;
	uint8 purpose;
	uint8 reserved[7];
} ResourceXDeliveryClaim;

extern ResourceXApplyResult
cluster_pcm_lock_resource_x_t1_grant_delivery_exact(const ResourceXAcquisitionRef *ref,
													const ResourceXDeliveryClaim *claim);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_apply_delivery_exact(const ResourceXAcquisitionRef *ref,
														   const ResourceXBufferInstallProof *proof,
														   const ResourceXDeliveryClaim *claim);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
	const ResourceXAcquisitionRef *ref, const ResourceXBufferActivationProof *proof,
	const ResourceXDeliveryClaim *claim);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_install_claim_bind_t1_delivery_exact(
	const ResourceXAcquisitionRef *ref, const struct ClusterPcmOwnSnapshot *reserved,
	const ResourceXDeliveryClaim *claim);

extern ResourceXApplyResult cluster_pcm_lock_resource_x_delivery_claim_begin_exact(
	const ResourceXAcquisitionRef *ref, const ResourceXDeliveryTarget *target, uint8 purpose,
	uint64 now_us, ResourceXDeliveryClaim *out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_claim_end_exact(const ResourceXDeliveryClaim *claim);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_dispatch_exact(const ResourceXDeliveryClaim *claim,
													ResourceXDecodedFrame *dispatch_out);
extern ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_delivery_exact(
	const ResourceXDecodedFrame *ack, int32 authenticated_master_node,
	uint32 authenticated_ingress_connection_generation, uint64 r4_record_generation, uint64 now_us,
	const ResourceXDeliveryClaim *claim, ResourceXDecodedFrame *assertion_out);
/* Call only after exact post-T3 BufferDesc hold release, before ending claim. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_complete_exact(const ResourceXDeliveryClaim *claim,
													const struct ClusterPcmOwnSnapshot *terminal_x);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_join_delivery_exact(
	const ResourceXDecodedFrame *frame, int32 authenticated_source_node,
	uint32 authenticated_source_ingress_generation, int32 current_master_node,
	uint32 current_master_ingress_generation, uint64 r4_record_generation,
	const ResourceXDeliveryClaim *claim, ResourceXRequesterJoinSnapshot *out);

extern ResourceXApplyResult cluster_pcm_lock_resource_x_delivery_bind_target_exact(
	const ResourceXInstallClaimJoinObservation *observation, int buffer_id,
	const struct ClusterPcmOwnSnapshot *before, const struct ClusterPcmOwnSnapshot *after);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_delivery_dispatch_observe_exact(
	const ResourceXDecodedFrame *dispatch, int32 master_node,
	ResourceXInstallClaimJoinObservation *observation_out, ResourceXDeliveryTarget *target_out,
	uint64 *direct_generation_out, uint64 *direct_token_out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_delivery_target_snapshot_exact(
	const ResourceXAcquisitionRef *ref, ResourceXInstallClaimJoinObservation *observation_out,
	ResourceXDeliveryTarget *target_out);

/* DUPLICATE discards a proved retired observer; it is not current authority. */
extern ResourceXApplyResult cluster_pcm_lock_resource_x_caller_observe_exact(
	const ResourceXAssertion *assertion, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation, ResourceXCallerWitness *caller);

/* Serialized observed driver; WAIT itself never disposes of terminal cover. */
extern ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_step_observed_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool allow_create, bool allow_remote_admission,
	bool cached_local_x, uint64 cached_ownership_generation, ResourceXCallerWitness *caller,
	const struct ClusterPcmOwnSnapshot *observed, ResourceXDecodedFrame *dispatch_out,
	ResourceXAcquisitionRef *terminal_ref_out);

extern ResourceXBootstrapRoundAction cluster_pcm_lock_resource_x_bootstrap_round_step_caller_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool allow_create, bool allow_remote_admission,
	bool cached_local_x, uint64 cached_ownership_generation, ResourceXCallerWitness *caller,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out);

extern ResourceXBootstrapRoundAction cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, bool cached_local_x, uint64 cached_ownership_generation,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out);
extern ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool cached_local_x, uint64 cached_ownership_generation,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_join_budget_exact(
	const ResourceXAssertion *assertion,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	uint64 now_us,
	uint64 *r4_record_generation_out,
	uint64 *absolute_deadline_us_out);
extern ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool cached_local_x, uint64 cached_ownership_generation,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out);
extern ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
	const ResourceXDecodedFrame *ack, int32 authenticated_master_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 now_us,
	ResourceXDecodedFrame *assertion_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
	const ResourceXDecodedFrame *request, int32 expected_master_node,
	uint64 expected_r4_record_generation,
	uint32 expected_master_ingress_connection_generation,
	uint64 expected_retry_slice_us, uint64 expected_absolute_deadline_us,
	uint64 expected_direct_init_ownership_generation,
	uint64 expected_direct_init_reservation_token);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 caller_absolute_deadline_us, long timeout_ms);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_bootstrap_round_wait_direct_init_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, uint64 caller_absolute_deadline_us, long timeout_ms);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_bootstrap_round_wait_caller_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 caller_absolute_deadline_us, long timeout_ms,
	ResourceXCallerWitness *caller);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_predecessor_wait_exact(
	const BufferTag *tag, int32 current_master_node, uint64 current_master_session,
	uint64 current_formation, uint64 expected_carrier_generation,
	uint64 caller_absolute_deadline_us, uint64 requested_sleep_slice_us);
struct ClusterPcmOwnSnapshot;
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	const struct ClusterPcmOwnSnapshot *observed);
/* An ordinary TARGET follower may observe the exact R9 executor's closed
 * N-reservation through T2-before-T3 interval.  This predicate grants no
 * authority: it only joins that BufferDesc observation to the same
 * ASSERT-dispatched requester round and active T1/T2 acquisition so the
 * caller may wait and re-probe instead of treating the closed fence as
 * corruption. */
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us,
	const struct ClusterPcmOwnSnapshot *observed);
extern ResourceXTargetInstallFollowState
cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 caller_absolute_deadline_us,
	const struct ClusterPcmOwnSnapshot *observed,
	ResourceXTargetInstallContinuation *continuation_out);
extern ResourceXTargetInstallFollowState
cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
	const ResourceXTargetInstallContinuation *continuation,
	const struct ClusterPcmOwnSnapshot *observed,
	ResourceXAcquisitionRef *terminal_ref_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
	const ResourceXTargetInstallContinuation *continuation,
	ResourceXTargetInstallFollowState expected_follow_state,
	long timeout_ms);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
	const ResourceXAcquisitionRef *ref, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	uint64 terminal_authority_generation, uint64 now_us);
/* TARGET cached-X eviction lifecycle.  PREPARE claims the sole entry-local
 * EVICTING owner and freezes kind-4 while the BufferDesc is still exact
 * X+REVOKING.  ABORT is pre-mutation only; COMMIT is called only after local
 * apply or reliable transport admission and clears cover+owner together. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_prepare_exact(
	const BufferTag *tag, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	uint64 reservation_token, uint32 sender_connection_generation,
	int32 owner_procno, ResourceXDecodedFrame *release_out,
	ResourceXLocalOwnerHandle *handle_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_abort_exact(
	const ResourceXLocalOwnerHandle *handle);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_commit_exact(
	const ResourceXDecodedFrame *release, int32 current_master_node,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	const ResourceXLocalOwnerHandle *handle);
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
	const ResourceXAcquisitionRef *ref,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token);
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation,
	ResourceXTerminalXLineage *lineage_out);
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation,
	ResourceXTerminalXLineage *lineage_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
	const ResourceXAcquisitionRef *ref,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 reservation_token,
	int32 owner_procno, uint64 now_us,
	ResourceXLocalOwnerHandle *handle_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_finish_exact(
	const ResourceXLocalOwnerHandle *handle, uint64 now_us);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(
	const ResourceXLocalOwnerHandle *handle);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 reservation_token,
	int32 owner_procno, uint64 now_us,
	ResourceXTerminalXLineage *lineage_out,
	ResourceXLocalOwnerHandle *handle_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 now_us,
	ResourceXTerminalXLineage *lineage_out);
extern bool
cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation,
	const ResourceXLocalOwnerHandle *handle,
	ResourceXTerminalXLineage *lineage_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
	const ResourceXLocalOwnerHandle *handle, uint64 now_us);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
	const ResourceXLocalOwnerHandle *handle);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_source_finish_defer_exact(
	const ResourceXDecodedFrame *block, int32 master_node,
	const struct ClusterPcmOwnSnapshot *revoking, const ResourceXLocalOwnerHandle *owner,
	int buffer_id);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_source_finish_claim_exact(
	const ResourceXAcquisitionRef *successor, int32 owner_procno, ResourceXSourceFinishClaim *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
	const ResourceXDecodedFrame *successor_block, int32 authenticated_master_node,
	uint64 r4_record_generation, const struct ClusterPcmOwnSnapshot *revoking,
	const struct ClusterPcmOwnSnapshot *dropped, const ResourceXLocalOwnerHandle *handle);
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
	const ResourceXAcquisitionRef *ref,
	uint64 *direct_init_ownership_generation_out,
	uint64 *direct_init_reservation_token_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation, uint64 retry_slice_us,
	ResourceXBootstrapRoundFailureSnapshot *out);
extern bool
cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
	const ResourceXAcquisitionRef *ref, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation, uint64 retry_slice_us,
	const struct ClusterPcmOwnSnapshot *observed);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_x_to_s_preflight_exact(
	const struct ClusterPcmOwnSnapshot *current);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_note_x_to_s_exact(
	const struct ClusterPcmOwnSnapshot *revoking,
	const struct ClusterPcmOwnSnapshot *shared);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_block_to_n_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const struct ClusterPcmOwnSnapshot *revoking,
	const ResourceXLocalOwnerHandle *owner);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_prepared_x_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const struct ClusterPcmOwnSnapshot *revoking,
	const ResourceXLocalOwnerHandle *owner);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const struct ClusterPcmOwnSnapshot *prepared_source,
	XLogRecPtr prepared_page_lsn, uint64 prepared_page_scn,
	uint32 prepared_page_checksum);
/* Retention is PENDING and transport-invisible.  Publish the indivisible pair
 * only after the caller completed the exact physical source revoke.  The
 * query returns NOT_FOUND when only an exact older same-domain attempt is
 * retained; replacement remains gated by its separate DRAIN contract. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_publish_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session);
/* Replay only a published, undrained immutable pair. Occupied physical slots
 * are validated and left untouched; an empty pair is rearmed atomically. */
extern ResourceXApplyResult cluster_pcm_lock_resource_x_holder_pair_replay_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence, int32 authenticated_master_node,
	uint64 authenticated_master_session);
/* Non-authoritative former-source retry classifier.  It binds one retained
 * physical generation to the exact still-undrained PENDING/PUBLISHED pair
 * under the resource entry lock; callers must separately revalidate
 * BufferDesc. */
extern bool
cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
	const BufferTag *tag, int32 current_master_node,
	uint64 current_master_session, uint64 current_formation,
	uint64 retained_generation);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_holder_status_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_holder_image_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *out);
/* Classify one old type-17 as superseded only when the current retained
 * status+image pair is atomically exact and has a strictly newer sequence. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session);
/* Validate and retire only the exact type-18/type-15 retained carrier pair
 * selected by one authenticated post-settlement DRAIN.  The prepare/commit
 * split keeps the GRD entry lock out of the BufferContent critical section. */
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 *source_generation_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 source_generation);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_blocked_to_n_exact(
	const ResourceXDecodedFrame *blocked, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_local_proof_exact(
	const ResourceXDecodedFrame *local_proof, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_durable_proof_exact(
	const ResourceXDurableProof *durable_proof, ResourceXMasterSnapshot *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_master_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXMasterSnapshot *out);
extern bool cluster_pcm_lock_resource_x_s_barrier_active_exact(
	const BufferTag *tag);
/* Requester-node projection of an in-flight bootstrap round.  This is a
 * retry-only local S-admission barrier; terminal cached X remains eligible
 * for the normal BufferDesc covering-grant path. */
extern bool
cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
	const BufferTag *tag);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_current_x_successor_exact(
	const BufferTag *tag, int32 holder_node, bool *preserve_current_x_out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_authority_grant_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_join_exact(
	const ResourceXDecodedFrame *frame, int32 authenticated_source_node,
	ResourceXRequesterJoinSnapshot *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_join_current_exact(
	const ResourceXDecodedFrame *frame, int32 authenticated_source_node,
	uint32 authenticated_source_ingress_generation, int32 current_master_node,
	uint32 current_master_ingress_generation, uint64 r4_record_generation,
	ResourceXRequesterJoinSnapshot *out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_join_frames_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *grant_out,
	ResourceXDecodedFrame *image_out, ResourceXRequesterJoinSnapshot *out);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_install_settlement_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity);

/* Process-local freshness evidence for one SourceSettlement ingress.  The
 * plan is never serialized or retained and grants no Resource-X authority. */
typedef enum ResourceXSourceSettlementCoverAction {
	RESOURCE_X_SETTLEMENT_COVER_NO_COVER = 0,
	RESOURCE_X_SETTLEMENT_COVER_CLOSE_EXACT_X = 1
} ResourceXSourceSettlementCoverAction;

typedef struct ResourceXSourceSettlementPlan {
	ResourceXAssertion assertion;
	ResourceXAcquisitionRef terminal_ref;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 pair_base_authority_generation;
	uint64 pair_image_authority_generation;
	uint64 settlement_authority_generation;
	uint64 source_generation;
	uint64 carrier_generation;
	uint64 terminal_cached_generation;
	uint64 terminal_authority_generation;
	uint64 terminal_r4_record_generation;
	uint32 status_semantic_crc32c;
	uint32 image_semantic_crc32c;
	int32 authenticated_master_node;
	uint8 source_mode;
	uint8 cover_action;
	uint8 authority_with_image;
	uint8 reserved;
	bool valid;
} ResourceXSourceSettlementPlan;

extern ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	ResourceXSourceSettlementPlan *plan_out);

/* Stack-only D1 observation of the entry-lock state that accepted or refused
 * the post-BufferDesc half of SourceSettlementV2.  It is neither wire nor
 * shared authority and must never be retained as protocol state. */
typedef enum ResourceXSourceSettlementCommitStage {
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_NONE = 0,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_ENTRY_LOOKUP,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_MASTER_STATE,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_DRAIN_DOMAIN,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_DECODE,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_IDENTITY,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_STATE,
	RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER
} ResourceXSourceSettlementCommitStage;

#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_PAIR_BYTES UINT32_C(0x00000001)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_GATE_FORMATION UINT32_C(0x00000002)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_PAIR_DESTINATION UINT32_C(0x00000004)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_SOURCE_GENERATION UINT32_C(0x00000008)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_ACTION UINT32_C(0x00000010)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_REQUESTER UINT32_C(0x00000020)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_MASTER UINT32_C(0x00000040)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_FORMATION UINT32_C(0x00000080)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_SESSION UINT32_C(0x00000100)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_AUTHORITY UINT32_C(0x00000200)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNERSHIP UINT32_C(0x00000400)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER UINT32_C(0x00000800)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNER_INVALID UINT32_C(0x00001000)
#define RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNER_NONEMPTY UINT32_C(0x00002000)

typedef struct ResourceXSourceSettlementCommitObservation {
	uint64 current_resource_formation;
	uint64 current_master_session;
	uint64 current_terminal_authority_generation;
	uint64 current_cached_ownership_generation;
	uint64 current_pair_resource_formation;
	uint64 current_pair_master_session;
	uint64 current_pair_assertion_sequence;
	uint64 current_pair_source_generation;
	uint64 current_pair_carrier_generation;
	uint32 mismatch_mask;
	int32 current_master_node;
	uint32 current_pair_destination_node;
	uint8 current_round_phase;
	uint8 current_owner_state;
	uint8 current_holder_status_valid;
	uint8 current_holder_image_valid;
	uint8 current_status_intent_state;
	uint8 current_image_intent_state;
	uint8 commit_stage;
	uint8 current_pair_observed_mode;
	uint8 current_cover_action;
	uint8 current_terminal_cover_present;
} ResourceXSourceSettlementCommitObservation;

StaticAssertDecl(sizeof(ResourceXSourceSettlementCommitObservation) == 96,
	"ResourceXSourceSettlementCommitObservation layout must remain 96 bytes");

extern ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_prepare_observed_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	ResourceXSourceSettlementPlan *plan_out,
	ResourceXSourceSettlementCommitObservation *observation_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_commit_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	const ResourceXSourceSettlementPlan *plan,
	ResourceXSourceSettlementCommitObservation *observation_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_ack_build_exact(
	const ResourceXDecodedFrame *settlement,
	uint32 sender_connection_generation, ResourceXDecodedFrame *ack_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_ack_exact(
	const ResourceXDecodedFrame *ack, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_settled_retire_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	const ResourceXMasterSnapshot *settled);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_release_x_exact(
	const ResourceXDecodedFrame *release, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out);
extern ResourceXReclaimResult cluster_pcm_lock_resource_x_reclaim_requester_exact(
	const BufferTag *tag, int32 dead_node, uint64 dead_formation,
	ResourceXReclaimWitness *out);
extern bool cluster_pcm_lock_resource_x_intent_arm_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentBodyHandle *body,
	uint64 logical_generation, uint64 authority_generation,
	uint64 now_us, uint32 destination_node, uint16 payload_bytes,
	ResourceXWireKind kind);
extern ResourceXIntentResult cluster_pcm_lock_resource_x_intent_not_admitted_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected,
	uint64 now_us);
extern ResourceXIntentResult cluster_pcm_lock_resource_x_intent_stage_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected,
	uint64 now_us);
extern ResourceXIntentResult cluster_pcm_lock_resource_x_intent_hard_rearm_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected,
	uint64 now_us);
extern bool cluster_pcm_lock_resource_x_intent_complete_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
	const ResourceXAssertion *assertion, int32 holder_node,
	ResourceXIntentSlot *slot_out, void *payload_out,
	uint16 payload_capacity);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity);
extern ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(
	const ResourceXIntentSlot *expected, uint64 now_us);
extern ResourceXIntentResult cluster_pcm_lock_resource_x_grant_intent_stage_exact(
	const ResourceXIntentSlot *expected, uint64 now_us);
extern ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
	const ResourceXIntentSlot *expected, uint64 now_us);
extern bool cluster_pcm_lock_resource_x_grant_intent_complete_exact(
	const ResourceXIntentSlot *expected);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(
	const ResourceXIntentSlot *expected, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity);
extern ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
	const ResourceXIntentSlot *expected, uint64 now_us);
extern ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
	const ResourceXIntentSlot *expected, uint64 now_us);
extern ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
	const ResourceXIntentSlot *expected, uint64 now_us);
extern bool cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
	const ResourceXIntentSlot *expected);
extern ResourceXIntentProbeResult
cluster_pcm_lock_resource_x_ready_intent_probe_exact(const BufferTag *tag, uint32 *owner_cursor,
													 ResourceXIntentSlot *slot_out);
extern ResourceXIntentProbeResult cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
	uint32 probe_budget, ResourceXIntentSlot *slot_out, void *payload_out, uint16 payload_capacity,
	uint32 *examined_out);
extern ResourceXIntentProbeResult cluster_pcm_lock_resource_x_outbound_work_probe_exact(
	uint32 probe_budget, ResourceXIntentSlot *slot_out, void *payload_out, uint16 payload_capacity,
	uint32 *examined_out, ResourceXAcquisitionRef *delivery_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_gate_bind_formation_exact(uint64 formation);
extern bool cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
	ResourceXGateSnapshot *snapshot_out);
extern bool cluster_pcm_lock_resource_x_gate_snapshot(
	ResourceXGateSnapshot *snapshot_out);
extern ResourceXApplyResult
cluster_pcm_lock_resource_x_gate_fail_closed_exact(
	const ResourceXGateSnapshot *expected);
extern bool cluster_pcm_lock_resource_x_gate_open_exact(uint64 formation);
extern bool cluster_pcm_lock_resource_x_executor_enter(
	const ResourceXAcquisitionRef *ref, ResourceXActivationGateToken *out_gate);
extern void
cluster_pcm_lock_resource_x_executor_leave(ResourceXActivationGateToken *gate);
extern uint64 cluster_pcm_lock_resource_x_activation_inflight_count(void);
extern bool cluster_resource_x_reconfig_freeze_pending(uint64 old_formation,
											ResourceXReconfigToken *out);
extern bool cluster_resource_x_reconfig_freeze_pending_exact(
	uint64 old_formation, uint32 dead_requester_bitmap,
	ResourceXReconfigToken *out);
extern bool cluster_resource_x_reconfig_bind_new_formation_exact(
	ResourceXReconfigToken *token, uint64 new_formation);
extern bool cluster_resource_x_reconfig_freeze(uint64 old_formation, uint64 new_formation,
											   ResourceXReconfigToken *out);
extern bool cluster_resource_x_reconfig_freeze_exact(
	uint64 old_formation, uint64 new_formation, uint32 dead_requester_bitmap,
	ResourceXReconfigToken *out);
/* Source-removal cutover enters the R8 owner without supplying a formation
 * value.  The Resource-X gate retains the exact current value and allocates
 * its own checked successor after the pending full sweep. */
extern bool cluster_resource_x_reconfig_cutover_begin_native_exact(
	ResourceXReconfigToken *out);
extern bool cluster_resource_x_reconfig_cutover_bind_native_successor_exact(
	ResourceXReconfigToken *token);
extern ResourceXReconfigResult cluster_resource_x_reconfig_sweep(
	const ResourceXReconfigToken *token, uint32 probe_budget, ResourceXReconfigBatch *out);
extern bool cluster_resource_x_reconfig_zero_proof_exact(
	const ResourceXReconfigToken *token, ResourceXZeroResidualProof *out);
extern bool cluster_pcm_lock_resource_x_clean_completion_prove_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *zero_proof,
	ResourceXCleanCompletionProof *out);
extern bool cluster_pcm_lock_resource_x_clean_completion_proof_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *zero_proof,
	ResourceXCleanCompletionProof *out);
/* Read-only R11 prerequisite view of the current frozen R8/R10 pair.  The
 * owning proof validators remain authoritative; this accessor only returns
 * exact copies after both live checks succeed for one token. */
extern bool cluster_pcm_lock_resource_x_cutover_proofs_exact(
	ResourceXReconfigToken *token_out,
	ResourceXZeroResidualProof *zero_proof_out,
	ResourceXCleanCompletionProof *clean_proof_out);
/* Read-only post-thaw view of the same retained pair.  The frozen accessor
 * above deliberately remains false after thaw. */
extern bool cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
	ResourceXReconfigToken *token_out,
	ResourceXZeroResidualProof *zero_proof_out,
	ResourceXCleanCompletionProof *clean_proof_out);
/* Read the exact current R8/R10 pair and a local digest without accepting an
 * external formation coordinate.  The returned token remains the only
 * Resource-X formation identity. */
extern bool cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
	bool thawed, ResourceXReconfigToken *token_out, uint64 *digest_out);
extern bool cluster_resource_x_reconfig_thaw_exact(const ResourceXReconfigToken *token);
extern void cluster_resource_x_reconfig_stats_snapshot(ResourceXReconfigStats *out);
extern ResourceXExecutorProbeResult cluster_pcm_lock_resource_x_executor_probe_exact(
	const ResourceXAcquisitionRef *ref, ResourceXExecutorSnapshot *out_snapshot);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_executor_wait_exact(
	const ResourceXAcquisitionRef *ref, long timeout_ms);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_executor_rearm_exact(
	const ResourceXAcquisitionRef *ref);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_apply_exact(
	const ResourceXAcquisitionRef *ref, const ResourceXBufferInstallProof *proof);
extern ResourceXApplyResult cluster_pcm_lock_resource_x_requester_activate_exact(
	const ResourceXAcquisitionRef *ref, const ResourceXBufferActivationProof *proof);
extern void cluster_pcm_lock_resource_x_o1_stats_snapshot(ResourceXO1Stats *out);
extern void cluster_pcm_lock_resource_x_publish_no_progress_exact(
	const ResourceXAcquisitionRef *ref, ResourceXNoProgressReason reason);
extern int cluster_pcm_grd_count(void);
extern void cluster_pcm_grd_get_summary(int *n_count, int *s_count, int *x_count,
										int *pi_holders_total, int *convert_queue_active);
extern Size cluster_pcm_grd_shmem_size(void);
extern void cluster_pcm_grd_init(void);


/* ============================================================
 * PGRAC: spec-2.30 D2 — transition validator + apply (file-private struct).
 *
 *	`struct GrdEntry` definition lives in cluster_pcm_lock.c (file-private
 *	per spec-2.30 §2.1 + spec-1.7 Q3 opaque ABI 严守);  callers MUST go
 *	through these accessors, never deref `GrdEntry *` directly.
 *
 *	cluster_pcm_transition_legal(from, to, trans):  returns true iff
 *	  (from, to, trans) combination matches AD-002 9-transition map.
 *	  Caller invokes before apply; illegal combination MUST
 *	  ereport(ERROR, ERRCODE_DATA_CORRUPTED) at caller side (HC56).
 *
 *	cluster_pcm_transition_apply(entry, trans, holder_node_id):  caller
 *	  MUST hold entry->entry_lock EXCLUSIVE (HC57 Asserted);  applies
 *	  master_state / holder bitmap mutation + bumps counters.  Trans-9
 *	  fail-closed ereport(ERRCODE_FEATURE_NOT_SUPPORTED) (HC60).
 * ============================================================ */
extern bool cluster_pcm_transition_legal(PcmState from, PcmState to, PcmLockTransition trans);
extern void cluster_pcm_transition_apply(struct GrdEntry *entry, PcmLockTransition trans,
										 int holder_node_id);
extern PcmGcsTransitionApplyResult
cluster_pcm_lock_apply_gcs_transition_result(BufferTag tag, PcmLockTransition trans,
											 int holder_node_id);
extern bool cluster_pcm_lock_apply_gcs_transition(BufferTag tag, PcmLockTransition trans,
												  int holder_node_id);


/* ============================================================
 * PGRAC: spec-2.30 D2 — 9 transition counter accessors.
 *
 *	Counters live in shmem header (ClusterPcmShared);  every backend sees
 *	cluster-wide values.  Trans-9 (S→X cleanout) counter永 0 in spec-2.30
 *	(HC60 apply-fail-closed) until Stage 3 AD-006 第五轮 wires ITL cleanout.
 *	When cluster.pcm_grd_max_entries=0 (disable path) accessors return 0.
 * ============================================================ */
extern uint64 cluster_pcm_get_trans_n_to_s_count(void);
extern uint64 cluster_pcm_get_trans_n_to_x_count(void);
extern uint64 cluster_pcm_get_trans_s_to_x_upgrade_count(void);
extern uint64 cluster_pcm_get_trans_x_to_s_downgrade_count(void);
/* PGRAC: spec-6.14a D2 — (b) fail-closed leg counter. */
extern uint64 cluster_pcm_get_local_s_revoke_nonholder_failclosed_count(void);
extern uint64 cluster_pcm_get_evict_release_deferred_aux_count(void);
/* PGRAC ownership-generation wave: cached-X writer re-verify observability. */
extern void cluster_pcm_note_writer_cover_stale_detected(void);
extern void cluster_pcm_note_writer_reverify_reacquire(void);
extern uint64 cluster_pcm_get_writer_cover_stale_detected_count(void);
extern uint64 cluster_pcm_get_writer_reverify_reacquire_count(void);
extern void cluster_pcm_note_restore_aba_detected(void);
extern uint64 cluster_pcm_get_restore_aba_detected_count(void);
extern void cluster_pcm_note_invalidate_parked_grant_pending(void);
extern uint64 cluster_pcm_get_invalidate_parked_grant_pending_count(void);
/* S3 forensics step 1b — advance-provenance table insert drops (table full). */
extern uint64 cluster_pcm_get_wm_prov_insert_fail_count(void);
extern uint64 cluster_pcm_get_trans_x_to_n_downgrade_count(void);
extern uint64 cluster_pcm_get_trans_x_to_n_release_count(void);
extern uint64 cluster_pcm_get_trans_s_to_n_invalidate_count(void);
extern uint64 cluster_pcm_get_trans_s_to_n_release_count(void);
extern uint64 cluster_pcm_get_trans_s_to_x_cleanout_count(void);


/*
 * Module-level shmem registration entry point.
 *
 *	Called from cluster_init_shmem_module() (cluster_shmem.c) to
 *	register the cluster_pcm_grd region with the spec-1.3 shmem
 *	registry.  Idempotent (registry checks for duplicate names).
 *
 *	spec-2.30 registers the region with size_fn = cluster_pcm_grd_shmem_size
 *	and init_fn = cluster_pcm_grd_init.  The HTAB lock and per-entry locks
 *	are initialized inside the region.
 */
extern void cluster_pcm_lock_module_init(void);

/* ============================================================
 * PGRAC: spec-2.36 D5 HC117 + HC124 — S barrier helpers.
 *
 *   cluster_pcm_lock_set_pending_x — record that an X request is
 *     in flight at this master for `tag`;  N→S handlers short-
 *     circuit with DENIED_PENDING_X while set.
 *   cluster_pcm_lock_clear_pending_x — clear the field after X
 *     grant install ack OR reconfig epoch advance.
 *   cluster_pcm_lock_query_pending_x_requester — read for N→S
 *     decision (returns -1 = none).
 *   cluster_pcm_lock_clear_pending_x_for_node (HC124) — LMON
 *     node-dead sweep;  scans all GrdEntry and clears any
 *     pending_x_requester_node matching the dead node.  Must
 *     be idempotent under concurrent X grant clear races.
 * ============================================================ */
/* Legacy request-scoped producer.  OCCUPIED means a live barrier is already
 * present; callers must retry/deny without overwriting it. */
extern PcmPendingXReserveResult cluster_pcm_lock_set_pending_x(BufferTag tag, int32 requester_node,
															   uint64 current_lsn);
/* Queue-head reservation: claim only an idle barrier.  Even the same node is
 * occupied without separate ticket-exact queue-engine proof. */
extern PcmPendingXReserveResult
cluster_pcm_lock_try_reserve_pending_x(BufferTag tag, int32 requester_node, uint64 ticket_id);
extern bool cluster_pcm_lock_queue_pending_x_exact(BufferTag tag, int32 requester_node,
												   uint64 ticket_id);
extern bool cluster_pcm_lock_clear_queue_pending_x_exact(BufferTag tag, int32 requester_node,
														 uint64 ticket_id);
extern void cluster_pcm_lock_clear_pending_x(BufferTag tag);
/* Identity-safe compare-and-clear: clears only while the mark still names
 * expected_requester (request-scoped clears MUST use this form; round-2
 * additional hardening).  Returns true when this call cleared it. */
extern bool cluster_pcm_lock_clear_pending_x_if(BufferTag tag, int32 expected_requester);
extern int32 cluster_pcm_lock_query_pending_x_requester(BufferTag tag);
extern uint64 cluster_pcm_lock_clear_pending_x_for_node(int32 dead_node);

/* PGRAC: spec-4.6a BUG-C2 — failure-path PCM holder cleanup.  Removes a
 * DEAD node from X/S/PI holder records in this master's PCM directory and
 * demotes entries to N when no live holder remains.  Idempotent under repeated
 * dead-sweep ticks. */
extern uint64 cluster_pcm_lock_cleanup_on_node_dead(int32 dead_node);

/* PGRAC: spec-5.13 D5 (clean-leave PCM release) — a leaving node clears its
 * OWN holder records (X / S / PI) from the local PCM directory after the GCS
 * flush seam has persisted all dirty X blocks (CL-I5).  Demotes an entry's
 * master_state to N when no X/S holder remains.  Mirrors clear_pending_x_for_
 * node's locking.  Returns the count of entries mutated. */
extern uint64 cluster_pcm_lock_clean_leave_release_all_self(uint64 leave_epoch);

/* PGRAC: spec-5.13 D5 (CL-I2 proof) — read-only scan: returns true iff no local
 * PCM entry still records the leaving node as X holder or in the S / PI holder
 * bitmaps.  Used by cluster_clean_leave_verify_no_leftover after drain. */
extern bool cluster_pcm_lock_clean_leave_verify_no_leftover(int32 leaving_node);

/* PGRAC: spec-2.36 D2/D3 — master broadcast invalidate needs raw bitmap
 * read.  Returns 0 if entry not present (treated as "no holders"). */
extern uint32 cluster_pcm_lock_query_s_holders_bitmap(BufferTag tag);

/* PGRAC: spec-4.7a D3 — strict master-side check "is `node` already a
 * recorded holder of `tag` whose grant covers `trans`?"  Enables idempotent
 * re-acknowledge of a holder's re-request instead of DENIED_MASTER_NOT_HOLDER
 * (which loops to 53R90).  S→X never qualifies (real writer path).  Missing
 * entry / uncertainty → false → caller fails closed. */
extern bool cluster_pcm_master_requester_is_holder(BufferTag tag, int32 node,
												   PcmLockTransition trans);

/* PGRAC: spec-4.7a D4 — does a node OTHER than `sender` hold `tag` (X or S)
 * and is still LIVE?  The GCS block master bounded-fail-closes an X request
 * before any state mutation when this is true (writer transfer deferred).
 * DEAD holders excluded (warm-recovery path).  See cluster_pcm_lock.c. */
extern bool cluster_pcm_master_other_live_holder_exists(BufferTag tag, int32 sender);

/* PGRAC: spec-4.7 D2/D3 — master-side rebuild of the minimal block-resource
 * view from one survivor re-declare.  Primitive args (not the wire payload)
 * to avoid a cluster_gcs_block.h ↔ cluster_pcm_lock.h include cycle; the
 * GCS_BLOCK_REDECLARE handler extracts the fields and calls this.  D2 records
 * the holder + max PI watermark;  D3 adds conflict detection (two X
 * declarers = protocol anomaly → fail-closed) + the not-double-X invariant.
 * source_node must equal the declared holder_node_id (the sender). */
extern bool cluster_gcs_block_master_rebuild_from_redeclare(BufferTag tag, uint8 held_mode,
															XLogRecPtr page_lsn, SCN page_scn,
															int32 source_node,
															uint64 cluster_epoch);

/*
 * cluster_pcm_mode_covers — spec-4.7a D2.
 *
 *	Does a node already holding PCM mode `have` cover a new request for `want`?
 *	X ⊇ {S, X}; S ⊇ {S}; N covers nothing.  The bufmgr acquire gate uses this
 *	to skip a remote master round-trip when the node still holds a sufficient
 *	mode (hold-until-revoked).  Safe ONLY because buf->pcm_state is cleared by
 *	the INVALIDATE handler and the eviction/drop paths, so a non-N value here
 *	means this node still genuinely holds the lock (Rule 8.A — no stale grant).
 */
static inline bool
cluster_pcm_mode_covers(PcmLockMode have, PcmLockMode want)
{
	if (have == PCM_LOCK_MODE_X)
		return true; /* X holder can read (S) and write (X) */
	if (have == PCM_LOCK_MODE_S)
		return want == PCM_LOCK_MODE_S; /* S holder can read only */
	return false;						/* N (or anything else) covers nothing */
}

/* ============================================================
 * PGRAC: spec-2.37 D2/D7/D8/D9 HC125-HC130 — PI watermark API.
 *   spec-2.41 D2 §2.8.1: split into a DUAL API (no unitless name).
 *
 *   pi_watermark_lsn_advance / _lsn_query:  the per-stream replay-position
 *     (XLogRecPtr) watermark.  Consumed ONLY by the spec-4.7 D5 redo-coverage
 *     serve-gate (cluster_gcs_block_redo_lsn_covered -> required_lsn).
 *     Monotone — never regress.  Fed by holder page_lsn on downgrade/transfer.
 *   pi_watermark_scn_advance / _scn_query:  the cross-node version (SCN,
 *     AD-008) watermark.  Consumed ONLY by the lost-write detector (§2.6).
 *     Fed by the local-page sources (take_x/grant_x) today; the ack/redeclare
 *     wire sources feed it once D3 carries pd_block_scn on the wire (D3-PENDING).
 *   The two are ORTHOGONAL: detector never reads lsn, serve-gate never reads
 *     scn.  retire/reset clears BOTH.
 *   pi_watermark_retire_for_tag:        single-tag retire (test fixture).
 *   pi_watermark_retire_for_relation_fork:  D8 — relation drop / relfilenode
 *     change sweep (db, relNumber, fork) range.
 *   pi_watermark_retire_for_truncate_range: D8 — relation truncate sweep
 *     all entries whose tag.blockNum >= new_nblocks within (db, relNumber,
 *     fork).
 *   pi_watermark_retire_if_durable:     D9 HC130 part 2 — checkpointer/smgr
 *     sync-complete path only.  Helper立 for unit test + future use;
 *     production callsite defer to spec-2.38/Stage3 (PG has no per-block
 *     durable-complete hook today).
 *
 *   HC130: retire is FORBIDDEN by epoch advance (reconfig is the scenario
 *   most likely to involve stale sources;  clearing watermark there would
 *   weaken detection).  Only tag lifecycle + durable-confirm retire.
 * ============================================================ */
#include "storage/relfilelocator.h" /* RelFileNumber */
#include "common/relpath.h"			/* ForkNumber */
/*
 * PGRAC: spec-2.41 D2 §2.8.1 — DUAL watermark API (no unitless name).
 *	*_lsn_*  serves ONLY the spec-4.7 D5 redo-coverage serve-gate (per-stream
 *	         replay position).  *_scn_*  serves ONLY the lost-write detector
 *	         (cross-node version, AD-008 SCN).  Callers must pick the unit
 *	         deliberately; the old unitless cluster_pcm_lock_pi_watermark_query
 *	         / _advance are removed so a reader can never mix the two.
 */
extern void cluster_pcm_lock_pi_watermark_lsn_advance(BufferTag tag, XLogRecPtr page_lsn);
extern XLogRecPtr cluster_pcm_lock_pi_watermark_lsn_query(BufferTag tag);

/*
 * S3 forensics step 1a/1b — SCN-watermark advance PROVENANCE.
 *
 *	The lost-write detector's expected_scn is only as trustworthy as the
 *	last advance that produced it: a late / wrong-generation invalidate-ACK
 *	feeding the monotone max would fabricate lost-write verdicts against
 *	perfectly current pages.  Every SCN-watermark feed site that ACTUALLY
 *	advances the monotone max records {source, sender, request_id, epoch,
 *	old->new} into a per-tag shmem table (step 1b: one insert-once slot per
 *	tag holding the LAST advancing feed — non-advancing late feeds never
 *	enter, so the record always explains the CURRENT watermark; the step-1a
 *	ring recycled under load and could return an unrelated late feed).  The
 *	53R93 emit sites on the MASTER (the only node whose table is
 *	authoritative for its tags) attach the record so a shipped<expected
 *	verdict can be qualified as branch-3 (watermark false-positive) without
 *	a rerun.
 */
typedef enum ClusterPcmWmSrc {
	CLUSTER_PCM_WM_SRC_NONE = 0,	 /* no advance recorded for the tag */
	CLUSTER_PCM_WM_SRC_REDECLARE,	 /* survivor re-declare wire (rebuild) */
	CLUSTER_PCM_WM_SRC_TAKE_X,		 /* local-master take-X after holder transfer */
	CLUSTER_PCM_WM_SRC_GRANT_X,		 /* master grant-X ship to remote requester */
	CLUSTER_PCM_WM_SRC_ACK_SLOTLESS, /* invalidate-ACK slotless e2 fan-out feed */
	CLUSTER_PCM_WM_SRC_ACK_SLOT,	 /* invalidate-ACK slot-claimed blocking feed */
} ClusterPcmWmSrc;

typedef struct ClusterPcmWmProv {
	ClusterPcmWmSrc source; /* SRC_NONE = no advance recorded for the tag */
	int32 sender_node;		/* wire sender / requester; -1 = local/unknown */
	uint64 request_id;		/* wire request id; 0 = none */
	uint64 epoch;			/* wire epoch; 0 = none */
	SCN old_scn;			/* watermark before the advancing feed */
	SCN new_scn;			/* fed page_scn == watermark right after the feed */
	bool table_full;		/* only when source==SRC_NONE: the table has dropped
						* at least one insert, so absence is INCONCLUSIVE
						* (the tag's advance may have gone unrecorded) */
} ClusterPcmWmProv;

extern const char *cluster_pcm_wm_src_text(ClusterPcmWmSrc src);
extern void cluster_pcm_lock_pi_watermark_scn_advance(BufferTag tag, SCN page_scn,
													  ClusterPcmWmSrc source, int32 sender_node,
													  uint64 request_id, uint64 epoch);
extern bool cluster_pcm_lock_pi_watermark_prov_query(BufferTag tag, ClusterPcmWmProv *out);
extern SCN cluster_pcm_lock_pi_watermark_scn_query(BufferTag tag);
extern void cluster_pcm_lock_pi_watermark_retire_for_tag(BufferTag tag);
extern uint64 cluster_pcm_lock_pi_watermark_retire_for_relation_fork(Oid db_oid,
																	 RelFileNumber rel_number,
																	 ForkNumber fork_num);
extern uint64 cluster_pcm_lock_pi_watermark_retire_for_truncate_range(Oid db_oid,
																	  RelFileNumber rel_number,
																	  ForkNumber fork_num,
																	  BlockNumber new_nblocks);
extern bool cluster_pcm_lock_pi_watermark_retire_if_durable(BufferTag tag,
															XLogRecPtr written_page_lsn);

/* ============================================================
 * PGRAC: spec-6.12h D-h2 — PI-holder discard protocol (master side).
 *
 *   cluster_pcm_pi_discard_covered:  the PURE coverage judge.  A durable
 *     write of the block's CURRENT copy proves every Past Image obsolete
 *     iff the written page's pd_block_scn reaches the SCN watermark (the
 *     newest shipped version): every PI is that version or older.  The
 *     SCN unit (AD-008 Lamport pd_block_scn, the lost-write detector
 *     unit) is the ONLY cross-node comparable unit — per-thread WAL
 *     (spec-4.1) gives every node its own LSN space, so the LSN
 *     watermark (fed from the SHIPPING holder's stream) and a durable
 *     note (from the WRITING holder's stream) are generally from
 *     DIFFERENT streams and numerically incomparable; the judge
 *     deliberately never reads the LSN unit.  SCN-unarmed (a
 *     recovery-rebuilt LSN-only entry) or SCN-unknown written page ->
 *     false, fail-safe: the PI merely lingers until buffer pressure /
 *     implicit-discard reread.
 *
 *   cluster_pcm_lock_pi_holder_note:  set `holder_node`'s bit in the
 *     authoritative entry's pi_holders_bitmap — a conversion site kept a
 *     real BUF_TYPE_PI buffer (D-h1) and reported it.  Advisory (missing
 *     entry -> no-op): an untracked PI only misses the discard notify.
 *
 *   cluster_pcm_lock_pi_discard_collect:  the D-h2 production retire.
 *     Under the entry lock: if the written pd_block_scn covers the SCN
 *     watermark, clear BOTH watermarks (a durable copy >= the newest
 *     shipped version also discharges the redo-coverage claim — the same
 *     "durable >= watermark" contract as retire_if_durable) + the PI
 *     holder bitmap, and hand the pre-clear bitmap to the caller, which
 *     owns notifying each PI holder (PI_DISCARD ride on the INVALIDATE
 *     wire).  This is the durable-confirm retire HC130 anticipated;
 *     retire_if_durable stays as the LSN-only single-stream fixture.
 * ============================================================ */
static inline bool
cluster_pcm_pi_discard_covered(SCN wm_scn, SCN written_scn)
{
	if (!SCN_VALID(wm_scn))
		return false; /* SCN unit unarmed -> nothing provable cross-node */
	if (!SCN_VALID(written_scn))
		return false; /* written version unknown -> fail-safe keep */
	/* SCN_CMP_OK: scn_time_cmp order via scn_local (raw compare would be
	 * node_id-dominated — same rule as the grant/take watermark advance). */
	return scn_local(written_scn) >= scn_local(wm_scn);
}

extern void cluster_pcm_lock_pi_holder_note(BufferTag tag, int32 holder_node);
extern bool cluster_pcm_lock_pi_discard_collect(BufferTag tag, SCN written_scn,
												uint32 *holders_out);

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PCM_LOCK_H */
