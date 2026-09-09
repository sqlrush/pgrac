/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block.c
 *	  pgrac cluster GCS block-shipping substrate (Cache Fusion data plane).
 *
 *	  spec-2.33 activates cross-node 8KB block shipping on top of the
 *	  spec-2.32 GCS control plane.  Implements:
 *	    - cluster_gcs_send_block_request_and_wait sender (BufferDesc-aware)
 *	    - Master-side handler: HC82 XLogFlush(page_lsn) BEFORE shipping bytes,
 *	      HC88 master-not-holder decisions, HC89 single-retry revalidation
 *	    - Sender-side handler: HC83 CRC32C verify, HC84 PageSetLSN install
 *	    - Per-backend outstanding-block-request table (LWLock protected)
 *	    - 8 block-plane observability counters
 *
 *	  Wire ABI definitions live in cluster_gcs_block.h (HC79/HC80).
 *	  Master lookup remains in cluster_gcs.c (shared with control plane);
 *	  this module focuses on the data-plane request/reply cycle.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_gcs_block.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.33-gcs-block-shipping-substrate.md (FROZEN v0.4)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#ifdef USE_PGRAC_CLUSTER

#include "access/multixact.h" /* MultiXactIdIsValid (spec-7.1 D3-b fetch) */
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_clean_leave.h" /* spec-5.13 S6 — CL-I5 serve gate */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h" /* spec-4.6 D4 — dead-master block-path guard */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_cr_server.h" /* spec-6.12b CR-server park/fetch */
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_lms_shard.h" /* PGRAC: spec-7.3 D4 — tag->worker shard */
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_gcs_reqid.h"		 /* PGRAC: spec-6.14a D1 — id domains */
#include "cluster/cluster_gcs_block_dedup.h" /* spec-2.34 D1 — counter forward */
#include "cluster/cluster_grd.h"			 /* spec-4.6 D4 — block_path_failclosed counter */
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_membership.h"		 /* spec-5.16 D3b — is_member master-side gate */
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_qvotec.h"			 /* spec-5.16 D3b — in_quorum master-side gate */
#include "cluster/cluster_reconfig.h"		 /* QVOTEC-observed live peer incarnation */
#include "cluster/cluster_recovery_merge.h"	 /* spec-4.7 D5 — recovered_through redo gate */
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_thread_recovery.h" /* spec-4.11 scope gate for online replay */
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_xnode_profile.h"	 /* spec-5.59 D2/D3/D4 profiling buckets */
#include "cluster/cluster_xnode_lever.h"	 /* spec-6.12a — downgrade counters */
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_itl.h" /* spec-5.2 D11 — active-ITL writer-transfer guard */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h" /* PGRAC: spec-7.3 D5 — my DATA channel = worker id */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_pcm_own.h" /* S3 forensics — ownership gen in 53R93 errdetail */
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "cluster/cluster_shmem.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_touched_peers.h" /* spec-5.14 D2 class 2 */
#include "cluster/cluster_write_fence.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/backendid.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/latch.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h" /* PGRAC: spec-6.12h D-h2 — PI-discard note ring lock */
#include "portability/instr_time.h"
#include "utils/elog.h"
#include "utils/pg_crc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/* ============================================================
 * Shared-memory layout.
 *
 *	Per-backend block-outstanding table mirrors spec-2.32 cluster_gcs.c
 *	layout but uses a separate shmem region + LWLock tranche so that
 *	observability can distinguish data-plane contention from control-plane
 *	contention.  HC80 reply routing uses the compound key
 *	(requester_backend_id, request_id) so master replies to the right
 *	backend slot without scanning all backends.
 * ============================================================ */

#define MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND CLUSTER_GCS_BLOCK_MAX_OUTSTANDING_PER_BACKEND

/* PGRAC: spec-6.12h D-h2 — PI-discard write-note ring capacity.  Sized for
 * one checkpoint cycle of tracked-block writes between LMON drains; overflow
 * drops the new note (fail-safe: the PI merely lingers, counted). */
#define CLUSTER_GCS_PI_NOTE_RING_SIZE 128

/* D4 keeps the legacy acquisition reply table and the R4 synchronous-CR
 * reply table logically disjoint even though their physical 8240-byte frames
 * share one C decoder.  The byte value is stored in existing slot padding;
 * no shared-memory size or subsequent field offset changes. */
typedef enum ClusterGcsBlockReplyDomain {
	CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE = 0,
	CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR = 1,
	CLUSTER_GCS_BLOCK_REPLY_DOMAIN_CURRENT_MX = 2
} ClusterGcsBlockReplyDomain;

/* M4 Candidate-2 remote origin work is process-local and bounded.  The
 * existing LMS DATA worker 0 drains synchronous phase transitions in one
 * turn and stops at the first genuinely pending phase; no shared slot,
 * actor, worker, queue or durable object is introduced.
 *
 * Stage-8 PRE admits C=32 on each of the three remote members.  A burst can
 * therefore place 96 authenticated exact fallbacks on one origin before the
 * event loop drains them.  The old four-entry implementation silently
 * consumed request 5 and later without a status-22 reply.  Keep a bounded
 * power-of-two process-local envelope large enough for that frozen burst;
 * every request retains its existing absolute reply deadline and all
 * freshness/fail-closed checks. */
#define GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS 128
#define GCS_BLOCK_R4_TX_ORIGIN_STEP_BUDGET 6
#define GCS_BLOCK_R4_TX_REQUIRED_HELLO_CAPS                                      \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1                                 \
	 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1                                         \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                            \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)

typedef enum GcsBlockR4TxOriginPhase {
	GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_BEGIN = 1,
	GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_POLL,
	GCS_BLOCK_R4_TX_ORIGIN_DATA_FREEZE,
	GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_BEGIN,
	GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_POLL,
	GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN,
	GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_POLL,
	GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE,
	GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_BEGIN,
	GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_POLL,
	GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN,
	GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_POLL,
	GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK,
	GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN,
	GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_POLL,
	GCS_BLOCK_R4_TX_ORIGIN_SEND
} GcsBlockR4TxOriginPhase;

typedef enum GcsBlockR4TxOriginDomain {
	GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE = 1,
	GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX = 2
} GcsBlockR4TxOriginDomain;

typedef enum GcsBlockCurrentMxOriginFailure {
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_NONE = 0,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_CONTEXT_FULL,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ADMISSION,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_LOCATOR,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ROOT,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SCUR_ACQUIRE,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SAMPLE,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SCUR_RELEASE,
	GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SEND_FRESHNESS
} GcsBlockCurrentMxOriginFailure;

typedef struct GcsBlockR4TxOriginContext {
	bool in_use;
	bool guard_active;
	GcsBlockR4TxOriginDomain domain;
	GcsBlockR4TxOriginPhase phase;
	GcsBlockR4TxOriginPhase failure_phase;
	ClusterUndoBlock0Result current_failure;
	ClusterTxResolveMode resolve_mode;
	TimestampTz deadline;
	uint32 requester_capability_generation;
	ClusterCtrcParticipantIdentity current_mx_requester_identity;
	ClusterR4CrForwardPayload forward;
	ClusterTxLocator locator;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0LogicalKey tt_logical;
	ClusterUndoBlock0ResolvedRoot tt_root;
	ClusterUndoBlock0ResolvedRoot recheck_root;
	ClusterUndoBlock0Generation expected_generation;
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard;
	ClusterRuntimeVisibilityOriginPlan origin_plan;
	ClusterCurrentMxProofForwardV2 current_mx_request;
	ClusterTTSlotPhysicalLocator current_mx_locators[
		CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentMemberProof current_mx_proofs[
		CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCtrcTxnKeyV1 current_mx_ctrc_keys[
		CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterTTStatusKey current_mx_sampled_keys[
		CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentUpdaterProof current_mx_updater_proof;
	ClusterMxResolveResult current_mx_result;
	GcsBlockCurrentMxOriginFailure current_mx_failure;
	TransactionId current_mx_failure_xid;
	uint16 current_mx_failure_index;
	bool current_mx_current_owner_found;
	ClusterTTDurableLocate current_mx_durable_locate_result;
	uint16 current_mx_proof_count;
	uint16 current_mx_locator_count;
	uint8 current_mx_sampled_bitmap;
	bool canonical_sampled;
	ClusterTxResolution resolution;
	ClusterTxOutcome outcome;
	ClusterTxResolveReason reason;
	uint8 reply_frame[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
} GcsBlockR4TxOriginContext;

static GcsBlockR4TxOriginContext
	gcs_block_r4_tx_origin_contexts[GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS];
static bool gcs_block_r4_tx_origin_first_denial_logged = false;
static bool gcs_block_current_mx_origin_first_unknown_logged = false;

/* Native Resource-X success diagnostics are proof breadcrumbs, not protocol
 * authority.  Synchronously emitting one LOG record for every frame can
 * starve the DATA/LMON workers that must complete the same 3-second round.
 * Keep one process-local timestamp per diagnostic/result class and retain a
 * short periodic sample.  Failure results bypass this sampler at call sites. */
#define GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_INTERVAL_US UINT64_C(250000)
#define GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_CLASS_COUNT 16

typedef enum GcsBlockResourceXDiagnostic {
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_REQUEST = 0,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_ACK,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_SOURCE_SETTLEMENT_ACK,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_SOURCE_SETTLEMENT_ACK_INGRESS,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_INSTALL_SETTLEMENT,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_ASSERT_X,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_BLOCK_TO_N,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_BLOCKED_TO_N,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_IMAGE_ENVELOPE,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_AUTHORITY_GRANT,
	GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_COUNT
} GcsBlockResourceXDiagnostic;

static uint64
	gcs_block_resource_x_diagnostic_next_log_at
		[GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_COUNT]
		[GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_CLASS_COUNT];

typedef struct ClusterGcsBlockOutstandingSlot {
	bool in_use;
	uint8 reply_domain;
	uint64 request_id;
	uint8 transition_id;
	BufferTag tag;
	int32 master_node;
	bool reply_received;
	GcsBlockReplyHeader reply_header;
	char reply_block_data[GCS_BLOCK_DATA_SIZE];
	bool reply_sf_dep_valid;
	uint8 reply_sf_flags;
	ClusterSfDepVec reply_sf_dep_vec;
	/* PGRAC: spec-6.12i D-i1 — authority trailer parsed off an
	 * UNDO_TT_FETCH_RESULT reply (epoch / live_hwm ride the header). */
	bool reply_undo_trailer_valid;
	uint64 reply_undo_tt_generation;
	uint64 reply_undo_authority_scn; /* PGRAC: spec-7.1a D3 (trailer SCN) */
	ConditionVariable reply_cv;
	/* PGRAC: spec-2.34 D3/D4 — HC100 stale-reply defense + epoch invalidation.
	 *  request_epoch:        snapshot of cluster_epoch at the time the
	 *                        current attempt was sent;  reply handler
	 *                        validates hdr->epoch >= request_epoch.
	 *  expected_master_node: master node the sender currently routes to;
	 *                        reply handler validates hdr->sender_node
	 *                        matches (defends against a stale reply from
	 *                        a previous master after reshuffle).
	 *  stale:                set by cluster_gcs_block_on_epoch_advance()
	 *                        when slot.request_epoch < new_epoch.  Sender
	 *                        observes on CV wake and falls through to
	 *                        retransmit path (re-lookup_master + retry). */
	uint64 request_epoch;
	int32 expected_master_node;
	uint8 expected_reply_status;
	bool expected_current_mx_key_valid;
	ClusterCurrentMxKey expected_current_mx_key;
	bool expected_current_mx_proof_valid;
	ClusterCurrentMxProofForwardV2 expected_current_mx_proof;
	bool stale;
	uint32 direct_generation;
	ClusterGcsBlockDirectState direct_state;
	int32 direct_expected_peer;
	uint32 direct_arm_id;
	ClusterGcsBlockDirectTargetKind direct_target_kind;
	BufferDesc *direct_target_buf;
	void *direct_target_addr;
	uint32 direct_target_lkey;
	bool direct_target_prepared;
	ClusterGcsBlockDirectAbortReason direct_abort_reason;
} ClusterGcsBlockOutstandingSlot;

StaticAssertDecl(CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE == 0
					 && CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR == 1
					 && CLUSTER_GCS_BLOCK_REPLY_DOMAIN_CURRENT_MX == 2,
				 "GCS block reply domain must remain the closed 0/1/2 set");
StaticAssertDecl(offsetof(ClusterGcsBlockOutstandingSlot, reply_domain) == 1,
				 "GCS block reply domain must consume byte-1 slot padding");
StaticAssertDecl(offsetof(ClusterGcsBlockOutstandingSlot, request_id) == 8,
				 "GCS block slot request_id offset must remain 8");
StaticAssertDecl(sizeof(ClusterGcsBlockOutstandingSlot) == 8688,
				 "GCS block outstanding slot ABI must remain 8688 bytes");

typedef struct ClusterGcsBlockBackendBlock {
	LWLockPadded lock;
	ClusterGcsBlockOutstandingSlot slots[MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND];
	uint64 next_request_id;
} ClusterGcsBlockBackendBlock;

static void
gcs_block_slot_clear_current_mx_expectation(
	ClusterGcsBlockOutstandingSlot *slot)
{
	slot->expected_reply_status = 0;
	slot->expected_current_mx_key_valid = false;
	memset(&slot->expected_current_mx_key, 0,
		   sizeof(slot->expected_current_mx_key));
	slot->expected_current_mx_proof_valid = false;
	memset(&slot->expected_current_mx_proof, 0,
		   sizeof(slot->expected_current_mx_proof));
}

static ClusterGcsBlockBackendBlock *gcs_block_backend_blocks = NULL;

/*
 * Count only live requester slots whose closed domain is R4_CR.  LMON uses
 * this read-only census after admission and transport close; an unavailable
 * table is deliberately nonzero so missing startup state cannot prove zero.
 */
uint64
cluster_gcs_block_r4_requester_count(void)
{
	uint64 count = 0;
	int backend_id;

	if (gcs_block_backend_blocks == NULL || MaxBackends <= 0)
		return UINT64_MAX;

	for (backend_id = 0; backend_id < MaxBackends; backend_id++) {
		ClusterGcsBlockBackendBlock *blk
			= &gcs_block_backend_blocks[backend_id];
		int slot_id;

		LWLockAcquire(&blk->lock.lock, LW_SHARED);
		for (slot_id = 0;
			 slot_id < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND;
			 slot_id++) {
			const ClusterGcsBlockOutstandingSlot *slot = &blk->slots[slot_id];

			if (slot->in_use
				&& slot->reply_domain == CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR)
				count++;
		}
		LWLockRelease(&blk->lock.lock);
	}

	return count;
}

typedef struct ClusterGcsBlockShared {
	pg_atomic_uint64 block_request_count;
	pg_atomic_uint64 block_reply_count;
	pg_atomic_uint64 block_timeout_count;
	pg_atomic_uint64 block_checksum_fail_count;
	pg_atomic_uint64 block_storage_fallback_count;
	pg_atomic_uint64 block_master_not_holder_count;
	pg_atomic_uint64 block_wal_flush_before_ship_count;
	pg_atomic_uint64 block_ship_bytes_total;
	/* PGRAC: spec-2.34 D1 — 4 reliability counters owned by cluster_gcs_block
	 * (sender + epoch wake);  4 more (dedup_hit/miss/collision/full) live in
	 * cluster_gcs_block_dedup module. */
	pg_atomic_uint64 retransmit_attempt_count;
	pg_atomic_uint64 retransmit_send_count;
	pg_atomic_uint64 retransmit_exhausted_count;
	pg_atomic_uint64 epoch_invalidate_wake_count;
	/* R8 D8-3: one exact Resource-X formation sweep per observed epoch.
	 * in_progress retains an interrupted pre-publication episode; completed
	 * makes duplicate epoch callbacks idempotent. */
	pg_atomic_uint64 resource_x_reconfig_in_progress_epoch;
	pg_atomic_uint64 resource_x_reconfig_actor_active;
	pg_atomic_uint64 resource_x_reconfig_old_formation;
	pg_atomic_uint64 resource_x_reconfig_completed_epoch;
	/* 8.15-PRE-CAP D1: diagnostic-only failure-domain counts. */
	pg_atomic_uint64 resource_x_pre_mutation_backpressure_count;
	pg_atomic_uint64 resource_x_authority_drift_count;
	pg_atomic_uint64 resource_x_post_mutation_ambiguity_count;
	pg_atomic_uint64 resource_x_internal_corruption_count;
	pg_atomic_uint64 stale_reply_drop_count;
	/* PGRAC: GCS-race round-2 RC-F — requester completion proofs emitted. */
	pg_atomic_uint64 done_sent_count;
	pg_atomic_uint64 done_enqueue_drop_count; /* review F7: outbound ring full */
	/* PGRAC: GCS serve-stall round-5 — per-family send admission outcomes
	 * under the four-state ownership contract (see ClusterICSendResult).
	 * queued = the transport ADMITTED the frame behind a backpressured
	 * tail (tier1 per-peer FIFO;  pre-fix these frames were silently
	 * LOST — the 33-54s S3 stall wall);  not_admitted = the transport
	 * REFUSED the frame (FIFO at capacity / peer mid-HELLO), retransmit
	 * machinery self-heals.  Families: REPLY (all master/holder reply
	 * sends incl. cached resends), FORWARD (master→holder), INVALIDATE
	 * (invalidate + invalidate-ack + redeclare). */
	pg_atomic_uint64 reply_send_queued_count;
	pg_atomic_uint64 reply_send_not_admitted_count;
	pg_atomic_uint64 forward_send_queued_count;
	pg_atomic_uint64 forward_send_not_admitted_count;
	pg_atomic_uint64 invalidate_send_queued_count;
	pg_atomic_uint64 invalidate_send_not_admitted_count;
	/* PGRAC: GCS serve-stall round-5 A2 — bounded-drop outcomes.  The
	 * dispatch pump never waits on a foreign buffer pin any more:
	 * a PINNED invalidate directive parks (parked) and retries from the
	 * LMS loop until the master's ack budget (park_expired = master
	 * timeout fail-closes;  park_overflow = lot full, same shape);  a
	 * PINNED drop on a grant/transfer path fail-closes with a retryable
	 * deny instead (drop_pinned_deny). */
	pg_atomic_uint64 invalidate_parked_count;
	pg_atomic_uint64 invalidate_park_expired_count;
	/* PGRAC ownership-generation wave (ruling ②): RETRYABLE_BUSY negative
	 * ACKs — holder side sent / master side consumed (slot-matching). */
	pg_atomic_uint64 invalidate_busy_sent_count;
	pg_atomic_uint64 invalidate_busy_received_count;
	/* Exact queue INVALIDATEs that broke the waiting-writer pin ring by
	 * normalizing a content-drained MAIN/INIT S mirror in place. */
	pg_atomic_uint64 invalidate_passive_s_release_count;
	/* Sole-requester S source fused the existing revoke into its grant, and
	 * the matching DRAIN preserved the resulting current X descriptor. */
	pg_atomic_uint64 pcm_x_self_handoff_count;
	pg_atomic_uint64 pcm_x_self_handoff_drain_count;
	pg_atomic_uint64 invalidate_park_overflow_count;
	pg_atomic_uint64 drop_pinned_deny_count;
	/* PGRAC: GCS serve-stall round-6 — the generation gate refused a drop
	 * because a local writer committed to the page between the ship-image
	 * copy and the drop (page LSN advanced past copy-time); the serve
	 * fail-closes with a retryable deny so the re-serve ships the current
	 * page.  A non-zero delta over a workload proves the copy->drop window
	 * was actually exercised and closed (the silent-lost-write guard). */
	pg_atomic_uint64 xfer_stale_deny_count;
	/* PGRAC: GCS-race round-4c FUNC-1 — storage-fallback SCN verify/refresh.
	 * A state=N GRANTED_STORAGE_FALLBACK now carries the master's
	 * pi_watermark_scn (reply page_lsn field reused as an SCN carrier); the
	 * requester proves its pre-read local copy current against it, or
	 * discards the bytes and re-reads the shared-storage page (closing the
	 * pre-read-vs-yield-flush lost-update window the R4 S3 53R93s hit). */
	pg_atomic_uint64 fallback_scn_verify_pass_count; /* local copy proven current (no I/O) */
	pg_atomic_uint64 fallback_scn_refresh_count;	 /* stale local copy re-read from storage */
	pg_atomic_uint64 fallback_scn_failclosed_count;	 /* dirty-stale / still-stale → 53R93 */
	/* PGRAC: spec-2.35 D12 — 7 NEW counters for CF 2-way protocol. */
	pg_atomic_uint64 block_forward_sent_count;	   /* master→holder FORWARD emitted */
	pg_atomic_uint64 block_forward_received_count; /* holder received FORWARD */
	pg_atomic_uint64 block_from_holder_ship_count; /* holder→sender direct GRANTED ship */
	pg_atomic_uint64 block_x_transfer_ship_count;  /* spec-5.2 D11 path A X-transfer ship+release */
	pg_atomic_uint64 block_x_self_ship_count; /* spec-5.2 D11 path B master==holder self-ship X */
	pg_atomic_uint64 block_forward_holder_evicted_count; /* holder evict race DENIED */
	pg_atomic_uint64 s_holders_bitmap_redirect_count;	 /* master chose forward over fallback */
	pg_atomic_uint64 master_holder_lifecycle_count;		 /* HC110 update events */
	pg_atomic_uint64 forward_replay_count;				 /* dedup FORWARDED re-forward */
	/* PGRAC: spec-2.36 D10 — 6 NEW counters for CF 3-way protocol. */
	pg_atomic_uint64 block_invalidate_broadcast_count; /* master invalidate emitted (per holder) */
	pg_atomic_uint64 block_invalidate_ack_received_count; /* holder ack collected by master */
	pg_atomic_uint64 block_invalidate_timeout_count;	/* master ack collection budget exhausted */
	pg_atomic_uint64 block_x_forward_sent_count;		/* master X-state forward emitted */
	pg_atomic_uint64 block_x_granted_from_holder_count; /* sender install X_GRANTED_FROM_HOLDER */
	pg_atomic_uint64 starvation_denied_pending_x_count; /* N→S short-circuit by pending_x */
	/* PGRAC: spec-2.37 D12 — 4 NEW counters for PI watermark + lost-write detection. */
	pg_atomic_uint64 pi_watermark_advance_count;  /* X→N/S downgrade caller advance ticks */
	pg_atomic_uint64 pi_watermark_retire_count;	  /* tag lifecycle + durable-confirm retire */
	pg_atomic_uint64 pi_durable_note_apply_count; /* target accepted status-3 DATA/CONTROL note */
	pg_atomic_uint64 lost_write_detected_count;	  /* master direct OR holder forward detect */
	pg_atomic_uint64 lost_write_avoid_count;	  /* durable-confirm retire avoided false-pos */
	/* PGRAC: spec-2.41 D7 — SCN lost-write detector + redo-coverage observability.
	 * Pure counters (no behavior change): the verdict still maps STALE+ANOMALY to
	 * DENIED_LOST_WRITE and bumps lost_write_detected_count;  these break that down
	 * by §2.6 branch and surface the redo-coverage serve-gate (§2.8 regression
	 * guard — redo_coverage_required_lsn_zero_count must stay 0 except real cold). */
	pg_atomic_uint64
		lost_write_invalidscn_failclosed_count; /* §2.6 b2: tracked block, shipped InvalidScn */
	pg_atomic_uint64
		lost_write_not_scn_tracked_skip_count; /* §2.6 b1: expected InvalidScn → skip */
	/* PGRAC: branch-1 (S3 step-2 forensics) — a STALE master-direct ship whose
	 * shared-storage version covers the watermark is rescued to
	 * GRANTED_STORAGE_FALLBACK instead of DENIED_LOST_WRITE (availability:
	 * the requester reads storage instead of aborting 53R93).  The refused
	 * twin (storage unprovable) keeps bumping lost_write_detected_count. */
	pg_atomic_uint64 lost_write_master_direct_storage_fallback_count;
	pg_atomic_uint64
		redo_coverage_required_lsn_zero_count;		 /* serve-gate required_lsn==0 (cold/degrade) */
	pg_atomic_uint64 redo_coverage_gate_block_count; /* serve-gate not-covered (block) */
	/* PGRAC: spec-5.2 D2 — X-holder shipped a one-shot read image (current
	 * block, holder kept X) for a cross-node N→S read. */
	pg_atomic_uint64 cf_xheld_read_ship_count;
	/* PGRAC: spec-5.2a D6 — clean-page X-transfer enabler (5 counters). */
	pg_atomic_uint64 clean_page_xfer_count; /* eligible clean X transfer completed */
	/* RESERVED for Stage 6 (Q3 amended 2026-06-21 — storage-fallback removed as
	 * unsound on non-cross-instance-coherent Stage-5 storage; these two stay 0
	 * until a sound storage-fallback lands in Stage 6). */
	pg_atomic_uint64 clean_page_xfer_storage_fallback_count;
	pg_atomic_uint64
		clean_page_xfer_fail_closed_count; /* eligible request fail-closed (53R9X), incl stale holder */
	pg_atomic_uint64
		clean_page_xfer_stale_holder_recover_count; /* RESERVED Stage 6 (was DENIED recover) */
	pg_atomic_uint64 clean_page_xfer_third_party_denied_count; /* 3-node third-party master DENY */
	/* PGRAC: spec-4.7 D6 — GCS/PCM warm-recovery observability (dump category
	 * 'gcs_recovery'). */
	pg_atomic_uint64 recovery_block_resources_recovering; /* phase_for_tag → RECOVERING hits */
	pg_atomic_uint64 recovery_buffers_redeclared;	 /* survivor re-declare sent (D2) */
	pg_atomic_uint64 recovery_block_state_rebuilt;	 /* master rebuild applied (D2/D3) */
	pg_atomic_uint64 recovery_redo_boundary_waits;	 /* redo gate: not yet covered (D5) */
	pg_atomic_uint64 recovery_redo_boundary_reached; /* redo gate: covered (D5) */
	pg_atomic_uint64 recovery_stale_block_drop;		 /* re-declare dropped: off-epoch/bad (D2) */
	pg_atomic_uint64 recovery_ambiguous_owner_failclosed; /* not-double-X conflict (D3) */
	pg_atomic_uint64 recovery_before_boundary_failclosed; /* served-before-redo gate fail (D5) */
	/* PGRAC: spec-2.36 D3 (HC116) — master broadcast invalidate slot.
	 * At most one broadcast in-flight per master node (Q-D3 simplification —
	 * cluster wide single-master serialization;  concurrent X requests on
	 * different tags compete for this slot, retry via DENIED_INVALIDATE_
	 * TIMEOUT if claim fails).  invalidate_broadcast_request_id == 0 means
	 * idle;  CAS to req->request_id claims the slot. */
	pg_atomic_uint64 invalidate_broadcast_request_id;  /* 0 = idle */
	uint64 invalidate_broadcast_epoch;				   /* HC116/HC100 validation */
	BufferTag invalidate_broadcast_tag;				   /* HC116/HC100 validation */
	pg_atomic_uint32 invalidate_broadcast_expected_bm; /* holders we awaited */
	pg_atomic_uint32 invalidate_broadcast_acked_bm;	   /* holders ack'd so far */
	/* PGRAC ownership-generation wave (ruling ②): a slot-matching
	 * RETRYABLE_BUSY(5) ACK arrived — the waiter aborts the round
	 * immediately instead of burning its timeout.  Claimed/released with
	 * the slot. */
	pg_atomic_uint32 invalidate_broadcast_busy;
	LWLockPadded invalidate_broadcast_lock; /* protects identity + ack bitmap */
	ConditionVariable invalidate_broadcast_cv;
	/* PGRAC: spec-6.12a — request-id source for the LOCAL-master S->X
	 * upgrade's invalidate broadcast (backend-context caller has no wire
	 * request to borrow an id from; uniqueness vs stale acks is all the
	 * slot needs). */
	pg_atomic_uint64 local_upgrade_request_seq;
	/* PGRAC: spec-6.14a D2 — successful local-master S->X upgrades (revoke
	 * granted; the L442 mechanism counter for the local arm). */
	pg_atomic_uint64 local_s_upgrade_grant_count;
	/* PGRAC: spec-6.14a D3 — remote-path X-vs-S non-holder legs: B2 grants
	 * (image captured before the revoke round) and B3/no-carrier denials. */
	pg_atomic_uint64 x_vs_s_nonholder_grant_count;
	pg_atomic_uint64 x_vs_s_no_carrier_denied_count;
	/* PGRAC: spec-6.13 D8 — RDMA tier3/direct-land copy observability. */
	pg_atomic_uint64 scratch_copy_count;
	pg_atomic_uint64 live_sge_send_count;
	pg_atomic_uint64 live_sge_fallback_count;
	pg_atomic_uint64 direct_install_count;
	pg_atomic_uint64 direct_install_abort_count;
	pg_atomic_uint64 install_copy_count;
	/* PGRAC: spec-6.12h D-h2 — PI-discard write-note ring (Q25-A dual
	 * trigger).  FlushBuffer appends a note per tracked-block write (the
	 * "写盘成功" face); the checkpointer brackets ProcessSyncRequests with
	 * presync_snapshot/confirm so pi_note_confirmed_seq only ever covers
	 * notes whose write is PROVEN durable (the "checkpoint 推进" face); the
	 * LMON tick drains [drain_seq, confirmed_seq) and routes each note to
	 * the block's master.  Multi-producer append under the spinlock; the
	 * seq fields are plain uint64 protected by the same spinlock (short
	 * hold, no I/O).  Ring full -> the NEW note is dropped (fail-safe: the
	 * PI merely lingers; dropping the oldest could starve a sealed note
	 * the drain is about to consume). */
	slock_t pi_note_lock;
	uint64 pi_note_append_seq;	  /* next seq to write (ring head) */
	uint64 pi_note_confirmed_seq; /* notes below are checkpoint-durable */
	uint64 pi_note_drain_seq;	  /* notes below were drained by LMON */
	struct {
		BufferTag tag;
		SCN page_scn; /* written pd_block_scn — the only cross-node
					   * comparable version unit (per-thread WAL makes
					   * cross-node LSN comparison meaningless) */
	} pi_note_ring[CLUSTER_GCS_PI_NOTE_RING_SIZE];

	/* PGRAC: spec-7.2 D6 — requester-side block-ship latency histogram.
	 * Bucketed at the single normal-exit funnel of
	 * cluster_gcs_send_block_request_and_wait (GRANTED / STORAGE_FALLBACK /
	 * READ_IMAGE completions only;  ereport exits lose the sample, mirroring
	 * the xp scopes).  This is the ruler for the spec-7.2 value gate
	 * (ship p99 < 20ms, p50 < 5ms) and the 7.7/7.8 wait-closure legs. */
	pg_atomic_uint64 ship_latency_hist[CLUSTER_GCS_SHIP_HIST_BUCKETS];
} ClusterGcsBlockShared;


static ClusterGcsBlockShared *ClusterGcsBlock = NULL;

/* PGRAC: spec-7.2 D6 — ship-latency histogram bucket upper bounds (us).
 * 15 bounds -> 16 buckets;  the last bucket is the +inf overflow. */
static const uint64 gcs_ship_hist_bounds_us[CLUSTER_GCS_SHIP_HIST_BUCKETS - 1]
	= { 500,	1000,	2000,	 5000,	  10000,   20000,	 50000,	  100000,
		200000, 500000, 1000000, 2000000, 5000000, 10000000, 30000000 };

/*
 * PGRAC: spec-7.2 D3/D4 — registry probe:  is the GCS block family on
 * the DATA plane?  REPLY stands in for all five (they flip atomically,
 * H-5).  Both LMON tick sites (ship_ready / pi_discard) and the LMS
 * data-plane loop consult this so the flip commit only edits the six
 * registration structs and everything pivots at once.
 */
bool
cluster_gcs_block_family_on_data_plane(void)
{
	const ClusterICMsgTypeInfo *info = cluster_ic_get_msg_type_info(PGRAC_IC_MSG_GCS_BLOCK_REPLY);

	return info != NULL && (ClusterICPlane)info->plane == CLUSTER_IC_PLANE_DATA;
}

/* Record one completed ship into the histogram (requester context). */
static void
gcs_block_ship_hist_record(TimestampTz started_at)
{
	uint64 elapsed_us;
	int b = 0;

	if (ClusterGcsBlock == NULL)
		return;
	elapsed_us = (uint64)(GetCurrentTimestamp() - started_at);
	while (b < CLUSTER_GCS_SHIP_HIST_BUCKETS - 1 && elapsed_us > gcs_ship_hist_bounds_us[b])
		b++;
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->ship_latency_hist[b], 1);
}


/* ============================================================
 * Test-only injection hooks (USE_CLUSTER_UNIT only).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT
void (*cluster_gcs_block_test_xlog_flush_hook)(uint64 page_lsn) = NULL;
int (*cluster_gcs_block_test_lsn_drift_hook)(void) = NULL;
static ClusterGcsBlockOutstandingSlot *cluster_gcs_block_test_requester_slot = NULL;
#endif


/* ============================================================
 * Forward decls (static helpers).
 * ============================================================ */
static ClusterGcsBlockBackendBlock *gcs_block_my_block(void);
static ClusterGcsBlockOutstandingSlot *gcs_block_reserve_slot(BufferTag tag, uint8 transition_id,
													  int32 master_node,
													  uint64 *out_request_id);
static ClusterGcsBlockOutstandingSlot *gcs_block_try_reserve_r4_slot(
	BufferTag tag, uint64 request_epoch, int32 expected_master_node,
	uint64 *out_request_id);
static ClusterCrBuildResult gcs_block_r4_cr_fetch_and_wait_raw(
	BufferTag tag, SCN read_scn, int32 real_master_node,
	char dst_page[GCS_BLOCK_DATA_SIZE], ClusterCrBuildReason *reason_out);
static void gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot);
static void gcs_block_send_reply(int32 dest_node, const GcsBlockRequestPayload *req,
								 GcsBlockReplyStatus status, XLogRecPtr page_lsn,
								 const char *block_data);
static ClusterICSendResult gcs_block_send_envelope_or_loopback(
	uint8 msg_type, int32 dest_node, const void *payload, uint32 payload_len);
static bool gcs_block_r4_tx_origin_try_accept(
	const ClusterICEnvelope *env, const ClusterR4CrForwardPayload *forward);
static bool gcs_block_current_mx_origin_try_accept(
	const ClusterICEnvelope *env, const void *payload);
static void gcs_block_r4_tx_origin_context_clear(
	GcsBlockR4TxOriginContext *context, bool cancel_guard);
static bool gcs_block_get_ship_image(BufferTag tag, int32 dest_node, bool allow_live_sge,
									 XLogRecPtr *out_page_lsn, char *copy_buf,
									 const char **out_block_payload, uint32 *out_block_lkey,
									 ClusterICSgeReleaseCallback *out_release_cb,
									 void **out_release_arg, ClusterSfDepVec *out_sf_dep_vec,
									 bool *out_sf_dep_valid,
									 ClusterBufmgrGcsCopyRefusal *out_copy_refusal);
static void gcs_block_release_ship_image(ClusterICSgeReleaseCallback release_cb, void *release_arg);
static uint32 gcs_block_compute_checksum(const char *block_data);
static uint32 gcs_block_compute_invalidate_checksum(const GcsBlockInvalidatePayload *inv);
static uint32 gcs_block_compute_invalidate_ack_checksum(const GcsBlockInvalidateAckPayload *ack);
static uint32 gcs_block_compute_redeclare_checksum(const GcsBlockRedeclarePayload *p);
static void gcs_block_resource_x_fail_closed_current(void);
static PcmXSessionAuthResult
gcs_block_pcm_x_authenticated_session_result(int32 node_id, uint64 expected_epoch,
											 uint64 *session_out,
											 ClusterGcsPcmXAuthSample *sample_out);
static bool gcs_block_pcm_x_authenticated_session(int32 node_id, uint64 expected_epoch,
												  uint64 *session_out);
static uint64 gcs_block_pcm_x_monotonic_us(void);
static uint64 gcs_block_pcm_x_saturating_add_us(uint64 base, uint64 delta);
static uint64 gcs_block_pcm_x_retry_timeout_us(void);
static void gcs_block_install_block(BufferDesc *buf, const char *block_data, XLogRecPtr page_lsn);
static void gcs_block_install_reply_block(BufferDesc *buf, const char *block_data,
										  XLogRecPtr page_lsn,
										  const ClusterGcsBlockOutstandingSlot *slot);
static bool gcs_block_decode_reply_payload(const ClusterICEnvelope *env, const void *payload,
										   const GcsBlockReplyHeader **out_hdr,
										   const char **out_block_data, bool *out_sf_dep_valid,
										   uint8 *out_sf_flags, ClusterSfDepVec *out_sf_dep_vec,
										   const ClusterGcsUndoAuthTrailer **out_undo_trailer);
/* PGRAC: spec-2.36 D3 (HC116) — master synchronous broadcast invalidate.
 * Enumerates `holders_bm` (1 bit per cluster node), emits INVALIDATE
 * envelope to each, waits for all INVALIDATE_ACK msg_type 18 within
 * cluster.gcs_block_invalidate_ack_timeout_ms;  retries failed/timed-out
 * holders per spec-2.34 retransmit budget;  returns true on full
 * collection, false on budget exhaustion.  The blocking form survives
 * only behind the backend-context local-master upgrade (_ext, outbound
 * ring); the LMON wire-request S-branch uses the nowait fan-out (e2
 * structural fix — the dispatch loop cannot sleep on ACKs it drains). */
static void gcs_block_broadcast_invalidate_nowait(const GcsBlockRequestPayload *req,
												  uint32 holders_bm);

/* PGRAC: spec-6.12h D-h2 — PI-holder discard protocol (definitions after the
 * invalidate machinery; the conversion sites above them need the decls). */
static void gcs_block_pi_kept_note_send(BufferTag tag, int32 master_node);
static void gcs_block_pi_discard_master_apply(BufferTag tag, SCN written_scn);


/* ============================================================
 * Module init + shmem registration.
 * ============================================================ */

Size
cluster_gcs_block_shmem_size(void)
{
	Size sz;

	sz = MAXALIGN(sizeof(ClusterGcsBlockShared));
	if (IsBootstrapProcessingMode())
		return sz;

	sz = add_size(sz, mul_size(MaxBackends, sizeof(ClusterGcsBlockBackendBlock)));
	return sz;
}

void
cluster_gcs_block_shmem_init(void)
{
	bool found;
	char *base;
	int i;
	int j;

	base = (char *)ShmemInitStruct("pgrac cluster gcs block", cluster_gcs_block_shmem_size(),
								   &found);
	ClusterGcsBlock = (ClusterGcsBlockShared *)base;
	gcs_block_backend_blocks
		= IsBootstrapProcessingMode()
			  ? NULL
			  : (ClusterGcsBlockBackendBlock *)(base + MAXALIGN(sizeof(ClusterGcsBlockShared)));

	if (!found) {
		memset(ClusterGcsBlock, 0, sizeof(*ClusterGcsBlock));
		/* PGRAC: spec-7.2 D6 — ship-latency histogram buckets. */
		for (i = 0; i < CLUSTER_GCS_SHIP_HIST_BUCKETS; i++)
			pg_atomic_init_u64(&ClusterGcsBlock->ship_latency_hist[i], 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_request_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_reply_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_timeout_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_checksum_fail_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_storage_fallback_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_master_not_holder_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_ship_bytes_total, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->retransmit_attempt_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->retransmit_send_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->retransmit_exhausted_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->epoch_invalidate_wake_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->resource_x_reconfig_in_progress_epoch, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->resource_x_reconfig_actor_active, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->resource_x_reconfig_old_formation, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->resource_x_reconfig_completed_epoch, 0);
		pg_atomic_init_u64(
			&ClusterGcsBlock->resource_x_pre_mutation_backpressure_count, 0);
		pg_atomic_init_u64(
			&ClusterGcsBlock->resource_x_authority_drift_count, 0);
		pg_atomic_init_u64(
			&ClusterGcsBlock->resource_x_post_mutation_ambiguity_count, 0);
		pg_atomic_init_u64(
			&ClusterGcsBlock->resource_x_internal_corruption_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->stale_reply_drop_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->done_sent_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->done_enqueue_drop_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->reply_send_queued_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->reply_send_not_admitted_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->forward_send_queued_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->forward_send_not_admitted_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_send_queued_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_send_not_admitted_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_parked_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_busy_sent_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_busy_received_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_passive_s_release_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->pcm_x_self_handoff_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->pcm_x_self_handoff_drain_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_park_expired_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_park_overflow_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->drop_pinned_deny_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->xfer_stale_deny_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->fallback_scn_verify_pass_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->fallback_scn_refresh_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->fallback_scn_failclosed_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_forward_sent_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_forward_received_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_from_holder_ship_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_x_transfer_ship_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_x_self_ship_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_forward_holder_evicted_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->s_holders_bitmap_redirect_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->master_holder_lifecycle_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->forward_replay_count, 0);
		/* PGRAC: spec-2.36 D10 — 6 NEW counters init. */
		pg_atomic_init_u64(&ClusterGcsBlock->block_invalidate_broadcast_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_invalidate_ack_received_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_invalidate_timeout_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_x_forward_sent_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_x_granted_from_holder_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 0);
		/* PGRAC: spec-2.37 D12 — 4 NEW counters init. */
		pg_atomic_init_u64(&ClusterGcsBlock->pi_watermark_advance_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->pi_watermark_retire_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->pi_durable_note_apply_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->lost_write_detected_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->lost_write_avoid_count, 0);
		/* PGRAC: spec-2.41 D7 — 4 NEW SCN detector + redo-coverage counters init. */
		pg_atomic_init_u64(&ClusterGcsBlock->lost_write_invalidscn_failclosed_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->lost_write_not_scn_tracked_skip_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->lost_write_master_direct_storage_fallback_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->redo_coverage_required_lsn_zero_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->redo_coverage_gate_block_count, 0);
		/* PGRAC: spec-5.2 D2 — X-holder read-image ship counter init. */
		pg_atomic_init_u64(&ClusterGcsBlock->cf_xheld_read_ship_count, 0);
		/* PGRAC: spec-5.2a D6 — 5 NEW clean-page X-transfer counters init. */
		pg_atomic_init_u64(&ClusterGcsBlock->clean_page_xfer_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->clean_page_xfer_storage_fallback_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->clean_page_xfer_stale_holder_recover_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->clean_page_xfer_third_party_denied_count, 0);
		/* PGRAC: spec-4.7 D6 — 8 NEW warm-recovery counters init. */
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_block_resources_recovering, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_buffers_redeclared, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_block_state_rebuilt, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_redo_boundary_waits, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_redo_boundary_reached, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_stale_block_drop, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_ambiguous_owner_failclosed, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->recovery_before_boundary_failclosed, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, 0);
		ClusterGcsBlock->invalidate_broadcast_epoch = 0;
		memset(&ClusterGcsBlock->invalidate_broadcast_tag, 0,
			   sizeof(ClusterGcsBlock->invalidate_broadcast_tag));
		pg_atomic_init_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, 0);
		pg_atomic_init_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
		pg_atomic_init_u32(&ClusterGcsBlock->invalidate_broadcast_busy, 0);
		LWLockInitialize(&ClusterGcsBlock->invalidate_broadcast_lock.lock,
						 LWTRANCHE_CLUSTER_GCS_BLOCK);
		ConditionVariableInit(&ClusterGcsBlock->invalidate_broadcast_cv);
		/* PGRAC: spec-6.12a — local-upgrade broadcast id source. */
		pg_atomic_init_u64(&ClusterGcsBlock->local_upgrade_request_seq, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->local_s_upgrade_grant_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->x_vs_s_nonholder_grant_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->x_vs_s_no_carrier_denied_count, 0);
		/* PGRAC: spec-6.13 D8 — RDMA tier3/direct-land copy counters init. */
		pg_atomic_init_u64(&ClusterGcsBlock->scratch_copy_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->live_sge_send_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->live_sge_fallback_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->direct_install_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->direct_install_abort_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->install_copy_count, 0);
		/* PGRAC: spec-6.12h D-h2 — PI-discard write-note ring. */
		SpinLockInit(&ClusterGcsBlock->pi_note_lock);
		ClusterGcsBlock->pi_note_append_seq = 0;
		ClusterGcsBlock->pi_note_confirmed_seq = 0;
		ClusterGcsBlock->pi_note_drain_seq = 0;
		memset(ClusterGcsBlock->pi_note_ring, 0, sizeof(ClusterGcsBlock->pi_note_ring));

		if (gcs_block_backend_blocks == NULL)
			return;

		for (i = 0; i < MaxBackends; i++) {
			ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[i];

			LWLockInitialize(&blk->lock.lock, LWTRANCHE_CLUSTER_GCS_BLOCK);
			blk->next_request_id = 1;
			for (j = 0; j < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; j++) {
				ClusterGcsBlockOutstandingSlot *slot = &blk->slots[j];

				slot->in_use = false;
				slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
				slot->request_id = 0;
				slot->reply_received = false;
				slot->reply_sf_dep_valid = false;
				slot->reply_sf_flags = 0;
				cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
				slot->request_epoch = 0; /* spec-2.34 HC100 */
				slot->expected_master_node = -1;
				gcs_block_slot_clear_current_mx_expectation(slot);
				slot->stale = false;
				slot->direct_generation = 0;
				slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
				slot->direct_expected_peer = -1;
				slot->direct_arm_id = 0;
				slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
				slot->direct_target_buf = NULL;
				slot->direct_target_addr = NULL;
				slot->direct_target_lkey = 0;
				slot->direct_target_prepared = false;
				slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
				ConditionVariableInit(&slot->reply_cv);
			}
		}
	}
}

static const ClusterShmemRegion cluster_gcs_block_region = {
	.name = "pgrac cluster gcs block",
	.size_fn = cluster_gcs_block_shmem_size,
	.init_fn = cluster_gcs_block_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_gcs_block",
	.reserved_flags = 0,
};

void
cluster_gcs_block_module_init(void)
{
	cluster_shmem_register_region(&cluster_gcs_block_region);
}


/* ============================================================
 * Outstanding-slot management.
 * ============================================================ */

static ClusterGcsBlockBackendBlock *
gcs_block_my_block(void)
{
	int idx;

	idx = MyBackendId - 1;
	if (idx < 0 || idx >= MaxBackends)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_block: MyBackendId=%d out of [1, MaxBackends=%d] range",
							   (int)MyBackendId, MaxBackends),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	return &gcs_block_backend_blocks[idx];
}


/* Mint a queue request id from the same per-backend domain as ordinary block
 * requests without consuming a block-reply slot.  The shared counter lock is
 * the collision boundary between the two users.  Queue identities are
 * durable, so wrapping the 40-bit wire sequence is exhaustion, not reuse. */
static bool
gcs_block_pcm_x_next_request_id(uint64 *request_id_out)
{
	ClusterGcsBlockBackendBlock *blk;
	uint64 sequence;

	if (request_id_out != NULL)
		*request_id_out = 0;
	if (request_id_out == NULL || MyBackendId <= 0 || MyBackendId > MaxBackends)
		return false;
	blk = gcs_block_my_block();
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	sequence = blk->next_request_id;
	if (sequence == 0 || sequence > GCS_REQID_REQUESTER_SEQ_MASK) {
		LWLockRelease(&blk->lock.lock);
		return false;
	}
	blk->next_request_id++;
	*request_id_out = gcs_reqid_requester(cluster_node_id, (int)MyBackendId - 1, sequence);
	LWLockRelease(&blk->lock.lock);
	return *request_id_out != 0;
}

static ClusterGcsBlockOutstandingSlot *
gcs_block_reserve_slot(BufferTag tag, uint8 transition_id, int32 master_node,
					   uint64 *out_request_id)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	int i;

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		if (!blk->slots[i].in_use) {
			slot = &blk->slots[i];
			slot->in_use = true;
			slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
			slot->reply_received = false;
			/* PGRAC: spec-6.14a D1 — domain-tagged id.  Raw per-backend
			 * counters all start at 1, so ids from different backends (or
			 * the local-upgrade counter) collide and a late invalidate ACK
			 * from an earlier same-tag round could falsely certify a holder
			 * in a newer round (ABA).  See cluster_gcs_reqid.h. */
			slot->request_id = gcs_reqid_requester(cluster_node_id, (int)MyBackendId - 1,
												   blk->next_request_id++);
			slot->transition_id = transition_id;
			slot->tag = tag;
			slot->master_node = master_node;
			/* PGRAC: spec-2.34 HC100 — reset stale-reply defense fields.
			 * Real request_epoch + expected_master_node are stamped by
			 * sender at each send (each retry refreshes both;  reply
			 * handler validates against the latest stamp). */
			slot->request_epoch = 0;
			slot->expected_master_node = master_node;
			gcs_block_slot_clear_current_mx_expectation(slot);
			slot->stale = false;
			slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
			slot->direct_expected_peer = -1;
			slot->direct_arm_id = 0;
			slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
			slot->direct_target_buf = NULL;
			slot->direct_target_addr = NULL;
			slot->direct_target_lkey = 0;
			slot->direct_target_prepared = false;
			slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
			*out_request_id = slot->request_id;
			break;
		}
	}
	LWLockRelease(&blk->lock.lock);

	if (slot == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("cluster_gcs_block: outstanding-block table full (max %d per backend)",
						MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND),
				 errhint("Reduce concurrent block-ship acquisitions; "
						 "per-backend cap GUC may land in spec-2.34+."),
				 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));
	return slot;
}

/* Reserve one requester slot in the disjoint R4 reply domain.  Selection,
 * no-reuse sequence validation and identity publication share the existing
 * per-backend lock, so capacity cannot consume an id and a wrapped sequence
 * can never be mapped back to one by gcs_reqid_requester(). */
static ClusterGcsBlockOutstandingSlot *
gcs_block_try_reserve_r4_slot(BufferTag tag, uint64 request_epoch,
							   int32 expected_master_node, uint64 *out_request_id)
{
	ClusterGcsBlockBackendBlock *blk;
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	uint64 sequence;
	int i;

	if (out_request_id != NULL)
		*out_request_id = 0;
	if (out_request_id == NULL || MyBackendId <= 0 || MyBackendId > MaxBackends
		|| expected_master_node < 0 || expected_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return NULL;
	blk = gcs_block_my_block();
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++)
		if (!blk->slots[i].in_use) {
			slot = &blk->slots[i];
			break;
		}
	sequence = blk->next_request_id;
	if (slot == NULL || sequence == 0 || sequence > GCS_REQID_REQUESTER_SEQ_MASK) {
		LWLockRelease(&blk->lock.lock);
		return NULL;
	}

	blk->next_request_id++;
	memset(&slot->reply_header, 0, sizeof(slot->reply_header));
	memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
	slot->in_use = true;
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
	slot->request_id
		= gcs_reqid_requester(cluster_node_id, (int)MyBackendId - 1, sequence);
	slot->transition_id = (uint8)PCM_TRANS_N_TO_S;
	slot->tag = tag;
	slot->master_node = expected_master_node;
	slot->reply_received = false;
	slot->reply_sf_dep_valid = false;
	slot->reply_sf_flags = 0;
	cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
	slot->reply_undo_trailer_valid = false;
	slot->reply_undo_tt_generation = 0;
	slot->reply_undo_authority_scn = 0;
	slot->request_epoch = request_epoch;
	slot->expected_master_node = expected_master_node;
	gcs_block_slot_clear_current_mx_expectation(slot);
	slot->stale = false;
	slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
	slot->direct_expected_peer = -1;
	slot->direct_arm_id = 0;
	slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
	slot->direct_target_buf = NULL;
	slot->direct_target_addr = NULL;
	slot->direct_target_lkey = 0;
	slot->direct_target_prepared = false;
	slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
	*out_request_id = slot->request_id;
#ifdef USE_CLUSTER_UNIT
	cluster_gcs_block_test_requester_slot = slot;
#endif
	LWLockRelease(&blk->lock.lock);
	return slot;
}

/* Reserve one exact Current-MX describe attempt.  The slot is published in
 * its own reply domain before the 128-byte request can reach the DATA ring. */
static ClusterGcsBlockOutstandingSlot *
gcs_block_try_reserve_current_mx_slot(
	const ClusterCurrentMxKey *key, uint64 request_epoch,
	int32 expected_origin_node, uint8 expected_reply_status,
	uint64 *out_request_id)
{
	ClusterGcsBlockBackendBlock *blk;
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	uint64 sequence;
	int i;

	if (out_request_id != NULL)
		*out_request_id = 0;
	if (key == NULL || out_request_id == NULL
		|| MyBackendId <= 0 || MyBackendId > MaxBackends
		|| expected_origin_node < 0
		|| expected_origin_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| (expected_reply_status
				!= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT
			&& expected_reply_status
				   != (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT))
		return NULL;
	blk = gcs_block_my_block();
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++)
		if (!blk->slots[i].in_use) {
			slot = &blk->slots[i];
			break;
		}
	sequence = blk->next_request_id;
	if (slot == NULL || sequence == 0
		|| sequence > GCS_REQID_REQUESTER_SEQ_MASK) {
		LWLockRelease(&blk->lock.lock);
		return NULL;
	}

	blk->next_request_id++;
	memset(&slot->reply_header, 0, sizeof(slot->reply_header));
	memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
	slot->in_use = true;
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_CURRENT_MX;
	slot->request_id
		= gcs_reqid_requester(cluster_node_id, (int)MyBackendId - 1, sequence);
	slot->transition_id = 0;
	slot->tag = GcsBlockCurrentMxRouteTagMake(
		slot->request_id, request_epoch, cluster_node_id, (int32)MyBackendId);
	slot->master_node = expected_origin_node;
	slot->reply_received = false;
	slot->reply_sf_dep_valid = false;
	slot->reply_sf_flags = 0;
	cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
	slot->reply_undo_trailer_valid = false;
	slot->reply_undo_tt_generation = 0;
	slot->reply_undo_authority_scn = 0;
	slot->request_epoch = request_epoch;
	slot->expected_master_node = expected_origin_node;
	slot->expected_reply_status = expected_reply_status;
	slot->expected_current_mx_key_valid = true;
	slot->expected_current_mx_key = *key;
	slot->stale = false;
	slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
	slot->direct_expected_peer = -1;
	slot->direct_arm_id = 0;
	slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
	slot->direct_target_buf = NULL;
	slot->direct_target_addr = NULL;
	slot->direct_target_lkey = 0;
	slot->direct_target_prepared = false;
	slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
	*out_request_id = slot->request_id;
#ifdef USE_CLUSTER_UNIT
	cluster_gcs_block_test_requester_slot = slot;
#endif
	LWLockRelease(&blk->lock.lock);
	return slot;
}


static void
gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
	BufferTag released_tag;
	uint64 released_request_id = 0;
	uint64 released_epoch = 0;
	int released_slot_index = -1;
	int released_direct_state = (int)GCS_BLOCK_DIRECT_UNARMED;
	bool released_reply_received = false;
	bool released_live_direct = false;

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	if (slot->in_use && slot->direct_target_prepared) {
		released_tag = slot->tag;
		released_request_id = slot->request_id;
		released_epoch = slot->request_epoch;
		released_slot_index = (int)(slot - &blk->slots[0]);
		released_direct_state = (int)slot->direct_state;
		released_reply_received = slot->reply_received;
		released_live_direct = true;
	}
	slot->in_use = false;
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
	slot->reply_received = false;
	slot->request_id = 0;
	slot->transition_id = 0;
	slot->master_node = -1;
	slot->reply_sf_dep_valid = false;
	slot->reply_sf_flags = 0;
	cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
	slot->request_epoch = 0; /* spec-2.34 HC100 */
	slot->expected_master_node = -1;
	gcs_block_slot_clear_current_mx_expectation(slot);
	slot->stale = false;
	slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
	slot->direct_expected_peer = -1;
	slot->direct_arm_id = 0;
	slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
	slot->direct_target_buf = NULL;
	slot->direct_target_addr = NULL;
	slot->direct_target_lkey = 0;
	slot->direct_target_prepared = false;
	slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
	LWLockRelease(&blk->lock.lock);
	if (released_live_direct)
		elog(LOG,
			 "cluster GCS block slot released with live direct target observation: "
			 "backend=%d slot=%d request_id=%llu epoch=%llu rel=%u fork=%d blk=%u "
			 "reply_received=%d direct_state=%d direct_prepared=1",
			 (int)MyBackendId - 1, released_slot_index, (unsigned long long)released_request_id,
			 (unsigned long long)released_epoch, released_tag.relNumber, (int)released_tag.forkNum,
			 released_tag.blockNum, released_reply_received ? 1 : 0, released_direct_state);
}

static uint32
gcs_block_direct_arm_id(int backend_idx, int slot_idx)
{
	return (uint32)(backend_idx * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND + slot_idx);
}

static bool
gcs_block_direct_decode_arm_id(uint32 arm_id, int *backend_idx, int *slot_idx)
{
	uint32 cap = (uint32)MaxBackends * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND;

	if (arm_id >= cap)
		return false;
	if (backend_idx != NULL)
		*backend_idx = (int)(arm_id / MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND);
	if (slot_idx != NULL)
		*slot_idx = (int)(arm_id % MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND);
	return true;
}

static int
gcs_block_slot_index(ClusterGcsBlockBackendBlock *blk, ClusterGcsBlockOutstandingSlot *slot)
{
	ptrdiff_t idx;

	Assert(blk != NULL);
	Assert(slot != NULL);
	idx = slot - &blk->slots[0];
	Assert(idx >= 0 && idx < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND);
	return (int)idx;
}

static void
gcs_block_direct_finish_target(BufferDesc *target_buf, bool prepared, bool valid,
							   XLogRecPtr page_lsn)
{
	if (target_buf != NULL && prepared)
		cluster_bufmgr_finish_direct_land_target_for_gcs(target_buf, valid, page_lsn);
}

static uint32
gcs_block_direct_envelope_crc(const ClusterICEnvelope *env, const GcsBlockReplyHeader *hdr,
							  const void *page)
{
	pg_crc32c crc;
	const uint8 *env_bytes = (const uint8 *)env;
	const size_t crc_offset = offsetof(ClusterICEnvelope, payload_crc32c);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, env_bytes, crc_offset);
	COMP_CRC32C(crc, hdr, sizeof(*hdr));
	COMP_CRC32C(crc, page, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
gcs_block_direct_prepare_attempt(ClusterGcsBlockOutstandingSlot *slot, BufferDesc *buf,
								 BufferTag tag, PcmLockTransition transition_id,
								 int32 expected_peer)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
	void *page_addr = NULL;
	int backend_idx = MyBackendId - 1;
	int slot_idx;
	int32 holder_node;
	bool slot_eligible;

	if (slot == NULL || buf == NULL)
		return false;
	if (transition_id != PCM_TRANS_N_TO_S)
		return false;
	/* A current-master INVALIDATE can now deliver a local exact denial before
	 * this attempt is sent.  Close both sides of the target-preparation window:
	 * do not begin after that denial, and recheck after the bufmgr prepare in
	 * case it landed while the target was being pinned/prepared. */
	LWLockAcquire(&blk->lock.lock, LW_SHARED);
	slot_eligible = slot->in_use && !slot->reply_received
					&& slot->direct_state == GCS_BLOCK_DIRECT_UNARMED
					&& !slot->direct_target_prepared;
	LWLockRelease(&blk->lock.lock);
	if (!slot_eligible)
		return false;
	holder_node = cluster_pcm_master_holder_node_by_tag(tag);
	if (!GcsBlockDirectCanArmExpectedPeer(holder_node, expected_peer))
		return false;
	if (!cluster_ic_rdma_block_reply_lane_connected(expected_peer, NULL))
		return false;
	if (!cluster_bufmgr_prepare_direct_land_target_for_gcs(buf, tag, &page_addr))
		return false;

	slot_idx = gcs_block_slot_index(blk, slot);
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	if (!slot->in_use || slot->reply_received || slot->direct_state != GCS_BLOCK_DIRECT_UNARMED
		|| slot->direct_target_prepared) {
		LWLockRelease(&blk->lock.lock);
		gcs_block_direct_finish_target(buf, true, false, InvalidXLogRecPtr);
		return false;
	}
	slot->direct_generation = cluster_ic_rdma_direct_land_next_generation(slot->direct_generation);
	slot->direct_state = GCS_BLOCK_DIRECT_ARMING;
	slot->direct_expected_peer = expected_peer;
	slot->direct_arm_id = gcs_block_direct_arm_id(backend_idx, slot_idx);
	slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_SHARED_BUFFER;
	slot->direct_target_buf = buf;
	slot->direct_target_addr = page_addr;
	slot->direct_target_lkey = 0;
	slot->direct_target_prepared = true;
	slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
	LWLockRelease(&blk->lock.lock);
	return true;
}

static bool
gcs_block_direct_mark_aborting(ClusterGcsBlockOutstandingSlot *slot,
							   ClusterGcsBlockDirectAbortReason reason)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
	bool marked = false;

	if (slot == NULL)
		return false;
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	if (slot->in_use
		&& (slot->direct_state == GCS_BLOCK_DIRECT_ARMED
			|| slot->direct_state == GCS_BLOCK_DIRECT_ARMING
			|| slot->direct_state == GCS_BLOCK_DIRECT_LANDED)) {
		slot->direct_state = GCS_BLOCK_DIRECT_ABORTING;
		slot->direct_abort_reason = reason;
		marked = true;
	}
	LWLockRelease(&blk->lock.lock);
	if (marked)
		cluster_lmon_wakeup();
	return marked;
}


/* ============================================================
 * Checksum + block install helpers.
 * ============================================================ */

static uint32
gcs_block_compute_checksum(const char *block_data)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block_data, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

/* PGRAC: spec-6.12b — public checksum for the CR-server reply builder
 * (cluster_cr_server.c ships GCS_BLOCK_REPLY frames from the LMON tick). */
uint32
cluster_gcs_block_compute_checksum(const char *block_data)
{
	return gcs_block_compute_checksum(block_data);
}

/*
 * A requester-side status=6 only says that some producer refused the image;
 * the historical aggregate counter is also bumped by requesters and therefore
 * cannot identify the producer branch.  Emit one reason-tagged record at the
 * producer with the unchanged wire identity and ONE entry-lock-coherent PCM
 * authority snapshot.  Do not reconstruct authority with separate state /
 * holder / bitmap queries here: that would turn diagnostics into another
 * observer race under the hot-block handoff this record is meant to explain.
 */
static void
gcs_block_log_master_not_holder_producer(const char *reason, BufferTag tag, uint64 request_id,
										 uint64 request_epoch, int32 requester_node,
										 uint8 transition_id)
{
	PcmAuthoritySnapshot authority;
	bool authority_found;

	authority_found = cluster_pcm_lock_authority_snapshot(tag, &authority);
	ereport(
		LOG,
		(errmsg_internal("cluster_gcs_block: master-not-holder reply produced"),
		 errdetail_internal(
			 "reason=%s producer=%d requester=%d request_id=" UINT64_FORMAT
			 " request_epoch=" UINT64_FORMAT
			 " transition=%u tag spc=%u db=%u relNumber=%u fork=%d block=%u "
			 "authority_found=%d state=%u x_holder=%d s_holders=0x%08x "
			 "master_holder_node=%u master_holder_procno=%u master_holder_epoch=" UINT64_FORMAT
			 " master_holder_request_id=" UINT64_FORMAT
			 " pending_x_requester=%d pending_x_since_lsn=" UINT64_FORMAT
			 " transition_count=" UINT64_FORMAT,
			 reason != NULL ? reason : "unknown", cluster_node_id, requester_node, request_id,
			 request_epoch, (unsigned int)transition_id, tag.spcOid, tag.dbOid,
			 (unsigned int)BufTagGetRelNumber(&tag), (int)tag.forkNum, (unsigned int)tag.blockNum,
			 (int)authority_found, (unsigned int)authority.state, authority.x_holder_node,
			 (unsigned int)authority.s_holders_bitmap, authority.master_holder.node_id,
			 authority.master_holder.procno, authority.master_holder.cluster_epoch,
			 authority.master_holder.request_id, authority.pending_x_requester_node,
			 authority.pending_x_since_lsn, authority.transition_count)));
}

#define GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, reason)                                       \
	gcs_block_log_master_not_holder_producer((reason), (req)->tag, (req)->request_id,              \
											 (req)->epoch, (req)->sender_node,                     \
											 (req)->transition_id)
#define GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(fwd, reason)                                       \
	gcs_block_log_master_not_holder_producer((reason), (fwd)->tag, (fwd)->request_id,              \
											 (fwd)->epoch, (fwd)->original_requester_node,         \
											 (fwd)->transition_id)

static void
gcs_block_release_ship_image(ClusterICSgeReleaseCallback release_cb, void *release_arg)
{
	if (release_cb != NULL)
		release_cb(release_arg);
}

static void
gcs_block_note_scratch_copy(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->scratch_copy_count, 1);
}

static void
gcs_block_note_live_sge_fallback(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->live_sge_fallback_count, 1);
}

static void
gcs_block_note_live_sge_send(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->live_sge_send_count, 1);
}

static void
gcs_block_note_install_copy(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->install_copy_count, 1);
}

static void
gcs_block_note_direct_install(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->direct_install_count, 1);
}

static void
gcs_block_note_direct_abort(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->direct_install_abort_count, 1);
}

static void
gcs_block_release_live_sge(void *arg)
{
	cluster_bufmgr_release_block_for_gcs_live_sge((BufferDesc *)arg);
}

static ClusterICSendResult
gcs_block_send_direct_reply_sge(int32 dest_node, const GcsBlockReplyHeader *hdr,
								const char *block_payload, uint32 block_lkey,
								ClusterICSgeReleaseCallback release_cb, void *release_arg)
{
	ClusterICSge sge[2];
	char *zero_page = NULL;
	ClusterICSendResult rc;

	if (hdr == NULL)
		return CLUSTER_IC_SEND_HARD_ERROR;

	if (block_payload == NULL) {
		zero_page = (char *)palloc0(GCS_BLOCK_DATA_SIZE);
		block_payload = zero_page;
		block_lkey = 0;
		release_cb = NULL;
		release_arg = NULL;
	}

	memset(sge, 0, sizeof(sge));
	sge[0].addr = (void *)hdr;
	sge[0].len = sizeof(*hdr);
	sge[1].addr = (void *)block_payload;
	sge[1].len = GCS_BLOCK_DATA_SIZE;
	sge[1].lkey = block_lkey;
	sge[1].release_cb = release_cb;
	sge[1].release_arg = release_arg;
	rc = cluster_ic_rdma_send_block_reply_direct(dest_node, sge, lengthof(sge),
												 GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE);
	if (zero_page != NULL)
		pfree(zero_page);
	return rc;
}

ClusterICSendResult
cluster_gcs_block_send_direct_zero_reply(int32 dest_node, const GcsBlockReplyHeader *header)
{
	ClusterICSendResult rc;

	if (header == NULL || dest_node < 0 || dest_node >= CLUSTER_MAX_NODES
		|| dest_node == cluster_node_id
		|| header->status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X)
		return CLUSTER_IC_SEND_HARD_ERROR;
	rc = gcs_block_send_direct_reply_sge(dest_node, header, NULL, 0, NULL, NULL);
	if (rc == CLUSTER_IC_SEND_DONE && ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
	return rc;
}

static bool
gcs_block_try_send_direct_reply(int32 dest_node, bool direct_armed, GcsBlockReplyHeader *hdr,
								const char *block_payload, uint32 block_lkey,
								ClusterICSgeReleaseCallback release_cb, void *release_arg)
{
	ClusterICSendResult rc;
	GcsBlockReplyHeader denial;
	char zero_page[GCS_BLOCK_DATA_SIZE];
	GcsBlockReplyStatus status;

	if (!direct_armed || hdr == NULL)
		return false;

	status = (GcsBlockReplyStatus)hdr->status;
	if (!GcsBlockReplyStatusIsDirectLandSendable(status)) {
		memset(&denial, 0, sizeof(denial));
		denial = *hdr;
		denial.page_lsn = 0;
		denial.status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		memset(zero_page, 0, sizeof(zero_page));
		denial.checksum = gcs_block_compute_checksum(zero_page);
		rc = gcs_block_send_direct_reply_sge(dest_node, &denial, zero_page, 0, NULL, NULL);
		if (release_cb != NULL)
			release_cb(release_arg);
	} else
		rc = gcs_block_send_direct_reply_sge(dest_node, hdr, block_payload, block_lkey, release_cb,
											 release_arg);

	if (rc == CLUSTER_IC_SEND_DONE && ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
	return true;
}

static bool
gcs_block_get_ship_image(BufferTag tag, int32 dest_node, bool allow_live_sge,
						 XLogRecPtr *out_page_lsn, char *copy_buf, const char **out_block_payload,
						 uint32 *out_block_lkey, ClusterICSgeReleaseCallback *out_release_cb,
						 void **out_release_arg, ClusterSfDepVec *out_sf_dep_vec,
						 bool *out_sf_dep_valid, ClusterBufmgrGcsCopyRefusal *out_copy_refusal)
{
	void *scratch = NULL;
	uint32 scratch_lkey = 0;
	bool rdma_sge_supported;
	bool smart_fusion_reply;

	if (out_block_payload != NULL)
		*out_block_payload = NULL;
	if (out_block_lkey != NULL)
		*out_block_lkey = 0;
	if (out_release_cb != NULL)
		*out_release_cb = NULL;
	if (out_release_arg != NULL)
		*out_release_arg = NULL;
	if (out_sf_dep_valid != NULL)
		*out_sf_dep_valid = false;
	if (out_copy_refusal != NULL)
		*out_copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	if (out_sf_dep_vec != NULL)
		cluster_sf_dep_vec_reset(out_sf_dep_vec);

	smart_fusion_reply = cluster_smart_fusion && cluster_sf_peer_supports_reply_v2(dest_node);
	rdma_sge_supported
		= cluster_ic_rdma_block_sge_supported(NULL)
		  && cluster_ic_mux_peer_transport(dest_node) == CLUSTER_IC_PEER_TRANSPORT_RDMA;

	if (allow_live_sge && !smart_fusion_reply && rdma_sge_supported) {
		void *live_page = NULL;
		BufferDesc *live_buf = NULL;
		uint32 live_lkey = 0;

		if (cluster_bufmgr_borrow_block_for_gcs_live_sge(tag, out_page_lsn, &live_page,
														 &live_buf)) {
			if (cluster_ic_rdma_shared_buffers_sge(live_page, GCS_BLOCK_DATA_SIZE, &live_lkey)) {
				*out_block_payload = (const char *)live_page;
				if (out_block_lkey != NULL)
					*out_block_lkey = live_lkey;
				if (out_release_cb != NULL)
					*out_release_cb = gcs_block_release_live_sge;
				if (out_release_arg != NULL)
					*out_release_arg = live_buf;
				return true;
			}
			cluster_bufmgr_release_block_for_gcs_live_sge(live_buf);
		}
		gcs_block_note_live_sge_fallback();
	}

	if (rdma_sge_supported) {
		void *release_arg = NULL;
		ClusterICSgeReleaseCallback release_cb = NULL;

		if (cluster_ic_rdma_borrow_block_scratch(dest_node, GCS_BLOCK_DATA_SIZE, &scratch,
												 &scratch_lkey, &release_cb, &release_arg)) {
			bool copied;

			if (smart_fusion_reply)
				copied = cluster_bufmgr_copy_block_for_gcs_smart_fusion(
					tag, out_page_lsn, (char *)scratch, out_sf_dep_vec);
			else
				copied = cluster_bufmgr_copy_block_for_gcs(tag, out_page_lsn, (char *)scratch,
														   out_copy_refusal);
			if (!copied) {
				if (smart_fusion_reply && out_copy_refusal != NULL)
					*out_copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_SMART_FUSION_UNCLASSIFIED;
				if (release_cb != NULL)
					release_cb(release_arg);
				return false;
			}
			gcs_block_note_scratch_copy();
			if (smart_fusion_reply && out_sf_dep_valid != NULL)
				*out_sf_dep_valid = true;
			*out_block_payload = (const char *)scratch;
			if (out_block_lkey != NULL)
				*out_block_lkey = scratch_lkey;
			if (out_release_cb != NULL)
				*out_release_cb = release_cb;
			if (out_release_arg != NULL)
				*out_release_arg = release_arg;
			return true;
		}
		if (allow_live_sge)
			gcs_block_note_live_sge_fallback();
	}

	if (smart_fusion_reply) {
		if (!cluster_bufmgr_copy_block_for_gcs_smart_fusion(tag, out_page_lsn, copy_buf,
															out_sf_dep_vec)) {
			if (out_copy_refusal != NULL)
				*out_copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_SMART_FUSION_UNCLASSIFIED;
			return false;
		}
		if (out_sf_dep_valid != NULL)
			*out_sf_dep_valid = true;
		gcs_block_note_scratch_copy();
	} else if (!cluster_bufmgr_copy_block_for_gcs(tag, out_page_lsn, copy_buf, out_copy_refusal))
		return false;
	else
		gcs_block_note_scratch_copy();
	*out_block_payload = copy_buf;
	if (out_block_lkey != NULL)
		*out_block_lkey = 0;
	return true;
}

static uint32
gcs_block_compute_invalidate_checksum(const GcsBlockInvalidatePayload *inv)
{
	const char *bytes = (const char *)inv;
	uint32 c = 0;
	size_t i;

	for (i = 0; i < offsetof(GcsBlockInvalidatePayload, checksum); i++)
		c = (c * 31u) + (uint8)bytes[i];
	return c;
}

static uint32
gcs_block_compute_invalidate_ack_checksum(const GcsBlockInvalidateAckPayload *ack)
{
	const char *bytes = (const char *)ack;
	uint32 c = 0;
	size_t checksum_off = offsetof(GcsBlockInvalidateAckPayload, checksum);
	size_t i;

	/* spec-2.37 D7 / spec-2.41 D3: ACK carries page_scn_bytes after checksum.
	 * Hash every payload byte except the checksum field itself so a stale or
	 * corrupted holder page_scn cannot advance the master detector SCN
	 * watermark (the @52 carrier is covered by this all-bytes-except-checksum
	 * hash). */
	for (i = 0; i < sizeof(GcsBlockInvalidateAckPayload); i++) {
		if (i >= checksum_off && i < checksum_off + sizeof(uint32))
			continue;
		c = (c * 31u) + (uint8)bytes[i];
	}
	return c;
}

/*
 * spec-4.7 D2 / spec-2.41 D3 — checksum over ALL GcsBlockRedeclarePayload bytes
 * EXCEPT the checksum field itself.  spec-4.7 originally covered only [0,48)
 * (page_lsn@28); spec-2.41 D3 adds page_scn@52 AFTER the checksum, so the
 * coverage was widened to all-bytes-except-checksum (the same pattern as the
 * invalidate ACK) — otherwise a corrupted holder page_scn could poison the
 * rebuilt detector SCN watermark (D3 mandatory; §4.3 P1-3 poison vector).
 */
static uint32
gcs_block_compute_redeclare_checksum(const GcsBlockRedeclarePayload *p)
{
	const char *bytes = (const char *)p;
	uint32 c = 0;
	size_t checksum_off = offsetof(GcsBlockRedeclarePayload, checksum);
	size_t i;

	for (i = 0; i < sizeof(GcsBlockRedeclarePayload); i++) {
		if (i >= checksum_off && i < checksum_off + sizeof(uint32))
			continue;
		c = (c * 31u) + (uint8)bytes[i];
	}
	return c;
}


/*
 * HC84:  install received block bytes into the requester's buffer under
 * content_lock EXCLUSIVE and preserve the image's origin-qualified page LSN.
 * Independent WAL streams are never numerically ordered across nodes.
 */
static void
PGRAC_PCM_X_FENCE_DOMINATED(cluster_bufmgr_pcm_x_content_write_permitted)
gcs_block_install_block(BufferDesc *buf, const char *block_data, XLogRecPtr page_lsn)
{
	LWLock *content_lock;
	Page page;

	Assert(buf != NULL);
	content_lock = BufferDescriptorGetContentLock(buf);

	LWLockAcquire(content_lock, LW_EXCLUSIVE);
	if (!cluster_bufmgr_pcm_x_content_write_permitted(buf)) {
		LWLockRelease(content_lock);
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("refusing to overwrite retained cluster PCM image"),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
								  "buffer=%d",
								  cluster_node_id, buf->buf_id)));
	}
	page = BufferGetPage(BufferDescriptorGetBuffer(buf));
	memcpy(page, block_data, GCS_BLOCK_DATA_SIZE);
	gcs_block_note_install_copy();
	PageSetLSNPreserveOrigin(page, page_lsn);
	/* The shipped image just proved these bytes current: a kept-pinned
	 * retained PI mirror regains CURRENT inside the same content-EXCLUSIVE
	 * hold, or the grant finish would refuse the frozen PI shape. */
	cluster_bufmgr_pcm_own_republish_grant_pending_image(buf);
	LWLockRelease(content_lock);
}


/* The network-fetch installer is the immutable-image half of the existing
 * queue reservation, not an ordinary writer.  Its exact GRANT_PENDING token
 * is deliberately nonzero until publish/commit, while writer activation is
 * still absent until that commit publishes it.  Require that sole legacy
 * reservation and an entirely absent Resource-X activation before touching
 * bytes.  The later target T2 owner captures these published bytes through
 * its own exact acquisition-generation proof. */
static bool
gcs_block_pcm_x_reserved_image_write_exact(const ClusterPcmOwnSnapshot *live,
											   const ClusterPcmOwnSnapshot *base,
											   uint64 reservation_token)
{
	return cluster_pcm_x_grant_reservation_kind(live, base, reservation_token)
			== CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW
		   && live->writer_activation_token == 0
		   && live->resource_x_activation_generation == 0
		   && base->writer_activation_token == 0
		   && base->resource_x_activation_generation == 0;
}


static void
gcs_block_install_reply_block(BufferDesc *buf, const char *block_data, XLogRecPtr page_lsn,
							  const ClusterGcsBlockOutstandingSlot *slot)
{
	if (slot != NULL && slot->reply_sf_dep_valid)
		cluster_sf_dep_install_vec(buf->tag, &slot->reply_sf_dep_vec);

	gcs_block_install_block(buf, block_data, page_lsn);

	if (slot != NULL && slot->reply_sf_dep_valid)
		cluster_sf_note_dep_touched(BufferDescriptorGetBuffer(buf));
}


/*
 * spec-5.14 D2 (class 2) — this backend just installed a Cache Fusion / GCS
 * block image shipped by one or two remote peers (the direct sender, and a
 * forwarding holder when the master forwarded a holder's image).  Record the
 * dependency so a fail-stop of any of them aborts this transaction (INV-TP2).
 * Read-only; never changes the block protocol.
 */
static inline void
gcs_block_stamp_touched(int32 sender_node, int32 forwarding_master)
{
	cluster_touched_peers_stamp(sender_node, CLUSTER_TOUCH_GCS_BLOCK);
	if (forwarding_master != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)
		cluster_touched_peers_stamp(forwarding_master, CLUSTER_TOUCH_GCS_BLOCK);
}


/*
 * PGRAC: GCS-race round-4c FUNC-1 — storage-fallback SCN verify / refresh.
 *
 *	A GRANTED_STORAGE_FALLBACK grant ships no page image: the requester is
 *	expected to use the shared-storage copy.  But the buffer bytes were
 *	pre-read by ReadBuffer BEFORE the acquire-gate negotiation, and the
 *	negotiation itself may have driven the live X holder through the BAST
 *	yield chain (X->S self-downgrade + FlushBuffer) — shared storage can
 *	then hold a NEWER version than the pre-read, and writing on the stale
 *	pre-read silently overwrites the flushed version (lost update; §2.6).
 *
 *	expected_scn is the master's authoritative pi_watermark_scn(tag)
 *	carried in the fallback reply's page_lsn field (the state=N site in
 *	gcs_block_produce_reply), or queried directly on the local-master
 *	tag-only grant paths (cluster_pcm_lock_acquire_buffer).  Decision:
 *
 *	  expected == InvalidScn  SKIP — old-binary master, holder re-ack
 *	                          (requester copy authoritative), or the block
 *	                          is not SCN-tracked.  Keep the local bytes
 *	                          (pre-fix behaviour).  A brand-new extension
 *	                          block always lands here, so the refresh can
 *	                          never smgrread past storage EOF.
 *	  local >= expected       PASS — local copy proven current; no I/O.
 *	  local stale/unstamped   discard + re-read the shared-storage page,
 *	                          then re-verdict: still below the watermark →
 *	                          53R93 fail-closed (action GUC: ERROR default,
 *	                          WARNING for staging diagnostics).
 *
 *	A DIRTY local copy is NEVER overwritten (the bufmgr helper refuses and
 *	we fail closed): real data dirt requires a covering X — those
 *	requesters take the holder re-ack fallbacks, which carry expected==0 —
 *	so dirt here is at most concurrent hint-bit dirt on a page whose
 *	staleness was just proven.  Flushing it would clobber the newer storage
 *	version and proceeding would lose the update, so ERROR is the only
 *	Rule-8.A-safe move (retry renegotiates from a clean slate).
 */
void
cluster_gcs_block_fallback_verify_refresh(BufferDesc *buf, BufferTag tag, SCN expected_scn)
{
	SCN page_scn;
	GcsLostWriteVerdict verdict;
	bool refreshed = false; /* S3 forensics — storage re-read happened */

	if (buf == NULL)
		return;

	/*
	 * fix 2 (crash-rejoin re-declare barrier, defense in depth): an InvalidScn
	 * master watermark normally SKIPs (not SCN-tracked / old-binary master /
	 * holder re-ack).  But if THIS self-home block is under the off-path crash-
	 * rejoin fence, the local GRD watermark was wiped by the restart, so an
	 * Invalid watermark can mask a stale home block — fail-closed instead of
	 * SKIP, except for a genuine extension block (never cross-node written).
	 * This is a second line behind the phase-gate boot barrier, which already
	 * fences the self-home block before the acquire reaches here.
	 */
	{
		bool self_fenced
			= (!cluster_online_join && cluster_gcs_lookup_master_static(tag) == cluster_node_id
			   && cluster_conf_node_count() > 1 && !cluster_grd_offpath_boot_decided());
		ClusterColdGrdVerdict cv = cluster_gcs_cold_grd_watermark_verdict(
			SCN_VALID(expected_scn), self_fenced,
			self_fenced && cluster_bufmgr_block_is_extension_for_gcs(tag));

		if (cv == CLUSTER_COLD_GRD_SKIP)
			return;
		if (cv == CLUSTER_COLD_GRD_FAIL_CLOSED) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->fallback_scn_failclosed_count, 1);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING),
					 errmsg("crash-rejoin: cannot prove home block ownership after restart "
							"(cold GRD watermark) for tag spc=%u db=%u rel=%u block=%u",
							tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
					 errhint("The block resource is recovering after an unclean restart; retry the "
							 "transaction, or enable cluster.online_join for an online re-declare "
							 "rejoin.")));
		}
		/* CLUSTER_COLD_GRD_PROVE: expected_scn valid — run the normal verdict. */
	}

	page_scn = cluster_bufmgr_read_block_scn_for_gcs(buf);
	verdict = gcs_block_lost_write_verdict(expected_scn, page_scn);
	if (verdict == GCS_LOST_WRITE_PASS) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->fallback_scn_verify_pass_count, 1);
		/* SCN proof: the local bytes (possibly a kept-pinned retained PI
		 * mirror) are at least the master watermark — republish CURRENT so
		 * the grant finish can commit over them. */
		cluster_bufmgr_pcm_own_republish_grant_pending_image(buf);
		return;
	}

	/* Local copy provably stale (or unstamped on a tracked tag): re-read. */
	if (cluster_bufmgr_refresh_block_from_storage_for_gcs(buf, &page_scn)) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->fallback_scn_refresh_count, 1);
		refreshed = true;

		/* Deterministic fail-closed drive (t/348 L7): pretend the storage
		 * copy came back unstamped (ANOMALY) — mirrors the master-direct
		 * cluster-gcs-block-stale-ship injection. */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-fallback-refresh-stale");
		if (cluster_injection_should_skip("cluster-gcs-block-fallback-refresh-stale"))
			page_scn = InvalidScn;

		verdict = gcs_block_lost_write_verdict(expected_scn, page_scn);
		if (verdict == GCS_LOST_WRITE_PASS) {
			/* Refreshed from shared storage and proven: same republish as
			 * the direct PASS proof above. */
			cluster_bufmgr_pcm_own_republish_grant_pending_image(buf);
			return;
		}
	}

	/* Refresh refused (dirty local copy) or the storage page is itself
	 * still below the master watermark: fail closed / staging WARN.
	 * S3 forensics step 1 — errdetail carries the verdict pair: refreshed
	 * distinguishes "shared-storage page itself below the watermark" (a
	 * true-lost-write signal: no replica reaches expected) from "dirty
	 * local copy refused refresh" (page_scn is then the pre-refresh local
	 * read). */
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->fallback_scn_failclosed_count, 1);
	{
		/* Step 1a — best-effort local provenance view: authoritative when this
		 * node masters the tag; otherwise the master's LOG line rules. */
		ClusterPcmWmProv wm_prov;
		bool wm_have = cluster_pcm_lock_pi_watermark_prov_query(tag, &wm_prov);

		if (cluster_gcs_block_lost_write_action == 0 /* ERROR */)
			ereport(
				ERROR,
				(errcode(ERRCODE_CLUSTER_LOST_WRITE_DETECTED),
				 errmsg("cluster_gcs_block: stale storage-fallback copy detected on tag "
						"spc=%u db=%u rel=%u block=%u",
						tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
				 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
						   "fork=%d expected pi_watermark_scn=" UINT64_FORMAT
						   " %s pd_block_scn=" UINT64_FORMAT
						   " local pi_watermark_scn=" UINT64_FORMAT " ownership_gen=" UINT64_FORMAT
						   " wm_src=%s wm_sender=%d wm_request_id=" UINT64_FORMAT
						   " wm_epoch=" UINT64_FORMAT " wm_old=" UINT64_FORMAT
						   " wm_new=" UINT64_FORMAT " wm_matches_expected=%d.",
						   cluster_node_id, (int)tag.forkNum, (uint64)expected_scn,
						   refreshed ? "storage" : "local(dirty-refused)", (uint64)page_scn,
						   (uint64)cluster_pcm_lock_pi_watermark_scn_query(tag),
						   cluster_pcm_own_gen_get(buf->buf_id),
						   wm_prov.table_full ? "none(prov-table-full)"
											  : cluster_pcm_wm_src_text(wm_prov.source),
						   wm_have ? wm_prov.sender_node : -1, wm_have ? wm_prov.request_id : 0,
						   wm_have ? wm_prov.epoch : 0, wm_have ? (uint64)wm_prov.old_scn : 0,
						   wm_have ? (uint64)wm_prov.new_scn : 0,
						   wm_have ? (int)(wm_prov.new_scn == expected_scn) : -1),
				 errhint("The local/storage page pd_block_scn is below the master "
						 "pi_watermark_scn carried by the GRANTED_STORAGE_FALLBACK "
						 "reply.  Inspect dump_gcs.fallback_scn_failclosed_count.  "
						 "Retry is safe (the next attempt renegotiates).")));
		ereport(WARNING,
				(errmsg("cluster_gcs_block: stale storage-fallback copy on tag "
						"spc=%u db=%u rel=%u block=%u (action=warn)",
						tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
				 errdetail("fork=%d expected pi_watermark_scn=" UINT64_FORMAT
						   " %s pd_block_scn=" UINT64_FORMAT
						   " local pi_watermark_scn=" UINT64_FORMAT " ownership_gen=" UINT64_FORMAT
						   " wm_src=%s wm_sender=%d wm_request_id=" UINT64_FORMAT
						   " wm_epoch=" UINT64_FORMAT " wm_old=" UINT64_FORMAT
						   " wm_new=" UINT64_FORMAT " wm_matches_expected=%d.",
						   (int)tag.forkNum, (uint64)expected_scn,
						   refreshed ? "storage" : "local(dirty-refused)", (uint64)page_scn,
						   (uint64)cluster_pcm_lock_pi_watermark_scn_query(tag),
						   cluster_pcm_own_gen_get(buf->buf_id),
						   wm_prov.table_full ? "none(prov-table-full)"
											  : cluster_pcm_wm_src_text(wm_prov.source),
						   wm_have ? wm_prov.sender_node : -1, wm_have ? wm_prov.request_id : 0,
						   wm_have ? wm_prov.epoch : 0, wm_have ? (uint64)wm_prov.old_scn : 0,
						   wm_have ? (uint64)wm_prov.new_scn : 0,
						   wm_have ? (int)(wm_prov.new_scn == expected_scn) : -1)));
	}
}


/* ============================================================
 * Sender API (D3).
 * ============================================================ */

/*
 * PGRAC: spec-2.34 D3 — sender retransmit loop with exponential backoff.
 *
 *	HC97 retry math:
 *	  attempt 0       initial send (no backoff)
 *	  retry 1..N      wait initial_backoff_ms × 2^(retry-1), resend
 *	  budget exhausted after retry N → ereport(ERROR, 53R90)
 *	  Default (N=4, initial=100):  100/200/400/800 ms, total backoff = 1500 ms
 *
 *	Status routing (HC94 + HC96):
 *	  GRANTED / STORAGE_FALLBACK   success, return
 *	  DENIED_INCOMPATIBLE / VALIDATOR_REJECT / CHECKSUM_FAIL /
 *	  MASTER_NOT_HOLDER             terminal, ereport
 *	  DENIED_EPOCH_STALE            re-lookup_master, retry within budget
 *	  DENIED_DEDUP_FULL             transient, retry within budget
 *	  timeout (no reply)            retry within budget
 *
 *	WARNING ereport at retry == N-1 ("budget 3/4") so DBA monitoring can
 *	alarm before exhaustion.  Pattern mirrors spec-2.27 GES retransmit.
 *
 *	HC98 budget exhausted SQLSTATE 53R90 — distinct from ERRCODE_QUERY_
 *	CANCELED so ops can differentiate GCS reliability failure from a
 *	backend cancellation.
 */

/* Compute backoff for retry attempt n (1-based;  n=1..max).  Returns ms. */
static long
gcs_block_backoff_ms_for_retry(int retry_attempt)
{
	long base;
	long shift;

	if (retry_attempt < 1)
		return 0;
	base = cluster_gcs_block_retransmit_initial_backoff_ms > 0
			   ? (long)cluster_gcs_block_retransmit_initial_backoff_ms
			   : 100L;
	/* attempt 1 → ×1, attempt 2 → ×2, attempt 3 → ×4, attempt 4 → ×8 ... */
	shift = retry_attempt - 1;
	if (shift > 16)
		shift = 16; /* defend against pathological max_retries */
	return base * (1L << shift);
}

/*
 * cluster_gcs_block_phase_for_tag -- spec-4.7 D1 block resource recovery phase.
 *
 *	Returns GCS_BLOCK_RECOVERING when this block's GCS master is a DEAD
 *	remote node: the master's volatile block-protocol state was lost with
 *	it and must be rebuilt (spec-4.7 D2/D3) before the block can be served.
 *	The bufmgr acquire gate fail-closes 53R9L for a RECOVERING block (see
 *	cluster_pcm_lock_acquire_buffer).
 *
 *	master == self (own master, or single-node fallback when declared_count
 *	<= 1) is GCS_BLOCK_NORMAL: a clean restart that lost the local master
 *	state rebuilds lazily on first request (spec-4.7 D3), not via this gate.
 *	Not cluster-active / PCM-inactive is always NORMAL (no block protocol).
 *
 *	NOTE: the survivor's CSSD must have CONVERGED on the master's DEAD edge
 *	for this to fire;  a node that just restarted optimistically sees a dead
 *	peer as alive until its own deadband re-fires (measure-first, spec-4.7
 *	D0 Impl note v0.1) — that window is the clean-restart path, not this one.
 */
ClusterGcsBlockPhase
cluster_gcs_block_phase_for_tag(BufferTag tag)
{
	int static_master;

	if (!cluster_pcm_is_active())
		return GCS_BLOCK_NORMAL;

	/*
	 * Gate on the STATIC declared master (the block's original master), NOT the
	 * recovery-aware routed master (which is already re-routed to a live
	 * survivor by D7).  Healthy operation: static master alive or self → NORMAL
	 * (unchanged).
	 */
	static_master = cluster_gcs_lookup_master_static(tag);

	/*
	 * TT lane / crash-rejoin re-declare barrier (Shape A) — off-path boot
	 * barrier.  With cluster.online_join=off a node that boots into a running
	 * cluster self-admits immediately (cluster_reconfig.c:206) with an EMPTY
	 * GRD and NO re-declare episode: for a block whose STATIC home is self,
	 * the acquire path would find master==self, read the empty local GRD, and
	 * cold-grant from the stale/empty disk page — a silent stale READ and a
	 * silently-diverging WRITE (the P0).  Until the off-path rejoin tick has
	 * classified this incarnation (crash-rejoin -> self-fence armed;
	 * bootstrap -> nothing), self cannot prove its home blocks' ownership, so
	 * fence them RECOVERING.  Both reads and writes reach this gate via
	 * cluster_pcm_lock_acquire_buffer, so this closes the boot-to-decision
	 * race with ZERO cold-serve window (Rule 8.A: uncertain -> fail-closed).
	 * Skipped for online_join=on (its admission + join fence govern) and for
	 * a single declared node (no peer can hold a conflicting copy).
	 */
	if (!cluster_online_join && static_master == cluster_node_id && cluster_conf_node_count() > 1
		&& !cluster_grd_offpath_boot_decided()) {
		cluster_grd_inc_join_block_failclosed();
		return GCS_BLOCK_RECOVERING;
	}

	/*
	 * spec-5.16 D3 (r1 P1-C) — online-join PCM block snap-back fence, placed
	 * BEFORE the non-DEAD-static-master early NORMAL below.  When a joiner (a
	 * non-DEAD static master) rejoins, block routing snaps its home blocks back
	 * to it the instant it is CSSD-ALIVE; if its block view is not yet rebuilt
	 * (survivors have not all re-declared their held joiner-home blocks), serving
	 * it cold would double-grant a block a survivor still holds X on (8.A).  Fence
	 * RECOVERING until the all-members re-declare barrier completes (view
	 * rebuilt — Hardening v1.1).  Bound to online_join via the armed fence epoch,
	 * INDEPENDENT of join_remaster_enabled (r2 P1-①).  This requester-side gate is
	 * the optimization; the master-side hard gate (cluster_gcs_handle_block_
	 * request_envelope) is the correctness backstop for stale-view requesters.
	 */
	if (cluster_grd_join_remaster_active_for_shard(tag) && !cluster_grd_block_view_rebuilt(tag)) {
		cluster_grd_inc_join_block_failclosed();
		return GCS_BLOCK_RECOVERING;
	}

	/*
	 * r3-P2-1 unseal-safety proof — this predicate is heartbeat LIVENESS
	 * (CSSD hysteresis flips DEAD->ALIVE on heartbeat receipt alone,
	 * cluster_cssd.c deadband scan), NOT a direct "instance recovery
	 * complete" signal.  It is nevertheless safe to return NORMAL here,
	 * because on a crash-restarted master the heartbeat source itself is
	 * recovery-gated:
	 *  (1) CSSD — the only heartbeat sender — is spawned by the cluster
	 *      phase-4 driver, which the postmaster reaper invokes only at the
	 *      PM_RUN transition, i.e. after the startup process exited 0 and
	 *      the node's crash recovery fully replayed its WAL thread to shared
	 *      storage (the ServerLoop respawn is equally PM_RUN-gated).  A
	 *      still-recovering node sends NO heartbeats, so a survivor's DEAD
	 *      verdict cannot flip back early.
	 *  (2) Belt-and-suspenders: even under a stale-ALIVE view (fast restart
	 *      inside the deadband), a fetch cannot complete against a
	 *      still-recovering node.  The master-side handler
	 *      (cluster_gcs_handle_block_request_envelope) default-denies unless
	 *      the node is an in-quorum MEMBER, and cluster_qvotec_in_quorum()
	 *      demands QUORUM_OK plus a live lease — state only the QVOTEC
	 *      process (phase-4 / post-PM_RUN as well) can establish after a
	 *      restart wiped shmem.  The deny replies map to bounded 53R9L; an
	 *      unresponsive endpoint exhausts the retransmit budget into bounded
	 *      53R90.  Neither path ever falls back to a silent local storage
	 *      read (STORAGE_FALLBACK is a master REPLY status, not a local
	 *      fallback).
	 *  (3) For online_join rejoin, MEMBER additionally requires coordinator
	 *      admission, which vets the joiner's post-recovery voting slot
	 *      (the slot_generation != 0 readiness sub-gate).
	 * So heartbeat-ALIVE implies the returned master completed its own
	 * instance recovery: its committed WAL is on shared storage and no
	 * merged-materialization proof is needed for its blocks.
	 */
	if (static_master == cluster_node_id
		|| cluster_cssd_get_peer_state(static_master) != CLUSTER_CSSD_PEER_DEAD)
		return GCS_BLOCK_NORMAL;

	/*
	 * spec-4.7 D7 (P0 code-review fix) — while this node's recovery FSM is
	 * mid-episode it has NOT yet seen every survivor's REDECLARE_DONE (now
	 * gated on their block re-declare scans completing), so a held block may
	 * not have been re-declared to its recovery-aware master yet.  Fence EVERY
	 * dead-static-master block RECOVERING for the whole episode — only once the
	 * episode reaches IDLE (all survivor scans complete) may the materialized
	 * + redo gate below decide NORMAL.  Without this, a held block scanned late
	 * would be served as cold mid-recovery → 8.A double-grant.
	 */
	if (cluster_grd_recovery_in_progress()) {
		if (ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_block_resources_recovering, 1);
		return GCS_BLOCK_RECOVERING;
	}

	/*
	 * spec-4.7 D7 + D5 — static master is DEAD;  the block is remastered to a
	 * live survivor (recovery-aware routing).  Two conditions are both
	 * required before the on-disk version may be served (Q5):
	 *  (a) is_materialized(origin):  the dead origin's merged replay completed
	 *      (publish is atomic at end-of-replay with the max EndRecPtr).  This
	 *      is the cold-block safety door — a block NO survivor observed has no
	 *      required_lsn to bound it, so the whole stream must be replayed
	 *      before the on-disk version is trusted current.
	 *  (b) redo_lsn_covered(origin, pi_watermark(tag)):  for a block some
	 *      survivor DID observe (rebuilt pi_watermark_lsn > 0), the dead
	 *      origin's recovered_lsn must reach that observed page_lsn — else the
	 *      dead node wrote a version a survivor saw but whose WAL never durably
	 *      reached us → lost-write → fail-closed.
	 *
	 * spec-4.6a Amendment v1.2 (R1):  this proof is UNCONDITIONAL.  Where
	 * online thread recovery cannot run (GUC off — the default — or any
	 * >2-node deployment) the materialization authority is never published,
	 * so a dead master's blocks stay RECOVERING until the failed node
	 * restarts and completes its own instance recovery (the unseal above is
	 * heartbeat liveness; the r3-P2-1 note explains why heartbeats imply
	 * recovery completion): a bounded, retryable
	 * ERROR on the request path (53R9L), never an unproven serve.  A scope
	 * predicate must never gate a correctness proof — a committed write on
	 * a cold block that only the dead node saw has NO other guard on this
	 * read path (GRD freeze ends with the episode; the HW gate covers only
	 * extend high-water marks; pd_block_scn checks ride the ship path).
	 */
	if (!cluster_merged_instance_is_materialized(static_master)) {
		if (ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_block_resources_recovering, 1);
		return GCS_BLOCK_RECOVERING;
	}
	if (!cluster_gcs_block_redo_lsn_covered(static_master,
											/* spec-2.41 §2.8 — redo-coverage uses the LSN watermark
											 * (per-stream replay position), NOT the detector SCN. */
											cluster_pcm_lock_pi_watermark_lsn_query(tag))) {
		/* materialized but a survivor observed a higher page_lsn than redo
		 * reached → lost-write boundary → fail-closed (53R9M class). */
		if (ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_before_boundary_failclosed, 1);
		return GCS_BLOCK_RECOVERING;
	}
	return GCS_BLOCK_NORMAL;
}

#define R4_CR_REQUIRED_HELLO_CAPS                                                        \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1        \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                                    \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)

/* A local R4 actor has no peer HELLO record.  Its compiled protocol family is
 * fixed by this binary, while the entered TARGET token binds that capability
 * to the locally committed OPEN/formation generation.  The final route
 * recheck remains the operation's freshness fence. */
static bool
gcs_block_r4_local_compiled_capability_matches(
	const ClusterSemanticAdmissionToken *token, uint32 required_capabilities,
	uint32 optional_capabilities, bool *optional_supported_out)
{
	uint32 compiled_capabilities
		= PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
		  | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
		  | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
		  | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1;

	if (!cluster_ic_suppress_gcs_done_cap)
		compiled_capabilities |= PGRAC_IC_HELLO_CAP_GCS_DONE_V1;
	if (optional_supported_out != NULL)
		*optional_supported_out
			= (compiled_capabilities & optional_capabilities) == optional_capabilities;
	return token != NULL && token->entered
		   && token->feature_bit == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		   && token->side == CLUSTER_SEMANTIC_TARGET_SIDE
		   && token->formation_epoch == cluster_epoch_get_current()
		   && (compiled_capabilities & required_capabilities) == required_capabilities;
}

/* The frozen R4 slot carrier stores capability generations as uint32, while
 * the local OPEN record generation is uint64.  Local actors have no peer
 * HELLO generation to substitute, so admit only an exact, nonzero checked
 * conversion under the same entered TARGET token. */
static bool
gcs_block_r4_local_capability_generation(
	const ClusterSemanticAdmissionToken *token, uint32 required_capabilities,
	uint32 optional_capabilities, bool *optional_supported_out,
	uint32 *generation_out)
{
	if (generation_out == NULL)
		return false;
	*generation_out = 0;
	if (!gcs_block_r4_local_compiled_capability_matches(
			token, required_capabilities, optional_capabilities,
			optional_supported_out)
		|| token->record_generation == 0
		|| token->record_generation > (uint64)PG_UINT32_MAX)
		return false;
	*generation_out = (uint32)token->record_generation;
	return true;
}

typedef struct GcsBlockR4ReplyExpectation {
	uint64 request_id;
	uint64 epoch;
	int32 requester_backend_id;
	uint8 transition_id;
	int32 sender_node;
	int32 forwarding_master_node;
	uint8 reply_domain;
} GcsBlockR4ReplyExpectation;

static bool gcs_block_decode_r4_reply_payload(
	const ClusterICEnvelope *env, const void *payload,
	const GcsBlockR4ReplyExpectation *expected) pg_attribute_unused();

/* Return true when D3 must publish an immediate refusal and false only for
 * the proved admitted FORWARD96 success.  Invalid result/reason pairs close
 * as status 26; they never inherit retry polarity from one member alone. */
static bool
gcs_block_r4_refusal_status_for_build(ClusterCrBuildResult result,
									  ClusterCrBuildReason reason, bool admitted_forward,
									  GcsBlockReplyStatus *status_out)
{
	if (status_out == NULL)
		return true;
	*status_out = GCS_BLOCK_REPLY_R4_DENIED;
	if (result == CLUSTER_CR_BUILD_FULL && reason == CLUSTER_CR_BUILD_NONE
		&& admitted_forward)
		return false;
	if (result == CLUSTER_CR_BUILD_RETRYABLE) {
		switch (reason) {
			case CLUSTER_CR_BUILD_TARGET_DISABLED:
			case CLUSTER_CR_BUILD_RF_DEFERRED:
			case CLUSTER_CR_BUILD_WRONG_MASTER:
			case CLUSTER_CR_BUILD_NO_HOLDER:
			case CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS:
			case CLUSTER_CR_BUILD_HOLDER_MOVED:
			case CLUSTER_CR_BUILD_RECOVERING:
			case CLUSTER_CR_BUILD_GENERATION_MISMATCH:
			case CLUSTER_CR_BUILD_CAPACITY:
			case CLUSTER_CR_BUILD_EPOCH_MISMATCH:
				*status_out = GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
				return true;
			default:
				break;
		}
	}
	if (result == CLUSTER_CR_BUILD_FAIL_CLOSED) {
		switch (reason) {
			case CLUSTER_CR_BUILD_BAD_LOCATOR:
			case CLUSTER_CR_BUILD_BAD_UNDO:
			case CLUSTER_CR_BUILD_CHAIN_LIMIT:
			case CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD:
			case CLUSTER_CR_BUILD_CANCELLED:
			case CLUSTER_CR_BUILD_IO_ERROR:
			case CLUSTER_CR_BUILD_PROTOCOL:
				return true;
			default:
				break;
		}
	}
	return true;
}

/* Exact D3 refusal decoder.  The independent expectation is supplied by the
 * R4 request slot owner; D3 exposes it to the focused unit seam while the R4
 * source slot remains a later deliverable.  Legacy reply mutation never calls
 * this path and rejects the entire 21..26 domain below. */
static bool
gcs_block_decode_r4_reply_payload(const ClusterICEnvelope *env, const void *payload,
								  const GcsBlockR4ReplyExpectation *expected)
{
	const GcsBlockReplyHeader *header;
	const char *block_data;
	const ClusterGcsUndoAuthTrailer *undo_auth = NULL;
	GcsBlockReplyStatus status;
	uint32 payload_size;
	int i;

	if (env == NULL || payload == NULL || expected == NULL
		|| env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
		|| env->payload_length < (uint32)sizeof(GcsBlockReplyHeader)
		|| env->source_node_id != (uint32)expected->sender_node
		|| env->dest_node_id != (uint32)cluster_node_id
		|| expected->reply_domain != CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR
		|| expected->sender_node < 0 || expected->sender_node >= CLUSTER_MAX_NODES
		|| expected->forwarding_master_node < GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| expected->forwarding_master_node >= CLUSTER_MAX_NODES)
		return false;
	header = (const GcsBlockReplyHeader *)payload;
	block_data = ((const char *)payload) + sizeof(*header);
	status = (GcsBlockReplyStatus)header->status;
	switch (status) {
		case GCS_BLOCK_REPLY_R4_CR_FULL:
		case GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT:
		case GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED:
		case GCS_BLOCK_REPLY_R4_DENIED:
			payload_size = GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE;
			break;
		case GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT:
			payload_size = GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE
						   + (uint32)sizeof(ClusterGcsUndoAuthTrailer);
			break;
		default:
			return false;
	}
	if (env->payload_length != payload_size || header->request_id != expected->request_id
		|| header->epoch != expected->epoch
		|| header->requester_backend_id != expected->requester_backend_id
		|| header->transition_id != expected->transition_id
		|| expected->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| header->sender_node != expected->sender_node
		|| GcsBlockReplyHeaderGetForwardingMasterNode(header)
			   != expected->forwarding_master_node
		|| header->checksum != gcs_block_compute_checksum(block_data))
		return false;
	if (status == GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT) {
		uint32 physical_generation;

		if (!GcsBlockReplyHeaderGetR4UndoGeneration(header, &physical_generation))
			return false;
	} else {
		for (i = 0; i < (int)sizeof(header->reserved_0); i++)
			if (header->reserved_0[i] != 0)
				return false;
	}

	if (status == GCS_BLOCK_REPLY_R4_CR_FULL)
		return expected->forwarding_master_node != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
	if (status == GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT)
		return expected->forwarding_master_node
				   == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
			   && expected->requester_backend_id > 0
			   && header->page_lsn != 0;
	if (status == GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT) {
		if (expected->requester_backend_id != CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT
			|| expected->forwarding_master_node != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
			|| header->page_lsn == 0)
			return false;
		undo_auth = (const ClusterGcsUndoAuthTrailer *)(block_data + GCS_BLOCK_DATA_SIZE);
		return ClusterGcsUndoAuthTrailerGetTtGeneration(undo_auth) != 0
			   && ClusterGcsUndoAuthTrailerGetAuthorityScn(undo_auth) != 0;
	}

	for (i = 0; i < GCS_BLOCK_DATA_SIZE; i++)
		if (block_data[i] != 0)
			return false;
	if (status == GCS_BLOCK_REPLY_R4_DENIED)
		return header->page_lsn == 0;
	if (expected->forwarding_master_node != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)
		return header->page_lsn == 0;
	return header->page_lsn <= (uint64)CLUSTER_MAX_NODES;
}

static bool
gcs_block_r4_publish_refusal(int worker_id, const ClusterICEnvelope *env,
							 const ClusterR4CrRequestPayload *request,
							 uint32 requester_capability_generation,
							 ClusterCrBuildResult result, ClusterCrBuildReason reason,
							 bool admitted_forward, int32 current_master_node)
{
	GcsBlockReplyHeader header;
	GcsBlockReplyStatus status;

	if (!gcs_block_r4_refusal_status_for_build(result, reason, admitted_forward, &status))
		return true;
	memset(&header, 0, sizeof(header));
	header.request_id = request->base.request_id;
	header.epoch = request->base.epoch;
	header.sender_node = cluster_node_id;
	header.requester_backend_id = request->base.requester_backend_id;
	header.transition_id = request->base.transition_id;
	header.status = (uint8)status;
	if (status == GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED
		&& reason == CLUSTER_CR_BUILD_WRONG_MASTER && current_master_node >= 0
		&& current_master_node < CLUSTER_MAX_NODES)
		header.page_lsn = (uint64)(uint32)(current_master_node + 1);
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	return cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
		worker_id, env->source_node_id, &header, R4_CR_REQUIRED_HELLO_CAPS,
		requester_capability_generation);
}

/*
 * A holder-produced pre-publication refusal is addressed directly to the
 * original requester and retains the real forwarding master.  This identity
 * is intentionally distinct from the D3 master-refusal shape above.
 */
static bool
gcs_block_r4_publish_holder_refusal(int worker_id,
								const ClusterR4CrForwardPayload *forward,
								uint32 requester_capability_generation,
								ClusterCrBuildResult result,
								ClusterCrBuildReason reason)
{
	GcsBlockReplyHeader header;
	GcsBlockReplyStatus status;

	if (forward == NULL
		|| !gcs_block_r4_refusal_status_for_build(result, reason, true, &status))
		return true;
	memset(&header, 0, sizeof(header));
	header.request_id = forward->base.request_id;
	header.epoch = forward->base.epoch;
	header.sender_node = cluster_node_id;
	header.requester_backend_id = forward->base.requester_backend_id;
	header.transition_id = forward->base.transition_id;
	header.status = (uint8)status;
	GcsBlockReplyHeaderSetForwardingMasterNode(&header, forward->base.master_node);
	return cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
		worker_id, (uint32)forward->base.original_requester_node, &header,
		R4_CR_REQUIRED_HELLO_CAPS, requester_capability_generation);
}

static bool
gcs_block_r4_request_base_valid(const ClusterICEnvelope *env,
								const ClusterR4CrRequestPayload *request,
								uint64 current_epoch, SCN *read_scn_out)
{
	int i;

	if (read_scn_out != NULL)
		*read_scn_out = InvalidScn;
	if (env == NULL || request == NULL || read_scn_out == NULL
		|| env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REQUEST
		|| env->payload_length != sizeof(*request)
		|| env->source_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| env->dest_node_id != (uint32)cluster_node_id
		|| request->base.sender_node != (int32)env->source_node_id
		|| request->base.request_id == 0 || request->base.requester_backend_id <= 0
		|| request->base.requester_backend_id > MaxBackends
		|| request->base.transition_id != (uint8)PCM_TRANS_N_TO_S
		|| request->base.epoch != env->epoch || request->base.epoch != current_epoch
		|| request->base.reserved_0[0] != 0 || request->base.reserved_0[1] != 0
		|| !ClusterR4RequestExtensionGetCr(&request->extension, read_scn_out))
		return false;
	for (i = 6; i < (int)sizeof(request->base.reserved_0); i++)
		if (request->base.reserved_0[i] != 0)
			return false;
	return true;
}

/* Stage 8 R4 D3 request-level TARGET wrapper.  One admission token dominates
 * the same-OPEN join, sole PCM sample, typed route arm, cap-bound enqueue,
 * send publication and final recheck. */
ClusterCrBuildResult
cluster_gcs_block_r4_route_cr(const ClusterICEnvelope *env,
							   const ClusterR4CrRequestPayload *request,
							   ClusterCrBuildReason *reason_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	PcmAuthoritySnapshot authority;
	GcsBlockR4RouteIdentity identity;
	ClusterR4CrRouteProof fresh_proof;
	GcsBlockR4RouteRecord stored_record;
	ClusterR4CrForwardPayload forward;
	uint64 current_epoch;
	uint64 master_authority_generation;
	SCN read_scn;
	SCN expected_page_scn;
	uint32 requester_capability_generation = 0;
	uint32 holder_capability_generation = 0;
	bool requester_done_capable = false;
	bool holder_optional = false;
	bool outbound_admitted = false;
	bool final_recheck_ok;
	bool admitted_forward;
	bool requester_is_local;
	bool holder_is_local;
	int32 real_master_node = -1;
	int32 current_holder_node = -1;
	int dedup_worker_id;
	GcsBlockR4RouteArmResult arm_result;
	GcsBlockR4RouteSendResult send_result = GCS_BLOCK_R4_ROUTE_SEND_INVALID;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	ClusterCrBuildResult result = CLUSTER_CR_BUILD_FAIL_CLOSED;

	if (reason_out != NULL)
		*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
	memset(&admission, 0, sizeof(admission));
	if (env == NULL || request == NULL || reason_out == NULL)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	dedup_worker_id = cluster_ic_tier1_my_data_channel();
	requester_is_local = (int32)env->source_node_id == cluster_node_id;

	if (!requester_is_local
		&& !cluster_sf_peer_capability_family_sample(
			(int32)env->source_node_id, R4_CR_REQUIRED_HELLO_CAPS,
			PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &requester_done_capable,
			&requester_capability_generation)) {
		*reason_out = CLUSTER_CR_BUILD_TARGET_DISABLED;
		return CLUSTER_CR_BUILD_RETRYABLE;
	}
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		reason = admission_result == CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
					 ? CLUSTER_CR_BUILD_TARGET_DISABLED
					 : CLUSTER_CR_BUILD_RF_DEFERRED;
		result = cluster_cr_build_result_for_reason(reason);
		(void)gcs_block_r4_publish_refusal(
			dedup_worker_id, env, request, requester_capability_generation, result, reason,
			false, -1);
		*reason_out = reason;
		return result;
	}

	PG_TRY();
	{
		if (requester_is_local
			? !gcs_block_r4_local_compiled_capability_matches(
				  &admission, R4_CR_REQUIRED_HELLO_CAPS,
				  PGRAC_IC_HELLO_CAP_GCS_DONE_V1, &requester_done_capable)
			: !cluster_semantic_activation_peer_open_matches(
				  &admission, (int32)env->source_node_id, R4_CR_REQUIRED_HELLO_CAPS,
				  requester_capability_generation)) {
			reason = CLUSTER_CR_BUILD_RF_DEFERRED;
			goto done;
		}
		current_epoch = cluster_epoch_get_current();
		if (!gcs_block_r4_request_base_valid(env, request, current_epoch, &read_scn)) {
			reason = CLUSTER_CR_BUILD_PROTOCOL;
			goto done;
		}

		real_master_node = cluster_gcs_lookup_master(request->base.tag);
		if (real_master_node < 0 || real_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| cluster_node_id != real_master_node) {
			reason = CLUSTER_CR_BUILD_WRONG_MASTER;
			goto done;
		}
		if (cluster_gcs_block_phase_for_tag(request->base.tag) == GCS_BLOCK_RECOVERING) {
			reason = CLUSTER_CR_BUILD_RECOVERING;
			goto done;
		}
		if (!cluster_pcm_lock_r4_route_snapshot(request->base.tag, &authority,
												&master_authority_generation,
												&expected_page_scn)) {
			reason = CLUSTER_CR_BUILD_NO_HOLDER;
			goto done;
		}
		reason = cluster_r4_route_policy_classify(&authority, current_epoch,
											  master_authority_generation,
											  &current_holder_node);
		if (reason != CLUSTER_CR_BUILD_NONE)
			goto done;

		holder_is_local = current_holder_node == cluster_node_id;
		if (holder_is_local
			? !gcs_block_r4_local_compiled_capability_matches(
				  &admission, R4_CR_REQUIRED_HELLO_CAPS, 0, &holder_optional)
			: (!cluster_sf_peer_capability_family_sample(
				   current_holder_node, R4_CR_REQUIRED_HELLO_CAPS, 0, &holder_optional,
				   &holder_capability_generation)
			   || !cluster_semantic_activation_peer_open_matches(
					   &admission, current_holder_node, R4_CR_REQUIRED_HELLO_CAPS,
					   holder_capability_generation))) {
			reason = CLUSTER_CR_BUILD_RF_DEFERRED;
			goto done;
		}

		memset(&identity, 0, sizeof(identity));
		identity.legacy_key.origin_node_id = env->source_node_id;
		identity.legacy_key.requester_backend_id = request->base.requester_backend_id;
		identity.legacy_key.request_id = request->base.request_id;
		identity.legacy_key.cluster_epoch = current_epoch;
		identity.tag = request->base.tag;
		identity.read_scn = read_scn;
		identity.activation_generation = admission.record_generation;

		memset(&fresh_proof, 0, sizeof(fresh_proof));
		fresh_proof.tag = request->base.tag;
		fresh_proof.read_scn = read_scn;
		fresh_proof.formation_epoch = current_epoch;
		fresh_proof.activation_generation = admission.record_generation;
		fresh_proof.master_authority_generation = master_authority_generation;
		fresh_proof.master_resource_transition_count = authority.transition_count;
		fresh_proof.expected_page_scn = expected_page_scn;
		fresh_proof.real_master_node = real_master_node;
		fresh_proof.selected_holder_node = current_holder_node;

		if (dedup_worker_id < 0 || dedup_worker_id >= cluster_lms_workers
			|| cluster_lms_shard_for_tag(&identity.tag, cluster_lms_workers)
				   != dedup_worker_id) {
			reason = CLUSTER_CR_BUILD_PROTOCOL;
			goto done;
		}
		arm_result = cluster_gcs_block_dedup_r4_route_arm_or_match(
			dedup_worker_id, &identity, (uint8)PCM_TRANS_N_TO_S, &fresh_proof,
			GcsBlockRequestPayloadGetLifetimeHintMs(&request->base), requester_done_capable,
			&stored_record);
		if (arm_result != GCS_BLOCK_R4_ROUTE_ARM_NEW
			&& arm_result != GCS_BLOCK_R4_ROUTE_ARM_REPLAY) {
			reason = arm_result == GCS_BLOCK_R4_ROUTE_ARM_FULL ? CLUSTER_CR_BUILD_CAPACITY
					 : arm_result == GCS_BLOCK_R4_ROUTE_ARM_INVALID
					 ? CLUSTER_CR_BUILD_PROTOCOL
					 : CLUSTER_CR_BUILD_HOLDER_MOVED;
			goto done;
		}
		if (arm_result == GCS_BLOCK_R4_ROUTE_ARM_NEW)
			cluster_r4_observe(CLUSTER_R4_EVENT_CR_ROUTE_STARTED,
						   CLUSTER_TX_RESOLVE_NONE, CLUSTER_CR_BUILD_NONE);

		memset(&forward, 0, sizeof(forward));
		forward.base.request_id = identity.legacy_key.request_id;
		forward.base.epoch = stored_record.proof.formation_epoch;
		forward.base.tag = stored_record.proof.tag;
		forward.base.original_requester_node = (int32)identity.legacy_key.origin_node_id;
		forward.base.requester_backend_id = identity.legacy_key.requester_backend_id;
		forward.base.master_node = stored_record.proof.real_master_node;
		forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&forward.base,
												 stored_record.proof.read_scn);
		GcsBlockForwardPayloadSetCrRequest(&forward.base, true);
		ClusterR4ForwardExtensionSetCrProof(
			&forward.extension, stored_record.proof.master_authority_generation,
			stored_record.proof.master_resource_transition_count,
			stored_record.proof.expected_page_scn);
		if (holder_is_local) {
			ClusterICSendResult local_send_result;

			local_send_result = gcs_block_send_envelope_or_loopback(
				PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
				stored_record.proof.selected_holder_node, &forward, sizeof(forward));
			outbound_admitted = local_send_result == CLUSTER_IC_SEND_DONE;
		} else
			outbound_admitted = cluster_lms_outbound_enqueue_cap_bound(
				dedup_worker_id, PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
				(uint32)stored_record.proof.selected_holder_node, &forward, sizeof(forward),
				R4_CR_REQUIRED_HELLO_CAPS, holder_capability_generation);
		send_result = cluster_gcs_block_dedup_r4_route_finish_send(
			dedup_worker_id, &identity, (uint8)PCM_TRANS_N_TO_S, &stored_record.proof,
			outbound_admitted);
		if (!outbound_admitted || send_result != GCS_BLOCK_R4_ROUTE_SEND_FORWARDED) {
			reason = send_result == GCS_BLOCK_R4_ROUTE_SEND_INVALID
						 ? CLUSTER_CR_BUILD_PROTOCOL
						 : CLUSTER_CR_BUILD_HOLDER_MOVED;
			goto done;
		}
		reason = CLUSTER_CR_BUILD_NONE;

done:
		final_recheck_ok = cluster_semantic_activation_recheck(&admission);
		if (!final_recheck_ok && reason == CLUSTER_CR_BUILD_NONE)
			reason = CLUSTER_CR_BUILD_RF_DEFERRED;
		result = cluster_cr_build_result_for_reason(reason);
		admitted_forward = final_recheck_ok && reason == CLUSTER_CR_BUILD_NONE
						   && outbound_admitted
						   && send_result == GCS_BLOCK_R4_ROUTE_SEND_FORWARDED;
		(void)gcs_block_r4_publish_refusal(
			dedup_worker_id, env, request, requester_capability_generation, result, reason,
			admitted_forward, real_master_node);
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();

	*reason_out = reason;
	return result;
}

static bool
gcs_block_try_r4_request80(const ClusterICEnvelope *env, const void *payload)
{
	ClusterCrBuildReason reason;

	if (env == NULL || payload == NULL || env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REQUEST
		|| env->payload_length != sizeof(ClusterR4CrRequestPayload))
		return false;
	(void)cluster_gcs_block_r4_route_cr(
		env, (const ClusterR4CrRequestPayload *)payload, &reason);
	return true;
}

static bool
gcs_block_try_r4_forward96(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterR4CrForwardPayload *forward = (const ClusterR4CrForwardPayload *)payload;
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterCrBuildResult submit_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
	ClusterCrBuildReason submit_reason = CLUSTER_CR_BUILD_PROTOCOL;
	uint32 requester_capability_generation = 0;
	uint32 master_capability_generation = 0;
	uint64 master_authority_generation;
	uint64 master_resource_transition_count;
	SCN expected_page_scn;
	int worker_id;
	bool master_is_local;
	bool requester_is_local;
	bool master_optional_supported = false;
	bool requester_optional_supported = false;

	if (env == NULL || payload == NULL || env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		|| env->payload_length != sizeof(ClusterR4CrForwardPayload))
		return false;
	/* Candidate-2 kind-2 is the sole pre-OPEN FORWARD96 subdomain.  Classify
	 * it before ordinary TARGET admission so malformed kind-2 frames are
	 * consumed fail-closed and can never fall through as holder CR work. */
	if (forward->extension.r4_version == CLUSTER_R4_WIRE_VERSION
		&& forward->extension.r4_kind == (uint8)CLUSTER_R4_WIRE_TX_RESOLVE)
		return gcs_block_r4_tx_origin_try_accept(env, forward);
	memset(&admission, 0, sizeof(admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return true;

	PG_TRY();
	{
		master_is_local = (int32)env->source_node_id == cluster_node_id;
		requester_is_local
			= forward->base.original_requester_node == cluster_node_id;
		if (master_is_local
				? !gcs_block_r4_local_capability_generation(
					  &admission, R4_CR_REQUIRED_HELLO_CAPS, 0,
					  &master_optional_supported, &master_capability_generation)
				: !cluster_sf_peer_capability_family_sample(
					  (int32)env->source_node_id, R4_CR_REQUIRED_HELLO_CAPS, 0,
					  &master_optional_supported, &master_capability_generation))
			goto done;
		if (requester_is_local
				? !gcs_block_r4_local_capability_generation(
					  &admission, R4_CR_REQUIRED_HELLO_CAPS, 0,
					  &requester_optional_supported,
					  &requester_capability_generation)
				: (!cluster_sf_peer_capability_family_sample(
					   forward->base.original_requester_node,
					   R4_CR_REQUIRED_HELLO_CAPS, 0,
					   &requester_optional_supported,
					   &requester_capability_generation)))
			goto done;
		if ((!master_is_local
			 && !cluster_semantic_activation_peer_open_matches(
				 &admission, (int32)env->source_node_id,
				 R4_CR_REQUIRED_HELLO_CAPS, master_capability_generation))
			|| (!requester_is_local
				&& !cluster_semantic_activation_peer_open_matches(
					&admission, forward->base.original_requester_node,
					R4_CR_REQUIRED_HELLO_CAPS,
					requester_capability_generation)))
			goto done;
		if (env->source_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| forward->base.master_node != (int32)env->source_node_id
			|| forward->base.original_requester_node < 0
			|| forward->base.original_requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| forward->base.requester_backend_id <= 0
			|| forward->base.requester_backend_id > MaxBackends
			|| forward->base.request_id == 0
			|| forward->base.transition_id != (uint8)PCM_TRANS_N_TO_S
			|| forward->base.epoch != env->epoch
			|| forward->base.epoch != cluster_epoch_get_current()
			|| cluster_gcs_lookup_master(forward->base.tag) != forward->base.master_node
			|| !GcsBlockForwardPayloadIsCrRequest(&forward->base)
			|| forward->base.reserved_0[0] != 0 || forward->base.reserved_0[1] != 0
			|| forward->base.reserved_0[2] != 0 || forward->base.reserved_0[3] != 0
			|| forward->base.reserved_0[4] != 1 || forward->base.reserved_0[5] != 0
			|| forward->base.reserved_0[6] != 0
			|| !SCN_VALID(GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&forward->base))
			|| !ClusterR4ForwardExtensionGetCrProof(
				&forward->extension, forward->base.epoch, &master_authority_generation,
				&master_resource_transition_count, &expected_page_scn))
			goto done;

		submit_result = cluster_lms_cr_submit_r4(
			forward, &admission, requester_capability_generation,
			master_capability_generation, &submit_reason);
		if (!cluster_semantic_activation_recheck(&admission))
			goto done;
		worker_id = cluster_ic_tier1_my_data_channel();
		(void)gcs_block_r4_publish_holder_refusal(
			worker_id, forward, requester_capability_generation, submit_result,
			submit_reason);

done:
		;
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();
	return true;
}

#ifdef USE_CLUSTER_UNIT
bool
cluster_gcs_block_test_r4_request80(const ClusterICEnvelope *env, const void *payload)
{
	return gcs_block_try_r4_request80(env, payload);
}

bool
cluster_gcs_block_test_r4_forward96(const ClusterICEnvelope *env, const void *payload)
{
	return gcs_block_try_r4_forward96(env, payload);
}

int
cluster_gcs_block_test_r4_tx_origin_context_count(void)
{
	int count = 0;
	int i;

	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++)
		if (gcs_block_r4_tx_origin_contexts[i].in_use)
			count++;
	return count;
}

void
cluster_gcs_block_test_r4_tx_origin_drain(void)
{
	cluster_gcs_block_r4_tx_resolve_drain();
}

bool
cluster_gcs_block_test_r4_refusal_status(ClusterCrBuildResult result,
									 ClusterCrBuildReason reason, bool admitted_forward,
									 GcsBlockReplyStatus *status_out)
{
	return gcs_block_r4_refusal_status_for_build(result, reason, admitted_forward, status_out);
}

bool
cluster_gcs_block_test_decode_r4_reply(
	const ClusterICEnvelope *env, const void *payload, uint64 expected_request_id,
	uint64 expected_epoch, int32 expected_requester_backend_id, uint8 expected_transition_id,
	int32 expected_sender_node, int32 expected_forwarding_master_node,
	uint8 expected_reply_domain)
{
	GcsBlockR4ReplyExpectation expected;

	memset(&expected, 0, sizeof(expected));
	expected.request_id = expected_request_id;
	expected.epoch = expected_epoch;
	expected.requester_backend_id = expected_requester_backend_id;
	expected.transition_id = expected_transition_id;
	expected.sender_node = expected_sender_node;
	expected.forwarding_master_node = expected_forwarding_master_node;
	expected.reply_domain = expected_reply_domain;
	return gcs_block_decode_r4_reply_payload(env, payload, &expected);
}

/* A single-backend real-slot fixture for the registered reply handler.  The
 * private slot layout remains owned by this translation unit; focused tests
 * can arm and inspect it without exporting the shared-memory ABI. */
static ClusterGcsBlockShared cluster_gcs_block_test_reply_shared;
static ClusterGcsBlockBackendBlock cluster_gcs_block_test_reply_backend;

static void
gcs_block_test_reset_r4_reply_table(uint64 next_sequence)
{
	memset(&cluster_gcs_block_test_reply_shared, 0,
		   sizeof(cluster_gcs_block_test_reply_shared));
	memset(&cluster_gcs_block_test_reply_backend, 0,
		   sizeof(cluster_gcs_block_test_reply_backend));
	pg_atomic_init_u64(&cluster_gcs_block_test_reply_shared.stale_reply_drop_count, 0);
	cluster_gcs_block_test_reply_backend.next_request_id = next_sequence;
	ClusterGcsBlock = &cluster_gcs_block_test_reply_shared;
	gcs_block_backend_blocks = &cluster_gcs_block_test_reply_backend;
	cluster_gcs_block_test_requester_slot = NULL;
}

bool
cluster_gcs_block_test_arm_r4_reply_slot(uint64 request_id, uint64 request_epoch,
										 int32 requester_backend_id,
										 uint8 transition_id,
										 int32 expected_master_node)
{
	ClusterGcsBlockOutstandingSlot *slot;

	if (requester_backend_id != 1)
		return false;
	gcs_block_test_reset_r4_reply_table(UINT64_C(1));
	slot = &cluster_gcs_block_test_reply_backend.slots[0];
	slot->in_use = true;
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
	slot->request_id = request_id;
	slot->transition_id = transition_id;
	slot->request_epoch = request_epoch;
	slot->master_node = expected_master_node;
	slot->expected_master_node = expected_master_node;
	slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
	return true;
}

bool
cluster_gcs_block_test_arm_legacy_reply_slot(uint64 request_id,
											uint64 request_epoch,
											int32 requester_backend_id,
											uint8 transition_id,
											int32 expected_master_node)
{
	ClusterGcsBlockOutstandingSlot *slot;

	if (requester_backend_id != 1)
		return false;
	gcs_block_test_reset_r4_reply_table(UINT64_C(1));
	slot = &cluster_gcs_block_test_reply_backend.slots[0];
	slot->in_use = true;
	slot->reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
	slot->request_id = request_id;
	slot->transition_id = transition_id;
	slot->request_epoch = request_epoch;
	slot->master_node = expected_master_node;
	slot->expected_master_node = expected_master_node;
	slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
	return true;
}

bool
cluster_gcs_block_test_r4_requester_arm(BufferTag tag, uint64 request_epoch,
										int32 expected_master_node,
										uint64 next_sequence, uint64 *request_id_out)
{
	gcs_block_test_reset_r4_reply_table(next_sequence);
	cluster_gcs_block_test_requester_slot = gcs_block_try_reserve_r4_slot(
		tag, request_epoch, expected_master_node, request_id_out);
	return cluster_gcs_block_test_requester_slot != NULL;
}

bool
cluster_gcs_block_test_snapshot_r4_requester_slot(
	bool *in_use_out, uint8 *reply_domain_out, uint64 *request_id_out,
	uint8 *transition_id_out, BufferTag *tag_out, uint64 *request_epoch_out,
	int32 *expected_master_node_out, ClusterGcsBlockDirectState *direct_state_out,
	bool *direct_target_prepared_out)
{
	ClusterGcsBlockOutstandingSlot *slot = cluster_gcs_block_test_requester_slot;

	if (slot == NULL || in_use_out == NULL || reply_domain_out == NULL
		|| request_id_out == NULL || transition_id_out == NULL || tag_out == NULL
		|| request_epoch_out == NULL || expected_master_node_out == NULL
		|| direct_state_out == NULL || direct_target_prepared_out == NULL)
		return false;
	*in_use_out = slot->in_use;
	*reply_domain_out = slot->reply_domain;
	*request_id_out = slot->request_id;
	*transition_id_out = slot->transition_id;
	*tag_out = slot->tag;
	*request_epoch_out = slot->request_epoch;
	*expected_master_node_out = slot->expected_master_node;
	*direct_state_out = slot->direct_state;
	*direct_target_prepared_out = slot->direct_target_prepared;
	return true;
}

bool
cluster_gcs_block_test_release_r4_requester_slot(void)
{
	if (cluster_gcs_block_test_requester_slot == NULL)
		return false;
	gcs_block_release_slot(cluster_gcs_block_test_requester_slot);
	return true;
}

bool
cluster_gcs_block_test_r4_fetch_and_wait(BufferTag tag, SCN read_scn,
										 int32 real_master_node,
										 char dst_page[GCS_BLOCK_DATA_SIZE])
{
	ClusterCrBuildReason reason;

	gcs_block_test_reset_r4_reply_table(UINT64_C(1));
	return gcs_block_r4_cr_fetch_and_wait_raw(
			   tag, read_scn, real_master_node, dst_page, &reason)
		   == CLUSTER_CR_BUILD_FULL;
}

bool
cluster_gcs_block_test_snapshot_r4_reply_slot(GcsBlockReplyHeader *header_out,
										  char block_out[GCS_BLOCK_DATA_SIZE],
										  bool *reply_received_out,
										  uint64 *stale_drop_count_out)
{
	ClusterGcsBlockOutstandingSlot *slot
		= &cluster_gcs_block_test_reply_backend.slots[0];

	if (!slot->in_use || header_out == NULL || block_out == NULL
		|| reply_received_out == NULL || stale_drop_count_out == NULL)
		return false;
	*header_out = slot->reply_header;
	memcpy(block_out, slot->reply_block_data, GCS_BLOCK_DATA_SIZE);
	*reply_received_out = slot->reply_received;
	*stale_drop_count_out
		= pg_atomic_read_u64(&cluster_gcs_block_test_reply_shared.stale_reply_drop_count);
	return true;
}
#endif

#undef R4_CR_REQUIRED_HELLO_CAPS

/*
 * cluster_gcs_block_redo_lsn_covered -- spec-4.7 D5 redo-before-unfreeze gate
 * (Q5, the core safety门).
 *
 *	True iff the dead origin's merged WAL recovery on THIS node has reached at
 *	least required_lsn — the survivor's observed max page_lsn for the block
 *	(PI watermark / re-declare).  recovered_through(origin) < required_lsn
 *	means the dead node wrote a version a survivor already saw but whose WAL
 *	has NOT been merged here yet → lost-write → the block must stay
 *	fail-closed (53R9M), NEVER served (a stale shared page).  This is the LSN
 *	comparison the spec demands (Q5):  a bool "marker exists" gate is too soft
 *	— it cannot prove redo covered the version a survivor already observed.
 *	required_lsn == 0 (no observed version) is trivially covered;  a missing /
 *	torn marker yields recovered_through == 0, so any required_lsn > 0 is
 *	NOT covered (fail-closed).
 */
bool
cluster_gcs_block_redo_lsn_covered(int dead_origin, XLogRecPtr required_lsn)
{
	bool covered;
	bool required_lsn_zero = XLogRecPtrIsInvalid(required_lsn);

	if (required_lsn_zero)
		covered = true;
	else
		covered = cluster_merged_instance_recovered_through(dead_origin) >= (uint64)required_lsn;

	if (ClusterGcsBlock != NULL) {
		pg_atomic_fetch_add_u64(covered ? &ClusterGcsBlock->recovery_redo_boundary_reached
										: &ClusterGcsBlock->recovery_redo_boundary_waits,
								1);
		/* spec-2.41 D7 (§2.8 regression guard) — required_lsn==0 means the LSN
		 * watermark feeding this serve-gate was absent (real cold block OR, if it
		 * spikes, the SCN migration wrongly zeroed the lsn watermark).  The
		 * block-count tracks how often the gate fail-closed (not covered). */
		if (required_lsn_zero)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->redo_coverage_required_lsn_zero_count, 1);
		if (!covered)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->redo_coverage_gate_block_count, 1);
	}
	return covered;
}

bool
cluster_gcs_send_block_request_and_wait(BufferDesc *buf, PcmLockTransition transition_id,
										int master_node, bool clean_eligible,
										bool *out_retry_denied)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	GcsBlockRequestPayload payload;
	BufferTag tag;
	bool granted = false;
	bool granted_storage_fallback = false;
	bool read_image = false; /* spec-5.2 D2: one-shot read image, non-durable */
	bool terminal_denied = false;
	bool retry_denied = false;
	bool retransmit_warning_emitted = false;
	bool suppress_direct_land = false;
	bool awaiting_holder_refusal_master_cleanup = false;
	uint8 final_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
	int32 final_forwarding_master = GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
	XLogRecPtr final_page_lsn = InvalidXLogRecPtr;
	int retry_attempt;
	int max_retries;
	int current_master;
	/* GCS-race round-2 review F4: the accepted attempt's identity, captured
	 * BEFORE gcs_block_release_slot zeroes the slot (use-after-release). */
	uint64 done_request_epoch = 0;
	int32 done_master_node = -1;
	/* PGRAC: spec-5.59 D2/D3/D4 — requester-wait + index-overlay scopes. */
	ClusterXpScope xp_req;
	ClusterXpScope xp_idx;
	ClusterXpScope xp_recv;
	bool xp_is_read;
	bool xp_is_index;
	/* PGRAC: spec-7.2 D6 — ship-latency histogram start stamp. */
	TimestampTz ship_started_at;

	Assert(out_retry_denied != NULL);
	if (out_retry_denied == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_send_block_request_and_wait: NULL retry result"),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	*out_retry_denied = false;
	if (buf == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_send_block_request_and_wait: NULL BufferDesc"),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	if (cluster_authority_readiness_managed()
		&& !cluster_serving_ready_is_current()) {
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_LMS_UNAVAILABLE),
				 errmsg("GCS block service is not serving-ready"),
				 errhint("Complete StartupXLOG and publish SERVING_READY before "
						 "requesting cache-fusion data.")));
		return false;
	}

	/*
	 * PGRAC: spec-4.6 D4 / L12 — block-path fail-closed toward a DEAD
	 * master.
	 *
	 *	GCS block routing hashes over the DECLARED node list (it does NOT
	 *	consult the GRD shard master map), so after a node death the hash
	 *	still routes this block's master role to the dead node.  spec-4.6
	 *	rebuilds only the GES/GRD logical-lock layer;  block/PCM state
	 *	rebuild is Stage 4.7.  Until then a block request whose master is
	 *	DEAD must fail closed EXPLICITLY (53R9K) instead of burning the
	 *	full retransmit budget against a corpse and surfacing an opaque
	 *	53R90 — and must never be served from stale local state.
	 */
	if (master_node != cluster_node_id
		&& cluster_cssd_get_peer_state(master_node) == CLUSTER_CSSD_PEER_DEAD) {
		cluster_grd_inc_block_path_failclosed();
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_GCS_BLOCK_PATH_NOT_REBUILT),
				 errmsg("block-level cache access requires the dead GCS master for this block"),
				 errhint("Block-state rebuild after node failure lands in Stage 4.7; the "
						 "GES logical-lock path is unaffected.  Retry after the node "
						 "rejoins.")));
	}

	if (transition_id < PCM_TRANS_N_TO_S || transition_id > PCM_TRANS_S_TO_X_CLEANOUT)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_gcs_send_block_request_and_wait: illegal transition_id=%d",
							   (int)transition_id),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));

	tag = buf->tag;
	current_master = master_node;

	/* PGRAC: spec-5.59 D2/D3 — total requester exclusive-wait for this
	 * acquisition (send -> final grant, spanning retries).  RECEIVE below is
	 * a nested diagnostic sub-bucket (service table, not additive).  D4: the
	 * index overlay dimension re-times the same interval into the I bucket
	 * when the caller-supplied relkind hint marks this tag as an index block
	 * (overlay, never added to the W/R decision sum). */
	xp_is_read = (transition_id == PCM_TRANS_N_TO_S);
	xp_is_index = cluster_xp_relkind_hint_is_index_for(&tag);
	cluster_xp_begin(&xp_req, xp_is_read ? CLXP_R_GCS_S_REQUEST : CLXP_W_GCS_X_REQUEST);
	if (xp_is_index)
		cluster_xp_begin(&xp_idx, CLXP_I_INDEX_BLOCK_XFER);
	else
		xp_idx.active = false;

	/* PGRAC: spec-7.2 D6 — ship-latency histogram start stamp (always on,
	 * unlike the GUC-gated xp scopes above;  the histogram is the value-
	 * gate ruler so it must not depend on profiling being enabled). */
	ship_started_at = GetCurrentTimestamp();

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_reserve_slot(tag, (uint8)transition_id, current_master, &request_id);

	max_retries = cluster_gcs_block_retransmit_max_retries >= 0
					  ? cluster_gcs_block_retransmit_max_retries
					  : 4;

	PG_TRY();
	{
		for (retry_attempt = 0; retry_attempt <= max_retries; retry_attempt++) {
			TimestampTz deadline;
			bool got_reply = false;
			bool direct_authoritative_denial = false;

			/* Apply backoff for retry attempts (not the initial send). */
			if (retry_attempt > 0) {
				long backoff_ms;

				backoff_ms = gcs_block_backoff_ms_for_retry(retry_attempt);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_attempt_count, 1);
				(void)WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, backoff_ms,
								WAIT_EVENT_GCS_BLOCK_RETRANSMIT_WAIT);
				ResetLatch(MyLatch);

				/* Budget 3/4 WARNING (mirrors spec-2.27 pattern).  Skip if
				 * max_retries < 4 so the warning never appears under
				 * disabled-retry configs. */
				if (!retransmit_warning_emitted && max_retries >= 4
					&& retry_attempt == max_retries - 1) {
					ereport(WARNING,
							(errcode(ERRCODE_WARNING),
							 errmsg("cluster_gcs_block: retransmit budget 3/4 for tag "
									"spc=%u db=%u relNumber=%u block=%u",
									tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
									(unsigned int)tag.blockNum),
							 errhint("Consider raising cluster.gcs_block_retransmit_max_retries "
									 "or investigating peer GCS responsiveness.")));
					retransmit_warning_emitted = true;
				}
			}

			/* Always rebuild payload with the current cluster_epoch so
			 * DENIED_EPOCH_STALE retries advance forward (HC94). */
			memset(&payload, 0, sizeof(payload));
			payload.request_id = request_id;
			payload.epoch = cluster_epoch_get_current();
			payload.tag = tag;
			payload.sender_node = cluster_node_id;
			payload.requester_backend_id = (int32)MyBackendId; /* HC80 */
			payload.transition_id = (uint8)transition_id;
			/* spec-5.2a D1/D2: mark a deliberately-clean (sequence-refill) X
			 * request so the master takes the clean-page X-transfer path. */
			GcsBlockRequestPayloadSetCleanEligible(&payload, clean_eligible);
			/* GCS-race round-2 RC-F: carry THIS requester's legal-request
			 * lifetime so the master pins the dedup entry TTL to the wire
			 * truth at registration (never to its own later GUC reads). */
			GcsBlockRequestPayloadSetLifetimeHintMs(
				&payload,
				cluster_gcs_block_dedup_lifetime_ms(cluster_gcs_block_retransmit_initial_backoff_ms,
													cluster_gcs_block_retransmit_max_retries,
													cluster_gcs_reply_timeout_ms));

			/* PGRAC: spec-2.34 HC100 — install the next attempt identity
			 * and clear any previous reply in a single critical section.
			 * Splitting those steps lets a late old reply validate against
			 * the old identity and survive into the new wait iteration. */
			{
				ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

				LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
				slot->reply_received = false;
				memset(&slot->reply_header, 0, sizeof(slot->reply_header));
				memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
				slot->reply_sf_dep_valid = false;
				slot->reply_sf_flags = 0;
				cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
				slot->request_epoch = payload.epoch;
				slot->expected_master_node = current_master;
				slot->stale = false;
				slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
				slot->direct_expected_peer = -1;
				slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
				slot->direct_target_buf = NULL;
				slot->direct_target_addr = NULL;
				slot->direct_target_lkey = 0;
				slot->direct_target_prepared = false;
				slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_NONE;
				LWLockRelease(&blk->lock.lock);
			}

			if (!suppress_direct_land)
				(void)gcs_block_direct_prepare_attempt(slot, buf, tag, transition_id,
													   current_master);

			if (retry_attempt == 0)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_request_count, 1);
			else
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_send_count, 1);

			if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
														  (uint32)current_master, &payload,
														  sizeof(payload))) {
				BufferDesc *direct_target_buf = NULL;
				bool direct_prepared = false;

				{
					ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

					LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
					if (slot->direct_state == GCS_BLOCK_DIRECT_ARMING) {
						direct_target_buf = slot->direct_target_buf;
						direct_prepared = slot->direct_target_prepared;
						slot->direct_state = GCS_BLOCK_DIRECT_ABORTED;
						slot->direct_target_buf = NULL;
						slot->direct_target_addr = NULL;
						slot->direct_target_lkey = 0;
						slot->direct_target_prepared = false;
						slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_ARM_FAILED;
					}
					LWLockRelease(&blk->lock.lock);
				}
				gcs_block_direct_finish_target(direct_target_buf, direct_prepared, false,
											   InvalidXLogRecPtr);
				ereport(
					ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("cluster_gcs_block: failed to enqueue "
							"GCS_BLOCK_REQUEST to node %d",
							current_master),
					 errdetail(
						 "PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						 "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						 cluster_node_id)));
			}

			deadline = GetCurrentTimestamp()
					   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

			ConditionVariablePrepareToSleep(&slot->reply_cv);
			for (;;) {
				TimestampTz now;
				long timeout_ms;
				ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
				bool have_reply;
				bool slot_stale;

				LWLockAcquire(&blk->lock.lock, LW_SHARED);
				have_reply = slot->in_use && slot->reply_received;
				slot_stale = slot->in_use && slot->stale;
				LWLockRelease(&blk->lock.lock);
				if (have_reply) {
					got_reply = true;
					break;
				}
				/* PGRAC: spec-2.34 D4 — eager epoch invalidation wake.
				 * Coordinator hook set slot.stale + broadcast our CV.
				 * Treat as timeout-equivalent to fall through to the
				 * retransmit path with a fresh epoch + re-lookup_master. */
				if (slot_stale)
					break;

				now = GetCurrentTimestamp();
				if (now >= deadline) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_timeout_count, 1);
					break;
				}
				timeout_ms = (long)((deadline - now) / 1000);
				if (timeout_ms <= 0)
					timeout_ms = 1;
				(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
												  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
			}
			ConditionVariableCancelSleep();

			if (!got_reply) {
				bool direct_abort_done = true;

				if (gcs_block_direct_mark_aborting(slot, GCS_BLOCK_DIRECT_ABORT_TIMEOUT)) {
					TimestampTz abort_deadline
						= GetCurrentTimestamp()
						  + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

					ConditionVariablePrepareToSleep(&slot->reply_cv);
					for (;;) {
						ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
						bool abort_done;
						TimestampTz now;
						long timeout_ms;

						LWLockAcquire(&blk->lock.lock, LW_SHARED);
						abort_done = !slot->in_use || slot->direct_state == GCS_BLOCK_DIRECT_ABORTED
									 || slot->direct_state == GCS_BLOCK_DIRECT_UNARMED;
						LWLockRelease(&blk->lock.lock);
						if (abort_done)
							break;
						now = GetCurrentTimestamp();
						if (now >= abort_deadline)
							break;
						timeout_ms = (long)((abort_deadline - now) / 1000);
						if (timeout_ms <= 0)
							timeout_ms = 1;
						(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
														  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
					}
					ConditionVariableCancelSleep();
					{
						ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

						LWLockAcquire(&blk->lock.lock, LW_SHARED);
						direct_abort_done = !slot->in_use
											|| slot->direct_state == GCS_BLOCK_DIRECT_ABORTED
											|| slot->direct_state == GCS_BLOCK_DIRECT_UNARMED;
						LWLockRelease(&blk->lock.lock);
					}
				}
				if (!direct_abort_done)
					break;
				/* timeout OR eager wake — retry within budget */
				if (retry_attempt < max_retries) {
					/* If we were waken by eager hook (slot.stale), advance
					 * the master via re-lookup so the next retry honors
					 * the new epoch's hash placement (HC94). */
					ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
					bool was_stale;

					LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
					was_stale = slot->stale;
					slot->stale = false;
					LWLockRelease(&blk->lock.lock);
					if (was_stale)
						current_master = cluster_gcs_lookup_master(tag);
					continue;
				}
				/* budget exhausted at timeout */
				break;
			}

			{
				ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

				LWLockAcquire(&blk->lock.lock, LW_SHARED);
				direct_authoritative_denial
					= slot->direct_state == GCS_BLOCK_DIRECT_ABORTED
					  && slot->direct_abort_reason == GCS_BLOCK_DIRECT_ABORT_BAD_STATUS
					  && slot->reply_received;
				LWLockRelease(&blk->lock.lock);
			}

			/*
			 * Lock-free consume invariant (S3 RC-A):  every delivery path
			 * (wire reply handler, direct-land completion, direct fail-slot)
			 * refuses to touch reply_header/reply_block_data once
			 * reply_received is set, so from the have_reply observation above
			 * until this backend rearms the slot the reply fields are
			 * immutable and may be read without blk->lock.  A duplicate
			 * reply (dedup CACHED_REPLY resend / re-forward) is dropped at
			 * delivery, counted in stale_reply_drop_count.
			 */
			final_status = slot->reply_header.status;
			final_page_lsn = (XLogRecPtr)slot->reply_header.page_lsn;
			/* spec-2.35 HC105:  capture forward source so DENIED_MASTER_
			 * NOT_HOLDER from forward path can be classified as transient
			 * retry (sender retransmit budget) rather than terminal. */
			final_forwarding_master
				= GcsBlockReplyHeaderGetForwardingMasterNode(&slot->reply_header);

			if (final_status == GCS_BLOCK_REPLY_GRANTED
				|| final_status == GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
				|| final_status == GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
				|| final_status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE) {
				uint32 expected = 0;
				uint32 got = 0;
				bool direct_installed;

				{
					ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

					LWLockAcquire(&blk->lock.lock, LW_SHARED);
					direct_installed = slot->direct_state == GCS_BLOCK_DIRECT_INSTALLED;
					LWLockRelease(&blk->lock.lock);
				}

				if (!direct_installed) {
					/* PGRAC: spec-5.59 D2/D3 — reply verify sub-bucket (nested). */
					cluster_xp_begin(&xp_recv,
									 xp_is_read ? CLXP_R_GCS_S_RECEIVE : CLXP_W_GCS_X_RECEIVE);
					expected = slot->reply_header.checksum;
					got = gcs_block_compute_checksum(slot->reply_block_data);
					cluster_xp_end(&xp_recv);

					if (expected != got) {
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
						final_status = GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL;
						terminal_denied = true;
						break;
					}
				}
				if (!direct_installed) {
					/* PGRAC: spec-5.59 D2 — image install sub-bucket (write axis
					 * only; read installs stay inside the S_REQUEST total). */
					if (!xp_is_read) {
						ClusterXpScope xp_inst;

						cluster_xp_begin(&xp_inst, CLXP_W_GCS_X_INSTALL);
						gcs_block_install_reply_block(buf, slot->reply_block_data, final_page_lsn,
													  slot);
						cluster_xp_end(&xp_inst);
					} else {
						gcs_block_install_reply_block(buf, slot->reply_block_data, final_page_lsn,
													  slot);
						/* PGRAC: spec-5.59 §3.6 read amortization probe — a durable
						 * S grant still shipped a full page image. */
						cluster_xp_note_read(true);
					}
				} else if (xp_is_read) {
					cluster_xp_note_read(true);
				}
				/* spec-5.14 D2 class 2: depend on the sender (+ forwarding holder). */
				gcs_block_stamp_touched((int32)slot->reply_header.sender_node,
										final_forwarding_master);
				if (final_status == GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
					|| final_status == GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
					|| final_status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE) {
					/*
					 * spec-2.35 HC111: requester S-holder bit represents real
					 * cache residency, not a forward intent.  The master
					 * therefore adds our bit only after the holder reply has
					 * passed HC108/HC100, checksum verification, and local
					 * block install.
					 */
					if (final_forwarding_master == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)
						ereport(
							ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("cluster_gcs_block: holder-granted reply missing "
									"forwarding master"),
							 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									   cluster_node_id)));
					if (final_status == GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER)
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_granted_from_holder_count,
												1);
					if (final_status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE) {
						/*
						 * PGRAC: spec-6.12a ㉕ — the remote holder downgraded
						 * X->S and shipped a durable S grant; its master notify
						 * (PCM_TRANS_X_TO_S_DOWNGRADE) travels independently.
						 * Register as an S holder with the NON-throwing
						 * transition: if the master already processed the
						 * notify our N->S lands on state S (grant + bitmap
						 * add); if the notify is still in flight (or lost, or
						 * a concurrent X-transfer won the race) the master
						 * denies N->S-on-X and we DEGRADE to the one-shot
						 * read-image semantics — install stands, pcm_state
						 * stays N, no durable copy the master does not track
						 * (Rule 8.A fail-closed; the next read converges).
						 */
						if (!cluster_gcs_try_send_transition_and_wait(
								tag, (PcmLockTransition)transition_id, final_forwarding_master)) {
							cluster_lever_a_note_remote_ack_degraded();
							read_image = true;
							break;
						}
					} else
						cluster_gcs_send_transition_and_wait(tag, (PcmLockTransition)transition_id,
															 final_forwarding_master);
				}
				granted = true;
				break;
			}
			if (final_status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER) {
				uint32 expected;
				uint32 got;

				/*
				 * spec-5.2a D4 BUSY: for a clean (sequence) eligible request a
				 * read-image reply means the master/holder (path-B) could not
				 * cleanly relinquish (transient pin / re-dirty) and KEPT its X.
				 * We must NOT install a non-owned read-image of a page we intend
				 * to write (no seq write guard) and must NOT storage-fallback.
				 * Fail closed RETRYABLE — the transaction retries; by then the
				 * holder is unpinned and the transfer completes (Rule 8.A).
				 */
				if (clean_eligible) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 1);
					ereport(ERROR,
							(errcode(ERRCODE_CLUSTER_CLEAN_PAGE_XFER_UNAVAILABLE),
							 errmsg("cluster_gcs_block: clean-page X-transfer master/holder "
									"transiently busy for tag spc=%u db=%u relNumber=%u block=%u",
									tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
									(unsigned int)tag.blockNum),
							 errhint("The X holder could not relinquish a clean page (pinned or "
									 "re-dirtied); retry the transaction."),
							 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									   cluster_node_id)));
				}

				/*
				 * PGRAC: spec-5.2 D2 — the X holder shipped its CURRENT image
				 * for this one read.  Install the bytes (so this read sees the
				 * holder's uncommitted ITL row-lock), but do NOT send a
				 * transition-ack: we never register as an S holder, and the
				 * caller leaves buf->pcm_state == N so the next access
				 * re-fetches.  A cached copy with no invalidation path would go
				 * stale once the holder writes again (Rule 8.A).
				 */
				/* PGRAC: spec-5.59 D3 — reply verify sub-bucket (nested). */
				cluster_xp_begin(&xp_recv,
								 xp_is_read ? CLXP_R_GCS_S_RECEIVE : CLXP_W_GCS_X_RECEIVE);
				expected = slot->reply_header.checksum;
				got = gcs_block_compute_checksum(slot->reply_block_data);
				cluster_xp_end(&xp_recv);

				if (expected != got) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
					final_status = GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL;
					terminal_denied = true;
					break;
				}
				gcs_block_install_reply_block(buf, slot->reply_block_data, final_page_lsn, slot);
				/* PGRAC: spec-5.59 §3.6 read amortization probe — a one-shot
				 * read-image ship is exactly the "reship" the probe counts. */
				if (xp_is_read)
					cluster_xp_note_read(true);
				/* spec-5.14 D2 class 2: depend on the X holder that shipped this image. */
				gcs_block_stamp_touched((int32)slot->reply_header.sender_node,
										final_forwarding_master);
				/* A non-durable WRITE reply remains N and returns false below.
				 * GCS installs bytes but publishes no shared ownership marker;
				 * the target-only Resource-X owner must retry/fail closed before
				 * any write. LockBuffer is the sole READ_IMAGE publisher, and
				 * only for its one-shot SHARE bracket. */
				read_image = true;
				break;
			}
			if (final_status == GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK) {
				granted_storage_fallback = true;
				break;
			}

			/* DENIED paths — decide terminal vs retryable. */
			if (final_status != GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
				awaiting_holder_refusal_master_cleanup = false;
			if (final_status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
				ereport(LOG,
						(errmsg_internal("GCS holder refusal classification diagnostic"),
						 errdetail("requester=%d request_id=%llu retry=%d/%d "
								   "master=%d forwarding_master=%d direct_denial=%u "
								   "transition=%u",
								   cluster_node_id,
								   (unsigned long long)request_id,
								   retry_attempt, max_retries, current_master,
								   final_forwarding_master,
								   direct_authoritative_denial ? 1U : 0U,
								   (unsigned)transition_id)));
			if (final_status == GCS_BLOCK_REPLY_DENIED_EPOCH_STALE
				|| final_status == GCS_BLOCK_REPLY_DENIED_DEDUP_FULL) {
				/* HC94 + HC96 — retry within budget; re-lookup master so
				 * deterministic hash mod-N reshuffle (post-reconfig) takes
				 * effect on the next attempt. */
				current_master = cluster_gcs_lookup_master(tag);
				if (retry_attempt < max_retries)
					continue;
				/* budget exhausted on transient denial */
				break;
			}

			/* spec-5.16 D3b (INV-R8/R14) — master-side join fence denied this
			 * request: the master (a rejoining node) is not yet a serving MEMBER
			 * or the joiner-home block view is still being rebuilt.  Retryable
			 * (re-lookup master so a completed rebuild / membership change takes
			 * effect); on budget exhaustion surface the dedicated 53R9L. */
			if (final_status == GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING) {
				current_master = cluster_gcs_lookup_master(tag);
				if (retry_attempt < max_retries)
					continue;
				terminal_denied = true; /* exhausted → terminal 53R9L below */
				break;
			}

			/*
			 * D6 direct-land forward handoff: if the master consumed our posted
			 * direct receive with a no-forward denial because it had to forward
			 * to another holder, retry on the normal/generic path.  The generic
			 * retry lets the holder reply reach this slot without racing a live
			 * direct receive armed to the master.
			 */
			if (final_status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
				&& final_forwarding_master == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
				&& direct_authoritative_denial) {
				awaiting_holder_refusal_master_cleanup = false;
				suppress_direct_land = true;
				current_master = cluster_gcs_lookup_master(tag);
				if (retry_attempt < max_retries)
					continue;
				terminal_denied = true;
				break;
			}

			/* A one-shot N->S image forward may lose its exact carrier while a
			 * Resource-X successor advances.  No holder/requester state changed:
			 * close this request identity and let bufmgr exact-abort/rearm its
			 * GRANT_PENDING reservation before issuing a fresh request. */
			if (gcs_block_forwarded_s_refusal_requires_fresh_retry(
					(GcsBlockReplyStatus)final_status, transition_id,
					final_forwarding_master)) {
				awaiting_holder_refusal_master_cleanup = false;
				retry_denied = true;
				break;
			}

			/* The holder-forward refusal removes the master's stale forwarding
			 * marker.  Its one direct cleanup response is part of the same exact
			 * retry round; a second direct response remains terminal. */
			if (final_status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
				&& gcs_block_holder_refusal_retry_exact(
					final_forwarding_master,
					&awaiting_holder_refusal_master_cleanup,
					retry_attempt, max_retries)) {
				current_master = cluster_gcs_lookup_master(tag);
				continue;
			}

			/*
			 * PGRAC: GCS-race round-4 FUNC-1 — third-party live-X handoff.
			 * A direct-from-master DENIED_MASTER_NOT_HOLDER on a WRITE
			 * transition is the HG7 live-X wall; with the BAST nudge armed
			 * the master has already asked the holder for the quiescent
			 * X->S yield and dropped the dedup entry, so a backed-off retry
			 * is re-evaluated against the post-yield state and converges
			 * through the S-invalidate + storage-fallback grant.  Reuse the
			 * starvation backoff knobs/wait event (same "wait for the
			 * holder to yield" semantics).  Budget exhaustion falls through
			 * to the terminal consume below (holder stayed unyielding:
			 * active ITL / pinned for the whole window) -- the pre-FUNC-1
			 * 0A000, never a guess.  Nudge disarmed -> terminal immediately
			 * (no progress to wait for).
			 */
			if (final_status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
				&& final_forwarding_master == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
				&& cluster_ges_bast && transition_id != PCM_TRANS_N_TO_S
				&& retry_attempt < max_retries) {
				long lx_backoff_ms;

				lx_backoff_ms = (long)cluster_gcs_block_starvation_backoff_ms
								* (1L << (retry_attempt < 16 ? retry_attempt : 16));
				if (lx_backoff_ms > 25000)
					lx_backoff_ms = 25000;
				(void)WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, lx_backoff_ms,
								WAIT_EVENT_GCS_BLOCK_STARVATION_RETRY);
				ResetLatch(MyLatch);
				current_master = cluster_gcs_lookup_master(tag);
				continue;
			}

			/* Queue arbitration owns this retry boundary.  Consume the exact
			 * denial, release this request slot, and return to bufmgr so it can
			 * abort the matching GRANT_PENDING token before any backoff.  The
			 * next acquire mints both a fresh token and a fresh request_id; a
			 * fixed starvation budget must never surface a client ERROR. */
			if (final_status == GCS_BLOCK_REPLY_DENIED_PENDING_X) {
				retry_denied = true;
				break;
			}

			/* PGRAC: spec-2.36 D6 — broadcast invalidate timeout reply maps
			 * to 53R91.  Terminal at the sender (the master already exhausted
			 * its retransmit budget;  retrying from the sender side would
			 * just hammer the same broken broadcast). */
			if (final_status == GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT) {
				terminal_denied = true;
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_GCS_BLOCK_INVALIDATE_TIMEOUT),
						 errmsg("cluster_gcs_block: master broadcast invalidate timed out (HC116)"),
						 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								   cluster_node_id)));
				break;
			}

			/* PGRAC: spec-2.37 D6 HC131 — lost-write detected at master direct
			 * ship OR holder forward validate.  Terminal (don't retry — lost-
			 * write is a data integrity issue, not a transient network event).
			 * GUC cluster.gcs_block_lost_write_action selects ereport(53R93)
			 * for production (default) or WARNING for staging/diagnostic. */
			if (final_status == GCS_BLOCK_REPLY_DENIED_LOST_WRITE) {
				/* S3 forensics step 1 — requester-side identity + local-view SCNs
				 * for the three-branch lost-write qualification (true stale ship
				 * vs true lost write vs watermark false-positive).  The verdict's
				 * (expected, shipped) SCN pair is only known on the PRODUCER
				 * (master / forwarding holder); its LOG line correlates with this
				 * errdetail by (tag, request_id).  Reads are pre-ereport and
				 * lock-safe: the content lock is NOT held here (installs above
				 * take it internally), gen read is a NULL-safe atomic. */
				SCN forens_local_scn = cluster_bufmgr_read_block_scn_for_gcs(buf);
				SCN forens_local_wm = cluster_pcm_lock_pi_watermark_scn_query(tag);
				uint64 forens_own_gen = cluster_pcm_own_gen_get(buf->buf_id);

				if (cluster_gcs_block_lost_write_action == 0 /* ERROR */) {
					terminal_denied = true;
					ereport(ERROR,
							(errcode(ERRCODE_CLUSTER_LOST_WRITE_DETECTED),
							 errmsg("cluster_gcs_block: lost write detected on tag "
									"spc=%u db=%u rel=%u block=%u",
									tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
							 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
									   "request_id=" UINT64_FORMAT " request_epoch=" UINT64_FORMAT
									   " master=%d fork=%d transition=%d retry_attempt=%d"
									   " local pd_block_scn=" UINT64_FORMAT
									   " local pi_watermark_scn=" UINT64_FORMAT
									   " ownership_gen=" UINT64_FORMAT ".",
									   cluster_node_id, request_id, slot->request_epoch,
									   current_master, (int)tag.forkNum, (int)transition_id,
									   retry_attempt, (uint64)forens_local_scn,
									   (uint64)forens_local_wm, forens_own_gen),
							 errhint("Shipped block.pd_block_scn is below the master "
									 "pi_watermark_scn (or the tracked block shipped an "
									 "unstamped page).  Inspect dump_gcs."
									 "lost_write_detected_count and cluster_pcm_grd "
									 "to identify the stale source.  spec-2.41 D1.")));
				} else {
					/* WARN action: do NOT error.  This diagnostic mode intentionally
					 * lets the caller proceed with the existing/storage-fallback block —
					 * which may be STALE.  Asymmetry (spec-2.41 review): unlike the
					 * holder-forward D5 WARN terminal (which ships no page and still
					 * fail-closes), this master-direct / storage-fallback WARN can serve
					 * a possibly-stale image — a staging-only, pre-existing diagnostic
					 * risk, never the production default.  Avoid terminal_denied,
					 * otherwise the post-loop switch raises a generic
					 * FEATURE_NOT_SUPPORTED. */
					ereport(WARNING,
							(errmsg("cluster_gcs_block: lost write detected on tag "
									"spc=%u db=%u rel=%u block=%u (action=warn)",
									tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
							 errdetail("request_id=" UINT64_FORMAT " request_epoch=" UINT64_FORMAT
									   " master=%d fork=%d transition=%d retry_attempt=%d"
									   " local pd_block_scn=" UINT64_FORMAT
									   " local pi_watermark_scn=" UINT64_FORMAT
									   " ownership_gen=" UINT64_FORMAT ".",
									   request_id, slot->request_epoch, current_master,
									   (int)tag.forkNum, (int)transition_id, retry_attempt,
									   (uint64)forens_local_scn, (uint64)forens_local_wm,
									   forens_own_gen)));
					granted_storage_fallback = true;
				}
				break;
			}

			/* Other denials are terminal — exit loop with final_status set. */
			terminal_denied = true;
			break;
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * GCS-race round-2 review F4: capture the accepted attempt's identity
	 * BEFORE the release -- gcs_block_release_slot zeroes request_epoch and
	 * expected_master_node, so the DONE funnel below reading the slot was a
	 * use-after-release that stamped epoch 0 (never matching the master's
	 * dedup key).  expected_master_node is the node the accepted attempt's
	 * REQUEST was sent to -- the dedup entry's owner (the reply's sender
	 * can legitimately be a forwarding holder instead).
	 */
	done_request_epoch = slot->request_epoch;
	done_master_node = slot->expected_master_node;

	gcs_block_release_slot(slot);

	/*
	 * spec-5.13 S6 (CL-I5 serve-gate): a cooperative leave in progress may have
	 * remastered a leaving node's shard to this survivor BEFORE the leaving node
	 * flushed its dirty/X image to shared storage.  In that window the new master
	 * holds no copy → a storage fallback would read the PRE-flush stale image
	 * (false-visible, 8.A).  Withhold the fallback (retryable 53R62) until the
	 * leave commits (flush + this survivor's cache invalidate).  `granted` (a
	 * cache-fusion ship that carried the holder's current image) is unaffected —
	 * only the storage fallback is gated.
	 */
	if (granted_storage_fallback && !cluster_clean_leave_block_serve_gate_allows())
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS),
				 errmsg("cluster_gcs_block: storage fallback withheld during a cooperative cluster "
						"leave for tag spc=%u db=%u rel=%u block=%u",
						tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
				 errhint("a node is leaving the cluster and may not yet have flushed this block; "
						 "retry after the leave commits — retry is safe"),
				 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));

	/*
	 * PGRAC: GCS-race round-4c FUNC-1 — a storage fallback ships no image:
	 * the buffer still holds whatever this backend pre-read BEFORE the
	 * acquire-gate negotiation (ReadBuffer runs first).  When the reply
	 * carried a valid master pi_watermark_scn, prove the local copy current
	 * or discard-and-re-read the shared-storage page; a terminal 53R93 here
	 * loses the xp/hist sample like every other terminal ereport above.
	 */
	if (granted_storage_fallback)
		cluster_gcs_block_fallback_verify_refresh(buf, tag, (SCN)final_page_lsn);

	/* PGRAC: spec-5.59 D2/D3/D4 — close the requester-wait (and index
	 * overlay) scopes at the single normal-exit funnel; terminal ereport
	 * paths above simply lose the sample (stack scope, harmless). */
	cluster_xp_end(&xp_idx);
	cluster_xp_end(&xp_req);

	/* PGRAC: spec-7.2 D6 — record the completed ship into the latency
	 * histogram (GRANTED / STORAGE_FALLBACK / READ_IMAGE all delivered a
	 * usable page;  the terminal-denied tail below ereports and loses the
	 * sample, mirroring the xp scopes). */
	if (granted || granted_storage_fallback || read_image)
		gcs_block_ship_hist_record(ship_started_at);

	if (granted || granted_storage_fallback || read_image || retry_denied) {
		/*
		 * PGRAC: GCS-race round-2 RC-F — completion proof.  The terminal
		 * reply was verified and consumed, so no retransmit of this
		 * request can ever fire again from this backend; tell the master
		 * so it can retire the dedup entry within its short done-linger
		 * quarantine instead of holding the 8KB slot for the full pinned
		 * lifetime.  Best-effort: enqueue failure or wire loss simply
		 * leaves the TTL backstop in charge.  The identity is the accepted
		 * attempt's REQUEST epoch + target master, captured before the
		 * slot release above (review F4) — the master's dedup key was
		 * built from req->epoch.
		 *
		 * Review F6 capability gate: only a peer that advertised
		 * GCS_DONE_V1 registers the DONE msg_type — sending it to an old
		 * binary would make the peer close the connection.  No capability
		 * -> no send, the pinned TTL stays in charge.  Review F7: a full
		 * outbound ring is COUNTED (done_enqueue_drop_count), never
		 * silent.
		 */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-done-drop");
		if (!cluster_ic_suppress_gcs_done_cap /* test-only old-binary sim */
			&& cluster_sf_peer_supports_gcs_done(done_master_node)
			&& !cluster_injection_should_skip("cluster-gcs-block-done-drop")) {
			GcsBlockDonePayload done;

			memset(&done, 0, sizeof(done));
			done.request_id = request_id;
			done.epoch = done_request_epoch;
			done.tag = tag;
			done.sender_node = cluster_node_id;
			done.requester_backend_id = (int32)MyBackendId;
			done.transition_id = (uint8)transition_id;
			if (cluster_grd_outbound_enqueue_backend_msg(
					PGRAC_IC_MSG_GCS_BLOCK_DONE, (uint32)done_master_node, &done, sizeof(done)))
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->done_sent_count, 1);
			else
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->done_enqueue_drop_count, 1);
		}
	}

	/* spec-5.2 D2: GRANTED / STORAGE_FALLBACK record durable ownership (the
	 * caller mirrors PCM state); READ_IMAGE is a one-shot non-durable read so
	 * the caller must leave buf->pcm_state == N. */
	if (granted || granted_storage_fallback)
		return true;
	if (read_image)
		return false;
	if (retry_denied) {
		*out_retry_denied = true;
		return false;
	}

	if (terminal_denied) {
		switch ((GcsBlockReplyStatus)final_status) {
		case GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT:
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster_gcs_block: master rejected transition_id=%d as illegal",
								   (int)transition_id),
							errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									  cluster_node_id)));
			break;
		case GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL:
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster_gcs_block: received block failed CRC32C verify"),
							errhint("Possible wire-ABI drift or network corruption."),
							errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									  cluster_node_id)));
			break;
		case GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER:
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
			if (clean_eligible) {
				/* spec-5.2a D3 branch ⑤ — eligible clean-page terminal DENIED is
				 * a ≥3-node third-party master fail-closed (the 2-node target
				 * never reaches here).  Surface the dedicated retryable code so it
				 * is distinguishable from the generic writer-transfer DENY. */
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 1);
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_CLEAN_PAGE_XFER_UNAVAILABLE),
						 errmsg("cluster_gcs_block: clean-page X-transfer master is neither "
								"requester nor holder (third-party master, >=3 nodes) for tag "
								"spc=%u db=%u relNumber=%u block=%u",
								tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
								(unsigned int)tag.blockNum),
						 errhint("Clean-page X-transfer with a third-party master lands in a "
								 "later spec; retry, or run the 2-node topology."),
						 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								   cluster_node_id)));
			}
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_gcs_block: master does not hold tag and state != N"),
							errhint("Cross-node holder migration / DRM handling lands in Stage 6; "
									"the cross-instance read-path boundary is Spec: spec-5.57."),
							errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									  cluster_node_id)));
			break;
		case GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING:
			/* spec-5.16 D3b — master-side join fence, retry budget exhausted.
			 * 53R9L (retry-safe): the joiner-home block view is still rebuilding
			 * or the master is not yet a serving MEMBER. */
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING),
					 errmsg("cluster_gcs_block: block master is recovering for tag "
							"spc=%u db=%u relNumber=%u block=%u (node rejoin)",
							tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
							(unsigned int)tag.blockNum),
					 errhint("A rejoining node's home block view is being rebuilt (survivors "
							 "re-declaring), or the master is not yet a quorum member; retry — "
							 "retry is safe."),
					 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
							   cluster_node_id)));
			break;
		case GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE:
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cluster_gcs_block: transition denied (status=%d)", (int)final_status),
					 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
							   cluster_node_id)));
			break;
		}
	}

	/* Budget exhausted (timeout or transient DENIED) — HC98 53R90. */
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_exhausted_count, 1);
	ereport(ERROR,
			(errcode(ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED),
			 errmsg("cluster_gcs_block: retransmit budget exhausted after %d retries "
					"for tag spc=%u db=%u relNumber=%u block=%u (last status=%d)",
					max_retries, tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
					(unsigned int)tag.blockNum, (int)final_status),
			 errhint("Possible peer GCS unresponsiveness, network partition, or "
					 "epoch reshuffle storm.  Inspect dump_gcs counters and "
					 "consider raising cluster.gcs_block_retransmit_max_retries."),
			 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN PGRAC_NODE=%d "
					   "PGRAC_ATTEMPT=0 ",
					   cluster_node_id)));
}


bool
cluster_gcs_local_master_read_image_and_wait(BufferDesc *buf, const PcmAuthoritySnapshot *expected,
											 bool force_one_shot,
											 bool *out_retry_denied)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	BufferTag tag;
	int max_retries;
	int retry_attempt;
	int attempts = 0;
	int last_status = -1;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool installed = false;
	bool durable_s = false; /* spec-6.12a ㉕ — holder downgraded, we registered */
	bool image_only;
	int32 holder_node;

	Assert(out_retry_denied != NULL);
	if (buf == NULL || expected == NULL || out_retry_denied == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_local_master_read_image_and_wait: invalid exact input"),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	*out_retry_denied = false;
	holder_node = expected->x_holder_node;
	if (expected->state != PCM_STATE_X || holder_node < 0 || holder_node >= 32
		|| holder_node == cluster_node_id || expected->s_holders_bitmap != 0
		|| expected->master_holder.node_id != (uint32)holder_node)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_gcs_block: invalid remote-X authority for exact read image"),
				 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));

	tag = buf->tag;
	/* P0-21 residual: the holder is an exact authority identity, not a route
	 * hint.  A queue winner may have displaced a caller's optimistic sample
	 * before this helper starts; let bufmgr abort/rearm GRANT_PENDING and
	 * drive the new authority rather than sending to the stale node. */
	if (!cluster_pcm_lock_authority_matches(tag, expected)) {
		*out_retry_denied = true;
		return false;
	}
	image_only = force_one_shot
				 || cluster_gcs_block_resource_x_local_s_barrier_active(tag);
	cluster_gcs_block_dedup_register_backend_exit_hook();
	/* expected_master == self:  the holder's reply carries forwarding_master =
	 * self, which the HC108 authorized chain validates against this slot. */
	slot = gcs_block_reserve_slot(tag, (uint8)PCM_TRANS_N_TO_S, cluster_node_id, &request_id);
	max_retries = cluster_gcs_block_retransmit_max_retries >= 0
					  ? cluster_gcs_block_retransmit_max_retries
					  : 4;

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		for (retry_attempt = 0; retry_attempt <= max_retries; retry_attempt++) {
			attempts = retry_attempt + 1;
			got_reply = false;
			if (retry_attempt > 0) {
				long backoff_ms = gcs_block_backoff_ms_for_retry(retry_attempt);

				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_attempt_count, 1);
				(void)WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, backoff_ms,
								WAIT_EVENT_GCS_BLOCK_RETRANSMIT_WAIT);
				ResetLatch(MyLatch);
				/* The bounded retry belongs only to the exact remote-X authority
				 * sampled by the caller.  Do not spend another request identity on
				 * a holder displaced while we backed off. */
				if (!cluster_pcm_lock_authority_matches(tag, expected)) {
					*out_retry_denied = true;
					break;
				}
				if (!gcs_block_pcm_x_next_request_id(&request_id))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("cluster_gcs_block: read-image request id exhausted"),
							 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									   cluster_node_id)));
			}
			if (!cluster_pcm_lock_authority_matches(tag, expected)) {
				*out_retry_denied = true;
				break;
			}

			LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
			slot->request_id = request_id;
			slot->reply_received = false;
			memset(&slot->reply_header, 0, sizeof(slot->reply_header));
			memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
			slot->reply_sf_dep_valid = false;
			slot->reply_sf_flags = 0;
			cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
			slot->request_epoch = cluster_epoch_get_current();
			slot->expected_master_node = cluster_node_id;
			slot->stale = false;
			LWLockRelease(&blk->lock.lock);

			memset(&fwd, 0, sizeof(fwd));
			fwd.request_id = request_id;
			fwd.epoch = cluster_epoch_get_current();
			fwd.tag = tag;
			fwd.original_requester_node = cluster_node_id; /* reply returns to us */
			fwd.requester_backend_id = (int32)MyBackendId;
			fwd.master_node = cluster_node_id;
			fwd.transition_id = (uint8)PCM_TRANS_N_TO_S;
			GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
				&fwd, cluster_pcm_lock_pi_watermark_scn_query(tag));
			GcsBlockForwardPayloadSetReadImage(&fwd, true);
			/* PGRAC: spec-6.12a ㉕ — ask the remote X holder to TRY the quiescent
		 * X->S downgrade so this read (and every later one) becomes a durable
		 * cached S.  We ARE the master here, so on the holder's durable reply
		 * the registration is a local transition apply — no ACK wire. */
			if (cluster_read_scache && !image_only)
				GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);

			if (retry_attempt > 0)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_send_count, 1);

			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);
			if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
														  (uint32)holder_node, &fwd, sizeof(fwd)))
				ereport(
					ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("cluster_gcs_block: failed to enqueue read-image FORWARD "
							"to X holder %d",
							holder_node),
					 errdetail(
						 "PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						 "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						 cluster_node_id)));

			deadline = GetCurrentTimestamp()
					   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

			ConditionVariablePrepareToSleep(&slot->reply_cv);
			for (;;) {
				TimestampTz now;
				long timeout_ms;
				bool have_reply;

				LWLockAcquire(&blk->lock.lock, LW_SHARED);
				have_reply = slot->in_use && slot->reply_received;
				LWLockRelease(&blk->lock.lock);
				if (have_reply) {
					got_reply = true;
					break;
				}
				now = GetCurrentTimestamp();
				if (now >= deadline)
					break;
				timeout_ms = (long)((deadline - now) / 1000);
				if (timeout_ms <= 0)
					timeout_ms = 1;
				(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
												  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
			}
			ConditionVariableCancelSleep();
			last_status = got_reply ? (int)slot->reply_header.status : -1;

			if (got_reply
				&& (slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
					|| (!image_only
						&& slot->reply_header.status
							   == (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE))) {
				uint32 expected = slot->reply_header.checksum;
				uint32 got = gcs_block_compute_checksum(slot->reply_block_data);

				if (expected == got) {
					gcs_block_install_reply_block(buf, slot->reply_block_data,
												  (XLogRecPtr)slot->reply_header.page_lsn, slot);
					/* spec-5.14 D2 class 2: this node (local master) consumed the
				 * remote holder's volatile image. */
					gcs_block_stamp_touched(holder_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
					installed = true;
					/*
				 * PGRAC: spec-6.12a ㉕ — the holder downgraded X->S and shipped
				 * a DURABLE S grant.  We are the master: register ourselves as
				 * an S holder with a LOCAL transition apply (no ACK wire).  The
				 * holder's own X->S notify travels on the LMON dispatch path;
				 * if it was applied first our N->S lands on state S (bitmap
				 * add); if it is still in flight the apply fails and we
				 * DEGRADE to the one-shot semantics — install stands,
				 * pcm_state stays N (Rule 8.A: never a durable copy the
				 * master entry does not track).
				 */
					if (slot->reply_header.status
						== (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE) {
						if (!image_only
							&& cluster_pcm_lock_apply_gcs_transition(
								tag, PCM_TRANS_N_TO_S, cluster_node_id))
							durable_s = true;
						else
							cluster_lever_a_note_remote_ack_degraded();
					}
				} else {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
				}
			}

			if (installed || durable_s)
				break;
			/* A holder-side conditional BufferContent miss is pre-mutation
			 * backpressure.  This helper must not retransmit the same
			 * GRANT_PENDING identity: return the denial to bufmgr, whose outer
			 * boundary exact-aborts the reservation and rearms a fresh token and
			 * request identity under the original deadline. */
			if (got_reply
				&& slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X) {
				*out_retry_denied = true;
				break;
			}
			/* P0-21 completion: holder-side conditional BufferContent refusal is
			 * HC105 transient.  Retry without ever blocking the DATA worker.  A new
		 * request id on the next iteration prevents a delayed old denial from
		 * winning the re-armed slot. */
			if (got_reply
				&& slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
				&& retry_attempt < max_retries) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
				/* A refusal is transient only while the complete holder identity
				 * remains authoritative.  Drift exits through the outer
				 * GRANT_PENDING abort/rearm path; this helper never guesses the
				 * successor holder. */
				if (!cluster_pcm_lock_authority_matches(tag, expected)) {
					*out_retry_denied = true;
					break;
				}
				continue;
			}
			/* Preserve the stable-authority terminal fail-close below, but do
			 * not surface it for an old identity displaced during a timeout,
			 * checksum failure, or other non-success terminal. */
			if (!cluster_pcm_lock_authority_matches(tag, expected))
				*out_retry_denied = true;
			break;
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	if (*out_retry_denied)
		return false;
	if (durable_s)
		return true; /* spec-6.12a ㉕ — durable S; caller mirrors pcm_state = S */
	if (installed)
		return false; /* one-shot read image — non-durable, leave pcm_state N */

	/* No read image obtained (timeout / holder evict / denial) — fail closed,
	 * never a silent stale read (Rule 8.A). */
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_gcs_block: could not obtain read image from X holder %d "
						   "for tag spc=%u db=%u relNumber=%u block=%u",
						   holder_node, tag.spcOid, tag.dbOid,
						   (unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum),
					errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
							  "attempts=%d last_status=%d",
							  cluster_node_id, attempts, last_status),
					errhint("The X holder did not ship a current image in time; retry, or "
							"inspect dump_gcs.cf_xheld_read_ship_count.")));
	return true; /* unreachable */
}


/* Stage 8 R4 D4 requester-side remote FULL fetch.  This target-only raw
 * helper remains separate from the still-live SOURCE dispatcher below.  It
 * arms the disjoint R4 reply domain before staging exact REQUEST80, never
 * prepares direct land, waits on monotonic time and copies a verified FULL
 * page only into the caller's scratch buffer.  A transported status 25
 * closes that attempt before a bounded fresh-id retry; status 26 closes the
 * operation without retry or caller-page mutation. */
ClusterMxDescribeResult
cluster_gcs_current_mx_describe_fetch_and_wait(
	int32 origin_node, const ClusterCurrentMxKey *key,
	ClusterCurrentMxMemberDesc *members, uint16 members_cap,
	uint16 *members_count, uint32 *reported_total_members)
{
	ClusterGcsBlockOutstandingSlot *slot;
	ClusterCurrentMxDescribeForwardV2 request;
	ClusterCurrentMxDescribeReplyPage reply_page;
	GcsBlockReplyHeader reply_header;
	BufferTag route_tag;
	uint64 request_id = 0;
	uint64 request_epoch;
	uint32 capability_generation = 0;
	int worker_id;
	bool got_reply = false;
	bool stale = false;
	ClusterMxDescribeResult result = CMX_DESC_UNKNOWN;

	if (members != NULL && members_cap > 0)
		memset(members, 0, sizeof(*members) * members_cap);
	if (members_count != NULL)
		*members_count = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;
	request_epoch = cluster_epoch_get_current();
	if (key == NULL || members == NULL || members_count == NULL
		|| reported_total_members == NULL || origin_node < 0
		|| origin_node == cluster_node_id || origin_node >= CLUSTER_MAX_NODES
		|| key->origin_node_id != (uint16)origin_node
		|| key->cluster_epoch != request_epoch
		|| !MultiXactIdIsValid(key->multixact_id)
		|| !cluster_gcs_block_family_on_data_plane())
		return CMX_DESC_UNKNOWN;
	if (!cluster_sf_peer_multixact_current_capability_generation(
			origin_node, &capability_generation))
		return CMX_DESC_UNKNOWN;

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_try_reserve_current_mx_slot(
		key, request_epoch, origin_node,
		(uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT, &request_id);
	if (slot == NULL)
		return CMX_DESC_UNKNOWN;

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		memset(&request, 0, sizeof(request));
		request.prefix.request_id = request_id;
		request.prefix.epoch = request_epoch;
		request.prefix.mxkey = *key;
		request.prefix.original_requester_node = cluster_node_id;
		request.prefix.requester_backend_id = (int32)MyBackendId;
		request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
		request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
		request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
		request.trailer.flags = CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE;

		route_tag = GcsBlockCurrentMxRouteTagMake(
			request_id, request_epoch, cluster_node_id, (int32)MyBackendId);
		worker_id = cluster_lms_shard_for_tag(&route_tag, cluster_lms_workers);
		if (worker_id < 0
			|| !cluster_lms_outbound_enqueue_cap_bound(
				worker_id, PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
				(uint32)origin_node, &request, sizeof(request),
				PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1,
				capability_generation))
			goto describe_done;

		deadline = GetCurrentTimestamp()
				   + ((TimestampTz)cluster_gcs_reply_timeout_ms)
						 * (TimestampTz)1000;
		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			stale = slot->in_use && slot->stale;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			if (stale)
				break;
			now = GetCurrentTimestamp();
			if (now >= deadline)
				break;
			timeout_ms = (long)((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void)ConditionVariableTimedSleep(
				&slot->reply_cv, timeout_ms,
				WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		if (!got_reply) {
			result = stale ? CMX_DESC_UNKNOWN : CMX_DESC_TIMEOUT;
			goto describe_done;
		}

		reply_header = slot->reply_header;
		memcpy(&reply_page, slot->reply_block_data, sizeof(reply_page));
		if (reply_header.status
				!= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT
			|| reply_header.sender_node != origin_node
			|| reply_header.request_id != request_id
			|| reply_header.epoch != request_epoch
			|| reply_header.requester_backend_id != (int32)MyBackendId
			|| reply_header.transition_id != 0 || reply_header.page_lsn != 0
			|| GcsBlockReplyHeaderGetForwardingMasterNode(&reply_header)
				   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
			|| reply_header.checksum
				   != cluster_gcs_block_compute_checksum(
					  (const char *)&reply_page)
			|| cluster_epoch_get_current() != request_epoch) {
			result = CMX_DESC_UNKNOWN;
			goto describe_done;
		}

		result = cluster_multixact_current_wire_validate_describe_reply(
			&reply_page, sizeof(reply_page), origin_node, request_epoch,
			request_id, key, members, members_cap, members_count,
			reported_total_members);
		if (result == CMX_DESC_OK)
			gcs_block_stamp_touched(
				origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

describe_done:
		;
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);
	return result;
}

ClusterMxResolveResult
cluster_gcs_current_mx_member_proof_fetch_and_wait(
	int32 origin_node, ClusterCurrentMxProofForwardV2 *request,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap,
	uint16 *proof_count, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out, TimestampTz deadline)
{
	ClusterGcsBlockOutstandingSlot *slot;
	ClusterCurrentMxProofForwardV2 decoded;
	ClusterCurrentMxProofReplyPage reply_page;
	GcsBlockReplyHeader reply_header;
	BufferTag route_tag;
	uint64 request_id = 0;
	uint64 request_epoch;
	uint32 capability_generation = 0;
	int worker_id;
	bool got_reply = false;
	bool stale = false;
	ClusterMxResolveResult result = CMX_RESOLVE_UNKNOWN;
	uint16 i;

	if (proofs != NULL) {
		memset(proofs, 0, sizeof(*proofs) * proofs_cap);
		for (i = 0; i < proofs_cap; i++)
			proofs[i].state = CCM_UNKNOWN;
	}
	if (proof_count != NULL)
		*proof_count = 0;
	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (requester_capability_generation_out != NULL)
		*requester_capability_generation_out = 0;
	request_epoch = cluster_epoch_get_current();
	if (request == NULL || proofs == NULL || proof_count == NULL
		|| updater_proof == NULL
		|| requester_capability_generation_out == NULL || origin_node < 0
		|| origin_node == cluster_node_id || origin_node >= CLUSTER_MAX_NODES
		|| !cluster_gcs_block_family_on_data_plane())
		return CMX_RESOLVE_UNKNOWN;
	if (deadline != 0 && GetCurrentTimestamp() >= deadline)
		return CMX_RESOLVE_TIMEOUT;
	request->prefix.epoch = request_epoch;
	request->prefix.original_requester_node = cluster_node_id;
	request->prefix.requester_backend_id = (int32)MyBackendId;
	if (!cluster_multixact_current_wire_validate_proof_forward(
			request, sizeof(*request), cluster_node_id, origin_node,
			request_epoch, &decoded)
		|| !cluster_sf_peer_multixact_current_capability_generation(
			origin_node, &capability_generation))
		return CMX_RESOLVE_UNKNOWN;

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_try_reserve_current_mx_slot(
		&request->prefix.mxkey, request_epoch, origin_node,
		(uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT,
		&request_id);
	if (slot == NULL)
		return CMX_RESOLVE_UNKNOWN;

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

		request->prefix.request_id = request_id;
		if (!cluster_multixact_current_wire_validate_proof_forward(
				request, sizeof(*request), cluster_node_id, origin_node,
				request_epoch, &decoded))
			goto proof_done;
		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		if (!slot->in_use || slot->request_id != request_id) {
			LWLockRelease(&blk->lock.lock);
			goto proof_done;
		}
		slot->expected_current_mx_proof_valid = true;
		slot->expected_current_mx_proof = *request;
		LWLockRelease(&blk->lock.lock);

		route_tag = GcsBlockCurrentMxRouteTagMake(
			request_id, request_epoch, cluster_node_id, (int32)MyBackendId);
		worker_id = cluster_lms_shard_for_tag(&route_tag, cluster_lms_workers);
		if (worker_id < 0
			|| !cluster_lms_outbound_enqueue_cap_bound(
				worker_id, PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
				(uint32)origin_node, request, sizeof(*request),
				PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1,
				capability_generation))
			goto proof_done;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			stale = slot->in_use && slot->stale;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			if (stale)
				break;
			now = GetCurrentTimestamp();
			if (deadline != 0 && now >= deadline)
				break;
			timeout_ms = deadline == 0
				? 1000L : (long)((deadline - now) / 1000);
			timeout_ms = Max(timeout_ms, 1L);
			(void)ConditionVariableTimedSleep(
				&slot->reply_cv, timeout_ms,
				WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();
		if (!got_reply) {
			result = stale ? CMX_RESOLVE_UNKNOWN : CMX_RESOLVE_TIMEOUT;
			goto proof_done;
		}

		reply_header = slot->reply_header;
		memcpy(&reply_page, slot->reply_block_data, sizeof(reply_page));
		if (reply_header.status
				!= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT
			|| reply_header.sender_node != origin_node
			|| reply_header.request_id != request_id
			|| reply_header.epoch != request_epoch
			|| reply_header.requester_backend_id != (int32)MyBackendId
			|| reply_header.transition_id != 0 || reply_header.page_lsn != 0
			|| GcsBlockReplyHeaderGetForwardingMasterNode(&reply_header)
				   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
			|| reply_header.checksum
				   != cluster_gcs_block_compute_checksum(
					  (const char *)&reply_page)
			|| cluster_epoch_get_current() != request_epoch)
			goto proof_done;
		if (!cluster_multixact_current_wire_validate_proof_reply_frame(
				&reply_page, sizeof(reply_page), origin_node, request_epoch,
				request, &result, proofs, proofs_cap, proof_count,
				updater_proof, requester_capability_generation_out))
			result = CMX_RESOLVE_UNKNOWN;
		if (result == CMX_RESOLVE_OK)
			gcs_block_stamp_touched(
				origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

proof_done:
		;
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);
	return result;
}

static ClusterCrBuildResult
gcs_block_r4_cr_fetch_and_wait_raw(BufferTag tag, SCN read_scn,
									  int32 real_master_node,
									  char dst_page[GCS_BLOCK_DATA_SIZE],
									  ClusterCrBuildReason *reason_out)
{
	int max_retries;
	int retry_attempt;

	if (reason_out != NULL)
		*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
	if (dst_page == NULL || reason_out == NULL || !SCN_VALID(read_scn)
		|| real_master_node < 0
		|| real_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	max_retries = cluster_gcs_block_retransmit_max_retries;
	if (max_retries < 0)
		max_retries = 4;
	if (max_retries > 30)
		max_retries = 30;
	cluster_gcs_block_dedup_register_backend_exit_hook();
	for (retry_attempt = 0; retry_attempt <= max_retries; retry_attempt++) {
		ClusterGcsBlockOutstandingSlot *slot;
		ClusterR4CrRequestPayload request;
		uint64 request_id = 0;
		uint64 request_epoch = cluster_epoch_get_current();
		volatile bool got_reply = false;
		volatile bool fetched = false;
		volatile uint8 reply_status = 0;

		slot = gcs_block_try_reserve_r4_slot(tag, request_epoch, real_master_node,
											  &request_id);
		if (slot == NULL) {
			*reason_out = CLUSTER_CR_BUILD_CAPACITY;
			return CLUSTER_CR_BUILD_RETRYABLE;
		}

		PG_TRY();
		{
			bool request_admitted = false;

			memset(&request, 0, sizeof(request));
			request.base.request_id = request_id;
			request.base.epoch = request_epoch;
			request.base.tag = tag;
			request.base.sender_node = cluster_node_id;
			request.base.requester_backend_id = (int32)MyBackendId;
			request.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
			GcsBlockRequestPayloadSetLifetimeHintMs(
				&request.base,
				cluster_gcs_block_dedup_lifetime_ms(
					cluster_gcs_block_retransmit_initial_backoff_ms,
					cluster_gcs_block_retransmit_max_retries,
					cluster_gcs_reply_timeout_ms));
			if (ClusterR4RequestExtensionSetCr(&request.extension, read_scn))
				request_admitted
					= cluster_grd_outbound_enqueue_backend_msg(
						PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
						(uint32)real_master_node, &request,
						sizeof(request));
			if (request_admitted) {
				ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
				instr_time started;

				INSTR_TIME_SET_CURRENT(started);
				ConditionVariablePrepareToSleep(&slot->reply_cv);
				for (;;) {
					instr_time now;
					instr_time elapsed;
					double remaining_ms;
					long timeout_ms;
					bool have_reply;
					bool stale;

					LWLockAcquire(&blk->lock.lock, LW_SHARED);
					have_reply = slot->in_use && slot->reply_received;
					stale = slot->in_use && slot->stale;
					LWLockRelease(&blk->lock.lock);
					if (have_reply) {
						got_reply = true;
						break;
					}
					if (stale)
						break;
					INSTR_TIME_SET_CURRENT(now);
					elapsed = now;
					INSTR_TIME_SUBTRACT(elapsed, started);
					remaining_ms = (double)cluster_gcs_reply_timeout_ms
									   - INSTR_TIME_GET_MILLISEC(elapsed);
					if (remaining_ms <= 0)
						break;
					timeout_ms = remaining_ms < 1.0 ? 1L : (long)remaining_ms;
					(void)ConditionVariableTimedSleep(
						&slot->reply_cv, timeout_ms, WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
				}
				ConditionVariableCancelSleep();

				if (got_reply
					&& slot->reply_domain == CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR
					&& slot->reply_header.request_id == request_id
					&& slot->reply_header.epoch == request_epoch
					&& slot->reply_header.requester_backend_id == (int32)MyBackendId
					&& slot->reply_header.transition_id == (uint8)PCM_TRANS_N_TO_S) {
					reply_status = slot->reply_header.status;
					if (reply_status == (uint8)GCS_BLOCK_REPLY_R4_CR_FULL
						&& slot->reply_header.checksum
							   == gcs_block_compute_checksum(slot->reply_block_data)) {
						memcpy(dst_page, slot->reply_block_data, GCS_BLOCK_DATA_SIZE);
						fetched = true;
					}
				}
			}
		}
		PG_CATCH();
		{
			gcs_block_release_slot(slot);
			PG_RE_THROW();
		}
		PG_END_TRY();

		gcs_block_release_slot(slot);
		if (fetched) {
			*reason_out = CLUSTER_CR_BUILD_NONE;
			return CLUSTER_CR_BUILD_FULL;
		}
		if (got_reply
			&& reply_status == (uint8)GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED
			&& retry_attempt < max_retries)
			continue;
		if (got_reply
			&& reply_status == (uint8)GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_exhausted_count, 1);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_GCS_BLOCK_RETRANSMIT_EXHAUSTED),
					 errmsg("cluster_gcs_block: R4 CR retry budget exhausted after %d retries "
							"for tag spc=%u db=%u relNumber=%u block=%u "
							"(master=%d attempts=%d last_status=%u)",
							max_retries, tag.spcOid, tag.dbOid,
							(unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum,
							real_master_node, retry_attempt + 1, (unsigned int)reply_status),
					 errhint("Possible peer GCS unresponsiveness, network partition, or "
							 "epoch reshuffle storm.  Inspect dump_gcs counters and "
							 "consider raising cluster.gcs_block_retransmit_max_retries."),
					 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
							   cluster_node_id)));
			*reason_out = CLUSTER_CR_BUILD_HOLDER_MOVED;
			return CLUSTER_CR_BUILD_RETRYABLE;
		}
		if (got_reply) {
			*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
			return CLUSTER_CR_BUILD_FAIL_CLOSED;
		}
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cluster_gcs_block: R4 CR request to master %d received no "
							   "terminal reply after %d attempt(s)",
							   real_master_node, retry_attempt + 1),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
		*reason_out = CLUSTER_CR_BUILD_IO_ERROR;
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	}
	*reason_out = CLUSTER_CR_BUILD_HOLDER_MOVED;
	return CLUSTER_CR_BUILD_RETRYABLE;
}

ClusterTxOutcome
cluster_gcs_block_r4_tx_resolve_fetch_and_wait(
	int32 origin_node, const ClusterTxLocator *locator,
	uint32 expected_physical_generation, uint64 formation_epoch,
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out)
{
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	ClusterR4CrForwardPayload forward;
	ClusterTxResolution decoded;
	BufferTag tag;
	uint64 request_id = 0;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	volatile bool got_reply = false;
	volatile bool sent = false;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = reason;
	memset(&decoded, 0, sizeof(decoded));
	if (out == NULL || locator == NULL || reason_out == NULL
		|| origin_node < 0 || origin_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| origin_node == cluster_node_id
		|| expected_physical_generation == UINT32_MAX
		|| !uba_decode(locator->uba, &segment_id, &block_no,
					   &tt_slot_offset, &row_offset)
		|| block_no == 0
		|| uba_origin_node_id(locator->uba) != (NodeId)origin_node)
		return CLUSTER_TX_UNKNOWN;
	tag = GcsBlockUndoFetchTagMake(segment_id, block_no);
	slot = gcs_block_try_reserve_r4_slot(
		tag, formation_epoch, origin_node, &request_id);
	if (slot == NULL)
		return CLUSTER_TX_UNKNOWN;

	PG_TRY();
	{
		memset(&forward, 0, sizeof(forward));
		forward.base.request_id = request_id;
		forward.base.epoch = formation_epoch;
		forward.base.tag = tag;
		forward.base.original_requester_node = cluster_node_id;
		forward.base.requester_backend_id = (int32)MyBackendId;
		forward.base.master_node = origin_node;
		forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
		if (ClusterR4ForwardExtensionSetLocatorGeneration(
				&forward.extension, CLUSTER_R4_WIRE_TX_RESOLVE,
				locator, expected_physical_generation))
			sent = cluster_grd_outbound_enqueue_backend_msg(
				PGRAC_IC_MSG_GCS_BLOCK_FORWARD, (uint32)origin_node,
				&forward, sizeof(forward));
		if (sent) {
			ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
			instr_time started;

			INSTR_TIME_SET_CURRENT(started);
			ConditionVariablePrepareToSleep(&slot->reply_cv);
			for (;;) {
				instr_time now;
				instr_time elapsed;
				double remaining_ms;
				long timeout_ms;
				bool have_reply;
				bool stale;

				LWLockAcquire(&blk->lock.lock, LW_SHARED);
				have_reply = slot->in_use && slot->reply_received;
				stale = slot->in_use && slot->stale;
				LWLockRelease(&blk->lock.lock);
				if (have_reply) {
					got_reply = true;
					break;
				}
				if (stale)
					break;
				INSTR_TIME_SET_CURRENT(now);
				elapsed = now;
				INSTR_TIME_SUBTRACT(elapsed, started);
				remaining_ms = (double)cluster_gcs_reply_timeout_ms
							   - INSTR_TIME_GET_MILLISEC(elapsed);
				if (remaining_ms <= 0)
					break;
				timeout_ms = remaining_ms < 1.0 ? 1L : (long)remaining_ms;
				(void)ConditionVariableTimedSleep(
					&slot->reply_cv, timeout_ms,
					WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
			}
			ConditionVariableCancelSleep();
		}

		if (!got_reply) {
			reason = CLUSTER_TX_RESOLVE_IO_ERROR;
			goto tx_done;
		}
		if (slot->reply_header.status
				!= (uint8)GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT) {
			reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
			goto tx_done;
		}
		if (!ClusterR4TxVerdictPageDecode(
				(const uint8 *)slot->reply_block_data, locator, &decoded)
			|| decoded.authority.origin_epoch != formation_epoch
			|| slot->reply_header.page_lsn == 0
			|| !SCN_VALID(decoded.authority.authority_scn)
			|| cluster_epoch_get_current() != formation_epoch) {
			reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
			goto tx_done;
		}
		decoded.authority.live_hwm_lsn
			= (XLogRecPtr)slot->reply_header.page_lsn;
		cluster_scn_observe(decoded.commit_scn);
		cluster_scn_observe(decoded.horizon_scn);
		cluster_scn_observe(decoded.authority.authority_scn);
		*out = decoded;
		outcome = decoded.outcome;
		reason = CLUSTER_TX_RESOLVE_NONE;

tx_done:
		;
	}
	PG_CATCH();
	{
		ConditionVariableCancelSleep();
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);
	if (reason_out != NULL)
		*reason_out = reason;
	return outcome;
}

/* Public TARGET requester boundary.  Admission dominates validation and
 * routing; the raw transport writes only private aligned scratch.  A caller
 * page is published only after the same token's positive final recheck. */
ClusterCrBuildResult
cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN read_scn,
									char dst_page[BLCKSZ],
									ClusterCrBuildReason *reason_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	PGAlignedBlock scratch;
	ClusterCrBuildReason raw_reason = CLUSTER_CR_BUILD_PROTOCOL;
	int32 real_master_node;
	volatile ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	volatile ClusterCrBuildResult result = CLUSTER_CR_BUILD_FAIL_CLOSED;

	if (reason_out != NULL)
		*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
	memset(&admission, 0, sizeof(admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		reason = admission_result == CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
					 ? CLUSTER_CR_BUILD_TARGET_DISABLED
					 : CLUSTER_CR_BUILD_RF_DEFERRED;
		result = CLUSTER_CR_BUILD_RETRYABLE;
		if (reason_out != NULL)
			*reason_out = (ClusterCrBuildReason)reason;
		return (ClusterCrBuildResult)result;
	}

	PG_TRY();
	{
		if (dst_page == NULL || reason_out == NULL || !SCN_VALID(read_scn))
			goto done;
		real_master_node = cluster_gcs_lookup_master(tag);
		if (real_master_node < 0
			|| real_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT) {
			reason = CLUSTER_CR_BUILD_WRONG_MASTER;
			result = CLUSTER_CR_BUILD_RETRYABLE;
			goto done;
		}

		result = gcs_block_r4_cr_fetch_and_wait_raw(
			tag, read_scn, real_master_node, scratch.data, &raw_reason);
		reason = raw_reason;
		if (result != CLUSTER_CR_BUILD_FULL)
			goto done;
		if (!cluster_semantic_activation_recheck(&admission)) {
			reason = CLUSTER_CR_BUILD_RF_DEFERRED;
			result = CLUSTER_CR_BUILD_RETRYABLE;
			goto done;
		}
		memcpy(dst_page, scratch.data, BLCKSZ);
		reason = CLUSTER_CR_BUILD_NONE;
		result = CLUSTER_CR_BUILD_FULL;

done:
		;
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();

	if (reason_out != NULL)
		*reason_out = (ClusterCrBuildReason)reason;
	return (ClusterCrBuildResult)result;
}


/*
 * PGRAC: spec-6.12b — requester-side CR fetch.
 *
 *	Ask origin_node's CR-server for the CR page of `tag` at read_scn.  The
 *	request rides the sub-case B wire shape (FORWARD payload direct to the
 *	serving node via the backend outbound ring; the HC108 chain on the
 *	direct-shipped reply validates forwarding_master == self).  The SCN
 *	carrier holds the snapshot read_scn (the CR path never runs the
 *	lost-write watermark verdict — the result is historical by intent).
 *
 *	true  -> dst_page holds the shipped CR page; *out_partial says whether
 *	         the local construction continues on it.
 *	false -> fail-closed: the caller keeps the unchanged 53R9G refusal
 *	         (timeout, DENIED, checksum failure — Rule 8.A).  The CR page
 *	         is NEVER installed as current and never flushed; it exists
 *	         only in the caller's CR destination.
 */
static bool
cluster_gcs_block_cr_fetch_and_wait_raw(BufferTag tag, SCN read_scn, int32 origin_node,
										char *dst_page, bool *out_partial)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool fetched = false;

	if (out_partial != NULL)
		*out_partial = false;
	if (dst_page == NULL || origin_node < 0 || origin_node == cluster_node_id)
		return false;

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_reserve_slot(tag, (uint8)PCM_TRANS_N_TO_S, cluster_node_id, &request_id);

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		slot->reply_received = false;
		memset(&slot->reply_header, 0, sizeof(slot->reply_header));
		memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->request_epoch = cluster_epoch_get_current();
		slot->expected_master_node = cluster_node_id;
		slot->stale = false;
		LWLockRelease(&blk->lock.lock);

		memset(&fwd, 0, sizeof(fwd));
		fwd.request_id = request_id;
		fwd.epoch = cluster_epoch_get_current();
		fwd.tag = tag;
		fwd.original_requester_node = cluster_node_id;
		fwd.requester_backend_id = (int32)MyBackendId;
		fwd.master_node = cluster_node_id;
		fwd.transition_id = (uint8)PCM_TRANS_N_TO_S;
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, read_scn);
		GcsBlockForwardPayloadSetCrRequest(&fwd, true);

		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)origin_node, &fwd, sizeof(fwd)))
			ereport(
				ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("cluster_gcs_block: failed to enqueue CR request to "
						"origin node %d",
						(int)origin_node),
				 errdetail("PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));

		deadline = GetCurrentTimestamp()
				   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			now = GetCurrentTimestamp();
			if (now >= deadline)
				break;
			timeout_ms = (long)((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
											  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		if (got_reply
			&& (slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_FULL
				|| slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL)) {
			uint32 expected = slot->reply_header.checksum;
			uint32 got = gcs_block_compute_checksum(slot->reply_block_data);

			if (expected == got) {
				memcpy(dst_page, slot->reply_block_data, BLCKSZ);
				/* spec-5.14 D2 class 2: this CR result is the origin's
				 * volatile construction — depend on it for fail-stop. */
				gcs_block_stamp_touched(origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
				if (out_partial != NULL)
					*out_partial
						= (slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL);
				fetched = true;
			} else {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
			}
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	return fetched; /* false -> caller keeps the unchanged 53R9G refusal */
}

ClusterSemanticAdmissionResult
cluster_r4_source_cr_dispatch(ClusterR4SourceCrOp op, const ClusterR4SourceCrRequest *request,
							  ClusterR4SourceCrResult *result)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticAdmissionResult admission;
	ClusterR4SourceCrResult local_result = { 0 };

	if (result != NULL)
		memset(result, 0, sizeof(*result));
	admission = cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												  CLUSTER_SEMANTIC_SOURCE_SIDE, &token);
	if (admission != CLUSTER_SEMANTIC_ADMISSION_OK)
		return admission;

	PG_TRY();
	{
		if (result == NULL || request == NULL || op != CLUSTER_R4_SOURCE_CR_FETCH
			|| request->dst_page == NULL)
			admission = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		else
			local_result.fetched = cluster_gcs_block_cr_fetch_and_wait_raw(
				request->tag, request->read_scn, request->origin_node, request->dst_page,
				&local_result.partial);
		if (admission == CLUSTER_SEMANTIC_ADMISSION_OK
			&& !cluster_semantic_activation_recheck(&token))
			admission = CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&token);
	}
	PG_END_TRY();

	if (admission == CLUSTER_SEMANTIC_ADMISSION_OK)
		*result = local_result;
	return admission;
}


/*
 * PGRAC: spec-6.12i D-i1 — requester-side undo-TT fetch.
 *
 *	Ask origin_node for its own TT-bearing undo header block (segment_id,
 *	block_no) plus the co-sampled live authority triple, riding the same
 *	sub-case B wire shape as the spec-6.12b CR fetch (FORWARD payload direct
 *	to the serving node; HC108 chain on the direct-shipped reply validates
 *	forwarding_master == self).  The tag is the SYNTHETIC undo address; the
 *	origin branches on the undo-fetch flag before any tag interpretation.
 *
 *	true  -> dst_page holds the origin-fresh block; *auth_out carries the
 *	         authority sampled ATOMICALLY with it (hdr.epoch / hdr.page_lsn /
 *	         trailer tt_generation).
 *	false -> fail-closed: timeout, DENIED, checksum failure, missing trailer
 *	         (Rule 8.A — the caller keeps its unchanged 53R97 refusal).  The
 *	         block is undo METADATA: never installed as current, never
 *	         flushed.
 */
bool
cluster_gcs_block_undo_tt_fetch_and_wait(int32 origin_node, uint32 segment_id, uint32 block_no,
										 char *dst_page, ClusterLiveAuthority *auth_out)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	BufferTag tag;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool fetched = false;

	if (dst_page == NULL || auth_out == NULL || origin_node < 0 || origin_node == cluster_node_id)
		return false;

	memset(auth_out, 0, sizeof(*auth_out));
	tag = GcsBlockUndoFetchTagMake(segment_id, block_no);

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_reserve_slot(tag, (uint8)PCM_TRANS_N_TO_S, cluster_node_id, &request_id);

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		slot->reply_received = false;
		memset(&slot->reply_header, 0, sizeof(slot->reply_header));
		memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->reply_undo_trailer_valid = false;
		slot->reply_undo_tt_generation = 0;
		slot->reply_undo_authority_scn = 0;
		slot->request_epoch = cluster_epoch_get_current();
		slot->expected_master_node = cluster_node_id;
		slot->stale = false;
		LWLockRelease(&blk->lock.lock);

		memset(&fwd, 0, sizeof(fwd));
		fwd.request_id = request_id;
		fwd.epoch = cluster_epoch_get_current();
		fwd.tag = tag;
		fwd.original_requester_node = cluster_node_id;
		fwd.requester_backend_id = (int32)MyBackendId;
		fwd.master_node = cluster_node_id;
		fwd.transition_id = (uint8)PCM_TRANS_N_TO_S;
		GcsBlockForwardPayloadSetUndoTtFetchRequest(&fwd, true);

		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)origin_node, &fwd, sizeof(fwd)))
			ereport(
				ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("cluster_gcs_block: failed to enqueue undo-TT fetch to "
						"origin node %d",
						(int)origin_node),
				 errdetail("PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));

		deadline = GetCurrentTimestamp()
				   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			now = GetCurrentTimestamp();
			if (now >= deadline)
				break;
			timeout_ms = (long)((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
											  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		if (got_reply && slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT
			&& slot->reply_undo_trailer_valid) {
			uint32 expected = slot->reply_header.checksum;
			uint32 got = gcs_block_compute_checksum(slot->reply_block_data);

			if (expected == got) {
				memcpy(dst_page, slot->reply_block_data, BLCKSZ);
				auth_out->origin_epoch = slot->reply_header.epoch;
				auth_out->live_hwm_lsn = (XLogRecPtr)slot->reply_header.page_lsn;
				auth_out->tt_generation = slot->reply_undo_tt_generation;
				auth_out->authority_scn = (SCN)slot->reply_undo_authority_scn;
				/* spec-5.14 D2: the authority is the origin's volatile
				 * co-sample — depend on it for fail-stop (D-i3). */
				gcs_block_stamp_touched(origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
				fetched = true;
			} else {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
			}
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	return fetched; /* false -> caller keeps the unchanged 53R97 refusal */
}


/*
 * gcs_block_undo_verdict_wire_exchange — shared TRANSPORT core of the two
 * verdict fetch wrappers below (spec-6.12i D-i4 owner-served kinds 2/3 and
 * spec-5.22d D4-6 authority-served kind 4): reserve slot, stamp, send, wait,
 * verify status + trailer + checksum, copy the raw reply material out.  ONE
 * implementation so the two wire legs can never drift apart mechanically
 * (Rule 8.A, the fill_page discipline); the acceptance POLICY — the page
 * structural gate, the authority reply binding, the co-sample extraction —
 * deliberately stays in the wrappers, so the v1 and authority policies can
 * never cross-contaminate.
 *
 *	stamped_epoch is written to BOTH slot->request_epoch and fwd.epoch (one
 *	read, one value — the pre-refactor code read the clock twice, which
 *	could straddle an epoch bump; single-stamping is strictly tighter).
 *
 *	true -> *hdr_out / *page_out / *tt_generation_out hold the checksum-
 *	verified reply material (page_out is the 48-byte verdict struct at the
 *	head of the BLCKSZ area; the checksum covered the whole area).
 *	false -> timeout / DENIED / wrong status / missing trailer / checksum
 *	mismatch (the caller keeps its 53R97 refusal, Rule 8.A).
 */
static bool
gcs_block_undo_verdict_wire_exchange(int32 dest_node, BufferTag tag, uint64 stamped_epoch,
									 TransactionId xid, SCN freshref_pair_scn,
									 bool authoritative, bool authority_kind,
									 GcsBlockReplyHeader *hdr_out,
									 ClusterGcsUndoVerdictPage *page_out, uint64 *tt_generation_out,
									 uint64 *authority_scn_out)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool fetched = false;
	bool freshref_pair = SCN_VALID(freshref_pair_scn);

	if (freshref_pair && (!authoritative || authority_kind))
		return false;

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_reserve_slot(tag, (uint8)PCM_TRANS_N_TO_S, cluster_node_id, &request_id);

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		slot->reply_received = false;
		memset(&slot->reply_header, 0, sizeof(slot->reply_header));
		memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->reply_undo_trailer_valid = false;
		slot->reply_undo_tt_generation = 0;
		slot->reply_undo_authority_scn = 0;
		slot->request_epoch = stamped_epoch;
		slot->expected_master_node = cluster_node_id;
		slot->stale = false;
		LWLockRelease(&blk->lock.lock);

		memset(&fwd, 0, sizeof(fwd));
		fwd.request_id = request_id;
		fwd.epoch = stamped_epoch;
		fwd.tag = tag;
		fwd.original_requester_node = cluster_node_id;
		fwd.requester_backend_id = (int32)MyBackendId;
		fwd.master_node = cluster_node_id;
		fwd.transition_id = (uint8)PCM_TRANS_N_TO_S;
		if (freshref_pair)
			GcsBlockForwardPayloadSetUndoFreshRefC1bPairRequest(&fwd);
		else if (authority_kind)
			GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(&fwd);
		else
			GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, authoritative);
		/* Kinds 2/4/5 carry the widened xid here.  Kind 10 binds xid in the
		 * synthetic tag and carries the exact retained page SCN instead. */
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
			&fwd, freshref_pair ? freshref_pair_scn : (SCN)(uint64)xid);

		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)dest_node, &fwd, sizeof(fwd))) {
			if (authority_kind)
				ereport(
					ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("cluster_gcs_block: failed to enqueue undo-verdict fetch "
							"to authority node %d",
							(int)dest_node),
					 errdetail(
						 "PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						 "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						 cluster_node_id)));
			ereport(
				ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("cluster_gcs_block: failed to enqueue undo-verdict fetch to "
						"origin node %d",
						(int)dest_node),
				 errdetail("PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));
		}

		deadline = GetCurrentTimestamp()
				   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			now = GetCurrentTimestamp();
			if (now >= deadline)
				break;
			timeout_ms = (long)((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
											  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		if (got_reply && slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
			&& slot->reply_undo_trailer_valid) {
			uint32 expected = slot->reply_header.checksum;
			uint32 got = gcs_block_compute_checksum(slot->reply_block_data);

			if (expected == got) {
				if (hdr_out != NULL)
					*hdr_out = slot->reply_header;
				if (page_out != NULL)
					memcpy(page_out, slot->reply_block_data, sizeof(*page_out));
				if (tt_generation_out != NULL)
					*tt_generation_out = slot->reply_undo_tt_generation;
				/* PGRAC: spec-7.1a D3 — the origin's co-sampled SCN clock
				 * rides the reply trailer; raw copy-out like the other
				 * carriers (the acceptance policy stays in the wrappers). */
				if (authority_scn_out != NULL)
					*authority_scn_out = slot->reply_undo_authority_scn;
				fetched = true;
			} else {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
			}
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	return fetched; /* false -> caller keeps the unchanged 53R97 refusal */
}

/*
 * PGRAC: spec-6.12i D-i4 / spec-6.15 D4 — requester-side undo verdict fetch.
 *
 *	Ask origin_node for a COMPLETE own-TT by-xid verdict on `xid`, riding the
 *	same sub-case B wire shape as the undo-TT fetch above.  The asked-for xid
 *	rides the widened watermark carrier; the synthetic tag keeps the ref's
 *	segment for tag validity + observability only (the verdict is complete
 *	over ALL origin segments).
 *
 *	true  -> *verdict_out holds the structurally validated verdict page
 *	         (cluster_vis_undo_verdict_page_usable) and *auth_out the
 *	         authority co-sampled with the scan.
 *	false -> fail-closed: timeout, DENIED, checksum failure, missing
 *	         trailer, malformed page (Rule 8.A — the caller keeps its
 *	         unchanged 53R97 refusal).
 */
bool
cluster_gcs_block_undo_verdict_fetch_and_wait(int32 origin_node, uint32 segment_id,
											  uint32 expected_tt_slot_id,
											  TransactionId xid, bool authoritative,
											  ClusterGcsUndoVerdictPage *verdict_out,
											  ClusterLiveAuthority *auth_out)
{
	GcsBlockReplyHeader hdr;
	ClusterGcsUndoVerdictPage page;
	uint64 tt_generation = 0;
	uint64 authority_scn = 0;
	BufferTag tag;

	if (verdict_out == NULL || auth_out == NULL || origin_node < 0 || origin_node == cluster_node_id
		|| !TransactionIdIsNormal(xid))
		return false;

	memset(verdict_out, 0, sizeof(*verdict_out));
	memset(auth_out, 0, sizeof(*auth_out));
	/*
	 * S3-P0-13: the existing synthetic tag's blockNum is unused by terminal
	 * complete-scan verdicts.  Reuse it (no wire-layout or ABI change) for
	 * the fresh-ref expected TT slot id.  An old origin ignores the value
	 * and can still return only the legacy terminal kinds; a new origin
	 * requires it before emitting the positive non-terminal kind 4.
	 */
	tag = GcsBlockUndoFetchTagMake(segment_id, expected_tt_slot_id);

	if (!gcs_block_undo_verdict_wire_exchange(origin_node, tag, cluster_epoch_get_current(), xid,
											  InvalidScn, authoritative,
											  false /* owner-served kind */, &hdr,
											  &page, &tt_generation, &authority_scn))
		return false;

	if (!cluster_vis_undo_verdict_page_usable(&page, xid))
		return false;

	*verdict_out = page;
	auth_out->origin_epoch = hdr.epoch;
	auth_out->live_hwm_lsn = (XLogRecPtr)hdr.page_lsn;
	auth_out->tt_generation = tt_generation;
	/* PGRAC: spec-7.1a D3 — the origin SCN clock co-sampled with the scan. */
	auth_out->authority_scn = (SCN)authority_scn;
	/* spec-5.14 D2: the verdict is the origin's volatile
	 * co-sample — depend on it for fail-stop (D-i3). */
	gcs_block_stamp_touched(origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	return true;
}

static bool
gcs_block_undo_freshref_c1b_pair_exchange_current(
	int32 origin_node, uint32 segment_id, uint32 expected_tt_slot_id,
	TransactionId xid, uint64 stamped_epoch, SCN proposed_scn,
	ClusterGcsUndoVerdictPage *verdict_out, ClusterLiveAuthority *auth_out)
{
	GcsBlockReplyHeader hdr;
	ClusterGcsUndoVerdictPage page;
	uint64 tt_generation = 0;
	uint64 authority_scn = 0;
	BufferTag tag;

	tag = GcsBlockUndoFreshRefC1bTagMake(
		segment_id, xid, expected_tt_slot_id);

	if (!gcs_block_undo_verdict_wire_exchange(
			origin_node, tag, stamped_epoch, xid, proposed_scn,
			true /* physical fresh-ref authority */, false /* live owner */, &hdr,
			&page, &tt_generation, &authority_scn))
		return false;
	if (hdr.sender_node != origin_node || hdr.epoch != stamped_epoch
		|| cluster_epoch_get_current() != stamped_epoch
		|| !cluster_vis_undo_verdict_page_usable(&page, xid)
		|| page.verdict != (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT
		|| page.commit_scn != proposed_scn || SCN_VALID(page.horizon_scn))
		return false;

	*verdict_out = page;
	auth_out->origin_epoch = hdr.epoch;
	auth_out->live_hwm_lsn = (XLogRecPtr)hdr.page_lsn;
	auth_out->tt_generation = tt_generation;
	auth_out->authority_scn = (SCN)authority_scn;
	gcs_block_stamp_touched(origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	return true;
}

/* S8-815PRE-FRESHREF-C1B-01 requester leg.  The exact physical ref binds xid
 * in tag.relNumber and the retained page SCN in the existing 64-bit scalar;
 * no wire byte or authority is added.  At the legitimate clean-formation
 * epoch zero, a current R4 TARGET admission is held across the whole exchange;
 * syntactic epoch equality alone is never authority. */
bool
cluster_gcs_block_undo_freshref_c1b_pair_fetch_and_wait(
	int32 origin_node, uint32 segment_id, uint32 expected_tt_slot_id,
	TransactionId xid, uint32 ref_epoch, SCN proposed_scn,
	ClusterGcsUndoVerdictPage *verdict_out, ClusterLiveAuthority *auth_out)
{
	ClusterSemanticAdmissionToken zero_epoch_admission;
	uint64 stamped_epoch;
	bool result = false;

	if (verdict_out == NULL || auth_out == NULL || origin_node < 0
		|| origin_node == cluster_node_id || segment_id == 0
		|| segment_id > UINT16_MAX || expected_tt_slot_id < 1
		|| expected_tt_slot_id > TT_SLOTS_PER_SEGMENT
		|| !TransactionIdIsNormal(xid) || !SCN_VALID(proposed_scn))
		return false;
	memset(verdict_out, 0, sizeof(*verdict_out));
	memset(auth_out, 0, sizeof(*auth_out));
	memset(&zero_epoch_admission, 0, sizeof(zero_epoch_admission));

	stamped_epoch = cluster_epoch_get_current();
	if (stamped_epoch > UINT32_MAX
		|| ref_epoch != (uint32)stamped_epoch)
		return false;
	if (stamped_epoch != 0)
		return gcs_block_undo_freshref_c1b_pair_exchange_current(
			origin_node, segment_id, expected_tt_slot_id, xid, stamped_epoch,
			proposed_scn, verdict_out, auth_out);
	if (!cluster_runtime_visibility_zero_epoch_pair_admission_enter(
			&zero_epoch_admission))
		return false;
	PG_TRY();
	{
		result = gcs_block_undo_freshref_c1b_pair_exchange_current(
			origin_node, segment_id, expected_tt_slot_id, xid, stamped_epoch,
			proposed_scn, verdict_out, auth_out);
		if (result
			&& !cluster_semantic_activation_recheck(&zero_epoch_admission)) {
			memset(verdict_out, 0, sizeof(*verdict_out));
			memset(auth_out, 0, sizeof(*auth_out));
			result = false;
		}
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&zero_epoch_admission);
	}
	PG_END_TRY();
	return result;
}

/*
 * PGRAC: spec-5.22d D4-6 — requester-side dead-owner AUTHORITY verdict fetch.
 *
 *	Ask the elected serve authority (a live survivor, NOT the dead owner) for
 *	a block0-proven verdict on the dead owner_node's `xid`.  Kind-4 wire: the
 *	owner rides in tag.relNumber (owner+1), the widened xid in the watermark
 *	carrier.  The caller has already gated on the peer's HELLO D4 capability.
 *
 *	Acceptance is the 8.A-amended FULL binding, strictly tighter than the v1
 *	leg above: the transport core's status/trailer/checksum verify, PLUS
 *	sender == elected authority AND reply epoch == stamped epoch EXACTLY
 *	(cluster_vis_undo_authority_reply_binding_ok; the transport's HC100 >= is
 *	only a pre-filter), PLUS the version-2 authority structural gate + mapper
 *	(cluster_undo_verdict_from_authority_wire_page — refuses a v1 page, a
 *	smuggled horizon bound, an echo mismatch).  The reply's hwm/tt_generation
 *	carriers are deliberately IGNORED: they describe an origin's own live TT
 *	plane, which does not exist for a dead owner — the block0 prove on the
 *	serve side already internalized generation/wrap coverage.
 *
 *	true -> *out holds COMMITTED_EXACT{commit_scn, wrap} or ABORTED.  The
 *	caller MUST Lamport-observe any commit_scn it consumes (AD-008).
 *	false -> fail-closed (caller keeps the 53R97 refusal, Rule 8.A).
 */
bool
cluster_gcs_block_undo_authority_verdict_fetch_and_wait(int32 authority_node, int32 owner_node,
														uint32 segment_id, TransactionId xid,
														ClusterUndoVerdictResult *out)
{
	GcsBlockReplyHeader hdr;
	ClusterGcsUndoVerdictPage page;
	uint64 tt_generation = 0;
	BufferTag tag;
	uint64 stamped_epoch;
	ClusterUndoVerdictResult r;

	if (out == NULL)
		return false;
	out->kind = (uint8)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	out->commit_scn = InvalidScn;
	out->wrap = 0;

	if (authority_node < 0 || authority_node == cluster_node_id || owner_node < 0
		|| owner_node == cluster_node_id || owner_node == authority_node
		|| !TransactionIdIsNormal(xid))
		return false;

	tag = GcsBlockUndoAuthorityFetchTagMake(segment_id, 0, owner_node);
	stamped_epoch = cluster_epoch_get_current();

	if (!gcs_block_undo_verdict_wire_exchange(authority_node, tag, stamped_epoch, xid,
											  InvalidScn,
											  false /* no owner-served sub-kind */,
											  true /* kind 4 */, &hdr, &page, &tt_generation,
											  NULL /* authority co-sample: live-TT plane
													* carriers are ignored on kind 4 */))
		return false;

	/* 8.A amend: full reply binding — sender IS the elected authority and
	 * the reply epoch IS the stamped epoch, EXACTLY. */
	if (!cluster_vis_undo_authority_reply_binding_ok((int32)hdr.sender_node, authority_node,
													 hdr.epoch, stamped_epoch))
		return false;

	r = cluster_undo_verdict_from_authority_wire_page(&page, xid);
	if (r.kind == (uint8)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED)
		return false;

	/* spec-5.14 D2: the verdict is the AUTHORITY's volatile derivation —
	 * depend on the authority for fail-stop (the dead owner cannot fail any
	 * further). */
	gcs_block_stamp_touched(authority_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	*out = r;
	return true;
}


/*
 * PGRAC: spec-7.1 D3-b — requester-side batched multixact member-verdict fetch.
 *
 *	Ask origin_node for a per-member verdict on the foreign multixact `mxid`,
 *	riding the same sub-case B wire shape as the single verdict fetch.  The
 *	asked-for MXID rides the widened watermark carrier (upper 32 bits zero);
 *	the synthetic tag keeps a placeholder segment (0) — the member scan is
 *	complete over the multi's own pg_multixact, so the tag scopes nothing.
 *
 *	true  -> page_out (BLCKSZ) holds the structurally validated SERVED page and
 *	         *auth_out the co-sampled authority; every member SCN that crossed
 *	         the wire is Lamport-observed (AD-008) so a below-horizon bound is
 *	         admissible on the next snapshot.
 *	false -> fail-closed: timeout, DENIED, checksum failure, missing trailer,
 *	         non-SERVED status, malformed page (Rule 8.A — the caller keeps its
 *	         unchanged 53R97 refusal).
 */
bool
cluster_gcs_block_undo_multi_verdict_fetch_and_wait(int32 origin_node, MultiXactId mxid,
													char *page_out, ClusterLiveAuthority *auth_out)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	BufferTag tag;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool fetched = false;

	if (page_out == NULL || auth_out == NULL || origin_node < 0 || origin_node == cluster_node_id
		|| !MultiXactIdIsValid(mxid))
		return false;

	memset(page_out, 0, BLCKSZ);
	memset(auth_out, 0, sizeof(*auth_out));
	tag = GcsBlockUndoFetchTagMake(0, 0); /* placeholder segment (scan is complete) */

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_reserve_slot(tag, (uint8)PCM_TRANS_N_TO_S, cluster_node_id, &request_id);

	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		slot->reply_received = false;
		memset(&slot->reply_header, 0, sizeof(slot->reply_header));
		memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->reply_undo_trailer_valid = false;
		slot->reply_undo_tt_generation = 0;
		slot->reply_undo_authority_scn = 0;
		slot->request_epoch = cluster_epoch_get_current();
		slot->expected_master_node = cluster_node_id;
		slot->stale = false;
		LWLockRelease(&blk->lock.lock);

		memset(&fwd, 0, sizeof(fwd));
		fwd.request_id = request_id;
		fwd.epoch = cluster_epoch_get_current();
		fwd.tag = tag;
		fwd.original_requester_node = cluster_node_id;
		fwd.requester_backend_id = (int32)MyBackendId;
		fwd.master_node = cluster_node_id;
		fwd.transition_id = (uint8)PCM_TRANS_N_TO_S;
		GcsBlockForwardPayloadSetUndoMultiVerdictRequest(&fwd, true);
		/* The widened mxid rides the watermark carrier (upper 32 bits zero). */
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)(uint64)mxid);

		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)origin_node, &fwd, sizeof(fwd)))
			ereport(
				ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("cluster_gcs_block: failed to enqueue undo-multi-verdict fetch "
						"to origin node %d",
						(int)origin_node),
				 errdetail("PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));

		deadline = GetCurrentTimestamp()
				   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			now = GetCurrentTimestamp();
			if (now >= deadline)
				break;
			timeout_ms = (long)((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
											  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		if (got_reply
			&& slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT
			&& slot->reply_undo_trailer_valid) {
			uint32 expected;
			uint32 got;

			/*
			 * PGRAC (spec-7.1 D3-b hardening, Rule 15/16): snapshot the volatile
			 * reply slot into the caller's STABLE page BEFORE checksum /
			 * validation / observe.  This consume runs without blk->lock, so a
			 * spec-2.34 retransmit that overwrites reply_block_data between a
			 * validate-on-slot and the copy would hand a torn, unvalidated
			 * nmembers to the variable-length member loop downstream (OOB read).
			 * Validating the LOCAL copy makes the bytes we act on exactly the
			 * bytes we prove usable: a torn copy fails the checksum (fail-closed),
			 * and nmembers is bounded to [2, MAX] before any member is read.  The
			 * checksum is read block-then-header, so an overwrite that lands mid-
			 * snapshot fails the compare rather than passing on a mixed pair.
			 */
			memcpy(page_out, slot->reply_block_data, GCS_BLOCK_DATA_SIZE);
			expected = slot->reply_header.checksum;
			got = gcs_block_compute_checksum(page_out);

			if (expected == got) {
				const ClusterGcsUndoMultiVerdictPage *v
					= (const ClusterGcsUndoMultiVerdictPage *)page_out;

				if (cluster_vis_undo_multi_verdict_page_usable(v, mxid)) {
					uint16 i;

					auth_out->origin_epoch = slot->reply_header.epoch;
					auth_out->live_hwm_lsn = (XLogRecPtr)slot->reply_header.page_lsn;
					auth_out->tt_generation = slot->reply_undo_tt_generation;
					auth_out->authority_scn = (SCN)slot->reply_undo_authority_scn;
					cluster_scn_observe(auth_out->authority_scn);
					/* AD-008: Lamport-observe every member SCN that crossed the
					 * wire so a below-horizon bound is admissible next snapshot. */
					for (i = 0; i < v->nmembers; i++) {
						if (SCN_VALID(v->members[i].commit_scn))
							cluster_scn_observe((SCN)v->members[i].commit_scn);
						if (SCN_VALID(v->members[i].horizon_scn))
							cluster_scn_observe((SCN)v->members[i].horizon_scn);
					}
					/* spec-5.14 D2: depend on the origin's volatile co-sample
					 * for fail-stop (D-i3). */
					gcs_block_stamp_touched(origin_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
					fetched = true;
				}
			} else {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
			}
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	return fetched; /* false -> caller keeps the unchanged 53R97 refusal */
}


/*
 * PGRAC: spec-5.2 D11 — local-master writer-transfer (revoke) + wait.
 *
 *	When THIS node is the GCS master for a block that a REMOTE node holds in X,
 *	and a LOCAL WRITER needs X (cross-node TX row-lock wait), the tag-only
 *	acquire path cannot serve the write (no data plane, and the 3-way
 *	writer-transfer needs a third-node master).  Here the master forwards an
 *	N→X X-transfer request straight to the holder; the holder ships its CURRENT
 *	image (carrying the uncommitted ITL row-lock the writer will wait on) AND
 *	releases its own X (invalidating its local copy so it can never flush a
 *	stale page).  This node installs the bytes under content_lock EXCLUSIVE and
 *	records itself as the new X holder on the master GRD entry — a DURABLE X
 *	grant (returns true).  The caller (bufmgr) then mirrors buf->pcm_state = X;
 *	the heap AM sees the remote row lock and enters the cross-node TX completion
 *	wait (spec-5.2 D4/D5).
 *
 *	Returns true (durable X).  Fails closed (ereport) if no X image can be
 *	obtained — never a silent stale grant (Rule 8.A).  This is the write analog
 *	of cluster_gcs_local_master_read_image_and_wait.
 */
bool
cluster_gcs_local_master_x_transfer_and_wait(BufferDesc *buf, const PcmAuthoritySnapshot *expected,
											 bool clean_eligible, bool *out_retry_denied)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	BufferTag tag;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool installed = false;
	bool read_image = false; /* spec-5.2 D11 — holder deferred (active ITL) */
	uint8 reply_status = (uint8)GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE; /* spec-5.2a D3 */
	XLogRecPtr installed_page_lsn = InvalidXLogRecPtr;
	SCN installed_page_scn = InvalidScn;
	int32 holder_node;
	PcmXTransferCommitResult commit_result;

	Assert(out_retry_denied != NULL);
	if (buf == NULL || expected == NULL || out_retry_denied == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_local_master_x_transfer_and_wait: invalid exact input"),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	*out_retry_denied = false;
	holder_node = expected->x_holder_node;
	if (expected->state != PCM_STATE_X || holder_node < 0 || holder_node >= 32
		|| holder_node == cluster_node_id || expected->s_holders_bitmap != 0
		|| expected->master_holder.node_id != (uint32)holder_node)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_gcs_block: invalid remote-X authority for exact transfer"),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));

	tag = buf->tag;
	/* P0-26 first barrier: do not emit a request for an authority token
	 * already displaced by another queue winner/session. */
	if (!cluster_pcm_lock_authority_matches(tag, expected)) {
		*out_retry_denied = true;
		return false;
	}
	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_reserve_slot(tag, (uint8)PCM_TRANS_N_TO_X, cluster_node_id, &request_id);
	PG_TRY();
	{
		ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
		TimestampTz deadline;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		slot->reply_received = false;
		memset(&slot->reply_header, 0, sizeof(slot->reply_header));
		memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->request_epoch = cluster_epoch_get_current();
		slot->expected_master_node = cluster_node_id;
		slot->stale = false;
		LWLockRelease(&blk->lock.lock);

		memset(&fwd, 0, sizeof(fwd));
		fwd.request_id = request_id;
		fwd.epoch = cluster_epoch_get_current();
		fwd.tag = tag;
		fwd.original_requester_node = cluster_node_id; /* reply returns to us */
		fwd.requester_backend_id = (int32)MyBackendId;
		fwd.master_node = cluster_node_id;
		fwd.transition_id = (uint8)PCM_TRANS_N_TO_X;
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
			&fwd, cluster_pcm_lock_pi_watermark_scn_query(tag));
		GcsBlockForwardPayloadSetXTransfer(&fwd, true);
		/* spec-5.2a D1/D3: an eligible (clean sequence-page) X-transfer tells
		 * the holder to flush the data page to shared storage before dropping
		 * (flush-data-before-drop, D4) so a later storage-fallback reads the
		 * current value, not a stale one (inv③). */
		GcsBlockForwardPayloadSetCleanEligible(&fwd, clean_eligible);

		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);
		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)holder_node, &fwd, sizeof(fwd)))
			ereport(
				ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("cluster_gcs_block: failed to enqueue X-transfer FORWARD "
						"to X holder %d",
						holder_node),
				 errdetail("PGRAC_FAMILY=TRANSPORT PGRAC_REASON=TRANSPORT_OUTBOUND_ENQUEUE_REFUSED "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
						   cluster_node_id)));

		deadline = GetCurrentTimestamp()
				   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;) {
			TimestampTz now;
			long timeout_ms;
			bool have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			LWLockRelease(&blk->lock.lock);
			if (have_reply) {
				got_reply = true;
				break;
			}
			now = GetCurrentTimestamp();
			if (now >= deadline)
				break;
			timeout_ms = (long)((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
											  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		/* spec-5.2a D3: capture the reply status before the slot is released so
		 * the clean-page stale-holder break (below) can distinguish a holder
		 * DENIED_MASTER_NOT_HOLDER (holder already dropped to N — durable on
		 * storage, safe to storage-fallback) from a timeout (cannot prove
		 * durable — must fail-closed). */
		if (got_reply)
			reply_status = slot->reply_header.status;

		if (got_reply
			&& slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER) {
			uint32 expected = slot->reply_header.checksum;
			uint32 got = gcs_block_compute_checksum(slot->reply_block_data);

			/*
			 * spec-5.2a D3 / L5 — FAITHFUL stale-holder injection.  The holder
			 * has REALLY shipped its current image, (eager-)flushed it to shared
			 * storage, and dropped its copy to N (drop_no_wire) before sending
			 * this X_GRANTED reply.  Skipping the install here leaves the master
			 * still recording the now-N holder (we never call
			 * master_take_x_after_transfer), which is exactly the F0-4 / F0-7
			 * stale-holder state (reachable in production via a checksum mismatch
			 * or an interrupt in the post-ship/pre-install window).  The next
			 * eligible request then forwards to the now-N holder, gets
			 * DENIED_MASTER_NOT_HOLDER, and exercises the storage-fallback break.
			 * One-shot (should_skip consumes the arm).
			 */
			CLUSTER_INJECTION_POINT("cluster-clean-xfer-stale-holder");
			if (clean_eligible
				&& cluster_injection_should_skip("cluster-clean-xfer-stale-holder")) {
				/* leave installed = false: faithful stale holder created. */
			} else if (expected == got) {
				installed_page_lsn = (XLogRecPtr)slot->reply_header.page_lsn;
				/* step 1b: capture the shipped pd_block_scn NOW — the slot is
				 * released before the take-X below, so a later read of
				 * reply_block_data would be use-after-release. */
				installed_page_scn = (SCN)((PageHeader)slot->reply_block_data)->pd_block_scn;
				gcs_block_install_reply_block(buf, slot->reply_block_data, installed_page_lsn,
											  slot);
				/* spec-5.14 D2 class 2: consumed the remote holder's X image. */
				gcs_block_stamp_touched(holder_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
				installed = true;
			} else {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
			}
		} else if (got_reply && !clean_eligible
				   && slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER) {
			/*
			 * spec-5.2a D4: for a clean (sequence) eligible request a read-image
			 * reply means the holder could not cleanly relinquish (transient pin
			 * / re-dirty) and KEPT its X.  We must NOT install a non-owned
			 * read-image of a page we intend to write (no seq write guard) and
			 * must NOT storage-fallback (the holder may hold a newer copy).
			 * Skip the install here; the post-loop fail-closed retryable path
			 * (clean_busy_retry) handles it.  The spec-5.2 §3.5 D11 install
			 * below is for the heap active-ITL deferral only (!clean_eligible).
			 *
			 * PGRAC: spec-5.2 §3.5 D11 — the holder DEFERRED the
			 * X-transfer because it still has an uncommitted ITL slot on this
			 * block (its own commit needs it).  It shipped a read-image and kept
			 * its X.  Install the bytes (so the heap AM sees the holder's row
			 * lock) and return NON-durable: pcm_state stays N, we do NOT record
			 * ourselves as the X holder.  The caller's heap_update/heap_lock_tuple
			 * sees the remote lock and enters the cross-node TX completion wait
			 * (spec-5.2 D4/D5); when the wait helper reacquires the buffer content
			 * lock (heapam.c) it re-runs this acquire — by then the holder is
			 * terminal, so the X-transfer is granted (X_GRANTED_FROM_HOLDER) with
			 * the committed image.  Rule 8.A: never a stale durable grant. */
			uint32 expected = slot->reply_header.checksum;
			uint32 got = gcs_block_compute_checksum(slot->reply_block_data);

			if (expected == got) {
				gcs_block_install_reply_block(buf, slot->reply_block_data,
											  (XLogRecPtr)slot->reply_header.page_lsn, slot);
				/* spec-5.14 D2 class 2: consumed the remote holder's deferred-writer image. */
				gcs_block_stamp_touched(holder_node, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
				/* This WRITE result is deliberately non-durable: keep PCM N and
				 * let the target-only writer owner retry/fail closed. GCS may
				 * install verified bytes but never publishes READ_IMAGE. */
				read_image = true;
			} else {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
			}
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	if (installed) {
		/* P0-26 final barrier: the slot/request id isolates late wire
		 * replies; this exact authority compare isolates a valid old reply
		 * from a newer queue handoff, holder generation, or session. */
		commit_result = cluster_pcm_lock_master_take_x_after_transfer(
			tag, expected, installed_page_lsn, installed_page_scn, holder_node,
			(uint32)MyProc->pgprocno, request_id, fwd.epoch);
		if (commit_result == PCM_X_TRANSFER_COMMIT_STALE
			|| commit_result == PCM_X_TRANSFER_COMMIT_NOT_FOUND) {
			*out_retry_denied = true;
			return false;
		}
		if (commit_result != PCM_X_TRANSFER_COMMIT_OK)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster_gcs_block: invalid exact X-transfer commit state"),
							errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
									  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
									  cluster_node_id)));
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_granted_from_holder_count, 1);
		if (clean_eligible)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_count, 1);
		return true;
	}
	/* A denial/timeout from a holder whose authority was concurrently
	 * replaced is not a client terminal.  Return to bufmgr so the exact
	 * GRANT_PENDING token and request id are both replaced. */
	if (!cluster_pcm_lock_authority_matches(tag, expected)) {
		*out_retry_denied = true;
		return false;
	}

	if (read_image)
		/* spec-5.2 D11 deferral (active ITL): non-durable read-image installed;
		 * leave buf->pcm_state == N so the caller falls back to the TX wait and
		 * re-acquires X after the holder is terminal. */
		return false;

	/*
	 * PGRAC: spec-5.2a D4 — clean-page BUSY (holder kept X).  A clean eligible
	 * request got a read-image reply: the holder could not relinquish (transient
	 * pin / re-dirty) and still owns X.  Fail closed RETRYABLE — never storage-
	 * fallback against a holder that may hold a newer copy, never write a
	 * non-owned read-image.  The transaction retries; by then the holder is
	 * unpinned and the transfer (or stale-holder recovery) completes (Rule 8.A).
	 */
	if (clean_eligible && got_reply
		&& reply_status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 1);
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CLEAN_PAGE_XFER_UNAVAILABLE),
						errmsg("cluster_gcs_block: clean-page X-transfer holder %d transiently "
							   "busy for tag spc=%u db=%u relNumber=%u block=%u",
							   holder_node, tag.spcOid, tag.dbOid,
							   (unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum),
						errhint("The X holder could not relinquish a clean page (pinned or "
								"re-dirtied); retry the transaction."),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	}

	/*
	 * PGRAC: spec-2.41 D5 — terminal lost-write from the holder-forward detector.
	 *
	 *	The holder ran gcs_block_lost_write_verdict() on its copied page and
	 *	replied DENIED_LOST_WRITE (the shipped pd_block_scn is below the master
	 *	pi_watermark_scn, or a tracked block shipped an unstamped page; §2.6).
	 *	This is a TERMINAL data-integrity event — NOT the transient "holder did
	 *	not ship in time" fail-closed below — so map it to the precise 53R93
	 *	instead of the retryable FEATURE_NOT_SUPPORTED.  Mirrors the master-direct
	 *	requester handling (the master-side ship detector twin);
	 *	cluster.gcs_block_lost_write_action selects ERROR (production, default) or
	 *	WARNING (staging diagnostic — then still fail-closed below, since the
	 *	holder shipped no page; on THIS holder-forward path no stale image is
	 *	granted, Rule 8.A.  Path-specific, NOT a blanket guarantee: the
	 *	master-direct / storage-fallback WARN terminal instead proceeds with a
	 *	possibly-stale storage-fallback block — a staging-only diagnostic risk).
	 */
	if (got_reply && reply_status == (uint8)GCS_BLOCK_REPLY_DENIED_LOST_WRITE) {
		/* S3 forensics step 1 — THIS node is the master on the local-master
		 * X-transfer path, so both the expected watermark SENT to the holder
		 * (fwd payload) and the authoritative watermark NOW are known here;
		 * the holder's LOG line carries the shipped pd_block_scn it refused
		 * (correlate by tag + request_id).  Three-branch qualification: a
		 * NOW > SENT drift flags a watermark advance racing the transfer;
		 * local pd_block_scn is this requester's (pre-transfer) copy. */
		SCN forens_expected_sent = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd);
		SCN forens_master_wm_now = cluster_pcm_lock_pi_watermark_scn_query(tag);
		SCN forens_local_scn = cluster_bufmgr_read_block_scn_for_gcs(buf);
		uint64 forens_own_gen = cluster_pcm_own_gen_get(buf->buf_id);
		/* Step 1a — this node is the master: the provenance of the advance
		 * that produced the expected watermark is authoritative here. */
		ClusterPcmWmProv wm_prov;
		bool wm_have = cluster_pcm_lock_pi_watermark_prov_query(tag, &wm_prov);

		if (cluster_gcs_block_lost_write_action == 0 /* ERROR */)
			ereport(
				ERROR,
				(errcode(ERRCODE_CLUSTER_LOST_WRITE_DETECTED),
				 errmsg("cluster_gcs_block: lost write detected on tag "
						"spc=%u db=%u relNumber=%u block=%u",
						tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
						(unsigned int)tag.blockNum),
				 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
						   "PGRAC_NODE=%d PGRAC_ATTEMPT=0 "
						   "request_id=" UINT64_FORMAT " epoch=" UINT64_FORMAT " holder=%d fork=%d"
						   " expected pi_watermark_scn sent=" UINT64_FORMAT
						   " master pi_watermark_scn now=" UINT64_FORMAT
						   " local pd_block_scn=" UINT64_FORMAT " ownership_gen=" UINT64_FORMAT
						   " wm_src=%s wm_sender=%d wm_request_id=" UINT64_FORMAT
						   " wm_epoch=" UINT64_FORMAT " wm_old=" UINT64_FORMAT
						   " wm_new=" UINT64_FORMAT " wm_matches_expected=%d.",
						   cluster_node_id, request_id, fwd.epoch, holder_node, (int)tag.forkNum,
						   (uint64)forens_expected_sent, (uint64)forens_master_wm_now,
						   (uint64)forens_local_scn, forens_own_gen,
						   wm_prov.table_full ? "none(prov-table-full)"
											  : cluster_pcm_wm_src_text(wm_prov.source),
						   wm_have ? wm_prov.sender_node : -1, wm_have ? wm_prov.request_id : 0,
						   wm_have ? wm_prov.epoch : 0, wm_have ? (uint64)wm_prov.old_scn : 0,
						   wm_have ? (uint64)wm_prov.new_scn : 0,
						   wm_have ? (int)(wm_prov.new_scn == forens_expected_sent) : -1),
				 errhint("The holder-forward shipped block.pd_block_scn is below the "
						 "master pi_watermark_scn (or a tracked block shipped an "
						 "unstamped page).  Inspect dump_gcs.lost_write_detected_count "
						 "and cluster_pcm_grd to find the stale source.  spec-2.41 D5.")));
		else
			ereport(WARNING,
					(errmsg("cluster_gcs_block: lost write detected on tag "
							"spc=%u db=%u relNumber=%u block=%u (action=warn)",
							tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
							(unsigned int)tag.blockNum),
					 errdetail("request_id=" UINT64_FORMAT " epoch=" UINT64_FORMAT
							   " holder=%d fork=%d expected pi_watermark_scn sent=" UINT64_FORMAT
							   " master pi_watermark_scn now=" UINT64_FORMAT
							   " local pd_block_scn=" UINT64_FORMAT " ownership_gen=" UINT64_FORMAT
							   " wm_src=%s wm_sender=%d wm_request_id=" UINT64_FORMAT
							   " wm_epoch=" UINT64_FORMAT " wm_old=" UINT64_FORMAT
							   " wm_new=" UINT64_FORMAT " wm_matches_expected=%d.",
							   request_id, fwd.epoch, holder_node, (int)tag.forkNum,
							   (uint64)forens_expected_sent, (uint64)forens_master_wm_now,
							   (uint64)forens_local_scn, forens_own_gen,
							   wm_prov.table_full ? "none(prov-table-full)"
												  : cluster_pcm_wm_src_text(wm_prov.source),
							   wm_have ? wm_prov.sender_node : -1, wm_have ? wm_prov.request_id : 0,
							   wm_have ? wm_prov.epoch : 0, wm_have ? (uint64)wm_prov.old_scn : 0,
							   wm_have ? (uint64)wm_prov.new_scn : 0,
							   wm_have ? (int)(wm_prov.new_scn == forens_expected_sent) : -1)));
	}

	/*
	 * PGRAC: spec-5.2a D3 — clean-page stale-holder break (Q3=A, inv② / inv③).
	 *
	 *	The holder we forwarded to replied DENIED_MASTER_NOT_HOLDER: it is LIVE
	 *	but no longer resident for this tag (it already dropped its copy to N).
	 *	For a normal heap transfer this is a transient evict race the requester
	 *	retransmits through — but for a clean (sequence) page that loops forever
	 *	against an ex-holder the master still records (F0-4 / F0-7).  We break
	 *	the loop here because a clean page is recoverable from shared storage:
	 *
	 *	  - 8.A storage currency: every cross-node X transfer of this (clean,
	 *	    eligible) page used flush-data-before-drop (spec-5.2a D4) OR a normal
	 *	    eviction FlushBuffer, so the shared data file reflects the current
	 *	    value.  The holder being NOT resident means it already dropped, after
	 *	    flushing.  (A timeout — got_reply == false — is NOT this case: we
	 *	    cannot prove the holder dropped/flushed, so it falls through to the
	 *	    fail-closed ereport below.  Rule 8.A.)
	 *	  - 8.A single-X owner: record self as the new X holder (clearing the
	 *	    stale holder) BEFORE returning; the ex-holder is already N.  No two-X
	 *	    window.
	 *	  - buf currency: the caller (ReadBuffer) populated buf from shared
	 *	    storage and this node holds no stale cached copy of a page it does
	 *	    not own (CF invalidation invariant), so buf reflects the current
	 *	    value — the same contract the remote GRANTED_STORAGE_FALLBACK path
	 *	    relies on.
	 */
	if (gcs_block_clean_xfer_should_stale_break(clean_eligible, got_reply, reply_status)) {
		/*
		 * PGRAC: spec-5.2a D3 — clean-page stale holder, FAIL CLOSED (Q3 amended
		 * 2026-06-21).  The recorded holder is LIVE but no longer resident (it
		 * dropped to N).  The frozen spec's Q3=A storage-fallback recovery is
		 * NOT sound on Stage-5 shared storage: it is not cross-instance coherent
		 * ("cross-instance cache invalidation ... not yet activated"), so a
		 * recovering node's storage read returns its OWN stale view, reissuing
		 * already-issued sequence values — a Rule 8.A duplicate-number violation
		 * (proven by t/284 L5).  So we fail closed RETRYABLE rather than read a
		 * stale page: the normal CF image-ship path self-heals once the holder's
		 * buffer is resident again, and a genuinely-gone holder is a retry /
		 * Stage-4 recovery concern.  A sound storage-fallback + cross-instance
		 * cache invalidation land in Stage 6.
		 */
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 1);
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CLEAN_PAGE_XFER_UNAVAILABLE),
						errmsg("cluster_gcs_block: clean-page X-transfer holder %d is no longer "
							   "resident (stale holder) for tag spc=%u db=%u relNumber=%u block=%u",
							   holder_node, tag.spcOid, tag.dbOid,
							   (unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum),
						errhint("The recorded holder dropped its copy and storage-fallback is not "
								"cross-instance coherent on this stage; retry the transaction."),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));
	}

	/* No X image obtained (timeout / holder evict / denial) — fail closed,
	 * never a silent stale grant (Rule 8.A). */
	if (clean_eligible)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 1);

	/*
	 * PGRAC: GCS serve-stall round-6 — a transient revoke deny (the holder
	 * fail-closed the drop with DENIED_MASTER_NOT_HOLDER / DENIED_PENDING_X
	 * because the copy was pinned (round-5 A2) or a local writer committed
	 * inside the copy->drop window (round-6 generation gate)) is a RETRYABLE
	 * condition: the re-serve ships the current page once the pin clears / the
	 * window is done.  Surface the retryable class-53 code (53R9X) so an
	 * application driver retries the statement, instead of FEATURE_NOT_SUPPORTED
	 * (0A000), which reads as "permanently unsupported" and is never retried.
	 * A genuine timeout (got_reply == false) keeps 0A000: we could not prove
	 * the holder's state, so it is not a bounded transient.
	 */
	if (got_reply
		&& (reply_status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
			|| reply_status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CLEAN_PAGE_XFER_UNAVAILABLE),
						errmsg("cluster_gcs_block: X holder %d transiently refused the transfer "
							   "for tag spc=%u db=%u relNumber=%u block=%u",
							   holder_node, tag.spcOid, tag.dbOid,
							   (unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum),
						errhint("The holder's copy was pinned, or a local writer committed during "
								"the transfer; retry the transaction."),
						errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
								  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
								  cluster_node_id)));

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_gcs_block: could not obtain X transfer from X holder %d "
						   "for tag spc=%u db=%u relNumber=%u block=%u",
						   holder_node, tag.spcOid, tag.dbOid,
						   (unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum),
					errhint("The X holder did not ship a current image in time; retry."),
					errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN "
							  "PGRAC_NODE=%d PGRAC_ATTEMPT=0 ",
							  cluster_node_id)));
	return true; /* unreachable */
}


/* ============================================================
 * Receiver: master-side (D5).
 *
 *	HC82 invariant: XLogFlush(page_lsn) BEFORE shipping bytes;  enforced by
 *	cluster_bufmgr_copy_block_for_gcs (D4).  HC88: master-not-holder + state=N
 *	→ GRANTED_STORAGE_FALLBACK; state!=N → DENIED_MASTER_NOT_HOLDER fail-closed.
 *	HC89 revalidation single-retry lives inside the bufmgr helper.
 *
 *	Transition apply MUST NOT precede buffer availability decision (HC88).
 * ============================================================ */

/*
 * cluster_gcs_block_note_send_outcome — GCS serve-stall round-5.
 *
 *	Per-family admission accounting under the four-state send ownership
 *	contract (see ClusterICSendResult).  DONE needs no extra row (the
 *	existing sent counters cover it);  WOULD_BLOCK = admitted into the
 *	tier1 per-peer FIFO (pre-fix: silently lost);  NOT_ADMITTED = the
 *	transport refused and the retransmit machinery self-heals (nonzero
 *	deltas here are the capacity red flag the S3 gate watches);
 *	HARD_ERROR is recorded at the tier1 peer-error surface already.
 *	Exported so cluster_cr_server's direct REPLY sends share the same
 *	accounting.
 */
void
cluster_gcs_block_note_send_outcome(GcsBlockSendFamily family, ClusterICSendResult rc)
{
	pg_atomic_uint64 *counter = NULL;

	if (ClusterGcsBlock == NULL)
		return;

	switch (rc) {
	case CLUSTER_IC_SEND_WOULD_BLOCK:
		switch (family) {
		case GCS_BLOCK_SEND_FAMILY_REPLY:
			counter = &ClusterGcsBlock->reply_send_queued_count;
			break;
		case GCS_BLOCK_SEND_FAMILY_FORWARD:
			counter = &ClusterGcsBlock->forward_send_queued_count;
			break;
		case GCS_BLOCK_SEND_FAMILY_INVALIDATE:
			counter = &ClusterGcsBlock->invalidate_send_queued_count;
			break;
		}
		break;
	case CLUSTER_IC_SEND_NOT_ADMITTED:
		switch (family) {
		case GCS_BLOCK_SEND_FAMILY_REPLY:
			counter = &ClusterGcsBlock->reply_send_not_admitted_count;
			break;
		case GCS_BLOCK_SEND_FAMILY_FORWARD:
			counter = &ClusterGcsBlock->forward_send_not_admitted_count;
			break;
		case GCS_BLOCK_SEND_FAMILY_INVALIDATE:
			counter = &ClusterGcsBlock->invalidate_send_not_admitted_count;
			break;
		}
		break;
	case CLUSTER_IC_SEND_DONE:
	case CLUSTER_IC_SEND_HARD_ERROR:
		break;
	}

	if (counter != NULL)
		pg_atomic_fetch_add_u64(counter, 1);
}

/*
 * gcs_block_forward_send_admitted — send one FORWARD frame and report
 * whether the transport now owns it (DONE on the wire, or WOULD_BLOCK
 * admitted into the per-peer FIFO — both deliver in order, so the
 * caller's forward-in-flight state installs either way).
 */
static bool
gcs_block_forward_send_admitted(int32 holder_node, const GcsBlockForwardPayload *fwd)
{
	ClusterICSendResult rc
		= cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, holder_node, fwd, sizeof(*fwd));

	cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_FORWARD, rc);
	return rc == CLUSTER_IC_SEND_DONE || rc == CLUSTER_IC_SEND_WOULD_BLOCK;
}

/* The generic IC sender intentionally treats self-destination as a no-op.
 * GCS block replies are completion signals, so a same-node denial must enter
 * the normal registered handler just like a wire reply. */
static ClusterICSendResult
gcs_block_send_envelope_or_loopback(uint8 msg_type, int32 dest_node, const void *payload,
									uint32 payload_len)
{
	ClusterICEnvelope envelope;

	if (dest_node != cluster_node_id)
		return cluster_ic_send_envelope(msg_type, dest_node, payload, payload_len);
	if (payload == NULL
		|| !cluster_ic_envelope_build(&envelope, msg_type, (uint32)cluster_node_id,
									  (uint32)cluster_node_id, payload, payload_len))
		return CLUSTER_IC_SEND_HARD_ERROR;
	return cluster_ic_dispatch_envelope(&envelope, payload, cluster_node_id)
			   ? CLUSTER_IC_SEND_DONE
			   : CLUSTER_IC_SEND_HARD_ERROR;
}

static bool
gcs_block_r4_tx_origin_locator_kind_valid(const ClusterTxLocator *locator)
{
	bool data_kind;

	if (locator == NULL || locator->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| !TransactionIdIsNormal(locator->xid)
		|| locator->tt_wrap != TT_WRAP_INVALID)
		return false;
	data_kind = locator->itl_kind == ITL_FLAG_ACTIVE
				|| locator->itl_kind == ITL_FLAG_COMMITTED
				|| locator->itl_kind == ITL_FLAG_ABORTED
				|| locator->itl_kind == ITL_FLAG_NEEDS_CLEANOUT;
	return data_kind || ITL_FLAG_IS_LOCK_ONLY(locator->itl_kind);
}

static void
gcs_block_r4_tx_origin_context_clear(GcsBlockR4TxOriginContext *context,
									 bool cancel_guard)
{
	if (context == NULL || !context->in_use)
		return;
	if (cancel_guard && context->guard_active)
		cluster_undo_block0_current_cancel(&context->guard);
	context->guard_active = false;
	if (context->admission.entered)
		cluster_semantic_activation_leave(&context->admission);
	memset(context, 0, sizeof(*context));
}

static bool
gcs_block_r4_tx_origin_admission_current(
	const GcsBlockR4TxOriginContext *context)
{
	if (context == NULL)
		return false;
	if (context->resolve_mode == CLUSTER_TX_RESOLVE_VISIBILITY)
		return cluster_semantic_activation_recheck(&context->admission);
	if (context->resolve_mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)
		return cluster_semantic_activation_recheck_r4_terminal_census(
			&context->admission);
	return false;
}

static bool
gcs_block_r4_tx_origin_resolve_root_current(
	const GcsBlockR4TxOriginContext *context,
	const ClusterUndoBlock0LogicalKey *logical,
	ClusterUndoBlock0ResolvedRoot *root)
{
	bool resolved = false;

	if (context == NULL || logical == NULL || root == NULL)
		return false;
	memset(root, 0, sizeof(*root));
	if (context->resolve_mode == CLUSTER_TX_RESOLVE_VISIBILITY)
		resolved = cluster_semantic_activation_resolve_shared_undo_root(
			&context->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			logical->owner_instance, logical->segment_id, root);
	else if (context->resolve_mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS)
		resolved
			= cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&context->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical->owner_instance, logical->segment_id, root);
	return resolved && gcs_block_r4_tx_origin_admission_current(context);
}

static int
gcs_block_r4_tx_origin_remaining_timeout_ms(
	const GcsBlockR4TxOriginContext *context)
{
	TimestampTz now;
	TimestampTz remaining_us;

	if (context == NULL)
		return 0;
	if (context->deadline == 0)
		return cluster_gcs_reply_timeout_ms;
	now = GetCurrentTimestamp();
	if (now >= context->deadline)
		return 0;
	remaining_us = context->deadline - now;
	if (remaining_us / 1000 >= INT_MAX)
		return INT_MAX;
	return (int)Max((remaining_us + 999) / 1000, 1);
}

static bool
gcs_block_r4_tx_origin_deadline_expired(
	const GcsBlockR4TxOriginContext *context)
{
	return context != NULL && context->deadline != 0
		   && GetCurrentTimestamp() >= context->deadline;
}

static void
gcs_block_r4_tx_origin_timeout(GcsBlockR4TxOriginContext *context)
{
	if (context == NULL)
		return;
	if (context->guard_active)
		cluster_undo_block0_current_cancel(&context->guard);
	context->guard_active = false;
	context->outcome = CLUSTER_TX_UNKNOWN;
	context->reason = CLUSTER_TX_RESOLVE_TIMEOUT;
	if (context->domain == GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
		context->current_mx_result = CMX_RESOLVE_TIMEOUT;
	memset(&context->resolution, 0, sizeof(context->resolution));
	context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
}

static bool
gcs_block_r4_tx_origin_try_accept(const ClusterICEnvelope *env,
								  const ClusterR4CrForwardPayload *forward)
{
	static const uint8 zero_watermark[8] = { 0 };
	ClusterTxLocator locator;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterTxResolveMode resolve_mode = CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;
	BufferTag expected_tag;
	GcsBlockR4TxOriginContext *free_context = NULL;
	uint32 expected_generation;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	uint32 capability_generation = 0;
	bool optional_supported = false;
	int i;

	memset(&locator, 0, sizeof(locator));
	memset(&logical, 0, sizeof(logical));
	memset(&root, 0, sizeof(root));
	memset(&admission, 0, sizeof(admission));
	if (env == NULL || forward == NULL
		|| env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		|| env->payload_length != sizeof(*forward)
		|| env->source_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| env->dest_node_id != (uint32)cluster_node_id
		|| forward->base.original_requester_node != (int32)env->source_node_id
		|| forward->base.original_requester_node == cluster_node_id
		|| forward->base.requester_backend_id <= 0
		|| forward->base.requester_backend_id > MaxBackends
		|| forward->base.master_node != cluster_node_id
		|| forward->base.request_id == 0
		|| forward->base.transition_id != (uint8)PCM_TRANS_N_TO_S
		|| forward->base.epoch != env->epoch
		|| forward->base.epoch != cluster_epoch_get_current()
		|| (forward->base.epoch == 0 && cluster_conf_node_count() != 4)
		|| cluster_ic_tier1_my_data_channel() != 0
		|| memcmp(forward->base.expected_pi_watermark_scn_bytes,
				  zero_watermark, sizeof(zero_watermark)) != 0
		|| forward->base.reserved_0[0] != 0
		|| forward->base.reserved_0[1] != 0
		|| forward->base.reserved_0[2] != 0
		|| forward->base.reserved_0[3] != 0
		|| forward->base.reserved_0[4] != 0
		|| forward->base.reserved_0[5] != 0
		|| forward->base.reserved_0[6] != 0
		|| !ClusterR4ForwardExtensionGetLocatorGeneration(
			&forward->extension, CLUSTER_R4_WIRE_TX_RESOLVE,
			&locator, &expected_generation)
		|| !gcs_block_r4_tx_origin_locator_kind_valid(&locator)
		|| !uba_decode(locator.uba, &segment_id, &block_no,
					   &tt_slot_offset, &row_offset)
		|| block_no == 0
		|| uba_origin_node_id(locator.uba) != (NodeId)cluster_node_id)
		return true;
	expected_tag = GcsBlockUndoFetchTagMake(segment_id, block_no);
	if (memcmp(&forward->base.tag, &expected_tag, sizeof(expected_tag)) != 0)
		return true;
	if (!cluster_sf_peer_capability_family_sample(
			forward->base.original_requester_node,
			GCS_BLOCK_R4_TX_REQUIRED_HELLO_CAPS, 0, &optional_supported,
			&capability_generation)
		|| capability_generation == 0)
		return true;

	/* An exact duplicate retains the original single token/context.  A
	 * conflicting reuse of the same authenticated request identity is
	 * consumed without a second action or reply. */
	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++) {
		GcsBlockR4TxOriginContext *context
			= &gcs_block_r4_tx_origin_contexts[i];

		if (!context->in_use) {
			if (free_context == NULL)
				free_context = context;
			continue;
		}
		if (context->forward.base.original_requester_node
				== forward->base.original_requester_node
			&& context->forward.base.requester_backend_id
				   == forward->base.requester_backend_id
			&& context->forward.base.request_id == forward->base.request_id
			&& context->forward.base.epoch == forward->base.epoch)
			return true;
	}
	if (free_context == NULL)
		return true;
	logical.owner_instance = (uint8)((uint32)cluster_node_id + 1);
	logical.segment_id = segment_id;
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		resolve_mode = CLUSTER_TX_RESOLVE_VISIBILITY;
		if (!cluster_semantic_activation_resolve_shared_undo_root(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &root)
			|| !cluster_semantic_activation_recheck(&admission)) {
			cluster_semantic_activation_leave(&admission);
			return true;
		}
	} else {
		if (admission_result != CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED)
			return true;
		if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
				!= CLUSTER_SEMANTIC_ADMISSION_OK)
			return true;
		resolve_mode = CLUSTER_TX_RESOLVE_TERMINAL_CENSUS;
		if (!cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &root)
			|| !cluster_semantic_activation_recheck_r4_terminal_census(&admission)) {
			cluster_semantic_activation_leave(&admission);
			return true;
		}
	}

	memset(free_context, 0, sizeof(*free_context));
	free_context->in_use = true;
	free_context->domain = GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE;
	free_context->phase = GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_BEGIN;
	free_context->resolve_mode = resolve_mode;
	free_context->deadline = cluster_gcs_reply_timeout_ms > 0
		? TimestampTzPlusMilliseconds(
			GetCurrentTimestamp(), cluster_gcs_reply_timeout_ms)
		: 0;
	free_context->requester_capability_generation = capability_generation;
	free_context->forward = *forward;
	free_context->locator = locator;
	free_context->logical = logical;
	free_context->root = root;
	free_context->expected_generation.known = true;
	free_context->expected_generation.value = expected_generation;
	free_context->admission = admission;
	free_context->outcome = CLUSTER_TX_UNKNOWN;
	free_context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	return true;
}

static bool
gcs_block_current_mx_origin_locate_physical(
	TransactionId xid, ClusterTTSlotPhysicalLocator *locator,
	bool *current_owner_found_out,
	ClusterTTDurableLocate *durable_locate_result_out)
{
	ClusterTTSlotCurrentOwner current_owner;
	ClusterTTDurableLocate durable_locate_result
		= CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE;
	uint16 segment_id = 0;
	uint16 slot_offset = 0;
	uint16 wrap = TT_WRAP_INVALID;

	if (current_owner_found_out != NULL)
		*current_owner_found_out = false;
	if (durable_locate_result_out != NULL)
		*durable_locate_result_out
			= CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE;
	if (locator == NULL || current_owner_found_out == NULL
		|| durable_locate_result_out == NULL || cluster_node_id < 0
		|| cluster_xid_origin_slot(xid) != cluster_node_id)
		return false;
	memset(locator, 0, sizeof(*locator));
	memset(&current_owner, 0, sizeof(current_owner));
	if (cluster_tt_slot_current_owner_by_xid(
			cluster_node_id, xid, &current_owner)) {
		*current_owner_found_out = true;
		locator->segment_id = current_owner.segment_id;
		locator->xid = xid;
		locator->slot_offset = current_owner.slot_offset;
		locator->wrap = current_owner.wrap;
		return locator->segment_id != 0;
	}

	/* Retention rollover deliberately removes old canonical slots from the
	 * current allocator index.  The durable scan supplies only a unique
	 * physical identity: its status output is not requested or consumed.
	 * Exact SCUR bytes plus the native bracket decide the verdict. */
	durable_locate_result = cluster_tt_slot_durable_locate_any_by_xid_origin(
		cluster_node_id, xid, &segment_id, &slot_offset, &wrap, NULL);
	*durable_locate_result_out = durable_locate_result;
	if (durable_locate_result != CLUSTER_TT_DURABLE_LOCATE_FOUND)
		return false;
	locator->segment_id = segment_id;
	locator->xid = xid;
	locator->slot_offset = slot_offset;
	locator->wrap = wrap;
	return locator->segment_id != 0
		&& locator->slot_offset < TT_SLOTS_PER_SEGMENT
		&& locator->wrap != TT_WRAP_INVALID;
}

static bool
gcs_block_current_mx_requester_identity_capture(
	int32 requester_node, uint32 capability_generation,
	const ClusterSemanticAdmissionToken *admission,
	ClusterCtrcParticipantIdentity *identity)
{
	uint64 observed_incarnation = 0;
	uint64 observed_generation = 0;
	uint32 required_capabilities
		= PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
		  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;

	if (identity == NULL)
		return false;
	memset(identity, 0, sizeof(*identity));
	if (requester_node < 0
		|| requester_node >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| capability_generation == 0 || admission == NULL
		|| !admission->entered || admission->record_generation == 0
		|| admission->formation_epoch != cluster_epoch_get_current()
		|| !cluster_membership_is_member(requester_node)
		|| !cluster_reconfig_get_observed_slot(
			requester_node, &observed_incarnation, &observed_generation)
		|| observed_incarnation == 0 || observed_generation == 0
		|| cluster_reconfig_get_observed_epoch(requester_node)
		   != admission->formation_epoch
		|| cluster_membership_get_last_admitted_incarnation(requester_node)
		   != observed_incarnation
		|| !cluster_sf_peer_capability_generation_matches(
			requester_node, required_capabilities, capability_generation))
		return false;

	identity->node_id = (uint16)requester_node;
	identity->capability_record_generation = capability_generation;
	identity->boot_incarnation = observed_incarnation;
	identity->formation_epoch = admission->formation_epoch;
	identity->admission_record_generation = admission->record_generation;
	return true;
}

static bool
gcs_block_current_mx_requester_identity_current(
	const GcsBlockR4TxOriginContext *context)
{
	ClusterCtrcParticipantIdentity current;

	if (context == NULL
		|| context->domain != GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
		|| !gcs_block_current_mx_requester_identity_capture(
			context->current_mx_request.prefix.original_requester_node,
			context->requester_capability_generation,
			&context->admission, &current))
		return false;
	return memcmp(&current, &context->current_mx_requester_identity,
				  sizeof(current)) == 0;
}

static bool
gcs_block_current_mx_ctrc_key(
	const GcsBlockR4TxOriginContext *context, uint16 index,
	const ClusterUndoBlock0Generation *generation,
	ClusterCtrcTxnKeyV1 *key)
{
	const ClusterTTSlotPhysicalLocator *locator;
	uint64 epoch;
	uint64 boot_incarnation;
	uint64 system_identifier;

	if (context == NULL || generation == NULL || key == NULL
		|| index >= context->current_mx_locator_count
		|| !generation->known || generation->value == UINT32_MAX
		|| context->tt_root.intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
		|| context->tt_root.root_id == 0
		|| context->tt_root.root_generation == 0
		|| context->admission.record_generation == 0)
		return false;
	locator = &context->current_mx_locators[index];
	epoch = cluster_epoch_get_current();
	boot_incarnation = cluster_qvotec_get_self_incarnation();
	system_identifier = GetSystemIdentifier();
	if (epoch > UINT32_MAX || epoch != context->admission.formation_epoch
		|| boot_incarnation == 0 || system_identifier == 0)
		return false;

	memset(key, 0, sizeof(*key));
	key->format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key->owner_instance = (uint8)((uint32)cluster_node_id + 1);
	key->origin_node_id = (uint16)cluster_node_id;
	key->segment_id = locator->segment_id;
	key->segment_generation = generation->value;
	key->slot_offset = locator->slot_offset;
	key->slot_wrap = locator->wrap;
	key->xid = locator->xid;
	key->cluster_epoch = (uint32)epoch;
	key->system_identifier = system_identifier;
	key->origin_boot_incarnation = boot_incarnation;
	key->formation_epoch = context->admission.formation_epoch;
	key->admission_record_generation = context->admission.record_generation;
	key->root_descriptor_incarnation = context->tt_root.root_generation;
	key->root_id = context->tt_root.root_id;
	key->root_generation = context->tt_root.root_generation;
	return true;
}

static bool
gcs_block_current_mx_origin_try_accept(
	const ClusterICEnvelope *env, const void *payload)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	GcsBlockR4TxOriginContext *free_context = NULL;
	BufferTag route_tag;
	uint32 capability_generation = 0;
	uint32 data_segment;
	uint32 data_block;
	uint16 data_tt_slot;
	uint16 data_row;
	uint16 owner_count;
	uint16 i;
	int recv_worker;
	int expected_worker;

	memset(&request, 0, sizeof(request));
	memset(&admission, 0, sizeof(admission));
	memset(&logical, 0, sizeof(logical));
	memset(&root, 0, sizeof(root));
	if (env == NULL || payload == NULL
		|| env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_gcs_block_family_on_data_plane()
		|| !cluster_multixact_current_wire_validate_proof_forward(
			payload, env->payload_length, (int32)env->source_node_id,
			cluster_node_id, cluster_epoch_get_current(), &request))
		return false;
	route_tag = GcsBlockCurrentMxRouteTagMake(
		request.prefix.request_id, request.prefix.epoch,
		request.prefix.original_requester_node,
		request.prefix.requester_backend_id);
	recv_worker = cluster_ic_tier1_my_data_channel();
	expected_worker = cluster_lms_shard_for_tag(&route_tag, cluster_lms_workers);
	Assert(expected_worker == recv_worker);
	if (expected_worker != recv_worker)
		return true;
	if (!cluster_sf_peer_multixact_current_capability_generation(
			request.prefix.original_requester_node, &capability_generation)
		|| capability_generation == 0
		|| !cluster_sf_peer_capability_generation_matches(
			request.prefix.original_requester_node,
			PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
				| PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1,
			capability_generation))
		return false;

	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++) {
		GcsBlockR4TxOriginContext *context
			= &gcs_block_r4_tx_origin_contexts[i];

		if (!context->in_use) {
			if (free_context == NULL)
				free_context = context;
			continue;
		}
		if (context->domain == GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
			&& context->current_mx_request.prefix.original_requester_node
				   == request.prefix.original_requester_node
			&& context->current_mx_request.prefix.requester_backend_id
				   == request.prefix.requester_backend_id
			&& context->current_mx_request.prefix.request_id
				   == request.prefix.request_id
			&& context->current_mx_request.prefix.epoch
				   == request.prefix.epoch)
			return true;
	}
	if (free_context == NULL) {
		if (!gcs_block_current_mx_origin_first_unknown_logged) {
			gcs_block_current_mx_origin_first_unknown_logged = true;
			ereport(LOG,
					(errmsg_internal(
						 "PGRAC current-MultiXact origin first unknown: "
						 "requester=%d backend=%d request=" UINT64_FORMAT
						 " failure=%d xid=0 index=0 contexts=%d",
						 request.prefix.original_requester_node,
						 request.prefix.requester_backend_id,
						 request.prefix.request_id,
						 (int)GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_CONTEXT_FULL,
						 GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS)));
		}
		return true;
	}
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		if (!gcs_block_current_mx_origin_first_unknown_logged) {
			gcs_block_current_mx_origin_first_unknown_logged = true;
			ereport(LOG,
					(errmsg_internal(
						 "PGRAC current-MultiXact origin first unknown: "
						 "requester=%d backend=%d request=" UINT64_FORMAT
						 " failure=%d admission=%d xid=0 index=0",
						 request.prefix.original_requester_node,
						 request.prefix.requester_backend_id,
						 request.prefix.request_id,
						 (int)GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ADMISSION,
						 (int)admission_result)));
		}
		return false;
	}

	memset(free_context, 0, sizeof(*free_context));
	free_context->in_use = true;
	free_context->domain = GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX;
	free_context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
	free_context->resolve_mode = CLUSTER_TX_RESOLVE_VISIBILITY;
	free_context->deadline = cluster_gcs_reply_timeout_ms > 0
		? TimestampTzPlusMilliseconds(
			GetCurrentTimestamp(), cluster_gcs_reply_timeout_ms)
		: 0;
	free_context->requester_capability_generation = capability_generation;
	free_context->current_mx_request = request;
	free_context->current_mx_result = CMX_RESOLVE_UNKNOWN;
	free_context->current_mx_failure
		= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_NONE;
	free_context->current_mx_updater_proof.verdict = CUCP_UNKNOWN;
	free_context->admission = admission;
	if (!gcs_block_current_mx_requester_identity_capture(
			request.prefix.original_requester_node, capability_generation,
			&admission, &free_context->current_mx_requester_identity)) {
		gcs_block_r4_tx_origin_context_clear(free_context, false);
		return false;
	}
	free_context->outcome = CLUSTER_TX_UNKNOWN;
	free_context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;

	if (request.prefix.body_kind
			== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
		const ClusterCurrentMxUpdaterChallengeWire *challenge
			= &request.trailer.body.updater.challenge;

		if (!cluster_multixact_current_successor_provenance_well_formed(
				&challenge->candidate_next_xmin_alias,
				&challenge->candidate_next_xmin_locator,
				challenge->updater_xid, (uint16)cluster_node_id,
				request.prefix.mxkey.cluster_epoch)
			|| !uba_decode(challenge->candidate_next_xmin_locator.uba,
						  &data_segment, &data_block, &data_tt_slot,
						  &data_row)
			|| data_block == 0) {
			free_context->current_mx_failure
				= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_LOCATOR;
			free_context->current_mx_failure_xid = challenge->updater_xid;
			return true;
		}
		free_context->locator = challenge->candidate_next_xmin_locator;
		logical.owner_instance = (uint8)((uint32)cluster_node_id + 1);
		logical.segment_id = data_segment;
		if (!cluster_semantic_activation_resolve_shared_undo_root(
				&free_context->admission,
				CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
				logical.segment_id, &root)
			|| !cluster_semantic_activation_recheck(
				&free_context->admission)) {
			free_context->current_mx_failure
				= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ROOT;
			free_context->current_mx_failure_xid = challenge->updater_xid;
			return true;
		}
		free_context->logical = logical;
		free_context->root = root;
		free_context->phase = GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_BEGIN;
		return true;
	}

	owner_count = request.prefix.body_kind
			== CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS
		? request.prefix.entry_count : 1;
	for (i = 0; i < owner_count; i++) {
		TransactionId xid = request.prefix.body_kind
				== CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS
			? request.trailer.body.asks[i].xid
			: request.trailer.body.updater.challenge.updater_xid;
		ClusterTTSlotPhysicalLocator *locator
			= &free_context->current_mx_locators[i];
		bool current_owner_found = false;
		ClusterTTDurableLocate durable_locate_result
			= CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE;

		if (!gcs_block_current_mx_origin_locate_physical(
				xid, locator, &current_owner_found,
				&durable_locate_result)) {
			free_context->current_mx_failure
				= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_LOCATOR;
			free_context->current_mx_failure_xid = xid;
			free_context->current_mx_failure_index = i;
			free_context->current_mx_current_owner_found
				= current_owner_found;
			free_context->current_mx_durable_locate_result
				= durable_locate_result;
			return true;
		}
	}
	free_context->current_mx_locator_count = owner_count;
	logical.owner_instance = (uint8)((uint32)cluster_node_id + 1);
	logical.segment_id = free_context->current_mx_locators[0].segment_id;
	if (!cluster_semantic_activation_resolve_shared_undo_root(
			&free_context->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			logical.owner_instance, logical.segment_id, &root)
		|| !cluster_semantic_activation_recheck(&free_context->admission)) {
		free_context->current_mx_failure
			= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ROOT;
		free_context->current_mx_failure_xid
			= free_context->current_mx_locators[0].xid;
		free_context->current_mx_failure_index = 0;
		return true;
	}
	free_context->tt_logical = logical;
	free_context->tt_root = root;
	free_context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN;
	return true;
}

static bool
gcs_block_r4_tx_origin_resolution_valid(
	const GcsBlockR4TxOriginContext *context)
{
	const ClusterTxResolution *resolution;

	if (context == NULL)
		return false;
	resolution = &context->resolution;
	if (context->outcome == CLUSTER_TX_PREPARED
		|| (context->outcome != CLUSTER_TX_COMMITTED
			&& context->outcome != CLUSTER_TX_ABORTED
			&& context->outcome != CLUSTER_TX_IN_PROGRESS)
		|| context->outcome != resolution->outcome
		|| context->reason != CLUSTER_TX_RESOLVE_NONE
		|| !cluster_tx_locator_reply_matches(&context->locator,
									 &resolution->locator_echo)
		|| !TransactionIdIsNormal(resolution->top_xid)
		|| !cluster_tx_outcome_proof_is_valid(resolution->outcome,
										 resolution->proof_kind)
		|| resolution->authority.origin_epoch
			   != context->admission.formation_epoch
		|| XLogRecPtrIsInvalid(resolution->authority.live_hwm_lsn)
		|| !SCN_VALID(resolution->authority.authority_scn))
		return false;
	if (resolution->outcome == CLUSTER_TX_COMMITTED)
		return SCN_VALID(resolution->commit_scn);
	return !SCN_VALID(resolution->commit_scn);
}

static bool
gcs_block_r4_tx_origin_failure_transition(
	GcsBlockR4TxOriginPhase phase_before,
	const GcsBlockR4TxOriginContext *context)
{
	if (context == NULL || context->reason == CLUSTER_TX_RESOLVE_NONE
		|| context->outcome != CLUSTER_TX_UNKNOWN)
		return false;
	switch (phase_before) {
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_FREEZE:
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK:
		return context->phase
			   == GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN;
	case GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE:
		return context->phase == GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_BEGIN
			   && !context->canonical_sampled;
	case GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_BEGIN:
	case GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_POLL:
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_BEGIN:
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_POLL:
	case GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN:
	case GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_POLL:
	case GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_BEGIN:
	case GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_POLL:
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN:
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_POLL:
	case GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN:
	case GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_POLL:
		return context->phase == GCS_BLOCK_R4_TX_ORIGIN_SEND;
	case GCS_BLOCK_R4_TX_ORIGIN_SEND:
	default:
		return false;
	}
}

static void
gcs_block_r4_tx_origin_log_first_denied(
	const GcsBlockR4TxOriginContext *context)
{
	ClusterRuntimeVisibilityCanonicalDiagnostic diagnostic;
	bool diagnostic_valid;

	if (context == NULL || gcs_block_r4_tx_origin_first_denial_logged)
		return;
	gcs_block_r4_tx_origin_first_denial_logged = true;
	memset(&diagnostic, 0, sizeof(diagnostic));
	diagnostic.resident_copy_result = -1;
	diagnostic_valid
		= cluster_runtime_visibility_origin_plan_canonical_diagnostic(
			&context->origin_plan, &diagnostic);
	ereport(
		LOG,
		(errmsg_internal(
			"PGRAC status-22 origin first denied: requester=%d "
			"backend=%d request=" UINT64_FORMAT
			" xid=%u failure_phase=%d reason=%d outcome=%d current_failure=%d "
			"canonical_sampled=%d expected_generation=%u "
			"formation=" UINT64_FORMAT " record_generation=" UINT64_FORMAT
			" canonical_diag=%d first_predicate=%d generation_result=%d "
			"generation_known=%d generation=%u resident_result=%d "
			"locator_wrap=%u tt_slot=%u slot_status=%u slot_xid=%u "
			"slot_wrap=%u slot_scn=" UINT64_FORMAT " live_owner_sampled=%d live_owner_exact=%d "
			"live_owner_segment=%u live_owner_xid=%u "
			"live_owner_slot=%u live_owner_wrap=%u live_owner_status=%u"
			" native_sampled=%d native_status=%u prepared_sampled=%d "
			"prepared=%d procarray_sampled=0 root=" UINT64_FORMAT "/" UINT64_FORMAT
			" final_root_sampled=%d final_root=" UINT64_FORMAT "/" UINT64_FORMAT
			" admission_initial=%d admission_final=%d",
			context->forward.base.original_requester_node,
			context->forward.base.requester_backend_id, context->forward.base.request_id,
			context->locator.xid, (int)context->failure_phase, (int)context->reason,
			(int)context->outcome, (int)context->current_failure, context->canonical_sampled,
			context->expected_generation.value, context->admission.formation_epoch,
			context->admission.record_generation, diagnostic_valid, (int)diagnostic.first_failure,
			diagnostic.generation_result, diagnostic.generation_known, diagnostic.generation_value,
			diagnostic.resident_copy_result, diagnostic.locator_wrap, diagnostic.tt_slot_offset,
			diagnostic.slot_status, diagnostic.slot_xid, diagnostic.slot_wrap,
			(uint64)diagnostic.slot_commit_scn, diagnostic.live_owner_sampled,
			diagnostic.live_owner_exact, diagnostic.live_owner_segment_id,
			diagnostic.live_owner_xid, diagnostic.live_owner_slot_offset,
			diagnostic.live_owner_wrap, diagnostic.live_owner_status, diagnostic.native_sampled,
			diagnostic.native_status, diagnostic.prepared_sampled, diagnostic.prepared,
			diagnostic.root_id, diagnostic.root_generation, diagnostic.final_root_sampled,
			diagnostic.final_root_id, diagnostic.final_root_generation,
			diagnostic.initial_admission_current, diagnostic.final_admission_current)));
}

static void
gcs_block_r4_tx_origin_prepare_reply(GcsBlockR4TxOriginContext *context)
{
	GcsBlockReplyHeader *header;
	uint8 *page;
	bool publish_result;

	memset(context->reply_frame, 0, sizeof(context->reply_frame));
	header = (GcsBlockReplyHeader *)context->reply_frame;
	page = context->reply_frame + sizeof(*header);
	header->request_id = context->forward.base.request_id;
	header->epoch = context->forward.base.epoch;
	header->sender_node = cluster_node_id;
	header->requester_backend_id
		= context->forward.base.requester_backend_id;
	header->transition_id = context->forward.base.transition_id;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	publish_result = gcs_block_r4_tx_origin_resolution_valid(context)
					 && ClusterR4TxVerdictPageEncode(
							page, &context->resolution);
	if (publish_result) {
		header->status = (uint8)GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT;
		header->page_lsn
			= (uint64)context->resolution.authority.live_hwm_lsn;
	} else {
		gcs_block_r4_tx_origin_log_first_denied(context);
		header->status = (uint8)GCS_BLOCK_REPLY_R4_DENIED;
	}
	header->checksum = gcs_block_compute_checksum((const char *)page);
}

static void
gcs_block_current_mx_origin_log_first_unknown(
	const GcsBlockR4TxOriginContext *context)
{
	const ClusterTTSlotPhysicalLocator *locator = NULL;

	if (context == NULL || gcs_block_current_mx_origin_first_unknown_logged)
		return;
	if (context->current_mx_failure_index
		< CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME)
		locator = &context->current_mx_locators[
			context->current_mx_failure_index];
	gcs_block_current_mx_origin_first_unknown_logged = true;
	ereport(LOG,
			(errmsg_internal(
				 "PGRAC current-MultiXact origin first unknown: requester=%d "
				 "backend=%d request=" UINT64_FORMAT " epoch=" UINT64_FORMAT
				 " kind=%u entries=%u failure=%d xid=%u index=%u phase=%d "
				 "current_owner_found=%d durable_locate=%d "
				 "locator_count=%u sampled=0x%02x locator_segment=%u "
				 "locator_slot=%u locator_wrap=%u tt_segment=%u root="
				 UINT64_FORMAT "/" UINT64_FORMAT " admission_formation="
				 UINT64_FORMAT " admission_generation=" UINT64_FORMAT,
				 context->current_mx_request.prefix.original_requester_node,
				 context->current_mx_request.prefix.requester_backend_id,
				 context->current_mx_request.prefix.request_id,
				 context->current_mx_request.prefix.epoch,
				 context->current_mx_request.prefix.body_kind,
				 context->current_mx_request.prefix.entry_count,
				 (int)context->current_mx_failure,
				 context->current_mx_failure_xid,
				 context->current_mx_failure_index, (int)context->phase,
				 context->current_mx_current_owner_found,
				 (int)context->current_mx_durable_locate_result,
				 context->current_mx_locator_count,
				 context->current_mx_sampled_bitmap,
				 locator != NULL ? locator->segment_id : 0,
				 locator != NULL ? locator->slot_offset : 0,
				 locator != NULL ? locator->wrap : 0,
				 context->tt_logical.segment_id,
				 context->tt_root.root_id,
				 context->tt_root.root_generation,
				 context->admission.formation_epoch,
				 context->admission.record_generation)));
}

static void
gcs_block_current_mx_origin_prepare_reply(
	GcsBlockR4TxOriginContext *context)
{
	GcsBlockReplyHeader *header;
	ClusterCurrentMxProofReplyPage *page;
	ClusterMxResolveResult built;
	uint32 requester_capability_generation = 0;
	uint16 i;

	if (context->current_mx_result == CMX_RESOLVE_OK
		&& !gcs_block_current_mx_requester_identity_current(context))
	{
		context->current_mx_result = CMX_RESOLVE_RETRY;
		context->current_mx_proof_count = 0;
		context->current_mx_failure
			= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SEND_FRESHNESS;
	}
	/* The proof was sampled before releasing block-0 SCUR.  Revalidate every
	 * positive CTRC grant immediately before encoding the reply so an origin
	 * that entered SEALING while the send was delayed cannot leak an old
	 * ACTIVE/SELF grant. */
	for (i = 0; context->current_mx_result == CMX_RESOLVE_OK
			 && i < context->current_mx_proof_count; i++)
	{
		const ClusterCurrentMemberProof *proof
			= &context->current_mx_proofs[i];
		uint32 grant = ClusterCurrentMemberProofGetCtrcGrant(proof);

		if ((proof->state == CCM_ACTIVE || proof->state == CCM_SELF)
			&& !cluster_ctrc_origin_grant_publishable(
				&context->current_mx_ctrc_keys[i],
				&context->current_mx_requester_identity, grant))
		{
			cluster_ctrc_stat_bump(CTRC_STAT_GRANT_REFUSED);
			context->current_mx_result = CMX_RESOLVE_RETRY;
			context->current_mx_proof_count = 0;
			context->current_mx_failure
				= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SEND_FRESHNESS;
		}
	}
	memset(context->reply_frame, 0, sizeof(context->reply_frame));
	header = (GcsBlockReplyHeader *)context->reply_frame;
	page = (ClusterCurrentMxProofReplyPage *)(
		context->reply_frame + sizeof(*header));
	if (context->current_mx_result == CMX_RESOLVE_UNKNOWN)
		gcs_block_current_mx_origin_log_first_unknown(context);
	if (context->current_mx_result == CMX_RESOLVE_OK)
		requester_capability_generation
			= context->current_mx_requester_identity.capability_record_generation;
	built = cluster_cr_server_current_mx_build_proof_page(
		(uint16)cluster_node_id, &context->current_mx_request,
		context->current_mx_result, requester_capability_generation,
		context->current_mx_proofs,
		context->current_mx_proof_count,
		&context->current_mx_updater_proof, page);
	if (built != context->current_mx_result) {
		context->current_mx_result = CMX_RESOLVE_UNKNOWN;
		context->current_mx_proof_count = 0;
		(void)cluster_cr_server_current_mx_build_proof_page(
			(uint16)cluster_node_id, &context->current_mx_request,
			CMX_RESOLVE_UNKNOWN, 0, NULL, 0, NULL, page);
	}
	header->request_id = context->current_mx_request.prefix.request_id;
	header->epoch = context->current_mx_request.prefix.epoch;
	header->sender_node = cluster_node_id;
	header->requester_backend_id
		= context->current_mx_request.prefix.requester_backend_id;
	header->transition_id = 0;
	header->status
		= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	header->checksum = gcs_block_compute_checksum((const char *)page);
}

static bool
gcs_block_current_mx_origin_sample_held(
	GcsBlockR4TxOriginContext *context)
{
	const ClusterCurrentMxProofForwardV2 *request;
	uint16 count;
	uint16 i;

	if (context == NULL
		|| context->domain != GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
		return false;
	request = &context->current_mx_request;
	count = context->current_mx_locator_count;

	for (i = 0; i < count; i++) {
		const ClusterCurrentMxProofAskWire *ask;
		ClusterCurrentMxProofAskWire updater_ask;
		ClusterTTStatusKey key;
		ClusterTTStatusResult result;
		ClusterUndoBlock0Generation generation = { false, 0 };
		ClusterUndoBlock0Generation final_generation = { false, 0 };
		ClusterUndoBlock0ResolvedRoot final_root;
		ClusterCtrcTxnKeyV1 ctrc_key;
		ClusterCtrcTouchResult touch_result;
		bool ctrc_physical_active = false;
		uint32 ctrc_grant = 0;

		memset(&final_root, 0, sizeof(final_root));
		memset(&ctrc_key, 0, sizeof(ctrc_key));

		if ((context->current_mx_sampled_bitmap & (uint8)(1U << i)) != 0
			|| context->current_mx_locators[i].segment_id
				   != context->tt_logical.segment_id)
			continue;
		if (request->prefix.body_kind
				== CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS)
			ask = &request->trailer.body.asks[i];
		else {
			memset(&updater_ask, 0, sizeof(updater_ask));
			updater_ask.xid
				= request->trailer.body.updater.challenge.updater_xid;
			updater_ask.member_ordinal
				= request->trailer.body.updater.challenge.member_ordinal;
			updater_ask.member_status
				= request->trailer.body.updater.challenge.member_status;
			ask = &updater_ask;
		}
		memset(&key, 0, sizeof(key));
		memset(&result, 0, sizeof(result));
		if (!cluster_runtime_visibility_physical_locator_sample_held(
				&context->current_mx_locators[i],
				&context->admission, &context->guard, &context->tt_root,
				&key, &result, &ctrc_physical_active))
			return false;
		context->current_mx_sampled_keys[i] = key;
		if (!cluster_multixact_current_resolve_origin_member_proof(
				ask->xid, ask->member_status, ask->member_ordinal,
				(uint16)cluster_node_id,
				request->prefix.mxkey.cluster_epoch, false, &key, &result,
				NULL, NULL, &context->current_mx_proofs[i]))
			return false;
		if (context->current_mx_proofs[i].state == CCM_ACTIVE
			|| context->current_mx_proofs[i].state == CCM_SELF)
		{
			if (!ctrc_physical_active)
				return false;
			if (cluster_undo_block0_current_sample_generation(
					&context->guard, &context->tt_root, &generation)
				!= CLUSTER_UNDO_BLOCK0_OK
				|| !gcs_block_current_mx_ctrc_key(
					context, i, &generation, &ctrc_key))
				return false;
			touch_result = cluster_ctrc_origin_touch_exact(
				&ctrc_key, &context->current_mx_requester_identity,
				context->current_mx_proofs[i].state == CCM_SELF
					? CTRC_PROOF_SELF : CTRC_PROOF_ACTIVE,
				&ctrc_grant);
			if ((touch_result != CLUSTER_CTRC_TOUCH_RECORDED
				 && touch_result != CLUSTER_CTRC_TOUCH_DUPLICATE)
				|| ctrc_grant == 0
				|| cluster_undo_block0_current_sample_generation(
					&context->guard, &context->tt_root, &final_generation)
				   != CLUSTER_UNDO_BLOCK0_OK
				|| !final_generation.known
				|| final_generation.value != generation.value
				|| !gcs_block_r4_tx_origin_resolve_root_current(
					context, &context->tt_logical, &final_root)
				|| !cluster_undo_block0_root_matches(
					&context->tt_root, &final_root)
				|| !gcs_block_r4_tx_origin_admission_current(context)
				|| cluster_qvotec_get_self_incarnation()
				   != ctrc_key.origin_boot_incarnation)
				return false;
			ClusterCurrentMemberProofSetCtrcGrant(
				&context->current_mx_proofs[i], ctrc_grant);
			if (!cluster_multixact_current_member_proof_bind_ctrc(
					&context->current_mx_proofs[i], &ctrc_key))
				return false;
			context->current_mx_ctrc_keys[i] = ctrc_key;
		}
		else
			ClusterCurrentMemberProofSetCtrcGrant(
				&context->current_mx_proofs[i], 0);
		context->current_mx_sampled_bitmap |= (uint8)(1U << i);
	}
	if (request->prefix.body_kind
			== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
		const ClusterCurrentMxUpdaterChallengeWire *challenge
			= &request->trailer.body.updater.challenge;

		context->current_mx_updater_proof.mxkey = request->prefix.mxkey;
		context->current_mx_updater_proof.candidate_next_xmin_alias
			= challenge->candidate_next_xmin_alias;
		context->current_mx_updater_proof.candidate_next_xmin_locator
			= challenge->candidate_next_xmin_locator;
		context->current_mx_updater_proof.updater_xid
			= challenge->updater_xid;
		context->current_mx_updater_proof.member_ordinal
			= challenge->member_ordinal;
		/* DATA requalification, not the canonical TT sample alone, promotes
		 * this exact echo to MATCH. */
		context->current_mx_updater_proof.verdict = CUCP_UNKNOWN;
	}
	return true;
}

typedef enum GcsBlockCurrentMxAdvance {
	GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED = 0,
	GCS_BLOCK_CURRENT_MX_ADVANCE_COMPLETE,
	GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT
} GcsBlockCurrentMxAdvance;

static GcsBlockCurrentMxAdvance
gcs_block_current_mx_origin_updater_provenance_advance(
	GcsBlockR4TxOriginContext *context)
{
	ClusterRuntimeVisibilityOriginStep origin_step;
	ClusterTxOutcome outcome;
	bool same_segment = false;

	if (context == NULL
		|| context->domain != GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
		|| context->current_mx_request.prefix.body_kind
			!= CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE
		|| gcs_block_r4_tx_origin_remaining_timeout_ms(context) == 0)
		return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;

	switch (context->phase) {
	case GCS_BLOCK_R4_TX_ORIGIN_DATA_FREEZE:
		origin_step
			= cluster_runtime_visibility_origin_plan_freeze_data_held(
				&context->locator, context->resolve_mode,
				&context->admission, NULL, &context->guard,
				&context->root, &context->origin_plan,
				&context->resolution, &context->reason);
		if (origin_step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_FAILED
			|| !cluster_runtime_visibility_origin_plan_canonical_physical(
				&context->origin_plan, &context->current_mx_locators[0],
				&same_segment))
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		context->current_mx_locator_count = 1;
		if (origin_step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL) {
			if (same_segment
				|| !cluster_runtime_visibility_origin_plan_canonical_logical(
					&context->origin_plan, &context->tt_logical)
				|| !gcs_block_r4_tx_origin_resolve_root_current(
					context, &context->tt_logical, &context->tt_root))
				return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
			return GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT;
		}
		if (!same_segment)
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		context->tt_logical = context->logical;
		context->tt_root = context->root;
		if (!gcs_block_current_mx_origin_sample_held(context))
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		context->canonical_sampled = true;
		outcome = cluster_runtime_visibility_origin_plan_recheck_data_held(
			&context->origin_plan, context->resolve_mode,
			&context->admission, &context->guard, &context->root,
			&context->resolution, &context->reason);
		if (outcome == CLUSTER_TX_UNKNOWN)
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		context->outcome = outcome;
		context->current_mx_updater_proof.candidate_next_xmin_locator
			= context->resolution.locator_echo;
		context->current_mx_updater_proof.verdict = CUCP_MATCH;
		return GCS_BLOCK_CURRENT_MX_ADVANCE_COMPLETE;

	case GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE:
		if (!cluster_runtime_visibility_origin_plan_sample_canonical_held(
				&context->origin_plan, context->resolve_mode,
				&context->admission, &context->guard, &context->tt_root,
				&context->reason)
			|| !gcs_block_current_mx_origin_sample_held(context))
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		context->canonical_sampled = true;
		return GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT;

	case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK:
		outcome = cluster_runtime_visibility_origin_plan_recheck_data_held(
			&context->origin_plan, context->resolve_mode,
			&context->admission, &context->guard, &context->recheck_root,
			&context->resolution, &context->reason);
		if (outcome == CLUSTER_TX_UNKNOWN)
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		context->outcome = outcome;
		context->current_mx_updater_proof.candidate_next_xmin_locator
			= context->resolution.locator_echo;
		context->current_mx_updater_proof.verdict = CUCP_MATCH;
		return GCS_BLOCK_CURRENT_MX_ADVANCE_COMPLETE;

	default:
		return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
	}
}

static GcsBlockCurrentMxAdvance
gcs_block_current_mx_origin_advance(
	GcsBlockR4TxOriginContext *context)
{
	uint16 i;

	if (context == NULL || context->current_mx_locator_count == 0)
		return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
	for (i = 0; i < context->current_mx_locator_count; i++) {
		if ((context->current_mx_sampled_bitmap & (uint8)(1U << i)) != 0)
			continue;
		context->tt_logical.owner_instance
			= (uint8)((uint32)cluster_node_id + 1);
		context->tt_logical.segment_id
			= context->current_mx_locators[i].segment_id;
		if (!gcs_block_r4_tx_origin_resolve_root_current(
				context, &context->tt_logical, &context->tt_root)) {
			context->current_mx_result = CMX_RESOLVE_UNKNOWN;
			context->current_mx_failure
				= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ROOT;
			context->current_mx_failure_xid
				= context->current_mx_locators[i].xid;
			context->current_mx_failure_index = i;
			return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
		}
		return GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT;
	}
	if (!gcs_block_current_mx_requester_identity_current(context))
	{
		context->current_mx_result = CMX_RESOLVE_RETRY;
		context->current_mx_proof_count = 0;
		context->current_mx_failure
			= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SEND_FRESHNESS;
		return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
	}
	if (context->current_mx_request.prefix.body_kind
			== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE
		&& context->current_mx_updater_proof.verdict != CUCP_MATCH) {
		context->current_mx_result = CMX_RESOLVE_UNKNOWN;
		context->current_mx_failure
			= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SAMPLE;
		return GCS_BLOCK_CURRENT_MX_ADVANCE_FAILED;
	}
	context->current_mx_proof_count = context->current_mx_locator_count;
	context->current_mx_result = CMX_RESOLVE_OK;
	context->reason = CLUSTER_TX_RESOLVE_NONE;
	if (context->current_mx_request.prefix.body_kind
			== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE
		&& context->current_mx_request.trailer.body.updater.challenge
			   .candidate_next_xmin_alias.undo_record_segment_id
			   != context->current_mx_locators[0].segment_id)
		cluster_multixact_current_stats_bump(
			CMX_STAT_UPDATER_PROVENANCE_CROSS_SEGMENT_MATCH);
	return GCS_BLOCK_CURRENT_MX_ADVANCE_COMPLETE;
}

static GcsBlockR4TxOriginPhase
gcs_block_current_mx_origin_after_release(
	GcsBlockR4TxOriginContext *context)
{
	GcsBlockCurrentMxAdvance advance;

	if (context == NULL || !context->canonical_sampled)
		return GCS_BLOCK_R4_TX_ORIGIN_SEND;
	advance = gcs_block_current_mx_origin_advance(context);
	if (advance == GCS_BLOCK_CURRENT_MX_ADVANCE_COMPLETE)
		cluster_ctrc_test_barrier_wait(
			CTRC_TEST_BARRIER_ACTIVE_PROOF_READY);
	return advance == GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT
		? GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN
		: GCS_BLOCK_R4_TX_ORIGIN_SEND;
}

/* The status-22 DATA worker is the one bounded resolver for all contexts in
 * this process.  Candidate-2 forbids a second active 0xFB current guard, so a
 * burst must yield in the existing event loop until the current holder has
 * released it.  Each waiting context retains its original deadline. */
static bool
gcs_block_r4_tx_origin_scur_available(
	const GcsBlockR4TxOriginContext *context)
{
	int i;

	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++) {
		const GcsBlockR4TxOriginContext *other
			= &gcs_block_r4_tx_origin_contexts[i];

		if (other != context && other->in_use && other->guard_active)
			return false;
	}
	return true;
}

/* Status-22 contexts are owned by worker 0.  Current-MX proof contexts are
 * owned by the DATA shard that accepted their exact request identity. */
static bool
gcs_block_r4_tx_origin_worker_current(
	const GcsBlockR4TxOriginContext *context)
{
	int current_worker;

	if (context == NULL)
		return false;
	current_worker = cluster_ic_tier1_my_data_channel();
	if (context->domain == GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX) {
		BufferTag route_tag = GcsBlockCurrentMxRouteTagMake(
			context->current_mx_request.prefix.request_id,
			context->current_mx_request.prefix.epoch,
			context->current_mx_request.prefix.original_requester_node,
			context->current_mx_request.prefix.requester_backend_id);

		return current_worker
			== cluster_lms_shard_for_tag(&route_tag, cluster_lms_workers);
	}
	return current_worker == 0;
}

static void
gcs_block_r4_tx_origin_step(GcsBlockR4TxOriginContext *context)
{
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_OK;
	ClusterRuntimeVisibilityOriginStep origin_step;
	GcsBlockCurrentMxAdvance current_mx_advance;
	int remaining_timeout_ms;

	switch (context->phase) {
		case GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_BEGIN:
			if (!gcs_block_r4_tx_origin_scur_available(context)) {
				if (gcs_block_r4_tx_origin_deadline_expired(context))
					gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			remaining_timeout_ms
				= gcs_block_r4_tx_origin_remaining_timeout_ms(context);
			if (context->deadline != 0 && remaining_timeout_ms == 0) {
				context->reason = CLUSTER_TX_RESOLVE_TIMEOUT;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
				break;
			}
			step = cluster_undo_block0_current_acquire_begin_admitted(
				&context->logical, CLUSTER_UNDO_BLOCK0_SCUR,
				remaining_timeout_ms, &context->admission,
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
				context->guard_active = true;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_DATA_FREEZE;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
				context->guard_active = true;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_POLL;
			} else
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_ACQUIRE_POLL:
			if (gcs_block_r4_tx_origin_deadline_expired(context)) {
				gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			step = cluster_undo_block0_current_acquire_poll(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_DATA_FREEZE;
			else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
				context->guard_active = false;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_DATA_FREEZE:
			if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX) {
				current_mx_advance
					= gcs_block_current_mx_origin_updater_provenance_advance(
						context);
				if (current_mx_advance
						== GCS_BLOCK_CURRENT_MX_ADVANCE_COMPLETE)
					context->phase
						= GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN;
				else if (current_mx_advance
						 == GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT)
					context->phase
						= GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_BEGIN;
				else {
					context->current_mx_failure
						= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SAMPLE;
					context->phase
						= GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN;
				}
				break;
			}
			origin_step
				= cluster_runtime_visibility_origin_plan_freeze_data_held(
					&context->locator, context->resolve_mode,
					&context->admission, &context->expected_generation,
					&context->guard, &context->root,
					&context->origin_plan, &context->resolution,
					&context->reason);
			if (origin_step == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_COMPLETE) {
				context->outcome = context->resolution.outcome;
				context->phase
					= GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN;
			} else if (origin_step
					   == CLUSTER_RUNTIME_VISIBILITY_ORIGIN_NEEDS_CANONICAL)
				context->phase
					= GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_BEGIN;
			else
				context->phase
					= GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN;
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_BEGIN:
			step = cluster_undo_block0_current_release_begin(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				context->guard_active = false;
				memset(&context->guard, 0, sizeof(context->guard));
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_POLL;
			else {
				context->guard_active = false;
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				memset(&context->resolution, 0,
					   sizeof(context->resolution));
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_DATA_RELEASE_POLL:
			if (gcs_block_r4_tx_origin_deadline_expired(context)) {
				gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			step = cluster_undo_block0_current_release_poll(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				context->guard_active = false;
				memset(&context->guard, 0, sizeof(context->guard));
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
				context->guard_active = false;
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				memset(&context->resolution, 0,
					   sizeof(context->resolution));
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_BEGIN:
			if (!gcs_block_r4_tx_origin_scur_available(context)) {
				if (gcs_block_r4_tx_origin_deadline_expired(context))
					gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			if ((context->domain
					 == GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
				 && (!gcs_block_r4_tx_origin_admission_current(context)
					 || context->tt_logical.segment_id == 0))
				|| (context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_TX_RESOLVE
					&& (!cluster_runtime_visibility_origin_plan_canonical_logical(
							&context->origin_plan, &context->tt_logical)
						|| !gcs_block_r4_tx_origin_resolve_root_current(
							context, &context->tt_logical,
							&context->tt_root)))) {
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
				if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX) {
					context->current_mx_failure
						= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_ROOT;
					if (context->current_mx_locator_count != 0)
						context->current_mx_failure_xid
							= context->current_mx_locators[0].xid;
				}
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
				break;
			}
			remaining_timeout_ms
				= gcs_block_r4_tx_origin_remaining_timeout_ms(context);
			if (context->deadline != 0 && remaining_timeout_ms == 0) {
				context->reason = CLUSTER_TX_RESOLVE_TIMEOUT;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
				break;
			}
			step = cluster_undo_block0_current_acquire_begin_admitted(
				&context->tt_logical, CLUSTER_UNDO_BLOCK0_SCUR,
				remaining_timeout_ms, &context->admission,
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
				context->guard_active = true;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
				context->guard_active = true;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_POLL;
			} else {
				if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
					context->current_mx_failure
						= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SCUR_ACQUIRE;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_TT_ACQUIRE_POLL:
			if (gcs_block_r4_tx_origin_deadline_expired(context)) {
				gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			step = cluster_undo_block0_current_acquire_poll(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE;
			else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
				context->guard_active = false;
				if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
					context->current_mx_failure
						= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SCUR_ACQUIRE;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_TT_SAMPLE:
			if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
				&& context->current_mx_request.prefix.body_kind
					== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE)
				context->canonical_sampled
					= gcs_block_current_mx_origin_updater_provenance_advance(
						context) == GCS_BLOCK_CURRENT_MX_ADVANCE_NEXT;
			else
				context->canonical_sampled = context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
					? gcs_block_current_mx_origin_sample_held(context)
					: cluster_runtime_visibility_origin_plan_sample_canonical_held(
						&context->origin_plan, context->resolve_mode,
						&context->admission, &context->guard,
						&context->tt_root, &context->reason);
			if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
				&& !context->canonical_sampled)
				context->current_mx_failure
					= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SAMPLE;
			context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_BEGIN;
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_BEGIN:
			step = cluster_undo_block0_current_release_begin(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				context->guard_active = false;
				memset(&context->guard, 0, sizeof(context->guard));
				if (context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
					&& context->current_mx_request.prefix.body_kind
						== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE)
					context->phase = context->canonical_sampled
						? GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN
						: GCS_BLOCK_R4_TX_ORIGIN_SEND;
				else
					context->phase = context->domain
							== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
						? gcs_block_current_mx_origin_after_release(context)
						: (context->canonical_sampled
							? GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN
							: GCS_BLOCK_R4_TX_ORIGIN_SEND);
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_POLL;
			else {
				context->guard_active = false;
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
					context->current_mx_failure
						= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SCUR_RELEASE;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_TT_RELEASE_POLL:
			if (gcs_block_r4_tx_origin_deadline_expired(context)) {
				gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			step = cluster_undo_block0_current_release_poll(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				context->guard_active = false;
				memset(&context->guard, 0, sizeof(context->guard));
				if (context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
					&& context->current_mx_request.prefix.body_kind
						== CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE)
					context->phase = context->canonical_sampled
						? GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN
						: GCS_BLOCK_R4_TX_ORIGIN_SEND;
				else
					context->phase = context->domain
							== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
						? gcs_block_current_mx_origin_after_release(context)
						: (context->canonical_sampled
							? GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN
							: GCS_BLOCK_R4_TX_ORIGIN_SEND);
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
				context->guard_active = false;
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
					context->current_mx_failure
						= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SCUR_RELEASE;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_BEGIN:
			if (!gcs_block_r4_tx_origin_scur_available(context)) {
				if (gcs_block_r4_tx_origin_deadline_expired(context))
					gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			if (!gcs_block_r4_tx_origin_resolve_root_current(
					context, &context->logical, &context->recheck_root)
				|| context->recheck_root.intent != context->root.intent
				|| context->recheck_root.root_id != context->root.root_id
				|| context->recheck_root.root_generation
					   != context->root.root_generation) {
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_STALE;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
				break;
			}
			remaining_timeout_ms
				= gcs_block_r4_tx_origin_remaining_timeout_ms(context);
			if (context->deadline != 0 && remaining_timeout_ms == 0) {
				context->reason = CLUSTER_TX_RESOLVE_TIMEOUT;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
				break;
			}
			step = cluster_undo_block0_current_acquire_begin_admitted(
				&context->logical, CLUSTER_UNDO_BLOCK0_SCUR,
				remaining_timeout_ms, &context->admission,
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
				context->guard_active = true;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
				context->guard_active = true;
				context->phase
					= GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_POLL;
			} else
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK_ACQUIRE_POLL:
			if (gcs_block_r4_tx_origin_deadline_expired(context)) {
				gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			step = cluster_undo_block0_current_acquire_poll(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK;
			else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
				context->guard_active = false;
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_DATA_RECHECK:
			if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
				(void)gcs_block_current_mx_origin_updater_provenance_advance(
					context);
			else
				context->outcome
					= cluster_runtime_visibility_origin_plan_recheck_data_held(
						&context->origin_plan, context->resolve_mode,
						&context->admission, &context->guard,
						&context->recheck_root, &context->resolution,
						&context->reason);
			context->phase = GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN;
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_BEGIN:
			step = cluster_undo_block0_current_release_begin(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				context->guard_active = false;
				context->phase = context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
					? gcs_block_current_mx_origin_after_release(context)
					: GCS_BLOCK_R4_TX_ORIGIN_SEND;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_POLL;
			else {
				context->guard_active = false;
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				memset(&context->resolution, 0,
					   sizeof(context->resolution));
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_FINAL_RELEASE_POLL:
			if (gcs_block_r4_tx_origin_deadline_expired(context)) {
				gcs_block_r4_tx_origin_timeout(context);
				break;
			}
			step = cluster_undo_block0_current_release_poll(
				&context->guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED) {
				context->guard_active = false;
				context->phase = context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX
					? gcs_block_current_mx_origin_after_release(context)
					: GCS_BLOCK_R4_TX_ORIGIN_SEND;
			} else if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
				context->guard_active = false;
				context->outcome = CLUSTER_TX_UNKNOWN;
				context->reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
				memset(&context->resolution, 0,
					   sizeof(context->resolution));
				context->phase = GCS_BLOCK_R4_TX_ORIGIN_SEND;
			}
			break;
		case GCS_BLOCK_R4_TX_ORIGIN_SEND:
		{
			ClusterICSendResult send_result;
			uint32 capability_generation = 0;
			bool optional_supported = false;
			int32 requester_node;
			uint64 request_epoch;
			bool capability_current;

			if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX) {
				requester_node
					= context->current_mx_request.prefix.original_requester_node;
				request_epoch = context->current_mx_request.prefix.epoch;
				capability_current
					= cluster_sf_peer_multixact_current_capability_generation(
						requester_node, &capability_generation)
					&& cluster_sf_peer_capability_generation_matches(
						requester_node,
						PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1,
						capability_generation);
			} else {
				requester_node
					= context->forward.base.original_requester_node;
				request_epoch = context->forward.base.epoch;
				capability_current = cluster_sf_peer_capability_family_sample(
					requester_node, GCS_BLOCK_R4_TX_REQUIRED_HELLO_CAPS, 0,
					&optional_supported, &capability_generation);
			}

			if (!gcs_block_r4_tx_origin_worker_current(context)
				|| cluster_epoch_get_current() != request_epoch
				|| !capability_current
					|| capability_generation
						   != context->requester_capability_generation
					|| !gcs_block_r4_tx_origin_admission_current(context)) {
					if (context->domain
						== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX) {
						context->current_mx_failure
							= GCS_BLOCK_CURRENT_MX_ORIGIN_FAILURE_SEND_FRESHNESS;
						gcs_block_current_mx_origin_log_first_unknown(context);
					}
					gcs_block_r4_tx_origin_context_clear(context, true);
				break;
			}
			if (context->domain
					== GCS_BLOCK_R4_TX_ORIGIN_DOMAIN_CURRENT_MX)
				gcs_block_current_mx_origin_prepare_reply(context);
			else
				gcs_block_r4_tx_origin_prepare_reply(context);
			send_result = gcs_block_send_envelope_or_loopback(
				PGRAC_IC_MSG_GCS_BLOCK_REPLY,
				requester_node,
				context->reply_frame, sizeof(context->reply_frame));
			cluster_gcs_block_note_send_outcome(
				GCS_BLOCK_SEND_FAMILY_REPLY, send_result);
			if (send_result == CLUSTER_IC_SEND_HARD_ERROR)
				cluster_lms_data_plane_close_peer_now(
					context->forward.base.original_requester_node);
			else if (ClusterGcsBlock != NULL)
				pg_atomic_fetch_add_u64(
					&ClusterGcsBlock->block_reply_count, 1);
			gcs_block_r4_tx_origin_context_clear(context, true);
			break;
		}
		default:
			gcs_block_r4_tx_origin_context_clear(context, true);
			break;
	}
	if (context->in_use && failure != CLUSTER_UNDO_BLOCK0_OK
		&& context->current_failure == CLUSTER_UNDO_BLOCK0_OK)
		context->current_failure = failure;
}

void
cluster_gcs_block_r4_tx_resolve_drain(void)
{
	int i;

	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++) {
		GcsBlockR4TxOriginContext *context
			= &gcs_block_r4_tx_origin_contexts[i];
		int step_budget;

		if (!context->in_use)
			continue;
		for (step_budget = 0;
			 step_budget < GCS_BLOCK_R4_TX_ORIGIN_STEP_BUDGET;
			 step_budget++) {
			GcsBlockR4TxOriginPhase phase_before = context->phase;

			PG_TRY();
			{
				gcs_block_r4_tx_origin_step(context);
			}
			PG_CATCH();
			{
				FlushErrorState();
				gcs_block_r4_tx_origin_context_clear(context, true);
			}
			PG_END_TRY();
			if (context->in_use && context->failure_phase == 0
				&& gcs_block_r4_tx_origin_failure_transition(
					phase_before, context))
				context->failure_phase = phase_before;
			if (!context->in_use || context->phase == phase_before)
				break;
		}
	}
}

bool
cluster_gcs_block_r4_tx_resolve_active(void)
{
	int i;

	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++)
		if (gcs_block_r4_tx_origin_contexts[i].in_use)
			return true;
	return false;
}

long
cluster_gcs_block_r4_tx_resolve_wait_timeout(long idle_timeout_ms)
{
	int active_contexts = 0;
	int i;

	for (i = 0; i < GCS_BLOCK_R4_TX_ORIGIN_CONTEXTS; i++)
		if (gcs_block_r4_tx_origin_contexts[i].in_use)
			active_contexts++;
	return cluster_gcs_block_r4_tx_resolve_wait_timeout_for_count(
		idle_timeout_ms, active_contexts);
}

static void
gcs_block_send_reply(int32 dest_node, const GcsBlockRequestPayload *req, GcsBlockReplyStatus status,
					 XLogRecPtr page_lsn, const char *block_data)
{
	uint32 total = (uint32)(sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	GcsBlockReplyHeader hdr;
	ClusterICSendResult rc;

	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = req->request_id;
	hdr.page_lsn = (uint64)page_lsn;
	hdr.epoch = cluster_epoch_get_current();
	hdr.sender_node = cluster_node_id;
	hdr.requester_backend_id = req->requester_backend_id;
	hdr.transition_id = req->transition_id;
	hdr.status = (uint8)status;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	if (status == GCS_BLOCK_REPLY_GRANTED && block_data != NULL)
		hdr.checksum = gcs_block_compute_checksum(block_data);

	if (GcsBlockRequestPayloadIsDirectLandArmed(req) && dest_node != cluster_node_id) {
		if (!GcsBlockReplyStatusIsDirectLandSendable(status))
			GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "direct-land-nonsendable");
		if (status != GCS_BLOCK_REPLY_GRANTED || block_data == NULL) {
			char zero_page[GCS_BLOCK_DATA_SIZE];

			memset(zero_page, 0, sizeof(zero_page));
			hdr.checksum = gcs_block_compute_checksum(zero_page);
		}
		(void)gcs_block_try_send_direct_reply(dest_node, true, &hdr, block_data, 0, NULL, NULL);
		return;
	}

	if (status == GCS_BLOCK_REPLY_GRANTED && block_data != NULL) {
		ClusterICSge sge[2];

		memset(sge, 0, sizeof(sge));
		sge[0].addr = &hdr;
		sge[0].len = sizeof(hdr);
		sge[1].addr = (void *)block_data;
		sge[1].len = GCS_BLOCK_DATA_SIZE;
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_ship_bytes_total, GCS_BLOCK_DATA_SIZE);
		rc = cluster_ic_rdma_send_envelope_sge(PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node, sge,
											   lengthof(sge), total);
	} else {
		/*
		 * GRANTED_STORAGE_FALLBACK + all DENIED_* carry a zero block image by
		 * ABI.  Keep the contiguous path because there is no shared_buffers
		 * page to point an SGE at.
		 */
		char *buf;
		GcsBlockReplyHeader *wire_hdr;

		buf = (char *)palloc0(total);
		wire_hdr = (GcsBlockReplyHeader *)buf;
		*wire_hdr = hdr;
		wire_hdr->checksum = gcs_block_compute_checksum(buf + sizeof(GcsBlockReplyHeader));
		rc = gcs_block_send_envelope_or_loopback(PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node, buf,
												 total);
		pfree(buf);
	}

	/* Round-5: an admitted reply (WOULD_BLOCK) is a sent reply — the
	 * transport owns the copy and delivers it in order. */
	cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, rc);
	if ((rc == CLUSTER_IC_SEND_DONE || rc == CLUSTER_IC_SEND_WOULD_BLOCK)
		&& ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
}

static bool
gcs_block_deny_direct_armed_forward_request(const GcsBlockRequestPayload *req)
{
	if (!GcsBlockRequestPayloadIsDirectLandArmed(req))
		return false;

	/*
	 * The requester posted its receive on this master's block-reply lane.  A
	 * generic holder reply would arrive while that receive is still live, so
	 * consume/abort the direct receive first and let the requester retry
	 * without direct-land.
	 */
	GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "direct-land-forward-rearm");
	gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
						 InvalidXLogRecPtr, NULL);
	return true;
}

/*
 * gcs_block_resend_cached_reply — for spec-2.34 D5 dedup CACHED_REPLY path.
 *
 *	Master saw the same 4-tuple key already + reply was already produced;
 *	resend the stored reply payload to the sender without re-flushing WAL
 *	or re-copying the page.  The cached reply still validates HC100 on
 *	the sender side because the cached hdr->epoch / hdr->sender_node match
 *	the values stamped into the sender's slot at the original send time.
 *	If the sender has since advanced its epoch (e.g. eager wake fired),
 *	the cached reply's stale hdr->epoch will be dropped by HC100 — sender
 *	then issues a new request with a fresh 4-tuple key (different cluster_
 *	epoch field) which will MISS_REGISTERED and produce a fresh reply.
 */
static bool
gcs_block_resend_cached_reply(int32 dest_node, const GcsBlockDedupEntry *entry)
{
	uint32 header_len;
	uint32 total;
	char *buf;
	GcsBlockReplyHeader *hdr;
	char *block_data;
	bool has_block_payload;
	ClusterICSendResult rc;

	if (entry == NULL)
		return false;

	header_len = entry->has_sf_dep && entry->sf_dep_count > 0
					 ? (uint32)sizeof(GcsBlockReplyHeaderV2)
					 : (uint32)sizeof(GcsBlockReplyHeader);
	total = header_len + GCS_BLOCK_DATA_SIZE;
	buf = (char *)palloc0(total);
	hdr = (GcsBlockReplyHeader *)buf;
	*hdr = entry->reply_header;
	if (entry->has_sf_dep && entry->sf_dep_count > 0) {
		GcsBlockReplyHeaderV2 *hdrv2 = (GcsBlockReplyHeaderV2 *)buf;
		int i;
		int n = 0;

		hdrv2->sf_flags = entry->sf_flags;
		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
			if (XLogRecPtrIsInvalid(entry->payload_meta.sf_dep_vec.required[i]))
				continue;
			hdrv2->sf_dep[n].origin_node = i;
			hdrv2->sf_dep[n].required_redo_lsn = (uint64)entry->payload_meta.sf_dep_vec.required[i];
			n++;
		}
		hdrv2->sf_dep_count = (uint8)n;
	}
	/* A READ_IMAGE forward MARKER (forwarding_master_node stamped, no
	 * payload, header checksum never computed) must never reach this
	 * resend — the dedup lookup classifies it FORWARDED.  Fail closed if
	 * one does: the requester times out and its retransmit takes the
	 * re-forward path.  (Resending would ship a zero page whose 31-hash
	 * checksum, 0, matches the never-computed header field — a verifying
	 * false-empty install, 8.A.)  The master-DIRECT xheld serve entry
	 * (NO_FORWARDING_MASTER + real page) resends normally below. */
	if (entry->status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		&& GcsBlockReplyHeaderGetForwardingMasterNode(&entry->reply_header)
			   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER) {
		Assert(false); /* classification bug — lookup must route FORWARDED */
		pfree(buf);
		return false;
	}
	block_data = buf + header_len;
	has_block_payload = entry->status == GCS_BLOCK_REPLY_GRANTED
						|| entry->status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	if (has_block_payload)
		memcpy(block_data, entry->block_data, GCS_BLOCK_DATA_SIZE);
	/* else: block_data already zeroed by palloc0 */
	hdr->checksum = gcs_block_compute_checksum(block_data);

	if ((entry->request_flags & GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND) != 0
		&& dest_node != cluster_node_id) {
		if (!GcsBlockReplyStatusIsDirectLandSendable((GcsBlockReplyStatus)hdr->status))
			gcs_block_log_master_not_holder_producer(
				"cached-direct-land-nonsendable", entry->tag, entry->key.request_id,
				entry->key.cluster_epoch, (int32)entry->key.origin_node_id, entry->transition_id);
		(void)gcs_block_try_send_direct_reply(dest_node, true, hdr,
											  has_block_payload ? block_data : NULL, 0, NULL, NULL);
		pfree(buf);
		return true;
	}
	rc = gcs_block_send_envelope_or_loopback(PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node, buf, total);
	cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, rc);
	pfree(buf);
	return rc == CLUSTER_IC_SEND_DONE || rc == CLUSTER_IC_SEND_WOULD_BLOCK;
}


/*
 * gcs_block_produce_reply — original (non-cached) master-side flow.
 *
 *	Implements the spec-2.33 §3.2 master decision tree.  Renamed +
 *	extracted from cluster_gcs_handle_block_request_envelope so spec-2.34
 *	D5 can wrap it with a dedup lookup_or_register / install_reply pair.
 *
 *	The caller is responsible for performing dedup_install_reply with the
 *	produced status + reply_header + block_data so duplicate retries hit
 *	CACHED_REPLY.  This function only computes the reply (or sends it for
 *	terminal-decision paths) and reports the status back to the caller.
 *
 *	Output parameters:
 *	  *out_status:        the GcsBlockReplyStatus to install in dedup HTAB
 *	  *out_page_lsn:      LSN for GRANTED;  InvalidXLogRecPtr otherwise
 *	  *out_block_payload: pointer to the BLCKSZ buffer for GRANTED;  NULL
 *	                     otherwise (use block_buf storage passed in)
 *
 *	Returns true if the caller should install_reply + send_reply;  false
 *	if a reply was already sent (e.g. early VALIDATOR_REJECT path) and no
 *	dedup install should happen.
 */
static bool
gcs_block_produce_reply(const GcsBlockRequestPayload *req, char *block_buf, bool preprepared_image,
						GcsBlockReplyStatus *out_status, XLogRecPtr *out_page_lsn,
						const char **out_block_payload, uint32 *out_block_lkey,
						ClusterICSgeReleaseCallback *out_release_cb, void **out_release_arg,
						ClusterSfDepVec *out_sf_dep_vec, bool *out_sf_dep_valid)
{
	uint64 current_epoch;
	PcmLockMode state;
	PcmGcsTransitionApplyResult apply_result;
	ClusterBufmgrGcsCopyRefusal copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	bool found;

	*out_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
	if (!preprepared_image) {
		*out_page_lsn = InvalidXLogRecPtr;
		*out_block_payload = NULL;
	}
	if (out_block_lkey != NULL)
		*out_block_lkey = 0;
	if (out_release_cb != NULL)
		*out_release_cb = NULL;
	if (out_release_arg != NULL)
		*out_release_arg = NULL;
	if (out_sf_dep_vec != NULL)
		cluster_sf_dep_vec_reset(out_sf_dep_vec);
	if (out_sf_dep_valid != NULL)
		*out_sf_dep_valid = false;
	if (preprepared_image
		&& (*out_block_payload != block_buf
			|| PageGetLSN((Page)block_buf) != *out_page_lsn)) {
		/* The X->S helper already committed the local downgrade, so never
		 * recopy or invent a carrier here.  A PostgreSQL page LSN of zero is
		 * valid (for example, a hint-only page); only disagreement between the
		 * copied bytes and their exact sampled LSN is unsafe. */
		*out_page_lsn = InvalidXLogRecPtr;
		*out_block_payload = NULL;
		*out_status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		return true;
	}

	/* HC73 epoch freshness. */
	current_epoch = cluster_epoch_get_current();
	if (req->epoch < current_epoch) {
		*out_status = GCS_BLOCK_REPLY_DENIED_EPOCH_STALE;
		return true;
	}

	/*
	 * spec-2.34 D17 — fault injection.  When the test fixture activates
	 * `cluster-gcs-block-force-epoch-stale-reply` with SKIP semantics,
	 * the master returns DENIED_EPOCH_STALE on the next request even if
	 * the real epoch matches.  Drives the HC94 lazy retry TAP surface.
	 */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-force-epoch-stale-reply");
	if (cluster_injection_should_skip("cluster-gcs-block-force-epoch-stale-reply")) {
		*out_status = GCS_BLOCK_REPLY_DENIED_EPOCH_STALE;
		return true;
	}

	/*
	 * HC88: inspect availability before mutating PCM state.  Master is an
	 * ownership coordinator, not necessarily a local data holder.
	 *  - no buffer && state == N: GRANTED_STORAGE_FALLBACK (apply transition,
	 *    requester reads from shared storage)
	 *  - no buffer && state != N: DENIED_MASTER_NOT_HOLDER (fail-closed)
	 *  - buffer present: D4 helper handles HC82 + HC89 then reply GRANTED
	 */
	state = cluster_pcm_lock_query(req->tag);
	found = preprepared_image || cluster_bufmgr_probe_block_for_gcs(req->tag);

	if (!found && state == PCM_LOCK_MODE_N) {
		SCN fallback_watermark_scn;

		/*
		 * PGRAC: GCS-race round-4c FUNC-1 — a state=N grant ships no image,
		 * so the requester keeps whatever bytes it PRE-READ from shared
		 * storage before this negotiation.  If the previous live X holder's
		 * BAST-yield flush landed in between, that pre-read is a stale
		 * version and writing on it silently overwrites the flushed one
		 * (the R4 S3 lost-update chain).  Snapshot the authoritative
		 * pi_watermark_scn BEFORE the transition mutates the entry and
		 * carry it in the reply's page_lsn field so the requester can prove
		 * its copy current or refresh (cluster_gcs_block_fallback_verify_
		 * refresh).  Wire compat: fallback replies historically carried
		 * page_lsn == 0 and old requesters ignore the field; an old master
		 * sends 0 == InvalidScn which a new requester maps to verdict SKIP
		 * (the pre-fix behaviour).  The holder re-ack fallbacks below and
		 * in the X path intentionally KEEP the zero carrier: there the
		 * requester's own copy is authoritative (it may hold a shipped
		 * image newer than storage) and must never be overwritten.
		 */
		fallback_watermark_scn = cluster_pcm_lock_pi_watermark_scn_query(req->tag);
		apply_result = cluster_pcm_lock_apply_gcs_transition_result(
			req->tag, (PcmLockTransition)req->transition_id, req->sender_node);
		if (apply_result != PCM_GCS_TRANSITION_APPLIED) {
			*out_status
				= GcsBlockApplyRefusalStatus(apply_result, (PcmLockTransition)req->transition_id);
			return true;
		}
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
		*out_status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
		*out_page_lsn = (XLogRecPtr)fallback_watermark_scn;
		return true;
	}

	/*
	 * PGRAC: spec-4.7a D3 — idempotent re-acknowledge for a requester the
	 * master already records as a holder.  WITHOUT this, a node that released
	 * its content_lock (buf->pcm_state → N) while still recorded as x_holder
	 * re-requests N→S and gets DENIED_MASTER_NOT_HOLDER → sender retransmit
	 * loop → 53R90 (the D0 bug).  Master state is UNCHANGED: do NOT call
	 * apply_gcs_transition (N→S on an X state is an illegal transition,
	 * spec-4.7a v0.2 amend 2); the requester already holds a covering local
	 * mode (X ⊇ S).  S→X is excluded by the helper (real writer path → spec-
	 * 2.36 invalidate-then-grant, no double X).  Strict GrdEntry read; any
	 * uncertainty falls through to the fail-closed DENIED below (Rule 8.A).
	 */
	if (!found
		&& cluster_pcm_master_requester_is_holder(req->tag, req->sender_node,
												  (PcmLockTransition)req->transition_id)) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
		*out_status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
		return true;
	}

	if (!found) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
		GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "produce-no-resident-authority");
		*out_status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		return true;
	}

	/*
	 * D4 bufmgr helper performs HC82 XLogFlush(page_lsn) + content_lock dance
	 * + HC89 single-retry revalidation.  Conditional BufferContent refusal is
	 * retried by the owning backend with a fresh reservation/request identity;
	 * structural and HC89 exhaustion remain DENIED_MASTER_NOT_HOLDER.
	 */
	if (!preprepared_image
		&& !gcs_block_get_ship_image(req->tag, req->sender_node, true, out_page_lsn, block_buf,
									 out_block_payload, out_block_lkey, out_release_cb,
									 out_release_arg, out_sf_dep_vec, out_sf_dep_valid,
									 &copy_refusal)) {
		char producer_reason[96];

		snprintf(producer_reason, sizeof(producer_reason), "produce-copy-refused:%s",
				 cluster_bufmgr_gcs_copy_refusal_name(copy_refusal));
		GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, producer_reason);
		*out_status = GcsBlockMasterDirectCopyRefusalStatus(copy_refusal);
		return true;
	}
	if (out_sf_dep_valid == NULL || !*out_sf_dep_valid)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count, 1);

	/* HC77: master-side is the single transition-apply owner. */
	apply_result = cluster_pcm_lock_apply_gcs_transition_result(
		req->tag, (PcmLockTransition)req->transition_id, req->sender_node);
	if (apply_result != PCM_GCS_TRANSITION_APPLIED) {
		gcs_block_release_ship_image(out_release_cb != NULL ? *out_release_cb : NULL,
									 out_release_arg != NULL ? *out_release_arg : NULL);
		*out_block_payload = NULL;
		if (out_release_cb != NULL)
			*out_release_cb = NULL;
		if (out_release_arg != NULL)
			*out_release_arg = NULL;
		*out_status
			= GcsBlockApplyRefusalStatus(apply_result, (PcmLockTransition)req->transition_id);
		return true;
	}

	*out_status = GCS_BLOCK_REPLY_GRANTED;
	return true;
}

static bool
gcs_block_queue_pending_x_authoritative(BufferTag tag)
{
	PcmAuthoritySnapshot authority;

	if (!cluster_pcm_lock_authority_snapshot(tag, &authority))
		return false;
	return authority.pending_x_requester_node >= 0
		   && (authority.pending_x_since_lsn & PCM_PENDING_X_QUEUE_KIND) != 0;
}

/*
 * R10/A' PGRAC adaptation for S admission at the tag master.  The exported
 * name is retained for the original master-local caller; remote N->S ingress
 * uses the same exact check on both sides of dedup registration.  Treat the
 * existing exact active-head locator, or the approved pre-ASSERT late-bind
 * successor head, as a retry barrier only.  No page/PI bytes are consulted
 * and this result carries no authority.  Unexpected locator drift is a
 * retry-safe fail-closed barrier, not permission to admit S.
 */
bool
cluster_gcs_block_resource_x_local_s_barrier_active(BufferTag tag)
{
	if (!cluster_pcm_lock_resource_x_s_barrier_active(&tag)
		&& !cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
			&tag))
		return false;
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
	return true;
}

static uint64
gcs_block_pcm_x_monotonic_us(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_MICROSEC(now);
}

static uint64
gcs_block_pcm_x_saturating_add_us(uint64 base, uint64 delta)
{
	return base > UINT64_MAX - delta ? UINT64_MAX : base + delta;
}

static bool
gcs_block_resource_x_diagnostic_should_log(
	GcsBlockResourceXDiagnostic diagnostic, int discriminator)
{
	uint32 diagnostic_class;
	uint64 now_us;
	uint64 *next_log_at;

	if (diagnostic < 0
		|| diagnostic >= GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_COUNT)
		return false;
	diagnostic_class = discriminator >= 0
		&& discriminator < GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_CLASS_COUNT - 1
		? (uint32)discriminator
		: GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_CLASS_COUNT - 1;
	next_log_at
		= &gcs_block_resource_x_diagnostic_next_log_at[diagnostic]
			[diagnostic_class];
	now_us = gcs_block_pcm_x_monotonic_us();
	if (now_us < *next_log_at)
		return false;
	*next_log_at = gcs_block_pcm_x_saturating_add_us(
		now_us, GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_INTERVAL_US);
	return true;
}

static GcsBlockResourceXDiagnostic
gcs_block_resource_x_frame_diagnostic(uint8 frame_kind)
{
	switch (frame_kind) {
	case RESOURCE_X_WIRE_ASSERT_X:
		return GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_ASSERT_X;
	case RESOURCE_X_WIRE_BLOCK_TO_N:
		return GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_BLOCK_TO_N;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
		return GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_BLOCKED_TO_N;
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE:
		return GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_IMAGE_ENVELOPE;
	case RESOURCE_X_WIRE_AUTHORITY_GRANT:
		return GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_FRAME_AUTHORITY_GRANT;
	default:
		return GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_COUNT;
	}
}

static uint64
gcs_block_pcm_x_retry_timeout_us(void)
{
	uint64 timeout_ms = (uint64)Max(cluster_gcs_reply_timeout_ms, 1);

	return timeout_ms > UINT64_MAX / UINT64_C(1000)
		? UINT64_MAX
		: timeout_ms * UINT64_C(1000);
}

static bool
gcs_block_resource_x_payload_candidate(uint8 msg_type, uint32 payload_length)
{
	if (msg_type == RESOURCE_X_MSG_ASSERT_X)
		return payload_length == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_length == RESOURCE_X_SHORT_V1_BYTES;
	if (msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT)
		return payload_length == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_length == RESOURCE_X_PROOF_V1_BYTES
			|| payload_length == RESOURCE_X_IMAGE_V1_BYTES;
	if (msg_type == RESOURCE_X_MSG_BLOCK_TO_N)
		return payload_length == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_length == RESOURCE_X_PROOF_V1_BYTES;
	if (msg_type == RESOURCE_X_MSG_BLOCKED_TO_N)
		return payload_length == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_length == RESOURCE_X_PROOF_V1_BYTES;
	if (msg_type == RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE)
		return payload_length == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_length == RESOURCE_X_SHORT_V1_BYTES;
	return false;
}

/* The temporary R10 ticket adapter is selected only when the exact peer
 * connection (or the local endpoint) advertises the complete Resource-X
 * consumer.  A missing/reconnected peer stays on the legacy queue path. */
static bool
gcs_block_pcm_x_resource_x_peer_ready_exact(int32 peer_node,
											uint32 *connection_generation_out)
{
	uint32 capability_word = 0;
	uint32 connection_generation = 0;

	if (connection_generation_out != NULL)
		*connection_generation_out = 0;
	if (peer_node < 0 || peer_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return false;
	if (peer_node == cluster_node_id) {
		if ((cluster_ic_local_capability_word()
				 & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) == 0)
			return false;
		/* Same-node loopback has no peer HELLO record.  Match the existing
		 * local DATA projection: capability-current generation 1 is sampled
		 * independently of every received sender-local wire field. */
		if (connection_generation_out != NULL)
			*connection_generation_out = 1;
		return true;
	}
	if (!cluster_sf_peer_capability_word_sample(
		peer_node, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
		&capability_word, &connection_generation)
		|| connection_generation == 0)
		return false;
	if (connection_generation_out != NULL)
		*connection_generation_out = connection_generation;
	return true;
}

/* Notify only already-ARMED work after the consumer releases its locks.
 * The existing ring takes the physical claim and its ordinary drain performs
 * all current identity, capability, fence and payload checks before sending.
 * No direct send or recursive dispatch occurs here. Refused/remaining work
 * is still discoverable by the unchanged global scanner. */
static void
gcs_block_resource_x_stage_ready_tag(const BufferTag *tag)
{
	uint32 owner_cursor = 0;
	int call;

	if ((MyBackendType != B_LMS && MyBackendType != B_LMS_WORKER) || tag == NULL)
		return;
	for (call = 0; call < 16; call++) {
		ResourceXIntentSlot intent;
		uint32 connection_generation = 0;
		uint64 now_us;
		uint64 timeout_us;
		uint64 deadline_us;
		int worker_id;

		if (cluster_pcm_lock_resource_x_ready_intent_probe_exact(tag, &owner_cursor, &intent)
			!= RESOURCE_X_INTENT_PROBE_FOUND)
			break;
		now_us = gcs_block_pcm_x_monotonic_us();
		if (now_us == 0 || now_us == UINT64_MAX)
			break;
		if (!gcs_block_pcm_x_resource_x_peer_ready_exact((int32)intent.destination_node,
														 &connection_generation)) {
			(void)cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&intent, now_us);
			continue;
		}
		worker_id = cluster_lms_shard_for_tag(tag, cluster_lms_workers);
		timeout_us = (uint64)Max(cluster_gcs_reply_timeout_ms, 1) * UINT64_C(1000);
		deadline_us = now_us > UINT64_MAX - timeout_us ? UINT64_MAX : now_us + timeout_us;
		if (!cluster_lms_outbound_enqueue_resource_x_intent(worker_id, &intent,
															connection_generation, deadline_us))
			(void)cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(&intent, now_us);
	}
}

static PcmXSessionAuthResult
gcs_block_resource_x_gate_session_snapshot_result(
	const BufferTag *tag, ResourceXGateSnapshot *gate_out,
	int32 *master_node_out, uint64 *master_session_out)
{
	ResourceXGateSnapshot gate;
	PcmXSessionAuthResult session_result;
	uint64 master_session = 0;
	int32 master_node;

	if (gate_out != NULL)
		memset(gate_out, 0, sizeof(*gate_out));
	if (master_node_out != NULL)
		*master_node_out = -1;
	if (master_session_out != NULL)
		*master_session_out = 0;
	if (tag == NULL || !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN)
		return PCM_X_SESSION_AUTH_INVALID;
	master_node = cluster_gcs_lookup_master(*tag);
	if (master_node < 0 || master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_SESSION_AUTH_INVALID;
	if (gate_out != NULL)
		*gate_out = gate;
	if (master_node_out != NULL)
		*master_node_out = master_node;
	session_result = gcs_block_pcm_x_authenticated_session_result(
		master_node, cluster_epoch_get_current(), &master_session, NULL);
	if (session_result != PCM_X_SESSION_AUTH_OK)
		return session_result;
	if (master_session == 0 || master_session == UINT64_MAX)
		return PCM_X_SESSION_AUTH_INVALID;
	if (master_session_out != NULL)
		*master_session_out = master_session;
	return PCM_X_SESSION_AUTH_OK;
}

static bool
gcs_block_resource_x_gate_session_snapshot(
	const BufferTag *tag, ResourceXGateSnapshot *gate_out,
	int32 *master_node_out, uint64 *master_session_out)
{
	ResourceXGateSnapshot gate;
	uint64 master_session = 0;
	int32 master_node = -1;

	if (gcs_block_resource_x_gate_session_snapshot_result(
			tag, &gate, &master_node, &master_session)
		!= PCM_X_SESSION_AUTH_OK)
		return false;
	if (gate_out != NULL)
		*gate_out = gate;
	if (master_node_out != NULL)
		*master_node_out = master_node;
	if (master_session_out != NULL)
		*master_session_out = master_session;
	return true;
}

static bool
gcs_block_resource_x_gate_session_recheck(
	const BufferTag *tag, const ResourceXGateSnapshot *expected_gate,
	int32 expected_master_node, uint64 expected_master_session)
{
	ResourceXGateSnapshot current_gate;
	uint64 current_master_session = 0;
	int32 current_master_node = -1;

	return expected_gate != NULL
		&& gcs_block_resource_x_gate_session_snapshot(
			tag, &current_gate, &current_master_node,
			&current_master_session)
		&& memcmp(&current_gate, expected_gate, sizeof(current_gate)) == 0
		&& current_master_node == expected_master_node
		&& current_master_session == expected_master_session;
}

static bool
gcs_block_resource_x_peer_session_matches_exact(
	const BufferTag *tag, int32 expected_master_node,
	uint64 expected_master_session)
{
	ResourceXGateSnapshot gate;
	uint64 master_session = 0;
	int32 master_node = -1;

	return gcs_block_resource_x_gate_session_snapshot(
			tag, &gate, &master_node, &master_session)
		&& master_node == expected_master_node
		&& master_session == expected_master_session;
}

static void
gcs_block_resource_x_fail_closed_current(void)
{
	ResourceXGateSnapshot gate;
	ResourceXApplyResult result;

	if (!cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN)
		return;
	result = cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate);
	if (result == RESOURCE_X_APPLY_APPLIED)
		ereport(LOG,
				(errmsg_internal("cluster PCM-X runtime fail-closed (recovery blocked): "
								 "Resource-X gate fenced")));
}

static void
gcs_block_resource_x_put_u32(uint8 *out, uint32 value)
{
	out[0] = (uint8)(value >> 24);
	out[1] = (uint8)(value >> 16);
	out[2] = (uint8)(value >> 8);
	out[3] = (uint8)value;
}

static void
gcs_block_resource_x_put_u64(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> ((7 - i) * 8));
}

static uint32
gcs_block_pcm_x_resource_x_dependency_crc(
	const uint64 dependencies[RESOURCE_X_DEPENDENCY_MAX])
{
	uint8 canonical[RESOURCE_X_DEPENDENCY_VECTOR_BYTES];
	pg_crc32c crc;
	int i;

	memset(canonical, 0, sizeof(canonical));
	for (i = 0; i < RESOURCE_X_DEPENDENCY_MAX; i++)
		gcs_block_resource_x_put_u64(canonical + i * 8,
			dependencies[i]);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, canonical, sizeof(canonical));
	FIN_CRC32C(crc);
	return (uint32)crc;
}

/* Canonical master/requester durable proof shared by both independent storage
 * reads.  The fixed big-endian record is process-local evidence, not a new
 * wire ABI; AuthorityGrant carries only its exact CRC and bound fields. */
static uint32
gcs_block_pcm_x_resource_x_durable_proof_crc(
	const ResourceXDurableProof *proof)
{
	uint8 canonical[96];
	pg_crc32c crc;

	if (proof == NULL
		|| !resource_x_assertion_valid(&proof->assertion)
		|| proof->base_authority_generation == 0
		|| proof->resource_formation == 0
		|| proof->master_session_incarnation == 0
		|| proof->assertion_sequence == 0
		|| proof->requester_target_generation == 0)
		return 0;
	memset(canonical, 0, sizeof(canonical));
	gcs_block_resource_x_put_u32(canonical, UINT32_C(0x52584450)); /* RXDP */
	canonical[5] = 1;
	canonical[7] = (uint8)sizeof(canonical);
	gcs_block_resource_x_put_u32(canonical + 8,
		proof->assertion.resource.spcOid);
	gcs_block_resource_x_put_u32(canonical + 12,
		proof->assertion.resource.dbOid);
	gcs_block_resource_x_put_u32(canonical + 16,
		proof->assertion.resource.relNumber);
	gcs_block_resource_x_put_u32(canonical + 20,
		(uint32)proof->assertion.resource.forkNum);
	gcs_block_resource_x_put_u32(canonical + 24,
		proof->assertion.resource.blockNum);
	gcs_block_resource_x_put_u32(canonical + 28,
		(uint32)proof->assertion.requester_node);
	gcs_block_resource_x_put_u64(canonical + 32,
		proof->base_authority_generation);
	gcs_block_resource_x_put_u64(canonical + 40,
		proof->resource_formation);
	gcs_block_resource_x_put_u64(canonical + 48,
		proof->master_session_incarnation);
	gcs_block_resource_x_put_u64(canonical + 56,
		proof->assertion_sequence);
	gcs_block_resource_x_put_u64(canonical + 64,
		proof->requester_target_generation);
	gcs_block_resource_x_put_u64(canonical + 72,
		proof->page_scn_lsn);
	gcs_block_resource_x_put_u32(canonical + 80,
		proof->page_checksum);
	canonical[86] = RESOURCE_X_PROOF_DURABLE_STORAGE;
	canonical[87] = RESOURCE_X_DISPOSITION_DURABLE_STORAGE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, canonical, sizeof(canonical));
	FIN_CRC32C(crc);
	return (uint32)crc;
}

/* Encode both requester-owned type-14 frames before publishing either.  They
 * share one DATA FIFO when LOCAL_PROOF exists.  A clean N requester sends
 * ASSERT_X only and leaves proof selection to the exact no-holder master. */
static ResourceXApplyResult
gcs_block_resource_x_assert_stage_exact(
	int32 master_node, const ResourceXDecodedFrame *assertion,
	const ResourceXDecodedFrame *local_proof)
{
	uint8 assertion_payload[RESOURCE_X_CONTROL_V1_BYTES];
	uint8 proof_payload[RESOURCE_X_SHORT_V1_BYTES];
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint16 assertion_bytes = 0;
	uint16 proof_bytes = 0;

	if (master_node < 0 || master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| assertion == NULL
		|| !cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_ASSERT_X, assertion, assertion_payload,
			sizeof(assertion_payload), &assertion_bytes, &reject)
		|| assertion_bytes != RESOURCE_X_CONTROL_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	if (local_proof != NULL
		&& (!cluster_resource_x_wire_encode(
				RESOURCE_X_MSG_ASSERT_X, local_proof, proof_payload,
				sizeof(proof_payload), &proof_bytes, &reject)
			|| proof_bytes != RESOURCE_X_SHORT_V1_BYTES))
		return RESOURCE_X_APPLY_INVALID;
	if (!cluster_grd_outbound_enqueue_backend_msg(
			RESOURCE_X_MSG_ASSERT_X, master_node, assertion_payload,
			assertion_bytes))
		return RESOURCE_X_APPLY_BAD_STATE;
	if (local_proof == NULL)
		return RESOURCE_X_APPLY_APPLIED;
	return cluster_grd_outbound_enqueue_backend_msg(
			   RESOURCE_X_MSG_ASSERT_X, master_node, proof_payload,
			   proof_bytes)
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_BAD_STATE;
}

/* The kind-9 ASSERT keeps its wire-neutral observed mode, but an exact local
 * S/X requester must append the frozen R10 LOCAL_PROOF on the same DATA FIFO.
 * Capture is bounded and conditional so the DATA callback never waits for
 * BufferContent; a refusal leaves the round in ASSERT_DISPATCHED for ordinary
 * R7 replay.  Direct-init and clean N rounds emit ASSERT only. */
/* The entry identity and BufferDesc hold are captured in separate lock
 * domains. No remote N delivery becomes visible before both are bound.
 * S keeps the existing ACK/local-proof route and can still drain an older
 * source pair; it is never blanket-pinned here. */
static ResourceXApplyResult
gcs_block_resource_x_delivery_arm_exact(int32 master_node, const ResourceXDecodedFrame *dispatch)
{
	ResourceXInstallClaimJoinObservation observation;
	ResourceXDeliveryTarget target;
	ClusterPcmOwnSnapshot before, after;
	ClusterPcmOwnResult own_result;
	ResourceXApplyResult result;
	int buffer_id = -1;
	uint64 attempt;
	uint64 direct_generation = 0, direct_token = 0;

	result = cluster_pcm_lock_resource_x_delivery_dispatch_observe_exact(
		dispatch, master_node, &observation, &target, &direct_generation, &direct_token);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	attempt = observation.request.assertion_sequence;
	if (target.buffer_id_plus_one != 0) {
		buffer_id = (int)target.buffer_id_plus_one - 1;
		if (buffer_id < 0 || buffer_id >= NBuffers)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		own_result = cluster_bufmgr_pcm_own_delivery_snapshot_exact(
			GetBufferDescriptor(buffer_id), &observation.request.logical_assertion.resource,
			attempt, &after);
		if (own_result != CLUSTER_PCM_OWN_OK)
			return RESOURCE_X_APPLY_STALE;
		return after.generation == target.generation || after.generation == target.generation + 1
				   ? RESOURCE_X_APPLY_APPLIED
				   : RESOURCE_X_APPLY_STALE;
	}
	if (direct_token != 0) {
		/* A real read-miss/extension initializer owns !VALID+IO until its
		 * foreground completes. Ordinary resident lookup deliberately cannot
		 * see it. Reuse its exact known-new proof, never a raw IO-bit bypass. */
		own_result = cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(
			&observation.request.logical_assertion.resource, direct_generation, direct_token,
			&buffer_id, &before);
		if (own_result != CLUSTER_PCM_OWN_OK || buffer_id < 0 || buffer_id >= NBuffers)
			return own_result == CLUSTER_PCM_OWN_CORRUPT ? RESOURCE_X_APPLY_RECOVERY_BLOCKED
														 : RESOURCE_X_APPLY_STALE;
		if ((before.semantic_buf_state & (BM_VALID | BM_IO_IN_PROGRESS)) == BM_IO_IN_PROGRESS) {
			ResourceXInstallClaimJoinObservation current;
			ResourceXDeliveryTarget current_target;
			uint64 current_generation = 0, current_token = 0;

			result = cluster_pcm_lock_resource_x_delivery_dispatch_observe_exact(
				dispatch, master_node, &current, &current_target, &current_generation,
				&current_token);
			if (result != RESOURCE_X_APPLY_APPLIED)
				return result;
			/* Keep its existing pin/IO/error owner, with no delivery hold.
			 * This permits publication only; master proof and actual T1/T2/T3
			 * still follow. No BM_VALID, X or INSTALL is manufactured here. */
			return memcmp(&observation, &current, sizeof(current)) == 0
						   && current_target.buffer_id_plus_one == 0
						   && current_generation == direct_generation
						   && current_token == direct_token
					   ? RESOURCE_X_APPLY_APPLIED
					   : RESOURCE_X_APPLY_STALE;
		}
	} else if (dispatch->kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP && dispatch->common.flags == 0)
		return RESOURCE_X_APPLY_APPLIED;
	else
		own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
			&observation.request.logical_assertion.resource, &buffer_id, &before);
	if (own_result != CLUSTER_PCM_OWN_OK || buffer_id < 0 || buffer_id >= NBuffers)
		return own_result == CLUSTER_PCM_OWN_CORRUPT ? RESOURCE_X_APPLY_RECOVERY_BLOCKED
													 : RESOURCE_X_APPLY_BAD_STATE;
	if (before.pcm_state != PCM_STATE_N)
		return dispatch->kind == RESOURCE_X_WIRE_ASSERT_X
					   && (before.pcm_state == PCM_STATE_S || before.pcm_state == PCM_STATE_X)
				   ? RESOURCE_X_APPLY_APPLIED
				   : RESOURCE_X_APPLY_BAD_STATE;
	/* An ordinary S reader owns its pending grant through image publication
	 * or abort. Only this round's verified direct initializer can delegate
	 * that token. Keep staging pending until the reader releases clean N;
	 * adopting its token would make both reader commit and abort return BUSY.
	 * The hold helper rechecks the complete snapshot under header authority,
	 * so a new reader racing this check also cannot be adopted. */
	if (before.flags == PCM_OWN_FLAG_GRANT_PENDING && direct_token == 0)
		return RESOURCE_X_APPLY_BAD_STATE;
	own_result = cluster_bufmgr_pcm_own_delivery_hold_begin_exact(GetBufferDescriptor(buffer_id),
																  &before, attempt);
	if (own_result != CLUSTER_PCM_OWN_OK)
		return own_result == CLUSTER_PCM_OWN_CORRUPT ? RESOURCE_X_APPLY_RECOVERY_BLOCKED
													 : RESOURCE_X_APPLY_BAD_STATE;
	own_result = cluster_bufmgr_pcm_own_delivery_snapshot_exact(GetBufferDescriptor(buffer_id),
																&before.tag, attempt, &after);
	if (own_result != CLUSTER_PCM_OWN_OK)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	result = cluster_pcm_lock_resource_x_delivery_bind_target_exact(&observation, buffer_id,
																	&before, &after);
	/* Unknown publication/namespace history is not permission to free a
	 * retained target. A refused bind sends nothing and stays fail closed. */
	return result == RESOURCE_X_APPLY_DUPLICATE ? RESOURCE_X_APPLY_APPLIED : result;
}

static ResourceXApplyResult
gcs_block_resource_x_native_assert_stage_exact(
	int32 master_node, const ResourceXDecodedFrame *assertion)
{
	PGAlignedBlock aligned_page;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;
	ClusterPcmOwnResult own_result;
	ClusterBufmgrGcsCopyRefusal copy_refusal
		= CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	ResourceXAcquisitionRef ref;
	ResourceXDecodedFrame local_proof;
	uint64 zero_dependencies[RESOURCE_X_DEPENDENCY_MAX] = { 0 };
	uint64 direct_generation = 0;
	uint64 direct_token = 0;
	uint32 page_checksum;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	SCN page_scn = InvalidScn;
	int before_buffer_id = -1;
	int after_buffer_id = -1;
	ResourceXApplyResult delivery_result;

	if (assertion == NULL
		|| assertion->kind != RESOURCE_X_WIRE_ASSERT_X
		|| assertion->common.observed_mode != (uint8)PCM_STATE_N
		|| assertion->common.target_mode != (uint8)PCM_STATE_X)
		return RESOURCE_X_APPLY_INVALID;
	delivery_result = gcs_block_resource_x_delivery_arm_exact(master_node, assertion);
	if (delivery_result != RESOURCE_X_APPLY_APPLIED)
		return delivery_result;
	memset(&ref, 0, sizeof(ref));
	ref.assertion = assertion->common.logical_assertion;
	ref.formation = assertion->common.resource_formation;
	ref.acquisition_generation = assertion->common.assertion_sequence;
	if (cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
			&ref, &direct_generation, &direct_token)) {
		own_result
			= cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(
				&ref.assertion.resource, direct_generation, direct_token,
				&before_buffer_id, &before);
		if (own_result != CLUSTER_PCM_OWN_OK || before_buffer_id < 0)
			return own_result == CLUSTER_PCM_OWN_BUSY
				? RESOURCE_X_APPLY_BAD_STATE
				: own_result == CLUSTER_PCM_OWN_STALE
				? RESOURCE_X_APPLY_STALE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		own_result = cluster_bufmgr_pcm_own_n_direct_init_candidate_exact(
			GetBufferDescriptor(before_buffer_id), &before);
		if (own_result != CLUSTER_PCM_OWN_OK)
			return own_result == CLUSTER_PCM_OWN_BUSY
				? RESOURCE_X_APPLY_BAD_STATE
				: own_result == CLUSTER_PCM_OWN_STALE
				? RESOURCE_X_APPLY_STALE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		return gcs_block_resource_x_assert_stage_exact(
			master_node, assertion, NULL);
	}

	own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
		&assertion->common.logical_assertion.resource,
		&before_buffer_id, &before);
	if (own_result != CLUSTER_PCM_OWN_OK || before_buffer_id < 0)
		return own_result == CLUSTER_PCM_OWN_BUSY
			? RESOURCE_X_APPLY_BAD_STATE
			: own_result == CLUSTER_PCM_OWN_STALE
			? RESOURCE_X_APPLY_STALE
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (!BufferTagsEqual(&before.tag,
			&assertion->common.logical_assertion.resource)
		|| before.flags != 0
		|| before.writer_activation_token != 0
		|| before.resource_x_activation_generation != 0
		|| before.generation == UINT64_MAX)
		return RESOURCE_X_APPLY_STALE;
	if (before.pcm_state == (uint8)PCM_STATE_N) {
		own_result = cluster_bufmgr_pcm_own_n_assertion_candidate_exact(
			GetBufferDescriptor(before_buffer_id), &before, NULL);
		if (own_result != CLUSTER_PCM_OWN_OK)
			return own_result == CLUSTER_PCM_OWN_BUSY
				? RESOURCE_X_APPLY_BAD_STATE
				: own_result == CLUSTER_PCM_OWN_STALE
				? RESOURCE_X_APPLY_STALE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		return gcs_block_resource_x_assert_stage_exact(
			master_node, assertion, NULL);
	}
	if (before.generation == 0
		|| (before.pcm_state != (uint8)PCM_STATE_S
			&& before.pcm_state != (uint8)PCM_STATE_X))
		return RESOURCE_X_APPLY_STALE;
	if (!cluster_bufmgr_copy_block_for_r4_cr(
			before.tag, InvalidScn, &page_lsn, &page_scn,
			aligned_page.data, &copy_refusal))
		return RESOURCE_X_APPLY_BAD_STATE;
	own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
		&assertion->common.logical_assertion.resource,
		&after_buffer_id, &after);
	if (own_result != CLUSTER_PCM_OWN_OK
		|| before_buffer_id != after_buffer_id
		|| memcmp(&before, &after, sizeof(before)) != 0
		|| PageGetLSN((Page)aligned_page.data) != page_lsn
		|| ((PageHeader)aligned_page.data)->pd_block_scn != page_scn)
		return own_result == CLUSTER_PCM_OWN_CORRUPT
			? RESOURCE_X_APPLY_RECOVERY_BLOCKED
			: RESOURCE_X_APPLY_BAD_STATE;
	page_checksum = cluster_gcs_block_compute_checksum(aligned_page.data);
	memset(&local_proof, 0, sizeof(local_proof));
	local_proof.kind = RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION;
	local_proof.payload_bytes = RESOURCE_X_SHORT_V1_BYTES;
	local_proof.common = assertion->common;
	local_proof.common.observed_mode = before.pcm_state;
	local_proof.common.outcome = RESOURCE_X_OUTCOME_OK;
	local_proof.body.local_proof.local_holder_authority_generation
		= before.generation;
	local_proof.body.local_proof.requester_target_generation
		= assertion->common.assertion_sequence;
	local_proof.body.local_proof.page_scn_lsn = (uint64)page_scn;
	local_proof.body.local_proof.dependency_count = 0;
	local_proof.body.local_proof.dependency_vector_crc32c
		= gcs_block_pcm_x_resource_x_dependency_crc(zero_dependencies);
	local_proof.body.local_proof.page_checksum = page_checksum;
	local_proof.body.local_proof.local_image_proof_crc32c = page_checksum;
	local_proof.body.local_proof.requester_connection_generation
		= assertion->common.sender_connection_generation;
	local_proof.body.local_proof.local_proof_generation = before.generation;
	return gcs_block_resource_x_assert_stage_exact(
		master_node, assertion, &local_proof);
}

/* PGRAC adaptation: requester round extraction has already released the
 * resource entry lock.  Encode and stage the type-14 bootstrap request only
 * from that frozen copy. */
static bool
gcs_block_resource_x_bootstrap_request_stage_exact(
	int32 master_node, const ResourceXDecodedFrame *request)
{
	uint8 payload[RESOURCE_X_CONTROL_V1_BYTES];
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint16 payload_bytes = 0;

	if (master_node < 0 || master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || request == NULL
		|| gcs_block_resource_x_delivery_arm_exact(master_node, request) != RESOURCE_X_APPLY_APPLIED
		|| !cluster_resource_x_wire_encode(RESOURCE_X_MSG_ASSERT_X, request, payload,
										   sizeof(payload), &payload_bytes, &reject)
		|| payload_bytes != RESOURCE_X_CONTROL_V1_BYTES)
		return false;
	return cluster_grd_outbound_enqueue_backend_msg(
		RESOURCE_X_MSG_ASSERT_X, (uint32)master_node, payload,
		payload_bytes);
}

/* PGRAC adaptation: the master receipt API has already copied the exact ACK
 * semantic snapshot and released the resource entry lock.  Only then may the
 * DATA-plane producer encode and enqueue the existing type-15 frame. */
static bool
gcs_block_resource_x_bootstrap_ack_stage_exact(
	int32 requester_node, const ResourceXDecodedFrame *ack)
{
	uint8 payload[RESOURCE_X_CONTROL_V1_BYTES];
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint16 payload_bytes = 0;

	if (requester_node < 0
		|| requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| ack == NULL
		|| !cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, ack, payload, sizeof(payload),
			&payload_bytes, &reject)
		|| payload_bytes != RESOURCE_X_CONTROL_V1_BYTES)
		return false;
	return cluster_grd_outbound_enqueue_backend_msg(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, (uint32)requester_node, payload,
		payload_bytes);
}

/* A READY probe is still only pre-T1 semantic evidence.  Ownership mutation
 * is intentionally absent above and begins only in the terminal helper. */

/* Build one proof-bound status/image pair from the already fenced X source.
 * requester_target_generation is the user-approved Option-A mapping: the
 * exact assertion sequence, with no ASSERT_X/BLOCK_TO_N ABI extension. */
static bool
gcs_block_pcm_x_resource_x_build_source_frames(
	const ResourceXDecodedFrame *block,
	const ClusterPcmOwnSnapshot *revoking, const char page_bytes[BLCKSZ],
	XLogRecPtr page_lsn, uint64 page_scn, uint32 requester_connection_generation,
	uint64 source_boot_incarnation, uint8 source_mode,
	ResourceXDecodedFrame *status,
	ResourceXDecodedFrame *image)
{
	ResourceXDecodedFrame canonical_image;
	ResourceXDecodedBlockedToN *blocked_body;
	ResourceXDecodedImageEnvelope *image_body;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 encoded_image[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 encoded_bytes = 0;
	uint64 source_carrier_generation;

	if (block == NULL || revoking == NULL || page_bytes == NULL
		|| status == NULL || image == NULL
		|| requester_connection_generation == 0
		|| source_boot_incarnation == 0
		|| revoking->generation == 0
		|| revoking->generation == UINT64_MAX
		|| revoking->reservation_token == 0
		|| revoking->flags != PCM_OWN_FLAG_REVOKING
		|| (source_mode != (uint8)PCM_STATE_X
			&& source_mode != (uint8)PCM_STATE_S)
		|| revoking->pcm_state != source_mode
		|| PageGetLSN((Page)page_bytes) != page_lsn)
		return false;
	source_carrier_generation = revoking->generation + 1;
	memset(status, 0, sizeof(*status));
	memset(image, 0, sizeof(*image));

	image->kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	image->payload_bytes = RESOURCE_X_IMAGE_V1_BYTES;
	image->common = block->common;
	image->common.action_node
		= block->common.logical_assertion.requester_node;
	image->common.observed_mode = source_mode;
	image->common.target_mode = (uint8)PCM_STATE_X;
	image->common.source_candidate = 0;
	image->common.retain_pi_if_dirty = 0;
	image->common.sender_connection_generation
		= requester_connection_generation;
	image->common.outcome = RESOURCE_X_OUTCOME_OK;
	image->common.flags = block->common.flags;
	image->common.authority_generation
		= (block->common.flags & RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE) != 0
			  ? block->common.authority_generation
			  : block->common.base_authority_generation + 1;
	image_body = &image->body.image_envelope;
	gcs_block_resource_x_put_u64(image_body->request_tail,
		(uint64)requester_connection_generation);
	gcs_block_resource_x_put_u64(image_body->request_tail + 8,
		block->common.assertion_sequence);
	image_body->conversion_base_generation
		= block->common.base_authority_generation;
	gcs_block_resource_x_put_u32(image_body->source_fence,
		(uint32)cluster_node_id);
	gcs_block_resource_x_put_u64(image_body->source_fence + 4,
		source_boot_incarnation);
	gcs_block_resource_x_put_u64(image_body->source_fence + 12,
		(uint64)requester_connection_generation);
	gcs_block_resource_x_put_u64(image_body->source_fence + 20,
		revoking->generation);
	image_body->source_fence[28] = source_mode;
	image_body->source_fence[29] = 1;
	image_body->source_carrier_generation = source_carrier_generation;
	image_body->requester_target_generation = block->common.assertion_sequence;
	image_body->page_scn_lsn = page_scn;
	image_body->dependency_count = 0;
	image_body->dependency_vector_crc32c
		= gcs_block_pcm_x_resource_x_dependency_crc(
			image_body->dependencies);
	image_body->page_checksum
		= cluster_gcs_block_compute_checksum(page_bytes);
	image_body->image_length = BLCKSZ;
	image_body->source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	image_body->proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	memcpy(image_body->page_bytes, page_bytes, BLCKSZ);
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, image, encoded_image,
			sizeof(encoded_image), &encoded_bytes, &reject)
		|| encoded_bytes != RESOURCE_X_IMAGE_V1_BYTES
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, encoded_image, encoded_bytes,
			&canonical_image, &reject)
		|| canonical_image.kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| canonical_image.common.semantic_crc32c == 0)
		return false;
	*image = canonical_image;
	image_body = &image->body.image_envelope;

	status->kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	status->payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	status->blocked_has_remote_proof = true;
	status->common = block->common;
	status->common.action_node = cluster_node_id;
	status->common.source_candidate = 0;
	status->common.retain_pi_if_dirty = 0;
	status->common.outcome = RESOURCE_X_OUTCOME_OK;
	status->common.flags = RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED;
	status->common.authority_generation
		= block->common.base_authority_generation;
	blocked_body = &status->body.blocked_to_n;
	memcpy(blocked_body->source_fence, image_body->source_fence,
		sizeof(blocked_body->source_fence));
	blocked_body->source_carrier_generation
		= image_body->source_carrier_generation;
	blocked_body->requester_target_generation = block->common.assertion_sequence;
	blocked_body->page_scn_lsn = image_body->page_scn_lsn;
	blocked_body->dependency_count = image_body->dependency_count;
	memcpy(blocked_body->dependencies, image_body->dependencies,
		sizeof(blocked_body->dependencies));
	blocked_body->source_proof_crc32c = image->common.semantic_crc32c;
	blocked_body->page_checksum = image_body->page_checksum;
	blocked_body->source_disposition
		= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	blocked_body->proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	blocked_body->holder_connection_generation
		= block->common.sender_connection_generation;
	blocked_body->acting_formation = block->common.resource_formation;
	return true;
}

static bool
gcs_block_pcm_x_resource_x_abort_pre_arm(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *revoking)
{
	ClusterPcmOwnResult abort_result;

	if (revoking != NULL
		&& revoking->pcm_state == (uint8)PCM_STATE_S)
		abort_result = cluster_bufmgr_pcm_own_abort_s_revoke(buf, revoking);
	else if (revoking != NULL
			 && revoking->pcm_state == (uint8)PCM_STATE_X)
		abort_result = cluster_bufmgr_pcm_own_abort_x_revoke(buf, revoking);
	else
		abort_result = CLUSTER_PCM_OWN_CORRUPT;
	if (abort_result != CLUSTER_PCM_OWN_OK) {
		gcs_block_resource_x_fail_closed_current();
		return false;
	}
	return true;
}

static bool
gcs_block_resource_x_terminal_owner_release(
	ResourceXLocalOwnerHandle *handle, bool *held)
{
	ResourceXApplyResult result;

	if (held == NULL || !*held)
		return true;
	result = cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
		handle);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		gcs_block_resource_x_fail_closed_current();
		return false;
	}
	memset(handle, 0, sizeof(*handle));
	*held = false;
	return true;
}

/* The source fence is established before copying.  Before L3 it is the local
 * tag gate; after L3 an exact terminal-X lineage plus a caller-held raw pin
 * and REVOKING token cover the same window.  The retained status and image
 * are committed atomically before source->N.  A post-arm failure never aborts
 * or reopens X; the target-only tagless branch cannot adopt a prior call's
 * untracked pin and therefore fails closed instead of retrying that finish. */
static ResourceXApplyResult
gcs_block_pcm_x_resource_x_source_block_to_n(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	uint64 r4_record_generation)
{
	MemoryContext error_context = CurrentMemoryContext;
	PGAlignedBlock aligned_page;
	BufferDesc *buf;
	ClusterPcmOwnFinishRefusal finish_refusal;
	ClusterPcmOwnHeldXRevoke held_x_revoke;
	ClusterPcmOwnResult own_result;
	ClusterPcmOwnSourcePrepareRefusal source_prepare_refusal;
	ClusterPcmOwnSnapshot current;
	ClusterPcmOwnSnapshot retained;
	ClusterPcmOwnSnapshot revoking;
	ResourceXGateSnapshot resource_gate;
	ResourceXLocalOwnerHandle target_revoke_owner;
	ResourceXTerminalXLineage lineage;
	ResourceXTerminalXLineage revalidated_lineage;
	ClusterPcmXRevokeFinishMode target_x_finish_mode
		= CLUSTER_PCM_X_REVOKE_FINISH_INVALID;
	ResourceXApplyResult failure_result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	ResourceXApplyResult image_result = RESOURCE_X_APPLY_NOT_FOUND;
	ResourceXApplyResult pair_result = RESOURCE_X_APPLY_NOT_FOUND;
	ResourceXApplyResult result;
	ResourceXApplyResult status_result = RESOURCE_X_APPLY_NOT_FOUND;
	ResourceXApplyResult replay_result = RESOURCE_X_APPLY_NOT_FOUND;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	uint32 capability_word = 0;
	uint32 requester_connection_generation = 0;
	uint64 source_boot_incarnation;
	uint64 resource_master_session = 0;
	uint64 page_scn = 0;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	uint64 terminal_holder_generation = 0;
	uint64 writer_r4_generation = 0;
	int buffer_id = -1;
	int32 event_owner_identity = -1;
	int32 resource_master_node = -1;
	uint8 source_mode;
	volatile ClusterPcmOwnResult finish_result = CLUSTER_PCM_OWN_INVALID;
	ResourceXWriterPath writer_path;
	bool carrier_superseded = false;
	bool finish_required = true;
	volatile bool held_x_revoke_active = false;
	bool semantic_retained = false;
	bool shared_s_source;
	bool tagless_target_x = false;
	bool target_x_drop = false;
	bool target_x_retain = false;
	bool target_revoke_owner_held = false;
	bool revoke_started = false;
	const char *failure_stage = "entry";

	if (block == NULL)
		return RESOURCE_X_APPLY_INVALID;
	memset(&held_x_revoke, 0, sizeof(held_x_revoke));
	memset(&target_revoke_owner, 0, sizeof(target_revoke_owner));
	memset(&lineage, 0, sizeof(lineage));
	memset(&revalidated_lineage, 0, sizeof(revalidated_lineage));
	memset(&current, 0, sizeof(current));
	source_mode = block->common.observed_mode;
	shared_s_source = source_mode == (uint8)PCM_STATE_S;
	if (block->kind != RESOURCE_X_WIRE_BLOCK_TO_N
		|| (source_mode != (uint8)PCM_STATE_X
			&& source_mode != (uint8)PCM_STATE_S)
		|| block->common.target_mode != (uint8)PCM_STATE_N
		|| block->common.source_candidate != 1
		|| block->common.retain_pi_if_dirty != 1
		|| block->common.outcome != RESOURCE_X_OUTCOME_NONE)
		return RESOURCE_X_APPLY_INVALID;
	/* Resource-X formation and the local BufferDesc owner are distinct
	 * monotone domains.  Authenticate the wire domain before touching the
	 * descriptor; the exact REVOKING token serializes the local copy window. */
	if (!gcs_block_resource_x_gate_session_snapshot(
			&block->common.logical_assertion.resource, &resource_gate,
			&resource_master_node, &resource_master_session)
		|| resource_gate.formation != block->common.resource_formation
		|| resource_master_node != authenticated_master_node
		|| resource_master_session
			!= block->common.master_session_incarnation) {
		failure_stage = "gate-session";
		failure_result = RESOURCE_X_APPLY_STALE;
		goto pre_retained_failure;
	}
	status_result = cluster_pcm_lock_resource_x_holder_status_exact(
		&block->common.logical_assertion, &status);
	if (status_result == RESOURCE_X_APPLY_APPLIED) {
		image_result = cluster_pcm_lock_resource_x_holder_image_exact(
			&block->common.logical_assertion, &image);
		if (image_result != RESOURCE_X_APPLY_APPLIED) {
			failure_stage = "retained-image";
			failure_result = image_result == RESOURCE_X_APPLY_NOT_FOUND
				? RESOURCE_X_APPLY_RECOVERY_BLOCKED : image_result;
			goto pre_retained_failure;
		}
		/* The retained pair's monotonic publication witness separates an exact
		 * PENDING finish retry from a type-17 replay after both intents were
		 * atomically published.  Independent DATA drain may make the two current
		 * slots asymmetric; never re-enter publish or recreate a completed half.
		 * A strictly older same-domain pair is not evidence for this attempt: build
		 * fresh frames and let block_to_n_source_exact enforce its DRAIN tombstone
		 * and newer-carrier replacement contract. */
		pair_result
			= cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
				&block->common.logical_assertion,
				block->common.assertion_sequence,
				authenticated_master_node,
				block->common.master_session_incarnation);
		if (pair_result == RESOURCE_X_APPLY_DUPLICATE)
			return RESOURCE_X_APPLY_DUPLICATE;
		if (pair_result == RESOURCE_X_APPLY_APPLIED)
			semantic_retained = true;
		else if (pair_result != RESOURCE_X_APPLY_NOT_FOUND) {
			failure_stage = "retained-pair-domain";
			failure_result = pair_result;
			goto pre_retained_failure;
		}
	} else if (status_result != RESOURCE_X_APPLY_NOT_FOUND
		&& status_result != RESOURCE_X_APPLY_STALE)
		return status_result;
	writer_path = cluster_resource_x_writer_path_snapshot(
		&writer_r4_generation);
	if (writer_path != RESOURCE_X_WRITER_TARGET
		|| writer_r4_generation == 0
		|| writer_r4_generation == UINT64_MAX) {
		failure_stage = "writer-path";
		failure_result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pre_retained_failure;
	}
	tagless_target_x = source_mode == (uint8)PCM_STATE_X;
	if (tagless_target_x) {
		target_x_finish_mode =
			cluster_pcm_x_revoke_finish_mode(
				&block->common.logical_assertion.resource, 0);
		target_x_retain = target_x_finish_mode
			== CLUSTER_PCM_X_REVOKE_FINISH_RETAIN;
		target_x_drop = target_x_finish_mode
			== CLUSTER_PCM_X_REVOKE_FINISH_DROP;
		if (!target_x_retain && !target_x_drop) {
			failure_stage = "target-x-finish-mode";
			failure_result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			goto pre_retained_failure;
		}
	}

	failure_stage = "own-snapshot";
	own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
		&block->common.logical_assertion.resource, &buffer_id, &current);
	if (own_result != CLUSTER_PCM_OWN_OK || buffer_id < 0) {
		if (semantic_retained) {
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		failure_result = own_result == CLUSTER_PCM_OWN_BUSY
			? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pre_retained_failure;
	}
	buf = GetBufferDescriptor(buffer_id);
	if (tagless_target_x) {
		failure_stage = "terminal-lineage";
		if (semantic_retained) {
			if (image.body.image_envelope.source_carrier_generation == 0)
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			terminal_holder_generation
				= image.body.image_envelope.source_carrier_generation - 1;
		} else
			terminal_holder_generation = current.generation;
		if (!cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(
				block, resource_master_node, writer_r4_generation,
				terminal_holder_generation, &lineage)) {
			failure_result = RESOURCE_X_APPLY_STALE;
			goto pre_retained_failure;
		}
	}
	if (semantic_retained) {
		if (current.pcm_state == (uint8)PCM_STATE_N
			&& current.flags == PCM_OWN_FLAG_REVOKING
			&& current.generation
				== image.body.image_envelope.source_carrier_generation)
			finish_required = false;
		else if (current.generation > image.body.image_envelope.source_carrier_generation) {
			/* The exact old carrier has already finished and a later local
			 * acquisition advanced the same BufferDesc incarnation.  Rearm only
			 * the retained pair below; never apply the old revoke to current X. */
			carrier_superseded = true;
			finish_required = false;
		}
		else if (current.pcm_state != source_mode
			|| current.flags != PCM_OWN_FLAG_REVOKING
			|| current.generation == UINT64_MAX
			|| current.generation + 1
				!= image.body.image_envelope.source_carrier_generation) {
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		} else {
			/* A retained tagless callback cannot adopt the raw pin owned by the
			 * callback that first claimed REVOKING.  An exact same-successor
			 * replay is bounded pre-mutation backpressure; the original owner
			 * remains solely responsible for finishing. */
			if (tagless_target_x) {
				memset(&revalidated_lineage, 0,
					   sizeof(revalidated_lineage));
				replay_result
					= cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
						block, resource_master_node, writer_r4_generation,
						terminal_holder_generation,
						gcs_block_pcm_x_monotonic_us(),
						&revalidated_lineage);
				if (replay_result == RESOURCE_X_APPLY_BAD_STATE)
					return RESOURCE_X_APPLY_BAD_STATE;
				if (replay_result == RESOURCE_X_APPLY_STALE)
					return RESOURCE_X_APPLY_STALE;
				gcs_block_resource_x_fail_closed_current();
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			revoking = current;
			page_lsn = PageGetLSN(
				(Page)image.body.image_envelope.page_bytes);
			page_scn = image.body.image_envelope.page_scn_lsn;
		}
	} else {
		if (current.pcm_state != source_mode || current.flags != 0
			|| current.generation == 0 || current.generation == UINT64_MAX
			|| block->common.base_authority_generation == UINT64_MAX
			|| block->common.logical_assertion.requester_node == cluster_node_id) {
			failure_result = RESOURCE_X_APPLY_BAD_STATE;
			goto pre_retained_failure;
		}
		failure_stage = "source-peer";
		if (!cluster_sf_peer_capability_word_sample(
				block->common.logical_assertion.requester_node,
				PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
				&capability_word, &requester_connection_generation)) {
			failure_result = RESOURCE_X_APPLY_BAD_STATE;
			goto pre_retained_failure;
		}
	}
	if (!semantic_retained) {
		source_boot_incarnation = cluster_qvotec_get_self_incarnation();
		failure_stage = "source-incarnation";
		if (source_boot_incarnation == 0)
			goto pre_retained_failure;
		failure_stage = "source-gate-recheck";
		if (!gcs_block_resource_x_gate_session_recheck(
				&block->common.logical_assertion.resource, &resource_gate,
				resource_master_node, resource_master_session)) {
			failure_result = RESOURCE_X_APPLY_STALE;
			goto pre_retained_failure;
		}
		memset(&source_prepare_refusal, 0, sizeof(source_prepare_refusal));
		failure_stage = "source-prepare";
		if (shared_s_source)
			own_result = cluster_bufmgr_pcm_own_prepare_s_source_image(
				buf, &current, (SCN)0, &revoking, aligned_page.data,
				&page_lsn, &page_scn, &source_prepare_refusal);
		else if (tagless_target_x) {
			ResourceXApplyResult owner_result;

			failure_stage = "terminal-owner-claim";
			event_owner_identity
				= cluster_gcs_resource_x_event_owner_identity(
					MyProc != NULL ? (int32)MyProc->pgprocno : -1,
					(int32)getpid());
			if (event_owner_identity < 0) {
				failure_result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
				goto pre_retained_failure;
			}
			memset(&revalidated_lineage, 0,
				   sizeof(revalidated_lineage));
			owner_result
				= cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
					block, resource_master_node, writer_r4_generation,
					terminal_holder_generation,
					current.reservation_token, event_owner_identity,
					gcs_block_pcm_x_monotonic_us(),
					&revalidated_lineage, &target_revoke_owner);
			if (owner_result == RESOURCE_X_APPLY_APPLIED)
				target_revoke_owner_held = true;
			if (owner_result != RESOURCE_X_APPLY_APPLIED
				|| memcmp(&lineage, &revalidated_lineage,
					sizeof(lineage)) != 0) {
				failure_result = owner_result
					== RESOURCE_X_APPLY_BAD_STATE
					? RESOURCE_X_APPLY_BAD_STATE
					: RESOURCE_X_APPLY_STALE;
				goto pre_retained_failure;
			}
			if (target_x_retain) {
				own_result
					= cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(
						&block->common.logical_assertion.resource, &current,
						&held_x_revoke);
				if (own_result == CLUSTER_PCM_OWN_OK) {
					revoking = held_x_revoke.revoking;
					held_x_revoke_active = true;
				}
			}
			else if (target_x_drop)
				own_result = cluster_bufmgr_pcm_own_begin_x_revoke(
					buf, &current, &revoking);
			else
				own_result = CLUSTER_PCM_OWN_CORRUPT;
		}
		else
			own_result = cluster_bufmgr_pcm_own_begin_x_revoke(
				buf, &current, &revoking);
		if (own_result != CLUSTER_PCM_OWN_OK) {
			failure_result = own_result == CLUSTER_PCM_OWN_BUSY
				? RESOURCE_X_APPLY_BAD_STATE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			goto pre_retained_failure;
		}
		revoke_started = true;
	}
	if (tagless_target_x) {
		uint64 current_writer_generation = 0;

		failure_stage = "terminal-lineage-recheck";
		writer_path = cluster_resource_x_writer_path_snapshot(
			&current_writer_generation);
		memset(&revalidated_lineage, 0, sizeof(revalidated_lineage));
		if (writer_path != RESOURCE_X_WRITER_TARGET
			|| current_writer_generation != writer_r4_generation
			|| !target_revoke_owner_held
			|| !cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
				block, resource_master_node, writer_r4_generation,
				terminal_holder_generation, &target_revoke_owner,
				&revalidated_lineage)
			|| memcmp(&lineage, &revalidated_lineage,
				sizeof(lineage)) != 0) {
			failure_result = RESOURCE_X_APPLY_STALE;
			goto pre_retained_failure;
		}
	}
	failure_stage = "source-final-gate";
	if (!gcs_block_resource_x_gate_session_recheck(
			&block->common.logical_assertion.resource, &resource_gate,
			resource_master_node, resource_master_session)) {
		if (semantic_retained) {
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		failure_result = RESOURCE_X_APPLY_STALE;
		goto pre_retained_failure;
	}
	if (tagless_target_x && target_x_retain && !semantic_retained) {
		ResourceXApplyResult yield_result;

		failure_stage = "terminal-content-drain";
		own_result
			= cluster_bufmgr_pcm_own_try_drain_held_x_revoke(
				&held_x_revoke);
		if (own_result == CLUSTER_PCM_OWN_BUSY) {
			/* Do not wait in LMON: the pre-existing content-S holder may be
			 * waiting for this same event loop to deliver a visibility reply.
			 * No bytes or retained intent have been published, so restore X
			 * and return the exact observed priority to HANDOFF. */
			own_result = cluster_bufmgr_pcm_own_abort_held_x_revoke(
				&held_x_revoke);
			if (own_result != CLUSTER_PCM_OWN_OK) {
				gcs_block_resource_x_fail_closed_current();
				(void)cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(
					&held_x_revoke);
				(void)gcs_block_resource_x_terminal_owner_release(
					&target_revoke_owner, &target_revoke_owner_held);
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			held_x_revoke_active = false;
			revoke_started = false;
			yield_result
				= cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
					&target_revoke_owner,
					gcs_block_pcm_x_monotonic_us());
			if (yield_result != RESOURCE_X_APPLY_APPLIED) {
				gcs_block_resource_x_fail_closed_current();
				if (target_revoke_owner_held)
					(void)cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
						&target_revoke_owner);
				target_revoke_owner_held = false;
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			memset(&target_revoke_owner, 0,
				   sizeof(target_revoke_owner));
			target_revoke_owner_held = false;
			return RESOURCE_X_APPLY_BAD_STATE;
		}
		if (own_result != CLUSTER_PCM_OWN_OK) {
			failure_result = own_result == CLUSTER_PCM_OWN_STALE
				? RESOURCE_X_APPLY_STALE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			goto pre_retained_failure;
		}
	}
	if (tagless_target_x && target_x_drop && !semantic_retained) {
		ResourceXApplyResult yield_result;

		failure_stage = "terminal-aux-pin-drain";
		own_result = cluster_bufmgr_pcm_own_try_drain_drop_x_revoke(
			buf, &revoking);
		if (own_result == CLUSTER_PCM_OWN_BUSY) {
			/* REVOKING already refuses every new passive VM/FSM first pin.
			 * A nonzero refcount therefore names only predecessors.  Do not
			 * wait in DATA/LMON or publish the pair: restore exact X and yield
			 * the existing terminal owner to the same HANDOFF identity. */
			own_result = cluster_bufmgr_pcm_own_abort_x_revoke(
				buf, &revoking);
			if (own_result != CLUSTER_PCM_OWN_OK) {
				gcs_block_resource_x_fail_closed_current();
				(void)gcs_block_resource_x_terminal_owner_release(
					&target_revoke_owner, &target_revoke_owner_held);
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			revoke_started = false;
			yield_result
				= cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
					&target_revoke_owner,
					gcs_block_pcm_x_monotonic_us());
			if (yield_result != RESOURCE_X_APPLY_APPLIED) {
				gcs_block_resource_x_fail_closed_current();
				if (target_revoke_owner_held)
					(void)cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
						&target_revoke_owner);
				target_revoke_owner_held = false;
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			memset(&target_revoke_owner, 0,
				   sizeof(target_revoke_owner));
			target_revoke_owner_held = false;
			return RESOURCE_X_APPLY_BAD_STATE;
		}
		if (own_result != CLUSTER_PCM_OWN_OK) {
			failure_result = own_result == CLUSTER_PCM_OWN_STALE
				? RESOURCE_X_APPLY_STALE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			goto pre_retained_failure;
		}
	}
	if (!semantic_retained) {
		failure_stage = "source-copy";
		if (!shared_s_source
			&& !cluster_bufmgr_copy_block_for_gcs(
				block->common.logical_assertion.resource, &page_lsn,
				aligned_page.data, NULL)) {
			failure_result = RESOURCE_X_APPLY_BAD_STATE;
			goto pre_retained_failure;
		}
		if (!shared_s_source)
			page_scn = (uint64)((PageHeader)aligned_page.data)->pd_block_scn;
		failure_stage = "source-frame-build";
		if (!gcs_block_pcm_x_resource_x_build_source_frames(
				block, &revoking, aligned_page.data, page_lsn, page_scn,
				requester_connection_generation, source_boot_incarnation,
				source_mode,
				&status, &image)) {
			failure_result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			goto pre_retained_failure;
		}
	}
	failure_stage = "retained-pair-apply";
	if (shared_s_source && semantic_retained && !finish_required)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (shared_s_source)
		result
			= cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(
				block, authenticated_master_node, &status, &image, &revoking,
				page_lsn, page_scn,
				image.body.image_envelope.page_checksum);
	else if (tagless_target_x && target_x_drop)
		result
			= cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
				block, authenticated_master_node, &status, &image, &revoking,
				&target_revoke_owner);
	else if (tagless_target_x && target_x_retain)
		result
			= cluster_pcm_lock_resource_x_block_to_n_prepared_x_source_exact(
				block, authenticated_master_node, &status, &image, &revoking,
				&target_revoke_owner);
	else
		result = cluster_pcm_lock_resource_x_block_to_n_source_exact(
			block, authenticated_master_node, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED
		&& result != RESOURCE_X_APPLY_DUPLICATE) {
		if (semantic_retained && carrier_superseded
			&& result == RESOURCE_X_APPLY_STALE) {
			pair_result
				= cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
					&block->common.logical_assertion,
					block->common.assertion_sequence,
					authenticated_master_node,
					block->common.master_session_incarnation);
			if (pair_result == RESOURCE_X_APPLY_APPLIED) {
				return RESOURCE_X_APPLY_STALE;
			}
		}
		if (semantic_retained) {
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		failure_result = result;
		goto pre_retained_failure;
	}
	semantic_retained = true;
	if (!finish_required) {
		pair_result
			= cluster_pcm_lock_resource_x_holder_pair_publish_exact(
				&block->common.logical_assertion,
				block->common.assertion_sequence,
				authenticated_master_node,
				block->common.master_session_incarnation);
		if (pair_result != RESOURCE_X_APPLY_APPLIED
			&& pair_result != RESOURCE_X_APPLY_DUPLICATE) {
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		if (!gcs_block_resource_x_terminal_owner_release(
				&target_revoke_owner, &target_revoke_owner_held)) {
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		return result;
	}

	memset(&finish_refusal, 0, sizeof(finish_refusal));
	memset(&retained, 0, sizeof(retained));
	PG_TRY();
	{
		if (held_x_revoke_active) {
			finish_result
				= cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(
					&held_x_revoke, page_lsn, &retained,
					&finish_refusal);
			if (finish_result == CLUSTER_PCM_OWN_OK)
				held_x_revoke_active = false;
		} else
			finish_result = cluster_bufmgr_pcm_own_finish_revoke_retain(
				buf, &revoking, page_lsn, &retained, &finish_refusal);
	}
	PG_CATCH();
	{
		ErrorData *original_error;
		bool pair_retained;

		MemoryContextSwitchTo(error_context);
		original_error = CopyErrorData();
		FlushErrorState();
		status_result = cluster_pcm_lock_resource_x_holder_status_exact(
			&block->common.logical_assertion, &status);
		image_result = cluster_pcm_lock_resource_x_holder_image_exact(
			&block->common.logical_assertion, &image);
		pair_retained = status_result == RESOURCE_X_APPLY_APPLIED
			&& image_result == RESOURCE_X_APPLY_APPLIED
			&& status.common.assertion_sequence
				   == block->common.assertion_sequence
			&& image.common.assertion_sequence
				   == block->common.assertion_sequence
			&& status.body.blocked_to_n.source_proof_crc32c
				   == image.common.semantic_crc32c;
		ereport(
			LOG,
			(errmsg_internal("PCM-X Resource-X finish-error evidence exact"),
			 errdetail("retained=%s tag=%u/%u/%u/%d/%u requester=%d "
					   "assertion_sequence=%llu base=%llu formation=%llu "
					   "master_session=%llu source_generation=%llu "
					   "reservation_token=%llu source_state=%u",
					   pair_retained ? "true" : "false",
					   block->common.logical_assertion.resource.spcOid,
					   block->common.logical_assertion.resource.dbOid,
					   block->common.logical_assertion.resource.relNumber,
					   (int)block->common.logical_assertion.resource.forkNum,
					   block->common.logical_assertion.resource.blockNum,
					   block->common.logical_assertion.requester_node,
					   (unsigned long long)block->common.assertion_sequence,
					   (unsigned long long)block->common.base_authority_generation,
					   (unsigned long long)block->common.resource_formation,
					   (unsigned long long)block->common.master_session_incarnation,
					   (unsigned long long)revoking.generation,
					   (unsigned long long)revoking.reservation_token,
					   (unsigned int)revoking.pcm_state)));
		gcs_block_resource_x_fail_closed_current();
		if (held_x_revoke_active) {
			(void)cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(
				&held_x_revoke);
			held_x_revoke_active = false;
		}
		ereport(LOG,
				(errmsg_internal("cluster PCM-X Resource-X source finish "
								 "FlushBuffer failed; preserved pending pair and "
								 "blocked recovery: %s",
								 original_error->message != NULL
									 ? original_error->message : "(no message)")));
		FreeErrorData(original_error);
		finish_result = CLUSTER_PCM_OWN_CORRUPT;
	}
	PG_END_TRY();
	if (finish_result != CLUSTER_PCM_OWN_OK
		|| retained.pcm_state != (uint8)PCM_STATE_N
		|| retained.generation
			!= image.body.image_envelope.source_carrier_generation) {
		ereport(
			LOG,
			(errmsg_internal("Resource-X type-17 finish diagnostic"),
			 errdetail("result=%d retained_state=%u retained_generation=%llu "
					   "expected_generation=%llu refusal_reason=%u refcount=%u "
					   "io=%s live_flags=%u live_token=%llu held=%s tagless=%s",
					   (int)finish_result,
					   (unsigned)retained.pcm_state,
					   (unsigned long long)retained.generation,
					   (unsigned long long)image.body.image_envelope
						   .source_carrier_generation,
					   (unsigned)finish_refusal.reason,
					   (unsigned)finish_refusal.shared_refcount,
					   finish_refusal.bm_io_in_progress ? "true" : "false",
					   (unsigned)finish_refusal.live_flags,
					   (unsigned long long)finish_refusal.live_token,
					   held_x_revoke_active ? "true" : "false",
					   tagless_target_x ? "true" : "false")));
		gcs_block_resource_x_fail_closed_current();
		if (held_x_revoke_active) {
			(void)cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(
				&held_x_revoke);
			held_x_revoke_active = false;
		}
		(void)gcs_block_resource_x_terminal_owner_release(
			&target_revoke_owner, &target_revoke_owner_held);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (target_x_drop) {
		/* VM/FSM DROP has already removed the exact descriptor mapping, so it
		 * cannot preserve the cached-X cover for the N+PI SourceSettlement
		 * path.  Close that cover and the exact REVOKING owner together after
		 * the physical generation commit, while the retained pair still blocks
		 * any successor bootstrap. */
		pair_result
			= cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
				block, resource_master_node, writer_r4_generation,
				&revoking, &retained, &target_revoke_owner);
		if (pair_result != RESOURCE_X_APPLY_APPLIED) {
			/* The physical drop is irreversible.  Preserve the entry-local owner
			 * and cover as ambiguity evidence after closing the current gate. */
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		memset(&target_revoke_owner, 0, sizeof(target_revoke_owner));
		target_revoke_owner_held = false;
	}
	pair_result = cluster_pcm_lock_resource_x_holder_pair_publish_exact(
		&block->common.logical_assertion,
		block->common.assertion_sequence,
		authenticated_master_node,
		block->common.master_session_incarnation);
	if (pair_result != RESOURCE_X_APPLY_APPLIED
		&& pair_result != RESOURCE_X_APPLY_DUPLICATE) {
		gcs_block_resource_x_fail_closed_current();
		(void)gcs_block_resource_x_terminal_owner_release(
			&target_revoke_owner, &target_revoke_owner_held);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (!gcs_block_resource_x_terminal_owner_release(
			&target_revoke_owner, &target_revoke_owner_held)) {
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	return result;

pre_retained_failure:
	ereport(LOG,
			(errmsg_internal("Resource-X type-17 holder diagnostic"),
			 errdetail("stage=%s result=%d requester=%d master=%d source=%d "
					   "attempt=%llu base=%llu formation=%llu session=%llu "
					   "status=%d image=%d pair=%d "
					   "mode=%u buffer=%d tag=%u/%u/%u/%d/%u generation=%llu "
					   "flags=%u reservation=%llu tagless=%s retained=%s revoke=%s",
					   failure_stage, (int)failure_result,
					   block->common.logical_assertion.requester_node,
					   authenticated_master_node, cluster_node_id,
					   (unsigned long long)block->common.assertion_sequence,
					   (unsigned long long)block->common.base_authority_generation,
					   (unsigned long long)block->common.resource_formation,
					   (unsigned long long)block->common.master_session_incarnation,
					   (int)status_result, (int)image_result, (int)pair_result,
					   (unsigned)source_mode,
					   buffer_id, block->common.logical_assertion.resource.spcOid,
					   block->common.logical_assertion.resource.dbOid,
					   block->common.logical_assertion.resource.relNumber,
					   (int)block->common.logical_assertion.resource.forkNum,
					   block->common.logical_assertion.resource.blockNum,
					   (unsigned long long)current.generation,
					   (unsigned)current.flags,
					   (unsigned long long)current.reservation_token,
					   tagless_target_x ? "true" : "false",
					   semantic_retained ? "true" : "false",
					   revoke_started ? "true" : "false")));
	if (revoke_started) {
		if (held_x_revoke_active) {
			own_result = cluster_bufmgr_pcm_own_abort_held_x_revoke(
				&held_x_revoke);
			if (own_result != CLUSTER_PCM_OWN_OK) {
				gcs_block_resource_x_fail_closed_current();
				(void)cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(
					&held_x_revoke);
				(void)gcs_block_resource_x_terminal_owner_release(
					&target_revoke_owner, &target_revoke_owner_held);
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			held_x_revoke_active = false;
		} else if (!gcs_block_pcm_x_resource_x_abort_pre_arm(
				buf, &revoking)) {
			(void)gcs_block_resource_x_terminal_owner_release(
				&target_revoke_owner, &target_revoke_owner_held);
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
	}
	revoke_started = false;
	if (!gcs_block_resource_x_terminal_owner_release(
			&target_revoke_owner, &target_revoke_owner_held))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	return failure_result;
}

/* Install the exact retained remote carrier while the target is still a
 * non-authoritative N+GRANT_PENDING descriptor.  This is the existing
 * Cache-Fusion-shaped image-before-grant order: the previous PI bytes are
 * never read, shipped, flushed, or relabelled as current.  Only the joined
 * type-15 image is copied, and X commit remains a later linearization. */
static ClusterPcmOwnResult
PGRAC_PCM_X_FENCE_DOMINATED(gcs_block_pcm_x_reserved_image_write_exact)
gcs_block_pcm_x_resource_x_install_target_image_exact(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *reservation_base,
	uint64 reservation_token, const ResourceXCurrentImage *image)
{
	PGAlignedBlock verified;
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnResult own_result;
	Page page;

	if (buf == NULL || reservation_base == NULL || image == NULL
		|| image->page_bytes == NULL || image->image_length != BLCKSZ
		|| reservation_token == 0
		|| !LWLockHeldByMe(BufferDescriptorGetContentLock(buf)))
		return CLUSTER_PCM_OWN_INVALID;
	memcpy(verified.data, image->page_bytes, BLCKSZ);
	if (PageGetLSN((Page)verified.data) != image->page_lsn
		|| ((PageHeader)verified.data)->pd_block_scn != image->page_scn
		|| cluster_gcs_block_compute_checksum(verified.data)
			!= image->page_checksum)
		return CLUSTER_PCM_OWN_CORRUPT;
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	if (own_result != CLUSTER_PCM_OWN_OK)
		return own_result;
	if (!gcs_block_pcm_x_reserved_image_write_exact(
			&live, reservation_base, reservation_token))
		return cluster_pcm_own_classify_live_flags(
			live.flags, live.reservation_token) == CLUSTER_PCM_OWN_CORRUPT
			? CLUSTER_PCM_OWN_CORRUPT : CLUSTER_PCM_OWN_STALE;

	page = BufferGetPage(BufferDescriptorGetBuffer(buf));
	memcpy(page, verified.data, BLCKSZ);
	gcs_block_note_install_copy();
	PageSetLSNPreserveOrigin(page, image->page_lsn);
	((PageHeader)page)->pd_block_scn = image->page_scn;
	own_result = cluster_bufmgr_pcm_own_publish_installed_x_image(
		buf, reservation_base, reservation_token);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		gcs_block_resource_x_fail_closed_current();
		return CLUSTER_PCM_OWN_CORRUPT;
	}
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	if (own_result != CLUSTER_PCM_OWN_OK
		|| !gcs_block_pcm_x_reserved_image_write_exact(
			&live, reservation_base, reservation_token)) {
		gcs_block_resource_x_fail_closed_current();
		return CLUSTER_PCM_OWN_CORRUPT;
	}
	return CLUSTER_PCM_OWN_OK;
}

static ResourceXBufferActivationResult
PGRAC_PCM_X_FENCE_DOMINATED(cluster_pcm_x_resource_x_t2_snapshot_exact)
	gcs_block_pcm_x_resource_x_prepare_target_x(
		const ResourceXAcquisitionRef *ref, uint64 expected_local_generation, bool target_native,
		bool direct_init_bound, uint64 direct_init_reservation_token, bool capture_local_image,
		bool require_clean_n, const ResourceXCurrentImage *precommit_image,
		PGAlignedBlock *aligned_page, ResourceXCurrentImage *image_out,
		uint64 *committed_generation_out, const ResourceXDeliveryClaim *delivery_claim)
{
	BufferDesc *buf;
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot current;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot pending;
	ClusterPcmOwnResult own_result;
	ResourceXApplyResult claim_result;
	LWLock *content_lock;
	Page page;
	uint64 committed_generation = 0;
	uint64 round_direct_generation = 0;
	uint64 round_direct_token = 0;
	uint64 expected_committed_generation;
	uint64 reservation_token = 0;
	uint64 claim_generation = 0;
	uint64 claim_token = 0;
	uint8 claim_source = RESOURCE_X_INSTALL_CLAIM_NONE;
	int buffer_id = -1;
	ResourceXBufferActivationResult result = RESOURCE_X_BUFFER_STALE;
	bool content_locked = false;
	bool direct_init_remote_install;

	if (image_out != NULL)
		memset(image_out, 0, sizeof(*image_out));
	if (committed_generation_out != NULL)
		*committed_generation_out = 0;
	if (ref == NULL || expected_local_generation >= UINT64_MAX - 1
		|| aligned_page == NULL || image_out == NULL
		|| committed_generation_out == NULL
		|| (direct_init_bound
			&& (!target_native
				|| !GcsBlockResourceXDirectInitProofAllowedExact(
					&ref->assertion.resource, require_clean_n,
					precommit_image != NULL)))
		|| (direct_init_bound && direct_init_reservation_token == 0)
		|| (!direct_init_bound && direct_init_reservation_token != 0)
		|| (precommit_image != NULL
			&& (capture_local_image || require_clean_n)))
		return RESOURCE_X_BUFFER_CORRUPT;
	direct_init_remote_install = direct_init_bound && !require_clean_n
		&& precommit_image != NULL
		&& GcsBlockResourceXDirectInitProofAllowedExact(
			&ref->assertion.resource, false, true);
	expected_committed_generation = expected_local_generation + 1;
	if (direct_init_bound) {
		if (!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
				ref, &round_direct_generation, &round_direct_token)
			|| round_direct_generation != expected_local_generation
			|| round_direct_token != direct_init_reservation_token)
			return RESOURCE_X_BUFFER_STALE;
		/* A remote image consumes the original known-new bytes.  Its
		 * installed replay is fenced by this claim, not by PageIsNew. */
		if (direct_init_remote_install)
			own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(&ref->assertion.resource,
																&buffer_id, &current);
		else
			own_result = cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(
				&ref->assertion.resource, round_direct_generation, round_direct_token, &buffer_id,
				&current);
	} else
		own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
			&ref->assertion.resource, &buffer_id, &current);
	if (own_result != CLUSTER_PCM_OWN_OK || buffer_id < 0)
		return own_result == CLUSTER_PCM_OWN_BUSY
			? RESOURCE_X_BUFFER_BUSY
			: own_result == CLUSTER_PCM_OWN_STALE
			? RESOURCE_X_BUFFER_STALE
			: own_result == CLUSTER_PCM_OWN_NOT_READY
			? RESOURCE_X_BUFFER_ABSENT : RESOURCE_X_BUFFER_CORRUPT;
	if (direct_init_bound
		&& !cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
			ref, expected_local_generation, direct_init_reservation_token))
		return RESOURCE_X_BUFFER_STALE;
	buf = GetBufferDescriptor(buffer_id);
	if (delivery_claim != NULL
		&& delivery_claim->target.buffer_id_plus_one != (uint32)buffer_id + 1)
		return RESOURCE_X_BUFFER_STALE;
	content_lock = BufferDescriptorGetContentLock(buf);
	if (!LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE))
		return RESOURCE_X_BUFFER_BUSY;
	content_locked = true;

	PG_TRY();
	{
		do
		{
			own_result = cluster_bufmgr_pcm_own_snapshot(buf, &current);
			if (own_result != CLUSTER_PCM_OWN_OK) {
				result = own_result == CLUSTER_PCM_OWN_BUSY
					? RESOURCE_X_BUFFER_BUSY
					: own_result == CLUSTER_PCM_OWN_STALE
					? RESOURCE_X_BUFFER_STALE
					: own_result == CLUSTER_PCM_OWN_NOT_READY
					? RESOURCE_X_BUFFER_ABSENT
					: RESOURCE_X_BUFFER_CORRUPT;
				break;
			}
			if (!BufferTagsEqual(&current.tag, &ref->assertion.resource)) {
				result = RESOURCE_X_BUFFER_STALE;
				break;
			}
			if (cluster_pcm_x_resource_x_t2_snapshot_exact(ref, &current)) {
				if (current.generation != expected_committed_generation
					|| (direct_init_bound
						&& !cluster_pcm_x_resource_x_claim_installed_exact(
							ref, &current, expected_local_generation,
							direct_init_reservation_token))) {
					result = RESOURCE_X_BUFFER_STALE;
					break;
				}
				committed_generation = current.generation;
			} else if (current.writer_activation_token != 0
					   || current.resource_x_activation_generation != 0) {
				result = RESOURCE_X_BUFFER_BUSY;
				break;
			} else if (current.generation != expected_local_generation) {
				result = current.flags != 0 ? RESOURCE_X_BUFFER_BUSY
											 : RESOURCE_X_BUFFER_STALE;
				break;
			} else if (direct_init_bound
					   && current.flags
						  != PCM_OWN_FLAG_GRANT_PENDING) {
				result = RESOURCE_X_BUFFER_STALE;
				break;
			} else {
				base = current;
				if (current.flags == PCM_OWN_FLAG_GRANT_PENDING) {
					if (current.reservation_token == 0) {
						result = RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
					/* The exact known-new reservation remains durable-only.  The
					 * one aux-only exception consumes an already-joined remote
					 * carrier through this ordinary image-before-X installer; the
					 * local direct-init proof never authorizes those bytes. */
					if (target_native && !direct_init_bound) {
						/* A retry may consume only the reservation published by
						 * this exact T1 installer, never whichever token it sees. */
						claim_result = cluster_pcm_lock_resource_x_install_claim_snapshot_exact(
							ref, &claim_source, &claim_generation, &claim_token);
						if (claim_result != RESOURCE_X_APPLY_APPLIED
							|| claim_source != RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1
							|| current.pcm_state != (uint8)PCM_STATE_N
							|| claim_generation != current.generation
							|| claim_token != current.reservation_token) {
							result = claim_result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
										 ? RESOURCE_X_BUFFER_CORRUPT
										 : RESOURCE_X_BUFFER_STALE;
							break;
						}
					} else if (target_native) {
						if (!direct_init_bound
							|| (!require_clean_n
								&& !direct_init_remote_install)
							|| current.pcm_state != (uint8)PCM_STATE_N)
							own_result = CLUSTER_PCM_OWN_STALE;
						else
							own_result
								= cluster_bufmgr_pcm_own_n_direct_init_candidate_exact(
									buf, &current);
						if (own_result != CLUSTER_PCM_OWN_OK) {
							result = own_result == CLUSTER_PCM_OWN_BUSY
								? RESOURCE_X_BUFFER_BUSY
								: own_result == CLUSTER_PCM_OWN_STALE
								? RESOURCE_X_BUFFER_STALE
								: RESOURCE_X_BUFFER_CORRUPT;
							break;
						}
						if (!cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
								ref, current.generation,
								current.reservation_token)) {
							result = RESOURCE_X_BUFFER_STALE;
							break;
						}
					}
					if (current.pcm_state == (uint8)PCM_STATE_N) {
						base.flags = 0;
						base.reservation_token--;
					} else if (current.pcm_state == (uint8)PCM_STATE_S
							   || current.pcm_state == (uint8)PCM_STATE_X)
						base.flags = PCM_OWN_FLAG_REVOKING;
					else {
						result = RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
					reservation_token = current.reservation_token;
				} else if (current.flags != 0) {
					result = RESOURCE_X_BUFFER_BUSY;
					break;
				} else if (current.pcm_state == (uint8)PCM_STATE_N) {
					if (require_clean_n) {
						own_result
							= cluster_bufmgr_pcm_own_n_storage_candidate_exact(
								buf, &current);
						if (own_result != CLUSTER_PCM_OWN_OK) {
							result = own_result == CLUSTER_PCM_OWN_BUSY
								? RESOURCE_X_BUFFER_BUSY
								: own_result == CLUSTER_PCM_OWN_STALE
								? RESOURCE_X_BUFFER_STALE
								: RESOURCE_X_BUFFER_CORRUPT;
							break;
						}
					}
					if (delivery_claim != NULL)
						own_result = cluster_bufmgr_pcm_own_delivery_begin_x_reservation(
							buf, &current, ref->acquisition_generation, &reservation_token);
					else
						own_result = cluster_bufmgr_pcm_own_begin_x_reservation(buf, &current,
																				&reservation_token);
					if (own_result != CLUSTER_PCM_OWN_OK) {
						result = own_result == CLUSTER_PCM_OWN_BUSY
							? RESOURCE_X_BUFFER_BUSY
							: own_result == CLUSTER_PCM_OWN_STALE
							? RESOURCE_X_BUFFER_STALE
							: RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
					if (target_native) {
						/* Still under the same content-X that created PENDING.
						 * Publish its exact physical tuple before image install/X. */
						own_result = cluster_bufmgr_pcm_own_snapshot(buf, &pending);
						if (own_result != CLUSTER_PCM_OWN_OK
							|| pending.generation != current.generation
							|| pending.reservation_token != reservation_token) {
							result = RESOURCE_X_BUFFER_CORRUPT;
							break;
						}
						claim_result
							= cluster_pcm_lock_resource_x_install_claim_bind_t1_delivery_exact(
								ref, &pending, delivery_claim);
						if (claim_result != RESOURCE_X_APPLY_APPLIED
							&& claim_result != RESOURCE_X_APPLY_DUPLICATE) {
							result = claim_result == RESOURCE_X_APPLY_STALE
										 ? RESOURCE_X_BUFFER_STALE
										 : RESOURCE_X_BUFFER_CORRUPT;
							break;
						}
					}
				} else if (current.pcm_state == (uint8)PCM_STATE_S) {
					own_result = cluster_bufmgr_pcm_own_begin_s_revoke(
						buf, &current, &revoking);
					if (own_result == CLUSTER_PCM_OWN_OK) {
						base = revoking;
						own_result
							= cluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation(
								buf, &revoking, &reservation_token);
					}
					if (own_result != CLUSTER_PCM_OWN_OK) {
						result = own_result == CLUSTER_PCM_OWN_BUSY
							? RESOURCE_X_BUFFER_BUSY
							: own_result == CLUSTER_PCM_OWN_STALE
							? RESOURCE_X_BUFFER_STALE
							: RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
				} else if (current.pcm_state == (uint8)PCM_STATE_X) {
					own_result = cluster_bufmgr_pcm_own_begin_x_revoke(
						buf, &current, &revoking);
					if (own_result == CLUSTER_PCM_OWN_OK) {
						base = revoking;
						own_result
							= cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(
								buf, &revoking, &reservation_token);
					}
					if (own_result != CLUSTER_PCM_OWN_OK) {
						result = own_result == CLUSTER_PCM_OWN_BUSY
							? RESOURCE_X_BUFFER_BUSY
							: own_result == CLUSTER_PCM_OWN_STALE
							? RESOURCE_X_BUFFER_STALE
							: RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
				} else {
					result = RESOURCE_X_BUFFER_STALE;
					break;
				}
				if (precommit_image != NULL) {
					if (base.pcm_state != (uint8)PCM_STATE_N
						|| current.pcm_state != (uint8)PCM_STATE_N) {
						result = RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
					own_result
						= gcs_block_pcm_x_resource_x_install_target_image_exact(
							buf, &base, reservation_token,
							precommit_image);
					if (own_result != CLUSTER_PCM_OWN_OK) {
						result = own_result == CLUSTER_PCM_OWN_BUSY
							? RESOURCE_X_BUFFER_BUSY
							: own_result == CLUSTER_PCM_OWN_STALE
							? RESOURCE_X_BUFFER_STALE
							: RESOURCE_X_BUFFER_CORRUPT;
						break;
					}
				}
				own_result = cluster_bufmgr_pcm_own_finish_x_commit(
					buf, &base, reservation_token, &committed_generation);
				if (own_result != CLUSTER_PCM_OWN_OK
					|| committed_generation != expected_committed_generation) {
					result = own_result == CLUSTER_PCM_OWN_BUSY
						? RESOURCE_X_BUFFER_BUSY
						: own_result == CLUSTER_PCM_OWN_STALE
						? RESOURCE_X_BUFFER_STALE
						: RESOURCE_X_BUFFER_CORRUPT;
					break;
				}
			}

			if (capture_local_image) {
				own_result = cluster_bufmgr_pcm_own_snapshot(buf, &current);
				if (own_result != CLUSTER_PCM_OWN_OK
					|| current.generation != committed_generation
					|| !cluster_pcm_x_resource_x_t2_snapshot_exact(
						ref, &current)) {
					result = own_result == CLUSTER_PCM_OWN_BUSY
						? RESOURCE_X_BUFFER_BUSY
						: own_result == CLUSTER_PCM_OWN_NOT_READY
						? RESOURCE_X_BUFFER_ABSENT
						: own_result == CLUSTER_PCM_OWN_CORRUPT
						? RESOURCE_X_BUFFER_CORRUPT
						: RESOURCE_X_BUFFER_STALE;
					break;
				}
				page = BufferGetPage(BufferDescriptorGetBuffer(buf));
				memcpy(aligned_page->data, page, BLCKSZ);
				image_out->page_bytes = aligned_page->data;
				image_out->page_lsn = PageGetLSN((Page)aligned_page->data);
				image_out->page_scn
					= ((PageHeader)aligned_page->data)->pd_block_scn;
				image_out->page_checksum
					= cluster_gcs_block_compute_checksum(aligned_page->data);
				image_out->image_length = BLCKSZ;
			}
			*committed_generation_out = committed_generation;
			result = RESOURCE_X_BUFFER_T2_INSTALLED;
		} while (false);
		LWLockRelease(content_lock);
		content_locked = false;
	}
	PG_CATCH();
	{
		if (content_locked && LWLockHeldByMe(content_lock))
			LWLockRelease(content_lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

/* Consume one complete retained type-15 join through R9's existing executor.
 * The exact BufferDesc base is derived directly from the target entry. */
static ResourceXApplyResult
gcs_block_pcm_x_resource_x_join_terminal_try(const ResourceXAssertion *assertion,
											 bool scheduled_retry,
											 const ResourceXDeliveryClaim *delivery_claim,
											 ResourceXAcquisitionRef *terminal_ref_out,
											 uint64 *terminal_ownership_generation_out,
											 uint64 *terminal_authority_generation_out)
{
	PGAlignedBlock aligned_image;
	ClusterPcmOwnSnapshot target_base;
	ClusterPcmOwnResult own_result;
	ResourceXRequesterJoinSnapshot join;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image_frame;
	ResourceXAcquisitionRef ref;
	ResourceXActivationGateToken gate;
	ResourceXExecutorSnapshot executor_snapshot;
	ResourceXExecutorProbeResult probe_result;
	ResourceXCurrentImage image;
	ResourceXCurrentImage durable_image;
	ResourceXCurrentImage joined_image;
	ResourceXDurableProof durable;
	ResourceXBufferInstallProof install_proof;
	ResourceXBufferActivationProof activation_proof;
	ResourceXBufferActivationResult buffer_result = RESOURCE_X_BUFFER_STALE;
	ResourceXApplyResult result;
	uint64 expected_local_generation = 0;
	uint64 committed_generation = 0;
	uint64 direct_init_generation = 0;
	uint64 direct_init_token = 0;
	uint64 claim_generation = 0;
	uint64 claim_token = 0;
	uint64 storage_scn = 0;
	XLogRecPtr storage_lsn = InvalidXLogRecPtr;
	int target_buffer_id = -1;
	uint8 proof_kind;
	uint8 claim_source = RESOURCE_X_INSTALL_CLAIM_NONE;
	bool entered = false;
	bool durable_proof;
	bool local_proof;
	bool remote_proof;
	bool direct_init_bound = false;

	if (terminal_ref_out != NULL)
		memset(terminal_ref_out, 0, sizeof(*terminal_ref_out));
	if (terminal_ownership_generation_out != NULL)
		*terminal_ownership_generation_out = 0;
	if (terminal_authority_generation_out != NULL)
		*terminal_authority_generation_out = 0;
	if (terminal_ref_out == NULL
		|| terminal_ownership_generation_out == NULL
		|| terminal_authority_generation_out == NULL)
		return RESOURCE_X_APPLY_INVALID;

	result = cluster_pcm_lock_resource_x_requester_join_frames_exact(
		assertion, &grant, &image_frame, &join);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	if ((join.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0
		|| join.requester_target_generation == 0
		|| join.requester_target_generation != join.assertion_sequence
		|| join.final_authority_generation <= join.base_authority_generation
		|| join.final_authority_generation == UINT64_MAX
		|| grant.kind != RESOURCE_X_WIRE_AUTHORITY_GRANT)
		return RESOURCE_X_APPLY_BAD_STATE;
	proof_kind = grant.body.authority_grant.proof_kind;
	local_proof = proof_kind == RESOURCE_X_PROOF_LOCAL_IMAGE;
	durable_proof = proof_kind == RESOURCE_X_PROOF_DURABLE_STORAGE;
	remote_proof = proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER;
	if ((!local_proof && !durable_proof && !remote_proof)
		|| (!local_proof && !durable_proof
			&& (image_frame.kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
				|| image_frame.body.image_envelope.image_length != BLCKSZ))
		|| (durable_proof
			&& (image_frame.kind != 0
				|| grant.common.flags != 0
				|| grant.body.authority_grant.source_carrier_generation != 0
				|| grant.body.authority_grant.dependency_count != 0)))
		return RESOURCE_X_APPLY_BAD_STATE;
	memset(&ref, 0, sizeof(ref));
	ref.assertion = join.assertion;
	ref.formation = join.resource_formation;
	ref.acquisition_generation = join.requester_target_generation;

	memset(&target_base, 0, sizeof(target_base));
	result = cluster_pcm_lock_resource_x_install_claim_snapshot_exact(
		&ref, &claim_source, &claim_generation, &claim_token);
	if (result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_NOT_FOUND)
		return result;
	if (claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT) {
		direct_init_bound = cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
			&ref, &direct_init_generation, &direct_init_token);
		if (direct_init_bound
			&& (direct_init_generation != claim_generation || direct_init_token != claim_token))
			return RESOURCE_X_APPLY_STALE;
	}
	if (direct_init_bound) {
		if (!GcsBlockResourceXDirectInitProofAllowedExact(
				&ref.assertion.resource, durable_proof, remote_proof))
			return RESOURCE_X_APPLY_STALE;
		if (remote_proof)
			own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(&ref.assertion.resource,
																&target_buffer_id, &target_base);
		else
			own_result = cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(
				&ref.assertion.resource, direct_init_generation, direct_init_token,
				&target_buffer_id, &target_base);
	} else
		own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
				&ref.assertion.resource, &target_buffer_id, &target_base);
	if (own_result != CLUSTER_PCM_OWN_OK || target_buffer_id < 0
			|| !BufferTagsEqual(&target_base.tag, &ref.assertion.resource)
			|| target_base.generation == UINT64_MAX)
			return own_result == CLUSTER_PCM_OWN_BUSY
				? RESOURCE_X_APPLY_BAD_STATE
				: own_result == CLUSTER_PCM_OWN_CORRUPT
				? RESOURCE_X_APPLY_RECOVERY_BLOCKED
				: RESOURCE_X_APPLY_STALE;
	if (delivery_claim != NULL) {
		ClusterPcmOwnSnapshot held;

		if (delivery_claim->target.buffer_id_plus_one != (uint32)target_buffer_id + 1
			|| delivery_claim->observation.request.assertion_sequence != ref.acquisition_generation
			|| !resource_x_assertion_equal(&delivery_claim->observation.request.logical_assertion,
										   &ref.assertion)
			|| delivery_claim->observation.request.resource_formation != ref.formation
			|| cluster_bufmgr_pcm_own_delivery_snapshot_exact(GetBufferDescriptor(target_buffer_id),
															  &ref.assertion.resource,
															  ref.acquisition_generation, &held)
				   != CLUSTER_PCM_OWN_OK
			|| !cluster_pcm_own_snapshot_equal_exact(&held, &target_base))
			return RESOURCE_X_APPLY_STALE;
	}
	if (target_base.flags == PCM_OWN_FLAG_GRANT_PENDING) {
		if (claim_source == RESOURCE_X_INSTALL_CLAIM_NONE
			|| target_base.pcm_state != (uint8)PCM_STATE_N
			|| target_base.generation != claim_generation
			|| target_base.reservation_token != claim_token
			|| target_base.writer_activation_token != 0
			|| target_base.resource_x_activation_generation != 0)
			return RESOURCE_X_APPLY_STALE;
		if (direct_init_bound) {
			if (!GcsBlockResourceXDirectInitProofAllowedExact(&ref.assertion.resource,
															  durable_proof, remote_proof))
				return RESOURCE_X_APPLY_STALE;
			own_result
				= cluster_bufmgr_pcm_own_n_direct_init_candidate_exact(
					GetBufferDescriptor(target_buffer_id), &target_base);
			if (own_result != CLUSTER_PCM_OWN_OK)
				return own_result == CLUSTER_PCM_OWN_BUSY
					? RESOURCE_X_APPLY_BAD_STATE
					: own_result == CLUSTER_PCM_OWN_STALE
					? RESOURCE_X_APPLY_STALE
					: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			direct_init_bound
				= direct_init_bound
				  && cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
					  &ref, target_base.generation,
					  target_base.reservation_token);
			if (!direct_init_bound)
				return RESOURCE_X_APPLY_STALE;
		}
	} else if (target_base.pcm_state == (uint8)PCM_STATE_N) {
			/* Bootstrap admitted a cold generation-zero N descriptor only
			 * through this exact BM_VALID/no-IO predicate.  Revalidate the
			 * same shape at the type-15 terminal before T1/T2/T3; generation
			 * zero supplies no authority, while the joined grant and image do. */
			own_result
				= cluster_bufmgr_pcm_own_n_assertion_candidate_exact(
					GetBufferDescriptor(target_buffer_id), &target_base, NULL);
			if (own_result != CLUSTER_PCM_OWN_OK)
				return own_result == CLUSTER_PCM_OWN_BUSY
					? RESOURCE_X_APPLY_BAD_STATE
					: own_result == CLUSTER_PCM_OWN_STALE
					? RESOURCE_X_APPLY_STALE
					: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	} else if (target_base.generation == 0)
		return RESOURCE_X_APPLY_STALE;
	/* A T1 reservation owns the installation base through I1/I2.  The
	 * current descriptor has already advanced once on an installed replay. */
	expected_local_generation
		= claim_source == RESOURCE_X_INSTALL_CLAIM_NONE ? target_base.generation : claim_generation;
	memset(&executor_snapshot, 0, sizeof(executor_snapshot));
	probe_result = cluster_pcm_lock_resource_x_executor_probe_exact(
			&ref, &executor_snapshot);
	/* A retained exact type-15 replay is one of the existing requester
	 * scheduling ticks.  It may re-drive a prior conditional content-lock
	 * miss once, but must not erase identity/image contradictions or spin in
	 * this LMS callback. */
	if (probe_result == RESOURCE_X_EXECUTOR_BLOCKED
		&& scheduled_retry
		&& executor_snapshot.no_progress_generation == ref.acquisition_generation
		&& executor_snapshot.no_progress_reason == RESOURCE_X_NO_PROGRESS_BUFFER_BUSY) {
		result = cluster_pcm_lock_resource_x_executor_rearm_exact(&ref);
		if (result == RESOURCE_X_APPLY_APPLIED
			|| result == RESOURCE_X_APPLY_DUPLICATE) {
			memset(&executor_snapshot, 0, sizeof(executor_snapshot));
			probe_result = cluster_pcm_lock_resource_x_executor_probe_exact(
				&ref, &executor_snapshot);
		}
	}
	if (probe_result == RESOURCE_X_EXECUTOR_COMPLETE) {
			if (target_base.pcm_state != (uint8)PCM_STATE_X
				|| target_base.flags != 0
				|| target_base.writer_activation_token != 0
				|| target_base.resource_x_activation_generation != 0)
				return RESOURCE_X_APPLY_STALE;
			*terminal_ref_out = ref;
			*terminal_ownership_generation_out = target_base.generation;
			*terminal_authority_generation_out
				= join.final_authority_generation;
			return RESOURCE_X_APPLY_DUPLICATE;
		}
	if (probe_result == RESOURCE_X_EXECUTOR_BLOCKED) {
			(void)cluster_pcm_lock_resource_x_executor_wait_exact(&ref, 0);
			return RESOURCE_X_APPLY_BAD_STATE;
		}
		/* A terminal round no longer exposes the live direct-init claim; only
	 * the COMPLETE branch above may consume that historical lineage. */
		if (claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT && !direct_init_bound)
			return RESOURCE_X_APPLY_STALE;
		if (claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
			&& target_base.pcm_state == (uint8)PCM_STATE_X
			&& !cluster_pcm_x_resource_x_claim_installed_exact(&ref, &target_base, claim_generation,
															   claim_token))
			return RESOURCE_X_APPLY_STALE;
		if (probe_result != RESOURCE_X_EXECUTOR_READY
			&& !(probe_result == RESOURCE_X_EXECUTOR_CHANGED
				 && executor_snapshot.ref.assertion.requester_node == -1
				 && executor_snapshot.ref.formation == 0
				 && executor_snapshot.ref.acquisition_generation == 0
				 && executor_snapshot.progress_flags == 0
				 && executor_snapshot.retired_acquisition_generation < ref.acquisition_generation))
			return probe_result == RESOURCE_X_EXECUTOR_CHANGED ? RESOURCE_X_APPLY_STALE
															   : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		memset(&gate, 0, sizeof(gate));
		if (!cluster_pcm_lock_resource_x_executor_enter(&ref, &gate))
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		entered = true;

		PG_TRY();
		{
			do
			{
				result = cluster_pcm_lock_resource_x_t1_grant_delivery_exact(&ref, delivery_claim);
				if (result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE)
					break;

				memset(&image, 0, sizeof(image));
				memset(&durable_image, 0, sizeof(durable_image));
				memset(&joined_image, 0, sizeof(joined_image));
				if (durable_proof) {
					if (!cluster_bufmgr_read_storage_image_for_resource_x(
							ref.assertion.resource, aligned_image.data, &storage_lsn,
							&storage_scn)) {
						result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
						break;
					}
					memset(&durable, 0, sizeof(durable));
					durable.assertion = grant.common.logical_assertion;
					durable.base_authority_generation = grant.common.base_authority_generation;
					durable.resource_formation = grant.common.resource_formation;
					durable.master_session_incarnation = grant.common.master_session_incarnation;
					durable.assertion_sequence = grant.common.assertion_sequence;
					durable.requester_target_generation
						= grant.body.authority_grant.requester_target_generation;
					durable.page_scn_lsn = storage_scn;
					durable.page_checksum = cluster_gcs_block_compute_checksum(aligned_image.data);
					durable.source_proof_crc32c
						= gcs_block_pcm_x_resource_x_durable_proof_crc(&durable);
					if (durable.source_proof_crc32c == 0
						|| grant.body.authority_grant.page_scn_lsn != durable.page_scn_lsn
						|| grant.body.authority_grant.page_checksum != durable.page_checksum
						|| grant.body.authority_grant.source_proof_crc32c
							   != durable.source_proof_crc32c) {
						result = RESOURCE_X_APPLY_STALE;
						break;
					}
					durable_image.page_bytes = aligned_image.data;
					durable_image.page_lsn = storage_lsn;
					durable_image.page_scn = (SCN)storage_scn;
					durable_image.page_checksum = durable.page_checksum;
					durable_image.image_length = BLCKSZ;
				} else if (remote_proof) {
					memcpy(aligned_image.data, image_frame.body.image_envelope.page_bytes, BLCKSZ);
					joined_image.page_bytes = aligned_image.data;
					joined_image.page_lsn = PageGetLSN((Page)aligned_image.data);
					joined_image.page_scn = ((PageHeader)aligned_image.data)->pd_block_scn;
					joined_image.page_checksum = image_frame.body.image_envelope.page_checksum;
					joined_image.image_length = BLCKSZ;
					if (image_frame.body.image_envelope.page_scn_lsn
						!= (uint64)joined_image.page_scn) {
						result = RESOURCE_X_APPLY_INVALID;
						break;
					}
				}

				buffer_result = gcs_block_pcm_x_resource_x_prepare_target_x(
					&ref, expected_local_generation, true, direct_init_bound, direct_init_token,
					local_proof, durable_proof, remote_proof ? &joined_image : NULL, &aligned_image,
					&image, &committed_generation, delivery_claim);
				if (buffer_result != RESOURCE_X_BUFFER_T2_INSTALLED
					&& buffer_result != RESOURCE_X_BUFFER_ALREADY_INSTALLED) {
					cluster_pcm_lock_resource_x_publish_no_progress_exact(
						&ref, buffer_result == RESOURCE_X_BUFFER_BUSY
								  ? RESOURCE_X_NO_PROGRESS_BUFFER_BUSY
							  : buffer_result == RESOURCE_X_BUFFER_ABSENT
								  ? RESOURCE_X_NO_PROGRESS_BUFFER_ABSENT
							  : buffer_result == RESOURCE_X_BUFFER_STALE
								  ? RESOURCE_X_NO_PROGRESS_BUFFER_STALE
								  : RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT);
					result = buffer_result == RESOURCE_X_BUFFER_STALE ? RESOURCE_X_APPLY_STALE
							 : buffer_result == RESOURCE_X_BUFFER_CORRUPT
								 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED
								 : RESOURCE_X_APPLY_BAD_STATE;
					break;
				}
			if (durable_proof)
				image = durable_image;
			else if (remote_proof)
				image = joined_image;
			else if (local_proof
					 && (grant.body.authority_grant.page_scn_lsn
					   != (uint64)image.page_scn
				   || grant.body.authority_grant.page_checksum
						  != image.page_checksum)) {
				result = RESOURCE_X_APPLY_STALE;
				break;
			}

			if (direct_init_bound && !remote_proof)
				buffer_result
					= cluster_bufmgr_pcm_own_direct_init_bind_x_by_tag_exact(
						&ref, committed_generation, direct_init_token,
						&install_proof);
			else
				buffer_result = cluster_bufmgr_pcm_own_activate_x_by_tag(
					&ref, &image, &install_proof);
			if (buffer_result != RESOURCE_X_BUFFER_T2_INSTALLED
				&& buffer_result != RESOURCE_X_BUFFER_ALREADY_INSTALLED) {
				cluster_pcm_lock_resource_x_publish_no_progress_exact(
					&ref, buffer_result == RESOURCE_X_BUFFER_BUSY
						? RESOURCE_X_NO_PROGRESS_BUFFER_BUSY
						: buffer_result == RESOURCE_X_BUFFER_ABSENT
						? RESOURCE_X_NO_PROGRESS_BUFFER_ABSENT
						: buffer_result == RESOURCE_X_BUFFER_STALE
						? RESOURCE_X_NO_PROGRESS_BUFFER_STALE
						: RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT);
				result = buffer_result == RESOURCE_X_BUFFER_STALE
					? RESOURCE_X_APPLY_STALE
					: buffer_result == RESOURCE_X_BUFFER_CORRUPT
					? RESOURCE_X_APPLY_RECOVERY_BLOCKED
					: RESOURCE_X_APPLY_BAD_STATE;
				break;
			}

			result = cluster_pcm_lock_resource_x_requester_apply_delivery_exact(
				&ref, &install_proof, delivery_claim);
			if (result != RESOURCE_X_APPLY_APPLIED
				&& result != RESOURCE_X_APPLY_DUPLICATE)
				break;
			if (direct_init_bound && !remote_proof)
				buffer_result
					= cluster_bufmgr_pcm_own_direct_init_clear_x_by_tag_exact(
						&ref, committed_generation, direct_init_token,
						&activation_proof);
			else
				buffer_result
					= cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(
						&ref, &activation_proof);
			if (buffer_result != RESOURCE_X_BUFFER_T2_INSTALLED
				&& buffer_result != RESOURCE_X_BUFFER_ALREADY_INSTALLED) {
				cluster_pcm_lock_resource_x_publish_no_progress_exact(
					&ref, buffer_result == RESOURCE_X_BUFFER_BUSY
						? RESOURCE_X_NO_PROGRESS_BUFFER_BUSY
						: buffer_result == RESOURCE_X_BUFFER_ABSENT
						? RESOURCE_X_NO_PROGRESS_BUFFER_ABSENT
						: buffer_result == RESOURCE_X_BUFFER_STALE
						? RESOURCE_X_NO_PROGRESS_BUFFER_STALE
						: RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT);
				result = buffer_result == RESOURCE_X_BUFFER_STALE
					? RESOURCE_X_APPLY_STALE
					: buffer_result == RESOURCE_X_BUFFER_CORRUPT
					? RESOURCE_X_APPLY_RECOVERY_BLOCKED
					: RESOURCE_X_APPLY_BAD_STATE;
				break;
			}
			result = cluster_pcm_lock_resource_x_requester_activate_delivery_exact(
				&ref, &activation_proof, delivery_claim);
			if (result != RESOURCE_X_APPLY_APPLIED
				&& result != RESOURCE_X_APPLY_DUPLICATE)
				break;
			*terminal_ref_out = ref;
			*terminal_ownership_generation_out
				= activation_proof.ownership_generation;
			*terminal_authority_generation_out
				= join.final_authority_generation;
			break;
		} while (false);
	}
	PG_CATCH();
	{
		if (entered)
			cluster_pcm_lock_resource_x_executor_leave(&gate);
		PG_RE_THROW();
	}
	PG_END_TRY();

	cluster_pcm_lock_resource_x_executor_leave(&gate);
	return result;
}

static ResourceXApplyResult
gcs_block_resource_x_delivery_finish_exact(const ResourceXDeliveryClaim *claim)
{
	ClusterPcmOwnSnapshot terminal;
	ClusterPcmOwnResult own_result;
	ResourceXAcquisitionRef ref;
	uint8 install_source = 0;
	uint64 pending_generation = 0;
	uint64 reservation_token = 0;
	int buffer_id;

	if (claim == NULL || claim->target.buffer_id_plus_one == 0)
		return RESOURCE_X_APPLY_INVALID;
	buffer_id = (int)claim->target.buffer_id_plus_one - 1;
	if (buffer_id < 0 || buffer_id >= NBuffers)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	ref.assertion = claim->observation.request.logical_assertion;
	ref.formation = claim->observation.request.resource_formation;
	ref.acquisition_generation = claim->observation.request.assertion_sequence;
	if (!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
			&ref, claim->observation.request.master_session_incarnation,
			claim->observation.r4_record_generation, claim->target.generation + 1))
		return RESOURCE_X_APPLY_STALE;
	if (cluster_pcm_lock_resource_x_install_claim_snapshot_exact(
			&ref, &install_source, &pending_generation, &reservation_token)
			!= RESOURCE_X_APPLY_APPLIED
		|| pending_generation != claim->target.generation)
		return RESOURCE_X_APPLY_STALE;
	own_result = cluster_bufmgr_pcm_own_snapshot(GetBufferDescriptor(buffer_id), &terminal);
	if (own_result != CLUSTER_PCM_OWN_OK)
		return own_result == CLUSTER_PCM_OWN_CORRUPT ? RESOURCE_X_APPLY_RECOVERY_BLOCKED
													 : RESOURCE_X_APPLY_BAD_STATE;
	if (!BufferTagsEqual(&terminal.tag, &ref.assertion.resource)
		|| terminal.generation != claim->target.generation + 1 || reservation_token == 0
		|| reservation_token == UINT64_MAX || terminal.reservation_token < reservation_token
		|| terminal.reservation_token == UINT64_MAX)
		return RESOURCE_X_APPLY_STALE;
	own_result = cluster_bufmgr_pcm_own_delivery_hold_release_exact(
		GetBufferDescriptor(buffer_id), &terminal, ref.acquisition_generation, reservation_token);
	if (own_result != CLUSTER_PCM_OWN_OK)
		return own_result == CLUSTER_PCM_OWN_CORRUPT ? RESOURCE_X_APPLY_RECOVERY_BLOCKED
			   : own_result == CLUSTER_PCM_OWN_STALE ? RESOURCE_X_APPLY_STALE
													 : RESOURCE_X_APPLY_BAD_STATE;
	return cluster_pcm_lock_resource_x_delivery_complete_exact(claim, &terminal);
}

/* Normal ingress and the background owner share the actual physical
 * installer, but only the background callback may claim CLEANUP. No implicit
 * deadline bypass is attached to the normal ingress process or resource. */
static ResourceXApplyResult
gcs_block_pcm_x_resource_x_join_terminal_owned_try(const ResourceXAcquisitionRef *ref,
												   bool scheduled_retry,
												   ResourceXAcquisitionRef *terminal_ref_out,
												   uint64 *ownership_out, uint64 *authority_out)
{
	ResourceXInstallClaimJoinObservation observation;
	ResourceXDeliveryTarget target;
	ResourceXDeliveryClaim claim;
	ResourceXApplyResult result;

	result = cluster_pcm_lock_resource_x_delivery_target_snapshot_exact(ref, &observation, &target);
	if (result == RESOURCE_X_APPLY_NOT_FOUND)
		return gcs_block_pcm_x_resource_x_join_terminal_try(
			&ref->assertion, scheduled_retry, NULL, terminal_ref_out, ownership_out, authority_out);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	result = cluster_pcm_lock_resource_x_delivery_claim_begin_exact(
		ref, &target, RESOURCE_X_DELIVERY_NORMAL, gcs_block_pcm_x_monotonic_us(), &claim);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	PG_TRY();
	{
		result = gcs_block_pcm_x_resource_x_join_terminal_try(&ref->assertion, scheduled_retry,
															  &claim, terminal_ref_out,
															  ownership_out, authority_out);
		if (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
			result = gcs_block_resource_x_delivery_finish_exact(&claim);
	}
	PG_FINALLY();
	{
		/* Completion clears the exact claim. In that case end is a stale no-op. */
		(void)cluster_pcm_lock_resource_x_delivery_claim_end_exact(&claim);
	}
	PG_END_TRY();
	return result;
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_master_durable_try(
	const ResourceXDecodedFrame *assertion,
	const ResourceXMasterSnapshot *captured,
	ResourceXMasterSnapshot *out)
{
	PGAlignedBlock aligned_image;
	ResourceXDurableProof durable;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	uint64 page_scn = 0;

	if (assertion == NULL || captured == NULL || out == NULL
		|| assertion->kind != RESOURCE_X_WIRE_ASSERT_X
		|| assertion->common.observed_mode != (uint8)PCM_STATE_N
		|| captured->phase != RESOURCE_X_MASTER_WAIT_PROOF
		|| captured->is_head != 1
		|| captured->proof_kind != 0
		|| captured->incompatible_holders_bitmap != captured->blocked_holders_bitmap
		|| !resource_x_assertion_equal(&captured->assertion,
			&assertion->common.logical_assertion)
		|| captured->base_authority_generation
			!= assertion->common.base_authority_generation
		|| captured->resource_formation
			!= assertion->common.resource_formation
		|| captured->master_session_incarnation
			!= assertion->common.master_session_incarnation
		|| captured->assertion_sequence
			!= assertion->common.assertion_sequence)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (!cluster_bufmgr_read_storage_image_for_resource_x(
			assertion->common.logical_assertion.resource,
			aligned_image.data, &page_lsn, &page_scn))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;

	memset(&durable, 0, sizeof(durable));
	durable.assertion = captured->assertion;
	durable.base_authority_generation = captured->base_authority_generation;
	durable.resource_formation = captured->resource_formation;
	durable.master_session_incarnation
		= captured->master_session_incarnation;
	durable.assertion_sequence = captured->assertion_sequence;
	durable.requester_target_generation = captured->assertion_sequence;
	durable.page_scn_lsn = page_scn;
	durable.page_checksum
		= cluster_gcs_block_compute_checksum(aligned_image.data);
	durable.source_proof_crc32c
		= gcs_block_pcm_x_resource_x_durable_proof_crc(&durable);
	if (durable.source_proof_crc32c == 0)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	return cluster_pcm_lock_resource_x_durable_proof_exact(&durable, out);
}

static ResourceXApplyResult
gcs_block_pcm_x_resource_x_remote_s_own_result(
	ClusterPcmOwnResult result)
{
	if (result == CLUSTER_PCM_OWN_OK)
		return RESOURCE_X_APPLY_APPLIED;
	if (result == CLUSTER_PCM_OWN_STALE)
		return RESOURCE_X_APPLY_STALE;
	if (result == CLUSTER_PCM_OWN_BUSY)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (result == CLUSTER_PCM_OWN_NOT_READY)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	gcs_block_resource_x_fail_closed_current();
	return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}

/* The remote-S transaction applies its failure-domain decision only after
 * exact rollback outcomes are known.  Unlike the older requester mapper,
 * this conversion must not fence as a side effect. */
static ResourceXApplyResult
gcs_block_resource_x_remote_s_own_result(ClusterPcmOwnResult result)
{
	if (result == CLUSTER_PCM_OWN_OK)
		return RESOURCE_X_APPLY_APPLIED;
	if (result == CLUSTER_PCM_OWN_STALE)
		return RESOURCE_X_APPLY_STALE;
	if (result == CLUSTER_PCM_OWN_BUSY)
		return RESOURCE_X_APPLY_BAD_STATE;
	return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}

static void
gcs_block_resource_x_failure_decision_apply(
	const ResourceXRemoteSFailureDecision *decision)
{
	if (decision != NULL && decision->global_fail_closed)
		gcs_block_resource_x_fail_closed_current();
}

typedef enum ResourceXSourceSettlementStage {
	RESOURCE_X_SOURCE_SETTLEMENT_STAGE_NONE = 0,
	RESOURCE_X_SOURCE_SETTLEMENT_STAGE_PREPARE,
	RESOURCE_X_SOURCE_SETTLEMENT_STAGE_LOCAL_RELEASE,
	RESOURCE_X_SOURCE_SETTLEMENT_STAGE_COMMIT,
	RESOURCE_X_SOURCE_SETTLEMENT_STAGE_ACK_BUILD,
	RESOURCE_X_SOURCE_SETTLEMENT_STAGE_ACK_ENCODE
} ResourceXSourceSettlementStage;

typedef struct ResourceXFirstFailureEvidence {
	BufferTag tag;
	uint64 binding_generation;
	uint64 request_sequence;
	uint64 admission_generation;
	uint64 buffer_generation_before;
	uint64 buffer_generation_after;
	uint64 buffer_token_before;
	uint64 buffer_token_after;
	uint64 buffer_writer_token_before;
	uint64 buffer_writer_token_after;
	uint64 buffer_resource_x_generation_before;
	uint64 buffer_resource_x_generation_after;
	uint32 buffer_flags_before;
	uint32 buffer_flags_after;
	int32 buffer_own_result;
	uint8 buffer_pcm_state_before;
	uint8 buffer_pcm_state_after;
	uint64 base_authority_generation;
	uint64 authority_generation;
	uint64 assertion_sequence;
	uint64 formation;
	uint64 master_session;
	uint64 r4_generation;
	uint64 absolute_deadline_us;
	uint32 round_progress_flags;
	uint8 round_phase;
	bool round_terminal;
	int32 requester_node;
	ResourceXRemoteSStage remote_s_stage;
	ResourceXSourceSettlementStage source_settlement_stage;
	ResourceXSourceSettlementCommitObservation source_settlement_commit;
	int32 source_settlement_own_result;
	ResourceXFailureDomain failure_domain;
	ResourceXApplyResult result;
	bool holder_mutation_started;
	bool revoke_reversible;
	bool rollback_cancel_attempted;
	bool rollback_cancel_ok;
	bool rollback_abort_attempted;
	bool rollback_abort_ok;
	bool status_staged;
	bool status_published;
	bool proof_staged;
	bool proof_published;
} ResourceXFirstFailureEvidence;

#define RESOURCE_X_FIRST_FAILURE_LOG_SLOTS 64
#define RESOURCE_X_FIRST_FAILURE_LOG_RATE_LIMIT 8
#define RESOURCE_X_FIRST_FAILURE_LOG_INTERVAL_US UINT64_C(1000000)

typedef struct ResourceXFirstFailureLogIdentity {
	BufferTag tag;
	uint64 request_sequence;
	uint64 admission_generation;
	uint64 assertion_sequence;
	uint64 formation;
	uint64 master_session;
	int32 requester_node;
	bool in_use;
} ResourceXFirstFailureLogIdentity;

/* Diagnostic suppression is deliberately process-local and bounded.  It is
 * neither shared authority nor a protocol registry.  Exact retransmission of
 * one wire request is logged once while resident; aggregate counters still
 * retain every later failure/cascade. */
static ResourceXFirstFailureLogIdentity
	resource_x_first_failure_log_slots[RESOURCE_X_FIRST_FAILURE_LOG_SLOTS];
static uint32 resource_x_first_failure_log_next_slot;
static uint64 resource_x_first_failure_log_window_start_us;
static uint32 resource_x_first_failure_log_window_count;
static uint64 resource_x_target_diagnostic_request_sequence;

static const char *
gcs_block_resource_x_rollback_outcome(bool attempted, bool ok)
{
	return !attempted ? "n/a" : ok ? "true" : "false";
}

static uint64
gcs_block_resource_x_next_diagnostic_request_sequence(void)
{
	resource_x_target_diagnostic_request_sequence++;
	if (resource_x_target_diagnostic_request_sequence == 0
		|| resource_x_target_diagnostic_request_sequence == UINT64_MAX)
		resource_x_target_diagnostic_request_sequence = 1;
	return resource_x_target_diagnostic_request_sequence;
}

static bool
gcs_block_resource_x_first_failure_should_log(
	const ResourceXFirstFailureEvidence *evidence, uint64 now_us)
{
	ResourceXFirstFailureLogIdentity identity;
	uint32 i;

	if (evidence == NULL || now_us == 0)
		return false;
	memset(&identity, 0, sizeof(identity));
	identity.tag = evidence->tag;
	identity.request_sequence = evidence->request_sequence;
	identity.admission_generation = evidence->admission_generation != 0
		? evidence->admission_generation : evidence->r4_generation;
	identity.assertion_sequence = evidence->assertion_sequence;
	identity.formation = evidence->formation;
	identity.master_session = evidence->master_session;
	identity.requester_node = evidence->requester_node;
	identity.in_use = true;
	for (i = 0; i < RESOURCE_X_FIRST_FAILURE_LOG_SLOTS; i++) {
		ResourceXFirstFailureLogIdentity *slot
			= &resource_x_first_failure_log_slots[i];

		if (slot->in_use
			&& BufferTagsEqual(&slot->tag, &identity.tag)
			&& slot->request_sequence == identity.request_sequence
			&& slot->admission_generation == identity.admission_generation
			&& slot->assertion_sequence == identity.assertion_sequence
			&& slot->formation == identity.formation
			&& slot->master_session == identity.master_session
			&& slot->requester_node == identity.requester_node)
			return false;
	}
	resource_x_first_failure_log_slots[resource_x_first_failure_log_next_slot]
		= identity;
	resource_x_first_failure_log_next_slot
		= (resource_x_first_failure_log_next_slot + 1)
		  % RESOURCE_X_FIRST_FAILURE_LOG_SLOTS;

	if (resource_x_first_failure_log_window_start_us == 0
		|| now_us < resource_x_first_failure_log_window_start_us
		|| now_us - resource_x_first_failure_log_window_start_us
			>= RESOURCE_X_FIRST_FAILURE_LOG_INTERVAL_US) {
		resource_x_first_failure_log_window_start_us = now_us;
		resource_x_first_failure_log_window_count = 0;
	}
	if (resource_x_first_failure_log_window_count
		>= RESOURCE_X_FIRST_FAILURE_LOG_RATE_LIMIT)
		return false;
	resource_x_first_failure_log_window_count++;
	return true;
}

/* D1 only records evidence and increments one domain counter.  It neither
 * maps the result nor fences/retries the request.  Callers invoke it at their
 * first local failure boundary after any exact rollback attempt has ended. */
static void
gcs_block_resource_x_first_failure_record(
	const ResourceXFirstFailureEvidence *evidence)
{
	PcmGrdLifecycleStats lifecycle;
	uint64 now_us;
	uint32 tag_hash;
	uint32 capacity_used;
	uint32 capacity_limit;
	int live_entries;

	if (evidence == NULL || evidence->failure_domain <= RESOURCE_X_FAIL_NONE
		|| evidence->failure_domain > RESOURCE_X_FAIL_INTERNAL_CORRUPTION)
		return;
	if (ClusterGcsBlock != NULL) {
		switch (evidence->failure_domain) {
		case RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE:
			pg_atomic_fetch_add_u64(
				&ClusterGcsBlock->resource_x_pre_mutation_backpressure_count, 1);
			break;
		case RESOURCE_X_FAIL_AUTHORITY_DRIFT:
			pg_atomic_fetch_add_u64(
				&ClusterGcsBlock->resource_x_authority_drift_count, 1);
			break;
		case RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY:
			pg_atomic_fetch_add_u64(
				&ClusterGcsBlock->resource_x_post_mutation_ambiguity_count, 1);
			break;
		case RESOURCE_X_FAIL_INTERNAL_CORRUPTION:
			pg_atomic_fetch_add_u64(
				&ClusterGcsBlock->resource_x_internal_corruption_count, 1);
			break;
		case RESOURCE_X_FAIL_NONE:
			break;
		}
	}
	now_us = gcs_block_pcm_x_monotonic_us();
	if (!gcs_block_resource_x_first_failure_should_log(evidence, now_us))
		return;
	cluster_pcm_grd_lifecycle_stats_snapshot(&lifecycle);
	live_entries = cluster_pcm_grd_count();
	capacity_used = live_entries > 0 ? (uint32)live_entries : 0;
	capacity_limit = cluster_pcm_grd_capacity() > 0
		? (uint32)cluster_pcm_grd_capacity() : 0;
	tag_hash = hash_any((const unsigned char *)&evidence->tag,
		(int)sizeof(evidence->tag));
	ereport(LOG,
		(errmsg_internal("Resource-X first-failure diagnostic"),
		 errdetail("tag_hash=%u binding_generation=%llu "
				   "request_sequence=%llu admission_generation=%llu "
				   "buffer_generation_before=%llu buffer_generation_after=%llu "
				   "buffer_token_before=%llu buffer_token_after=%llu "
				   "buffer_writer_token_before=%llu buffer_writer_token_after=%llu "
				   "buffer_resource_x_generation_before=%llu "
				   "buffer_resource_x_generation_after=%llu "
				   "buffer_flags_before=0x%08x buffer_flags_after=0x%08x "
				   "buffer_pcm_state_before=%u buffer_pcm_state_after=%u "
				   "buffer_own_result=%d "
				   "formation=%llu master_session=%llu r4_generation=%llu "
				   "base_authority_generation=%llu authority_generation=%llu "
				   "assertion_sequence=%llu remote_s_stage=%u "
				   "source_settlement_stage=%u source_settlement_own_result=%d "
				   "source_settlement_commit_stage=%u "
				   "source_settlement_commit_mismatch=0x%08x "
				   "source_settlement_current_formation=%llu "
				   "source_settlement_current_master=%d "
				   "source_settlement_current_session=%llu "
				   "source_settlement_current_terminal_authority=%llu "
				   "source_settlement_current_cached_generation=%llu "
				   "source_settlement_current_round_phase=%u "
				   "source_settlement_current_owner_state=%u "
				   "source_settlement_pair_formation=%llu "
				   "source_settlement_pair_session=%llu "
				   "source_settlement_pair_sequence=%llu "
				   "source_settlement_pair_source_generation=%llu "
				   "source_settlement_pair_observed_mode=%u "
				   "source_settlement_pair_destination=%u "
				   "source_settlement_pair_status_valid=%u "
				   "source_settlement_pair_image_valid=%u "
				   "source_settlement_status_intent=%u "
				   "source_settlement_image_intent=%u "
				   "failure_domain=%u "
				   "result=%d holder_mutation_started=%s revoke_reversible=%s "
				   "rollback_cancel_ok=%s rollback_abort_ok=%s "
				   "status_staged=%s status_published=%s "
				   "proof_staged=%s proof_published=%s "
				   "round_terminal=%s round_phase=%u round_progress_flags=0x%08x "
				   "capacity_kind=%s capacity_used=%u capacity_limit=%u "
				   "reclaim_attempts=%llu reclaim_successes=%llu "
				   "reclaim_reuses=%llu capacity_retries=%llu "
				   "capacity_failures=%llu refused_pcm_mode=%llu "
				   "refused_holder=%llu refused_pi=%llu refused_watermark=%llu "
				   "refused_resource_x=%llu refused_retained=%llu "
				   "refused_requester=%llu refused_sidecar=%llu "
				   "deadline=%llu",
				   tag_hash,
				   (unsigned long long)evidence->binding_generation,
				   (unsigned long long)evidence->request_sequence,
				   (unsigned long long)(evidence->admission_generation != 0
					   ? evidence->admission_generation
					   : evidence->r4_generation),
				   (unsigned long long)evidence->buffer_generation_before,
				   (unsigned long long)evidence->buffer_generation_after,
				   (unsigned long long)evidence->buffer_token_before,
				   (unsigned long long)evidence->buffer_token_after,
				   (unsigned long long)evidence->buffer_writer_token_before,
				   (unsigned long long)evidence->buffer_writer_token_after,
				   (unsigned long long)
					   evidence->buffer_resource_x_generation_before,
				   (unsigned long long)
					   evidence->buffer_resource_x_generation_after,
				   evidence->buffer_flags_before,
				   evidence->buffer_flags_after,
				   (unsigned)evidence->buffer_pcm_state_before,
				   (unsigned)evidence->buffer_pcm_state_after,
				   evidence->remote_s_stage != RESOURCE_X_REMOTE_S_STAGE_NONE
					   ? evidence->buffer_own_result : -1,
				   (unsigned long long)evidence->formation,
				   (unsigned long long)evidence->master_session,
				   (unsigned long long)evidence->r4_generation,
				   (unsigned long long)evidence->base_authority_generation,
				   (unsigned long long)evidence->authority_generation,
				   (unsigned long long)evidence->assertion_sequence,
				   (unsigned)evidence->remote_s_stage,
				   (unsigned)evidence->source_settlement_stage,
				   evidence->source_settlement_stage
						   != RESOURCE_X_SOURCE_SETTLEMENT_STAGE_NONE
					   ? evidence->source_settlement_own_result : -1,
				   (unsigned)evidence->source_settlement_commit.commit_stage,
				   evidence->source_settlement_commit.mismatch_mask,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_resource_formation,
				   evidence->source_settlement_commit.current_master_node,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_master_session,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_terminal_authority_generation,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_cached_ownership_generation,
				   (unsigned)evidence->source_settlement_commit
					   .current_round_phase,
				   (unsigned)evidence->source_settlement_commit.current_owner_state,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_pair_resource_formation,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_pair_master_session,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_pair_assertion_sequence,
				   (unsigned long long)evidence->source_settlement_commit
					   .current_pair_source_generation,
				   (unsigned)evidence->source_settlement_commit
					   .current_pair_observed_mode,
				   evidence->source_settlement_commit
					   .current_pair_destination_node,
				   (unsigned)evidence->source_settlement_commit
					   .current_holder_status_valid,
				   (unsigned)evidence->source_settlement_commit
					   .current_holder_image_valid,
				   (unsigned)evidence->source_settlement_commit
					   .current_status_intent_state,
				   (unsigned)evidence->source_settlement_commit
					   .current_image_intent_state,
				   (unsigned)evidence->failure_domain,
				   (int)evidence->result,
				   evidence->holder_mutation_started ? "true" : "false",
				   evidence->revoke_reversible ? "true" : "false",
				   gcs_block_resource_x_rollback_outcome(
					   evidence->rollback_cancel_attempted,
					   evidence->rollback_cancel_ok),
				   gcs_block_resource_x_rollback_outcome(
					   evidence->rollback_abort_attempted,
					   evidence->rollback_abort_ok),
				   evidence->status_staged ? "true" : "false",
				   evidence->status_published ? "true" : "false",
				   evidence->proof_staged ? "true" : "false",
				   evidence->proof_published ? "true" : "false",
				   evidence->round_terminal ? "true" : "false",
				   (unsigned)evidence->round_phase,
				   evidence->round_progress_flags,
				   "pcm_grd_live",
				   capacity_used,
				   capacity_limit,
				   (unsigned long long)lifecycle.reclaim_attempt_count,
				   (unsigned long long)lifecycle.reclaim_success_count,
				   (unsigned long long)lifecycle.reclaim_reuse_count,
				   (unsigned long long)lifecycle.capacity_retry_count,
				   (unsigned long long)lifecycle.capacity_fail_count,
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_PCM_MODE_NOT_N],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_HOLDER_PRESENT],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_PI_PRESENT],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_WATERMARK_PRESENT],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_RESOURCE_X_ACTIVE],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_RETAINED_PAIR_PRESENT],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL],
				   (unsigned long long)lifecycle.reclaim_refused[
					   PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL],
				   (unsigned long long)evidence->absolute_deadline_us)));
}

static ResourceXFailureDomain
gcs_block_resource_x_pre_mutation_domain(ClusterPcmOwnResult own_result)
{
	if (own_result == CLUSTER_PCM_OWN_BUSY
		|| own_result == CLUSTER_PCM_OWN_NOT_READY)
		return RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE;
	if (own_result == CLUSTER_PCM_OWN_STALE)
		return RESOURCE_X_FAIL_AUTHORITY_DRIFT;
	return RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
}

static ResourceXFailureDomain
gcs_block_resource_x_target_failure_domain(
	ResourceXApplyResult result, ResourceXApplyResult round_snapshot_result,
	const ResourceXBootstrapRoundFailureSnapshot *round,
	ClusterPcmOwnResult live_snapshot_result)
{
	if (live_snapshot_result == CLUSTER_PCM_OWN_CORRUPT
		|| live_snapshot_result == CLUSTER_PCM_OWN_NOT_READY
		|| round_snapshot_result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| round_snapshot_result == RESOURCE_X_APPLY_INVALID)
		return RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
	if (round_snapshot_result == RESOURCE_X_APPLY_STALE
		|| result == RESOURCE_X_APPLY_STALE)
		return RESOURCE_X_FAIL_AUTHORITY_DRIFT;
	if (round_snapshot_result == RESOURCE_X_APPLY_APPLIED
		&& round != NULL) {
		if ((round->progress_flags & RESOURCE_X_PROGRESS_T2) != 0)
			return RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY;
		if (round->terminal != 0
			|| (round->progress_flags & RESOURCE_X_PROGRESS_T1) != 0)
			return RESOURCE_X_FAIL_AUTHORITY_DRIFT;
	}
	if (result == RESOURCE_X_APPLY_BAD_STATE
		|| result == RESOURCE_X_APPLY_NOT_FOUND)
		return RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE;
	return RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
}

static void
gcs_block_resource_x_first_failure_from_block(
	ResourceXFirstFailureEvidence *evidence,
	const ResourceXDecodedFrame *block, ResourceXRemoteSStage stage,
	ResourceXFailureDomain domain, ResourceXApplyResult result)
{
	memset(evidence, 0, sizeof(*evidence));
	if (block != NULL) {
		evidence->tag = block->common.logical_assertion.resource;
		evidence->base_authority_generation
			= block->common.base_authority_generation;
		evidence->authority_generation
			= block->common.authority_generation;
		evidence->assertion_sequence
			= block->common.assertion_sequence;
		evidence->request_sequence = block->common.assertion_sequence;
		evidence->requester_node
			= block->common.logical_assertion.requester_node;
		evidence->formation = block->common.resource_formation;
		evidence->master_session
			= block->common.master_session_incarnation;
	}
	evidence->remote_s_stage = stage;
	evidence->buffer_own_result = -1;
	evidence->source_settlement_own_result = -1;
	evidence->failure_domain = domain;
	evidence->result = result;
}

static ResourceXFailureDomain
gcs_block_resource_x_source_settlement_apply_domain(
	ResourceXApplyResult result)
{
	if (result == RESOURCE_X_APPLY_BAD_STATE)
		return RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE;
	if (result == RESOURCE_X_APPLY_STALE)
		return RESOURCE_X_FAIL_AUTHORITY_DRIFT;
	return RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
}

static ResourceXFailureDomain
gcs_block_resource_x_source_settlement_own_domain(
	ClusterPcmOwnResult own_result, bool release_applied)
{
	if (own_result == CLUSTER_PCM_OWN_CORRUPT
		|| own_result == CLUSTER_PCM_OWN_INVALID)
		return RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
	if (release_applied)
		return RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY;
	if (own_result == CLUSTER_PCM_OWN_BUSY
		|| own_result == CLUSTER_PCM_OWN_NOT_READY)
		return RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE;
	if (own_result == CLUSTER_PCM_OWN_STALE)
		return RESOURCE_X_FAIL_AUTHORITY_DRIFT;
	return RESOURCE_X_FAIL_INTERNAL_CORRUPTION;
}

static void
gcs_block_resource_x_source_settlement_failure_record(
	const ResourceXDecodedFrame *settlement,
	const ResourceXSourceSettlementCommitObservation *commit_observation,
	ResourceXSourceSettlementStage source_settlement_stage,
	ResourceXFailureDomain failure_domain, ResourceXApplyResult result,
	uint64 source_generation, int32 source_settlement_own_result,
	bool release_applied, bool status_staged)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	ResourceXFirstFailureEvidence first_failure;

	gcs_block_resource_x_first_failure_from_block(
		&first_failure, settlement, RESOURCE_X_REMOTE_S_STAGE_NONE,
		failure_domain, result);
	first_failure.source_settlement_stage = source_settlement_stage;
	first_failure.source_settlement_own_result
		= source_settlement_own_result;
	if (commit_observation != NULL)
		first_failure.source_settlement_commit = *commit_observation;
	first_failure.binding_generation = source_generation;
	first_failure.holder_mutation_started = release_applied;
	first_failure.status_staged = status_staged;
	if (settlement != NULL
		&& cluster_semantic_activation_r11_cutover_snapshot(&cutover)
		&& cutover.formation_epoch
			== settlement->common.resource_formation) {
		first_failure.admission_generation = cutover.record_generation;
		first_failure.r4_generation = cutover.record_generation;
	}
	gcs_block_resource_x_first_failure_record(&first_failure);
}

/* PGRAC adaptation approved for the current happy path only.  A
 * non-requester S holder (remote from the requester, including the
 * master/holder same-node transport shape) has no local Resource-X authority
 * entry.  The
 * authenticated exact type-17 supplies the master-side instruction, while
 * the resident BufferDesc supplies only local S state/generation evidence.
 * The existing LMS DATA ring first retains an unsendable status, then the
 * exact REVOKING S tuple commits to N under content EXCLUSIVE, and only after
 * unlock does that same slot become sendable.  No shadow authority or
 * registry exists. */
static ResourceXApplyResult
gcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(
	const ResourceXDecodedFrame *block,
	int32 authenticated_master_node,
	uint32 authenticated_capability_generation,
	uint64 r4_record_generation,
	const ClusterSemanticAdmissionToken *admission)
{
	BufferDesc *buf;
	ClusterPcmOwnResult own_result;
	ClusterPcmOwnResult abort_result;
	ClusterPcmOwnResult cancel_result;
	ClusterPcmOwnSnapshot current;
	ClusterPcmOwnSnapshot released;
	ClusterPcmOwnSnapshot revalidated;
	ClusterPcmOwnSnapshot revoking;
	ClusterLmsRemoteSStatusHandle status_handle;
	LWLock *content_lock;
	ResourceXDecodedFrame status;
	ResourceXFirstFailureEvidence first_failure;
	ResourceXRemoteSStage remote_s_stage
		= RESOURCE_X_REMOTE_S_STAGE_VALIDATE;
	ResourceXFailureDomain failure_domain = RESOURCE_X_FAIL_NONE;
	ResourceXRemoteSFailureDecision failure_decision;
	ResourceXApplyResult mapped_result;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_CONTROL_V1_BYTES];
	uint16 payload_len = 0;
	uint32 outbound_connection_generation = 0;
	uint32 rechecked_outbound_connection_generation = 0;
	int buffer_id = -1;
	int worker_id;
	bool rollback_cancel_ok = false;
	bool rollback_abort_ok = false;

	if (block == NULL || block->kind != RESOURCE_X_WIRE_BLOCK_TO_N
		|| !resource_x_assertion_valid(&block->common.logical_assertion)
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_capability_generation == 0
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| admission == NULL || !admission->entered
		|| admission->record_generation != r4_record_generation
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| block->common.logical_assertion.requester_node == cluster_node_id
		|| block->common.action_node != cluster_node_id
		|| block->common.base_authority_generation == 0
		|| block->common.base_authority_generation == UINT64_MAX
		|| block->common.authority_generation
			   != block->common.base_authority_generation
		|| block->common.resource_formation == 0
		|| block->common.master_session_incarnation == 0
		|| block->common.assertion_sequence == 0
		|| block->common.sender_connection_generation == 0
		|| block->common.observed_mode != (uint8)PCM_STATE_S
		|| block->common.target_mode != (uint8)PCM_STATE_N
		|| block->common.source_candidate != 0
		|| block->common.retain_pi_if_dirty != 0
		|| block->common.outcome != RESOURCE_X_OUTCOME_NONE
		|| cluster_gcs_lookup_master(
			   block->common.logical_assertion.resource)
			   != authenticated_master_node
		|| !cluster_pcm_lock_resource_x_gate_open_exact(
			block->common.resource_formation))
		return RESOURCE_X_APPLY_INVALID;
	if (!gcs_block_resource_x_peer_session_matches_exact(
			&block->common.logical_assertion.resource,
			authenticated_master_node,
			block->common.master_session_incarnation)) {
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, RESOURCE_X_FAIL_AUTHORITY_DRIFT,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage,
			failure_decision.domain, RESOURCE_X_APPLY_STALE);
		first_failure.r4_generation = r4_record_generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return RESOURCE_X_APPLY_STALE;
	}

	remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_SNAPSHOT;
	memset(&current, 0, sizeof(current));
	own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(
		&block->common.logical_assertion.resource, &buffer_id, &current);
	if (own_result != CLUSTER_PCM_OWN_OK || buffer_id < 0) {
		if (own_result == CLUSTER_PCM_OWN_OK)
			own_result = CLUSTER_PCM_OWN_CORRUPT;
		mapped_result = gcs_block_resource_x_remote_s_own_result(own_result);
		failure_domain = gcs_block_resource_x_pre_mutation_domain(own_result);
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, failure_domain,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return mapped_result;
	}
	if (current.pcm_state == (uint8)PCM_STATE_N
		&& current.flags == PCM_OWN_FLAG_GRANT_PENDING) {
		own_result
			= cluster_pcm_x_remote_s_holder_pending_grant_result(&current);
		mapped_result = gcs_block_resource_x_remote_s_own_result(own_result);
		failure_domain = gcs_block_resource_x_pre_mutation_domain(own_result);
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, failure_domain,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = current.generation;
		first_failure.buffer_generation_after = current.generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return mapped_result;
	}
	buf = GetBufferDescriptor(buffer_id);
	if (current.pcm_state == (uint8)PCM_STATE_N) {
		/* A prior exact S->N may have left its status on an old DATA
		 * connection.  Revalidate the stable N tuple, then re-emit only the
		 * existing status shape on the current connection.  This branch never
		 * claims a BufferDesc/PCM owner or changes the local page state. */
		memset(&revalidated, 0, sizeof(revalidated));
		own_result
			= cluster_pcm_x_remote_s_holder_stable_n_result(&current);
		if (own_result == CLUSTER_PCM_OWN_OK)
			own_result = cluster_bufmgr_pcm_own_n_assertion_candidate_exact(
				buf, &current, &revalidated);
		if (own_result != CLUSTER_PCM_OWN_OK) {
			mapped_result
				= gcs_block_resource_x_remote_s_own_result(own_result);
			failure_domain
				= gcs_block_resource_x_pre_mutation_domain(own_result);
			failure_decision
				= cluster_gcs_resource_x_remote_s_failure_decide(
					remote_s_stage, failure_domain,
					false, false, false, false);
			gcs_block_resource_x_first_failure_from_block(
				&first_failure, block, remote_s_stage,
				failure_decision.domain, mapped_result);
			first_failure.r4_generation = r4_record_generation;
			first_failure.buffer_generation_before = current.generation;
			first_failure.buffer_generation_after = revalidated.generation;
			first_failure.buffer_token_before = current.reservation_token;
			first_failure.buffer_token_after = revalidated.reservation_token;
			first_failure.buffer_writer_token_before
				= current.writer_activation_token;
			first_failure.buffer_writer_token_after
				= revalidated.writer_activation_token;
			first_failure.buffer_resource_x_generation_before
				= current.resource_x_activation_generation;
			first_failure.buffer_resource_x_generation_after
				= revalidated.resource_x_activation_generation;
			first_failure.buffer_flags_before = current.flags;
			first_failure.buffer_flags_after = revalidated.flags;
			first_failure.buffer_pcm_state_before = current.pcm_state;
			first_failure.buffer_pcm_state_after = revalidated.pcm_state;
			first_failure.buffer_own_result = (int32)own_result;
			gcs_block_resource_x_first_failure_record(&first_failure);
			gcs_block_resource_x_failure_decision_apply(&failure_decision);
			return mapped_result;
		}
		if (!cluster_semantic_activation_recheck(admission)
			|| ((authenticated_master_node == cluster_node_id)
				&& (cluster_ic_local_capability_word()
					& PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) == 0)
			|| ((authenticated_master_node != cluster_node_id)
				&& !cluster_sf_peer_capability_generation_matches(
					authenticated_master_node,
					PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
					authenticated_capability_generation))
			|| !cluster_pcm_lock_resource_x_gate_open_exact(
				block->common.resource_formation)
			|| !gcs_block_resource_x_peer_session_matches_exact(
				&block->common.logical_assertion.resource,
				authenticated_master_node,
				block->common.master_session_incarnation)
			|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
				authenticated_master_node,
				&outbound_connection_generation)) {
			failure_decision
				= cluster_gcs_resource_x_remote_s_failure_decide(
					remote_s_stage, RESOURCE_X_FAIL_AUTHORITY_DRIFT,
					false, false, false, false);
			gcs_block_resource_x_first_failure_from_block(
				&first_failure, block, remote_s_stage,
				failure_decision.domain, RESOURCE_X_APPLY_STALE);
			first_failure.r4_generation = r4_record_generation;
			first_failure.buffer_generation_before = current.generation;
			first_failure.buffer_generation_after = revalidated.generation;
			gcs_block_resource_x_first_failure_record(&first_failure);
			gcs_block_resource_x_failure_decision_apply(&failure_decision);
			return RESOURCE_X_APPLY_STALE;
		}
		memset(&status, 0, sizeof(status));
		status.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
		status.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
		status.common = block->common;
		status.common.action_node = cluster_node_id;
		status.common.source_candidate = 0;
		status.common.retain_pi_if_dirty = 0;
		status.common.sender_connection_generation
			= outbound_connection_generation;
		status.common.outcome = RESOURCE_X_OUTCOME_OK;
		if (!cluster_resource_x_wire_encode(
				RESOURCE_X_MSG_BLOCKED_TO_N, &status, payload,
				sizeof(payload), &payload_len, &reject)
			|| payload_len != RESOURCE_X_CONTROL_V1_BYTES) {
			failure_decision
				= cluster_gcs_resource_x_remote_s_failure_decide(
					remote_s_stage, RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
					false, false, false, false);
			gcs_block_resource_x_first_failure_from_block(
				&first_failure, block, remote_s_stage,
				failure_decision.domain,
				RESOURCE_X_APPLY_RECOVERY_BLOCKED);
			first_failure.r4_generation = r4_record_generation;
			first_failure.buffer_generation_before = current.generation;
			first_failure.buffer_generation_after = revalidated.generation;
			gcs_block_resource_x_first_failure_record(&first_failure);
			gcs_block_resource_x_failure_decision_apply(&failure_decision);
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		worker_id = cluster_lms_shard_for_tag(
			&block->common.logical_assertion.resource, cluster_lms_workers);
		if (worker_id < 0 || worker_id >= cluster_lms_workers) {
			failure_decision
				= cluster_gcs_resource_x_remote_s_failure_decide(
					remote_s_stage, RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
					false, false, false, false);
			gcs_block_resource_x_first_failure_from_block(
				&first_failure, block, remote_s_stage,
				failure_decision.domain,
				RESOURCE_X_APPLY_RECOVERY_BLOCKED);
			first_failure.r4_generation = r4_record_generation;
			first_failure.buffer_generation_before = current.generation;
			first_failure.buffer_generation_after = revalidated.generation;
			gcs_block_resource_x_first_failure_record(&first_failure);
			gcs_block_resource_x_failure_decision_apply(&failure_decision);
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		if (!cluster_semantic_activation_recheck(admission)
			|| !cluster_pcm_lock_resource_x_gate_open_exact(
				block->common.resource_formation)
			|| !gcs_block_resource_x_peer_session_matches_exact(
				&block->common.logical_assertion.resource,
				authenticated_master_node,
				block->common.master_session_incarnation)
			|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
				authenticated_master_node,
				&rechecked_outbound_connection_generation)
			|| rechecked_outbound_connection_generation
				!= outbound_connection_generation) {
			failure_decision
				= cluster_gcs_resource_x_remote_s_failure_decide(
					remote_s_stage, RESOURCE_X_FAIL_AUTHORITY_DRIFT,
					false, false, false, false);
			gcs_block_resource_x_first_failure_from_block(
				&first_failure, block, remote_s_stage,
				failure_decision.domain, RESOURCE_X_APPLY_STALE);
			first_failure.r4_generation = r4_record_generation;
			first_failure.buffer_generation_before = current.generation;
			first_failure.buffer_generation_after = revalidated.generation;
			gcs_block_resource_x_first_failure_record(&first_failure);
			gcs_block_resource_x_failure_decision_apply(&failure_decision);
			return RESOURCE_X_APPLY_STALE;
		}
		if (!cluster_lms_outbound_enqueue_cap_bound(
				worker_id, RESOURCE_X_MSG_BLOCKED_TO_N,
				(uint32)authenticated_master_node, payload, payload_len,
				PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
				outbound_connection_generation)) {
			failure_decision
				= cluster_gcs_resource_x_remote_s_failure_decide(
					remote_s_stage,
					RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE,
					false, false, false, false);
			gcs_block_resource_x_first_failure_from_block(
				&first_failure, block, remote_s_stage,
				failure_decision.domain, RESOURCE_X_APPLY_BAD_STATE);
			first_failure.r4_generation = r4_record_generation;
			first_failure.buffer_generation_before = current.generation;
			first_failure.buffer_generation_after = revalidated.generation;
			gcs_block_resource_x_first_failure_record(&first_failure);
			gcs_block_resource_x_failure_decision_apply(&failure_decision);
			return RESOURCE_X_APPLY_BAD_STATE;
		}
		return RESOURCE_X_APPLY_APPLIED;
	}
	own_result = cluster_bufmgr_pcm_own_s_holder_candidate_exact(
		buf, &current);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		mapped_result = gcs_block_resource_x_remote_s_own_result(own_result);
		failure_domain = gcs_block_resource_x_pre_mutation_domain(own_result);
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, failure_domain,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = current.generation;
		first_failure.buffer_generation_after = current.generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return mapped_result;
	}

	memset(&status, 0, sizeof(status));
	status.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	status.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	status.common = block->common;
	status.common.action_node = cluster_node_id;
	status.common.source_candidate = 0;
	status.common.retain_pi_if_dirty = 0;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_BLOCKED_TO_N, &status, payload,
			sizeof(payload), &payload_len, &reject)
			|| payload_len != RESOURCE_X_CONTROL_V1_BYTES) {
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage,
			failure_decision.domain,
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = current.generation;
		first_failure.buffer_generation_after = current.generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	worker_id = cluster_lms_shard_for_tag(
		&block->common.logical_assertion.resource, cluster_lms_workers);
	if (worker_id < 0 || worker_id >= cluster_lms_workers) {
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage,
			failure_decision.domain,
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = current.generation;
		first_failure.buffer_generation_after = current.generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}

	content_lock = BufferDescriptorGetContentLock(buf);
	if (!LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)) {
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage,
				RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage,
			failure_decision.domain,
			RESOURCE_X_APPLY_BAD_STATE);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = current.generation;
		first_failure.buffer_generation_after = current.generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	memset(&revalidated, 0, sizeof(revalidated));
	memset(&revoking, 0, sizeof(revoking));
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &revalidated);
	if (own_result == CLUSTER_PCM_OWN_OK
		&& memcmp(&current, &revalidated, sizeof(current)) != 0)
		own_result = CLUSTER_PCM_OWN_STALE;
	if (own_result == CLUSTER_PCM_OWN_OK)
		own_result = cluster_bufmgr_pcm_own_s_holder_candidate_exact(
			buf, &revalidated);
	if (own_result == CLUSTER_PCM_OWN_OK)
		own_result = cluster_bufmgr_pcm_own_begin_s_revoke(
			buf, &revalidated, &revoking);
	LWLockRelease(content_lock);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		mapped_result = gcs_block_resource_x_remote_s_own_result(own_result);
		failure_domain = gcs_block_resource_x_pre_mutation_domain(own_result);
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, failure_domain,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = current.generation;
		first_failure.buffer_generation_after = revalidated.generation;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return mapped_result;
	}
	remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_REVOKE_HELD;

	memset(&status_handle, 0, sizeof(status_handle));
	own_result = cluster_lms_outbound_stage_resource_x_remote_s_status_exact(
		worker_id, (uint32)authenticated_master_node,
		payload, payload_len, &revoking, &status_handle);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		abort_result = cluster_bufmgr_pcm_own_abort_s_revoke(buf, &revoking);
		rollback_abort_ok = abort_result == CLUSTER_PCM_OWN_OK;
		mapped_result = rollback_abort_ok
			? gcs_block_resource_x_remote_s_own_result(own_result)
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		failure_domain = gcs_block_resource_x_pre_mutation_domain(own_result);
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, failure_domain,
				false, false, true, rollback_abort_ok);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = revoking.generation;
		first_failure.buffer_generation_after = current.generation;
		first_failure.holder_mutation_started = true;
		first_failure.revoke_reversible = true;
		first_failure.rollback_abort_attempted = true;
		first_failure.rollback_abort_ok = rollback_abort_ok;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		if (!failure_decision.rollback_complete)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		return mapped_result;
	}
	remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_PENDING_STAGED;

	/* The type-17's authority is sampled again after cross-domain staging.
	 * A drift cancels the unsendable slot before reopening the local S tuple. */
	if (((authenticated_master_node == cluster_node_id)
		 && (cluster_ic_local_capability_word()
			 & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) == 0)
		|| (authenticated_master_node != cluster_node_id
			&& !cluster_sf_peer_capability_generation_matches(
				authenticated_master_node,
				PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
				authenticated_capability_generation))
		|| !cluster_pcm_lock_resource_x_gate_open_exact(
			block->common.resource_formation)
		|| !gcs_block_resource_x_peer_session_matches_exact(
			&block->common.logical_assertion.resource,
			authenticated_master_node,
			block->common.master_session_incarnation)) {
		cancel_result
			= cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(
				&status_handle);
		abort_result = cluster_bufmgr_pcm_own_abort_s_revoke(buf, &revoking);
		rollback_cancel_ok = cancel_result == CLUSTER_PCM_OWN_OK;
		rollback_abort_ok = abort_result == CLUSTER_PCM_OWN_OK;
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, RESOURCE_X_FAIL_AUTHORITY_DRIFT,
				true, rollback_cancel_ok, true, rollback_abort_ok);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			RESOURCE_X_APPLY_STALE);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = revoking.generation;
		first_failure.buffer_generation_after = current.generation;
		first_failure.holder_mutation_started = true;
		first_failure.revoke_reversible = true;
		first_failure.rollback_cancel_attempted = true;
		first_failure.rollback_cancel_ok = rollback_cancel_ok;
		first_failure.rollback_abort_attempted = true;
		first_failure.rollback_abort_ok = rollback_abort_ok;
		first_failure.status_staged = true;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return failure_decision.rollback_complete
			? RESOURCE_X_APPLY_STALE
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}

	if (!LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)) {
		cancel_result
			= cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(
				&status_handle);
		abort_result = cluster_bufmgr_pcm_own_abort_s_revoke(buf, &revoking);
		rollback_cancel_ok = cancel_result == CLUSTER_PCM_OWN_OK;
		rollback_abort_ok = abort_result == CLUSTER_PCM_OWN_OK;
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage,
				RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE,
				true, rollback_cancel_ok, true, rollback_abort_ok);
		mapped_result = rollback_cancel_ok && rollback_abort_ok
			? RESOURCE_X_APPLY_BAD_STATE
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = revoking.generation;
		first_failure.buffer_generation_after = current.generation;
		first_failure.holder_mutation_started = true;
		first_failure.revoke_reversible = true;
		first_failure.rollback_cancel_attempted = true;
		first_failure.rollback_cancel_ok = rollback_cancel_ok;
		first_failure.rollback_abort_attempted = true;
		first_failure.rollback_abort_ok = rollback_abort_ok;
		first_failure.status_staged = true;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return failure_decision.rollback_complete
			? RESOURCE_X_APPLY_BAD_STATE
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	memset(&released, 0, sizeof(released));
	own_result = cluster_bufmgr_pcm_own_finish_remote_s_block_to_n(
		buf, &revoking, &released);
	LWLockRelease(content_lock);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		cancel_result
			= cluster_lms_outbound_cancel_resource_x_remote_s_status_exact(
				&status_handle);
		abort_result = cluster_bufmgr_pcm_own_abort_s_revoke(buf, &revoking);
		rollback_cancel_ok = cancel_result == CLUSTER_PCM_OWN_OK;
		rollback_abort_ok = abort_result == CLUSTER_PCM_OWN_OK;
		mapped_result = rollback_cancel_ok && rollback_abort_ok
			? gcs_block_resource_x_remote_s_own_result(own_result)
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		failure_domain = gcs_block_resource_x_pre_mutation_domain(own_result);
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage, failure_domain,
				true, rollback_cancel_ok, true, rollback_abort_ok);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage, failure_decision.domain,
			mapped_result);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = revoking.generation;
		first_failure.buffer_generation_after = current.generation;
		first_failure.holder_mutation_started = true;
		first_failure.revoke_reversible = true;
		first_failure.rollback_cancel_attempted = true;
		first_failure.rollback_cancel_ok = rollback_cancel_ok;
		first_failure.rollback_abort_attempted = true;
		first_failure.rollback_abort_ok = rollback_abort_ok;
		first_failure.status_staged = true;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return failure_decision.rollback_complete
			? mapped_result : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_COMMITTED_N;
	own_result
		= cluster_lms_outbound_publish_resource_x_remote_s_status_exact(
			&status_handle, &released);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		failure_decision
			= cluster_gcs_resource_x_remote_s_failure_decide(
				remote_s_stage,
				RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY,
				false, false, false, false);
		gcs_block_resource_x_first_failure_from_block(
			&first_failure, block, remote_s_stage,
			failure_decision.domain,
			RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		first_failure.r4_generation = r4_record_generation;
		first_failure.buffer_generation_before = revoking.generation;
		first_failure.buffer_generation_after = released.generation;
		first_failure.holder_mutation_started = true;
		first_failure.status_staged = true;
		gcs_block_resource_x_first_failure_record(&first_failure);
		gcs_block_resource_x_failure_decision_apply(&failure_decision);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_READY_PUBLISHED;
	return RESOURCE_X_APPLY_APPLIED;
}

static bool
gcs_block_resource_x_target_peer_matches_exact(
	const ClusterSemanticAdmissionToken *admission, int32 peer_node,
	uint32 authenticated_connection_generation)
{
	if (admission == NULL || !admission->entered
		|| admission->feature_bit
			!= CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1
		|| admission->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| admission->record_generation == 0
		|| admission->record_generation == UINT64_MAX
		|| admission->formation_epoch != cluster_epoch_get_current()
		|| peer_node < 0 || peer_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_connection_generation == 0)
		return false;
	if (peer_node == cluster_node_id)
		return (cluster_ic_local_capability_word()
				& PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) != 0;
	return cluster_semantic_activation_resource_x_peer_open_matches(
		admission, peer_node, authenticated_connection_generation);
}

/* Called only under a current target admission in the existing LMS process.
 * Claim creation precedes PG_TRY, so every ordinary unwind returns exactly
 * that owner; process death without unwind stays frozen recovery debt. */
static ResourceXApplyResult
gcs_block_resource_x_delivery_run_exact(const ResourceXAcquisitionRef *ref,
										const ClusterSemanticAdmissionToken *admission,
										const ResourceXGateSnapshot *gate, int32 master_node,
										uint64 master_session, uint32 master_ingress)
{
	ResourceXInstallClaimJoinObservation observation;
	ResourceXDeliveryTarget target;
	ResourceXDeliveryClaim claim;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal;
	ResourceXApplyResult result;
	uint64 ownership = 0, authority = 0;
	uint64 now_us = gcs_block_pcm_x_monotonic_us();

	result = cluster_pcm_lock_resource_x_delivery_target_snapshot_exact(ref, &observation, &target);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	if (observation.r4_record_generation != admission->record_generation
		|| observation.master_node != master_node
		|| observation.request.master_session_incarnation != master_session
		|| observation.request.resource_formation != gate->formation
		|| observation.master_ingress_connection_generation != master_ingress
		|| observation.request.sender_connection_generation != master_ingress) {
		cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_NAMESPACE_BLOCKED);
		return RESOURCE_X_APPLY_STALE;
	}
	result = cluster_pcm_lock_resource_x_delivery_claim_begin_exact(
		ref, &target, RESOURCE_X_DELIVERY_NORMAL, now_us, &claim);
	if (result == RESOURCE_X_APPLY_BAD_STATE)
		result = cluster_pcm_lock_resource_x_delivery_claim_begin_exact(
			ref, &target, RESOURCE_X_DELIVERY_CLEANUP, now_us, &claim);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	PG_TRY();
	{
		if (!cluster_semantic_activation_recheck(admission)
			|| !gcs_block_resource_x_gate_session_recheck(&ref->assertion.resource, gate,
														  master_node, master_session)
			|| !gcs_block_resource_x_target_peer_matches_exact(admission, master_node,
															   master_ingress)) {
			cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_NAMESPACE_BLOCKED);
			result = RESOURCE_X_APPLY_STALE;
		} else if (cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
					   ref, master_session, admission->record_generation, target.generation + 1))
			result = gcs_block_resource_x_delivery_finish_exact(&claim);
		else {
			result = gcs_block_pcm_x_resource_x_join_terminal_try(
				&ref->assertion, true, &claim, &terminal, &ownership, &authority);
			if (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
				result = gcs_block_resource_x_delivery_finish_exact(&claim);
			else if ((result == RESOURCE_X_APPLY_NOT_FOUND || result == RESOURCE_X_APPLY_BAD_STATE)
					 && cluster_semantic_activation_recheck(admission)
					 && gcs_block_resource_x_gate_session_recheck(&ref->assertion.resource, gate,
																  master_node, master_session)
					 && gcs_block_resource_x_target_peer_matches_exact(admission, master_node,
																	   master_ingress)
					 && cluster_pcm_lock_resource_x_delivery_dispatch_exact(&claim, &dispatch)
							== RESOURCE_X_APPLY_APPLIED) {
				/* Retry only the original kind9/ASSERT identity. The master
				 * redrives its selected source from immutable retained state. */
				if ((dispatch.kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP
					 && gcs_block_resource_x_bootstrap_request_stage_exact(master_node, &dispatch))
					|| (dispatch.kind == RESOURCE_X_WIRE_ASSERT_X
						&& gcs_block_resource_x_native_assert_stage_exact(master_node, &dispatch)
							   == RESOURCE_X_APPLY_APPLIED))
					cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_REQUEST_RETRY);
			}
		}
	}
	PG_FINALLY();
	{
		(void)cluster_pcm_lock_resource_x_delivery_claim_end_exact(&claim);
	}
	PG_END_TRY();
	return result;
}

ResourceXApplyResult
cluster_gcs_block_resource_x_delivery_tick(const ResourceXAcquisitionRef *ref)
{
	ClusterSemanticAdmissionToken admission;
	ResourceXGateSnapshot gate;
	volatile ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	uint64 session = 0;
	uint64 started_us;
	int32 master_node = -1;
	uint32 master_ingress = 0;

	if (MyBackendType != B_LMS || ref == NULL || ref->assertion.requester_node != cluster_node_id)
		return RESOURCE_X_APPLY_INVALID;
	memset(&admission, 0, sizeof(admission));
	if (cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
										  CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK) {
		cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_NAMESPACE_BLOCKED);
		return RESOURCE_X_APPLY_STALE;
	}
	started_us = gcs_block_pcm_x_monotonic_us();
	cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_CALLBACK);
	PG_TRY();
	{
		if (gcs_block_resource_x_gate_session_snapshot(&ref->assertion.resource, &gate,
													   &master_node, &session)
			&& gate.formation == ref->formation
			&& gcs_block_pcm_x_resource_x_peer_ready_exact(master_node, &master_ingress)
			&& gcs_block_resource_x_target_peer_matches_exact(&admission, master_node,
															  master_ingress))
			result = gcs_block_resource_x_delivery_run_exact(ref, &admission, &gate, master_node,
															 session, master_ingress);
		else
			cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_NAMESPACE_BLOCKED);
	}
	PG_FINALLY();
	{
		ResourceXTraceEvent event = { 0 };
		uint64 ended_us = gcs_block_pcm_x_monotonic_us();

		cluster_semantic_activation_leave(&admission);
		event.kind = RESOURCE_X_TRACE_APPLY;
		event.detail = RESOURCE_X_DELIVERY_TRACE_CALLBACK;
		event.assertion = ref->assertion;
		event.formation = ref->formation;
		event.attempt = ref->acquisition_generation;
		event.flags = (uint32)result;
		if (started_us != 0 && ended_us != UINT64_MAX && ended_us >= started_us) {
			event.value[0] = ended_us - started_us;
			cluster_pcm_rx_delivery_max_note(PCM_RX_DELIVERY_CALLBACK_MAX_US, event.value[0]);
		}
		cluster_pcm_lock_resource_x_trace_note(&event);
	}
	PG_END_TRY();
	if (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
		gcs_block_resource_x_stage_ready_tag(&ref->assertion.resource);
	return result;
}

/* Normal admission has already rejected this late frame. The LMS delivery
 * owner may retain it under its own explicit cleanup claim, using the same
 * authenticated coordinates. It creates neither an image arena nor an ACK. */
static ResourceXApplyResult
gcs_block_resource_x_cleanup_frame_exact(const ResourceXDecodedFrame *frame, int32 source,
										 uint32 source_ingress, int32 master_node,
										 uint32 master_ingress, uint64 r4_record_generation,
										 ResourceXRequesterJoinSnapshot *join_out,
										 ResourceXDecodedFrame *assertion_out)
{
	ResourceXAcquisitionRef ref;
	ResourceXInstallClaimJoinObservation observation;
	ResourceXDeliveryTarget target;
	ResourceXDeliveryClaim claim;
	ClusterPcmOwnSnapshot held;
	ResourceXApplyResult result;
	uint64 now_us = gcs_block_pcm_x_monotonic_us();

	if (MyBackendType != B_LMS || frame == NULL
		|| frame->common.logical_assertion.requester_node != cluster_node_id
		|| (frame->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
			&& frame->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
			&& frame->kind != RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP))
		return RESOURCE_X_APPLY_STALE;
	ref.assertion = frame->common.logical_assertion;
	ref.formation = frame->common.resource_formation;
	ref.acquisition_generation = frame->common.assertion_sequence;
	result
		= cluster_pcm_lock_resource_x_delivery_target_snapshot_exact(&ref, &observation, &target);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	if (observation.r4_record_generation != r4_record_generation
		|| observation.master_node != master_node
		|| observation.master_ingress_connection_generation != master_ingress
		|| observation.request.master_session_incarnation
			   != frame->common.master_session_incarnation
		|| target.buffer_id_plus_one == 0 || target.buffer_id_plus_one > (uint32)NBuffers)
		return RESOURCE_X_APPLY_STALE;
	result = cluster_pcm_lock_resource_x_delivery_claim_begin_exact(
		&ref, &target, RESOURCE_X_DELIVERY_CLEANUP, now_us, &claim);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	PG_TRY();
	{
		if (cluster_bufmgr_pcm_own_delivery_snapshot_exact(
				GetBufferDescriptor(target.buffer_id_plus_one - 1), &ref.assertion.resource,
				ref.acquisition_generation, &held)
				!= CLUSTER_PCM_OWN_OK
			|| (held.generation != target.generation && held.generation != target.generation + 1))
			result = RESOURCE_X_APPLY_STALE;
		else if (frame->kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP) {
			ResourceXBootstrapRoundAction action;

			action = cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_delivery_exact(
				frame, source, source_ingress, r4_record_generation, now_us, &claim, assertion_out);
			result = action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT ? RESOURCE_X_APPLY_APPLIED
																		  : RESOURCE_X_APPLY_STALE;
		} else
			result = cluster_pcm_lock_resource_x_requester_join_delivery_exact(
				frame, source, source_ingress, master_node, master_ingress, r4_record_generation,
				&claim, join_out);
	}
	PG_FINALLY();
	{
		(void)cluster_pcm_lock_resource_x_delivery_claim_end_exact(&claim);
	}
	PG_END_TRY();
	if (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE) {
		cluster_pcm_rx_metric_note(PCM_RX_DELIVERY_LATE_FRAME);
		cluster_lms_wakeup(0);
	}
	return result;
}

/* One target admission owns every type-17 holder action.  The authenticated
 * master, exact gate/session, and R4 record remain pinned across BufferDesc,
 * Resource-X local-owner, and outbound staging work; no branch may resnapshot
 * into the retired source implementation. */
static ResourceXApplyResult
gcs_block_resource_x_type17_ingress(
	const ClusterICEnvelope *env, const ResourceXDecodedFrame *frame,
	uint32 authenticated_connection_generation)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterSemanticResourceXPeerOpenResult peer_check
		= CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED;
	ClusterGcsPcmXAuthSample diagnostic_session_sample;
	ResourceXGateSnapshot diagnostic_gate;
	ResourceXGateSnapshot gate;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	PcmXSessionAuthResult diagnostic_session_check
		= PCM_X_SESSION_AUTH_INVALID;
	uint64 diagnostic_master_session = 0;
	uint64 master_session = 0;
	int32 diagnostic_lookup_master = -1;
	int32 master_node = -1;
	int32 source_node;
	bool admission_exact = false;
	bool diagnostic_gate_open = false;
	bool diagnostic_gate_snapshot = false;
	bool formation_exact = false;
	bool gate_exact = false;
	bool master_exact = false;
	bool peer_exact = false;
	bool session_exact = false;

	memset(&admission, 0, sizeof(admission));
	if (env == NULL || frame == NULL
		|| frame->kind != RESOURCE_X_WIRE_BLOCK_TO_N
		|| authenticated_connection_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	source_node = (int32)env->source_node_id;
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return RESOURCE_X_APPLY_STALE;

	PG_TRY();
	{
		if (source_node != cluster_node_id)
			peer_check = cluster_semantic_activation_resource_x_peer_open_check(
				&admission, source_node,
				authenticated_connection_generation);
		peer_exact = gcs_block_resource_x_target_peer_matches_exact(
				&admission, source_node,
				authenticated_connection_generation);
		gate_exact = peer_exact
			&& gcs_block_resource_x_gate_session_snapshot(
				&frame->common.logical_assertion.resource, &gate,
				&master_node, &master_session);
		master_exact = gate_exact && master_node == source_node;
		formation_exact = master_exact
			&& gate.formation == frame->common.resource_formation;
		session_exact = formation_exact
			&& master_session
				== frame->common.master_session_incarnation;
		admission_exact = session_exact
			&& cluster_semantic_activation_recheck(&admission);
		if (admission_exact) {
			if ((frame->common.observed_mode == (uint8)PCM_STATE_X
					 || frame->common.observed_mode == (uint8)PCM_STATE_S)
				&& frame->common.source_candidate == 1
				&& frame->common.retain_pi_if_dirty == 1)
				result = gcs_block_pcm_x_resource_x_source_block_to_n(
					frame, master_node, admission.record_generation);
			else if (frame->common.observed_mode == (uint8)PCM_STATE_S
					 && frame->common.logical_assertion.requester_node
						!= cluster_node_id)
				result
					= gcs_block_pcm_x_resource_x_remote_s_holder_block_to_n(
						frame, master_node,
						authenticated_connection_generation,
						admission.record_generation, &admission);
			else
				result = cluster_pcm_lock_resource_x_block_to_n_exact(
					frame, master_node);
		} else {
			memset(&diagnostic_gate, 0, sizeof(diagnostic_gate));
			memset(&diagnostic_session_sample, 0,
				   sizeof(diagnostic_session_sample));
			diagnostic_gate_snapshot
				= cluster_pcm_lock_resource_x_gate_snapshot(
					&diagnostic_gate);
			diagnostic_gate_open = diagnostic_gate_snapshot
				&& diagnostic_gate.phase == RESOURCE_X_GATE_OPEN;
			if (diagnostic_gate_open) {
				diagnostic_lookup_master = cluster_gcs_lookup_master(
					frame->common.logical_assertion.resource);
				if (diagnostic_lookup_master >= 0
					&& diagnostic_lookup_master
					   < RESOURCE_X_PROTOCOL_NODE_LIMIT)
					diagnostic_session_check
						= gcs_block_pcm_x_authenticated_session_result(
							diagnostic_lookup_master,
							cluster_epoch_get_current(),
							&diagnostic_master_session,
							&diagnostic_session_sample);
			}
			ereport(LOG,
					(errmsg_internal("Resource-X type-17 ingress predicate diagnostic"),
					 errdetail("peer=%s peer_check=%d gate=%s master=%s formation=%s "
							   "session=%s admission=%s source=%d current_master=%d "
							   "wire_formation=%llu current_formation=%llu "
							   "wire_session=%llu current_session=%llu "
							   "connection_generation=%u r4_generation=%llu",
							   peer_exact ? "true" : "false",
							   (int)peer_check,
							   gate_exact ? "true" : "false",
							   master_exact ? "true" : "false",
							   formation_exact ? "true" : "false",
							   session_exact ? "true" : "false",
							   admission_exact ? "true" : "false",
							   source_node, master_node,
							   (unsigned long long)frame->common.resource_formation,
							   (unsigned long long)(gate_exact
								   ? gate.formation : 0),
							   (unsigned long long)frame->common
								   .master_session_incarnation,
							   (unsigned long long)master_session,
							   authenticated_connection_generation,
							   (unsigned long long)admission.record_generation),
					 errdetail_log("gate_snapshot=%s gate_phase=%u "
							   "gate_formation=%llu gate_freeze=%llu "
							   "lookup_master=%d session_check=%d "
							   "sampled_session=%llu slot_before=%s "
							   "slot_after=%s epoch_before=%llu "
							   "epoch_after=%llu fresh_before=%s "
							   "fresh_after=%s connection_before=%s "
							   "connection_after=%s",
							   diagnostic_gate_snapshot ? "true" : "false",
							   diagnostic_gate.phase,
							   (unsigned long long)diagnostic_gate.formation,
							   (unsigned long long)
								diagnostic_gate.freeze_generation,
							   diagnostic_lookup_master,
							   (int)diagnostic_session_check,
							   (unsigned long long)diagnostic_master_session,
							   diagnostic_session_sample.slot_before_valid
								? "true" : "false",
							   diagnostic_session_sample.slot_after_valid
								? "true" : "false",
							   (unsigned long long)
								diagnostic_session_sample.observed_epoch_before,
							   (unsigned long long)
								diagnostic_session_sample.observed_epoch_after,
							   diagnostic_session_sample.fresh_before
								? "true" : "false",
							   diagnostic_session_sample.fresh_after
								? "true" : "false",
							   diagnostic_session_sample.connection_before_valid
								? "true" : "false",
							   diagnostic_session_sample.connection_after_valid
								? "true" : "false")));
		}
	}
	PG_CATCH();
	{
		cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	return result;
}

/* PGRAC adaptation: kind-9 is consumed as a closed subdomain.  Admission,
 * receipt/round mutation and frozen-frame extraction complete before either
 * encoder/stager runs; no malformed or stale kind-9 frame reaches legacy. */
static void
gcs_block_resource_x_kind9_ingress(
	const ClusterICEnvelope *env, const ResourceXDecodedFrame *frame,
	uint32 authenticated_connection_generation)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ResourceXGateSnapshot gate;
	ResourceXDecodedFrame outbound;
	ResourceXApplyResult apply_result;
	ResourceXBootstrapRoundAction round_action;
	ResourceXApplyResult stage_result;
	uint32 outbound_connection_generation = 0;
	uint32 rechecked_connection_generation = 0;
	uint64 master_session = 0;
	int32 source_node;
	int32 master_node;
	bool ready = false;

	memset(&admission, 0, sizeof(admission));
	memset(&outbound, 0, sizeof(outbound));
	if (env == NULL || frame == NULL
		|| frame->kind != RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP)
		return;
	source_node = (int32)env->source_node_id;
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return;
	if (!gcs_block_resource_x_target_peer_matches_exact(
			&admission, source_node, authenticated_connection_generation))
		goto done;
	if (!gcs_block_resource_x_gate_session_snapshot(
			&frame->common.logical_assertion.resource, &gate,
			&master_node, &master_session)
		|| gate.formation != frame->common.resource_formation
		|| master_session
			!= frame->common.master_session_incarnation
		|| !cluster_semantic_activation_recheck(&admission))
		goto done;

	if (env->msg_type == RESOURCE_X_MSG_ASSERT_X) {
		if (master_node != cluster_node_id
			|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
				source_node, &outbound_connection_generation))
			goto done;
		apply_result = cluster_pcm_lock_resource_x_bootstrap_request_exact(
			frame, source_node, authenticated_connection_generation,
			admission.record_generation,
			master_session,
			outbound_connection_generation, &outbound);
		if ((apply_result != RESOURCE_X_APPLY_APPLIED
				&& apply_result != RESOURCE_X_APPLY_DUPLICATE)
			|| gcs_block_resource_x_diagnostic_should_log(
				GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_REQUEST,
				(int)apply_result))
			ereport(LOG,
				(errmsg_internal("Resource-X kind-9 request diagnostic"),
				 errdetail("source=%d requester=%d attempt=%llu result=%d "
						   "ack_base=%llu formation=%llu session=%llu",
						   source_node,
						   frame->common.logical_assertion.requester_node,
						   (unsigned long long)
							frame->common.assertion_sequence,
						   (int)apply_result,
						   (unsigned long long)
							outbound.common.base_authority_generation,
						   (unsigned long long)
							frame->common.resource_formation,
						   (unsigned long long)
							frame->common.master_session_incarnation)));
		if (apply_result != RESOURCE_X_APPLY_APPLIED
			&& apply_result != RESOURCE_X_APPLY_DUPLICATE)
			goto done;
		ready = true;
		if (outbound.kind == RESOURCE_X_WIRE_ASSERT_X
			&& frame->common.flags == RESOURCE_X_COMMON_FLAG_REMOTE_ADMISSION) {
			/* Master already linearized the canonical request under entry_lock.
			 * This stack snapshot is not an ACK or a requester wire ASSERT. */
			cluster_pcm_lock_resource_x_trace_frame(RESOURCE_X_TRACE_APPLY, frame, source_node,
													(int32)apply_result);
			goto done;
		}
		if (!cluster_semantic_activation_recheck(&admission)
			|| !gcs_block_resource_x_gate_session_recheck(
				&frame->common.logical_assertion.resource, &gate,
				cluster_node_id, master_session)
			|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
				source_node, &rechecked_connection_generation)
			|| rechecked_connection_generation
				!= outbound_connection_generation)
			goto done;
		(void)gcs_block_resource_x_bootstrap_ack_stage_exact(
			source_node, &outbound);
	} else if (env->msg_type == RESOURCE_X_MSG_IMAGE_OR_GRANT) {
		if (master_node != source_node)
			goto done;
		round_action
			= cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
				frame, source_node, authenticated_connection_generation,
				admission.record_generation,
				gcs_block_pcm_x_monotonic_us(), &outbound);
		if (round_action == RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED
			&& cluster_semantic_activation_recheck(&admission)
			&& gcs_block_resource_x_cleanup_frame_exact(
				   frame, source_node, authenticated_connection_generation, master_node,
				   authenticated_connection_generation, admission.record_generation, NULL,
				   &outbound)
				   == RESOURCE_X_APPLY_APPLIED)
			round_action = RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT;
		if (gcs_block_resource_x_diagnostic_should_log(
				GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_KIND9_ACK,
				(int)round_action))
			ereport(LOG,
				(errmsg_internal("Resource-X kind-9 ACK diagnostic"),
				 errdetail("source=%d requester=%d attempt=%llu base=%llu "
						   "action=%d",
						   source_node,
						   frame->common.logical_assertion.requester_node,
						   (unsigned long long)
							frame->common.assertion_sequence,
						   (unsigned long long)
							frame->common.base_authority_generation,
						   (int)round_action)));
		if (round_action != RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT
			|| !cluster_semantic_activation_recheck(&admission))
			goto done;
		if (!gcs_block_resource_x_gate_session_recheck(
				&frame->common.logical_assertion.resource, &gate,
				master_node, master_session))
			goto done;
		stage_result = gcs_block_resource_x_native_assert_stage_exact(
			master_node, &outbound);
		(void)stage_result;
	}

done:
	cluster_semantic_activation_leave(&admission);
	if (ready)
		gcs_block_resource_x_stage_ready_tag(&frame->common.logical_assertion.resource);
}

static ResourceXApplyResult
gcs_block_resource_x_bootstrapped_assert_ingress(
	const ClusterICEnvelope *env, const ResourceXDecodedFrame *assertion,
	uint32 authenticated_connection_generation,
	ResourceXMasterSnapshot *snapshot)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ResourceXGateSnapshot gate;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	uint64 master_session = 0;
	uint32 master_sender_connection_generation = 0;
	int32 master_node = -1;
	int32 source_node;

	memset(&admission, 0, sizeof(admission));
	if (env == NULL || assertion == NULL || snapshot == NULL
		|| assertion->kind != RESOURCE_X_WIRE_ASSERT_X
		|| assertion->common.ordered_lane != 0)
		return RESOURCE_X_APPLY_INVALID;
	source_node = (int32)env->source_node_id;
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return RESOURCE_X_APPLY_STALE;
	if (!gcs_block_resource_x_target_peer_matches_exact(
			&admission, source_node, authenticated_connection_generation))
		goto done;
	if (!gcs_block_resource_x_gate_session_snapshot(
			&assertion->common.logical_assertion.resource, &gate,
			&master_node, &master_session)
		|| gate.formation != assertion->common.resource_formation
		|| master_session
			!= assertion->common.master_session_incarnation
		|| master_node != cluster_node_id
		|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
			source_node, &master_sender_connection_generation)
		|| !cluster_semantic_activation_recheck(&admission))
		goto done;
	result = cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
		assertion, source_node, authenticated_connection_generation,
		admission.record_generation, master_session,
		master_sender_connection_generation, snapshot);
done:
	cluster_semantic_activation_leave(&admission);
	return result;
}

/* A complete type-15 join executes R9 on the selected writer side.  TARGET
 * never falls back to the local ticket projection and publishes the exact
 * post-T3 ref/BufferDesc generation into the requester round before waking
 * its same-node followers. */
static ResourceXApplyResult
gcs_block_resource_x_requester_join_ingress(const ClusterICEnvelope *env,
											const ResourceXDecodedFrame *frame,
											uint32 authenticated_connection_generation,
											ResourceXRequesterJoinSnapshot *out)
{
	ClusterSemanticAdmissionToken admission = { 0 };
	ResourceXGateSnapshot gate;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	uint64 master_session = 0;
	uint32 master_ingress = 0;
	int32 master_node = -1;
	int32 source;

	if (env == NULL || frame == NULL || out == NULL || authenticated_connection_generation == 0
		|| (frame->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
			&& frame->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT))
		return RESOURCE_X_APPLY_INVALID;
	source = (int32)env->source_node_id;
	if (cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
										  CLUSTER_SEMANTIC_TARGET_SIDE, &admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return RESOURCE_X_APPLY_STALE;
	PG_TRY();
	{
		if (gcs_block_resource_x_target_peer_matches_exact(&admission, source,
														   authenticated_connection_generation)
			&& gcs_block_resource_x_gate_session_snapshot(&frame->common.logical_assertion.resource,
														  &gate, &master_node, &master_session)
			&& gate.formation == frame->common.resource_formation
			&& master_session == frame->common.master_session_incarnation
			&& gcs_block_pcm_x_resource_x_peer_ready_exact(master_node, &master_ingress)
			&& gcs_block_resource_x_target_peer_matches_exact(&admission, master_node,
															  master_ingress)
			&& cluster_semantic_activation_recheck(&admission)
			&& gcs_block_resource_x_gate_session_recheck(&frame->common.logical_assertion.resource,
														 &gate, master_node, master_session)) {
			result = cluster_pcm_lock_resource_x_requester_join_current_exact(
				frame, source, authenticated_connection_generation, master_node, master_ingress,
				admission.record_generation, out);
			if (result == RESOURCE_X_APPLY_STALE && cluster_semantic_activation_recheck(&admission))
				result = gcs_block_resource_x_cleanup_frame_exact(
					frame, source, authenticated_connection_generation, master_node, master_ingress,
					admission.record_generation, out, NULL);
		}
	}
	PG_CATCH();
	{
		cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	return result;
}

static ResourceXApplyResult
gcs_block_resource_x_requester_terminal_try(
	const ClusterICEnvelope *env, const ResourceXDecodedFrame *frame,
	uint32 authenticated_connection_generation, bool scheduled_retry)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ResourceXGateSnapshot gate;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	ResourceXApplyResult publish_result;
	uint64 terminal_ownership_generation = 0;
	uint64 terminal_authority_generation = 0;
	uint64 master_session = 0;
	int32 master_node = -1;
	int32 source_node;

	if (env == NULL || frame == NULL
		|| authenticated_connection_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	source_node = (int32)env->source_node_id;
	memset(&admission, 0, sizeof(admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return RESOURCE_X_APPLY_STALE;

	memset(&terminal_ref, 0, sizeof(terminal_ref));
	PG_TRY();
	{
		if (gcs_block_resource_x_target_peer_matches_exact(&admission, source_node,
														   authenticated_connection_generation)
			&& gcs_block_resource_x_gate_session_snapshot(&frame->common.logical_assertion.resource,
														  &gate, &master_node, &master_session)
			/* Ingress has already admitted the exact READY join.  A delegated
			 * image arrives from its authenticated physical source, not
			 * necessarily the authority master.  join_terminal_try rereads the
			 * retained proof/ref before T1/T2/T3; both peer and master/session
			 * fences below and after installation remain mandatory. */
			&& (master_node == source_node
				|| (frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
					&& frame->common.flags == RESOURCE_X_COMMON_FLAG_AUTHORITY_WITH_IMAGE))
			&& gate.formation == frame->common.resource_formation
			&& master_session == frame->common.master_session_incarnation
			&& cluster_semantic_activation_recheck(&admission)) {
			ResourceXAcquisitionRef delivery_ref;

			delivery_ref.assertion = frame->common.logical_assertion;
			delivery_ref.formation = frame->common.resource_formation;
			delivery_ref.acquisition_generation = frame->common.assertion_sequence;
			result = gcs_block_pcm_x_resource_x_join_terminal_owned_try(
				&delivery_ref, scheduled_retry, &terminal_ref, &terminal_ownership_generation,
				&terminal_authority_generation);
			if (result == RESOURCE_X_APPLY_APPLIED
				|| result == RESOURCE_X_APPLY_DUPLICATE) {
				if (!cluster_semantic_activation_recheck(&admission)
					|| !gcs_block_resource_x_gate_session_recheck(
						&frame->common.logical_assertion.resource,
						&gate, master_node, master_session)
					|| !gcs_block_resource_x_target_peer_matches_exact(
						&admission, source_node,
						authenticated_connection_generation))
					result = RESOURCE_X_APPLY_STALE;
				else {
					publish_result
						= cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
							&terminal_ref,
							master_session,
							admission.record_generation,
							terminal_ownership_generation,
							terminal_authority_generation,
							gcs_block_pcm_x_monotonic_us());
					if (publish_result != RESOURCE_X_APPLY_APPLIED
						&& publish_result != RESOURCE_X_APPLY_DUPLICATE) {
						result = publish_result;
					}
				}
			}
		}
	}
	PG_CATCH();
	{
		cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	return result;
}

/* PGRAC adaptation: SourceSettlementV2 consumes only the exact retained
 * type-18/type-15 pair selected by the authenticated master.  Entry-lock
 * prepare/commit bracket, but never overlap, the BufferDesc release.  The
 * response is an ordinary unretained typed ACK; a full outbound queue is
 * healed by the master's still-retained kind-10 debt replay. */
static ResourceXApplyResult
gcs_block_resource_x_source_settlement_ingress(
	const ClusterICEnvelope *env, const ResourceXDecodedFrame *settlement,
	uint32 sender_connection_generation)
{
	ResourceXDecodedFrame ack;
	ResourceXSourceSettlementPlan plan;
	ResourceXSourceSettlementCommitObservation commit_observation;
	ResourceXSourceSettlementCommitObservation prepare_observation;
	ResourceXApplyResult ack_result;
	ResourceXApplyResult result;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	ClusterPcmOwnResult own_result;
	ClusterPcmXRevokeFinishMode settlement_finish_mode;
	bool ack_enqueued;
	bool carrier_release_complete = false;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
	uint16 payload_bytes = 0;

	if (env == NULL || settlement == NULL
		|| settlement->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| env->source_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| sender_connection_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	memset(&plan, 0, sizeof(plan));
	memset(&commit_observation, 0, sizeof(commit_observation));
	memset(&prepare_observation, 0, sizeof(prepare_observation));
	result
		= cluster_pcm_lock_resource_x_source_settlement_prepare_observed_exact(
			settlement, (int32)env->source_node_id, &plan,
			&prepare_observation);
	if (result == RESOURCE_X_APPLY_APPLIED && !plan.valid) {
		gcs_block_resource_x_source_settlement_failure_record(
			settlement, &prepare_observation,
			RESOURCE_X_SOURCE_SETTLEMENT_STAGE_PREPARE,
			RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
			RESOURCE_X_APPLY_RECOVERY_BLOCKED,
			prepare_observation.current_pair_source_generation,
			-1, false, false);
		gcs_block_resource_x_fail_closed_current();
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (result == RESOURCE_X_APPLY_APPLIED && plan.valid) {
		settlement_finish_mode
			= cluster_pcm_x_revoke_finish_mode(&plan.assertion.resource, 0);
		if (settlement_finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_RETAIN) {
			own_result
				= cluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(
				&plan.assertion.resource, plan.source_generation,
				&carrier_release_complete);
			if (own_result == CLUSTER_PCM_OWN_BUSY
				|| own_result == CLUSTER_PCM_OWN_NOT_READY) {
				gcs_block_resource_x_source_settlement_failure_record(
					settlement, NULL,
					RESOURCE_X_SOURCE_SETTLEMENT_STAGE_LOCAL_RELEASE,
					gcs_block_resource_x_source_settlement_own_domain(
						own_result, carrier_release_complete),
					RESOURCE_X_APPLY_BAD_STATE, plan.source_generation,
					(int32)own_result, carrier_release_complete, false);
				return RESOURCE_X_APPLY_BAD_STATE;
			}
			if (own_result != CLUSTER_PCM_OWN_OK) {
				gcs_block_resource_x_source_settlement_failure_record(
					settlement, NULL,
					RESOURCE_X_SOURCE_SETTLEMENT_STAGE_LOCAL_RELEASE,
					gcs_block_resource_x_source_settlement_own_domain(
						own_result, carrier_release_complete),
					RESOURCE_X_APPLY_RECOVERY_BLOCKED,
					plan.source_generation, (int32)own_result,
					carrier_release_complete, false);
				gcs_block_resource_x_fail_closed_current();
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
			if (!carrier_release_complete) {
				gcs_block_resource_x_source_settlement_failure_record(
					settlement, NULL,
					RESOURCE_X_SOURCE_SETTLEMENT_STAGE_LOCAL_RELEASE,
					RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
					RESOURCE_X_APPLY_RECOVERY_BLOCKED,
					plan.source_generation, (int32)own_result,
					false, false);
				gcs_block_resource_x_fail_closed_current();
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
		}
		else if (settlement_finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_DROP) {
			/* VM/FSM publish this exact pair only after the unpinned DROP and
			 * terminal-cover close both succeed.  No BufferDesc remains to
			 * release; prepare already revalidated the PUBLISHED pair identity. */
			carrier_release_complete = true;
		}
		else {
			gcs_block_resource_x_source_settlement_failure_record(
				settlement, NULL,
				RESOURCE_X_SOURCE_SETTLEMENT_STAGE_LOCAL_RELEASE,
				RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
				RESOURCE_X_APPLY_RECOVERY_BLOCKED,
				plan.source_generation, -1,
				false, false);
			gcs_block_resource_x_fail_closed_current();
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		result = cluster_pcm_lock_resource_x_source_settlement_commit_exact(
			settlement, (int32)env->source_node_id, &plan,
			&commit_observation);
		if (result != RESOURCE_X_APPLY_APPLIED
			&& result != RESOURCE_X_APPLY_DUPLICATE) {
			gcs_block_resource_x_source_settlement_failure_record(
				settlement, &commit_observation,
				RESOURCE_X_SOURCE_SETTLEMENT_STAGE_COMMIT,
				RESOURCE_X_FAIL_POST_MUTATION_AMBIGUITY, result,
				plan.source_generation, -1, carrier_release_complete, false);
			gcs_block_resource_x_fail_closed_current();
			return result;
		}
	}
	else if (result != RESOURCE_X_APPLY_DUPLICATE) {
		gcs_block_resource_x_source_settlement_failure_record(
			settlement, &prepare_observation,
			RESOURCE_X_SOURCE_SETTLEMENT_STAGE_PREPARE,
			gcs_block_resource_x_source_settlement_apply_domain(result),
			result, prepare_observation.current_pair_source_generation,
			-1, false, false);
		return result;
	}
	ack_result = cluster_pcm_lock_resource_x_source_settlement_ack_build_exact(
		settlement, sender_connection_generation, &ack);
	if (ack_result != RESOURCE_X_APPLY_APPLIED) {
		gcs_block_resource_x_source_settlement_failure_record(
			settlement, NULL, RESOURCE_X_SOURCE_SETTLEMENT_STAGE_ACK_BUILD,
			RESOURCE_X_FAIL_INTERNAL_CORRUPTION, ack_result,
			plan.source_generation, -1, carrier_release_complete, false);
		gcs_block_resource_x_fail_closed_current();
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_BLOCKED_TO_N, &ack, payload, sizeof(payload),
			&payload_bytes, &reject)
		|| payload_bytes != RESOURCE_X_PROOF_V1_BYTES) {
		gcs_block_resource_x_source_settlement_failure_record(
			settlement, NULL, RESOURCE_X_SOURCE_SETTLEMENT_STAGE_ACK_ENCODE,
			RESOURCE_X_FAIL_INTERNAL_CORRUPTION,
			RESOURCE_X_APPLY_INVALID, plan.source_generation, -1,
			carrier_release_complete, false);
		gcs_block_resource_x_fail_closed_current();
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	ack_enqueued = cluster_grd_outbound_enqueue_backend_msg(
		RESOURCE_X_MSG_BLOCKED_TO_N, env->source_node_id, payload,
		payload_bytes);
	if ((result == RESOURCE_X_APPLY_APPLIED || !ack_enqueued)
		&& ((result != RESOURCE_X_APPLY_APPLIED
				&& result != RESOURCE_X_APPLY_DUPLICATE)
			|| !ack_enqueued
			|| gcs_block_resource_x_diagnostic_should_log(
				GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_SOURCE_SETTLEMENT_ACK,
				(int)result)))
		ereport(LOG,
				(errmsg_internal("Resource-X source settlement ACK diagnostic"),
				 errdetail("master=%u requester=%d attempt=%llu result=%d "
						   "source_generation=%llu enqueue=%u sender_generation=%u",
						   env->source_node_id,
						   settlement->common.logical_assertion.requester_node,
						   (unsigned long long)
							settlement->common.assertion_sequence,
							(int)result,
							(unsigned long long)plan.source_generation,
						   ack_enqueued ? 1U : 0U,
						   sender_connection_generation)));
	return result;
}

/* Consume the exact Resource-X subdomain before any reused legacy parser.
 * A candidate length never falls back after a malformed header, stale DATA
 * connection, or unavailable consumer.  Capability publication remains off
 * until the type-15/type-17 consumers and retained C-intent egress close. */
static bool
gcs_block_try_resource_x_frame(const ClusterICEnvelope *env,
							   const void *payload)
{
#ifdef CLUSTER_R4_ROUTE_POLICY_UNIT
	/* This focused object feeds only R4 non-candidate lengths into the shared
	 * handlers.  Resource-X ingress is executed by its dedicated GCS fixture. */
	(void)env;
	(void)payload;
	return false;
#else
	ResourceXDecodedFrame frame;
	ResourceXMasterSnapshot snapshot;
	ResourceXRequesterJoinSnapshot join_snapshot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	ResourceXApplyResult result = RESOURCE_X_APPLY_INVALID;
	uint32 capability_word = 0;
	uint32 connection_generation = 0;
	uint32 authenticated_capability_generation = 0;

	if (env == NULL
		|| !gcs_block_resource_x_payload_candidate(env->msg_type,
											 env->payload_length))
		return false;
	if (payload == NULL || env->source_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| env->dest_node_id != (uint32)cluster_node_id
		|| env->payload_length > PG_UINT16_MAX
		|| !cluster_resource_x_wire_decode(env->msg_type, payload,
			(uint16)env->payload_length, &frame, &reject)) {
		return true;
	}
	cluster_pcm_lock_resource_x_trace_frame(RESOURCE_X_TRACE_RECEIVE, &frame,
		(int32)env->source_node_id, 0);
	if (env->epoch != cluster_epoch_get_current()) {
		return true;
	}
	if ((int32)env->source_node_id == cluster_node_id) {
		if (!gcs_block_pcm_x_resource_x_peer_ready_exact(
				cluster_node_id, &connection_generation)) {
			return true;
		}
	} else if (!cluster_sf_peer_capability_word_sample(
			   (int32)env->source_node_id,
			   PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
			   &capability_word, &connection_generation)) {
		return true;
	}
	/* Tier1 envelope verification has already bound this frame to the
	 * current authenticated DATA fd and its source peer.  The capability
	 * generation sampled above is receiver-local; the sender's nonzero,
	 * pre-send rebound generation is deliberately not numerically comparable
	 * with it.  Retain the local sample only for same-node drift checks. */
	authenticated_capability_generation = connection_generation;
	if (authenticated_capability_generation == 0)
		return true;
	memset(&snapshot, 0, sizeof(snapshot));
	memset(&join_snapshot, 0, sizeof(join_snapshot));

	switch (frame.kind) {
	case RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP:
		gcs_block_resource_x_kind9_ingress(
			env, &frame, authenticated_capability_generation);
		break;
	case RESOURCE_X_WIRE_ASSERT_X:
		if (frame.common.ordered_lane == 0)
			result = gcs_block_resource_x_bootstrapped_assert_ingress(
				env, &frame, authenticated_capability_generation,
				&snapshot);
		if ((result == RESOURCE_X_APPLY_APPLIED
				|| result == RESOURCE_X_APPLY_DUPLICATE)
			&& snapshot.phase == RESOURCE_X_MASTER_WAIT_PROOF
			&& frame.common.observed_mode == (uint8)PCM_STATE_N)
			result = gcs_block_pcm_x_resource_x_master_durable_try(
				&frame, &snapshot, &snapshot);
		break;
	case RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION:
		result = cluster_pcm_lock_resource_x_local_proof_exact(
			&frame, (int32)env->source_node_id, &snapshot);
		break;
	case RESOURCE_X_WIRE_BLOCK_TO_N:
		result = gcs_block_resource_x_type17_ingress(
			env, &frame, authenticated_capability_generation);
		break;
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
		result = gcs_block_resource_x_source_settlement_ingress(
			env, &frame, authenticated_capability_generation);
		break;
	case RESOURCE_X_WIRE_BLOCKED_TO_N:
		result = cluster_pcm_lock_resource_x_blocked_to_n_exact(
			&frame, (int32)env->source_node_id, &snapshot);
		break;
	case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2:
		result = cluster_pcm_lock_resource_x_source_settlement_ack_exact(
			&frame, (int32)env->source_node_id, &snapshot);
		if ((result != RESOURCE_X_APPLY_APPLIED
				&& result != RESOURCE_X_APPLY_DUPLICATE)
			|| gcs_block_resource_x_diagnostic_should_log(
				GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_SOURCE_SETTLEMENT_ACK_INGRESS,
				(int)result))
			ereport(LOG,
				(errmsg_internal("Resource-X source settlement ACK ingress diagnostic"),
				 errdetail("source=%u requester=%d attempt=%llu result=%d "
						   "phase=%u final=%llu",
						   env->source_node_id,
						   frame.common.logical_assertion.requester_node,
						   (unsigned long long)
							frame.common.assertion_sequence,
						   (int)result, (unsigned)snapshot.phase,
						   (unsigned long long)
							snapshot.final_authority_generation)));
		break;
	case RESOURCE_X_WIRE_INSTALL_SETTLEMENT:
	{
		result = cluster_pcm_lock_resource_x_install_settlement_exact(
			&frame, (int32)env->source_node_id, &snapshot);
		if ((result != RESOURCE_X_APPLY_APPLIED
				&& result != RESOURCE_X_APPLY_DUPLICATE)
			|| gcs_block_resource_x_diagnostic_should_log(
				GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_INSTALL_SETTLEMENT,
				(int)result))
			ereport(LOG,
					(errmsg_internal("Resource-X install settlement diagnostic"),
					 errdetail("source=%u requester=%d attempt=%llu result=%d "
							   "phase=%u lane=%u final=%llu",
							   env->source_node_id, frame.common.logical_assertion.requester_node,
							   (unsigned long long)frame.common.assertion_sequence, (int)result,
							   (unsigned)snapshot.phase, (unsigned)frame.common.ordered_lane,
							   (unsigned long long)frame.common.authority_generation)));
		break;
	}
	case RESOURCE_X_WIRE_RELEASE_X:
		result = cluster_pcm_lock_resource_x_release_x_exact(
			&frame, (int32)env->source_node_id, &snapshot);
		break;
	case RESOURCE_X_WIRE_IMAGE_ENVELOPE:
	case RESOURCE_X_WIRE_AUTHORITY_GRANT:
		result = gcs_block_resource_x_requester_join_ingress(
			env, &frame, authenticated_capability_generation, &join_snapshot);
		if ((result == RESOURCE_X_APPLY_APPLIED
				|| result == RESOURCE_X_APPLY_DUPLICATE)
			&& (join_snapshot.flags & RESOURCE_X_REQUESTER_JOIN_READY) != 0) {
			(void)gcs_block_resource_x_requester_terminal_try(
				env, &frame, authenticated_capability_generation,
				result == RESOURCE_X_APPLY_DUPLICATE);
		}
		break;
	}
	/* The second leg may be the source proof or the install notification.
	 * Both close the identical retained master request exactly once. */
	if ((frame.kind == RESOURCE_X_WIRE_BLOCKED_TO_N
		 || frame.kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT)
		&& (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
		&& snapshot.phase == RESOURCE_X_MASTER_SETTLED) {
		ResourceXApplyResult retire_result = cluster_pcm_lock_resource_x_settled_retire_exact(
			&frame.common.logical_assertion, snapshot.assertion_sequence, &snapshot);

		if (retire_result != RESOURCE_X_APPLY_APPLIED
			&& retire_result != RESOURCE_X_APPLY_DUPLICATE) {
			gcs_block_resource_x_fail_closed_current();
			result = retire_result;
		}
	}
	/* Bootstrap owns its own typed action; the local sentinel is not its result. */
	if (frame.kind != RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP)
		cluster_pcm_lock_resource_x_trace_frame(RESOURCE_X_TRACE_APPLY, &frame,
			(int32)env->source_node_id, (int32)result);
	if (gcs_block_resource_x_frame_diagnostic(frame.kind)
			!= GCS_BLOCK_RESOURCE_X_DIAGNOSTIC_COUNT
		&& ((result != RESOURCE_X_APPLY_APPLIED
				&& result != RESOURCE_X_APPLY_DUPLICATE)
			|| gcs_block_resource_x_diagnostic_should_log(
				gcs_block_resource_x_frame_diagnostic(frame.kind),
				(int)result)))
		ereport(LOG,
				(errmsg_internal("Resource-X frame ingress diagnostic"),
				 errdetail("kind=%u msg_type=%u source=%u requester=%d "
						   "attempt=%llu result=%d master_phase=%u head=%u "
						   "incompatible=0x%08x blocked=0x%08x proof=%u "
						   "join_flags=0x%08x grant_source=%d image_source=%d",
						   (unsigned)frame.kind, (unsigned)env->msg_type,
						   env->source_node_id,
						   frame.common.logical_assertion.requester_node,
						   (unsigned long long)
							frame.common.assertion_sequence,
						   (int)result, (unsigned)snapshot.phase,
						   (unsigned)snapshot.is_head,
						   snapshot.incompatible_holders_bitmap,
						   snapshot.blocked_holders_bitmap,
						   (unsigned)snapshot.proof_kind,
						   join_snapshot.flags,
						   join_snapshot.grant_source_node,
						   join_snapshot.image_source_node)));
	if (frame.kind != RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP
		&& (result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE))
		gcs_block_resource_x_stage_ready_tag(&frame.common.logical_assertion.resource);
	return true;
#endif
}

static ResourceXApplyResult
gcs_block_resource_x_target_install_snapshot_result(
	ClusterPcmOwnResult snapshot_result)
{
	if (snapshot_result == CLUSTER_PCM_OWN_OK)
		return RESOURCE_X_APPLY_APPLIED;
	if (snapshot_result == CLUSTER_PCM_OWN_BUSY)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (snapshot_result == CLUSTER_PCM_OWN_STALE)
		return RESOURCE_X_APPLY_STALE;
	return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}

/* Bracket the entry-locked continuation capture with two exact BufferDesc
 * projections.  A changed B invalidates the raw E result and the candidate
 * continuation before either can reach the caller. */
static ResourceXApplyResult
gcs_block_resource_x_target_install_capture_coherent(
	BufferDesc *buf,
	const ResourceXAssertion *assertion,
	int32 current_master_node,
	uint64 resource_formation,
	uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us,
	uint64 caller_absolute_deadline_us,
	const ClusterPcmOwnSnapshot *before,
	ResourceXTargetInstallFollowState *follow_state_out,
	ResourceXTargetInstallContinuation *continuation_out)
{
	ClusterPcmOwnSnapshot after;
	ClusterPcmOwnResult snapshot_result;
	ResourceXTargetInstallContinuation candidate;
	ResourceXTargetInstallFollowState raw_state;

	if (follow_state_out != NULL)
		*follow_state_out = RESOURCE_X_TARGET_INSTALL_INVALID;
	if (continuation_out != NULL)
		memset(continuation_out, 0, sizeof(*continuation_out));
	if (buf == NULL || assertion == NULL || before == NULL
		|| follow_state_out == NULL || continuation_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	memset(&candidate, 0, sizeof(candidate));
	raw_state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
			assertion, current_master_node, resource_formation,
			master_session_incarnation, r4_record_generation,
			requester_sender_connection_generation,
			master_ingress_connection_generation, retry_slice_us,
			caller_absolute_deadline_us, before, &candidate);
	memset(&after, 0, sizeof(after));
	snapshot_result = cluster_bufmgr_pcm_own_snapshot(buf, &after);
	if (snapshot_result != CLUSTER_PCM_OWN_OK)
		return gcs_block_resource_x_target_install_snapshot_result(
			snapshot_result);
	*follow_state_out
		= cluster_pcm_x_target_install_follow_adjudicate_exact(
			before, &after, raw_state, NULL, &candidate);
	*continuation_out = candidate;
	return RESOURCE_X_APPLY_APPLIED;
}

/* The retained continuation already fixes E identity.  This wrapper brackets
 * its raw entry classification with B and releases a terminal ref only after
 * exact whole-snapshot equality. */
static ResourceXApplyResult
gcs_block_resource_x_target_install_classify_coherent(
	BufferDesc *buf,
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *before,
	ResourceXTargetInstallFollowState *follow_state_out,
	ResourceXAcquisitionRef *terminal_ref_out)
{
	ClusterPcmOwnSnapshot after;
	ClusterPcmOwnResult snapshot_result;
	ResourceXAcquisitionRef candidate_ref;
	ResourceXTargetInstallFollowState raw_state;

	if (follow_state_out != NULL)
		*follow_state_out = RESOURCE_X_TARGET_INSTALL_INVALID;
	if (terminal_ref_out != NULL)
		memset(terminal_ref_out, 0, sizeof(*terminal_ref_out));
	if (buf == NULL || continuation == NULL || before == NULL
		|| follow_state_out == NULL || terminal_ref_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	memset(&candidate_ref, 0, sizeof(candidate_ref));
	raw_state
		= cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
			continuation, before, &candidate_ref);
	memset(&after, 0, sizeof(after));
	snapshot_result = cluster_bufmgr_pcm_own_snapshot(buf, &after);
	if (snapshot_result != CLUSTER_PCM_OWN_OK)
		return gcs_block_resource_x_target_install_snapshot_result(
			snapshot_result);
	*follow_state_out
		= cluster_pcm_x_target_install_follow_adjudicate_exact(
			before, &after, raw_state, &candidate_ref, NULL);
	*terminal_ref_out = candidate_ref;
	return RESOURCE_X_APPLY_APPLIED;
}

/* PGRAC adaptation: one foreground TARGET caller drives or joins the fixed
 * per-resource bootstrap round.  The round owns fan-in and retransmit state;
 * this backend owns only bounded staging/wait slices and the returned ref. */
static struct {
	int buffer_id;
	ResourceXApplyResult result;
	uint64 attempt;
	const char *reason;
	bool valid;
} gcs_resource_x_acquire_diagnostic;

/* Only the immediate acquire consumer uses this process-local, one-shot
 * observation. Other writer operations must not inherit a previous failure. */
const char *
cluster_gcs_resource_x_take_acquire_failure_reason(int buffer_id, ResourceXApplyResult result,
												   uint64 *attempt_out)
{
	const char *reason = "ACQUIRE_FAILURE_CAUSE_UNPROVEN";

	*attempt_out = 0;
	if (gcs_resource_x_acquire_diagnostic.valid
		&& gcs_resource_x_acquire_diagnostic.buffer_id == buffer_id
		&& gcs_resource_x_acquire_diagnostic.result == result) {
		reason = gcs_resource_x_acquire_diagnostic.reason;
		*attempt_out = gcs_resource_x_acquire_diagnostic.attempt;
	}
	gcs_resource_x_acquire_diagnostic.valid = false;
	return reason;
}

static ResourceXApplyResult
gcs_block_resource_x_target_acquire_internal(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 r4_record_generation,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	bool join_only,
	uint64 *absolute_deadline_us_io,
	ResourceXAcquisitionRef *ref_out)
{
	BufferTag resource;
	ClusterPcmOwnSnapshot own;
	ClusterPcmOwnSnapshot failure_live;
	ClusterPcmOwnResult own_result;
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterSemanticResourceXPeerOpenResult peer_open_result
		= CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_NOT_CHECKED;
	PcmXSessionAuthResult preflight_session_check
		= PCM_X_SESSION_AUTH_INVALID;
	ResourceXGateSnapshot gate;
	ResourceXGateSnapshot terminal_gate;
	ResourceXAssertion assertion;
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal_ref;
	ResourceXBootstrapRoundAction action
		= RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	ResourceXBootstrapRoundFailureSnapshot failure_round;
	ResourceXTargetInstallContinuation target_install_follow;
	ResourceXCallerWitness caller_witness;
	ResourceXTargetInstallFollowState target_install_follow_state
		= RESOURCE_X_TARGET_INSTALL_INVALID;
	ResourceXFirstFailureEvidence first_failure;
	ResourceXApplyResult wait_result = RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult target_install_observation_result
		= RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult ownership_loss_result = RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult failure_snapshot_result = RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult discard_result = RESOURCE_X_APPLY_INVALID;
	ResourceXApplyResult result = RESOURCE_X_APPLY_BAD_STATE;
	ClusterPcmOwnResult direct_candidate_result;
	PcmXSessionAuthResult dispatch_session_check
		= PCM_X_SESSION_AUTH_INVALID;
	PcmXSessionAuthResult terminal_session_check
		= PCM_X_SESSION_AUTH_INVALID;
	ResourceXGateSnapshot rebound_gate;
	volatile uint64 absolute_deadline_us = 0;
	uint64 diagnostic_caller_budget_us = gcs_block_pcm_x_retry_timeout_us();
	bool diagnostic_join_recorded = false;
	bool diagnostic_deadline_expired = false;
	bool diagnostic_head_expired = false;
	PcmRxWaitFailure diagnostic_wait_failure = PCM_RX_WAIT_FAILURE_NONE;
	uint64 observed_head_deadline_us = 0;
	uint64 admission_record_generation = 0;
	uint64 direct_init_committed_generation = 0;
	uint64 diagnostic_request_sequence = 0;
	uint64 master_session = 0;
	uint64 rebound_master_session = 0;
	uint64 terminal_master_session = 0;
	uint64 pending_terminal_resample_mismatch
		= RESOURCE_X_PENDING_TERMINAL_MISMATCH_NONE;
	uint64 now_us = 0;
	uint64 remaining_us;
	uint64 retry_slice_us = 0;
	uint32 requester_sender_connection_generation = 0;
	uint32 master_ingress_connection_generation = 0;
	uint32 requester_sender_recheck = 0;
	uint32 master_ingress_recheck = 0;
	uint32 rebound_requester_connection_generation = 0;
	uint32 rebound_master_connection_generation = 0;
	uint32 dispatch_recheck_failure_mask
		= RESOURCE_X_DISPATCH_RECHECK_OK;
	long timeout_ms;
	int32 master_node = -1;
	int32 rebound_master_node = -1;
	int32 terminal_master_node = -1;
	bool cached_local_x = false;
	bool direct_init;
	bool direct_init_pending_n = false;
	bool dispatch_admission_current = false;
	bool dispatch_gate_session_current = false;
	bool dispatch_master_sampled = false;
	bool dispatch_requester_sampled = false;
	bool rebound_master_sampled = false;
	bool rebound_peer_matches = false;
	bool rebound_requester_sampled = false;
	bool round_drift_authority_current = false;
	bool round_drift_retained_buffer_exact = false;
	bool round_drift_retained_pair_exact = false;
	bool first_failure_recorded = false;
	bool preflight_backpressure = false;
	bool preflight_current = false;
	bool stage_ok = false;
	bool target_retained_release_inflight = false;
	bool target_retained_release_post_mutation = false;
	bool target_install_preuse_retry_seen = false;
	bool terminal_admission_current = false;
	bool terminal_gate_session_current = false;
	const char *volatile diagnostic_stage = "entry";

	gcs_resource_x_acquire_diagnostic.valid = false;
	if (ref_out != NULL)
		memset(ref_out, 0, sizeof(*ref_out));
	memset(&own, 0, sizeof(own));
	memset(&target_install_follow, 0, sizeof(target_install_follow));
	memset(&caller_witness, 0, sizeof(caller_witness));
	memset(&gate, 0, sizeof(gate));
	memset(&terminal_gate, 0, sizeof(terminal_gate));
	memset(&assertion, 0, sizeof(assertion));
	memset(&resource, 0, sizeof(resource));
	direct_init = direct_init_reservation_token != 0;
	if (buf == NULL || ref_out == NULL
		|| (!join_only && (r4_record_generation == 0
			|| r4_record_generation == UINT64_MAX))
		|| (join_only && (r4_record_generation != 0 || !direct_init))
		|| (!direct_init && direct_init_ownership_generation != 0)
		|| direct_init_ownership_generation == UINT64_MAX
		|| direct_init_reservation_token == UINT64_MAX
		|| (absolute_deadline_us_io != NULL
			&& *absolute_deadline_us_io == UINT64_MAX)
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return RESOURCE_X_APPLY_INVALID;
	resource = expected_resource != NULL ? *expected_resource : buf->tag;
	if (join_only) {
		now_us = gcs_block_pcm_x_monotonic_us();
		if (now_us == 0
			|| !resource_x_assertion_init(
				&resource, cluster_node_id, &assertion))
			return RESOURCE_X_APPLY_INVALID;
		result = cluster_pcm_lock_resource_x_bootstrap_round_direct_init_join_budget_exact(
			&assertion, direct_init_ownership_generation, direct_init_reservation_token, now_us,
			&r4_record_generation, &observed_head_deadline_us);
		if (result != RESOURCE_X_APPLY_APPLIED)
			return result;
	}
	diagnostic_request_sequence
		= gcs_block_resource_x_next_diagnostic_request_sequence();
	if (direct_init)
		direct_init_committed_generation
			= direct_init_ownership_generation + 1;
	memset(&admission, 0, sizeof(admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		ereport(LOG,
			(errmsg_internal("Resource-X target acquire diagnostic"),
			 errdetail("stage=semantic-enter result=%d admission=%d buffer=%d",
				(int)RESOURCE_X_APPLY_BAD_STATE, (int)admission_result,
				buf->buf_id)));
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (admission.record_generation != r4_record_generation) {
		ereport(LOG,
			(errmsg_internal("Resource-X target acquire diagnostic"),
			 errdetail("stage=semantic-generation result=%d record=" UINT64_FORMAT
				" expected=" UINT64_FORMAT " buffer=%d",
				(int)RESOURCE_X_APPLY_STALE,
				admission.record_generation, r4_record_generation,
				buf->buf_id)));
		cluster_semantic_activation_leave(&admission);
		return RESOURCE_X_APPLY_STALE;
	}
	admission_record_generation = admission.record_generation;
	diagnostic_stage = "preflight";

	PG_TRY();
	{
		do
		{
			now_us = gcs_block_pcm_x_monotonic_us();
			retry_slice_us
				= (uint64)Max(
					cluster_gcs_block_retransmit_initial_backoff_ms, 1)
				  * UINT64_C(1000);
			if (absolute_deadline_us_io != NULL && *absolute_deadline_us_io != 0)
				absolute_deadline_us = *absolute_deadline_us_io;
			else if (absolute_deadline_us == 0) {
				absolute_deadline_us = gcs_block_pcm_x_saturating_add_us(
					now_us, gcs_block_pcm_x_retry_timeout_us());
				if (absolute_deadline_us_io != NULL
					&& absolute_deadline_us != UINT64_MAX)
					*absolute_deadline_us_io = absolute_deadline_us;
			}
			if (now_us == 0 || retry_slice_us == 0
				|| absolute_deadline_us == UINT64_MAX
				|| now_us >= absolute_deadline_us) {
				diagnostic_deadline_expired = now_us != 0 && absolute_deadline_us != UINT64_MAX
											  && now_us >= absolute_deadline_us;
				result = RESOURCE_X_APPLY_INVALID;
				break;
			}

			/* The full-member OPEN carrier is terminal R4 evidence, while its
			 * stack-only QVOTEC/current-membership revalidation can be briefly
			 * unavailable between publications.  Before a Resource-X round exists
			 * this is pre-mutation backpressure: keep the entered admission and
			 * current gate/transport identity, and retry only under the first R7
			 * absolute deadline.  No round, proof, or image exists to preserve. */
			diagnostic_stage = "preflight-membership";
			for (;;)
			{
				bool master_ready;
				bool requester_ready;

				MemSet(&gate, 0, sizeof(gate));
				MemSet(&assertion, 0, sizeof(assertion));
				requester_sender_connection_generation = 0;
				master_ingress_connection_generation = 0;
				preflight_session_check
					= gcs_block_resource_x_gate_session_snapshot_result(
						&resource, &gate, &master_node, &master_session);
				if (preflight_session_check != PCM_X_SESSION_AUTH_OK)
				{
					if (cluster_gcs_pcm_x_auth_result_retryable(
							preflight_session_check))
						goto preflight_membership_wait;
					result = RESOURCE_X_APPLY_BAD_STATE;
					break;
				}
				if (!resource_x_assertion_init(
						&resource, cluster_node_id, &assertion))
				{
					result = RESOURCE_X_APPLY_BAD_STATE;
					break;
				}
				/* admission.formation_epoch names the cluster membership
				 * formation.  gate.formation names this Resource-X entry's
				 * formation and is frozen with master_session below; the two
				 * counters are intentionally independent and must not be compared. */
				if (!cluster_semantic_activation_recheck(&admission))
				{
					result = RESOURCE_X_APPLY_STALE;
					break;
				}
				requester_ready
					= gcs_block_pcm_x_resource_x_peer_ready_exact(
						master_node,
						&requester_sender_connection_generation);
				master_ready
					= gcs_block_pcm_x_resource_x_peer_ready_exact(
						master_node,
						&master_ingress_connection_generation);
				if (requester_ready && master_ready)
				{
					bool peer_matches;

					if (master_node == cluster_node_id)
					{
						peer_matches
							= gcs_block_resource_x_target_peer_matches_exact(
								&admission, master_node,
								master_ingress_connection_generation);
						peer_open_result = peer_matches
							? CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH
							: CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_INVALID_INPUT;
					}
					else
					{
						peer_open_result
							= cluster_semantic_activation_resource_x_peer_open_check(
								&admission, master_node,
								master_ingress_connection_generation);
						peer_matches
							= peer_open_result
							  == CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
					}
					if (peer_matches)
					{
						preflight_current = true;
						break;
					}
				}

			preflight_membership_wait:
				diagnostic_stage = "preflight-membership-wait";
				now_us = gcs_block_pcm_x_monotonic_us();
				if (now_us >= absolute_deadline_us)
				{
					diagnostic_deadline_expired = true;
					preflight_backpressure = true;
					result = RESOURCE_X_APPLY_BAD_STATE;
					break;
				}
				remaining_us = absolute_deadline_us - now_us;
				timeout_ms = (long) Min(
					(uint64) Max(
						cluster_gcs_block_retransmit_initial_backoff_ms, 1),
					(remaining_us + UINT64_C(999)) / UINT64_C(1000));
				if (timeout_ms <= 0)
					timeout_ms = 1;
				CHECK_FOR_INTERRUPTS();
				pg_usleep(timeout_ms * 1000L);
				continue;
			}
			if (!preflight_current)
				break;

			diagnostic_stage = "round-loop";

				for (;;) {
					CHECK_FOR_INTERRUPTS();
					diagnostic_head_expired = false;
					now_us = gcs_block_pcm_x_monotonic_us();
					if (now_us >= absolute_deadline_us) {
						diagnostic_deadline_expired = true;
						result = RESOURCE_X_APPLY_BAD_STATE;
						break;
					}
					target_retained_release_inflight = false;
					result = cluster_pcm_lock_resource_x_caller_observe_exact(
						&assertion, gate.formation, master_session, admission.record_generation,
						&caller_witness);
					if (result != RESOURCE_X_APPLY_APPLIED) {
						diagnostic_stage = "caller-history-reject";
						break;
					}
				target_retained_release_post_mutation = false;
				memset(&own, 0, sizeof(own));
					diagnostic_stage = "own-snapshot";
					own_result = cluster_bufmgr_pcm_own_snapshot(buf, &own);
				if (own_result != CLUSTER_PCM_OWN_OK) {
					result = own_result == CLUSTER_PCM_OWN_BUSY
						? RESOURCE_X_APPLY_BAD_STATE
						: own_result == CLUSTER_PCM_OWN_STALE
						? RESOURCE_X_APPLY_STALE
						: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
					break;
				}
					if (!BufferTagsEqual(&own.tag, &assertion.resource)
						|| own.generation == UINT64_MAX) {
						result = RESOURCE_X_APPLY_STALE;
						break;
					}
					if (target_install_follow.valid) {
						target_install_observation_result
							= gcs_block_resource_x_target_install_classify_coherent(
								buf, &target_install_follow, &own,
								&target_install_follow_state, &terminal_ref);
						if (target_install_observation_result
								!= RESOURCE_X_APPLY_APPLIED) {
							result = target_install_observation_result;
							break;
						}
						if (target_install_follow_state == RESOURCE_X_TARGET_INSTALL_RESAMPLE) {
							memset(&target_install_follow, 0, sizeof(target_install_follow));
							target_install_preuse_retry_seen = false;
							continue;
						}
						if (target_install_follow_state
								== RESOURCE_X_TARGET_INSTALL_TERMINAL) {
							action = RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL;
							goto target_install_terminal_recheck;
						}
						if (target_install_follow_state
								== RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY) {
							target_install_preuse_retry_seen = true;
							diagnostic_stage = "target-install-preuse-wait";
							now_us = gcs_block_pcm_x_monotonic_us();
							if (now_us >= absolute_deadline_us) {
								diagnostic_deadline_expired = true;
								result = RESOURCE_X_APPLY_BAD_STATE;
								break;
							}
							remaining_us = absolute_deadline_us - now_us;
							timeout_ms = (long) Min(
								(uint64) Max(
									cluster_gcs_block_retransmit_initial_backoff_ms,
									1),
								(remaining_us + UINT64_C(999))
									/ UINT64_C(1000));
							if (timeout_ms <= 0)
								timeout_ms = 1;
							wait_result
								= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
									&target_install_follow,
									RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY,
									timeout_ms);
							if (wait_result == RESOURCE_X_APPLY_DUPLICATE) {
								memset(&target_install_follow, 0,
									sizeof(target_install_follow));
								target_install_preuse_retry_seen = false;
								continue;
							}
							if (wait_result == RESOURCE_X_APPLY_APPLIED) {
								memset(&target_install_follow, 0, sizeof(target_install_follow));
								target_install_preuse_retry_seen = false;
								continue;
							}
							if (wait_result == RESOURCE_X_APPLY_STALE
								&& target_install_preuse_retry_seen) {
								memset(&target_install_follow, 0,
									sizeof(target_install_follow));
								target_install_preuse_retry_seen = false;
								continue;
							}
							result = wait_result;
							break;
						}
						if (target_install_follow_state
								== RESOURCE_X_TARGET_INSTALL_INFLIGHT) {
							diagnostic_stage = "target-install-continuation-wait";
							now_us = gcs_block_pcm_x_monotonic_us();
							if (now_us >= absolute_deadline_us) {
								diagnostic_deadline_expired = true;
								result = RESOURCE_X_APPLY_BAD_STATE;
								break;
							}
							remaining_us = absolute_deadline_us - now_us;
							timeout_ms = (long) Min(
								(uint64) Max(
									cluster_gcs_block_retransmit_initial_backoff_ms,
									1),
								(remaining_us + UINT64_C(999))
									/ UINT64_C(1000));
							if (timeout_ms <= 0)
								timeout_ms = 1;
							wait_result
								= cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
									&target_install_follow,
									RESOURCE_X_TARGET_INSTALL_INFLIGHT,
									timeout_ms);
							if (wait_result == RESOURCE_X_APPLY_APPLIED
								|| wait_result == RESOURCE_X_APPLY_DUPLICATE) {
								memset(&target_install_follow, 0, sizeof(target_install_follow));
								target_install_preuse_retry_seen = false;
								continue;
							}
							if (wait_result == RESOURCE_X_APPLY_STALE
								&& target_install_preuse_retry_seen) {
								memset(&target_install_follow, 0,
									sizeof(target_install_follow));
								target_install_preuse_retry_seen = false;
								continue;
							}
							result = wait_result;
							break;
						}
						if (target_install_follow_state
								== RESOURCE_X_TARGET_INSTALL_STALE
							&& target_install_preuse_retry_seen) {
							memset(&target_install_follow, 0,
								sizeof(target_install_follow));
							target_install_preuse_retry_seen = false;
							continue;
						}
						result = target_install_follow_state
								== RESOURCE_X_TARGET_INSTALL_STALE
							? RESOURCE_X_APPLY_STALE
							: target_install_follow_state
								== RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED
							? RESOURCE_X_APPLY_RECOVERY_BLOCKED
							: RESOURCE_X_APPLY_INVALID;
						break;
					}
					target_install_observation_result
						= gcs_block_resource_x_target_install_capture_coherent(
							buf, &assertion, master_node, gate.formation,
							master_session, admission.record_generation,
							requester_sender_connection_generation,
							master_ingress_connection_generation,
							retry_slice_us, absolute_deadline_us, &own,
							&target_install_follow_state,
							&target_install_follow);
					if (target_install_observation_result
							!= RESOURCE_X_APPLY_APPLIED) {
						result = target_install_observation_result;
						break;
					}
					if (target_install_follow_state
							== RESOURCE_X_TARGET_INSTALL_RESAMPLE)
						continue;
					if (target_install_follow_state
							== RESOURCE_X_TARGET_INSTALL_INFLIGHT
						|| target_install_follow_state
							== RESOURCE_X_TARGET_INSTALL_TERMINAL
						|| target_install_follow_state
							== RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
						continue;
					if (direct_init) {
					cached_local_x
						= own.pcm_state == (uint8)PCM_STATE_X
						  && own.flags == 0
						  && own.generation
							== direct_init_committed_generation
						  && own.writer_activation_token == 0
						  && own.resource_x_activation_generation == 0;
					if (!cached_local_x) {
						direct_init_pending_n
							= own.pcm_state == (uint8)PCM_STATE_N
							  && own.flags
								== PCM_OWN_FLAG_GRANT_PENDING
							  && own.generation
								== direct_init_ownership_generation
							  && own.reservation_token
								== direct_init_reservation_token
							  && own.writer_activation_token == 0
							  && own.resource_x_activation_generation == 0;
							if (!direct_init_pending_n) {
								result = RESOURCE_X_APPLY_STALE;
								break;
							}
						if (direct_init_pending_n) {
							direct_candidate_result
								= cluster_bufmgr_pcm_own_n_direct_init_candidate_exact(
									buf, &own);
							if (direct_candidate_result != CLUSTER_PCM_OWN_OK) {
								if (direct_candidate_result
										== CLUSTER_PCM_OWN_STALE) {
									memset(&failure_live, 0,
										sizeof(failure_live));
									own_result
										= cluster_bufmgr_pcm_own_snapshot(
											buf, &failure_live);
									if (own_result == CLUSTER_PCM_OWN_OK) {
										target_install_observation_result
											= gcs_block_resource_x_target_install_capture_coherent(
												buf, &assertion, master_node,
												gate.formation, master_session,
												admission.record_generation,
												requester_sender_connection_generation,
												master_ingress_connection_generation,
												retry_slice_us, absolute_deadline_us,
												&failure_live, &target_install_follow_state,
												&target_install_follow);
										if (target_install_observation_result
												!= RESOURCE_X_APPLY_APPLIED) {
											result = target_install_observation_result;
											break;
										}
										if (target_install_follow_state
												== RESOURCE_X_TARGET_INSTALL_RESAMPLE)
											continue;
										if (target_install_follow_state
												== RESOURCE_X_TARGET_INSTALL_INFLIGHT
											|| target_install_follow_state
												== RESOURCE_X_TARGET_INSTALL_TERMINAL
											|| target_install_follow_state
												== RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
										continue;
									}
								}
								result
									= direct_candidate_result == CLUSTER_PCM_OWN_BUSY
									? RESOURCE_X_APPLY_BAD_STATE
									: direct_candidate_result
											== CLUSTER_PCM_OWN_STALE
									? RESOURCE_X_APPLY_STALE
									: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
								break;
							}
						}
					}
				} else {
					if (own.pcm_state == (uint8)PCM_STATE_N
						&& own.flags == PCM_OWN_FLAG_GRANT_PENDING) {
						/* The exact T1 install may have crossed both the
						 * BufferDesc and requester-round terminal transitions
						 * after the snapshot above.  Re-sample both lock domains;
						 * only the same token's one-generation terminal X may
						 * restart this driver under its original deadline. */
						diagnostic_stage = "pending-install-terminal-resample";
						memset(&failure_live, 0, sizeof(failure_live));
						own_result = cluster_bufmgr_pcm_own_snapshot(
							buf, &failure_live);
						if (own_result == CLUSTER_PCM_OWN_OK) {
							target_install_observation_result
								= gcs_block_resource_x_target_install_capture_coherent(
									buf, &assertion, master_node, gate.formation,
									master_session, admission.record_generation,
									requester_sender_connection_generation,
									master_ingress_connection_generation,
									retry_slice_us, absolute_deadline_us,
									&failure_live, &target_install_follow_state,
									&target_install_follow);
							if (target_install_observation_result
									!= RESOURCE_X_APPLY_APPLIED) {
								result = target_install_observation_result;
								break;
							}
							if (target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_RESAMPLE)
								continue;
							if (target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_INFLIGHT
								|| target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_TERMINAL
								|| target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
							continue;
						}
						now_us = gcs_block_pcm_x_monotonic_us();
						if (now_us >= absolute_deadline_us) {
							diagnostic_deadline_expired = true;
							result = RESOURCE_X_APPLY_BAD_STATE;
							break;
						}
						if (cluster_gcs_resource_x_target_local_n_reservation_retry_exact(
								&own, own_result, &failure_live,
								now_us, absolute_deadline_us)) {
							/* Another local caller owns only the reversible
							 * pre-mutation reservation word.  It cannot satisfy
							 * this round and must not fence the global gate. */
							diagnostic_stage = "local-pending-reservation-wait";
							remaining_us = absolute_deadline_us - now_us;
							timeout_ms = (long) Min(
								(uint64) Max(
									cluster_gcs_block_retransmit_initial_backoff_ms,
									1),
								(remaining_us + UINT64_C(999))
									/ UINT64_C(1000));
							if (timeout_ms <= 0)
								timeout_ms = 1;
							CHECK_FOR_INTERRUPTS();
							pg_usleep(timeout_ms * 1000L);
							continue;
						}
					}
					if (own.pcm_state == (uint8)PCM_STATE_N) {
						ClusterPcmOwnResult n_candidate_result;

						if ((own.flags == PCM_OWN_FLAG_REVOKING
								|| own.flags == 0)
							&& own.reservation_token != 0
							&& own.reservation_token != UINT64_MAX
							&& own.writer_activation_token == 0
							&& own.resource_x_activation_generation == 0) {
							ClusterPcmOwnSnapshot resampled;
							ClusterPcmOwnResult current_candidate_result
								= CLUSTER_PCM_OWN_INVALID;
							bool pair_exact;
							bool buffer_exact;
							bool undrained_current_predecessor = false;

							pair_exact
								= cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
									&assertion.resource, master_node,
									master_session, gate.formation,
									own.generation);
							buffer_exact = pair_exact
								&& cluster_bufmgr_pcm_own_n_retained_release_inflight_exact(
									buf, &own);
							target_retained_release_inflight
								= pair_exact && buffer_exact;
							target_retained_release_post_mutation
								= target_retained_release_inflight
								  && own.flags == 0;
							if (pair_exact && !buffer_exact
								&& own.flags == 0) {
								/* VM/FSM DROP has no N+PI carrier.  Revalidate the
								 * exact clean N+CURRENT shape without reading page
								 * bytes, then let the existing round step observe the
								 * retained predecessor under the original deadline. */
								current_candidate_result
									= cluster_bufmgr_pcm_own_n_storage_candidate_exact(
										buf, &own);
								undrained_current_predecessor
									= cluster_gcs_resource_x_target_undrained_current_predecessor_exact(
										pair_exact, buffer_exact,
										current_candidate_result, &own);
							}
							if ((pair_exact && !buffer_exact
									&& !undrained_current_predecessor)
								|| (own.flags == PCM_OWN_FLAG_REVOKING
									&& !target_retained_release_inflight)) {
								memset(&resampled, 0, sizeof(resampled));
								own_result = cluster_bufmgr_pcm_own_snapshot(
									buf, &resampled);
								if (own_result == CLUSTER_PCM_OWN_OK
									&& memcmp(&resampled, &own,
										sizeof(own)) != 0)
									continue;
								if (own_result == CLUSTER_PCM_OWN_OK)
									gcs_block_resource_x_fail_closed_current();
								result = own_result
									== CLUSTER_PCM_OWN_STALE
									? RESOURCE_X_APPLY_STALE
									: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
								break;
							}
						}
						if (!target_retained_release_inflight) {
							/* A generation-zero descriptor is the ordinary cold-N
							 * starting point, not an ownership proof.  Admit it only
							 * through the existing exact BM_VALID/no-IO assertion
							 * predicate; the master still selects DURABLE_STORAGE and
							 * the requester installs only after the exact grant. */
							memset(&failure_live, 0, sizeof(failure_live));
							n_candidate_result
								= cluster_bufmgr_pcm_own_n_assertion_candidate_exact(
									buf, &own, &failure_live);
							if (n_candidate_result != CLUSTER_PCM_OWN_OK) {
								if (n_candidate_result == CLUSTER_PCM_OWN_STALE) {
									target_install_observation_result
										= gcs_block_resource_x_target_install_capture_coherent(
											buf, &assertion, master_node,
											gate.formation, master_session,
											admission.record_generation,
											requester_sender_connection_generation,
											master_ingress_connection_generation,
											retry_slice_us, absolute_deadline_us,
											&failure_live,
											&target_install_follow_state,
											&target_install_follow);
									if (target_install_observation_result
											!= RESOURCE_X_APPLY_APPLIED) {
										result = target_install_observation_result;
										break;
									}
									if (target_install_follow_state
											== RESOURCE_X_TARGET_INSTALL_RESAMPLE)
										continue;
									if (target_install_follow_state
											== RESOURCE_X_TARGET_INSTALL_INFLIGHT
										|| target_install_follow_state
											== RESOURCE_X_TARGET_INSTALL_TERMINAL
										|| target_install_follow_state
											== RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
										continue;
									now_us = gcs_block_pcm_x_monotonic_us();
									if (cluster_gcs_resource_x_target_preassert_resample_exact(
											&own, n_candidate_result, &failure_live,
											target_install_follow_state, now_us,
											absolute_deadline_us)) {
										diagnostic_stage = "preassert-observation-resample";
										continue;
									}
								}
									result
										= gcs_block_pcm_x_resource_x_remote_s_own_result(
											n_candidate_result);
									memset(&failure_round, 0,
										sizeof(failure_round));
									failure_snapshot_result
										= cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
											&assertion, master_node, gate.formation,
											master_session,
											admission.record_generation,
											requester_sender_connection_generation,
											master_ingress_connection_generation,
											retry_slice_us, &failure_round);
										memset(&first_failure, 0,
										sizeof(first_failure));
									first_failure.tag = assertion.resource;
									first_failure.request_sequence
										= diagnostic_request_sequence;
									first_failure.admission_generation
										= admission.record_generation;
									first_failure.requester_node
										= assertion.requester_node;
									first_failure.buffer_generation_before
										= own.generation;
									first_failure.buffer_generation_after
										= failure_live.generation;
									first_failure.buffer_token_before
										= own.reservation_token;
									first_failure.buffer_token_after
										= failure_live.reservation_token;
									first_failure.buffer_writer_token_before
										= own.writer_activation_token;
									first_failure.buffer_writer_token_after
										= failure_live.writer_activation_token;
									first_failure.buffer_resource_x_generation_before
										= own.resource_x_activation_generation;
									first_failure.buffer_resource_x_generation_after
										= failure_live.resource_x_activation_generation;
									first_failure.formation = gate.formation;
									first_failure.master_session = master_session;
									first_failure.r4_generation
										= admission.record_generation;
									first_failure.absolute_deadline_us
										= absolute_deadline_us;
									first_failure.remote_s_stage
										= RESOURCE_X_REMOTE_S_STAGE_NONE;
									first_failure.failure_domain
										= gcs_block_resource_x_pre_mutation_domain(
											n_candidate_result);
									first_failure.result = result;
									if (failure_snapshot_result
											== RESOURCE_X_APPLY_APPLIED) {
										first_failure.base_authority_generation
											= failure_round.base_authority_generation;
										first_failure.authority_generation
											= failure_round.authority_generation;
										first_failure.assertion_sequence
											= failure_round.ref.acquisition_generation;
										first_failure.round_terminal
											= failure_round.terminal != 0;
										first_failure.round_phase
											= failure_round.round_phase;
										first_failure.round_progress_flags
											= failure_round.progress_flags;
									}
									gcs_block_resource_x_first_failure_record(
										&first_failure);
									first_failure_recorded = true;
									break;
							}
						}
					} else if (own.generation == 0) {
						result = RESOURCE_X_APPLY_STALE;
						break;
					}
					cached_local_x
						= own.pcm_state == (uint8)PCM_STATE_X
						  && own.flags == 0
						  && own.writer_activation_token == 0
						  && own.resource_x_activation_generation == 0;
				}
				if (target_retained_release_inflight) {
					if (!cluster_semantic_activation_recheck(&admission)
						|| !gcs_block_resource_x_gate_session_recheck(
							&resource, &gate, master_node,
							master_session)) {
						if (target_retained_release_post_mutation)
							gcs_block_resource_x_fail_closed_current();
						result = target_retained_release_post_mutation
							? RESOURCE_X_APPLY_RECOVERY_BLOCKED
							: RESOURCE_X_APPLY_STALE;
						break;
					}
					diagnostic_stage = "retained-release-wait";
					now_us = gcs_block_pcm_x_monotonic_us();
					if (now_us >= absolute_deadline_us) {
						diagnostic_deadline_expired = true;
						if (target_retained_release_post_mutation)
							gcs_block_resource_x_fail_closed_current();
						result = target_retained_release_post_mutation
							? RESOURCE_X_APPLY_RECOVERY_BLOCKED
							: RESOURCE_X_APPLY_BAD_STATE;
						break;
					}
					CHECK_FOR_INTERRUPTS();
					wait_result = cluster_pcm_lock_resource_x_predecessor_wait_exact(
						&resource, master_node, master_session, gate.formation, own.generation,
						absolute_deadline_us, retry_slice_us);
					if (wait_result != RESOURCE_X_APPLY_APPLIED
						&& wait_result != RESOURCE_X_APPLY_DUPLICATE) {
						result = wait_result;
						break;
					}
					continue;
				}
				diagnostic_stage = "round-step";
				now_us = gcs_block_pcm_x_monotonic_us();
				memset(&dispatch, 0, sizeof(dispatch));
				memset(&terminal_ref, 0, sizeof(terminal_ref));
				if (direct_init && own.pcm_state == (uint8)PCM_STATE_N
					&& own.flags == PCM_OWN_FLAG_GRANT_PENDING) {
					ResourceXInstallClaimJoinObservation claim_join;
					ClusterPcmOwnSnapshot after;
					ResourceXApplyResult claim_result;

					/* own is B-before; this read-only capture is E.  No claim
					 * mutation is allowed until the complete B-after matches. */
					claim_result = cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
						&assertion, master_node, gate.formation, master_session,
						admission.record_generation, requester_sender_connection_generation,
						master_ingress_connection_generation, &claim_join);
					if (claim_result == RESOURCE_X_APPLY_APPLIED) {
						own_result = cluster_bufmgr_pcm_own_snapshot(buf, &after);
						if (own_result != CLUSTER_PCM_OWN_OK) {
							result
								= gcs_block_resource_x_target_install_snapshot_result(own_result);
							break;
						}
						if (!cluster_pcm_own_snapshot_equal_exact(&own, &after))
							continue;
						claim_result
							= cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(
								&claim_join, &own, &after);
						if (claim_result != RESOURCE_X_APPLY_APPLIED
							&& claim_result != RESOURCE_X_APPLY_DUPLICATE
							&& claim_result != RESOURCE_X_APPLY_BAD_STATE) {
							result = claim_result;
							break;
						}
						/* BAD_STATE here includes T1 winning before the bind.
						 * The ordinary step may wait but cannot adopt our token. */
					} else if (claim_result != RESOURCE_X_APPLY_NOT_FOUND
							   && claim_result != RESOURCE_X_APPLY_BAD_STATE) {
						result = claim_result;
						break;
					}
				}
				action = cluster_pcm_lock_resource_x_bootstrap_round_step_observed_exact(
					&assertion, master_node, gate.formation, master_session,
					admission.record_generation, requester_sender_connection_generation,
					master_ingress_connection_generation, absolute_deadline_us,
					gcs_block_pcm_x_retry_timeout_us(), now_us, retry_slice_us,
					direct_init_ownership_generation, direct_init_reservation_token, !join_only,
					own.pcm_state == PCM_STATE_N, cached_local_x,
					cached_local_x ? own.generation : 0, &caller_witness, &own, &dispatch,
					&terminal_ref);

			target_install_terminal_recheck:
				if (action == RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED
					&& cluster_pcm_rx_last_step_head_failure()
						   == RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED)
					diagnostic_head_expired = true;
				if (action == RESOURCE_X_BOOTSTRAP_ROUND_WAIT && !diagnostic_join_recorded) {
					cluster_pcm_rx_metric_note(PCM_RX_HEAD_JOIN);
					cluster_pcm_vm_metric_note(&resource, PCM_VM_HEAD_JOIN);
					diagnostic_join_recorded = true;
				}
					if (action == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL) {
						diagnostic_stage = "terminal-recheck";
						if (target_install_follow.valid) {
							memset(&failure_live, 0, sizeof(failure_live));
							own_result = cluster_bufmgr_pcm_own_snapshot(
								buf, &failure_live);
							if (own_result != CLUSTER_PCM_OWN_OK) {
								result = own_result == CLUSTER_PCM_OWN_BUSY
									? RESOURCE_X_APPLY_BAD_STATE
									: own_result == CLUSTER_PCM_OWN_STALE
									? RESOURCE_X_APPLY_STALE
									: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
								break;
							}
							target_install_observation_result
								= gcs_block_resource_x_target_install_classify_coherent(
									buf, &target_install_follow, &failure_live,
									&target_install_follow_state, &terminal_ref);
							if (target_install_observation_result
									!= RESOURCE_X_APPLY_APPLIED) {
								result = target_install_observation_result;
								break;
							}
							if (target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_RESAMPLE)
								continue;
							if (target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY) {
								target_install_preuse_retry_seen = true;
								continue;
							}
							if (target_install_follow_state
									== RESOURCE_X_TARGET_INSTALL_STALE
								&& target_install_preuse_retry_seen) {
								memset(&target_install_follow, 0,
									sizeof(target_install_follow));
								target_install_preuse_retry_seen = false;
								continue;
							}
							if (target_install_follow_state
									!= RESOURCE_X_TARGET_INSTALL_TERMINAL) {
								result = target_install_follow_state
										== RESOURCE_X_TARGET_INSTALL_STALE
									? RESOURCE_X_APPLY_STALE
									: target_install_follow_state
										== RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED
									? RESOURCE_X_APPLY_RECOVERY_BLOCKED
									: RESOURCE_X_APPLY_INVALID;
								break;
							}
						}
						terminal_admission_current
						= cluster_semantic_activation_recheck(&admission);
					if (terminal_admission_current) {
						memset(&terminal_gate, 0, sizeof(terminal_gate));
						terminal_master_node = -1;
						terminal_master_session = 0;
						terminal_session_check
							= gcs_block_resource_x_gate_session_snapshot_result(
								&resource, &terminal_gate,
								&terminal_master_node,
								&terminal_master_session);
						terminal_gate_session_current
							= terminal_session_check == PCM_X_SESSION_AUTH_OK
							  && memcmp(&terminal_gate, &gate,
								  sizeof(terminal_gate)) == 0
							  && terminal_master_node == master_node
							  && terminal_master_session == master_session;
					}
					if (!terminal_admission_current
						|| !terminal_gate_session_current) {
						result = RESOURCE_X_APPLY_STALE;
						break;
					}
					memset(&target_install_follow, 0,
						sizeof(target_install_follow));
					target_install_preuse_retry_seen = false;
					*ref_out = terminal_ref;
					/* All success paths, including coherent install-follow and
					 * cached-X/direct-init resampling, converge here.  A background
					 * T3 cannot resurrect a caller attached to a failed attempt. */
					result = cluster_pcm_lock_resource_x_caller_observe_exact(
						&assertion, gate.formation, master_session, admission.record_generation,
						&caller_witness);
					now_us = gcs_block_pcm_x_monotonic_us();
					if (result != RESOURCE_X_APPLY_APPLIED || now_us == 0
						|| now_us >= absolute_deadline_us) {
						memset(ref_out, 0, sizeof(*ref_out));
						diagnostic_stage = "terminal-caller-history-reject";
						diagnostic_deadline_expired = now_us >= absolute_deadline_us;
						if (result == RESOURCE_X_APPLY_APPLIED)
							result = RESOURCE_X_APPLY_BAD_STATE;
						break;
					}
					result = RESOURCE_X_APPLY_APPLIED;
					break;
				}
				if (action == RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE) {
					diagnostic_stage = "round-capacity-backpressure";
					result = RESOURCE_X_APPLY_BAD_STATE;
					break;
				}
				if (action == RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED) {
					/* The cached-X sample can be invalidated by an exact type-17
					 * settlement before this caller creates any round.  Re-sample
					 * both lock domains and every authority input.  Either the exact
					 * retained predecessor or its exact clean successor may start one
					 * fresh iteration under the unchanged first deadline; no old
					 * proof, image, status, or attempt is retained. */
					memset(&failure_live, 0, sizeof(failure_live));
					own_result
						= cluster_bufmgr_pcm_own_snapshot(buf, &failure_live);
					memset(&failure_round, 0, sizeof(failure_round));
					failure_snapshot_result
						= cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
							&assertion, master_node, gate.formation,
							master_session, admission.record_generation,
							requester_sender_connection_generation,
							master_ingress_connection_generation,
							retry_slice_us, &failure_round);
					requester_sender_recheck = 0;
					master_ingress_recheck = 0;
					dispatch_admission_current
						= cluster_semantic_activation_recheck(&admission);
					dispatch_gate_session_current
						= gcs_block_resource_x_gate_session_recheck(
							&resource, &gate, master_node, master_session);
					dispatch_requester_sampled
						= gcs_block_pcm_x_resource_x_peer_ready_exact(
							master_node, &requester_sender_recheck);
					dispatch_master_sampled
						= gcs_block_pcm_x_resource_x_peer_ready_exact(
							master_node, &master_ingress_recheck);
					dispatch_recheck_failure_mask
						= cluster_gcs_resource_x_dispatch_recheck_failure_mask(
							dispatch_admission_current,
							dispatch_gate_session_current,
							dispatch_requester_sampled,
							dispatch_master_sampled,
							requester_sender_connection_generation,
							master_ingress_connection_generation,
							requester_sender_recheck,
							master_ingress_recheck);
					round_drift_authority_current
						= dispatch_recheck_failure_mask
						  == RESOURCE_X_DISPATCH_RECHECK_OK;
					if (round_drift_authority_current) {
						if (master_node == cluster_node_id)
							round_drift_authority_current
								= gcs_block_resource_x_target_peer_matches_exact(
									&admission, master_node,
									master_ingress_recheck);
						else {
							peer_open_result
								= cluster_semantic_activation_resource_x_peer_open_check(
									&admission, master_node,
									master_ingress_recheck);
							round_drift_authority_current
								= peer_open_result
								  == CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
						}
					}
					round_drift_retained_pair_exact = false;
					round_drift_retained_buffer_exact = false;
					if (round_drift_authority_current
						&& own_result == CLUSTER_PCM_OWN_OK
						&& failure_live.pcm_state == (uint8)PCM_STATE_N) {
						round_drift_retained_pair_exact
							= cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
								&assertion.resource, master_node, master_session,
								gate.formation, failure_live.generation);
						round_drift_retained_buffer_exact
							= round_drift_retained_pair_exact
							  && cluster_bufmgr_pcm_own_n_retained_release_inflight_exact(
								  buf, &failure_live);
					}
					now_us = gcs_block_pcm_x_monotonic_us();
					if (cluster_gcs_resource_x_target_retained_predecessor_retry_exact(
							action, direct_init, join_only, &own,
							own_result, &failure_live,
							round_drift_retained_pair_exact,
							round_drift_retained_buffer_exact,
							round_drift_authority_current, now_us,
							absolute_deadline_us)
						|| cluster_gcs_resource_x_target_empty_round_drift_retry_exact(
							action, direct_init, join_only, &own,
							own_result, &failure_live,
							failure_snapshot_result,
							round_drift_authority_current, now_us,
							absolute_deadline_us))
						continue;
					diagnostic_stage = "round-fail-closed";
					diagnostic_deadline_expired = now_us >= absolute_deadline_us;
					result = now_us >= absolute_deadline_us
						? RESOURCE_X_APPLY_BAD_STATE
						: RESOURCE_X_APPLY_STALE;
					break;
				}
				if (action
					== RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT) {
					/* The exact predecessor pair is older than this local request.
					 * Keep the requester round empty.  Register against the existing
					 * settlement predicate on the same entry CV under this caller's
					 * original deadline, without inventing a head or a lease. */
					diagnostic_stage = "predecessor-settlement-wait";
					if (!cluster_semantic_activation_recheck(&admission)
						|| !gcs_block_resource_x_gate_session_recheck(
							&resource, &gate, master_node,
							master_session)) {
						result = RESOURCE_X_APPLY_STALE;
						break;
					}
					now_us = gcs_block_pcm_x_monotonic_us();
					if (now_us >= absolute_deadline_us) {
						diagnostic_deadline_expired = true;
						result = RESOURCE_X_APPLY_BAD_STATE;
						break;
					}
					CHECK_FOR_INTERRUPTS();
					wait_result = cluster_pcm_lock_resource_x_predecessor_wait_exact(
						&resource, master_node, master_session, gate.formation, 0,
						absolute_deadline_us, retry_slice_us);
					diagnostic_wait_failure = cluster_pcm_rx_take_wait_failure();
					diagnostic_deadline_expired
						= diagnostic_wait_failure == PCM_RX_WAIT_CALLER_DEADLINE_EXPIRED;
					if (wait_result != RESOURCE_X_APPLY_APPLIED
						&& wait_result != RESOURCE_X_APPLY_DUPLICATE) {
						result = wait_result;
						break;
					}
					continue;
				}
				if (action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST
					|| action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT) {
					diagnostic_stage = "dispatch-recheck";
					requester_sender_recheck = 0;
					master_ingress_recheck = 0;
					dispatch_admission_current
						= cluster_semantic_activation_recheck(&admission);
					dispatch_gate_session_current
						= gcs_block_resource_x_gate_session_recheck(
							&resource, &gate, master_node,
							master_session);
					dispatch_requester_sampled
						= gcs_block_pcm_x_resource_x_peer_ready_exact(
							master_node, &requester_sender_recheck);
					dispatch_master_sampled
						= gcs_block_pcm_x_resource_x_peer_ready_exact(
							master_node, &master_ingress_recheck);
					dispatch_recheck_failure_mask
						= cluster_gcs_resource_x_dispatch_recheck_failure_mask(
							dispatch_admission_current,
							dispatch_gate_session_current,
							dispatch_requester_sampled,
							dispatch_master_sampled,
							requester_sender_connection_generation,
							master_ingress_connection_generation,
							requester_sender_recheck,
							master_ingress_recheck);
					if (dispatch_recheck_failure_mask
						!= RESOURCE_X_DISPATCH_RECHECK_OK) {
						/* D1 AUTHORITY_DRIFT: a pre-ACK/pre-ASSERT kind-9 round
						 * is not authority even if its request reached a master
						 * RECEIVED receipt.  A transient session observation keeps
						 * the exact round and waits under its first R7 deadline.  A
						 * fully current but changed gate/session/connection tuple
						 * may discard only the byte-exact REQUEST_DISPATCHED
						 * binding, retain its attempt floor, and recapture current
						 * authority under that same deadline.  The higher attempt
						 * replaces any unconsumed old receipt; no proof or image is
						 * reused. */
						if (action
								== RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST
							&& !join_only && dispatch_admission_current) {
							memset(&rebound_gate, 0, sizeof(rebound_gate));
							rebound_master_node = -1;
							rebound_master_session = 0;
							rebound_requester_connection_generation = 0;
							rebound_master_connection_generation = 0;
							rebound_requester_sampled = false;
							rebound_master_sampled = false;
							rebound_peer_matches = false;
							dispatch_session_check
								= gcs_block_resource_x_gate_session_snapshot_result(
									&resource, &rebound_gate,
									&rebound_master_node,
									&rebound_master_session);
							if (cluster_gcs_pcm_x_auth_result_retryable(
									dispatch_session_check))
								goto dispatch_recheck_wait;
							if (dispatch_session_check
									== PCM_X_SESSION_AUTH_OK) {
								rebound_requester_sampled
									= gcs_block_pcm_x_resource_x_peer_ready_exact(
										rebound_master_node,
										&rebound_requester_connection_generation);
								rebound_master_sampled
									= gcs_block_pcm_x_resource_x_peer_ready_exact(
										rebound_master_node,
										&rebound_master_connection_generation);
								if (!rebound_requester_sampled
									|| !rebound_master_sampled)
									goto dispatch_recheck_wait;
								if (rebound_master_node == cluster_node_id)
									rebound_peer_matches
										= gcs_block_resource_x_target_peer_matches_exact(
											&admission, rebound_master_node,
											rebound_master_connection_generation);
								else {
									peer_open_result
										= cluster_semantic_activation_resource_x_peer_open_check(
											&admission, rebound_master_node,
											rebound_master_connection_generation);
									rebound_peer_matches
										= peer_open_result
										  == CLUSTER_SEMANTIC_RESOURCE_X_PEER_OPEN_MATCH;
								}
								if (!rebound_peer_matches)
									goto dispatch_recheck_wait;
								if (!cluster_semantic_activation_recheck(
										&admission)) {
									result = RESOURCE_X_APPLY_STALE;
									break;
								}
								if (memcmp(&rebound_gate, &gate,
										sizeof(rebound_gate)) == 0
									&& rebound_master_node == master_node
									&& rebound_master_session == master_session
									&& rebound_requester_connection_generation
										== requester_sender_connection_generation
									&& rebound_master_connection_generation
										== master_ingress_connection_generation)
									continue;
								discard_result
									= cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
										&dispatch, master_node,
										admission.record_generation,
										master_ingress_connection_generation,
										retry_slice_us, absolute_deadline_us,
										direct_init_ownership_generation,
										direct_init_reservation_token);
								if (discard_result
										!= RESOURCE_X_APPLY_APPLIED) {
									result = discard_result;
									break;
								}
								gate = rebound_gate;
								/* The preceding production owner proved exact
								 * pre-admission discard, not a failed delivery.
								 * Keep the caller deadline while leaving that
								 * retired, non-authoritative attempt. */
								memset(&caller_witness, 0, sizeof(caller_witness));
								master_node = rebound_master_node;
								master_session = rebound_master_session;
								requester_sender_connection_generation
									= rebound_requester_connection_generation;
								master_ingress_connection_generation
									= rebound_master_connection_generation;
								continue;
							}
							result = RESOURCE_X_APPLY_STALE;
							break;

					dispatch_recheck_wait:
							now_us = gcs_block_pcm_x_monotonic_us();
							if (now_us >= absolute_deadline_us) {
								diagnostic_deadline_expired = true;
								result = RESOURCE_X_APPLY_BAD_STATE;
								break;
							}
							remaining_us = absolute_deadline_us - now_us;
							timeout_ms = (long)Min(
								(uint64)Max(
									cluster_gcs_block_retransmit_initial_backoff_ms,
									1),
								(remaining_us + UINT64_C(999))
									/ UINT64_C(1000));
							if (timeout_ms <= 0)
								timeout_ms = 1;
							CHECK_FOR_INTERRUPTS();
							pg_usleep(timeout_ms * 1000L);
							continue;
						}
						result = RESOURCE_X_APPLY_STALE;
						break;
					}
					cluster_pcm_rx_dispatch_note(
						action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST
						|| action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
					if (action == RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST) {
						diagnostic_stage = "dispatch-bootstrap-request";
						stage_ok
							= gcs_block_resource_x_bootstrap_request_stage_exact(
								master_node, &dispatch);
					} else {
						diagnostic_stage = "dispatch-assert";
						stage_ok
							= gcs_block_resource_x_native_assert_stage_exact(
								master_node, &dispatch)
							  == RESOURCE_X_APPLY_APPLIED;
					}
					(void)stage_ok;
					continue;
				}
				if (action != RESOURCE_X_BOOTSTRAP_ROUND_WAIT) {
					result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
					break;
				}
				/* The serialized step already adjudicated the physical sample.
				 * WAIT only enrolls/rechecks; it cannot revoke its own install. */
				now_us = gcs_block_pcm_x_monotonic_us();
				if (now_us >= absolute_deadline_us) {
					diagnostic_deadline_expired = true;
					result = RESOURCE_X_APPLY_BAD_STATE;
					break;
				}
				remaining_us = absolute_deadline_us - now_us;
				timeout_ms = (long)Min(
					(uint64)Max(
						cluster_gcs_block_retransmit_initial_backoff_ms, 1),
					(remaining_us + UINT64_C(999)) / UINT64_C(1000));
				if (timeout_ms <= 0)
					timeout_ms = 1;
				diagnostic_stage = "round-wait";
					if (direct_init)
						wait_result
							= cluster_pcm_lock_resource_x_bootstrap_round_wait_direct_init_exact(
								&assertion, master_node, gate.formation, master_session,
								admission.record_generation, requester_sender_connection_generation,
								master_ingress_connection_generation, retry_slice_us,
								direct_init_ownership_generation, direct_init_reservation_token,
								absolute_deadline_us, timeout_ms);
					else
						wait_result = cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
							&assertion, master_node, gate.formation, master_session,
							admission.record_generation, requester_sender_connection_generation,
							master_ingress_connection_generation, retry_slice_us,
							absolute_deadline_us, timeout_ms);
					diagnostic_wait_failure = cluster_pcm_rx_take_wait_failure();
					diagnostic_deadline_expired
						= diagnostic_wait_failure == PCM_RX_WAIT_CALLER_DEADLINE_EXPIRED;
					diagnostic_head_expired
						= diagnostic_wait_failure == PCM_RX_WAIT_HEAD_NO_PROGRESS_EXPIRED;
					if (wait_result != RESOURCE_X_APPLY_APPLIED
						&& wait_result != RESOURCE_X_APPLY_DUPLICATE) {
						/* The registered wait and BufferDesc use independent lock
					 * domains.  A same-node T1 executor can publish terminal X
					 * between the wait predicate and this return, making the old
					 * wait identity STALE even though the exact requested install
					 * completed.  Admit no generic STALE retry: independently
					 * resample both domains and require the frozen pending(T) ->
					 * terminal-X(T+1) proof before restarting the full driver. */
						if (!direct_init && !join_only && wait_result == RESOURCE_X_APPLY_STALE
							&& own.pcm_state == (uint8)PCM_STATE_N
							&& own.flags == PCM_OWN_FLAG_GRANT_PENDING) {
							diagnostic_stage = "wait-terminal-resample";
							memset(&failure_live, 0, sizeof(failure_live));
							own_result = cluster_bufmgr_pcm_own_snapshot(buf, &failure_live);
							memset(&failure_round, 0, sizeof(failure_round));
							failure_snapshot_result
								= cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
									&assertion, master_node, gate.formation, master_session,
									admission.record_generation,
									requester_sender_connection_generation,
									master_ingress_connection_generation, retry_slice_us,
									&failure_round);
							pending_terminal_resample_mismatch
								= own_result == CLUSTER_PCM_OWN_OK
									  ? cluster_gcs_resource_x_pending_terminal_resample_mismatch(
											&own, &failure_live, failure_snapshot_result,
											&failure_round)
									  : RESOURCE_X_PENDING_TERMINAL_MISMATCH_INPUT;
							ereport(LOG,
							(errmsg_internal("Resource-X pending terminal resample diagnostic"),
							 errdetail("mismatch=0x%016llx live_result=%d round_result=%d "
								"before_state=%u before_flags=0x%x before_generation=%llu "
								"before_token=%llu before_writer=%llu before_resource_x=%llu "
								"live_state=%u live_flags=0x%x live_generation=%llu "
								"live_token=%llu live_writer=%llu live_resource_x=%llu "
								"round_terminal=%u round_phase=%u round_buffer_generation=%llu "
								"round_requester=%d round_formation=%llu round_acquisition=%llu "
								"round_base_authority=%llu round_authority=%llu round_r4=%llu "
								"round_session=%llu round_deadline=%llu round_retired=%llu "
								"round_progress=0x%x",
								(unsigned long long) pending_terminal_resample_mismatch,
								(int) own_result, (int) failure_snapshot_result,
								(unsigned) own.pcm_state, own.flags,
								(unsigned long long) own.generation,
								(unsigned long long) own.reservation_token,
								(unsigned long long) own.writer_activation_token,
								(unsigned long long) own.resource_x_activation_generation,
								(unsigned) failure_live.pcm_state, failure_live.flags,
								(unsigned long long) failure_live.generation,
								(unsigned long long) failure_live.reservation_token,
								(unsigned long long) failure_live.writer_activation_token,
								(unsigned long long) failure_live.resource_x_activation_generation,
								(unsigned) failure_round.terminal,
								(unsigned) failure_round.round_phase,
								(unsigned long long) failure_round.buffer_ownership_generation,
								failure_round.ref.assertion.requester_node,
								(unsigned long long) failure_round.ref.formation,
								(unsigned long long) failure_round.ref.acquisition_generation,
								(unsigned long long) failure_round.base_authority_generation,
								(unsigned long long) failure_round.authority_generation,
								(unsigned long long) failure_round.r4_record_generation,
								(unsigned long long) failure_round.master_session_incarnation,
								(unsigned long long) failure_round.absolute_deadline_us,
								(unsigned long long) failure_round.retired_acquisition_generation,
								failure_round.progress_flags)));
					}
					result = wait_result;
					break;
				}
			}
		} while (false);
	}
	PG_CATCH();
	{
		memset(&target_install_follow, 0, sizeof(target_install_follow));
		if (absolute_deadline_us >= diagnostic_caller_budget_us
			&& absolute_deadline_us != UINT64_MAX) {
			uint64 ended_us = gcs_block_pcm_x_monotonic_us();
			uint64 began_us = absolute_deadline_us - diagnostic_caller_budget_us;

			if (ended_us >= began_us)
				cluster_pcm_wait_margin_note_exact(false, ended_us - began_us,
												   diagnostic_caller_budget_us, &resource,
												   cluster_node_id, diagnostic_stage, 0, ended_us);
		}
		cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	memset(&target_install_follow, 0, sizeof(target_install_follow));
	if (absolute_deadline_us >= diagnostic_caller_budget_us && absolute_deadline_us != UINT64_MAX) {
		uint64 ended_us = gcs_block_pcm_x_monotonic_us();
		uint64 began_us = absolute_deadline_us - diagnostic_caller_budget_us;

		if (ended_us >= began_us)
			cluster_pcm_wait_margin_note_exact(
				false, ended_us - began_us, diagnostic_caller_budget_us, &resource, cluster_node_id,
				diagnostic_stage, diagnostic_request_sequence, ended_us);
	}
	cluster_semantic_activation_leave(&admission);
	if (result != RESOURCE_X_APPLY_APPLIED && !first_failure_recorded) {
		memset(&failure_round, 0, sizeof(failure_round));
		failure_snapshot_result
			= cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
				&assertion, master_node, gate.formation, master_session,
				admission_record_generation,
				requester_sender_connection_generation,
				master_ingress_connection_generation, retry_slice_us,
				&failure_round);
		memset(&failure_live, 0, sizeof(failure_live));
		own_result = cluster_bufmgr_pcm_own_snapshot(buf, &failure_live);
		memset(&first_failure, 0, sizeof(first_failure));
		first_failure.tag = resource;
		first_failure.request_sequence = diagnostic_request_sequence;
		first_failure.admission_generation = admission_record_generation;
		first_failure.requester_node = cluster_node_id;
		first_failure.buffer_generation_before = own.generation;
		first_failure.buffer_generation_after
			= own_result == CLUSTER_PCM_OWN_OK ? failure_live.generation : 0;
		first_failure.buffer_token_before = own.reservation_token;
		first_failure.buffer_token_after
			= own_result == CLUSTER_PCM_OWN_OK
			? failure_live.reservation_token : 0;
		first_failure.buffer_writer_token_before
			= own.writer_activation_token;
		first_failure.buffer_writer_token_after
			= own_result == CLUSTER_PCM_OWN_OK
			? failure_live.writer_activation_token : 0;
		first_failure.buffer_resource_x_generation_before
			= own.resource_x_activation_generation;
		first_failure.buffer_resource_x_generation_after
			= own_result == CLUSTER_PCM_OWN_OK
			? failure_live.resource_x_activation_generation : 0;
		first_failure.formation = gate.formation;
		first_failure.master_session = master_session;
		first_failure.r4_generation = admission_record_generation;
		first_failure.absolute_deadline_us = absolute_deadline_us;
		first_failure.remote_s_stage = RESOURCE_X_REMOTE_S_STAGE_NONE;
		first_failure.failure_domain = preflight_backpressure
			? RESOURCE_X_FAIL_PRE_MUTATION_BACKPRESSURE
			: gcs_block_resource_x_target_failure_domain(
				result, failure_snapshot_result, &failure_round, own_result);
		first_failure.result = result;
		if (failure_snapshot_result == RESOURCE_X_APPLY_APPLIED) {
			first_failure.base_authority_generation
				= failure_round.base_authority_generation;
			first_failure.authority_generation
				= failure_round.authority_generation;
			first_failure.assertion_sequence
				= failure_round.ref.acquisition_generation;
			first_failure.round_terminal = failure_round.terminal != 0;
			first_failure.round_phase = failure_round.round_phase;
			first_failure.round_progress_flags
				= failure_round.progress_flags;
		}
		gcs_block_resource_x_first_failure_record(&first_failure);
		first_failure_recorded = true;
	}
	if (result != RESOURCE_X_APPLY_APPLIED)
		ereport(
			LOG,
			(errmsg_internal("Resource-X target acquire diagnostic"),
			 errdetail("stage=%s result=%d action=%d wait=%d ownership_loss=%d "
					   "stage_ok=%s requester=%d master=%d buffer=%d tag=%u/%u/%u/%d/%u "
					   "formation=%llu session=%llu r4_generation=%llu "
					   "requester_connection=%u master_connection=%u peer_open_reason=%d "
					   "preflight_session_check=%d "
					   "dispatch_recheck_mask=0x%08x requester_recheck=%u "
					   "master_recheck=%u "
					   "terminal_admission_current=%s terminal_session_check=%d "
					   "terminal_gate_current=%s terminal_master=%d "
					   "terminal_session=%llu "
					   "own_state=%u "
					   "own_flags=%u own_generation=%llu own_writer_token=%llu "
					   "own_resource_x_generation=%llu now=%llu deadline=%llu "
					   "caller_joined_attempt=%llu caller_failed_attempt=%llu",
					   diagnostic_stage, (int)result, (int)action, (int)wait_result,
					   (int)ownership_loss_result, stage_ok ? "true" : "false",
					   assertion.requester_node, master_node, buf->buf_id, resource.spcOid,
					   resource.dbOid, resource.relNumber, (int)resource.forkNum, resource.blockNum,
					   (unsigned long long)gate.formation, (unsigned long long)master_session,
					   (unsigned long long)admission_record_generation,
					   requester_sender_connection_generation, master_ingress_connection_generation,
					   (int)peer_open_result, (int)preflight_session_check,
					   dispatch_recheck_failure_mask, requester_sender_recheck,
					   master_ingress_recheck, terminal_admission_current ? "true" : "false",
					   (int)terminal_session_check,
					   terminal_gate_session_current ? "true" : "false", terminal_master_node,
					   (unsigned long long)terminal_master_session, (unsigned)own.pcm_state,
					   (unsigned)own.flags, (unsigned long long)own.generation,
					   (unsigned long long)own.writer_activation_token,
					   (unsigned long long)own.resource_x_activation_generation,
					   (unsigned long long)now_us, (unsigned long long)absolute_deadline_us,
					   (unsigned long long)caller_witness.joined_request.assertion_sequence,
					   (unsigned long long)caller_witness.failed_attempt)));
	if (result != RESOURCE_X_APPLY_APPLIED && result != RESOURCE_X_APPLY_DUPLICATE) {
		gcs_resource_x_acquire_diagnostic.buffer_id = buf->buf_id;
		gcs_resource_x_acquire_diagnostic.result = result;
		/* The diagnostic request sequence is not the protocol attempt. The
		 * latter can be unavailable on a rejected follower; preserve zero. */
		gcs_resource_x_acquire_diagnostic.attempt = 0;
		gcs_resource_x_acquire_diagnostic.reason = cluster_gcs_resource_x_acquire_failure_reason(
			diagnostic_head_expired, diagnostic_deadline_expired, result);
		if (caller_witness.failed_attempt != 0) {
			gcs_resource_x_acquire_diagnostic.attempt = caller_witness.failed_attempt;
			gcs_resource_x_acquire_diagnostic.reason = "FAILED_CALLER_ATTEMPT";
		}
		gcs_resource_x_acquire_diagnostic.valid = true;
		if (diagnostic_deadline_expired && !diagnostic_head_expired)
			cluster_pcm_rx_metric_note(PCM_RX_FOLLOWER_DEADLINE);
	}
	return result;
}

/* Freeze the existing kind-4 RELEASE_X while the descriptor is still the
 * exact X+REVOKING residency.  Entry-local EVICTING is lifecycle ownership,
 * never authority; every failure before local commit drops it exactly. */
ResourceXApplyResult
cluster_gcs_resource_x_target_evict_prepare_exact(
	const BufferTag *tag, const ClusterPcmOwnSnapshot *exact_x,
	uint64 r4_record_generation, uint64 reservation_token,
	ResourceXTargetEvictionPlan *plan_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ResourceXGateSnapshot gate;
	ResourceXDecodedFrame release;
	ResourceXLocalOwnerHandle owner;
	volatile ResourceXLocalOwnerHandle cleanup_owner;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	ResourceXApplyResult abort_result;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	uint8 payload[RESOURCE_X_CONTROL_V1_BYTES];
	uint64 master_session = 0;
	uint16 payload_bytes = 0;
	uint32 sender_connection_generation = 0;
	uint32 sender_connection_recheck = 0;
	int32 master_node = -1;
	volatile bool owner_claimed = false;

	if (plan_out != NULL)
		memset(plan_out, 0, sizeof(*plan_out));
	if (tag == NULL || exact_x == NULL || plan_out == NULL
		|| !BufferTagsEqual(tag, &exact_x->tag)
		|| exact_x->pcm_state != (uint8)PCM_STATE_X
		|| exact_x->flags != PCM_OWN_FLAG_REVOKING
		|| exact_x->generation == 0 || exact_x->generation == UINT64_MAX
		|| reservation_token == 0 || reservation_token == UINT64_MAX
		|| exact_x->reservation_token != reservation_token
		|| exact_x->writer_activation_token != 0
		|| exact_x->resource_x_activation_generation != 0
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX || MyProc == NULL)
		return RESOURCE_X_APPLY_INVALID;
	memset(&admission, 0, sizeof(admission));
	memset(&gate, 0, sizeof(gate));
	memset(&release, 0, sizeof(release));
	memset(&owner, 0, sizeof(owner));
	memset((void *)&cleanup_owner, 0, sizeof(cleanup_owner));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (admission.record_generation != r4_record_generation) {
		cluster_semantic_activation_leave(&admission);
		return RESOURCE_X_APPLY_STALE;
	}

	PG_TRY();
	{
		if (!gcs_block_resource_x_gate_session_snapshot(
				tag, &gate, &master_node, &master_session)
			|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
				master_node, &sender_connection_generation)
			|| !gcs_block_resource_x_target_peer_matches_exact(
				&admission, master_node, sender_connection_generation)
			|| !cluster_semantic_activation_recheck(&admission))
			result = RESOURCE_X_APPLY_STALE;
		else
			result
				= cluster_pcm_lock_resource_x_target_evict_prepare_exact(
					tag, master_node, gate.formation, master_session,
					r4_record_generation, exact_x->generation,
					reservation_token, sender_connection_generation,
					(int32)MyProc->pgprocno, &release, &owner);
		if (result == RESOURCE_X_APPLY_APPLIED
			|| result == RESOURCE_X_APPLY_DUPLICATE) {
			memcpy((void *)&cleanup_owner, &owner, sizeof(owner));
			owner_claimed = true;
		}
		if (owner_claimed
			&& (!cluster_resource_x_wire_encode(
					RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &release,
					payload, sizeof(payload), &payload_bytes, &reject)
				|| payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
				|| release.kind != RESOURCE_X_WIRE_RELEASE_X))
			result = RESOURCE_X_APPLY_INVALID;
		if (owner_claimed && result != RESOURCE_X_APPLY_INVALID
			&& (!gcs_block_resource_x_gate_session_recheck(
					tag, &gate, master_node, master_session)
				|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
					master_node, &sender_connection_recheck)
				|| sender_connection_recheck
					!= sender_connection_generation
				|| !gcs_block_resource_x_target_peer_matches_exact(
					&admission, master_node,
					sender_connection_recheck)
					|| !cluster_semantic_activation_recheck(&admission)))
			result = RESOURCE_X_APPLY_STALE;
		if (result == RESOURCE_X_APPLY_APPLIED
			|| result == RESOURCE_X_APPLY_DUPLICATE) {
			plan_out->release = release;
			plan_out->owner = owner;
			plan_out->gate = gate;
			plan_out->tag = *tag;
			plan_out->cached_ownership_generation = exact_x->generation;
			plan_out->r4_record_generation = r4_record_generation;
			plan_out->sender_connection_generation
				= sender_connection_generation;
			plan_out->master_node = master_node;
			plan_out->payload_bytes = payload_bytes;
			memcpy(plan_out->release_payload, payload, payload_bytes);
			plan_out->prepared = true;
		}
		else if (owner_claimed) {
			abort_result
				= cluster_pcm_lock_resource_x_target_evict_abort_exact(
					&owner);
			if (abort_result != RESOURCE_X_APPLY_APPLIED)
				result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
	}
	PG_CATCH();
	{
		if (owner_claimed) {
			ResourceXLocalOwnerHandle catch_owner;

			memcpy(&catch_owner, (const void *)&cleanup_owner,
				sizeof(catch_owner));
			abort_result
				= cluster_pcm_lock_resource_x_target_evict_abort_exact(
					&catch_owner);
			if (abort_result != RESOURCE_X_APPLY_APPLIED)
				gcs_block_resource_x_fail_closed_current();
		}
		cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	return result;
}

/* Publish exactly the bytes frozen by PREPARE.  A successful local apply or
 * reliable outbound admission is recorded before the close attempt so a
 * failed close can be retried without a second dispatch or rebuilt frame. */
ResourceXApplyResult
cluster_gcs_resource_x_target_evict_publish_exact(
	ResourceXTargetEvictionPlan *plan)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ResourceXMasterSnapshot master_snapshot;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	uint32 sender_connection_recheck = 0;
	uint64 master_session;

	if (plan == NULL || !plan->prepared || !plan->local_n_committed
		|| plan->payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
		|| plan->release.kind != RESOURCE_X_WIRE_RELEASE_X
		|| plan->release.payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
		|| !BufferTagsEqual(
			&plan->tag, &plan->release.common.logical_assertion.resource)
		|| plan->cached_ownership_generation == 0
		|| plan->cached_ownership_generation == UINT64_MAX
		|| plan->r4_record_generation == 0
		|| plan->r4_record_generation == UINT64_MAX
		|| plan->sender_connection_generation == 0
		|| plan->master_node < 0
		|| plan->master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return RESOURCE_X_APPLY_INVALID;
	master_session = plan->release.common.master_session_incarnation;
	memset(&admission, 0, sizeof(admission));
	memset(&master_snapshot, 0, sizeof(master_snapshot));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (admission.record_generation != plan->r4_record_generation) {
		cluster_semantic_activation_leave(&admission);
		return RESOURCE_X_APPLY_STALE;
	}

	PG_TRY();
	{
		if (!gcs_block_resource_x_gate_session_recheck(
				&plan->tag, &plan->gate, plan->master_node,
				master_session)
			|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
				plan->master_node, &sender_connection_recheck)
			|| sender_connection_recheck
				!= plan->sender_connection_generation
			|| !gcs_block_resource_x_target_peer_matches_exact(
				&admission, plan->master_node,
				sender_connection_recheck)
			|| !cluster_semantic_activation_recheck(&admission))
			result = RESOURCE_X_APPLY_STALE;
		else if (!plan->release_admitted) {
			if (plan->master_node == cluster_node_id)
				result = cluster_pcm_lock_resource_x_release_x_exact(
					&plan->release, cluster_node_id, &master_snapshot);
			else
				result = cluster_grd_outbound_enqueue_backend_msg(
					RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
					(uint32)plan->master_node, plan->release_payload,
					plan->payload_bytes)
					? RESOURCE_X_APPLY_APPLIED
					: RESOURCE_X_APPLY_BAD_STATE;
			if (result == RESOURCE_X_APPLY_APPLIED
				|| result == RESOURCE_X_APPLY_DUPLICATE)
				plan->release_admitted = true;
		}
		else
			result = RESOURCE_X_APPLY_DUPLICATE;

		if ((result == RESOURCE_X_APPLY_APPLIED
				 || result == RESOURCE_X_APPLY_DUPLICATE)
			&& (!gcs_block_resource_x_gate_session_recheck(
					&plan->tag, &plan->gate, plan->master_node,
					master_session)
				|| !gcs_block_pcm_x_resource_x_peer_ready_exact(
					plan->master_node, &sender_connection_recheck)
				|| sender_connection_recheck
					!= plan->sender_connection_generation
				|| !gcs_block_resource_x_target_peer_matches_exact(
					&admission, plan->master_node,
					sender_connection_recheck)
				|| !cluster_semantic_activation_recheck(&admission)))
			result = RESOURCE_X_APPLY_STALE;
		if ((result == RESOURCE_X_APPLY_APPLIED
				 || result == RESOURCE_X_APPLY_DUPLICATE)
			&& plan->release_admitted) {
			result = cluster_pcm_lock_resource_x_target_evict_commit_exact(
				&plan->release, plan->master_node,
				plan->r4_record_generation,
				plan->cached_ownership_generation, &plan->owner);
			if (result == RESOURCE_X_APPLY_APPLIED
				|| result == RESOURCE_X_APPLY_DUPLICATE)
				plan->prepared = false;
		}
	}
	PG_CATCH();
	{
		cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	return result;
}

/* PREPARE is reversible only until BufferDesc has committed X->N and before
 * transport/apply owns the frozen release. */
ResourceXApplyResult
cluster_gcs_resource_x_target_evict_abort_exact(
	ResourceXTargetEvictionPlan *plan)
{
	ResourceXApplyResult result;

	if (plan == NULL || !plan->prepared)
		return RESOURCE_X_APPLY_INVALID;
	if (plan->local_n_committed || plan->release_admitted)
		return RESOURCE_X_APPLY_BAD_STATE;
	result = cluster_pcm_lock_resource_x_target_evict_abort_exact(
		&plan->owner);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		plan->prepared = false;
		memset(&plan->owner, 0, sizeof(plan->owner));
	}
	return result;
}

ResourceXApplyResult
cluster_gcs_resource_x_target_acquire_exact(
	BufferDesc *buf, uint64 r4_record_generation,
	ResourceXAcquisitionRef *ref_out)
{
	return gcs_block_resource_x_target_acquire_internal(
		buf, NULL, r4_record_generation, 0, 0, false, NULL, ref_out);
}

ResourceXApplyResult
cluster_gcs_resource_x_target_acquire_until_exact(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 r4_record_generation,
	uint64 *absolute_deadline_us_io, ResourceXAcquisitionRef *ref_out)
{
	if (expected_resource == NULL || absolute_deadline_us_io == NULL)
		return RESOURCE_X_APPLY_INVALID;
	return gcs_block_resource_x_target_acquire_internal(
		buf, expected_resource, r4_record_generation, 0, 0, false,
		absolute_deadline_us_io, ref_out);
}

ResourceXApplyResult
cluster_gcs_resource_x_target_direct_init_acquire_exact(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 r4_record_generation,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	ResourceXAcquisitionRef *ref_out)
{
	if (expected_resource == NULL || direct_init_reservation_token == 0)
		return RESOURCE_X_APPLY_INVALID;
	return gcs_block_resource_x_target_acquire_internal(
		buf, expected_resource, r4_record_generation,
		direct_init_ownership_generation,
		direct_init_reservation_token, false, NULL, ref_out);
}

ResourceXApplyResult
cluster_gcs_resource_x_target_direct_init_join_exact(
	BufferDesc *buf, const BufferTag *expected_resource,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	ResourceXAcquisitionRef *ref_out)
{
	if (expected_resource == NULL || direct_init_reservation_token == 0)
		return RESOURCE_X_APPLY_INVALID;
	return gcs_block_resource_x_target_acquire_internal(
		buf, expected_resource, 0, direct_init_ownership_generation,
		direct_init_reservation_token, true, NULL, ref_out);
}

bool
cluster_gcs_resource_x_target_context_recheck_exact(
	const ResourceXWriterUseContext *context)
{
	ResourceXGateSnapshot gate;
	ResourceXWriterPath writer_path;
	uint64 master_session = 0;
	uint64 writer_generation = 0;
	int32 master_node = -1;

	if (context == NULL
		|| context->r4_record_generation == 0
		|| context->r4_record_generation == UINT64_MAX
		|| context->buffer_ownership_generation == 0
		|| context->buffer_ownership_generation == UINT64_MAX
		|| context->writer_activation_token != 0
		|| context->resource_x_activation_generation != 0)
		return false;
	writer_path = cluster_resource_x_writer_path_snapshot(&writer_generation);
	if (writer_path != RESOURCE_X_WRITER_TARGET
		|| writer_generation != context->r4_record_generation
		|| !gcs_block_resource_x_gate_session_snapshot(
			&context->ref.assertion.resource, &gate, &master_node,
			&master_session)
		|| gate.formation != context->ref.formation)
		return false;
	return cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
		&context->ref, master_session, context->r4_record_generation,
		context->buffer_ownership_generation);
}

static bool
gcs_block_resource_x_target_recycle_inputs_exact(
	const ResourceXWriterUseContext *context,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXGateSnapshot *gate_out, int32 *master_node_out,
	uint64 *master_session_out)
{
	ResourceXWriterPath writer_path;
	uint64 writer_generation = 0;

	if (context == NULL || observed == NULL
		|| !BufferTagsEqual(
			&context->ref.assertion.resource, &observed->tag)
		|| context->buffer_ownership_generation
			!= observed->generation
		|| observed->reservation_token == 0
		|| observed->reservation_token == UINT64_MAX
		|| observed->pcm_state != (uint8)PCM_STATE_X
		|| observed->flags != 0
		|| observed->writer_activation_token != 0
		|| observed->resource_x_activation_generation != 0)
		return false;
	writer_path = cluster_resource_x_writer_path_snapshot(
		&writer_generation);
	return writer_path == RESOURCE_X_WRITER_TARGET
		&& writer_generation == context->r4_record_generation
		&& gcs_block_resource_x_gate_session_snapshot(
			&context->ref.assertion.resource, gate_out,
			master_node_out, master_session_out)
		&& gate_out->formation == context->ref.formation;
}

ResourceXApplyResult
cluster_gcs_resource_x_target_itl_recycle_begin_exact(
	const ResourceXWriterUseContext *context,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXLocalOwnerHandle *handle_out)
{
	ResourceXGateSnapshot gate;
	ResourceXApplyResult cancel_result;
	ResourceXApplyResult result;
	uint64 master_session = 0;
	int32 master_node = -1;

	if (handle_out != NULL)
		memset(handle_out, 0, sizeof(*handle_out));
	memset(&gate, 0, sizeof(gate));
	if (handle_out == NULL || MyProc == NULL
		|| !gcs_block_resource_x_target_recycle_inputs_exact(
			context, observed, &gate, &master_node, &master_session)
		|| !cluster_gcs_resource_x_target_context_recheck_exact(context))
		return RESOURCE_X_APPLY_STALE;
	result = cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
		&context->ref, master_session, context->r4_record_generation,
		context->buffer_ownership_generation,
		observed->reservation_token, (int32)MyProc->pgprocno,
		gcs_block_pcm_x_monotonic_us(), handle_out);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	if (gcs_block_resource_x_gate_session_recheck(
			&context->ref.assertion.resource, &gate,
			master_node, master_session))
		return RESOURCE_X_APPLY_APPLIED;
	cancel_result = cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(
		handle_out);
	memset(handle_out, 0, sizeof(*handle_out));
	return cancel_result == RESOURCE_X_APPLY_APPLIED
		? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}

ResourceXApplyResult
cluster_gcs_resource_x_target_itl_recycle_finish_exact(
	const ResourceXWriterUseContext *context,
	const ClusterPcmOwnSnapshot *observed,
	const ResourceXLocalOwnerHandle *handle)
{
	ResourceXGateSnapshot gate;
	uint64 master_session = 0;
	int32 master_node = -1;

	memset(&gate, 0, sizeof(gate));
	if (!gcs_block_resource_x_target_recycle_inputs_exact(
			context, observed, &gate, &master_node, &master_session)
		|| !cluster_gcs_resource_x_target_context_recheck_exact(context)
		|| handle == NULL
		|| !resource_x_assertion_equal(
			&handle->ref.assertion, &context->ref.assertion)
		|| handle->ref.formation != context->ref.formation
		|| handle->ref.acquisition_generation
			!= context->ref.acquisition_generation
		|| handle->master_session_incarnation != master_session
		|| handle->r4_record_generation
			!= context->r4_record_generation
		|| handle->buffer_ownership_generation
			!= context->buffer_ownership_generation
		|| handle->reservation_token != observed->reservation_token
		|| !gcs_block_resource_x_gate_session_recheck(
			&context->ref.assertion.resource, &gate,
			master_node, master_session))
		return RESOURCE_X_APPLY_STALE;
	return cluster_pcm_lock_resource_x_itl_recycle_finish_exact(
		handle, gcs_block_pcm_x_monotonic_us());
}

ResourceXApplyResult
cluster_gcs_resource_x_target_itl_recycle_cancel_exact(
	const ResourceXLocalOwnerHandle *handle)
{
	return cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(handle);
}

/*
 * cluster_gcs_handle_block_request_envelope — master-side dispatcher.
 *
 *	spec-2.34 D5 wraps the original spec-2.33 §3.2 master flow with a
 *	dedup HTAB lookup_or_register to absorb retransmits without redoing
 *	XLogFlush + copy_block_for_gcs.  Flow:
 *	  1. Wire validation (env / payload size).  Bad envelope → drop.
 *	  2. HC75 transition_id range guard.  Out of range → reply
 *	     VALIDATOR_REJECT (NOT cached — collision is pre-payload).
 *	  3. dedup_lookup_or_register(key, tag, transition_id):
 *	       MISS_REGISTERED      run gcs_block_produce_reply + install +
 *	                            send reply
 *	       IN_FLIGHT_DUPLICATE  silent drop (concurrent retry; original
 *	                            arrival's reply will broadcast)
 *	       CACHED_REPLY         resend cached reply payload (no re-flush)
 *	       VALIDATION_FAIL      HC91 — reply VALIDATOR_REJECT
 *	       FULL                 HC92 — reply DENIED_DEDUP_FULL (transient)
 */
void
cluster_gcs_handle_block_request_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockRequestPayload *req;
	GcsBlockDedupKey key;
	GcsBlockDedupEntry cached_entry;
	GcsBlockDedupResult dr;
	int dedup_worker_id; /* PGRAC: spec-7.3 D5 — this request's dedup shard */
	char block_buf[GCS_BLOCK_DATA_SIZE];
	GcsBlockReplyStatus status;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	const char *block_payload = NULL;
	uint32 block_payload_lkey = 0;
	ClusterICSgeReleaseCallback block_payload_release_cb = NULL;
	void *block_payload_release_arg = NULL;
	ClusterSfDepVec sf_dep_vec;
	bool sf_dep_valid = false;
	bool scache_image_prepared = false;
	bool resource_x_preserve_current_x = false;
	ClusterBufmgrGcsDowngradeOutcome local_downgrade_outcome
		= CLUSTER_BUFMGR_GCS_DOWNGRADE_REFUSED_PRE_NOTIFY;
	ResourceXApplyResult resource_x_successor_result;
	bool queue_pending_x_before = false;
	bool queue_pending_x_after = false;
	bool resource_x_s_barrier_before = false;
	bool resource_x_s_barrier_after = false;
	PcmAuthoritySnapshot s_barrier_authority_before;
	PcmAuthoritySnapshot s_barrier_authority_after;
	bool s_barrier_authority_before_valid = false;
	bool s_barrier_authority_after_valid = false;
	GcsBlockSBarrierReadAction s_barrier_read_action = GCS_BLOCK_S_BARRIER_NONE;
	bool s_barrier_read_image_only = false;
	uint8 request_flags = 0;
	bool master_gate_in_quorum;
	bool master_gate_member;
	bool master_gate_join_active;
	bool master_gate_join_rebuilt;

	cluster_sf_dep_vec_reset(&sf_dep_vec);
	memset(&s_barrier_authority_before, 0, sizeof(s_barrier_authority_before));
	memset(&s_barrier_authority_after, 0, sizeof(s_barrier_authority_after));
	if (cluster_authority_readiness_managed()
		&& !cluster_serving_ready_is_current())
		return;
	if (gcs_block_try_resource_x_frame(env, payload))
		return;
	if (gcs_block_try_r4_request80(env, payload))
		return;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(GcsBlockRequestPayload))
		return;

	req = (const GcsBlockRequestPayload *)payload;

	/*
	 * spec-5.16 D3b (r3 P1 + sr1-②, INV-R8/R14) — master-side hard gate, BEFORE
	 * any dedup / state change.  This is the authoritative fail-closed point: a
	 * remote requester with a stale membership view may route a joiner-home
	 * block request here while this node (the joiner) is not yet a serving
	 * MEMBER, or while its block view is still being rebuilt — serving cold
	 * would double-grant (8.A).  Default-deny until BOTH (a) this node is an
	 * in-quorum MEMBER (closes the CSSD-ALIVE-before-commit window) AND (b) the
	 * requested block's joiner-home view is rebuilt (closes the committed-but-
	 * not-rebuilt window, Hardening v1.1 all-members barrier).  A no-op in
	 * steady state (every node is an in-quorum MEMBER, no fence armed).  Reply
	 * DENIED_RESOURCE_RECOVERING -> sender maps to 53R9L (retry-safe).
	 */
	master_gate_in_quorum = cluster_qvotec_in_quorum();
	master_gate_member = cluster_membership_is_member(cluster_node_id);
	master_gate_join_active = cluster_grd_join_remaster_active_for_shard(req->tag);
	master_gate_join_rebuilt
		= !master_gate_join_active || cluster_grd_block_view_rebuilt(req->tag);
	if (!master_gate_in_quorum || !master_gate_member || !master_gate_join_rebuilt) {
		ereport(LOG,
				(errmsg_internal("GCS block recovering master-gate diagnostic"),
				 errdetail_internal("node=%d sender=%d tag=%u/%u/%u/%u in_quorum=%d "
								"member=%d join_active=%d join_rebuilt=%d",
								cluster_node_id, req->sender_node, req->tag.spcOid,
								req->tag.dbOid,
								(unsigned int)BufTagGetRelNumber(&req->tag),
								(unsigned int)req->tag.blockNum,
								master_gate_in_quorum ? 1 : 0,
								master_gate_member ? 1 : 0,
								master_gate_join_active ? 1 : 0,
								master_gate_join_rebuilt ? 1 : 0)));
		cluster_grd_inc_join_block_failclosed();
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	/*
	 * PGRAC: spec-7.2 D5 — master-side fail-stop episode fence, symmetric
	 * with the requester-side acquire gate (cluster_pcm_lock.c).  While a
	 * dead static master's block resources are mid-episode (survivor
	 * re-declare not yet complete, or merged replay not yet materialized),
	 * serving a request here could grant a block whose surviving holder
	 * has not re-declared yet (phantom-holder overtake window).  The
	 * requester-side gate cannot close this alone: a remote requester with
	 * a stale view routes here directly.  Fail-closed before any dedup /
	 * state change;  DENIED_RESOURCE_RECOVERING -> sender maps to 53R9L
	 * (retry-safe).  phase_for_tag counts the hit itself.
	 */
	{
		ClusterGcsBlockPhase resource_phase = cluster_gcs_block_phase_for_tag(req->tag);

		if (resource_phase == GCS_BLOCK_RECOVERING) {
			int static_master = cluster_gcs_lookup_master_static(req->tag);
			int peer_state = static_master == cluster_node_id
				? (int)CLUSTER_CSSD_PEER_ALIVE
				: (int)cluster_cssd_get_peer_state(static_master);

			ereport(LOG,
					(errmsg_internal("GCS block recovering phase-gate diagnostic"),
					 errdetail_internal("node=%d sender=%d tag=%u/%u/%u/%u "
									"static_master=%d peer_state=%d recovery_in_progress=%d",
									cluster_node_id, req->sender_node, req->tag.spcOid,
									req->tag.dbOid,
									(unsigned int)BufTagGetRelNumber(&req->tag),
									(unsigned int)req->tag.blockNum, static_master,
									peer_state,
									cluster_grd_recovery_in_progress() ? 1 : 0)));
			gcs_block_send_reply(req->sender_node, req,
								 GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING,
								 InvalidXLogRecPtr, NULL);
			return;
		}
	}

	/* HC75 range guard — out of range never enters dedup HTAB. */
	if (req->transition_id < PCM_TRANS_N_TO_S || req->transition_id > PCM_TRANS_S_TO_X_CLEANOUT) {
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	/*
	 * PGRAC: spec-7.3 D5 — per-worker dedup shard routing guard.  A block
	 * request's tag routes to exactly one LMS worker (worker[shard(tag)],
	 * D4), which owns that tag's private dedup shard.  This handler runs in
	 * the worker whose DATA channel received the envelope; verify it is the
	 * routed worker before touching the shard.  A mismatch is a mis-route
	 * (序破坏, 8.A): D3 negotiates a cluster-wide n_workers and D1 shard()
	 * is byte-identical on both ends, so this cannot happen without a code
	 * bug — fail closed (drop; sender retransmits via 53R90) rather than
	 * serve from, or contend on, a shard this worker does not own.
	 */
	dedup_worker_id = cluster_ic_tier1_my_data_channel();
	{
		int tag_shard = cluster_lms_shard_for_tag(&req->tag, cluster_lms_workers);

		Assert(tag_shard == dedup_worker_id);
		if (tag_shard != dedup_worker_id) {
			static bool misroute_logged = false;

			cluster_gcs_block_dedup_note_misroute();
			if (!misroute_logged) {
				misroute_logged = true;
				ereport(LOG,
						(errmsg_internal("gcs block request misrouted to LMS worker %d (tag shard "
										 "%d); dropping (spec-7.3 D5 8.A fail-closed)",
										 dedup_worker_id, tag_shard)));
			}
			return;
		}
	}

	/* PGRAC: spec-2.34 D5 — dedup lookup_or_register (HC90 + HC91 + HC92). */
	memset(&key, 0, sizeof(key));
	key.origin_node_id = (uint32)req->sender_node;
	key.requester_backend_id = req->requester_backend_id;
	key.request_id = req->request_id;
	key.cluster_epoch = req->epoch;
	memset(&cached_entry, 0, sizeof(cached_entry));
	if (req->transition_id == PCM_TRANS_N_TO_S) {
		queue_pending_x_before = gcs_block_queue_pending_x_authoritative(req->tag);
		resource_x_s_barrier_before =
			cluster_gcs_block_resource_x_local_s_barrier_active(req->tag);
		s_barrier_authority_before_valid = cluster_pcm_lock_authority_snapshot(
			req->tag, &s_barrier_authority_before);
	}

	dr = cluster_gcs_block_dedup_lookup_or_register(
		dedup_worker_id, &key, req->tag, req->transition_id,
		GcsBlockRequestPayloadGetLifetimeHintMs(req),
		cluster_sf_peer_supports_gcs_done(req->sender_node), &cached_entry);
	if (dr != GCS_BLOCK_DEDUP_VALIDATION_FAIL && dr != GCS_BLOCK_DEDUP_FULL) {
		if (GcsBlockRequestPayloadIsDirectLandArmed(req))
			request_flags |= GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND;
		if (!cluster_gcs_block_dedup_set_request_flags_exact(dedup_worker_id, &key, &req->tag,
															 req->transition_id, request_flags)) {
			/* The tuple was removed/replaced or its immutable request properties
			 * changed between lookup and pinning.  Neither case may inherit the
			 * earlier entry's grant rights. */
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
								 InvalidXLogRecPtr, NULL);
			return;
		}
	}
	if (req->transition_id == PCM_TRANS_N_TO_S) {
		queue_pending_x_after = gcs_block_queue_pending_x_authoritative(req->tag);
		resource_x_s_barrier_after =
			cluster_gcs_block_resource_x_local_s_barrier_active(req->tag);
		s_barrier_authority_after_valid = cluster_pcm_lock_authority_snapshot(
			req->tag, &s_barrier_authority_after);
		s_barrier_read_action = gcs_block_s_barrier_read_action_exact(
			queue_pending_x_before, queue_pending_x_after,
			resource_x_s_barrier_before, resource_x_s_barrier_after,
			&s_barrier_authority_before, s_barrier_authority_before_valid,
			&s_barrier_authority_after, s_barrier_authority_after_valid,
			req->sender_node);
		/* A cached durable S grant cannot be reinterpreted as the narrow
		 * image-only exception after a native head appears. */
		if (s_barrier_read_action == GCS_BLOCK_S_BARRIER_IMAGE_ONLY
			&& dr == GCS_BLOCK_DEDUP_CACHED_REPLY
			&& cached_entry.status != (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER)
			s_barrier_read_action = GCS_BLOCK_S_BARRIER_DENY;
		s_barrier_read_image_only =
			s_barrier_read_action == GCS_BLOCK_S_BARRIER_IMAGE_ONLY;
	}

	/* Check both sides of registration.  A legacy queue claim or exact
	 * Resource-X head/active barrier that linearized before lookup routes this
	 * request directly to a cached exact denial; either arbitration domain
	 * catches the opposite race with its drive-side dedup scan.  Queue
	 * ownership has no same-node exemption. */
	if (req->transition_id == PCM_TRANS_N_TO_S
		&& s_barrier_read_action == GCS_BLOCK_S_BARRIER_DENY) {
		GcsBlockPendingXDenyResult deny_result;

		if (dr == GCS_BLOCK_DEDUP_VALIDATION_FAIL) {
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
								 InvalidXLogRecPtr, NULL);
			return;
		}
		if (dr == GCS_BLOCK_DEDUP_FULL) {
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_DEDUP_FULL,
								 InvalidXLogRecPtr, NULL);
			return;
		}
		deny_result = cluster_gcs_block_dedup_pending_x_deny_exact(
			dedup_worker_id, &key, &req->tag, req->transition_id, &cached_entry);
		if (deny_result != GCS_BLOCK_PENDING_X_DENY_NEW
			&& deny_result != GCS_BLOCK_PENDING_X_DENY_REPLAY) {
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
								 InvalidXLogRecPtr, NULL);
			return;
		}
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
		(void)gcs_block_resend_cached_reply(req->sender_node, &cached_entry);
		return;
	}
	switch (dr) {
	case GCS_BLOCK_DEDUP_CACHED_REPLY:
		gcs_block_resend_cached_reply(req->sender_node, &cached_entry);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
		return;

	case GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE:
	case GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT:
		/* Original arrival is mid-processing;  it will broadcast the
		 * reply.  Drop this duplicate silently. */
		return;

	case GCS_BLOCK_DEDUP_VALIDATION_FAIL:
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
							 InvalidXLogRecPtr, NULL);
		return;

	case GCS_BLOCK_DEDUP_FULL:
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_DEDUP_FULL,
							 InvalidXLogRecPtr, NULL);
		return;

	case GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE:
		/* PGRAC: spec-2.35 HC113 — master already forwarded this request
		 * to a holder; sender is retrying (network drop / holder evict
		 * race).  The marker caches routing, not authority: re-forward to
		 * the same holder only if one current coherent PCM snapshot still
		 * proves the exact route.  A writer barrier or holder handoff makes
		 * the marker stale; remove it and return one retryable denial so the
		 * next arrival re-enters the current master decision tree.  Holder
		 * side is idempotent (copy_block_for_gcs is read-only) only after
		 * this authority revalidation. */
		{
			GcsBlockForwardPayload fwd;
			int32 holder_node = cached_entry.reply_header.sender_node;
			PcmAuthoritySnapshot authority;

			if (holder_node < 0 || holder_node == cluster_node_id)
				return; /* malformed dedup entry; silent drop */
			if (!cluster_pcm_lock_authority_snapshot(req->tag, &authority)
				|| !gcs_block_forward_replay_authority_exact(
					(GcsBlockReplyStatus)cached_entry.status, req->transition_id,
					holder_node, req->sender_node, &authority)
				|| (s_barrier_read_image_only
					&& memcmp(&authority, &s_barrier_authority_after,
							  sizeof(authority)) != 0)) {
				cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
				gcs_block_send_reply(req->sender_node, req,
								 GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
								 InvalidXLogRecPtr, NULL);
				return;
			}
			if (gcs_block_deny_direct_armed_forward_request(req))
				return;

			memset(&fwd, 0, sizeof(fwd));
			fwd.request_id = req->request_id;
			fwd.epoch = cluster_epoch_get_current();
			fwd.tag = req->tag;
			fwd.original_requester_node = req->sender_node;
			fwd.requester_backend_id = req->requester_backend_id;
			fwd.master_node = cluster_node_id;
			fwd.transition_id = req->transition_id;
			GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, req, false);
			/* PGRAC: spec-2.37 D3 HC127 (spec-2.41 SCN migration) — stamp
			 * expected pi_watermark_scn so the holder can validate the copied
			 * page's pd_block_scn before ship. */
			GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
				&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));
			/* A READ_IMAGE forward marker must replay as a READ-IMAGE
			 * forward: without the flag the holder treats the replay as a
			 * holder-transfer and gives up its X.  (The marker itself must
			 * never be CACHED-resent — its header checksum was never
			 * computed and the entry carries no page, and the 31-hash of an
			 * all-zero page is ALSO 0, so a resent marker VERIFIES and
			 * installs a zero page: a PageIsNew false-empty read, 8.A.) */
			if (cached_entry.status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER) {
				GcsBlockForwardPayloadSetReadImage(&fwd, true);
			}

			{
				ClusterICSendResult fwd_rc;

				/* A reply-13 marker is image-only.  In particular, replay never
				 * upgrades an earlier one-shot route into a durable X->S request. */
				if (s_barrier_read_image_only
					&& !cluster_pcm_lock_authority_matches(
						req->tag, &s_barrier_authority_after)) {
					cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
					gcs_block_send_reply(req->sender_node, req,
						GCS_BLOCK_REPLY_DENIED_PENDING_X,
						InvalidXLogRecPtr, NULL);
					return;
				}
				fwd_rc = cluster_ic_send_envelope(
					PGRAC_IC_MSG_GCS_BLOCK_FORWARD, holder_node, &fwd, sizeof(fwd));

				cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_FORWARD, fwd_rc);
				if (fwd_rc == CLUSTER_IC_SEND_DONE || fwd_rc == CLUSTER_IC_SEND_WOULD_BLOCK) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->forward_replay_count, 1);
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);
				}
			}
		}
		return;

	case GCS_BLOCK_DEDUP_MISS_REGISTERED:
		/* fall through to forward-or-direct decision + install + send. */
		break;
	}

	/*
	 * PGRAC: spec-2.36 D6 (HC117) — S barrier reader starvation guard.
	 *
	 *	When an X writer's request is in flight at this master (pending_x_
	 *	requester_node is set on the GrdEntry), short-circuit any concurrent
	 *	N→S request with DENIED_PENDING_X.  The reader backs off (D6
	 *	exponential backoff) and retries;  after the X writer's transition
	 *	install ack arrives at the master, pending_x is cleared and the
	 *	reader's next retry succeeds.
	 *
	 *	Why before HC101 spec-2.35 forward decision:  the S-barrier deny is
	 *	cheaper than computing forward candidacy, and the deny must apply
	 *	regardless of master_holder state (HC117 protects the X writer's
	 *	priority, not just direct-grant paths).
	 *
	 *	Exception:  if pending_x_requester == req->sender_node, the reader
	 *	is the X requester itself (different backend on same node) — grant
	 *	normally (no starvation against self).
	 */
	if (req->transition_id == PCM_TRANS_N_TO_S) {
		int32 pending_x;

		/* spec-2.36 D16 inject — force DENIED_PENDING_X for TAP coverage of
		 * exact abort + fresh-identity reader backoff. */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-starvation-force-denied");
		if (cluster_injection_should_skip("cluster-gcs-block-starvation-force-denied")) {
			GcsBlockPendingXDenyResult deny_result;

			pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
			deny_result = cluster_gcs_block_dedup_pending_x_deny_exact(
				dedup_worker_id, &key, &req->tag, req->transition_id, &cached_entry);
			if (deny_result == GCS_BLOCK_PENDING_X_DENY_NEW
				|| deny_result == GCS_BLOCK_PENDING_X_DENY_REPLAY)
				(void)gcs_block_resend_cached_reply(req->sender_node, &cached_entry);
			else
				gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
									 InvalidXLogRecPtr, NULL);
			return;
		}

		pending_x = cluster_pcm_lock_query_pending_x_requester(req->tag);
		if (pending_x >= 0 && pending_x != req->sender_node) {
			GcsBlockPendingXDenyResult deny_result;

			pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
			deny_result = cluster_gcs_block_dedup_pending_x_deny_exact(
				dedup_worker_id, &key, &req->tag, req->transition_id, &cached_entry);
			if (deny_result == GCS_BLOCK_PENDING_X_DENY_NEW
				|| deny_result == GCS_BLOCK_PENDING_X_DENY_REPLAY)
				(void)gcs_block_resend_cached_reply(req->sender_node, &cached_entry);
			else
				gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
									 InvalidXLogRecPtr, NULL);
			return;
		}
	}

	/*
	 * PGRAC: spec-2.36 D2 (HC115/HC116/HC118) — master decision tree
	 * X-state path for N→X / S→X requests.
	 *
	 *	Sets HC117 pending_x_requester immediately (so concurrent N→S
	 *	readers see the barrier even before broadcast completes).  Then
	 *	dispatches by pre_state:
	 *	  N → master direct grant + ship block (reuse spec-2.33 path).
	 *	  S → enumerate s_holders_bitmap (exclude self for S→X upgrade
	 *		  per Q5=A merged path);  if non-empty, broadcast INVALIDATE
	 *		  + wait all acks (HC116);  on success, fall through to
	 *		  direct grant + ship.  On budget exhaustion, reply
	 *		  DENIED_INVALIDATE_TIMEOUT (sender → 53R91).
	 *	  X → forward to current x_holder peer (HC115);  holder copies +
	 *		  XLogFlush + direct-ships X_GRANTED_FROM_HOLDER to requester;
	 *		  requester post-install ACK to master triggers x_holder
	 *		  switch (mirrors spec-2.35 HC111 "real cache residency").
	 *
	 *	On any failure path the pending_x is cleared before returning so a
	 *	subsequent N→S retry does not see a stale barrier.
	 */
	if (req->transition_id == PCM_TRANS_N_TO_X || req->transition_id == PCM_TRANS_S_TO_X_UPGRADE) {
		PcmPendingXReserveResult reserve_result;
		PcmLockMode pre_state;
		uint64 current_lsn;
		int32 x_holder;

		/*
		 * PGRAC: spec-5.2 D11 path B — master==holder==self self-ship X to a
		 * REMOTE requester (writer-transfer-revoke).  THIS node is both the GCS
		 * master and the X holder; the requester wants X.  We ship our current
		 * image and revoke our own X, then record the requester as the new X
		 * holder.  Must run BEFORE the HG7 other-live-holder fail-closed below,
		 * which counts self as an "other holder" and would DENY.  Single-phase:
		 * reply GRANTED with the image; the requester installs + takes X off the
		 * GRANTED reply with no post-install ACK (we switch ownership here).
		 *
		 * 8.A: copy image -> drop self copy NO-WIRE (XLogFlush + InvalidateBuffer;
		 * we run in the §3.5 / LMON IC context with no backend slot, and as the
		 * master there is no peer to notify) -> record requester as X holder.
		 * Dropping self before recording the requester means there is never a
		 * two-X window; the PI watermark advances to the shipped page_lsn.
		 * Respects the spec-2.36 x-forward injection skip (test fallback).
		 */
		if (cluster_pcm_lock_query(req->tag) == PCM_LOCK_MODE_X
			&& cluster_pcm_master_holder_node_by_tag(req->tag) == cluster_node_id
			&& req->sender_node != cluster_node_id
			&& !cluster_injection_should_skip("cluster-gcs-block-x-forward-master-side")) {
			uint64 pathb_epoch = cluster_epoch_get_current();

			if (req->epoch < pathb_epoch) {
				gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
									 InvalidXLogRecPtr, NULL);
				return;
			}
			cluster_sf_dep_vec_reset(&sf_dep_vec);
			sf_dep_valid = false;
			if (gcs_block_get_ship_image(req->tag, req->sender_node, false, &page_lsn, block_buf,
										 &block_payload, &block_payload_lkey,
										 &block_payload_release_cb, &block_payload_release_arg,
										 &sf_dep_vec, &sf_dep_valid, NULL)) {
				/* Active writer bytes may be observed, but the holder keeps X. */
				if (cluster_itl_page_has_active_slot((Page)block_payload)) {
					status = GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
					if (ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->cf_xheld_read_ship_count, 1);
					goto build_and_send_reply;
				}

				{
					XLogRecPtr drop_lsn = InvalidXLogRecPtr;

					/*
					 * PGRAC: spec-5.2a D4 — a clean (sequence) eligible page has
					 * no active ITL, so the master==holder self-ship is the same
					 * as the existing path-B drop below: ship the image, grant X,
					 * drop our copy no-wire.  No data flush in LMON — the backend
					 * eager-flushed the page at write time (storage is current for
					 * the stale-holder storage-fallback); drop_no_wire's XLogFlush
					 * satisfies WAL-before-share.  Count a clean transfer. */
					if (GcsBlockRequestPayloadIsCleanEligible(req) && ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_count, 1);

					/* Materialize an SGE-backed image before invalidating its buffer. */
					if (block_payload_release_cb != NULL) {
						memcpy(block_buf, block_payload, GCS_BLOCK_DATA_SIZE);
						gcs_block_release_ship_image(block_payload_release_cb,
													 block_payload_release_arg);
						block_payload = block_buf;
						block_payload_lkey = 0;
						block_payload_release_cb = NULL;
						block_payload_release_arg = NULL;
					}

					/*
					 * The image is already captured (copy_block_for_gcs succeeded
					 * above) BEFORE this drop.  NOT_RESIDENT is fine: no local
					 * copy left to stale-flush — exactly the safe precondition
					 * for granting.
					 *
					 * GCS serve-stall round-5 (A2): PINNED no longer parks this
					 * dispatch worker in InvalidateBuffer's pin-wait loop (the
					 * old "LMON pin-wait follow-up").  Granting with a live
					 * pinned copy would leave a stale local X resident (8.A),
					 * so fail-closed with the retryable PENDING_X deny — the
					 * requester's starvation backoff re-asks and the pin (its
					 * holder is typically a backend waiting on a reply this
					 * very worker delivers once unblocked) clears meanwhile.
					 * Same dedup-entry release as every retryable deny: the
					 * deny contract is "back off and retry, the retry
					 * re-evaluates".
					 */
					/* GCS serve-stall round-6 RED harness: hold the copy->drop
					 * window open (see cluster_inject.c registry note). */
					CLUSTER_INJECTION_POINT("cluster-gcs-xfer-copy-drop-window");
					{
						/* GCS serve-stall round-6: pass the copy-time page_lsn
						 * (captured by get_ship_image above) as the generation
						 * token.  PINNED (a live foreign pin) and STALE (a local
						 * writer committed since the copy) both mean the shipped
						 * image must NOT be granted — fail-closed with the same
						 * retryable deny so the requester re-asks and the re-serve
						 * copies the current page (Rule 8.A). */
						ClusterBufmgrGcsDropResult dres = cluster_bufmgr_drop_block_for_gcs_no_wire(
							req->tag, page_lsn, &drop_lsn);

						if (dres == CLUSTER_BUFMGR_GCS_DROP_PINNED
							|| dres == CLUSTER_BUFMGR_GCS_DROP_STALE) {
							if (dres == CLUSTER_BUFMGR_GCS_DROP_STALE)
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->xfer_stale_deny_count, 1);
							else
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->drop_pinned_deny_count,
														1);
							cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
							gcs_block_send_reply(req->sender_node, req,
												 GCS_BLOCK_REPLY_DENIED_PENDING_X,
												 InvalidXLogRecPtr, NULL);
							return;
						}
					}
					/* PGRAC: spec-6.12h D-h3a — ordering pin: the PI
					 * conversion (inside the drop above) samples its
					 * ship-SCN stamp BEFORE the grant reply leaves
					 * (build_and_send_reply below), so the requester's
					 * envelope observe — and every post-ship record it
					 * stamps — is strictly above the recovery boundary
					 * (cluster_pi_shadow.h proof item 2). */
					/* spec-2.41 D2 — advance detector SCN watermark from the
					 * shipped page's pd_block_scn (local-page source = block_buf). */
					cluster_pcm_lock_master_grant_x_to(
						req->tag, req->sender_node, page_lsn,
						(SCN)((PageHeader)block_payload)->pd_block_scn, req->request_id,
						req->epoch);
					/* PGRAC: spec-6.12h D-h2 — if the D-h1 conversion kept our
					 * outgoing copy as a Past Image, record ourselves on the
					 * authoritative PI bitmap (master == self: local note). */
					if (cluster_bufmgr_block_is_pi(req->tag))
						cluster_pcm_lock_pi_holder_note(req->tag, cluster_node_id);
					status = GCS_BLOCK_REPLY_GRANTED;
					if (ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_self_ship_count, 1);
					goto build_and_send_reply;
				}
			}
			/* evict race — fall through to the normal HG7 / master flow. */
		}

		/*
		 * PGRAC: spec-5.2a D3 (branch ⑤) — eligible clean-page third-party
		 * master fail-closed.  Inserted BEFORE the HG7 conservative DENY so an
		 * eligible (sequence) request is not lumped into the generic
		 * writer-transfer fail-closed.  In the 2-node target master ∈ {requester,
		 * holder} always (path-B above handles master == holder; the local-master
		 * acquire path handles master == requester), so this only fires with ≥3
		 * nodes where a third live node holds X.  That case needs a two-phase
		 * post-install ACK to avoid a stale window and is out of scope — fail
		 * closed with a clean terminal DENIED so the requester maps it to 53R9X
		 * (ERRCODE_CLUSTER_CLEAN_PAGE_XFER_UNAVAILABLE).  IDEMPOTENT / no-holder /
		 * self-ship decisions fall through to the existing (already correct)
		 * flow below.
		 */
		if (GcsBlockRequestPayloadIsCleanEligible(req)
			&& gcs_block_clean_xfer_master_decision(cluster_pcm_master_holder_node_by_tag(req->tag),
													req->sender_node, cluster_node_id)
				   == GCS_CLEAN_XFER_THIRD_PARTY_DENY) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_third_party_denied_count, 1);
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count, 1);
			GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "clean-third-party-master");
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
								 InvalidXLogRecPtr, NULL);
			return;
		}

		/*
		 * PGRAC: spec-4.7a D4 / HG7 — bounded fail-closed for cross-node X
		 * contention.  Granting X to this requester would require invalidating
		 * or transferring the block away from ANOTHER LIVE node that holds it
		 * (in X, or in S).  That is the writer-transfer path, deferred to
		 * spec-2.36 completion / 4.7 / Stage 6 and NOT implemented here.  Fail
		 * closed RIGHT NOW — before set_pending_x, before any invalidate
		 * broadcast or forward — so:
		 *   (1) no master state is mutated on the failure path;
		 *   (2) the requester gets a bounded terminal DENIED (FEATURE_NOT_
		 *       SUPPORTED), never a GRANTED_* and never a long invalidate-budget
		 *       wait / hang;
		 *   (3) pending_x is not set, so there is nothing to leak/clear;
		 *   (4) the existing holder is untouched and stays usable;
		 *   (5) a second X holder can never be granted (HG5 no double-X).
		 * A dead holder is NOT handled here — that is the dead-master / warm-
		 * recovery path (53R9K / spec-4.7); leave it to the flow below.
		 *
		 * PGRAC: spec-6.12a ㉕ — the gate is narrowed to the live X-HOLDER
		 * case.  Live S holders now have a REAL invalidate path: the
		 * pre_state==S branch below broadcasts GCS_BLOCK_INVALIDATE, collects
		 * every ack, clears the invalidated bits, then grants — the same
		 * invalidate-before-X contract the LOCAL-master upgrade
		 * (cluster_gcs_block_local_x_upgrade) already enforces.  Before ㉕ the
		 * S-holder combination was unreachable here (a remote-mastered block a
		 * node had written stayed X at that node), so the blanket deny was
		 * safe; after ㉕ the quiescent downgrade deliberately creates
		 * "requester holds S, other nodes hold S" and the blanket deny would
		 * make every downgraded block permanently unwritable by its former
		 * holder.
		 *
		 * PGRAC: GCS-race round-4 FUNC-1 — the live X-HOLDER deny below is
		 * no longer terminal in the default configuration: with
		 * cluster.ges_bast on, the nudge (sent just below) asks the holder
		 * for the quiescent X->S yield, the dedup entry is dropped so the
		 * requester's bounded backoff-retry is re-evaluated, and the retry
		 * converges through the shipped S-invalidate + storage-fallback
		 * grant path (the holder's yield flushed the page storage-current).
		 * No double-X window exists at any step: X is only granted after
		 * every S copy is invalidated, and the holder's yield itself blocks
		 * further local writes.  The direct wire-ship 3-way transfer
		 * (retain-X-until-post-install-ACK) remains wave-g territory.
		 */
		if (cluster_pcm_lock_query(req->tag) == PCM_LOCK_MODE_X
			&& cluster_pcm_master_other_live_holder_exists(req->tag, req->sender_node)) {
			/*
			 * PGRAC: spec-6.12e2 (㉔) — BAST nudge.  We are about to DENY
			 * because a live X holder blocks this requester; with the wave
			 * GUC on, additionally nudge that holder (fire-and-forget
			 * FORWARD, request_id 0, no reply of any kind) so its LMON
			 * tries the quiescent X->S self-downgrade NOW instead of
			 * waiting for a natural release — the requester's bounded
			 * retry then proceeds through the S-invalidate grant path.
			 * The deny below is unchanged in every case (the nudge is
			 * advisory; refusal keeps today's e1 release-side fallback).
			 */
			if (cluster_ges_bast) {
				int nudge_holder = cluster_pcm_master_holder_node_by_tag(req->tag);

				if (nudge_holder >= 0 && nudge_holder != cluster_node_id
					&& nudge_holder != req->sender_node) {
					GcsBlockForwardPayload nudge;

					memset(&nudge, 0, sizeof(nudge));
					nudge.request_id = 0; /* HC74 shape: nobody waits */
					nudge.epoch = cluster_epoch_get_current();
					nudge.tag = req->tag;
					nudge.original_requester_node = req->sender_node;
					nudge.requester_backend_id = req->requester_backend_id;
					nudge.master_node = cluster_node_id;
					nudge.transition_id = req->transition_id;
					GcsBlockForwardPayloadSetBastNudge(&nudge);
					{
						ClusterICSendResult nudge_rc = cluster_ic_send_envelope(
							PGRAC_IC_MSG_GCS_BLOCK_FORWARD, nudge_holder, &nudge, sizeof(nudge));

						cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_FORWARD,
															nudge_rc);
						if (nudge_rc == CLUSTER_IC_SEND_DONE
							|| nudge_rc == CLUSTER_IC_SEND_WOULD_BLOCK)
							cluster_lever_e2_note_nudge_sent();
					}
				}
			}
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
			/*
			 * GCS-race round-4 FUNC-1: with the nudge armed this deny is a
			 * RETRYABLE step of the live-X handoff, not a terminal wall --
			 * the requester backs off and re-asks while the holder yields
			 * X->S, and the retry must be re-evaluated against the
			 * post-yield state.  Drop the in-flight dedup entry (same
			 * (request_id, epoch) key) or the retry is swallowed as
			 * IN_FLIGHT_DUPLICATE until the TTL sweep (PENDING_X deny
			 * precedent above).
			 */
			cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
			GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "live-x-other-holder");
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
								 InvalidXLogRecPtr, NULL);
			return;
		}

		current_lsn = (uint64)GetXLogInsertRecPtr();
		reserve_result = cluster_pcm_lock_set_pending_x(req->tag, req->sender_node, current_lsn);
		if (reserve_result != PCM_PENDING_X_RESERVE_OK) {
			/* Another exact round owns the starvation barrier.  The dedup key
			 * for this request must be released so its bounded retry can be
			 * reconsidered after that owner completes. */
			if (reserve_result == PCM_PENDING_X_RESERVE_OCCUPIED) {
				cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
				gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_PENDING_X,
									 InvalidXLogRecPtr, NULL);
			} else {
				GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "pending-x-reserve-failed");
				gcs_block_send_reply(req->sender_node, req,
									 GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, InvalidXLogRecPtr,
									 NULL);
			}
			return;
		}

		pre_state = cluster_pcm_lock_query(req->tag);

		/* spec-2.36 D16 — fault injection: force the X-state decision to
		 * fall through to the original spec-2.33 master flow. */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-x-forward-master-side");
		if (cluster_injection_should_skip("cluster-gcs-block-x-forward-master-side")) {
			/* Fall through; pending_x stays set, the original master path
			 * will clear it on grant or DENIED_MASTER_NOT_HOLDER fallback. */
			goto x_path_skipped;
		}

		if (pre_state == PCM_LOCK_MODE_X) {
			x_holder = cluster_pcm_master_holder_node_by_tag(req->tag);
			/*
			 * PGRAC: spec-4.7a D3 — the requester already IS the x_holder
			 * (it released its content_lock locally but the master still
			 * records it).  Idempotent re-grant: do NOT self-forward (would
			 * loop back to the sender) and do NOT change master state.  Clear
			 * the pending_x we set above so a later N→S is not falsely
			 * barriered (HG3).  Covers an N→X/S→X re-request from the node
			 * that already holds X. */
			if (x_holder == req->sender_node) {
				(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
				status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
				page_lsn = InvalidXLogRecPtr;
				block_payload = NULL;
				goto build_and_send_reply;
			}
			if (x_holder >= 0 && x_holder != cluster_node_id) {
				GcsBlockForwardPayload fwd;
				GcsBlockReplyHeader fwd_hdr;
				uint64 current_epoch = cluster_epoch_get_current();

				if (req->epoch < current_epoch) {
					(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
					gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
										 InvalidXLogRecPtr, NULL);
					return;
				}
				if (GcsBlockRequestPayloadIsDirectLandArmed(req)) {
					(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
					/* The direct-land deny asks the requester to retry with
					 * direct-land suppressed — same (request_id, epoch) key,
					 * so drop the in-flight dedup entry or the retry is
					 * swallowed as IN_FLIGHT_DUPLICATE (see the PENDING_X
					 * sites; S3 RC-B). */
					cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
					(void)gcs_block_deny_direct_armed_forward_request(req);
					return;
				}

				memset(&fwd, 0, sizeof(fwd));
				fwd.request_id = req->request_id;
				fwd.epoch = current_epoch;
				fwd.tag = req->tag;
				fwd.original_requester_node = req->sender_node;
				fwd.requester_backend_id = req->requester_backend_id;
				fwd.master_node = cluster_node_id;
				fwd.transition_id = req->transition_id;
				GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, req, false);
				/* PGRAC: spec-2.37 D3 HC127 (spec-2.41 SCN migration) — stamp
				 * expected pi_watermark_scn. */
				GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
					&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));

				if (gcs_block_forward_send_admitted(x_holder, &fwd)) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_forward_sent_count, 1);
					/* HC111 / HC118:  do NOT switch master_holder.node_id /
					 * x_holder_node here.  The current X holder retains its
					 * claim until requester post-install ACK arrives at this
						 * master via cluster_gcs_send_transition_and_wait
						 * (same callback path that spec-2.35 N→S uses).  This
						 * avoids the two-X-holder transient window (codereview F2). */
					memset(&fwd_hdr, 0, sizeof(fwd_hdr));
					fwd_hdr.request_id = req->request_id;
					fwd_hdr.requester_backend_id = req->requester_backend_id;
					fwd_hdr.transition_id = req->transition_id;
					fwd_hdr.sender_node = x_holder;
					fwd_hdr.status = (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER;
					GcsBlockReplyHeaderSetForwardingMasterNode(&fwd_hdr, cluster_node_id);
					cluster_gcs_block_dedup_install_reply(dedup_worker_id, &key,
														  GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER,
														  &fwd_hdr, NULL);
					return;
				}
				GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "x-forward-send-failed");
				(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
				status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
				page_lsn = InvalidXLogRecPtr;
				block_payload = NULL;
				goto build_and_send_reply;
			}
			GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "x-state-holder-unroutable");
			(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
			status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
			page_lsn = InvalidXLogRecPtr;
			block_payload = NULL;
			goto build_and_send_reply;
		} else if (pre_state == PCM_LOCK_MODE_S) {
			uint32 holders_bm = cluster_pcm_lock_query_s_holders_bitmap(req->tag);
			bool requester_is_s_holder = (req->sender_node >= 0 && req->sender_node < 32
										  && (holders_bm & ((uint32)1u << req->sender_node)) != 0);
			bool xvs_b2_captured = false; /* PGRAC: spec-6.14a D3 (B2) */

			/* Q5=A merged path + spec-4.7a D3:  exclude the requester's own S
			 * bit from the invalidate set.  It is upgrading its OWN access to X
			 * and must not invalidate itself (self-invalidate self-loops to
			 * DENIED_INVALIDATE_TIMEOUT — the D0 bug).  Applies whether the
			 * request is labeled S→X_UPGRADE or N→X:  the bufmgr acquire path
			 * emits N→X even when the node already holds S (state-agnostic), so
			 * the master keys off its own authoritative s_holders record, not
			 * the requester's transition label. */
			if (requester_is_s_holder)
				holders_bm &= ~((uint32)1u << req->sender_node);

			/*
			 * PGRAC: spec-6.14a D3 — requester is NOT an S holder (plain
			 * N→X against read-shared copies).  Classify BEFORE any copy is
			 * dropped:
			 *   B2: the master itself holds S — capture the ship image
			 *       FIRST (image-survival: after the revoke round the only
			 *       guaranteed current carriers are deliberately preserved
			 *       copies; a revoked dirty-S copy is dropped after only an
			 *       XLogFlush, so shared storage may be stale post-revoke),
			 *       then let the self-drop + broadcast below run and grant
			 *       WITH the image — never STORAGE_FALLBACK.
			 *   B3: only third-party nodes hold S (master not resident;
			 *       unreachable on 2 nodes) — fail closed, counted: no
			 *       capturable current carrier would survive the revoke.
			 */
			if (!requester_is_s_holder) {
				/*
				 * allow_live_sge = false: the B2 capture must be an
				 * INDEPENDENT copy, because the self-drop just below
				 * invalidates this very buffer.  A live-SGE borrow raw-pins
				 * the shared buffer itself: no copy would survive the drop
				 * (s3.1 image-survival), and InvalidateBuffer would spin on
				 * the foreign pin.  The read-image serve paths keep
				 * allow_live_sge = true -- they never drop the pinned block
				 * before the reply goes out.
				 */
				if ((holders_bm & ((uint32)1u << cluster_node_id)) != 0
					&& gcs_block_get_ship_image(
						req->tag, req->sender_node, false, &page_lsn, block_buf, &block_payload,
						&block_payload_lkey, &block_payload_release_cb, &block_payload_release_arg,
						&sf_dep_vec, &sf_dep_valid, NULL))
					xvs_b2_captured = true;

				if (!xvs_b2_captured) {
					/*
					 * PGRAC: spec-6.12e2 × spec-6.14a merge — a THIRD-PARTY-ONLY
					 * S set with no master carrier must NOT terminal-deny here:
					 * the S bits are only ever cleared by the self-drop +
					 * nowait-invalidate blocks below, which a terminal deny
					 * never reaches, so the e2 3-corner nudge flow would
					 * live-lock on an eternal B3 (t/348 L3).  Count the
					 * no-carrier round and FALL THROUGH: the e2 blocks below
					 * fire the invalidates and reply DENIED_PENDING_X, and the
					 * requester's retry finds the S set cleared and takes the
					 * original spec-2.33 flow (whose storage fallback stays
					 * under the spec-2.41 lost-write detector — no un-carried
					 * GRANT is ever produced on this path, deny direction
					 * preserved, only liveness added).
					 */
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->x_vs_s_no_carrier_denied_count, 1);
				}
			}

			/*
			 * PGRAC: spec-6.12a ㉕ — the master itself may hold an S copy (it
			 * registered as a reader after a remote-holder downgrade).  The
			 * wire INVALIDATE cannot self-deliver; perform the holder-side
			 * actions synchronously — drop the local copy and clear our own
			 * bit on the authoritative entry — then broadcast only to the
			 * remaining REMOTE holders.  (Idempotent when the copy is not
			 * resident: the transition apply early-returns on a cleared bit.)
			 */
			if ((holders_bm & ((uint32)1u << cluster_node_id)) != 0) {
				XLogRecPtr self_lsn = InvalidXLogRecPtr;
				SCN self_scn = InvalidScn;

				/*
				 * GCS serve-stall round-5 (A2): the self-drop is bounded now.
				 * PINNED keeps our S bit SET (clearing it with the copy still
				 * resident would let this node's readers keep serving a page
				 * the grant machinery believes is gone — 8.A) and this deny
				 * round replies PENDING_X below as before;  the requester's
				 * retry re-attempts the self-drop once the pin clears.
				 */
				if (cluster_bufmgr_invalidate_block_for_gcs(req->tag, PCM_LOCK_MODE_S, &self_lsn,
															&self_scn)
					== CLUSTER_BUFMGR_GCS_DROP_PINNED) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->drop_pinned_deny_count, 1);
				} else {
					(void)cluster_pcm_lock_apply_gcs_transition(
						req->tag, PCM_TRANS_S_TO_N_INVALIDATE, cluster_node_id);
					/* PGRAC: spec-6.12h D-h2 — the self-drop may have kept a Past
					 * Image (D-h1 conversion inside the helper); master == self,
					 * so record it on the authoritative PI bitmap directly (the
					 * wire kept_pi ACK flag cannot self-deliver either). */
					if (cluster_bufmgr_block_is_pi(req->tag))
						cluster_pcm_lock_pi_holder_note(req->tag, cluster_node_id);
					/* PGRAC: spec-6.12h D-h3a — ordering pin: this
					 * self-conversion runs before the X grant is issued below,
					 * and the grant envelope leaves this same node stamped
					 * scn_current >= the ship-SCN stamp, so the upgrader's
					 * observe puts every post-upgrade record strictly above the
					 * boundary (cluster_pi_shadow.h proof item 2). */
					holders_bm &= ~((uint32)1u << cluster_node_id);
				}
			}

			if (holders_bm != 0) {
				/*
				 * PGRAC: spec-6.12e2 (structural fix) — NEVER sleep for the
				 * ACKs here.  This handler runs in the LMON dispatch loop and
				 * the very ACKs a blocking wait would collect are drained by
				 * this same loop, so the CV sleep could only ever time out
				 * (observed: guaranteed HC116 DENIED_INVALIDATE_TIMEOUT for
				 * any REMOTE S holder; unreachable in two-node clusters where
				 * the S set reduces to {master, requester}, both handled
				 * above — first reached by the 3-corner e2 nudge flow).  Fire
				 * the INVALIDATEs and reply DENIED_PENDING_X: the requester's
				 * own HC117 starvation backoff retries the request; meanwhile
				 * each holder drops its copy and its ACK — epoch/checksum
				 * validated in the ACK handler — clears its S bit on the
				 * authoritative entry, so a following retry finds no remote
				 * holder left and grants.  Deny direction throughout (8.A).
				 */
				if (xvs_b2_captured) {
					/* PGRAC: spec-6.14a D3 — drop the captured image: this
					 * PENDING_X deny replies without it, and the requester's
					 * retry recaptures against the post-revoke state. */
					gcs_block_release_ship_image(block_payload_release_cb,
												 block_payload_release_arg);
					block_payload = NULL;
				}
				gcs_block_broadcast_invalidate_nowait(req, holders_bm);
				(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
				/* Release the IN_FLIGHT dedup entry BEFORE the deny goes out:
				 * the convergence this reply promises ("a following retry
				 * finds no remote holder left and grants") only works if the
				 * retry is re-evaluated.  The retry reuses the same
				 * (request_id, epoch) dedup key, so a leftover in-flight
				 * entry silently swallows it (IN_FLIGHT_DUPLICATE) until the
				 * TTL sweep — every swallowed round burns a full
				 * cluster.gcs_reply_timeout_ms at the requester and the
				 * S3-observed 53R90 retransmit-exhaustion storm follows. */
				cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
				gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_PENDING_X,
									 InvalidXLogRecPtr, NULL);
				return;
			}

			/*
			 * GCS-race round-2 additional hardening: exact-epoch recheck
			 * before EITHER holders-cleared grant leg below.  The invalidate
			 * round-trips above span reconfiguration windows; granting X to
			 * a stale-epoch request would hand out ownership computed from a
			 * bitmap a newer epoch's rebuild may have re-seeded.  Same deny
			 * idiom as the X-branch forward leg (HC73 recheck), incl. the
			 * dedup release (the epoch-bumped retry uses a NEW key; the old
			 * in-flight entry would only waste a slot until the sweep).
			 */
			{
				uint64 grant_epoch = cluster_epoch_get_current();

				if (req->epoch < grant_epoch) {
					if (xvs_b2_captured) {
						gcs_block_release_ship_image(block_payload_release_cb,
													 block_payload_release_arg);
						block_payload = NULL;
					}
					(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
					cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
					gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
										 InvalidXLogRecPtr, NULL);
					return;
				}
			}

			/*
			 * PGRAC: spec-4.7a D3 — explicit X grant to the upgrading S holder.
			 * After the OTHER S holders are invalidated the requester is the
			 * sole remaining S holder, so S→X_UPGRADE is now legal:  apply it
			 * (master_state→X, x_holder=sender, clear s-bits — single x_holder,
			 * HG5) and reply STORAGE_FALLBACK so the requester writes its own
			 * resident / shared-storage copy.  WITHOUT this, produce_reply
			 * (state still S, master not resident) replies DENIED_MASTER_NOT_
			 * HOLDER and the sender retransmit-loops to 53R90.  A non-holder
			 * requester takes the spec-6.14a D3 B2 grant below instead.
			 */
			if (requester_is_s_holder
				&& cluster_pcm_lock_apply_gcs_transition(req->tag, PCM_TRANS_S_TO_X_UPGRADE,
														 req->sender_node)) {
				(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
				status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
				page_lsn = InvalidXLogRecPtr;
				block_payload = NULL;
				goto build_and_send_reply;
			}

			/*
			 * PGRAC: spec-6.14a D3 (B2) — explicit grant for the non-holder
			 * requester.  Every S copy (the master's own included) has been
			 * dropped and ack-certified; the image captured BEFORE the
			 * revoke round is the sole guaranteed-current carrier.  Record
			 * the requester as the X holder atomically and reply GRANTED
			 * with the image (storage currency is unproven post-revoke, so
			 * STORAGE_FALLBACK is not a legal reply here).
			 */
			if (!requester_is_s_holder && xvs_b2_captured) {
				cluster_pcm_lock_master_grant_x_to(req->tag, req->sender_node, page_lsn,
												   (SCN)((PageHeader)block_payload)->pd_block_scn,
												   req->request_id, req->epoch);
				(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->x_vs_s_nonholder_grant_count, 1);
				status = GCS_BLOCK_REPLY_GRANTED;
				goto build_and_send_reply;
			}
		}
		/* pre_state == N OR fell through after successful S broadcast OR
		 * X branch fell through (forward send failed):  continue to the
		 * original spec-2.33 master grant flow below.  pending_x will be
		 * cleared when the requester sends the post-grant transition ack
		 * (HC111 pattern).  For now, clear after a successful master grant
		 * inside the original flow once it sees status == GRANTED. */
	}
x_path_skipped:

	/*
	 * PGRAC: spec-2.35 D6 (HC101) — master forward decision.
	 *
	 *	Before spec-2.33's direct-ship flow, check if we (master) can
	 *	delegate the ship to a cached S-holder peer:
	 *	  state == S
	 *	  + master not locally-resident (we don't hold the buffer)
	 *	  + master_holder is a valid peer node (HC110 maintained;
	 *	    cluster_pcm_master_holder_node_by_tag returns >= 0)
	 *	  + that peer is not ourselves (avoid self-forward loop)
	 *	  + transition is N→S (2-way read sharing only; S→X writer transfer
	 *	    lands in spec-2.36 with holder invalidation)
	 *	→ send GCS_BLOCK_FORWARD to peer, keep requester out of
	 *	  s_holders_bitmap until sender installs the holder reply and sends
	 *	  a GCS control ACK (HC111 cache-residency semantics),
	 *	  install dedup entry as FORWARDED_IN_FLIGHT (HC113;  Step 6
	 *	  wires the dedup state machine), return without replying —
	 *	  the holder will direct-ship the reply to the original sender
	 *	  with `status=GRANTED_FROM_HOLDER` + `forwarding_master_node=us`
	 *	  (HC108 authorized chain;  Step 5 wires the sender HC108 check).
	 *
	 *	On evict race or holder bitmap mismatch, fall through to the
	 *	original spec-2.33 master flow which will reply
	 *	DENIED_MASTER_NOT_HOLDER and let spec-2.34 retransmit retry.
	 */
	{
		PcmLockMode pre_state;
		bool local_resident;
		int32 holder_node;

		pre_state = cluster_pcm_lock_query(req->tag);
		local_resident = cluster_bufmgr_probe_block_for_gcs(req->tag);
		holder_node = cluster_pcm_master_holder_node_by_tag(req->tag);
		if (s_barrier_read_image_only) {
			if (!cluster_pcm_lock_authority_matches(
					req->tag, &s_barrier_authority_after)) {
				status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
				pg_atomic_fetch_add_u64(
					&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
				goto build_and_send_reply;
			}
			pre_state = (PcmLockMode)s_barrier_authority_after.state;
			holder_node = s_barrier_authority_after.x_holder_node;
		}

		/* spec-2.35 D15 — fault injection.  SKIP makes the master skip the
		 * forward decision so the test fixture can exercise the fallback
		 * paths (STORAGE_FALLBACK / MASTER_NOT_HOLDER) under the same
		 * topology that would otherwise trigger forward. */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-forward-master-side");

		/*
		 * PGRAC: spec-5.2 D2 — X-held cross-node read.  An N→S read targeting
		 * a block currently held in X still needs the holder's CURRENT image
		 * (e.g. to see an uncommitted ITL row-lock before a cross-node TX
		 * wait).  Ship a one-shot read image; the X holder is undisturbed (no
		 * ownership transfer, no downgrade).  Cases we cannot serve safely
		 * fall through to the pre-spec-5.2 fail-closed (Rule 8.A).
		 */
		{
			GcsXheldReadShipDecision rd = gcs_block_xheld_read_ship_decision(
				req->transition_id, (int)pre_state, holder_node, req->sender_node, cluster_node_id,
				local_resident);

			if (rd == GCS_XHELD_READ_DIRECT_FROM_MASTER
				&& !cluster_injection_should_skip("cluster-gcs-block-forward-master-side")) {
				/* PGRAC: spec-5.59 D3 — holder-side read-image ship service
				 * time (master-local X: block copy).  Service-time bucket,
				 * never folded into requester pp. */
				ClusterXpScope xp_ship;

				/*
				 * PGRAC: spec-6.12a — quiescent S-cache.  Master==holder and the
				 * read targets our X-held block: when the wave GUC is on and the
				 * block is quiescent, flush it storage-current, self-downgrade
				 * X->S and DON'T take the one-shot read-image path — control
				 * falls through to the base master flow below, which now sees
				 * state S with a resident buffer and serves a durable GRANTED
				 * (image + requester N->S registration).  Repeat reads then hit
				 * the requester's cached S copy locally.  Refusal (active ITL /
				 * state raced / flush unavailable) keeps today's one-shot ship.
				 */
				resource_x_successor_result
					= cluster_pcm_lock_resource_x_current_x_successor_exact(
						&req->tag, cluster_node_id,
						&resource_x_preserve_current_x);
				if (!s_barrier_read_image_only && cluster_read_scache
					&& (resource_x_successor_result
							== RESOURCE_X_APPLY_NOT_FOUND
						|| (resource_x_successor_result
								== RESOURCE_X_APPLY_APPLIED
							&& !resource_x_preserve_current_x))) {
					local_downgrade_outcome = cluster_bufmgr_downgrade_x_to_s_for_gcs_prepare_image(
						req->tag, &page_lsn, block_buf, NULL);
					cluster_lever_a_note_downgrade(local_downgrade_outcome
												   == CLUSTER_BUFMGR_GCS_DOWNGRADE_COMMITTED);
					if (local_downgrade_outcome
						== CLUSTER_BUFMGR_GCS_DOWNGRADE_FAILCLOSED_POST_NOTIFY)
						return;
					if (local_downgrade_outcome == CLUSTER_BUFMGR_GCS_DOWNGRADE_COMMITTED) {
						scache_image_prepared = true;
						block_payload = block_buf;
						goto scache_downgraded_fall_through;
					}
				}

				cluster_xp_begin(&xp_ship, CLXP_R_READIMAGE_SHIP);
				/* Master holds X locally: ship its current image without revoking X. */
				if (gcs_block_get_ship_image(req->tag, req->sender_node, true, &page_lsn, block_buf,
											 &block_payload, &block_payload_lkey,
											 &block_payload_release_cb, &block_payload_release_arg,
											 &sf_dep_vec, &sf_dep_valid, NULL)) {
					status = GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
					if (ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->cf_xheld_read_ship_count, 1);
					cluster_xp_end(&xp_ship);
					goto build_and_send_reply;
				}
				cluster_xp_abort(&xp_ship);
				if (s_barrier_read_image_only) {
					status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
					pg_atomic_fetch_add_u64(
						&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
					goto build_and_send_reply;
				}
				/* Evict race — fall through to the fail-closed master flow. */
			} else if (rd == GCS_XHELD_READ_FORWARD_TO_HOLDER
					   && !cluster_injection_should_skip("cluster-gcs-block-forward-master-side")) {
				GcsBlockForwardPayload fwd;
				uint64 current_epoch = cluster_epoch_get_current();

				if (req->epoch < current_epoch) {
					gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
										 InvalidXLogRecPtr, NULL);
					return;
				}
				if (gcs_block_deny_direct_armed_forward_request(req)) {
					/* Retry comes back with the same (request_id, epoch)
					 * key and direct-land suppressed — release the in-flight
					 * dedup entry so it is re-evaluated (S3 RC-B). */
					cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
					return;
				}

				memset(&fwd, 0, sizeof(fwd));
				fwd.request_id = req->request_id;
				fwd.epoch = current_epoch;
				fwd.tag = req->tag;
				fwd.original_requester_node = req->sender_node;
				fwd.requester_backend_id = req->requester_backend_id;
				fwd.master_node = cluster_node_id;
				fwd.transition_id = req->transition_id;
				GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, req, false);
				GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
					&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));
				/* D2: tell the holder to ship a read image and keep its X. */
				GcsBlockForwardPayloadSetReadImage(&fwd, true);
				/* PGRAC: spec-6.12a ㉕ — with the wave GUC on, additionally ask
				 * the holder to TRY the quiescent X->S downgrade so this (and
				 * every later) read becomes a durable cached S instead of a
				 * one-shot re-ship.  The holder alone judges quiescence and
				 * falls back to the read-image ship on refusal; with the GUC
				 * off this is the pre-㉕ one-shot (counted for D0 ceiling). */
				resource_x_preserve_current_x = false;
				resource_x_successor_result
					= cluster_pcm_lock_resource_x_current_x_successor_exact(
						&req->tag, holder_node,
						&resource_x_preserve_current_x);
				if (!s_barrier_read_image_only && cluster_read_scache
					&& (resource_x_successor_result
							== RESOURCE_X_APPLY_NOT_FOUND
						|| (resource_x_successor_result
								== RESOURCE_X_APPLY_APPLIED
							&& !resource_x_preserve_current_x)))
					GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);
				else
					cluster_lever_a_note_fwd_oneshot();

				if (s_barrier_read_image_only
					&& !cluster_pcm_lock_authority_matches(
						req->tag, &s_barrier_authority_after)) {
					status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
					pg_atomic_fetch_add_u64(
						&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
					goto build_and_send_reply;
				}

				if (gcs_block_forward_send_admitted(holder_node, &fwd)) {
					GcsBlockReplyHeader fwd_hdr;

					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);

					memset(&fwd_hdr, 0, sizeof(fwd_hdr));
					fwd_hdr.request_id = req->request_id;
					fwd_hdr.requester_backend_id = req->requester_backend_id;
					fwd_hdr.transition_id = req->transition_id;
					fwd_hdr.sender_node = holder_node;
					fwd_hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
					GcsBlockReplyHeaderSetForwardingMasterNode(&fwd_hdr, cluster_node_id);
					cluster_gcs_block_dedup_install_reply(dedup_worker_id, &key,
														  GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER,
														  &fwd_hdr, NULL);
					return;
				}
				if (s_barrier_read_image_only) {
					status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
					pg_atomic_fetch_add_u64(
						&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
					goto build_and_send_reply;
				}
				/* Forward send failed — fall through to the fail-closed flow. */
			}
			/* NOT_APPLICABLE / DENY → existing S-forward + master flow below
			 * (X-held that we cannot serve stays fail-closed, unchanged). */
		}
		if (s_barrier_read_image_only) {
			status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
			pg_atomic_fetch_add_u64(
				&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
			goto build_and_send_reply;
		}

		if (req->transition_id == PCM_TRANS_N_TO_S && pre_state == PCM_LOCK_MODE_S
			&& !local_resident && holder_node >= 0
			&& holder_node != cluster_node_id
			/* PGRAC: spec-4.7a D3 — never forward to the requester itself.  When
			 * the recorded S-holder IS the sender (it released its content_lock
			 * but the master still records it), forwarding would self-loop.  Fall
			 * through to produce_reply, whose D3 self-holder branch idempotently
			 * re-grants.  Without this guard the N→S re-request self-forwards and
			 * the sender retransmit-loops to 53R90 (the D0 bug). */
			&& holder_node != req->sender_node
			&& !cluster_injection_should_skip("cluster-gcs-block-forward-master-side")) {
			GcsBlockForwardPayload fwd;
			uint64 current_epoch;

			current_epoch = cluster_epoch_get_current();
			if (req->epoch < current_epoch) {
				/* HC73 epoch freshness still applies before forwarding. */
				gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
									 InvalidXLogRecPtr, NULL);
				return;
			}
			if (gcs_block_deny_direct_armed_forward_request(req)) {
				/* Retry comes back with the same (request_id, epoch) key and
				 * direct-land suppressed — release the in-flight dedup entry
				 * so it is re-evaluated (S3 RC-B). */
				cluster_gcs_block_dedup_remove(dedup_worker_id, &key);
				return;
			}

			/* Build and send GCS_BLOCK_FORWARD to holder. */
			memset(&fwd, 0, sizeof(fwd));
			fwd.request_id = req->request_id;
			fwd.epoch = current_epoch;
			fwd.tag = req->tag;
			fwd.original_requester_node = req->sender_node;
			fwd.requester_backend_id = req->requester_backend_id;
			fwd.master_node = cluster_node_id;
			fwd.transition_id = req->transition_id;
			GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, req, false);
			/* PGRAC: spec-2.37 D3 HC127 (spec-2.41 SCN migration) — stamp
			 * expected pi_watermark_scn so the holder can validate the copied
			 * page's pd_block_scn before ship. */
			GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
				&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));

			if (gcs_block_forward_send_admitted(holder_node, &fwd)) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->s_holders_bitmap_redirect_count, 1);

				/* Step 6 will install FORWARDED_IN_FLIGHT into dedup
				 * entry so duplicate requests are routed correctly.  In
				 * Step 4 we use a placeholder install: mark the entry as
				 * a generic in-flight slot with the holder node stored
				 * in the reply_header.sender_node field per HC113. */
				{
					GcsBlockReplyHeader fwd_hdr;

					memset(&fwd_hdr, 0, sizeof(fwd_hdr));
					fwd_hdr.request_id = req->request_id;
					fwd_hdr.requester_backend_id = req->requester_backend_id;
					fwd_hdr.transition_id = req->transition_id;
					fwd_hdr.sender_node = holder_node; /* HC113: holder stored here */
					fwd_hdr.status = (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
					GcsBlockReplyHeaderSetForwardingMasterNode(&fwd_hdr, cluster_node_id);
					cluster_gcs_block_dedup_install_reply(
						dedup_worker_id, &key, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &fwd_hdr, NULL);
				}
				return;
			}
			/* Forward send failed (transport issue); fall through to
			 * direct-ship attempt below.  No PCM state was mutated, so there is
			 * no stale requester holder bit to clean up. */
		}
	}

	/* Produce the reply through the original master flow. */
/* PGRAC: spec-6.12a — landing point after a quiescent X->S self-downgrade:
 * master state is now S with a resident clean buffer, so produce_reply
 * serves a durable GRANTED (image + requester N->S quick re-grant). */
scache_downgraded_fall_through:
	(void)gcs_block_produce_reply(req, block_buf, scache_image_prepared, &status, &page_lsn,
								  &block_payload, &block_payload_lkey, &block_payload_release_cb,
								  &block_payload_release_arg, &sf_dep_vec, &sf_dep_valid);
	if (req->transition_id == PCM_TRANS_N_TO_X || req->transition_id == PCM_TRANS_S_TO_X_UPGRADE)
		(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);

	/* PGRAC: spec-2.41 D1 — master-direct ship self-checks lost-write via SCN.
	 *
	 *	If the master is about to GRANT a block, compare the SHIPPED page's
	 *	pd_block_scn against the master's authoritative pi_watermark_scn(tag)
	 *	through gcs_block_lost_write_verdict() (§2.6).  STALE (shipped < expected)
	 *	and ANOMALY (tracked block, shipped InvalidScn) both fail-closed with
	 *	DENIED_LOST_WRITE so the sender ereport(53R93).  SKIP (not SCN-tracked)
	 *	and PASS ship normally.  Cross-node version order is the global SCN, NOT
	 *	page_lsn (per-node WAL position; spec-2.41 §0) — the old page_lsn check is
	 *	removed.  Master self-check (not sender): only the master holds the
	 *	authoritative watermark. */
	if (status == GCS_BLOCK_REPLY_GRANTED) {
		SCN expected_scn = cluster_pcm_lock_pi_watermark_scn_query(req->tag);
		SCN shipped_scn
			= (block_payload != NULL) ? ((PageHeader)block_payload)->pd_block_scn : InvalidScn;
		GcsLostWriteVerdict verdict;

		/* spec-2.41 D15/P1-C inject — force the SHIPPED pd_block_scn to InvalidScn
		 * to simulate a tracked-but-unstamped (anomaly) source; with a valid
		 * watermark this drives the fail-closed path.  (Was: force page_lsn=0.) */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-stale-ship");
		if (cluster_injection_should_skip("cluster-gcs-block-stale-ship"))
			shipped_scn = InvalidScn;

		/* branch-1 (S3 step-2 forensics) inject — simulate a RETAINED STALE
		 * RESIDENT (a kept Past Image serving as the grant payload): force the
		 * SHIPPED pd_block_scn one time-step below the valid watermark so the
		 * verdict is STALE (§2.6 branch 1) while shared storage keeps its real
		 * version.  Master-direct site ONLY — the holder-forward twin must not
		 * fire this, so a test that greens can only have exercised THIS path.
		 * The predecessor comes from the SCN layer (fails closed to InvalidScn
		 * = leave the shipped value alone; the test round simply retries). */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-stale-ship-resident");
		if (cluster_injection_should_skip("cluster-gcs-block-stale-ship-resident")) {
			SCN forced_stale_scn = cluster_scn_time_predecessor(expected_scn);

			if (SCN_VALID(forced_stale_scn))
				shipped_scn = forced_stale_scn;
		}

		verdict = gcs_block_lost_write_verdict(expected_scn, shipped_scn);
		if (verdict == GCS_LOST_WRITE_SKIP) {
			/* spec-2.41 D7 observability — block not SCN-tracked (no fire). */
			if (ClusterGcsBlock != NULL)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->lost_write_not_scn_tracked_skip_count, 1);
		} else if (verdict == GCS_LOST_WRITE_FAIL_STALE || verdict == GCS_LOST_WRITE_FAIL_ANOMALY) {
			/* S3 forensics step 1 — the (expected, shipped) verdict SCN pair is
			 * only known on this producer; LOG it so the requester's 53R93
			 * errdetail correlates by the unambiguous {requester, request_id,
			 * epoch, tag} 4-tuple (step 1b: epoch is the WIRE request epoch
			 * req->epoch — the requester correlates by it;  a reply-time
			 * cluster_epoch_get_current() could differ across a reconfig).
			 * Step 1a/1b: THIS node is the master, so the provenance of the
			 * advance that produced expected_scn is queryable here — the
			 * branch-3 (watermark false-positive) discriminator. */
			ClusterPcmWmProv wm_prov;
			bool wm_have = cluster_pcm_lock_pi_watermark_prov_query(req->tag, &wm_prov);

			ereport(
				LOG,
				(errmsg_internal(
					"cluster_gcs_block: lost-write verdict %s on master-direct ship: tag "
					"spc=%u db=%u rel=%u block=%u fork=%d expected pi_watermark_scn=" UINT64_FORMAT
					" shipped pd_block_scn=" UINT64_FORMAT " requester=%d request_id=" UINT64_FORMAT
					" epoch=" UINT64_FORMAT
					" transition=%d wm_src=%s wm_sender=%d wm_request_id=" UINT64_FORMAT
					" wm_epoch=" UINT64_FORMAT " wm_old=" UINT64_FORMAT " wm_new=" UINT64_FORMAT
					" wm_matches_expected=%d",
					verdict == GCS_LOST_WRITE_FAIL_STALE ? "STALE" : "ANOMALY", req->tag.spcOid,
					req->tag.dbOid, req->tag.relNumber, req->tag.blockNum, (int)req->tag.forkNum,
					(uint64)expected_scn, (uint64)shipped_scn, req->sender_node, req->request_id,
					req->epoch, (int)req->transition_id,
					wm_prov.table_full ? "none(prov-table-full)"
									   : cluster_pcm_wm_src_text(wm_prov.source),
					wm_have ? wm_prov.sender_node : -1, wm_have ? wm_prov.request_id : 0,
					wm_have ? wm_prov.epoch : 0, wm_have ? (uint64)wm_prov.old_scn : 0,
					wm_have ? (uint64)wm_prov.new_scn : 0,
					wm_have ? (int)(wm_prov.new_scn == expected_scn) : -1)));

			/*
			 * PGRAC: branch-1 (S3 step-2 forensics) — storage-fallback rescue.
			 *
			 *	A STALE verdict here means the master is holding a RETAINED
			 *	stale resident (a kept Past Image) as the would-be grant
			 *	payload.  When the shared-storage page already covers the
			 *	authoritative watermark, the cluster-proven current version is
			 *	durably readable: convert the reply to
			 *	GRANTED_STORAGE_FALLBACK (ship no image; page_lsn carries the
			 *	watermark — the same contract as the state=N grant above) so
			 *	the requester proves/refreshes its copy through
			 *	cluster_gcs_block_fallback_verify_refresh instead of aborting
			 *	53R93 on every hit.  The verdict re-check uses the same
			 *	gcs_block_lost_write_verdict SCN order the detector itself
			 *	trusts (spec-2.41 §2.6).
			 *
			 *	ANOMALY (shipped InvalidScn on a tracked tag) is NOT rescued:
			 *	an unstamped resident says the master's own view is broken —
			 *	stay fail-closed.  A failed/unverifiable storage read keeps
			 *	the fail-closed DENIED too (Rule 8.A).
			 */
			if (verdict == GCS_LOST_WRITE_FAIL_STALE) {
				SCN storage_scn = InvalidScn;
				bool storage_read_ok = false;
				MemoryContext probe_cxt = CurrentMemoryContext;

				/*
				 * PGRAC: the storage probe is the only disk I/O on this
				 * self-check path and smgrread can ereport(ERROR) (short
				 * read on a concurrent truncate/drop, real I/O failure).
				 * An uncaught throw here would leak the ship image — a
				 * live_sge borrow is a raw pin outside ResourceOwner
				 * tracking — and drop the reply after produce_reply already
				 * applied the PCM transition, wedging the requester.  Catch
				 * locally and fall through to the fail-closed DENIED arm,
				 * which releases the image and still replies (Rule 8.A).
				 */
				PG_TRY();
				{
					storage_read_ok
						= cluster_bufmgr_read_storage_scn_for_gcs(req->tag, &storage_scn);
				}
				PG_CATCH();
				{
					ErrorData *edata;

					MemoryContextSwitchTo(probe_cxt);
					edata = CopyErrorData();
					FlushErrorState();
					ereport(LOG, (errmsg_internal(
									 "cluster_gcs_block: master-direct storage-fallback probe "
									 "failed; keeping fail-closed DENIED_LOST_WRITE: %s",
									 edata->message != NULL ? edata->message : "(no message)")));
					FreeErrorData(edata);
					storage_read_ok = false;
					storage_scn = InvalidScn;
				}
				PG_END_TRY();

				/* Test hook: pretend storage is unverifiable so the rescue
				 * refuses and the original fail-closed DENIED ships. */
				CLUSTER_INJECTION_POINT("cluster-gcs-block-master-direct-fallback-storage-stale");
				if (cluster_injection_should_skip(
						"cluster-gcs-block-master-direct-fallback-storage-stale"))
					storage_scn = InvalidScn;

				if (storage_read_ok
					&& gcs_block_lost_write_verdict(expected_scn, storage_scn)
						   == GCS_LOST_WRITE_PASS) {
					ereport(
						LOG,
						(errmsg_internal(
							"cluster_gcs_block: master-direct stale ship rescued to "
							"GRANTED_STORAGE_FALLBACK: tag spc=%u db=%u rel=%u block=%u "
							"fork=%d expected pi_watermark_scn=" UINT64_FORMAT
							" shipped pd_block_scn=" UINT64_FORMAT
							" storage pd_block_scn=" UINT64_FORMAT
							" requester=%d request_id=" UINT64_FORMAT " epoch=" UINT64_FORMAT,
							req->tag.spcOid, req->tag.dbOid, req->tag.relNumber, req->tag.blockNum,
							(int)req->tag.forkNum, (uint64)expected_scn, (uint64)shipped_scn,
							(uint64)storage_scn, req->sender_node, req->request_id, req->epoch)));
					status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
					page_lsn = (XLogRecPtr)expected_scn;
					gcs_block_release_ship_image(block_payload_release_cb,
												 block_payload_release_arg);
					block_payload = NULL;
					block_payload_lkey = 0;
					block_payload_release_cb = NULL;
					block_payload_release_arg = NULL;
					if (ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(
							&ClusterGcsBlock->lost_write_master_direct_storage_fallback_count, 1);
					goto build_and_send_reply;
				}
			}

			status = GCS_BLOCK_REPLY_DENIED_LOST_WRITE;
			page_lsn = InvalidXLogRecPtr;
			gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
			block_payload = NULL;
			block_payload_lkey = 0;
			block_payload_release_cb = NULL;
			block_payload_release_arg = NULL;
			if (ClusterGcsBlock != NULL) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->lost_write_detected_count, 1);
				if (verdict == GCS_LOST_WRITE_FAIL_ANOMALY)
					pg_atomic_fetch_add_u64(
						&ClusterGcsBlock->lost_write_invalidscn_failclosed_count, 1);
			}
		}
	}

	/*
	 * Build the canonical reply header ONCE so that the dedup install
	 * (cached entry) and the wire send share identical bytes (epoch,
	 * checksum, etc).  Avoids a micro-second race where two
	 * cluster_epoch_get_current() calls observe different epochs and
	 * cause a cached re-send to mismatch the originally-sent reply.
	 *
	 * Install BEFORE send so a duplicate retry arriving between send
	 * and install still hits CACHED_REPLY rather than
	 * IN_FLIGHT_DUPLICATE.
	 */
build_and_send_reply: {
	bool has_block_payload;
	bool send_sf_dep;
	uint32 header_len;
	uint32 total;
	char *buf;
	GcsBlockReplyHeader *hdr;

	if (s_barrier_read_image_only
		&& status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		&& !cluster_pcm_lock_authority_matches(
			req->tag, &s_barrier_authority_after)) {
		gcs_block_release_ship_image(block_payload_release_cb,
			block_payload_release_arg);
		block_payload = NULL;
		block_payload_lkey = 0;
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
		page_lsn = InvalidXLogRecPtr;
		status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
		pg_atomic_fetch_add_u64(
			&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
	}
	has_block_payload
		= (status == GCS_BLOCK_REPLY_GRANTED
		   || status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER)
		  && block_payload != NULL;
	send_sf_dep = sf_dep_valid && has_block_payload;
	header_len = send_sf_dep ? (uint32)sizeof(GcsBlockReplyHeaderV2)
							 : (uint32)sizeof(GcsBlockReplyHeader);
	total = header_len + GCS_BLOCK_DATA_SIZE;

	buf = (char *)palloc0(total);
	hdr = (GcsBlockReplyHeader *)buf;
	hdr->request_id = req->request_id;
	hdr->page_lsn = (uint64)page_lsn;
	hdr->epoch = cluster_epoch_get_current();
	hdr->sender_node = cluster_node_id;
	hdr->requester_backend_id = req->requester_backend_id;
	hdr->transition_id = req->transition_id;
	hdr->status = (uint8)status;
	GcsBlockReplyHeaderSetForwardingMasterNode(hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	if (send_sf_dep) {
		GcsBlockReplyHeaderV2 *hdrv2 = (GcsBlockReplyHeaderV2 *)buf;
		int i;
		int n = 0;

		hdrv2->sf_flags = GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC;
		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
			if (XLogRecPtrIsInvalid(sf_dep_vec.required[i]))
				continue;
			hdrv2->sf_dep[n].origin_node = i;
			hdrv2->sf_dep[n].required_redo_lsn = (uint64)sf_dep_vec.required[i];
			n++;
		}
		hdrv2->sf_dep_count = (uint8)n;
	}

	if (has_block_payload) {
		hdr->checksum = gcs_block_compute_checksum(block_payload);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_ship_bytes_total, GCS_BLOCK_DATA_SIZE);
	} else {
		hdr->checksum = gcs_block_compute_checksum(buf + header_len);
	}

	cluster_gcs_block_dedup_install_reply_ex(dedup_worker_id, &key, status, hdr,
											 has_block_payload ? block_payload : NULL,
											 send_sf_dep ? &sf_dep_vec : NULL, send_sf_dep);

	/*
	 * Duplicate-reply injection (S3 RC-A test surface).  When armed with
	 * SKIP, ship the just-installed cached copy AHEAD of the normal send so
	 * the requester receives the same reply twice back-to-back — the
	 * deterministic stand-in for the protocol-normal duplicate (dedup
	 * CACHED_REPLY resend racing the original under retransmit).  The
	 * requester-side first-reply-wins guard must drop the second delivery
	 * (stale_reply_drop_count++) instead of overwriting the slot mid-consume.
	 */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-duplicate-grant-reply");
	if (cluster_injection_should_skip("cluster-gcs-block-duplicate-grant-reply")) {
		GcsBlockDedupEntry dup_entry;

		memset(&dup_entry, 0, sizeof(dup_entry));
		if (cluster_gcs_block_dedup_lookup_or_register(
				dedup_worker_id, &key, req->tag, req->transition_id,
				GcsBlockRequestPayloadGetLifetimeHintMs(req),
				cluster_sf_peer_supports_gcs_done(req->sender_node), &dup_entry)
			== GCS_BLOCK_DEDUP_CACHED_REPLY)
			gcs_block_resend_cached_reply(req->sender_node, &dup_entry);
	}

	/*
		 * spec-2.34 D17 — drop-reply injection.  When active with SKIP,
		 * master DOES NOT send the reply envelope (sender experiences
		 * timeout → retransmit).  The dedup entry was installed above so
		 * a duplicate retry from the sender will hit CACHED_REPLY and
		 * the cached reply WILL be re-sent (unless the inject is still
		 * active on that retry).  Useful for driving the
		 * retransmit_send_count + dedup_hit_count TAP surfaces.
		 */
	/*
	 * spec-7.2a: gate the drop-reply dispatch on the test target relfilenode.
	 * A :skipn:N count is per-process global; without this gate an unrelated
	 * (catalog / internal) block ship consumes the countdown before the test's
	 * user-relation ship reaches the point.  Gating the CLUSTER_INJECTION_POINT
	 * itself (not just should_skip) ensures only matching ships consume the
	 * count.  0 (default) keeps the un-targeted behaviour for spec-2.34 tests.
	 *
	 * Current TAP coverage uses target=0 only (shared_catalog remaps the
	 * catalog-visible relfilenode to a different physical relNumber, so SQL
	 * cannot name the shipped block); the non-zero filter is reserved for
	 * precise spec-2.34-style targeting on non-shared-catalog rigs and is not
	 * yet exercised by any test.
	 */
	if (cluster_gcs_block_drop_target_relfilenode == 0
		|| BufTagGetRelNumber(&req->tag)
			   == (RelFileNumber)cluster_gcs_block_drop_target_relfilenode) {
		CLUSTER_INJECTION_POINT("cluster-gcs-block-drop-reply-before-send");
		if (cluster_injection_should_skip("cluster-gcs-block-drop-reply-before-send")) {
			gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
			block_payload_release_cb = NULL;
			block_payload_release_arg = NULL;
			pfree(buf);
			return;
		}
	}

	{
		ClusterICSendResult send_rc;
		bool live_sge_payload
			= has_block_payload && block_payload_release_cb == gcs_block_release_live_sge;

		if (GcsBlockRequestPayloadIsDirectLandArmed(req)) {
			if (!GcsBlockReplyStatusIsDirectLandSendable((GcsBlockReplyStatus)hdr->status))
				GCS_BLOCK_LOG_MASTER_NOT_HOLDER_REQUEST(req, "direct-land-nonsendable");
			(void)gcs_block_try_send_direct_reply(
				req->sender_node, true, hdr, has_block_payload ? block_payload : NULL,
				has_block_payload ? block_payload_lkey : 0, block_payload_release_cb,
				block_payload_release_arg);
			block_payload_release_cb = NULL;
			block_payload_release_arg = NULL;
			pfree(buf);
			return;
		}

		if (has_block_payload) {
			ClusterICSge sge[2];

			memset(sge, 0, sizeof(sge));
			sge[0].addr = hdr;
			sge[0].len = header_len;
			sge[1].addr = (void *)block_payload;
			sge[1].len = GCS_BLOCK_DATA_SIZE;
			sge[1].lkey = block_payload_lkey;
			sge[1].release_cb = block_payload_release_cb;
			sge[1].release_arg = block_payload_release_arg;
			send_rc = cluster_ic_rdma_send_envelope_sge(
				PGRAC_IC_MSG_GCS_BLOCK_REPLY, req->sender_node, sge, lengthof(sge), total);
			block_payload_release_cb = NULL;
			block_payload_release_arg = NULL;
		} else {
			send_rc = cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, req->sender_node, buf,
											   total);
		}

		/* Round-5: an admitted reply (WOULD_BLOCK) is a sent reply. */
		cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, send_rc);
		if ((send_rc == CLUSTER_IC_SEND_DONE || send_rc == CLUSTER_IC_SEND_WOULD_BLOCK)
			&& ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
		if (send_rc == CLUSTER_IC_SEND_DONE && live_sge_payload)
			gcs_block_note_live_sge_send();
	}

	gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
	block_payload_release_cb = NULL;
	block_payload_release_arg = NULL;
	pfree(buf);
}
}


/* cluster_gcs_block_payload_shard (spec-7.3 D4) lives in
 * cluster_gcs_block_shard.c — extracted at D9 as a pure file so the
 * cluster_unit suite links the REAL staging-path router. */


/* ============================================================
 * Receiver: sender-side (D6).
 *
 *	HC80 compound key (requester_backend_id, request_id) so this handler
 *	does NOT scan all backends to find the matching outstanding slot — it
 *	indexes directly via requester_backend_id.
 * ============================================================ */

void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req, int32 dest_node)
{
	int backend_idx;
	ClusterGcsBlockBackendBlock *blk;
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	BufferDesc *abort_target_buf = NULL;
	bool abort_prepared = false;
	void *target_addr = NULL;
	uint32 target_lkey = 0;
	uint32 arm_id = 0;
	uint32 generation = 0;
	bool posted = false;
	int i;

	if (req == NULL)
		return;
	GcsBlockRequestPayloadSetDirectLandArmed(req, false);

	backend_idx = req->requester_backend_id - 1;
	if (backend_idx < 0 || backend_idx >= MaxBackends)
		return;
	blk = &gcs_block_backend_blocks[backend_idx];

	LWLockAcquire(&blk->lock.lock, LW_SHARED);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		ClusterGcsBlockOutstandingSlot *candidate = &blk->slots[i];

		if (candidate->in_use && candidate->request_id == req->request_id) {
			slot = candidate;
			if (slot->direct_state == GCS_BLOCK_DIRECT_ARMING
				&& slot->direct_expected_peer == dest_node && slot->direct_target_prepared
				&& slot->direct_target_kind == GCS_BLOCK_DIRECT_TARGET_SHARED_BUFFER) {
				target_addr = slot->direct_target_addr;
				target_lkey = slot->direct_target_lkey;
				arm_id = slot->direct_arm_id;
				generation = slot->direct_generation;
			}
			break;
		}
	}
	LWLockRelease(&blk->lock.lock);

	if (slot == NULL || target_addr == NULL)
		return;
	if (target_lkey == 0
		&& !cluster_ic_rdma_shared_buffers_sge(target_addr, GCS_BLOCK_DATA_SIZE, &target_lkey))
		target_lkey = 0;

	posted = target_lkey != 0
			 && cluster_ic_rdma_block_reply_post_recv(dest_node, arm_id, generation, target_addr,
													  target_lkey);

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	if (slot->in_use && slot->request_id == req->request_id
		&& slot->direct_state == GCS_BLOCK_DIRECT_ARMING && slot->direct_generation == generation
		&& slot->direct_arm_id == arm_id) {
		if (posted) {
			slot->direct_state = GCS_BLOCK_DIRECT_ARMED;
			GcsBlockRequestPayloadSetDirectLandArmed(req, true);
		} else {
			abort_target_buf = slot->direct_target_buf;
			abort_prepared = slot->direct_target_prepared;
			slot->direct_state = GCS_BLOCK_DIRECT_UNARMED;
			slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
			slot->direct_target_buf = NULL;
			slot->direct_target_addr = NULL;
			slot->direct_target_lkey = 0;
			slot->direct_target_prepared = false;
			slot->direct_abort_reason = GCS_BLOCK_DIRECT_ABORT_ARM_FAILED;
		}
	} else if (posted) {
		/*
		 * The backend timed out/cancelled while LMON posted the receive.  Reset
		 * the lane before the target can be released by any abort cleanup.
		 */
		cluster_ic_rdma_block_reply_abort_peer(dest_node, "direct-land arm raced slot cleanup");
	}
	LWLockRelease(&blk->lock.lock);

	gcs_block_direct_finish_target(abort_target_buf, abort_prepared, false, InvalidXLogRecPtr);
}

static void
gcs_block_direct_fail_slot(ClusterGcsBlockBackendBlock *blk, ClusterGcsBlockOutstandingSlot *slot,
						   ClusterGcsBlockDirectAbortReason reason, bool authoritative_denial,
						   const GcsBlockReplyHeader *hdr)
{
	BufferDesc *target_buf;
	BufferTag target_tag;
	ClusterPcmOwnSnapshot own;
	ClusterPcmOwnResult own_result = CLUSTER_PCM_OWN_INVALID;
	uint64 request_id;
	uint64 request_epoch;
	int backend_idx;
	int slot_idx;
	int direct_state;
	int buffer_id = -1;
	bool prepared;
	bool reply_received;

	Assert(blk != NULL);
	Assert(slot != NULL);

	target_buf = slot->direct_target_buf;
	target_tag = slot->tag;
	request_id = slot->request_id;
	request_epoch = slot->request_epoch;
	backend_idx = (int)(blk - &gcs_block_backend_blocks[0]);
	slot_idx = (int)(slot - &blk->slots[0]);
	direct_state = (int)slot->direct_state;
	reply_received = slot->reply_received;
	prepared = slot->direct_target_prepared;
	slot->direct_state = GCS_BLOCK_DIRECT_ABORTED;
	slot->direct_abort_reason = reason;
	slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
	slot->direct_target_buf = NULL;
	slot->direct_target_addr = NULL;
	slot->direct_target_lkey = 0;
	slot->direct_target_prepared = false;
	gcs_block_note_direct_abort();
	/* First-reply-wins (S3 RC-A): if a wire reply already landed for this
	 * attempt, leave the slot reply fields untouched — the requester may be
	 * consuming them without the lock — and do not mark stale either (the
	 * landed reply is the outcome).  Just signal. */
	if (slot->reply_received) {
		/* keep landed reply */
	} else if (authoritative_denial && hdr != NULL) {
		slot->reply_header = *hdr;
		memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
		slot->reply_received = true;
	} else {
		slot->stale = true;
	}
	ConditionVariableSignal(&slot->reply_cv);
	LWLockRelease(&blk->lock.lock);
	gcs_block_direct_finish_target(target_buf, prepared, false, InvalidXLogRecPtr);
	if (prepared || authoritative_denial) {
		memset(&own, 0, sizeof(own));
		own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(&target_tag, &buffer_id, &own);
		elog(LOG,
			 "cluster GCS block direct abort observation: backend=%d slot=%d request_id=%llu "
			 "epoch=%llu rel=%u fork=%d blk=%u reason=%d authoritative_denial=%d "
			 "reply_received_before=%d direct_state_before=%d prepared=%d target_cleanup_done=%d "
			 "own_result=%d buffer=%d own_state=%u own_generation=%llu own_token=%llu "
			 "own_flags=0x%x",
			 backend_idx, slot_idx, (unsigned long long)request_id,
			 (unsigned long long)request_epoch, target_tag.relNumber, (int)target_tag.forkNum,
			 target_tag.blockNum, (int)reason, authoritative_denial ? 1 : 0, reply_received ? 1 : 0,
			 direct_state, prepared ? 1 : 0, prepared ? 1 : 0, (int)own_result, buffer_id,
			 own.pcm_state, (unsigned long long)own.generation,
			 (unsigned long long)own.reservation_token, own.flags);
	}
}

void
cluster_gcs_block_lmon_handle_direct_land_completion(int32 peer_node, uint64 wr_id,
													 bool cqe_success, uint32 byte_len,
													 const void *sidecar)
{
	uint32 wr_peer = 0;
	uint32 arm_id = 0;
	uint32 generation = 0;
	int backend_idx;
	int slot_idx;
	ClusterGcsBlockBackendBlock *blk;
	ClusterGcsBlockOutstandingSlot *slot;
	const ClusterICEnvelope *env;
	const GcsBlockReplyHeader *hdr;
	void *page;
	uint32 env_crc;
	GcsBlockReplyStatus status;
	bool success_status;
	bool identity_ok;
	int32 fwd_master;

	if (!cluster_ic_rdma_direct_land_decode_wr_id(wr_id, &wr_peer, &arm_id, &generation)
		|| (int32)wr_peer != peer_node
		|| !gcs_block_direct_decode_arm_id(arm_id, &backend_idx, &slot_idx))
		return;

	blk = &gcs_block_backend_blocks[backend_idx];
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	slot = &blk->slots[slot_idx];
	if (!slot->in_use || slot->direct_state != GCS_BLOCK_DIRECT_ARMED
		|| slot->direct_generation != generation || slot->direct_arm_id != arm_id
		|| slot->direct_expected_peer != peer_node) {
		LWLockRelease(&blk->lock.lock);
		return;
	}
	if (!cqe_success) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_CQE_ERROR, false, NULL);
		return;
	}
	slot->direct_state = GCS_BLOCK_DIRECT_LANDED;
	if (byte_len != CLUSTER_IC_RDMA_DIRECT_LAND_REPLY_BYTES || sidecar == NULL) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_BAD_LENGTH, false, NULL);
		return;
	}

	env = (const ClusterICEnvelope *)sidecar;
	hdr = (const GcsBlockReplyHeader *)((const char *)sidecar + PGRAC_IC_ENVELOPE_BYTES);
	page = slot->direct_target_addr;
	if (page == NULL || env->magic != PGRAC_IC_ENVELOPE_MAGIC
		|| env->version != PGRAC_IC_ENVELOPE_VERSION_V1
		|| env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY || (int32)env->source_node_id != peer_node
		|| env->dest_node_id != (uint32)cluster_node_id
		|| env->payload_length != GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_BAD_SIDECAR, false, NULL);
		return;
	}
	env_crc = gcs_block_direct_envelope_crc(env, hdr, page);
	if (env->payload_crc32c != env_crc) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_BAD_CHECKSUM, false, NULL);
		return;
	}

	status = (GcsBlockReplyStatus)hdr->status;
	success_status = GcsBlockReplyStatusAllowsDirectLandInstall(status);
	fwd_master = GcsBlockReplyHeaderGetForwardingMasterNode(hdr);
	identity_ok = hdr->request_id == slot->request_id
				  && hdr->requester_backend_id == backend_idx + 1
				  && hdr->transition_id == slot->transition_id && hdr->epoch >= slot->request_epoch
				  && hdr->sender_node == peer_node;
	if (identity_ok) {
		if (fwd_master == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)
			identity_ok = GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(status);
		else
			identity_ok = fwd_master == slot->expected_master_node
						  && (status == GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
							  || status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE
							  || status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
							  || status == GCS_BLOCK_REPLY_DENIED_LOST_WRITE);
	}
	if (!identity_ok) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_BAD_IDENTITY, false, NULL);
		return;
	}
	if (hdr->checksum != gcs_block_compute_checksum((const char *)page)) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_BAD_CHECKSUM, false, NULL);
		return;
	}
	if (!success_status) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_BAD_STATUS, true, hdr);
		return;
	}

	/* First-reply-wins (S3 RC-A): a wire reply that already landed for this
	 * attempt owns the slot reply fields (the requester may be consuming
	 * them without the lock).  Clean up the direct target without touching
	 * them; the landed reply is the outcome. */
	if (slot->reply_received) {
		gcs_block_direct_fail_slot(blk, slot, GCS_BLOCK_DIRECT_ABORT_TIMEOUT, false, NULL);
		return;
	}

	cluster_bufmgr_finish_direct_land_target_for_gcs(slot->direct_target_buf, true,
													 (XLogRecPtr)hdr->page_lsn);
	slot->direct_target_prepared = false;
	slot->direct_target_buf = NULL;
	slot->direct_target_addr = NULL;
	slot->direct_target_lkey = 0;
	slot->direct_target_kind = GCS_BLOCK_DIRECT_TARGET_NONE;
	slot->direct_state = GCS_BLOCK_DIRECT_INSTALLED;
	slot->reply_header = *hdr;
	memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
	slot->reply_sf_dep_valid = false;
	slot->reply_sf_flags = 0;
	cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
	slot->reply_received = true;
	gcs_block_note_direct_install();
	ConditionVariableSignal(&slot->reply_cv);
	LWLockRelease(&blk->lock.lock);
}

void
cluster_gcs_block_lmon_abort_direct_land_peer(int32 peer_node,
											  ClusterGcsBlockDirectAbortReason reason)
{
	int i;

	if (gcs_block_backend_blocks == NULL)
		return;
	for (i = 0; i < MaxBackends; i++) {
		ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[i];
		int j;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		for (j = 0; j < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; j++) {
			ClusterGcsBlockOutstandingSlot *slot = &blk->slots[j];

			if (!slot->in_use || slot->direct_expected_peer != peer_node)
				continue;
			if (slot->direct_state == GCS_BLOCK_DIRECT_ARMED
				|| slot->direct_state == GCS_BLOCK_DIRECT_ARMING
				|| slot->direct_state == GCS_BLOCK_DIRECT_LANDED
				|| slot->direct_state == GCS_BLOCK_DIRECT_ABORTING) {
				gcs_block_direct_fail_slot(blk, slot, reason, false, NULL);
				LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
			}
		}
		LWLockRelease(&blk->lock.lock);
	}
}

int
cluster_gcs_block_lmon_drain_direct_land_aborts(void)
{
	int i;
	int drained = 0;

	if (gcs_block_backend_blocks == NULL)
		return 0;
	for (i = 0; i < MaxBackends; i++) {
		ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[i];
		int j;

		LWLockAcquire(&blk->lock.lock, LW_SHARED);
		for (j = 0; j < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; j++) {
			ClusterGcsBlockOutstandingSlot *slot = &blk->slots[j];
			int32 peer;

			if (!slot->in_use || slot->direct_state != GCS_BLOCK_DIRECT_ABORTING)
				continue;
			peer = slot->direct_expected_peer;
			LWLockRelease(&blk->lock.lock);
			cluster_ic_rdma_block_reply_abort_peer(peer, "GCS direct-land abort requested");
			drained++;
			LWLockAcquire(&blk->lock.lock, LW_SHARED);
		}
		LWLockRelease(&blk->lock.lock);
	}
	return drained;
}

static bool
gcs_block_decode_reply_payload(const ClusterICEnvelope *env, const void *payload,
							   const GcsBlockReplyHeader **out_hdr, const char **out_block_data,
							   bool *out_sf_dep_valid, uint8 *out_sf_flags,
							   ClusterSfDepVec *out_sf_dep_vec,
							   const ClusterGcsUndoAuthTrailer **out_undo_trailer)
{
	uint32 v1_size = (uint32)(sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	uint32 v2_size = (uint32)(sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE);
	uint32 undo_size = v1_size + (uint32)sizeof(ClusterGcsUndoAuthTrailer);

	if (out_hdr != NULL)
		*out_hdr = NULL;
	if (out_block_data != NULL)
		*out_block_data = NULL;
	if (out_sf_dep_valid != NULL)
		*out_sf_dep_valid = false;
	if (out_sf_flags != NULL)
		*out_sf_flags = 0;
	if (out_sf_dep_vec != NULL)
		cluster_sf_dep_vec_reset(out_sf_dep_vec);
	if (out_undo_trailer != NULL)
		*out_undo_trailer = NULL;

	if (env == NULL || payload == NULL)
		return false;

	if (env->payload_length == v1_size) {
		const GcsBlockReplyHeader *h = (const GcsBlockReplyHeader *)payload;

		if (!GcsBlockReplyStatusIsLegacy((GcsBlockReplyStatus)h->status))
			return false;
		if (out_hdr != NULL)
			*out_hdr = h;
		if (out_block_data != NULL)
			*out_block_data = ((const char *)payload) + sizeof(GcsBlockReplyHeader);
		return true;
	}

	/*
	 * PGRAC: spec-6.12i D-i1/D-i4 — undo-TT fetch / undo-verdict reply: v1
	 * header + page + 16B authority trailer (8256B; distinct from both the
	 * 8240B v1 and the 8504B v2 sizes).  Only accepted when the status says
	 * so — any other status at this size is malformed and dropped.
	 */
	if (env->payload_length == undo_size) {
		const GcsBlockReplyHeader *h = (const GcsBlockReplyHeader *)payload;

		if (!GcsBlockReplyStatusCarriesUndoAuthTrailer((GcsBlockReplyStatus)h->status))
			return false;
		if (out_hdr != NULL)
			*out_hdr = h;
		if (out_block_data != NULL)
			*out_block_data = ((const char *)payload) + sizeof(GcsBlockReplyHeader);
		if (out_undo_trailer != NULL)
			*out_undo_trailer = (const ClusterGcsUndoAuthTrailer *)(((const char *)payload)
																	+ sizeof(GcsBlockReplyHeader)
																	+ GCS_BLOCK_DATA_SIZE);
		return true;
	}

	if (env->payload_length == v2_size) {
		const GcsBlockReplyHeaderV2 *hdrv2 = (const GcsBlockReplyHeaderV2 *)payload;
		ClusterSfDepVec dep_vec;

		cluster_sf_dep_vec_reset(&dep_vec);
		if (!GcsBlockReplyStatusIsLegacy((GcsBlockReplyStatus)hdrv2->v1.status)
			|| !cluster_smart_fusion
			|| !cluster_gcs_block_reply_v2_extract_dep_vec(hdrv2, &dep_vec)) {
			cluster_sf_dep_note_lost_failclosed();
			return false;
		}
		if (out_hdr != NULL)
			*out_hdr = &hdrv2->v1;
		if (out_block_data != NULL)
			*out_block_data = ((const char *)payload) + sizeof(GcsBlockReplyHeaderV2);
		if (out_sf_flags != NULL)
			*out_sf_flags = hdrv2->sf_flags;
		if (out_sf_dep_vec != NULL)
			*out_sf_dep_vec = dep_vec;
		if (out_sf_dep_valid != NULL)
			*out_sf_dep_valid = !cluster_sf_dep_vec_is_empty(&dep_vec);
		return true;
	}

	return false;
}

/* The undo cleaner owns CTRC progress but reuses the existing DATA outbound
 * queues.  The correlation is armed in CTRC shared state before this call;
 * an enqueue refusal leaves the same request id available for idempotent
 * retry on the next cleaner pass. */
/* The caller owns every entered token, including a refused prepare. */
static bool
gcs_ctrc_dispatch_prepare(const ClusterCtrcCloseDispatch *dispatch,
						  ClusterSemanticAdmissionToken *admission, uint8 *request)
{
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	uint64 participant_incarnation = 0;
	uint64 participant_observed_generation = 0;
	uint64 self_incarnation;
	uint32 required_capabilities
		= PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
		  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;
	bool participant_current;

	MemSet(admission, 0, sizeof(*admission));
	MemSet(&logical, 0, sizeof(logical));
	MemSet(&root, 0, sizeof(root));
	if (dispatch == NULL || dispatch->reserved32 != 0
		|| memcmp(dispatch->reserved8,
				  (const uint8[sizeof(dispatch->reserved8)]){0},
				  sizeof(dispatch->reserved8)) != 0
		|| (dispatch->suboperation != CTRC_SEAL_CLOSE_AND_CLEAN
			&& dispatch->suboperation
			   != CTRC_SEAL_CERTIFICATE_COMMITTED)
		|| dispatch->request_id == 0 || dispatch->grant_generation == 0
		|| dispatch->seal_generation == 0 || cluster_node_id < 0
		|| dispatch->key.origin_node_id != (uint16)cluster_node_id
		|| dispatch->key.owner_instance != (uint8)(cluster_node_id + 1)
		|| dispatch->participant.node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| dispatch->participant.reserved16 != 0
		|| dispatch->participant.capability_record_generation == 0
		|| dispatch->key.cluster_epoch != cluster_epoch_get_current()
		|| dispatch->key.system_identifier != GetSystemIdentifier()
		|| dispatch->key.formation_epoch
		   != dispatch->participant.formation_epoch
		|| dispatch->key.admission_record_generation
		   != dispatch->participant.admission_record_generation
		|| !cluster_gcs_block_family_on_data_plane()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(dispatch->participant.node_id)
		|| (!cluster_write_fence_enforcing()
			? false : !cluster_write_fence_allowed()))
		return false;

	self_incarnation = cluster_qvotec_get_self_incarnation();
	if (self_incarnation == 0 || self_incarnation != dispatch->key.origin_boot_incarnation
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id) != self_incarnation
		|| cluster_semantic_activation_enter_r4_terminal_census(admission)
			   != CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;

	/* A local participant has no peer HELLO record.  Its receipt identity is
	 * bound to the same locally committed OPEN record generation captured by
	 * the TARGET admission token, plus the capabilities compiled into this
	 * running binary.  Remote participants retain the exact observed-slot and
	 * HELLO-generation fence. */
	if (dispatch->participant.node_id == (uint16)cluster_node_id)
		participant_current = admission->record_generation != 0
							  && admission->record_generation <= (uint64)PG_UINT32_MAX
							  && dispatch->participant.capability_record_generation
									 == (uint32)admission->record_generation
							  && dispatch->participant.boot_incarnation == self_incarnation
							  && (cluster_ic_local_capability_word() & required_capabilities)
									 == required_capabilities;
	else
		participant_current
			= cluster_reconfig_get_observed_slot(
				dispatch->participant.node_id, &participant_incarnation,
				&participant_observed_generation)
			  && participant_incarnation != 0
			  && participant_observed_generation != 0
			  && participant_incarnation
				 == dispatch->participant.boot_incarnation
			  && cluster_membership_get_last_admitted_incarnation(
				dispatch->participant.node_id) == participant_incarnation
			  && cluster_reconfig_get_observed_epoch(
				dispatch->participant.node_id)
				 == dispatch->key.formation_epoch
			  && cluster_sf_peer_capability_generation_matches(
				dispatch->participant.node_id, required_capabilities,
				dispatch->participant.capability_record_generation);
	if (!participant_current)
		return false;

	logical.owner_instance = dispatch->key.owner_instance;
	logical.segment_id = dispatch->key.segment_id;
	return admission->formation_epoch == dispatch->key.formation_epoch
		   && admission->record_generation == dispatch->key.admission_record_generation
		   && cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
			   admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, logical.owner_instance,
			   logical.segment_id, &root)
		   && root.intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED
		   && root.root_id == dispatch->key.root_id
		   && root.root_generation == dispatch->key.root_generation
		   && dispatch->key.root_descriptor_incarnation == root.root_generation
		   && cluster_semantic_activation_recheck_r4_terminal_census(admission)
		   && cluster_ctrc_seal_request_encode(
			   &dispatch->key, dispatch->request_id, dispatch->grant_generation,
			   dispatch->seal_generation, dispatch->participant.capability_record_generation,
			   (ClusterCtrcSealSuboperation)dispatch->suboperation, request,
			   CLUSTER_CTRC_SEAL_REQUEST_BYTES);
}

bool
cluster_gcs_ctrc_dispatch_close(const ClusterCtrcCloseDispatch *dispatch)
{
	ClusterSemanticAdmissionToken admission;
	uint8 request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
	bool dispatched = false;

	if (gcs_ctrc_dispatch_prepare(dispatch, &admission, request)) {
		if (dispatch->participant.node_id == (uint16)cluster_node_id)
		{
			ClusterCtrcLocalReleaseAckV1 ack;
			ClusterCtrcSealReplyResult result;
			uint16 first_reason = CTRC_SEAL_REASON_IDENTITY;

			MemSet(&ack, 0, sizeof(ack));
			result = cluster_ctrc_participant_request_shared(
				&dispatch->key, &dispatch->participant,
				dispatch->grant_generation, dispatch->seal_generation,
				(ClusterCtrcSealSuboperation)dispatch->suboperation,
				&first_reason, &ack);
			if (cluster_semantic_activation_recheck_r4_terminal_census(
					&admission))
			{
				if (dispatch->suboperation
					== CTRC_SEAL_CERTIFICATE_COMMITTED)
					dispatched
						= cluster_ctrc_origin_note_certificate_reply_shared(
							dispatch->request_id,
							dispatch->participant.node_id, result);
				else if (result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
					dispatched = cluster_ctrc_origin_ack_land_shared(
						dispatch->request_id, &ack);
				else
					dispatched = cluster_ctrc_origin_note_close_reply_shared(
						dispatch->request_id,
						dispatch->participant.node_id, result);
			}
		}
		else
		{
			BufferTag route_tag
				= GcsBlockCurrentMxRouteTagMake(dispatch->request_id, dispatch->key.cluster_epoch,
												cluster_node_id, CLUSTER_CTRC_INTERNAL_ENDPOINT);
			int worker_id = cluster_lms_shard_for_tag(&route_tag, cluster_lms_workers);
			uint32 required_capabilities
				= PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1 | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;
			dispatched = worker_id >= 0
				&& worker_id < cluster_lms_workers
				&& cluster_lms_outbound_enqueue_cap_bound(
					worker_id, PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
					dispatch->participant.node_id, request,
					sizeof(request), required_capabilities,
					dispatch->participant.capability_record_generation);
		}
	}
	if (admission.entered)
		cluster_semantic_activation_leave(&admission);
	return dispatched;
}

static void
gcs_ctrc_dispatch_local_certificates(const ClusterCtrcCloseDispatch *dispatches, Size count)
{
	ClusterSemanticAdmissionToken *admissions;
	ClusterCtrcCloseDispatch accepted[CLUSTER_CTRC_RECLAIM_BATCH_MAX];
	ClusterCtrcSealReplyResult results[CLUSTER_CTRC_RECLAIM_BATCH_MAX];
	Size accepted_to_input[CLUSTER_CTRC_RECLAIM_BATCH_MAX];
	Size accepted_count = 0;

	/* Same physical slot twice must retain the original serial ordering. */
	for (Size i = 0; i < count; i++)
		for (Size j = 0; j < i; j++)
			if (dispatches[i].key.segment_id == dispatches[j].key.segment_id
				&& dispatches[i].key.slot_offset == dispatches[j].key.slot_offset) {
				for (Size k = 0; k < count; k++)
					(void)cluster_gcs_ctrc_dispatch_close(&dispatches[k]);
				return;
			}
	admissions = palloc_extended(count * sizeof(*admissions), MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
	if (admissions == NULL) {
		for (Size i = 0; i < count; i++)
			(void)cluster_gcs_ctrc_dispatch_close(&dispatches[i]);
		return;
	}
	/* Token state is in owned heap storage, not an indeterminate automatic
	 * object after longjmp. Even an ERROR in prepare is paired with leave. */
	PG_TRY();
	{
		for (Size i = 0; i < count; i++) {
			uint8 request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];

			if (!gcs_ctrc_dispatch_prepare(&dispatches[i], &admissions[i], request)) {
				if (admissions[i].entered)
					cluster_semantic_activation_leave(&admissions[i]);
				continue;
			}
			accepted[accepted_count] = dispatches[i];
			accepted_to_input[accepted_count++] = i;
		}
		/* Each token was rechecked in the original prepare. Recheck again at
		 * this batch's use boundary; do not borrow the first member's token. */
		for (Size i = 0; i < accepted_count;) {
			if (cluster_semantic_activation_recheck_r4_terminal_census(
					&admissions[accepted_to_input[i]])) {
				i++;
				continue;
			}
			cluster_semantic_activation_leave(&admissions[accepted_to_input[i]]);
			accepted_count--;
			memmove(&accepted[i], &accepted[i + 1], (accepted_count - i) * sizeof(accepted[0]));
			memmove(&accepted_to_input[i], &accepted_to_input[i + 1],
					(accepted_count - i) * sizeof(accepted_to_input[0]));
		}
		if (accepted_count != 0
			&& cluster_ctrc_participant_certificate_batch_shared(accepted, accepted_count, results)) {
			Size reply_count = 0;

			for (Size i = 0; i < accepted_count; i++)
				if (cluster_semantic_activation_recheck_r4_terminal_census(
						&admissions[accepted_to_input[i]])) {
					accepted[reply_count] = accepted[i];
					accepted_to_input[reply_count] = accepted_to_input[i];
					results[reply_count++] = results[i];
				}
			/* Every surviving token stays entered through the shared census.
			 * Unsupported input shape retains the original per-item consumer. */
			if (reply_count != 0
				&& !cluster_ctrc_origin_note_local_certificate_batch_shared(
					accepted, results, reply_count))
				for (Size i = 0; i < reply_count; i++)
					if (cluster_semantic_activation_recheck_r4_terminal_census(
							&admissions[accepted_to_input[i]]))
						(void)cluster_ctrc_origin_note_certificate_reply_shared(
							accepted[i].request_id, accepted[i].participant.node_id, results[i]);
		}
	}
	PG_FINALLY();
	{
		for (Size i = 0; i < count; i++)
			if (admissions[i].entered)
				cluster_semantic_activation_leave(&admissions[i]);
		pfree(admissions);
	}
	PG_END_TRY();
}

/* Different physical slots have independent participant state. Defer only
 * their origin replies to one census, never their per-item admission or ACK
 * checks. No reply crosses a remote request or a certificate notification. */
static void
gcs_ctrc_dispatch_local_closes(const ClusterCtrcCloseDispatch *dispatches, Size count)
{
	ClusterSemanticAdmissionToken *admissions;
	ClusterCtrcLocalReleaseAckV1 *acks;
	ClusterCtrcCloseDispatch accepted[CLUSTER_CTRC_RECLAIM_BATCH_MAX];
	ClusterCtrcSealReplyResult results[CLUSTER_CTRC_RECLAIM_BATCH_MAX];
	Size accepted_to_input[CLUSTER_CTRC_RECLAIM_BATCH_MAX];
	Size accepted_count = 0;

	for (Size i = 0; i < count; i++)
		for (Size j = 0; j < i; j++)
			if ((dispatches[i].key.segment_id == dispatches[j].key.segment_id
				 && dispatches[i].key.slot_offset == dispatches[j].key.slot_offset)
				|| dispatches[i].request_id == dispatches[j].request_id) {
				for (Size k = 0; k < count; k++)
					(void)cluster_gcs_ctrc_dispatch_close(&dispatches[k]);
				return;
			}
	admissions = palloc_extended(count * sizeof(*admissions), MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
	if (admissions == NULL) {
		for (Size i = 0; i < count; i++)
			(void)cluster_gcs_ctrc_dispatch_close(&dispatches[i]);
		return;
	}
	acks = palloc_extended(count * sizeof(*acks), MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
	if (acks == NULL) {
		pfree(admissions);
		for (Size i = 0; i < count; i++)
			(void)cluster_gcs_ctrc_dispatch_close(&dispatches[i]);
		return;
	}
	PG_TRY();
	{
		for (Size i = 0; i < count; i++) {
			uint8 request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
			uint16 first_reason = CTRC_SEAL_REASON_IDENTITY;

			if (!gcs_ctrc_dispatch_prepare(&dispatches[i], &admissions[i], request)
				|| !cluster_semantic_activation_recheck_r4_terminal_census(&admissions[i])) {
				if (admissions[i].entered)
					cluster_semantic_activation_leave(&admissions[i]);
				continue;
			}
			results[accepted_count] = cluster_ctrc_participant_request_shared(
				&dispatches[i].key, &dispatches[i].participant, dispatches[i].grant_generation,
				dispatches[i].seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &first_reason,
				&acks[accepted_count]);
			if (cluster_semantic_activation_recheck_r4_terminal_census(&admissions[i])) {
				accepted[accepted_count] = dispatches[i];
				accepted_to_input[accepted_count++] = i;
			}
		}
		{
			Size reply_count = 0;

			for (Size i = 0; i < accepted_count; i++)
				if (cluster_semantic_activation_recheck_r4_terminal_census(
						&admissions[accepted_to_input[i]])) {
					accepted[reply_count] = accepted[i];
					accepted_to_input[reply_count] = accepted_to_input[i];
					results[reply_count] = results[i];
					acks[reply_count++] = acks[i];
				}
			if (reply_count != 0
				&& !cluster_ctrc_origin_note_local_close_batch_shared(accepted, results, acks,
																	  reply_count))
				for (Size i = 0; i < reply_count; i++)
					if (cluster_semantic_activation_recheck_r4_terminal_census(
							&admissions[accepted_to_input[i]])) {
						/* Consume this existing reply; do not repeat participant work. */
						if (results[i] == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
							(void)cluster_ctrc_origin_ack_land_shared(accepted[i].request_id,
																	  &acks[i]);
						else
							(void)cluster_ctrc_origin_note_close_reply_shared(
								accepted[i].request_id, accepted[i].participant.node_id,
								results[i]);
					}
		}
	}
	PG_FINALLY();
	{
		for (Size i = 0; i < count; i++)
			if (admissions[i].entered)
				cluster_semantic_activation_leave(&admissions[i]);
		pfree(acks);
		pfree(admissions);
	}
	PG_END_TRY();
}

void
cluster_gcs_ctrc_dispatch_batch(const ClusterCtrcCloseDispatch *dispatches, Size count)
{
	if (dispatches == NULL || count > CLUSTER_CTRC_RECLAIM_BATCH_MAX)
		return;
	for (Size i = 0; i < count;) {
		Size end = i;
		uint8 suboperation = dispatches[i].suboperation;

		/* Maximal contiguous same-operation local subsequences, in old order. */
		while (end < count
			   && (suboperation == CTRC_SEAL_CERTIFICATE_COMMITTED
				   || suboperation == CTRC_SEAL_CLOSE_AND_CLEAN)
			   && dispatches[end].suboperation == suboperation && cluster_node_id >= 0
			   && dispatches[end].participant.node_id == (uint16)cluster_node_id)
			end++;
		if (end == i) {
			(void)cluster_gcs_ctrc_dispatch_close(&dispatches[i++]);
			continue;
		}
		if (suboperation == CTRC_SEAL_CERTIFICATE_COMMITTED)
			gcs_ctrc_dispatch_local_certificates(&dispatches[i], end - i);
		else
			gcs_ctrc_dispatch_local_closes(&dispatches[i], end - i);
		i = end;
	}
}

/* CTRC status 30 uses the legacy-sized outer carriage but has no backend
 * outstanding slot: requester_backend_id is the closed internal endpoint.
 * Reconstruct the exact staged CLOSE or certificate-notification request and
 * land it in the origin registry before any generic backend-id arithmetic. */
static bool
gcs_block_try_land_ctrc_reply(
	const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockReplyHeader *outer;
	const uint8 *page;
	ClusterCtrcLocalReleaseAckV1 decoded_ack;
	ClusterCtrcSealReplyHeaderV1 reply_header;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity identity;
	ClusterCtrcSealSuboperation suboperation = 0;
	uint8 request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
	uint32 grant_generation = 0;
	uint64 seal_generation = 0;
	bool reserved_zero = true;
	int i;

	if (env == NULL || payload == NULL
		|| env->payload_length < (uint32)sizeof(GcsBlockReplyHeader))
		return false;
	outer = (const GcsBlockReplyHeader *)payload;
	if (outer->status
		!= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_CTRC_SEAL_RESULT)
		return false;
	if (env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
		|| env->payload_length != GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE)
		return true;
	for (i = 0; i < (int)sizeof(outer->reserved_0); i++)
		if (outer->reserved_0[i] != 0)
		{
			reserved_zero = false;
			break;
		}
	page = (const uint8 *)payload + sizeof(*outer);
	if (!reserved_zero
		|| env->source_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| env->dest_node_id != (uint32)cluster_node_id
		|| outer->sender_node != (int32)env->source_node_id
		|| outer->requester_backend_id != CLUSTER_CTRC_INTERNAL_ENDPOINT
		|| outer->transition_id != 0 || outer->page_lsn != 0
		|| outer->epoch != cluster_epoch_get_current()
		|| GcsBlockReplyHeaderGetForwardingMasterNode(outer)
		   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| outer->checksum != gcs_block_compute_checksum((const char *)page))
		return true;

	MemSet(&key, 0, sizeof(key));
	MemSet(&identity, 0, sizeof(identity));
	MemSet(&decoded_ack, 0, sizeof(decoded_ack));
	MemSet(&reply_header, 0, sizeof(reply_header));
	if (!cluster_ctrc_origin_request_snapshot_shared(
			outer->request_id, (uint16)env->source_node_id,
			&key, &identity, &grant_generation, &seal_generation,
			&suboperation)
		|| key.origin_node_id != (uint16)cluster_node_id
		|| !cluster_ctrc_seal_request_encode(
			&key, outer->request_id, grant_generation, seal_generation,
			identity.capability_record_generation,
			suboperation, request, sizeof(request))
		|| !cluster_ctrc_seal_reply_decode(
			page, GCS_BLOCK_DATA_SIZE, request, sizeof(request),
			(int32)env->source_node_id, cluster_node_id,
			&reply_header, &decoded_ack)
		|| reply_header.request_id != outer->request_id
		|| reply_header.cluster_epoch != outer->epoch
		|| reply_header.source_node != outer->sender_node
		|| reply_header.destination_node != cluster_node_id)
		return true;

	if (suboperation == CTRC_SEAL_CERTIFICATE_COMMITTED)
		(void)cluster_ctrc_origin_note_certificate_reply_shared(
			outer->request_id, (uint16)env->source_node_id,
			(ClusterCtrcSealReplyResult)reply_header.result);
	else if (reply_header.result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
		(void)cluster_ctrc_origin_ack_land_shared(
			outer->request_id, &decoded_ack);
	else
		(void)cluster_ctrc_origin_note_close_reply_shared(
			outer->request_id, (uint16)env->source_node_id,
			(ClusterCtrcSealReplyResult)reply_header.result);
	return true;
}

/* Current-MX statuses 27/28 have the same outer 48+BLCKSZ carriage as legacy
 * replies, but a disjoint slot domain and typed Spec-3.6b bodies.  Consume
 * every current-MX candidate here so it can never alias the legacy decoder. */
static bool
gcs_block_try_land_current_mx_reply(
	const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockReplyHeader *hdr;
	const char *block_data;
	int backend_idx;
	ClusterGcsBlockBackendBlock *blk;
	int i;

	if (env == NULL || payload == NULL
		|| env->payload_length < (uint32)sizeof(GcsBlockReplyHeader))
		return false;
	hdr = (const GcsBlockReplyHeader *)payload;
	if (hdr->status
		!= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT
		&& hdr->status
			   != (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT)
		return false;
	if (env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
		|| env->payload_length != GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE)
		return true;
	backend_idx = hdr->requester_backend_id - 1;
	if (backend_idx < 0 || backend_idx >= MaxBackends
		|| gcs_block_backend_blocks == NULL)
		return true;
	blk = &gcs_block_backend_blocks[backend_idx];
	block_data = ((const char *)payload) + sizeof(*hdr);

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		ClusterGcsBlockOutstandingSlot *slot = &blk->slots[i];
		ClusterCurrentMxMemberDesc scratch_members
			[CLUSTER_CURRENT_MX_MAX_MEMBERS];
		ClusterCurrentMemberProof scratch_proofs
			[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
		ClusterCurrentUpdaterProof scratch_updater;
		uint16 scratch_count = 0;
		uint32 scratch_capability_generation = 0;
		uint32 scratch_total = 0;
		ClusterMxDescribeResult typed_result = CMX_DESC_UNKNOWN;
		ClusterMxResolveResult proof_result = CMX_RESOLVE_UNKNOWN;
		bool typed_valid = false;
		bool outer_valid = false;
		bool reserved_zero = true;
		int reserved_index;

		if (!slot->in_use || slot->request_id != hdr->request_id)
			continue;
		for (reserved_index = 0;
			 reserved_index < (int)sizeof(hdr->reserved_0);
			 reserved_index++)
			if (hdr->reserved_0[reserved_index] != 0) {
				reserved_zero = false;
				break;
			}
		outer_valid
			= slot->reply_domain == CLUSTER_GCS_BLOCK_REPLY_DOMAIN_CURRENT_MX
			&& !slot->reply_received
			&& slot->direct_state == GCS_BLOCK_DIRECT_UNARMED
			&& !slot->direct_target_prepared
			&& slot->expected_reply_status == hdr->status
			&& env->source_node_id == (uint32)hdr->sender_node
			&& env->dest_node_id == (uint32)cluster_node_id
			&& hdr->sender_node == slot->expected_master_node
			&& hdr->requester_backend_id == backend_idx + 1
			&& hdr->epoch == slot->request_epoch
			&& hdr->transition_id == 0 && hdr->page_lsn == 0
			&& reserved_zero
			&& GcsBlockReplyHeaderGetForwardingMasterNode(hdr)
				   == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
			&& hdr->checksum == gcs_block_compute_checksum(block_data);
		if (outer_valid) {
			if (hdr->status
					== (uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT
				&& slot->expected_current_mx_key_valid) {
				typed_result
					= cluster_multixact_current_wire_validate_describe_reply(
						block_data, GCS_BLOCK_DATA_SIZE,
						slot->expected_master_node, slot->request_epoch,
						slot->request_id, &slot->expected_current_mx_key,
						scratch_members, lengthof(scratch_members),
						&scratch_count, &scratch_total);
				typed_valid = typed_result != CMX_DESC_UNKNOWN
							  && typed_result != CMX_DESC_TIMEOUT;
			} else if (hdr->status
						   == (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT
					   && slot->expected_current_mx_proof_valid)
				typed_valid
					= cluster_multixact_current_wire_validate_proof_reply_frame(
						block_data, GCS_BLOCK_DATA_SIZE,
						slot->expected_master_node, slot->request_epoch,
						&slot->expected_current_mx_proof, &proof_result,
						scratch_proofs, lengthof(scratch_proofs),
						&scratch_count, &scratch_updater,
						&scratch_capability_generation);
		}
		if (!typed_valid) {
			const ClusterCurrentMxDescribeReplyPage *describe_page
				= (const ClusterCurrentMxDescribeReplyPage *)block_data;

			ereport(LOG,
					(errmsg("cluster_gcs_block: dropped stale current-MX reply"),
					 errdetail("request_id=" UINT64_FORMAT
							   " outer_valid=%d typed_result=%d domain=%u"
							   " reply_received=%d direct_state=%d direct_prepared=%d"
							   " expected_status=%u status=%u env_source=%u env_dest=%u"
							   " expected_source=%d sender=%d backend=%d expected_backend=%d"
							   " expected_epoch=" UINT64_FORMAT " epoch=" UINT64_FORMAT
							   " transition=%u page_lsn=" UINT64_FORMAT
							   " reserved_zero=%d forwarding_master=%d checksum_match=%d"
							   " body_magic=%u body_version=%u body_kind=%u body_result=%u"
							   " body_source=%u body_request=" UINT64_FORMAT
							   " body_origin=%u body_mxid=%u body_epoch=%u"
							   " expected_origin=%u expected_mxid=%u expected_key_epoch=%u"
							   " body_entries=%u body_total=%u body_wire_length=%u",
							 slot->request_id, outer_valid, (int)typed_result,
							 (unsigned int)slot->reply_domain, slot->reply_received,
							 (int)slot->direct_state, slot->direct_target_prepared,
							 (unsigned int)slot->expected_reply_status,
							 (unsigned int)hdr->status, env->source_node_id,
							 env->dest_node_id, slot->expected_master_node,
							 hdr->sender_node, hdr->requester_backend_id,
							 backend_idx + 1, slot->request_epoch, hdr->epoch,
							 (unsigned int)hdr->transition_id, hdr->page_lsn,
							 reserved_zero,
							 GcsBlockReplyHeaderGetForwardingMasterNode(hdr),
							 hdr->checksum
								 == gcs_block_compute_checksum(block_data),
							 describe_page->header.magic,
							 (unsigned int)describe_page->header.version,
							 (unsigned int)describe_page->header.kind,
							 (unsigned int)describe_page->header.result,
							 describe_page->header.source_node_id,
							 describe_page->header.request_id,
							 (unsigned int)describe_page->header.mxkey.origin_node_id,
							 describe_page->header.mxkey.multixact_id,
							 describe_page->header.mxkey.cluster_epoch,
							 (unsigned int)slot->expected_current_mx_key.origin_node_id,
							 slot->expected_current_mx_key.multixact_id,
							 slot->expected_current_mx_key.cluster_epoch,
							 (unsigned int)describe_page->header.entry_count,
							 describe_page->header.total_count,
							 (unsigned int)describe_page->header.wire_length)));
			pg_atomic_fetch_add_u64(
				&ClusterGcsBlock->stale_reply_drop_count, 1);
			LWLockRelease(&blk->lock.lock);
			return true;
		}

		slot->reply_header = *hdr;
		memcpy(slot->reply_block_data, block_data, GCS_BLOCK_DATA_SIZE);
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->reply_undo_trailer_valid = false;
		slot->reply_undo_tt_generation = 0;
		slot->reply_undo_authority_scn = 0;
		slot->reply_received = true;
		ConditionVariableSignal(&slot->reply_cv);
		LWLockRelease(&blk->lock.lock);
		return true;
	}
	LWLockRelease(&blk->lock.lock);
	return true;
}

/* D4/M4 R4 reply landing is selected by the closed outstanding-slot domain,
 * not by widening the legacy 8240-byte decoder.  Return false only when the
 * frame is not a status-21/22/25/26 candidate; a malformed R4 candidate is
 * consumed and dropped so it can never alias a legacy acquisition reply. */
static bool
gcs_block_try_land_r4_terminal_reply(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockReplyHeader *hdr;
	const char *block_data;
	GcsBlockR4ReplyExpectation expected;
	int backend_idx;
	ClusterGcsBlockBackendBlock *blk;
	int i;

	if (env == NULL || payload == NULL
		|| env->payload_length < (uint32)sizeof(GcsBlockReplyHeader))
		return false;
	hdr = (const GcsBlockReplyHeader *)payload;
	if (hdr->status != (uint8)GCS_BLOCK_REPLY_R4_CR_FULL
		&& hdr->status != (uint8)GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT
		&& hdr->status != (uint8)GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED
		&& hdr->status != (uint8)GCS_BLOCK_REPLY_R4_DENIED)
		return false;
	if (env->payload_length != GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE)
		return true;

	backend_idx = hdr->requester_backend_id - 1;
	if (backend_idx < 0 || backend_idx >= MaxBackends
		|| gcs_block_backend_blocks == NULL)
		return true;
	blk = &gcs_block_backend_blocks[backend_idx];

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		ClusterGcsBlockOutstandingSlot *slot = &blk->slots[i];

		if (!slot->in_use || slot->request_id != hdr->request_id)
			continue;
		if (slot->reply_domain != CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR
			|| slot->reply_received
			|| slot->direct_state == GCS_BLOCK_DIRECT_ARMED
			|| slot->direct_state == GCS_BLOCK_DIRECT_LANDED
			|| slot->direct_state == GCS_BLOCK_DIRECT_ABORTING) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
			LWLockRelease(&blk->lock.lock);
			return true;
		}

		memset(&expected, 0, sizeof(expected));
		expected.request_id = slot->request_id;
		expected.epoch = slot->request_epoch;
		expected.requester_backend_id = backend_idx + 1;
		expected.transition_id = slot->transition_id;
		if (hdr->status != (uint8)GCS_BLOCK_REPLY_R4_CR_FULL
			&& GcsBlockReplyHeaderGetForwardingMasterNode(hdr)
				   == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER) {
			expected.sender_node = slot->expected_master_node;
			expected.forwarding_master_node
				= GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
		} else {
			expected.sender_node = (int32)env->source_node_id;
			expected.forwarding_master_node = slot->expected_master_node;
		}
		expected.reply_domain = CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
		if (!gcs_block_decode_r4_reply_payload(env, payload, &expected)) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
			LWLockRelease(&blk->lock.lock);
			return true;
		}

		block_data = ((const char *)payload) + sizeof(*hdr);
		slot->reply_header = *hdr;
		memcpy(slot->reply_block_data, block_data, GCS_BLOCK_DATA_SIZE);
		slot->reply_sf_dep_valid = false;
		slot->reply_sf_flags = 0;
		cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
		slot->reply_undo_trailer_valid = false;
		slot->reply_undo_tt_generation = 0;
		slot->reply_undo_authority_scn = 0;
		slot->reply_received = true;
		ConditionVariableSignal(&slot->reply_cv);
		LWLockRelease(&blk->lock.lock);
		return true;
	}
	LWLockRelease(&blk->lock.lock);
	return true;
}

void
cluster_gcs_handle_block_reply_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockReplyHeader *hdr;
	const char *block_data;
	bool sf_dep_valid = false;
	uint8 sf_flags = 0;
	ClusterSfDepVec sf_dep_vec;
	const ClusterGcsUndoAuthTrailer *undo_trailer = NULL;
	int backend_idx;
	ClusterGcsBlockBackendBlock *blk;
	int i;

	if (gcs_block_try_resource_x_frame(env, payload))
		return;
	if (gcs_block_try_land_ctrc_reply(env, payload))
		return;
	if (gcs_block_try_land_current_mx_reply(env, payload))
		return;
	if (gcs_block_try_land_r4_terminal_reply(env, payload))
		return;
	cluster_sf_dep_vec_reset(&sf_dep_vec);
	if (!gcs_block_decode_reply_payload(env, payload, &hdr, &block_data, &sf_dep_valid, &sf_flags,
										&sf_dep_vec, &undo_trailer))
		return;
	/* Bind the payload's claimed sender to the authenticated DATA envelope.
	 * The canonical PCM-X image-id path is direct holder -> requester, so
	 * accepting a forged header source here would bypass its holder proof. */
	if (env->source_node_id != (uint32)hdr->sender_node
		|| env->dest_node_id != (uint32)cluster_node_id)
		return;

	/* D4 status 24 is a worker-0 internal response, never a backend-table
	 * reply.  The decoder has already established its exact authenticated
	 * 8256-byte shape; route it to the foreign-slot owner before deriving any
	 * backend index so it cannot alias a legacy acquisition slot. */
	if (hdr->status == (uint8)GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT) {
		if (hdr->requester_backend_id != CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT)
			return;
		(void)cluster_cr_server_r4_land_foreign_undo(
			env, hdr, block_data, undo_trailer);
		return;
	}

	/* HC80: direct index by requester_backend_id (1..MaxBackends → 0..MaxBackends-1). */
	backend_idx = hdr->requester_backend_id - 1;
	if (backend_idx < 0 || backend_idx >= MaxBackends)
		return; /* malformed key; drop */

	blk = &gcs_block_backend_blocks[backend_idx];

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		ClusterGcsBlockOutstandingSlot *slot = &blk->slots[i];

		if (slot->in_use && slot->request_id == hdr->request_id) {
			int32 fwd_master;
			bool authorized = false;
			uint8 reply_domain = GcsBlockReplyStatusIsR4((GcsBlockReplyStatus)hdr->status)
								 ? CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR
								 : CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;

			/* Reply identity includes the closed slot domain.  Reject before
			 * touching reply_received or any other slot-owned result byte. */
			if (slot->reply_domain != reply_domain) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
				LWLockRelease(&blk->lock.lock);
				return;
			}

			/*
			 * First-reply-wins (S3 RC-A).  Duplicate replies for the same
			 * armed attempt are protocol-normal (dedup CACHED_REPLY resend
			 * after a requester retransmit, FORWARDED re-forward), but the
			 * requester consumes reply_header/reply_block_data WITHOUT this
			 * lock once it has observed reply_received under it — the slot
			 * reply fields are immutable from reply_received=true until the
			 * owner rearms the slot.  Overwriting here mid-consume tears the
			 * 8KB image under the CRC32C verify and surfaces a false
			 * DENIED_CHECKSUM_FAIL (the S3 loopback "CRC verify reject").
			 */
			if (slot->reply_received) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
				LWLockRelease(&blk->lock.lock);
				return;
			}

			if (slot->direct_state == GCS_BLOCK_DIRECT_ARMED
				|| slot->direct_state == GCS_BLOCK_DIRECT_LANDED
				|| slot->direct_state == GCS_BLOCK_DIRECT_ABORTING) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
				LWLockRelease(&blk->lock.lock);
				return;
			}

			/*
			 * PGRAC: spec-2.34 HC100 — stale-reply defense (epoch +
			 *   transition_id checks remain).
			 * PGRAC: spec-2.35 HC108 — authorized holder source chain.
			 *
			 *	Two reply path classes:
			 *	  (a) direct-from-master:
			 *	      hdr.sender_node == slot.expected_master_node
			 *	      AND hdr.forwarding_master_node ==
			 *	          GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
			 *	      AND hdr.status != GRANTED_FROM_HOLDER
			 *	  (b) forwarded-by-master-to-holder:
			 *	      hdr.forwarding_master_node == slot.expected_master_node
			 *	      AND hdr.status in {GRANTED_FROM_HOLDER,
			 *	                         DENIED_MASTER_NOT_HOLDER,
			 *	                         DENIED_PENDING_X}
			 *	      (HC105 evict race must be accepted; otherwise the
			 *	      sender's spec-2.34 retransmit budget cannot
			 *	      recover from holder eviction during forward.  A
			 *	      conditional holder copy miss uses DENIED_PENDING_X
			 *	      to reach the requester's existing fresh-identity
			 *	      retry boundary.)
			 *
			 *	Mismatch ⇒ drop (stale_reply_drop_count++).  Reply
			 *	identity is fully decided before slot mutation per
			 *	spec-2.34 P0 race-A fix discipline.
			 */
			fwd_master = GcsBlockReplyHeaderGetForwardingMasterNode(hdr);

			if (fwd_master == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER) {
				/* direct-from-master path — a master never self-claims a
				 * holder-shipped status (spec-6.12a ㉕ status included). */
				if (hdr->sender_node == slot->expected_master_node
					&& hdr->status != (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
					&& hdr->status != (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
					&& hdr->status != (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE)
					authorized = true;
			} else {
				/* forwarded-by-master path */
				if (fwd_master == slot->expected_master_node
					&& (hdr->status == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_FULL
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
						|| hdr->status == (uint8)GCS_BLOCK_REPLY_DENIED_LOST_WRITE))
					authorized = true;
			}

			if (!authorized || hdr->epoch < slot->request_epoch
				|| hdr->transition_id != slot->transition_id) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
				LWLockRelease(&blk->lock.lock);
				return;
			}
			slot->reply_header = *hdr;
			memcpy(slot->reply_block_data, block_data, GCS_BLOCK_DATA_SIZE);
			slot->reply_sf_dep_valid = sf_dep_valid;
			slot->reply_sf_flags = sf_flags;
			slot->reply_sf_dep_vec = sf_dep_vec;
			slot->reply_undo_trailer_valid = (undo_trailer != NULL);
			slot->reply_undo_tt_generation
				= (undo_trailer != NULL) ? ClusterGcsUndoAuthTrailerGetTtGeneration(undo_trailer)
										 : 0;
			slot->reply_undo_authority_scn
				= (undo_trailer != NULL) ? ClusterGcsUndoAuthTrailerGetAuthorityScn(undo_trailer)
										 : 0;
			slot->reply_received = true;
			ConditionVariableSignal(&slot->reply_cv);
			LWLockRelease(&blk->lock.lock);
			return;
		}
	}
	LWLockRelease(&blk->lock.lock);
	/* No matching slot — stale/late reply; drop silently (HC74 semantics). */
}


/* ============================================================
 * Receiver: holder-side (D7;  spec-2.35).
 *
 *	HC103: holder receives GCS_BLOCK_FORWARD from master.  Copies the
 *	page bytes via spec-2.33 D4 bufmgr helper, builds reply with
 *	`forwarding_master_node = forward.master_node` (HC109), sends direct
 *	to `original_requester_node` with status GRANTED_FROM_HOLDER (HC104).
 *	If evict race causes bufmgr copy to fail, reply DENIED_MASTER_NOT_
 *	HOLDER (HC105) so sender's spec-2.34 retransmit budget covers
 *	recovery.
 * ============================================================ */

/*
 * PGRAC: spec-6.12b/6.12i — immediate fail-closed DENIED reply for a forward
 * request this node refused to park (data plane off / malformed synthetic
 * payload / no free LMS slot).  The requester keeps its unchanged 53R9G /
 * 53R97 refusal (Rule 8.A).  Shared by the CR, undo-fetch and undo-verdict
 * branches of the forward handler.
 */
static void
gcs_block_forward_reply_immediate_deny(const GcsBlockForwardPayload *fwd)
{
	uint32 deny_total = (uint32)sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE;
	char *deny_buf = (char *)palloc0(deny_total);
	GcsBlockReplyHeader *deny_hdr = (GcsBlockReplyHeader *)deny_buf;

	GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(fwd, "holder-immediate-deny");
	deny_hdr->request_id = fwd->request_id;
	deny_hdr->epoch = cluster_epoch_get_current();
	deny_hdr->sender_node = cluster_node_id;
	deny_hdr->requester_backend_id = fwd->requester_backend_id;
	deny_hdr->transition_id = fwd->transition_id;
	deny_hdr->status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(deny_hdr, fwd->master_node);
	deny_hdr->checksum = cluster_gcs_block_compute_checksum(deny_buf + sizeof(GcsBlockReplyHeader));
	cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY,
										cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY,
																 fwd->original_requester_node,
																 deny_buf, deny_total));
	pfree(deny_buf);
}

/* The 136-byte CTRC family is disjoint from every BufferTag forward.  It is
 * consumed here even when malformed, so no byte collision can reach legacy
 * holder logic.  A positive ACK is built only while the exact target R4
 * formation/root and both endpoint identities remain current. */
static bool
gcs_block_try_ctrc_forward136(
	const ClusterICEnvelope *env, const void *payload)
{
	ClusterCtrcSealRequestV1 request;
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity identity;
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	BufferTag route_tag;
	uint64 source_incarnation = 0;
	uint64 source_observed_generation = 0;
	uint64 self_incarnation;
	uint32 source_capability_generation = 0;
	uint32 required_capabilities
		= PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
		  | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;
	uint16 first_reason = CTRC_SEAL_REASON_IDENTITY;
	ClusterCtrcSealReplyResult result = CTRC_SEAL_REPLY_BLOCKED_RETAIN;
	int expected_worker;
	uint32 reply_total
		= (uint32)sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE;
	char *reply;
	GcsBlockReplyHeader *outer;
	uint8 *page;
	ClusterICSendResult send_result;
	bool source_capability_current;
	bool bracket_current = false;

	if (env == NULL || payload == NULL
		|| env->payload_length != CLUSTER_CTRC_SEAL_REQUEST_BYTES)
		return false;
	MemSet(&request, 0, sizeof(request));
	MemSet(&key, 0, sizeof(key));
	MemSet(&identity, 0, sizeof(identity));
	MemSet(&ack, 0, sizeof(ack));
	MemSet(&admission, 0, sizeof(admission));
	MemSet(&logical, 0, sizeof(logical));
	MemSet(&root, 0, sizeof(root));
	if (env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		|| env->dest_node_id != (uint32)cluster_node_id
		|| env->epoch != cluster_epoch_get_current()
		|| !cluster_gcs_block_family_on_data_plane()
		|| !cluster_ctrc_seal_request_decode(
			(const uint8 *)payload, env->payload_length,
			GetSystemIdentifier(), (int32)env->source_node_id,
			cluster_node_id, cluster_epoch_get_current(), &request, &key))
		return true;

	route_tag = GcsBlockCurrentMxRouteTagMake(
		request.request_id, request.cluster_epoch,
		request.original_requester_node, request.requester_backend_id);
	expected_worker = cluster_lms_shard_for_tag(&route_tag,
		cluster_lms_workers);
	Assert(expected_worker == cluster_ic_tier1_my_data_channel());
	if (expected_worker != cluster_ic_tier1_my_data_channel())
		return true;

	self_incarnation = cluster_qvotec_get_self_incarnation();
	if ((int32)env->source_node_id == cluster_node_id)
	{
		source_incarnation = self_incarnation;
		source_observed_generation = self_incarnation;
		source_capability_current
			= (cluster_ic_local_capability_word() & required_capabilities)
			  == required_capabilities;
	}
	else
	{
		source_capability_current
			= cluster_sf_peer_multixact_current_capability_generation(
				(int32)env->source_node_id, &source_capability_generation)
			  && source_capability_generation != 0
			  && cluster_sf_peer_capability_generation_matches(
				(int32)env->source_node_id, required_capabilities,
				source_capability_generation);
		(void)cluster_reconfig_get_observed_slot(
			(int32)env->source_node_id, &source_incarnation,
			&source_observed_generation);
	}
	if (source_capability_current && source_incarnation != 0
		&& source_observed_generation != 0 && self_incarnation != 0
		&& cluster_membership_is_member((int32)env->source_node_id)
		&& cluster_membership_is_member(cluster_node_id)
		&& cluster_membership_get_last_admitted_incarnation(
			(int32)env->source_node_id) == source_incarnation
		&& cluster_membership_get_last_admitted_incarnation(cluster_node_id)
		   == self_incarnation
		&& source_incarnation == key.origin_boot_incarnation
		&& cluster_reconfig_get_observed_epoch((int32)env->source_node_id)
		   == key.formation_epoch
		&& (!cluster_write_fence_enforcing()
			|| cluster_write_fence_allowed())
		&& cluster_semantic_activation_enter_r4_terminal_census(&admission)
		   == CLUSTER_SEMANTIC_ADMISSION_OK)
	{
		logical.owner_instance = key.owner_instance;
		logical.segment_id = key.segment_id;
		bracket_current
			= admission.formation_epoch == key.formation_epoch
			  && admission.record_generation
				 == key.admission_record_generation
			  && cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &root)
			  && root.intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED
			  && root.root_id == key.root_id
			  && root.root_generation == key.root_generation
			  && key.root_descriptor_incarnation == root.root_generation
			  && cluster_semantic_activation_recheck_r4_terminal_census(
				&admission);
		if (bracket_current)
		{
			identity.node_id = (uint16)cluster_node_id;
			identity.capability_record_generation
				= request.participant_capability_record_generation;
			identity.boot_incarnation = self_incarnation;
			identity.formation_epoch = admission.formation_epoch;
			identity.admission_record_generation
				= admission.record_generation;
			result = cluster_ctrc_participant_request_shared(
				&key, &identity, request.grant_generation,
				request.seal_generation,
				(ClusterCtrcSealSuboperation)request.suboperation,
				&first_reason, &ack);
			if (result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK
				&& !cluster_semantic_activation_recheck_r4_terminal_census(
					&admission))
			{
				MemSet(&ack, 0, sizeof(ack));
				first_reason = CTRC_SEAL_REASON_IDENTITY;
				result = CTRC_SEAL_REPLY_BLOCKED_RETAIN;
			}
		}
		cluster_semantic_activation_leave(&admission);
	}

	reply = (char *)palloc0(reply_total);
	outer = (GcsBlockReplyHeader *)reply;
	page = (uint8 *)(reply + sizeof(*outer));
	if (!cluster_ctrc_seal_reply_encode(
			(const uint8 *)payload, env->payload_length, cluster_node_id,
			(int32)env->source_node_id, result, first_reason,
			result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK ? &ack : NULL,
			page, GCS_BLOCK_DATA_SIZE))
	{
		pfree(reply);
		return true;
	}
	outer->request_id = request.request_id;
	outer->epoch = request.cluster_epoch;
	outer->sender_node = cluster_node_id;
	outer->requester_backend_id = CLUSTER_CTRC_INTERNAL_ENDPOINT;
	outer->transition_id = 0;
	outer->status
		= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_CTRC_SEAL_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		outer, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	outer->checksum = gcs_block_compute_checksum((const char *)page);

	if ((int32)env->source_node_id == cluster_node_id)
		send_result = gcs_block_send_envelope_or_loopback(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, cluster_node_id, reply, reply_total);
	else if (cluster_sf_peer_capability_generation_matches(
			(int32)env->source_node_id, required_capabilities,
			source_capability_generation))
		send_result = cluster_ic_send_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, (int32)env->source_node_id,
			reply, reply_total);
	else
		send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	cluster_gcs_block_note_send_outcome(
		GCS_BLOCK_SEND_FAMILY_REPLY, send_result);
	pfree(reply);
	return true;
}

/* Exact 128-byte Current-MX demultiplexing precedes every legacy BufferTag
 * interpretation.  Kind 8 remains reserved/dormant and is consumed here. */
static bool
gcs_block_try_current_mx_forward128(
	const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockForwardPayload *routing;

	if (env == NULL || payload == NULL
		|| env->payload_length != CLUSTER_CURRENT_MX_DESCRIBE_FORWARD_SIZE)
		return false;
	routing = (const GcsBlockForwardPayload *)payload;
	if (GcsBlockForwardPayloadIsCurrentMxDescribe(routing))
		cluster_gcs_current_mx_describe_serve_inline(env, payload);
	else if (GcsBlockForwardPayloadIsCurrentMxMemberProof(routing)) {
		if (!gcs_block_current_mx_origin_try_accept(env, payload))
			cluster_gcs_current_mx_member_proof_serve_inline(env, payload);
	}
	/* Stats is intentionally dormant under the approved migration scope. */
	return true;
}

#ifdef USE_CLUSTER_UNIT
bool
cluster_gcs_block_test_current_mx_forward128(
	const ClusterICEnvelope *env, const void *payload)
{
	return gcs_block_try_current_mx_forward128(env, payload);
}
#endif

void
cluster_gcs_handle_block_forward_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockForwardPayload *fwd;
	char block_buf[GCS_BLOCK_DATA_SIZE];
	const char *block_payload = NULL;
	uint32 block_payload_lkey = 0;
	ClusterICSgeReleaseCallback block_payload_release_cb = NULL;
	void *block_payload_release_arg = NULL;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	bool holder_ship_ok;
	ClusterBufmgrGcsCopyRefusal copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	bool holder_evicted_injected = false;
	ClusterBufmgrGcsDowngradeOutcome remote_downgrade_outcome
		= CLUSTER_BUFMGR_GCS_DOWNGRADE_REFUSED_PRE_NOTIFY;
	bool remote_downgraded = false; /* spec-6.12a ㉕ — holder accepted the
									 * master's downgrade request */
	ClusterSfDepVec sf_dep_vec;
	bool sf_dep_valid = false;
	bool sf_peer_v2 = false;
	bool send_sf_dep = false;
	uint32 header_len;
	uint32 total;
	char *buf;
	GcsBlockReplyHeader *hdr;
	/* PGRAC: spec-5.59 D3 — holder-forward read-image ship scope (started
	 * only by the read-image branch below; inactive otherwise). */
	ClusterXpScope xp_fwd_ship = { .active = false };

	if (gcs_block_try_ctrc_forward136(env, payload))
		return;
	if (gcs_block_try_current_mx_forward128(env, payload))
		return;
	if (gcs_block_try_r4_forward96(env, payload))
		return;
	if (env == NULL || payload == NULL || env->payload_length != sizeof(GcsBlockForwardPayload))
		return;

	cluster_sf_dep_vec_reset(&sf_dep_vec);
	fwd = (const GcsBlockForwardPayload *)payload;
	if (GcsBlockForwardPayloadIsCurrentMxRuntime(fwd)
		|| fwd->reserved_0[6] == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS)
		return;
	if (GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(fwd)) {
		/* Kind 10 is usable only on the authenticated direct requester->origin
		 * leg with a byte-canonical tuple.  Never let a malformed pair fall
		 * through to ordinary BufferTag/holder interpretation. */
		if (env->dest_node_id != (uint32)cluster_node_id
			|| env->source_node_id != (uint32)fwd->original_requester_node)
			return;
		if (!cluster_cr_server_freshref_c1b_pair_request_decode(
				fwd, (int32)env->source_node_id, cluster_node_id,
				cluster_epoch_get_current(), MaxBackends, NULL, NULL, NULL, NULL)) {
			gcs_block_forward_reply_immediate_deny(fwd);
			return;
		}
	}
	sf_peer_v2
		= cluster_smart_fusion && cluster_sf_peer_supports_reply_v2(fwd->original_requester_node);
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_received_count, 1);

	/* HC75 transition_id range guard (same as request handler). */
	if (fwd->transition_id < PCM_TRANS_N_TO_S || fwd->transition_id > PCM_TRANS_S_TO_X_CLEANOUT)
		return; /* malformed, silently drop;  master
								 * dedup TTL will sweep stale entry */

	/*
	 * PGRAC: spec-6.12b + spec-7.3 D6 — CR-server request.  When the family is
	 * on the DATA plane this handler runs in the receiving worker[shard], so it
	 * serves the request INLINE (construct under the PG_TRY -> DENIED envelope)
	 * and ships on its own channel — the park -> LMS-poll -> LMON-ship
	 * indirection is retired there (D6).  On the CONTROL plane the handler runs
	 * in LMON, whose tight IC dispatch loop must NOT walk undo I/O (the
	 * light-work rule), so it parks for LMS worker 0 instead; a refused park
	 * (data plane off / no free slot) replies a fail-closed DENIED immediately.
	 * Either way the requester keeps its unchanged 53R9G on refusal (Rule 8.A).
	 * Never falls through to the current-image ship below: a CR result is
	 * HISTORICAL by intent and the lost-write watermark verdict does not apply
	 * (the SCN carrier holds the requester's read_scn on this path).
	 */
	if (GcsBlockForwardPayloadIsCrRequest(fwd)) {
		if (cluster_gcs_block_family_on_data_plane())
			cluster_gcs_block_forward_serve_inline(fwd, CLUSTER_LMS_SLOT_KIND_CR);
		else if (!cluster_lms_cr_submit(fwd))
			gcs_block_forward_reply_immediate_deny(fwd);
		return;
	}

	/*
	 * PGRAC: spec-6.12i D-i1 + spec-7.3 D6 — undo-TT fetch request.  DATA plane
	 * serves inline (the undo file read runs in the worker[shard]); CONTROL
	 * plane parks for LMS worker 0 (light-work rule).  MUST branch here, before
	 * any holder / GRD logic: the tag is a synthetic undo address, not a block
	 * identity.  A refusal (wave GUC off / malformed tag / no free slot) ships
	 * DENIED so the requester keeps its unchanged 53R97 (Rule 8.A).
	 */
	if (GcsBlockForwardPayloadIsUndoTtFetchRequest(fwd)) {
		if (cluster_gcs_block_family_on_data_plane())
			cluster_gcs_block_forward_serve_inline(fwd, CLUSTER_LMS_SLOT_KIND_UNDO_FETCH);
		else if (!cluster_lms_undo_fetch_submit(fwd))
			gcs_block_forward_reply_immediate_deny(fwd);
		return;
	}

	/*
	 * PGRAC: spec-6.12i D-i4 / spec-6.15 D4 + spec-7.3 D6 — undo-verdict
	 * request; spec-5.22d D4-6 adds the kind-4 dead-owner AUTHORITY verdict
	 * on the same wire shape (the inline/park carrier decode recognizes the
	 * owner carrier and cr_serve_slot routes it to the block0 authority
	 * prove instead of the own-TT scan).  DATA plane serves inline (the
	 * complete durable-TT scan + CLOG cross-check runs in the worker[shard]);
	 * CONTROL plane parks for LMS worker 0 (light-work rule).  MUST branch
	 * here, before any holder / GRD logic: the tag is a synthetic undo
	 * address and the SCN carrier holds the widened xid.  A refusal (wave
	 * GUC off / malformed tag or carrier / bad owner carrier / no free slot)
	 * ships DENIED so the requester keeps its unchanged 53R97 (Rule 8.A).
	 */
	if (GcsBlockForwardPayloadIsUndoVerdictRequest(fwd)
		|| GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(fwd)) {
		if (cluster_gcs_block_family_on_data_plane())
			cluster_gcs_block_forward_serve_inline(fwd, CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT);
		else if (!cluster_lms_undo_verdict_submit(fwd))
			gcs_block_forward_reply_immediate_deny(fwd);
		return;
	}

	/*
	 * PGRAC: spec-7.1 D3-b — undo-MULTI-verdict request.  Same LMON shape as
	 * the single verdict branch above (validate + park; the member enumeration
	 * + per-updater terminal scan runs in LMS, the LMON tick ships).  MUST
	 * branch here, before any holder / GRD logic: the tag is a synthetic undo
	 * address and the SCN carrier holds the widened MXID.  A refused park
	 * (wave GUC off / malformed tag or carrier / no capacity) replies the
	 * fail-closed DENIED immediately so the requester keeps its unchanged
	 * 53R97 refusal (Rule 8.A).
	 */
	if (GcsBlockForwardPayloadIsUndoMultiVerdictRequest(fwd)) {
		if (!cluster_lms_undo_multi_verdict_submit(fwd))
			gcs_block_forward_reply_immediate_deny(fwd);
		return;
	}

	/*
	 * PGRAC: spec-6.12e2 (㉔) — BAST nudge.  The master denied a peer's X
	 * request because WE hold this block in X; it asks us to TRY the
	 * quiescent X->S self-downgrade right away (Oracle BAST -> holder LMS
	 * background yield; the foreground session is never interrupted).
	 * Fire-and-forget: NO reply of any kind — the requester already got
	 * its bounded DENIED and retries.  The downgrade helper alone judges
	 * quiescence (active ITL / pinned / raced / flush unavailable all
	 * refuse), flushes WAL-before-share, flips our pcm_state X->S and
	 * nowait-notifies the master; any refusal leaves today's deny-retry
	 * (e1 release-side) path untouched (§3.4b: never force the holder).
	 * MUST branch before the ship-image copy below: a nudge ships nothing.
	 */
	if (GcsBlockForwardPayloadIsBastNudge(fwd)) {
		bool yielded = false;

		CLUSTER_INJECTION_POINT("cluster-gcs-block-bast-nudge");
		if (cluster_ges_bast && !cluster_injection_should_skip("cluster-gcs-block-bast-nudge")) {
			yielded = cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(fwd->tag, fwd->master_node);

			/*
			 * PGRAC: GCS-race round-4c P1 — yield-notify liveness self-heal.
			 * A nudge for a block we ALREADY hold in S means our earlier
			 * yield's fire-and-forget notify was lost (the master still
			 * records X@us, or it would have served the S state instead of
			 * nudging).  Re-send the idempotent downgrade notify so the
			 * master converges; a master that already knows rejects the
			 * duplicate transition and nothing changes.  Counted as a
			 * refusal (no fresh yield happened) — the requester's bounded
			 * deny-retry picks up the healed state on its next attempt.
			 */
			if (!yielded && cluster_bufmgr_renotify_s_for_gcs(fwd->tag, fwd->master_node))
				ereport(DEBUG1, (errmsg("cluster_gcs_block: re-sent lost X->S yield notify for tag "
										"spc=%u db=%u rel=%u block=%u to master node %d",
										fwd->tag.spcOid, fwd->tag.dbOid, fwd->tag.relNumber,
										fwd->tag.blockNum, (int)fwd->master_node)));
		}
		cluster_lever_e2_note_nudge_result(yielded);
		return;
	}

	/* Decide the holder-eviction fault before any irreversible downgrade.
	 * A forced DENIED must never first publish X->S and then discard the only
	 * image prepared for the requester. */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-evict-holder-before-ship");
	holder_evicted_injected
		= cluster_injection_should_skip("cluster-gcs-block-evict-holder-before-ship");

	/*
	 * PGRAC: spec-6.12a ㉕ — remote-holder downgrade.  The master asked us
	 * (the X holder) to TRY the quiescent X->S self-downgrade before
	 * shipping.  This MUST run before the ship-image copy below so a
	 * successful downgrade ships the post-flush storage-consistent S page —
	 * copying first would open a copy-vs-downgrade window where a local
	 * write lands between the two and the requester caches a stale image
	 * (Rule 8.A).  Refusal of any kind (wave GUC off on this node, active
	 * ITL, state raced, flush unavailable, master notify send failure,
	 * injection) falls back to today's one-shot read-image ship — the flag
	 * is a request, never a command.
	 */
	if (cluster_read_scache && GcsBlockForwardPayloadIsReadImage(fwd)
		&& GcsBlockForwardPayloadIsDowngradeRequest(fwd)
		&& fwd->transition_id == PCM_TRANS_N_TO_S) {
		CLUSTER_INJECTION_POINT("cluster-gcs-block-remote-downgrade");
		if (!holder_evicted_injected
			&& !cluster_injection_should_skip("cluster-gcs-block-remote-downgrade"))
			remote_downgrade_outcome = cluster_bufmgr_downgrade_x_to_s_remote_for_gcs_prepare_image(
				fwd->tag, fwd->master_node, &page_lsn, block_buf, &copy_refusal);
		remote_downgraded = remote_downgrade_outcome == CLUSTER_BUFMGR_GCS_DOWNGRADE_COMMITTED;
		cluster_lever_a_note_remote_downgrade(remote_downgraded);
	}
	if (remote_downgrade_outcome == CLUSTER_BUFMGR_GCS_DOWNGRADE_FAILCLOSED_POST_NOTIFY)
		return;

	/* spec-2.35 D15 — SKIP simulates the evict race. */
	if (holder_evicted_injected) {
		copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INJECTED_EVICT;
		holder_ship_ok = false;
	} else if (remote_downgraded) {
		/* The exact downgrade prepared these bytes before notify+commit while
		 * holding the same content EXCLUSIVE lock.  A second conditional copy
		 * would reopen the grant->ship race P0-32 closes. */
		holder_ship_ok = remote_downgraded;
		block_payload = block_buf;
	} else
		holder_ship_ok = gcs_block_get_ship_image(
			fwd->tag, fwd->original_requester_node, true, &page_lsn, block_buf, &block_payload,
			&block_payload_lkey, &block_payload_release_cb, &block_payload_release_arg, &sf_dep_vec,
			&sf_dep_valid, &copy_refusal);

	/* Build reply (header + 8KB block or zero pad) and direct-ship to
	 * the original requester.  HC109 stores fwd->master_node in the
	 * reply's forwarding_master_node_bytes so sender's HC108 authorized
	 * chain validates the chain master→holder→sender. */
	header_len = (uint32)sizeof(GcsBlockReplyHeader);
	total = header_len + GCS_BLOCK_DATA_SIZE;
	buf = (char *)palloc0((uint32)sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE);
	hdr = (GcsBlockReplyHeader *)buf;
	hdr->request_id = fwd->request_id;
	hdr->page_lsn = (uint64)page_lsn;
	hdr->epoch = cluster_epoch_get_current();
	hdr->sender_node = cluster_node_id; /* holder is the reply origin */
	hdr->requester_backend_id = fwd->requester_backend_id;
	hdr->transition_id = fwd->transition_id;
	GcsBlockReplyHeaderSetForwardingMasterNode(hdr, fwd->master_node);

	if (holder_ship_ok) {
		/* PGRAC: spec-2.41 D1 — holder-forward path validates lost-write via SCN.
		 * The master stamped expected_pi_watermark_scn into the forward payload
		 * (@49); after reading the stable block bytes, compare the page's
		 * pd_block_scn against it through gcs_block_lost_write_verdict() (§2.6).
		 * STALE / ANOMALY → reply DENIED_LOST_WRITE so the sender ereport(53R93).
		 * page_lsn is no longer the detector quantity (per-node WAL position is
		 * not cross-node comparable; §0). */
		SCN expected_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		SCN shipped_scn = ((PageHeader)block_payload)->pd_block_scn;
		GcsLostWriteVerdict verdict;

		/* spec-2.41 D5/P1-C inject — force the SHIPPED pd_block_scn to InvalidScn
		 * to simulate a tracked-but-unstamped (anomaly) source.  This is the
		 * REACHABLE detector twin of the master-direct inject (:2559): in a real
		 * 2-node cluster every master-side ship of a held block bypasses the
		 * master-direct detector (spec-5.2/5.2a self-ship + read-image goto), so a
		 * cross-node transfer is validated HERE on the holder-forward path.  With a
		 * valid master pi_watermark_scn carried in the forward payload, forcing the
		 * shipped page InvalidScn drives the fail-closed ANOMALY path → the original
		 * requester ereport(53R93).  One-shot (should_skip consumes). */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-stale-ship");
		if (cluster_injection_should_skip("cluster-gcs-block-stale-ship"))
			shipped_scn = InvalidScn;

		verdict = gcs_block_lost_write_verdict(expected_scn, shipped_scn);

		if (verdict == GCS_LOST_WRITE_FAIL_STALE || verdict == GCS_LOST_WRITE_FAIL_ANOMALY) {
			/* S3 forensics step 1 — the (expected, shipped) verdict SCN pair is
			 * only known on this holder; LOG it so the original requester's
			 * 53R93 errdetail correlates by (tag, request_id).  The holder's
			 * LOCAL watermark view (usually behind the master's authoritative
			 * one carried in the forward payload) separates a genuinely stale
			 * holder copy from a master-side watermark false-positive. */
			ereport(
				LOG,
				(errmsg_internal(
					"cluster_gcs_block: lost-write verdict %s on holder-forward ship: tag "
					"spc=%u db=%u rel=%u block=%u fork=%d expected pi_watermark_scn=" UINT64_FORMAT
					" shipped pd_block_scn=" UINT64_FORMAT
					" holder-local pi_watermark_scn=" UINT64_FORMAT
					" requester=%d master=%d request_id=" UINT64_FORMAT " epoch=" UINT64_FORMAT,
					verdict == GCS_LOST_WRITE_FAIL_STALE ? "STALE" : "ANOMALY", fwd->tag.spcOid,
					fwd->tag.dbOid, fwd->tag.relNumber, fwd->tag.blockNum, (int)fwd->tag.forkNum,
					(uint64)expected_scn, (uint64)shipped_scn,
					(uint64)cluster_pcm_lock_pi_watermark_scn_query(fwd->tag),
					fwd->original_requester_node, fwd->master_node, fwd->request_id, fwd->epoch)));
			gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
			block_payload = NULL;
			block_payload_lkey = 0;
			block_payload_release_cb = NULL;
			block_payload_release_arg = NULL;
			hdr->checksum = gcs_block_compute_checksum(buf + header_len);
			hdr->status = (uint8)GCS_BLOCK_REPLY_DENIED_LOST_WRITE;
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->lost_write_detected_count, 1);
			/* spec-2.41 D7 observability — break out the §2.6 anomaly branch. */
			if (verdict == GCS_LOST_WRITE_FAIL_ANOMALY)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->lost_write_invalidscn_failclosed_count,
										1);
		} else {
			/* spec-2.41 D7 observability — SKIP = block not SCN-tracked (still ships). */
			if (verdict == GCS_LOST_WRITE_SKIP)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->lost_write_not_scn_tracked_skip_count, 1);
			hdr->checksum = gcs_block_compute_checksum(block_payload);
			/*
			 * PGRAC: spec-5.2 §3.5 D11
			 * — active-ITL hard boundary for writer-transfer-revoke.  Ship a
			 * read-image (keep our X, NO destructive drop) when EITHER this is a
			 * plain cross-node read (D2 IsReadImage) OR it is an X-transfer but we
			 * still hold an uncommitted ITL slot (ITL_FLAG_ACTIVE /
			 * LOCK_ONLY_ACTIVE) on this block.  In the X-transfer case our own
			 * commit's itl_finish_stamp_page still needs that in-memory slot;
			 * no-wire dropping the block now would discard the state our COMMIT
			 * must stamp, so the holder's COMMIT would re-read the pre-lock storage
			 * image and trip the stamp assert (the P0-2 crash).  Deferring lets the
			 * requesting writer install the image, see our row lock, enter the
			 * cross-node TX completion wait (spec-5.2 D4/D5), and retry the
			 * X-transfer only after we go terminal — at which point no active slot
			 * remains and the destructive transfer is safe.  "Active ITL is the
			 * hard boundary of writer-transfer; wait terminal first, then
			 * transfer."  Rule 8.A: a GCS ownership transfer must satisfy the
			 * holder's local commit dependency, not just the bufmgr API contract.
			 */
			if (remote_downgraded) {
				/*
				 * PGRAC: spec-6.12a ㉕ — we accepted the downgrade: our copy is
				 * S, the page is flushed storage-current, and the master
				 * notify is on the wire.  Ship a DURABLE S grant; the
				 * requester installs + registers as an S holder (wire try-ACK
				 * or local apply), degrading to one-shot on denial.  HC109
				 * chain fields were stamped above as for every holder ship.
				 */
				hdr->status = (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE;
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_from_holder_ship_count, 1);
			} else if (GcsBlockForwardPayloadIsReadImage(fwd)
					   || (GcsBlockForwardPayloadIsXTransfer(fwd)
						   && (fwd->transition_id == PCM_TRANS_N_TO_X
							   || fwd->transition_id == PCM_TRANS_S_TO_X_UPGRADE)
						   && cluster_itl_page_has_active_slot((Page)block_payload))) {
				/* The requester consumes a one-shot image; the holder keeps X. */
				hdr->status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->cf_xheld_read_ship_count, 1);
				/* PGRAC: spec-5.59 D3 — holder-forward read-image ship: time
				 * from here through the reply send below (the block copy
				 * earlier in this handler is excluded; approximate service
				 * time, count parity via cf_xheld_read_ship_count). */
				cluster_xp_begin(&xp_fwd_ship, CLXP_R_READIMAGE_SHIP);
			} else {
				hdr->status = (fwd->transition_id == PCM_TRANS_N_TO_X
							   || fwd->transition_id == PCM_TRANS_S_TO_X_UPGRADE)
								  ? (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
								  : (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_from_holder_ship_count, 1);

				/*
					 * PGRAC: spec-5.2 D11 — X-transfer (writer-transfer-revoke).
					 * The local master forwarded an N→X request with IsXTransfer
					 * set: it needs X for a local writer while we (a REMOTE node)
					 * hold X.  Unlike the 3-way path (master is a third node and
					 * we retain X until the requester's post-install ACK reaches
					 * the master), here the master IS the requester, so there is
					 * no separate ACK round-trip — release our own X NOW.  The
					 * current image is materialized into block_buf before the drop
					 * if it is backed by RDMA scratch SGE storage, so dropping our local
					 * copy cannot lose data;  invalidate it so we can never flush
					 * a stale page (Rule 8.A no-stale-flush), then apply the local X→N
					 * downgrade.  We drop locally only —
					 * no round-trip back to the master — so there is no release-
					 * to-the-invalidating-master deadlock.  The master records
					 * itself as the new X holder on install.
					 */
				if (GcsBlockForwardPayloadIsXTransfer(fwd)
					&& hdr->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER) {
					XLogRecPtr drop_lsn = InvalidXLogRecPtr;

					if (block_payload_release_cb != NULL) {
						memcpy(block_buf, block_payload, GCS_BLOCK_DATA_SIZE);
						gcs_block_release_ship_image(block_payload_release_cb,
													 block_payload_release_arg);
						block_payload = block_buf;
						block_payload_lkey = 0;
						block_payload_release_cb = NULL;
						block_payload_release_arg = NULL;
					}

					/*
						 * spec-5.2 D11 (BLOCKER A resolved): drop our local copy
						 * with NO GCS release wire.  We run in the §3.5 IC-dispatch
						 * (LMON) context, which has no backend slot
						 * (MyProcNumber/MyBackendId).  The ordinary eviction path
						 * (cluster_bufmgr_invalidate_block_for_gcs →
						 * InvalidateBuffer → cache-eviction hook →
						 * cluster_pcm_lock_release_buffer_for_eviction(buf, X))
						 * would, because our master is the REMOTE requester, send
						 * an X→N release transition wire → gcs_reserve_slot →
						 * ERROR in LMON → §3.5 frame drop → reply lost.  No wire is
						 * correct: in path A the requester IS the local master, so
						 * it already owns the transfer and records itself as the
						 * new X holder on install;  we (the previous holder) drop
						 * unilaterally — nobody to notify.  The image was already
						 * copied into block_buf above, so dropping cannot lose data, and the
						 * helper's XLogFlush + InvalidateBuffer preserves Rule 8.A
						 * no-stale-flush.  node0 holds no authoritative GRD entry
						 * for this tag (the master is node1), so there is no local
						 * GRD state to transition — clearing the BufferDesc
						 * pcm_state to N inside the helper is the full release.
						 * NOT_RESIDENT is fine: no stale copy left, the image
						 * was already shipped into the reply above (Rule 8.A;
						 * same reasoning as the path-B drop site).  PINNED is
						 * handled below (round-5 A2 retryable deny).
						 */
					/*
					 * PGRAC: spec-5.2a D4 — a clean (sequence) eligible page has
					 * no active ITL, so this is the same destructive writer-
					 * transfer drop as the spec-5.2 D11 heap path: ship X and drop
					 * our copy no-wire.  No data flush in LMON (the backend
					 * eager-flushed the page to shared storage at write time, so
					 * storage is already current for the stale-holder
					 * storage-fallback); drop_no_wire's XLogFlush of the
					 * already-flushed page_lsn satisfies WAL-before-share.  A
					 * clean transfer increments clean_page_xfer_count too.
					 *
					 * GCS serve-stall round-5 (A2): the drop is bounded.  On
					 * PINNED we must NOT ship X while a live pinned copy stays
					 * resident here (stale local X, 8.A) — flip the reply to
					 * the HC105 retryable deny (the requester's retransmit
					 * budget covers recovery, exactly like the evict race) and
					 * keep our X untouched.  NOT_RESIDENT still grants: no
					 * copy left to stale-flush.
					 */
					/* GCS serve-stall round-6 RED harness: hold the copy->drop
					 * window open (see cluster_inject.c registry note). */
					CLUSTER_INJECTION_POINT("cluster-gcs-xfer-copy-drop-window");
					/* GCS serve-stall round-6: page_lsn (copy-time, from
					 * get_ship_image above) is the generation token.  STALE (a
					 * local writer committed since the copy) joins PINNED as a
					 * retryable deny — never ship a stale image over a committed
					 * write (Rule 8.A). */
					{
						ClusterBufmgrGcsDropResult dres = cluster_bufmgr_drop_block_for_gcs_no_wire(
							fwd->tag, page_lsn, &drop_lsn);

						if (dres == CLUSTER_BUFMGR_GCS_DROP_PINNED
							|| dres == CLUSTER_BUFMGR_GCS_DROP_STALE) {
							if (dres == CLUSTER_BUFMGR_GCS_DROP_STALE) {
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->xfer_stale_deny_count, 1);
								GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(fwd, "holder-drop-stale");
							} else {
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->drop_pinned_deny_count,
														1);
								GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(fwd, "holder-drop-pinned");
							}
							/* undo the from-holder ship count taken with the
							 * grant status above — this reply is a deny now */
							pg_atomic_fetch_sub_u64(&ClusterGcsBlock->block_from_holder_ship_count,
													1);
							hdr->status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
						} else {
							/* PGRAC: spec-6.12h D-h3a — ordering pin: the PI
							 * conversion (inside the drop above) precedes the reply
							 * send at the bottom of this handler
							 * (cluster_ic_rdma_send_envelope_sge), so the requester
							 * observes an envelope stamped at-or-above the ship-SCN
							 * boundary (cluster_pi_shadow.h proof item 2). */
							pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_transfer_ship_count,
													1);
							if (GcsBlockForwardPayloadIsCleanEligible(fwd))
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_count, 1);
							/* PGRAC: spec-6.12h D-h2 — if the D-h1 conversion kept our
							 * outgoing copy as a Past Image, report it to the master
							 * (unsolicited PI_KEPT ride; fire-and-forget — a lost note
							 * only leaves the PI untracked, fail-safe lingering). */
							if (cluster_bufmgr_block_is_pi(fwd->tag))
								gcs_block_pi_kept_note_send(fwd->tag, fwd->master_node);
						}
					}
				}
			}
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_ship_bytes_total, GCS_BLOCK_DATA_SIZE);
		}
	} else {
		/* HC105 evict race */
		const char *copy_refusal_name = cluster_bufmgr_gcs_copy_refusal_name(copy_refusal);

		hdr->checksum = gcs_block_compute_checksum(buf + header_len);
		hdr->status = (uint8)GcsBlockMasterDirectCopyRefusalStatus(copy_refusal);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_holder_evicted_count, 1);
		GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(fwd, "holder-copy-refused");
		ereport(LOG, (errmsg("cluster_gcs_block: holder ship image refused"),
					  errdetail("reason=%s request_id=" UINT64_FORMAT
								" requester=%d master=%d tag spc=%u db=%u relNumber=%u block=%u",
								copy_refusal_name, fwd->request_id, fwd->original_requester_node,
								fwd->master_node, fwd->tag.spcOid, fwd->tag.dbOid,
								(unsigned int)BufTagGetRelNumber(&fwd->tag),
								(unsigned int)fwd->tag.blockNum)));
	}

	send_sf_dep = sf_peer_v2 && sf_dep_valid && block_payload != NULL
				  && (hdr->status == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
					  || hdr->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
					  || hdr->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER);
	header_len
		= send_sf_dep ? (uint32)sizeof(GcsBlockReplyHeaderV2) : (uint32)sizeof(GcsBlockReplyHeader);
	total = header_len + GCS_BLOCK_DATA_SIZE;
	if (send_sf_dep) {
		GcsBlockReplyHeaderV2 *hdrv2 = (GcsBlockReplyHeaderV2 *)buf;
		int i;
		int n = 0;

		hdrv2->sf_flags = GCS_BLOCK_REPLY_SF_EARLY_TRANSFER | GCS_BLOCK_REPLY_SF_HAS_DEP_VEC;
		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
			if (XLogRecPtrIsInvalid(sf_dep_vec.required[i]))
				continue;
			hdrv2->sf_dep[n].origin_node = i;
			hdrv2->sf_dep[n].required_redo_lsn = (uint64)sf_dep_vec.required[i];
			n++;
		}
		hdrv2->sf_dep_count = (uint8)n;
	}

	if (GcsBlockForwardPayloadIsDirectLandArmed(fwd)) {
		if (!GcsBlockReplyStatusIsDirectLandSendable((GcsBlockReplyStatus)hdr->status))
			GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(fwd, "holder-direct-land-nonsendable");
		(void)gcs_block_try_send_direct_reply(fwd->original_requester_node, true, hdr,
											  holder_ship_ok ? block_payload : NULL,
											  holder_ship_ok ? block_payload_lkey : 0,
											  block_payload_release_cb, block_payload_release_arg);
		if (hdr->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER)
			cluster_xp_end(&xp_fwd_ship);
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
		pfree(buf);
		return;
	}

	if (holder_ship_ok && block_payload != NULL
		&& (hdr->status == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
			|| hdr->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
			|| hdr->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
			/* PGRAC: spec-6.12a ㉕ — the downgraded durable S grant ships page
			 * bytes exactly like the statuses above; leaving it off this list
			 * would send the palloc0 zero pad under a real-page checksum
			 * (guaranteed verify failure at the requester). */
			|| hdr->status == (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE)) {
		ClusterICSge sge[2];
		ClusterICSendResult send_rc;
		bool live_sge_payload = block_payload_release_cb == gcs_block_release_live_sge;

		memset(sge, 0, sizeof(sge));
		sge[0].addr = hdr;
		sge[0].len = header_len;
		sge[1].addr = (void *)block_payload;
		sge[1].len = GCS_BLOCK_DATA_SIZE;
		sge[1].lkey = block_payload_lkey;
		sge[1].release_cb = block_payload_release_cb;
		sge[1].release_arg = block_payload_release_arg;
		send_rc = cluster_ic_rdma_send_envelope_sge(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, fwd->original_requester_node, sge, lengthof(sge), total);
		cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, send_rc);
		if (send_rc == CLUSTER_IC_SEND_DONE && live_sge_payload)
			gcs_block_note_live_sge_send();
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
	} else {
		gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
		cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY,
											cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY,
																	 fwd->original_requester_node,
																	 buf, total));
	}
	/* PGRAC: spec-5.59 D3 — close the holder-forward read-image ship scope
	 * (no-op unless the read-image branch above started it). */
	cluster_xp_end(&xp_fwd_ship);

	pfree(buf);
}


/* ============================================================
 * Dispatch table registration.
 *
 * PGRAC: spec-7.2 flip — the five block-family msg_types (REQUEST /
 * REPLY / FORWARD / INVALIDATE / INVALIDATE_ACK) are registered on
 * the DATA plane:  the LMS-owned tier1 instance carries their frames,
 * the LMS loop dispatches them, and the producer mask admits the LMS
 * family for the drain-and-send leg.  All five flip in this one edit
 * (H-5: no half-migrated window;  the registry probe above pivots the
 * LMON tick sites and the LMS loop automatically).  REDECLARE alone
 * stays on the CONTROL plane (r4): recovery re-declare must survive a
 * DATA-mesh teardown mid-episode, and the REDECLARE -> REDECLARE_DONE
 * pair may not be split across planes.
 * ============================================================ */

/* Resolve the boot session used by the PCM-X frontier.  A remote session is
 * fresh-alive QVOTEC authority, sampled twice inside one exact capability
 * record generation (CONTROL-owned on the tier1 S3 path).  DATA source is
 * already bound by envelope verify.
 * This deliberately does not use membership.last_admitted: that value is a
 * historical anti-rejoin floor and can remain zero for initial members. */
static PcmXSessionAuthResult
gcs_block_pcm_x_authenticated_session_result(int32 node_id, uint64 expected_epoch,
											 uint64 *session_out,
											 ClusterGcsPcmXAuthSample *sample_out)
{
	ClusterGcsPcmXAuthSample sample;
	PcmXSessionAuthResult result;
	uint64 session;

	memset(&sample, 0, sizeof(sample));
	if (session_out != NULL)
		*session_out = 0;
	if (sample_out != NULL)
		memset(sample_out, 0, sizeof(*sample_out));
	if (node_id < 0 || node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_SESSION_AUTH_INVALID;
	if (node_id == cluster_node_id) {
		session = cluster_qvotec_get_self_incarnation();
		if (session == 0)
			return PCM_X_SESSION_AUTH_SLOT_NOT_READY;
		if (session_out != NULL)
			*session_out = session;
		return PCM_X_SESSION_AUTH_OK;
	}

	sample.connection_before_valid = cluster_sf_peer_pcm_x_connection_generation(
		node_id, &sample.connection_generation_before);
	sample.slot_before_valid = cluster_reconfig_get_observed_slot(node_id, &sample.session_before,
																  &sample.slot_generation_before);
	sample.observed_epoch_before = cluster_reconfig_get_observed_epoch(node_id);
	sample.fresh_before = cluster_reconfig_get_observed_fresh_alive(node_id);
	sample.slot_after_valid = cluster_reconfig_get_observed_slot(node_id, &sample.session_after,
																 &sample.slot_generation_after);
	sample.observed_epoch_after = cluster_reconfig_get_observed_epoch(node_id);
	sample.fresh_after = cluster_reconfig_get_observed_fresh_alive(node_id);
	sample.connection_after_valid
		= cluster_sf_peer_pcm_x_connection_generation(node_id, &sample.connection_generation_after);
	result = cluster_gcs_pcm_x_auth_sample_classify(&sample, expected_epoch);
	if (sample_out != NULL)
		*sample_out = sample;
	if (result != PCM_X_SESSION_AUTH_OK)
		return result;
	if (session_out != NULL)
		*session_out = sample.session_before;
	return PCM_X_SESSION_AUTH_OK;
}


static bool
gcs_block_pcm_x_authenticated_session(int32 node_id, uint64 expected_epoch, uint64 *session_out)
{
	return gcs_block_pcm_x_authenticated_session_result(node_id, expected_epoch, session_out, NULL)
		   == PCM_X_SESSION_AUTH_OK;
}

static void
gcs_block_legacy_pcm_x_fail_closed(int32 source_node)
{
	if (source_node >= 0
		&& source_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& source_node != cluster_node_id)
		cluster_lms_data_plane_close_peer_now(source_node);
}

/* D11-05: values 41-64 remain permanently reserved.  A verified DATA frame
 * in that range has no semantic decoder in the source-removed build.  The
 * one shared disposition confirms exact TARGET admission and then drops the
 * frame without resource state, buffer, outbound-ring, allocation, or ACK. */
static void
gcs_block_legacy_pcm_x_stale_ingress(
	const ClusterICEnvelope *env, const void *payload)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	uint32 capability_word = 0;
	uint32 connection_generation = 0;
	int32 source_node;
	bool admission_current;
	bool malformed;
	bool peer_exact;
	bool peer_sampled = true;

	if (env == NULL)
		return;
	source_node = (int32)env->source_node_id;
	malformed = env->msg_type < PGRAC_IC_MSG_PCM_X_ENQUEUE
		|| env->msg_type > PGRAC_IC_MSG_PCM_X_RETIRE_ACK
		|| source_node < 0
		|| source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| env->dest_node_id != (uint32)cluster_node_id
		|| env->payload_length == 0
		|| env->payload_length > PGRAC_IC_PAYLOAD_MAX
		|| payload == NULL
		|| env->epoch != cluster_epoch_get_current();
	if (malformed) {
		gcs_block_legacy_pcm_x_fail_closed(source_node);
		return;
	}

	memset(&admission, 0, sizeof(admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		gcs_block_legacy_pcm_x_fail_closed(source_node);
		return;
	}
	if (source_node == cluster_node_id) {
		capability_word = cluster_ic_local_capability_word();
		connection_generation = 1;
	}
	else
		peer_sampled = cluster_sf_peer_capability_word_sample(
			source_node, PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
			&capability_word, &connection_generation);
	(void)capability_word;
	peer_exact = peer_sampled
		&& gcs_block_resource_x_target_peer_matches_exact(
			&admission, source_node, connection_generation);
	admission_current = cluster_semantic_activation_recheck(&admission);
	cluster_semantic_activation_leave(&admission);
	if (!peer_sampled || !peer_exact || !admission_current)
		gcs_block_legacy_pcm_x_fail_closed(source_node);
}

#define LEGACY_PCM_X_STALE_INFO(msg, label)                                    \
	{                                                                          \
		.msg_type = (msg), .name = (label),                                     \
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS             \
			| CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,           \
		.broadcast_ok = false, .handler = gcs_block_legacy_pcm_x_stale_ingress, \
		.plane = CLUSTER_IC_PLANE_DATA                                           \
	}

static const ClusterICMsgTypeInfo legacy_pcm_x_stale_infos[] = {
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_ENQUEUE, "retired_pcm_x_41"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_ADMIT_ACK, "retired_pcm_x_42"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM, "retired_pcm_x_43"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK, "retired_pcm_x_44"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN, "retired_pcm_x_45"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE, "retired_pcm_x_46"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, "retired_pcm_x_47"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK, "retired_pcm_x_48"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_REVOKE, "retired_pcm_x_49"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_IMAGE_READY, "retired_pcm_x_50"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_PREPARE_GRANT, "retired_pcm_x_51"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_INSTALL_READY, "retired_pcm_x_52"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_COMMIT_X, "retired_pcm_x_53"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_FINAL_ACK, "retired_pcm_x_54"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK, "retired_pcm_x_55"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM, "retired_pcm_x_56"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL, "retired_pcm_x_57"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK, "retired_pcm_x_58"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_CANCEL, "retired_pcm_x_59"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_CANCEL_ACK, "retired_pcm_x_60"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_DRAIN_POLL, "retired_pcm_x_61"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_DRAIN_ACK, "retired_pcm_x_62"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO, "retired_pcm_x_63"),
	LEGACY_PCM_X_STALE_INFO(PGRAC_IC_MSG_PCM_X_RETIRE_ACK, "retired_pcm_x_64"),
};

#undef LEGACY_PCM_X_STALE_INFO


static const ClusterICMsgTypeInfo gcs_block_request_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
	.name = "gcs_block_request",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_request_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo gcs_block_reply_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY,
	.name = "gcs_block_reply",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_reply_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

/*
 * cluster_gcs_handle_block_done_envelope — GCS-race round-2 RC-F: master-side
 * completion-proof consumer.  Verifies the shard route (mirror of the REQUEST
 * handler's spec-7.3 D5 guard) and hands the FULL identity to
 * cluster_gcs_block_dedup_mark_done, which checks key + tag + transition +
 * COMPLETED under the shard lock.  Every mismatch/miss is counted there and
 * dropped: DONE is advisory, the entry's pinned TTL remains the backstop, so
 * this handler never replies and never errors.
 */
static void
cluster_gcs_handle_block_done_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockDonePayload *done;
	GcsBlockDedupKey key;
	int dedup_worker_id;

	if (gcs_block_try_resource_x_frame(env, payload))
		return;
	if (env == NULL || payload == NULL || env->payload_length != sizeof(GcsBlockDonePayload))
		return;
	done = (const GcsBlockDonePayload *)payload;

	dedup_worker_id = cluster_ic_tier1_my_data_channel();
	{
		int tag_shard = cluster_lms_shard_for_tag(&done->tag, cluster_lms_workers);

		Assert(tag_shard == dedup_worker_id);
		if (tag_shard != dedup_worker_id) {
			cluster_gcs_block_dedup_note_misroute();
			return;
		}
	}

	/*
	 * GCS-race round-2 review F6: bind the wire identity to the transport
	 * and reject unknown payload bits.  The dedup key's origin node MUST
	 * be the connection's verified source (a forged sender_node would let
	 * one node retire another node's entry -> premature reclaim ->
	 * re-execution), and the reserved pad must be all-zero so a future
	 * sender cannot smuggle semantics past this validator.  Count + drop;
	 * DONE stays advisory.
	 */
	if (done->sender_node != (int32)env->source_node_id) {
		cluster_gcs_block_dedup_note_done_mismatch(dedup_worker_id);
		return;
	}
	{
		int i;

		for (i = 0; i < (int)sizeof(done->reserved_0); i++)
			if (done->reserved_0[i] != 0) {
				cluster_gcs_block_dedup_note_done_mismatch(dedup_worker_id);
				return;
			}
	}

	memset(&key, 0, sizeof(key));
	key.origin_node_id = (uint32)done->sender_node;
	key.requester_backend_id = done->requester_backend_id;
	key.request_id = done->request_id;
	key.cluster_epoch = done->epoch;

	(void)cluster_gcs_block_dedup_mark_done(dedup_worker_id, &key, &done->tag, done->transition_id);
}

/* PGRAC: GCS-race round-2 RC-F — completion-proof msg_type registration. */
static const ClusterICMsgTypeInfo gcs_block_done_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_DONE,
	.name = "gcs_block_done",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_done_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

/* PGRAC: spec-2.35 D8 — holder-side forward handler msg_type registration. */
static const ClusterICMsgTypeInfo gcs_block_forward_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
	.name = "gcs_block_forward",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_forward_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};


/* ============================================================
 * PGRAC: spec-2.36 D3 (HC116) — broadcast invalidate implementation.
 *
 *	Master-side sender:  claims the global broadcast slot via CAS on
 *	invalidate_broadcast_request_id;  sends INVALIDATE to every set
 *	bit in holders_bm;  sleeps on invalidate_broadcast_cv with timeout
 *	= cluster.gcs_block_invalidate_ack_timeout_ms;  wakes when
 *	acked_bm == expected_bm or timeout fires;  releases slot.
 *
 *	HC100 ack validation lives in the ACK handler (request_id +
 *	epoch + tag + sender_node ∈ expected bitmap).  Failed
 *	holders / timeouts surface as `return false` → master replies
 *	DENIED_INVALIDATE_TIMEOUT (status 11) → sender 53R91.
 * ============================================================ */
/*
 * Ruling ② review P1 — explicit per-round outcome, so a BUSY negative ACK can
 * never mask a harder failure: an epoch fence or a dropped send must surface
 * as fail-closed (no retry-with-backoff), and only a PURE busy round is the
 * caller's cue to retry.  Priority: EPOCH_STALE > SEND_FAIL > BUSY (TIMEOUT
 * is mutually exclusive with BUSY -- the busy wake breaks the wait early).
 */
typedef enum GcsInvalRoundOutcome {
	GCS_INVAL_ROUND_FULL_ACK = 0,
	GCS_INVAL_ROUND_EPOCH_STALE,
	GCS_INVAL_ROUND_SEND_FAIL,
	GCS_INVAL_ROUND_BUSY,
	GCS_INVAL_ROUND_TIMEOUT,
} GcsInvalRoundOutcome;

static bool
gcs_block_broadcast_invalidate_and_wait_ext(const GcsBlockRequestPayload *req, uint32 holders_bm,
											bool via_outbound_ring,
											GcsInvalRoundOutcome *out_outcome)
{
	GcsBlockInvalidatePayload inv;
	uint64 current_epoch;
	int n;
	uint32 acked_bm;
	int timeout_ms = cluster_gcs_block_invalidate_ack_timeout_ms;
	bool full_ack = false;
	long start_lsn;
	long elapsed_ms = 0;
	/* PGRAC: spec-5.59 D2 — invalidate broadcast + ack-collection interval
	 * (runs at the master; service-time when master != requester). */
	ClusterXpScope xp_inv = { .active = false };

	bool send_fail = false;
	bool round_busy = false;

	if (out_outcome != NULL)
		*out_outcome = GCS_INVAL_ROUND_TIMEOUT;
	if (ClusterGcsBlock == NULL)
		return false;

	cluster_xp_begin(&xp_inv, CLXP_W_GCS_X_INVALIDATE);

	/* Claim and stamp the broadcast slot as one critical section.  ACK
	 * validation reads the same identity under this lock, so a late ACK from
	 * an older broadcast cannot match a newly claimed slot by request_id alone.
	 *
	 * The slot is a node-wide singleton, and every caller of this blocking
	 * variant runs in BACKEND context (the LMON/LMS dispatch paths use the
	 * nowait fan-out instead), so a busy slot must be WAITED OUT, not failed
	 * instantly:  two concurrent local S->X upgrades — even on unrelated
	 * blocks — collide here, and the pre-wait behavior surfaced the loser as
	 * a spurious "S->X upgrade invalidate did not complete" ERROR (the
	 * S3-observed low-concurrency failure class).  Bound the wait by the
	 * same ACK-collection budget;  a genuine exhaustion still returns false.
	 *
	 * Exact-epoch fence #1: the epoch is captured INSIDE the claim critical
	 * section, after any wait.  Capturing it before the wait would stamp the
	 * slot with a pre-reconfiguration epoch: every holder at the new epoch
	 * answers epoch_stale (no drop), and worse, an ACK produced at the old
	 * epoch could still match the stale slot identity — an old-epoch drop
	 * proof authorizing a new-epoch grant (8.A stale-proof). */
	{
		TimestampTz claim_deadline
			= GetCurrentTimestamp() + (TimestampTz)timeout_ms * (TimestampTz)1000;
		bool claimed = false;

		ConditionVariablePrepareToSleep(&ClusterGcsBlock->invalidate_broadcast_cv);
		for (;;) {
			TimestampTz now;
			long remaining_ms;

			LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
			if (pg_atomic_read_u64(&ClusterGcsBlock->invalidate_broadcast_request_id) == 0) {
				current_epoch = cluster_epoch_get_current();
				ClusterGcsBlock->invalidate_broadcast_epoch = current_epoch;
				ClusterGcsBlock->invalidate_broadcast_tag = req->tag;
				pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, holders_bm);
				pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
				pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_busy, 0);
				pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id,
									req->request_id);
				LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
				claimed = true;
				break;
			}
			LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);

			CHECK_FOR_INTERRUPTS();
			now = GetCurrentTimestamp();
			if (now >= claim_deadline)
				break;
			remaining_ms = (long)((claim_deadline - now) / 1000);
			if (remaining_ms <= 0)
				remaining_ms = 1;
			(void)ConditionVariableTimedSleep(&ClusterGcsBlock->invalidate_broadcast_cv,
											  remaining_ms,
											  WAIT_EVENT_GCS_BLOCK_INVALIDATE_ACK_WAIT);
		}
		ConditionVariableCancelSleep();

		if (!claimed) {
			cluster_xp_abort(&xp_inv); /* PGRAC: spec-5.59 — slot busy, no sample */
			return false;
		}
	}

	/* The slot is held from here on.  Any ereport out of the send/wait
	 * region (an armed :error injection, a future throwing send path)
	 * would otherwise leak the claimed singleton forever — every later
	 * local upgrade on this node would wait out its claim budget and
	 * fail.  PG_CATCH releases the slot, wakes claim waiters, re-throws. */
	PG_TRY();
	{
		/* Build and dispatch INVALIDATE to each holder bit. */
		memset(&inv, 0, sizeof(inv));
		inv.request_id = req->request_id;
		inv.epoch = current_epoch;
		inv.tag = req->tag;
		inv.master_node = cluster_node_id;
		inv.invalidating_for_x_node = (uint8)(req->sender_node & 0xff);
		inv.checksum = gcs_block_compute_invalidate_checksum(&inv);

		for (n = 0; n < 32; n++) {
			if ((holders_bm & ((uint32)1u << n)) == 0)
				continue;
			/* D16 inject — drop a single broadcast envelope. */
			CLUSTER_INJECTION_POINT("cluster-gcs-block-invalidate-drop-broadcast");
			if (cluster_injection_should_skip("cluster-gcs-block-invalidate-drop-broadcast"))
				continue;
			/* PGRAC: spec-6.12a — a backend-context caller (local-master S->X
			 * upgrade) cannot use the LMON-owned connections directly; route
			 * through the backend outbound ring instead (LMON flushes it).
			 *
			 * PGRAC ownership-generation wave — the enqueue CAN fail (DATA-
			 * plane shard refuse, LMS outbound ring full, CONTROL ring
			 * reserved-budget refuse).  The old (void) swallow made a dropped
			 * INVALIDATE indistinguishable from a sent one: the ack wait then
			 * times out every round while the master's stale S bit never
			 * clears (the holder never receives what it should drop) — a
			 * permanent upgrade wedge.  Count the drop and do NOT count it as
			 * broadcast; the wait below then fails fast and honest.
			 */
			if (via_outbound_ring) {
				if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE,
															  (uint32)n, &inv, sizeof(inv))) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_send_not_admitted_count,
											1);
					send_fail = true;
					continue;
				}
			} else
				cluster_gcs_block_note_send_outcome(
					GCS_BLOCK_SEND_FAMILY_INVALIDATE,
					cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, n, &inv,
											 sizeof(inv)));
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_broadcast_count, 1);
		}

		/* Poll-with-CV wait for full ack collection or timeout.  A dropped
		 * send makes full collection impossible -- fail the round honestly
		 * NOW instead of burning the budget against a holder that never got
		 * the directive (review P1: send-fail must not be masked). */
		start_lsn = (long)GetCurrentTimestamp();
		ConditionVariablePrepareToSleep(&ClusterGcsBlock->invalidate_broadcast_cv);
		for (; !send_fail;) {
			acked_bm = pg_atomic_read_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm);
			if ((acked_bm & holders_bm) == holders_bm) {
				full_ack = true;
				break;
			}
			/* Ruling ② — a slot-matching RETRYABLE_BUSY aborts the round NOW:
			 * the blocked holder cannot make progress while this waiter holds
			 * pending_x, so waiting longer only burns the budget.  The caller
			 * clears pending_x, backs off briefly and retries with a NEW
			 * round identity. */
			if (pg_atomic_read_u32(&ClusterGcsBlock->invalidate_broadcast_busy) != 0) {
				round_busy = true;
				break;
			}
			elapsed_ms = (long)((GetCurrentTimestamp() - start_lsn) / 1000);
			if (elapsed_ms >= timeout_ms)
				break;
			if (ConditionVariableTimedSleep(&ClusterGcsBlock->invalidate_broadcast_cv,
											timeout_ms - elapsed_ms,
											WAIT_EVENT_GCS_BLOCK_INVALIDATE_ACK_WAIT))
				break; /* timeout */
		}
		ConditionVariableCancelSleep();
	}
	PG_CATCH();
	{
		ConditionVariableCancelSleep();
		LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
		pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, 0);
		ClusterGcsBlock->invalidate_broadcast_epoch = 0;
		memset(&ClusterGcsBlock->invalidate_broadcast_tag, 0,
			   sizeof(ClusterGcsBlock->invalidate_broadcast_tag));
		pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, 0);
		pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
		pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_busy, 0);
		LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		ConditionVariableBroadcast(&ClusterGcsBlock->invalidate_broadcast_cv);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Exact-epoch fence #3: a full bitmap collected across an epoch bump is
	 * an old-epoch proof — after a reconfiguration the S set may have been
	 * rebuilt (rejoin re-declare), so certifying those drops would authorize
	 * an X grant against holders the acks never covered (8.A stale-proof).
	 * Fail closed; the caller surfaces the retryable "did not complete".
	 */
	if (full_ack && cluster_epoch_get_current() != current_epoch)
		full_ack = false;

	/* Review P1 — resolve the round outcome with hard failures first. */
	if (out_outcome != NULL) {
		if (full_ack)
			*out_outcome = GCS_INVAL_ROUND_FULL_ACK;
		else if (cluster_epoch_get_current() != current_epoch)
			*out_outcome = GCS_INVAL_ROUND_EPOCH_STALE;
		else if (send_fail)
			*out_outcome = GCS_INVAL_ROUND_SEND_FAIL;
		else if (round_busy)
			*out_outcome = GCS_INVAL_ROUND_BUSY;
		else
			*out_outcome = GCS_INVAL_ROUND_TIMEOUT;
	}

	/* Release the slot.  Broadcast the CV afterwards: concurrent claimants
	 * sleep on the same invalidate_broadcast_cv (see the bounded claim-wait
	 * above), so the release must wake them or they only recheck on their
	 * timeout slices. */
	LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, 0);
	ClusterGcsBlock->invalidate_broadcast_epoch = 0;
	memset(&ClusterGcsBlock->invalidate_broadcast_tag, 0,
		   sizeof(ClusterGcsBlock->invalidate_broadcast_tag));
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, 0);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_busy, 0);
	LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
	ConditionVariableBroadcast(&ClusterGcsBlock->invalidate_broadcast_cv);

	cluster_xp_end(&xp_inv); /* PGRAC: spec-5.59 D2 */
	return full_ack;
}

/*
 * PGRAC: spec-6.12e2 (structural fix) — fire-and-forget INVALIDATE fan-out
 * for the LMON wire-request S-branch.  No broadcast slot, no CV wait: the
 * dispatch loop must never sleep on ACKs it alone can drain.  Ack-side
 * bookkeeping happens in the ACK handler (authoritative S-bit clear); the
 * requester converges through its DENIED_PENDING_X backoff retries.
 */
static void
gcs_block_broadcast_invalidate_nowait(const GcsBlockRequestPayload *req, uint32 holders_bm)
{
	GcsBlockInvalidatePayload inv;
	int n;

	if (ClusterGcsBlock == NULL)
		return;

	memset(&inv, 0, sizeof(inv));
	inv.request_id = req->request_id;
	inv.epoch = cluster_epoch_get_current();
	inv.tag = req->tag;
	inv.master_node = cluster_node_id;
	inv.invalidating_for_x_node = (uint8)(req->sender_node & 0xff);
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);

	for (n = 0; n < 32; n++) {
		if ((holders_bm & ((uint32)1u << n)) == 0)
			continue;
		/* D16 inject — drop a single broadcast envelope. */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-invalidate-drop-broadcast");
		if (cluster_injection_should_skip("cluster-gcs-block-invalidate-drop-broadcast"))
			continue;
		cluster_gcs_block_note_send_outcome(
			GCS_BLOCK_SEND_FAMILY_INVALIDATE,
			cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, n, &inv, sizeof(inv)));
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_broadcast_count, 1);
	}
}

/* ============================================================
 * PGRAC: spec-6.12h D-h2 — PI-holder discard protocol (Q25-A dual trigger).
 *
 *	Pipeline (every hop fire-and-forget fail-safe — a lost note/notify only
 *	leaves a PI lingering until buffer pressure or the implicit-discard
 *	reread; §3.4b):
 *
 *	  FlushBuffer                      pi_write_note        ("写盘成功" face)
 *	  checkpointer ProcessSyncRequests presync_snapshot/confirm
 *	                                                        ("checkpoint 推进" face)
 *	  LMON tick                        pi_discard_drain -> route to master
 *	  master                           pi_discard_master_apply:
 *	                                     collect (retire watermarks + bitmap)
 *	                                     -> PI_DISCARD per holder
 *	  holder                           cluster_bufmgr_discard_pi_block
 * ============================================================ */

/*
 * FlushBuffer just wrote a cluster-tracked block toward shared storage.
 * Record (tag, pd_block_scn) of the flushed image; the note only becomes a
 * discard trigger after the checkpoint sync phase proves it durable.  The
 * page LSN is deliberately NOT recorded: under per-thread WAL every node has
 * its own LSN space, so only the pd_block_scn (AD-008 Lamport) version is
 * cross-node comparable.  Multi-producer (checkpointer / bgwriter /
 * backends), so the append runs under the ring spinlock; ring full drops the
 * NEW note (dropping the oldest could starve a sealed note the drain is
 * consuming).
 */
void
cluster_gcs_block_pi_write_note(BufferTag tag, SCN page_scn)
{
	bool overflowed = false;

	if (ClusterGcsBlock == NULL || !cluster_past_image)
		return;

	SpinLockAcquire(&ClusterGcsBlock->pi_note_lock);
	if (ClusterGcsBlock->pi_note_append_seq - ClusterGcsBlock->pi_note_drain_seq
		>= CLUSTER_GCS_PI_NOTE_RING_SIZE) {
		overflowed = true;
	} else {
		uint64 slot = ClusterGcsBlock->pi_note_append_seq % CLUSTER_GCS_PI_NOTE_RING_SIZE;

		ClusterGcsBlock->pi_note_ring[slot].tag = tag;
		ClusterGcsBlock->pi_note_ring[slot].page_scn = page_scn;
		ClusterGcsBlock->pi_note_append_seq++;
	}
	SpinLockRelease(&ClusterGcsBlock->pi_note_lock);

	cluster_lever_h_note_write_note(overflowed);
}

/*
 * Checkpointer, right BEFORE ProcessSyncRequests: snapshot the append seq.
 * Every note recorded before the sync phase begins is durable once it
 * returns (its smgrwrite happened before the fsync sweep that is about to
 * run).  Notes appended DURING the sync phase wait for the next checkpoint
 * — conservative by one cycle, never wrong.
 */
uint64
cluster_gcs_block_pi_note_presync_snapshot(void)
{
	uint64 seq;

	if (ClusterGcsBlock == NULL)
		return 0;
	SpinLockAcquire(&ClusterGcsBlock->pi_note_lock);
	seq = ClusterGcsBlock->pi_note_append_seq;
	SpinLockRelease(&ClusterGcsBlock->pi_note_lock);
	return seq;
}

/*
 * Checkpointer, right AFTER ProcessSyncRequests returned: everything below
 * the presync snapshot is now provably durable.  Monotone (a concurrent
 * end-of-recovery checkpoint cannot regress the seal).
 */
void
cluster_gcs_block_pi_note_confirm(uint64 presync_seq)
{
	if (ClusterGcsBlock == NULL)
		return;
	SpinLockAcquire(&ClusterGcsBlock->pi_note_lock);
	if (presync_seq > ClusterGcsBlock->pi_note_confirmed_seq)
		ClusterGcsBlock->pi_note_confirmed_seq = presync_seq;
	SpinLockRelease(&ClusterGcsBlock->pi_note_lock);
}

/*
 * Report "our destructive drop kept a Past Image" to the block's master so
 * it lands on the authoritative pi_holders_bitmap.  Local master -> direct
 * note; remote -> unsolicited PI_KEPT ride on the invalidate-ACK wire.
 */
static void
gcs_block_pi_kept_note_send(BufferTag tag, int32 master_node)
{
	if (master_node == cluster_node_id) {
		cluster_pcm_lock_pi_holder_note(tag, cluster_node_id);
		return;
	}
	if (master_node < 0 || master_node >= 32)
		return;

	{
		GcsBlockInvalidateAckPayload note;

		memset(&note, 0, sizeof(note));
		note.request_id = 0; /* unsolicited: diverted before the slot logic */
		note.epoch = cluster_epoch_get_current();
		note.tag = tag;
		note.sender_node = cluster_node_id;
		note.ack_status = GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_KEPT_NOTE;
		note.checksum = gcs_block_compute_invalidate_ack_checksum(&note);
		cluster_gcs_block_note_send_outcome(
			GCS_BLOCK_SEND_FAMILY_INVALIDATE,
			cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, master_node, &note,
									 sizeof(note)));
	}
}

/*
 * Send one unsolicited PI_DISCARD INVALIDATE ride to `target_node` for `tag`:
 * "drop your Past Image of this block".  request_id 0, fire-and-forget, never
 * ACKed (spec-6.12h D-h2).  Public so the shared-undo data plane (spec-5.22b
 * D2-4, owner-as-master) reuses the exact wire + checksum rather than
 * duplicating the payload build.  Emits tier-1 IC -> must run in LMON
 * dispatch/tick context (L172 family); the caller owns the self/range guard.
 */
void
cluster_gcs_block_send_pi_discard_invalidate(BufferTag tag, int32 target_node)
{
	GcsBlockInvalidatePayload inv;

	memset(&inv, 0, sizeof(inv));
	inv.request_id = 0; /* unsolicited: no broadcast slot, no ACK */
	inv.epoch = cluster_epoch_get_current();
	inv.tag = tag;
	inv.master_node = cluster_node_id;
	inv.invalidating_for_x_node = 0;
	inv.reserved_0[0] = GCS_BLOCK_INVALIDATE_KIND_PI_DISCARD;
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);
	cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_INVALIDATE,
										cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE,
																 target_node, &inv, sizeof(inv)));
}

/*
 * Master side: a durable-note for `tag` arrived (locally routed or via the
 * status-3 wire ride).  If the written pd_block_scn covers the SCN watermark
 * (the only cross-node comparable unit), collect + clear the PI holder
 * bitmap and direct every holder to drop its Past Image: self drops locally
 * (the wire cannot send to self, ㉕ precedent), remote holders get a
 * PI_DISCARD INVALIDATE ride (nowait, never ACKed).  Runs in LMON
 * dispatch/tick context — sends must not block.
 */
static void
gcs_block_pi_discard_master_apply(BufferTag tag, SCN written_scn)
{
	uint32 holders = 0;
	int n;

	if (!cluster_pcm_lock_pi_discard_collect(tag, written_scn, &holders))
		return;
	/* The durable-confirm retire HC130 anticipated (counter shared with the
	 * tag-lifecycle retire family). */
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->pi_watermark_retire_count, 1);

	for (n = 0; n < 32; n++) {
		if ((holders & ((uint32)1u << n)) == 0)
			continue;
		cluster_lever_h_note_discard_notify();
		if (n == cluster_node_id) {
			cluster_lever_h_note_discard_result(cluster_bufmgr_discard_pi_block(tag));
		} else {
			cluster_gcs_block_send_pi_discard_invalidate(tag, n);
		}
	}
}

/*
 * LMON tick: drain confirmed notes [drain_seq, confirmed_seq) and route each
 * to its master — locally when this node masters the tag, else as a status-3
 * durable-note ride (page_scn_bytes@52 = the written pd_block_scn;
 * request_id stays 0, unsolicited).  Bounded by the ring size per tick; only
 * LMON owns the tier-1 IC fds (L172 family).
 */
void
cluster_gcs_block_pi_discard_drain(void)
{
	bool data_plane;

	if (ClusterGcsBlock == NULL || !cluster_past_image)
		return;
	data_plane = cluster_gcs_block_family_on_data_plane();

	for (;;) {
		BufferTag tag;
		SCN page_scn;
		uint64 slot;
		int master_node;

		SpinLockAcquire(&ClusterGcsBlock->pi_note_lock);
		if (ClusterGcsBlock->pi_note_drain_seq >= ClusterGcsBlock->pi_note_confirmed_seq) {
			SpinLockRelease(&ClusterGcsBlock->pi_note_lock);
			break;
		}
		slot = ClusterGcsBlock->pi_note_drain_seq % CLUSTER_GCS_PI_NOTE_RING_SIZE;
		tag = ClusterGcsBlock->pi_note_ring[slot].tag;
		page_scn = ClusterGcsBlock->pi_note_ring[slot].page_scn;
		if (!data_plane)
			ClusterGcsBlock->pi_note_drain_seq++;
		SpinLockRelease(&ClusterGcsBlock->pi_note_lock);

		master_node = cluster_gcs_lookup_master(tag);
		if (data_plane) {
			GcsBlockInvalidateAckPayload note;
			int worker;

			if (master_node < 0 || master_node >= 32)
				break;
			memset(&note, 0, sizeof(note));
			note.request_id = 0; /* unsolicited: diverted before slot logic */
			note.epoch = cluster_epoch_get_current();
			note.tag = tag;
			note.sender_node = cluster_node_id;
			note.ack_status = GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE;
			GcsBlockInvalidateAckPayloadSetPageScn(&note, page_scn);
			note.checksum = gcs_block_compute_invalidate_ack_checksum(&note);
			worker = cluster_lms_shard_for_tag(&tag, cluster_lms_workers);
			if (!cluster_lms_outbound_enqueue(worker, PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK,
											  (uint32)master_node, &note, sizeof(note)))
				break;

			/* The staged ring owns a byte copy now.  Advancing afterwards gives
			 * at-least-once delivery across a crash between these two operations;
			 * a duplicate status-3 apply is idempotent.  Never take the outbound
			 * LWLock while holding this spinlock. */
			SpinLockAcquire(&ClusterGcsBlock->pi_note_lock);
			ClusterGcsBlock->pi_note_drain_seq++;
			SpinLockRelease(&ClusterGcsBlock->pi_note_lock);
			continue;
		}
		if (master_node == cluster_node_id) {
			gcs_block_pi_discard_master_apply(tag, page_scn);
		} else if (master_node >= 0 && master_node < 32) {
			GcsBlockInvalidateAckPayload note;

			memset(&note, 0, sizeof(note));
			note.request_id = 0; /* unsolicited: diverted before slot logic */
			note.epoch = cluster_epoch_get_current();
			note.tag = tag;
			note.sender_node = cluster_node_id;
			note.ack_status = GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE;
			GcsBlockInvalidateAckPayloadSetPageScn(&note, page_scn);
			note.checksum = gcs_block_compute_invalidate_ack_checksum(&note);
			cluster_gcs_block_note_send_outcome(
				GCS_BLOCK_SEND_FAMILY_INVALIDATE,
				cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, master_node, &note,
										 sizeof(note)));
		}
	}
}

/* ============================================================
 * PGRAC: spec-6.12a — LOCAL-master S->X upgrade with remote-S invalidate.
 *
 *	The backend-side PCM acquire loop (cluster_pcm_lock_acquire) is the
 *	LOCAL-master path: before spec-6.12a it had no cross-node invalidate
 *	and bounded-fail-closed on any live remote S holder (the spec-4.7a
 *	HG7 gate).  The quiescent X->S downgrade deliberately creates
 *	remote S holders, so the wave must close this gap: revoke every
 *	remote S copy, then upgrade self to X on the authoritative entry.
 *
 *	Sequence (backend context, master == self, caller holds NO buffer
 *	content lock):
 *	  1. pending_x barrier (HC117) so concurrent N->S readers back off.
 *	  2. Broadcast INVALIDATE to every remote S holder through the
 *	     backend outbound ring + collect acks on the shared slot (the
 *	     ack handler runs in LMON and only touches shmem + CV).
 *	  3. Every ack certifies that node dropped its copy and applied its
 *	     local S->N; clear its bit on the authoritative entry with an
 *	     explicit S_TO_N_INVALIDATE apply (idempotent when a release
 *	     raced ahead).
 *	  4. Now sole-S: apply S_TO_X_UPGRADE for self.
 *
 *	Any failure (slot busy / ack timeout / raced state) returns false
 *	with pending_x cleared; the caller keeps the pre-6.12a bounded
 *	fail-closed behaviour (Rule 8.A: never write past an unconfirmed
 *	invalidate).
 * ============================================================ */
bool
cluster_gcs_block_local_x_upgrade_ext(BufferTag tag, bool *out_busy)
{
	GcsBlockRequestPayload synth;
	PcmPendingXReserveResult reserve_result;
	uint32 holders_bm;
	uint32 self_bit;
	int n;
	bool upgraded = false;

	if (out_busy != NULL)
		*out_busy = false;

	if (ClusterGcsBlock == NULL || cluster_node_id < 0 || cluster_node_id >= 32)
		return false;
	self_bit = (uint32)1u << cluster_node_id;

	reserve_result
		= cluster_pcm_lock_set_pending_x(tag, cluster_node_id, (uint64)GetXLogInsertRecPtr());
	if (reserve_result != PCM_PENDING_X_RESERVE_OK) {
		if (out_busy != NULL && reserve_result == PCM_PENDING_X_RESERVE_OCCUPIED)
			*out_busy = true;
		return false;
	}

	/* pending_x is armed from here on: readers are being PENDING_X-denied.
	 * A throw anywhere below (cancel in the claim wait, an armed :error
	 * injection) must not leak it, or every reader of this tag starves
	 * behind a barrier nobody clears. */
	PG_TRY();
	{
		uint64 upgrade_epoch = cluster_epoch_get_current();
		bool covered = true;

		holders_bm = cluster_pcm_lock_query_s_holders_bitmap(tag) & ~self_bit;
		if (holders_bm != 0) {
			GcsInvalRoundOutcome outcome = GCS_INVAL_ROUND_TIMEOUT;

			memset(&synth, 0, sizeof(synth));
			/* PGRAC: spec-6.14a D1 — domain-tagged id (top bit = local-upgrade
			 * domain; holds for node 0 too).  See cluster_gcs_reqid.h. */
			synth.request_id = gcs_reqid_local_upgrade(
				cluster_node_id,
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->local_upgrade_request_seq, 1) + 1);
			synth.epoch = upgrade_epoch;
			synth.tag = tag;
			synth.sender_node = cluster_node_id;

			if (!gcs_block_broadcast_invalidate_and_wait_ext(&synth, holders_bm, true, &outcome)
				/* Exact-epoch fence (grant side): the acks certify drops at
				 * the epoch the broadcast ran under.  If the epoch moved at
				 * any point across this upgrade, the rebuilt S set may not
				 * be covered — fail closed, retryable (8.A stale-proof). */
				|| cluster_epoch_get_current() != upgrade_epoch) {
				/* Ruling ② review P1 — only a PURE busy round retries with
				 * backoff.  The OUTER epoch fence takes priority over BUSY
				 * too (review round 2): the epoch can move between the
				 * upgrade_epoch capture and the slot claim, in which case
				 * the round runs (and may collect a BUSY) entirely at the
				 * NEW epoch — the inner outcome then reads BUSY while the
				 * upgrade's own epoch premise is already dead.  Retrying
				 * that with backoff would spin against a fence; it must
				 * fail closed to the statement level like any epoch move.
				 * A dropped send / genuine timeout are lost-directive
				 * shapes counted as timeouts (never masked by BUSY). */
				if (outcome == GCS_INVAL_ROUND_BUSY
					&& cluster_epoch_get_current() == upgrade_epoch) {
					if (out_busy != NULL)
						*out_busy = true;
				} else if (outcome != GCS_INVAL_ROUND_EPOCH_STALE
						   && cluster_epoch_get_current() == upgrade_epoch)
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_timeout_count, 1);
				covered = false;
			} else {
				/* Acks certify the drops; clear the acked bits on the
				 * authoritative entry (idempotent vs racing releases). */
				for (n = 0; n < 32; n++) {
					if ((holders_bm & ((uint32)1u << n)) == 0)
						continue;
					(void)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_INVALIDATE,
																n);
				}
			}
		}

		if (covered)
			upgraded = cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_X_UPGRADE,
															 cluster_node_id);
	}
	PG_CATCH();
	{
		(void)cluster_pcm_lock_clear_pending_x_if(tag, cluster_node_id);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (upgraded)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->local_s_upgrade_grant_count, 1);
	(void)cluster_pcm_lock_clear_pending_x_if(tag, cluster_node_id);
	return upgraded;
}

/*
 * cluster_gcs_block_local_x_upgrade — ruling ② busy-retry wrapper.
 *
 *	A RETRYABLE_BUSY round is not a failure of the protocol, it is the
 *	holder saying "the thing blocking me is YOUR pending_x" — by the time
 *	the round aborted, pending_x was cleared (the _ext exit path), so the
 *	blocked acquire drains and an immediate short-backoff retry usually
 *	completes.  Each attempt mints a fresh request_id inside _ext (the new
 *	round identity: a late BUSY/ACK from an aborted round cannot match the
 *	next round's slot).  Genuine timeouts / epoch fences are NOT retried
 *	here — they stay fail-closed retryable at the statement level, the
 *	posture for lost packets and dead nodes.
 */
#define GCS_INVAL_BUSY_MAX_RETRIES 5
#define GCS_INVAL_BUSY_BACKOFF_BASE_US 2000L /* 2,4,8,16,32ms — 62ms total */

bool
cluster_gcs_block_local_x_upgrade(BufferTag tag)
{
	int attempt;

	for (attempt = 0; attempt <= GCS_INVAL_BUSY_MAX_RETRIES; attempt++) {
		bool round_busy = false;

		if (attempt > 0) {
			pg_usleep(GCS_INVAL_BUSY_BACKOFF_BASE_US << (attempt - 1));
			CHECK_FOR_INTERRUPTS();
		}
		if (cluster_gcs_block_local_x_upgrade_ext(tag, &round_busy))
			return true;
		if (!round_busy)
			return false;
	}
	return false; /* busy budget exhausted — caller fail-closes retryable */
}

/* ============================================================
 * PGRAC: spec-2.36 D4 — invalidate handler (holder side).
 *
 *	Receives PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE from master.  Validates
 *	epoch (HC100), looks up local buffer state, calls
 *	cluster_bufmgr_invalidate_block_for_gcs which:
 *	  - XLogFlush page_lsn (HC123: lost-write safety since no PI copy)
 *	  - InvalidateBuffer
 *	then applies PCM transition (S→N invalidate or X→N downgrade) and
 *	replies ACK msg_type 18 to master.
 *
 *	GCS serve-stall round-5 (A2): the drop is BOUNDED now.  A foreign
 *	pin no longer parks this dispatch worker in InvalidateBuffer's
 *	pin-wait loop (the measured 33-96s stall: the pin's holder is
 *	typically a backend waiting on a GCS reply only this worker can
 *	deliver — a circular wait the reply-wait timeout alone resolved).
 *	A PINNED drop parks the directive in a bounded per-worker lot and
 *	the LMS loop retries it each pass;  the ACK is sent only when the
 *	drop really happened (deny direction preserved — the master's ack
 *	budget fail-closes if the pin outlives it, exactly as an unreachable
 *	holder would).
 * ============================================================ */

/*
 * Break the mirror-N GRANT_PENDING side of the pin/INVALIDATE cycle without
 * touching another backend's ownership token.  A backend reserves its GCS
 * slot only after it has installed the descriptor's single GRANT_PENDING
 * reservation, so at most one live N->S slot can match this tag.  The current
 * authenticated master INVALIDATE is sufficient negative authority for that
 * exact attempt: publish a local DENIED_PENDING_X and let the owning backend
 * perform the established token-exact abort/rearm sequence.
 *
 * Direct-land attempts are deliberately excluded while their target is live;
 * their lane has its own LMON abort protocol.  A later INVALIDATE retry can
 * wake the slot after that protocol reaches ABORTED.  First-reply-wins remains
 * intact because reply_received is tested and set under the backend block's
 * exclusive lock.
 */
static bool
gcs_block_wake_local_pending_s_request(const GcsBlockInvalidatePayload *inv)
{
	int backend_idx;

	if (inv == NULL || gcs_block_backend_blocks == NULL || ClusterGcsBlock == NULL)
		return false;

	for (backend_idx = 0; backend_idx < MaxBackends; backend_idx++) {
		ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[backend_idx];
		int slot_idx;

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		for (slot_idx = 0; slot_idx < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; slot_idx++) {
			ClusterGcsBlockOutstandingSlot *slot = &blk->slots[slot_idx];
			GcsBlockReplyHeader *hdr;

			if (!GcsBlockLocalPendingSDenialMatches(
					slot->in_use, slot->reply_received, slot->stale, slot->transition_id,
					&slot->tag, slot->request_epoch, slot->expected_master_node, slot->direct_state,
					slot->direct_target_prepared, &inv->tag, inv->epoch, inv->master_node))
				continue;

			hdr = &slot->reply_header;
			memset(hdr, 0, sizeof(*hdr));
			hdr->request_id = slot->request_id;
			hdr->epoch = inv->epoch;
			hdr->sender_node = inv->master_node;
			hdr->requester_backend_id = backend_idx + 1;
			hdr->transition_id = slot->transition_id;
			hdr->status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
			GcsBlockReplyHeaderSetForwardingMasterNode(hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
			memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
			hdr->checksum = gcs_block_compute_checksum(slot->reply_block_data);
			slot->reply_sf_dep_valid = false;
			slot->reply_sf_flags = 0;
			cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
			slot->reply_undo_trailer_valid = false;
			slot->reply_undo_tt_generation = 0;
			slot->reply_undo_authority_scn = 0;
			slot->reply_received = true;
			ConditionVariableSignal(&slot->reply_cv);
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
			LWLockRelease(&blk->lock.lock);
			return true;
		}
		LWLockRelease(&blk->lock.lock);
	}
	return false;
}

/* Observation-only rejection bits for the closest local N->S request slot.
 * They must never feed protocol control flow; the authority predicate remains
 * GcsBlockLocalPendingSDenialMatches(). */
#define GCS_BLOCK_PENDING_OBS_NO_SLOT UINT32_C(0x001)
#define GCS_BLOCK_PENDING_OBS_REPLY UINT32_C(0x002)
#define GCS_BLOCK_PENDING_OBS_STALE UINT32_C(0x004)
#define GCS_BLOCK_PENDING_OBS_TRANSITION UINT32_C(0x008)
#define GCS_BLOCK_PENDING_OBS_TAG UINT32_C(0x010)
#define GCS_BLOCK_PENDING_OBS_EPOCH UINT32_C(0x020)
#define GCS_BLOCK_PENDING_OBS_MASTER UINT32_C(0x040)
#define GCS_BLOCK_PENDING_OBS_DIRECT_STATE UINT32_C(0x080)
#define GCS_BLOCK_PENDING_OBS_DIRECT_PREPARED UINT32_C(0x100)

typedef struct GcsBlockGrantPendingObservation {
	bool valid;
	uint64 invalidate_request_id;
	uint64 invalidate_epoch;
	BufferTag tag;
	int32 invalidate_master;
	int own_result;
	int buffer_id;
	uint8 own_state;
	uint64 own_generation;
	uint64 own_token;
	uint32 own_flags;
	bool woke_local;
	int slot_backend;
	int slot_index;
	uint64 slot_request_id;
	uint64 slot_epoch;
	int32 slot_master;
	uint8 slot_transition;
	bool slot_reply_received;
	bool slot_stale;
	int slot_direct_state;
	bool slot_direct_prepared;
	uint32 reject_mask;
} GcsBlockGrantPendingObservation;

/* LMON is single-threaded.  A tiny process-local cache suppresses identical
 * BUSY retries while retaining the first sighting and every identity/state
 * change for the exact INVALIDATE. */
static void
gcs_block_observe_grant_pending_invalidate(const GcsBlockInvalidatePayload *inv, bool woke_local)
{
	static GcsBlockGrantPendingObservation cache[8];
	static uint32 next_cache_slot = 0;
	GcsBlockGrantPendingObservation obs;
	ClusterPcmOwnSnapshot own;
	int backend_idx;
	int candidate_priority = 0;
	int cache_idx = -1;
	int i;

	if (inv == NULL)
		return;
	memset(&obs, 0, sizeof(obs));
	memset(&own, 0, sizeof(own));
	obs.valid = true;
	obs.invalidate_request_id = inv->request_id;
	obs.invalidate_epoch = inv->epoch;
	obs.tag = inv->tag;
	obs.invalidate_master = inv->master_node;
	obs.buffer_id = -1;
	obs.slot_backend = -1;
	obs.slot_index = -1;
	obs.slot_master = -1;
	obs.woke_local = woke_local;
	obs.own_result = (int)cluster_bufmgr_pcm_own_snapshot_by_tag(&inv->tag, &obs.buffer_id, &own);
	obs.own_state = own.pcm_state;
	obs.own_generation = own.generation;
	obs.own_token = own.reservation_token;
	obs.own_flags = own.flags;

	if (gcs_block_backend_blocks != NULL) {
		for (backend_idx = 0; backend_idx < MaxBackends; backend_idx++) {
			ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[backend_idx];
			int slot_idx;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			for (slot_idx = 0; slot_idx < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; slot_idx++) {
				ClusterGcsBlockOutstandingSlot *slot = &blk->slots[slot_idx];
				int priority;

				if (!slot->in_use)
					continue;
				priority = BufferTagsEqual(&slot->tag, &inv->tag) ? 2 : 1;
				if (priority <= candidate_priority)
					continue;
				candidate_priority = priority;
				obs.slot_backend = backend_idx;
				obs.slot_index = slot_idx;
				obs.slot_request_id = slot->request_id;
				obs.slot_epoch = slot->request_epoch;
				obs.slot_master = slot->expected_master_node;
				obs.slot_transition = slot->transition_id;
				obs.slot_reply_received = slot->reply_received;
				obs.slot_stale = slot->stale;
				obs.slot_direct_state = (int)slot->direct_state;
				obs.slot_direct_prepared = slot->direct_target_prepared;
				obs.reject_mask = 0;
				if (slot->reply_received)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_REPLY;
				if (slot->stale)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_STALE;
				if (slot->transition_id != (uint8)PCM_TRANS_N_TO_S)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_TRANSITION;
				if (!BufferTagsEqual(&slot->tag, &inv->tag))
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_TAG;
				if (slot->request_epoch != inv->epoch)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_EPOCH;
				if (slot->expected_master_node != inv->master_node)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_MASTER;
				if (slot->direct_state != GCS_BLOCK_DIRECT_UNARMED
					&& slot->direct_state != GCS_BLOCK_DIRECT_ABORTED)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_DIRECT_STATE;
				if (slot->direct_target_prepared)
					obs.reject_mask |= GCS_BLOCK_PENDING_OBS_DIRECT_PREPARED;
			}
			LWLockRelease(&blk->lock.lock);
			if (candidate_priority == 2)
				break;
		}
	}
	if (candidate_priority == 0)
		obs.reject_mask = GCS_BLOCK_PENDING_OBS_NO_SLOT;

	for (i = 0; i < lengthof(cache); i++) {
		if (cache[i].valid && cache[i].invalidate_request_id == obs.invalidate_request_id
			&& cache[i].invalidate_epoch == obs.invalidate_epoch
			&& cache[i].invalidate_master == obs.invalidate_master
			&& BufferTagsEqual(&cache[i].tag, &obs.tag)) {
			cache_idx = i;
			break;
		}
	}
	if (cache_idx >= 0 && memcmp(&cache[cache_idx], &obs, sizeof(obs)) == 0)
		return;
	if (cache_idx < 0) {
		cache_idx = (int)(next_cache_slot % lengthof(cache));
		next_cache_slot++;
	}
	cache[cache_idx] = obs;

	elog(LOG,
		 "cluster PCM grant-pending invalidate observation: invalidate_request_id=%llu "
		 "epoch=%llu master=%d x_requester=%u rel=%u fork=%d blk=%u own_result=%d "
		 "buffer=%d own_state=%u own_generation=%llu own_token=%llu own_flags=0x%x "
		 "woke_local=%d slot_backend=%d slot_index=%d slot_request_id=%llu "
		 "slot_epoch=%llu slot_master=%d slot_transition=%u reply_received=%d stale=%d "
		 "direct_state=%d direct_prepared=%d reject_mask=0x%x",
		 (unsigned long long)obs.invalidate_request_id, (unsigned long long)obs.invalidate_epoch,
		 obs.invalidate_master, (unsigned int)inv->invalidating_for_x_node, inv->tag.relNumber,
		 (int)inv->tag.forkNum, inv->tag.blockNum, obs.own_result, obs.buffer_id, obs.own_state,
		 (unsigned long long)obs.own_generation, (unsigned long long)obs.own_token, obs.own_flags,
		 obs.woke_local ? 1 : 0, obs.slot_backend, obs.slot_index,
		 (unsigned long long)obs.slot_request_id, (unsigned long long)obs.slot_epoch,
		 obs.slot_master, obs.slot_transition, obs.slot_reply_received ? 1 : 0,
		 obs.slot_stale ? 1 : 0, obs.slot_direct_state, obs.slot_direct_prepared ? 1 : 0,
		 obs.reject_mask);
}

/*
 * gcs_block_invalidate_execute — apply one INVALIDATE directive + ACK.
 *
 *	Returns true when the directive reached a terminal outcome (ACK
 *	sent);  false when the local copy was PINNED — nothing was changed,
 *	no ACK was sent, and the caller parks the directive for retry.
 */
static bool
gcs_block_invalidate_execute(const GcsBlockInvalidatePayload *inv)
{
	GcsBlockInvalidateAckPayload ack;
	PcmLockMode pre_state;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	SCN page_scn = InvalidScn; /* spec-2.41 D3 — ACK SCN carrier */
	uint8 ack_status = 0; /* OK */
	bool kept_pi = false; /* spec-6.12h D-h2 — drop converted to a PI */
	bool woke_local = false;
	uint64 current_epoch = cluster_epoch_get_current();

	if (inv->epoch != current_epoch) {
		ack_status = 1; /* epoch_stale */
		goto send_ack;
	}

	/*
	 * PGRAC: spec-6.12a ㉕ (latent-bug fix) — this handler runs on the
	 * HOLDER, so the residency question must be answered by the node-local
	 * buffer mirror, NOT cluster_pcm_lock_query: that reads the local MASTER
	 * hash table, which on a non-master holder has no entry and always
	 * answered N — every remote-holder INVALIDATE short-circuited to
	 * "already invalidated" while the cached S copy silently survived
	 * (Rule 8.A stale-S; masked pre-㉕ by the AD-015 phantom-harness
	 * divergence deferral, exposed by the ㉕ remote-downgrade S caches).
	 */
	pre_state = cluster_bufmgr_block_pcm_state(inv->tag);
	if (pre_state == PCM_LOCK_MODE_N) {
		/*
		 * PGRAC ownership-generation wave (W3) — do NOT treat N as
		 * already-invalidated while a grant for this tag is in flight to
		 * install (GRANT_PENDING).  The requester's install completes and its
		 * LockBuffer finalize then sets pcm_state=X; acking already_invalidated
		 * here would let the master clear this node's holder bit and re-grant X
		 * elsewhere, stranding the just-finalized stale X (double X holder,
		 * Rule 8.A).  A BUSY-capable master gets a negative ACK and retries;
		 * legacy peers use the round-5 park lot.  Either route re-runs only after
		 * the in-flight grant has finalized or its owner has aborted PENDING.
		 */
		if (cluster_bufmgr_block_grant_pending(inv->tag)) {
			cluster_pcm_note_invalidate_parked_grant_pending();
			/* The master-side denial may be queued behind this same-tag BUSY
			 * loop.  Deliver its exact local equivalent now so the reservation
			 * owner clears GRANT_PENDING before the next INVALIDATE retry. */
			woke_local = gcs_block_wake_local_pending_s_request(inv);
			gcs_block_observe_grant_pending_invalidate(inv, woke_local);
			/*
			 * Ruling ② — a BUSY-capable master gets the negative ACK RIGHT
			 * NOW instead of a silent park: it aborts the round (clears
			 * pending_x, releases the slot) and retries with a new round
			 * identity, which breaks the timeout-mediated loop (the
			 * GRANT_PENDING owner is typically an S acquire waiting on that
			 * very pending_x).  Nothing local changed; terminal for THIS
			 * directive.  An old master falls back to the round-5 park.
			 */
			if (inv->master_node == cluster_node_id
				|| cluster_sf_peer_supports_gcs_inval_busy(inv->master_node)) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_busy_sent_count, 1);
				ack_status = (uint8)GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY;
				goto send_ack;
			}
			return false;
		}
		ack_status = 2; /* already_invalidated */
		goto send_ack;
	}

	/*
	 * PGRAC: spec-4.7a invariant note (D2/D4).  With hold-until-revoked X
	 * (D2), pre_state can now be X here, which would drive an X->N downgrade
	 * whose remote release path (cluster_pcm_lock_release_buffer_for_eviction
	 * -> send_transition_and_wait) blocks on the very master that is
	 * invalidating us.  In 4.7a scope that case is UNREACHABLE: D4 bounded-
	 * fail-closes the only trigger that could make a peer acquire X while this
	 * node holds X (cross-node writer transfer), so a live X holder never
	 * receives an INVALIDATE; the S->X grant path uses S_TO_N_INVALIDATE on S
	 * holders only.  When the deferred writer-transfer (spec-2.36 / 4.7 /
	 * Stage 6) lands, this X branch goes live and must be hardened against the
	 * release-to-the-invalidating-master round-trip (codereview P2-1).
	 */
	switch (cluster_bufmgr_invalidate_block_for_gcs(inv->tag, pre_state, &page_lsn, &page_scn)) {
	case CLUSTER_BUFMGR_GCS_DROP_DROPPED: {
		PcmLockTransition trans = (pre_state == PCM_LOCK_MODE_X) ? PCM_TRANS_X_TO_N_DOWNGRADE
																 : PCM_TRANS_S_TO_N_INVALIDATE;
		(void)cluster_pcm_lock_apply_gcs_transition(inv->tag, trans, cluster_node_id);

		/* spec-2.41 D3: the dropping page's pd_block_scn is carried back in the
		 * ACK (replacing the spec-2.37 page_lsn carrier) and applied to the
		 * master's detector SCN watermark by the ACK handler.  Do not advance
		 * the holder-local HTAB: the master GrdEntry is the authoritative owner. */

		/* PGRAC: spec-6.12h D-h2 — the D-h1 conversion may have kept our
		 * dropped copy as a Past Image; flag it on the solicited ACK so the
		 * master records this node on the PI holder bitmap. */
		kept_pi = cluster_bufmgr_block_is_pi(inv->tag);

		/* PGRAC: spec-6.12h D-h3a — ordering pin (two-hop chain): the PI
		 * conversion (inside the invalidate above) samples its ship-SCN
		 * stamp before this ACK is sent below; the master observes the ACK
		 * envelope and only then clears our holder bit and grants X, and
		 * the upgrader observes the grant envelope — each observe is a
		 * strict Lamport bump (max+1), so every post-upgrade record sits
		 * strictly above the boundary (cluster_pi_shadow.h proof item 2). */
		break;
	}
	case CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT:
		ack_status = 2; /* race: not resident */
		break;
	case CLUSTER_BUFMGR_GCS_DROP_PINNED:
		/* FALLTHROUGH */
	case CLUSTER_BUFMGR_GCS_DROP_STALE:
		/* GCS serve-stall round-5 (A2): nothing changed, no ACK — the
		 * caller parks the directive and the LMS loop retries it.  STALE is
		 * unreachable here (the invalidate wrapper passes no expected_lsn, so
		 * its generation gate never fires); treated like PINNED defensively
		 * (round-6).
		 *
		 * Ruling ② — a BUSY-capable master gets the negative ACK instead
		 * (same rationale as the GRANT_PENDING arm above: the pin's holder
		 * is often itself waiting behind the master's pending_x, so parking
		 * only burns the master's timeout).  Nothing local changed.
		 */
		if (inv->master_node == cluster_node_id
			|| cluster_sf_peer_supports_gcs_inval_busy(inv->master_node)) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_busy_sent_count, 1);
			ack_status = (uint8)GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY;
			goto send_ack;
		}
		return false;
	}

send_ack:
	memset(&ack, 0, sizeof(ack));
	ack.request_id = inv->request_id;
	ack.epoch = inv->epoch;
	ack.tag = inv->tag;
	ack.sender_node = cluster_node_id;
	ack.ack_status = ack_status;
	/* PGRAC: spec-6.12h D-h2 — kept-PI report ride (checksum-covered). */
	ack.reserved_0[0] = kept_pi ? (uint8)GCS_BLOCK_INVALIDATE_ACK_KEPT_PI : (uint8)0;
	GcsBlockInvalidateAckPayloadSetPageScn(&ack, page_scn); /* spec-2.41 D3 — SCN carrier @52 */
	ack.checksum = gcs_block_compute_invalidate_ack_checksum(&ack);

	/*
	 * The generic IC path deliberately treats dest=self as a successful
	 * no-op.  That is not delivery for this application ACK: a resource
	 * master which is also an S holder must consume its own drop proof before
	 * it can advance the PCM-X transfer.  Stage only that local arm through
	 * the tag-sharded DATA ring; its LMS worker performs real loopback
	 * dispatch and preserves same-tag ordering.  Keep remote ACKs on their
	 * existing direct DATA connection.
	 */
	if (inv->master_node == cluster_node_id) {
		ClusterICSendResult local_result
			= cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK,
													   (uint32)inv->master_node, &ack, sizeof(ack))
				  ? CLUSTER_IC_SEND_DONE
				  : CLUSTER_IC_SEND_NOT_ADMITTED;

		cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_INVALIDATE, local_result);
	} else
		cluster_gcs_block_note_send_outcome(
			GCS_BLOCK_SEND_FAMILY_INVALIDATE,
			cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, inv->master_node, &ack,
									 sizeof(ack)));
	return true;
}

/*
 * GCS serve-stall round-5 (A2) — bounded per-worker parking lot for
 * PINNED invalidate directives.
 *
 *	Process-local (each LMS worker process owns its own lot — the shard
 *	router already pins a tag to exactly one worker).  Entries are
 *	deduped by tag (a master retransmit replaces the parked directive)
 *	and expire at the master's own ack budget — an expired entry is
 *	dropped WITHOUT an ACK, which is exactly the unreachable-holder
 *	shape the master's timeout machinery already fail-closes (counted,
 *	never silent).
 */
#define GCS_BLOCK_INVALIDATE_PARK_MAX 64

typedef struct GcsBlockParkedInvalidate {
	bool in_use;
	GcsBlockInvalidatePayload inv;
	TimestampTz deadline;
} GcsBlockParkedInvalidate;

static GcsBlockParkedInvalidate gcs_block_invalidate_park[GCS_BLOCK_INVALIDATE_PARK_MAX];

static void
gcs_block_invalidate_park_add(const GcsBlockInvalidatePayload *inv)
{
	int free_slot = -1;
	int i;
	TimestampTz deadline
		= GetCurrentTimestamp()
		  + (TimestampTz)cluster_gcs_block_invalidate_ack_timeout_ms * (TimestampTz)1000;

	for (i = 0; i < GCS_BLOCK_INVALIDATE_PARK_MAX; i++) {
		if (gcs_block_invalidate_park[i].in_use) {
			if (BufferTagsEqual(&gcs_block_invalidate_park[i].inv.tag, &inv->tag)) {
				/* master retransmit — the newer directive replaces ours */
				gcs_block_invalidate_park[i].inv = *inv;
				gcs_block_invalidate_park[i].deadline = deadline;
				return;
			}
		} else if (free_slot < 0)
			free_slot = i;
	}

	if (free_slot < 0) {
		static bool overflow_logged = false;

		if (ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_park_overflow_count, 1);
		if (!overflow_logged) {
			overflow_logged = true;
			ereport(LOG, (errmsg_internal("gcs block invalidate parking lot full; directive "
										  "dropped (master ack budget fail-closes)")));
		}
		return;
	}

	gcs_block_invalidate_park[free_slot].in_use = true;
	gcs_block_invalidate_park[free_slot].inv = *inv;
	gcs_block_invalidate_park[free_slot].deadline = deadline;
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_parked_count, 1);
}

/*
 * cluster_gcs_block_invalidate_park_tick — retry parked invalidates.
 *
 *	Called from the LMS worker loop each pass.  One bounded execute
 *	attempt per parked directive;  success (or any terminal outcome)
 *	frees the slot, a still-pinned entry stays until its deadline.
 */
void
cluster_gcs_block_invalidate_park_tick(void)
{
	TimestampTz now = 0;
	int i;

	for (i = 0; i < GCS_BLOCK_INVALIDATE_PARK_MAX; i++) {
		if (!gcs_block_invalidate_park[i].in_use)
			continue;

		if (gcs_block_invalidate_execute(&gcs_block_invalidate_park[i].inv)) {
			gcs_block_invalidate_park[i].in_use = false;
			continue;
		}

		if (now == 0)
			now = GetCurrentTimestamp();
		if (now >= gcs_block_invalidate_park[i].deadline) {
			gcs_block_invalidate_park[i].in_use = false;
			if (ClusterGcsBlock != NULL)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_park_expired_count, 1);
		}
	}
}

/*
 * cluster_gcs_block_test_deliver_self_invalidate — ownership-generation wave
 * (W3) test-only delivery shim.
 *
 *	Drives the REAL invalidate handler (gcs_block_invalidate_execute) with a
 *	synthetic same-tag directive from inside the LockBuffer grant-finalize
 *	window (armed via the cluster-pcm-grant-finalize-deliver-invalidate
 *	inject).  Rationale: the mis-ack race this exercises is real but not
 *	SQL-deterministic — a master INVALIDATE targets S-holders (bitmap), so
 *	it reaches a mirror-N node only through master/mirror asymmetry (e.g. a
 *	deferred eviction release) racing a fresh re-acquire; timing that from
 *	SQL is not deterministic.  The shim delivers the directive at the exact
 *	window point instead.  Same force-behavior inject pattern as
 *	cluster-gcs-block-duplicate-grant-reply / -stale-ship.
 *
 *	With GRANT_PENDING staged the handler refuses a positive ACK and bumps
 *	pcm.invalidate_parked_grant_pending_count.  Modern/self masters receive
 *	RETRYABLE_BUSY; legacy peers park.  The synthetic request id cannot match
 *	a real invalidate slot, so even this deterministic test arm cannot mutate
 *	master holder state.
 *
 *	Caller (bufmgr LockBuffer) holds the buffer's content lock; the handler's
 *	park path takes only the mapping partition (SHARED) + header spinlock —
 *	the same order the by-tag probes use from LMS context (no path acquires
 *	a content lock while holding a partition lock, so partition-under-content
 *	cannot invert).
 */
bool
cluster_gcs_block_test_deliver_self_invalidate(BufferTag tag)
{
	GcsBlockInvalidatePayload inv;

	memset(&inv, 0, sizeof(inv));
	inv.request_id = 0; /* synthetic; never reaches the ACK path */
	inv.epoch = cluster_epoch_get_current();
	inv.tag = tag;
	inv.master_node = cluster_node_id;
	return gcs_block_invalidate_execute(&inv);
}

static void
cluster_gcs_handle_block_invalidate_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockInvalidatePayload *inv = (const GcsBlockInvalidatePayload *)payload;
	uint64 current_epoch;
	uint64 source_session;

	if (gcs_block_try_resource_x_frame(env, payload))
		return;
	/* D16 inject — stall ack for timeout testing. */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-invalidate-stall-ack");
	if (cluster_injection_should_skip("cluster-gcs-block-invalidate-stall-ack"))
		return; /* never ack — master sees timeout */

	if (inv->checksum != gcs_block_compute_invalidate_checksum(inv))
		return;

	/*
	 * Review P0 (ownership-gen wave) — bind inv->master_node to the
	 * transport source.  The execute path sends the (possibly holder-state-
	 * mutating) ACK to inv->master_node and consults the BUSY capability of
	 * that node: a forged master_node would steer the drop proof to a node
	 * that never ran the broadcast (its slot logic drops it) while the REAL
	 * master times out and retries against an already-dropped copy.  Count
	 * via the dedup misroute counter family; drop without executing.
	 */
	if (inv->master_node != (int32)env->source_node_id) {
		cluster_gcs_block_dedup_note_misroute();
		return;
	}
	current_epoch = cluster_epoch_get_current();
	if (inv->epoch != current_epoch || cluster_gcs_lookup_master(inv->tag) != inv->master_node
		|| !gcs_block_pcm_x_authenticated_session(inv->master_node, current_epoch,
												  &source_session)) {
		cluster_gcs_block_dedup_note_misroute();
		return;
	}

	/*
	 * PGRAC: spec-7.3 D5 (review P2-1) — per-worker shard routing guard,
	 * INVALIDATE receive side.  Same invariant as the REQUEST dedup guard
	 * above: INVALIDATE is staged and routed by shard(tag) (payload_shard),
	 * so the receiving worker must be the routed worker.  A mismatch is a
	 * mis-route (per-tag order break, 8.A — an out-of-order invalidate
	 * could drop a copy a later grant relies on) that cannot happen without
	 * a code bug — fail closed (drop without ACK; the master's broadcast
	 * fail-closes on its own budget) rather than apply out of order.
	 */
	{
		int recv_worker = cluster_ic_tier1_my_data_channel();
		int tag_shard = cluster_lms_shard_for_tag(&inv->tag, cluster_lms_workers);

		Assert(tag_shard == recv_worker);
		if (tag_shard != recv_worker) {
			static bool misroute_logged = false;

			cluster_gcs_block_dedup_note_misroute();
			if (!misroute_logged) {
				misroute_logged = true;
				ereport(LOG,
						(errmsg_internal("gcs block invalidate misrouted to LMS worker %d (tag "
										 "shard %d); dropping (spec-7.3 P2-1 8.A fail-closed)",
										 recv_worker, tag_shard)));
			}
			return;
		}
	}

	/*
	 * PGRAC: spec-6.12h D-h2 — PI_DISCARD directive ride.  Strictly drops a
	 * BUF_TYPE_PI buffer (a live current copy is never touched — the strict
	 * check lives in cluster_bufmgr_discard_pi_block); unsolicited and NEVER
	 * ACKed (an ACK would hit the e2 slotless branch and clear an S bit this
	 * node may legitimately hold).  Off-epoch directives are dropped: the
	 * reconfig epoch bump owns cross-generation hygiene.
	 */
	if (inv->reserved_0[0] == GCS_BLOCK_INVALIDATE_KIND_PI_DISCARD) {
		if (inv->epoch == cluster_epoch_get_current())
			cluster_lever_h_note_discard_result(cluster_bufmgr_discard_pi_block(inv->tag));
		return;
	}

	/* GCS serve-stall round-5 (A2): a PINNED local copy parks the
	 * directive instead of spinning the dispatch pump (see the header
	 * note above). */
	if (!gcs_block_invalidate_execute(inv))
		gcs_block_invalidate_park_add(inv);
}


/* ============================================================
 * PGRAC: spec-2.36 D3 — invalidate ACK handler (master side).
 *
 *	HC100 stale-reply validation:  request_id MUST match the current
 *	in-flight broadcast slot;  tag MUST match;  epoch MUST match
 *	(stale rejected silently).  On valid ack, sets the sender's bit
 *	in invalidate_broadcast_acked_bm and broadcasts the CV.
 * ============================================================ */
static void
cluster_gcs_handle_block_invalidate_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockInvalidateAckPayload *ack = (const GcsBlockInvalidateAckPayload *)payload;
	uint64 current_req_id;
	uint64 expected_epoch;
	uint32 expected_bm;
	SCN ack_page_scn = InvalidScn; /* spec-2.41 D3 — ACK now carries pd_block_scn */
	BufferTag ack_tag = { 0 };
	bool valid = false;

	if (gcs_block_try_resource_x_frame(env, payload))
		return;
	if (ClusterGcsBlock == NULL)
		return;

	if (ack->checksum != gcs_block_compute_invalidate_ack_checksum(ack)) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
		return;
	}

	/*
	 * Review P0 (ownership-gen wave) — bind the payload identity to the
	 * TRANSPORT.  Every consumer below keys off ack->sender_node (the
	 * slotless S-bit clear, the PI notes, acked_bm, the BUSY abort): a
	 * mismatched sender could forge a drop proof for ANOTHER holder (the
	 * master clears that holder's bit / fills its acked_bm slot -> grants X
	 * against a copy that still exists, 8.A).  Same discipline as the DONE
	 * handler's F6 validator.  Count + drop; no state may change.
	 */
	if (ack->sender_node != (int32)env->source_node_id) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
		return;
	}

	/*
	 * PGRAC: spec-7.3 D5 (review P2-1) — per-worker shard routing guard,
	 * INVALIDATE-ACK receive side (master).  The ACK is direct-sent from
	 * the holder worker that received the INVALIDATE (worker[shard(tag)]),
	 * and worker channels pair i<->i across nodes, so a well-routed ACK
	 * always lands on this master's worker[shard(tag)].  A mismatch is a
	 * mis-route (per-tag order break, 8.A — an out-of-order ACK could
	 * certify a drop the holder has not applied yet) that cannot happen
	 * without a code bug — fail closed (drop; the broadcast fail-closes on
	 * its own budget) rather than apply out of order.
	 */
	{
		int recv_worker = cluster_ic_tier1_my_data_channel();
		int tag_shard = cluster_lms_shard_for_tag(&ack->tag, cluster_lms_workers);

		Assert(tag_shard == recv_worker);
		if (tag_shard != recv_worker) {
			static bool misroute_logged = false;

			cluster_gcs_block_dedup_note_misroute();
			if (!misroute_logged) {
				misroute_logged = true;
				ereport(LOG, (errmsg_internal("gcs block invalidate-ack misrouted to LMS worker %d "
											  "(tag shard %d); dropping (spec-7.3 P2-1 8.A "
											  "fail-closed)",
											  recv_worker, tag_shard)));
			}
			return;
		}
	}

	/*
	 * PGRAC: spec-6.12h D-h2 — unsolicited rides on the ACK wire, diverted
	 * BEFORE the e2 slotless branch and the HC100 slot logic (both reject
	 * status > 2).  Status 3 = a writer's durable-note (page_scn_bytes@52
	 * carries the written pd_block_scn — the only cross-node comparable
	 * version unit) -> retire watermarks + fan out PI_DISCARD.  Status 4 =
	 * a forwarded holder kept a Past Image -> record it on the bitmap.
	 * Off-epoch notes are dropped (fail-safe: the PI merely lingers).
	 */
	if (ack->ack_status == GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE) {
		if (ack->epoch == cluster_epoch_get_current()) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->pi_durable_note_apply_count, 1);
			gcs_block_pi_discard_master_apply(ack->tag,
											  GcsBlockInvalidateAckPayloadGetPageScn(ack));
		}
		return;
	}
	if (ack->ack_status == GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_KEPT_NOTE) {
		if (ack->epoch == cluster_epoch_get_current() && ack->sender_node >= 0
			&& ack->sender_node < 32)
			cluster_pcm_lock_pi_holder_note(ack->tag, ack->sender_node);
		return;
	}

	/*
	 * PGRAC ownership-generation wave (ruling ②) — RETRYABLE_BUSY(5),
	 * solicited negative ACK.  Diverted BEFORE the slotless S-bit clear (a
	 * BUSY holder changed NOTHING locally — crediting a drop would be a
	 * false proof) and BEFORE the HC100 slot logic (which rejects status>2
	 * as stale).  Full slot-identity validation under the slot lock — the
	 * same request_id + epoch + tag + expected-sender checks a positive ACK
	 * must pass — so a late BUSY from an older round cannot abort a newer
	 * round (round-identity ABA).  On a match: flag the slot busy and wake
	 * the waiter; it aborts the round (no acked_bm credit, no holder clear,
	 * no watermark advance, no X grant), clears pending_x, releases the
	 * slot and retries with a NEW round identity after a short backoff.
	 */
	if (ack->ack_status == GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY) {
		LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
		if (pg_atomic_read_u64(&ClusterGcsBlock->invalidate_broadcast_request_id) == ack->request_id
			&& ack->epoch == ClusterGcsBlock->invalidate_broadcast_epoch
			&& ClusterGcsBlock->invalidate_broadcast_epoch == cluster_epoch_get_current()
			&& BufferTagsEqual(&ack->tag, &ClusterGcsBlock->invalidate_broadcast_tag)
			&& ack->sender_node >= 0 && ack->sender_node < 32
			&& (pg_atomic_read_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm)
				& ((uint32)1u << ack->sender_node))
				   != 0) {
			pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_busy, 1);
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_busy_received_count, 1);
			LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
			ConditionVariableBroadcast(&ClusterGcsBlock->invalidate_broadcast_cv);
		} else {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
			LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		}
		return;
	}

	/*
	 * PGRAC: spec-6.12e2 (structural fix) — clear the sender's S bit on the
	 * authoritative entry FIRST, independent of the broadcast-slot match
	 * below.  The LMON wire-request S-branch fires its INVALIDATEs without
	 * sleeping (sleeping in the dispatch loop would deadlock on the ACKs
	 * this very loop drains), so by the time an ACK lands no slot is
	 * claimed and the slot logic would drop it as stale — leaving the S
	 * bit set forever and every requester retry re-invalidating.  The
	 * holder self-reports that it no longer caches the block (status 0 =
	 * dropped, 2 = not resident); per-peer IC streams are FIFO, so an S
	 * the holder re-acquires later (a REQUEST that follows this ACK on the
	 * same stream, granted by this master) cannot be clobbered by this
	 * earlier ACK.  Epoch must equal the current epoch (reconfig fences
	 * stale reports); the transition apply itself early-returns when the
	 * bit is already clear (idempotent).
	 */
	if ((ack->ack_status == 0 || ack->ack_status == 2) && ack->epoch == cluster_epoch_get_current()
		&& ack->sender_node >= 0 && ack->sender_node < 32) {
		(void)cluster_pcm_lock_apply_gcs_transition(
			ack->tag, PCM_TRANS_S_TO_N_INVALIDATE, ack->sender_node);
		/* PGRAC: spec-6.12h D-h2 — the holder reported its dropped copy was
		 * kept as a Past Image (D-h1); record it on the PI holder bitmap so
		 * the discard protocol can target it later.  Runs here (before the
		 * slot logic) so both the slotless e2 fan-out and the slot-claimed
		 * blocking broadcast get the report. */
		if (ack->ack_status == 0 && ack->reserved_0[0] == (uint8)GCS_BLOCK_INVALIDATE_ACK_KEPT_PI)
			cluster_pcm_lock_pi_holder_note(ack->tag, ack->sender_node);
		/* spec-2.41 D3 parity — the nowait fan-out has no slot-valid branch
		 * below, so feed the detector SCN watermark here too (monotonic max;
		 * skipping it would under-advance the lost-write expectation).  Skip
		 * when the ACK matches the claimed slot: the slot-valid branch below
		 * advances for the blocking (backend) sender, and advancing twice
		 * would double-count the observability counter. */
		if (ack->ack_status == 0
			&& pg_atomic_read_u64(&ClusterGcsBlock->invalidate_broadcast_request_id)
				   != ack->request_id) {
			SCN pre_scn = GcsBlockInvalidateAckPayloadGetPageScn(ack);

			if (SCN_VALID(pre_scn)) {
				cluster_pcm_lock_pi_watermark_scn_advance(
					ack->tag, pre_scn, CLUSTER_PCM_WM_SRC_ACK_SLOTLESS, ack->sender_node,
					ack->request_id, ack->epoch);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->pi_watermark_advance_count, 1);
			}
		}
	}
	LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
	current_req_id = pg_atomic_read_u64(&ClusterGcsBlock->invalidate_broadcast_request_id);
	if (current_req_id != ack->request_id) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
		LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		return;
	}

	expected_epoch = ClusterGcsBlock->invalidate_broadcast_epoch;
	/* Exact-epoch fence #2: the ACK must match the slot's epoch AND the slot
	 * epoch must still be the CURRENT epoch.  An ACK produced before a
	 * reconfiguration can arrive after it; the slot identity (stamped at the
	 * same old epoch) would match, and the old-epoch drop proof would fill
	 * the bitmap toward a new-epoch X grant (8.A stale-proof).  The slotless
	 * e2 branch above already carries the same current-epoch requirement. */
	if (ack->epoch != expected_epoch || expected_epoch != cluster_epoch_get_current()
		|| !BufferTagsEqual(&ack->tag, &ClusterGcsBlock->invalidate_broadcast_tag)
		|| ack->ack_status > 2 || ack->ack_status == 1) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
		LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		return;
	}

	expected_bm = pg_atomic_read_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm);
	if (ack->sender_node < 0 || ack->sender_node >= 32) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
		LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		return;
	}
	if ((expected_bm & ((uint32)1u << ack->sender_node)) == 0) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
		LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		return;
	}

	if (ack->ack_status == 0) {
		ack_page_scn = GcsBlockInvalidateAckPayloadGetPageScn(ack);
		ack_tag = ack->tag;
	}

	pg_atomic_fetch_or_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm,
						   (uint32)1u << ack->sender_node);
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_ack_received_count, 1);
	valid = true;
	LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);

	if (valid) {
		if (SCN_VALID(ack_page_scn)) {
			/* spec-2.41 D3 — the invalidate-ACK now carries the dropping page's
			 * pd_block_scn (@52, reinterpreted from the spec-2.37 page_lsn slot);
			 * advance the detector's SCN watermark.  The redo-coverage LSN
			 * watermark is NOT fed from the ACK: recovery rebuilds it from the
			 * REDECLARE wire (§2.8.2; the F-ACK test at D9 proves this is safe). */
			cluster_pcm_lock_pi_watermark_scn_advance(ack_tag, ack_page_scn,
													  CLUSTER_PCM_WM_SRC_ACK_SLOT, ack->sender_node,
													  ack->request_id, ack->epoch);
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->pi_watermark_advance_count, 1);
		}
		ConditionVariableBroadcast(&ClusterGcsBlock->invalidate_broadcast_cv);
	}
}


/* PGRAC: spec-7.2 flip — DATA plane (see the dispatch-table comment). */
static const ClusterICMsgTypeInfo gcs_block_invalidate_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE,
	.name = "gcs_block_invalidate",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_invalidate_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo gcs_block_invalidate_ack_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK,
	.name = "gcs_block_invalidate_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_invalidate_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};


/*
 * cluster_gcs_block_send_redeclare -- spec-4.7 D2 survivor → master re-declare.
 *
 *	One fire-and-forget announce of a locally-held S/X buffer to the block's
 *	current (remastered) master.  Self-mastered blocks need no wire (their
 *	master state rebuilds locally — D3 lazy rebuild), so skip master == self.
 */
void
cluster_gcs_block_send_redeclare(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn, SCN page_scn,
								 uint64 cluster_epoch, int master_node)
{
	GcsBlockRedeclarePayload p;

	if (master_node < 0 || master_node == cluster_node_id)
		return;

	memset(&p, 0, sizeof(p));
	p.cluster_epoch = cluster_epoch;
	p.tag = tag;
	GcsBlockRedeclarePayloadSetPageLsn(&p, page_lsn); /* redo-coverage required_lsn */
	GcsBlockRedeclarePayloadSetPageScn(&p, page_scn); /* spec-2.41 D3 — detector SCN */
	p.holder_node_id = cluster_node_id;
	p.held_mode = held_mode;
	p.checksum = gcs_block_compute_redeclare_checksum(&p);

	cluster_gcs_block_note_send_outcome(
		GCS_BLOCK_SEND_FAMILY_INVALIDATE,
		cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REDECLARE, master_node, &p, sizeof(p)));
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_buffers_redeclared, 1);
}


/*
 * cluster_gcs_handle_block_redeclare_envelope -- spec-4.7 D2 master-side recv.
 *
 *	Validate (checksum + episode epoch + sender identity + mode), then rebuild
 *	the minimal block-resource view.  Fire-and-forget: every failure is a
 *	silent drop (the survivor re-sends next reconfig tick) — never a partial
 *	or off-epoch rebuild (L235/L236: only the current accepted episode epoch
 *	is trusted;  a stale or mid-episode-bumped declare is dropped).
 */
void
cluster_gcs_handle_block_redeclare_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockRedeclarePayload *p = (const GcsBlockRedeclarePayload *)payload;
	uint64 episode_epoch;

	if (ClusterGcsBlock == NULL)
		return;

	if (p->checksum != gcs_block_compute_redeclare_checksum(p)) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_stale_block_drop, 1);
		return;
	}

	/* L235/L236 epoch-coherent gate: trust only the master's current accepted
	 * episode epoch (locally-tracked, not a fresh event read). */
	episode_epoch = cluster_grd_redeclare_episode_epoch();
	if (p->cluster_epoch != episode_epoch) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_stale_block_drop, 1);
		return;
	}

	/* Anti-spoof: the declared holder must be the envelope's source node. */
	if (p->holder_node_id < 0 || p->holder_node_id >= 32
		|| (int32)env->source_node_id != p->holder_node_id) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_stale_block_drop, 1);
		return;
	}

	if (p->held_mode != (uint8)PCM_STATE_S && p->held_mode != (uint8)PCM_STATE_X) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_stale_block_drop, 1);
		return;
	}

	if (cluster_gcs_block_master_rebuild_from_redeclare(
			p->tag, p->held_mode, GcsBlockRedeclarePayloadGetPageLsn(p),
			GcsBlockRedeclarePayloadGetPageScn(p), p->holder_node_id, p->cluster_epoch))
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_block_state_rebuilt, 1);
	else
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->recovery_ambiguous_owner_failclosed, 1);
}


/* PGRAC: spec-7.2 flip (r4) — REDECLARE stays on the CONTROL plane:
 * survivor re-declare and its DONE barrier belong to the LMON-owned
 * recovery episode and must not depend on the DATA mesh being up.
 * None of the five migrated handlers emits REDECLARE (D0-①b audit),
 * so no cross-plane staging leg is required here. */
static const ClusterICMsgTypeInfo gcs_block_redeclare_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REDECLARE,
	.name = "gcs_block_redeclare",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_redeclare_envelope,
	.plane = CLUSTER_IC_PLANE_CONTROL,
};


void
cluster_gcs_register_block_msg_types(void)
{
	int i;

	for (i = 0; i < (int)lengthof(legacy_pcm_x_stale_infos); i++)
		cluster_ic_register_msg_type(&legacy_pcm_x_stale_infos[i]);
	cluster_ic_register_msg_type(&gcs_block_request_info);
	cluster_ic_register_msg_type(&gcs_block_reply_info);
	cluster_ic_register_msg_type(&gcs_block_done_info);
	cluster_ic_register_msg_type(&gcs_block_forward_info);
	cluster_ic_register_msg_type(&gcs_block_invalidate_info);
	cluster_ic_register_msg_type(&gcs_block_invalidate_ack_info);
	cluster_ic_register_msg_type(&gcs_block_redeclare_info);
}


/* ============================================================
 * Observability accessors (dump_gcs +8 NEW rows for block plane).
 * ============================================================ */

uint64
cluster_gcs_get_block_request_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_request_count) : 0;
}

uint64
cluster_gcs_get_block_reply_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_reply_count) : 0;
}

uint64
cluster_gcs_get_block_timeout_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_timeout_count) : 0;
}

uint64
cluster_gcs_get_block_checksum_fail_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_checksum_fail_count) : 0;
}

uint64
cluster_gcs_get_block_storage_fallback_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_storage_fallback_count) : 0;
}

uint64
cluster_gcs_get_block_master_not_holder_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_master_not_holder_count)
						   : 0;
}

uint64
cluster_gcs_get_block_wal_flush_before_ship_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count)
						   : 0;
}

uint64
cluster_gcs_get_block_ship_bytes_total(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_ship_bytes_total) : 0;
}

/* PGRAC: spec-7.2 D6 — ship-latency histogram accessors (dump + tests). */
uint64
cluster_gcs_block_ship_hist_bound_us(int bucket)
{
	if (bucket < 0 || bucket >= CLUSTER_GCS_SHIP_HIST_BUCKETS)
		return 0;
	if (bucket == CLUSTER_GCS_SHIP_HIST_BUCKETS - 1)
		return UINT64_MAX; /* +inf overflow bucket */
	return gcs_ship_hist_bounds_us[bucket];
}

uint64
cluster_gcs_block_ship_hist_count(int bucket)
{
	if (ClusterGcsBlock == NULL || bucket < 0 || bucket >= CLUSTER_GCS_SHIP_HIST_BUCKETS)
		return 0;
	return pg_atomic_read_u64(&ClusterGcsBlock->ship_latency_hist[bucket]);
}

/* PGRAC: spec-6.13 D8 — RDMA tier3/direct-land copy observability. */
uint64
cluster_gcs_get_scratch_copy_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->scratch_copy_count) : 0;
}

uint64
cluster_gcs_get_live_sge_send_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->live_sge_send_count) : 0;
}

uint64
cluster_gcs_get_live_sge_fallback_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->live_sge_fallback_count) : 0;
}

uint64
cluster_gcs_get_direct_install_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->direct_install_count) : 0;
}

uint64
cluster_gcs_get_direct_install_abort_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->direct_install_abort_count) : 0;
}

uint64
cluster_gcs_get_install_copy_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->install_copy_count) : 0;
}


/* ============================================================
 * PGRAC: spec-2.34 D1 — 9 NEW reliability counter accessors.
 *
 *	5 sender/wake counters live in ClusterGcsBlockShared;  4 dedup-side
 *	counters (hit/miss/collision/full) are forwarded from
 *	cluster_gcs_block_dedup.c so dump_gcs sees one unified set.
 * ============================================================ */

uint64
cluster_gcs_get_block_retransmit_attempt_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->retransmit_attempt_count) : 0;
}

uint64
cluster_gcs_get_block_retransmit_send_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->retransmit_send_count) : 0;
}

uint64
cluster_gcs_get_block_retransmit_exhausted_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->retransmit_exhausted_count) : 0;
}

uint64
cluster_gcs_get_block_epoch_invalidate_wake_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->epoch_invalidate_wake_count) : 0;
}

uint64
cluster_gcs_get_block_stale_reply_drop_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->stale_reply_drop_count) : 0;
}

/* PGRAC: GCS-race round-2 RC-F — requester-side DONE emission counter. */
uint64
cluster_gcs_get_block_done_sent_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->done_sent_count) : 0;
}

uint64
cluster_gcs_get_block_done_enqueue_drop_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->done_enqueue_drop_count) : 0;
}

/* PGRAC: GCS serve-stall round-5 — 6 per-family send admission accessors. */
uint64
cluster_gcs_get_reply_send_queued_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->reply_send_queued_count) : 0;
}

uint64
cluster_gcs_get_reply_send_not_admitted_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->reply_send_not_admitted_count)
						   : 0;
}

uint64
cluster_gcs_get_forward_send_queued_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->forward_send_queued_count) : 0;
}

uint64
cluster_gcs_get_forward_send_not_admitted_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->forward_send_not_admitted_count)
						   : 0;
}

uint64
cluster_gcs_get_invalidate_send_queued_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_send_queued_count) : 0;
}

uint64
cluster_gcs_get_invalidate_send_not_admitted_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_send_not_admitted_count)
			   : 0;
}

/* PGRAC ownership-generation wave (ruling ②): RETRYABLE_BUSY accessors. */
uint64
cluster_gcs_get_invalidate_busy_sent_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_busy_sent_count) : 0;
}

uint64
cluster_gcs_get_invalidate_busy_received_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_busy_received_count)
						   : 0;
}

uint64
cluster_gcs_get_invalidate_passive_s_release_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_passive_s_release_count)
			   : 0;
}

uint64
cluster_gcs_get_pcm_x_self_handoff_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->pcm_x_self_handoff_count) : 0;
}

uint64
cluster_gcs_get_pcm_x_self_handoff_drain_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->pcm_x_self_handoff_drain_count)
						   : 0;
}

/* PGRAC: GCS serve-stall round-5 A2 — 4 bounded-drop accessors. */
uint64
cluster_gcs_get_invalidate_parked_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_parked_count) : 0;
}

uint64
cluster_gcs_get_invalidate_park_expired_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_park_expired_count)
						   : 0;
}

uint64
cluster_gcs_get_invalidate_park_overflow_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->invalidate_park_overflow_count)
						   : 0;
}

uint64
cluster_gcs_get_drop_pinned_deny_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->drop_pinned_deny_count) : 0;
}

uint64
cluster_gcs_get_xfer_stale_deny_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->xfer_stale_deny_count) : 0;
}

/* PGRAC: GCS-race round-4c FUNC-1 — 3 storage-fallback verify accessors. */
uint64
cluster_gcs_get_fallback_scn_verify_pass_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->fallback_scn_verify_pass_count)
						   : 0;
}

uint64
cluster_gcs_get_fallback_scn_refresh_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->fallback_scn_refresh_count) : 0;
}

uint64
cluster_gcs_get_fallback_scn_failclosed_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->fallback_scn_failclosed_count)
						   : 0;
}

/* PGRAC: spec-2.35 D12 — 7 NEW counter accessors. */
uint64
cluster_gcs_get_block_forward_sent_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_forward_sent_count) : 0;
}

uint64
cluster_gcs_get_block_forward_received_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_forward_received_count) : 0;
}

uint64
cluster_gcs_get_block_from_holder_ship_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_from_holder_ship_count) : 0;
}

uint64
cluster_gcs_get_block_forward_holder_evicted_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->block_forward_holder_evicted_count)
			   : 0;
}

uint64
cluster_gcs_get_block_s_holders_bitmap_redirect_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->s_holders_bitmap_redirect_count)
						   : 0;
}

uint64
cluster_gcs_get_block_master_holder_lifecycle_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->master_holder_lifecycle_count)
						   : 0;
}

uint64
cluster_gcs_get_block_forward_replay_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->forward_replay_count) : 0;
}

/* PGRAC: spec-2.36 D10 — 6 NEW counter accessors for CF 3-way protocol. */
uint64
cluster_gcs_get_block_invalidate_broadcast_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_invalidate_broadcast_count)
						   : 0;
}

uint64
cluster_gcs_get_block_invalidate_ack_received_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->block_invalidate_ack_received_count)
			   : 0;
}

uint64
cluster_gcs_get_block_invalidate_timeout_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_invalidate_timeout_count)
						   : 0;
}

/* PGRAC: spec-6.14a D5 — 3 NEW counter accessors for the X-vs-S arms. */
uint64
cluster_gcs_get_local_s_upgrade_grant_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->local_s_upgrade_grant_count) : 0;
}

uint64
cluster_gcs_get_x_vs_s_nonholder_grant_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->x_vs_s_nonholder_grant_count) : 0;
}

uint64
cluster_gcs_get_x_vs_s_no_carrier_denied_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->x_vs_s_no_carrier_denied_count)
						   : 0;
}

uint64
cluster_gcs_get_block_x_forward_sent_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_x_forward_sent_count) : 0;
}

uint64
cluster_gcs_get_block_x_granted_from_holder_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_x_granted_from_holder_count)
						   : 0;
}

uint64
cluster_gcs_get_starvation_denied_pending_x_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->starvation_denied_pending_x_count)
						   : 0;
}

/* PGRAC: spec-2.37 D12 — 4 NEW counter accessors for PI watermark + lost-write. */
uint64
cluster_gcs_get_pi_watermark_advance_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->pi_watermark_advance_count) : 0;
}

uint64
cluster_gcs_get_pi_watermark_retire_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->pi_watermark_retire_count) : 0;
}

uint64
cluster_gcs_get_pi_durable_note_apply_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->pi_durable_note_apply_count) : 0;
}

uint64
cluster_gcs_get_lost_write_detected_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->lost_write_detected_count) : 0;
}

uint64
cluster_gcs_get_lost_write_avoid_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->lost_write_avoid_count) : 0;
}

/* PGRAC: branch-1 master-direct storage-fallback rescue accessor. */
uint64
cluster_gcs_get_lost_write_master_direct_storage_fallback_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(
								 &ClusterGcsBlock->lost_write_master_direct_storage_fallback_count)
						   : 0;
}

/* PGRAC: spec-2.41 D7 — SCN detector + redo-coverage observability accessors. */
uint64
cluster_gcs_get_lost_write_invalidscn_failclosed_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->lost_write_invalidscn_failclosed_count)
			   : 0;
}

uint64
cluster_gcs_get_lost_write_not_scn_tracked_skip_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->lost_write_not_scn_tracked_skip_count)
			   : 0;
}

uint64
cluster_gcs_get_redo_coverage_required_lsn_zero_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->redo_coverage_required_lsn_zero_count)
			   : 0;
}

uint64
cluster_gcs_get_redo_coverage_gate_block_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->redo_coverage_gate_block_count)
						   : 0;
}

uint64
cluster_gcs_get_cf_xheld_read_ship_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->cf_xheld_read_ship_count) : 0;
}

/* PGRAC: spec-5.2a D6 — clean-page X-transfer enabler observability (5). */
uint64
cluster_gcs_get_clean_page_xfer_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->clean_page_xfer_count) : 0;
}

uint64
cluster_gcs_get_clean_page_xfer_storage_fallback_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->clean_page_xfer_storage_fallback_count)
			   : 0;
}

uint64
cluster_gcs_get_clean_page_xfer_fail_closed_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->clean_page_xfer_fail_closed_count)
						   : 0;
}

uint64
cluster_gcs_get_clean_page_xfer_stale_holder_recover_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->clean_page_xfer_stale_holder_recover_count)
			   : 0;
}

uint64
cluster_gcs_get_clean_page_xfer_third_party_denied_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->clean_page_xfer_third_party_denied_count)
			   : 0;
}

/* PGRAC: spec-5.2 D11 path A — writer-transfer-revoke ship+release counter. */
uint64
cluster_gcs_get_block_x_transfer_ship_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_x_transfer_ship_count) : 0;
}

/* PGRAC: spec-5.2 D11 path B — master==holder self-ship X counter. */
uint64
cluster_gcs_get_block_x_self_ship_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_x_self_ship_count) : 0;
}

/*
 * PGRAC: spec-5.2 D2 — gcs_block_xheld_read_ship_decision() is a pure
 * static-inline helper in cluster_gcs_block.h (unit-tested standalone, U3).
 */

/* PGRAC: spec-4.7 D6 — 8 warm-recovery observability accessors (dump category
 * 'gcs_recovery'). */
uint64
cluster_gcs_get_recovery_block_resources_recovering(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_block_resources_recovering)
			   : 0;
}

uint64
cluster_gcs_get_recovery_buffers_redeclared(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_buffers_redeclared) : 0;
}
uint64
cluster_gcs_get_recovery_block_state_rebuilt(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_block_state_rebuilt) : 0;
}
uint64
cluster_gcs_get_recovery_redo_boundary_waits(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_redo_boundary_waits) : 0;
}
uint64
cluster_gcs_get_recovery_redo_boundary_reached(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_redo_boundary_reached)
						   : 0;
}
uint64
cluster_gcs_get_recovery_stale_block_drop(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_stale_block_drop) : 0;
}
uint64
cluster_gcs_get_recovery_ambiguous_owner_failclosed(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_ambiguous_owner_failclosed)
			   : 0;
}
uint64
cluster_gcs_get_recovery_before_boundary_failclosed(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->recovery_before_boundary_failclosed)
			   : 0;
}

/* PGRAC: spec-2.35 D3 (HC110) — extern bump for master_holder lifecycle. */
void
cluster_gcs_block_bump_master_holder_lifecycle(void)
{
	if (ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->master_holder_lifecycle_count, 1);
}

uint64
cluster_gcs_get_block_dedup_hit_count(void)
{
	return cluster_gcs_block_dedup_get_hit_count();
}

uint64
cluster_gcs_get_block_dedup_miss_count(void)
{
	return cluster_gcs_block_dedup_get_miss_count();
}

uint64
cluster_gcs_get_block_dedup_collision_count(void)
{
	return cluster_gcs_block_dedup_get_collision_count();
}

uint64
cluster_gcs_get_block_dedup_full_count(void)
{
	return cluster_gcs_block_dedup_get_full_count();
}

/*
 * spec-7.2a D5:  dedup capacity/occupancy observability wrappers.  The
 * entry_count wrapper reads the historical _get_in_flight_count accessor,
 * whose backing counter (entry_count) tracks every live entry (in-flight
 * slots plus completed cached replies) -- that live total is what dump_gcs
 * surfaces as dedup_entry_count for the saturation ratio.
 */
uint64
cluster_gcs_get_block_dedup_entry_count(void)
{
	return cluster_gcs_block_dedup_get_in_flight_count();
}

uint64
cluster_gcs_get_block_dedup_evict_count(void)
{
	return cluster_gcs_block_dedup_get_evict_count();
}

uint64
cluster_gcs_get_block_dedup_max_entries(void)
{
	return cluster_gcs_block_dedup_get_max_entries();
}

/* PGRAC: GCS-race round-2 RC-F — master-side DONE consumption counters. */
uint64
cluster_gcs_get_block_dedup_done_marked_count(void)
{
	return cluster_gcs_block_dedup_get_done_marked_count();
}

uint64
cluster_gcs_get_block_dedup_done_mismatch_count(void)
{
	return cluster_gcs_block_dedup_get_done_mismatch_count();
}

uint64
cluster_gcs_get_block_dedup_hint_violation_count(void)
{
	return cluster_gcs_block_dedup_get_hint_violation_count();
}

uint64
cluster_gcs_get_block_dedup_legacy_pin_count(void)
{
	return cluster_gcs_block_dedup_get_legacy_pin_count();
}

/* R8 D8-3/D8-6: close the target Resource-X formation before the existing
 * epoch wake becomes visible.  Each sweep call is fixed at four slots and a
 * checkpoint performs at most sixteen calls (64 slots).  The coordinator may
 * need several interruptible checkpoints, but never publishes the epoch from
 * this callback until an exact complete wrap has thawed the new formation. */
#define GCS_BLOCK_RESOURCE_X_RECONFIG_PROBE_BUDGET 4
#define GCS_BLOCK_RESOURCE_X_RECONFIG_CALLS_PER_CHECKPOINT 16

/* R11 D11-09: the source-removed build retains the native R8/R10 cutover
 * owner.  One LMON tick advances at most 64 registry slots.  A true return
 * requests one cooperative event-loop reschedule only after actual bounded
 * progress or a completed state transition; no-progress and retry paths use
 * the ordinary timer.  This never revives the retired PCM-X runtime, ticket
 * authority, or worker family. */
bool
cluster_gcs_block_resource_x_cutover_tick(void)
{
	ClusterSemanticR11CutoverSnapshot cutover;
	ResourceXCleanCompletionProof clean;
	ResourceXCleanCompletionProof observed_clean;
	ResourceXGateSnapshot gate;
	ResourceXReconfigBatch batch;
	ResourceXReconfigResult result = RESOURCE_X_RECONFIG_RETRY;
	ResourceXReconfigToken observed_token;
	ResourceXReconfigToken token;
	ResourceXZeroResidualProof observed_zero;
	ResourceXZeroResidualProof zero;
	uint32 calls;

	if (!cluster_semantic_activation_r11_cutover_snapshot(&cutover))
		return false;
	if (cutover.record_generation == 0
		|| cutover.record_generation == UINT64_MAX
		|| cutover.formation_epoch == UINT64_MAX)
		return false;
	if (cutover.phase == CLUSTER_SEMANTIC_R11_CUTOVER_TARGET_OPEN)
		return false;
	if (!cluster_pcm_lock_resource_x_gate_snapshot(&gate)) {
		if (cutover.phase
				!= CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED
			|| !cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
				&gate))
			return false;
	}
	if (gate.reserved != 0)
		return false;

	if (cutover.phase == CLUSTER_SEMANTIC_R11_CUTOVER_SOURCE_CLOSED) {
		if ((gate.phase != RESOURCE_X_GATE_OPEN
			 && gate.phase != RESOURCE_X_GATE_FROZEN)
			|| !cluster_resource_x_reconfig_cutover_begin_native_exact(
				&token)
			|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
			|| gate.phase != RESOURCE_X_GATE_FROZEN
			|| gate.formation != token.old_formation
			|| gate.freeze_generation != token.freeze_generation)
			return false;

		if (token.new_formation == 0) {
			for (calls = 0;
				 calls < GCS_BLOCK_RESOURCE_X_RECONFIG_CALLS_PER_CHECKPOINT;
				 calls++) {
				result = cluster_resource_x_reconfig_sweep(
					&token, GCS_BLOCK_RESOURCE_X_RECONFIG_PROBE_BUDGET,
					&batch);
				if (result == RESOURCE_X_RECONFIG_DONE)
					break;
				if (result != RESOURCE_X_RECONFIG_MORE
					&& result != RESOURCE_X_RECONFIG_RETRY)
					return false;
			}
			if (result != RESOURCE_X_RECONFIG_DONE) {
				return result == RESOURCE_X_RECONFIG_MORE
					&& batch.examined_count != 0;
			}
			if (!cluster_resource_x_reconfig_cutover_bind_native_successor_exact(
					&token))
				return false;
			/* Binding is itself progress.  Resume through the event loop so
			 * no tick traverses more than one 64-slot checkpoint. */
			return true;
		}
		for (calls = 0;
			 calls < GCS_BLOCK_RESOURCE_X_RECONFIG_CALLS_PER_CHECKPOINT;
			 calls++) {
			result = cluster_resource_x_reconfig_sweep(
				&token, GCS_BLOCK_RESOURCE_X_RECONFIG_PROBE_BUDGET,
				&batch);
			if (result == RESOURCE_X_RECONFIG_DONE)
				break;
			if (result != RESOURCE_X_RECONFIG_MORE
				&& result != RESOURCE_X_RECONFIG_RETRY)
				return false;
		}
		if (result != RESOURCE_X_RECONFIG_DONE) {
			return result == RESOURCE_X_RECONFIG_MORE
				&& batch.examined_count != 0;
		}
		if (!cluster_resource_x_reconfig_zero_proof_exact(
				&token, &zero)
			|| !cluster_pcm_lock_resource_x_clean_completion_prove_exact(
				&token, &zero, &clean)
			|| !cluster_pcm_lock_resource_x_cutover_proofs_exact(
				&observed_token, &observed_zero, &observed_clean)
			|| memcmp(&observed_token, &token, sizeof(token)) != 0
			|| memcmp(&observed_zero, &zero, sizeof(zero)) != 0
			|| memcmp(&observed_clean, &clean, sizeof(clean)) != 0)
			return false;
		return batch.examined_count != 0;
	}

	if (cutover.phase
			!= CLUSTER_SEMANTIC_R11_CUTOVER_DURABLE_OPEN_PENDING_LOCAL
		|| !cluster_pcm_lock_resource_x_cutover_proofs_exact(
			&token, &zero, &clean)
		|| !cluster_resource_x_reconfig_thaw_exact(&token)
		|| !cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
			&observed_token, &observed_zero, &observed_clean)
		|| memcmp(&observed_token, &token, sizeof(token)) != 0
		|| memcmp(&observed_zero, &zero, sizeof(zero)) != 0
		|| memcmp(&observed_clean, &clean, sizeof(clean)) != 0)
		return false;
	return true;
}

static uint32
gcs_block_resource_x_dead_requester_bitmap(
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	uint32 result = 0;
	int node;

	if (dead_bitmap == NULL)
		return 0;
	for (node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++)
		if ((dead_bitmap[node / 8] & (UINT8_C(1) << (node % 8))) != 0)
			result |= UINT32_C(1) << (uint32)node;
	return result;
}

static bool
gcs_block_resource_x_reconfig_epoch(uint64 new_epoch,
									uint32 dead_requester_bitmap)
{
	ResourceXGateSnapshot gate;
	ResourceXReconfigBatch batch;
	ResourceXReconfigResult result;
	ResourceXReconfigStats stats;
	ResourceXReconfigToken token;
	ResourceXReconfigToken observed_token;
	ResourceXZeroResidualProof zero;
	ResourceXZeroResidualProof observed_zero;
	ResourceXCleanCompletionProof clean;
	ResourceXCleanCompletionProof observed_clean;
	TimestampTz deadline;
	uint64 completed_epoch;
	uint64 expected_actor;
	uint64 expected_epoch;
	uint32 calls;
	bool claimed_epoch;

	if (ClusterGcsBlock == NULL)
		return true;
	if (new_epoch == 0 || new_epoch == UINT64_MAX)
		return false;
	completed_epoch
		= pg_atomic_read_u64(&ClusterGcsBlock->resource_x_reconfig_completed_epoch);
	if (new_epoch <= completed_epoch)
		return true;

	expected_epoch = 0;
	claimed_epoch = pg_atomic_compare_exchange_u64(
		&ClusterGcsBlock->resource_x_reconfig_in_progress_epoch, &expected_epoch,
		new_epoch);
	if (!claimed_epoch && expected_epoch != new_epoch)
		return false;
	deadline = GetCurrentTimestamp()
			   + (TimestampTz)Max(cluster_gcs_reply_timeout_ms, 1) * (TimestampTz)1000;
	if (!claimed_epoch && expected_epoch == new_epoch) {
		/* The epoch is an episode identity, not an actor lease.  A prior
		 * actor may have returned a transient no-progress result; only the
		 * actor_active CAS below decides who may resume it. */
	}
	for (;;) {
		expected_actor = 0;
		if (pg_atomic_compare_exchange_u64(
				&ClusterGcsBlock->resource_x_reconfig_actor_active, &expected_actor,
				new_epoch))
			break;
		if (expected_actor != new_epoch)
			return false;
		completed_epoch
			= pg_atomic_read_u64(&ClusterGcsBlock->resource_x_reconfig_completed_epoch);
		if (completed_epoch >= new_epoch)
			return true;
		if (GetCurrentTimestamp() >= deadline)
			return false;
		pg_usleep(1000L);
		CHECK_FOR_INTERRUPTS();
	}

	memset(&gate, 0, sizeof(gate));
	if (!cluster_pcm_lock_resource_x_gate_snapshot(&gate)) {
		/* No target formation has ever opened, so there is no Resource-X
		 * authority to sweep for this epoch. */
		pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_completed_epoch,
							new_epoch);
		pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_old_formation, 0);
		pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_in_progress_epoch, 0);
		pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_actor_active, 0);
		return true;
	}
	if (claimed_epoch)
		pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_old_formation,
								gate.formation);
	expected_epoch
		= pg_atomic_read_u64(&ClusterGcsBlock->resource_x_reconfig_old_formation);
	if (expected_epoch == 0
		|| !cluster_resource_x_reconfig_freeze_pending_exact(
			expected_epoch, dead_requester_bitmap, &token))
		goto actor_failed;

	deadline = GetCurrentTimestamp()
			   + (TimestampTz)Max(cluster_gcs_reply_timeout_ms, 1) * (TimestampTz)1000;
	if (token.new_formation == 0) {
		/* The source-removed build never predicts a successor formation from
		 * the membership epoch.  Its external R4 owner must bind the exact new
		 * formation before this bounded sweep may proceed. */
		gcs_block_resource_x_fail_closed_current();
		while (cluster_pcm_lock_resource_x_activation_inflight_count() != 0) {
			if (GetCurrentTimestamp() >= deadline)
				goto actor_failed;
			pg_usleep(1000L);
			CHECK_FOR_INTERRUPTS();
		}
		goto actor_failed;
	}

	for (;;) {
		for (calls = 0; calls < GCS_BLOCK_RESOURCE_X_RECONFIG_CALLS_PER_CHECKPOINT;
			 calls++) {
			result = cluster_resource_x_reconfig_sweep(
				&token, GCS_BLOCK_RESOURCE_X_RECONFIG_PROBE_BUDGET, &batch);
			if (result == RESOURCE_X_RECONFIG_DONE)
				goto complete;
			if (result != RESOURCE_X_RECONFIG_MORE
				&& result != RESOURCE_X_RECONFIG_RETRY)
				goto actor_failed;
		}
		if (GetCurrentTimestamp() >= deadline)
			goto actor_failed;
		CHECK_FOR_INTERRUPTS();
	}

complete:
	if (!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero)
		|| !cluster_pcm_lock_resource_x_clean_completion_prove_exact(
			&token, &zero, &clean)
		|| !cluster_pcm_lock_resource_x_cutover_proofs_exact(
			&observed_token, &observed_zero, &observed_clean)
		|| memcmp(&observed_token, &token, sizeof(token)) != 0
		|| memcmp(&observed_zero, &zero, sizeof(zero)) != 0
		|| memcmp(&observed_clean, &clean, sizeof(clean)) != 0
		|| !cluster_resource_x_reconfig_thaw_exact(&token))
		goto actor_failed;
	cluster_resource_x_reconfig_stats_snapshot(&stats);
	elog(LOG,
		 "Resource-X reconfiguration completed for epoch %llu, formation %llu->%llu "
		 "(freeze=%llu examined=%llu neutralized=%llu successor=%llu thaw=%llu)",
		 (unsigned long long)new_epoch, (unsigned long long)token.old_formation,
		 (unsigned long long)token.new_formation, (unsigned long long)stats.freeze_count,
		 (unsigned long long)stats.slot_examined_count,
		 (unsigned long long)stats.sidecar_neutralized_count,
		 (unsigned long long)stats.successor_count, (unsigned long long)stats.thaw_count);
	pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_completed_epoch, new_epoch);
	pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_old_formation, 0);
	pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_in_progress_epoch, 0);
	pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_actor_active, 0);
	return true;

actor_failed:
	pg_atomic_write_u64(&ClusterGcsBlock->resource_x_reconfig_actor_active, 0);
	return false;
}


/* ============================================================
 * PGRAC: spec-2.34 D4 — eager wake on epoch advance.
 *
 *	Called by spec-2.29 reconfig coordinator inside
 *	cluster_reconfig_apply_epoch_bump_as_coordinator() AFTER
 *	cluster_epoch_advance_for_reconfig() + cluster_epoch_set_changed_at_lsn()
 *	and BEFORE cluster_reconfig_publish_event() (HC95 ordering).
 *
 *	Action: sweep every per-backend block-outstanding slot;  for slots
 *	whose request_epoch < new_epoch, set slot.stale = true and broadcast
 *	the reply CV so the sender wakes immediately rather than waiting on
 *	the reply timeout safety net.  Each broadcast bumps
 *	epoch_invalidate_wake_count for observability.
 *
 *	Concurrency: per-backend LWLock — same lock used by sender/reply
 *	handler.  Caller (LMON/reconfig context) holds no buffer pins and
 *	does not touch backend-local ResourceOwner state (per L150).
 * ============================================================ */
void
cluster_gcs_block_on_epoch_advance_exact(
	uint64 new_epoch,
	const uint8 dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	ResourceXReconfigStats resource_x_stats;
	uint32 dead_requester_bitmap;
	int b;
	int j;

	dead_requester_bitmap
		= gcs_block_resource_x_dead_requester_bitmap(dead_bitmap);
	if (!gcs_block_resource_x_reconfig_epoch(new_epoch, dead_requester_bitmap)) {
		gcs_block_resource_x_fail_closed_current();
		cluster_resource_x_reconfig_stats_snapshot(&resource_x_stats);
		ereport(
			ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("Resource-X reconfiguration blocked epoch %llu", (unsigned long long)new_epoch),
			 errdetail("PGRAC_FAMILY=RESOURCE_X PGRAC_REASON=CALLER_CAUSE_UNPROVEN PGRAC_NODE=%d "
					   "PGRAC_ATTEMPT=0 "
					   "freeze=%llu examined=%llu orphan=%llu stale=%llu blocked=%llu",
					   cluster_node_id, (unsigned long long)resource_x_stats.freeze_count,
					   (unsigned long long)resource_x_stats.slot_examined_count,
					   (unsigned long long)resource_x_stats.orphan_count,
					   (unsigned long long)resource_x_stats.sidecar_stale_count,
					   (unsigned long long)resource_x_stats.blocked_count)));
	}
	(void)cluster_gcs_block_dedup_r4_route_sweep_epoch(new_epoch);
	if (gcs_block_backend_blocks == NULL || ClusterGcsBlock == NULL)
		return; /* not initialized — nothing to invalidate */

	for (b = 0; b < MaxBackends; b++) {
		ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[b];

		LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
		for (j = 0; j < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; j++) {
			ClusterGcsBlockOutstandingSlot *slot = &blk->slots[j];

			if (cluster_gcs_block_epoch_advance_stales_slot(slot->in_use, slot->request_epoch,
															new_epoch)
				&& !slot->stale) {
				slot->stale = true;
				ConditionVariableBroadcast(&slot->reply_cv);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->epoch_invalidate_wake_count, 1);
			}
		}
		LWLockRelease(&blk->lock.lock);
	}
}

void
cluster_gcs_block_on_epoch_advance(uint64 new_epoch)
{
	cluster_gcs_block_on_epoch_advance_exact(new_epoch, NULL);
}


/* ============================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-5.13 D5 (clean-leave GCS data-plane
 * drain: leaving-node flush orchestration + survivor cache invalidate).
 * ============================================================ */

/*
 * cluster_gcs_block_clean_leave_flush_all_dirty -- leaving node: force every
 * dirty block it holds X on to shared storage and release that X.
 *
 *	Thin orchestration over the bufmgr-owned seam (FlushBuffer is a bufmgr
 *	private static, so the scan + flush + release-X lives in bufmgr.c).  Runs
 *	in the leaving node's own backend/checkpointer (CL-I9 / L367), NEVER in
 *	LMON.  After this returns the leaving node holds no in-memory current for
 *	any block (CL-I5).  The S5 driver records the returned count and, on a flush
 *	error (which fail-closes via ereport from the bufmgr seam), goes
 *	ABORTED_ESCALATE rather than assume a half-completed drain (Rule 8.B).
 */
uint32
cluster_gcs_block_clean_leave_flush_all_dirty(void)
{
	return cluster_bufmgr_flush_and_release_x_for_leave();
}

/*
 * cluster_gcs_block_clean_leave_invalidate_for -- survivor: invalidate stale
 * cache of a leaving node's blocks once the leave epoch is observed.
 *
 *	POST-epoch, automatic, no second-round ACK (§3.1/§3.2 non-cycle proof):
 *	the survivor observes the cluster epoch reach the leave epoch and then
 *	invalidates.  The leaving node held X (exclusive) on every dirty block it
 *	flushed, so NO survivor holds a conflicting current copy; the only resident
 *	buffers a survivor can have for those tags are (a) blocks it held S on
 *	(still storage-current — no invalidate needed) or (b) stale / PI images
 *	(routed to storage on access).  Reusing on_epoch_advance — which marks the
 *	outstanding block-request slots whose request_epoch < new_epoch stale and
 *	wakes them — is therefore sufficient; no resident-buffer sweep is required.
 *	This invalidate is the happens-before boundary that must complete before
 *	any post-epoch storage read (CL-I5).
 */
void
cluster_gcs_block_clean_leave_invalidate_for(int32 leaving_node, uint64 new_epoch)
{
	(void)leaving_node; /* X-exclusivity makes a per-node resident sweep unnecessary */
	cluster_gcs_block_on_epoch_advance(new_epoch);
}


#endif /* USE_PGRAC_CLUSTER */
