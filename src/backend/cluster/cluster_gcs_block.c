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
#include "cluster/cluster_qvotec.h"			 /* spec-5.16 D3b — in_quorum master-side gate */
#include "cluster/cluster_reconfig.h"		 /* QVOTEC-observed live peer incarnation */
#include "cluster/cluster_recovery_merge.h"	 /* spec-4.7 D5 — recovered_through redo gate */
#include "cluster/cluster_thread_recovery.h" /* spec-4.11 scope gate for online replay */
#include "cluster/cluster_xnode_profile.h"	 /* spec-5.59 D2/D3/D4 profiling buckets */
#include "cluster/cluster_xnode_lever.h"	 /* spec-6.12a — downgrade counters */
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
#include "cluster/cluster_pcm_x_image_fetch.h"
#include "cluster/cluster_shmem.h"
#include "cluster/storage/cluster_shared_fs.h"
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

typedef struct ClusterGcsBlockOutstandingSlot {
	bool in_use;
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

typedef struct ClusterGcsBlockBackendBlock {
	LWLockPadded lock;
	ClusterGcsBlockOutstandingSlot slots[MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND];
	uint64 next_request_id;
} ClusterGcsBlockBackendBlock;

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
	pg_atomic_uint64 pi_watermark_advance_count; /* X→N/S downgrade caller advance ticks */
	pg_atomic_uint64 pi_watermark_retire_count;	 /* tag lifecycle + durable-confirm retire */
	pg_atomic_uint64 lost_write_detected_count;	 /* master direct OR holder forward detect */
	pg_atomic_uint64 lost_write_avoid_count;	 /* durable-confirm retire avoided false-pos */
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
	/* PCM-X requester received a retryable RESOURCE_RECOVERING denial while
	 * fetching its generation-exact holder image. */
	pg_atomic_uint64 pcm_x_image_fetch_recovering_retry_count;
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
static ClusterGcsBlockBackendBlock *gcs_block_backend_blocks = NULL;


/* ============================================================
 * Test-only injection hooks (USE_CLUSTER_UNIT only).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT
void (*cluster_gcs_block_test_xlog_flush_hook)(uint64 page_lsn) = NULL;
int (*cluster_gcs_block_test_lsn_drift_hook)(void) = NULL;
#endif


/* ============================================================
 * Forward decls (static helpers).
 * ============================================================ */
static ClusterGcsBlockBackendBlock *gcs_block_my_block(void);
static ClusterGcsBlockOutstandingSlot *gcs_block_reserve_slot(BufferTag tag, uint8 transition_id,
															  int32 master_node,
															  uint64 *out_request_id);
static ClusterGcsBlockOutstandingSlot *gcs_block_try_reserve_exact_slot(BufferTag tag,
																		uint8 transition_id,
																		int32 expected_source_node,
																		uint64 request_id);
static void gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot);
static void gcs_block_send_reply(int32 dest_node, const GcsBlockRequestPayload *req,
								 GcsBlockReplyStatus status, XLogRecPtr page_lsn,
								 const char *block_data);
static bool gcs_block_get_ship_image(BufferTag tag, int32 dest_node, bool allow_live_sge,
									 XLogRecPtr *out_page_lsn, char *copy_buf,
									 const char **out_block_payload, uint32 *out_block_lkey,
									 ClusterICSgeReleaseCallback *out_release_cb,
									 void **out_release_arg, ClusterSfDepVec *out_sf_dep_vec,
									 bool *out_sf_dep_valid);
static void gcs_block_release_ship_image(ClusterICSgeReleaseCallback release_cb, void *release_arg);
static uint32 gcs_block_compute_checksum(const char *block_data);
static uint32 gcs_block_compute_invalidate_checksum(const GcsBlockInvalidatePayload *inv);
static uint32 gcs_block_compute_invalidate_ack_checksum(const GcsBlockInvalidateAckPayload *ack);
static uint32 gcs_block_compute_redeclare_checksum(const GcsBlockRedeclarePayload *p);
static PcmXSessionAuthResult
gcs_block_pcm_x_authenticated_session_result(int32 node_id, uint64 expected_epoch,
											 uint64 *session_out,
											 ClusterGcsPcmXAuthSample *sample_out);
static bool gcs_block_pcm_x_authenticated_session(int32 node_id, uint64 expected_epoch,
												  uint64 *session_out);
static bool gcs_block_pcm_x_revalidate_peer_binding(int32 node_id, uint64 epoch, uint64 session);
static bool gcs_block_pcm_x_source_capable(int32 node_id);
static PcmXQueueResult gcs_block_pcm_x_fetch_own_result(ClusterPcmOwnResult result);
static PcmXQueueResult
gcs_block_pcm_x_fetch_reservation_mismatch(const ClusterPcmOwnSnapshot *live);
static void gcs_block_install_block(BufferDesc *buf, const char *block_data, XLogRecPtr page_lsn);
static PcmXQueueResult gcs_block_pcm_x_install_reserved_image_exact(
	BufferDesc *buf, const ClusterPcmOwnSnapshot *reservation_base, uint64 reservation_token,
	const char *block_data, XLogRecPtr page_lsn, const PcmXRuntimeSnapshot *request_runtime);
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
		pg_atomic_init_u64(&ClusterGcsBlock->pcm_x_image_fetch_recovering_retry_count, 0);
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
				slot->request_id = 0;
				slot->reply_received = false;
				slot->reply_sf_dep_valid = false;
				slot->reply_sf_flags = 0;
				cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
				slot->request_epoch = 0; /* spec-2.34 HC100 */
				slot->expected_master_node = -1;
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
							   (int)MyBackendId, MaxBackends)));
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
						 "per-backend cap GUC may land in spec-2.34+.")));
	return slot;
}


/* Reserve the established reply table with a canonical PCM-X image handle.
 * The handle is already generation-exact, so consuming a second generated
 * request id would sever the record/reply correlation.  Refuse a concurrent
 * duplicate in this backend instead of letting one reply wake an arbitrary
 * same-id slot. */
static ClusterGcsBlockOutstandingSlot *
gcs_block_try_reserve_exact_slot(BufferTag tag, uint8 transition_id, int32 expected_source_node,
								 uint64 request_id)
{
	ClusterGcsBlockBackendBlock *blk;
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	int i;

	if (!cluster_pcm_x_image_id_decode(request_id, NULL, NULL) || expected_source_node < 0
		|| expected_source_node >= PCM_X_PROTOCOL_NODE_LIMIT)
		return NULL;
	blk = gcs_block_my_block();
	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		if (blk->slots[i].in_use && blk->slots[i].request_id == request_id)
			goto reserve_done;
	}
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		if (!blk->slots[i].in_use) {
			slot = &blk->slots[i];
			slot->in_use = true;
			slot->request_id = request_id;
			slot->transition_id = transition_id;
			slot->tag = tag;
			slot->master_node = expected_source_node;
			slot->reply_received = false;
			memset(&slot->reply_header, 0, sizeof(slot->reply_header));
			memset(slot->reply_block_data, 0, sizeof(slot->reply_block_data));
			slot->reply_sf_dep_valid = false;
			slot->reply_sf_flags = 0;
			cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
			slot->reply_undo_trailer_valid = false;
			slot->reply_undo_tt_generation = 0;
			slot->reply_undo_authority_scn = 0;
			slot->request_epoch = 0;
			slot->expected_master_node = expected_source_node;
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
			break;
		}
	}

reserve_done:
	LWLockRelease(&blk->lock.lock);
	return slot;
}

static void
gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	slot->in_use = false;
	slot->reply_received = false;
	slot->request_id = 0;
	slot->transition_id = 0;
	slot->master_node = -1;
	slot->reply_sf_dep_valid = false;
	slot->reply_sf_flags = 0;
	cluster_sf_dep_vec_reset(&slot->reply_sf_dep_vec);
	slot->request_epoch = 0; /* spec-2.34 HC100 */
	slot->expected_master_node = -1;
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

	if (slot == NULL || buf == NULL)
		return false;
	if (transition_id != PCM_TRANS_N_TO_S)
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
						 bool *out_sf_dep_valid)
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
				copied = cluster_bufmgr_copy_block_for_gcs(tag, out_page_lsn, (char *)scratch);
			if (!copied) {
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
															out_sf_dep_vec))
			return false;
		if (out_sf_dep_valid != NULL)
			*out_sf_dep_valid = true;
		gcs_block_note_scratch_copy();
	} else if (!cluster_bufmgr_copy_block_for_gcs(tag, out_page_lsn, copy_buf))
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
 * content_lock EXCLUSIVE and PageSetLSN to the master-side LSN so recovery
 * sees a monotonic LSN across nodes.
 */
static void
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
						errdetail("buffer=%d", buf->buf_id)));
	}
	page = BufferGetPage(BufferDescriptorGetBuffer(buf));
	memcpy(page, block_data, GCS_BLOCK_DATA_SIZE);
	gcs_block_note_install_copy();
	PageSetLSN(page, page_lsn);
	/* The shipped image just proved these bytes current: a kept-pinned
	 * retained PI mirror regains CURRENT inside the same content-EXCLUSIVE
	 * hold, or the grant finish would refuse the frozen PI shape. */
	cluster_bufmgr_pcm_own_republish_grant_pending_image(buf);
	LWLockRelease(content_lock);
}


/*
 * Install one immutable PCM-X holder image without owning the surrounding
 * reservation lifecycle.  The queue driver began GRANT_PENDING and remains
 * solely responsible for commit/abort.  Recheck the complete ownership tuple
 * on both sides of the content-lock window so descriptor reuse or a competing
 * lifecycle can never turn a valid image into a write to the wrong page.
 */
static PcmXQueueResult
gcs_block_pcm_x_install_reserved_image_exact(BufferDesc *buf,
											 const ClusterPcmOwnSnapshot *reservation_base,
											 uint64 reservation_token, const char *block_data,
											 XLogRecPtr page_lsn,
											 const PcmXRuntimeSnapshot *request_runtime)
{
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnResult own_result;
	PcmXRuntimeSnapshot runtime;
	LWLock *content_lock;
	Page page;

	if (buf == NULL || reservation_base == NULL || block_data == NULL || request_runtime == NULL)
		return PCM_X_QUEUE_INVALID;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (!cluster_gcs_pcm_x_requester_runtime_exact(request_runtime, &runtime))
		return PCM_X_QUEUE_NOT_READY;
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	if (own_result != CLUSTER_PCM_OWN_OK)
		return gcs_block_pcm_x_fetch_own_result(own_result);
	if (!cluster_pcm_x_image_fetch_reservation_exact(&live, reservation_base, reservation_token))
		return gcs_block_pcm_x_fetch_reservation_mismatch(&live);

	content_lock = BufferDescriptorGetContentLock(buf);
	LWLockAcquire(content_lock, LW_EXCLUSIVE);
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	if (own_result != CLUSTER_PCM_OWN_OK
		|| !cluster_pcm_x_image_fetch_reservation_exact(&live, reservation_base,
														reservation_token)) {
		LWLockRelease(content_lock);
		return own_result != CLUSTER_PCM_OWN_OK ? gcs_block_pcm_x_fetch_own_result(own_result)
												: gcs_block_pcm_x_fetch_reservation_mismatch(&live);
	}
	runtime = cluster_pcm_x_runtime_snapshot();
	if (!cluster_gcs_pcm_x_requester_runtime_exact(request_runtime, &runtime)) {
		LWLockRelease(content_lock);
		return PCM_X_QUEUE_NOT_READY;
	}

	page = BufferGetPage(BufferDescriptorGetBuffer(buf));
	memcpy(page, block_data, GCS_BLOCK_DATA_SIZE);
	gcs_block_note_install_copy();
	PageSetLSN(page, page_lsn);
	own_result = cluster_bufmgr_pcm_own_publish_installed_x_image(buf, reservation_base,
																  reservation_token);
	if (own_result != CLUSTER_PCM_OWN_OK) {
		/* Bytes changed without a publishable exact reservation.  No prior
		 * local image is available for rollback, so retain all evidence and
		 * stop queue admission. */
		cluster_pcm_x_runtime_fail_closed();
		LWLockRelease(content_lock);
		return PCM_X_QUEUE_CORRUPT;
	}
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	runtime = cluster_pcm_x_runtime_snapshot();
	if (own_result != CLUSTER_PCM_OWN_OK
		|| !cluster_pcm_x_image_fetch_reservation_exact(&live, reservation_base, reservation_token)
		|| !cluster_gcs_pcm_x_requester_runtime_exact(request_runtime, &runtime)) {
		/* Bytes were copied while the reservation identity changed.  Core has
		 * no sound rollback image for that boundary. */
		cluster_pcm_x_runtime_fail_closed();
		LWLockRelease(content_lock);
		return PCM_X_QUEUE_CORRUPT;
	}
	LWLockRelease(content_lock);
	return PCM_X_QUEUE_OK;
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
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_gcs_send_block_request_and_wait: NULL retry result")));
	*out_retry_denied = false;
	if (buf == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_send_block_request_and_wait: NULL BufferDesc")));

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
							   (int)transition_id)));

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
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
								errmsg("cluster_gcs_block: failed to enqueue "
									   "GCS_BLOCK_REQUEST to node %d",
									   current_master)));
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
						ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
										errmsg("cluster_gcs_block: holder-granted reply missing "
											   "forwarding master")));
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
									 "re-dirtied); retry the transaction.")));
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
				/* spec-5.2 §3.5 D11: a read-image returned for a WRITE request
				 * (N->X / S->X) means the master/holder deferred the
				 * writer-transfer because it still holds an uncommitted ITL
				 * slot.  Mark the buffer so a write that does not first
				 * re-acquire X fails closed in cluster_itl (Rule 8.A); a plain
				 * read (N->S, D2) leaves pcm_state = N. */
				if (transition_id == PCM_TRANS_N_TO_X || transition_id == PCM_TRANS_S_TO_X_UPGRADE)
					buf->pcm_state = (uint8)PCM_STATE_READ_IMAGE;
				read_image = true;
				break;
			}
			if (final_status == GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK) {
				granted_storage_fallback = true;
				break;
			}

			/* DENIED paths — decide terminal vs retryable. */
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
				suppress_direct_land = true;
				current_master = cluster_gcs_lookup_master(tag);
				if (retry_attempt < max_retries)
					continue;
				terminal_denied = true;
				break;
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
				ereport(
					ERROR,
					(errcode(ERRCODE_CLUSTER_GCS_BLOCK_INVALIDATE_TIMEOUT),
					 errmsg("cluster_gcs_block: master broadcast invalidate timed out (HC116)")));
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
							 errdetail("request_id=" UINT64_FORMAT " request_epoch=" UINT64_FORMAT
									   " master=%d fork=%d transition=%d retry_attempt=%d"
									   " local pd_block_scn=" UINT64_FORMAT
									   " local pi_watermark_scn=" UINT64_FORMAT
									   " ownership_gen=" UINT64_FORMAT ".",
									   request_id, slot->request_epoch, current_master,
									   (int)tag.forkNum, (int)transition_id, retry_attempt,
									   (uint64)forens_local_scn, (uint64)forens_local_wm,
									   forens_own_gen),
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

			/*
			 * spec-2.35 HC105 — DENIED_MASTER_NOT_HOLDER from holder forward
			 * path is transient (holder evict race during forward); sender
			 * retries within budget. Direct-from-master DENIED_MASTER_NOT_HOLDER
			 * is terminal (master truly doesn't know any holder). HC108 authorized
			 * chain already validated the forwarding_master_node field.
			 */
			if (final_status == GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
				&& final_forwarding_master != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER) {
				if (retry_attempt < max_retries)
					continue;
				/* budget exhausted on transient holder-evict denial */
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
						 "retry after the leave commits — retry is safe")));

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
								   (int)transition_id)));
			break;
		case GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL:
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster_gcs_block: received block failed CRC32C verify"),
							errhint("Possible wire-ABI drift or network corruption.")));
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
								 "later spec; retry, or run the 2-node topology.")));
			}
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_gcs_block: master does not hold tag and state != N"),
							errhint("Cross-node holder migration / DRM handling lands in Stage 6; "
									"the cross-instance read-path boundary is Spec: spec-5.57.")));
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
							 "retry is safe.")));
			break;
		case GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE:
		default:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_gcs_block: transition denied (status=%d)",
								   (int)final_status)));
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
					 "consider raising cluster.gcs_block_retransmit_max_retries.")));
}


/* The canonical image handle is bound to one epoch, holder, and master
 * incarnation.  Unlike an ordinary block request it must never follow an
 * epoch-driven master rehash: loss of any authority component makes this
 * exact attempt retryable only by the outer convert-queue driver. */
static bool
gcs_block_pcm_x_fetch_authority_exact(const PcmXLocalProgress *progress)
{
	uint64 holder_session;
	uint64 master_session;
	uint64 epoch;
	int32 holder_node;
	int32 master_node;

	if (progress == NULL)
		return false;
	epoch = progress->identity.cluster_epoch;
	holder_node = (int32)progress->image.source_node;
	master_node = progress->master_node;
	if (cluster_epoch_get_current() != epoch || holder_node < 0
		|| holder_node >= PCM_X_PROTOCOL_NODE_LIMIT || master_node < 0
		|| master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| cluster_gcs_lookup_master(progress->identity.tag) != master_node
		|| !cluster_qvotec_in_quorum() || !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(holder_node) || !cluster_membership_is_member(master_node)
		|| cluster_gcs_block_phase_for_tag(progress->identity.tag) == GCS_BLOCK_RECOVERING
		|| (cluster_write_fence_enforcing() && !cluster_write_fence_allowed())
		|| !gcs_block_pcm_x_source_capable(holder_node)
		|| !gcs_block_pcm_x_source_capable(master_node)
		|| !gcs_block_pcm_x_authenticated_session(holder_node, epoch, &holder_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(holder_node, epoch, holder_session)
		|| !gcs_block_pcm_x_authenticated_session(master_node, epoch, &master_session)
		|| master_session != progress->master_session_incarnation
		|| !gcs_block_pcm_x_revalidate_peer_binding(master_node, epoch, master_session))
		return false;
	return true;
}


/* Bind every requester-side mutation and sleep to the exact formation that
 * admitted it.  The runtime token protects the local gate generation/session;
 * the peer binding protects the authenticated master connection inside that
 * formation. */
static bool
gcs_block_pcm_x_requester_authority_exact(const PcmXRuntimeSnapshot *request_runtime,
										  int32 master_node, uint64 cluster_epoch,
										  uint64 master_session)
{
	PcmXRuntimeSnapshot current = cluster_pcm_x_runtime_snapshot();

	return cluster_gcs_pcm_x_requester_runtime_exact(request_runtime, &current)
		   && gcs_block_pcm_x_revalidate_peer_binding(master_node, cluster_epoch, master_session);
}


/* Image fetch also crosses the holder data-plane connection.  Reuse the
 * canonical fetch authority proof so both holder and master bindings remain
 * exact while additionally pinning the original requester runtime token. */
static bool
gcs_block_pcm_x_fetch_requester_authority_exact(const PcmXRuntimeSnapshot *request_runtime,
												const PcmXLocalProgress *progress)
{
	PcmXRuntimeSnapshot current = cluster_pcm_x_runtime_snapshot();

	return cluster_gcs_pcm_x_requester_runtime_exact(request_runtime, &current)
		   && gcs_block_pcm_x_fetch_authority_exact(progress);
}


/* Map the opaque ownership substrate to the queue driver's result domain.
 * A malformed live flag shape is evidence, not contention, and closes the
 * PCM-X runtime before returning. */
static PcmXQueueResult
gcs_block_pcm_x_fetch_own_result(ClusterPcmOwnResult result)
{
	switch (result) {
	case CLUSTER_PCM_OWN_OK:
		return PCM_X_QUEUE_OK;
	case CLUSTER_PCM_OWN_STALE:
		return PCM_X_QUEUE_STALE;
	case CLUSTER_PCM_OWN_BUSY:
		cluster_pcm_x_stats_note_own_busy();
		return PCM_X_QUEUE_BUSY;
	case CLUSTER_PCM_OWN_EXHAUSTED:
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	case CLUSTER_PCM_OWN_NOT_READY:
		return PCM_X_QUEUE_NOT_READY;
	case CLUSTER_PCM_OWN_CORRUPT:
		cluster_pcm_x_stats_note_own_corrupt();
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	case CLUSTER_PCM_OWN_INVALID:
	default:
		return PCM_X_QUEUE_INVALID;
	}
}


static PcmXQueueResult
gcs_block_pcm_x_fetch_reservation_mismatch(const ClusterPcmOwnSnapshot *live)
{
	ClusterPcmOwnResult shape;

	if (live == NULL)
		return PCM_X_QUEUE_INVALID;
	shape = cluster_pcm_own_classify_live_flags(live->flags, live->reservation_token);
	if (shape == CLUSTER_PCM_OWN_CORRUPT)
		return gcs_block_pcm_x_fetch_own_result(shape);
	/* A well-formed but non-exact live tuple belongs to another lifecycle. */
	return PCM_X_QUEUE_STALE;
}


static bool
gcs_block_pcm_x_self_page_exact(Page page, const PcmXImageToken *image)
{
	return page != NULL && image != NULL && PageGetLSN(page) == (XLogRecPtr)image->page_lsn
		   && (uint64)((PageHeader)page)->pd_block_scn == image->page_scn
		   && cluster_gcs_block_compute_checksum((const char *)page) == image->page_checksum;
}


static bool
gcs_block_pcm_x_self_progress_exact(const PcmXLocalProgress *progress,
									const PcmXLocalHandle *leader,
									const ClusterPcmOwnSnapshot *revoking_base,
									uint16 expected_response)
{
	uint32 expected_member_state;

	if (progress == NULL || leader == NULL || revoking_base == NULL
		|| (expected_response != PGRAC_IC_MSG_PCM_X_PREPARE_GRANT
			&& expected_response != PGRAC_IC_MSG_PCM_X_COMMIT_X))
		return false;
	expected_member_state = expected_response == PGRAC_IC_MSG_PCM_X_PREPARE_GRANT
								? PCM_XL_REMOTE_WAIT
								: PCM_XL_CONTENT_ACTIVE;
	return memcmp(&progress->identity, &leader->identity, sizeof(progress->identity)) == 0
		   && BufferTagsEqual(&progress->identity.tag, &revoking_base->tag)
		   && progress->identity.base_own_generation == revoking_base->generation
		   && progress->role == PCM_X_LOCAL_ROLE_NODE_LEADER
		   && progress->member_state == expected_member_state && progress->pending_opcode == 0
		   && progress->last_response_opcode == expected_response
		   && progress->image.source_node == (uint32)cluster_node_id
		   && progress->image.source_own_generation == revoking_base->generation
		   && progress->image.image_id != 0 && progress->master_session_incarnation != 0;
}


static ClusterPcmXGrantReservationKind
gcs_block_pcm_x_self_handoff_kind(uint8 pcm_state)
{
	switch ((PcmState)pcm_state) {
	case PCM_STATE_N:
		return CLUSTER_PCM_X_GRANT_RESERVATION_N_REVOKE_HANDOFF;
	case PCM_STATE_S:
		return CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF;
	case PCM_STATE_X:
		return CLUSTER_PCM_X_GRANT_RESERVATION_X_REVOKE_HANDOFF;
	case PCM_STATE_READ_IMAGE:
	default:
		return CLUSTER_PCM_X_GRANT_RESERVATION_INVALID;
	}
}


PcmXQueueResult
cluster_gcs_pcm_x_adopt_self_image(BufferDesc *buf, const PcmXLocalHandle *leader,
								   const ClusterPcmOwnSnapshot *revoking_base,
								   uint64 *out_reservation_token)
{
	GcsBlockRequestPayload request_proof;
	PcmXLocalProgress progress_before;
	PcmXLocalProgress progress_after;
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnResult own_result;
	PcmXQueueResult queue_result;
	LWLock *content_lock;
	Page page;
	volatile bool handed_off = false;
	volatile bool handoff_transitioned = false;
	volatile bool exact_revoking_before = false;
	volatile bool content_locked = false;
	uint64 reservation_token = 0;
	ClusterPcmXGrantReservationKind expected_kind;

	if (out_reservation_token != NULL)
		*out_reservation_token = 0;
	if (buf == NULL || leader == NULL || revoking_base == NULL || out_reservation_token == NULL
		|| MyBackendId <= 0 || revoking_base->flags != PCM_OWN_FLAG_REVOKING
		|| revoking_base->reservation_token == 0)
		return PCM_X_QUEUE_INVALID;
	expected_kind = gcs_block_pcm_x_self_handoff_kind(revoking_base->pcm_state);
	if (expected_kind == CLUSTER_PCM_X_GRANT_RESERVATION_INVALID)
		return PCM_X_QUEUE_INVALID;
	queue_result = cluster_pcm_x_local_progress_exact(leader, &progress_before);
	cluster_pcm_x_stats_note_queue_result(queue_result);
	if (queue_result != PCM_X_QUEUE_OK)
		return queue_result;
	if (!gcs_block_pcm_x_self_progress_exact(&progress_before, leader, revoking_base,
											 PGRAC_IC_MSG_PCM_X_PREPARE_GRANT)
		|| !cluster_pcm_x_image_fetch_build_request(&progress_before, cluster_node_id,
													(int32)MyBackendId, &request_proof)
		|| !gcs_block_pcm_x_fetch_authority_exact(&progress_before))
		return PCM_X_QUEUE_STALE;

	content_lock = BufferDescriptorGetContentLock(buf);
	PG_TRY();
	{
		LWLockAcquire(content_lock, LW_EXCLUSIVE);
		content_locked = true;
		own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
		queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
		if (queue_result == PCM_X_QUEUE_OK) {
			bool exact_revoking = BufferTagsEqual(&live.tag, &revoking_base->tag)
								  && live.generation == revoking_base->generation
								  && live.reservation_token == revoking_base->reservation_token
								  && live.flags == PCM_OWN_FLAG_REVOKING
								  && live.pcm_state == revoking_base->pcm_state;
			bool exact_handoff = cluster_pcm_x_grant_reservation_kind(
									 &live, revoking_base, revoking_base->reservation_token)
								 == expected_kind;

			if (!exact_revoking && !exact_handoff)
				queue_result = gcs_block_pcm_x_fetch_reservation_mismatch(&live);
			exact_revoking_before = exact_revoking;
		}
		page = BufferGetPage(BufferDescriptorGetBuffer(buf));
		if (queue_result == PCM_X_QUEUE_OK
			&& !gcs_block_pcm_x_self_page_exact(page, &progress_before.image)) {
			cluster_pcm_x_runtime_fail_closed();
			queue_result = PCM_X_QUEUE_CORRUPT;
		}
		if (queue_result == PCM_X_QUEUE_OK) {
			own_result = cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(buf, revoking_base,
																				&reservation_token);
			queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
			handed_off = queue_result == PCM_X_QUEUE_OK;
			handoff_transitioned = handed_off && exact_revoking_before;
		}
		if (queue_result == PCM_X_QUEUE_OK) {
			own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
			if (gcs_block_pcm_x_fetch_own_result(own_result) != PCM_X_QUEUE_OK
				|| cluster_pcm_x_grant_reservation_kind(&live, revoking_base, reservation_token)
					   != expected_kind) {
				cluster_pcm_x_runtime_fail_closed();
				queue_result = PCM_X_QUEUE_CORRUPT;
			}
		}
		LWLockRelease(content_lock);
		content_locked = false;
	}
	PG_CATCH();
	{
		if (handed_off)
			cluster_pcm_x_runtime_fail_closed();
		if (content_locked && LWLockHeldByMe(content_lock))
			LWLockRelease(content_lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (queue_result != PCM_X_QUEUE_OK)
		return queue_result;

	queue_result = cluster_pcm_x_local_progress_exact(leader, &progress_after);
	cluster_pcm_x_stats_note_queue_result(queue_result);
	if (queue_result != PCM_X_QUEUE_OK
		|| memcmp(&progress_before, &progress_after, sizeof(progress_after)) != 0
		|| !gcs_block_pcm_x_fetch_authority_exact(&progress_after)) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	gcs_block_stamp_touched(cluster_node_id, progress_after.master_node);
	if (handoff_transitioned && ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->pcm_x_self_handoff_count, 1);
	*out_reservation_token = reservation_token;
	return PCM_X_QUEUE_OK;
}


PcmXQueueResult
cluster_gcs_pcm_x_finish_self_image_x(BufferDesc *buf, const PcmXLocalHandle *leader,
									  const ClusterPcmOwnSnapshot *revoking_base,
									  uint64 reservation_token, uint64 *out_committed_generation)
{
	PcmXLocalProgress progress = { 0 };
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnResult own_result;
	volatile PcmXQueueResult queue_result;
	LWLock *content_lock;
	Page page;
	volatile uint64 committed_generation = 0;
	volatile bool content_locked = false;
	volatile bool committed = false;
	volatile bool handoff_live = false;
	ClusterPcmXGrantReservationKind expected_kind;

	if (out_committed_generation != NULL)
		*out_committed_generation = 0;
	if (buf == NULL || leader == NULL || revoking_base == NULL || out_committed_generation == NULL
		|| reservation_token == 0)
		return PCM_X_QUEUE_INVALID;
	expected_kind = gcs_block_pcm_x_self_handoff_kind(revoking_base->pcm_state);
	if (expected_kind == CLUSTER_PCM_X_GRANT_RESERVATION_INVALID)
		return PCM_X_QUEUE_INVALID;

	/* PREPARE's role handoff is irreversible: first prove that exact tuple is
	 * live, so every later phase/session/interrupt failure is recovery-blocked
	 * rather than exposed as an ordinary retry. */
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
	queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
	if (queue_result != PCM_X_QUEUE_OK)
		return queue_result;
	if (cluster_pcm_x_grant_reservation_kind(&live, revoking_base, reservation_token)
		!= expected_kind)
		return gcs_block_pcm_x_fetch_reservation_mismatch(&live);
	handoff_live = true;

	queue_result = cluster_pcm_x_local_progress_exact(leader, &progress);
	cluster_pcm_x_stats_note_queue_result(queue_result);
	if (queue_result != PCM_X_QUEUE_OK
		|| !gcs_block_pcm_x_self_progress_exact(&progress, leader, revoking_base,
												PGRAC_IC_MSG_PCM_X_COMMIT_X)
		|| !gcs_block_pcm_x_fetch_authority_exact(&progress)) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}

	content_lock = BufferDescriptorGetContentLock(buf);
	PG_TRY();
	{
		LWLockAcquire(content_lock, LW_EXCLUSIVE);
		content_locked = true;
		own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live);
		queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
		if (queue_result == PCM_X_QUEUE_OK
			&& cluster_pcm_x_grant_reservation_kind(&live, revoking_base, reservation_token)
				   != expected_kind)
			queue_result = gcs_block_pcm_x_fetch_reservation_mismatch(&live);
		page = BufferGetPage(BufferDescriptorGetBuffer(buf));
		if (queue_result == PCM_X_QUEUE_OK
			&& !gcs_block_pcm_x_self_page_exact(page, &progress.image))
			queue_result = PCM_X_QUEUE_CORRUPT;
		if (queue_result == PCM_X_QUEUE_OK) {
			uint64 next_generation = 0;

			own_result = cluster_bufmgr_pcm_own_finish_x_commit(
				buf, revoking_base, reservation_token, &next_generation);
			queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
			if (queue_result == PCM_X_QUEUE_OK) {
				committed_generation = next_generation;
				committed = true;
			}
		}
		LWLockRelease(content_lock);
		content_locked = false;
	}
	PG_CATCH();
	{
		/* LWLockRelease resumes interrupts.  A pending cancel/die after the
		 * ownership bump must never bypass FINAL as an ordinary backend error. */
		if (handoff_live || committed)
			cluster_pcm_x_runtime_fail_closed();
		if (content_locked && LWLockHeldByMe(content_lock))
			LWLockRelease(content_lock);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (queue_result != PCM_X_QUEUE_OK) {
		/* Handoff is irreversible even when the ownership bump itself failed.
		 * Preserve exact evidence for recovery instead of aborting. */
		cluster_pcm_x_runtime_fail_closed();
		return queue_result;
	}
	if (revoking_base->generation == UINT64_MAX
		|| committed_generation != revoking_base->generation + 1) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	*out_committed_generation = committed_generation;
	return PCM_X_QUEUE_OK;
}


/*
 * Fetch the immutable holder READY record through the established GCS block
 * data plane and install it under the exact GRANT_PENDING reservation.  This
 * function neither advances the queue phase nor commits/aborts ownership;
 * the caller retains both lifecycles and must present the same leader/base/
 * token tuple to its next protocol step.
 */
PcmXQueueResult
cluster_gcs_pcm_x_fetch_image_and_install(BufferDesc *buf, const PcmXLocalHandle *leader,
										  const ClusterPcmOwnSnapshot *reservation_base,
										  uint64 reservation_token,
										  const PcmXRuntimeSnapshot *request_runtime)
{
	ClusterGcsBlockOutstandingSlot *slot;
	PcmXLocalProgress progress_before;
	ClusterPcmOwnSnapshot live_own;
	GcsBlockRequestPayload request;
	PcmXQueueResult queue_result;
	ClusterPcmOwnResult own_result;
	volatile PcmXQueueResult result = PCM_X_QUEUE_NOT_READY;
	int max_retries;
	int retry_attempt;

	if (buf == NULL || leader == NULL || reservation_base == NULL || request_runtime == NULL
		|| MyBackendId <= 0)
		return PCM_X_QUEUE_INVALID;
	queue_result = cluster_pcm_x_local_progress_exact(leader, &progress_before);
	cluster_pcm_x_stats_note_queue_result(queue_result);
	if (queue_result != PCM_X_QUEUE_OK)
		return queue_result;
	if (!BufferTagsEqual(&buf->tag, &reservation_base->tag)
		|| !BufferTagsEqual(&leader->identity.tag, &reservation_base->tag)
		|| reservation_base->generation
			   != (progress_before.grant_base_own_generation != 0
					   ? progress_before.grant_base_own_generation
					   : leader->identity.base_own_generation))
		return PCM_X_QUEUE_STALE;
	if (!cluster_pcm_x_image_fetch_build_request(&progress_before, cluster_node_id,
												 (int32)MyBackendId, &request))
		return PCM_X_QUEUE_NOT_READY;
	if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime, &progress_before))
		return PCM_X_QUEUE_NOT_READY;
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live_own);
	queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
	if (queue_result != PCM_X_QUEUE_OK)
		return queue_result;
	if (!cluster_pcm_x_image_fetch_reservation_exact(&live_own, reservation_base,
													 reservation_token))
		return gcs_block_pcm_x_fetch_reservation_mismatch(&live_own);

	cluster_gcs_block_dedup_register_backend_exit_hook();
	slot = gcs_block_try_reserve_exact_slot(request.tag, request.transition_id,
											(int32)progress_before.image.source_node,
											request.request_id);
	if (slot == NULL)
		return PCM_X_QUEUE_BUSY;
	max_retries = cluster_gcs_block_retransmit_max_retries >= 0
					  ? cluster_gcs_block_retransmit_max_retries
					  : 4;

	PG_TRY();
	{
		for (retry_attempt = 0; retry_attempt <= max_retries; retry_attempt++) {
			ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
			PcmXLocalProgress progress_now;
			GcsBlockReplyHeader reply;
			char reply_block[GCS_BLOCK_DATA_SIZE];
			TimestampTz deadline;
			bool fence_lost = false;
			bool got_reply = false;
			bool slot_stale = false;

			if (retry_attempt > 0) {
				long backoff_ms = gcs_block_backoff_ms_for_retry(retry_attempt);

				if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime,
																	 &progress_before)) {
					result = PCM_X_QUEUE_NOT_READY;
					break;
				}
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_attempt_count, 1);
				(void)WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, backoff_ms,
								WAIT_EVENT_GCS_BLOCK_RETRANSMIT_WAIT);
				ResetLatch(MyLatch);
				if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime,
																	 &progress_before)) {
					result = PCM_X_QUEUE_NOT_READY;
					break;
				}
			}

			queue_result = cluster_pcm_x_local_progress_exact(leader, &progress_now);
			cluster_pcm_x_stats_note_queue_result(queue_result);
			if (queue_result != PCM_X_QUEUE_OK) {
				result = queue_result;
				break;
			}
			if (memcmp(&progress_before, &progress_now, sizeof(progress_now)) != 0) {
				result = PCM_X_QUEUE_STALE;
				break;
			}
			if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime, &progress_now)) {
				result = cluster_epoch_get_current() == request.epoch ? PCM_X_QUEUE_NOT_READY
																	  : PCM_X_QUEUE_STALE;
				break;
			}
			own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live_own);
			queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
			if (queue_result != PCM_X_QUEUE_OK) {
				result = queue_result;
				break;
			}
			if (!cluster_pcm_x_image_fetch_reservation_exact(&live_own, reservation_base,
															 reservation_token)) {
				result = gcs_block_pcm_x_fetch_reservation_mismatch(&live_own);
				break;
			}

			/* Rearm the exact image id atomically with its fixed authority. */
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
			slot->request_epoch = request.epoch;
			slot->expected_master_node = (int32)progress_now.image.source_node;
			slot->stale = false;
			LWLockRelease(&blk->lock.lock);

			ConditionVariablePrepareToSleep(&slot->reply_cv);
			if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime, &progress_now)) {
				ConditionVariableCancelSleep();
				result = PCM_X_QUEUE_NOT_READY;
				break;
			}
			if (retry_attempt == 0)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_request_count, 1);
			else
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_send_count, 1);
			if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
														  progress_now.image.source_node, &request,
														  sizeof(request))) {
				ConditionVariableCancelSleep();
				if (retry_attempt < max_retries)
					continue;
				break;
			}

			deadline = GetCurrentTimestamp()
					   + ((TimestampTz)cluster_gcs_reply_timeout_ms) * (TimestampTz)1000;
			for (;;) {
				TimestampTz now;
				long timeout_ms;

				LWLockAcquire(&blk->lock.lock, LW_SHARED);
				got_reply = slot->in_use && slot->reply_received;
				slot_stale = slot->in_use && slot->stale;
				LWLockRelease(&blk->lock.lock);
				if (got_reply || slot_stale)
					break;
				now = GetCurrentTimestamp();
				if (now >= deadline) {
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_timeout_count, 1);
					break;
				}
				timeout_ms = (long)((deadline - now) / 1000);
				if (timeout_ms <= 0)
					timeout_ms = 1;
				if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime,
																	 &progress_now)) {
					fence_lost = true;
					break;
				}
				(void)ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
												  WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
				if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime,
																	 &progress_now)) {
					fence_lost = true;
					break;
				}
			}
			ConditionVariableCancelSleep();
			if (fence_lost) {
				result = PCM_X_QUEUE_NOT_READY;
				break;
			}
			if (slot_stale) {
				result = PCM_X_QUEUE_STALE;
				break;
			}
			if (!got_reply)
				continue;
			if (!gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime, &progress_now)) {
				result = PCM_X_QUEUE_NOT_READY;
				break;
			}

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			reply = slot->reply_header;
			memcpy(reply_block, slot->reply_block_data, sizeof(reply_block));
			LWLockRelease(&blk->lock.lock);
			/* Both denials describe transient source authority, not malformed
			 * image bytes.  Re-arm the same generation-exact request within the
			 * existing bounded retry budget; only a putative READ_IMAGE reply is
			 * eligible for byte/checksum validation below.
			 */
			if (reply.status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
				|| reply.status == (uint8)GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING) {
				if (reply.status == (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
				else
					pg_atomic_fetch_add_u64(
						&ClusterGcsBlock->pcm_x_image_fetch_recovering_retry_count, 1);
				continue;
			}
			if (!cluster_pcm_x_image_fetch_reply_exact(&reply, reply_block, &progress_now,
													   cluster_node_id, (int32)MyBackendId)) {
				cluster_pcm_x_runtime_fail_closed();
				result = PCM_X_QUEUE_CORRUPT;
				break;
			}

			/* The reply is valid only while the queue and reservation that armed
			 * it remain byte-exact. */
			queue_result = cluster_pcm_x_local_progress_exact(leader, &progress_now);
			cluster_pcm_x_stats_note_queue_result(queue_result);
			if (queue_result != PCM_X_QUEUE_OK
				|| memcmp(&progress_before, &progress_now, sizeof(progress_now)) != 0
				|| !gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime,
																	&progress_now)) {
				result = queue_result == PCM_X_QUEUE_OK ? PCM_X_QUEUE_STALE : queue_result;
				break;
			}
			own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live_own);
			queue_result = gcs_block_pcm_x_fetch_own_result(own_result);
			if (queue_result != PCM_X_QUEUE_OK
				|| !cluster_pcm_x_image_fetch_reservation_exact(&live_own, reservation_base,
																reservation_token)) {
				result = queue_result == PCM_X_QUEUE_OK
							 ? gcs_block_pcm_x_fetch_reservation_mismatch(&live_own)
							 : queue_result;
				break;
			}

			queue_result = gcs_block_pcm_x_install_reserved_image_exact(
				buf, reservation_base, reservation_token, reply_block, (XLogRecPtr)reply.page_lsn,
				request_runtime);
			if (queue_result != PCM_X_QUEUE_OK) {
				result = queue_result;
				break;
			}

			/* Once bytes landed, a disappearing queue/reservation has no local
			 * rollback proof.  Preserve evidence and close the runtime. */
			queue_result = cluster_pcm_x_local_progress_exact(leader, &progress_now);
			cluster_pcm_x_stats_note_queue_result(queue_result);
			own_result = cluster_bufmgr_pcm_own_snapshot(buf, &live_own);
			if (queue_result != PCM_X_QUEUE_OK
				|| memcmp(&progress_before, &progress_now, sizeof(progress_now)) != 0
				|| !gcs_block_pcm_x_fetch_requester_authority_exact(request_runtime, &progress_now)
				|| gcs_block_pcm_x_fetch_own_result(own_result) != PCM_X_QUEUE_OK
				|| !cluster_pcm_x_image_fetch_reservation_exact(&live_own, reservation_base,
																reservation_token)) {
				cluster_pcm_x_runtime_fail_closed();
				result = PCM_X_QUEUE_CORRUPT;
				break;
			}
			gcs_block_stamp_touched((int32)progress_now.image.source_node,
									progress_now.master_node);
			result = PCM_X_QUEUE_OK;
			break;
		}
	}
	PG_CATCH();
	{
		ConditionVariableCancelSleep();
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);
	return (PcmXQueueResult)result;
}


/*
 * PGRAC: spec-5.2 D2 (sub-case B) — local-master read-image forward.
 *
 *	When THIS node is the GCS master for a block that a REMOTE node holds in
 *	X, and a local reader needs an N→S image (e.g. to see an uncommitted ITL
 *	row-lock before a cross-node TX wait), the tag-only acquire path cannot
 *	serve the read (it has no data plane).  Here the master forwards a
 *	read-image request straight to the holder and waits for the holder to
 *	direct-ship the current image (status READ_IMAGE_FROM_XHOLDER).  The
 *	holder keeps its X; this node installs the bytes for THIS read only and
 *	never registers as an S holder (returns false so buf->pcm_state stays N).
 *
 *	spec-6.12a ㉕: with cluster.read_scache on, the forward also carries the
 *	downgrade-request flag.  If the holder accepts (ships
 *	S_GRANTED_XHOLDER_DOWNGRADE), this node — being the master — registers
 *	itself as an S holder via a LOCAL transition apply and returns true
 *	(durable S; the caller mirrors pcm_state).  Registration failure
 *	degrades to the one-shot semantics below.
 *
 *	Returns false for the one-shot read image (non-durable), true for the
 *	㉕ durable downgraded S grant.  Fails closed (ereport) if no image can
 *	be obtained — never a silent stale read (Rule 8.A).
 */
bool
cluster_gcs_local_master_read_image_and_wait(BufferDesc *buf, int32 holder_node)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	BufferTag tag;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool installed = false;
	bool durable_s = false; /* spec-6.12a ㉕ — holder downgraded, we registered */

	if (buf == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_local_master_read_image_and_wait: NULL BufferDesc")));

	tag = buf->tag;
	cluster_gcs_block_dedup_register_backend_exit_hook();
	/* expected_master == self:  the holder's reply carries forwarding_master =
	 * self, which the HC108 authorized chain validates against this slot. */
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
		if (cluster_read_scache)
			GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);

		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);
		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)holder_node, &fwd, sizeof(fwd)))
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("cluster_gcs_block: failed to enqueue read-image FORWARD "
								   "to X holder %d",
								   holder_node)));

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
			&& (slot->reply_header.status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
				|| slot->reply_header.status
					   == (uint8)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE)) {
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
					if (cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S,
															  cluster_node_id))
						durable_s = true;
					else
						cluster_lever_a_note_remote_ack_degraded();
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
					errhint("The X holder did not ship a current image in time; retry, or "
							"inspect dump_gcs.cf_xheld_read_ship_count.")));
	return true; /* unreachable */
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
bool
cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN read_scn, int32 origin_node, char *dst_page,
									bool *out_partial)
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
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("cluster_gcs_block: failed to enqueue CR request to "
								   "origin node %d",
								   (int)origin_node)));

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
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("cluster_gcs_block: failed to enqueue undo-TT fetch to "
								   "origin node %d",
								   (int)origin_node)));

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
									 TransactionId xid, bool authoritative, bool authority_kind,
									 GcsBlockReplyHeader *hdr_out,
									 ClusterGcsUndoVerdictPage *page_out, uint64 *tt_generation_out,
									 uint64 *authority_scn_out)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	GcsBlockForwardPayload fwd;
	bool got_reply = false;
	bool fetched = false;

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
		if (authority_kind)
			GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(&fwd);
		else
			GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, authoritative);
		/* The widened xid rides the watermark carrier (upper 32 bits zero). */
		GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)(uint64)xid);

		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
													  (uint32)dest_node, &fwd, sizeof(fwd))) {
			if (authority_kind)
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
								errmsg("cluster_gcs_block: failed to enqueue undo-verdict fetch "
									   "to authority node %d",
									   (int)dest_node)));
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("cluster_gcs_block: failed to enqueue undo-verdict fetch to "
								   "origin node %d",
								   (int)dest_node)));
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
	tag = GcsBlockUndoFetchTagMake(segment_id, 0);

	if (!gcs_block_undo_verdict_wire_exchange(origin_node, tag, cluster_epoch_get_current(), xid,
											  authoritative, false /* owner-served kind */, &hdr,
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
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("cluster_gcs_block: failed to enqueue undo-multi-verdict fetch "
								   "to origin node %d",
								   (int)origin_node)));

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
cluster_gcs_local_master_x_transfer_and_wait(BufferDesc *buf, int32 holder_node,
											 bool clean_eligible)
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

	if (buf == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_gcs_local_master_x_transfer_and_wait: NULL BufferDesc")));

	tag = buf->tag;
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
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("cluster_gcs_block: failed to enqueue X-transfer FORWARD "
								   "to X holder %d",
								   holder_node)));

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
				/* spec-5.2 §3.5 D11: mark this buffer a deferred-writer
				 * read-image so a write that does NOT first re-acquire X (the
				 * non-contended-row case) fails closed in cluster_itl rather
				 * than mutate a non-owned copy (Rule 8.A).  Cleared to N on
				 * content-lock unlock / overwritten by X on re-acquire. */
				buf->pcm_state = (uint8)PCM_STATE_READ_IMAGE;
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
		/* The holder shipped its current image and released its X; record self
		 * as the new authoritative X holder (durable).  No master round-trip:
		 * THIS node is the master.  spec-2.41 D2 — also advance the detector's
		 * SCN watermark from the installed image's pd_block_scn (local-page
		 * source; bytes are slot->reply_block_data). */
		cluster_pcm_lock_master_take_x_after_transfer(tag, installed_page_lsn, installed_page_scn,
													  holder_node, request_id, fwd.epoch);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_granted_from_holder_count, 1);
		if (clean_eligible)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_count, 1);
		return true; /* durable X grant — bufmgr mirrors buf->pcm_state = X */
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
								"re-dirtied); retry the transaction.")));
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
				 errdetail("request_id=" UINT64_FORMAT " epoch=" UINT64_FORMAT " holder=%d fork=%d"
						   " expected pi_watermark_scn sent=" UINT64_FORMAT
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
								"cross-instance coherent on this stage; retry the transaction.")));
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
								"the transfer; retry the transaction.")));

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_gcs_block: could not obtain X transfer from X holder %d "
						   "for tag spc=%u db=%u relNumber=%u block=%u",
						   holder_node, tag.spcOid, tag.dbOid,
						   (unsigned int)BufTagGetRelNumber(&tag), (unsigned int)tag.blockNum),
					errhint("The X holder did not ship a current image in time; retry.")));
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
 * Serve the canonical PCM-X image-id request subdomain from the holder's
 * dedicated dedup record.  This branch deliberately precedes the generic
 * request registration path: canonical image ids are reserved capabilities,
 * not ordinary request ids, and generic dedup correctly rejects them.
 *
 * The request carries no new overlay byte.  Its image_id names one immutable
 * record; the live holder ledger supplies the full ref/image/master-session
 * binding.  Both the requester and the authoritative master are re-proved
 * against the current peer frontier before the record is copied.  A second
 * holder snapshot closes the lookup-to-send window.  No dedup pointer or pin
 * escapes the lookup: cached is a by-value copy of the complete 8KB record.
 */
static bool
gcs_block_pcm_x_serve_image_fetch(const ClusterICEnvelope *env, const GcsBlockRequestPayload *req,
								  int worker_id)
{
	GcsBlockDedupEntry cached;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageResult image_result;
	PcmXLocalHolderProgress holder_after;
	PcmXLocalHolderProgress holder_before;
	PcmXQueueResult progress_result;
	uint64 master_session;
	uint64 requester_session;
	uint64 current_epoch;
	int32 encoded_master;
	int32 tag_master;

	if (req == NULL || !cluster_pcm_x_image_id_decode(req->request_id, &encoded_master, NULL))
		return false;
	/* From here onward the reserved namespace is always consumed here. */
	if (env == NULL || worker_id < 0 || worker_id >= cluster_lms_workers)
		return true;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(req->tag);
	if (encoded_master != tag_master || req->epoch != current_epoch
		|| !gcs_block_pcm_x_source_capable(req->sender_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(req->sender_node)
		|| !cluster_membership_is_member(tag_master)
		|| !gcs_block_pcm_x_authenticated_session(req->sender_node, current_epoch,
												  &requester_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(req->sender_node, current_epoch,
													requester_session))
		return true;

	progress_result = cluster_pcm_x_local_holder_progress_exact(&req->tag, &holder_before);
	if (progress_result != PCM_X_QUEUE_OK
		|| !cluster_pcm_x_image_fetch_request_exact(env, req, &holder_before, cluster_node_id,
													tag_master, current_epoch)
		|| !gcs_block_pcm_x_authenticated_session(tag_master, current_epoch, &master_session)
		|| master_session != holder_before.master_session_incarnation
		|| !gcs_block_pcm_x_revalidate_peer_binding(tag_master, current_epoch, master_session))
		return true;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = (uint32)req->sender_node;
	key.requester_backend_id = req->requester_backend_id;
	key.request_id = req->request_id;
	key.cluster_epoch = req->epoch;
	memset(&binding, 0, sizeof(binding));
	binding.identity.ref = holder_before.ref;
	binding.identity.image = holder_before.image;
	binding.master_session = holder_before.master_session_incarnation;
	memset(&cached, 0, sizeof(cached));
	image_result
		= cluster_gcs_block_dedup_pcm_x_lookup(worker_id, &key, &req->tag, &binding, &cached);
	if (image_result == GCS_BLOCK_PCM_X_IMAGE_NOT_READY) {
		/* Materialization/publication is still progressing on this same shard. */
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
							 InvalidXLogRecPtr, NULL);
		return true;
	}
	if (image_result != GCS_BLOCK_PCM_X_IMAGE_REPLAY) {
		/* NOT_FOUND is an old/released handle and is safely retried by timeout.
		 * Any other result says live holder evidence and its dedicated record
		 * disagree, so retain both and close the runtime. */
		if (image_result != GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND)
			cluster_pcm_x_runtime_fail_closed();
		return true;
	}

	progress_result = cluster_pcm_x_local_holder_progress_exact(&req->tag, &holder_after);
	if (progress_result != PCM_X_QUEUE_OK
		|| memcmp(&holder_before, &holder_after, sizeof(holder_before)) != 0
		|| !cluster_pcm_x_image_fetch_request_exact(env, req, &holder_after, cluster_node_id,
													tag_master, current_epoch))
		return true;

	gcs_block_resend_cached_reply(req->sender_node, &cached);
	if (ClusterGcsBlock != NULL) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_ship_bytes_total, GCS_BLOCK_DATA_SIZE);
	}
	return true;
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
gcs_block_produce_reply(const GcsBlockRequestPayload *req, char *block_buf,
						GcsBlockReplyStatus *out_status, XLogRecPtr *out_page_lsn,
						const char **out_block_payload, uint32 *out_block_lkey,
						ClusterICSgeReleaseCallback *out_release_cb, void **out_release_arg,
						ClusterSfDepVec *out_sf_dep_vec, bool *out_sf_dep_valid)
{
	uint64 current_epoch;
	PcmLockMode state;
	bool found;

	*out_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
	*out_page_lsn = InvalidXLogRecPtr;
	*out_block_payload = NULL;
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
	found = cluster_bufmgr_probe_block_for_gcs(req->tag);

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
		if (!cluster_pcm_lock_apply_gcs_transition(req->tag, (PcmLockTransition)req->transition_id,
												   req->sender_node)) {
			*out_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
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
		*out_status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		return true;
	}

	/*
	 * D4 bufmgr helper performs HC82 XLogFlush(page_lsn) + content_lock dance
	 * + HC89 single-retry revalidation.  Returns false if revalidation cannot
	 * stabilize after one retry → DENIED_MASTER_NOT_HOLDER fail-closed.
	 */
	if (!gcs_block_get_ship_image(req->tag, req->sender_node, true, out_page_lsn, block_buf,
								  out_block_payload, out_block_lkey, out_release_cb,
								  out_release_arg, out_sf_dep_vec, out_sf_dep_valid)) {
		*out_status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		return true;
	}
	if (out_sf_dep_valid == NULL || !*out_sf_dep_valid)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count, 1);

	/* HC77: master-side is the single transition-apply owner. */
	if (!cluster_pcm_lock_apply_gcs_transition(req->tag, (PcmLockTransition)req->transition_id,
											   req->sender_node)) {
		gcs_block_release_ship_image(out_release_cb != NULL ? *out_release_cb : NULL,
									 out_release_arg != NULL ? *out_release_arg : NULL);
		*out_block_payload = NULL;
		if (out_release_cb != NULL)
			*out_release_cb = NULL;
		if (out_release_arg != NULL)
			*out_release_arg = NULL;
		*out_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
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
	bool queue_pending_x_before = false;
	uint8 request_flags = 0;

	(void)env;
	cluster_sf_dep_vec_reset(&sf_dep_vec);

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
	if (!cluster_qvotec_in_quorum() || !cluster_membership_is_member(cluster_node_id)
		|| (cluster_grd_join_remaster_active_for_shard(req->tag)
			&& !cluster_grd_block_view_rebuilt(req->tag))) {
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
	if (cluster_gcs_block_phase_for_tag(req->tag) == GCS_BLOCK_RECOVERING) {
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING,
							 InvalidXLogRecPtr, NULL);
		return;
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

	/* A canonical PCM-X image handle is served by the current holder, not by
	 * the generic resource-master decision tree.  Intercept it before generic
	 * dedup, whose namespace guard intentionally rejects this id domain. */
	if (gcs_block_pcm_x_serve_image_fetch(env, req, dedup_worker_id))
		return;

	/* PGRAC: spec-2.34 D5 — dedup lookup_or_register (HC90 + HC91 + HC92). */
	memset(&key, 0, sizeof(key));
	key.origin_node_id = (uint32)req->sender_node;
	key.requester_backend_id = req->requester_backend_id;
	key.request_id = req->request_id;
	key.cluster_epoch = req->epoch;
	memset(&cached_entry, 0, sizeof(cached_entry));
	if (req->transition_id == PCM_TRANS_N_TO_S)
		queue_pending_x_before = gcs_block_queue_pending_x_authoritative(req->tag);

	dr = cluster_gcs_block_dedup_lookup_or_register(
		dedup_worker_id, &key, req->tag, req->transition_id,
		GcsBlockRequestPayloadGetLifetimeHintMs(req),
		cluster_sf_peer_supports_gcs_done(req->sender_node), &cached_entry);
	if (dr != GCS_BLOCK_DEDUP_VALIDATION_FAIL && dr != GCS_BLOCK_DEDUP_FULL) {
		if (GcsBlockRequestPayloadIsDirectLandArmed(req))
			request_flags |= GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND;
		if (!cluster_gcs_block_dedup_set_request_flags_exact(
				dedup_worker_id, &key, &req->tag, req->transition_id, request_flags)) {
			/* The tuple was removed/replaced or its immutable request properties
			 * changed between lookup and pinning.  Neither case may inherit the
			 * earlier entry's grant rights. */
			gcs_block_send_reply(req->sender_node, req,
								 GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, InvalidXLogRecPtr,
								 NULL);
			return;
		}
	}

	/* Check both sides of registration.  A queue claim that linearized before
	 * lookup routes this request directly to a cached exact denial; a claim
	 * that races after lookup is caught either here or by the claim-side scan.
	 * Queue ownership has no same-node exemption. */
	if (req->transition_id == PCM_TRANS_N_TO_S
		&& (queue_pending_x_before || gcs_block_queue_pending_x_authoritative(req->tag))) {
		GcsBlockPendingXDenyResult deny_result;

		if (dr == GCS_BLOCK_DEDUP_VALIDATION_FAIL) {
			gcs_block_send_reply(req->sender_node, req,
								 GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, InvalidXLogRecPtr,
								 NULL);
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
			gcs_block_send_reply(req->sender_node, req,
								 GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, InvalidXLogRecPtr,
								 NULL);
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
		 * race).  Re-forward to the same holder; holder side is
		 * idempotent (copy_block_for_gcs is read-only) so a second
		 * arrival simply re-ships the same bytes.  The cached reply header
		 * carries the holder id stored by the forward install path. */
		{
			GcsBlockForwardPayload fwd;
			int32 holder_node = cached_entry.reply_header.sender_node;

			if (holder_node < 0 || holder_node == cluster_node_id)
				return; /* malformed dedup entry; silent drop */
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
				if (cluster_read_scache)
					GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);
			}

			{
				ClusterICSendResult fwd_rc = cluster_ic_send_envelope(
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
				gcs_block_send_reply(req->sender_node, req,
									 GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
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
				gcs_block_send_reply(req->sender_node, req,
									 GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
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
										 &sf_dep_vec, &sf_dep_valid)) {
				/*
				 * PGRAC: spec-5.2 §3.5 D11 — active-ITL hard boundary.  Even as master+holder,
				 * if WE still have an uncommitted ITL slot on this block
				 * (ITL_FLAG_ACTIVE / LOCK_ONLY_ACTIVE), our own commit's
				 * itl_finish_stamp_page needs that in-memory slot.  Revoking our X
				 * and no-wire dropping the block now would discard the state our
				 * COMMIT must stamp -> the stamp assert trips on re-read (the P0-2
				 * crash).  Defer: ship a read-image (keep our X, NO drop, NO
				 * grant), so the remote requester sees our row lock, enters the
				 * cross-node TX completion wait, and retries the transfer only
				 * after we go terminal.  The requester's send_block_request_and_wait
				 * already treats READ_IMAGE_FROM_XHOLDER as a non-durable one-shot
				 * image (returns false; pcm_state stays N).  Rule 8.A: a GCS
				 * ownership transfer must satisfy the holder's local commit
				 * dependency, not just the bufmgr API contract.
				 *
				 * PGRAC: spec-6.12g D-g2 — with block self-containment this
				 * deferral is lifted HERE TOO (twin site of the holder-forward
				 * handler's gate below): the block ships WITH its uncommitted
				 * ITL and our copy is dropped; our later commit skips the stamp
				 * for the drifted block (D-g1 resident-for-stamp) and readers
				 * resolve the migrated ACTIVE slot through the TT authority
				 * (AD-006).  A same-ROW writer still serializes through the
				 * cross-node TX enqueue.  This site was missed by the original
				 * D-g2 change; a committed-but-unstamped (Fast Commit) ITL
				 * keeps the ACTIVE bit until cleanout, so without the gate a
				 * peer writer waits for a "terminal" that already happened.
				 */
				if (cluster_itl_page_has_active_slot((Page)block_payload)
					&& !cluster_block_self_contained) {
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

					/*
					 * PGRAC: spec-6.12g D-g2 (twin site) — count a self-ship
					 * transfer that carries an uncommitted ITL slot, and
					 * materialize an SGE-backed image into block_buf BEFORE the
					 * drop (the drop invalidates the buffer the SGE may point
					 * at; same recipe as the holder-forward destructive branch).
					 */
					if (cluster_block_self_contained
						&& cluster_itl_page_has_active_slot((Page)block_payload))
						cluster_lever_g_note_active_itl_transfer();
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
			} else
				gcs_block_send_reply(req->sender_node, req,
									 GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, InvalidXLogRecPtr,
									 NULL);
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
				(void)cluster_pcm_lock_clear_pending_x_if(req->tag, req->sender_node);
				status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
				page_lsn = InvalidXLogRecPtr;
				block_payload = NULL;
				goto build_and_send_reply;
			}
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
						&sf_dep_vec, &sf_dep_valid))
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
				if (cluster_read_scache) {
					bool downgraded = cluster_bufmgr_downgrade_x_to_s_for_gcs(req->tag);

					cluster_lever_a_note_downgrade(downgraded);
					if (downgraded)
						goto scache_downgraded_fall_through;
				}

				cluster_xp_begin(&xp_ship, CLXP_R_READIMAGE_SHIP);
				/* Master holds X locally: ship its current image without revoking X. */
				if (gcs_block_get_ship_image(req->tag, req->sender_node, true, &page_lsn, block_buf,
											 &block_payload, &block_payload_lkey,
											 &block_payload_release_cb, &block_payload_release_arg,
											 &sf_dep_vec, &sf_dep_valid)) {
					status = GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
					if (ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->cf_xheld_read_ship_count, 1);
					cluster_xp_end(&xp_ship);
					goto build_and_send_reply;
				}
				cluster_xp_abort(&xp_ship);
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
				if (cluster_read_scache)
					GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);
				else
					cluster_lever_a_note_fwd_oneshot();

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
				/* Forward send failed — fall through to the fail-closed flow. */
			}
			/* NOT_APPLICABLE / DENY → existing S-forward + master flow below
			 * (X-held that we cannot serve stays fail-closed, unchanged). */
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
	(void)gcs_block_produce_reply(req, block_buf, &status, &page_lsn, &block_payload,
								  &block_payload_lkey, &block_payload_release_cb,
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
	bool has_block_payload
		= (status == GCS_BLOCK_REPLY_GRANTED || status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER)
		  && block_payload != NULL;
	bool send_sf_dep = sf_dep_valid && has_block_payload;
	uint32 header_len
		= send_sf_dep ? (uint32)sizeof(GcsBlockReplyHeaderV2) : (uint32)sizeof(GcsBlockReplyHeader);
	uint32 total = header_len + GCS_BLOCK_DATA_SIZE;
	char *buf;
	GcsBlockReplyHeader *hdr;

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
	bool prepared;

	Assert(blk != NULL);
	Assert(slot != NULL);

	target_buf = slot->direct_target_buf;
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
		if (out_hdr != NULL)
			*out_hdr = (const GcsBlockReplyHeader *)payload;
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
		if (!cluster_smart_fusion || !cluster_gcs_block_reply_v2_extract_dep_vec(hdrv2, &dep_vec)) {
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
			 *	                         DENIED_MASTER_NOT_HOLDER}
			 *	      (HC105 evict race must be accepted; otherwise the
			 *	      sender's spec-2.34 retransmit budget cannot
			 *	      recover from holder eviction during forward)
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

	if (env == NULL || payload == NULL || env->payload_length != sizeof(GcsBlockForwardPayload))
		return;

	cluster_sf_dep_vec_reset(&sf_dep_vec);
	fwd = (const GcsBlockForwardPayload *)payload;
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
		if (!cluster_injection_should_skip("cluster-gcs-block-remote-downgrade"))
			remote_downgraded
				= cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(fwd->tag, fwd->master_node);
		cluster_lever_a_note_remote_downgrade(remote_downgraded);
	}

	/* spec-2.35 D15 — fault injection.  SKIP simulates evict race:
	 * holder pretends buffer is not cached so we exercise the HC105
	 * DENIED_MASTER_NOT_HOLDER + sender retransmit budget path. */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-evict-holder-before-ship");
	if (cluster_injection_should_skip("cluster-gcs-block-evict-holder-before-ship"))
		holder_ship_ok = false;
	else
		holder_ship_ok = gcs_block_get_ship_image(
			fwd->tag, fwd->original_requester_node, true, &page_lsn, block_buf, &block_payload,
			&block_payload_lkey, &block_payload_release_cb, &block_payload_release_arg, &sf_dep_vec,
			&sf_dep_valid);

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
						   && cluster_itl_page_has_active_slot((Page)block_payload)
						   /* PGRAC: spec-6.12g D-g2 — with block self-containment
							* the active-ITL X-transfer is NO LONGER deferred: the
							* block ships WITH its uncommitted ITL and is dropped
							* here (falls to the destructive-transfer branch below).
							* The holder's later commit skips the stamp for this
							* now-drifted block (D-g1) and readers resolve the
							* migrated ACTIVE slot through the TT authority (AD-006).
							* A same-ROW writer on the new holder still serializes
							* through the cross-node TX enqueue wait (spec-5.2 D4/D5,
							* t/280); only the same-block DIFFERENT-row false
							* serialization is removed. */
						   && !cluster_block_self_contained)) {
				/* Holder keeps its X; the requester consumes this image —
				 * read-image (D2) reads once and never registers as an S holder,
				 * X-transfer deferral (D11) sees the row lock and waits, retrying
				 * the transfer once we are terminal.  No local state change: the
				 * holder forward handler never mutates its own PCM lock and never
				 * drops a block on which it still owns an active ITL slot. */
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

					/*
					 * PGRAC: spec-6.12g D-g2 — count a transfer that carries an
					 * uncommitted ITL slot (the D11 deferral we just lifted).
					 * block_payload still points at the current image here, so
					 * the active-ITL probe is exact.
					 */
					if (cluster_block_self_contained
						&& cluster_itl_page_has_active_slot((Page)block_payload))
						cluster_lever_g_note_active_itl_transfer();

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
							if (dres == CLUSTER_BUFMGR_GCS_DROP_STALE)
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->xfer_stale_deny_count, 1);
							else
								pg_atomic_fetch_add_u64(&ClusterGcsBlock->drop_pinned_deny_count,
														1);
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
		hdr->checksum = gcs_block_compute_checksum(buf + header_len);
		hdr->status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_holder_evicted_count, 1);
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
	if (node_id < 0 || node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
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


typedef struct GcsBlockPcmXRequesterCleanupContext {
	bool active;
	bool wait_published;
	bool handle_live;
	bool claim_live;
	bool wfg_live;
	bool cutoff_started;
	bool reservation_started;
	bool ownership_committed;
	bool fail_closed_required;
	uint64 wfg_generation;
	bool *claim_handoff_out;
	PcmXLocalHandle handle;
	PcmXLocalWriterClaim claim;
} GcsBlockPcmXRequesterCleanupContext;

static GcsBlockPcmXRequesterCleanupContext gcs_block_pcm_x_requester_cleanup_context;
static bool gcs_block_pcm_x_requester_exit_hook_registered = false;

/* Source line of this backend's most recent non-OK acquire_writer_impl exit.
 * Diagnostic only: consumed by the bufmgr failure report so a client-visible
 * writer error names the exact escape arm instead of just the result code. */
static int gcs_block_pcm_x_requester_fail_line = 0;

/* Own-tuple evidence captured at the most recent non-OK remote-reservation
 * preflight.  Diagnostic only: printed by the fail-closed LOG so a STALE
 * verdict names which preflight arm fired (base-generation drift vs a
 * non-N pcm_state vs a live lifecycle flag) without a debugger. */
typedef struct GcsBlockPcmXPreflightEvidence {
	bool valid;
	bool rebase_wire_active;
	uint8 pcm_state;
	uint32 own_flags;
	uint64 live_generation;
	uint64 base_own_generation;
} GcsBlockPcmXPreflightEvidence;

static GcsBlockPcmXPreflightEvidence gcs_block_pcm_x_requester_preflight_evidence;

/* A' rebase: publication is legal only under an ACTIVE runtime whose bound
 * formation had full PCM_X_REBASE_V1 coverage at activation. */
static bool
gcs_block_pcm_x_rebase_wire_active(const PcmXRuntimeSnapshot *runtime)
{
	return runtime != NULL && runtime->state == PCM_X_RUNTIME_ACTIVE && runtime->rebase_wire_active;
}

/* The V2 (112-byte) frame exists on the wire only to carry a nonzero rebase;
 * every other INSTALL_READY stays the V1 104-byte exact frame, so a formation
 * without full V2 coverage never emits a length an old master would refuse
 * (a nonzero rebase is impossible there: publication is gated on coverage). */
static uint16
gcs_block_pcm_x_install_ready_wire_len(const PcmXInstallReadyPayload *install_ready)
{
	return install_ready->rebased_own_generation != 0 ? (uint16)sizeof(*install_ready)
													  : (uint16)PCM_X_INSTALL_READY_V1_LEN;
}

/* Drift is rebase-eligible only as the exact clean-N shape: same tag, idle
 * lifecycle flags, pcm_state N and a strictly newer live generation.  Any
 * other preflight STALE (tag churn, non-N state, live flags) keeps the
 * fail-closed verdict, as does a formation without full V2 coverage. */
static bool
gcs_block_pcm_x_reservation_rebase_eligible(const ClusterPcmOwnSnapshot *live,
											const PcmXWaitIdentity *identity,
											const PcmXRuntimeSnapshot *runtime)
{
	return gcs_block_pcm_x_rebase_wire_active(runtime) && live != NULL && identity != NULL
		   && BufferTagsEqual(&live->tag, &identity->tag) && live->pcm_state == (uint8)PCM_STATE_N
		   && live->flags == 0 && live->generation != UINT64_MAX
		   && live->generation > identity->base_own_generation;
}

#define GCS_BLOCK_PCM_X_REQUESTER_DONE()                                                           \
	do {                                                                                           \
		gcs_block_pcm_x_requester_fail_line = __LINE__;                                            \
		goto requester_done;                                                                       \
	} while (0)

#define GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED()                                                    \
	do {                                                                                           \
		gcs_block_pcm_x_requester_fail_line = __LINE__;                                            \
		goto requester_fail_closed;                                                                \
	} while (0)

int
cluster_gcs_pcm_x_requester_last_fail_line(void)
{
	return gcs_block_pcm_x_requester_fail_line;
}

static bool gcs_block_pcm_x_revalidate_peer_binding(int32 node_id, uint64 epoch, uint64 session);

static void
gcs_block_pcm_x_requester_wait(uint32 *wait_index)
{
	uint32 current;

	if (wait_index == NULL)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster PCM-X requester wait has no backoff state")));
	current = *wait_index;
	CHECK_FOR_INTERRUPTS();
	(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					cluster_pcm_x_holder_retry_delay_ms(current),
					WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT);
	ResetLatch(MyLatch);
	*wait_index = cluster_gcs_pcm_x_requester_wait_index_advance(current);
	CHECK_FOR_INTERRUPTS();
}

/* Retry only inside the exact formation that admitted this requester.  Both
 * sides of the sleep are checked: a runtime fence or peer-session change that
 * lands while the latch is asleep must be observed before the next protocol
 * read or mutation. */
static bool
gcs_block_pcm_x_requester_wait_exact(uint32 *wait_index, const PcmXRuntimeSnapshot *request_runtime,
									 int32 master_node, uint64 cluster_epoch, uint64 master_session)
{
	if (!gcs_block_pcm_x_requester_authority_exact(request_runtime, master_node, cluster_epoch,
												   master_session))
		return false;
	gcs_block_pcm_x_requester_wait(wait_index);
	return gcs_block_pcm_x_requester_authority_exact(request_runtime, master_node, cluster_epoch,
													 master_session);
}

static void
gcs_block_pcm_x_requester_clear_wait(GcsBlockPcmXRequesterCleanupContext *cleanup)
{
	if (cleanup == NULL || !cleanup->wait_published)
		return;
	if (MyProc != NULL)
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	cleanup->wait_published = false;
}

/* Bound the STALE cancel refresh loop:  each retry means the slot churned
 * again inside a lock-to-lock window, so more than a few consecutive hits
 * are no longer plausible scheduling and keep the fail-closed verdict. */
#define GCS_BLOCK_PCM_X_CLEANUP_REFRESH_MAX 3

static PcmXQueueResult
gcs_block_pcm_x_requester_cleanup_impl(GcsBlockPcmXRequesterCleanupContext *cleanup,
									   bool owner_exit)
{
	GcsBlockPcmXCleanupAction action;
	PcmXLocalHandle promoted;
	PcmXLocalHandle refreshed;
	PcmXQueueResult result;
	PcmXRuntimeSnapshot runtime;

	if (cleanup == NULL || !cleanup->active)
		return PCM_X_QUEUE_OK;
	if (cleanup->claim_handoff_out != NULL && *cleanup->claim_handoff_out) {
		/* bufmgr published the exact claim in its owner-exit ledger before
		 * requester authority was cleared.  From that instant it is the sole
		 * cleanup owner, including an exit in this short handoff window. */
		pg_read_barrier();
		memset(cleanup, 0, sizeof(*cleanup));
		return PCM_X_QUEUE_OK;
	}
	if (cleanup->wfg_live) {
		ClusterLmdGraphRemoveResult remove_result;
		ClusterLmdVertex waiter;

		cluster_gcs_pcm_x_vertex_from_identity(&cleanup->handle.identity, &waiter);
		remove_result = cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter);
		if (remove_result == CLUSTER_LMD_GRAPH_REMOVE_STALE) {
			cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();
			cluster_pcm_x_runtime_fail_closed();
			gcs_block_pcm_x_requester_clear_wait(cleanup);
			return PCM_X_QUEUE_CORRUPT;
		}
		if (remove_result == CLUSTER_LMD_GRAPH_REMOVE_REMOVED)
			cluster_lmd_pcm_convert_wfg_note_remove();
		result = cluster_pcm_x_local_follower_wfg_clear_exact(&cleanup->handle,
															  cleanup->wfg_generation);
		cluster_pcm_x_stats_note_queue_result(result);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
			runtime = cluster_pcm_x_runtime_snapshot();
			if (owner_exit
				&& cluster_pcm_x_owner_exit_action(result, false,
												   runtime.state == PCM_X_RUNTIME_ACTIVE
													   && runtime.master_session_incarnation != 0)
					   == CLUSTER_PCM_X_OWNER_EXIT_RETRY) {
				gcs_block_pcm_x_requester_clear_wait(cleanup);
				return result;
			}
			cluster_pcm_x_runtime_fail_closed();
			gcs_block_pcm_x_requester_clear_wait(cleanup);
			return result;
		}
		cleanup->wfg_live = false;
		cleanup->wfg_generation = 0;
	}
	gcs_block_pcm_x_requester_clear_wait(cleanup);
	action = cluster_gcs_pcm_x_requester_cleanup_action(
		cleanup->handle_live, cleanup->claim_live,
		cleanup->cutoff_started || cleanup->reservation_started, cleanup->ownership_committed);
	/* A claimed writer is already the active member of a closed FIFO cohort.
	 * Every exit path must record WRITER_COMPLETE before either cancelling a
	 * still-reversible membership or preserving irreversible evidence behind the
	 * recovery gate.  abort_exact would clear active_writer without COMPLETE and
	 * permanently strand every successor behind this backend. */
	if (cleanup->claim_live) {
		result = cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(&cleanup->claim);
		cluster_pcm_x_stats_note_queue_result(result);
		if (result != PCM_X_QUEUE_OK) {
			runtime = cluster_pcm_x_runtime_snapshot();
			if (owner_exit
				&& cluster_pcm_x_owner_exit_action(result, false,
												   runtime.state == PCM_X_RUNTIME_ACTIVE
													   && runtime.master_session_incarnation != 0)
					   == CLUSTER_PCM_X_OWNER_EXIT_RETRY)
				return result;
			cluster_pcm_x_runtime_fail_closed();
			return result;
		}
		cleanup->claim_live = false;
	}
	if (action == GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_NOT_READY;
	}
	if (action == GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL) {
		int refresh_attempts = 0;

		for (;;) {
			memset(&promoted, 0, sizeof(promoted));
			result = cluster_pcm_x_local_cancel_exact(&cleanup->handle, &promoted);
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
				result = cluster_pcm_x_local_detach_terminal_exact(&cleanup->handle);
				cluster_pcm_x_stats_note_queue_result(result);
			}
			if (result != PCM_X_QUEUE_STALE
				|| refresh_attempts >= GCS_BLOCK_PCM_X_CLEANUP_REFRESH_MAX)
				break;
			/* STALE proves the membership advanced (promotion or round churn)
			 * under an identity that is still exactly ours.  Cancelling
			 * releases rather than confers authority, so rebuild the handle
			 * from the live slot and retry;  a vanished membership is already
			 * terminal and leaves nothing to cancel. */
			refresh_attempts++;
			result = cluster_pcm_x_local_lookup_exact(&cleanup->handle.identity, &refreshed);
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_NOT_FOUND)
				break;
			if (result != PCM_X_QUEUE_OK)
				break;
			cleanup->handle = refreshed;
		}
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_NOT_FOUND) {
			runtime = cluster_pcm_x_runtime_snapshot();
			if (owner_exit
				&& cluster_pcm_x_owner_exit_action(result, false,
												   runtime.state == PCM_X_RUNTIME_ACTIVE
													   && runtime.master_session_incarnation != 0)
					   == CLUSTER_PCM_X_OWNER_EXIT_RETRY)
				return result;
			cluster_pcm_x_runtime_fail_closed();
			return result;
		}
	}
	memset(cleanup, 0, sizeof(*cleanup));
	return PCM_X_QUEUE_OK;
}

static PcmXQueueResult
gcs_block_pcm_x_requester_cleanup_attempt_noexcept(GcsBlockPcmXRequesterCleanupContext *cleanup,
												   bool owner_exit)
{
	volatile PcmXQueueResult result = PCM_X_QUEUE_CORRUPT;

	PG_TRY();
	{
		result = gcs_block_pcm_x_requester_cleanup_impl(cleanup, owner_exit);
	}
	PG_CATCH();
	{
		FlushErrorState();
		cluster_pcm_x_runtime_fail_closed();
		gcs_block_pcm_x_requester_clear_wait(cleanup);
		result = PCM_X_QUEUE_CORRUPT;
	}
	PG_END_TRY();
	return (PcmXQueueResult)result;
}

static void
gcs_block_pcm_x_requester_cleanup_noexcept(GcsBlockPcmXRequesterCleanupContext *cleanup)
{
	/* Ordinary ERROR cleanup stays non-waiting.  If one exact attempt cannot
	 * close the claim/handle, cleanup_impl preserves shared evidence and closes
	 * the runtime; there is no safe reason to stall an error stack indefinitely.
	 * Backend death uses the dedicated retry loop below. */
	(void)gcs_block_pcm_x_requester_cleanup_attempt_noexcept(cleanup, false);
}

static void
gcs_block_pcm_x_requester_owner_exit(int code pg_attribute_unused(),
									 Datum arg pg_attribute_unused())
{
	GcsBlockPcmXRequesterCleanupContext *cleanup = &gcs_block_pcm_x_requester_cleanup_context;

	while (cleanup->active) {
		PcmXQueueResult result;
		PcmXRuntimeSnapshot runtime;

		result = gcs_block_pcm_x_requester_cleanup_attempt_noexcept(cleanup, true);
		if (!cleanup->active || result == PCM_X_QUEUE_OK)
			return;
		runtime = cluster_pcm_x_runtime_snapshot();
		if (cluster_pcm_x_owner_exit_action(result, false,
											runtime.state == PCM_X_RUNTIME_ACTIVE
												&& runtime.master_session_incarnation != 0)
			!= CLUSTER_PCM_X_OWNER_EXIT_RETRY) {
			cluster_pcm_x_runtime_fail_closed();
			return;
		}
		pg_usleep(1000L);
	}
}


/* Bind an idle node leader to one stable ownership tuple before its first
 * ENQUEUE.  A predecessor round may have advanced ownership while this member
 * waited as a follower.  The queue key moves under its admission gate, then a
 * second snapshot proves that no ownership lifecycle crossed that move. */
static PcmXQueueResult
gcs_block_pcm_x_rekey_leader_base_exact(BufferDesc *buf, PcmXLocalHandle *leader,
										ClusterPcmOwnSnapshot *base_out)
{
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;
	PcmXLocalHandle rekeyed;
	ClusterPcmOwnResult own_result;
	ClusterPcmOwnResult live_result;
	PcmXQueueResult result;

	if (base_out != NULL)
		memset(base_out, 0, sizeof(*base_out));
	if (buf == NULL || leader == NULL || base_out == NULL
		|| leader->role != PCM_X_LOCAL_ROLE_NODE_LEADER)
		return PCM_X_QUEUE_INVALID;
	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &before);
	result = gcs_block_pcm_x_fetch_own_result(own_result);
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (!BufferTagsEqual(&before.tag, &leader->identity.tag)
		|| (before.pcm_state != (uint8)PCM_STATE_N && before.pcm_state != (uint8)PCM_STATE_S
			&& before.pcm_state != (uint8)PCM_STATE_X))
		return PCM_X_QUEUE_STALE;
	if (before.generation == UINT64_MAX)
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	live_result = cluster_pcm_own_classify_live_flags(before.flags, before.reservation_token);
	result = gcs_block_pcm_x_fetch_own_result(live_result);
	if (result != PCM_X_QUEUE_OK)
		return result;

	result = cluster_pcm_x_local_leader_rekey_generation_exact(leader, before.generation, &rekeyed);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	*leader = rekeyed;

	own_result = cluster_bufmgr_pcm_own_snapshot(buf, &after);
	result = gcs_block_pcm_x_fetch_own_result(own_result);
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (!BufferTagsEqual(&after.tag, &before.tag) || after.generation < before.generation)
		return PCM_X_QUEUE_STALE;
	live_result = cluster_pcm_own_classify_live_flags(after.flags, after.reservation_token);
	result = gcs_block_pcm_x_fetch_own_result(live_result);
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (after.generation != before.generation || after.reservation_token != before.reservation_token
		|| after.flags != before.flags || after.pcm_state != before.pcm_state)
		return PCM_X_QUEUE_BUSY;
	*base_out = after;
	return PCM_X_QUEUE_OK;
}


/*
 * Drive one ordinary BM_VALID N/S/X -> X request through the node FIFO.
 *
 * The returned writer claim is deliberately still live.  It is the local
 * execution right, not merely an admission receipt: bufmgr keeps it across
 * the content LWLock and records WRITER_COMPLETE only after UNLOCK.  A node
 * leader owns the one remote protocol; followers publish an exact local WFG
 * edge and consume the already-granted node X in FIFO order.
 */
static PcmXQueueResult
gcs_block_pcm_x_acquire_writer_impl(BufferDesc *buf, PcmXLocalWriterClaim *claim_out)
{
	PcmXLocalFollowerWfgSnapshot follower_snapshot;
	PcmXLocalHandle handle;
	PcmXLocalHandle fresh_handle;
	PcmXLocalCutoff cutoff;
	PcmXLocalProgress progress;
	PcmXRuntimeSnapshot request_runtime;
	ClusterLmdVertex blocker_vertex;
	ClusterLmdVertex waiter_vertex;
	ClusterPcmOwnSnapshot initial_own;
	ClusterPcmOwnSnapshot reservation_base;
	ClusterPcmOwnResult live_result;
	ClusterPcmOwnResult own_result;
	GcsBlockPcmXRetryAction retry_action;
	PcmXQueueResult arm_result = PCM_X_QUEUE_NOT_READY;
	PcmXQueueResult result;
	TransactionId xid;
	uint64 cluster_epoch;
	uint64 committed_generation = 0;
	uint64 follower_graph_generation = 0;
	uint64 master_session = 0;
	uint64 request_id = 0;
	uint64 reservation_token = 0;
	uint64 wait_seq = 0;
	uint32 wait_index = 0;
	int32 master_node;
	const char *fail_site = "entry";
	bool image_installed = false;
	bool ownership_committed = false;
	bool reservation_started = false;
	bool self_source = false;
	bool wait_published = false;

	if (claim_out != NULL)
		memset(claim_out, 0, sizeof(*claim_out));
	if (buf == NULL || claim_out == NULL || ClusterPcmXConvertShmem == NULL || MyProc == NULL
		|| MyProc->pgprocno < 0 || MyBackendId <= 0 || cluster_node_id < 0
		|| cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_QUEUE_INVALID;
	/* Preflight evidence must belong to this acquire, never a previous one. */
	memset(&gcs_block_pcm_x_requester_preflight_evidence, 0,
		   sizeof(gcs_block_pcm_x_requester_preflight_evidence));
	memset(&handle, 0, sizeof(handle));
	handle.tag_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	handle.membership_slot.slot_index = PCM_X_INVALID_SLOT_INDEX;
	memset(&progress, 0, sizeof(progress));
	for (;;) {
		GcsBlockPcmXFormationAction formation_action;

		request_runtime = cluster_pcm_x_runtime_snapshot();
		formation_action = cluster_gcs_pcm_x_requester_formation_action(&request_runtime);
		if (formation_action == GCS_BLOCK_PCM_X_FORMATION_PROCEED)
			break;
		if (formation_action != GCS_BLOCK_PCM_X_FORMATION_WAIT)
			return PCM_X_QUEUE_NOT_READY;
		gcs_block_pcm_x_requester_wait(&wait_index);
	}
	cluster_epoch = cluster_epoch_get_current();
	master_node = cluster_gcs_lookup_master(buf->tag);
	if (master_node < 0 || master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| !gcs_block_pcm_x_authenticated_session(master_node, cluster_epoch, &master_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(master_node, cluster_epoch, master_session))
		return PCM_X_QUEUE_NOT_READY;
	/* The authenticated session above is a current-view proof.  Before the
	 * first wait identity becomes visible, bind it back to the exact formation
	 * snapshot that admitted this call so a close/rejoin window cannot publish
	 * or enqueue a mixed-generation requester. */
	if (!gcs_block_pcm_x_requester_authority_exact(&request_runtime, master_node, cluster_epoch,
												   master_session))
		return PCM_X_QUEUE_NOT_READY;
	if (!gcs_block_pcm_x_next_request_id(&request_id)) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_COUNTER_EXHAUSTED;
	}
	xid = GetTopTransactionIdIfAny();
	wait_seq = cluster_lmd_wait_state_publish(
		&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_PCM_CONVERT, request_id, cluster_epoch, xid);
	if (wait_seq == 0) {
		cluster_pcm_x_runtime_fail_closed();
		result = PCM_X_QUEUE_COUNTER_EXHAUSTED;
		GCS_BLOCK_PCM_X_REQUESTER_DONE();
	}
	wait_published = true;
	gcs_block_pcm_x_requester_cleanup_context.wait_published = true;

	/* Publish first, then close the freeze-before-wait race for every content
	 * lock already held by this backend.  If a holder barrier is already frozen
	 * we must not block: that snapshot cannot retroactively acquire this nested
	 * edge.  If it freezes later, the published PCM_CONVERT identity is visible
	 * to the normal blocker snapshot/WFG path. */
	for (;;) {
		result = cluster_pcm_x_nested_wait_guard_before_block();
		cluster_pcm_x_stats_note_queue_result(result);
		if (result == PCM_X_QUEUE_OK)
			break;
		if (result == PCM_X_QUEUE_CORRUPT)
			GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
		if (!cluster_gcs_pcm_x_nested_guard_retryable(result))
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
												  cluster_epoch, master_session)) {
			result = PCM_X_QUEUE_NOT_READY;
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		}
	}

	/* Ownership reservations and READ_IMAGE are protocol contention too.  Keep
	 * their wait inside the published PCM_CONVERT interval; bufmgr must never
	 * sleep on these shapes before the nested-wait guard. */
	for (;;) {
		own_result = cluster_bufmgr_pcm_own_snapshot(buf, &initial_own);
		result = gcs_block_pcm_x_fetch_own_result(own_result);
		if (result != PCM_X_QUEUE_OK) {
			if (result == PCM_X_QUEUE_CORRUPT)
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		}
		if (!BufferTagsEqual(&initial_own.tag, &buf->tag) || initial_own.generation == UINT64_MAX) {
			result = PCM_X_QUEUE_STALE;
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		}
		live_result
			= cluster_pcm_own_classify_live_flags(initial_own.flags, initial_own.reservation_token);
		result = gcs_block_pcm_x_fetch_own_result(live_result);
		if (result == PCM_X_QUEUE_CORRUPT)
			GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
		if (initial_own.pcm_state == (uint8)PCM_STATE_READ_IMAGE || result == PCM_X_QUEUE_BUSY) {
			if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
													  cluster_epoch, master_session)) {
				result = PCM_X_QUEUE_NOT_READY;
				GCS_BLOCK_PCM_X_REQUESTER_DONE();
			}
			continue;
		}
		if (result != PCM_X_QUEUE_OK)
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		if (initial_own.pcm_state != (uint8)PCM_STATE_N
			&& initial_own.pcm_state != (uint8)PCM_STATE_S
			&& initial_own.pcm_state != (uint8)PCM_STATE_X) {
			result = PCM_X_QUEUE_STALE;
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		}
		break;
	}
	for (;;) {
		PcmXWaitIdentity identity;

		memset(&identity, 0, sizeof(identity));
		identity.tag = buf->tag;
		identity.node_id = cluster_node_id;
		identity.procno = (uint32)MyProc->pgprocno;
		identity.xid = xid;
		identity.cluster_epoch = cluster_epoch;
		identity.request_id = request_id;
		identity.wait_seq = wait_seq;
		identity.base_own_generation = initial_own.generation;
		fail_site = "local-join";
		result = cluster_pcm_x_local_join_begin(&identity, master_node, master_session, &handle);
		cluster_pcm_x_stats_note_queue_result(result);
		if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
			break;
		retry_action
			= cluster_gcs_pcm_x_requester_retry_action(GCS_BLOCK_PCM_X_RETRY_SITE_JOIN, result);
		if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
			GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
		if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
												  cluster_epoch, master_session)) {
			result = PCM_X_QUEUE_NOT_READY;
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		}
	}
	gcs_block_pcm_x_requester_cleanup_context.handle = handle;
	gcs_block_pcm_x_requester_cleanup_context.handle_live = true;


requester_role_dispatch:
	/* The leader claims before ENQUEUE, so every later local writer has one
	 * concrete FIFO/WFG blocker while the remote grant is in flight. */
	if (handle.role == PCM_X_LOCAL_ROLE_NODE_LEADER) {
		for (;;) {
			fail_site = "leader-rekey";
			result = gcs_block_pcm_x_rekey_leader_base_exact(buf, &handle, &initial_own);
			gcs_block_pcm_x_requester_cleanup_context.handle = handle;
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_OK)
				break;
			retry_action = cluster_gcs_pcm_x_requester_retry_action(
				GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY, result);
			if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
													  cluster_epoch, master_session)) {
				result = PCM_X_QUEUE_NOT_READY;
				GCS_BLOCK_PCM_X_REQUESTER_DONE();
			}
		}
		for (;;) {
			fail_site = "leader-claim";
			result = cluster_pcm_x_local_writer_claim_exact(&handle, claim_out);
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_OK) {
				gcs_block_pcm_x_requester_cleanup_context.claim = *claim_out;
				gcs_block_pcm_x_requester_cleanup_context.claim_live = true;
				break;
			}
			retry_action = cluster_gcs_pcm_x_requester_retry_action(
				GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM, result);
			if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
													  cluster_epoch, master_session)) {
				result = PCM_X_QUEUE_NOT_READY;
				GCS_BLOCK_PCM_X_REQUESTER_DONE();
			}
		}
		for (;;) {
			/* Treat the call window as irreversible.  The freeze helper may have
			 * closed the cohort before an injected ERROR reaches this frame. */
			gcs_block_pcm_x_requester_cleanup_context.cutoff_started = true;
			fail_site = "leader-cutoff";
			result = cluster_pcm_x_local_begin_revoke_cutoff_exact(&handle, &cutoff);
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
				break;
			retry_action = cluster_gcs_pcm_x_requester_retry_action(
				GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF, result);
			if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			gcs_block_pcm_x_requester_cleanup_context.cutoff_started = false;
			if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
													  cluster_epoch, master_session)) {
				result = PCM_X_QUEUE_NOT_READY;
				GCS_BLOCK_PCM_X_REQUESTER_DONE();
			}
		}
	} else if (handle.role == PCM_X_LOCAL_ROLE_FOLLOWER) {
		for (;;) {
			ClusterLmdGraphRemoveResult remove_result;

			ResetLatch(MyLatch);
			if (follower_graph_generation != 0) {
				cluster_gcs_pcm_x_vertex_from_identity(&handle.identity, &waiter_vertex);
				remove_result
					= cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter_vertex);
				if (remove_result == CLUSTER_LMD_GRAPH_REMOVE_STALE) {
					cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();
					GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
				if (remove_result == CLUSTER_LMD_GRAPH_REMOVE_REMOVED)
					cluster_lmd_pcm_convert_wfg_note_remove();
				result = cluster_pcm_x_local_follower_wfg_clear_exact(&handle,
																	  follower_graph_generation);
				cluster_pcm_x_stats_note_queue_result(result);
				if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
					GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				follower_graph_generation = 0;
				gcs_block_pcm_x_requester_cleanup_context.wfg_live = false;
				gcs_block_pcm_x_requester_cleanup_context.wfg_generation = 0;
			}
			fail_site = "follower-claim";
			result = cluster_pcm_x_local_writer_claim_exact(&handle, claim_out);
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_OK) {
				gcs_block_pcm_x_requester_cleanup_context.claim = *claim_out;
				gcs_block_pcm_x_requester_cleanup_context.claim_live = true;
				break;
			}
			if (result == PCM_X_QUEUE_STALE) {
				fail_site = "follower-refresh-lookup";
				result = cluster_pcm_x_local_lookup_exact(&handle.identity, &fresh_handle);
				cluster_pcm_x_stats_note_queue_result(result);
				if (result == PCM_X_QUEUE_OK) {
					fail_site = "follower-refresh-compare";
					if (!cluster_gcs_pcm_x_role_refresh_exact(&handle, &fresh_handle))
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					handle = fresh_handle;
					gcs_block_pcm_x_requester_cleanup_context.handle = handle;
					goto requester_role_dispatch;
				}
				retry_action = cluster_gcs_pcm_x_requester_retry_action(
					GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, result);
				if (retry_action == GCS_BLOCK_PCM_X_RETRY_WAIT) {
					if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime,
															  master_node, cluster_epoch,
															  master_session)) {
						result = PCM_X_QUEUE_NOT_READY;
						GCS_BLOCK_PCM_X_REQUESTER_DONE();
					}
					continue;
				}
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			}
			retry_action = cluster_gcs_pcm_x_requester_retry_action(
				GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM, result);
			if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();

			fail_site = "follower-wfg-snapshot";
			result = cluster_pcm_x_local_follower_wfg_snapshot_exact(&handle, &follower_snapshot);
			cluster_pcm_x_stats_note_queue_result(result);
			if (result == PCM_X_QUEUE_OK) {
				cluster_gcs_pcm_x_vertex_from_identity(&follower_snapshot.waiter.identity,
													   &waiter_vertex);
				cluster_gcs_pcm_x_vertex_from_identity(&follower_snapshot.blocker, &blocker_vertex);
				follower_graph_generation = cluster_lmd_graph_replace_waiter_edges_exact(
					&waiter_vertex, &blocker_vertex, 1, handle.identity.request_id);
				if (follower_graph_generation == 0) {
					cluster_lmd_pcm_convert_wfg_note_replace_fail();
					GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
				cluster_lmd_pcm_convert_wfg_note_replace();
				gcs_block_pcm_x_requester_cleanup_context.wfg_live = true;
				gcs_block_pcm_x_requester_cleanup_context.wfg_generation
					= follower_graph_generation;
				result = cluster_pcm_x_local_follower_wfg_commit_exact(&follower_snapshot,
																	   follower_graph_generation);
				cluster_pcm_x_stats_note_queue_result(result);
				if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
					retry_action = cluster_gcs_pcm_x_requester_retry_action(
						GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT, result);
					remove_result
						= cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter_vertex);
					if (remove_result == CLUSTER_LMD_GRAPH_REMOVE_STALE) {
						cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					}
					if (remove_result == CLUSTER_LMD_GRAPH_REMOVE_REMOVED)
						cluster_lmd_pcm_convert_wfg_note_remove();
					follower_graph_generation = 0;
					gcs_block_pcm_x_requester_cleanup_context.wfg_live = false;
					gcs_block_pcm_x_requester_cleanup_context.wfg_generation = 0;
					if (retry_action != GCS_BLOCK_PCM_X_RETRY_RESNAPSHOT_WFG)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
			} else {
				retry_action = cluster_gcs_pcm_x_requester_retry_action(
					GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, result);
				if (retry_action == GCS_BLOCK_PCM_X_RETRY_REFRESH_ROLE) {
					/* The snapshot proved this handle no longer byte-matches
					 * its membership slot -- the same promotion / round churn
					 * the claim site recovers from.  Rebuild the handle and
					 * re-dispatch by its current role instead of closing the
					 * runtime over a normal FIFO progress event. */
					fail_site = "follower-refresh-lookup";
					result = cluster_pcm_x_local_lookup_exact(&handle.identity, &fresh_handle);
					cluster_pcm_x_stats_note_queue_result(result);
					if (result == PCM_X_QUEUE_OK) {
						fail_site = "follower-refresh-compare";
						if (!cluster_gcs_pcm_x_role_refresh_exact(&handle, &fresh_handle))
							GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
						handle = fresh_handle;
						gcs_block_pcm_x_requester_cleanup_context.handle = handle;
						goto requester_role_dispatch;
					}
					retry_action = cluster_gcs_pcm_x_requester_retry_action(
						GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, result);
				}
				if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
					GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			}
			if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
													  cluster_epoch, master_session)) {
				result = PCM_X_QUEUE_NOT_READY;
				GCS_BLOCK_PCM_X_REQUESTER_DONE();
			}
		}
		result = PCM_X_QUEUE_OK;
		GCS_BLOCK_PCM_X_REQUESTER_DONE();
	} else {
		result = PCM_X_QUEUE_CORRUPT;
		GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
	}

	/* Node leader: every iteration reconstructs the exact durable outbound
	 * leg.  A full DATA ring is backpressure, never a client-visible timeout. */
	for (;;) {
		bool staged = false;

		ResetLatch(MyLatch);
		fail_site = "leader-progress";
		result = cluster_pcm_x_local_progress_exact(&handle, &progress);
		cluster_pcm_x_stats_note_queue_result(result);
		if (result != PCM_X_QUEUE_OK) {
			retry_action = cluster_gcs_pcm_x_requester_retry_action(
				GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, result);
			if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			goto requester_wait;
		}
		if (progress.role != PCM_X_LOCAL_ROLE_NODE_LEADER || progress.master_node != master_node
			|| progress.master_session_incarnation != master_session)
			GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();

		if (progress.member_state == PCM_XL_NODE_LEADER) {
			if ((progress.pending_opcode == 0 && progress.last_response_opcode == 0)
				|| progress.pending_opcode == PGRAC_IC_MSG_PCM_X_ENQUEUE) {
				PcmXEnqueuePayload enqueue;
				PcmXLocalReliableToken token;

				arm_result = cluster_pcm_x_local_enqueue_arm_exact(&handle, &enqueue, &token);
				cluster_pcm_x_stats_note_queue_result(arm_result);
				result = arm_result;
				if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
					staged = cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_ENQUEUE, master_node,
														   &enqueue, sizeof(enqueue));
				else {
					retry_action = cluster_gcs_pcm_x_requester_retry_action(
						GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, arm_result);
					if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT
						&& retry_action != GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
			} else if (progress.pending_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM
					   || (progress.pending_opcode == 0
						   && progress.last_response_opcode == PGRAC_IC_MSG_PCM_X_ADMIT_ACK)) {
				PcmXPhasePayload confirm;
				PcmXLocalReliableToken token;

				arm_result = cluster_pcm_x_local_admit_confirm_arm_exact(&handle, &confirm, &token);
				cluster_pcm_x_stats_note_queue_result(arm_result);
				result = arm_result;
				if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
					staged = cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM,
														   master_node, &confirm, sizeof(confirm));
				else {
					retry_action = cluster_gcs_pcm_x_requester_retry_action(
						GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, arm_result);
					if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT
						&& retry_action != GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
			}
		} else if (progress.member_state == PCM_XL_REMOTE_WAIT) {
			if (progress.pending_opcode == 0
				&& progress.last_response_opcode == PGRAC_IC_MSG_PCM_X_PREPARE_GRANT) {
				PcmXInstallReadyPayload install_ready;
				PcmXLocalReliableToken token;

				if (!reservation_started) {
					own_result = cluster_bufmgr_pcm_own_snapshot(buf, &reservation_base);
					result = gcs_block_pcm_x_fetch_own_result(own_result);
					if (result != PCM_X_QUEUE_OK)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					self_source = progress.image.source_node == (uint32)cluster_node_id;
					if (self_source)
						result = cluster_gcs_pcm_x_adopt_self_image(buf, &handle, &reservation_base,
																	&reservation_token);
					else {
						result = cluster_gcs_pcm_x_remote_reservation_preflight(&reservation_base,
																				&progress.identity);
						cluster_pcm_x_stats_note_queue_result(result);
						if (result == PCM_X_QUEUE_STALE
							&& gcs_block_pcm_x_reservation_rebase_eligible(
								&reservation_base, &progress.identity, &request_runtime)) {
							/* An interleaved revoke consumed the enqueue-time
							 * base while this request was queued.  Publish the
							 * live clean-N generation as the effective grant
							 * base (one-shot; replays are DUPLICATE) and carry
							 * it on the writer claim for the bufmgr cross
							 * check.  The immutable identity never changes. */
							result = cluster_pcm_x_local_grant_rebase_publish_exact(
								&handle, reservation_base.generation);
							cluster_pcm_x_stats_note_queue_result(result);
							if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
								claim_out->grant_base_own_generation = reservation_base.generation;
								result = PCM_X_QUEUE_OK;
							}
						}
						if (result != PCM_X_QUEUE_OK) {
							GcsBlockPcmXPreflightEvidence *evidence
								= &gcs_block_pcm_x_requester_preflight_evidence;

							evidence->valid = true;
							evidence->rebase_wire_active
								= gcs_block_pcm_x_rebase_wire_active(&request_runtime);
							evidence->pcm_state = reservation_base.pcm_state;
							evidence->own_flags = reservation_base.flags;
							evidence->live_generation = reservation_base.generation;
							evidence->base_own_generation = progress.identity.base_own_generation;
							retry_action = cluster_gcs_pcm_x_requester_retry_action(
								GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, result);
							if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
								GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
							goto requester_wait;
						}
						own_result = cluster_bufmgr_pcm_own_begin_x_reservation(
							buf, &reservation_base, &reservation_token);
						result = gcs_block_pcm_x_fetch_own_result(own_result);
					}
					cluster_pcm_x_stats_note_queue_result(result);
					if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					reservation_started = true;
					gcs_block_pcm_x_requester_cleanup_context.reservation_started = true;
				}
				if (!image_installed) {
					if (self_source)
						result = PCM_X_QUEUE_OK;
					else
						result = cluster_gcs_pcm_x_fetch_image_and_install(
							buf, &handle, &reservation_base, reservation_token, &request_runtime);
					cluster_pcm_x_stats_note_queue_result(result);
					if (result == PCM_X_QUEUE_OK)
						image_installed = true;
					else {
						retry_action = cluster_gcs_pcm_x_requester_retry_action(
							GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH, result);
						if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT)
							GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					}
				}
				if (image_installed) {
					arm_result = cluster_pcm_x_local_install_ready_arm_exact(
						&handle, &progress.ref, &progress.image, &install_ready, &token);
					cluster_pcm_x_stats_note_queue_result(arm_result);
					result = arm_result;
					if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
						staged = cluster_gcs_pcm_x_stage_frame(
							PGRAC_IC_MSG_PCM_X_INSTALL_READY, master_node, &install_ready,
							gcs_block_pcm_x_install_ready_wire_len(&install_ready));
					else {
						retry_action = cluster_gcs_pcm_x_requester_retry_action(
							GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, arm_result);
						if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT
							&& retry_action != GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS)
							GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					}
				}
			}
		} else if (progress.member_state == PCM_XL_CONTENT_ACTIVE) {
			if (progress.pending_opcode == PGRAC_IC_MSG_PCM_X_INSTALL_READY) {
				PcmXInstallReadyPayload install_ready;
				PcmXLocalReliableToken token;

				arm_result = cluster_pcm_x_local_install_ready_arm_exact(
					&handle, &progress.ref, &progress.image, &install_ready, &token);
				cluster_pcm_x_stats_note_queue_result(arm_result);
				result = arm_result;
				if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
					staged = cluster_gcs_pcm_x_stage_frame(
						PGRAC_IC_MSG_PCM_X_INSTALL_READY, master_node, &install_ready,
						gcs_block_pcm_x_install_ready_wire_len(&install_ready));
				else {
					retry_action = cluster_gcs_pcm_x_requester_retry_action(
						GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, arm_result);
					if (retry_action != GCS_BLOCK_PCM_X_RETRY_WAIT
						&& retry_action != GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
			} else if (progress.pending_opcode == 0
					   && progress.last_response_opcode == PGRAC_IC_MSG_PCM_X_COMMIT_X) {
				PcmXFinalAckPayload final_ack;
				PcmXLocalReliableToken token;

				if (!ownership_committed) {
					if (self_source)
						result = cluster_gcs_pcm_x_finish_self_image_x(
							buf, &handle, &reservation_base, reservation_token,
							&committed_generation);
					else {
						own_result = cluster_bufmgr_pcm_own_finish_x_commit(
							buf, &reservation_base, reservation_token, &committed_generation);
						result = gcs_block_pcm_x_fetch_own_result(own_result);
					}
					cluster_pcm_x_stats_note_queue_result(result);
					if (result != PCM_X_QUEUE_OK)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
					ownership_committed = true;
					gcs_block_pcm_x_requester_cleanup_context.ownership_committed = true;
				}
				arm_result = cluster_pcm_x_local_final_ack_arm_exact(&handle, committed_generation,
																	 &final_ack, &token);
				cluster_pcm_x_stats_note_queue_result(arm_result);
				result = arm_result;
				if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
					staged = cluster_gcs_pcm_x_stage_frame(
						PGRAC_IC_MSG_PCM_X_FINAL_ACK, master_node, &final_ack, sizeof(final_ack));
				else
					GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
			} else if (progress.pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_ACK) {
				PcmXFinalAckPayload final_ack;
				PcmXLocalReliableToken token;

				arm_result = cluster_pcm_x_local_final_ack_arm_exact(&handle, committed_generation,
																	 &final_ack, &token);
				cluster_pcm_x_stats_note_queue_result(arm_result);
				result = arm_result;
				if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
					staged = cluster_gcs_pcm_x_stage_frame(
						PGRAC_IC_MSG_PCM_X_FINAL_ACK, master_node, &final_ack, sizeof(final_ack));
				else {
					retry_action = cluster_gcs_pcm_x_requester_retry_action(
						GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM, arm_result);
					if (retry_action != GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS)
						GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
				}
			}
		} else if (progress.member_state == PCM_XL_GRANTED) {
			if (progress.pending_opcode == PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM) {
				PcmXPhasePayload final_confirm;

				memset(&final_confirm, 0, sizeof(final_confirm));
				final_confirm.ref = progress.ref;
				final_confirm.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
				staged
					= cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM, master_node,
													&final_confirm, sizeof(final_confirm));
				if (staged) {
					result = PCM_X_QUEUE_OK;
					break;
				}
			} else if (progress.pending_opcode == 0) {
				result = PCM_X_QUEUE_OK;
				break;
			} else
				GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();
		} else
			GCS_BLOCK_PCM_X_REQUESTER_FAIL_CLOSED();

	requester_wait:
		if (!gcs_block_pcm_x_requester_wait_exact(&wait_index, &request_runtime, master_node,
												  cluster_epoch, master_session)) {
			result = PCM_X_QUEUE_NOT_READY;
			GCS_BLOCK_PCM_X_REQUESTER_DONE();
		}
	}
	GCS_BLOCK_PCM_X_REQUESTER_DONE();

requester_fail_closed:
	/* Cleanup may cancel a purely local membership.  PREPARE/COMMIT evidence
	 * is never guessed away; the wrapper preserves it behind the recovery
	 * gate after clearing any exact external WFG edge. */
	gcs_block_pcm_x_requester_cleanup_context.fail_closed_required = true;
	ereport(LOG,
			(errmsg("PCM-X requester entered fail-closed at %s", fail_site),
			 errdetail("result=%d role=%u state=%u pending=%u last=%u request=%llu wait_seq=%llu",
					   (int)result, (unsigned int)handle.role, (unsigned int)progress.member_state,
					   (unsigned int)progress.pending_opcode,
					   (unsigned int)progress.last_response_opcode, (unsigned long long)request_id,
					   (unsigned long long)wait_seq)));
	if (gcs_block_pcm_x_requester_preflight_evidence.valid)
		ereport(
			LOG,
			(errmsg("PCM-X requester reservation-preflight evidence"),
			 errdetail(
				 "live_gen=%llu base_gen=%llu pcm_state=%u own_flags=%u rebase_active=%d",
				 (unsigned long long)gcs_block_pcm_x_requester_preflight_evidence.live_generation,
				 (unsigned long long)
					 gcs_block_pcm_x_requester_preflight_evidence.base_own_generation,
				 (unsigned int)gcs_block_pcm_x_requester_preflight_evidence.pcm_state,
				 (unsigned int)gcs_block_pcm_x_requester_preflight_evidence.own_flags,
				 gcs_block_pcm_x_requester_preflight_evidence.rebase_wire_active ? 1 : 0)));
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		result = PCM_X_QUEUE_CORRUPT;

requester_done:
	if (wait_published)
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	gcs_block_pcm_x_requester_cleanup_context.wait_published = false;
	return result;
}


PcmXQueueResult
cluster_gcs_pcm_x_acquire_writer(BufferDesc *buf, PcmXLocalWriterClaim *claim_out,
								 bool *claim_handed_off)
{
	GcsBlockPcmXRequesterCleanupContext *cleanup = &gcs_block_pcm_x_requester_cleanup_context;
	MemoryContext error_context = CurrentMemoryContext;
	ErrorData *original_error;
	PcmXQueueResult result = PCM_X_QUEUE_INVALID;
	bool fail_closed_required;

	if (claim_handed_off != NULL)
		*claim_handed_off = false;
	if (claim_handed_off == NULL)
		return PCM_X_QUEUE_INVALID;

	if (!gcs_block_pcm_x_requester_exit_hook_registered) {
		before_shmem_exit(gcs_block_pcm_x_requester_owner_exit, (Datum)0);
		gcs_block_pcm_x_requester_exit_hook_registered = true;
	}
	if (cleanup->active) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	memset(cleanup, 0, sizeof(*cleanup));
	cleanup->claim_handoff_out = claim_handed_off;
	cleanup->active = true;
	PG_TRY();
	{
		result = gcs_block_pcm_x_acquire_writer_impl(buf, claim_out);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(error_context);
		original_error = CopyErrorData();
		FlushErrorState();
		gcs_block_pcm_x_requester_cleanup_noexcept(cleanup);
		ReThrowError(original_error);
	}
	PG_END_TRY();

	fail_closed_required = cleanup->fail_closed_required;
	if (result == PCM_X_QUEUE_OK) {
		/* The caller pre-publishes claim_out in its owner-exit ledger.  Transfer
		 * cleanup authority before clearing our requester context, so every
		 * asynchronous exit observes exactly one owner of the live claim. */
		pg_write_barrier();
		*claim_handed_off = true;
		pg_write_barrier();
		memset(cleanup, 0, sizeof(*cleanup));
	} else
		gcs_block_pcm_x_requester_cleanup_noexcept(cleanup);
	if (fail_closed_required)
		cluster_pcm_x_runtime_fail_closed();
	return result;
}


static bool
gcs_block_pcm_x_revalidate_peer_binding(int32 node_id, uint64 epoch, uint64 session)
{
	PcmXQueueResult result;

	result = cluster_pcm_x_runtime_peer_binding_revalidate_exact(node_id, epoch, session);
	cluster_pcm_x_stats_note_queue_result(result);
	return result == PCM_X_QUEUE_OK;
}


/* A self-originating PCM-X frame has no HELLO record: its authority is the
 * already-bound local formation entry.  Remote frames must retain the exact
 * current-connection capability proof. */
static bool
gcs_block_pcm_x_source_capable(int32 node_id)
{
	return node_id == cluster_node_id || cluster_sf_peer_supports_pcm_x_convert(node_id);
}


/*
 * Capture the complete MEMBER formation together with one epoch and one live
 * incarnation per protocol peer.  Membership has no public snapshot lock for
 * this consumer, so bracket the authority reads with full before/after arrays,
 * epoch and quorum.  A transition inside one collection is an inconsistent
 * sample; the formation tick ignores it and retries on its next pass.  A stable
 * epoch/session value that differs from the already-bound runtime is positive
 * drift evidence and fails closed there.  A MEMBER outside the core protocol's
 * 32-node wire bitmap is likewise not representable.
 */
static bool
gcs_block_pcm_x_collect_formation(PcmXPeerBinding bindings[PCM_X_PROTOCOL_NODE_LIMIT],
								  uint64 *epoch_out, uint64 *self_session_out, bool *rebase_all_out)
{
	ClusterMembershipState membership_after[CLUSTER_MAX_NODES];
	ClusterMembershipState membership_before[CLUSTER_MAX_NODES];
	uint32 cap_generation_before[CLUSTER_MAX_NODES];
	bool cap_rebase_before[CLUSTER_MAX_NODES];
	uint64 epoch_after;
	uint64 epoch_before;
	uint64 peer_session;
	int i;

	if (rebase_all_out != NULL)
		*rebase_all_out = true;
	if (bindings == NULL || epoch_out == NULL || self_session_out == NULL || cluster_node_id < 0
		|| cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return false;
	memset(bindings, 0, sizeof(PcmXPeerBinding) * PCM_X_PROTOCOL_NODE_LIMIT);
	memset(cap_generation_before, 0, sizeof(cap_generation_before));
	memset(cap_rebase_before, 0, sizeof(cap_rebase_before));
	*epoch_out = 0;
	*self_session_out = 0;
	epoch_before = cluster_epoch_get_current();
	if (!cluster_qvotec_in_quorum())
		return false;
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		membership_before[i] = cluster_membership_get_state(i);
	if (membership_before[cluster_node_id] != CLUSTER_MEMBER_MEMBER)
		return false;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		PcmXSessionAuthResult auth_result;

		if (membership_before[i] != CLUSTER_MEMBER_MEMBER)
			continue;
		if (i >= PCM_X_PROTOCOL_NODE_LIMIT)
			return false;
		/* review P0-2: the CONVERT requirement, the REBASE coverage bit and
		 * the capability-record generation are one lock-coherent sample, so
		 * the after-pass below can prove the peer connection that advertised
		 * them is the SAME one the session binding names.  Rebase coverage
		 * never refuses the base protocol: a member without the V2 bit only
		 * pins the whole formation to V1 frames. */
		if (i != cluster_node_id) {
			if (!cluster_sf_peer_pcm_x_capability_sample(i, &cap_rebase_before[i],
														 &cap_generation_before[i]))
				return false;
			if (rebase_all_out != NULL && !cap_rebase_before[i])
				*rebase_all_out = false;
		}
		auth_result
			= gcs_block_pcm_x_authenticated_session_result(i, epoch_before, &peer_session, NULL);
		if (auth_result != PCM_X_SESSION_AUTH_OK)
			return false;
		bindings[i].cluster_epoch = epoch_before;
		bindings[i].peer_session_incarnation = peer_session;
	}

	pg_read_barrier();
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		membership_after[i] = cluster_membership_get_state(i);
	epoch_after = cluster_epoch_get_current();
	if (epoch_before != epoch_after || !cluster_qvotec_in_quorum()
		|| memcmp(membership_before, membership_after, sizeof(membership_before)) != 0)
		return false;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		bool cap_rebase_after;
		uint32 cap_generation_after;

		if (membership_before[i] != CLUSTER_MEMBER_MEMBER || i == cluster_node_id)
			continue;
		/* review P0-2 after-pass: any reconnect inside this collection moved
		 * the capability-record generation; refuse the tick rather than bind
		 * a stale connection's capability word to the fresh session. */
		if (!cluster_sf_peer_pcm_x_capability_sample(i, &cap_rebase_after, &cap_generation_after)
			|| cap_rebase_after != cap_rebase_before[i]
			|| cap_generation_after != cap_generation_before[i])
			return false;
	}

	*epoch_out = epoch_before;
	*self_session_out = bindings[cluster_node_id].peer_session_incarnation;
	return *self_session_out != 0;
}


#define PCM_X_MASTER_DRIVE_SCAN_BUDGET 1024

static void gcs_block_pcm_x_master_drive_retry_tick(void);
static void gcs_block_pcm_x_terminal_retry_tick(void);
static PcmXQueueResult
gcs_block_pcm_x_cancel_claimed_probe_exact(const PcmXMasterPendingXReleaseToken *token);


/*
 * Steady-state core formation gate.  Only pristine startup may activate; an
 * ACTIVE runtime whose complete peer binding can no longer be re-proved is
 * permanently closed until the deferred crash-recovery protocol intervenes.
 */
void
cluster_gcs_block_pcm_x_formation_tick(void)
{
	PcmXPeerBinding bindings_after[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXPeerBinding bindings_before[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXRuntimeSnapshot runtime;
	PcmXRuntimeSnapshot runtime_after;
	PcmXQueueResult result;
	uint64 epoch_after;
	uint64 epoch_before;
	uint64 self_session_after;
	uint64 self_session_before;
	bool rebase_all = false;
	int i;

	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state == PCM_X_RUNTIME_ACTIVE) {
		/*
		 * Do not compare a half-old/half-new membership view with the bound
		 * runtime.  Both complete samples must first agree; any transient read,
		 * including an unrelated membership flip, is a no-op for this tick.
		 */
		if (!gcs_block_pcm_x_collect_formation(bindings_before, &epoch_before, &self_session_before,
											   NULL))
			return;
		if (!gcs_block_pcm_x_collect_formation(bindings_after, &epoch_after, &self_session_after,
											   NULL))
			return;
		if (!cluster_gcs_pcm_x_formation_samples_stable(true, bindings_before, true, bindings_after)
			|| epoch_before != epoch_after || self_session_before != self_session_after)
			return;
		for (i = 0; i < PCM_X_PROTOCOL_NODE_LIMIT; i++) {
			if (bindings_before[i].peer_session_incarnation == 0)
				continue;
			result = cluster_pcm_x_runtime_peer_binding_revalidate_exact(
				i, bindings_before[i].cluster_epoch, bindings_before[i].peer_session_incarnation);
			if (result == PCM_X_QUEUE_STALE || result == PCM_X_QUEUE_CORRUPT)
				goto fail_closed;
			if (result != PCM_X_QUEUE_OK)
				return;
		}
		runtime_after = cluster_pcm_x_runtime_snapshot();
		if (runtime_after.state != PCM_X_RUNTIME_ACTIVE
			|| runtime_after.gate_generation != runtime.gate_generation
			|| runtime_after.master_session_incarnation != runtime.master_session_incarnation)
			return;
		gcs_block_pcm_x_master_drive_retry_tick();
		gcs_block_pcm_x_terminal_retry_tick();
		return;
	}

	/* ACTIVATING and any post-activation BLOCKED state are non-pristine. */
	if (runtime.gate_generation != 0 || runtime.master_session_incarnation != 0)
		return;
	if (!gcs_block_pcm_x_collect_formation(bindings_before, &epoch_before, &self_session_before,
										   &rebase_all))
		return;
	/* Connection-bound capabilities are stable for the runtime's lifetime (a
	 * peer restart changes its session incarnation and permanently closes the
	 * steady-state core), so formation-wide V2 coverage is sampled once. */
	cluster_pcm_x_runtime_set_rebase_wire_active(rebase_all);
	(void)cluster_pcm_x_runtime_activate_bound(self_session_before, bindings_before);
	return;

fail_closed:
	(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE, PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


/*
 * PCM-X is an application protocol, including when resource master and
 * requester are the same node.  Always stage through the tag-sharded DATA
 * ring: remote frames are sent by the owning LMS worker, while self frames
 * are loopback-dispatched by that same worker.  This preserves the per-tag
 * single-consumer premise used by confirm compensation and blocker-set
 * publication; cluster_ic_send_envelope(dest=self) would only return DONE
 * without executing a handler.
 */
bool
cluster_gcs_pcm_x_stage_frame(uint8 msg_type, int32 dest_node_id, const void *payload,
							  uint16 payload_len)
{
	if (msg_type < PGRAC_IC_MSG_PCM_X_ENQUEUE || msg_type > PGRAC_IC_MSG_PCM_X_RETIRE_ACK
		|| dest_node_id < 0 || dest_node_id >= PCM_X_PROTOCOL_NODE_LIMIT || payload == NULL
		|| payload_len == 0)
		return false;

	return cluster_grd_outbound_enqueue_backend_msg(msg_type, (uint32)dest_node_id, payload,
													payload_len);
}


/* RETIRE deliberately carries a cluster-wide ticket watermark rather than a
 * BufferTag.  The LMON terminal driver must therefore supply the ticket's tag
 * out of band when handing the request to the DATA plane.  The receiving LMS
 * worker sends the tagless ACK directly on its own DATA connection. */
static bool
gcs_block_pcm_x_stage_retire_up_to(int32 dest_node_id, const PcmXRetirePayload *payload,
								   const BufferTag *tag)
{
	int worker;

	if (dest_node_id < 0 || dest_node_id >= PCM_X_PROTOCOL_NODE_LIMIT || payload == NULL
		|| tag == NULL || cluster_lms_workers <= 0)
		return false;
	worker = cluster_lms_shard_for_tag(tag, cluster_lms_workers);
	if (worker < 0 || worker >= cluster_lms_workers)
		return false;
	return cluster_lms_outbound_enqueue(worker, PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO,
										(uint32)dest_node_id, payload, sizeof(*payload));
}


static bool
gcs_block_pcm_x_send_retire_ack(int32 dest_node_id, const PcmXRetirePayload *payload)
{
	ClusterICSendResult send_result;

	if (dest_node_id < 0 || dest_node_id >= PCM_X_PROTOCOL_NODE_LIMIT || payload == NULL)
		return false;
	send_result = cluster_ic_send_envelope(PGRAC_IC_MSG_PCM_X_RETIRE_ACK, dest_node_id, payload,
										   sizeof(*payload));
	return send_result == CLUSTER_IC_SEND_DONE || send_result == CLUSTER_IC_SEND_WOULD_BLOCK;
}


/*
 * ADMIT_CONFIRM and BLOCKER_SET mutate an engine snapshot and the LMD graph
 * as one logical per-tag action.  Their compensation arms are safe only while
 * every frame for the tag is serialized on worker[shard(tag)].  Keep this
 * runtime guard beside the handlers as well as the staging-router tests: an
 * assertion catches developer builds, and the explicit branch protects
 * production builds from deleting a newer worker's graph generation.
 */
static bool
gcs_block_pcm_x_handler_tag_shard_exact(const BufferTag *tag)
{
	int receive_worker;
	int tag_shard;

	if (tag == NULL || cluster_lms_workers <= 0)
		return false;
	receive_worker = cluster_ic_tier1_my_data_channel();
	tag_shard = cluster_lms_shard_for_tag(tag, cluster_lms_workers);
	Assert(tag_shard == receive_worker);
	if (tag_shard != receive_worker) {
		ereport(LOG,
				(errmsg_internal("PCM-X graph handler misrouted to LMS worker %d (tag shard %d); "
								 "dropping to preserve per-tag serialization",
								 receive_worker, tag_shard)));
		return false;
	}
	return true;
}


static void
gcs_block_pcm_x_master_drive_fail_closed(PcmXQueueResult result)
{
	if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED
		|| result == PCM_X_QUEUE_BAD_STATE || result == PCM_X_QUEUE_INVALID
		|| result == PCM_X_QUEUE_NO_CAPACITY)
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
}


/* Process-local BAD_STATE damping for the drive dispatch (one table per
 * driving process;  LMON's periodic retry tick is the guaranteed observer
 * that escalates a persisting per-ticket anomaly to the fail-closed verdict). */
static GcsBlockPcmXDriveAnomaly
	gcs_block_pcm_x_drive_anomaly_table[GCS_BLOCK_PCM_X_DRIVE_ANOMALY_SLOTS];


static PcmXQueueResult
gcs_block_pcm_x_master_authority(const PcmXMasterDriveSnapshot *snapshot,
								 PcmAuthoritySnapshot *authority_out, uint32 *holders_out,
								 int32 *source_out)
{
	if (snapshot == NULL || authority_out == NULL || holders_out == NULL || source_out == NULL)
		return PCM_X_QUEUE_INVALID;
	if (!cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
												snapshot->ref.identity.node_id,
												snapshot->ref.handle.ticket_id))
		return PCM_X_QUEUE_BAD_STATE;
	if (!cluster_pcm_lock_authority_snapshot(snapshot->ref.identity.tag, authority_out))
		return PCM_X_QUEUE_NOT_READY;
	return cluster_gcs_pcm_x_authority_holders_exact(authority_out, snapshot->ref.identity.node_id,
													 snapshot->ref.handle.ticket_id, holders_out,
													 source_out);
}


/* Bind the node-only GRD starvation barrier to one immutable queue ticket.
 * The GRD transition is idle-only, while the master-ticket flag supplies the
 * durable replay proof that node identity alone cannot provide. */
static PcmXQueueResult
gcs_block_pcm_x_ensure_pending_x_claim(const PcmXMasterDriveSnapshot *snapshot)
{
	PcmAuthoritySnapshot authority;
	PcmXQueueResult result;
	PcmPendingXReserveResult reserve_result;
	bool claimed;

	if (snapshot == NULL || snapshot->ticket_state != PCM_XT_ACTIVE_PROBE
		|| snapshot->ref.grant_generation != 0)
		return PCM_X_QUEUE_INVALID;
	result = cluster_pcm_x_master_pending_x_claim_state_exact(&snapshot->ref, &claimed);
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (claimed) {
		if (!cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
													snapshot->ref.identity.node_id,
													snapshot->ref.handle.ticket_id)) {
			/* Mirror the reserve-path recheck below:  CANCEL clears the GRD
			 * cookie before it finalizes the ticket, so a missing cookie can
			 * be an in-progress cancel rather than corruption.  Re-read the
			 * ticket under its own domain lock;  durable cancel/terminal
			 * progress is retryable, and only a ticket that still claims with
			 * no cookie stays anomalous for the caller's damping streak. */
			claimed = false;
			result = cluster_pcm_x_master_pending_x_claim_state_exact(&snapshot->ref, &claimed);
			if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_STALE
				|| result == PCM_X_QUEUE_NOT_FOUND || result == PCM_X_QUEUE_RETIRED)
				return PCM_X_QUEUE_NOT_READY;
			if (result != PCM_X_QUEUE_OK)
				return result;
			return claimed ? PCM_X_QUEUE_BAD_STATE : PCM_X_QUEUE_NOT_READY;
		}
		return PCM_X_QUEUE_OK;
	}

	reserve_result = cluster_pcm_lock_try_reserve_pending_x(
		snapshot->ref.identity.tag, snapshot->ref.identity.node_id, snapshot->ref.handle.ticket_id);
	if (reserve_result != PCM_PENDING_X_RESERVE_OK) {
		if (reserve_result == PCM_PENDING_X_RESERVE_NO_CAPACITY)
			return PCM_X_QUEUE_NO_CAPACITY;
		if (reserve_result == PCM_PENDING_X_RESERVE_INVALID)
			return PCM_X_QUEUE_INVALID;
		if (!cluster_pcm_lock_authority_snapshot(snapshot->ref.identity.tag, &authority)
			|| authority.pending_x_requester_node == -1)
			return PCM_X_QUEUE_NOT_READY;
		return PCM_X_QUEUE_BUSY;
	}

	result = cluster_pcm_x_master_pending_x_claim_exact(&snapshot->ref);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
		/* Only RESERVE_OK reaches this point, so this driver owns the
		 * half-published cookie.  Deterministic non-mutating claim failures
		 * must compensate it exactly; corruption keeps the evidence behind the
		 * already-closed runtime. */
		if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED
			|| result == PCM_X_QUEUE_NO_CAPACITY)
			return result;
		if (!cluster_pcm_lock_clear_queue_pending_x_exact(snapshot->ref.identity.tag,
														  snapshot->ref.identity.node_id,
														  snapshot->ref.handle.ticket_id)
			&& cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
													  snapshot->ref.identity.node_id,
													  snapshot->ref.handle.ticket_id)) {
			cluster_pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		return result == PCM_X_QUEUE_STALE || result == PCM_X_QUEUE_BAD_STATE
				   ? PCM_X_QUEUE_NOT_READY
				   : result;
	}
	/* A same-node legacy clear can race the cross-substrate publication window.
	 * Never drive a probe unless both halves of the proof are still present. */
	if (!cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
												snapshot->ref.identity.node_id,
												snapshot->ref.handle.ticket_id)) {
		/* CANCEL may win after the engine claim and clear the GRD cookie before
		 * this LMON continuation revalidates it.  Re-read the ticket under its
		 * own domain lock: durable cancel/terminal progress is retryable, while
		 * an ACTIVE_PROBE+CLAIMED ticket with no cookie remains corruption. */
		claimed = false;
		result = cluster_pcm_x_master_pending_x_claim_state_exact(&snapshot->ref, &claimed);
		if (result == PCM_X_QUEUE_NOT_READY || result == PCM_X_QUEUE_STALE
			|| result == PCM_X_QUEUE_NOT_FOUND || result == PCM_X_QUEUE_RETIRED)
			return PCM_X_QUEUE_NOT_READY;
		if (result != PCM_X_QUEUE_OK)
			return result;
		return claimed ? PCM_X_QUEUE_BAD_STATE : PCM_X_QUEUE_NOT_READY;
	}
	return PCM_X_QUEUE_OK;
}

/* Once the queue-kind cookie is visible, every older same-tag legacy reader
 * must lose its grant/forward right before PROBE/REVOKE can advance.  Drain
 * NEW victims one by one (no bounded receiver array), then replay one exact
 * unacknowledged denial per drive tick until requester DONE removes it. */
static PcmXQueueResult
gcs_block_pcm_x_deny_legacy_readers(const PcmXMasterDriveSnapshot *snapshot)
{
	GcsBlockDedupEntry denied;
	GcsBlockPendingXDenyResult deny_result;
	int worker_id;

	if (snapshot == NULL
		|| !cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
												snapshot->ref.identity.node_id,
												snapshot->ref.handle.ticket_id))
		return PCM_X_QUEUE_BAD_STATE;
	worker_id = cluster_lms_shard_for_tag(&snapshot->ref.identity.tag, cluster_lms_workers);
	if (worker_id < 0 || worker_id >= cluster_lms_workers)
		return PCM_X_QUEUE_INVALID;

	for (;;) {
		memset(&denied, 0, sizeof(denied));
		deny_result = cluster_gcs_block_dedup_pending_x_deny_next(
			worker_id, &snapshot->ref.identity.tag, &denied);
		if (deny_result == GCS_BLOCK_PENDING_X_DENY_NOT_FOUND)
			return PCM_X_QUEUE_OK;
		if (deny_result != GCS_BLOCK_PENDING_X_DENY_NEW
			&& deny_result != GCS_BLOCK_PENDING_X_DENY_REPLAY)
			return PCM_X_QUEUE_CORRUPT;
		if (denied.key.origin_node_id >= PCM_X_PROTOCOL_NODE_LIMIT
			|| !gcs_block_resend_cached_reply((int32)denied.key.origin_node_id, &denied))
			return PCM_X_QUEUE_BUSY;
		if (deny_result == GCS_BLOCK_PENDING_X_DENY_REPLAY)
			return PCM_X_QUEUE_OK;
	}
}


static PcmXQueueResult
gcs_block_pcm_x_stage_queue_invalidations(const PcmXMasterDriveSnapshot *snapshot,
										  uint32 invalidate_bitmap)
{
	GcsBlockInvalidatePayload inv;
	uint64 holder_session;
	int node;

	if (snapshot == NULL || snapshot->ref.grant_generation == 0)
		return PCM_X_QUEUE_INVALID;
	memset(&inv, 0, sizeof(inv));
	inv.request_id = snapshot->ref.identity.request_id;
	inv.epoch = snapshot->ref.identity.cluster_epoch;
	inv.tag = snapshot->ref.identity.tag;
	inv.master_node = cluster_node_id;
	inv.invalidating_for_x_node = (uint8)snapshot->ref.identity.node_id;
	inv.checksum = gcs_block_compute_invalidate_checksum(&inv);

	for (node = 0; node < PCM_X_PROTOCOL_NODE_LIMIT; node++) {
		if ((invalidate_bitmap & (UINT32_C(1) << node)) == 0)
			continue;
		if (!cluster_membership_is_member(node) || !gcs_block_pcm_x_source_capable(node)
			|| !gcs_block_pcm_x_authenticated_session(node, inv.epoch, &holder_session)
			|| !gcs_block_pcm_x_revalidate_peer_binding(node, inv.epoch, holder_session))
			return PCM_X_QUEUE_NOT_READY;
		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE,
													  (uint32)node, &inv, sizeof(inv))) {
			if (ClusterGcsBlock != NULL)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->invalidate_send_not_admitted_count, 1);
			return PCM_X_QUEUE_BUSY;
		}
		if (ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_broadcast_count, 1);
	}
	return PCM_X_QUEUE_OK;
}


static PcmXQueueResult
gcs_block_pcm_x_master_drive_transfer(const PcmXMasterDriveSnapshot *snapshot)
{
	PcmXRevokePayload revoke;
	PcmXQueueResult result;
	uint64 source_session = 0;
	uint32 source_bit;
	uint32 unacked;
	int32 source;

	if (snapshot == NULL || snapshot->ticket_state != PCM_XT_ACTIVE_TRANSFER)
		return PCM_X_QUEUE_INVALID;
	if (!cluster_gcs_pcm_x_transfer_pre_handoff_phase(snapshot->pending_opcode))
		return PCM_X_QUEUE_NOT_READY;
	if (!cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
												snapshot->ref.identity.node_id,
												snapshot->ref.handle.ticket_id))
		return PCM_X_QUEUE_BAD_STATE;
	if (snapshot->pending_opcode == PGRAC_IC_MSG_PCM_X_REVOKE)
		source = snapshot->expected_responder_node;
	else {
		uint32 source_bitmap = snapshot->acked_s_holders_bitmap;

		if (source_bitmap == 0 || (source_bitmap & (source_bitmap - 1)) != 0)
			return PCM_X_QUEUE_NOT_READY;
		result = cluster_gcs_pcm_x_next_unacked_holder(source_bitmap, 0, &source);
		if (result != PCM_X_QUEUE_OK)
			return result;
	}
	if (source < 0 || source >= PCM_X_PROTOCOL_NODE_LIMIT)
		return PCM_X_QUEUE_CORRUPT;
	source_bit = UINT32_C(1) << source;
	if ((snapshot->pending_s_holders_bitmap & source_bit) == 0
		|| (snapshot->acked_s_holders_bitmap & source_bit) == 0)
		return PCM_X_QUEUE_CORRUPT;
	if (!gcs_block_pcm_x_authenticated_session(source, snapshot->ref.identity.cluster_epoch,
											   &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source, snapshot->ref.identity.cluster_epoch,
													source_session))
		return PCM_X_QUEUE_NOT_READY;
	result = cluster_pcm_x_master_revoke_arm_exact(&snapshot->ref, source, source_session, &revoke);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	unacked = snapshot->pending_s_holders_bitmap & ~snapshot->acked_s_holders_bitmap;
	if (unacked != 0)
		return gcs_block_pcm_x_stage_queue_invalidations(snapshot, unacked);
	return cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_REVOKE, source, &revoke, sizeof(revoke))
			   ? PCM_X_QUEUE_OK
			   : PCM_X_QUEUE_BUSY;
}


static PcmXQueueResult
gcs_block_pcm_x_master_begin_transfer(PcmXMasterDriveSnapshot *snapshot, uint32 holders,
									  int32 source)
{
	PcmXMasterDriveSnapshot transfer;
	PcmXMasterDriveSnapshot replaced;
	PcmXTicketRef transfer_ref;
	PcmXQueueResult begin_result;
	PcmXQueueResult result;
	uint32 source_bit;

	if (snapshot == NULL || source < 0 || source >= PCM_X_PROTOCOL_NODE_LIMIT
		|| (holders & (UINT32_C(1) << source)) == 0)
		return PCM_X_QUEUE_INVALID;
	if (!cluster_pcm_lock_queue_pending_x_exact(snapshot->ref.identity.tag,
												snapshot->ref.identity.node_id,
												snapshot->ref.handle.ticket_id))
		return PCM_X_QUEUE_BAD_STATE;
	begin_result = cluster_pcm_x_master_begin_transfer_exact(
		&snapshot->ref, snapshot->graph_generation, &transfer_ref);
	cluster_pcm_x_stats_note_queue_result(begin_result);
	if (begin_result != PCM_X_QUEUE_OK && begin_result != PCM_X_QUEUE_DUPLICATE)
		return begin_result;
	result = cluster_pcm_x_master_drive_snapshot_exact(
		&snapshot->ref.identity.tag, snapshot->ref.identity.cluster_epoch, &transfer);
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (transfer.ticket_state != PCM_XT_ACTIVE_TRANSFER
		|| transfer.ref.handle.ticket_id != transfer_ref.handle.ticket_id
		|| transfer.ref.handle.queue_generation != transfer_ref.handle.queue_generation)
		return PCM_X_QUEUE_STALE;
	if (begin_result == PCM_X_QUEUE_DUPLICATE)
		return gcs_block_pcm_x_master_drive_transfer(&transfer);
	source_bit = UINT32_C(1) << source;
	result = cluster_pcm_x_master_drive_bitmap_replace_exact(&transfer, holders, source_bit,
															 &replaced);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	*snapshot = replaced;
	return gcs_block_pcm_x_master_drive_transfer(snapshot);
}


static PcmXQueueResult
gcs_block_pcm_x_master_drive_probe(PcmXMasterDriveSnapshot *snapshot)
{
	PcmAuthoritySnapshot authority;
	PcmXMasterDriveSnapshot replaced;
	PcmXQueueResult result;
	uint32 acknowledged;
	uint32 holders;
	uint32 responder_bit;
	int32 responder;
	int32 source;

	if (snapshot == NULL || snapshot->ticket_state != PCM_XT_ACTIVE_PROBE)
		return PCM_X_QUEUE_INVALID;
	if (snapshot->pending_s_holders_bitmap == 0) {
		result = gcs_block_pcm_x_master_authority(snapshot, &authority, &holders, &source);
		if (result != PCM_X_QUEUE_OK)
			return result;
		result = cluster_pcm_x_master_drive_bitmap_replace_exact(snapshot, holders, 0, &replaced);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
			return result;
		*snapshot = replaced;
	}
	if (snapshot->pending_opcode == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK) {
		cluster_gcs_pcm_x_blocker_probe_kick(&snapshot->ref, snapshot->expected_responder_node);
		return PCM_X_QUEUE_OK;
	}
	if (snapshot->pending_opcode != 0)
		return PCM_X_QUEUE_BAD_STATE;
	if (snapshot->last_response_opcode == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT
		&& snapshot->blocker_set_generation != 0) {
		responder = (int32)snapshot->last_responder_node;
		if (responder < 0 || responder >= PCM_X_PROTOCOL_NODE_LIMIT)
			return PCM_X_QUEUE_CORRUPT;
		responder_bit = UINT32_C(1) << responder;
		if ((snapshot->pending_s_holders_bitmap & responder_bit) != 0
			&& (snapshot->acked_s_holders_bitmap & responder_bit) == 0) {
			if (snapshot->blocker_count != 0) {
				cluster_gcs_pcm_x_blocker_probe_kick(&snapshot->ref, responder);
				return PCM_X_QUEUE_NOT_READY;
			}
			acknowledged = snapshot->acked_s_holders_bitmap | responder_bit;
			result = cluster_pcm_x_master_drive_bitmap_replace_exact(
				snapshot, snapshot->pending_s_holders_bitmap, acknowledged, &replaced);
			if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
				return result;
			*snapshot = replaced;
		}
	}
	result = cluster_gcs_pcm_x_next_unacked_holder(snapshot->pending_s_holders_bitmap,
												   snapshot->acked_s_holders_bitmap, &responder);
	if (result == PCM_X_QUEUE_OK) {
		cluster_gcs_pcm_x_blocker_probe_kick(&snapshot->ref, responder);
		return PCM_X_QUEUE_OK;
	}
	if (result != PCM_X_QUEUE_NOT_FOUND)
		return result;
	result = gcs_block_pcm_x_master_authority(snapshot, &authority, &holders, &source);
	if (result != PCM_X_QUEUE_OK)
		return result;
	if (holders != snapshot->pending_s_holders_bitmap) {
		acknowledged = snapshot->acked_s_holders_bitmap & holders;
		result = cluster_pcm_x_master_drive_bitmap_replace_exact(snapshot, holders, acknowledged,
																 &replaced);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
			return result;
		*snapshot = replaced;
		if (acknowledged != holders) {
			result = cluster_gcs_pcm_x_next_unacked_holder(holders, acknowledged, &responder);
			if (result != PCM_X_QUEUE_OK)
				return result;
			cluster_gcs_pcm_x_blocker_probe_kick(&snapshot->ref, responder);
			return PCM_X_QUEUE_OK;
		}
	}
	return gcs_block_pcm_x_master_begin_transfer(snapshot, holders, source);
}


static PcmXQueueResult
gcs_block_pcm_x_master_drive_cancel_requested(const PcmXMasterDriveSnapshot *snapshot)
{
	PcmXMasterPendingXReleaseToken release;
	PcmXPhasePayload ack;
	PcmXQueueResult result;

	if (snapshot == NULL || snapshot->ticket_state != PCM_XT_ACTIVE_PROBE
		|| snapshot->flags
			   != (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
				   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED))
		return PCM_X_QUEUE_INVALID;
	/* Preserve the holder reliable FIFO: retry the one already-armed PROBE and
	 * let its normal BLOCKER_SET ACK path consume the leg. */
	if (snapshot->pending_opcode == PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK) {
		cluster_gcs_pcm_x_blocker_probe_kick(&snapshot->ref, snapshot->expected_responder_node);
		return PCM_X_QUEUE_OK;
	}
	if (snapshot->pending_opcode != 0)
		return PCM_X_QUEUE_CORRUPT;

	result = cluster_pcm_x_master_pending_x_cancel_prepare_exact(&snapshot->ref, &release);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	if (release.master_session_incarnation == 0)
		return PCM_X_QUEUE_CORRUPT;
	result = gcs_block_pcm_x_cancel_claimed_probe_exact(&release);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;

	memset(&ack, 0, sizeof(ack));
	ack.ref = release.ref;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	return cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_CANCEL_ACK,
										 release.ref.identity.node_id, &ack, sizeof(ack))
			   ? PCM_X_QUEUE_OK
			   : PCM_X_QUEUE_BUSY;
}


/* Log-once evidence for the periodic master drive: a non-progress result
 * that repeats consecutively with the same shape is a stalled drive, and a
 * frozen wedge must name its refusing arm.  Progress resets the streak. */
static void
gcs_block_pcm_x_master_drive_note(const char *stage, PcmXQueueResult result, uint64 ticket_id)
{
	static const char *last_stage = NULL;
	static uint64 last_ticket = 0;
	static int last_result = 0;
	static bool logged = false;

	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		last_stage = NULL;
		logged = false;
		return;
	}
	if (stage == last_stage && (int)result == last_result && ticket_id == last_ticket) {
		if (!logged) {
			logged = true;
			ereport(LOG, (errmsg("PCM-X master drive is repeating %s (result %d, ticket %llu)",
								 stage, (int)result, (unsigned long long)ticket_id)));
		}
		return;
	}
	last_stage = stage;
	last_result = (int)result;
	last_ticket = ticket_id;
	logged = false;
}


static void
gcs_block_pcm_x_master_drive_tag(const BufferTag *tag, uint64 cluster_epoch)
{
	PcmXMasterDriveSnapshot snapshot;
	PcmXTicketRef active;
	PcmXQueueResult result;
	const char *drive_stage;

	if (tag == NULL || cluster_epoch != cluster_epoch_get_current() || cluster_node_id < 0
		|| cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| cluster_gcs_lookup_master(*tag) != cluster_node_id
		|| cluster_gcs_block_phase_for_tag(*tag) == GCS_BLOCK_RECOVERING
		|| !cluster_qvotec_in_quorum() || !cluster_membership_is_member(cluster_node_id))
		return;
	result = cluster_pcm_x_master_promote_head_exact(tag, cluster_epoch, &active);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_BUSY) {
		/* No promotable head means any earlier per-ticket anomaly for this
		 * tag has resolved (cancelled / retired);  settle its streaks. */
		if (result == PCM_X_QUEUE_NOT_FOUND)
			cluster_gcs_pcm_x_drive_anomaly_settle(gcs_block_pcm_x_drive_anomaly_table,
												   GCS_BLOCK_PCM_X_DRIVE_ANOMALY_SLOTS, tag);
		gcs_block_pcm_x_master_drive_note("promote", result, 0);
		gcs_block_pcm_x_master_drive_fail_closed(result);
		return;
	}
	result = cluster_pcm_x_master_drive_snapshot_exact(tag, cluster_epoch, &snapshot);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK) {
		gcs_block_pcm_x_master_drive_note("snapshot", result, 0);
		gcs_block_pcm_x_master_drive_fail_closed(result);
		return;
	}
	if (snapshot.ticket_state == PCM_XT_ACTIVE_PROBE
		&& snapshot.flags
			   == (PCM_X_MASTER_TICKET_F_PENDING_X_CLAIMED
				   | PCM_X_MASTER_TICKET_F_PENDING_X_CANCEL_REQUESTED)) {
		drive_stage = "cancel-requested";
		result = gcs_block_pcm_x_master_drive_cancel_requested(&snapshot);
	} else if (snapshot.ticket_state == PCM_XT_ACTIVE_PROBE) {
		drive_stage = "probe";
		result = gcs_block_pcm_x_ensure_pending_x_claim(&snapshot);
		if (result == PCM_X_QUEUE_OK)
			result = gcs_block_pcm_x_deny_legacy_readers(&snapshot);
		if (result == PCM_X_QUEUE_OK)
			result = gcs_block_pcm_x_master_drive_probe(&snapshot);
	} else if (snapshot.ticket_state == PCM_XT_ACTIVE_TRANSFER) {
		drive_stage = "transfer";
		result = gcs_block_pcm_x_deny_legacy_readers(&snapshot);
		if (result == PCM_X_QUEUE_OK)
			result = gcs_block_pcm_x_master_drive_transfer(&snapshot);
	} else {
		drive_stage = "state";
		result = PCM_X_QUEUE_CORRUPT;
	}
	cluster_pcm_x_stats_note_queue_result(result);
	gcs_block_pcm_x_master_drive_note(drive_stage, result, snapshot.ref.handle.ticket_id);
	/* Only definite drive progress settles the tag;  indeterminate results
	 * (NOT_READY / BUSY / STALE) must not reset a live streak, or a real
	 * wedge interleaved with transients would never fuse. */
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		cluster_gcs_pcm_x_drive_anomaly_settle(gcs_block_pcm_x_drive_anomaly_table,
											   GCS_BLOCK_PCM_X_DRIVE_ANOMALY_SLOTS, tag);
	/* A lone dispatch BAD_STATE can be another actor's two-phase window (an
	 * in-progress claimed cancel, an identity-keyed serve-path clear).  Damp
	 * it per ticket and let the periodic re-drive re-observe;  only a streak
	 * that survives consecutive ticks reaches the runtime fuse. */
	if (result == PCM_X_QUEUE_BAD_STATE
		&& !cluster_gcs_pcm_x_drive_anomaly_note(gcs_block_pcm_x_drive_anomaly_table,
												 GCS_BLOCK_PCM_X_DRIVE_ANOMALY_SLOTS, tag,
												 snapshot.ref.handle.ticket_id))
		return;
	gcs_block_pcm_x_master_drive_fail_closed(result);
}


/*
 * First convert-queue DATA slice: authenticate and publish an ENQUEUE at the
 * tag master, then return the exact admission handle.  Transport completion
 * is not application completion: a dropped/blocked ACK leaves the live master
 * ticket available for byte-exact replay of the same prehandle.
 */
static void
cluster_gcs_handle_pcm_x_enqueue_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXEnqueuePayload *request;
	PcmXMasterAdmission admission;
	PcmXAdmitAckPayload ack;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session = 0;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXEnqueuePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	request = (const PcmXEnqueuePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(request->identity.tag);
	if (!cluster_gcs_pcm_x_enqueue_ingress_valid(request, env->payload_length, source_node,
												 current_epoch, tag_master, cluster_node_id))
		return;
	if (!gcs_block_pcm_x_source_capable(source_node))
		return;
	if (!cluster_qvotec_in_quorum() || !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node))
		return;
	if (cluster_gcs_block_phase_for_tag(request->identity.tag) == GCS_BLOCK_RECOVERING)
		return;
	if (!gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| request->prehandle.sender_session_incarnation != source_session)
		return;
	if (!gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;

	cluster_pcm_x_stats_note_enqueue();
	result = cluster_pcm_x_master_admit_begin(request, &admission);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;

	memset(&ack, 0, sizeof(ack));
	ack.ref = admission.ref;
	ack.prehandle = admission.prehandle;
	ack.result = (uint32)result;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ENQUEUE;
	ack.flags = (uint16)admission.flags;
	(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_ADMIT_ACK, source_node, &ack,
										sizeof(ack));
}


/*
 * Apply the exact ADMIT_ACK to the requester-node leader and wake its backend.
 * WFG/blocker publication is the next layer: this handler deliberately does
 * not arm or send ADMIT_CONFIRM.
 */
static void
cluster_gcs_handle_pcm_x_admit_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXAdmitAckPayload *ack;
	PcmXLocalHandle leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXAdmitAckPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	ack = (const PcmXAdmitAckPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	if (ack->ref.identity.node_id != cluster_node_id
		|| ack->ref.identity.cluster_epoch != current_epoch
		|| cluster_gcs_lookup_master(ack->ref.identity.tag) != source_node
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(ack->ref.identity.tag) == GCS_BLOCK_RECOVERING)
		return;
	if (!gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&ack->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_apply_admit_ack_exact(&leader, ack, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		PcmXPhasePayload confirm;
		PcmXLocalReliableToken token;
		PcmXQueueResult arm_result;

		arm_result = cluster_pcm_x_local_admit_confirm_arm_exact(&leader, &confirm, &token);
		cluster_pcm_x_stats_note_queue_result(arm_result);
		if (arm_result == PCM_X_QUEUE_OK || arm_result == PCM_X_QUEUE_DUPLICATE)
			(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM, source_node,
												&confirm, sizeof(confirm));
	}
	if ((result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) && ProcGlobal != NULL
		&& ack->ref.identity.procno < (uint32)ProcGlobal->allProcCount)
		SetLatch(&ProcGlobal->allProcs[ack->ref.identity.procno].procLatch);
}


/* Publish the exact queued-leader blocker set into the existing WFG before
 * making ADMITTING visible as QUEUED.  The queue snapshot holds no lock on
 * return; graph replace therefore obeys the C2 lock-order boundary. */
static void
cluster_gcs_handle_pcm_x_admit_confirm_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *request;
	PcmXMasterWfgSnapshot snapshot;
	PcmXPhasePayload ack;
	ClusterLmdVertex waiter;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 graph_generation;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	request = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(request->ref.identity.tag);
	if (!cluster_gcs_pcm_x_admit_confirm_ingress_valid(request, env->payload_length, source_node,
													   current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(request->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	if (!gcs_block_pcm_x_handler_tag_shard_exact(&request->ref.identity.tag))
		return;

	memset(&snapshot, 0, sizeof(snapshot));
	result = cluster_pcm_x_master_admit_wfg_snapshot_exact(&request->ref, &snapshot);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_DUPLICATE)
		goto send_ack;
	if (result != PCM_X_QUEUE_OK)
		return;

	cluster_gcs_pcm_x_vertex_from_identity(&request->ref.identity, &waiter);
	graph_generation = cluster_lmd_graph_replace_waiter_edges_exact(
		&waiter, snapshot.blockers, (int)snapshot.blocker_count, waiter.request_id);
	if (graph_generation == 0) {
		cluster_lmd_pcm_convert_wfg_note_replace_fail();
		return;
	}
	cluster_lmd_pcm_convert_wfg_note_replace();
	result = cluster_pcm_x_master_admit_confirm_revalidate_exact(&snapshot.token, graph_generation);
	cluster_pcm_x_stats_note_queue_result(result);
	if (cluster_gcs_pcm_x_confirm_compensation_required(graph_generation, result)) {
		if (cluster_lmd_graph_remove_edge_by_waiter_exact(&waiter))
			cluster_lmd_pcm_convert_wfg_note_remove();
		return;
	}
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;

send_ack:
	memset(&ack, 0, sizeof(ack));
	ack.ref = request->ref;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;
	if (cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK, source_node, &ack,
									  sizeof(ack)))
		gcs_block_pcm_x_master_drive_tag(&request->ref.identity.tag,
										 request->ref.identity.cluster_epoch);
}


static void
cluster_gcs_handle_pcm_x_admit_confirm_ack_envelope(const ClusterICEnvelope *env,
													const void *payload)
{
	const PcmXPhasePayload *ack;
	PcmXLocalHandle leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	ack = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(ack->ref.identity.tag);
	if (!cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(
			ack, env->payload_length, source_node, current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(ack->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&ack->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_admit_confirm_ack_exact(&leader, ack, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if ((result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) && ProcGlobal != NULL
		&& ack->ref.identity.procno < (uint32)ProcGlobal->allProcCount)
		SetLatch(&ProcGlobal->allProcs[ack->ref.identity.procno].procLatch);
}


/*
 * BLOCKER_SET BEGIN and EDGE intentionally have no application ACK.  The
 * holder retains one immutable outbound snapshot and retries the whole
 * BEGIN/EDGE/COMMIT sequence until the final COMMIT ACK arrives.  This keeps
 * the 96-byte phase ACK exact: it identifies one complete set generation,
 * rather than ambiguously acknowledging a truncated per-chunk cursor.
 */
static void
cluster_gcs_handle_pcm_x_blocker_set_begin_envelope(const ClusterICEnvelope *env,
													const void *payload)
{
	const PcmXBlockerSetHeaderPayload *begin;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXBlockerSetHeaderPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	begin = (const PcmXBlockerSetHeaderPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(begin->ref.identity.tag);
	if (!cluster_gcs_pcm_x_blocker_header_ingress_valid(begin, env->payload_length, source_node,
														current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(begin->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	if (!gcs_block_pcm_x_handler_tag_shard_exact(&begin->ref.identity.tag))
		return;

	result = cluster_pcm_x_master_blocker_stage_begin_exact(begin, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
}


static PcmXQueueResult
cluster_gcs_pcm_x_remove_cancelled_waiter(const PcmXWaitIdentity *identity)
{
	ClusterLmdGraphRemoveResult graph_result;
	ClusterLmdVertex waiter;

	cluster_gcs_pcm_x_vertex_from_identity(identity, &waiter);
	graph_result = cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter);
	if (graph_result == CLUSTER_LMD_GRAPH_REMOVE_REMOVED) {
		cluster_lmd_pcm_convert_wfg_note_remove();
		return PCM_X_QUEUE_OK;
	}
	if (graph_result == CLUSTER_LMD_GRAPH_REMOVE_ABSENT)
		return PCM_X_QUEUE_OK;
	if (graph_result == CLUSTER_LMD_GRAPH_REMOVE_STALE) {
		cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_STALE;
	}
	cluster_pcm_x_runtime_fail_closed();
	return PCM_X_QUEUE_CORRUPT;
}


/* Finish an already-prepared claimed cancellation without ever nesting the
 * master queue lock with WFG or GRD locks.  WFG goes first: STALE proves that
 * the same 4-tuple names a newer wait_seq and therefore must retain the GRD
 * barrier rather than expose that successor to an unsafe grant. */
static PcmXQueueResult
gcs_block_pcm_x_cancel_claimed_probe_exact(const PcmXMasterPendingXReleaseToken *token)
{
	ClusterLmdGraphRemoveResult graph_result;
	ClusterLmdVertex waiter;
	PcmXQueueResult result;

	if (token == NULL || token->master_session_incarnation == 0)
		return PCM_X_QUEUE_INVALID;
	cluster_gcs_pcm_x_vertex_from_identity(&token->ref.identity, &waiter);
	graph_result = cluster_lmd_graph_remove_edge_by_waiter_exact_result(&waiter);
	if (graph_result == CLUSTER_LMD_GRAPH_REMOVE_REMOVED)
		cluster_lmd_pcm_convert_wfg_note_remove();
	else if (graph_result == CLUSTER_LMD_GRAPH_REMOVE_STALE) {
		cluster_lmd_pcm_convert_wfg_note_exact_remove_stale();
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_STALE;
	}
	if (!cluster_pcm_lock_clear_queue_pending_x_exact(
			token->ref.identity.tag, token->ref.identity.node_id, token->ref.handle.ticket_id)
		&& cluster_pcm_lock_queue_pending_x_exact(
			token->ref.identity.tag, token->ref.identity.node_id, token->ref.handle.ticket_id)) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	result = cluster_pcm_x_master_pending_x_cancel_finalize_exact(token);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		return result;
	/* WFG and GRD have already crossed their irreversible exact barriers.
	 * Retaining RELEASE_PENDING without a recovery gate would expose a queue
	 * that can neither resume nor safely grant its successor. */
	cluster_pcm_x_runtime_fail_closed();
	return result;
}


/* Reconstruct cancellation cleanup entirely from shared ticket state.  This
 * is intentionally callable by LMON after the original LMS/backend exits:
 * exact WFG removal is idempotent, the claimed path also clears only the
 * ticket-bound GRD cookie, and finalize consumes RELEASE_PENDING last. */
static PcmXQueueResult
gcs_block_pcm_x_cancel_terminal_cleanup_exact(const PcmXTicketRef *ref)
{
	PcmXMasterPendingXReleaseToken release;
	PcmXQueueResult result;

	memset(&release, 0, sizeof(release));
	result = cluster_pcm_x_master_pending_x_cancel_prepare_exact(ref, &release);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return result;
	if (release.master_session_incarnation != 0)
		return gcs_block_pcm_x_cancel_claimed_probe_exact(&release);
	return cluster_gcs_pcm_x_remove_cancelled_waiter(&ref->identity);
}


static void
cluster_gcs_handle_pcm_x_blocker_set_edge_envelope(const ClusterICEnvelope *env,
												   const void *payload)
{
	const PcmXBlockerChunkPayload *edge;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;
	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXBlockerChunkPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	edge = (const PcmXBlockerChunkPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(edge->tag);
	if (!cluster_gcs_pcm_x_blocker_edge_ingress_valid(edge, env->payload_length, source_node,
													  current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(edge->tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	if (!gcs_block_pcm_x_handler_tag_shard_exact(&edge->tag))
		return;

	result = cluster_pcm_x_master_blocker_stage_edge_exact(edge, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
}


/* Publish one complete holder blocker set into both the exact ticket snapshot
 * and the existing LMD graph before returning its final application ACK. */
static void
cluster_gcs_handle_pcm_x_blocker_set_commit_envelope(const ClusterICEnvelope *env,
													 const void *payload)
{
	const PcmXBlockerSetHeaderPayload *commit;
	PcmXMasterBlockerEntry *entries = NULL;
	PcmXMasterBlockerSnapshot snapshot;
	ClusterLmdVertex *blockers = NULL;
	ClusterLmdVertex waiter;
	PcmXPhasePayload ack;
	PcmXQueueResult result;
	PcmXQueueResult stage_result;
	uint64 current_epoch;
	uint64 graph_generation;
	uint64 source_session;
	uint32 i;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXBlockerSetHeaderPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	commit = (const PcmXBlockerSetHeaderPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(commit->ref.identity.tag);
	if (!cluster_gcs_pcm_x_blocker_header_ingress_valid(commit, env->payload_length, source_node,
														current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(commit->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	if (!gcs_block_pcm_x_handler_tag_shard_exact(&commit->ref.identity.tag))
		return;

	/* Ask the engine to prove the dynamic max_wait_edges bound and exact live
	 * stage before allocating from an untrusted wire count. */
	if (commit->nblockers != 0) {
		result = cluster_pcm_x_master_blocker_stage_commit_exact(
			commit, source_node, source_session, NULL, 0, &snapshot);
		if (result != PCM_X_QUEUE_NO_CAPACITY) {
			cluster_pcm_x_stats_note_queue_result(result);
			goto blocker_commit_done;
		}
		entries = palloc0(mul_size(commit->nblockers, sizeof(*entries)));
		blockers = palloc0(mul_size(commit->nblockers, sizeof(*blockers)));
	}
	result = cluster_pcm_x_master_blocker_stage_commit_exact(commit, source_node, source_session,
															 entries, commit->nblockers, &snapshot);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		goto blocker_commit_done;
	stage_result = result;
	if (result == PCM_X_QUEUE_DUPLICATE && snapshot.graph_generation != 0)
		goto complete_blocker_probe;
	result = cluster_pcm_x_master_blocker_snapshot_revalidate_exact(&snapshot, entries,
																	commit->nblockers);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK) {
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		goto blocker_commit_done;
	}
	for (i = 0; i < commit->nblockers; i++)
		blockers[i] = entries[i].blocker;

	cluster_gcs_pcm_x_vertex_from_identity(&commit->ref.identity, &waiter);
	graph_generation = cluster_lmd_graph_replace_waiter_edges_exact(
		&waiter, blockers, (int)commit->nblockers, waiter.request_id);
	if (graph_generation == 0) {
		cluster_lmd_pcm_convert_wfg_note_replace_fail();
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		goto blocker_commit_done;
	}
	cluster_lmd_pcm_convert_wfg_note_replace();
	result = cluster_pcm_x_master_blocker_graph_commit_exact(&snapshot, entries, commit->nblockers,
															 graph_generation);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
		/* The ticket set has already been published.  Removing by waiter here
		 * could delete a concurrently newer graph generation; retain evidence
		 * and close the runtime for recovery instead. */
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		goto blocker_commit_done;
	}

complete_blocker_probe:
	/* Admit the generation-exact ACK into the durable per-peer outbound FIFO
	 * before consuming the master probe leg.  A refused enqueue retains that
	 * leg, so the next bounded PROBE retry can replay the same immutable bytes.
	 * Type 49 is staged later on the same FIFO and therefore cannot overtake
	 * this ACK. */
	if (!cluster_gcs_pcm_x_blocker_ack_build(&commit->ref, commit->set_generation, &ack)) {
		Assert(false);
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		goto blocker_commit_done;
	}
	if (!cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK, source_node, &ack,
									   sizeof(ack))) {
		cluster_pcm_x_stats_note_queue_result(PCM_X_QUEUE_BUSY);
		goto blocker_commit_done;
	}
	result = cluster_pcm_x_master_blocker_probe_complete_exact(&commit->ref, commit->set_generation,
															   source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (cluster_pcm_x_blocker_commit_completion_requires_recovery(stage_result, result)) {
		/* The first application may have put its ACK in flight without consuming
		 * the probe leg, so an unexpected completion remains fail-closed.  A
		 * DUPLICATE stage is different: the same set was already published and
		 * the ACK above was merely replayed after a legal phase advance.  The
		 * classifier keeps BAD_STATE/STALE/NOT_READY/BUSY benign in that replay
		 * case while structural CORRUPT still halts the runtime. */
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		goto blocker_commit_done;
	}
	gcs_block_pcm_x_master_drive_tag(&commit->ref.identity.tag, commit->ref.identity.cluster_epoch);

blocker_commit_done:
	if (blockers != NULL)
		pfree(blockers);
	if (entries != NULL)
		pfree(entries);
}


static void
cluster_gcs_pcm_x_wake_cancel_rotation(const PcmXWaitIdentity *cancelled,
									   const PcmXLocalHandle *new_leader)
{
	if (ProcGlobal == NULL)
		return;
	if (cancelled != NULL && cancelled->procno < (uint32)ProcGlobal->allProcCount)
		SetLatch(&ProcGlobal->allProcs[cancelled->procno].procLatch);
	if (new_leader != NULL && new_leader->identity.request_id != 0
		&& new_leader->identity.procno < (uint32)ProcGlobal->allProcCount)
		SetLatch(&ProcGlobal->allProcs[new_leader->identity.procno].procLatch);
}


static void
cluster_gcs_handle_pcm_x_prehandle_cancel_envelope(const ClusterICEnvelope *env,
												   const void *payload)
{
	const PcmXPrehandleCancelPayload *request;
	PcmXMasterAdmission cancelled;
	PcmXAdmitAckPayload ack;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPrehandleCancelPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	request = (const PcmXPrehandleCancelPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(request->identity.tag);
	if (!cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(
			request, env->payload_length, source_node, current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(request->identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| request->prehandle.sender_session_incarnation != source_session
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;

	memset(&cancelled, 0, sizeof(cancelled));
	result = cluster_pcm_x_master_prehandle_cancel_exact(request, &cancelled);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;
	result = cluster_gcs_pcm_x_remove_cancelled_waiter(&cancelled.ref.identity);
	if (result != PCM_X_QUEUE_OK)
		return;
	memset(&ack, 0, sizeof(ack));
	ack.ref = cancelled.ref;
	ack.prehandle = cancelled.prehandle;
	/* Canonical application outcome is byte-identical for first apply/replay. */
	ack.result = PCM_X_QUEUE_OK;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL;
	(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK, source_node, &ack,
										sizeof(ack));
}


static void
cluster_gcs_handle_pcm_x_prehandle_cancel_ack_envelope(const ClusterICEnvelope *env,
													   const void *payload)
{
	const PcmXAdmitAckPayload *ack;
	PcmXLocalHandle leader;
	PcmXLocalHandle new_leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXAdmitAckPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	ack = (const PcmXAdmitAckPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(ack->ref.identity.tag);
	if (!cluster_gcs_pcm_x_prehandle_cancel_ack_ingress_valid(
			ack, env->payload_length, source_node, current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(ack->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&ack->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_prehandle_cancel_ack_exact(&leader, ack, source_node,
															source_session, &new_leader);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		cluster_gcs_pcm_x_wake_cancel_rotation(&ack->ref.identity, &new_leader);
}


static void
cluster_gcs_handle_pcm_x_cancel_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *request;
	PcmXPhasePayload ack;
	PcmXMasterPendingXReleaseToken release;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	request = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(request->ref.identity.tag);
	if (!cluster_gcs_pcm_x_cancel_ingress_valid(request, env->payload_length, source_node,
												current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(request->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	/* One master-lock classification closes the ordinary CANCEL-vs-transfer
	 * race.  A claimed in-flight PROBE records CANCEL_REQUESTED and returns
	 * NOT_READY; its normal application ACK path will resume this release. */
	result = cluster_pcm_x_master_pending_x_cancel_prepare_exact(&request->ref, &release);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;
	if (release.master_session_incarnation != 0) {
		result = gcs_block_pcm_x_cancel_claimed_probe_exact(&release);
		cluster_pcm_x_stats_note_queue_result(result);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
			return;
	} else {
		result = cluster_gcs_pcm_x_remove_cancelled_waiter(&request->ref.identity);
		if (result != PCM_X_QUEUE_OK)
			return;
	}
	memset(&ack, 0, sizeof(ack));
	ack.ref = request->ref;
	ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_CANCEL_ACK, source_node, &ack,
										sizeof(ack));
}


static int
gcs_block_pcm_x_blocker_compare(const void *left_ptr, const void *right_ptr)
{
	const ClusterLmdVertex *left = (const ClusterLmdVertex *)left_ptr;
	const ClusterLmdVertex *right = (const ClusterLmdVertex *)right_ptr;

	if (left->node_id != right->node_id)
		return left->node_id < right->node_id ? -1 : 1;
	if (left->procno != right->procno)
		return left->procno < right->procno ? -1 : 1;
	if (left->cluster_epoch != right->cluster_epoch)
		return left->cluster_epoch < right->cluster_epoch ? -1 : 1;
	if (left->request_id != right->request_id)
		return left->request_id < right->request_id ? -1 : 1;
	return 0;
}


/*
 * Resolve a type-48 PROBE into one exact holder/blocker snapshot and replay
 * its complete 45-47 sequence.  Passive holders remain barrier occupancy but
 * contribute no WFG edge.  A torn PGPROC seqlock is explicitly BUSY and is
 * never normalized to an empty set.  The snapshot persists until the nonzero
 * generation-exact type-48 ACK consumes it, so duplicate probes replay the
 * same application bytes after a partial/refused send.
 */
static PcmXQueueResult
gcs_block_pcm_x_probe_collect_and_stage(const PcmXPhasePayload *probe, int32 master_node,
										uint64 master_session)
{
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXLocalBlockerSnapshot blocker_snapshot;
	PcmXBlockerSetHeaderPayload header_payload;
	PcmXLocalHolderHandle *holders = NULL;
	PcmXBlockerChunkPayload *edges = NULL;
	ClusterLmdVertex *blockers = NULL;
	PcmXQueueResult result;
	Size blocker_count = 0;
	Size i;

	/* Replay is a lookup, never a re-sample.  Once a 45-47 snapshot exists its
	 * token and bytes are immutable until the generation-exact ACK consumes
	 * it; this path therefore remains available even when the blocker pool is
	 * otherwise full or holder wait-state changes after the first arm. */
	result = cluster_pcm_x_local_blocker_snapshot_lookup_exact(&probe->ref, master_node,
															   master_session, &blocker_snapshot);
	if (result == PCM_X_QUEUE_OK)
		goto probe_copy;
	if (result != PCM_X_QUEUE_NOT_FOUND)
		goto probe_done;

	/* Freeze the local FIFO round and holder registry before consulting any
	 * PGPROC wait state.  The generation-zero reservation is durable across
	 * this count-first allocation retry, so a late writer cannot enter the
	 * sampled round and a duplicate PROBE cannot reopen the barrier. */
	result = cluster_pcm_x_local_probe_freeze_snapshot_exact(
		&probe->ref, master_node, master_session, NULL, 0, &holder_snapshot);
	if (result == PCM_X_QUEUE_NO_CAPACITY) {
		holders = palloc0(mul_size(holder_snapshot.holder_count, sizeof(*holders)));
		result = cluster_pcm_x_local_probe_freeze_snapshot_exact(
			&probe->ref, master_node, master_session, holders, holder_snapshot.holder_count,
			&holder_snapshot);
	}
	if (result == PCM_X_QUEUE_DUPLICATE) {
		result = cluster_pcm_x_local_blocker_snapshot_lookup_exact(
			&probe->ref, master_node, master_session, &blocker_snapshot);
		if (result == PCM_X_QUEUE_OK)
			goto probe_copy;
	}
	if (result != PCM_X_QUEUE_OK)
		goto probe_done;
	if (holder_snapshot.holder_count != 0) {
		if (holders == NULL) {
			result = PCM_X_QUEUE_CORRUPT;
			goto probe_done;
		}
		if (ProcGlobal == NULL) {
			result = PCM_X_QUEUE_NOT_READY;
			goto probe_done;
		}
		blockers = palloc0(mul_size(holder_snapshot.holder_count, sizeof(*blockers)));
		for (i = 0; i < holder_snapshot.holder_count; i++) {
			const PcmXWaitIdentity *holder = &holders[i].key.identity;
			ClusterLmdWaitStateSnapshot wait_snapshot;
			ClusterLmdWaitStateReadResult read_result;
			ClusterLmdVertex *blocker;

			if (holder->node_id != cluster_node_id
				|| holder->cluster_epoch != probe->ref.identity.cluster_epoch
				|| holder->procno >= (uint32)ProcGlobal->allProcCount) {
				result = PCM_X_QUEUE_STALE;
				goto probe_done;
			}
			read_result = cluster_lmd_wait_state_read_exact(
				&ProcGlobal->allProcs[holder->procno].cluster_lmd_wait, &wait_snapshot);
			if (read_result == CLUSTER_LMD_WAIT_STATE_READ_BUSY) {
				result = PCM_X_QUEUE_BUSY;
				goto probe_done;
			}
			if (read_result == CLUSTER_LMD_WAIT_STATE_READ_INACTIVE)
				continue;
			if (read_result != CLUSTER_LMD_WAIT_STATE_READ_ACTIVE
				|| (wait_snapshot.kind != CLUSTER_LMD_WAIT_GES
					&& wait_snapshot.kind != CLUSTER_LMD_WAIT_TX
					&& wait_snapshot.kind != CLUSTER_LMD_WAIT_PCM_CONVERT)
				|| wait_snapshot.wait_seq == 0
				|| wait_snapshot.cluster_epoch != probe->ref.identity.cluster_epoch
				|| (wait_snapshot.kind == CLUSTER_LMD_WAIT_TX
						? (wait_snapshot.request_id != 0
						   || !TransactionIdIsValid(wait_snapshot.xid))
						: wait_snapshot.request_id == 0)) {
				result = PCM_X_QUEUE_STALE;
				goto probe_done;
			}
			blocker = &blockers[blocker_count++];
			blocker->node_id = cluster_node_id;
			blocker->procno = holder->procno;
			blocker->cluster_epoch = wait_snapshot.cluster_epoch;
			blocker->request_id = wait_snapshot.request_id;
			blocker->xid = wait_snapshot.xid;
			blocker->local_start_ts_ms = 0;
			blocker->wait_seq = wait_snapshot.wait_seq;
		}
		if (blocker_count > 1)
			qsort(blockers, blocker_count, sizeof(*blockers), gcs_block_pcm_x_blocker_compare);
	}

	result = cluster_pcm_x_local_blocker_snapshot_arm_exact(
		&probe->ref, master_node, master_session, &holder_snapshot, holders,
		holder_snapshot.holder_count, blockers, blocker_count, &blocker_snapshot);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		goto probe_done;

probe_copy:
	if (blocker_snapshot.blocker_count != 0)
		edges = palloc0(mul_size(blocker_snapshot.blocker_count, sizeof(*edges)));
	result = cluster_pcm_x_local_blocker_snapshot_copy_exact(&blocker_snapshot, &header_payload,
															 edges, blocker_snapshot.blocker_count);
	if (result != PCM_X_QUEUE_OK)
		goto probe_done;
	/* The ACK producer may never collapse into the reserved PROBE value. */
	Assert(header_payload.set_generation != 0 && header_payload.set_generation != UINT64_MAX);
	if (!cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN, master_node,
									   &header_payload, sizeof(header_payload))) {
		result = PCM_X_QUEUE_BUSY;
		goto probe_done;
	}
	for (i = 0; i < blocker_snapshot.blocker_count; i++) {
		if (!cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE, master_node,
										   &edges[i], sizeof(edges[i]))) {
			result = PCM_X_QUEUE_BUSY;
			goto probe_done;
		}
	}
	if (!cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, master_node,
									   &header_payload, sizeof(header_payload))) {
		result = PCM_X_QUEUE_BUSY;
		goto probe_done;
	}
	result = PCM_X_QUEUE_OK;

probe_done:
	if (edges != NULL)
		pfree(edges);
	if (blockers != NULL)
		pfree(blockers);
	if (holders != NULL)
		pfree(holders);
	return result;
}


/* Arm and stage one exact zero-generation PROBE.  The tag-sharded master
 * driver may call this once per bounded retry tick; DUPLICATE replays the
 * identical durable type-48 leg. */
void
cluster_gcs_pcm_x_blocker_probe_kick(const PcmXTicketRef *ref, int32 holder_node)
{
	PcmXMasterProbeToken token;
	PcmXPhasePayload probe;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 holder_session;

	current_epoch = cluster_epoch_get_current();
	if (!cluster_gcs_pcm_x_ticket_ref_wire_valid(ref, current_epoch) || holder_node < 0
		|| holder_node >= PCM_X_PROTOCOL_NODE_LIMIT || cluster_node_id < 0
		|| cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| cluster_gcs_lookup_master(ref->identity.tag) != cluster_node_id
		|| cluster_gcs_block_phase_for_tag(ref->identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_source_capable(holder_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(holder_node)
		|| !gcs_block_pcm_x_authenticated_session(holder_node, current_epoch, &holder_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(holder_node, current_epoch, holder_session))
		return;
	result = cluster_pcm_x_master_blocker_probe_arm_exact(ref, holder_node, holder_session, &probe,
														  &token);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;
	Assert(cluster_gcs_pcm_x_blocker_ack_generation(&probe) == 0);
	(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK, holder_node, &probe,
										sizeof(probe));
}


static void
cluster_gcs_handle_pcm_x_blocker_set_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *control;
	PcmXQueueResult result;
	uint64 set_generation;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	control = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(control->ref.identity.tag);
	if (!cluster_gcs_pcm_x_blocker_control_ingress_valid(control, env->payload_length, source_node,
														 current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(control->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	set_generation = cluster_gcs_pcm_x_blocker_ack_generation(control);
	if (set_generation == 0) {
		if (!cluster_gcs_pcm_x_blocker_probe_ingress_valid(control, env->payload_length,
														   source_node, current_epoch, tag_master,
														   cluster_node_id))
			return;
		result = gcs_block_pcm_x_probe_collect_and_stage(control, source_node, source_session);
	} else {
		if (!cluster_gcs_pcm_x_blocker_ack_ingress_valid(control, env->payload_length, source_node,
														 current_epoch, tag_master,
														 cluster_node_id))
			return;
		/* Every 45-47 commit ACK is generation-exact and therefore nonzero. */
		Assert(set_generation != UINT64_MAX);
		result = cluster_pcm_x_local_blocker_ack_exact(&control->ref, set_generation, source_node,
													   source_session);
	}
	cluster_pcm_x_stats_note_queue_result(result);
}


static bool
gcs_block_pcm_x_transfer_ingress_authorized(const BufferTag *tag, int32 source_node,
											uint64 current_epoch, uint64 *source_session_out)
{
	uint64 source_session;

	if (source_session_out != NULL)
		*source_session_out = 0;
	if (tag == NULL || source_node < 0 || source_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(*tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return false;
	if (source_session_out != NULL)
		*source_session_out = source_session;
	return true;
}


static void
gcs_block_pcm_x_wake_requester(const PcmXWaitIdentity *identity)
{
	if (identity != NULL && identity->node_id == cluster_node_id && ProcGlobal != NULL
		&& identity->procno < (uint32)ProcGlobal->allProcCount)
		SetLatch(&ProcGlobal->allProcs[identity->procno].procLatch);
}


/* RETIRE may promote a different local writer than the backend that drove the
 * terminal exchange.  Validate the complete seqlock-protected wait identity
 * before using its procno; a missed hint is recovered by the requester's
 * bounded progress recheck, whereas waking a recycled wait generation is not
 * accepted as exact protocol behavior. */
static bool
gcs_block_pcm_x_wake_requester_exact(const PcmXWaitIdentity *identity, uint32 all_proc_count)
{
	ClusterLmdWaitStateSnapshot snapshot;
	ClusterLmdWaitStateReadResult read_result;

	if (identity == NULL || ProcGlobal == NULL || ProcGlobal->allProcCount <= 0
		|| (uint32)ProcGlobal->allProcCount != all_proc_count
		|| identity->node_id != cluster_node_id || identity->procno >= all_proc_count
		|| identity->request_id == 0 || identity->wait_seq == 0)
		return false;
	read_result = cluster_lmd_wait_state_read_exact(
		&ProcGlobal->allProcs[identity->procno].cluster_lmd_wait, &snapshot);
	if (!cluster_gcs_pcm_x_wait_identity_matches(identity, cluster_node_id, all_proc_count,
												 read_result, &snapshot))
		return false;
	SetLatch(&ProcGlobal->allProcs[identity->procno].procLatch);
	return true;
}


/* Completing a local writer is the condition transition for its direct FIFO
 * successor.  The engine copies that successor under the same local-tag lock
 * that records WRITER_COMPLETE; this adapter validates the full published wait
 * generation before setting the latch.  Polling remains only a lost-wake
 * fallback, and a missed hint never changes the committed release result. */
PcmXQueueResult
cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(const PcmXLocalWriterClaim *claim)
{
	PcmXWaitIdentity successor;
	PcmXQueueResult result;
	uint32 all_proc_count;

	memset(&successor, 0, sizeof(successor));
	result = cluster_pcm_x_local_writer_claim_release_collect_exact(claim, &successor);
	if (result != PCM_X_QUEUE_OK || successor.request_id == 0 || ProcGlobal == NULL
		|| ProcGlobal->allProcCount <= 0)
		return result;
	all_proc_count = (uint32)ProcGlobal->allProcCount;
	(void)gcs_block_pcm_x_wake_requester_exact(&successor, all_proc_count);
	return result;
}

/* Cleanup runs while an ERROR stack is already active or after backend owner
 * death.  Contain any ERROR from the exact engine/wake adapter: the caller
 * retains its ledger entry unless OK is returned, and RECOVERY_BLOCKED makes
 * the preserved claim explicit recovery evidence instead of an orphan. */
PcmXQueueResult
cluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(const PcmXLocalWriterClaim *claim)
{
	volatile PcmXQueueResult result = PCM_X_QUEUE_CORRUPT;

	PG_TRY();
	{
		result = cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(claim);
	}
	PG_CATCH();
	{
		FlushErrorState();
		cluster_pcm_x_runtime_fail_closed();
		result = PCM_X_QUEUE_CORRUPT;
	}
	PG_END_TRY();
	return (PcmXQueueResult)result;
}


/* Allocate before the engine call so no ERROR-capable allocation follows a
 * committed watermark.  The engine publishes batch.count only after every
 * terminal detach, the retired frontier, and the global admission gate have
 * committed.  Exact wake attempts are best-effort latency hints and never
 * suppress the RETIRE ACK on BUSY, INACTIVE, or identity mismatch. */
static PcmXQueueResult
gcs_block_pcm_x_local_retire_apply_and_wake(const PcmXRetirePayload *request,
											int32 authenticated_master_node,
											uint64 authenticated_master_session)
{
	PcmXWaitIdentity *wake_items;
	PcmXLocalWakeBatch wake_batch;
	PcmXQueueResult result;
	Size i;
	uint32 all_proc_count;

	if (request == NULL)
		return PCM_X_QUEUE_INVALID;
	if (ProcGlobal == NULL || ProcGlobal->allProcCount <= 0)
		return PCM_X_QUEUE_NOT_READY;
	all_proc_count = (uint32)ProcGlobal->allProcCount;
	wake_items = palloc0(mul_size((Size)all_proc_count, sizeof(*wake_items)));
	wake_batch.items = wake_items;
	wake_batch.capacity = (Size)all_proc_count;
	wake_batch.count = 0;
	result = cluster_pcm_x_local_retire_up_to_collect_exact(
		request, authenticated_master_node, authenticated_master_session, &wake_batch);
	if (result == PCM_X_QUEUE_OK) {
		for (i = 0; i < wake_batch.count && i < wake_batch.capacity; i++)
			(void)gcs_block_pcm_x_wake_requester_exact(&wake_batch.items[i], all_proc_count);
	}
	pfree(wake_items);
	return result;
}


/* A type-49 ref names the remote requester, never a local holder backend.
 * Locate holder processes only through the exact per-tag holder registry; a
 * cached page with no registered holder remains durably staged for the outer
 * data-plane worker and must not wake an unrelated same-numbered procno. */
static void
gcs_block_pcm_x_wake_registered_holders(const BufferTag *tag)
{
	PcmXLocalHolderSnapshot snapshot;
	PcmXLocalHolderHandle *holders = NULL;
	PcmXQueueResult result;
	Size i;

	if (tag == NULL || ProcGlobal == NULL)
		return;
	result = cluster_pcm_x_local_holder_snapshot(tag, NULL, 0, &snapshot);
	if (result != PCM_X_QUEUE_NO_CAPACITY || snapshot.holder_count == 0)
		return;
	holders = palloc0(mul_size(snapshot.holder_count, sizeof(*holders)));
	result = cluster_pcm_x_local_holder_snapshot(tag, holders, snapshot.holder_count, &snapshot);
	if (result == PCM_X_QUEUE_OK
		&& cluster_pcm_x_local_holder_snapshot_revalidate(tag, &snapshot) == PCM_X_QUEUE_OK) {
		for (i = 0; i < snapshot.holder_count; i++) {
			const PcmXWaitIdentity *identity = &holders[i].key.identity;

			if (identity->node_id == cluster_node_id
				&& identity->procno < (uint32)ProcGlobal->allProcCount)
				SetLatch(&ProcGlobal->allProcs[identity->procno].procLatch);
		}
	}
	pfree(holders);
}


static bool
gcs_block_pcm_x_ticket_ref_equal(const PcmXTicketRef *left, const PcmXTicketRef *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}


static bool
gcs_block_pcm_x_image_key(const PcmXTicketRef *ref, uint64 image_id, GcsBlockDedupKey *key_out)
{
	int32 requester_backend_id;
	int32 requester_node;

	if (key_out != NULL)
		memset(key_out, 0, sizeof(*key_out));
	if (ref == NULL || key_out == NULL || ref->identity.node_id < 0
		|| ref->identity.node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| !cluster_pcm_x_image_id_decode(image_id, NULL, NULL)
		|| !cluster_gcs_requester_id_decode(ref->identity.request_id, &requester_node,
											&requester_backend_id, NULL)
		|| requester_node != ref->identity.node_id || requester_backend_id <= 0)
		return false;
	key_out->origin_node_id = (uint32)requester_node;
	key_out->requester_backend_id = requester_backend_id;
	key_out->request_id = image_id;
	key_out->cluster_epoch = ref->identity.cluster_epoch;
	return true;
}


static void
gcs_block_pcm_x_reserved_binding(const PcmXRevokePayload *revoke, uint64 source_generation,
								 uint64 master_session, GcsBlockPcmXImageBinding *binding_out)
{
	memset(binding_out, 0, sizeof(*binding_out));
	binding_out->identity.ref = revoke->ref;
	binding_out->identity.image.image_id = revoke->image_id;
	binding_out->identity.image.source_own_generation = source_generation;
	binding_out->identity.image.source_node = (uint32)cluster_node_id;
	binding_out->master_session = master_session;
}


static void
gcs_block_pcm_x_note_own_result(ClusterPcmOwnResult result)
{
	if (result == CLUSTER_PCM_OWN_BUSY)
		cluster_pcm_x_stats_note_own_busy();
	else if (result == CLUSTER_PCM_OWN_CORRUPT)
		cluster_pcm_x_stats_note_own_corrupt();
}


static bool
gcs_block_pcm_x_release_image_exact(int worker_id, const GcsBlockPcmXImageWork *work,
									const GcsBlockPcmXImageBinding *binding)
{
	return cluster_gcs_block_dedup_pcm_x_release_exact(worker_id, &work->key, &work->tag, binding)
		   == GCS_BLOCK_PCM_X_IMAGE_RELEASED;
}


/* Before ownership commit, a failed byte leg must unstage its exact capacity
 * reservation before clearing REVOKING.  After commit this helper is never
 * legal: the immutable READY bytes are then the only recoverable copy owned by
 * this protocol attempt. */
static void
gcs_block_pcm_x_abort_image_before_finish(int worker_id, const GcsBlockPcmXImageWork *work,
										  const GcsBlockPcmXImageBinding *binding, BufferDesc *buf,
										  const ClusterPcmOwnSnapshot *revoking,
										  bool revoke_started)
{
	ClusterPcmOwnResult abort_result = CLUSTER_PCM_OWN_OK;
	bool released;

	released = gcs_block_pcm_x_release_image_exact(worker_id, work, binding);
	if (revoke_started) {
		if (revoking->pcm_state == (uint8)PCM_STATE_N)
			abort_result = cluster_bufmgr_pcm_own_abort_n_revoke(buf, revoking);
		else if (revoking->pcm_state == (uint8)PCM_STATE_S)
			abort_result = cluster_bufmgr_pcm_own_abort_s_revoke(buf, revoking);
		else if (revoking->pcm_state == (uint8)PCM_STATE_X)
			abort_result = cluster_bufmgr_pcm_own_abort_x_revoke(buf, revoking);
		else
			abort_result = CLUSTER_PCM_OWN_CORRUPT;
	}
	gcs_block_pcm_x_note_own_result(abort_result);
	if (!released || abort_result != CLUSTER_PCM_OWN_OK)
		cluster_pcm_x_runtime_fail_closed();
}


static void
gcs_block_pcm_x_stage_ready_work(int worker_id, const GcsBlockPcmXImageWork *work)
{
	PcmXGrantPayload image_ready;
	PcmXGrantPayload replay;
	PcmXQueueResult result;
	GcsBlockPcmXImageResult mark_result;
	GcsBlockPcmXImageResult rollback_result;
	int32 master_node;

	if (!cluster_pcm_x_image_id_decode(work->binding.identity.image.image_id, &master_node, NULL)) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	memset(&image_ready, 0, sizeof(image_ready));
	image_ready.ref = work->binding.identity.ref;
	image_ready.image = work->binding.identity.image;
	result = cluster_pcm_x_local_holder_image_ready_arm_exact(
		&image_ready, master_node, work->binding.master_session, &replay);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_BUSY || result == PCM_X_QUEUE_NOT_READY)
		return;
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	mark_result = cluster_gcs_block_dedup_pcm_x_mark_staged_exact(worker_id, &work->key, &work->tag,
																  &work->binding);
	if (mark_result == GCS_BLOCK_PCM_X_IMAGE_DUPLICATE)
		return;
	if (mark_result != GCS_BLOCK_PCM_X_IMAGE_STAGED) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	if (cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_IMAGE_READY, master_node, &replay,
									  sizeof(replay)))
		return;
	rollback_result = cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(worker_id, &work->key,
																		&work->tag, &work->binding);
	if (rollback_result != GCS_BLOCK_PCM_X_IMAGE_REARMED
		&& rollback_result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE)
		cluster_pcm_x_runtime_fail_closed();
}


static void gcs_block_pcm_x_revoke_refusal_note(const char *site, int own_result,
												const ClusterPcmOwnSnapshot *snap);

static void
gcs_block_pcm_x_materialize_reserved_work(int worker_id, const GcsBlockPcmXImageWork *work)
{
	char block_data[GCS_BLOCK_DATA_SIZE];
	ClusterPcmOwnSnapshot current;
	ClusterPcmOwnSnapshot retained;
	ClusterPcmOwnSnapshot revoking;
	GcsBlockPcmXImageBinding ready_binding;
	GcsBlockPcmXImageResult image_result;
	GcsBlockReplyHeader reply_header;
	PcmXLocalHolderSnapshot holder_snapshot;
	PcmXQueueResult holder_result;
	ClusterPcmOwnResult own_result;
	ClusterPcmXRevokeFinishMode finish_mode;
	PageHeaderData page_header;
	BufferDesc *buf;
	XLogRecPtr page_lsn;
	uint64 page_scn = 0;
	bool source_is_n;
	bool source_is_s;
	bool source_is_x;
	bool descriptor_retained;
	bool self_source_handoff;
	int buffer_id;

	holder_result = cluster_pcm_x_local_holder_snapshot(&work->tag, NULL, 0, &holder_snapshot);
	if (holder_result == PCM_X_QUEUE_NO_CAPACITY && holder_snapshot.holder_count > 0) {
		gcs_block_pcm_x_revoke_refusal_note("materialize-holder-capacity", (int)holder_result,
											NULL);
		return;
	}
	if (holder_result == PCM_X_QUEUE_NOT_READY || holder_result == PCM_X_QUEUE_BUSY) {
		gcs_block_pcm_x_revoke_refusal_note("materialize-holder-snapshot", (int)holder_result,
											NULL);
		return;
	}
	if (holder_result != PCM_X_QUEUE_OK || holder_snapshot.holder_count != 0) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}

	own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(&work->tag, &buffer_id, &current);
	gcs_block_pcm_x_note_own_result(own_result);
	if (own_result == CLUSTER_PCM_OWN_NOT_READY) {
		gcs_block_pcm_x_revoke_refusal_note("materialize-snapshot", (int)own_result, &current);
		return;
	}
	source_is_n = current.pcm_state == (uint8)PCM_STATE_N;
	source_is_s = current.pcm_state == (uint8)PCM_STATE_S;
	source_is_x = current.pcm_state == (uint8)PCM_STATE_X;
	self_source_handoff = (source_is_n || source_is_s || source_is_x)
						  && work->binding.identity.ref.identity.node_id == cluster_node_id;
	if (own_result != CLUSTER_PCM_OWN_OK || (!source_is_n && !source_is_s && !source_is_x)
		|| (source_is_n && !self_source_handoff) || current.flags != 0
		|| current.generation != work->binding.identity.image.source_own_generation) {
		(void)gcs_block_pcm_x_release_image_exact(worker_id, work, &work->binding);
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	buf = GetBufferDescriptor(buffer_id);
	if (source_is_n)
		own_result = cluster_bufmgr_pcm_own_prepare_n_source_image(
			buf, &current, &revoking, block_data, &page_lsn, &page_scn);
	else if (source_is_s)
		own_result = cluster_bufmgr_pcm_own_begin_s_revoke(buf, &current, &revoking);
	else
		own_result = cluster_bufmgr_pcm_own_begin_x_revoke(buf, &current, &revoking);
	gcs_block_pcm_x_note_own_result(own_result);
	if (own_result == CLUSTER_PCM_OWN_BUSY || own_result == CLUSTER_PCM_OWN_NOT_READY) {
		gcs_block_pcm_x_revoke_refusal_note("materialize-begin", (int)own_result, &current);
		return;
	}
	if (own_result != CLUSTER_PCM_OWN_OK) {
		(void)gcs_block_pcm_x_release_image_exact(worker_id, work, &work->binding);
		cluster_pcm_x_runtime_fail_closed();
		return;
	}

	if (!source_is_n && !cluster_bufmgr_copy_block_for_gcs(work->tag, &page_lsn, block_data)) {
		gcs_block_pcm_x_abort_image_before_finish(worker_id, work, &work->binding, buf, &revoking,
												  true);
		return;
	}
	memcpy(&page_header, block_data, sizeof(page_header));
	if ((uint64)page_lsn != (uint64)PageXLogRecPtrGet(page_header.pd_lsn)
		|| (source_is_n && page_scn != (uint64)page_header.pd_block_scn)) {
		gcs_block_pcm_x_abort_image_before_finish(worker_id, work, &work->binding, buf, &revoking,
												  true);
		return;
	}
	page_scn = (uint64)page_header.pd_block_scn;

	ready_binding = work->binding;
	ready_binding.identity.image.page_scn = page_scn;
	ready_binding.identity.image.page_lsn = (uint64)page_lsn;
	ready_binding.identity.image.page_checksum = cluster_gcs_block_compute_checksum(block_data);
	memset(&reply_header, 0, sizeof(reply_header));
	reply_header.request_id = work->key.request_id;
	reply_header.page_lsn = ready_binding.identity.image.page_lsn;
	reply_header.epoch = work->key.cluster_epoch;
	reply_header.checksum = ready_binding.identity.image.page_checksum;
	reply_header.sender_node = cluster_node_id;
	reply_header.requester_backend_id = work->key.requester_backend_id;
	reply_header.transition_id = (uint8)PCM_TRANS_N_TO_S;
	reply_header.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&reply_header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	image_result = cluster_gcs_block_dedup_pcm_x_materialize(
		worker_id, &work->key, &work->tag, &ready_binding, &reply_header, block_data);
	if (image_result != GCS_BLOCK_PCM_X_IMAGE_STORED
		&& image_result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE) {
		gcs_block_pcm_x_abort_image_before_finish(worker_id, work, &work->binding, buf, &revoking,
												  true);
		return;
	}

	if (self_source_handoff) {
		/* A requester acting as its own N/S/X source keeps the one exact
		 * source frozen under the same REVOKING token until PREPARE atomically
		 * reclassifies it as GRANT_PENDING.  Turning it into a retained remote
		 * source here would create a FINAL-before-DRAIN self-cycle. */
		own_result = cluster_bufmgr_pcm_own_snapshot(buf, &current);
		gcs_block_pcm_x_note_own_result(own_result);
		if (own_result != CLUSTER_PCM_OWN_OK || !BufferTagsEqual(&current.tag, &revoking.tag)
			|| current.generation != revoking.generation
			|| current.reservation_token != revoking.reservation_token
			|| current.flags != PCM_OWN_FLAG_REVOKING || current.pcm_state != revoking.pcm_state) {
			/* Immutable record materialization already succeeded.  Preserve both
			 * pieces of evidence and stop admission; a rollback could race a
			 * duplicate PREPARE using the same exact lifecycle. */
			cluster_pcm_x_runtime_fail_closed();
			return;
		}
	} else {
		own_result
			= cluster_bufmgr_pcm_own_finish_revoke_retain(buf, &revoking, page_lsn, &retained);
		gcs_block_pcm_x_note_own_result(own_result);
		if (own_result != CLUSTER_PCM_OWN_OK) {
			gcs_block_pcm_x_abort_image_before_finish(worker_id, work, &ready_binding, buf,
													  &revoking, true);
			return;
		}
		finish_mode = cluster_pcm_x_revoke_finish_mode(&revoking.tag, 0);
		descriptor_retained = finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_RETAIN;
		if (revoking.generation == UINT64_MAX || retained.generation != revoking.generation + 1
			|| retained.reservation_token != revoking.reservation_token
			|| (descriptor_retained ? retained.flags != PCM_OWN_FLAG_REVOKING : retained.flags != 0)
			|| retained.pcm_state != (uint8)PCM_STATE_N
			|| (finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_RETAIN
				&& finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_DROP)
			|| !BufferTagsEqual(&retained.tag, &revoking.tag)) {
			/* Finish already crossed the S/X->N byte boundary.  A malformed retained
			 * projection cannot be rolled back without inventing bytes or ownership. */
			cluster_pcm_x_runtime_fail_closed();
			return;
		}
	}
	image_result = cluster_gcs_block_dedup_pcm_x_publish_ready_exact(worker_id, &work->key,
																	 &work->tag, &ready_binding);
	if (image_result != GCS_BLOCK_PCM_X_IMAGE_STORED
		&& image_result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE) {
		/* The source revoke is already irreversible (remote S/X or self-frozen N/S/X):
		 * retain the immutable intermediate evidence and stop the protocol.  Only
		 * recovery may resolve this boundary. */
		cluster_pcm_x_runtime_fail_closed();
		return;
	}

	/* Remote-source ownership is now N at generation+1; requester-as-source
	 * instead remains exact N/S/X+REVOKING at its base generation for the
	 * fused PREPARE handoff.  No later failure may unstage the A-record; READY
	 * remains retryable until DRAIN. */
	{
		GcsBlockPcmXImageWork ready_work = *work;

		ready_work.binding = ready_binding;
		ready_work.entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE;
		gcs_block_pcm_x_stage_ready_work(worker_id, &ready_work);
	}
}


static void
gcs_block_pcm_x_owner_exit(int code, Datum arg pg_attribute_unused())
{
	/* A nonzero aux-process exit can interrupt any instruction between the
	 * holder barrier, byte staging, X->N, and outbound admission.  Core has no
	 * crash resolver, so never infer which side committed. */
	if (code != 0)
		cluster_pcm_x_runtime_fail_closed();
}


void
cluster_gcs_block_pcm_x_owner_start(int worker_id)
{
	if (worker_id < 0 || worker_id >= cluster_lms_workers) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	before_shmem_exit(gcs_block_pcm_x_owner_exit, Int32GetDatum(worker_id));
	if (cluster_gcs_block_dedup_pcm_x_restart_audit(worker_id))
		cluster_pcm_x_runtime_fail_closed();
}


/* One bounded unit of holder-byte work, owned by the same DATA worker that
 * dispatches every frame for this BufferTag. */
void
cluster_gcs_block_pcm_x_image_pump_tick(int worker_id)
{
	GcsBlockPcmXImageWork work;
	GcsBlockPcmXImageResult result;

	if (worker_id < 0 || worker_id >= cluster_lms_workers
		|| (cluster_write_fence_enforcing() && !cluster_write_fence_allowed()))
		return;
	result = cluster_gcs_block_dedup_pcm_x_next_work(worker_id, &work);
	if (result == GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND || result == GCS_BLOCK_PCM_X_IMAGE_FULL)
		return;
	if (result != GCS_BLOCK_PCM_X_IMAGE_RESERVED && result != GCS_BLOCK_PCM_X_IMAGE_REPLAY) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	if (cluster_lms_shard_for_tag(&work.tag, cluster_lms_workers) != worker_id) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}
	if (result == GCS_BLOCK_PCM_X_IMAGE_RESERVED)
		gcs_block_pcm_x_materialize_reserved_work(worker_id, &work);
	else
		gcs_block_pcm_x_stage_ready_work(worker_id, &work);
}


/* 49 only installs the holder-side revoke ledger and wakes exact registered
 * holders.  It never copies a page, drops ownership, or synthesizes type 50
 * inside the IC handler. */
/* Log-once evidence for a repeating source-side REVOKE refusal: the live
 * descriptor lifecycle blocking the transfer leg must name itself, because
 * every refusal arm here is silent and the master keeps re-sending forever.
 * A NULL site resets the streak (the revoke made progress). */
static void
gcs_block_pcm_x_revoke_refusal_note(const char *site, int own_result,
									const ClusterPcmOwnSnapshot *snap)
{
	static const char *last_site = NULL;
	static uint32 last_flags = 0;
	static int last_result = 0;
	static bool logged = false;
	uint32 flags = snap != NULL ? snap->flags : 0;

	if (site == NULL) {
		last_site = NULL;
		logged = false;
		return;
	}
	if (site == last_site && own_result == last_result && flags == last_flags) {
		if (!logged) {
			logged = true;
			ereport(LOG,
					(errmsg("PCM-X source revoke is repeating a refusal at %s", site),
					 errdetail("result=%d pcm_state=%u own_flags=%u generation=%llu token=%llu",
							   own_result, (unsigned int)(snap != NULL ? snap->pcm_state : 0),
							   (unsigned int)flags,
							   (unsigned long long)(snap != NULL ? snap->generation : 0),
							   (unsigned long long)(snap != NULL ? snap->reservation_token : 0))));
		}
		return;
	}
	last_site = site;
	last_result = own_result;
	last_flags = flags;
	logged = false;
}


static void
cluster_gcs_handle_pcm_x_revoke_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXRevokePayload *revoke;
	ClusterPcmOwnSnapshot own_snapshot;
	GcsBlockDedupKey image_key;
	GcsBlockPcmXImageBinding reserved_binding;
	GcsBlockPcmXImageResult image_result;
	PcmXLocalHolderProgress holder_progress;
	PcmXQueueResult progress_result;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	uint64 source_generation = 0;
	int buffer_id;
	int worker_id;
	int32 source_node;
	int32 tag_master;
	bool have_source_generation = false;
	bool new_reservation = false;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXRevokePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	revoke = (const PcmXRevokePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(revoke->ref.identity.tag);
	if (!cluster_gcs_pcm_x_revoke_ingress_valid(revoke, env->payload_length, source_node,
												current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(&revoke->ref.identity.tag, source_node,
														current_epoch, &source_session)) {
		gcs_block_pcm_x_revoke_refusal_note("ingress-auth", 0, NULL);
		return;
	}
	if (!gcs_block_pcm_x_handler_tag_shard_exact(&revoke->ref.identity.tag))
		return;
	worker_id = cluster_ic_tier1_my_data_channel();
	if (!gcs_block_pcm_x_image_key(&revoke->ref, revoke->image_id, &image_key)) {
		cluster_pcm_x_runtime_fail_closed();
		return;
	}

	/* Once type 50 is armed, the resident source buffer has already been retired.
	 * Recover its source generation from the exact holder ledger instead of
	 * looking through the now-empty buffer mapping.  A fully drained duplicate
	 * is application-complete and must not reserve a fresh 8KB slot. */
	progress_result
		= cluster_pcm_x_local_holder_progress_exact(&revoke->ref.identity.tag, &holder_progress);
	if (progress_result == PCM_X_QUEUE_OK) {
		if (!gcs_block_pcm_x_ticket_ref_equal(&holder_progress.ref, &revoke->ref)
			|| holder_progress.image.image_id != revoke->image_id) {
			cluster_pcm_x_stats_note_queue_result(PCM_X_QUEUE_STALE);
			gcs_block_pcm_x_revoke_refusal_note("holder-ledger-stale", (int)PCM_X_QUEUE_STALE,
												NULL);
			return;
		}
		if ((holder_progress.flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) != 0) {
			if ((holder_progress.flags & PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK)
				!= PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK) {
				cluster_pcm_x_runtime_fail_closed();
				return;
			}
			result = cluster_pcm_x_local_holder_revoke_apply_exact(revoke, source_node,
																   source_session);
			cluster_pcm_x_stats_note_queue_result(result);
			return;
		}
		if (holder_progress.pending_opcode == PGRAC_IC_MSG_PCM_X_IMAGE_READY
			|| holder_progress.phase == PGRAC_IC_MSG_PCM_X_IMAGE_READY) {
			if (!cluster_gcs_pcm_x_holder_image_ready_exact(&holder_progress, &revoke->ref,
															revoke->image_id, cluster_node_id)) {
				cluster_pcm_x_runtime_fail_closed();
				return;
			}
			source_generation = holder_progress.image.source_own_generation;
			have_source_generation = true;
		}
	} else if (progress_result != PCM_X_QUEUE_NOT_FOUND) {
		cluster_pcm_x_stats_note_queue_result(progress_result);
		gcs_block_pcm_x_revoke_refusal_note("holder-progress", (int)progress_result, NULL);
		return;
	}

	if (!have_source_generation) {
		ClusterPcmOwnResult own_result;
		bool source_is_n;
		bool source_is_s;
		bool source_is_x;

		own_result = cluster_bufmgr_pcm_own_snapshot_by_tag(&revoke->ref.identity.tag, &buffer_id,
															&own_snapshot);
		gcs_block_pcm_x_note_own_result(own_result);
		source_is_n = own_snapshot.pcm_state == (uint8)PCM_STATE_N;
		source_is_s = own_snapshot.pcm_state == (uint8)PCM_STATE_S;
		source_is_x = own_snapshot.pcm_state == (uint8)PCM_STATE_X;
		if (own_result != CLUSTER_PCM_OWN_OK || (!source_is_n && !source_is_s && !source_is_x)
			|| own_snapshot.flags != 0) {
			gcs_block_pcm_x_revoke_refusal_note("ingress-snapshot", (int)own_result, &own_snapshot);
			return;
		}
		source_generation = own_snapshot.generation;
	}
	gcs_block_pcm_x_reserved_binding(revoke, source_generation, source_session, &reserved_binding);
	image_result = cluster_gcs_block_dedup_pcm_x_reserve(
		worker_id, &image_key, &revoke->ref.identity.tag, &reserved_binding);
	if (image_result != GCS_BLOCK_PCM_X_IMAGE_RESERVED
		&& image_result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE) {
		gcs_block_pcm_x_revoke_refusal_note("image-reserve", (int)image_result, NULL);
		return;
	}
	new_reservation = image_result == GCS_BLOCK_PCM_X_IMAGE_RESERVED;
	if (!new_reservation) {
		image_result = cluster_gcs_block_dedup_pcm_x_rearm_exact(
			worker_id, &image_key, &revoke->ref.identity.tag, &reserved_binding);
		if (image_result != GCS_BLOCK_PCM_X_IMAGE_REARMED
			&& image_result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE
			&& image_result != GCS_BLOCK_PCM_X_IMAGE_NOT_READY) {
			cluster_pcm_x_runtime_fail_closed();
			return;
		}
	}
	result = cluster_pcm_x_local_holder_revoke_apply_exact(revoke, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		gcs_block_pcm_x_revoke_refusal_note(NULL, 0, NULL);
		gcs_block_pcm_x_wake_registered_holders(&revoke->ref.identity.tag);
	} else {
		gcs_block_pcm_x_revoke_refusal_note("apply", (int)result, NULL);
		if (new_reservation
			&& cluster_gcs_block_dedup_pcm_x_release_exact(
				   worker_id, &image_key, &revoke->ref.identity.tag, &reserved_binding)
				   != GCS_BLOCK_PCM_X_IMAGE_RELEASED)
			cluster_pcm_x_runtime_fail_closed();
	}
}


static void
cluster_gcs_handle_pcm_x_image_ready_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXGrantPayload *image_ready;
	PcmXGrantPayload prepare;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXGrantPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	image_ready = (const PcmXGrantPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(image_ready->ref.identity.tag);
	if (!cluster_gcs_pcm_x_image_ready_ingress_valid(image_ready, env->payload_length, source_node,
													 current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(&image_ready->ref.identity.tag, source_node,
														current_epoch, &source_session))
		return;
	result = cluster_pcm_x_master_image_ready_exact(image_ready, source_node, source_session,
													&prepare);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_PREPARE_GRANT,
											prepare.ref.identity.node_id, &prepare,
											sizeof(prepare));
}


static void
cluster_gcs_handle_pcm_x_prepare_grant_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXGrantPayload *prepare;
	PcmXLocalHandle leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXGrantPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	prepare = (const PcmXGrantPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(prepare->ref.identity.tag);
	if (!cluster_gcs_pcm_x_prepare_grant_ingress_valid(prepare, env->payload_length, source_node,
													   current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(&prepare->ref.identity.tag, source_node,
														current_epoch, &source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&prepare->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_prepare_grant_exact(&leader, prepare, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		gcs_block_pcm_x_wake_requester(&prepare->ref.identity);
}


static void
cluster_gcs_handle_pcm_x_install_ready_envelope(const ClusterICEnvelope *env, const void *payload)
{
	PcmXInstallReadyPayload frame;
	PcmXPhasePayload commit;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;
	bool rebase_wire_active = false;
	bool source_rebase_capable = false;

	/* Both exact lengths are legal: the V1 104-byte frame normalizes to a
	 * zero rebase, the V2 112-byte frame carries the published grant base. */
	if (env == NULL || payload == NULL
		|| (env->payload_length != sizeof(PcmXInstallReadyPayload)
			&& env->payload_length != PCM_X_INSTALL_READY_V1_LEN)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	memset(&frame, 0, sizeof(frame));
	memcpy(&frame, payload, env->payload_length);
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(frame.ref.identity.tag);
	/* Receiver-side V2 admission: the sender-side coverage gate is not an
	 * ingress invariant, so a V2 frame re-proves BOTH the activated
	 * formation-wide coverage and the source's REBASE capability.  A
	 * SELF-loopback frame (master == requester node) has no HELLO record —
	 * exactly like gcs_block_pcm_x_source_capable, its capability is the
	 * local binary itself; the coverage flag still applies. */
	if (env->payload_length == sizeof(frame)) {
		rebase_wire_active = cluster_pcm_x_runtime_snapshot().rebase_wire_active;
		source_rebase_capable
			= source_node == cluster_node_id || cluster_sf_peer_supports_pcm_x_rebase(source_node);
	}
	if (!cluster_gcs_pcm_x_install_ready_ingress_valid(&frame, env->payload_length, source_node,
													   current_epoch, tag_master, cluster_node_id,
													   rebase_wire_active, source_rebase_capable)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(&frame.ref.identity.tag, source_node,
														current_epoch, &source_session))
		return;
	result = cluster_pcm_x_master_install_ready_exact(&frame, frame.rebased_own_generation,
													  source_node, source_session, &commit);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_COMMIT_X,
											commit.ref.identity.node_id, &commit, sizeof(commit));
}


static void
cluster_gcs_handle_pcm_x_commit_x_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *commit;
	PcmXLocalHandle leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	commit = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(commit->ref.identity.tag);
	if (!cluster_gcs_pcm_x_commit_x_ingress_valid(commit, env->payload_length, source_node,
												  current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(&commit->ref.identity.tag, source_node,
														current_epoch, &source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&commit->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_commit_x_exact(&leader, commit, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		gcs_block_pcm_x_wake_requester(&commit->ref.identity);
}


static void
cluster_gcs_handle_pcm_x_final_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXFinalAckPayload *final_ack;
	PcmAuthoritySnapshot authority;
	PcmXGrdHandoffToken handoff;
	PcmXGrdHandoffResult handoff_result;
	PcmXMasterFinalAckToken token;
	PcmXPhasePayload final_commit;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXFinalAckPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	final_ack = (const PcmXFinalAckPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(final_ack->ref.identity.tag);
	if (!cluster_gcs_pcm_x_final_ack_ingress_valid(final_ack, env->payload_length, source_node,
												   current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(&final_ack->ref.identity.tag, source_node,
														current_epoch, &source_session))
		return;
	result = cluster_pcm_x_master_final_ack_prepare_exact(final_ack, source_node, source_session,
														  &token);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
		gcs_block_pcm_x_master_drive_fail_closed(result);
		return;
	}
	if (!cluster_pcm_lock_authority_snapshot(final_ack->ref.identity.tag, &authority)
		|| !cluster_gcs_pcm_x_grd_handoff_token_build(&token, &authority, &handoff)) {
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		return;
	}
	handoff_result = cluster_pcm_lock_queue_handoff_x_exact(&handoff);
	if (handoff_result != PCM_X_GRD_HANDOFF_OK && handoff_result != PCM_X_GRD_HANDOFF_DUPLICATE) {
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		return;
	}
	result = cluster_pcm_x_master_final_ack_finalize_exact(&token, &final_commit);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
		/* GRD already committed; retaining the engine/image evidence and
		 * closing the runtime is the only 8.A-safe outcome. */
		(void)cluster_pcm_x_runtime_transition(PCM_X_RUNTIME_ACTIVE,
											   PCM_X_RUNTIME_RECOVERY_BLOCKED);
		return;
	}
	(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK,
										final_commit.ref.identity.node_id, &final_commit,
										sizeof(final_commit));
}


static void
cluster_gcs_handle_pcm_x_final_commit_ack_envelope(const ClusterICEnvelope *env,
												   const void *payload)
{
	const PcmXPhasePayload *final_commit;
	PcmXLocalReliableToken token;
	PcmXPhasePayload final_confirm;
	PcmXLocalHandle leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	final_commit = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(final_commit->ref.identity.tag);
	if (!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(final_commit, env->payload_length,
														  source_node, current_epoch, tag_master,
														  cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(
			&final_commit->ref.identity.tag, source_node, current_epoch, &source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&final_commit->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_final_commit_ack_exact(&leader, final_commit, source_node,
														source_session, &final_confirm, &token);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE) {
		(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM, source_node,
											&final_confirm, sizeof(final_confirm));
		gcs_block_pcm_x_wake_requester(&final_commit->ref.identity);
	}
}


static void
cluster_gcs_handle_pcm_x_final_confirm_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *final_confirm;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	final_confirm = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(final_confirm->ref.identity.tag);
	if (!cluster_gcs_pcm_x_final_confirm_ingress_valid(final_confirm, env->payload_length,
													   source_node, current_epoch, tag_master,
													   cluster_node_id)
		|| !gcs_block_pcm_x_transfer_ingress_authorized(
			&final_confirm->ref.identity.tag, source_node, current_epoch, &source_session))
		return;
	result = cluster_pcm_x_master_final_confirm_exact(final_confirm, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		cluster_gcs_pcm_x_terminal_kick(&final_confirm->ref);
}


static void
cluster_gcs_handle_pcm_x_cancel_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *ack;
	PcmXLocalHandle leader;
	PcmXLocalHandle new_leader;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	ack = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(ack->ref.identity.tag);
	if (!cluster_gcs_pcm_x_cancel_ack_ingress_valid(ack, env->payload_length, source_node,
													current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(ack->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = cluster_pcm_x_local_lookup_exact(&ack->ref.identity, &leader);
	if (result != PCM_X_QUEUE_OK)
		return;
	result = cluster_pcm_x_local_cancel_ack_exact(&leader, ack, source_node, source_session,
												  &new_leader);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		cluster_gcs_pcm_x_wake_cancel_rotation(&ack->ref.identity, &new_leader);
}


static PcmXQueueResult
gcs_block_pcm_x_local_drain_apply_exact(const PcmXDrainPollPayload *poll,
										int32 authenticated_master_node,
										uint64 authenticated_master_session, int worker_id)
{
	GcsBlockPcmXImageBinding binding;
	GcsBlockDedupKey key;
	PcmXLocalHolderProgress progress;
	PcmXLocalDrainCertificate certificate;
	PcmXQueueResult progress_result;
	PcmXQueueResult result;
	ClusterPcmOwnSelfHandoffSample handoff_sample;
	ClusterPcmOwnResult handoff_result;
	ClusterPcmXRevokeFinishMode finish_mode;
	uint64 holder_image_id = 0;
	bool certificate_exact;
	bool holder_image = false;
	bool holder_ref = false;
	bool self_source_handoff = false;

	progress_result = cluster_pcm_x_local_holder_progress_exact(&poll->ref.identity.tag, &progress);
	if (progress_result == PCM_X_QUEUE_OK
		&& gcs_block_pcm_x_ticket_ref_equal(&progress.ref, &poll->ref)) {
		holder_ref = true;
		holder_image_id = progress.image.image_id;
	} else if (progress_result != PCM_X_QUEUE_OK && progress_result != PCM_X_QUEUE_NOT_FOUND)
		return progress_result;

	result = cluster_pcm_x_local_drain_poll_certificate_exact(
		poll, authenticated_master_node, authenticated_master_session, &certificate);
	if (result != PCM_X_QUEUE_OK)
		return result;

	/* DRAIN mutates the holder leg.  Capture the image after that transition so
	 * IMAGE_READY cannot race between an old snapshot and the drain commit. */
	progress_result = cluster_pcm_x_local_holder_progress_exact(&poll->ref.identity.tag, &progress);
	if (progress_result == PCM_X_QUEUE_OK
		&& gcs_block_pcm_x_ticket_ref_equal(&progress.ref, &poll->ref)) {
		holder_ref = true;
		if (holder_image_id == 0)
			holder_image_id = progress.image.image_id;
		if (!cluster_gcs_pcm_x_holder_image_drained_exact(&progress, &poll->ref, holder_image_id,
														  cluster_node_id)
			|| !cluster_pcm_x_image_id_decode(progress.image.image_id, NULL, NULL)
			|| !gcs_block_pcm_x_image_key(&progress.ref, progress.image.image_id, &key)) {
			cluster_pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		memset(&binding, 0, sizeof(binding));
		binding.identity.ref = progress.ref;
		binding.identity.image = progress.image;
		binding.master_session = authenticated_master_session;
		holder_image = true;
	} else if (progress_result != PCM_X_QUEUE_OK && progress_result != PCM_X_QUEUE_NOT_FOUND) {
		/* Local DRAIN is already durable; losing its exact image evidence cannot
		 * be retried as ordinary queue work. */
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (holder_ref && !holder_image) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (!holder_image)
		return PCM_X_QUEUE_OK;
	self_source_handoff = binding.identity.image.source_node == (uint32)cluster_node_id
						  && poll->ref.identity.node_id == cluster_node_id;
	if (self_source_handoff) {
		/* The exact completion certificate - the writer-round protocol ledger
		 * captured under the drain's own tag lock - is the release authority.
		 * The live descriptor is NOT: its per-buf_id generation carries no
		 * BufferTag lineage, and the tag may legitimately have moved on
		 * (X->S downgrade for a reader, a later lifecycle) or been evicted
		 * since FINAL_ACK.  The descriptor probe below only reports a
		 * structurally malformed flags/token shape. */
		certificate_exact
			= certificate.valid && gcs_block_pcm_x_ticket_ref_equal(&certificate.ref, &poll->ref)
			  && certificate.image.image_id == binding.identity.image.image_id
			  && certificate.image.source_own_generation
					 == binding.identity.image.source_own_generation
			  && certificate.image.source_node == binding.identity.image.source_node
			  && certificate.master_node == authenticated_master_node
			  && certificate.master_session_incarnation == authenticated_master_session
			  && certificate.committed_own_generation
					 == binding.identity.image.source_own_generation + 1;
		handoff_result
			= cluster_bufmgr_pcm_own_self_handoff_probe(&poll->ref.identity.tag, &handoff_sample);
		if (handoff_result == CLUSTER_PCM_OWN_CORRUPT) {
			ereport(LOG, (errmsg("PCM-X self-source DRAIN found a malformed descriptor lifecycle"),
						  errdetail("own_flags=%u own_token=%llu live_gen=%llu pcm_state=%u "
									"buffer_type=%u bm_valid=%d found=%d",
									(unsigned int)handoff_sample.own_flags,
									(unsigned long long)handoff_sample.live_token,
									(unsigned long long)handoff_sample.live_generation,
									(unsigned int)handoff_sample.pcm_state,
									(unsigned int)handoff_sample.buffer_type,
									handoff_sample.bm_valid ? 1 : 0,
									handoff_sample.buffer_found ? 1 : 0)));
			cluster_pcm_x_runtime_fail_closed();
			return PCM_X_QUEUE_CORRUPT;
		}
		if (!certificate_exact) {
			/* Preserve the immutable record: without the exact certificate the
			 * release cannot be proven, but a mismatched ledger is not node
			 * corruption.  The master's next poll replays as DUPLICATE. */
			ereport(LOG,
					(errmsg("PCM-X self-source DRAIN certificate is not exact"),
					 errdetail("valid=%d cert_committed=%llu cert_image=%llu cert_source_gen=%llu "
							   "image=%llu source_gen=%llu source_node=%u live_gen=%llu "
							   "own_flags=%u pcm_state=%u found=%d",
							   certificate.valid ? 1 : 0,
							   (unsigned long long)certificate.committed_own_generation,
							   (unsigned long long)certificate.image.image_id,
							   (unsigned long long)certificate.image.source_own_generation,
							   (unsigned long long)binding.identity.image.image_id,
							   (unsigned long long)binding.identity.image.source_own_generation,
							   (unsigned int)binding.identity.image.source_node,
							   (unsigned long long)handoff_sample.live_generation,
							   (unsigned int)handoff_sample.own_flags,
							   (unsigned int)handoff_sample.pcm_state,
							   handoff_sample.buffer_found ? 1 : 0)));
			return PCM_X_QUEUE_NOT_READY;
		}
	}
	if (cluster_gcs_block_dedup_pcm_x_release_exact(worker_id, &key, &poll->ref.identity.tag,
													&binding)
		!= GCS_BLOCK_PCM_X_IMAGE_RELEASED) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (self_source_handoff) {
		if (ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->pcm_x_self_handoff_drain_count, 1);
		return PCM_X_QUEUE_OK;
	}
	finish_mode = cluster_pcm_x_revoke_finish_mode(&poll->ref.identity.tag, 0);
	if (finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_DROP)
		return PCM_X_QUEUE_OK;
	if (finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_RETAIN) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	if (cluster_bufmgr_pcm_own_release_retained_image(&poll->ref.identity.tag,
													  binding.identity.image.source_own_generation)
		!= CLUSTER_PCM_OWN_OK) {
		cluster_pcm_x_runtime_fail_closed();
		return PCM_X_QUEUE_CORRUPT;
	}
	return PCM_X_QUEUE_OK;
}


static void
cluster_gcs_handle_pcm_x_drain_poll_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXDrainPollPayload *poll;
	PcmXPhasePayload ack;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int worker_id;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXDrainPollPayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	poll = (const PcmXDrainPollPayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(poll->ref.identity.tag);
	if (!cluster_gcs_pcm_x_drain_poll_ingress_valid(poll, env->payload_length, source_node,
													current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(poll->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	if (!gcs_block_pcm_x_handler_tag_shard_exact(&poll->ref.identity.tag))
		return;
	worker_id = cluster_ic_tier1_my_data_channel();
	/* The 49-56 integration drains exact holder type 50 and requester type 56
	 * here first.  This DRAIN_POLL proves master application and publishes each
	 * local terminal tombstone; cancellation is already ready. */
	result = gcs_block_pcm_x_local_drain_apply_exact(poll, source_node, source_session, worker_id);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;
	memset(&ack, 0, sizeof(ack));
	ack.ref = poll->ref;
	(void)cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_DRAIN_ACK, source_node, &ack,
										sizeof(ack));
}


static void
cluster_gcs_handle_pcm_x_drain_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXPhasePayload *ack;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;
	int32 tag_master;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXPhasePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	ack = (const PcmXPhasePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	tag_master = cluster_gcs_lookup_master(ack->ref.identity.tag);
	if (!cluster_gcs_pcm_x_drain_ack_ingress_valid(ack, env->payload_length, source_node,
												   current_epoch, tag_master, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| cluster_gcs_block_phase_for_tag(ack->ref.identity.tag) == GCS_BLOCK_RECOVERING
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = cluster_pcm_x_master_terminal_leg_ack_exact(&ack->ref, PCM_X_TERMINAL_LEG_DRAIN,
														 source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		cluster_gcs_pcm_x_terminal_kick(&ack->ref);
}


static void
cluster_gcs_handle_pcm_x_retire_up_to_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const PcmXRetirePayload *request;
	PcmXRetirePayload ack;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXRetirePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	request = (const PcmXRetirePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	if (!gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !cluster_gcs_pcm_x_retire_request_ingress_valid(request, env->payload_length,
														   source_node, source_session,
														   current_epoch, cluster_node_id)
		|| !gcs_block_pcm_x_source_capable(source_node) || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = gcs_block_pcm_x_local_retire_apply_and_wake(request, source_node, source_session);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
		return;
	ack = *request;
	(void)gcs_block_pcm_x_send_retire_ack(source_node, &ack);
}


static void
cluster_gcs_handle_pcm_x_retire_up_to_ack_envelope(const ClusterICEnvelope *env,
												   const void *payload)
{
	const PcmXRetirePayload *ack;
	PcmXRuntimeSnapshot runtime;
	PcmXTicketRef ref;
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;
	int32 source_node;

	if (env == NULL || payload == NULL || env->payload_length != sizeof(PcmXRetirePayload)
		|| env->source_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	source_node = (int32)env->source_node_id;
	ack = (const PcmXRetirePayload *)payload;
	current_epoch = cluster_epoch_get_current();
	runtime = cluster_pcm_x_runtime_snapshot();
	if (!cluster_gcs_pcm_x_retire_ack_ingress_valid(
			ack, env->payload_length, source_node, current_epoch,
			runtime.master_session_incarnation, cluster_node_id)
		|| runtime.state != PCM_X_RUNTIME_ACTIVE || !gcs_block_pcm_x_source_capable(source_node)
		|| !cluster_qvotec_in_quorum() || !cluster_membership_is_member(cluster_node_id)
		|| !cluster_membership_is_member(source_node)
		|| !gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return;
	result = cluster_pcm_x_master_retire_ack_resolve_exact(ack, source_node, source_session, &ref);
	cluster_pcm_x_stats_note_queue_result(result);
	if (result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE)
		cluster_gcs_pcm_x_terminal_kick(&ref);
}


/* Drive one ticket without time-based reclamation.  An already-armed leg
 * returns an exact duplicate token and is resent; a refused send leaves that
 * durable leg intact for the next LMON formation tick. */
void
cluster_gcs_pcm_x_terminal_kick(const PcmXTicketRef *ref)
{
	static uint8 last_auth_failure[PCM_X_PROTOCOL_NODE_LIMIT];
	ClusterGcsPcmXAuthSample auth_sample;
	PcmXRuntimeSnapshot runtime;
	PcmXMasterAdmission cancelled;
	PcmXTerminalLegToken token;
	PcmXDrainPollPayload poll;
	PcmXPhasePayload cancel_ack;
	PcmXAdmitAckPayload prehandle_ack;
	PcmXRetirePayload retire;
	PcmXTicketRef resolved;
	PcmXQueueResult result;
	PcmXSessionAuthResult auth_result;
	uint64 current_epoch;
	uint64 responder_session;
	int kind;
	int node;
	int local_steps;
	bool prehandle_cancel;

	current_epoch = cluster_epoch_get_current();
	if (!cluster_gcs_pcm_x_terminal_ref_wire_valid(ref, current_epoch))
		return;
	if (cluster_node_id < 0 || cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT)
		return;
	if (cluster_gcs_lookup_master(ref->identity.tag) != cluster_node_id)
		return;
	if (cluster_gcs_block_phase_for_tag(ref->identity.tag) == GCS_BLOCK_RECOVERING)
		return;
	if (!cluster_qvotec_in_quorum() || !cluster_membership_is_member(cluster_node_id))
		return;
	if (ref->grant_generation == 0) {
		result = gcs_block_pcm_x_cancel_terminal_cleanup_exact(ref);
		cluster_pcm_x_stats_note_queue_result(result);
		cluster_pcm_x_terminal_note(PCM_X_TERMINAL_NOTE_CANCEL_CLEANUP, (uint32)result,
									ref->handle.ticket_id);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
			gcs_block_pcm_x_master_drive_fail_closed(result);
			return;
		}
		result = cluster_pcm_x_master_cancel_ack_snapshot_exact(ref, &cancelled, &prehandle_cancel);
		cluster_pcm_x_stats_note_queue_result(result);
		if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
			gcs_block_pcm_x_master_drive_fail_closed(result);
			return;
		}
		/* The requester must observe the phase-exact cancel ACK before DRAIN.
		 * Both frames use the same tag-shard DATA FIFO; refusing either ACK arm
		 * leaves DRAIN unarmed so the next LMON tick retries this order exactly. */
		if (prehandle_cancel) {
			memset(&prehandle_ack, 0, sizeof(prehandle_ack));
			prehandle_ack.ref = cancelled.ref;
			prehandle_ack.prehandle = cancelled.prehandle;
			prehandle_ack.result = PCM_X_QUEUE_OK;
			prehandle_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL;
			if (!cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK,
											   ref->identity.node_id, &prehandle_ack,
											   sizeof(prehandle_ack)))
				return;
		} else {
			memset(&cancel_ack, 0, sizeof(cancel_ack));
			cancel_ack.ref = cancelled.ref;
			cancel_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
			if (!cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_CANCEL_ACK, ref->identity.node_id,
											   &cancel_ack, sizeof(cancel_ack)))
				return;
		}
	}

	for (local_steps = 0; local_steps < 2 * PCM_X_PROTOCOL_NODE_LIMIT; local_steps++) {
		/* All ACKs may already be present.  Detach first; global ticket order is
		 * enforced inside the engine and the periodic oldest-ticket scan retries. */
		result = cluster_pcm_x_master_detach_terminal_exact(ref);
		cluster_pcm_x_terminal_note(PCM_X_TERMINAL_NOTE_DETACH, (uint32)result,
									ref->handle.ticket_id);
		if (result == PCM_X_QUEUE_OK) {
			gcs_block_pcm_x_master_drive_tag(&ref->identity.tag, ref->identity.cluster_epoch);
			return;
		}
		if (result == PCM_X_QUEUE_STALE) {
			gcs_block_pcm_x_master_drive_tag(&ref->identity.tag, ref->identity.cluster_epoch);
			return;
		}
		if (result == PCM_X_QUEUE_CORRUPT || result == PCM_X_QUEUE_COUNTER_EXHAUSTED)
			return;

		for (kind = PCM_X_TERMINAL_LEG_DRAIN; kind <= PCM_X_TERMINAL_LEG_RETIRE; kind++) {
			for (node = 0; node < PCM_X_PROTOCOL_NODE_LIMIT; node++) {
				if (!cluster_membership_is_member(node) || !gcs_block_pcm_x_source_capable(node))
					continue;
				auth_result = gcs_block_pcm_x_authenticated_session_result(
					node, current_epoch, &responder_session, &auth_sample);
				if (auth_result != PCM_X_SESSION_AUTH_OK) {
					if (last_auth_failure[node] != (uint8)auth_result) {
						last_auth_failure[node] = (uint8)auth_result;
						ereport(
							LOG,
							(errmsg("PCM-X terminal authority for node %d is not ready (reason %u)",
									node, (unsigned int)auth_result),
							 errdetail(
								 "expected epoch %llu, slot=(%llu,%llu,%llu)->(%llu,%llu,%llu), "
								 "fresh=%d->%d, connection=%s/%u->%s/%u",
								 (unsigned long long)current_epoch,
								 (unsigned long long)auth_sample.session_before,
								 (unsigned long long)auth_sample.slot_generation_before,
								 (unsigned long long)auth_sample.observed_epoch_before,
								 (unsigned long long)auth_sample.session_after,
								 (unsigned long long)auth_sample.slot_generation_after,
								 (unsigned long long)auth_sample.observed_epoch_after,
								 auth_sample.fresh_before, auth_sample.fresh_after,
								 auth_sample.connection_before_valid ? "valid" : "invalid",
								 auth_sample.connection_generation_before,
								 auth_sample.connection_after_valid ? "valid" : "invalid",
								 auth_sample.connection_generation_after)));
					}
					continue;
				}
				last_auth_failure[node] = (uint8)PCM_X_SESSION_AUTH_OK;
				if (!gcs_block_pcm_x_revalidate_peer_binding(node, current_epoch,
															 responder_session))
					continue;
				result = cluster_pcm_x_master_terminal_leg_arm_exact(
					ref, (PcmXTerminalLegKind)kind, node, responder_session, &token);
				if (result == PCM_X_QUEUE_STALE || result == PCM_X_QUEUE_NOT_READY)
					continue;
				if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE) {
					cluster_pcm_x_terminal_note(kind == PCM_X_TERMINAL_LEG_DRAIN
													? PCM_X_TERMINAL_NOTE_ARM_DRAIN
													: PCM_X_TERMINAL_NOTE_ARM_RETIRE,
												(uint32)result, ref->handle.ticket_id);
					return;
				}
				if (kind == PCM_X_TERMINAL_LEG_DRAIN) {
					memset(&poll, 0, sizeof(poll));
					poll.ref = *ref;
					poll.drain_generation = token.drain_generation;
					/* Even self DRAIN is application traffic.  Dispatch it through
					 * the tag shard so holder-byte release and terminal publication
					 * remain one single-consumer action. */
					cluster_pcm_x_terminal_note(
						PCM_X_TERMINAL_NOTE_DRAIN_STAGE,
						cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_DRAIN_POLL, node, &poll,
													  sizeof(poll))
							? (uint32)PCM_X_QUEUE_OK
							: (uint32)PCM_X_QUEUE_NOT_READY,
						ref->handle.ticket_id);
					return;
				} else {
					runtime = cluster_pcm_x_runtime_snapshot();
					memset(&retire, 0, sizeof(retire));
					retire.cluster_epoch = ref->identity.cluster_epoch;
					retire.master_session_incarnation = runtime.master_session_incarnation;
					retire.retire_through_ticket_id = ref->handle.ticket_id;
					retire.sender_node = node;
					if (node != cluster_node_id) {
						cluster_pcm_x_terminal_note(
							PCM_X_TERMINAL_NOTE_RETIRE_STAGE,
							gcs_block_pcm_x_stage_retire_up_to(node, &retire, &ref->identity.tag)
								? (uint32)PCM_X_QUEUE_OK
								: (uint32)PCM_X_QUEUE_NOT_READY,
							ref->handle.ticket_id);
						return;
					}
					result = gcs_block_pcm_x_local_retire_apply_and_wake(&retire, cluster_node_id,
																		 responder_session);
					cluster_pcm_x_terminal_note(PCM_X_TERMINAL_NOTE_RETIRE_LOCAL, (uint32)result,
												ref->handle.ticket_id);
					if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
						return;
					result = cluster_pcm_x_master_retire_ack_resolve_exact(
						&retire, cluster_node_id, responder_session, &resolved);
					cluster_pcm_x_terminal_note(PCM_X_TERMINAL_NOTE_RETIRE_ACK_RESOLVE,
												(uint32)result, ref->handle.ticket_id);
				}
				if (result != PCM_X_QUEUE_OK && result != PCM_X_QUEUE_DUPLICATE)
					return;
				/* A local leg completed synchronously; restart from detach/next node. */
				break;
			}
			if (node < PCM_X_PROTOCOL_NODE_LIMIT)
				break;
		}
		if (kind > PCM_X_TERMINAL_LEG_RETIRE)
			return;
	}
}


static void
gcs_block_pcm_x_master_drive_retry_tick(void)
{
	static Size cursor = 0;
	BufferTag tag;
	uint64 cluster_epoch;

	if (cluster_pcm_x_master_drive_work_next(&cursor, PCM_X_MASTER_DRIVE_SCAN_BUDGET, &tag,
											 &cluster_epoch)
		== PCM_X_QUEUE_OK)
		gcs_block_pcm_x_master_drive_tag(&tag, cluster_epoch);
}


#define GCS_BLOCK_PCM_X_TERMINAL_TICK_BUDGET 8

static void
gcs_block_pcm_x_terminal_retry_tick(void)
{
	PcmXTicketRef ref;
	uint64 after = 0;
	int kicks;

	/* The oldest ticket keeps frontier priority, but a stuck head must not
	 * starve younger terminal tickets: the head's own RETIRE preflight can be
	 * waiting for exactly the younger ticket's DRAIN evidence (cross-lane
	 * holder interlock), so each tick walks the terminal set in id order. */
	for (kicks = 0; kicks < GCS_BLOCK_PCM_X_TERMINAL_TICK_BUDGET; kicks++) {
		if (cluster_pcm_x_master_terminal_work_next_after(after, &ref) != PCM_X_QUEUE_OK)
			return;
		cluster_gcs_pcm_x_terminal_kick(&ref);
		after = ref.handle.ticket_id;
	}
}


static const ClusterICMsgTypeInfo pcm_x_enqueue_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_ENQUEUE,
	.name = "pcm_x_enqueue",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_enqueue_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_admit_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_ADMIT_ACK,
	.name = "pcm_x_admit_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_admit_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_admit_confirm_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM,
	.name = "pcm_x_admit_confirm",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_admit_confirm_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_admit_confirm_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK,
	.name = "pcm_x_admit_confirm_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_admit_confirm_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_blocker_set_begin_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN,
	.name = "pcm_x_blocker_set_begin",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_blocker_set_begin_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_blocker_set_edge_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE,
	.name = "pcm_x_blocker_set_edge",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_blocker_set_edge_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_blocker_set_commit_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT,
	.name = "pcm_x_blocker_set_commit",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_blocker_set_commit_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_blocker_set_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK,
	.name = "pcm_x_blocker_set_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_blocker_set_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_revoke_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_REVOKE,
	.name = "pcm_x_revoke",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_revoke_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_image_ready_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_IMAGE_READY,
	.name = "pcm_x_image_ready",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_image_ready_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_prepare_grant_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_PREPARE_GRANT,
	.name = "pcm_x_prepare_grant",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_prepare_grant_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_install_ready_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_INSTALL_READY,
	.name = "pcm_x_install_ready",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_install_ready_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_commit_x_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_COMMIT_X,
	.name = "pcm_x_commit_x",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_commit_x_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_final_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_FINAL_ACK,
	.name = "pcm_x_final_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_final_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_final_commit_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK,
	.name = "pcm_x_final_commit_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_final_commit_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_final_confirm_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM,
	.name = "pcm_x_final_confirm",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_final_confirm_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_prehandle_cancel_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL,
	.name = "pcm_x_prehandle_cancel",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_prehandle_cancel_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_prehandle_cancel_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK,
	.name = "pcm_x_prehandle_cancel_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_prehandle_cancel_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_cancel_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_CANCEL,
	.name = "pcm_x_cancel",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_cancel_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_cancel_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_CANCEL_ACK,
	.name = "pcm_x_cancel_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_cancel_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_drain_poll_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_DRAIN_POLL,
	.name = "pcm_x_drain_poll",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_drain_poll_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_drain_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_DRAIN_ACK,
	.name = "pcm_x_drain_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_drain_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_retire_up_to_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO,
	.name = "pcm_x_retire_up_to",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_retire_up_to_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

static const ClusterICMsgTypeInfo pcm_x_retire_up_to_ack_info = {
	.msg_type = PGRAC_IC_MSG_PCM_X_RETIRE_ACK,
	.name = "pcm_x_retire_ack",
	.allowed_producer_mask
	= CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON | CLUSTER_IC_PRODUCER_LMS_DATA,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_pcm_x_retire_up_to_ack_envelope,
	.plane = CLUSTER_IC_PLANE_DATA,
};

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
	ClusterXpScope xp_inv;

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
	if (ClusterGcsBlock == NULL || !cluster_past_image)
		return;

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
		ClusterGcsBlock->pi_note_drain_seq++;
		SpinLockRelease(&ClusterGcsBlock->pi_note_lock);

		master_node = cluster_gcs_lookup_master(tag);
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
	ClusterPcmOwnResult own_result;
	PcmXQueueResult queue_result;
	PcmLockMode pre_state;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	SCN page_scn = InvalidScn; /* spec-2.41 D3 — ACK SCN carrier */
	uint64 page_scn_raw = 0;
	uint64 master_session = 0;
	uint8 ack_status = 0; /* OK */
	bool kept_pi = false; /* spec-6.12h D-h2 — drop converted to a PI */
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
		 * Rule 8.A).  Return false so the caller PARKS the directive (the round-5
		 * A2 park lot); the LMS-loop retry re-runs once the grant finalized
		 * (PENDING cleared), and then sees the real X/S and invalidates it.
		 */
		if (cluster_bufmgr_block_grant_pending(inv->tag)) {
			cluster_pcm_note_invalidate_parked_grant_pending();
			/*
			 * Ruling ② — a BUSY-capable master gets the negative ACK RIGHT
			 * NOW instead of a silent park: it aborts the round (clears
			 * pending_x, releases the slot) and retries with a new round
			 * identity, which breaks the timeout-mediated loop (the
			 * GRANT_PENDING owner is typically an S acquire waiting on that
			 * very pending_x).  Nothing local changed; terminal for THIS
			 * directive.  An old master falls back to the round-5 park.
			 */
			if (cluster_sf_peer_supports_gcs_inval_busy(inv->master_node)) {
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
		/* A generation-less type-17 may bypass the physical drop only while
		 * the local convert engine proves it names the exact frozen non-source
		 * participant whose blocker set this master already ACKed.  This keeps
		 * old/legacy INVALIDATEs on their original pin-intolerant path. */
		if (pre_state == PCM_LOCK_MODE_S && cluster_gcs_lookup_master(inv->tag) == inv->master_node
			&& gcs_block_pcm_x_authenticated_session(inv->master_node, current_epoch,
													 &master_session)
			&& gcs_block_pcm_x_revalidate_peer_binding(inv->master_node, current_epoch,
													   master_session)) {
			queue_result = cluster_pcm_x_local_queue_invalidate_authorize_exact(
				&inv->tag, inv->epoch, inv->request_id, (int32)inv->invalidating_for_x_node,
				inv->master_node, master_session);
			cluster_pcm_x_stats_note_queue_result(queue_result);
			if (queue_result == PCM_X_QUEUE_OK) {
				own_result = cluster_bufmgr_pcm_own_release_pinned_s_for_gcs(&inv->tag, &page_lsn,
																			 &page_scn_raw);
				if (own_result == CLUSTER_PCM_OWN_OK) {
					page_scn = (SCN)page_scn_raw;
					(void)cluster_pcm_lock_apply_gcs_transition(
						inv->tag, PCM_TRANS_S_TO_N_INVALIDATE, cluster_node_id);
					if (ClusterGcsBlock != NULL)
						pg_atomic_fetch_add_u64(
							&ClusterGcsBlock->invalidate_passive_s_release_count, 1);
					break;
				}
				if (own_result == CLUSTER_PCM_OWN_CORRUPT || own_result == CLUSTER_PCM_OWN_EXHAUSTED
					|| own_result == CLUSTER_PCM_OWN_INVALID) {
					cluster_pcm_x_runtime_fail_closed();
					return false;
				}
			} else if (queue_result == PCM_X_QUEUE_CORRUPT
					   || queue_result == PCM_X_QUEUE_COUNTER_EXHAUSTED
					   || queue_result == PCM_X_QUEUE_INVALID) {
				gcs_block_pcm_x_master_drive_fail_closed(queue_result);
				return false;
			}
		}
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
		if (cluster_sf_peer_supports_gcs_inval_busy(inv->master_node)) {
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
 *	With GRANT_PENDING staged the handler parks (returns false, bumps
 *	pcm.invalidate_parked_grant_pending_count) BEFORE any wire send, so the
 *	synthetic request_id/master_node never reach the ACK path.  Without the
 *	park fix it would have acked already_invalidated (the W3 defect) — the
 *	ACK then goes to a stale request_id slot and is rejected (HC100), so
 *	even the defect arm cannot corrupt master state from this shim.
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
static PcmXQueueResult
gcs_block_pcm_x_queue_invalidate_ack_match(const GcsBlockInvalidateAckPayload *ack,
										   int32 source_node, PcmXMasterDriveSnapshot *snapshot_out)
{
	PcmXQueueResult result;
	uint64 current_epoch;
	uint64 source_session;

	if (ack == NULL || snapshot_out == NULL)
		return PCM_X_QUEUE_INVALID;
	memset(snapshot_out, 0, sizeof(*snapshot_out));
	current_epoch = cluster_epoch_get_current();
	if (cluster_gcs_lookup_master(ack->tag) != cluster_node_id)
		return PCM_X_QUEUE_NOT_FOUND;
	result = cluster_pcm_x_master_drive_snapshot_exact(&ack->tag, current_epoch, snapshot_out);
	if (result != PCM_X_QUEUE_OK)
		return result == PCM_X_QUEUE_CORRUPT ? result : PCM_X_QUEUE_NOT_FOUND;
	result = cluster_gcs_pcm_x_invalidate_ack_match_exact(snapshot_out, ack, current_epoch,
														  source_node);
	if (result == PCM_X_QUEUE_NOT_FOUND)
		return result;
	if (!gcs_block_pcm_x_authenticated_session(source_node, current_epoch, &source_session)
		|| !gcs_block_pcm_x_revalidate_peer_binding(source_node, current_epoch, source_session))
		return PCM_X_QUEUE_STALE;
	return result;
}


static void
cluster_gcs_handle_block_invalidate_ack_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockInvalidateAckPayload *ack = (const GcsBlockInvalidateAckPayload *)payload;
	PcmXMasterDriveSnapshot queue_snapshot;
	PcmXMasterDriveSnapshot queue_updated;
	PcmXQueueResult queue_result = PCM_X_QUEUE_NOT_FOUND;
	uint64 current_req_id;
	uint64 expected_epoch;
	uint32 expected_bm;
	SCN ack_page_scn = InvalidScn; /* spec-2.41 D3 — ACK now carries pd_block_scn */
	BufferTag ack_tag = { 0 };
	bool queue_authority_applied = false;
	bool queue_positive = false;
	bool valid = false;

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
		if (ack->epoch == cluster_epoch_get_current())
			gcs_block_pi_discard_master_apply(ack->tag,
											  GcsBlockInvalidateAckPayloadGetPageScn(ack));
		return;
	}
	if (ack->ack_status == GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_KEPT_NOTE) {
		if (ack->epoch == cluster_epoch_get_current() && ack->sender_node >= 0
			&& ack->sender_node < 32)
			cluster_pcm_lock_pi_holder_note(ack->tag, ack->sender_node);
		return;
	}

	queue_result = gcs_block_pcm_x_queue_invalidate_ack_match(ack, (int32)env->source_node_id,
															  &queue_snapshot);
	if (queue_result != PCM_X_QUEUE_NOT_FOUND) {
		cluster_pcm_x_stats_note_queue_result(queue_result);
		if (queue_result == PCM_X_QUEUE_OK)
			queue_positive = true;
		else if (queue_result == PCM_X_QUEUE_BAD_STATE || queue_result == PCM_X_QUEUE_CORRUPT
				 || queue_result == PCM_X_QUEUE_COUNTER_EXHAUSTED
				 || queue_result == PCM_X_QUEUE_INVALID)
			gcs_block_pcm_x_master_drive_fail_closed(queue_result);
		if (!queue_positive)
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
		queue_authority_applied = cluster_pcm_lock_apply_gcs_transition(
			ack->tag, PCM_TRANS_S_TO_N_INVALIDATE, ack->sender_node);
		if (queue_positive && !queue_authority_applied) {
			gcs_block_pcm_x_master_drive_fail_closed(PCM_X_QUEUE_BAD_STATE);
			return;
		}
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
	if (queue_positive) {
		uint32 sender_bit = UINT32_C(1) << (uint32)ack->sender_node;

		queue_result = cluster_pcm_x_master_drive_bitmap_replace_exact(
			&queue_snapshot, queue_snapshot.pending_s_holders_bitmap,
			queue_snapshot.acked_s_holders_bitmap | sender_bit, &queue_updated);
		cluster_pcm_x_stats_note_queue_result(queue_result);
		if (queue_result == PCM_X_QUEUE_OK || queue_result == PCM_X_QUEUE_DUPLICATE)
			gcs_block_pcm_x_master_drive_tag(&ack->tag, ack->epoch);
		else
			gcs_block_pcm_x_master_drive_fail_closed(queue_result);
		return;
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
	cluster_ic_register_msg_type(&pcm_x_enqueue_info);
	cluster_ic_register_msg_type(&pcm_x_admit_ack_info);
	cluster_ic_register_msg_type(&pcm_x_admit_confirm_info);
	cluster_ic_register_msg_type(&pcm_x_admit_confirm_ack_info);
	cluster_ic_register_msg_type(&pcm_x_blocker_set_begin_info);
	cluster_ic_register_msg_type(&pcm_x_blocker_set_edge_info);
	cluster_ic_register_msg_type(&pcm_x_blocker_set_commit_info);
	cluster_ic_register_msg_type(&pcm_x_blocker_set_ack_info);
	cluster_ic_register_msg_type(&pcm_x_revoke_info);
	cluster_ic_register_msg_type(&pcm_x_image_ready_info);
	cluster_ic_register_msg_type(&pcm_x_prepare_grant_info);
	cluster_ic_register_msg_type(&pcm_x_install_ready_info);
	cluster_ic_register_msg_type(&pcm_x_commit_x_info);
	cluster_ic_register_msg_type(&pcm_x_final_ack_info);
	cluster_ic_register_msg_type(&pcm_x_final_commit_ack_info);
	cluster_ic_register_msg_type(&pcm_x_final_confirm_info);
	cluster_ic_register_msg_type(&pcm_x_prehandle_cancel_info);
	cluster_ic_register_msg_type(&pcm_x_prehandle_cancel_ack_info);
	cluster_ic_register_msg_type(&pcm_x_cancel_info);
	cluster_ic_register_msg_type(&pcm_x_cancel_ack_info);
	cluster_ic_register_msg_type(&pcm_x_drain_poll_info);
	cluster_ic_register_msg_type(&pcm_x_drain_ack_info);
	cluster_ic_register_msg_type(&pcm_x_retire_up_to_info);
	cluster_ic_register_msg_type(&pcm_x_retire_up_to_ack_info);
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
cluster_gcs_get_pcm_x_image_fetch_recovering_retry_count(void)
{
	return ClusterGcsBlock
			   ? pg_atomic_read_u64(&ClusterGcsBlock->pcm_x_image_fetch_recovering_retry_count)
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

uint64
cluster_gcs_get_block_dedup_pcm_x_stage_count(void)
{
	return cluster_gcs_block_dedup_get_pcm_x_stage_count();
}

uint64
cluster_gcs_get_block_dedup_pcm_x_replay_count(void)
{
	return cluster_gcs_block_dedup_get_pcm_x_replay_count();
}

uint64
cluster_gcs_get_block_dedup_pcm_x_release_count(void)
{
	return cluster_gcs_block_dedup_get_pcm_x_release_count();
}

uint64
cluster_gcs_get_block_dedup_pcm_x_failclosed_count(void)
{
	return cluster_gcs_block_dedup_get_pcm_x_failclosed_count();
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
cluster_gcs_block_on_epoch_advance(uint64 new_epoch)
{
	int b;
	int j;

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
