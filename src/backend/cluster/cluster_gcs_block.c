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

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_clean_leave.h" /* spec-5.13 S6 — CL-I5 serve gate */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h" /* spec-4.6 D4 — dead-master block-path guard */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h" /* spec-2.34 D1 — counter forward */
#include "cluster/cluster_grd.h"			 /* spec-4.6 D4 — block_path_failclosed counter */
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_membership.h"		/* spec-5.16 D3b — is_member master-side gate */
#include "cluster/cluster_qvotec.h"			/* spec-5.16 D3b — in_quorum master-side gate */
#include "cluster/cluster_recovery_merge.h" /* spec-4.7 D5 — recovered_through redo gate */
#include "cluster/cluster_xnode_profile.h"	/* spec-5.59 D2/D3/D4 profiling buckets */
#include "cluster/cluster_xnode_lever.h"	/* spec-6.12a — downgrade counters */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_itl.h" /* spec-5.2 D11 — active-ITL writer-transfer guard */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_touched_peers.h" /* spec-5.14 D2 class 2 */
#include "common/hashfn.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/backendid.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/latch.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
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

#define MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND 8

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
	 * 'gcs_recovery').  8 counters per spec §2.4. */
	pg_atomic_uint64 recovery_block_resources_recovering; /* phase_for_tag → RECOVERING hits */
	pg_atomic_uint64 recovery_buffers_redeclared;		  /* survivor re-declare sent (D2) */
	pg_atomic_uint64 recovery_block_state_rebuilt;		  /* master rebuild applied (D2/D3) */
	pg_atomic_uint64 recovery_redo_boundary_waits;		  /* redo gate: not yet covered (D5) */
	pg_atomic_uint64 recovery_redo_boundary_reached;	  /* redo gate: covered (D5) */
	pg_atomic_uint64 recovery_stale_block_drop; /* re-declare dropped: off-epoch/bad (D2) */
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
	LWLockPadded invalidate_broadcast_lock;			   /* protects identity + ack bitmap */
	ConditionVariable invalidate_broadcast_cv;
	/* PGRAC: spec-6.12a — request-id source for the LOCAL-master S->X
	 * upgrade's invalidate broadcast (backend-context caller has no wire
	 * request to borrow an id from; uniqueness vs stale acks is all the
	 * slot needs). */
	pg_atomic_uint64 local_upgrade_request_seq;
} ClusterGcsBlockShared;


static ClusterGcsBlockShared *ClusterGcsBlock = NULL;
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
static void gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot);
static void gcs_block_send_reply(int32 dest_node, const GcsBlockRequestPayload *req,
								 GcsBlockReplyStatus status, XLogRecPtr page_lsn,
								 const char *block_data);
static bool gcs_block_get_ship_image(BufferTag tag, int32 dest_node, XLogRecPtr *out_page_lsn,
									 char *copy_buf, const char **out_block_payload,
									 uint32 *out_block_lkey,
									 ClusterICSgeReleaseCallback *out_release_cb,
									 void **out_release_arg, ClusterSfDepVec *out_sf_dep_vec,
									 bool *out_sf_dep_valid);
static void gcs_block_release_ship_image(ClusterICSgeReleaseCallback release_cb, void *release_arg);
static uint32 gcs_block_compute_checksum(const char *block_data);
static uint32 gcs_block_compute_invalidate_checksum(const GcsBlockInvalidatePayload *inv);
static uint32 gcs_block_compute_invalidate_ack_checksum(const GcsBlockInvalidateAckPayload *ack);
static uint32 gcs_block_compute_redeclare_checksum(const GcsBlockRedeclarePayload *p);
static void gcs_block_install_block(BufferDesc *buf, const char *block_data, XLogRecPtr page_lsn);
static void gcs_block_install_reply_block(BufferDesc *buf, const char *block_data,
										  XLogRecPtr page_lsn,
										  const ClusterGcsBlockOutstandingSlot *slot);
static bool gcs_block_decode_reply_payload(const ClusterICEnvelope *env, const void *payload,
										   const GcsBlockReplyHeader **out_hdr,
										   const char **out_block_data, bool *out_sf_dep_valid,
										   uint8 *out_sf_flags, ClusterSfDepVec *out_sf_dep_vec);
/* PGRAC: spec-2.36 D3 (HC116) — master synchronous broadcast invalidate.
 * Enumerates `holders_bm` (1 bit per cluster node), emits INVALIDATE
 * envelope to each, waits for all INVALIDATE_ACK msg_type 18 within
 * cluster.gcs_block_invalidate_ack_timeout_ms;  retries failed/timed-out
 * holders per spec-2.34 retransmit budget;  returns true on full
 * collection, false on budget exhaustion. */
static bool gcs_block_broadcast_invalidate_and_wait(const GcsBlockRequestPayload *req,
													uint32 holders_bm);


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
		LWLockInitialize(&ClusterGcsBlock->invalidate_broadcast_lock.lock,
						 LWTRANCHE_CLUSTER_GCS_BLOCK);
		ConditionVariableInit(&ClusterGcsBlock->invalidate_broadcast_cv);
		/* PGRAC: spec-6.12a — local-upgrade broadcast id source. */
		pg_atomic_init_u64(&ClusterGcsBlock->local_upgrade_request_seq, 0);

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
			slot->request_id = blk->next_request_id++;
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
	LWLockRelease(&blk->lock.lock);
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

static void
gcs_block_release_ship_image(ClusterICSgeReleaseCallback release_cb, void *release_arg)
{
	if (release_cb != NULL)
		release_cb(release_arg);
}

static bool
gcs_block_get_ship_image(BufferTag tag, int32 dest_node, XLogRecPtr *out_page_lsn, char *copy_buf,
						 const char **out_block_payload, uint32 *out_block_lkey,
						 ClusterICSgeReleaseCallback *out_release_cb, void **out_release_arg,
						 ClusterSfDepVec *out_sf_dep_vec, bool *out_sf_dep_valid)
{
	void *scratch = NULL;
	uint32 scratch_lkey = 0;
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

	if (cluster_ic_rdma_block_sge_supported(NULL)
		&& cluster_ic_mux_peer_transport(dest_node) == CLUSTER_IC_PEER_TRANSPORT_RDMA) {
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
	}

	if (smart_fusion_reply) {
		if (!cluster_bufmgr_copy_block_for_gcs_smart_fusion(tag, out_page_lsn, copy_buf,
															out_sf_dep_vec))
			return false;
		if (out_sf_dep_valid != NULL)
			*out_sf_dep_valid = true;
	} else if (!cluster_bufmgr_copy_block_for_gcs(tag, out_page_lsn, copy_buf))
		return false;
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
	page = BufferGetPage(BufferDescriptorGetBuffer(buf));
	memcpy(page, block_data, GCS_BLOCK_DATA_SIZE);
	PageSetLSN(page, page_lsn);
	LWLockRelease(content_lock);
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
	 * live survivor (recovery-aware routing).  The survivor may SERVE only
	 * after the dead origin's merged WAL recovery passes the redo-before-
	 * unfreeze gate;  before that the shared-storage / re-declared version may
	 * be stale → fail-closed RECOVERING (never a stale page).
	 *
	 * TWO conditions, both required (Q5):
	 *  (a) is_materialized(origin):  the dead origin's merged replay completed
	 *      (publish is atomic at end-of-replay with the max EndRecPtr).  This
	 *      is the cold-block safety door — a block NO survivor observed has no
	 *      required_lsn to bound it, so the whole stream must be replayed
	 *      before the on-disk version is trusted current.
	 *  (b) redo_lsn_covered(origin, pi_watermark(tag)):  for a block some
	 *      survivor DID observe (rebuilt pi_watermark_lsn > 0), the dead
	 *      origin's recovered_lsn must reach that observed page_lsn — else the
	 *      dead node wrote a version a survivor saw but whose WAL never durably
	 *      reached us → lost-write → fail-closed.  This is the LSN comparison
	 *      (NOT a bool), live in the serve path;  required_lsn == 0 (cold) is
	 *      trivially covered and (a) carries the safety.
	 *
	 * Once both hold → NORMAL → the re-routed survivor serves (rebuilt-from-
	 * redeclare for held blocks, lazy minimal view for cold blocks).
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
										int master_node, bool clean_eligible)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64 request_id = 0;
	GcsBlockRequestPayload payload;
	BufferTag tag;
	bool granted = false;
	bool granted_storage_fallback = false;
	bool read_image = false; /* spec-5.2 D2: one-shot read image, non-durable */
	bool terminal_denied = false;
	bool retransmit_warning_emitted = false;
	uint8 final_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
	int32 final_forwarding_master = GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
	XLogRecPtr final_page_lsn = InvalidXLogRecPtr;
	int retry_attempt;
	int max_retries;
	int current_master;
	/* PGRAC: spec-5.59 D2/D3/D4 — requester-wait + index-overlay scopes. */
	ClusterXpScope xp_req;
	ClusterXpScope xp_idx;
	ClusterXpScope xp_recv;
	bool xp_is_read;
	bool xp_is_index;

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
				LWLockRelease(&blk->lock.lock);
			}

			if (retry_attempt == 0)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_request_count, 1);
			else
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->retransmit_send_count, 1);

			if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
														  (uint32)current_master, &payload,
														  sizeof(payload)))
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
								errmsg("cluster_gcs_block: failed to enqueue "
									   "GCS_BLOCK_REQUEST to node %d",
									   current_master)));

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
				uint32 expected;
				uint32 got;

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

			/* PGRAC: spec-2.36 D6 (HC117) — reader starvation guard transient
			 * denial.  N→S request was rejected because an X writer's broadcast
			 * is in flight at the master;  reader exponential-backoffs per
			 * cluster.gcs_block_starvation_max_retries and
			 * cluster.gcs_block_starvation_backoff_ms.  Budget exhaustion
			 * surfaces as 53R92 ERRCODE_CLUSTER_GCS_BLOCK_STARVATION_EXHAUSTED. */
			if (final_status == GCS_BLOCK_REPLY_DENIED_PENDING_X) {
				int starvation_attempt = retry_attempt;
				int starvation_max = cluster_gcs_block_starvation_max_retries;
				long backoff_ms;

				if (starvation_attempt >= starvation_max) {
					terminal_denied = true;
					ereport(ERROR, (errcode(ERRCODE_CLUSTER_GCS_BLOCK_STARVATION_EXHAUSTED),
									errmsg("cluster_gcs_block: reader starvation retry budget "
										   "exhausted (HC117)")));
					break;
				}
				backoff_ms = (long)cluster_gcs_block_starvation_backoff_ms
							 * (1L << (starvation_attempt < 16 ? starvation_attempt : 16));
				if (backoff_ms > 25000)
					backoff_ms = 25000;
				(void)WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, backoff_ms,
								WAIT_EVENT_GCS_BLOCK_STARVATION_RETRY);
				ResetLatch(MyLatch);
				current_master = cluster_gcs_lookup_master(tag);
				if (retry_attempt < max_retries)
					continue;
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
				if (cluster_gcs_block_lost_write_action == 0 /* ERROR */) {
					terminal_denied = true;
					ereport(ERROR, (errcode(ERRCODE_CLUSTER_LOST_WRITE_DETECTED),
									errmsg("cluster_gcs_block: lost write detected on tag "
										   "spc=%u db=%u rel=%u block=%u",
										   tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum),
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
					ereport(WARNING, (errmsg("cluster_gcs_block: lost write detected on tag "
											 "spc=%u db=%u rel=%u block=%u (action=warn)",
											 tag.spcOid, tag.dbOid, tag.relNumber, tag.blockNum)));
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

	/* PGRAC: spec-5.59 D2/D3/D4 — close the requester-wait (and index
	 * overlay) scopes at the single normal-exit funnel; terminal ereport
	 * paths above simply lose the sample (stack scope, harmless). */
	cluster_xp_end(&xp_idx);
	cluster_xp_end(&xp_req);

	/* spec-5.2 D2: GRANTED / STORAGE_FALLBACK record durable ownership (the
	 * caller mirrors PCM state); READ_IMAGE is a one-shot non-durable read so
	 * the caller must leave buf->pcm_state == N. */
	if (granted || granted_storage_fallback)
		return true;
	if (read_image)
		return false;

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
		cluster_pcm_lock_master_take_x_after_transfer(
			tag, installed_page_lsn, (SCN)((PageHeader)slot->reply_block_data)->pd_block_scn);
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
		if (cluster_gcs_block_lost_write_action == 0 /* ERROR */)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_LOST_WRITE_DETECTED),
					 errmsg("cluster_gcs_block: lost write detected on tag "
							"spc=%u db=%u relNumber=%u block=%u",
							tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
							(unsigned int)tag.blockNum),
					 errhint("The holder-forward shipped block.pd_block_scn is below the "
							 "master pi_watermark_scn (or a tracked block shipped an "
							 "unstamped page).  Inspect dump_gcs.lost_write_detected_count "
							 "and cluster_pcm_grd to find the stale source.  spec-2.41 D5.")));
		else
			ereport(WARNING, (errmsg("cluster_gcs_block: lost write detected on tag "
									 "spc=%u db=%u relNumber=%u block=%u (action=warn)",
									 tag.spcOid, tag.dbOid, (unsigned int)BufTagGetRelNumber(&tag),
									 (unsigned int)tag.blockNum)));
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

	if (status == GCS_BLOCK_REPLY_GRANTED && block_data != NULL) {
		ClusterICSge sge[2];

		memset(sge, 0, sizeof(sge));
		hdr.checksum = gcs_block_compute_checksum(block_data);
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
		rc = cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node, buf, total);
		pfree(buf);
	}

	if (rc == CLUSTER_IC_SEND_DONE && ClusterGcsBlock != NULL)
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
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
static void
gcs_block_resend_cached_reply(int32 dest_node, const GcsBlockDedupEntry *entry)
{
	uint32 header_len;
	uint32 total;
	char *buf;
	GcsBlockReplyHeader *hdr;
	char *block_data;
	bool has_block_payload;

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
			if (XLogRecPtrIsInvalid(entry->sf_dep_vec.required[i]))
				continue;
			hdrv2->sf_dep[n].origin_node = i;
			hdrv2->sf_dep[n].required_redo_lsn = (uint64)entry->sf_dep_vec.required[i];
			n++;
		}
		hdrv2->sf_dep_count = (uint8)n;
	}
	block_data = buf + header_len;
	has_block_payload = entry->status == GCS_BLOCK_REPLY_GRANTED
						|| entry->status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	if (has_block_payload)
		memcpy(block_data, entry->block_data, GCS_BLOCK_DATA_SIZE);
	/* else: block_data already zeroed by palloc0 */

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node, buf, total);
	pfree(buf);
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
		if (!cluster_pcm_lock_apply_gcs_transition(req->tag, (PcmLockTransition)req->transition_id,
												   req->sender_node)) {
			*out_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
			return true;
		}
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
		*out_status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
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
	if (!gcs_block_get_ship_image(req->tag, req->sender_node, out_page_lsn, block_buf,
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
	char block_buf[GCS_BLOCK_DATA_SIZE];
	GcsBlockReplyStatus status;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	const char *block_payload = NULL;
	uint32 block_payload_lkey = 0;
	ClusterICSgeReleaseCallback block_payload_release_cb = NULL;
	void *block_payload_release_arg = NULL;
	ClusterSfDepVec sf_dep_vec;
	bool sf_dep_valid = false;

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

	/* HC75 range guard — out of range never enters dedup HTAB. */
	if (req->transition_id < PCM_TRANS_N_TO_S || req->transition_id > PCM_TRANS_S_TO_X_CLEANOUT) {
		gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	/* PGRAC: spec-2.34 D5 — dedup lookup_or_register (HC90 + HC91 + HC92). */
	memset(&key, 0, sizeof(key));
	key.origin_node_id = (uint32)req->sender_node;
	key.requester_backend_id = req->requester_backend_id;
	key.request_id = req->request_id;
	key.cluster_epoch = req->epoch;
	memset(&cached_entry, 0, sizeof(cached_entry));

	dr = cluster_gcs_block_dedup_lookup_or_register(&key, req->tag, req->transition_id,
													&cached_entry);
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

			memset(&fwd, 0, sizeof(fwd));
			fwd.request_id = req->request_id;
			fwd.epoch = cluster_epoch_get_current();
			fwd.tag = req->tag;
			fwd.original_requester_node = req->sender_node;
			fwd.requester_backend_id = req->requester_backend_id;
			fwd.master_node = cluster_node_id;
			fwd.transition_id = req->transition_id;
			/* PGRAC: spec-2.37 D3 HC127 (spec-2.41 SCN migration) — stamp
			 * expected pi_watermark_scn so the holder can validate the copied
			 * page's pd_block_scn before ship. */
			GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
				&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));

			if (cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, holder_node, &fwd,
										 sizeof(fwd))
				== CLUSTER_IC_SEND_DONE) {
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->forward_replay_count, 1);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);
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
		 * reader starvation backoff + 53R92 budget exhaustion. */
		CLUSTER_INJECTION_POINT("cluster-gcs-block-starvation-force-denied");
		if (cluster_injection_should_skip("cluster-gcs-block-starvation-force-denied")) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_PENDING_X,
								 InvalidXLogRecPtr, NULL);
			return;
		}

		pending_x = cluster_pcm_lock_query_pending_x_requester(req->tag);
		if (pending_x >= 0 && pending_x != req->sender_node) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->starvation_denied_pending_x_count, 1);
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_PENDING_X,
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
			if (gcs_block_get_ship_image(req->tag, req->sender_node, &page_lsn, block_buf,
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
				 */
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

					/*
					 * The image is already captured (copy_block_for_gcs succeeded
					 * above) BEFORE this drop.  The drop's bool is intentionally
					 * ignored: it returns false ONLY when the buffer is not validly
					 * resident (BufTable miss / tag-mismatch / !BM_VALID), i.e.
					 * there is no local copy left to stale-flush — exactly the safe
					 * precondition for granting.  (A resident-but-pinned buffer does
					 * not return false; it makes InvalidateBuffer wait — tracked
					 * separately as the LMON pin-wait follow-up.)  So in every case
					 * reachable here there is no stale copy when we hand X to the
					 * requester.  Rule 8.A holds.
					 */
					(void)cluster_bufmgr_drop_block_for_gcs_no_wire(req->tag, &drop_lsn);
					/* spec-2.41 D2 — advance detector SCN watermark from the
					 * shipped page's pd_block_scn (local-page source = block_buf). */
					cluster_pcm_lock_master_grant_x_to(
						req->tag, req->sender_node, page_lsn,
						(SCN)((PageHeader)block_payload)->pd_block_scn);
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
		 * holder.  Taking the block from a live X HOLDER remains wave-g
		 * territory and stays denied.
		 */
		if (cluster_pcm_lock_query(req->tag) == PCM_LOCK_MODE_X
			&& cluster_pcm_master_other_live_holder_exists(req->tag, req->sender_node)) {
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
			gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
								 InvalidXLogRecPtr, NULL);
			return;
		}

		current_lsn = (uint64)GetXLogInsertRecPtr();
		cluster_pcm_lock_set_pending_x(req->tag, req->sender_node, current_lsn);

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
				cluster_pcm_lock_clear_pending_x(req->tag);
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
					cluster_pcm_lock_clear_pending_x(req->tag);
					gcs_block_send_reply(req->sender_node, req, GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
										 InvalidXLogRecPtr, NULL);
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
				/* PGRAC: spec-2.37 D3 HC127 (spec-2.41 SCN migration) — stamp
				 * expected pi_watermark_scn. */
				GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
					&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));

				if (cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, x_holder, &fwd,
											 sizeof(fwd))
					== CLUSTER_IC_SEND_DONE) {
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
					cluster_gcs_block_dedup_install_reply(
						&key, GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, &fwd_hdr, NULL);
					return;
				}
				cluster_pcm_lock_clear_pending_x(req->tag);
				status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
				page_lsn = InvalidXLogRecPtr;
				block_payload = NULL;
				goto build_and_send_reply;
			}
			cluster_pcm_lock_clear_pending_x(req->tag);
			status = GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
			page_lsn = InvalidXLogRecPtr;
			block_payload = NULL;
			goto build_and_send_reply;
		} else if (pre_state == PCM_LOCK_MODE_S) {
			uint32 holders_bm = cluster_pcm_lock_query_s_holders_bitmap(req->tag);
			bool requester_is_s_holder = (req->sender_node >= 0 && req->sender_node < 32
										  && (holders_bm & ((uint32)1u << req->sender_node)) != 0);

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

				(void)cluster_bufmgr_invalidate_block_for_gcs(req->tag, PCM_LOCK_MODE_S, &self_lsn,
															  &self_scn);
				(void)cluster_pcm_lock_apply_gcs_transition(req->tag, PCM_TRANS_S_TO_N_INVALIDATE,
															cluster_node_id);
				holders_bm &= ~((uint32)1u << cluster_node_id);
			}

			if (holders_bm != 0) {
				if (!gcs_block_broadcast_invalidate_and_wait(req, holders_bm)) {
					/* Budget exhausted — reply DENIED_INVALIDATE_TIMEOUT;
					 * sender backend maps to 53R91. */
					cluster_pcm_lock_clear_pending_x(req->tag);
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_timeout_count, 1);
					gcs_block_send_reply(req->sender_node, req,
										 GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT,
										 InvalidXLogRecPtr, NULL);
					return;
				}
				/* All acks collected; fall through to direct grant. */

				/*
				 * PGRAC: spec-6.12a ㉕ — clear the invalidated holders' bits on
				 * the authoritative entry (mirror of the LOCAL-master upgrade,
				 * cluster_gcs_block_local_x_upgrade).  The invalidate ACK
				 * handler only collects the acked bitmap; without this clear
				 * the sole-holder legality inside the S→X_UPGRADE apply below
				 * still sees the dropped holders' bits and fails, sending the
				 * requester a spurious DENIED_MASTER_NOT_HOLDER.  pending_x is
				 * set, so no NEW S grant can slip in between the ack collection
				 * and this clear (HC117).
				 */
				for (int inv_n = 0; inv_n < 32; inv_n++) {
					if ((holders_bm & ((uint32)1u << inv_n)) != 0)
						(void)cluster_pcm_lock_apply_gcs_transition(
							req->tag, PCM_TRANS_S_TO_N_INVALIDATE, inv_n);
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
			 * HOLDER and the sender retransmit-loops to 53R90.  Cross-node X
			 * contention (requester was NOT an S holder) falls to the flow below.
			 */
			if (requester_is_s_holder
				&& cluster_pcm_lock_apply_gcs_transition(req->tag, PCM_TRANS_S_TO_X_UPGRADE,
														 req->sender_node)) {
				cluster_pcm_lock_clear_pending_x(req->tag);
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
				status = GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK;
				page_lsn = InvalidXLogRecPtr;
				block_payload = NULL;
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
				if (gcs_block_get_ship_image(req->tag, req->sender_node, &page_lsn, block_buf,
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

				memset(&fwd, 0, sizeof(fwd));
				fwd.request_id = req->request_id;
				fwd.epoch = current_epoch;
				fwd.tag = req->tag;
				fwd.original_requester_node = req->sender_node;
				fwd.requester_backend_id = req->requester_backend_id;
				fwd.master_node = cluster_node_id;
				fwd.transition_id = req->transition_id;
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

				if (cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, holder_node, &fwd,
											 sizeof(fwd))
					== CLUSTER_IC_SEND_DONE) {
					GcsBlockReplyHeader fwd_hdr;

					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_forward_sent_count, 1);

					memset(&fwd_hdr, 0, sizeof(fwd_hdr));
					fwd_hdr.request_id = req->request_id;
					fwd_hdr.requester_backend_id = req->requester_backend_id;
					fwd_hdr.transition_id = req->transition_id;
					fwd_hdr.sender_node = holder_node;
					fwd_hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
					GcsBlockReplyHeaderSetForwardingMasterNode(&fwd_hdr, cluster_node_id);
					cluster_gcs_block_dedup_install_reply(
						&key, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &fwd_hdr, NULL);
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

			/* Build and send GCS_BLOCK_FORWARD to holder. */
			memset(&fwd, 0, sizeof(fwd));
			fwd.request_id = req->request_id;
			fwd.epoch = current_epoch;
			fwd.tag = req->tag;
			fwd.original_requester_node = req->sender_node;
			fwd.requester_backend_id = req->requester_backend_id;
			fwd.master_node = cluster_node_id;
			fwd.transition_id = req->transition_id;
			/* PGRAC: spec-2.37 D3 HC127 (spec-2.41 SCN migration) — stamp
			 * expected pi_watermark_scn so the holder can validate the copied
			 * page's pd_block_scn before ship. */
			GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
				&fwd, cluster_pcm_lock_pi_watermark_scn_query(req->tag));

			if (cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, holder_node, &fwd,
										 sizeof(fwd))
				== CLUSTER_IC_SEND_DONE) {
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
					cluster_gcs_block_dedup_install_reply(&key, GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER,
														  &fwd_hdr, NULL);
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
		cluster_pcm_lock_clear_pending_x(req->tag);

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

		verdict = gcs_block_lost_write_verdict(expected_scn, shipped_scn);
		if (verdict == GCS_LOST_WRITE_SKIP) {
			/* spec-2.41 D7 observability — block not SCN-tracked (no fire). */
			if (ClusterGcsBlock != NULL)
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->lost_write_not_scn_tracked_skip_count, 1);
		} else if (verdict == GCS_LOST_WRITE_FAIL_STALE || verdict == GCS_LOST_WRITE_FAIL_ANOMALY) {
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

	cluster_gcs_block_dedup_install_reply_ex(&key, status, hdr,
											 has_block_payload ? block_payload : NULL,
											 send_sf_dep ? &sf_dep_vec : NULL, send_sf_dep);

	/*
		 * spec-2.34 D17 — drop-reply injection.  When active with SKIP,
		 * master DOES NOT send the reply envelope (sender experiences
		 * timeout → retransmit).  The dedup entry was installed above so
		 * a duplicate retry from the sender will hit CACHED_REPLY and
		 * the cached reply WILL be re-sent (unless the inject is still
		 * active on that retry).  Useful for driving the
		 * retransmit_send_count + dedup_hit_count TAP surfaces.
		 */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-drop-reply-before-send");
	if (cluster_injection_should_skip("cluster-gcs-block-drop-reply-before-send")) {
		gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
		pfree(buf);
		return;
	}

	{
		ClusterICSendResult send_rc;

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

		if (send_rc == CLUSTER_IC_SEND_DONE && ClusterGcsBlock != NULL)
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
	}

	gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
	block_payload_release_cb = NULL;
	block_payload_release_arg = NULL;
	pfree(buf);
}
}


/* ============================================================
 * Receiver: sender-side (D6).
 *
 *	HC80 compound key (requester_backend_id, request_id) so this handler
 *	does NOT scan all backends to find the matching outstanding slot — it
 *	indexes directly via requester_backend_id.
 * ============================================================ */

static bool
gcs_block_decode_reply_payload(const ClusterICEnvelope *env, const void *payload,
							   const GcsBlockReplyHeader **out_hdr, const char **out_block_data,
							   bool *out_sf_dep_valid, uint8 *out_sf_flags,
							   ClusterSfDepVec *out_sf_dep_vec)
{
	uint32 v1_size = (uint32)(sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	uint32 v2_size = (uint32)(sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE);

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

	if (env == NULL || payload == NULL)
		return false;

	if (env->payload_length == v1_size) {
		if (out_hdr != NULL)
			*out_hdr = (const GcsBlockReplyHeader *)payload;
		if (out_block_data != NULL)
			*out_block_data = ((const char *)payload) + sizeof(GcsBlockReplyHeader);
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
	int backend_idx;
	ClusterGcsBlockBackendBlock *blk;
	int i;

	cluster_sf_dep_vec_reset(&sf_dep_vec);
	if (!gcs_block_decode_reply_payload(env, payload, &hdr, &block_data, &sf_dep_valid, &sf_flags,
										&sf_dep_vec))
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
			fwd->tag, fwd->original_requester_node, &page_lsn, block_buf, &block_payload,
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
						 * The drop's bool is intentionally ignored: false = buffer
						 * not validly resident (no stale copy); the image was
						 * already shipped into the reply above (Rule 8.A; same
						 * reasoning as the path-B drop site).
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
					 * clean transfer increments clean_page_xfer_count too. */
					(void)cluster_bufmgr_drop_block_for_gcs_no_wire(fwd->tag, &drop_lsn);
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_x_transfer_ship_count, 1);
					if (GcsBlockForwardPayloadIsCleanEligible(fwd))
						pg_atomic_fetch_add_u64(&ClusterGcsBlock->clean_page_xfer_count, 1);
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

		memset(sge, 0, sizeof(sge));
		sge[0].addr = hdr;
		sge[0].len = header_len;
		sge[1].addr = (void *)block_payload;
		sge[1].len = GCS_BLOCK_DATA_SIZE;
		sge[1].lkey = block_payload_lkey;
		sge[1].release_cb = block_payload_release_cb;
		sge[1].release_arg = block_payload_release_arg;
		(void)cluster_ic_rdma_send_envelope_sge(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, fwd->original_requester_node, sge, lengthof(sge), total);
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
	} else {
		gcs_block_release_ship_image(block_payload_release_cb, block_payload_release_arg);
		block_payload_release_cb = NULL;
		block_payload_release_arg = NULL;
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, fwd->original_requester_node,
									   buf, total);
	}
	/* PGRAC: spec-5.59 D3 — close the holder-forward read-image ship scope
	 * (no-op unless the read-image branch above started it). */
	cluster_xp_end(&xp_fwd_ship);

	pfree(buf);
}


/* ============================================================
 * Dispatch table registration.
 * ============================================================ */

static const ClusterICMsgTypeInfo gcs_block_request_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
	.name = "gcs_block_request",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_request_envelope,
};

static const ClusterICMsgTypeInfo gcs_block_reply_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY,
	.name = "gcs_block_reply",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_reply_envelope,
};

/* PGRAC: spec-2.35 D8 — holder-side forward handler msg_type registration. */
static const ClusterICMsgTypeInfo gcs_block_forward_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD,
	.name = "gcs_block_forward",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_forward_envelope,
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
static bool
gcs_block_broadcast_invalidate_and_wait_ext(const GcsBlockRequestPayload *req, uint32 holders_bm,
											bool via_outbound_ring)
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

	if (ClusterGcsBlock == NULL)
		return false;

	cluster_xp_begin(&xp_inv, CLXP_W_GCS_X_INVALIDATE);
	current_epoch = cluster_epoch_get_current();

	/* Claim and stamp the broadcast slot as one critical section.  ACK
	 * validation reads the same identity under this lock, so a late ACK from
	 * an older broadcast cannot match a newly claimed slot by request_id alone. */
	LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u64(&ClusterGcsBlock->invalidate_broadcast_request_id) != 0) {
		LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);
		cluster_xp_abort(&xp_inv); /* PGRAC: spec-5.59 — slot busy, no sample */
		return false;
	}
	ClusterGcsBlock->invalidate_broadcast_epoch = current_epoch;
	ClusterGcsBlock->invalidate_broadcast_tag = req->tag;
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, holders_bm);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
	pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, req->request_id);
	LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);

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
		 * through the backend outbound ring instead (LMON flushes it). */
		if (via_outbound_ring)
			(void)cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE,
														   (uint32)n, &inv, sizeof(inv));
		else
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, n, &inv, sizeof(inv));
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_broadcast_count, 1);
	}

	/* Poll-with-CV wait for full ack collection or timeout. */
	start_lsn = (long)GetCurrentTimestamp();
	ConditionVariablePrepareToSleep(&ClusterGcsBlock->invalidate_broadcast_cv);
	for (;;) {
		acked_bm = pg_atomic_read_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm);
		if ((acked_bm & holders_bm) == holders_bm) {
			full_ack = true;
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

	/* Release the slot. */
	LWLockAcquire(&ClusterGcsBlock->invalidate_broadcast_lock.lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&ClusterGcsBlock->invalidate_broadcast_request_id, 0);
	ClusterGcsBlock->invalidate_broadcast_epoch = 0;
	memset(&ClusterGcsBlock->invalidate_broadcast_tag, 0,
		   sizeof(ClusterGcsBlock->invalidate_broadcast_tag));
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_expected_bm, 0);
	pg_atomic_write_u32(&ClusterGcsBlock->invalidate_broadcast_acked_bm, 0);
	LWLockRelease(&ClusterGcsBlock->invalidate_broadcast_lock.lock);

	cluster_xp_end(&xp_inv); /* PGRAC: spec-5.59 D2 */
	return full_ack;
}

/* Original spec-2.36 entry point (LMON master-handler context). */
static bool
gcs_block_broadcast_invalidate_and_wait(const GcsBlockRequestPayload *req, uint32 holders_bm)
{
	return gcs_block_broadcast_invalidate_and_wait_ext(req, holders_bm, false);
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
cluster_gcs_block_local_x_upgrade(BufferTag tag)
{
	GcsBlockRequestPayload synth;
	uint32 holders_bm;
	uint32 self_bit;
	int n;
	bool upgraded = false;

	if (ClusterGcsBlock == NULL || cluster_node_id < 0 || cluster_node_id >= 32)
		return false;
	self_bit = (uint32)1u << cluster_node_id;

	cluster_pcm_lock_set_pending_x(tag, cluster_node_id, (uint64)GetXLogInsertRecPtr());

	holders_bm = cluster_pcm_lock_query_s_holders_bitmap(tag) & ~self_bit;
	if (holders_bm != 0) {
		memset(&synth, 0, sizeof(synth));
		synth.request_id
			= pg_atomic_fetch_add_u64(&ClusterGcsBlock->local_upgrade_request_seq, 1) + 1;
		synth.epoch = cluster_epoch_get_current();
		synth.tag = tag;
		synth.sender_node = cluster_node_id;

		if (!gcs_block_broadcast_invalidate_and_wait_ext(&synth, holders_bm, true)) {
			cluster_pcm_lock_clear_pending_x(tag);
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_invalidate_timeout_count, 1);
			return false;
		}

		/* Acks certify the drops; clear the acked bits on the
		 * authoritative entry (idempotent vs racing releases). */
		for (n = 0; n < 32; n++) {
			if ((holders_bm & ((uint32)1u << n)) == 0)
				continue;
			(void)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_INVALIDATE, n);
		}
	}

	upgraded
		= cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_X_UPGRADE, cluster_node_id);
	cluster_pcm_lock_clear_pending_x(tag);
	return upgraded;
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
 * ============================================================ */
extern bool cluster_bufmgr_invalidate_block_for_gcs(BufferTag tag, PcmLockMode expected_mode,
													XLogRecPtr *out_page_lsn, SCN *out_page_scn);

static void
cluster_gcs_handle_block_invalidate_envelope(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockInvalidatePayload *inv = (const GcsBlockInvalidatePayload *)payload;
	GcsBlockInvalidateAckPayload ack;
	PcmLockMode pre_state;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	SCN page_scn = InvalidScn; /* spec-2.41 D3 — ACK SCN carrier */
	uint8 ack_status = 0;	   /* OK */
	uint64 current_epoch;

	/* D16 inject — stall ack for timeout testing. */
	CLUSTER_INJECTION_POINT("cluster-gcs-block-invalidate-stall-ack");
	if (cluster_injection_should_skip("cluster-gcs-block-invalidate-stall-ack"))
		return; /* never ack — master sees timeout */

	if (inv->checksum != gcs_block_compute_invalidate_checksum(inv))
		return;

	current_epoch = cluster_epoch_get_current();
	if (inv->epoch < current_epoch) {
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
	if (cluster_bufmgr_invalidate_block_for_gcs(inv->tag, pre_state, &page_lsn, &page_scn)) {
		PcmLockTransition trans = (pre_state == PCM_LOCK_MODE_X) ? PCM_TRANS_X_TO_N_DOWNGRADE
																 : PCM_TRANS_S_TO_N_INVALIDATE;
		(void)cluster_pcm_lock_apply_gcs_transition(inv->tag, trans, cluster_node_id);

		/* spec-2.41 D3: the dropping page's pd_block_scn is carried back in the
		 * ACK (replacing the spec-2.37 page_lsn carrier) and applied to the
		 * master's detector SCN watermark by the ACK handler.  Do not advance
		 * the holder-local HTAB: the master GrdEntry is the authoritative owner. */
	} else {
		ack_status = 2; /* race: not resident */
	}

send_ack:
	memset(&ack, 0, sizeof(ack));
	ack.request_id = inv->request_id;
	ack.epoch = inv->epoch;
	ack.tag = inv->tag;
	ack.sender_node = cluster_node_id;
	ack.ack_status = ack_status;
	GcsBlockInvalidateAckPayloadSetPageScn(&ack, page_scn); /* spec-2.41 D3 — SCN carrier @52 */
	ack.checksum = gcs_block_compute_invalidate_ack_checksum(&ack);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, inv->master_node, &ack,
								   sizeof(ack));
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

	if (ClusterGcsBlock == NULL)
		return;

	if (ack->checksum != gcs_block_compute_invalidate_ack_checksum(ack)) {
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->stale_reply_drop_count, 1);
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
	if (ack->epoch != expected_epoch
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
			cluster_pcm_lock_pi_watermark_scn_advance(ack_tag, ack_page_scn);
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->pi_watermark_advance_count, 1);
		}
		ConditionVariableBroadcast(&ClusterGcsBlock->invalidate_broadcast_cv);
	}
}


static const ClusterICMsgTypeInfo gcs_block_invalidate_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE,
	.name = "gcs_block_invalidate",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_invalidate_envelope,
};

static const ClusterICMsgTypeInfo gcs_block_invalidate_ack_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK,
	.name = "gcs_block_invalidate_ack",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_invalidate_ack_envelope,
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

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REDECLARE, master_node, &p, sizeof(p));
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


static const ClusterICMsgTypeInfo gcs_block_redeclare_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REDECLARE,
	.name = "gcs_block_redeclare",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BUFFER_CLIENTS | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_redeclare_envelope,
};


void
cluster_gcs_register_block_msg_types(void)
{
	cluster_ic_register_msg_type(&gcs_block_request_info);
	cluster_ic_register_msg_type(&gcs_block_reply_info);
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

			if (slot->in_use && slot->request_epoch != 0 && slot->request_epoch < new_epoch
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
