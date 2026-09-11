/*-------------------------------------------------------------------------
 *
 * cluster_cr_server.c
 *	  pgrac spec-6.12b / spec-7.3 D6 — cross-instance CR-server runtime.
 *
 *	  spec-6.12b split the origin serve across LMON (validate + park in a
 *	  shmem slot) → LMS (drain + construct) → LMON (tick-ship), because the
 *	  IC dispatch loop could not walk undo I/O, the 72-byte outbound ring
 *	  could not carry a page, and only LMON owned the IC connections.
 *
 *	  spec-7.3 D6: when the GCS block family is on the DATA plane (the LMS
 *	  worker pool owns the DATA connections + a page-carrying outbound ring),
 *	  the worker[shard] that receives a GCS_BLOCK_FORWARD serves the request
 *	  INLINE (cluster_gcs_block_forward_serve_inline) — no park → poll → ship
 *	  indirection, no worker-0 handoff, no 100 ms idle latency; a slow
 *	  construction only stalls that worker's shard (1/N).  The park-serve
 *	  path is RETAINED for the CONTROL-plane fallback: a node whose data
 *	  plane is off (no data_addr) still dispatches the FORWARD in LMON, and
 *	  LMON must not walk undo I/O in its tight IC loop — so it parks, LMS
 *	  worker 0 drains, and LMON ships (the light-work rule, unchanged).
 *
 *	  Both paths share cr_serve_slot() (the PG_TRY → DENIED serve envelope)
 *	  and cr_build_and_send_reply() (the reply build).  8.A envelope: CR
 *	  construction (cluster_cr.c) re-throws on any uncertainty; the wrapper
 *	  converts it to a fail-closed DENIED, never a worker/LMS exit or a
 *	  wrong-order construction.
 *
 *	  The slot state word is atomic.  Every reservation claim additionally
 *	  uses the existing ClusterLmsSharedState lwlock so legacy and R4
 *	  claimants share one proof window without adding a CR-server lock.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_server.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b/i)
 *	  Spec: spec-7.3-lms-worker-pool.md (D6 — inline serve on the DATA plane;
 *	  D7 — fence ×N: the inline serve refuses to ship on a write-fenced node)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h"     /* C0 literal raw CLOG status */
#include "access/multixact.h" /* GetMultiXactIdMembers / MultiXactMember (spec-7.1 D3-b) */
#include "access/transam.h"	  /* TransactionIdDidCommit/DidAbort (D-i4 CLOG cross) */
#include "access/xlog.h"	  /* GetFlushRecPtr (spec-6.12i live_hwm_lsn) */
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_elog.h" /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block_dedup.h" /* PGRAC: spec-7.3 P2-1 — note_misroute */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope */
#include "cluster/cluster_ic_tier1.h"  /* PGRAC: spec-7.3 P2-1 — my DATA channel */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h"		 /* PGRAC: spec-7.2 D1 READY-publish wakeup */
#include "cluster/cluster_lms.h"		 /* PGRAC: spec-7.3 D8 per-worker serve counters */
#include "cluster/cluster_lms_shard.h"	 /* PGRAC: spec-7.3 P2-1 — tag->worker shard */
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_mxid_stripe.h" /* cluster_mxid_is_mine (spec-7.1 D3-b) */
#include "cluster/cluster_scn.h" /* cluster_scn_current (spec-7.1a authority_scn co-sample) */
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_conf.h"			 /* CLUSTER_MAX_NODES (D4-6 owner range) */
#include "cluster/cluster_tt_durable.h"		 /* resolve_by_xid (D-i4 complete scan) */
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_slot.h"		 /* max_recycle_horizon (D-i4 bound) */
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_authority.h"	 /* authority lookup + block0 prove (D4-6) */
#include "cluster/cluster_undo_record_api.h" /* tt_retention_rollover_count */
#include "cluster/cluster_undo_smgr.h"		 /* cluster_undo_smgr_read_block */
#include "cluster/cluster_write_fence.h"	 /* PGRAC: spec-7.3 D7 fence ×N gate */
#include "cluster/cluster_xid_stripe.h"		 /* cluster_xid_is_mine (spec-6.15 D4) */
#include "cluster/storage/cluster_undo_block0_current.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/lwlock.h" /* C0 XactTruncationLock arbitrary-xid gate */
#include "storage/proc.h"
#include "storage/procarray.h" /* TT-P013: origin-own exact live proof */
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h" /* PGRAC: spec-7.3 D8 serve duration (GetCurrentTimestamp) */

/*
 * Shmem: the slot table + the published LMS latch pointer (set by LmsMain
 * at entry so the LMON submit path can cut the 100 ms idle-poll latency;
 * a stale pointer after an LMS crash only risks a spurious latch set on a
 * reused PGPROC — benign).  Used by the CONTROL-plane park-serve path only
 * (spec-7.3 D6: the DATA plane serves inline and never parks).
 */
typedef struct ClusterCrServerShared {
	pg_atomic_uint64 lms_latch_ptr; /* (uintptr_t) Latch*; 0 = not running */
	ClusterLmsCrSlot slots[CLUSTER_LMS_CR_SLOTS];
} ClusterCrServerShared;

typedef enum ClusterR4ForeignScurPhase {
	CLUSTER_R4_FOREIGN_SCUR_UNUSED = 0,
	CLUSTER_R4_FOREIGN_SCUR_ACQUIRE = 1,
	CLUSTER_R4_FOREIGN_SCUR_RELEASE = 2
} ClusterR4ForeignScurPhase;

typedef enum ClusterR4ForeignSampleStep {
	CLUSTER_R4_FOREIGN_SAMPLE_PENDING = 0,
	CLUSTER_R4_FOREIGN_SAMPLE_READY = 1,
	CLUSTER_R4_FOREIGN_SAMPLE_FAILED = 2
} ClusterR4ForeignSampleStep;

/* One process-local worker-0 episode per physical R4 slot. */
typedef struct ClusterR4CrWorkerContext {
	bool in_use;
	bool builder_forgotten;
	uint64 slot_generation;
	uint64 builder_incarnation;
	int32 requester_node;
	int32 requester_backend_id;
	uint64 request_id;
	ClusterSemanticAdmissionToken admission;
	uint32 expected_foreign_physical_generation;
	ClusterUndoBlock0CurrentGuard foreign_scur;
	ClusterUndoBlock0ResolvedRoot foreign_resolved_root;
	ClusterUndoBlock0Generation foreign_sampled_generation;
	uint8 foreign_scur_phase;
	bool foreign_physical_generation_frozen;
	uint8 reserved[2];
} ClusterR4CrWorkerContext;

#define CR_SERVER_R4_REQUIRED_HELLO_CAPS                                               \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1       \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                                   \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)

/* A local R4 identity has no self HELLO record.  The compiled protocol
 * family is fixed by this binary; its frozen uint32 carrier must be the exact
 * checked view of the same entered local TARGET OPEN generation. */
static bool
cr_server_r4_local_open_generation_matches(
	const ClusterSemanticAdmissionToken *admission, uint32 stored_generation)
{
	const uint32 compiled_capabilities
		= PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
		  | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
		  | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
		  | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1;

	return admission != NULL && admission->entered
		   && admission->feature_bit == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		   && admission->side == CLUSTER_SEMANTIC_TARGET_SIDE
		   && admission->record_generation > 0
		   && admission->record_generation <= (uint64)PG_UINT32_MAX
		   && (compiled_capabilities & CR_SERVER_R4_REQUIRED_HELLO_CAPS)
				  == CR_SERVER_R4_REQUIRED_HELLO_CAPS
		   && (uint64)stored_generation == admission->record_generation;
}

static bool
cr_server_r4_identity_open_matches(
	const ClusterSemanticAdmissionToken *admission, int32 identity_node,
	uint32 stored_generation)
{
	if (identity_node == cluster_node_id)
		return cr_server_r4_local_open_generation_matches(
			admission, stored_generation);
	return cluster_semantic_activation_peer_open_matches(
		admission, identity_node, CR_SERVER_R4_REQUIRED_HELLO_CAPS,
		stored_generation);
}

const char *
cluster_cr_build_reason_name(ClusterCrBuildReason reason)
{
	switch (reason) {
		case CLUSTER_CR_BUILD_NONE:
			return "none";
		case CLUSTER_CR_BUILD_TARGET_DISABLED:
			return "target_disabled";
		case CLUSTER_CR_BUILD_RF_DEFERRED:
			return "rf_deferred";
		case CLUSTER_CR_BUILD_WRONG_MASTER:
			return "wrong_master";
		case CLUSTER_CR_BUILD_NO_HOLDER:
			return "no_holder";
		case CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS:
			return "holder_ambiguous";
		case CLUSTER_CR_BUILD_HOLDER_MOVED:
			return "holder_moved";
		case CLUSTER_CR_BUILD_RECOVERING:
			return "recovering";
		case CLUSTER_CR_BUILD_GENERATION_MISMATCH:
			return "generation_mismatch";
		case CLUSTER_CR_BUILD_CAPACITY:
			return "capacity";
		case CLUSTER_CR_BUILD_BAD_LOCATOR:
			return "bad_locator";
		case CLUSTER_CR_BUILD_BAD_UNDO:
			return "bad_undo";
		case CLUSTER_CR_BUILD_CHAIN_LIMIT:
			return "chain_limit";
		case CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD:
			return "snapshot_too_old";
		case CLUSTER_CR_BUILD_EPOCH_MISMATCH:
			return "epoch_mismatch";
		case CLUSTER_CR_BUILD_CANCELLED:
			return "cancelled";
		case CLUSTER_CR_BUILD_IO_ERROR:
			return "io_error";
		case CLUSTER_CR_BUILD_PROTOCOL:
			return "protocol";
	}
	return "unknown";
}

ClusterCrBuildResult
cluster_cr_build_on_holder(const BufferTag *tag, SCN read_scn, char dst[BLCKSZ],
							ClusterCrBuildReason *reason_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterBufmgrGcsCopyRefusal refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	ClusterCrBuildReason reason = CLUSTER_CR_BUILD_NONE;
	ClusterCrBuildResult result = CLUSTER_CR_BUILD_FAIL_CLOSED;
	PGAlignedBlock current_copy;
	XLogRecPtr page_lsn = InvalidXLogRecPtr;
	SCN page_scn = InvalidScn;
	bool partial = false;
	bool constructed = false;

	if (dst != NULL)
		memset(dst, 0, BLCKSZ);
	if (reason_out != NULL)
		*reason_out = CLUSTER_CR_BUILD_NONE;
	memset(&admission, 0, sizeof(admission));

	if (dst == NULL || reason_out == NULL)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;

	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		*reason_out = admission_result == CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
						  ? CLUSTER_CR_BUILD_TARGET_DISABLED
						  : CLUSTER_CR_BUILD_RF_DEFERRED;
		return CLUSTER_CR_BUILD_RETRYABLE;
	}

	PG_TRY();
	{
		if (tag == NULL || !SCN_VALID(read_scn)) {
			reason = CLUSTER_CR_BUILD_PROTOCOL;
			goto admitted_done;
		}

		if (!cluster_bufmgr_copy_block_for_r4_cr(*tag, InvalidScn, &page_lsn, &page_scn,
											 current_copy.data, &refusal)) {
			if (refusal == CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INVALID_ARGUMENT) {
				reason = CLUSTER_CR_BUILD_PROTOCOL;
				goto admitted_done;
			}
			switch (refusal) {
			case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT:
			case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID:
			case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST:
			case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND:
			case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_OWNERSHIP_REVOKE_BUSY:
				reason = CLUSTER_CR_BUILD_HOLDER_MOVED;
				result = CLUSTER_CR_BUILD_RETRYABLE;
				goto admitted_done;
			default:
				reason = CLUSTER_CR_BUILD_PROTOCOL;
				goto admitted_done;
			}
		}

		PG_TRY();
		{
			cluster_cr_construct_page_for_server(current_copy.data, read_scn, *tag, dst, &partial);
			constructed = true;
		}
		PG_CATCH();
		{
			constructed = false;
			FlushErrorState();
		}
		PG_END_TRY();

		if (!cluster_semantic_activation_recheck(&admission)) {
			memset(dst, 0, BLCKSZ);
			reason = CLUSTER_CR_BUILD_RF_DEFERRED;
			result = CLUSTER_CR_BUILD_RETRYABLE;
			goto admitted_done;
		}

		if (!constructed || partial) {
			memset(dst, 0, BLCKSZ);
			reason = CLUSTER_CR_BUILD_BAD_UNDO;
			goto admitted_done;
		}

		reason = CLUSTER_CR_BUILD_NONE;
		result = CLUSTER_CR_BUILD_FULL;

admitted_done:
		switch (result) {
		case CLUSTER_CR_BUILD_FULL:
			cluster_r4_observe(CLUSTER_R4_EVENT_CR_HOLDER_FULL,
						   CLUSTER_TX_RESOLVE_NONE, reason);
			break;
		case CLUSTER_CR_BUILD_RETRYABLE:
			cluster_r4_observe(CLUSTER_R4_EVENT_CR_HOLDER_RETRY,
						   CLUSTER_TX_RESOLVE_NONE, reason);
			break;
		case CLUSTER_CR_BUILD_FAIL_CLOSED:
			cluster_r4_observe(CLUSTER_R4_EVENT_CR_HOLDER_FAIL_CLOSED,
						   CLUSTER_TX_RESOLVE_NONE, reason);
			break;
		}
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&admission);
	}
	PG_END_TRY();

	*reason_out = reason;
	return result;
}

static ClusterCrServerShared *CrServerShared = NULL;
static ClusterR4CrWorkerContext CrServerR4Contexts[CLUSTER_LMS_CR_SLOTS];
static uint32 cluster_lms_cr_legacy_drain_cursor;

/*
 * This is deliberately only worker 0's process-local half of the close
 * proof.  Stale nonterminal shared slots belong to LMON's later proved
 * RECLAIMING/FREE edge; worker 0 merely proves that it retains no local
 * context and has finished every terminal/SHIPPING positive edge.
 */
bool
cluster_cr_server_r4_worker0_drained(void)
{
	ClusterR4CrWorkerContext zero;
	int i;

	if (CrServerShared == NULL)
		return false;

	memset(&zero, 0, sizeof(zero));
	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		uint32 state = pg_atomic_read_u32(&CrServerShared->slots[i].state);

		if (memcmp(&CrServerR4Contexts[i], &zero, sizeof(zero)) != 0)
			return false;
		switch (state) {
			case CLUSTER_LMS_CR_R4_READY_FULL:
			case CLUSTER_LMS_CR_R4_READY_RETRY:
			case CLUSTER_LMS_CR_R4_READY_FAIL:
			case CLUSTER_LMS_CR_R4_CANCELLED:
			case CLUSTER_LMS_CR_R4_SHIPPING:
				return false;
			default:
				break;
		}
	}

	return true;
}

static Size
cluster_cr_server_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCrServerShared));
}

static void
cluster_cr_server_shmem_init(void)
{
	bool found;

	CrServerShared
		= ShmemInitStruct("pgrac cluster cr server", cluster_cr_server_shmem_size(), &found);

	if (!found) {
		memset(CrServerShared, 0, sizeof(*CrServerShared));
		pg_atomic_init_u64(&CrServerShared->lms_latch_ptr, 0);
		for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
			pg_atomic_init_u32(&CrServerShared->slots[i].state, CLUSTER_LMS_CR_FREE);
	}
}

static const ClusterShmemRegion cluster_cr_server_region = {
	.name = "pgrac cluster cr server",
	.size_fn = cluster_cr_server_shmem_size,
	.init_fn = cluster_cr_server_shmem_init,
	.lwlock_count = 0, /* atomic slot states; no lock */
	.owner_subsys = "cluster_cr_server",
	.reserved_flags = 0,
};

void
cluster_cr_server_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_server_region);
}

/* LmsMain lifecycle hooks: publish / retract the LMS wake latch. */
void
cluster_cr_server_publish_lms_latch(struct Latch *latch)
{
	if (CrServerShared != NULL)
		pg_atomic_write_u64(&CrServerShared->lms_latch_ptr, (uint64)(uintptr_t)latch);
}

static void
cr_server_wake_lms(void)
{
	uint64 raw;

	if (CrServerShared == NULL)
		return;
	raw = pg_atomic_read_u64(&CrServerShared->lms_latch_ptr);
	if (raw != 0)
		SetLatch((Latch *)(uintptr_t)raw);
}

/*
 * spec-8.4 D4-B — common proof window for every legacy reservation claim.
 * A busy slot belongs to its current writer and must not be canonicalized by
 * a losing legacy claimant.  A FREE winner clears only the complete R4 owner
 * stamp, publishes that zero stamp, and performs its existing state CAS while
 * holding the one shared LMS proof lock.
 */
static bool
cr_server_reserve_legacy_slot(ClusterLmsCrSlot *slot, uint32 reserved_state)
{
	ClusterLmsSharedState *lms_state;
	uint32 expected = CLUSTER_LMS_CR_FREE;
	bool reserved = false;

	if (slot == NULL
		|| (reserved_state != CLUSTER_LMS_CR_PENDING
			&& reserved_state != CLUSTER_LMS_CR_FILLING))
		return false;
	lms_state = cluster_lms_shared_state();
	if (lms_state == NULL)
		return false;

	LWLockAcquire(&lms_state->lwlock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&slot->state) == CLUSTER_LMS_CR_FREE) {
		memset(&slot->r4.owner, 0, sizeof(slot->r4.owner));
		pg_write_barrier();
		reserved = pg_atomic_compare_exchange_u32(&slot->state, &expected, reserved_state);
	}
	LWLockRelease(&lms_state->lwlock);

	return reserved;
}

/*
 * Preserve the per-physical-slot no-reuse generation while making every
 * other unpublished byte canonical.  The caller exclusively owns FILLING;
 * the atomic state word itself is deliberately outside this byte range.
 */
static void
cr_server_r4_canonicalize_unpublished(ClusterLmsCrSlot *slot, uint64 slot_generation)
{
	if (slot == NULL)
		return;

	memset((char *)slot + sizeof(slot->state), 0,
		   sizeof(*slot) - sizeof(slot->state));
	slot->r4.slot_generation = slot_generation;
}

/* The receive DATA worker owns the sole pre-publication FILLING->FREE edge. */
static bool
cr_server_r4_release_unpublished(ClusterLmsCrSlot *slot, uint64 slot_generation)
{
	uint32 expected = CLUSTER_LMS_CR_FILLING;

	if (slot == NULL || pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_FILLING)
		return false;
	cr_server_r4_canonicalize_unpublished(slot, slot_generation);
	pg_write_barrier();
	return pg_atomic_compare_exchange_u32(
		&slot->state, &expected, CLUSTER_LMS_CR_FREE);
}

#ifdef USE_CLUSTER_UNIT
bool
cluster_cr_server_test_reserve_legacy_slot(ClusterLmsCrSlot *slot, uint32 reserved_state)
{
	return cr_server_reserve_legacy_slot(slot, reserved_state);
}
#endif

/*
 * cluster_lms_cr_submit — CONTROL-plane park.  The caller (the GCS_BLOCK_
 * FORWARD handler running in LMON when the family is on the control plane)
 * has already range-checked the transition id and knows the payload carries
 * the CR flag.  false = data plane off / no capacity: the caller replies the
 * fail-closed DENIED immediately (the requester keeps 53R9G — Rule 8.A).
 */
bool
cluster_lms_cr_submit(const GcsBlockForwardPayload *fwd)
{
	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_cr_data_plane)
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];

		if (!cr_server_reserve_legacy_slot(slot, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_CR;

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * cluster_lms_cr_submit_r4 — typed R4 FORWARD96 holder-submit boundary.
 *
 * D3 deliberately does not reinterpret the 96-byte route proof as the
 * legacy 64-byte payload: that would discard the proof.  The receive worker
 * keeps ownership of receive_admission throughout this call; no entered
 * token or semantic debt is copied into shared memory.
 */
ClusterCrBuildResult
cluster_lms_cr_submit_r4(const ClusterR4CrForwardPayload *forward,
						 const ClusterSemanticAdmissionToken *receive_admission,
						 uint32 requester_capability_generation,
						 uint32 master_capability_generation,
						 ClusterCrBuildReason *reason_out)
{
	ClusterLmsSharedState *lms_state;
	ClusterLmsCrSlot *slot = NULL;
	ClusterR4CrRouteProof route_proof;
	ClusterBufmgrGcsCopyRefusal copy_refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	XLogRecPtr copied_page_lsn = InvalidXLogRecPtr;
	SCN copied_page_scn = InvalidScn;
	SCN read_scn;
	uint64 master_authority_generation = 0;
	uint64 master_resource_transition_count = 0;
	uint64 slot_generation = 0;
	uint64 worker_incarnation;
	uint32 expected;
	int worker_id;
	bool saw_exhausted_free = false;
	bool copy_ok = false;
	bool final_match = false;
	volatile bool cleanup_needed = false;
	volatile ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	volatile ClusterCrBuildResult result = CLUSTER_CR_BUILD_FAIL_CLOSED;

	if (reason_out != NULL)
		*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
	if (forward == NULL || receive_admission == NULL || reason_out == NULL
		|| !receive_admission->entered || requester_capability_generation == 0
		|| master_capability_generation == 0)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	if (receive_admission->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| receive_admission->side != (uint8)CLUSTER_SEMANTIC_TARGET_SIDE
		|| receive_admission->record_generation == 0
		|| receive_admission->formation_epoch != forward->base.epoch
		|| forward->base.request_id == 0
		|| forward->base.original_requester_node < 0
		|| forward->base.original_requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| forward->base.requester_backend_id <= 0
		|| forward->base.master_node < 0
		|| forward->base.master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| forward->base.transition_id != (uint8)PCM_TRANS_N_TO_S
		|| !GcsBlockForwardPayloadIsCrRequest(&forward->base)
		|| forward->base.reserved_0[0] != 0 || forward->base.reserved_0[1] != 0
		|| forward->base.reserved_0[2] != 0 || forward->base.reserved_0[3] != 0
		|| forward->base.reserved_0[4] != 1 || forward->base.reserved_0[5] != 0
		|| forward->base.reserved_0[6] != 0)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	if ((forward->base.master_node == cluster_node_id
		 && !cr_server_r4_local_open_generation_matches(
			 receive_admission, master_capability_generation))
		|| (forward->base.original_requester_node == cluster_node_id
			&& !cr_server_r4_local_open_generation_matches(
				receive_admission, requester_capability_generation)))
		return CLUSTER_CR_BUILD_FAIL_CLOSED;

	read_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&forward->base);
	if (!SCN_VALID(read_scn)
		|| !ClusterR4ForwardExtensionGetCrProof(
			&forward->extension, forward->base.epoch, &master_authority_generation,
			&master_resource_transition_count, &copied_page_scn))
		return CLUSTER_CR_BUILD_FAIL_CLOSED;

	memset(&route_proof, 0, sizeof(route_proof));
	route_proof.tag = forward->base.tag;
	route_proof.read_scn = read_scn;
	route_proof.formation_epoch = forward->base.epoch;
	route_proof.activation_generation = receive_admission->record_generation;
	route_proof.master_authority_generation = master_authority_generation;
	route_proof.master_resource_transition_count = master_resource_transition_count;
	route_proof.expected_page_scn = copied_page_scn;
	route_proof.real_master_node = forward->base.master_node;
	route_proof.selected_holder_node = cluster_node_id;

	lms_state = cluster_lms_shared_state();
	worker_id = cluster_ic_tier1_my_data_channel();
	if (CrServerShared == NULL || lms_state == NULL || worker_id < 0
		|| worker_id >= CLUSTER_LMS_MAX_WORKERS || cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;

	/*
	 * The common LMS lock is only the reservation proof window.  A complete
	 * producer stamp and the consumed generation precede FREE->FILLING; no
	 * buffer, semantic or transport work occurs while it is held.
	 */
	LWLockAcquire(&lms_state->lwlock, LW_EXCLUSIVE);
	worker_incarnation = lms_state->r4_controls.data_worker_incarnation[worker_id];
	if (worker_incarnation != 0) {
		for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
			ClusterLmsCrSlot *candidate = &CrServerShared->slots[i];
			uint64 previous_generation;

			if (pg_atomic_read_u32(&candidate->state) != CLUSTER_LMS_CR_FREE)
				continue;
			previous_generation = candidate->r4.slot_generation;
			if (previous_generation >= (UINT64_MAX >> 2)) {
				saw_exhausted_free = true;
				continue;
			}

			slot_generation = previous_generation + 1;
			cr_server_r4_canonicalize_unpublished(candidate, slot_generation);
			candidate->r4.owner.edge_owner_incarnation = worker_incarnation;
			candidate->r4.owner.edge_owner_pid = MyProcPid;
			candidate->r4.owner.edge_owner_worker_id = (uint8)worker_id;
			candidate->r4.owner.edge_owner_role
				= (uint8)(worker_id == 0 ? B_LMS : B_LMS_WORKER);
			pg_write_barrier();
			expected = CLUSTER_LMS_CR_FREE;
			if (pg_atomic_compare_exchange_u32(
					&candidate->state, &expected, CLUSTER_LMS_CR_FILLING)) {
				slot = candidate;
				break;
			}
			/* Every legitimate claimant holds this lock; a loss is structural. */
			saw_exhausted_free = true;
			break;
		}
	}
	LWLockRelease(&lms_state->lwlock);

	if (slot == NULL) {
		if (saw_exhausted_free || worker_incarnation == 0) {
			*reason_out = CLUSTER_CR_BUILD_PROTOCOL;
			return CLUSTER_CR_BUILD_FAIL_CLOSED;
		}
		*reason_out = CLUSTER_CR_BUILD_CAPACITY;
		return CLUSTER_CR_BUILD_RETRYABLE;
	}
	cleanup_needed = true;

	/* Fill only immutable work identity while this producer owns FILLING. */
	slot->tag = forward->base.tag;
	slot->read_scn = read_scn;
	slot->request_id = forward->base.request_id;
	slot->epoch = forward->base.epoch;
	slot->requester_node = forward->base.original_requester_node;
	slot->requester_backend = forward->base.requester_backend_id;
	slot->reply_master_node = forward->base.master_node;
	slot->transition_id = forward->base.transition_id;
	slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD;
	slot->r4.route_proof = route_proof;
	slot->r4.requester_capability_generation = requester_capability_generation;
	slot->r4.master_capability_generation = master_capability_generation;

	PG_TRY();
	{
		copy_ok = cluster_bufmgr_copy_block_for_r4_cr(
			forward->base.tag, route_proof.expected_page_scn, &copied_page_lsn,
			&copied_page_scn, slot->result_page, &copy_refusal);
		if (!copy_ok) {
			switch (copy_refusal) {
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_OWNERSHIP_REVOKE_BUSY:
					reason = CLUSTER_CR_BUILD_HOLDER_MOVED;
					break;
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INVALID_ARGUMENT:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_HC89_LSN_DRIFT:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_SMART_FUSION_UNCLASSIFIED:
				case CLUSTER_BUFMGR_GCS_COPY_REFUSAL_INJECTED_EVICT:
				default:
					reason = CLUSTER_CR_BUILD_PROTOCOL;
					break;
			}
			result = cluster_cr_build_result_for_reason(reason);
		}
		else {
			slot->r4.copied_page_lsn = copied_page_lsn;
			slot->r4.copied_page_scn = copied_page_scn;
			final_match = cluster_semantic_activation_recheck(receive_admission)
						  && cr_server_r4_identity_open_matches(
							  receive_admission, forward->base.master_node,
							  master_capability_generation)
						  && cr_server_r4_identity_open_matches(
							  receive_admission,
							  forward->base.original_requester_node,
							  requester_capability_generation);
			if (!final_match) {
				reason = CLUSTER_CR_BUILD_RF_DEFERRED;
				result = CLUSTER_CR_BUILD_RETRYABLE;
			}
			else {
				pg_write_barrier();
				pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_R4_QUEUED);
				cleanup_needed = false;
				cr_server_wake_lms();
				reason = CLUSTER_CR_BUILD_NONE;
				result = CLUSTER_CR_BUILD_FULL;
			}
		}
	}
	PG_CATCH();
	{
		if (cleanup_needed)
			(void)cr_server_r4_release_unpublished(slot, slot_generation);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (cleanup_needed
		&& !cr_server_r4_release_unpublished(slot, slot_generation)) {
		reason = CLUSTER_CR_BUILD_PROTOCOL;
		result = CLUSTER_CR_BUILD_FAIL_CLOSED;
	}

	*reason_out = reason;
	return result;
}

static bool
cr_server_bytes_zero(const void *ptr, Size size)
{
	const uint8 *bytes = (const uint8 *)ptr;
	Size i;

	for (i = 0; i < size; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static bool
cr_server_r4_reclaimable_slot(const ClusterLmsCrSlot *slot, uint32 state,
							  const ClusterLmsSharedState *lms_state,
							  bool live_close_proved)
{
	const ClusterR4CrOwnerStamp *owner;
	uint64 current_incarnation;
	uint8 expected_role;

	if (slot == NULL || lms_state == NULL)
		return false;
	if (state == CLUSTER_LMS_CR_FREE)
		return true;
	if (slot->r4.slot_generation == 0)
		return false;
	if (state == CLUSTER_LMS_CR_R4_RECLAIMING)
		return true;
	if (state != CLUSTER_LMS_CR_FILLING
		&& (state < CLUSTER_LMS_CR_R4_QUEUED
			|| state > CLUSTER_LMS_CR_R4_SHIPPING))
		return false;
	if (state != CLUSTER_LMS_CR_FILLING
		&& slot->req_kind != (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD)
		return false;

	owner = &slot->r4.owner;
	if (owner->edge_owner_worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return false;
	expected_role = (uint8)(owner->edge_owner_worker_id == 0 ? B_LMS : B_LMS_WORKER);
	if (owner->edge_owner_incarnation == 0 || owner->edge_owner_pid <= 0
		|| owner->edge_owner_role != expected_role
		|| !cr_server_bytes_zero(owner->reserved, sizeof(owner->reserved)))
		return false;

	current_incarnation = lms_state->r4_controls.data_worker_incarnation[
		owner->edge_owner_worker_id];
	if (state == CLUSTER_LMS_CR_FILLING
		|| state == CLUSTER_LMS_CR_R4_QUEUED) {
		if (owner->builder_incarnation != 0 || owner->builder_pid != 0)
			return false;
		return live_close_proved
			   || current_incarnation != owner->edge_owner_incarnation;
	}
	if (owner->builder_incarnation == 0 || owner->builder_pid <= 0
		|| owner->builder_worker_id != 0)
		return false;
	current_incarnation
		= lms_state->r4_controls.data_worker_incarnation[owner->builder_worker_id];
	return live_close_proved
		   || current_incarnation != owner->builder_incarnation;
}

/*
 * LMON's sole closed/dead recovery edge.  Preflight the complete four-slot
 * image before the first mutation, then claim each R4 slot through state 15,
 * canonicalize every reusable byte while preserving the no-reuse generation,
 * and require four FREE publications before reporting zero.
 */
bool
cluster_cr_server_r4_lmon_reclaim_closed(uint64 worker_incarnation,
									 uint64 generation)
{
	ClusterLmsSharedState *lms_state;
	bool claimed = false;
	bool live_close_proved;
	int i;

	if (CrServerShared == NULL || worker_incarnation == 0 || generation == 0)
		return false;
	lms_state = cluster_lms_shared_state();
	if (lms_state == NULL)
		return false;

	LWLockAcquire(&lms_state->lwlock, LW_EXCLUSIVE);
	live_close_proved
		= lms_state->r4_controls.data_worker_incarnation[0]
			  == worker_incarnation
		  && lms_state->r4_controls.drain_request_generation == generation
		  && lms_state->r4_controls.drain_ack_generation == generation;
	if (!live_close_proved)
		goto release_proof_lock;

	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 state = pg_atomic_read_u32(&slot->state);

		if (!cr_server_r4_reclaimable_slot(
				slot, state, lms_state, live_close_proved))
			goto release_proof_lock;
	}

	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 state = pg_atomic_read_u32(&slot->state);
		uint32 expected;

		if (state == CLUSTER_LMS_CR_FREE
			|| state == CLUSTER_LMS_CR_R4_RECLAIMING)
			continue;
		expected = state;
		if (!pg_atomic_compare_exchange_u32(
				&slot->state, &expected, CLUSTER_LMS_CR_R4_RECLAIMING))
			goto release_proof_lock;
	}
	claimed = true;

release_proof_lock:
	LWLockRelease(&lms_state->lwlock);
	if (!claimed)
		return false;

	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 state = pg_atomic_read_u32(&slot->state);
		uint32 expected;
		uint64 slot_generation;

		if (state == CLUSTER_LMS_CR_FREE)
			continue;
		if (state != CLUSTER_LMS_CR_R4_RECLAIMING)
			return false;
		slot_generation = slot->r4.slot_generation;
		cr_server_r4_canonicalize_unpublished(slot, slot_generation);
		pg_write_barrier();
		expected = CLUSTER_LMS_CR_R4_RECLAIMING;
		if (!pg_atomic_compare_exchange_u32(
				&slot->state, &expected, CLUSTER_LMS_CR_FREE))
			return false;
	}

	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		if (pg_atomic_read_u32(&CrServerShared->slots[i].state)
			!= CLUSTER_LMS_CR_FREE)
			return false;
	}
	return true;
}

/* Validate only the immutable, already-published QUEUED identity. */
static bool
cr_server_r4_queued_identity_valid(const ClusterLmsCrSlot *slot,
								   const ClusterLmsSharedState *lms_state)
{
	const ClusterR4CrOwnerStamp *owner;
	uint8 expected_role;

	if (slot == NULL || lms_state == NULL
		|| slot->req_kind != (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD
		|| slot->r4.slot_generation == 0
		|| slot->r4.slot_generation > (UINT64_MAX >> 2)
		|| slot->request_id == 0 || !SCN_VALID(slot->read_scn)
		|| slot->epoch != slot->r4.route_proof.formation_epoch
		|| !BufferTagsEqual(&slot->tag, &slot->r4.route_proof.tag)
		|| slot->read_scn != slot->r4.route_proof.read_scn
		|| slot->r4.route_proof.activation_generation == 0
		|| slot->reply_master_node != slot->r4.route_proof.real_master_node
		|| slot->r4.route_proof.selected_holder_node != cluster_node_id
		|| slot->requester_node < 0 || slot->requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| slot->requester_backend <= 0
		|| slot->reply_master_node < 0
		|| slot->reply_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| slot->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| slot->r4.requester_capability_generation == 0
		|| slot->r4.master_capability_generation == 0
		|| slot->r4.terminal_reason != (uint8)CLUSTER_CR_BUILD_NONE
		|| slot->r4.flags != 0
		|| !cr_server_bytes_zero(slot->r4.reserved, sizeof(slot->r4.reserved)))
		return false;

	owner = &slot->r4.owner;
	if (owner->edge_owner_worker_id >= CLUSTER_LMS_MAX_WORKERS
		|| owner->edge_owner_incarnation == 0 || owner->builder_incarnation != 0
		|| owner->builder_pid != 0 || owner->builder_worker_id != 0
		|| !cr_server_bytes_zero(owner->reserved, sizeof(owner->reserved)))
		return false;
	expected_role = (uint8)(owner->edge_owner_worker_id == 0 ? B_LMS : B_LMS_WORKER);
	return owner->edge_owner_role == expected_role
		   && lms_state->r4_controls.data_worker_incarnation[owner->edge_owner_worker_id]
			  == owner->edge_owner_incarnation;
}

/*
 * Start the distinct worker-0 TARGET episode and claim one immutable queued
 * slot.  The builder step is deliberately a later unit; this boundary owns
 * only QUEUED->BUILDING plus the keyed process-local token context.
 */
static bool
cr_server_r4_claim_queued(uint32 slot_index)
{
	ClusterLmsSharedState *lms_state;
	ClusterLmsCrSlot *slot;
	ClusterR4CrWorkerContext *context;
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	uint64 worker_incarnation;
	uint32 expected;
	bool has_local_identity;

	if (CrServerShared == NULL || slot_index >= CLUSTER_LMS_CR_SLOTS
		|| cluster_ic_tier1_my_data_channel() != 0 || MyBackendType != B_LMS)
		return false;
	lms_state = cluster_lms_shared_state();
	if (lms_state == NULL)
		return false;
	slot = &CrServerShared->slots[slot_index];
	context = &CrServerR4Contexts[slot_index];
	if (pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_R4_QUEUED || context->in_use)
		return false;
	pg_read_barrier();
	worker_incarnation = lms_state->r4_controls.data_worker_incarnation[0];
	if (worker_incarnation == 0 || !cr_server_r4_queued_identity_valid(slot, lms_state))
		return false;

	memset(&admission, 0, sizeof(admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE,
		&admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	has_local_identity = slot->reply_master_node == cluster_node_id
						 || slot->requester_node == cluster_node_id;
	if (admission.record_generation != slot->r4.route_proof.activation_generation
		|| admission.formation_epoch != slot->epoch
		|| (has_local_identity && !cluster_semantic_activation_recheck(&admission))
		|| !cr_server_r4_identity_open_matches(
			&admission, slot->reply_master_node,
			slot->r4.master_capability_generation)
		|| !cr_server_r4_identity_open_matches(
			&admission, slot->requester_node,
			slot->r4.requester_capability_generation)) {
		cluster_semantic_activation_leave(&admission);
		return false;
	}

	memset(&slot->r4.owner, 0, sizeof(slot->r4.owner));
	slot->r4.owner.edge_owner_incarnation = worker_incarnation;
	slot->r4.owner.builder_incarnation = worker_incarnation;
	slot->r4.owner.edge_owner_pid = MyProcPid;
	slot->r4.owner.builder_pid = MyProcPid;
	slot->r4.owner.edge_owner_worker_id = 0;
	slot->r4.owner.builder_worker_id = 0;
	slot->r4.owner.edge_owner_role = (uint8)B_LMS;
	pg_write_barrier();
	expected = CLUSTER_LMS_CR_R4_QUEUED;
	if (!pg_atomic_compare_exchange_u32(
			&slot->state, &expected, CLUSTER_LMS_CR_R4_BUILDING)) {
		cluster_semantic_activation_leave(&admission);
		return false;
	}

	memset(context, 0, sizeof(*context));
	context->slot_generation = slot->r4.slot_generation;
	context->builder_incarnation = worker_incarnation;
	context->requester_node = slot->requester_node;
	context->requester_backend_id = slot->requester_backend;
	context->request_id = slot->request_id;
	context->admission = admission;
	pg_write_barrier();
	context->in_use = true;
	return true;
}

/*
 * A builder ERROR owns shared mutation only while the complete worker-0 key
 * still matches.  Preserve immutable request/route/owner bytes, discard both
 * scratch pages, and publish one typed terminal for the existing ship scan.
 * The process-local TARGET token remains in context until that terminal is
 * shipped or dropped; only the builder's private walk context is forgotten
 * here.
 */
static bool
cr_server_r4_terminalize_build_error(
	uint32 slot_index, ClusterLmsCrSlot *slot, ClusterR4CrWorkerContext *context,
	const ClusterR4CrWorkerContext *key, int sqlerrcode)
{
	ClusterCrBuildReason reason;
	uint32 terminal_state;
	uint32 expected = CLUSTER_LMS_CR_R4_BUILDING;

	if (slot == NULL || context == NULL || key == NULL || !key->in_use
		|| key->builder_forgotten
		|| pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_R4_BUILDING
		|| memcmp(context, key, sizeof(*key)) != 0
		|| slot->r4.slot_generation != key->slot_generation
		|| slot->r4.owner.builder_incarnation != key->builder_incarnation
		|| slot->r4.owner.builder_pid != MyProcPid
		|| slot->r4.owner.builder_worker_id != 0
		|| slot->requester_node != key->requester_node
		|| slot->requester_backend != key->requester_backend_id
		|| slot->request_id != key->request_id)
		return false;

	if (sqlerrcode == ERRCODE_QUERY_CANCELED) {
		reason = CLUSTER_CR_BUILD_CANCELLED;
		terminal_state = CLUSTER_LMS_CR_R4_CANCELLED;
	}
	else {
		reason = CLUSTER_CR_BUILD_IO_ERROR;
		terminal_state = CLUSTER_LMS_CR_R4_READY_FAIL;
	}

	memset(slot->result_page, 0, sizeof(slot->result_page));
	memset(slot->foreign_undo_page, 0, sizeof(slot->foreign_undo_page));
	cluster_cr_build_on_holder_forget(slot_index, key->slot_generation);
	context->builder_forgotten = true;
	slot->r4.terminal_reason = (uint8)reason;
	pg_write_barrier();
	if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, terminal_state))
		return false;
	cr_server_wake_lms();
	return true;
}

/*
 * Drive one already-claimed worker-0 build step and release-publish its typed
 * terminal.  NEED_UNDO is intentionally not a legal result until the adjacent
 * foreign-fetch unit installs the frozen request tuple and state edge.
 */
static bool
cr_server_r4_build_step(uint32 slot_index)
{
	ClusterLmsCrSlot *slot;
	ClusterR4CrWorkerContext *context;
	ClusterR4CrWorkerContext key;
	ClusterCrBuildReason step_reason = CLUSTER_CR_BUILD_PROTOCOL;
	MemoryContext saved_context;
	volatile ClusterR4CrBuildStepResult step_result = CLUSTER_R4_CR_STEP_FAIL;
	volatile ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;
	volatile bool error_terminalized = false;
	uint32 terminal_state;
	uint32 entry_state;
	uint32 expected;
	bool foreign_undo_ready;

	if (CrServerShared == NULL || slot_index >= CLUSTER_LMS_CR_SLOTS
		|| cluster_ic_tier1_my_data_channel() != 0 || MyBackendType != B_LMS)
		return false;
	slot = &CrServerShared->slots[slot_index];
	context = &CrServerR4Contexts[slot_index];
	entry_state = pg_atomic_read_u32(&slot->state);
	foreign_undo_ready = entry_state == CLUSTER_LMS_CR_R4_UNDO_READY;
	if ((entry_state != CLUSTER_LMS_CR_R4_BUILDING && !foreign_undo_ready) || !context->in_use
		|| context->slot_generation != slot->r4.slot_generation
		|| context->builder_incarnation != slot->r4.owner.builder_incarnation
		|| context->requester_node != slot->requester_node
		|| context->requester_backend_id != slot->requester_backend
		|| context->request_id != slot->request_id)
		return false;
	if (foreign_undo_ready) {
		ClusterLmsSharedState *lms_state = cluster_lms_shared_state();
		const ClusterR4CrOwnerStamp *owner = &slot->r4.owner;

		pg_read_barrier(); /* status-24 publishes the bytes before READY */
		if (lms_state == NULL || context->builder_forgotten || !context->admission.entered
			|| context->admission.record_generation != slot->r4.route_proof.activation_generation
			|| context->admission.formation_epoch != slot->epoch
			|| owner->builder_incarnation != lms_state->r4_controls.data_worker_incarnation[0]
			|| owner->builder_incarnation == 0
			|| owner->edge_owner_incarnation != owner->builder_incarnation
			|| owner->builder_pid != MyProcPid || owner->edge_owner_pid != MyProcPid
			|| owner->builder_worker_id != 0 || owner->edge_owner_worker_id != 0
			|| owner->edge_owner_role != (uint8)B_LMS
			|| !cr_server_bytes_zero(owner->reserved, sizeof(owner->reserved))
			|| slot->req_kind != (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD
			|| slot->epoch != slot->r4.route_proof.formation_epoch
			|| !BufferTagsEqual(&slot->tag, &slot->r4.route_proof.tag)
			|| slot->read_scn != slot->r4.route_proof.read_scn
			|| !context->foreign_physical_generation_frozen
			|| context->expected_foreign_physical_generation == UINT32_MAX
			|| context->foreign_scur_phase != CLUSTER_R4_FOREIGN_SCUR_UNUSED
			|| slot->r4.foreign_request_id == 0 || slot->r4.foreign_wrap > TT_WRAP_MAX
			|| slot->r4.origin_formation_epoch != slot->epoch
			|| slot->r4.origin_live_hwm_lsn == InvalidXLogRecPtr
			|| !SCN_VALID(slot->r4.origin_authority_scn)
			|| scn_time_cmp(slot->r4.origin_authority_scn, slot->read_scn) < 0
			|| !cluster_semantic_activation_recheck(&context->admission)
			|| !cr_server_r4_identity_open_matches(&context->admission, slot->reply_master_node,
												   slot->r4.master_capability_generation)
			|| !cr_server_r4_identity_open_matches(&context->admission, slot->requester_node,
												   slot->r4.requester_capability_generation))
			return false;
		expected = CLUSTER_LMS_CR_R4_UNDO_READY;
		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_R4_BUILDING))
			return false;
	}
	key = *context;
	saved_context = CurrentMemoryContext;

	PG_TRY();
	{
		step_result = cluster_cr_build_on_holder_step(
			slot_index, key.slot_generation, foreign_undo_ready, &slot->r4, slot->result_page,
			slot->foreign_undo_page, &step_reason);
		reason = step_reason;
		if (foreign_undo_ready) {
			memset(slot->foreign_undo_page, 0, BLCKSZ);
			context->foreign_physical_generation_frozen = false;
			context->expected_foreign_physical_generation = 0;
		}
	}
	PG_CATCH();
	{
		error_terminalized = cr_server_r4_terminalize_build_error(
			slot_index, slot, context, &key, geterrcode());
		if (!error_terminalized)
			PG_RE_THROW();
		MemoryContextSwitchTo(saved_context);
		FlushErrorState();
	}
	PG_END_TRY();
	if (error_terminalized)
		return true;
	switch (step_result) {
		case CLUSTER_R4_CR_STEP_FULL:
			if (reason == CLUSTER_CR_BUILD_NONE)
				terminal_state = CLUSTER_LMS_CR_R4_READY_FULL;
			else {
				reason = CLUSTER_CR_BUILD_PROTOCOL;
				terminal_state = CLUSTER_LMS_CR_R4_READY_FAIL;
			}
			break;
		case CLUSTER_R4_CR_STEP_RETRY:
			if (reason != CLUSTER_CR_BUILD_NONE
				&& cluster_cr_build_result_for_reason(reason) == CLUSTER_CR_BUILD_RETRYABLE)
				terminal_state = CLUSTER_LMS_CR_R4_READY_RETRY;
			else {
				reason = CLUSTER_CR_BUILD_PROTOCOL;
				terminal_state = CLUSTER_LMS_CR_R4_READY_FAIL;
			}
			break;
		case CLUSTER_R4_CR_STEP_FAIL:
			if (reason == CLUSTER_CR_BUILD_NONE
				|| cluster_cr_build_result_for_reason(reason) != CLUSTER_CR_BUILD_FAIL_CLOSED)
				reason = CLUSTER_CR_BUILD_PROTOCOL;
			terminal_state = CLUSTER_LMS_CR_R4_READY_FAIL;
			break;
		case CLUSTER_R4_CR_STEP_NEED_UNDO:
			if (reason == CLUSTER_CR_BUILD_NONE)
				terminal_state = CLUSTER_LMS_CR_R4_NEED_UNDO;
			else {
				reason = CLUSTER_CR_BUILD_PROTOCOL;
				terminal_state = CLUSTER_LMS_CR_R4_READY_FAIL;
			}
			break;
		default:
			return false;
	}

	slot->r4.terminal_reason = (uint8)reason;
	pg_write_barrier();
	expected = CLUSTER_LMS_CR_R4_BUILDING;
	return pg_atomic_compare_exchange_u32(&slot->state, &expected, terminal_state);
}

static bool
cr_server_r4_publish_foreign_terminal(ClusterLmsCrSlot *slot, uint32 from_state,
									  uint32 terminal_state,
									  ClusterCrBuildReason reason)
{
	uint32 expected = from_state;

	slot->r4.terminal_reason = (uint8)reason;
	pg_write_barrier();
	return pg_atomic_compare_exchange_u32(&slot->state, &expected, terminal_state);
}

static bool
cr_server_r4_foreign_request_valid(uint32 slot_index, const ClusterLmsCrSlot *slot,
								  const ClusterR4CrWorkerContext *context,
								  bool allow_unresolved_wrap,
								  ClusterTxLocator *locator_out,
								  uint32 *segment_out, uint32 *block_out)
{
	uint16 tt_slot_offset;
	uint16 row_offset;
	bool data_kind;

	if (slot == NULL || context == NULL || locator_out == NULL || segment_out == NULL
		|| block_out == NULL || slot->req_kind != (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD
		|| !context->in_use || !context->admission.entered || context->slot_generation == 0
		|| context->slot_generation > (UINT64_MAX >> 18)
		|| context->slot_generation != slot->r4.slot_generation
		|| context->builder_incarnation != slot->r4.owner.builder_incarnation
		|| context->requester_node != slot->requester_node
		|| context->requester_backend_id != slot->requester_backend
		|| context->request_id != slot->request_id
		|| (slot->epoch == 0 && cluster_conf_node_count() != 4)
		|| slot->epoch != context->admission.formation_epoch
		|| slot->epoch != slot->r4.route_proof.formation_epoch
		|| !BufferTagsEqual(&slot->tag, &slot->r4.route_proof.tag)
		|| slot->read_scn != slot->r4.route_proof.read_scn || !SCN_VALID(slot->read_scn)
		|| slot->r4.route_proof.selected_holder_node != cluster_node_id
		|| slot->r4.foreign_request_id
			   != cluster_cr_r4_dependency_request_id(slot_index, context->slot_generation,
													  slot->r4.build_steps)
		|| slot->r4.foreign_request_id == 0 || slot->r4.origin_formation_epoch != slot->epoch
		|| slot->r4.origin_live_hwm_lsn != 0 || slot->r4.origin_tt_generation != 0
		|| SCN_VALID(slot->r4.origin_authority_scn) || slot->r4.foreign_origin_node < 0
		|| slot->r4.foreign_origin_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| slot->r4.foreign_origin_node == cluster_node_id
		|| !TransactionIdIsNormal(slot->r4.foreign_xid)
		|| (slot->r4.foreign_wrap > TT_WRAP_MAX
			&& !(allow_unresolved_wrap && slot->r4.foreign_wrap == TT_WRAP_INVALID))
		|| slot->r4.terminal_reason != (uint8)CLUSTER_CR_BUILD_NONE || slot->r4.flags != 0
		|| !cr_server_bytes_zero(slot->r4.reserved, sizeof(slot->r4.reserved))
		|| !cluster_cr_build_on_holder_pending_locator(slot_index, context->slot_generation,
													   locator_out)
		|| locator_out->uba.raw[0] != slot->r4.foreign_uba.raw[0]
		|| locator_out->uba.raw[1] != slot->r4.foreign_uba.raw[1]
		|| !uba_decode(locator_out->uba, segment_out, block_out, &tt_slot_offset, &row_offset)
		|| *block_out == 0 || uba_origin_node_id(locator_out->uba) != slot->r4.foreign_origin_node
		|| *segment_out != slot->r4.foreign_segment_id || *block_out != slot->r4.foreign_block_no
		|| tt_slot_offset != slot->r4.foreign_tt_slot_offset
		|| row_offset != slot->r4.foreign_row_offset || locator_out->xid != slot->r4.foreign_xid
		|| locator_out->tt_wrap != (uint16)slot->r4.foreign_wrap)
		return false;

	data_kind = locator_out->itl_kind == ITL_FLAG_ACTIVE
				|| locator_out->itl_kind == ITL_FLAG_COMMITTED
				|| locator_out->itl_kind == ITL_FLAG_ABORTED
				|| locator_out->itl_kind == ITL_FLAG_NEEDS_CLEANOUT;
	return locator_out->itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT
		   && (data_kind || ITL_FLAG_IS_LOCK_ONLY(locator_out->itl_kind));
}

static bool
cr_server_r4_resolved_root_matches(
	const ClusterUndoBlock0ResolvedRoot *left,
	const ClusterUndoBlock0ResolvedRoot *right)
{
	return left != NULL && right != NULL && left->intent == right->intent
		   && left->root_id == right->root_id
		   && left->root_generation == right->root_generation;
}

static void
cr_server_r4_foreign_scur_abort(ClusterR4CrWorkerContext *context)
{
	if (context == NULL)
		return;
	if (context->foreign_scur_phase != CLUSTER_R4_FOREIGN_SCUR_UNUSED)
		cluster_undo_block0_current_cancel(&context->foreign_scur);
	memset(&context->foreign_scur, 0, sizeof(context->foreign_scur));
	memset(&context->foreign_resolved_root, 0,
		   sizeof(context->foreign_resolved_root));
	memset(&context->foreign_sampled_generation, 0,
		   sizeof(context->foreign_sampled_generation));
	context->foreign_scur_phase = CLUSTER_R4_FOREIGN_SCUR_UNUSED;
	context->expected_foreign_physical_generation = 0;
	context->foreign_physical_generation_frozen = false;
}

static ClusterR4ForeignSampleStep
cr_server_r4_sample_foreign_generation(
	uint32 slot_index, ClusterLmsCrSlot *slot,
	ClusterR4CrWorkerContext *context, ClusterTxLocator *locator_out,
	uint32 *segment_out, uint32 *block_out)
{
	ClusterUndoBlock0CurrentStep current_step;
	ClusterUndoBlock0ResolvedRoot current_root;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0Result current_result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	uint32 owner_instance;

	if (slot == NULL || context == NULL || locator_out == NULL
		|| segment_out == NULL || block_out == NULL
		|| !cr_server_r4_foreign_request_valid(
			slot_index, slot, context, true, locator_out, segment_out,
			block_out))
		goto failed;
	owner_instance = (uint32)slot->r4.foreign_origin_node + 1;
	if (owner_instance == 0 || owner_instance > CLUSTER_MAX_NODES
		|| !cluster_semantic_activation_resolve_shared_undo_root(
			&context->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			owner_instance, *segment_out, &current_root))
		goto failed;
	if (context->foreign_scur_phase != CLUSTER_R4_FOREIGN_SCUR_UNUSED
		&& !cr_server_r4_resolved_root_matches(
			&context->foreign_resolved_root, &current_root))
		goto failed;

	if (context->foreign_scur_phase == CLUSTER_R4_FOREIGN_SCUR_UNUSED) {
		memset(&logical, 0, sizeof(logical));
		logical.segment_id = *segment_out;
		logical.owner_instance = (uint8)owner_instance;
		memset(&context->foreign_scur, 0,
			   sizeof(context->foreign_scur));
		memset(&context->foreign_sampled_generation, 0,
			   sizeof(context->foreign_sampled_generation));
		context->foreign_resolved_root = current_root;
		context->foreign_scur_phase = CLUSTER_R4_FOREIGN_SCUR_ACQUIRE;
		current_step = cluster_undo_block0_current_acquire_begin(
			&logical, CLUSTER_UNDO_BLOCK0_SCUR, 0,
			&context->foreign_scur, &current_result);
	} else if (context->foreign_scur_phase
			   == CLUSTER_R4_FOREIGN_SCUR_ACQUIRE) {
		current_step = cluster_undo_block0_current_acquire_poll(
			&context->foreign_scur, &current_result);
	} else if (context->foreign_scur_phase
			   == CLUSTER_R4_FOREIGN_SCUR_RELEASE) {
		current_step = cluster_undo_block0_current_release_poll(
			&context->foreign_scur, &current_result);
		if (current_step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
			return CLUSTER_R4_FOREIGN_SAMPLE_PENDING;
		if (current_step != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
			goto failed;
		goto released;
	} else
		goto failed;

	if (current_step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
		return CLUSTER_R4_FOREIGN_SAMPLE_PENDING;
	if (current_step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
		goto failed;
	current_result = cluster_undo_block0_current_sample_generation(
		&context->foreign_scur, &context->foreign_resolved_root,
		&context->foreign_sampled_generation);
	if (current_result != CLUSTER_UNDO_BLOCK0_OK
		|| !context->foreign_sampled_generation.known
		|| context->foreign_sampled_generation.value == UINT32_MAX)
		goto failed;
	context->foreign_scur_phase = CLUSTER_R4_FOREIGN_SCUR_RELEASE;
	current_step = cluster_undo_block0_current_release_begin(
		&context->foreign_scur, &current_result);
	if (current_step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
		return CLUSTER_R4_FOREIGN_SAMPLE_PENDING;
	if (current_step != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
		goto failed;

released:
	if (!cluster_semantic_activation_resolve_shared_undo_root(
			&context->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
			owner_instance, *segment_out, &current_root)
		|| !cr_server_r4_resolved_root_matches(
			&context->foreign_resolved_root, &current_root))
		goto failed;
	context->expected_foreign_physical_generation
		= context->foreign_sampled_generation.value;
	pg_write_barrier();
	context->foreign_physical_generation_frozen = true;
	memset(&context->foreign_scur, 0, sizeof(context->foreign_scur));
	memset(&context->foreign_resolved_root, 0,
		   sizeof(context->foreign_resolved_root));
	memset(&context->foreign_sampled_generation, 0,
		   sizeof(context->foreign_sampled_generation));
	context->foreign_scur_phase = CLUSTER_R4_FOREIGN_SCUR_UNUSED;
	return CLUSTER_R4_FOREIGN_SAMPLE_READY;

failed:
	cr_server_r4_foreign_scur_abort(context);
	return CLUSTER_R4_FOREIGN_SAMPLE_FAILED;
}

static bool
cr_server_r4_send_foreign_undo(uint32 slot_index)
{
	ClusterLmsCrSlot *slot;
	ClusterR4CrWorkerContext *context;
	ClusterR4CrForwardPayload forward;
	ClusterTxLocator locator;
	ClusterICSendResult send_result;
	ClusterR4ForeignSampleStep sample_step;
	uint32 segment_id;
	uint32 block_no;
	uint32 expected;

	if (CrServerShared == NULL || slot_index >= CLUSTER_LMS_CR_SLOTS
		|| cluster_ic_tier1_my_data_channel() != 0 || MyBackendType != B_LMS
		|| !cluster_gcs_block_family_on_data_plane())
		return false;
	slot = &CrServerShared->slots[slot_index];
	context = &CrServerR4Contexts[slot_index];
	if (pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_R4_NEED_UNDO)
		return false;
	pg_read_barrier();
	if (!context->foreign_physical_generation_frozen) {
		sample_step = cr_server_r4_sample_foreign_generation(
			slot_index, slot, context, &locator, &segment_id, &block_no);
		if (sample_step == CLUSTER_R4_FOREIGN_SAMPLE_PENDING)
			return false;
		if (sample_step != CLUSTER_R4_FOREIGN_SAMPLE_READY)
			return cr_server_r4_publish_foreign_terminal(
				slot, CLUSTER_LMS_CR_R4_NEED_UNDO,
				CLUSTER_LMS_CR_R4_READY_FAIL, CLUSTER_CR_BUILD_PROTOCOL);
	} else if (!cr_server_r4_foreign_request_valid(
				   slot_index, slot, context, true, &locator, &segment_id,
				   &block_no))
		return cr_server_r4_publish_foreign_terminal(
			slot, CLUSTER_LMS_CR_R4_NEED_UNDO, CLUSTER_LMS_CR_R4_READY_FAIL,
			CLUSTER_CR_BUILD_PROTOCOL);
	/* Generation zero is valid.  The separate frozen bit proves that the
	 * exact PGRD-resolved SCUR sample completed release before publication. */
	if (context->expected_foreign_physical_generation == UINT32_MAX)
		return cr_server_r4_publish_foreign_terminal(
			slot, CLUSTER_LMS_CR_R4_NEED_UNDO, CLUSTER_LMS_CR_R4_READY_FAIL,
			CLUSTER_CR_BUILD_PROTOCOL);

	memset(&forward, 0, sizeof(forward));
	forward.base.request_id = slot->r4.foreign_request_id;
	forward.base.epoch = slot->r4.origin_formation_epoch;
	forward.base.tag = GcsBlockUndoFetchTagMake(segment_id, block_no);
	forward.base.original_requester_node = cluster_node_id;
	forward.base.requester_backend_id = CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT;
	forward.base.master_node = cluster_node_id;
	forward.base.transition_id = (uint8)PCM_TRANS_N_TO_S;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(
		&forward.base, slot->r4.route_proof.read_scn);
	forward.base.reserved_0[6] = CLUSTER_R4_FORWARD_EXTENDED;
	if (!ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward.extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator,
			context->expected_foreign_physical_generation))
		return cr_server_r4_publish_foreign_terminal(
			slot, CLUSTER_LMS_CR_R4_NEED_UNDO, CLUSTER_LMS_CR_R4_READY_FAIL,
			CLUSTER_CR_BUILD_PROTOCOL);

	expected = CLUSTER_LMS_CR_R4_NEED_UNDO;
	if (!pg_atomic_compare_exchange_u32(
			&slot->state, &expected, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT))
		return false;
	pg_read_barrier();
	send_result = cluster_ic_send_envelope(
		PGRAC_IC_MSG_GCS_BLOCK_FORWARD, slot->r4.foreign_origin_node, &forward,
		sizeof(forward));
	cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_FORWARD, send_result);
	switch (send_result) {
		case CLUSTER_IC_SEND_DONE:
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			return true;
		case CLUSTER_IC_SEND_NOT_ADMITTED:
			return cr_server_r4_publish_foreign_terminal(
				slot, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT,
				CLUSTER_LMS_CR_R4_READY_RETRY, CLUSTER_CR_BUILD_CAPACITY);
		case CLUSTER_IC_SEND_HARD_ERROR:
			cluster_lms_data_plane_close_peer_now(slot->r4.foreign_origin_node);
			return cr_server_r4_publish_foreign_terminal(
				slot, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT,
				CLUSTER_LMS_CR_R4_READY_FAIL, CLUSTER_CR_BUILD_PROTOCOL);
	}
	return false;
}

static bool
cr_server_r4_foreign_landing_key_valid(
	uint32 slot_index, ClusterLmsCrSlot *slot,
	const ClusterR4CrWorkerContext *context,
	const ClusterR4CrWorkerContext *frozen_context)
{
	ClusterLmsSharedState *lms_state = cluster_lms_shared_state();

	return slot != NULL && context != NULL && frozen_context != NULL && lms_state != NULL
		   && slot_index < CLUSTER_LMS_CR_SLOTS
		   && pg_atomic_read_u32(&slot->state) == CLUSTER_LMS_CR_R4_UNDO_INFLIGHT
		   && memcmp(context, frozen_context, sizeof(*context)) == 0 && context->in_use
		   && !context->builder_forgotten && context->foreign_physical_generation_frozen
		   && cr_server_bytes_zero(context->reserved, sizeof(context->reserved))
		   && context->slot_generation != 0 && context->slot_generation <= (UINT64_MAX >> 18)
		   && context->slot_generation == slot->r4.slot_generation
		   && context->builder_incarnation != 0
		   && context->builder_incarnation == slot->r4.owner.builder_incarnation
		   && context->builder_incarnation == slot->r4.owner.edge_owner_incarnation
		   && context->builder_incarnation == lms_state->r4_controls.data_worker_incarnation[0]
		   && slot->r4.owner.edge_owner_pid == MyProcPid && slot->r4.owner.builder_pid == MyProcPid
		   && slot->r4.owner.edge_owner_worker_id == 0 && slot->r4.owner.builder_worker_id == 0
		   && slot->r4.owner.edge_owner_role == (uint8)B_LMS
		   && cr_server_bytes_zero(slot->r4.owner.reserved, sizeof(slot->r4.owner.reserved))
		   && context->requester_node == slot->requester_node
		   && context->requester_backend_id == slot->requester_backend
		   && context->request_id == slot->request_id
		   && slot->r4.foreign_request_id
				  == cluster_cr_r4_dependency_request_id(slot_index, context->slot_generation,
														 slot->r4.build_steps)
		   && slot->r4.foreign_request_id != 0
		   && slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD
		   && slot->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && (slot->epoch != 0 || cluster_conf_node_count() == 4)
		   && slot->epoch == context->admission.formation_epoch
		   && slot->epoch == slot->r4.route_proof.formation_epoch
		   && slot->r4.origin_formation_epoch == slot->epoch && slot->r4.foreign_origin_node >= 0
		   && slot->r4.foreign_origin_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && slot->r4.foreign_origin_node != cluster_node_id && slot->r4.origin_live_hwm_lsn == 0
		   && slot->r4.origin_tt_generation == 0 && !SCN_VALID(slot->r4.origin_authority_scn)
		   && cr_server_bytes_zero(slot->foreign_undo_page, BLCKSZ)
		   && slot->r4.terminal_reason == (uint8)CLUSTER_CR_BUILD_NONE && slot->r4.flags == 0
		   && cr_server_bytes_zero(slot->r4.reserved, sizeof(slot->r4.reserved));
}

bool
cluster_cr_server_r4_land_foreign_undo(
	const ClusterICEnvelope *env, const GcsBlockReplyHeader *header,
	const char undo_page[BLCKSZ], const ClusterGcsUndoAuthTrailer *undo_auth)
{
	ClusterLmsCrSlot *slot;
	ClusterR4CrWorkerContext *context;
	ClusterR4CrWorkerContext frozen_context;
	ClusterSemanticAdmissionToken receive_admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterTxLocator locator;
	ClusterTxLocator canonical_locator;
	PGAlignedBlock record;
	size_t record_length = 0;
	uint64 tt_generation;
	SCN authority_scn;
	uint32 physical_generation;
	uint32 segment_id;
	uint32 block_no;
	uint32 slot_index;
	uint32 expected;
	bool admitted = false;
	bool landed = false;
	bool refusal = header != NULL && GcsBlockReplyStatusIsR4Refusal(header->status);

	if (CrServerShared == NULL || env == NULL || header == NULL || undo_page == NULL
		|| (!refusal && undo_auth == NULL) || cluster_ic_tier1_my_data_channel() != 0
		|| MyBackendType != B_LMS || !cluster_gcs_block_family_on_data_plane()
		|| env->msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
		|| env->payload_length
			   != GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE
					  + (refusal ? 0 : (uint32)sizeof(ClusterGcsUndoAuthTrailer))
		|| env->dest_node_id != (uint32)cluster_node_id || env->epoch != header->epoch
		|| (!refusal && header->status != (uint8)GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT)
		|| header->requester_backend_id != CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT
		|| header->transition_id != (uint8)PCM_TRANS_N_TO_S || header->sender_node < 0
		|| header->sender_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| env->source_node_id != (uint32)header->sender_node
		|| GcsBlockReplyHeaderGetForwardingMasterNode(header)
			   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| (refusal ? (header->page_lsn != InvalidXLogRecPtr || undo_auth != NULL
					   || !cr_server_bytes_zero(header->reserved_0, sizeof(header->reserved_0))
					   || !cr_server_bytes_zero(undo_page, BLCKSZ))
					: header->page_lsn == InvalidXLogRecPtr)
		|| header->request_id == 0
		|| (!refusal && !GcsBlockReplyHeaderGetR4UndoGeneration(header, &physical_generation))
		|| header->checksum != cluster_gcs_block_compute_checksum(undo_page))
		return false;

	slot_index = (uint32)(header->request_id & (CLUSTER_LMS_CR_SLOTS - 1));
	slot = &CrServerShared->slots[slot_index];
	context = &CrServerR4Contexts[slot_index];
	if (pg_atomic_read_u32(&slot->state)
		!= CLUSTER_LMS_CR_R4_UNDO_INFLIGHT)
		return false;
	pg_read_barrier();
	frozen_context = *context;
	if (!cr_server_r4_foreign_landing_key_valid(slot_index, slot, context, &frozen_context)
		|| header->request_id != slot->r4.foreign_request_id
		|| header->epoch != slot->r4.origin_formation_epoch
		|| header->sender_node != slot->r4.foreign_origin_node
		|| (!refusal && physical_generation != context->expected_foreign_physical_generation)
		|| !cr_server_r4_foreign_request_valid(slot_index, slot, context, true, &locator,
											   &segment_id, &block_no))
		return false;

	tt_generation = refusal ? 0 : ClusterGcsUndoAuthTrailerGetTtGeneration(undo_auth);
	authority_scn = refusal ? InvalidScn : (SCN)ClusterGcsUndoAuthTrailerGetAuthorityScn(undo_auth);
	if (!refusal
		&& (!SCN_VALID(authority_scn) || scn_time_cmp(authority_scn, slot->read_scn) < 0
			|| !cluster_cr_r4_extract_resident_record(undo_page, &locator, record.data,
													  &record_length, &canonical_locator)
			|| record_length == 0 || canonical_locator.uba.raw[0] != locator.uba.raw[0]
			|| canonical_locator.uba.raw[1] != locator.uba.raw[1]
			|| canonical_locator.xid != locator.xid
			|| canonical_locator.itl_kind != locator.itl_kind
			|| canonical_locator.itl_slot_index != locator.itl_slot_index
			|| canonical_locator.tt_wrap > TT_WRAP_MAX
			|| (locator.tt_wrap != TT_WRAP_INVALID
				&& canonical_locator.tt_wrap != locator.tt_wrap)))
		return false;

	memset(&receive_admission, 0, sizeof(receive_admission));
	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &receive_admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	admitted = true;
	if (receive_admission.record_generation
			!= frozen_context.admission.record_generation
		|| receive_admission.formation_epoch != header->epoch
		|| !cluster_semantic_activation_recheck(&receive_admission)
		|| !cr_server_r4_foreign_landing_key_valid(
			slot_index, slot, context, &frozen_context))
		goto out;

	if (refusal) {
		bool retry = header->status == GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;

		landed = cr_server_r4_publish_foreign_terminal(
			slot, CLUSTER_LMS_CR_R4_UNDO_INFLIGHT,
			retry ? CLUSTER_LMS_CR_R4_READY_RETRY : CLUSTER_LMS_CR_R4_READY_FAIL,
			retry ? CLUSTER_CR_BUILD_CAPACITY : CLUSTER_CR_BUILD_PROTOCOL);
		goto out;
	}
	memcpy(slot->foreign_undo_page, undo_page, BLCKSZ);
	slot->r4.foreign_wrap = canonical_locator.tt_wrap;
	slot->r4.origin_live_hwm_lsn = header->page_lsn;
	slot->r4.origin_tt_generation = tt_generation;
	slot->r4.origin_authority_scn = authority_scn;
	pg_write_barrier();
	expected = CLUSTER_LMS_CR_R4_UNDO_INFLIGHT;
	landed = pg_atomic_compare_exchange_u32(
		&slot->state, &expected, CLUSTER_LMS_CR_R4_UNDO_READY);

out:
	if (admitted)
		cluster_semantic_activation_leave(&receive_admission);
	if (landed)
		cr_server_wake_lms();
	return landed;
}

static bool
cr_server_r4_release_terminal(uint32 slot_index, uint64 slot_generation)
{
	ClusterLmsCrSlot *slot = &CrServerShared->slots[slot_index];
	ClusterR4CrWorkerContext *context = &CrServerR4Contexts[slot_index];
	uint32 expected = CLUSTER_LMS_CR_R4_SHIPPING;

	cr_server_r4_canonicalize_unpublished(slot, slot_generation);
	pg_write_barrier();
	if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_FREE))
		return false;
	if (!context->builder_forgotten)
		cluster_cr_build_on_holder_forget(slot_index, slot_generation);
	cluster_semantic_activation_leave(&context->admission);
	memset(context, 0, sizeof(*context));
	return true;
}

/*
 * Ship one finished R4 page directly from LMS DATA worker 0.  The retained
 * TARGET token and requester capability generation are rechecked after the
 * terminal claim and immediately before the image-send fence/transport
 * admission.  The transport owns a private frame on DONE or WOULD_BLOCK.
 */
static bool
cr_server_r4_ship_terminal(uint32 slot_index)
{
	ClusterLmsCrSlot *slot;
	ClusterR4CrWorkerContext *context;
	char frame[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE];
	GcsBlockReplyHeader *header = (GcsBlockReplyHeader *)frame;
	ClusterICEnvelope envelope;
	ClusterICSendResult send_result;
	ClusterCrBuildReason terminal_reason;
	uint64 slot_generation;
	uint32 terminal_state;
	uint32 expected;
	uint8 reply_status;

	if (CrServerShared == NULL || slot_index >= CLUSTER_LMS_CR_SLOTS
		|| cluster_ic_tier1_my_data_channel() != 0 || MyBackendType != B_LMS
		|| !cluster_gcs_block_family_on_data_plane())
		return false;
	slot = &CrServerShared->slots[slot_index];
	context = &CrServerR4Contexts[slot_index];
	terminal_state = pg_atomic_read_u32(&slot->state);
	terminal_reason = (ClusterCrBuildReason)slot->r4.terminal_reason;
	switch (terminal_state) {
		case CLUSTER_LMS_CR_R4_READY_FULL:
			if (terminal_reason != CLUSTER_CR_BUILD_NONE)
				return false;
			reply_status = (uint8)GCS_BLOCK_REPLY_R4_CR_FULL;
			break;
		case CLUSTER_LMS_CR_R4_READY_RETRY:
			if (terminal_reason <= CLUSTER_CR_BUILD_NONE
				|| terminal_reason > CLUSTER_CR_BUILD_PROTOCOL
				|| cluster_cr_build_result_for_reason(terminal_reason)
					   != CLUSTER_CR_BUILD_RETRYABLE)
				return false;
			reply_status = (uint8)GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
			break;
		case CLUSTER_LMS_CR_R4_READY_FAIL:
			if (terminal_reason <= CLUSTER_CR_BUILD_NONE
				|| terminal_reason > CLUSTER_CR_BUILD_PROTOCOL
				|| cluster_cr_build_result_for_reason(terminal_reason)
					   != CLUSTER_CR_BUILD_FAIL_CLOSED)
				return false;
			reply_status = (uint8)GCS_BLOCK_REPLY_R4_DENIED;
			break;
		case CLUSTER_LMS_CR_R4_CANCELLED:
			if (terminal_reason != CLUSTER_CR_BUILD_CANCELLED)
				return false;
			reply_status = (uint8)GCS_BLOCK_REPLY_R4_DENIED;
			break;
		default:
			return false;
	}
	if (!context->in_use || !context->admission.entered
		|| context->slot_generation == 0
		|| context->slot_generation != slot->r4.slot_generation
		|| context->builder_incarnation != slot->r4.owner.builder_incarnation
		|| context->requester_node != slot->requester_node
		|| context->requester_backend_id != slot->requester_backend
		|| context->request_id != slot->request_id)
		return false;

	slot_generation = context->slot_generation;
	expected = terminal_state;
	if (!pg_atomic_compare_exchange_u32(
			&slot->state, &expected, CLUSTER_LMS_CR_R4_SHIPPING))
		return false;
	pg_read_barrier();

	memset(frame, 0, sizeof(frame));
	header->request_id = slot->request_id;
	header->epoch = slot->epoch;
	header->sender_node = cluster_node_id;
	header->requester_backend_id = slot->requester_backend;
	header->transition_id = slot->transition_id;
	header->status = reply_status;
	GcsBlockReplyHeaderSetForwardingMasterNode(header, slot->reply_master_node);
	if (terminal_state == CLUSTER_LMS_CR_R4_READY_FULL) {
		header->page_lsn = slot->r4.copied_page_lsn;
		memcpy(frame + sizeof(*header), slot->result_page, BLCKSZ);
	}
	header->checksum = cluster_gcs_block_compute_checksum(frame + sizeof(*header));

	if (!cr_server_r4_identity_open_matches(
			&context->admission, slot->requester_node,
			slot->r4.requester_capability_generation)
		|| !cluster_semantic_activation_recheck(&context->admission)
		|| (terminal_state == CLUSTER_LMS_CR_R4_READY_FULL
			&& cluster_write_fence_enforcing() && !cluster_write_fence_allowed()))
		return cr_server_r4_release_terminal(slot_index, slot_generation);

	if (terminal_reason != CLUSTER_CR_BUILD_NONE)
		cluster_r4_observe_refusal(CLUSTER_R4_REFUSAL_HOLDER_SHIP, terminal_reason, &slot->tag,
								   slot->request_id, slot->epoch, slot->requester_node,
								   slot->reply_master_node, slot->read_scn);

	if (slot->requester_node == cluster_node_id) {
		if (!cluster_ic_envelope_build(
				&envelope, PGRAC_IC_MSG_GCS_BLOCK_REPLY,
				(uint32)cluster_node_id, (uint32)cluster_node_id,
				frame, sizeof(frame)))
			send_result = CLUSTER_IC_SEND_HARD_ERROR;
		else
			send_result = cluster_ic_dispatch_envelope(
				&envelope, frame, cluster_node_id)
					? CLUSTER_IC_SEND_DONE
					: CLUSTER_IC_SEND_HARD_ERROR;
	}
	else
		send_result = cluster_ic_send_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY, slot->requester_node, frame,
			sizeof(frame));
	switch (send_result) {
		case CLUSTER_IC_SEND_DONE:
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			return cr_server_r4_release_terminal(slot_index, slot_generation);
		case CLUSTER_IC_SEND_NOT_ADMITTED:
			expected = CLUSTER_LMS_CR_R4_SHIPPING;
			(void)pg_atomic_compare_exchange_u32(
				&slot->state, &expected, terminal_state);
			return false;
		case CLUSTER_IC_SEND_HARD_ERROR:
			cluster_lms_data_plane_close_peer_now(slot->requester_node);
			return cr_server_r4_release_terminal(slot_index, slot_generation);
	}
	return false;
}

#ifdef USE_CLUSTER_UNIT
bool
cluster_cr_server_test_r4_claim_queued(uint32 slot_index)
{
	return cr_server_r4_claim_queued(slot_index);
}

bool
cluster_cr_server_test_r4_build_step(uint32 slot_index)
{
	return cr_server_r4_build_step(slot_index);
}

bool
cluster_cr_server_test_r4_send_foreign_undo(uint32 slot_index)
{
	return cr_server_r4_send_foreign_undo(slot_index);
}

bool
cluster_cr_server_test_r4_freeze_foreign_generation(
	uint32 slot_index, uint32 physical_generation)
{
	ClusterLmsCrSlot *slot;
	ClusterR4CrWorkerContext *context;

	if (CrServerShared == NULL || slot_index >= CLUSTER_LMS_CR_SLOTS
		|| physical_generation == UINT32_MAX)
		return false;
	slot = &CrServerShared->slots[slot_index];
	context = &CrServerR4Contexts[slot_index];
	if (pg_atomic_read_u32(&slot->state)
			!= CLUSTER_LMS_CR_R4_NEED_UNDO
		|| !context->in_use || context->builder_forgotten
		|| context->foreign_physical_generation_frozen
		|| context->foreign_scur_phase != CLUSTER_R4_FOREIGN_SCUR_UNUSED
		|| context->slot_generation != slot->r4.slot_generation
		|| context->builder_incarnation
			   != slot->r4.owner.builder_incarnation
		|| context->requester_node != slot->requester_node
		|| context->requester_backend_id != slot->requester_backend
		|| context->request_id != slot->request_id)
		return false;
	context->expected_foreign_physical_generation = physical_generation;
	pg_write_barrier();
	context->foreign_physical_generation_frozen = true;
	return true;
}

bool
cluster_cr_server_test_r4_ship_terminal(uint32 slot_index)
{
	return cr_server_r4_ship_terminal(slot_index);
}

void
cluster_cr_server_test_r4_reset_contexts(void)
{
	memset(CrServerR4Contexts, 0, sizeof(CrServerR4Contexts));
}

bool
cluster_cr_server_test_r4_context_matches(
	uint32 slot_index, bool expect_present, uint64 slot_generation,
	uint64 builder_incarnation, const ClusterSemanticAdmissionToken *admission)
{
	ClusterR4CrWorkerContext zero;
	ClusterR4CrWorkerContext *context;
	ClusterLmsCrSlot *slot;

	if (slot_index >= CLUSTER_LMS_CR_SLOTS)
		return false;
	context = &CrServerR4Contexts[slot_index];
	if (!expect_present) {
		memset(&zero, 0, sizeof(zero));
		return memcmp(context, &zero, sizeof(zero)) == 0;
	}
	if (CrServerShared == NULL || admission == NULL)
		return false;
	slot = &CrServerShared->slots[slot_index];
	return context->in_use && context->slot_generation == slot_generation
		   && context->builder_incarnation == builder_incarnation
		   && context->requester_node == slot->requester_node
		   && context->requester_backend_id == slot->requester_backend
		   && context->request_id == slot->request_id
		   && memcmp(&context->admission, admission, sizeof(*admission)) == 0;
}
#endif

/*
 * cluster_lms_undo_fetch_submit — CONTROL-plane park (spec-6.12i D-i1).
 *
 *	Park an undo-TT fetch request.  The caller branches on the undo-fetch
 *	flag BEFORE any GRD / holder logic can interpret the synthetic tag.
 *	false = wave GUC off on this node / malformed synthetic tag / no capacity:
 *	the caller replies the fail-closed DENIED immediately (the requester keeps
 *	53R97 — Rule 8.A).
 */
bool
cluster_lms_undo_fetch_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];

		if (!cr_server_reserve_legacy_slot(slot, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = InvalidScn; /* no snapshot semantics on this kind */
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_FETCH;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		slot->undo_xid = InvalidTransactionId;
		slot->undo_owner = -1; /* D4-6: never inherit a recycled slot's owner */
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * cluster_lms_undo_verdict_submit — CONTROL-plane park (spec-6.12i D-i4 /
 * spec-6.15 D4 / spec-5.22d D4-6).
 *
 *	Park a complete-scan verdict request (kinds 2/3) or a kind-4 dead-owner
 *	AUTHORITY verdict request.  The asked-for xid rides the widened
 *	watermark carrier: any non-zero upper 32 bits or a non-normal 32-bit
 *	value is a malformed carrier — refuse (the caller replies the
 *	fail-closed DENIED; the requester keeps 53R97, Rule 8.A).  The synthetic
 *	tag is validated for shape only; on kinds 2/3 the verdict scan is
 *	complete over ALL self-owned segments (the tag's segment does not scope
 *	the answer) and the owner carrier MUST be empty, on kind 4 the dead
 *	OWNER is decoded from tag.relNumber (range-checked, never self — the
 *	live-owner kinds answer own xids) and the serve-side triple check owns
 *	all trust decisions.
 */
bool
cluster_lms_undo_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	int32 wire_owner = -1;
	uint64 carrier = 0;
	TransactionId pair_xid = InvalidTransactionId;
	SCN pair_scn = InvalidScn;
	bool freshref_pair;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	freshref_pair = GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(fwd);
	if (freshref_pair) {
		if (!cluster_cr_server_freshref_c1b_pair_request_decode(
				fwd, fwd->original_requester_node, cluster_node_id,
				cluster_epoch_get_current(), MaxBackends, &segment_id, &pair_xid,
				&block_no, &pair_scn))
			return false;
	} else if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	if (!freshref_pair && GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(fwd)) {
		/* kind 4: decode + range-check the owner carrier; a request naming
		 * US as the (dead) owner is malformed — our own xids are answered
		 * by the live-owner kinds, never by an authority detour. */
		if (!GcsBlockUndoAuthorityFetchTagDecodeOwner(fwd->tag, &wire_owner))
			return false;
		if (wire_owner < 0 || wire_owner >= CLUSTER_MAX_NODES || wire_owner == cluster_node_id)
			return false;
	} else if (!freshref_pair && fwd->tag.relNumber != (RelFileNumber)0)
		return false; /* owner-served kinds must leave the carrier empty */

	if (!freshref_pair) {
		carrier = (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		if (carrier > (uint64)PG_UINT32_MAX
			|| !TransactionIdIsNormal((TransactionId)carrier))
			return false;
	}

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];

		if (!cr_server_reserve_legacy_slot(slot, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = freshref_pair ? pair_scn : InvalidScn;
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		slot->undo_xid = freshref_pair ? pair_xid : (TransactionId)carrier;
		slot->undo_authoritative = GcsBlockForwardPayloadIsUndoVerdictAuthoritative(fwd);
		slot->undo_owner = wire_owner; /* -1 on kinds 2/3; the dead owner on kind 4 */
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * spec-7.1 D3-b (Q-D3b1): a MultiXactId is the same 32-bit width as a
 * TransactionId, so the multi-verdict request carries the asked-for MXID in
 * the SAME slot->undo_xid field the single-xid verdict uses -- disambiguated
 * purely by slot->req_kind (KIND_UNDO_MULTI_VERDICT).  Assert the width so a
 * future MultiXactId widening cannot silently truncate the carrier.
 */
StaticAssertDecl(sizeof(MultiXactId) == sizeof(TransactionId),
				 "spec-7.1 D3-b reuses the undo_xid carrier width for MultiXactId (Q-D3b1)");

/*
 * cluster_lms_undo_multi_verdict_submit — LMON dispatch side (spec-7.1 D3-b).
 *
 *	Park a validated multi member-verdict request.  Same shape as the single
 *	verdict submit, but the widened watermark carrier holds a MultiXactId
 *	(not a TransactionId): a non-zero upper 32 bits or an invalid mxid is a
 *	malformed carrier — refuse (the caller replies the fail-closed DENIED; the
 *	requester keeps 53R97, Rule 8.A).  The synthetic tag is validated for
 *	shape only; the member scan is complete over the multi's own pg_multixact.
 */
bool
cluster_lms_undo_multi_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;
	uint64 carrier;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	carrier = (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
	if (carrier > (uint64)PG_UINT32_MAX || !MultiXactIdIsValid((MultiXactId)carrier))
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];

		/* Reserve producer-only FILLING first (spec-7.1 integration review):
		 * landing directly on PENDING would let the LMS drain acquire the
		 * slot before the request fields below are written. */
		if (!cr_server_reserve_legacy_slot(slot, CLUSTER_LMS_CR_FILLING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = InvalidScn; /* the carrier held the mxid, not a snapshot */
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		slot->undo_xid = (TransactionId)carrier; /* Q-D3b1: carries the MXID */
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * lms_undo_fetch_serve — LMS side of one KIND_UNDO_FETCH slot (spec-6.12i).
 *
 *	Samples the live authority triple FIRST, then reads the block: the
 *	watermark is then conservative relative to the shipped content (every
 *	TT stamp the authority claims coverage for is already in the bytes the
 *	requester receives; a stamp landing between sample and read only makes
 *	the content newer than claimed, which is additive and safe — a stamp
 *	can never be retracted).  live_hwm_lsn = GetFlushRecPtr() is sound as
 *	the "durable AND TT-applied" high-water because the durable TT stamp is
 *	a pre-commit targeted pwrite issued BEFORE the commit record is even
 *	inserted (cluster_tt_durable.h): any commit whose record LSN is at or
 *	below the flush pointer has a peer-readable TT stamp.
 *
 *	Only block 0 (the TT-bearing segment header) is served: undo DATA
 *	blocks can lag their pool image under the spec-3.25 D1b keep-clean
 *	WAL deferral, so shipping them from the file would not be origin-fresh
 *	— refuse fail-closed (feature #119 full undo-block CF is the
 *	downstream forward of this slice).
 *
 *	true = result_page holds the block and slot->undo_auth the co-sampled
 *	triple; false = refuse (caller ships DENIED — requester keeps 53R97).
 */
static bool
lms_undo_fetch_serve(ClusterLmsCrSlot *slot)
{
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (slot->undo_block_no != 0)
		return false;
	if (slot->undo_segment_id == 0 || slot->undo_segment_id > UINT16_MAX)
		return false;

	/* Co-sample the authority triple BEFORE the content read (see above). */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();
	/* PGRAC: spec-7.1a D3 -- co-sample the origin SCN clock with the same
	 * pre-content-read ordering (a stamp landing after the sample only makes
	 * the content newer than claimed; additive and safe). */
	slot->undo_auth.authority_scn = cluster_scn_current();

	/* Serve only SELF-owned undo: the owner derives from this node's own
	 * id, never from the wire (a forged request cannot redirect the read). */
	return cluster_undo_smgr_read_block(cluster_undo_intent_for_owner((uint8)(cluster_node_id + 1)),
										slot->undo_segment_id, (uint8)(cluster_node_id + 1),
										slot->undo_block_no, slot->result_page);
}

/*
 * lms_resolve_own_xid_verdict — shared core resolving ONE own xid's terminal
 * verdict over this node's COMPLETE durable TT + CLOG (spec-6.12i D-i4 /
 * spec-6.15 D4 / spec-7.1 D1-serve, D3-b).
 *
 *	Used by BOTH the single-xid verdict serve and the multi member-verdict
 *	serve so the terminal decision lives in exactly one place (no fork).  The
 *	caller must have already gated cluster_xid_is_mine(xid) and co-sampled the
 *	live authority triple (the coverage claim never exceeds the scanned durable
 *	state).  Fills *out_verdict + the scn/wrap fields on a proven terminal and
 *	returns a reason so each caller attributes its OWN census (this core bumps
 *	NO counter):
 *	  RESOLVED_SCN        exact COMMITTED match: CLOG must confirm (C1b — the
 *	                      TT stamp is pre-commit, a stamp without a commit
 *	                      record is in-doubt).  Acceptance-gate PASS ->
 *	                      COMMITTED_EXACT{commit_scn, wrap}; a wrap-suspect scn
 *	                      (spec-7.1a hardening) ships COMMITTED_BELOW_HORIZON{H}
 *	                      over max(scn, gated-recycle horizon) instead of
 *	                      refusing (no gated recycle -> refuse, like zero-match).
 *	  RECYCLED_ZERO_MATCH the slot is provably gone: the spec-3.22 retention
 *	                      origin legs (a)-(d) + the monotonic max recycle
 *	                      horizon sampled AFTER the scan, then CLOG decides:
 *	                        COMMITTED -> COMMITTED_BELOW_HORIZON{H} (recycle was
 *	                        horizon-gated, so the lost commit_scn is <= H);
 *	                        explicit ABORTED -> ABORTED; neither -> refuse.
 *	  XID_MATCH_INVALID_SCN  spec-7.1 D1 serve: our own xid matched but carries
 *	                      no stamped commit_scn (delayed-cleanout window).  8.A
 *	                      positive proof only: ONLY an explicit CLOG abort
 *	                      upgrades to a positive ABORTED (the pure, unit-tested
 *	                      cluster_cr_server_invalid_scn_verdict); a committed-
 *	                      but-unstamped / in-flight / 2PC / crashed-without-abort
 *	                      xid stays fail-closed (we never fabricate a scn).
 *	  anything else       AMBIGUOUS_WRAP / SCAN_UNAVAILABLE -> refuse.
 */
typedef enum LmsOwnXidReason {
	LMS_OWN_XID_PROVEN = 0,			/* out_* holds a proven terminal */
	LMS_OWN_XID_PROVEN_UPGRADE,		/* proven ABORTED via the invalid_scn CLOG upgrade */
	LMS_OWN_XID_PROVEN_IN_PROGRESS, /* exact fresh-ref binding + origin ProcArray live */
	LMS_OWN_XID_REFUSE_OTHER,		/* not-committed / wrap-suspect / retention-fail / ambiguous */
	LMS_OWN_XID_REFUSE_ZERO_MATCH,	/* recycled 0-match with no explicit CLOG terminal */
	LMS_OWN_XID_REFUSE_INVALID_SCN	/* delayed-cleanout, not provably aborted */
} LmsOwnXidReason;

/* A new reader's not-yet-published SCN prevents NEW reclamation; it does
 * not revoke an already gated recycle.  The origin's complete scan must
 * precede this sample of the historical upper bound.  Its caller still
 * proves the terminal outcome and retains exact/bound consumer checks.
 * Never substitute the current clock or current reader horizon here. */
static bool
lms_own_xid_recycle_bound(SCN *out_horizon)
{
	SCN horizon;

	*out_horizon = InvalidScn;
	if (cluster_node_id < 0 || !cluster_undo_retention_horizon_enabled
		|| cluster_tt_slot_retention_off_recycle_count() != 0)
		return false;
	horizon = cluster_tt_slot_max_recycle_horizon();
	if (!SCN_VALID(horizon))
		return false;
	*out_horizon = horizon;
	return true;
}

/* Request-local observation only; none of these fields grants authority. */
typedef struct LmsFreshrefPairDiagnostic {
	const char *predicate;
	bool native_sampled;
	bool no_raw_reuse;
	bool own_xid;
	bool scan_sampled;
	ClusterTTDurableResolve resolve;
	uint16 matched_segment;
	uint16 matched_slot;
	uint16 matched_wrap;
	SCN resolved_scn;
	bool clog_sampled;
	TransactionId clog_floor;
	int raw_status;
	bool retention_sampled;
	bool retention_ok;
	SCN horizon_scn;
} LmsFreshrefPairDiagnostic;

static bool
lms_freshref_pair_detail_admit(uint32 *emitted)
{
	if (*emitted >= 64)
		return false;
	(*emitted)++;
	return true;
}

static const char *
lms_freshref_pair_refusal_predicate(const volatile LmsFreshrefPairDiagnostic *d,
									uint32 expected_segment, uint32 expected_slot, SCN proposed_scn)
{
	if (!d->no_raw_reuse)
		return "NATIVE_REUSE_GUARD_DISABLED";
	if (!d->own_xid)
		return "ORIGIN_NOT_OWNED";
	switch (d->resolve) {
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
		return "TT_SCAN_UNAVAILABLE";
	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
		return "TT_AMBIGUOUS";
	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
		return "TT_UNSTAMPED";
	default:
		break;
	}
	if (!d->clog_sampled)
		return "CLOG_TRUNCATED";
	if (d->raw_status != TRANSACTION_STATUS_COMMITTED)
		return "CLOG_NOT_COMMITTED";
	if (d->resolve == CLUSTER_TT_DURABLE_RESOLVED_SCN) {
		if (d->matched_segment != expected_segment)
			return "TT_SEGMENT_MISMATCH";
		if ((uint32)d->matched_slot + 1 != expected_slot)
			return "TT_SLOT_MISMATCH";
		if (!SCN_VALID(d->resolved_scn) || d->resolved_scn != proposed_scn)
			return "TT_SCN_MISMATCH";
	} else if (d->resolve == CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH) {
		if (!d->retention_ok || !SCN_VALID(d->horizon_scn))
			return "RETENTION_UNAVAILABLE";
		return "RETENTION_NOT_COVERED";
	}
	return "POLICY_REFUSED";
}

/* S8-815PRE-FRESHREF-C1B-01: exact retained-page pairing.  The native
 * prehistory reader fence continuously covers the complete durable scan,
 * literal CLOG C1b sample and (for a zero-match) frozen retention horizon.
 * This is deliberately separate from the ordinary zero-match path: it may
 * echo only the request's exact cached SCN, never reinterpret a horizon as
 * an exact value. */
static LmsOwnXidReason
lms_resolve_own_xid_freshref_c1b_pair(TransactionId xid, uint32 expected_segment_id,
									  uint32 expected_tt_slot_id, SCN proposed_scn,
									  uint8 *out_verdict, SCN *out_commit_scn, SCN *out_horizon_scn,
									  uint16 *out_wrap,
									  volatile LmsFreshrefPairDiagnostic *diagnostic)
{
	volatile LmsFreshrefPairDiagnostic ignored;
	ClusterUndoVerdictKind pair_verdict
		= CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	ClusterTTDurableResolve resolve = CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE;
	SCN resolved_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 matched_segment = 0;
	uint16 matched_slot = 0;
	uint16 matched_wrap = 0;
	int raw_clog_status = TRANSACTION_STATUS_IN_PROGRESS;
	bool retention_ok = false;

	if (diagnostic == NULL)
		diagnostic = &ignored;
	memset((void *)diagnostic, 0, sizeof(*diagnostic));
	diagnostic->predicate = "BAD_INPUT";
	diagnostic->raw_status = -1;
	*out_verdict = 0;
	*out_commit_scn = InvalidScn;
	*out_horizon_scn = InvalidScn;
	*out_wrap = 0;

	if (!TransactionIdIsNormal(xid) || expected_segment_id == 0
		|| expected_segment_id > UINT16_MAX || expected_tt_slot_id < 1
		|| expected_tt_slot_id > TT_SLOTS_PER_SEGMENT
		|| !SCN_VALID(proposed_scn))
		return LMS_OWN_XID_REFUSE_OTHER;

	PG_TRY(freshref_pair);
	{
		bool no_raw_reuse_window;
		bool xid_is_mine;

		cluster_cr_native_prehistory_reader_lock();
		/* The pair is restricted to a derivable cluster-era xid below.  A
		 * clean formation may legitimately have no native-era prehistory, so
		 * covered_hw is not an admission predicate.  The existing reader
		 * fence plus its one-way disable is the exact no-raw-reuse window. */
		no_raw_reuse_window = !cluster_cr_native_prehistory_disabled();
		xid_is_mine = no_raw_reuse_window && cluster_xid_is_mine(xid);
		diagnostic->native_sampled = true;
		diagnostic->no_raw_reuse = no_raw_reuse_window;
		diagnostic->own_xid = xid_is_mine;

		if (xid_is_mine) {
			resolve = cluster_tt_slot_durable_resolve_by_xid(
				xid, CLUSTER_TT_WRAP_ANY, &resolved_scn, &matched_segment,
				&matched_slot, &matched_wrap);
			diagnostic->scan_sampled = true;
			diagnostic->resolve = resolve;
			diagnostic->matched_segment = matched_segment;
			diagnostic->matched_slot = matched_slot;
			diagnostic->matched_wrap = matched_wrap;
			diagnostic->resolved_scn = resolved_scn;
			if (resolve == CLUSTER_TT_DURABLE_RESOLVED_SCN
				|| resolve == CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH) {
				XLogRecPtr clog_lsn = InvalidXLogRecPtr;
				TransactionId clog_floor;
				bool clog_sampled = false;

				LWLockAcquire(XactTruncationLock, LW_SHARED);
				clog_floor = ShmemVariableCache->oldestClogXid;
				diagnostic->clog_floor = clog_floor;
				if (!TransactionIdPrecedes(xid, clog_floor)) {
					raw_clog_status = TransactionIdGetStatus(xid, &clog_lsn);
					clog_sampled = true;
					diagnostic->clog_sampled = true;
					diagnostic->raw_status = raw_clog_status;
				}
				LWLockRelease(XactTruncationLock);

				if (clog_sampled
					&& resolve == CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH
					&& raw_clog_status == TRANSACTION_STATUS_COMMITTED) {
					retention_ok = lms_own_xid_recycle_bound(&horizon_scn);
					diagnostic->retention_sampled = true;
					diagnostic->retention_ok = retention_ok;
					diagnostic->horizon_scn = horizon_scn;
				}

				if (clog_sampled)
					pair_verdict = cluster_cr_server_freshref_c1b_pair_verdict(
						true, true, expected_segment_id, expected_tt_slot_id,
						no_raw_reuse_window, raw_clog_status, resolve,
						matched_segment, matched_slot, resolved_scn, proposed_scn,
						retention_ok, horizon_scn);
			}
		}
		cluster_cr_native_prehistory_reader_unlock();
	}
	PG_CATCH(freshref_pair);
	{
		diagnostic->predicate = "CORE_EXCEPTION";
		/* The pair owns no persistent state.  Release both the native fence
		 * and any CLOG/retention LWLocks before the LMS converts the error to
		 * a fail-closed DENIED reply. */
		HOLD_INTERRUPTS();
		LWLockReleaseAll();
		RESUME_INTERRUPTS();
		PG_RE_THROW();
	}
	PG_END_TRY(freshref_pair);

	if (pair_verdict != CLUSTER_UNDO_VERDICT_COMMITTED_EXACT) {
		diagnostic->predicate = lms_freshref_pair_refusal_predicate(
			diagnostic, expected_segment_id, expected_tt_slot_id, proposed_scn);
		return LMS_OWN_XID_REFUSE_OTHER;
	}
	diagnostic->predicate = "PROVEN";

	*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
	*out_commit_scn = proposed_scn;
	*out_wrap = resolve == CLUSTER_TT_DURABLE_RESOLVED_SCN ? matched_wrap : 0;
	return LMS_OWN_XID_PROVEN;
}

/* Backend-local consumer of the same origin C1b pair used by the LMS wire
 * handler.  It publishes no authority or cache entry: callers receive only
 * the exact COMMITTED conjunction, with the retained SCN remaining the
 * proposed value. */
bool
cluster_cr_server_local_freshref_c1b_pair_exact(
	TransactionId xid, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, SCN proposed_scn, uint16 *out_wrap)
{
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;
	LmsOwnXidReason reason;

	if (out_wrap != NULL)
		*out_wrap = 0;
	reason = lms_resolve_own_xid_freshref_c1b_pair(xid, expected_segment_id, expected_tt_slot_id,
												   proposed_scn, &verdict, &commit_scn,
												   &horizon_scn, &wrap, NULL);
	if (reason != LMS_OWN_XID_PROVEN
		|| verdict != (uint8) CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT
		|| commit_scn != proposed_scn || SCN_VALID(horizon_scn))
		return false;
	if (out_wrap != NULL)
		*out_wrap = wrap;
	return true;
}

static LmsOwnXidReason
lms_resolve_own_xid_verdict(TransactionId xid, uint32 expected_segment_id,
							uint32 expected_tt_slot_id, bool allow_live, uint8 *out_verdict,
							SCN *out_commit_scn, SCN *out_horizon_scn, uint16 *out_wrap)
{
	SCN scn = InvalidScn;
	SCN horizon = InvalidScn;
	uint16 wrap = 0;
	uint16 matched_segment = 0;
	uint16 matched_slot = 0;
	ClusterTTDurableResolve resolve;

	*out_verdict = 0;
	*out_commit_scn = InvalidScn;
	*out_horizon_scn = InvalidScn;
	*out_wrap = 0;

	resolve = cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY, &scn,
												 &matched_segment, &matched_slot, &wrap);
	switch (resolve) {
	case CLUSTER_TT_DURABLE_RESOLVED_SCN: {
		bool did_commit = TransactionIdDidCommit(xid);
		bool did_abort = false;
		bool xid_is_mine = false;
		bool xid_is_in_progress = false;
		bool durable_binding_stable = false;
		bool exact_binding = false;
		bool exact_live = false;
		bool terminal_rechecked = false;

		/*
		 * RESOLVED_SCN is stamped before the CLOG terminal record.  Only an
		 * authoritative fresh ref carrying the exact physical segment and
		 * 1-based slot may widen the old !commit/OTHER bucket.  DidAbort is
		 * sampled only inside that gate; otherwise crash-lost and wrong
		 * bindings remain fail-closed.
		 */
		if (!did_commit) {
			SCN confirm_scn = InvalidScn;
			uint16 confirm_segment = 0;
			uint16 confirm_slot = 0;
			uint16 confirm_wrap = 0;

			xid_is_mine = allow_live && cluster_xid_is_mine(xid);
			exact_binding
				= xid_is_mine && expected_segment_id > 0 && expected_segment_id <= UINT16_MAX
				  && expected_segment_id == (uint32)matched_segment && expected_tt_slot_id >= 1
				  && expected_tt_slot_id <= TT_SLOTS_PER_SEGMENT
				  && expected_tt_slot_id == (uint32)matched_slot + 1;
			if (exact_binding) {
				did_abort = TransactionIdDidAbort(xid);
				if (!did_abort)
					xid_is_in_progress = TransactionIdIsInProgress(xid);

				/*
				 * The xid can finish between the first DidCommit sample and
				 * the following ProcArray sample.  In that interval both old
				 * observations are false even though CLOG is now terminal.  A
				 * second terminal sample closes that TOCTOU, but only while the
				 * request's exact durable segment/slot/scn binding remains
				 * byte-identical below.  No negative observation is promoted.
				 */
				if (!did_abort && !xid_is_in_progress) {
					bool recheck_commit = TransactionIdDidCommit(xid);
					bool recheck_abort = false;

					if (!recheck_commit)
						recheck_abort = TransactionIdDidAbort(xid);
					if (recheck_commit || recheck_abort) {
						did_commit = recheck_commit;
						did_abort = recheck_abort;
						terminal_rechecked = true;
					}
				}
				if (xid_is_in_progress || terminal_rechecked) {
					durable_binding_stable
						= cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY,
																	 &confirm_scn, &confirm_segment,
																	 &confirm_slot, &confirm_wrap)
							  == CLUSTER_TT_DURABLE_RESOLVED_SCN
						  && confirm_segment == matched_segment && confirm_slot == matched_slot
						  && confirm_wrap == wrap && confirm_scn == scn;
					if (terminal_rechecked && !durable_binding_stable) {
						did_commit = false;
						did_abort = false;
					}
				}
			}
		exact_live = cluster_cr_server_live_binding_exact(
			xid_is_mine, expected_segment_id, expected_tt_slot_id, matched_segment,
			matched_slot, xid_is_in_progress, durable_binding_stable);
		}

		switch (cluster_cr_server_resolved_scn_verdict(did_commit, did_abort, exact_live)) {
		case CLUSTER_UNDO_VERDICT_COMMITTED_EXACT:
			break;
		case CLUSTER_UNDO_VERDICT_ABORTED:
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
			return LMS_OWN_XID_PROVEN_UPGRADE;
		case CLUSTER_UNDO_VERDICT_IN_PROGRESS:
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS;
			return LMS_OWN_XID_PROVEN_IN_PROGRESS;
		case CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED:
		default:
			return LMS_OWN_XID_REFUSE_OTHER;
		}
	}
		if (cluster_cr_accept_resolved_scn(scn)) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT;
			*out_commit_scn = scn;
			*out_wrap = wrap;
			return LMS_OWN_XID_PROVEN;
		}

		/*
		 * PGRAC: spec-7.1a hardening -- wrap-suspect stamped scn (below the
		 * retention horizon), reached routinely now that the requester
		 * finalizes EVERY shipped COMMITTED stamp here instead of concluding
		 * on the fetch fast leg.  The EXACT value cannot be shipped (a
		 * same-valued xid recurrence across TT wrap could own a different
		 * scn), but "committed at/below a FROZEN bound" still can:
		 *   - a LIVE recurrence would be a second by-value match ->
		 *     AMBIGUOUS_WRAP (refused below), so the single live match has
		 *     no live rival;
		 *   - a RECYCLED recurrence's lost scn is at/below the max gated-
		 *     recycle horizon (the zero-match arm's own bound);
		 *   - this slot's own stamped scn bounds itself.
		 * Ship BELOW_HORIZON over the max of both candidates -- the same
		 * frozen, non-clock-chasing consumer contract as the zero-match arm
		 * (never cached; judged against the requester's read_scn, leg (e)).
		 * No gated recycle this incarnation -> the recycled-recurrence
		 * candidate is unboundable -> refuse, exactly like zero-match.
		 * Absorbed into the shared core (spec-7.1 D3-b integration) so BOTH
		 * the single-xid serve and each multi member-verdict resolve through
		 * exactly this bound -- no serve forks on the wrap-suspect leg.
		 */
		if (!lms_own_xid_recycle_bound(&horizon))
			return LMS_OWN_XID_REFUSE_OTHER;
		if (scn_time_cmp(scn, horizon) > 0)
			horizon = scn;
		*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
		*out_horizon_scn = horizon;
		return LMS_OWN_XID_PROVEN;

	case CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH:
		/*
		 * TT-P013-RULE25-C0: the committed-only scan's zero-match also
		 * contains exact ACTIVE and ABORTED cluster-era xids.  Before the
		 * first raw-xid reuse, the existing native-prehistory consume/disable
		 * drain makes {origin, raw xid} an incarnation-safe key: DISABLE takes
		 * the same lock EXCLUSIVE and cannot ACK (or open epoch allocation)
		 * until this SHARED reader has selected its verdict.
		 *
		 * The full durable scan intentionally stays outside this drain.  If a
		 * DISABLE won during/after it, the in-lock covered/disabled recheck
		 * fails; if this reader wins, raw reuse cannot begin until it releases.
		 * A nonzero covered_hw is only the current-boot "drain armed" witness,
		 * NEVER a numeric xid bound (do not use native_prehistory_provable).
		 *
		 * Read literal raw CLOG under XactTruncationLock.  DidAbort recursively
		 * follows SUB_COMMITTED, which C0 must keep UNKNOWN.  Only raw COMMITTED
		 * may compose with the unchanged retention/bound branch; SUB_COMMITTED,
		 * a gone ProcArray entry, truncation and every other sampled doubt refuse
		 * before that branch can reinterpret the CLOG byte.
		 */
		if (allow_live && expected_segment_id > 0 && expected_segment_id <= UINT16_MAX
			&& expected_tt_slot_id >= 1 && expected_tt_slot_id <= TT_SLOTS_PER_SEGMENT
			&& cluster_cr_native_prehistory_covered_hw() != 0) {
			volatile ClusterUndoVerdictKind c0_verdict
				= CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
			volatile bool c0_hard_refuse = false;

			PG_TRY(c0_window);
			{
				bool no_raw_reuse_window = false;
				bool xid_is_mine;

				cluster_cr_native_prehistory_reader_lock();
				no_raw_reuse_window = cluster_cr_native_prehistory_covered_hw() != 0
					  && !cluster_cr_native_prehistory_disabled();
				xid_is_mine = no_raw_reuse_window && cluster_xid_is_mine(xid);

				if (xid_is_mine) {
					bool clog_sampled = false;
					bool clog_is_committed = false;
					bool clog_is_aborted = false;
					bool clog_is_in_progress = false;
					bool xid_is_in_progress = false;

					LWLockAcquire(XactTruncationLock, LW_SHARED);
					if (!TransactionIdPrecedes(xid, ShmemVariableCache->oldestClogXid)) {
						XLogRecPtr clog_lsn = InvalidXLogRecPtr;
						XidStatus raw_status = TransactionIdGetStatus(xid, &clog_lsn);

						clog_sampled = true;
						clog_is_committed = raw_status == TRANSACTION_STATUS_COMMITTED;
						clog_is_aborted = raw_status == TRANSACTION_STATUS_ABORTED;
						clog_is_in_progress = raw_status == TRANSACTION_STATUS_IN_PROGRESS;
					} else {
						/* A truncated CLOG byte is doubt, never a recursive fallback. */
						c0_hard_refuse = true;
					}
					LWLockRelease(XactTruncationLock);

					/* ProcArray need not retain the truncation guard; the native drain
					 * still protects raw-xid uniqueness through verdict selection. */
					if (clog_is_in_progress)
						xid_is_in_progress = TransactionIdIsInProgress(xid);
					c0_verdict = cluster_cr_server_c0_zero_match_verdict(
						allow_live, xid_is_mine, expected_segment_id, expected_tt_slot_id,
						no_raw_reuse_window, clog_is_committed, clog_is_aborted,
						clog_is_in_progress, xid_is_in_progress);
					if (clog_sampled
						&& c0_verdict == CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED
						&& !clog_is_committed)
						c0_hard_refuse = true;
				}
				cluster_cr_native_prehistory_reader_unlock();
			}
			PG_CATCH(c0_window);
			{
				/* CLOG/ProcArray can ERROR with internal LWLocks held.  Every
				 * C0 serve entry is lock-free, so release the complete C0 stack
				 * before the long-lived LMS converts the error to DENIED. */
				HOLD_INTERRUPTS();
				LWLockReleaseAll();
				RESUME_INTERRUPTS();
				PG_RE_THROW();
			}
			PG_END_TRY(c0_window);

			switch (c0_verdict) {
			case CLUSTER_UNDO_VERDICT_ABORTED:
				*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
				return LMS_OWN_XID_PROVEN;
			case CLUSTER_UNDO_VERDICT_IN_PROGRESS:
				*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS;
				return LMS_OWN_XID_PROVEN_IN_PROGRESS;
			case CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED:
			default:
				break;
			}
			if (c0_hard_refuse)
				return LMS_OWN_XID_REFUSE_ZERO_MATCH;
		}
		if (!lms_own_xid_recycle_bound(&horizon))
			return LMS_OWN_XID_REFUSE_OTHER;
		if (TransactionIdDidCommit(xid)) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON;
			*out_horizon_scn = horizon;
			return LMS_OWN_XID_PROVEN;
		}
		if (TransactionIdDidAbort(xid)) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
			return LMS_OWN_XID_PROVEN;
		}
		return LMS_OWN_XID_REFUSE_ZERO_MATCH; /* neither explicit CLOG state -> refuse */

	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
		if (cluster_cr_server_invalid_scn_verdict(TransactionIdDidAbort(xid))
			== CLUSTER_CR_INVALID_SCN_ABORTED) {
			*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_ABORTED;
			return LMS_OWN_XID_PROVEN_UPGRADE;
		}
		return LMS_OWN_XID_REFUSE_INVALID_SCN;

	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
	default:
		return LMS_OWN_XID_REFUSE_OTHER;
	}
}

#ifdef USE_CLUSTER_UNIT
/* Execute the real static resolver from the focused C0 fixture. */
ClusterUndoVerdictKind
cluster_cr_server_test_own_xid_verdict(TransactionId xid, uint32 expected_segment_id,
										 uint32 expected_tt_slot_id, bool authoritative)
{
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;

	(void)lms_resolve_own_xid_verdict(xid, expected_segment_id, expected_tt_slot_id,
										authoritative, &verdict, &commit_scn, &horizon_scn,
										&wrap);
	switch (verdict) {
	case CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT:
		return CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	case CLUSTER_GCS_UNDO_VERDICT_COMMITTED_BELOW_HORIZON:
		return CLUSTER_UNDO_VERDICT_COMMITTED_BOUND;
	case CLUSTER_GCS_UNDO_VERDICT_ABORTED:
		return CLUSTER_UNDO_VERDICT_ABORTED;
	case CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS:
		return CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	default:
		return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	}
}

ClusterUndoVerdictResult
cluster_cr_server_test_own_xid_pair_verdict(TransactionId xid,
											uint32 expected_segment_id,
											uint32 expected_tt_slot_id,
											SCN proposed_scn)
{
	ClusterUndoVerdictResult result
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED,
			.commit_scn = InvalidScn,
			.wrap = 0 };
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;

	if (lms_resolve_own_xid_freshref_c1b_pair(xid, expected_segment_id, expected_tt_slot_id,
											  proposed_scn, &verdict, &commit_scn, &horizon_scn,
											  &wrap, NULL)
			== LMS_OWN_XID_PROVEN
		&& verdict == (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT) {
		result.kind = (uint8)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		result.commit_scn = commit_scn;
		result.wrap = wrap;
	}
	return result;
}

const char *
cluster_cr_server_test_own_xid_pair_reason(TransactionId xid, uint32 expected_segment_id,
										   uint32 expected_tt_slot_id, SCN proposed_scn,
										   ClusterUndoVerdictResult *out)
{
	volatile LmsFreshrefPairDiagnostic diagnostic;
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;

	memset(out, 0, sizeof(*out));
	(void)lms_resolve_own_xid_freshref_c1b_pair(xid, expected_segment_id, expected_tt_slot_id,
												proposed_scn, &verdict, &commit_scn, &horizon_scn,
												&wrap, &diagnostic);
	if (verdict == (uint8)CLUSTER_GCS_UNDO_VERDICT_COMMITTED_EXACT) {
		out->kind = CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
		out->commit_scn = commit_scn;
		out->wrap = wrap;
	}
	return diagnostic.predicate;
}

bool
cluster_cr_server_test_pair_detail_admit(uint32 *emitted)
{
	return lms_freshref_pair_detail_admit(emitted);
}
#endif

/* Bounded refusal evidence. Never read authority again or log under its locks. */
static void
lms_freshref_pair_log_refusal(const ClusterLmsCrSlot *slot, const char *phase,
							  const char *predicate,
							  const volatile LmsFreshrefPairDiagnostic *diagnostic)
{
	static uint32 emitted;
	LmsFreshrefPairDiagnostic empty;

	if (!SCN_VALID(slot->read_scn) || !lms_freshref_pair_detail_admit(&emitted))
		return;
	memset(&empty, 0, sizeof(empty));
	empty.raw_status = -1;
	if (diagnostic == NULL)
		diagnostic = &empty;
	elog(LOG,
		 "PGRAC freshref C1b refused: node=%d requester=%d backend=%d "
		 "request=" UINT64_FORMAT " xid=%u segment=%u slot=%u proposed=" UINT64_FORMAT
		 " epoch=" UINT64_FORMAT " phase=%s predicate=%s "
		 "native_sampled=%d no_raw_reuse=%d own_xid=%d scan_sampled=%d scan=%d "
		 "matched_segment=%u matched_slot=%u matched_wrap=%u resolved=" UINT64_FORMAT
		 " clog_sampled=%d clog_floor=%u raw_status=%d retention_sampled=%d "
		 "retention_ok=%d horizon=" UINT64_FORMAT " authority_scn=" UINT64_FORMAT
		 " detail_budget_exhausted=%d",
		 cluster_node_id, slot->requester_node, slot->requester_backend, slot->request_id,
		 slot->undo_xid, slot->undo_segment_id, slot->undo_block_no, (uint64)slot->read_scn,
		 slot->epoch, phase, predicate, diagnostic->native_sampled, diagnostic->no_raw_reuse,
		 diagnostic->own_xid, diagnostic->scan_sampled, (int)diagnostic->resolve,
		 diagnostic->matched_segment, diagnostic->matched_slot, diagnostic->matched_wrap,
		 (uint64)diagnostic->resolved_scn, diagnostic->clog_sampled, diagnostic->clog_floor,
		 diagnostic->raw_status, diagnostic->retention_sampled, diagnostic->retention_ok,
		 (uint64)diagnostic->horizon_scn, (uint64)slot->undo_auth.authority_scn, emitted == 64);
}

/*
 * lms_undo_verdict_serve — LMS side of one KIND_UNDO_VERDICT slot
 * (spec-6.12i D-i4 / spec-6.15 D4).
 *
 *	The complete-scan verdict: the requester's single fetched TT block came
 *	back 0-match, which proves nothing (the xid's slot may live in another
 *	segment) — so the ORIGIN, the one node whose durable TT and CLOG are
 *	authoritative for its own xids, answers over its COMPLETE own state:
 *
 *	  1. spec-6.15 D4 self-check: serve ONLY xids the stripe derivation
 *	     proves OURS (cluster_xid_is_mine; false whenever underivable —
 *	     striping off / below the activation floor / not our congruence
 *	     class).  Above the floor the xid value space is globally unique,
 *	     so a requester that derived the wrong origin, or a pre-striping
 *	     xid whose origin is unknowable, is refused instead of answered
 *	     from the wrong authority (the original 6.12i P0).
 *	  2. co-sample the live authority triple BEFORE the scan (the coverage
 *	     claim then never exceeds the scanned durable state — same
 *	     conservative direction as the fetch serve above).
 *	  3. complete by-xid scan over ALL self-owned durable TT headers
 *	     (cluster_tt_slot_durable_resolve_by_xid, WRAP_ANY):
 *	       RESOLVED_SCN        exact COMMITTED match: CLOG must confirm
 *	                           (C1b — the TT stamp is pre-commit, a stamp
 *	                           without a commit record is in-doubt) and the
 *	                           wrap-suspect acceptance gate must pass ->
 *	                           COMMITTED_EXACT{commit_scn, wrap}.
 *	       RECYCLED_ZERO_MATCH the slot is provably gone: evaluate the
 *	                           historical gated-recycle proof and
 *	                           sample its bound AFTER the scan (the
 *	                           monotonicity ordering contract), then let
 *	                           CLOG decide the terminal state:
 *	                             COMMITTED -> COMMITTED_BELOW_HORIZON{H}
 *	                             (recycle was horizon-gated, so the lost
 *	                             commit_scn is provably <= H);
 *	                             explicit ABORTED -> ABORTED.
 *	                           Neither (in-progress / crash-lost with no
 *	                           CLOG abort) -> refuse.
 *	       anything else       XID_MATCH_INVALID_SCN (delayed cleanout:
 *	                           recent, not recycled, scn unknown) /
 *	                           AMBIGUOUS_WRAP / SCAN_UNAVAILABLE -> refuse.
 *
 *	true = result_page holds the ClusterGcsUndoVerdictPage and
 *	slot->undo_auth the co-sampled triple; false = refuse (caller ships
 *	DENIED — requester keeps 53R97).  Runs under the drain's PG_TRY: a CLOG
 *	page truncated under an old xid, an unreadable segment, or any other
 *	throw becomes a refusal, never an LMS exit.
 */
static bool
lms_undo_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoVerdictPage *v = (ClusterGcsUndoVerdictPage *)slot->result_page;
	TransactionId xid = slot->undo_xid;
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;
	uint32 pair_segment = 0;
	uint32 pair_slot = 0;
	TransactionId pair_xid = InvalidTransactionId;
	bool freshref_pair = SCN_VALID(slot->read_scn);
	bool zero_epoch_pair = freshref_pair && slot->epoch == 0;
	bool zero_epoch_current = false;
	ClusterSemanticAdmissionToken zero_epoch_admission;
	volatile LmsFreshrefPairDiagnostic pair_diagnostic;
	LmsOwnXidReason reason;

	memset(&zero_epoch_admission, 0, sizeof(zero_epoch_admission));
	memset((void *)&pair_diagnostic, 0, sizeof(pair_diagnostic));

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!TransactionIdIsNormal(xid)) {
		cluster_vis53r97_note_srv_other();
		lms_freshref_pair_log_refusal(slot, "ENTRY", "BAD_XID", NULL);
		return false;
	}
	if (freshref_pair
		&& (!slot->undo_authoritative || slot->undo_owner != -1
			|| slot->epoch != cluster_epoch_get_current()
			|| !GcsBlockUndoFreshRefC1bTagDecode(
				slot->tag, &pair_segment, &pair_xid, &pair_slot)
			|| pair_segment != slot->undo_segment_id || pair_xid != xid
			|| pair_slot != slot->undo_block_no)) {
		cluster_vis53r97_note_srv_other();
		lms_freshref_pair_log_refusal(slot, "ENTRY", "PAIR_FIELDS", NULL);
		return false;
	}

	/*
	 * spec-6.15 D4: only answer for provably-own xids (see banner) -- UNLESS
	 * the request is spec-5.22f D6-7 AUTHORITATIVE (origin chosen from the fresh
	 * ref's physical ITL binding, so the requester already proved this is the
	 * correct owner; the underivable own seed xid is answered over the durable-TT
	 * + CLOG authority below, the positive proof unchanged).
	 */
	if (!slot->undo_authoritative && !cluster_xid_is_mine(xid)) {
		cluster_vis53r97_note_srv_other();
		return false;
	}

	/* Exact epoch zero is a legitimate clean-formation identity, but only
	 * while an ordinary current R4 TARGET token continuously covers the
	 * origin proof.  Non-zero rounds retain their existing epoch binding. */
	if (zero_epoch_pair) {
		if (!cluster_runtime_visibility_zero_epoch_pair_admission_enter(
				&zero_epoch_admission)) {
			cluster_vis53r97_note_srv_other();
			lms_freshref_pair_log_refusal(slot, "ENTRY", "ZERO_EPOCH_ADMISSION", NULL);
			return false;
		}
		PG_TRY();
		{
			slot->undo_auth.origin_epoch = cluster_epoch_get_current();
			slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
			slot->undo_auth.tt_generation
				= cluster_undo_tt_retention_rollover_count();
			slot->undo_auth.authority_scn = cluster_scn_current();
			reason = lms_resolve_own_xid_freshref_c1b_pair(
				xid, slot->undo_segment_id, slot->undo_block_no, slot->read_scn, &verdict,
				&commit_scn, &horizon_scn, &wrap, &pair_diagnostic);
			zero_epoch_current
				= cluster_semantic_activation_recheck(
					&zero_epoch_admission);
		}
		PG_FINALLY();
		{
			cluster_semantic_activation_leave(&zero_epoch_admission);
		}
		PG_END_TRY();
		if (!zero_epoch_current) {
			cluster_vis53r97_note_srv_other();
			lms_freshref_pair_log_refusal(slot, "EXIT", "ADMISSION_DRIFT", &pair_diagnostic);
			return false;
		}
	} else {
		/* Co-sample the authority triple BEFORE the scan (see banner). */
		slot->undo_auth.origin_epoch = cluster_epoch_get_current();
		slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
		slot->undo_auth.tt_generation
			= cluster_undo_tt_retention_rollover_count();
		/* PGRAC: spec-7.1a D3 -- co-sample the origin SCN clock with the same
		 * pre-content-read ordering (a stamp landing after the sample only makes
		 * the content newer than claimed; additive and safe). */
		slot->undo_auth.authority_scn = cluster_scn_current();

		/* Resolve the terminal/live verdict via the shared core; attribute the
		 * census leg. */
		if (freshref_pair)
			reason = lms_resolve_own_xid_freshref_c1b_pair(
				xid, slot->undo_segment_id, slot->undo_block_no, slot->read_scn, &verdict,
				&commit_scn, &horizon_scn, &wrap, &pair_diagnostic);
		else
		reason = lms_resolve_own_xid_verdict(
			xid, slot->undo_segment_id, slot->undo_block_no,
			slot->undo_authoritative, &verdict, &commit_scn, &horizon_scn, &wrap);
	}
	if (freshref_pair) {
		uint64 exit_epoch = cluster_epoch_get_current();

		if (slot->epoch != exit_epoch || slot->undo_auth.origin_epoch != slot->epoch
			|| !SCN_VALID(slot->undo_auth.authority_scn)
			|| scn_time_cmp(slot->undo_auth.authority_scn, slot->read_scn) < 0) {
			cluster_vis53r97_note_srv_other();
			lms_freshref_pair_log_refusal(
				slot, "EXIT",
				(slot->epoch != exit_epoch || slot->undo_auth.origin_epoch != slot->epoch)
					? "EPOCH_DRIFT"
					: "AUTHORITY_SCN_NOT_COVERED",
				&pair_diagnostic);
			return false;
		}
	}
	switch (reason) {
	case LMS_OWN_XID_PROVEN:
		break;
	case LMS_OWN_XID_PROVEN_UPGRADE:
		cluster_vis53r97_note_live_upgrade_hit(); /* spec-7.1 D1 serve upgrade */
		break;
	case LMS_OWN_XID_PROVEN_IN_PROGRESS:
		break;
	case LMS_OWN_XID_REFUSE_ZERO_MATCH:
		cluster_vis53r97_note_srv_zero_match();
		return false;
	case LMS_OWN_XID_REFUSE_INVALID_SCN:
		cluster_vis53r97_note_srv_invalid_scn();
		return false;
	case LMS_OWN_XID_REFUSE_OTHER:
	default:
		cluster_vis53r97_note_srv_other();
		if (freshref_pair)
			lms_freshref_pair_log_refusal(slot, "CORE", pair_diagnostic.predicate,
										  &pair_diagnostic);
		return false;
	}

	/*
	 * Zero the full BLCKSZ wire page (the reply checksum covers it all) and
	 * fill the verdict fields from the values the shared core just proved.
	 * The master==self local verdict resolve (D3-4 fill_page) runs the SAME
	 * lms_resolve_own_xid_verdict core, so the served and self answers over
	 * one xid can never diverge (Rule 8.A).
	 */
	memset(slot->result_page, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v->xid_echo = (uint64)xid;
	v->verdict = verdict;
	v->commit_scn = commit_scn;
	v->horizon_scn = horizon_scn;
	v->wrap = wrap;
	return true;
}

/*
 * lms_undo_authority_verdict_serve — LMS side of one kind-4 dead-owner
 * AUTHORITY verdict slot (spec-5.22d D4-6).
 *
 *	WE are asked to serve a verdict about a DEAD owner's xid from that
 *	owner's durable shared block0 — never from our own TT (the xid is not
 *	ours).  The wire is never trusted: the spec-5.22d §2.4 triple check must
 *	pass before a byte is answered —
 *
 *	  (a) request epoch == our CURRENT epoch (slot->epoch carries fwd.epoch,
 *	      which the requester stamped from its current epoch; any reconfig
 *	      since then invalidates the election the request was routed under);
 *	  (b) re-derive: the elected authority for (owner, current epoch) is
 *	      US (this implicitly re-proves the owner is dead-decided — a live
 *	      or undecided owner never elects an authority);
 *	  (c) the shared block0 prove passes (cluster_undo_authority_block0_
 *	      prove — the SAME core the requester's self-authority leg runs, so
 *	      the wire-served and self answers over one (owner, segment, xid)
 *	      can never diverge, Rule 8.A; it re-checks the epoch axis, reads
 *	      the owner's block0 under the AUTHORITY_BLOCK0 intent and demands
 *	      the exact xid+wrap positive proof, bumping the undo_authority_*
 *	      counters itself).
 *
 *	The reply page carries the version-2 AUTHORITY provenance (an old
 *	requester's strict ==1 gate refuses it).  slot->undo_auth: origin_epoch
 *	is our current epoch — the ship path copies it into hdr.epoch, which the
 *	requester binds strictly == its stamped epoch (8.A amend); live_hwm_lsn
 *	and tt_generation ride as ZERO — they describe an origin's OWN live TT
 *	plane, which does not exist for a dead owner; the prove already
 *	internalized generation/wrap coverage and the requester's authority leg
 *	ignores both.
 *
 *	true = result_page holds the version-2 ClusterGcsUndoVerdictPage;
 *	false = refuse (caller ships DENIED — requester keeps 53R97).  Runs
 *	under the drain's PG_TRY: any throw becomes a refusal, never an LMS
 *	exit.
 */
static bool
lms_undo_authority_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoVerdictPage *v = (ClusterGcsUndoVerdictPage *)slot->result_page;
	TransactionId xid = slot->undo_xid;
	uint64 cur_epoch;
	int32 authority = -1;
	ClusterUndoVerdictResult r;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!TransactionIdIsNormal(xid))
		return false;
	if (slot->undo_owner < 0 || slot->undo_owner == cluster_node_id)
		return false;

	/* (a) the request's epoch must still be OUR current epoch */
	cur_epoch = cluster_epoch_get_current();
	if (slot->epoch != cur_epoch)
		return false;

	/* (b) re-derive: the elected authority for (owner, cur) must be US */
	if (cluster_undo_serve_authority_lookup(slot->undo_owner, cur_epoch, &authority)
			!= CLUSTER_UNDO_AUTHORITY_OK
		|| authority != cluster_node_id)
		return false;

	/* the reply's epoch carrier (see banner); hwm/tt_gen deliberately zero */
	slot->undo_auth.origin_epoch = cur_epoch;
	slot->undo_auth.live_hwm_lsn = 0;
	slot->undo_auth.tt_generation = 0;

	/* (c) shared block0 prove core (the D4-4 self leg's implementation) */
	r = cluster_undo_authority_block0_prove(slot->undo_owner, slot->undo_segment_id, xid,
											cur_epoch);

	memset(slot->result_page, 0, BLCKSZ);
	return cluster_undo_authority_verdict_page_fill(v, xid, &r);
}

/*
 * cluster_lms_undo_verdict_fill_page -- resolve an OWN xid into a verdict page
 * over the COMPLETE own durable TT (cluster_tt_slot_durable_resolve_by_xid)
 * cross-checked against the OWN CLOG (AD-006: CLOG is the committed-ness
 * authority; the TT carries commit_scn).  The caller has already zeroed the
 * page buffer (origin: the full BLCKSZ wire page; D3-4 self: sizeof the
 * struct) and owns any authority co-sampling; this fills only
 * magic/version/xid_echo and the verdict taxonomy fields.  true = *v holds
 * the verdict; false = refuse (in-doubt / ambiguous / not-own / unresolvable
 * -> caller keeps the 53R97 fail-closed boundary, Rule 8.A).  Shared so the
 * origin-served verdict and the master==self local verdict are byte-for-byte
 * the same decision.
 */
bool
cluster_lms_undo_verdict_fill_page(TransactionId xid, bool authoritative,
								   ClusterGcsUndoVerdictPage *v)
{
	uint8 verdict = 0;
	SCN commit_scn = InvalidScn;
	SCN horizon_scn = InvalidScn;
	uint16 wrap = 0;

	if (!TransactionIdIsNormal(xid))
		return false;

	/* spec-6.15 D4: only answer for provably-own xids (see banner) -- UNLESS the
	 * caller is spec-5.22f D6-7 AUTHORITATIVE (physical-binding fresh ref), in
	 * which case the durable-TT + CLOG core below is the authority for an
	 * underivable own xid.  The derived (recycled) and master==self callers pass
	 * false and keep the self-check. */
	if (!authoritative && !cluster_xid_is_mine(xid))
		return false;

	/* One decision core for the served and self answers (Rule 8.A): the
	 * spec-7.1 refactor moved the scan + C1b CLOG cross-check + invalid-scn
	 * abort upgrade into lms_resolve_own_xid_verdict; this wrapper only
	 * shapes the page.  Census attribution is the caller's (the self leg
	 * keeps the rtvis counters; this core bumps none). */
	switch (
		lms_resolve_own_xid_verdict(xid, 0, 0, false, &verdict, &commit_scn, &horizon_scn, &wrap)) {
	case LMS_OWN_XID_PROVEN:
	case LMS_OWN_XID_PROVEN_UPGRADE:
		break;
	default:
		return false; /* refuse: caller keeps 53R97 fail-closed */
	}

	v->magic = CLUSTER_GCS_UNDO_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_VERDICT_VERSION;
	v->xid_echo = (uint64)xid;
	v->verdict = verdict;
	v->commit_scn = commit_scn;
	v->horizon_scn = horizon_scn;
	v->wrap = wrap;
	return true;
}

/*
 * lms_undo_multi_verdict_serve — LMS side of one KIND_UNDO_MULTI_VERDICT slot
 * (spec-7.1 D3-b).
 *
 *	The requester's member overlay structurally missed a FOREIGN multixact
 *	xmax (the updater had no compose-time TT binding, IN-12), so THIS node —
 *	the sole owner of the multi's pg_multixact members — answers over its own
 *	state:
 *	  1. serve ONLY multis the stripe derivation proves ours
 *	     (cluster_mxid_is_mine; underivable / foreign -> refuse, mirroring the
 *	     single-xid D4 self-check);
 *	  2. co-sample the live authority triple BEFORE enumerating (same
 *	     conservative ordering as the single serve);
 *	  3. GetMultiXactIdMembers over our own pg_multixact; a set < 2 or over the
 *	     wire capacity is refused (never truncate a member set, 8.A);
 *	  4. for each UPDATER member (status 4-5) resolve its terminal via the
 *	     shared lms_resolve_own_xid_verdict; lock-only members (A2) record
 *	     {xid, status} only.  A member xid that is not provably ours (a foreign
 *	     xid that locked/updated our row) is not resolvable from our TT/CLOG,
 *	     so any unprovable UPDATER makes the WHOLE multi UNPROVABLE -> refuse
 *	     (the requester keeps 53R97; the residue is the feature #119 forward).
 *
 *	true = result_page holds a SERVED ClusterGcsUndoMultiVerdictPage; false =
 *	refuse (caller ships DENIED).  Runs under the drain's PG_TRY: a truncated
 *	pg_multixact / CLOG page or any throw becomes a refusal, never an LMS exit.
 */
static bool
lms_undo_multi_verdict_serve(ClusterLmsCrSlot *slot)
{
	ClusterGcsUndoMultiVerdictPage *v = (ClusterGcsUndoMultiVerdictPage *)slot->result_page;
	MultiXactId mxid = (MultiXactId)slot->undo_xid; /* Q-D3b1: carrier holds the mxid */
	MultiXactMember *members = NULL;
	int nmembers;
	int i;

	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!MultiXactIdIsValid(mxid))
		return false;
	/* spec-7.1 D3-b: only answer for provably-own multis (see banner). */
	if (!cluster_mxid_is_mine(mxid))
		return false;

	/* Co-sample the authority triple BEFORE enumerating the members. */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();
	slot->undo_auth.authority_scn = cluster_scn_current();

	nmembers = GetMultiXactIdMembers(mxid, &members, false, false);
	if (nmembers < 1 || members == NULL) {
		if (members != NULL)
			pfree(members);
		return false; /* empty/unreadable descriptor */
	}
	if (nmembers > CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS) {
		pfree(members);
		return false; /* over wire capacity -> refuse (never truncate) */
	}

	memset(slot->result_page, 0, BLCKSZ);
	v->magic = CLUSTER_GCS_UNDO_MULTI_VERDICT_MAGIC;
	v->version = CLUSTER_GCS_UNDO_MULTI_VERDICT_VERSION;
	v->mxid_echo = (uint64)mxid;
	v->nmembers = (uint16)nmembers;

	for (i = 0; i < nmembers; i++) {
		ClusterGcsUndoMultiVerdictMember *out = &v->members[i];
		TransactionId member_xid = members[i].xid;
		uint8 status = (uint8)members[i].status;

		out->xid = member_xid;
		out->member_status = status;

		/* Lock-only members never gate visibility (A2): {xid, status} only —
		 * no terminal needed even for a foreign lock-only member. */
		if (status <= (uint8)MultiXactStatusForUpdate)
			continue;

		/* Updater member (4-5): its terminal decides tuple visibility, so it
		 * must be provably ours.  A foreign / underivable updater xid or an
		 * unprovable terminal makes the WHOLE multi UNPROVABLE (8.A). */
		if (!TransactionIdIsNormal(member_xid) || !cluster_xid_is_mine(member_xid)) {
			pfree(members);
			return false;
		}
		switch (lms_resolve_own_xid_verdict(member_xid, 0, 0, false, &out->verdict,
											&out->commit_scn, &out->horizon_scn, &out->wrap)) {
		case LMS_OWN_XID_PROVEN:
			break;
		case LMS_OWN_XID_PROVEN_UPGRADE:
			cluster_vis53r97_note_live_upgrade_hit(); /* spec-7.1 D1 serve upgrade (multi member) */
			break;
		default:
			pfree(members);
			return false; /* an unprovable updater -> whole multi UNPROVABLE */
		}
	}

	pfree(members);
	v->status = (uint8)CLUSTER_GCS_UNDO_MULTI_VERDICT_SERVED;
	return true;
}

/*
 * cr_serve_slot — serve one populated request carrier.  Shared by the
 * CONTROL-plane park drain and the D6 DATA-plane inline serve.  Sets
 * slot->result_status and fills result_page / undo_auth; every failure
 * (interleaved homes, snapshot-too-old, corruption, non-own xid, read
 * failure, injection, wave GUC off) becomes a DENIED result under the
 * PG_TRY -> DENIED envelope — the worker/LMS NEVER exits over a serve and
 * NEVER constructs out of order (Rule 8.A).  Does not touch the slot state
 * word or ship (callers own those).  Assumes the carrier is already
 * validated + populated (submit / serve_inline decode the synthetic address
 * + carrier before calling).
 */
static void
cr_serve_slot(ClusterLmsCrSlot *slot)
{
	slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;

	/*
	 * spec-6.12i D-i1 / D-i4 — undo-TT fetch + undo-verdict kinds.  No
	 * construction: read the self-owned TT header block (FETCH), or run the
	 * complete own-TT by-xid scan + CLOG cross-check (VERDICT); both co-sample
	 * the live authority triple.  The injection point is shared by both kinds:
	 * it models "the origin's undo serve plane is down", refusing fetches and
	 * verdicts alike.
	 */
	if (slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_FETCH
		|| slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT
		|| slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT) {
		bool served = false;
		uint8 result_status;
		ClusterCrServerStat served_stat;
		ClusterCrServerStat denied_stat;

		switch (slot->req_kind) {
		case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:
			result_status = (uint8)GCS_BLOCK_REPLY_UNDO_MULTI_VERDICT_RESULT;
			served_stat = CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_SERVED;
			denied_stat = CLUSTER_CR_SERVER_STAT_MULTI_VERDICT_DENIED;
			break;
		case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT:
			result_status = (uint8)GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT;
			served_stat = CLUSTER_CR_SERVER_STAT_VERDICT_SERVED;
			denied_stat = CLUSTER_CR_SERVER_STAT_VERDICT_DENIED;
			break;
		default: /* CLUSTER_LMS_SLOT_KIND_UNDO_FETCH */
			result_status = (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT;
			served_stat = CLUSTER_CR_SERVER_STAT_UNDO_SERVED;
			denied_stat = CLUSTER_CR_SERVER_STAT_UNDO_DENIED;
			break;
		}

		CLUSTER_INJECTION_POINT("cluster-lms-undo-fetch");
		if (!cluster_injection_should_skip("cluster-lms-undo-fetch")) {
			PG_TRY();
			{
				switch (slot->req_kind) {
				case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:
					served = lms_undo_multi_verdict_serve(slot);
					break;
				case (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT:
					/* spec-5.22d D4-6: a verdict slot carrying a dead owner
					 * is the kind-4 authority serve (block0 prove), never
					 * the own-TT scan. */
					served = slot->undo_owner >= 0 ? lms_undo_authority_verdict_serve(slot)
												   : lms_undo_verdict_serve(slot);
					break;
				default:
					served = lms_undo_fetch_serve(slot);
					break;
				}
			}
			PG_CATCH();
			{
				/* Fail-closed serve; keep the worker/LMS alive. */
				served = false;
				MemoryContextSwitchTo(TopMemoryContext);
				FlushErrorState();
				if (slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT)
					lms_freshref_pair_log_refusal(slot, "EXCEPTION", "CORE_EXCEPTION", NULL);
			}
			PG_END_TRY();
		}

		if (served) {
			slot->result_status = result_status;
			cluster_cr_server_stat_bump(served_stat);
		} else {
			cluster_cr_server_stat_bump(denied_stat);
		}
		return;
	}

	/* spec-6.12b CR construction. */
	{
		PGAlignedBlock cur_copy;
		XLogRecPtr page_lsn = InvalidXLogRecPtr;
		bool partial = false;
		bool constructed = false;

		/* spec-6.12b injection — force the DENIED serve path. */
		CLUSTER_INJECTION_POINT("cluster-lms-cr-construct");
		if (!cluster_injection_should_skip("cluster-lms-cr-construct")
			&& cluster_crossnode_cr_data_plane
			&& cluster_bufmgr_copy_block_for_gcs(slot->tag, &page_lsn, cur_copy.data, NULL)) {
			PG_TRY();
			{
				cluster_cr_construct_page_for_server(cur_copy.data, slot->read_scn, slot->tag,
													 slot->result_page, &partial);
				constructed = true;
			}
			PG_CATCH();
			{
				/*
				 * Fail-closed serve: keep the DENIED status and keep the
				 * worker/LMS alive.  The taxonomy counters were already bumped
				 * by the construction wrapper; drop the error state entirely
				 * (an aux-process ERROR would otherwise escalate to exit).
				 */
				constructed = false;
				MemoryContextSwitchTo(TopMemoryContext);
				FlushErrorState();
			}
			PG_END_TRY();
		}

		if (constructed) {
			slot->result_status = (uint8)(partial ? GCS_BLOCK_REPLY_CR_RESULT_PARTIAL
												  : GCS_BLOCK_REPLY_CR_RESULT_FULL);
			cluster_cr_server_stat_bump(partial ? CLUSTER_CR_SERVER_STAT_PARTIAL
												: CLUSTER_CR_SERVER_STAT_FULL);
		} else {
			cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_DENIED);
		}
	}
}

/*
 * cr_build_and_send_reply — build the standard GCS_BLOCK_REPLY (header +
 * BLCKSZ page, + ClusterGcsUndoAuthTrailer for served undo kinds) with the
 * HC109 forwarding-master echo the requester's HC108 chain expects, and send
 * it to the requester.  A DENIED result ships a zero page under a matching
 * checksum (the requester never consumes DENIED bytes).  Shared by the
 * CONTROL-plane LMON ship and the D6 DATA-plane inline serve.
 */
static void
cr_build_and_send_reply(const ClusterLmsCrSlot *slot)
{
	uint32 header_len = (uint32)sizeof(GcsBlockReplyHeader);
	uint32 total = header_len + GCS_BLOCK_DATA_SIZE;
	char *buf;
	GcsBlockReplyHeader *hdr;

	/* spec-6.12i: a served undo-TT fetch / undo verdict appends the authority
	 * trailer (tt_generation); DENIED undo replies stay v1-sized. */
	if (GcsBlockReplyStatusCarriesUndoAuthTrailer((GcsBlockReplyStatus)slot->result_status))
		total += (uint32)sizeof(ClusterGcsUndoAuthTrailer);
	buf = (char *)palloc0(total);
	hdr = (GcsBlockReplyHeader *)buf;
	hdr->request_id = slot->request_id;
	hdr->page_lsn = 0;
	hdr->epoch = cluster_epoch_get_current();
	hdr->sender_node = cluster_node_id;
	hdr->requester_backend_id = slot->requester_backend;
	hdr->transition_id = slot->transition_id;
	hdr->status = slot->result_status;
	GcsBlockReplyHeaderSetForwardingMasterNode(hdr, slot->reply_master_node);

	if (hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_FULL
		|| hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL)
		memcpy(buf + header_len, slot->result_page, BLCKSZ);
	else if (GcsBlockReplyStatusCarriesUndoAuthTrailer((GcsBlockReplyStatus)hdr->status)) {
		ClusterGcsUndoAuthTrailer *trailer
			= (ClusterGcsUndoAuthTrailer *)(buf + header_len + GCS_BLOCK_DATA_SIZE);

		/*
		 * spec-6.12i co-sample carriage: the authority sampled ATOMICALLY
		 * with the content read overrides the ship-time header fields —
		 * epoch carries the sampled origin epoch (the HC100 `hdr.epoch >=
		 * request_epoch` check then drops a mid-reconfig reply, which IS the
		 * D-i3 fail-closed) and page_lsn carries live_hwm_lsn.
		 */
		hdr->epoch = slot->undo_auth.origin_epoch;
		hdr->page_lsn = slot->undo_auth.live_hwm_lsn;
		memcpy(buf + header_len, slot->result_page, BLCKSZ);
		ClusterGcsUndoAuthTrailerSetTtGeneration(trailer, slot->undo_auth.tt_generation);
		ClusterGcsUndoAuthTrailerSetAuthorityScn(trailer, (uint64)slot->undo_auth.authority_scn);
	}
	hdr->checksum = cluster_gcs_block_compute_checksum(buf + header_len);

	/* GCS serve-stall round-5: share the block-family send admission
	 * accounting (an admitted reply is queued;  a refused one is the
	 * capacity red flag the S3 gate watches). */
	cluster_gcs_block_note_send_outcome(
		GCS_BLOCK_SEND_FAMILY_REPLY,
		cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, slot->requester_node, buf, total));
	pfree(buf);
}

/*
 * cluster_lms_cr_drain — CONTROL-plane park drain (LMS worker 0 main loop).
 * Serve every PENDING slot into a READY result (errors become DENIED; LMS
 * never exits over a serve).  DATA-plane requests are served inline and never
 * reach this table, so on a data-plane node the loop is a no-op.
 */
void
cluster_lms_cr_drain(void)
{
	int i;

	/* M4 Candidate-2 origin work shares this existing worker-0 tick and is
	 * process-local, so it must progress even when the legacy CR table is
	 * absent. */
	cluster_gcs_block_r4_tx_resolve_drain();
	if (cluster_gcs_block_r4_tx_resolve_active())
		return;
	if (CrServerShared == NULL)
		return;

	/* R4 work remains independently bounded and retains its existing scan. */
	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 state = pg_atomic_read_u32(&slot->state);

		if (state == CLUSTER_LMS_CR_R4_QUEUED) {
			if (cr_server_r4_claim_queued((uint32)i)
				&& cr_server_r4_build_step((uint32)i))
				(void)cr_server_r4_ship_terminal((uint32)i);
			continue;
		}
		if (state == CLUSTER_LMS_CR_R4_BUILDING || state == CLUSTER_LMS_CR_R4_UNDO_READY) {
			if (cr_server_r4_build_step((uint32)i))
				(void)cr_server_r4_ship_terminal((uint32)i);
			continue;
		}
		if (state == CLUSTER_LMS_CR_R4_NEED_UNDO) {
			(void)cr_server_r4_send_foreign_undo((uint32)i);
			continue;
		}
		if (state == CLUSTER_LMS_CR_R4_UNDO_INFLIGHT)
			continue;
		if (state == CLUSTER_LMS_CR_R4_READY_FULL
			|| state == CLUSTER_LMS_CR_R4_READY_RETRY
			|| state == CLUSTER_LMS_CR_R4_READY_FAIL
			|| state == CLUSTER_LMS_CR_R4_CANCELLED) {
			(void)cr_server_r4_ship_terminal((uint32)i);
		}
	}

	/*
	 * A complete-TT legacy verdict may perform a synchronous durable scan.
	 * Serve at most one per worker-0 tick so the DATA event loop can dispatch
	 * exact status-22 continuations between scans.  The process-local cursor
	 * keeps the four fixed slots fair without adding shared protocol state.
	 */
	for (i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		uint32 slot_index
			= (cluster_lms_cr_legacy_drain_cursor + (uint32)i)
			  % CLUSTER_LMS_CR_SLOTS;
		ClusterLmsCrSlot *slot = &CrServerShared->slots[slot_index];
		uint32 expected = CLUSTER_LMS_CR_PENDING;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_BUSY))
			continue;
		pg_read_barrier(); /* pair with the submit-side publish barrier */

		cr_serve_slot(slot);

		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_READY);
		/* PGRAC: spec-7.2 D1 -- wake the shipper (LMON) right away; without
		 * this the READY result sat until LMON's next natural wakeup (typ.
		 * 100-250ms).  Publish-before-signal: READY store above precedes the
		 * kick. */
		cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_SHIP_READY);
		cluster_lmon_wakeup();
		cluster_lms_cr_legacy_drain_cursor = (slot_index + 1) % CLUSTER_LMS_CR_SLOTS;
		break;
	}
}

/*
 * cluster_lms_cr_ship_ready — CONTROL-plane ship (LMON tick).  Ship every
 * READY result to its requester and free the slot.  DATA-plane requests are
 * shipped inline (see cluster_gcs_block_forward_serve_inline) and never reach
 * this table.
 */
void
cluster_lms_cr_ship_ready(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];

		if (pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_READY)
			continue;
		pg_read_barrier();

		cr_build_and_send_reply(slot);

		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_FREE);
	}
}

/*
 * spec-7.3 D6 — scratch context for one inline serve.  CR construction and
 * the reply build palloc transient state; the inline serve runs inside the
 * worker's long-lived DATA-dispatch loop, so it must reset its own scratch
 * rather than leak into that context.  Lazily created per worker process.
 */
static MemoryContext CrServeScratchCtx = NULL;

static MemoryContext
cr_serve_scratch_context(void)
{
	if (CrServeScratchCtx == NULL)
		CrServeScratchCtx = AllocSetContextCreate(TopMemoryContext, "cluster cr inline serve",
												  ALLOCSET_DEFAULT_SIZES);
	return CrServeScratchCtx;
}

static ClusterMxDescribeResult
cr_current_mx_build_describe_page(
	uint16 source_node_id, uint64 request_id, const ClusterCurrentMxKey *key,
	const MultiXactMember *native_members, int native_count,
	ClusterCurrentMxDescribeReplyPage *page)
{
	ClusterMxDescribeResult result = CMX_DESC_DENIED;
	int i;

	if (page == NULL)
		return CMX_DESC_UNKNOWN;
	memset(page, 0, sizeof(*page));
	if (key == NULL || request_id == 0 || source_node_id >= CLUSTER_MAX_NODES)
		return CMX_DESC_UNKNOWN;

	page->header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page->header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page->header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page->header.result = (uint8)CMX_DESC_DENIED;
	page->header.flags = CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE;
	page->header.source_node_id = source_node_id;
	page->header.request_id = request_id;
	page->header.mxkey = *key;
	page->header.wire_length = sizeof(page->header);

	if (native_count > CLUSTER_CURRENT_MX_MAX_MEMBERS) {
		page->header.result = (uint8)CMX_DESC_SUPPORTED_LIMIT;
		page->header.total_count = (uint32)native_count;
		return CMX_DESC_SUPPORTED_LIMIT;
	}
	if (native_members == NULL || native_count < 1)
		return CMX_DESC_DENIED;

	for (i = 0; i < native_count; i++) {
		page->members[i].xid = native_members[i].xid;
		page->members[i].member_status = (uint8)native_members[i].status;
	}
	if (cluster_multixact_current_validate_descriptor(
			key, source_node_id, key->cluster_epoch, page->members,
			(uint16)native_count, (uint32)native_count)
		== CMX_DESC_OK) {
		page->header.descriptor_hash
			= cluster_multixact_current_descriptor_hash(
				key, page->members, (uint16)native_count);
		if (page->header.descriptor_hash != 0) {
			page->header.total_count = (uint32)native_count;
			page->header.entry_count = (uint16)native_count;
			page->header.wire_length
				= sizeof(page->header)
				  + (uint16)native_count
						* sizeof(ClusterCurrentMxMemberDesc);
			page->header.result = (uint8)CMX_DESC_OK;
			result = CMX_DESC_OK;
		}
	}
	if (result != CMX_DESC_OK) {
		memset(page->members, 0, sizeof(page->members));
		page->header.descriptor_hash = 0;
		page->header.total_count = 0;
		page->header.entry_count = 0;
		page->header.wire_length = sizeof(page->header);
	}
	return result;
}

ClusterMxResolveResult
cluster_cr_server_current_mx_build_proof_page(
	uint16 source_node_id, const ClusterCurrentMxProofForwardV2 *request,
	ClusterMxResolveResult result, uint32 requester_capability_generation,
	const ClusterCurrentMemberProof *proofs,
	uint16 proof_count, const ClusterCurrentUpdaterProof *updater_proof,
	ClusterCurrentMxProofReplyPage *page)
{
	Size wire_length;

	if (page == NULL)
		return CMX_RESOLVE_UNKNOWN;
	memset(page, 0, sizeof(*page));
	if (request == NULL || request->prefix.request_id == 0
		|| source_node_id >= CLUSTER_MAX_NODES)
		return CMX_RESOLVE_UNKNOWN;

	page->header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page->header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page->header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	page->header.result = (uint8)CMX_RESOLVE_DENIED;
	page->header.flags = CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE;
	page->header.source_node_id = source_node_id;
	page->header.request_id = request->prefix.request_id;
	page->header.mxkey = request->prefix.mxkey;
	page->header.descriptor_hash
		= ClusterCurrentMxProofPrefixGetDescriptorHash(&request->prefix);
	page->header.total_count = request->prefix.total_count;
	page->header.chunk_ordinal = request->prefix.chunk_ordinal;
	page->header.chunk_count_minus_one
		= request->prefix.chunk_count_minus_one;
	page->header.wire_length = sizeof(page->header);

	if (result == CMX_RESOLVE_TIMEOUT || result > CMX_RESOLVE_RETRY)
		return CMX_RESOLVE_UNKNOWN;
	if (result != CMX_RESOLVE_OK) {
		if (requester_capability_generation != 0)
			return CMX_RESOLVE_UNKNOWN;
		page->header.result = (uint8)result;
		return result;
	}
	if (requester_capability_generation == 0 || proofs == NULL
		|| proof_count != request->prefix.entry_count)
		return CMX_RESOLVE_UNKNOWN;

	switch ((ClusterCurrentMxProofBodyKind)request->prefix.body_kind) {
	case CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS:
		if (proof_count == 0
			|| proof_count > CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME)
			return CMX_RESOLVE_UNKNOWN;
		memcpy(page->body.proofs, proofs, sizeof(*proofs) * proof_count);
		wire_length = sizeof(page->header) + sizeof(*proofs) * proof_count;
		break;
	case CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE:
		if (proof_count != 1 || updater_proof == NULL)
			return CMX_RESOLVE_UNKNOWN;
		page->body.updater.member_proof = proofs[0];
		page->body.updater.updater_proof = *updater_proof;
		wire_length = sizeof(page->header) + sizeof(proofs[0])
					  + sizeof(*updater_proof);
		break;
	default:
		return CMX_RESOLVE_UNKNOWN;
	}

	page->header.entry_count = proof_count;
	page->header.requester_capability_generation
		= requester_capability_generation;
	page->header.wire_length = (uint16)wire_length;
	page->header.result = (uint8)CMX_RESOLVE_OK;
	return CMX_RESOLVE_OK;
}

#ifdef USE_CLUSTER_UNIT
ClusterMxDescribeResult
cluster_cr_server_test_current_mx_build_describe_page(
	uint16 source_node_id, uint64 request_id, const ClusterCurrentMxKey *key,
	const MultiXactMember *native_members, int native_count,
	ClusterCurrentMxDescribeReplyPage *page)
{
	return cr_current_mx_build_describe_page(
		source_node_id, request_id, key, native_members, native_count, page);
}

ClusterMxResolveResult
cluster_cr_server_test_current_mx_build_proof_page(
	uint16 source_node_id, const ClusterCurrentMxProofForwardV2 *request,
	ClusterMxResolveResult result, uint32 requester_capability_generation,
	const ClusterCurrentMemberProof *proofs,
	uint16 proof_count, const ClusterCurrentUpdaterProof *updater_proof,
	ClusterCurrentMxProofReplyPage *page)
{
	return cluster_cr_server_current_mx_build_proof_page(
		source_node_id, request, result, requester_capability_generation,
		proofs, proof_count,
		updater_proof, page);
}
#endif

/* Spec-3.6b: the MXID origin enumerates its own immutable member list and
 * returns one typed page.  Every validation, fence or connection doubt is a
 * DENIED page or a dropped send; native pg_multixact bytes never leave. */
void
cluster_gcs_current_mx_describe_serve_inline(
	const ClusterICEnvelope *env, const void *payload)
{
	ClusterCurrentMxDescribeForwardV2 request;
	BufferTag route_tag;
	MultiXactMember *native_members = NULL;
	int native_count = -1;
	int recv_worker;
	int expected_worker;
	uint32 capability_generation = 0;
	uint32 reply_total
		= (uint32)sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE;
	char *reply;
	GcsBlockReplyHeader *outer;
	ClusterCurrentMxDescribeReplyPage *page;
	MemoryContext old_context;

	if (env == NULL || payload == NULL
		|| env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_gcs_block_family_on_data_plane()
		|| !cluster_multixact_current_wire_validate_describe_forward(
			payload, env->payload_length, (int32)env->source_node_id,
			cluster_node_id, cluster_epoch_get_current(), &request))
		return;
	route_tag = GcsBlockCurrentMxRouteTagMake(
		request.prefix.request_id, request.prefix.epoch,
		request.prefix.original_requester_node,
		request.prefix.requester_backend_id);
	recv_worker = cluster_ic_tier1_my_data_channel();
	expected_worker = cluster_lms_shard_for_tag(
		&route_tag, cluster_lms_workers);
	Assert(expected_worker == recv_worker);
	if (expected_worker != recv_worker)
		return;

	old_context = MemoryContextSwitchTo(cr_serve_scratch_context());
	reply = (char *)palloc0(reply_total);
	outer = (GcsBlockReplyHeader *)reply;
	page = (ClusterCurrentMxDescribeReplyPage *)(reply + sizeof(*outer));
	(void)cr_current_mx_build_describe_page(
		(uint16)cluster_node_id, request.prefix.request_id,
		&request.prefix.mxkey, NULL, -1, page);

	if ((!cluster_write_fence_enforcing() || cluster_write_fence_allowed())
		&& cluster_mxid_is_mine(request.prefix.mxkey.multixact_id)) {
		PG_TRY();
		{
			native_count = GetMultiXactIdMembers(
				request.prefix.mxkey.multixact_id, &native_members,
				false, false);
			(void)cr_current_mx_build_describe_page(
				(uint16)cluster_node_id, request.prefix.request_id,
				&request.prefix.mxkey, native_members, native_count, page);
			if (native_members != NULL) {
				pfree(native_members);
				native_members = NULL;
			}
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(cr_serve_scratch_context());
			FlushErrorState();
			(void)cr_current_mx_build_describe_page(
				(uint16)cluster_node_id, request.prefix.request_id,
				&request.prefix.mxkey, NULL, -1, page);
		}
		PG_END_TRY();
	}

	/* Positive bytes sampled before an epoch/fence transition are not
	 * publishable. */
	if (cluster_epoch_get_current() != request.prefix.epoch
		|| (cluster_write_fence_enforcing()
			&& !cluster_write_fence_allowed()))
		(void)cr_current_mx_build_describe_page(
			(uint16)cluster_node_id, request.prefix.request_id,
			&request.prefix.mxkey, NULL, -1, page);

	outer->request_id = request.prefix.request_id;
	outer->epoch = request.prefix.epoch;
	outer->sender_node = cluster_node_id;
	outer->requester_backend_id = request.prefix.requester_backend_id;
	outer->transition_id = 0;
	outer->status
		= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		outer, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	outer->checksum
		= cluster_gcs_block_compute_checksum((const char *)page);

	if (cluster_sf_peer_multixact_current_capability_generation(
			request.prefix.original_requester_node,
			&capability_generation)
		&& cluster_sf_peer_capability_generation_matches(
			request.prefix.original_requester_node,
			PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1,
			capability_generation))
		(void)cluster_ic_send_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY,
			request.prefix.original_requester_node, reply, reply_total);
	MemoryContextSwitchTo(old_context);
	MemoryContextReset(CrServeScratchCtx);
}

/* Positive current-member authority is staged by the
 * bounded TARGET canonical-TT resolver in cluster_gcs_block.c.  This inline
 * entry is now only the fail-closed fallback for a request that could not be
 * admitted to that existing event-loop context; it never consults SOURCE or
 * synthesizes a positive proof. */
void
cluster_gcs_current_mx_member_proof_serve_inline(
	const ClusterICEnvelope *env, const void *payload)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterMxResolveResult proof_result = CMX_RESOLVE_UNKNOWN;
	BufferTag route_tag;
	int recv_worker;
	int expected_worker;
	uint32 capability_generation = 0;
	uint32 reply_total
		= (uint32)sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE;
	char *reply;
	GcsBlockReplyHeader *outer;
	ClusterCurrentMxProofReplyPage *page;
	MemoryContext old_context;

	if (env == NULL || payload == NULL
		|| env->dest_node_id != (uint32)cluster_node_id
		|| !cluster_gcs_block_family_on_data_plane()
		|| !cluster_multixact_current_wire_validate_proof_forward(
			payload, env->payload_length, (int32)env->source_node_id,
			cluster_node_id, cluster_epoch_get_current(), &request))
		return;

	route_tag = GcsBlockCurrentMxRouteTagMake(
		request.prefix.request_id, request.prefix.epoch,
		request.prefix.original_requester_node,
		request.prefix.requester_backend_id);
	recv_worker = cluster_ic_tier1_my_data_channel();
	expected_worker = cluster_lms_shard_for_tag(
		&route_tag, cluster_lms_workers);
	Assert(expected_worker == recv_worker);
	if (expected_worker != recv_worker)
		return;

	old_context = MemoryContextSwitchTo(cr_serve_scratch_context());
	reply = (char *)palloc0(reply_total);
	outer = (GcsBlockReplyHeader *)reply;
	page = (ClusterCurrentMxProofReplyPage *)(reply + sizeof(*outer));
	if (cluster_write_fence_enforcing()
		&& !cluster_write_fence_allowed())
		proof_result = CMX_RESOLVE_DENIED;

	if (cluster_epoch_get_current() != request.prefix.epoch
		|| (cluster_write_fence_enforcing()
			&& !cluster_write_fence_allowed()))
		proof_result = CMX_RESOLVE_DENIED;
	if (cluster_cr_server_current_mx_build_proof_page(
			(uint16)cluster_node_id, &request, proof_result, 0, NULL,
			0, NULL, page)
		!= proof_result) {
		proof_result = CMX_RESOLVE_UNKNOWN;
		(void)cluster_cr_server_current_mx_build_proof_page(
			(uint16)cluster_node_id, &request, proof_result, 0, NULL, 0,
			NULL, page);
	}

	outer->request_id = request.prefix.request_id;
	outer->epoch = request.prefix.epoch;
	outer->sender_node = cluster_node_id;
	outer->requester_backend_id = request.prefix.requester_backend_id;
	outer->transition_id = 0;
	outer->status
		= (uint8)GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		outer, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	outer->checksum
		= cluster_gcs_block_compute_checksum((const char *)page);

	if (cluster_sf_peer_multixact_current_capability_generation(
			request.prefix.original_requester_node,
			&capability_generation)
		&& cluster_sf_peer_capability_generation_matches(
			request.prefix.original_requester_node,
			PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1,
			capability_generation))
		(void)cluster_ic_send_envelope(
			PGRAC_IC_MSG_GCS_BLOCK_REPLY,
			request.prefix.original_requester_node, reply, reply_total);
	MemoryContextSwitchTo(old_context);
	MemoryContextReset(CrServeScratchCtx);
}

/*
 * cluster_gcs_block_forward_serve_inline — spec-7.3 D6 entry.  When the GCS
 * block family is on the DATA plane, the worker[shard] that received a
 * GCS_BLOCK_FORWARD CR / undo-fetch / undo-verdict request serves it inline
 * and ships the reply on its own DATA channel — no shmem slot, no worker-0
 * poll handoff, no 100 ms idle latency.  The forward handler routes to this
 * only when cluster_gcs_block_family_on_data_plane(): on the CONTROL plane
 * the request must go through the light-work park path instead (LMON must not
 * walk undo I/O in its dispatch loop).
 *
 *	Populates the request carrier from the forward payload (decoding the
 *	synthetic undo address / xid carrier as the submit path does), then serves
 *	it through the shared cr_serve_slot() envelope and ships exactly one reply
 *	(the result, or a fail-closed DENIED on any refusal / wave-GUC-off /
 *	malformed request — the requester keeps its unchanged 53R9G / 53R97, Rule
 *	8.A).  Everything runs inside a per-call scratch context that is reset on
 *	return so the long-lived DATA-dispatch loop never accumulates transients.
 */
void
cluster_gcs_block_forward_serve_inline(const GcsBlockForwardPayload *fwd, ClusterLmsCrSlotKind kind)
{
	ClusterLmsCrSlot slot;
	MemoryContext old;
	uint32 segment_id = 0;
	uint32 block_no = 0;
	TransactionId pair_xid = InvalidTransactionId;
	SCN pair_scn = InvalidScn;
	bool inject_refuse;
	TimestampTz serve_started_at;

	if (fwd == NULL)
		return;

	/*
	 * PGRAC: spec-7.3 D5 (review P2-1) — per-worker shard routing guard,
	 * FORWARD inline-serve entry.  Same invariant as the REQUEST dedup
	 * guard (cluster_gcs_block.c): a block-family frame's tag routes to
	 * exactly one LMS worker (worker[shard(tag)], D4), and this serve runs
	 * in the worker whose DATA channel received the envelope.  A mismatch
	 * is a mis-route (per-tag order break, 8.A) that cannot happen without
	 * a code bug — fail closed (drop without serving; the requester
	 * retransmits within its budget and 53R90/53R9G fail-closes) rather
	 * than serve a tag this worker does not own.
	 */
	{
		int recv_worker = cluster_ic_tier1_my_data_channel();
		int tag_shard = cluster_lms_shard_for_tag(&fwd->tag, cluster_lms_workers);

		Assert(tag_shard == recv_worker);
		if (tag_shard != recv_worker) {
			static bool misroute_logged = false;

			cluster_gcs_block_dedup_note_misroute();
			if (!misroute_logged) {
				misroute_logged = true;
				ereport(LOG,
						(errmsg_internal("gcs block forward misrouted to LMS worker %d (tag shard "
										 "%d); dropping (spec-7.3 P2-1 8.A fail-closed)",
										 recv_worker, tag_shard)));
			}
			return;
		}
	}

	/* Populate the request carrier from the forward payload (was submit). */
	memset(&slot, 0, sizeof(slot));
	slot.tag = fwd->tag;
	slot.request_id = fwd->request_id;
	slot.epoch = fwd->epoch;
	slot.requester_node = fwd->original_requester_node;
	slot.requester_backend = fwd->requester_backend_id;
	slot.reply_master_node = fwd->master_node;
	slot.transition_id = fwd->transition_id;
	slot.result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
	slot.req_kind = (uint8)kind;
	/* spec-5.22d D4-6: -1 = live-owner kinds; only a decoded kind-4 owner
	 * carrier below may set it (memset left 0, which is a VALID node id —
	 * never let it leak into the authority routing). */
	slot.undo_owner = -1;

	/*
	 * PGRAC: spec-7.3 D7 — fence ×N.  A write-fenced node must not let block
	 * images / undo bytes / commit verdicts leave: while the cooperative
	 * write-fence is enforcing but this node is NOT authorized for the live
	 * epoch, any served payload could be stale relative to the cluster's
	 * authoritative state — a split-brain read (CR image) or a false-committed
	 * verdict (stale commit_scn) surface (Rule 8.A).  The worker-0 park ship
	 * path keeps this gate (cluster_lms.c fence-gates cr_ship_ready); D6's move
	 * to inline serve dropped it.  Restoring it HERE — ahead of the kind switch
	 * and any construct/read — covers every worker (0..7) and all three kinds
	 * uniformly.  Refuse by shipping the pre-set DENIED result WITHOUT reading
	 * or constructing anything; the requester retransmits within its budget and
	 * 53R90 fail-closes if the fence outlasts it — never a stale ship.  The
	 * injection forces the same branch deterministically for the TAP fence leg.
	 */
	CLUSTER_INJECTION_POINT("cluster-lms-cr-fence-refuse");
	/* Consume a pending injection arm unconditionally (F6-1 local-var idiom):
	 * evaluating it as the second || operand would let a genuine fence
	 * short-circuit past the consume, leaking the arm to a later call. */
	inject_refuse = cluster_injection_should_skip("cluster-lms-cr-fence-refuse");
	if ((cluster_write_fence_enforcing() && !cluster_write_fence_allowed()) || inject_refuse) {
		cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_FENCE_REFUSED);
		old = MemoryContextSwitchTo(cr_serve_scratch_context());
		cr_build_and_send_reply(&slot); /* slot.result_status == DENIED */
		MemoryContextSwitchTo(old);
		MemoryContextReset(CrServeScratchCtx);
		cluster_lms_obs_note_direct_reply(); /* spec-7.3 D8 */
		return;
	}

	switch (kind) {
	case CLUSTER_LMS_SLOT_KIND_CR:
		slot.read_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		break;

	case CLUSTER_LMS_SLOT_KIND_UNDO_FETCH:
		slot.read_scn = InvalidScn;
		/* A malformed tag leaves segment_id 0 -> lms_undo_fetch_serve refuses. */
		(void)GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no);
		slot.undo_segment_id = segment_id;
		slot.undo_block_no = block_no;
		slot.undo_xid = InvalidTransactionId;
		break;

	case CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT: {
		uint64 carrier = (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		bool freshref_pair
			= GcsBlockForwardPayloadIsUndoFreshRefC1bPairRequest(fwd);

		slot.read_scn = InvalidScn;
		if (freshref_pair) {
			if (cluster_cr_server_freshref_c1b_pair_request_decode(
					fwd, fwd->original_requester_node, cluster_node_id,
					cluster_epoch_get_current(), MaxBackends, &segment_id,
					&pair_xid, &block_no, &pair_scn)) {
				slot.read_scn = pair_scn;
				slot.undo_xid = pair_xid;
			} else
				slot.undo_xid = InvalidTransactionId;
		} else {
			(void)GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no);
			/* A malformed carrier (upper 32 bits set / non-normal) leaves xid
			 * Invalid -> lms_undo_verdict_serve refuses. */
			slot.undo_xid
				= (carrier <= (uint64)PG_UINT32_MAX
				   && TransactionIdIsNormal((TransactionId)carrier))
					  ? (TransactionId)carrier
					  : InvalidTransactionId;
		}
		slot.undo_segment_id = segment_id;
		slot.undo_block_no = block_no;
		/* spec-5.22f D6-7: the AUTHORITATIVE sub-flag widens the own-xid
		 * gate on the serve side (same decode as the park path). */
		slot.undo_authoritative = GcsBlockForwardPayloadIsUndoVerdictAuthoritative(fwd);
		/* spec-5.22d D4-6: kind-4 dead-owner AUTHORITY verdict — decode +
		 * range-check the owner carrier exactly as the park path does; a
		 * malformed or self-naming carrier leaves the xid Invalid so the
		 * serve refuses (our own xids are answered by the live-owner
		 * kinds, never by an authority detour). */
		if (!freshref_pair && GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(fwd)) {
			int32 wire_owner = -1;

			if (!GcsBlockUndoAuthorityFetchTagDecodeOwner(fwd->tag, &wire_owner) || wire_owner < 0
				|| wire_owner >= CLUSTER_MAX_NODES || wire_owner == cluster_node_id)
				slot.undo_xid = InvalidTransactionId;
			else
				slot.undo_owner = wire_owner;
		} else if (!freshref_pair && fwd->tag.relNumber != (RelFileNumber)0) {
			/* owner-served kinds must leave the owner carrier empty */
			slot.undo_xid = InvalidTransactionId;
		}
		break;
	}

	case CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:
	case CLUSTER_LMS_SLOT_KIND_R4_CR_BUILD:
		/* Multi-verdict is park-only.  R4 CR-build is intercepted as the
		 * 96-byte forward and owns the separate queued worker-0 state machine.
		 * A defensive inline call for either kind keeps the pre-set DENIED
		 * result and must not enter the generic cr_serve_slot(). */
		old = MemoryContextSwitchTo(cr_serve_scratch_context());
		goto inline_deny_no_serve;
	}

	old = MemoryContextSwitchTo(cr_serve_scratch_context());
	/* PGRAC: spec-7.3 D8 — time the serve into the calling worker's duration
	 * histogram (the R4 slow-shard backstop:  a slow CR construction stalls
	 * only this shard, and the per-worker hist is where that shows). */
	serve_started_at = GetCurrentTimestamp();
	cr_serve_slot(&slot);
	cluster_lms_obs_note_inline_serve((uint64)(GetCurrentTimestamp() - serve_started_at));
	/* A caught construction throw left CurrentMemoryContext at TopMemoryContext;
	 * normalize back to the scratch context before the reply build. */
	MemoryContextSwitchTo(cr_serve_scratch_context());

	/*
	 * PGRAC: spec-7.3 D7 (review P1-1) — re-check the fence at SHIP time.
	 * The gate above runs BEFORE construction; the serve itself walks undo
	 * I/O / TT scans (ms-scale), so a qvotec lease that expires DURING the
	 * serve would otherwise let the just-constructed image / verdict leave
	 * the now-fenced node — stale bytes the cluster's authoritative state
	 * has moved past (Rule 8.A).  The park ship path never had this window
	 * (cluster_lms.c gates cr_ship_ready at ship time, per tick); restore
	 * that timing here by discarding the constructed result and shipping
	 * the fail-closed DENIED instead — the requester retransmits within its
	 * budget and 53R90/53R9G fail-closes if the fence outlasts it.  The
	 * probe is a pure in-memory time comparison (no I/O on the hot path).
	 * The injection forces this branch deterministically for the TAP
	 * TOCTOU leg (a genuine mid-serve lease expiry is not schedulable from
	 * TAP); same unconditional-consume idiom as the gate above (F6-1).
	 */
	CLUSTER_INJECTION_POINT("cluster-lms-cr-fence-recheck");
	inject_refuse = cluster_injection_should_skip("cluster-lms-cr-fence-recheck");
	if ((cluster_write_fence_enforcing() && !cluster_write_fence_allowed()) || inject_refuse) {
		slot.result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_FENCE_REFUSED);
	}
inline_deny_no_serve:
	cr_build_and_send_reply(&slot);
	MemoryContextSwitchTo(old);
	MemoryContextReset(CrServeScratchCtx);
	cluster_lms_obs_note_direct_reply(); /* spec-7.3 D8 */
}

#endif /* USE_PGRAC_CLUSTER */
