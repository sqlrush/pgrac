/*-------------------------------------------------------------------------
 *
 * cluster_undo_horizon_ic.c
 *	  Cluster-wide undo retention horizon: wire carrier, per-peer seqlock
 *	  report slots and the LMON publish tick (spec-5.22e D5-2).
 *
 *	  Report flow: every node's LMON tick samples its own retention floor
 *	  (spec-3.12 own-instance min, further bounded by the node's minimum
 *	  future snapshot origin -- consistent_scn on an MRP-active node,
 *	  S3.0 row 4) and sends it per-peer p2p over a dedicated msg_type,
 *	  gated on the peer's CURRENT-connection UNDO_HORIZON_V1 capability
 *	  (an old peer treats an unregistered msg_type as a peer-level
 *	  failure and closes the connection, cluster_ic_router.c; so no
 *	  capability = no send).  The receive handler validates the payload
 *	  strictly (length / sender / SCN / interval range / same-epoch
 *	  monotonicity) and publishes it into a per-peer seqlock slot; any
 *	  invalid frame is counted and NOT published, so the stale slot ages
 *	  into a fold stall (fail-closed direction, spec-5.22e Q5' amend).
 *	  A same-epoch regression additionally latches a violation flag on
 *	  the slot: the previously accepted (higher) value must not be
 *	  consumed once the peer has contradicted it (S3.0 corollary).
 *
 *	  Single writer: both the handler and the tick run in LMON.  Readers
 *	  (the undo cleaner pass) snapshot the slots through the seqlock
 *	  double-read protocol into plain ClusterUndoHorizonReportView
 *	  structs and hand them to the pure fold (cluster_undo_horizon.c).
 *	  Exact-length V2 reports use separate slots bound to the current
 *	  CONTROL connection and local admission identity. Their readonly
 *	  sampler returns observations for a separately guarded cohort fold.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_horizon_ic.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22e-undo-cluster-retention-horizon.md (D5-2, §2.1/§2.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_cleaner.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/snapmgr.h" /* Actual snapshot lifetime and retention evidence. */
#include "utils/timestamp.h"

/*
 * Per-peer report slot.  Writer = LMON only (the IC dispatch loop is
 * single-threaded), so the seqlock needs no writer-side CAS: seq goes
 * odd -> fields -> even with write barriers; readers double-read with
 * read barriers and retry (spec-5.22e F-C4: horizon values may lawfully
 * differ between reports, and the slot is multi-field -- a torn read
 * could pair an old higher horizon with a fresh recv_at and misjudge a
 * dangerous floor as fresh).
 */
typedef struct ClusterUndoHorizonSlotShmem {
	pg_atomic_uint32 seq;
	uint64 epoch;
	uint64 horizon_scn;
	uint64 recv_at_us;
	uint32 sender_interval_ms;
	bool valid;
	bool regression_flagged;
	/* writer-private monotonicity base (LMON only; not part of the
	 * seqlock-published view) */
	uint64 accepted_scn;
	uint64 accepted_epoch;
	bool accepted_any;
} ClusterUndoHorizonSlotShmem;

/* Separate from the V1 SCN lane. LMON is still the only writer. The full
 * local admission/cohort cut and actual CONTROL generation bind the wire
 * identity to its receive authority; neither is supplied by the payload. */
typedef struct ClusterHeapFreezeReportV2SlotShmem {
	pg_atomic_uint32 seq;
	ClusterHeapFreezeReportV2View view;
	ClusterSemanticActiveSampleFence fence;
	uint32 capability_generation;
	/* LMON-private replay base, including an identity-proven H regression. */
	uint64 last_report_sequence;
	bool valid;
} ClusterHeapFreezeReportV2SlotShmem;

#define UNDO_HORIZON_V2_STATUS_COUNT (CLUSTER_HEAP_FREEZE_COHORT_INVALID + 1)

/* One volatile intent for all local VACUUM requesters. It carries no floor
 * authority. Only this short copy/coalesce section has multiple writers. */
typedef struct UndoHorizonMaintenanceIntent {
	ClusterSemanticActiveSampleFence fence;
	uint64 sequence;
	uint64 requested;
} UndoHorizonMaintenanceIntent;

typedef struct ClusterUndoHorizonShmem {
	ClusterUndoHorizonSlotShmem slots[CLUSTER_MAX_NODES];
	ClusterHeapFreezeReportV2SlotShmem freeze_slots[CLUSTER_MAX_NODES];
	uint64 report_sequence;			/* LMON only; survives a process restart; never wraps */
	uint64 freeze_last_observed_us; /* LMON only; rate limit refusals as well */
	slock_t maintenance_lock;
	UndoHorizonMaintenanceIntent maintenance;
	/* Latest READY fold, published only as an identity-bound candidate. */
	pg_atomic_uint32 freeze_watermark_seq;
	ClusterHeapFreezeWatermark freeze_watermark;
	ClusterSemanticActiveSampleFence freeze_watermark_fence;
	uint64 freeze_watermark_active_sample_id;
	/* Exact READY-fold source publications, protected by the same seqlock.
	 * F has no age expiry; a replaced source needs a new complete fold. */
	ClusterHeapFreezeCohortPeerObservation freeze_watermark_sources[CLUSTER_MAX_NODES];
	/* Diagnostic outcomes; the candidate above is not release authority. */
	pg_atomic_uint64 freeze_observation_count[UNDO_HORIZON_V2_STATUS_COUNT];
	/* observability (D5-5); bumped via the note_* helpers below */
	pg_atomic_uint64 stall_count;
	pg_atomic_uint64 peer_stale_count;
	pg_atomic_uint64 pass_abort_count;
	pg_atomic_uint64 wire_reject_count;
	pg_atomic_uint64 admission_refuse_count;
	pg_atomic_uint64 idle_sentinel_sent_count; /* TT lane: sentinel sends */
	pg_atomic_uint64 last_floor_scn;		   /* gauge: last OK fold result */
	pg_atomic_uint64 self_admitted_epoch;	   /* D5-8: (epoch self last became
										   * MEMBER) + 1; 0 = never admitted.
										   * Biased so a cold-formation
										   * admission at epoch 0 is distinct
										   * from the never-admitted sentinel. */
} ClusterUndoHorizonShmem;

static ClusterUndoHorizonShmem *UndoHorizonShmem = NULL;

typedef struct UndoHorizonV2MaintenanceObservation {
	ClusterSemanticActiveSampleFence fence;
	UndoHorizonMaintenanceIntent intent;
	uint64 covered;
	uint64 active_sample_id;
} UndoHorizonV2MaintenanceObservation;

static ClusterHeapFreezeCohortStatus
undo_horizon_v2_cohort_observe(ClusterHeapFreezeCohortObservation *out,
							   const ClusterUndoRetentionSample *local_history,
							   UndoHorizonV2MaintenanceObservation *maintenance);

static bool
undo_horizon_publish_freeze_watermark(const ClusterHeapFreezeCohortObservation *cohort,
									  const UndoHorizonV2MaintenanceObservation *observed,
									  uint64 now_us);

static Size
cluster_undo_horizon_shmem_size(void)
{
	return sizeof(ClusterUndoHorizonShmem);
}

static void
cluster_undo_horizon_shmem_init(void)
{
	bool found;
	ClusterUndoHorizonShmem *shmem;

	shmem = (ClusterUndoHorizonShmem *)ShmemInitStruct("pgrac cluster undo horizon",
													   cluster_undo_horizon_shmem_size(), &found);
	if (!found) {
		int i;

		memset(shmem, 0, sizeof(*shmem));
		SpinLockInit(&shmem->maintenance_lock);
		pg_atomic_init_u32(&shmem->freeze_watermark_seq, 0);
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			pg_atomic_init_u32(&shmem->slots[i].seq, 0);
			pg_atomic_init_u32(&shmem->freeze_slots[i].seq, 0);
		}
		for (i = 0; i < UNDO_HORIZON_V2_STATUS_COUNT; i++)
			pg_atomic_init_u64(&shmem->freeze_observation_count[i], 0);
		pg_atomic_init_u64(&shmem->stall_count, 0);
		pg_atomic_init_u64(&shmem->peer_stale_count, 0);
		pg_atomic_init_u64(&shmem->pass_abort_count, 0);
		pg_atomic_init_u64(&shmem->wire_reject_count, 0);
		pg_atomic_init_u64(&shmem->admission_refuse_count, 0);
		pg_atomic_init_u64(&shmem->idle_sentinel_sent_count, 0);
		pg_atomic_init_u64(&shmem->last_floor_scn, 0);
		pg_atomic_init_u64(&shmem->self_admitted_epoch, 0);
	}
	UndoHorizonShmem = shmem;
}

static const ClusterShmemRegion cluster_undo_horizon_region = {
	.name = "pgrac cluster undo horizon",
	.size_fn = cluster_undo_horizon_shmem_size,
	.init_fn = cluster_undo_horizon_shmem_init,
	.lwlock_count = 0, /* seqlock/atomics and a short local intent spinlock */
	.owner_subsys = "cluster_undo_horizon",
	.reserved_flags = 0,
};

void
cluster_undo_horizon_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_undo_horizon_region);
}

static UndoHorizonMaintenanceIntent
undo_horizon_maintenance_sample(void)
{
	UndoHorizonMaintenanceIntent intent;

	SpinLockAcquire(&UndoHorizonShmem->maintenance_lock);
	intent = UndoHorizonShmem->maintenance;
	SpinLockRelease(&UndoHorizonShmem->maintenance_lock);
	return intent;
}

/* LMON retires a source intent, never a QVOTEC promise. The sequence and
 * identity comparison preserve a newer concurrent VACUUM publication. */
static void
undo_horizon_maintenance_retire(const UndoHorizonMaintenanceIntent *observed)
{
	UndoHorizonMaintenanceIntent *intent = &UndoHorizonShmem->maintenance;

	SpinLockAcquire(&UndoHorizonShmem->maintenance_lock);
	if (intent->sequence == observed->sequence
		&& memcmp(&intent->fence, &observed->fence, sizeof(intent->fence)) == 0)
		intent->requested = 0;
	SpinLockRelease(&UndoHorizonShmem->maintenance_lock);
}

/* floor_to_origin(request) <= future iff request < future + stride. This
 * is coverage of an intent, not the ACTIVE minimum or a global F proof.
 * Non-stripe observations are retained conservatively without rounding. */
static uint64
undo_horizon_maintenance_coverage(int32 origin, uint64 future)
{
	if (origin >= 0 && origin < CLUSTER_XID_STRIDE && future % CLUSTER_XID_STRIDE == (uint64)origin
		&& future <= UINT64_MAX - (CLUSTER_XID_STRIDE - 1))
		return future + CLUSTER_XID_STRIDE - 1;
	return future;
}

/* ---------------- wire handler (LMON dispatch context) --------------- */

static void
undo_horizon_reject(const char *why, uint32 sender)
{
	if (UndoHorizonShmem != NULL)
		pg_atomic_fetch_add_u64(&UndoHorizonShmem->wire_reject_count, 1);
	ereport(DEBUG1,
			(errmsg_internal("pgrac undo-horizon report rejected: %s (sender %u)", why, sender)));
}

/*
 * Publish one accepted report into the sender's slot (seqlock write).
 * regression_flagged is part of the published view: an accept clears it,
 * a same-epoch regression sets it WITHOUT updating the payload fields
 * (the stale-but-once-accepted values stay visible for diagnostics; the
 * flag makes the fold refuse them).
 */
static void
undo_horizon_slot_publish(ClusterUndoHorizonSlotShmem *slot, const ClusterUndoHorizonWire *wire,
						  uint64 now_us, bool regression)
{
	uint32 seq = pg_atomic_read_u32(&slot->seq);

	Assert((seq & 1) == 0); /* single writer: LMON */
	pg_atomic_write_u32(&slot->seq, seq + 1);
	pg_write_barrier();
	if (!regression) {
		slot->epoch = wire->epoch;
		slot->horizon_scn = wire->horizon_scn;
		slot->recv_at_us = now_us;
		slot->sender_interval_ms = wire->sender_interval_ms;
		slot->valid = true;
		slot->regression_flagged = false;
	} else
		slot->regression_flagged = true;
	pg_write_barrier();
	pg_atomic_write_u32(&slot->seq, seq + 2);
}

static void
undo_horizon_v2_publish(ClusterHeapFreezeReportV2SlotShmem *slot,
						const ClusterHeapFreezeReportV2 *report,
						const ClusterSemanticActiveSampleFence *fence, uint32 generation,
						uint64 now_us, bool regression)
{
	uint32 seq = pg_atomic_read_u32(&slot->seq);

	/* Do not start publication over an in-progress update. */
	if (seq & 1)
		return;
	pg_atomic_write_u32(&slot->seq, seq + 1);
	pg_write_barrier();
	slot->last_report_sequence = report->report_sequence;
	if (!regression) {
		slot->view.report = *report;
		slot->view.recv_at_us = now_us;
		slot->fence = *fence;
		slot->capability_generation = generation;
		slot->valid = true;
	}
	slot->view.horizon_regression_flagged = regression;
	pg_write_barrier();
	pg_atomic_write_u32(&slot->seq, seq + 2);
}

static void
undo_horizon_v2_receive(const ClusterICEnvelope *env, const void *payload)
{
	ClusterHeapFreezeReportV2 report;
	ClusterSemanticActiveSampleFence fence;
	ClusterICControlIngress ingress;
	ClusterICTier1Stream stream;
	ClusterHeapFreezeReportV2SlotShmem *slot;
	const ClusterHeapFreezeReportV2 *old;
	uint32 sender = env->source_node_id;
	uint32 word, generation;
	TimestampTz now;
	bool same_identity;
	bool regression = false;

	if (!cluster_enabled || cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| sender >= CLUSTER_MAX_NODES || (int32)sender == cluster_node_id
		|| env->dest_node_id != cluster_node_id
		|| !cluster_heap_freeze_report_v2_decode(payload, env->payload_length, &report)
		|| report.origin != sender || !TransactionIdIsNormal((TransactionId)report.future_full_xid)
		|| !TransactionIdIsNormal((TransactionId)report.active_full_xid_min)
		|| (report.requested_full_xid != 0
			&& !TransactionIdIsNormal((TransactionId)report.requested_full_xid))
		|| scn_time_cmp(report.horizon_scn, report.h_sample_clock_scn) > 0) {
		undo_horizon_reject("invalid v2 report", sender);
		return;
	}
	if (!cluster_ic_dispatch_control_ingress(env, &ingress)
		|| ingress.carrier != CLUSTER_IC_CONTROL_TCP || ingress.owner_pid != MyProcPid
		|| ingress.source_node != (int32)sender || ingress.epoch != env->epoch
		|| ingress.epoch != report.formation_epoch) {
		undo_horizon_reject("unproven v2 control ingress", sender);
		return;
	}
	/* The private stream serial spans peers, whereas HELLO's shared
	 * generation is per peer. Validate each through its original owner. */
	stream = (ClusterICTier1Stream){ .serial = ingress.serial,
									 .epoch = ingress.epoch,
									 .owner_pid = ingress.owner_pid,
									 .peer = (int32)sender,
									 .plane = CLUSTER_IC_PLANE_CONTROL,
									 .channel = -1 };
	if (!cluster_ic_tier1_stream_current(&stream)
		|| !cluster_sf_peer_capability_word_sample(sender, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2,
												   &word, &generation)
		|| (word & PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2) == 0 || generation == 0
		|| !cluster_semantic_activation_active_sample_capture(&fence)
		|| fence.local_node_id != cluster_node_id || report.sysid != fence.system_identifier
		|| report.record_generation != fence.record_generation
		|| report.formation_epoch != fence.formation_epoch
		|| ((sender < 64 ? fence.admitted_members_lo : fence.admitted_members_hi)
			& (UINT64CONST(1) << (sender % 64)))
			   == 0
		|| report.incarnation != fence.admitted_incarnation[sender]) {
		undo_horizon_reject("unproven v2 receive identity", sender);
		return;
	}

	slot = &UndoHorizonShmem->freeze_slots[sender];
	old = &slot->view.report;
	/* Retain the base even across CONTROL reconnect or local republish.
	 * Only an actually changed wire identity permits sequence restart. */
	same_identity = slot->valid && old->sysid == report.sysid
					&& old->record_generation == report.record_generation
					&& old->formation_epoch == report.formation_epoch
					&& old->incarnation == report.incarnation;
	if (same_identity) {
		if (report.report_sequence <= slot->last_report_sequence
			|| report.f_sample_id < old->f_sample_id
			|| report.future_full_xid < old->future_full_xid
			|| report.active_full_xid_min < old->active_full_xid_min
			|| (report.f_sample_id == old->f_sample_id
				&& (report.future_full_xid != old->future_full_xid
					|| report.active_full_xid_min != old->active_full_xid_min))) {
			undo_horizon_reject("v2 replay or f regression", sender);
			return;
		}
		regression = scn_time_cmp(report.horizon_scn, old->horizon_scn) < 0
					 || scn_time_cmp(report.h_sample_clock_scn, old->h_sample_clock_scn) < 0;
	}
	now = GetCurrentTimestamp();
	if (now <= 0 || !cluster_ic_tier1_stream_current(&stream)
		|| !cluster_sf_peer_capability_generation_matches(
			sender, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2, generation)
		|| !cluster_semantic_activation_active_sample_current(&fence)) {
		undo_horizon_reject("v2 identity changed before publish", sender);
		return;
	}
	if (regression)
		undo_horizon_reject("v2 h regression", sender);
	/* A contradictory H blocks H without destroying the conservative F
	 * or refreshing its receive timestamp. A conforming report clears it. */
	undo_horizon_v2_publish(slot, &report, &fence, generation, (uint64)now, regression);
}

static void
cluster_undo_horizon_report_handler(const ClusterICEnvelope *env, const void *payload)
{
	ClusterUndoHorizonWire wire;
	ClusterUndoHorizonSlotShmem *slot;
	uint32 sender;
	uint64 now_us;

	if (UndoHorizonShmem == NULL)
		return;
	if (env == NULL || payload == NULL) {
		undo_horizon_reject("missing envelope or payload", 0);
		return;
	}
	sender = env->source_node_id;

	/* t/370 L2: force-drop the report before publish (stall leg). */
	CLUSTER_INJECTION_POINT("cluster-undo-horizon-report-drop");
	if (cluster_injection_should_skip("cluster-undo-horizon-report-drop"))
		return;
	if (env->payload_length == CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES) {
		undo_horizon_v2_receive(env, payload);
		return;
	}

	/*
	 * Strict wire validation (Q5' amend): exact length, sender within the
	 * declared range and not self, valid SCN, interval within
	 * [100,60000]ms.  Anything invalid is counted and NOT published: the
	 * old slot value keeps aging and the fold stalls on MISSING/STALE --
	 * never publish a value we could not prove well-formed.
	 */
	if (env->payload_length != sizeof(ClusterUndoHorizonWire)) {
		undo_horizon_reject("bad length", sender);
		return;
	}
	if (sender >= CLUSTER_MAX_NODES || (int32)sender == cluster_node_id) {
		undo_horizon_reject("bad sender", sender);
		return;
	}
	memcpy(&wire, payload, sizeof(wire));
	if ((SCN)wire.horizon_scn == InvalidScn
		|| (SCN)wire.horizon_scn == CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED) {
		undo_horizon_reject("invalid scn", sender);
		return;
	}
	if (wire.sender_interval_ms < CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS
		|| wire.sender_interval_ms > CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS) {
		undo_horizon_reject("interval out of range", sender);
		return;
	}

	slot = &UndoHorizonShmem->slots[sender];
	now_us = (uint64)GetCurrentTimestamp();

	/*
	 * Same-epoch monotonicity (S3.0 corollary): an accepted report is a
	 * lower bound on the sender's future snapshots, so within one epoch
	 * the accepted sequence must be non-decreasing.  A regression means a
	 * snapshot BELOW the accepted bound exists over there -- latch the
	 * violation (the fold stalls, U14) and do not move the payload.  A
	 * later conforming report clears the latch.
	 *
	 * The idle-unconstrained sentinel (TT lane) is OUTSIDE the monotone
	 * sequence: it claims "no constraint", not a bound, so it neither
	 * regresses against the accepted base (its scn_local is the ceiling
	 * anyway) nor BECOMES the base -- otherwise the sender's first real
	 * report after waking (its clock, numerically below the sentinel)
	 * would latch a spurious regression and stall the fold on every
	 * idle->active transition.  A real report after a sentinel still
	 * compares against the last REAL accepted value (the sender's clock
	 * is monotone, so that comparison holds across the idle gap).
	 */
	if ((SCN)wire.horizon_scn != CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED) {
		if (slot->accepted_any && slot->accepted_epoch == wire.epoch
			&& scn_time_cmp((SCN)wire.horizon_scn, (SCN)slot->accepted_scn) < 0) {
			undo_horizon_reject("same-epoch regression", sender);
			undo_horizon_slot_publish(slot, &wire, now_us, true);
			return;
		}

		undo_horizon_slot_publish(slot, &wire, now_us, false);
		slot->accepted_scn = wire.horizon_scn;
		slot->accepted_epoch = wire.epoch;
		slot->accepted_any = true;
		return;
	}

	undo_horizon_slot_publish(slot, &wire, now_us, false);
}

void
cluster_undo_horizon_register_ic_msg_types(void)
{
	const ClusterICMsgTypeInfo report_info = {
		.msg_type = PGRAC_IC_MSG_UNDO_HORIZON,
		.name = "undo_horizon",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false, /* per-peer p2p, HEARTBEAT discipline */
		.handler = cluster_undo_horizon_report_handler,
	};

	cluster_ic_register_msg_type(&report_info);
}

/* ---------------- LMON publish tick ---------------------------------- */

/*
 * Sample this node's report value (S2.1 sampling rule): the spec-3.12
 * own-instance ProcArray floor, further bounded by the node's minimum
 * future snapshot origin.  On an MRP-active (ADG standby) node future
 * snapshots read at consistent_scn -- which may sit far below the
 * no-reader clock fallback -- so the report must not exceed it (S3.0
 * row 4; on a primary node the origin is the Lamport clock, which the
 * ProcArray floor never exceeds).  InvalidScn (e.g. consistent_scn not
 * yet rebuilt after a restart) => no report this tick: peers stall on
 * staleness, which is the conservative direction.
 */
static bool
undo_horizon_sample_local_report(ClusterUndoRetentionSample *out)
{
	ClusterUndoRetentionSample sample;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_undo_retention_horizon_sample(&sample))
		return false;
	if (cluster_mrp_should_start()) {
		SCN bound = cluster_mrp_standby_consistent_scn();
		int cmp;

		if (bound == InvalidScn)
			return false;
		cmp = scn_time_cmp(bound, sample.horizon_scn);
		if (cmp < 0)
			sample.horizon_scn = bound;
		if (cmp < 0
			|| (cmp == 0
				&& (sample.contributor_kind == CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK
					|| sample.contributor_kind
						   == CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN))) {
			sample.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION;
			/* No stable per-consumer identity is exposed by the MRP bound.
			 * A zero diagnostic token must not become a release certificate. */
			sample.contributor_token = 0;
		}
	}
	*out = sample;
	return true;
}

/* Separate observation lane: neither sending nor receiving V2 enables the
 * global F fold or native freeze/truncation. V1 retains its original L2 path.
 * Each attempted V2 send consumes a shared sequence even if transport fails.
 * An incomplete identity cover cannot export F; refuse it before capability
 * sampling or consuming a sequence. Read-only cohort observations retain it. */
static void
undo_horizon_send_v2(int32 peer, const ClusterUndoCleanerActiveSample *active,
					 const ClusterUndoRetentionSample *h)
{
	const ClusterSemanticActiveSampleFence *fence = &active->admission_fence;
	ClusterHeapFreezeReportV2 report = { 0 };
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];
	uint32 word, generation;
	uint64 members = peer < 64 ? fence->admitted_members_lo : fence->admitted_members_hi;

	if (active->scan.status != CTRC_ACTIVE_SCAN_COMPLETE || active->sample_id == 0
		|| active->scan.unqualified_entries != 0
		|| fence->local_node_id != cluster_node_id || (members & (UINT64_C(1) << (peer % 64))) == 0
		|| fence->admitted_incarnation[peer] == 0 || scn_time_cmp(h->horizon_scn, h->clock_scn) > 0
		|| UndoHorizonShmem->report_sequence == UINT64_MAX
		|| !cluster_sf_peer_capability_word_sample(peer, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2,
												   &word, &generation)
		|| generation == 0)
		return;
	report.version = CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION;
	report.length = CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES;
	report.sysid = fence->system_identifier;
	report.record_generation = fence->record_generation;
	report.formation_epoch = fence->formation_epoch;
	report.incarnation = fence->local_boot_incarnation;
	report.origin = (uint32)cluster_node_id;
	report.report_sequence = UndoHorizonShmem->report_sequence + 1;
	report.future_full_xid = U64FromFullTransactionId(active->scan.future_full);
	{
		UndoHorizonMaintenanceIntent intent = undo_horizon_maintenance_sample();

		if (memcmp(&intent.fence, fence, sizeof(*fence)) == 0)
			report.requested_full_xid = intent.requested;
	}
	report.active_full_xid_min = U64FromFullTransactionId(active->scan.min_full);
	report.f_sample_id = active->sample_id;
	report.horizon_scn = (uint64)h->horizon_scn;
	report.h_sample_clock_scn = (uint64)h->clock_scn;
	report.h_contributor_kind = h->contributor_kind;
	report.h_contributor_token = h->contributor_token;
	report.sender_interval_ms = (uint32)cluster_lmon_main_loop_interval;
	if (!cluster_heap_freeze_report_v2_encode(&report, bytes)
		|| !cluster_undo_cleaner_active_sample_current(active)
		|| !cluster_sf_peer_capability_generation_matches(
			peer, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2, generation))
		return;
	UndoHorizonShmem->report_sequence = report.report_sequence;
	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_UNDO_HORIZON, peer, bytes, sizeof(bytes));
}

/* A V2 request is only an intent. Reuse the current local worker fences and
 * round DOWN to this origin, so alignment cannot exceed either the request
 * or the complete cohort's independent future ceiling. The original QVOTEC
 * entry rechecks the cohort, slack/window/lease identity and durable promise;
 * LMON performs no voting-disk IO and publishes no completion certificate. */
static void
undo_horizon_relay_maintenance(const ClusterHeapFreezeCohortObservation *cohort)
{
	ClusterUndoCleanerActiveSample active;
	uint64 requested = cohort->requested_full_xid_max;
	uint64 remainder;
	int slot;

	if (requested == 0 || requested > cohort->future_full_xid_max
		|| !cluster_undo_cleaner_active_sample(&active))
		return;
	slot = active.lease_fence.slot;
	if (slot < 0 || slot >= CLUSTER_XID_STRIDE || active.admission_fence.local_node_id != slot
		|| active.admission_fence.record_generation != cohort->record_generation
		|| active.admission_fence.formation_epoch != cohort->formation_epoch
		|| active.admission_fence.admitted_members_lo != cohort->admitted_members_lo
		|| active.admission_fence.admitted_members_hi != cohort->admitted_members_hi)
		return;
	remainder
		= (requested % CLUSTER_XID_STRIDE + CLUSTER_XID_STRIDE - (uint64)slot) % CLUSTER_XID_STRIDE;
	if (requested < remainder)
		return;
	requested -= remainder;
	if (!TransactionIdIsNormal((TransactionId)requested)
		|| requested <= U64FromFullTransactionId(active.scan.future_full)
		|| !cluster_undo_cleaner_active_sample_current(&active))
		return;
	(void)cluster_xid_stripe_request_maintenance(&active.admission_fence, &active.lease_fence,
												 FullTransactionIdFromU64(requested));
}

/* Observe the complete cohort on the original LMON cadence, including local
 * evidence failures. Reuse the sender's H cut; only outcome counts survive.
 * Producer protection must close before any global F can be published. */
static void
undo_horizon_observe_v2(uint64 now_us, const ClusterUndoRetentionSample *history)
{
	ClusterHeapFreezeCohortObservation observation;
	ClusterHeapFreezeCohortStatus status;
	UndoHorizonV2MaintenanceObservation maintenance;
	UndoHorizonMaintenanceIntent pending;
	uint64 last = UndoHorizonShmem->freeze_last_observed_us;

	if (last != 0 && now_us >= last
		&& now_us - last < (uint64)cluster_lmon_main_loop_interval * 1000)
		return;
	UndoHorizonShmem->freeze_last_observed_us = now_us;
	pending = undo_horizon_maintenance_sample();
	if (pending.requested != 0
		&& !cluster_semantic_activation_active_sample_current(&pending.fence))
		undo_horizon_maintenance_retire(&pending);
	status = history == NULL ? CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE
							 : undo_horizon_v2_cohort_observe(&observation, history, &maintenance);
	if ((unsigned)status >= UNDO_HORIZON_V2_STATUS_COUNT)
		status = CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	pg_atomic_fetch_add_u64(&UndoHorizonShmem->freeze_observation_count[status], 1);
	if (status == CLUSTER_HEAP_FREEZE_COHORT_READY) {
		(void)undo_horizon_publish_freeze_watermark(&observation, &maintenance, now_us);
		undo_horizon_relay_maintenance(&observation);
		if (maintenance.intent.requested != 0
			&& maintenance.covered >= maintenance.intent.requested)
			undo_horizon_maintenance_retire(&maintenance.intent);
	}
}

void
cluster_undo_horizon_lmon_tick(void)
{
	static uint64 last_sent_us = 0;
	ClusterUndoHorizonWire wire;
	ClusterUndoRetentionSample sample;
	ClusterUndoCleanerActiveSample active;
	bool have_active;
	uint64 now_us;
	int pi;

	if (!cluster_enabled || cluster_node_id < 0 || UndoHorizonShmem == NULL)
		return;
	/* All-member post-checkpoint proof also proves every retention consumer
	 * permanently parked. Do not start new gossip after peers may complete
	 * final release. Existing transport work and finite reports stay intact. */
	if (cluster_normal_stop_service_control_sealed() || cluster_normal_stop_pi_retirement_allowed())
		return;

	/* one report per main-loop interval even when the latch churns */
	now_us = (uint64)GetCurrentTimestamp();
	if (last_sent_us != 0 && now_us - last_sent_us < (uint64)cluster_lmon_main_loop_interval * 1000)
		return;

	if (!undo_horizon_sample_local_report(&sample)) {
		undo_horizon_observe_v2(now_us, NULL);
		return;
	}
	last_sent_us = now_us;
	undo_horizon_observe_v2(now_us, &sample);

	/* Idleness at this instant does not bound a future snapshot by infinity.
	 * Publish only the finite pre-scan clock / retained-snapshot minimum.
	 * A new reader can then start before the next tick without a revoke
	 * protocol; its read point cannot precede the previously published floor. */

	wire.epoch = cluster_epoch_get_current();
	wire.sender_interval_ms = (uint32)cluster_lmon_main_loop_interval;
	have_active = cluster_undo_cleaner_active_sample(&active);

	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		if (pi == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(pi) == NULL)
			continue; /* not declared */

		/*
		 * Q1' amend hard gate: only send to a peer whose CURRENT
		 * connection advertised UNDO_HORIZON_V1.  An old peer replies to
		 * an unregistered msg_type by closing the connection
		 * (cluster_ic_router.c inbound contract), so a blind send per
		 * tick would be a reconnect storm, not "ignored".
		 */
		if (cluster_sf_peer_supports_undo_horizon(pi)) {
			wire.horizon_scn = (uint64)sample.horizon_scn;
			(void)cluster_ic_send_envelope(PGRAC_IC_MSG_UNDO_HORIZON, pi, &wire, sizeof(wire));
		}
		/* fire-and-forget (L456): transport retention / errors surface
		 * through the transport's own paths; a lost report just ages the
		 * peer's view of us into a stall. */
		if (have_active)
			undo_horizon_send_v2(pi, &active, &sample);
	}
}

/* ---------------- cleaner-side sampling (reader) ---------------------- */

#define UNDO_HORIZON_SEQLOCK_RETRIES 3

static ClusterHeapFreezeCohortStatus
undo_horizon_v2_slot_sample(int32 peer, ClusterHeapFreezeReportV2View *out, uint32 *out_generation,
							uint32 *out_seq)
{
	ClusterHeapFreezeReportV2SlotShmem *slot = &UndoHorizonShmem->freeze_slots[peer];
	int attempt;

	for (attempt = 0; attempt < UNDO_HORIZON_SEQLOCK_RETRIES; attempt++) {
		uint32 seq = pg_atomic_read_u32(&slot->seq);
		ClusterHeapFreezeReportV2View view;
		ClusterSemanticActiveSampleFence fence;
		uint32 generation;
		bool valid;

		if (seq & 1)
			continue;
		pg_read_barrier();
		view = slot->view;
		fence = slot->fence;
		generation = slot->capability_generation;
		valid = slot->valid;
		pg_read_barrier();
		if (seq != pg_atomic_read_u32(&slot->seq))
			continue;
		if (!valid)
			return CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
		if (!cluster_semantic_activation_active_sample_current(&fence)
			|| !cluster_sf_peer_capability_generation_matches(
				peer, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2, generation))
			return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
		*out = view;
		*out_generation = generation;
		*out_seq = seq;
		return CLUSTER_HEAP_FREEZE_COHORT_READY;
	}
	return CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH;
}

bool
cluster_heap_freeze_report_v2_sample(int32 peer, ClusterHeapFreezeReportV2View *out)
{
	uint32 generation, seq;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (UndoHorizonShmem == NULL || !cluster_enabled || peer < 0 || peer >= CLUSTER_MAX_NODES
		|| peer == cluster_node_id)
		return false;
	return undo_horizon_v2_slot_sample(peer, out, &generation, &seq)
		   == CLUSTER_HEAP_FREEZE_COHORT_READY;
}

/* Recheck only retained producer identity, never refold reports or expire F
 * using the H clock. All connection callbacks precede the final semantic cut
 * and locked generation/source-sequence reads in both admitted mask halves. */
static bool
undo_horizon_watermark_sources_current(
	const ClusterSemanticActiveSampleFence *fence,
	const ClusterHeapFreezeCohortPeerObservation *sources)
{
	int32 peer;

	if (fence->local_node_id != cluster_node_id || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| ((cluster_node_id < 64 ? fence->admitted_members_lo : fence->admitted_members_hi)
			& (UINT64CONST(1) << (cluster_node_id % 64))) == 0)
		return false;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		const ClusterHeapFreezeCohortPeerObservation *source = &sources[peer];

		if (peer == cluster_node_id
			|| ((peer < 64 ? fence->admitted_members_lo : fence->admitted_members_hi)
				& (UINT64CONST(1) << (peer % 64))) == 0)
			continue;
		if (source->seq == 0 || (source->seq & 1) != 0 || source->generation == 0
			|| !cluster_sf_peer_capability_generation_matches(
				peer, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2, source->generation))
			return false;
	}
	if (!cluster_semantic_activation_active_sample_current(fence))
		return false;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		uint32 word, generation;

		if (peer == cluster_node_id
			|| ((peer < 64 ? fence->admitted_members_lo : fence->admitted_members_hi)
				& (UINT64CONST(1) << (peer % 64))) == 0)
			continue;
		/* This original CONTROL-locked read has no identity callbacks. A
		 * later peer's earlier callback may have reconnected this peer. */
		if (!cluster_sf_peer_capability_word_sample(
				peer, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2, &word, &generation)
			|| generation != sources[peer].generation)
			return false;
		pg_read_barrier();
		if (sources[peer].seq != pg_atomic_read_u32(&UndoHorizonShmem->freeze_slots[peer].seq))
			return false;
	}
	return true;
}

bool
cluster_heap_freeze_watermark_sample(ClusterHeapFreezeWatermark *out)
{
	int attempt;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (UndoHorizonShmem == NULL || !cluster_enabled)
		return false;

	for (attempt = 0; attempt < UNDO_HORIZON_SEQLOCK_RETRIES; attempt++) {
		uint32 seq1 = pg_atomic_read_u32(&UndoHorizonShmem->freeze_watermark_seq);
		ClusterHeapFreezeWatermark watermark;
		ClusterSemanticActiveSampleFence fence;
		ClusterHeapFreezeCohortPeerObservation sources[CLUSTER_MAX_NODES];
		uint64 active_sample_id;
		FullTransactionId future;
		FullTransactionId active_min;
		ClusterUndoCleanerActiveSample active;
		uint32 seq2;

		if (seq1 & 1)
			continue;
		pg_read_barrier();
		watermark = UndoHorizonShmem->freeze_watermark;
		fence = UndoHorizonShmem->freeze_watermark_fence;
		active_sample_id = UndoHorizonShmem->freeze_watermark_active_sample_id;
		memcpy(sources, UndoHorizonShmem->freeze_watermark_sources, sizeof(sources));
		future = FullTransactionIdFromU64(watermark.future_full_xid_max);
		active_min = FullTransactionIdFromU64(watermark.active_full_xid_min);
		pg_read_barrier();
		seq2 = pg_atomic_read_u32(&UndoHorizonShmem->freeze_watermark_seq);
		if (seq1 != seq2)
			continue;
		if (watermark.publication_sequence == 0 || watermark.published_at_us == 0
			|| watermark.f_sample_id == 0
			|| active_sample_id == 0 || watermark.f_sample_id != active_sample_id
			|| watermark.record_generation != fence.record_generation
			|| watermark.formation_epoch != fence.formation_epoch
			|| watermark.admitted_members_lo != fence.admitted_members_lo
			|| watermark.admitted_members_hi != fence.admitted_members_hi
			|| watermark.future_full_xid_max == 0
			|| watermark.active_full_xid_min == 0
			|| !FullTransactionIdIsValid(future) || !FullTransactionIdIsValid(active_min)
			|| FullTransactionIdFollows(active_min, future)
			|| !TransactionIdIsNormal(
				   XidFromFullTransactionId(future))
			|| !TransactionIdIsNormal(
				   XidFromFullTransactionId(active_min))
			|| !SCN_VALID((SCN)watermark.horizon_scn)
			|| watermark.horizon_scn == CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED
			|| !cluster_semantic_activation_active_sample_current(&fence)
			|| !cluster_undo_cleaner_active_sample(&active)
			|| active.sample_id != active_sample_id
			|| active.scan.unqualified_entries != 0
			|| memcmp(&active.admission_fence, &fence, sizeof(fence)) != 0
			|| !cluster_undo_cleaner_active_sample_current(&active)
			|| !undo_horizon_watermark_sources_current(&fence, sources)
			/* Peer observers may overlap new unknown local registration.
			 * This last read has no callbacks, locks, or replacement sample. */
			|| !cluster_ctrc_active_scan_registration_current(&active.scan))
			continue;
		/* Connection/admission observers may overlap another publication.
		 * Never return bytes from an earlier cut after those callbacks. */
		pg_read_barrier();
		if (seq1 != pg_atomic_read_u32(&UndoHorizonShmem->freeze_watermark_seq))
			continue;
		*out = watermark;
		return true;
	}
	return false;
}

typedef ClusterHeapFreezeCohortPeerObservation UndoHorizonV2PeerObservation;

static bool
undo_horizon_v2_required(const ClusterSemanticActiveSampleFence *fence, int32 peer)
{
	return ((peer < 64 ? fence->admitted_members_lo : fence->admitted_members_hi)
			& (UINT64CONST(1) << (peer % 64)))
		   != 0;
}

static ClusterHeapFreezeCohortStatus
undo_horizon_v2_observe_peer(int32 peer, const ClusterSemanticActiveSampleFence *fence,
							 UndoHorizonV2PeerObservation *identity,
							 ClusterHeapFreezeCohortObservation *candidate)
{
	ClusterHeapFreezeReportV2View view;
	ClusterHeapFreezeReportV2 *report = &view.report;
	ClusterHeapFreezeCohortStatus status;
	uint8 image[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	status = undo_horizon_v2_slot_sample(peer, &view, &identity->generation, &identity->seq);
	if (status != CLUSTER_HEAP_FREEZE_COHORT_READY)
		return status;
	if (report->origin != peer || report->sysid != fence->system_identifier
		|| report->record_generation != fence->record_generation
		|| report->formation_epoch != fence->formation_epoch
		|| report->incarnation != fence->admitted_incarnation[peer])
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	if (view.horizon_regression_flagged || !cluster_heap_freeze_report_v2_encode(report, image)
		|| !TransactionIdIsNormal((TransactionId)report->future_full_xid)
		|| !TransactionIdIsNormal((TransactionId)report->active_full_xid_min)
		|| (report->requested_full_xid != 0
			&& !TransactionIdIsNormal((TransactionId)report->requested_full_xid))
		|| scn_time_cmp(report->horizon_scn, report->h_sample_clock_scn) > 0)
		return CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	identity->recv_at_us = view.recv_at_us;
	identity->interval_ms = report->sender_interval_ms;
	identity->covered = undo_horizon_maintenance_coverage(peer, report->future_full_xid);
	candidate->future_full_xid_max = Max(candidate->future_full_xid_max, report->future_full_xid);
	candidate->requested_full_xid_max
		= Max(candidate->requested_full_xid_max, report->requested_full_xid);
	candidate->active_full_xid_min
		= Min(candidate->active_full_xid_min, report->active_full_xid_min);
	if (scn_time_cmp(report->horizon_scn, candidate->horizon_scn) < 0)
		candidate->horizon_scn = report->horizon_scn;
	return CLUSTER_HEAP_FREEZE_COHORT_READY;
}

/* Second collection brackets every required peer, including ones already
 * copied before a later peer reconnected or published a contradictory H.
 * H is checked against the END clock, not a pre-lock/pre-traversal clock. */
static ClusterHeapFreezeCohortStatus
undo_horizon_v2_cohort_current(const ClusterSemanticActiveSampleFence *fence,
							   const UndoHorizonV2PeerObservation *peers, TimestampTz started,
							   uint32 source_interval_ms)
{
	TimestampTz now;
	uint32 local_ms
		= Max(CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS,
			  Min(cluster_lmon_main_loop_interval, CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS));
	int32 peer;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		const UndoHorizonV2PeerObservation *p = &peers[peer];

		if (peer == fence->local_node_id || !undo_horizon_v2_required(fence, peer))
			continue;
		pg_read_barrier();
		if (p->seq != pg_atomic_read_u32(&UndoHorizonShmem->freeze_slots[peer].seq))
			return CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH;
		if (!cluster_sf_peer_capability_generation_matches(
				peer, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2, p->generation))
			return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	}
	/* Read the end clock after identity callbacks, including for a single
	 * local member. This expires the retained H cut, never F by its age. */
	now = GetCurrentTimestamp();
	if (now <= 0 || now < started
		|| (uint64)(now - started)
			> (uint64)CLUSTER_UNDO_HORIZON_FRESHNESS_FACTOR
				* Max(source_interval_ms, local_ms) * 1000)
		return CLUSTER_HEAP_FREEZE_COHORT_H_STALE;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		const UndoHorizonV2PeerObservation *p = &peers[peer];
		uint64 window;

		if (peer == fence->local_node_id || !undo_horizon_v2_required(fence, peer))
			continue;
		window
			= (uint64)CLUSTER_UNDO_HORIZON_FRESHNESS_FACTOR * Max(p->interval_ms, local_ms) * 1000;
		if (p->recv_at_us == 0 || (uint64)now < p->recv_at_us
			|| (uint64)now - p->recv_at_us > window)
			return CLUSTER_HEAP_FREEZE_COHORT_H_STALE;
	}
	return CLUSTER_HEAP_FREEZE_COHORT_READY;
}

static ClusterHeapFreezeCohortStatus
undo_horizon_v2_cohort_observe(ClusterHeapFreezeCohortObservation *out,
							   const ClusterUndoRetentionSample *local_history,
							   UndoHorizonV2MaintenanceObservation *maintenance)
{
	ClusterHeapFreezeCohortObservation candidate = { 0 };
	ClusterSemanticActiveSampleFence fence;
	ClusterUndoCleanerActiveSample active;
	ClusterUndoRetentionSample history;
	UndoHorizonV2MaintenanceObservation observed = { 0 };
	ClusterHeapFreezeCohortStatus status;
	TimestampTz started;
	int32 peer;

	if (out == NULL || !cluster_enabled || UndoHorizonShmem == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	if (!cluster_semantic_activation_active_sample_capture(&fence))
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	if (fence.local_node_id != cluster_node_id || !undo_horizon_v2_required(&fence, cluster_node_id)
		|| fence.local_boot_incarnation == 0
		|| fence.admitted_incarnation[cluster_node_id] != fence.local_boot_incarnation)
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	started = GetCurrentTimestamp();
	if (started <= 0)
		return CLUSTER_HEAP_FREEZE_COHORT_H_STALE;
	if (!cluster_undo_cleaner_active_sample(&active))
		return CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
	if (local_history != NULL)
		history = *local_history;
	else if (!undo_horizon_sample_local_report(&history))
		return CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
	if (active.scan.status != CTRC_ACTIVE_SCAN_COMPLETE || active.sample_id == 0
		|| active.worker_pid <= 0 || !FullTransactionIdIsValid(active.scan.future_full)
		|| !FullTransactionIdIsValid(active.scan.min_full)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(active.scan.future_full))
		|| !TransactionIdIsNormal(XidFromFullTransactionId(active.scan.min_full))
		|| FullTransactionIdFollows(active.scan.min_full, active.scan.future_full)
		|| !SCN_VALID(history.horizon_scn) || !SCN_VALID(history.clock_scn)
		|| history.horizon_scn == CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED
		|| scn_time_cmp(history.horizon_scn, history.clock_scn) > 0)
		return CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	candidate.source_local_sample_id = active.sample_id;
	candidate.source_started_at_us = (uint64)started;
	candidate.source_interval_ms = Max(CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS,
		Min(cluster_lmon_main_loop_interval, CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS));
	candidate.future_full_xid_max = U64FromFullTransactionId(active.scan.future_full);
	observed.fence = fence;
	observed.active_sample_id = active.sample_id;
	observed.intent = undo_horizon_maintenance_sample();
	if (memcmp(&observed.intent.fence, &fence, sizeof(fence)) == 0)
		candidate.requested_full_xid_max = observed.intent.requested;
	else
		observed.intent.requested = 0;
	observed.covered
		= undo_horizon_maintenance_coverage(cluster_node_id, candidate.future_full_xid_max);
	candidate.active_full_xid_min = U64FromFullTransactionId(active.scan.min_full);
	candidate.local_unqualified_entries = active.scan.unqualified_entries;
	memcpy(&candidate.admission_fence, &fence, sizeof(fence));
	candidate.horizon_scn = history.horizon_scn;
	candidate.record_generation = fence.record_generation;
	candidate.formation_epoch = fence.formation_epoch;
	candidate.admitted_members_lo = fence.admitted_members_lo;
	candidate.admitted_members_hi = fence.admitted_members_hi;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		if (!undo_horizon_v2_required(&fence, peer))
			continue;
		if (fence.admitted_incarnation[peer] == 0)
			return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
		if (peer == cluster_node_id)
			continue;
		status = undo_horizon_v2_observe_peer(peer, &fence, &candidate.source_peers[peer], &candidate);
		if (status != CLUSTER_HEAP_FREEZE_COHORT_READY)
			return status;
		observed.covered = Min(observed.covered, candidate.source_peers[peer].covered);
	}
	status = undo_horizon_v2_cohort_current(&fence, candidate.source_peers, started,
											  candidate.source_interval_ms);
	if (status != CLUSTER_HEAP_FREEZE_COHORT_READY)
		return status;
	if (!cluster_undo_cleaner_active_sample_current(&active)
		|| !cluster_semantic_activation_active_sample_current(&fence))
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	memcpy(out, &candidate, sizeof(candidate));
	if (maintenance != NULL)
		*maintenance = observed;
	return CLUSTER_HEAP_FREEZE_COHORT_READY;
}

/* Publish only after the fixed-cohort fold has been bracketed and the local
 * worker sample plus admission fence have been rechecked.  This is a
 * read-only 2.4 handoff candidate: the producer/consumer retention contract
 * and native truncation path still need their own proof in 2.5. */
static bool
undo_horizon_publish_freeze_watermark(const ClusterHeapFreezeCohortObservation *cohort,
								  const UndoHorizonV2MaintenanceObservation *observed,
								  uint64 now_us)
{
	ClusterUndoCleanerActiveSample active;
	FullTransactionId future;
	FullTransactionId active_min;
	ClusterHeapFreezeWatermark watermark;
	uint64 next;
	uint32 seq;

	if (cohort == NULL || observed == NULL || now_us == 0 || UndoHorizonShmem == NULL
		|| !cluster_enabled || !cluster_semantic_activation_active_sample_current(&observed->fence)
		|| !cluster_undo_cleaner_active_sample(&active))
		return false;
	future = FullTransactionIdFromU64(cohort->future_full_xid_max);
	active_min = FullTransactionIdFromU64(cohort->active_full_xid_min);
	if (active.scan.status != CTRC_ACTIVE_SCAN_COMPLETE || active.sample_id == 0
		|| active.scan.unqualified_entries != 0 || cohort->local_unqualified_entries != 0
		|| active.sample_id != observed->active_sample_id || active.worker_pid <= 0
		|| !FullTransactionIdIsValid(active.scan.future_full)
		|| !FullTransactionIdIsValid(active.scan.min_full)
		|| !FullTransactionIdIsValid(future) || !FullTransactionIdIsValid(active_min)
		|| !TransactionIdIsNormal(XidFromFullTransactionId(future))
		|| !TransactionIdIsNormal(XidFromFullTransactionId(active_min))
		|| FullTransactionIdFollows(active_min, future)
		|| cohort->horizon_scn == CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED
		|| !SCN_VALID((SCN)cohort->horizon_scn)
		|| cohort->record_generation != observed->fence.record_generation
		|| cohort->formation_epoch != observed->fence.formation_epoch
		|| cohort->admitted_members_lo != observed->fence.admitted_members_lo
		|| cohort->admitted_members_hi != observed->fence.admitted_members_hi
		|| memcmp(&active.admission_fence, &observed->fence, sizeof(active.admission_fence)) != 0
		|| !cluster_undo_cleaner_active_sample_current(&active)
		|| !undo_horizon_watermark_sources_current(&observed->fence, cohort->source_peers)
		/* Keep the old candidate if peer validation overlapped local ingress. */
		|| !cluster_ctrc_active_scan_registration_current(&active.scan))
		return false;
	seq = pg_atomic_read_u32(&UndoHorizonShmem->freeze_watermark_seq);
	if ((seq & 1) != 0 || seq >= UINT32_MAX - 1)
		return false;
	next = UndoHorizonShmem->freeze_watermark.publication_sequence;
	if (next == UINT64_MAX)
		return false;
	memset(&watermark, 0, sizeof(watermark));
	watermark.future_full_xid_max = cohort->future_full_xid_max;
	watermark.active_full_xid_min = cohort->active_full_xid_min;
	watermark.horizon_scn = cohort->horizon_scn;
	watermark.publication_sequence = next + 1;
	watermark.f_sample_id = observed->active_sample_id;
	watermark.published_at_us = now_us;
	watermark.record_generation = observed->fence.record_generation;
	watermark.formation_epoch = observed->fence.formation_epoch;
	watermark.admitted_members_lo = observed->fence.admitted_members_lo;
	watermark.admitted_members_hi = observed->fence.admitted_members_hi;
	pg_atomic_write_u32(&UndoHorizonShmem->freeze_watermark_seq, seq + 1);
	pg_write_barrier();
	UndoHorizonShmem->freeze_watermark = watermark;
	UndoHorizonShmem->freeze_watermark_fence = observed->fence;
	UndoHorizonShmem->freeze_watermark_active_sample_id = observed->active_sample_id;
	memcpy(UndoHorizonShmem->freeze_watermark_sources, cohort->source_peers,
		   sizeof(UndoHorizonShmem->freeze_watermark_sources));
	pg_write_barrier();
	pg_atomic_write_u32(&UndoHorizonShmem->freeze_watermark_seq, seq + 2);
	return true;
}

ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_observe(ClusterHeapFreezeCohortObservation *out)
{
	return undo_horizon_v2_cohort_observe(out, NULL, NULL);
}

ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_current(const ClusterHeapFreezeCohortObservation *expected)
{
	ClusterUndoCleanerActiveSample active;
	ClusterHeapFreezeCohortStatus status;
	const ClusterSemanticActiveSampleFence *fence;
	int32 peer;

	if (expected == NULL || !cluster_enabled || UndoHorizonShmem == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| expected->source_local_sample_id == 0 || expected->source_started_at_us == 0
		|| expected->source_started_at_us > INT64_MAX
		|| expected->source_interval_ms < CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS
		|| expected->source_interval_ms > CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS)
		return CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	fence = &expected->admission_fence;
	if (fence->local_node_id != cluster_node_id
		|| !undo_horizon_v2_required(fence, cluster_node_id)
		|| expected->record_generation != fence->record_generation
		|| expected->formation_epoch != fence->formation_epoch
		|| expected->admitted_members_lo != fence->admitted_members_lo
		|| expected->admitted_members_hi != fence->admitted_members_hi)
		return CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		const ClusterHeapFreezeCohortPeerObservation *p = &expected->source_peers[peer];

		if (peer == cluster_node_id || !undo_horizon_v2_required(fence, peer))
			continue;
		if (p->seq == 0 || (p->seq & 1) != 0 || p->generation == 0 || p->recv_at_us == 0
			|| p->interval_ms < CLUSTER_UNDO_HORIZON_INTERVAL_MIN_MS
			|| p->interval_ms > CLUSTER_UNDO_HORIZON_INTERVAL_MAX_MS)
			return CLUSTER_HEAP_FREEZE_COHORT_INVALID;
	}
	/* This reads the completed publication, not nextXid, PGPROC or H. Its
	 * original current getter checks lease/admission then the same worker cut. */
	if (!cluster_undo_cleaner_active_sample(&active))
		return CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
	if (active.sample_id != expected->source_local_sample_id
		|| active.scan.status != CTRC_ACTIVE_SCAN_COMPLETE
		|| memcmp(&active.admission_fence, fence, sizeof(*fence)) != 0
		|| !cluster_undo_cleaner_active_sample_current(&active))
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	status = undo_horizon_v2_cohort_current(fence, expected->source_peers,
		(TimestampTz)expected->source_started_at_us, expected->source_interval_ms);
	if (status != CLUSTER_HEAP_FREEZE_COHORT_READY)
		return status;
	/* The original local getter ran before peer observers. Recheck only
	 * its retained ingress cut now; never resample N/PGPROC/H or a peer. */
	if (!cluster_ctrc_active_scan_registration_current(&active.scan))
		return CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	return CLUSTER_HEAP_FREEZE_COHORT_READY;
}

bool
cluster_heap_freeze_request_maintenance(void)
{
	ClusterHeapFreezeCohortObservation cohort;
	UndoHorizonV2MaintenanceObservation observed;
	UndoHorizonMaintenanceIntent *intent;
	bool queued = false;

	if (undo_horizon_v2_cohort_observe(&cohort, NULL, &observed) != CLUSTER_HEAP_FREEZE_COHORT_READY
		|| !cluster_semantic_activation_active_sample_current(&observed.fence))
		return false;
	intent = &UndoHorizonShmem->maintenance;
	SpinLockAcquire(&UndoHorizonShmem->maintenance_lock);
	/* An old cut must not replace a newer publication for another identity. */
	if (intent->sequence != observed.intent.sequence
		&& memcmp(&intent->fence, &observed.fence, sizeof(intent->fence)) != 0)
		queued = false;
	else if (intent->requested != 0
			 && memcmp(&intent->fence, &observed.fence, sizeof(intent->fence)) == 0
			 && intent->requested >= cohort.future_full_xid_max)
		queued = true;
	else if (intent->sequence != UINT64_MAX) {
		intent->sequence++;
		intent->fence = observed.fence;
		intent->requested = cohort.future_full_xid_max;
		queued = true;
	}
	SpinLockRelease(&UndoHorizonShmem->maintenance_lock);
	return queued;
}

/*
 * Snapshot every peer slot into plain views for the pure fold.  Returns
 * the number of views filled (CLUSTER_MAX_NODES).  stable=false after
 * bounded seqlock retries => the fold reports TORN for that peer if it
 * is required.  Capability is sampled here too, so the fold's NOCAP arm
 * sees the CURRENT connection state (Q3'').
 */
int
cluster_undo_horizon_sample_views(ClusterUndoHorizonReportView *views, int maxviews)
{
	int n = Min(maxviews, CLUSTER_MAX_NODES);
	int i;

	if (UndoHorizonShmem == NULL)
		return 0;

	for (i = 0; i < n; i++) {
		ClusterUndoHorizonSlotShmem *slot = &UndoHorizonShmem->slots[i];
		ClusterUndoHorizonReportView *v = &views[i];
		int attempt;

		memset(v, 0, sizeof(*v));
		v->has_capability = cluster_sf_peer_supports_undo_horizon(i);
		v->stable = false;

		for (attempt = 0; attempt < UNDO_HORIZON_SEQLOCK_RETRIES; attempt++) {
			uint32 seq1 = pg_atomic_read_u32(&slot->seq);
			uint32 seq2;

			if (seq1 & 1)
				continue; /* writer mid-publish */
			pg_read_barrier();
			v->epoch = slot->epoch;
			v->horizon_scn = (SCN)slot->horizon_scn;
			v->recv_at_us = slot->recv_at_us;
			v->sender_interval_ms = slot->sender_interval_ms;
			v->valid = slot->valid;
			v->regression_flagged = slot->regression_flagged;
			pg_read_barrier();
			seq2 = pg_atomic_read_u32(&slot->seq);
			if (seq1 == seq2) {
				v->stable = true;
				break;
			}
		}
	}
	return n;
}

/*
 * Sample the required MEMBER peer set consistently with the reconfig
 * epoch: epoch -> membership walk -> epoch re-check, bounded retries.
 * Returns false when the epoch would not hold still (reconfig in
 * flight): the caller treats that exactly like a fold stall.
 */
bool
cluster_undo_horizon_required_members(uint8 *required, uint64 *out_epoch)
{
	int attempt;

	memset(required, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);

	for (attempt = 0; attempt < UNDO_HORIZON_SEQLOCK_RETRIES; attempt++) {
		uint64 e1 = cluster_epoch_get_current();
		uint64 e2;
		int i;

		memset(required, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (i == cluster_node_id)
				continue;
			if (cluster_conf_lookup_node(i) == NULL)
				continue; /* not declared */
			if (cluster_membership_get_state(i) == CLUSTER_MEMBER_MEMBER)
				required[i >> 3] |= (uint8)(1u << (i & 7));
		}
		e2 = cluster_epoch_get_current();
		if (e1 == e2) {
			*out_epoch = e1;
			return true;
		}
	}
	return false;
}

/* ---------------- D5-8 read admission (consumer side) ----------------- */

/*
 * Self admission epoch: the reconfig epoch at which THIS node last became
 * MEMBER (captured by the cluster_membership_set_state choke, so every
 * admission path -- bootstrap formation, online join, rejoin -- records it
 * exactly).  Stored BIASED BY ONE: 0 = never admitted this incarnation
 * (conservative refuse), N+1 = admitted at epoch N.  Cold formation admits
 * at CLUSTER_EPOCH_INITIAL (= 0), so the raw epoch value cannot double as
 * the sentinel -- conflating them refused every cross-node read on a
 * freshly formed cluster with "snapshot predates admission" (t/369 L1-L3b
 * regression driver).
 */
void
cluster_undo_horizon_note_self_member(void)
{
	if (UndoHorizonShmem == NULL)
		return;
	pg_atomic_write_u64(&UndoHorizonShmem->self_admitted_epoch, cluster_epoch_get_current() + 1);
}

/*
 * cluster_undo_horizon_read_admission_enforce -- D5-8 gate at every
 * cross-node undo consumption entry (D3 verdict wire leg, D2-3 CP3
 * acquire, 6.12i live resolve).  Own-instance reads are NOT gated (the
 * callers only invoke this on their foreign arms).
 *
 *	Refusal arms (S3.0 (4): a snapshot must either be covered by the fold
 *	or be unable to reference foreign undo at all):
 *	  - self not MEMBER: a JOINING/ABSENT node's reads are legal locally
 *	    but must not consult foreign undo -- its snapshots are not covered
 *	    by any owner's required set.  53R60, retry-safe: succeeds once the
 *	    join commits and a NEW snapshot is taken.
 *	  - pre-join snapshot: snapshot->read_epoch predates this node's
 *	    admission -- it existed before owners were required to cover us
 *	    (F-D3(3); a mere at-consumption MEMBER check would re-admit it).
 *	    Also 53R60 with a new-snapshot hint.
 *	  - mixed capability: an old-binary MEMBER owner has no brake, so a
 *	    new consumer must not trust ANY owner's retention until the whole
 *	    cluster is capable (S3.3 matrix).  Returns false; the caller keeps
 *	    its existing UNKNOWN/53R97 fail-closed shape (LOG-once here).
 *
 *	The evaluator supplies the actual snapshot, whose native Active or
 *	Registered lifetime must cover the read. Terminal callers without an
 *	evaluation scope retain their original ActiveSnapshot boundary.
 *
 *	Rejoin residual note (S3.0 (3)): a joiner adopts its admitted epoch off
 *	the voting disk (cluster_epoch_adopt_admitted), which carries no SCN
 *	observe; but every foreign consumption below rides IC envelopes whose
 *	reply observe (cluster_ic_envelope.c) advances the clock before any
 *	verdict is consumed, and a pre-observe snapshot that meets an
 *	already-recycled slot lands in the COMMITTED_BELOW_HORIZON catch-up
 *	bound arm -- fail-closed, never false-visible.
 */
bool
cluster_undo_horizon_read_admission_enforce(SCN read_scn)
{
	uint64 admitted;
	Snapshot snap = NULL;
	SCN retained_floor = InvalidScn;
	const char *refuse_reason = NULL;
	int pi;

	if (cluster_node_id < 0)
		return true; /* cluster off: nothing to gate */

	if (cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER) {
		if (UndoHorizonShmem != NULL)
			pg_atomic_fetch_add_u64(&UndoHorizonShmem->admission_refuse_count, 1);
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						errmsg("cross-node undo access refused: this node is not an admitted "
							   "cluster member yet"),
						errhint("Retry after the node's join completes; local reads are "
								"unaffected.")));
	}

	/* biased store: 0 = never admitted, N+1 = admitted at epoch N */
	admitted
		= UndoHorizonShmem == NULL ? 0 : pg_atomic_read_u64(&UndoHorizonShmem->self_admitted_epoch);
	if (admitted == 0)
		refuse_reason = "self admission epoch is not published";
	else if (cluster_snapshot_read_evidence_v1(read_scn, &snap, &retained_floor, &refuse_reason)
			 && snap->read_epoch < admitted - 1)
		refuse_reason = "snapshot epoch predates self admission epoch";

	if (refuse_reason != NULL) {
		if (UndoHorizonShmem != NULL)
			pg_atomic_fetch_add_u64(&UndoHorizonShmem->admission_refuse_count, 1);
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cross-node undo access refused: snapshot predates this node's "
						"cluster admission"),
				 errdetail("reason=%s admitted_epoch=" UINT64_FORMAT
						   " snapshot_epoch=" UINT64_FORMAT " current_epoch=" UINT64_FORMAT
						   " resolver_read_scn=" UINT64_FORMAT " snapshot_read_scn=" UINT64_FORMAT
						   " snapshot=%p retained_floor=" UINT64_FORMAT,
						   refuse_reason, admitted == 0 ? 0 : admitted - 1,
						   snap == NULL ? 0 : snap->read_epoch, cluster_epoch_get_current(),
						   (uint64)read_scn, snap == NULL ? 0 : (uint64)snap->read_scn, snap,
						   (uint64)retained_floor),
				 errhint("Take a new snapshot (new statement or transaction) after the "
						 "join completed and retry.")));
	}

	/*
	 * Mixed-capability hard gate (Q3''/S3.3): every MEMBER peer must be
	 * horizon-capable on its current connection before cross-node
	 * consumption may trust any owner's retention.
	 */
	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		if (pi == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(pi) == NULL)
			continue;
		if (cluster_membership_get_state(pi) != CLUSTER_MEMBER_MEMBER)
			continue;
		if (!cluster_sf_peer_supports_undo_horizon(pi)) {
			static bool nocap_logged = false;

			if (UndoHorizonShmem != NULL)
				pg_atomic_fetch_add_u64(&UndoHorizonShmem->admission_refuse_count, 1);
			if (!nocap_logged) {
				nocap_logged = true;
				ereport(LOG, (errmsg("cross-node undo access fail-closed: cluster member %d "
									 "lacks the undo-horizon capability (mixed-version "
									 "cluster)",
									 pi),
							  errhint("Upgrade every node or keep cross-node consumption "
									  "GUCs off during the upgrade window.")));
			}
			return false; /* caller keeps its UNKNOWN/53R97 fail-closed shape */
		}
	}

	return true;
}

/*
 * F-D2 epoch fence: re-verify -- at every recycle mutation point -- that the
 * reconfig epoch still equals the one the floor was folded at.  A mid-pass
 * join/epoch bump means the folded floor's member set no longer covers the
 * cluster (the new MEMBER's snapshots were not required), so the caller must
 * abort the whole pass immediately, not just wait for the next one.  The
 * injection point lets the t/370 L6 leg force the tripped arm at the first
 * mutation without a real reconfig.
 */
bool
cluster_undo_horizon_epoch_fence_tripped(uint64 expected_epoch)
{
	CLUSTER_INJECTION_POINT("cluster-undo-horizon-epoch-fence");
	if (cluster_injection_should_skip("cluster-undo-horizon-epoch-fence"))
		return true;
	return cluster_epoch_get_current() != expected_epoch;
}

/*
 * Per-peer report summary for the dump surface (D5-5): one static line
 * "n<id>:v=<valid>,cap=<cap>,e=<epoch>,scn=<scn>,age_ms=<age>,iv=<ms>,fl=<flag>"
 * per declared peer, space-separated.  Diagnostic only (torn reads are
 * acceptable here; the fold uses the seqlock-protocol sampler).
 */
const char *
cluster_undo_horizon_peer_reports_summary(void)
{
	static const char *const names[]
		= { "READY", "INCOMPLETE", "H_STALE", "BUSY_PUBLISH", "IDENTITY_CHANGED", "INVALID" };
	static char buf[256 + CLUSTER_MAX_NODES * 96];
	ClusterHeapFreezeCohortObservation observation;
	ClusterHeapFreezeCohortStatus status;
	uint64 now_us = (uint64)GetCurrentTimestamp();
	size_t off = 0;
	int i;

	buf[0] = '\0';
	if (UndoHorizonShmem == NULL)
		return buf;
	status = cluster_heap_freeze_cohort_observe(&observation);
	off = (size_t)snprintf(buf, sizeof(buf), "v2_observation=%s", names[status]);
	if (status == CLUSTER_HEAP_FREEZE_COHORT_READY)
		off += (size_t)snprintf(buf + off, sizeof(buf) - off,
								",f=" UINT64_FORMAT ",h=" UINT64_FORMAT,
								observation.active_full_xid_min, observation.horizon_scn);
	for (i = 0; i < UNDO_HORIZON_V2_STATUS_COUNT; i++)
		off += (size_t)snprintf(buf + off, sizeof(buf) - off, "%s%s:" UINT64_FORMAT,
								i == 0 ? " v2_lmon=" : ",", names[i],
								pg_atomic_read_u64(&UndoHorizonShmem->freeze_observation_count[i]));
	for (i = 0; i < CLUSTER_MAX_NODES && off + 96 < sizeof(buf); i++) {
		ClusterUndoHorizonSlotShmem *slot = &UndoHorizonShmem->slots[i];
		int64 age_ms;

		if (i == cluster_node_id || cluster_conf_lookup_node(i) == NULL)
			continue;
		age_ms = slot->recv_at_us == 0 || now_us < slot->recv_at_us
					 ? -1
					 : (int64)((now_us - slot->recv_at_us) / 1000);
		off += (size_t)snprintf(
			buf + off, sizeof(buf) - off,
			"%sn%d:v=%d,cap=%d,e=" UINT64_FORMAT ",scn=" UINT64_FORMAT ",age_ms=%lld,iv=%u,fl=%d",
			off > 0 ? " " : "", i, slot->valid ? 1 : 0,
			cluster_sf_peer_supports_undo_horizon(i) ? 1 : 0, slot->epoch, slot->horizon_scn,
			(long long)age_ms, slot->sender_interval_ms, slot->regression_flagged ? 1 : 0);
	}
	return buf;
}

/* ---------------- observability (D5-5 accessors) ---------------------- */

#define UNDO_HORIZON_NOTE(field)                                                                   \
	void cluster_undo_horizon_note_##field(void)                                                   \
	{                                                                                              \
		if (UndoHorizonShmem != NULL)                                                              \
			pg_atomic_fetch_add_u64(&UndoHorizonShmem->field##_count, 1);                          \
	}                                                                                              \
	uint64 cluster_undo_horizon_##field##_count(void)                                              \
	{                                                                                              \
		return UndoHorizonShmem == NULL ? 0                                                        \
										: pg_atomic_read_u64(&UndoHorizonShmem->field##_count);    \
	}

UNDO_HORIZON_NOTE(stall)
UNDO_HORIZON_NOTE(peer_stale)
UNDO_HORIZON_NOTE(pass_abort)
UNDO_HORIZON_NOTE(wire_reject)
UNDO_HORIZON_NOTE(admission_refuse)

/* TT lane: sender-side sentinel gauge (bumped inline in the tick; only the
 * reader half of the NOTE pattern is needed). */
uint64
cluster_undo_horizon_idle_sentinel_sent_count(void)
{
	return UndoHorizonShmem == NULL
			   ? 0
			   : pg_atomic_read_u64(&UndoHorizonShmem->idle_sentinel_sent_count);
}

void
cluster_undo_horizon_note_floor(SCN scn)
{
	if (UndoHorizonShmem != NULL)
		pg_atomic_write_u64(&UndoHorizonShmem->last_floor_scn, (uint64)scn);
}

SCN
cluster_undo_horizon_last_floor(void)
{
	return UndoHorizonShmem == NULL ? InvalidScn
									: (SCN)pg_atomic_read_u64(&UndoHorizonShmem->last_floor_scn);
}
