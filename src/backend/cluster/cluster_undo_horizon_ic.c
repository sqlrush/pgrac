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
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "storage/shmem.h"
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

typedef struct ClusterUndoHorizonShmem {
	ClusterUndoHorizonSlotShmem slots[CLUSTER_MAX_NODES];
	/* observability (D5-5); bumped via the note_* helpers below */
	pg_atomic_uint64 stall_count;
	pg_atomic_uint64 peer_stale_count;
	pg_atomic_uint64 pass_abort_count;
	pg_atomic_uint64 wire_reject_count;
	pg_atomic_uint64 admission_refuse_count;
	pg_atomic_uint64 last_floor_scn; /* gauge: last OK fold result */
} ClusterUndoHorizonShmem;

static ClusterUndoHorizonShmem *UndoHorizonShmem = NULL;

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
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			pg_atomic_init_u32(&shmem->slots[i].seq, 0);
		pg_atomic_init_u64(&shmem->stall_count, 0);
		pg_atomic_init_u64(&shmem->peer_stale_count, 0);
		pg_atomic_init_u64(&shmem->pass_abort_count, 0);
		pg_atomic_init_u64(&shmem->wire_reject_count, 0);
		pg_atomic_init_u64(&shmem->admission_refuse_count, 0);
		pg_atomic_init_u64(&shmem->last_floor_scn, 0);
	}
	UndoHorizonShmem = shmem;
}

static const ClusterShmemRegion cluster_undo_horizon_region = {
	.name = "pgrac cluster undo horizon",
	.size_fn = cluster_undo_horizon_shmem_size,
	.init_fn = cluster_undo_horizon_shmem_init,
	.lwlock_count = 0, /* seqlock + atomics only */
	.owner_subsys = "cluster_undo_horizon",
	.reserved_flags = 0,
};

void
cluster_undo_horizon_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_undo_horizon_region);
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
cluster_undo_horizon_report_handler(const ClusterICEnvelope *env, const void *payload)
{
	ClusterUndoHorizonWire wire;
	ClusterUndoHorizonSlotShmem *slot;
	uint32 sender = env->source_node_id;
	uint64 now_us;

	if (UndoHorizonShmem == NULL)
		return;

	/* t/370 L2: force-drop the report before publish (stall leg). */
	CLUSTER_INJECTION_POINT("cluster-undo-horizon-report-drop");
	if (cluster_injection_should_skip("cluster-undo-horizon-report-drop"))
		return;

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
	if ((SCN)wire.horizon_scn == InvalidScn) {
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
	 */
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
static SCN
undo_horizon_sample_local_report(void)
{
	SCN floor = cluster_undo_retention_horizon();

	if (floor == InvalidScn)
		return InvalidScn;
	if (cluster_mrp_should_start()) {
		SCN bound = cluster_mrp_standby_consistent_scn();

		if (bound == InvalidScn)
			return InvalidScn;
		if (scn_time_cmp(bound, floor) < 0)
			floor = bound;
	}
	return floor;
}

void
cluster_undo_horizon_lmon_tick(void)
{
	static uint64 last_sent_us = 0;
	ClusterUndoHorizonWire wire;
	uint64 now_us;
	SCN report;
	int pi;

	if (!cluster_enabled || cluster_node_id < 0 || UndoHorizonShmem == NULL)
		return;

	/* one report per main-loop interval even when the latch churns */
	now_us = (uint64)GetCurrentTimestamp();
	if (last_sent_us != 0 && now_us - last_sent_us < (uint64)cluster_lmon_main_loop_interval * 1000)
		return;

	report = undo_horizon_sample_local_report();
	if (report == InvalidScn)
		return;
	last_sent_us = now_us;

	wire.epoch = cluster_epoch_get_current();
	wire.horizon_scn = (uint64)report;
	wire.sender_interval_ms = (uint32)cluster_lmon_main_loop_interval;

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
		if (!cluster_sf_peer_supports_undo_horizon(pi))
			continue;

		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_UNDO_HORIZON, pi, &wire, sizeof(wire));
		/* fire-and-forget (L456): transport retention / errors surface
		 * through the transport's own paths; a lost report just ages the
		 * peer's view of us into a stall. */
	}
}

/* ---------------- cleaner-side sampling (reader) ---------------------- */

#define UNDO_HORIZON_SEQLOCK_RETRIES 3

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
