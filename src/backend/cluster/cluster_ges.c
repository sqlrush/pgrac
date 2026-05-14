/*-------------------------------------------------------------------------
 *
 * cluster_ges.c
 *	  GES (Global Enqueue Service) request protocol skeleton — spec-2.13.
 *
 *	  Implements the 2 ICMsgType handler stubs (GES_REQUEST=4 /
 *	  GES_REPLY=5), 2 atomic defer counters, 2 read accessors, and the
 *	  cluster_ges shmem region lifecycle.
 *
 *	  See cluster_ges.h for the protocol contract and skeleton scope.
 *	  See spec-2.13-ges-request-protocol-skeleton.md (frozen v0.2) for
 *	  design rationale.
 *
 *	  AD-002 PCM vs GES 分工 + AD-011 不移植 LC/RC Lock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Skeleton phase: 2 handler stubs永远 DEFER (no state change beyond
 *	  the atomic counter bump);  caller-side (spec-2.14+) replaces the
 *	  DEFER body with real routing / grant / convert logic.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_guc.h" /* cluster_node_id */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_in_quorum */
#include "cluster/cluster_conf.h"	/* cluster_conf_lookup_node */
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/* ============================================================
 * Shmem region state (Q3.1=B independent region).
 * ============================================================ */

static ClusterGesSharedState *cluster_ges_state = NULL;


/* ============================================================
 * Shmem region lifecycle (mirror cluster_scn pattern).
 * ============================================================ */

Size
cluster_ges_shmem_size(void)
{
	return sizeof(ClusterGesSharedState);
}

void
cluster_ges_shmem_init(void)
{
	bool found;

	cluster_ges_state = ShmemInitStruct("pgrac cluster ges", cluster_ges_shmem_size(), &found);
	if (!found) {
		/* spec-2.13 D3 init zero (Q4.1=A all-atomic, no LWLock). */
		pg_atomic_init_u64(&cluster_ges_state->request_defer_count, 0);
		pg_atomic_init_u64(&cluster_ges_state->reply_defer_count, 0);
	}
}

static const ClusterShmemRegion cluster_ges_region = {
	.name = "pgrac cluster ges",
	.size_fn = cluster_ges_shmem_size,
	.init_fn = cluster_ges_shmem_init,
	.lwlock_count = 0, /* spec-2.13: lock-free (L106 inherit) */
	.owner_subsys = "cluster_ges",
	.reserved_flags = 0,
};

void
cluster_ges_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ges_region);
}


/* ============================================================
 * Handler stubs (spec-2.13 D2).
 *
 *	Skeleton phase: increment defer counter + DEBUG2 log + return.
 *	NO state change beyond the atomic counter (caller MUST fall back
 *	to PG-native lock manager when stub fires).
 *
 *	Handler 4 硬约束 (spec-2.3 Q6) enforced by this stub body:
 *	  (1) no block / no LWLock wait — atomic only;
 *	  (2) no catalog SQL — bare counter bump + elog;
 *	  (3) no ERROR / FATAL — only Assert (debug-build only) + DEBUG2;
 *	  (4) bounded shmem only — single 8-byte field touched.
 * ============================================================ */

/*
 * Inbound 5-item validation per spec-2.16 v0.4 L1.8 (I36-I37).
 *
 *   Returns true if payload passes all 5 checks;  false → caller
 *   drops + bumps ges_inbound_validation_fail_count.
 *
 *   For request handler (msg_type == GES_REQUEST):
 *     opcode_min=1, opcode_max=3 (REQUEST / CONVERT / RELEASE)
 *   For reply handler (msg_type == GES_REPLY):
 *     opcode_min=1, opcode_max=2 (GRANT / REJECT)
 */
static bool
ges_validate_inbound(const ClusterICEnvelope *env, uint32 payload_node_id, uint64 payload_epoch,
					 uint32 payload_opcode, uint32 opcode_min, uint32 opcode_max,
					 bool payload_node_must_be_source)
{
	uint64 accepted_epoch;

	/*
	 * (1) Request payloads identify the remote holder and must match the
	 * envelope source.  Reply payloads echo the original local holder tuple,
	 * so holder_node_id must be this node, not the replying master.
	 */
	if (payload_node_must_be_source) {
		if (payload_node_id != env->source_node_id)
			return false;
	} else {
		if ((int32)payload_node_id != cluster_node_id)
			return false;
	}

	/* (2) payload.epoch == env.epoch */
	if (payload_epoch != env->epoch)
		return false;

	/* (3) payload.epoch == local accepted_epoch */
	accepted_epoch = cluster_epoch_get_current();
	if (payload_epoch != accepted_epoch)
		return false;

	/* (4) source node declared + in_quorum */
	if (cluster_conf_lookup_node((int32)env->source_node_id) == NULL)
		return false;
	if (!cluster_qvotec_in_quorum())
		return false;

	/* (5) opcode 属 family + self-source drop */
	if (payload_opcode < opcode_min || payload_opcode > opcode_max)
		return false;
	if ((int)env->source_node_id == cluster_node_id)
		return false;

	return true;
}

void
cluster_ges_request_handler(const ClusterICEnvelope *env, const void *payload)
{
	const GesRequestPayload *req;
	uint64 holder_epoch;
	bool payload_node_must_be_source;

	Assert(env != NULL);
	Assert(cluster_ges_state != NULL);

	pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);

	if (payload == NULL) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}
	req = (const GesRequestPayload *)payload;

	holder_epoch
		= ((uint64)req->holder_cluster_epoch_lo) | (((uint64)req->holder_cluster_epoch_hi) << 32);

	/* spec-2.16 v0.4 L1.8 + v0.5:  5-item validation.
	 *
	 * spec-2.17 adds BAST as master->holder advisory.  For that opcode the
	 * payload holder node is the local target, not the envelope source.  The
	 * remaining request-family opcodes still identify the remote sender and
	 * must match env->source_node_id. */
	payload_node_must_be_source = (req->opcode != GES_REQ_OPCODE_BAST);
	if (!ges_validate_inbound(env, req->holder_node_id, holder_epoch, req->opcode,
							  GES_REQ_OPCODE_REQUEST, GES_REQ_OPCODE_CANCEL_PENDING,
							  payload_node_must_be_source)) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	/* spec-2.17 checkpoint dispatch.  These opcodes are accepted and
	 * accounted explicitly so they are not misclassified as validation
	 * failures.  Full BAST/CANCEL/deadlock state machines land with the
	 * caller-side activation path; until then, keep behavior fail-closed. */
	switch ((GesRequestOpcode)req->opcode) {
	case GES_REQ_OPCODE_BAST:
		cluster_grd_inc_bast_received();
		return;
	case GES_REQ_OPCODE_BAST_ACK:
		cluster_grd_inc_bast_ack();
		return;
	case GES_REQ_OPCODE_DEADLOCK_PROBE:
		/*
		 * spec-2.19 Sprint A Step 3 D8 — LMD ownership gate.  HC4 exact
		 * predicate (v0.3 codex P1.5):cluster_lmd_is_ready() returns true
		 * iff state == CLUSTER_LMD_READY.  禁止 `state >= LMD_READY` 数值
		 * 比较 (enum 不连续 DRAINING=3 / STOPPED=4 / DISABLED=5).
		 *
		 *   - LMD READY → submit_wait_edge consumes the probe (HC6 skeleton:
		 *     ++lmd_edge_submission_count + ConditionVariableBroadcast; real
		 *     wait-for graph maintenance + Tarjan defer spec-2.20+).
		 *   - LMD not READY (DISABLED / NOT_STARTED / STARTING / DRAINING /
		 *     STOPPED / CRASHED) → caller-side legacy drop (spec-2.17
		 *     placeholder retained; cluster.lmd_enabled=off startup-time
		 *     fallback path).  HC1 fail-closed for backend-facing call paths
		 *     applies in spec-2.20+ when caller-side legacy is fully
		 *     deprecated — inter-instance opcode handler here cannot
		 *     ereport(ERROR) without crashing LMON.
		 *
		 * v0.2 P1.3 (53R81 backend-facing fail-closed):this opcode handler
		 * is the inter-instance receiver path (probe arrives from another
		 * node);53R81 ereport applies to backend-facing local API at the
		 * lock acquisition path which lands in spec-2.20+ with the 7-step
		 * state machine wire callsite.
		 *
		 * v0.3 P1.6 / HC6 skeleton 占位不等于假装工作:LMD does not save
		 * the probe / does not maintain graph;only submission counter +
		 * CV broadcast.  Real graph maintenance推 spec-2.20+ 同 spec ship
		 * producer + consumer (L114 family).
		 */
		if (cluster_lmd_is_ready())
		{
			cluster_lmd_submit_wait_edge();
			return;
		}
		cluster_grd_inc_deadlock_probe_drop();
		return;
	case GES_REQ_OPCODE_CANCEL_PENDING:
		return;
	case GES_REQ_OPCODE_REQUEST:
	case GES_REQ_OPCODE_CONVERT:
	case GES_REQ_OPCODE_RELEASE:
		break;
	}

	/* Phase 1 (handler):  enqueue into work_queue.  Grant decision runs
	 * Phase 2 in LMON tick body (Step 4 D9 wires).  work_queue full →
	 * REJECT_BUSY reply via reserved pool (I46 nofail). */
	if (!cluster_grd_work_queue_enqueue(env->source_node_id, payload, sizeof(*req))) {
		GesReplyPayload reject;

		cluster_grd_inc_ges_work_queue_full();
		memset(&reject, 0, sizeof(reject));
		reject.opcode = GES_REPLY_OPCODE_REJECT;
		reject.reject_reason = GES_REJECT_REASON_WORK_QUEUE_FULL;
		reject.holder_node_id = req->holder_node_id;
		reject.holder_procno = req->holder_procno;
		reject.holder_cluster_epoch_lo = req->holder_cluster_epoch_lo;
		reject.holder_cluster_epoch_hi = req->holder_cluster_epoch_hi;
		reject.holder_request_id_lo = req->holder_request_id_lo;
		reject.holder_request_id_hi = req->holder_request_id_hi;
		memcpy(reject.resid, req->resid, sizeof(reject.resid));
		cluster_grd_outbound_enqueue_lmon_reply(env->source_node_id, &reject, sizeof(reject));
		return;
	}

	/*
	 * spec-2.18 Sprint A Step 1-6 skeleton: LMS daemon does not yet own
	 * the work_queue drain loop; LMON tick body remains the sole consumer.
	 * The cluster_lms_wake_drain() producer hook is retained as a no-op
	 * compatibility surface and will be wired (alongside the LMS-side CV
	 * consumer) in the Hardening round once the LMS ownership transfer is
	 * verified safe end-to-end.
	 */
}

void
cluster_ges_reply_handler(const ClusterICEnvelope *env, const void *payload)
{
	const GesReplyPayload *rep;
	uint64 holder_epoch;

	Assert(env != NULL);
	Assert(cluster_ges_state != NULL);

	pg_atomic_fetch_add_u64(&cluster_ges_state->reply_defer_count, 1);

	if (payload == NULL) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}
	rep = (const GesReplyPayload *)payload;

	holder_epoch
		= ((uint64)rep->holder_cluster_epoch_lo) | (((uint64)rep->holder_cluster_epoch_hi) << 32);

	if (!ges_validate_inbound(env, rep->holder_node_id, holder_epoch, rep->opcode,
							  GES_REPLY_OPCODE_GRANT, GES_REPLY_OPCODE_REJECT, false)) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	/* Step 4 D9 wires pending_signal (CAS state + SetLatch).  For Step 3,
	 * the validation pass is recorded via reply_defer_count.  Real
	 * signal lands when pending table HTAB is allocated in Step 4. */
	ereport(DEBUG2, (errmsg_internal("cluster_ges_reply: validated reply opcode=%u "
									 "reject_reason=%u from peer %u (Step 3 — pending signal "
									 "wires Step 4)",
									 rep->opcode, rep->reject_reason, env->source_node_id)));
}

int
cluster_ges_lmon_drain_work_queue(void)
{
	ClusterGrdWorkItem item;
	int drained = 0;

	while (drained < 64 && cluster_grd_work_queue_dequeue(&item)) {
		const GesRequestPayload *req;
		GesReplyPayload reply;

		drained++;

		if (item.payload_len < sizeof(GesRequestPayload)) {
			cluster_grd_inc_ges_inbound_validation_fail();
			continue;
		}

		req = (const GesRequestPayload *)item.payload;

		/* RELEASE is cleanup-only in the current substrate path. */
		if (req->opcode == GES_REQ_OPCODE_RELEASE)
			continue;

		/*
		 * The full grant/convert state machine is not wired to PG lock.c yet.
		 * Reject rather than grant so a future caller can fail closed instead
		 * of observing a false grant.
		 */
		memset(&reply, 0, sizeof(reply));
		reply.opcode = GES_REPLY_OPCODE_REJECT;
		reply.reject_reason = GES_REJECT_REASON_LOCK_CONFLICT;
		reply.holder_node_id = req->holder_node_id;
		reply.holder_procno = req->holder_procno;
		reply.holder_cluster_epoch_lo = req->holder_cluster_epoch_lo;
		reply.holder_cluster_epoch_hi = req->holder_cluster_epoch_hi;
		reply.holder_request_id_lo = req->holder_request_id_lo;
		reply.holder_request_id_hi = req->holder_request_id_hi;
		memcpy(reply.resid, req->resid, sizeof(reply.resid));

		cluster_grd_outbound_enqueue_lmon_reply(item.source_node_id, &reply, sizeof(reply));
	}

	return drained;
}


/* ============================================================
 * Counter accessors (spec-2.13 D4).
 *
 *	Used by cluster_debug emit_row to surface counters in
 *	pg_cluster_state (category='ges').
 * ============================================================ */

uint64
cluster_ges_request_defer_count(void)
{
	Assert(cluster_ges_state != NULL);
	return pg_atomic_read_u64(&cluster_ges_state->request_defer_count);
}

uint64
cluster_ges_reply_defer_count(void)
{
	Assert(cluster_ges_state != NULL);
	return pg_atomic_read_u64(&cluster_ges_state->reply_defer_count);
}
