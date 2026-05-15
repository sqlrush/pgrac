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
#include "cluster/cluster_ges_reply_wait.h" /* spec-2.23 D1 5-tuple wait HTAB */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_guc.h" /* cluster_node_id + cluster_ges_request_timeout_ms */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_in_quorum */
#include "cluster/cluster_conf.h"	/* cluster_conf_lookup_node */
#include "cluster/cluster_shmem.h"
#include "storage/condition_variable.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/wait_event.h"


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
	uint32 opcode;
	uint64 holder_epoch;
	bool payload_node_must_be_source;

	Assert(env != NULL);
	Assert(cluster_ges_state != NULL);

	pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);

	if (payload == NULL) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}
	if (env->payload_length < sizeof(uint32)) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	memcpy(&opcode, payload, sizeof(opcode));

	/*
	 * DEADLOCK_PROBE / DEADLOCK_REPORT use dedicated payload structs, not the
	 * 48-byte GesRequestPayload.  Handle them before the generic request cast
	 * so a short PROBE frame cannot be misparsed as holder metadata.
	 */
	if (opcode == GES_REQ_OPCODE_DEADLOCK_PROBE) {
		char report_buf[sizeof(GesDeadlockReportHeader)];
		Size report_len = sizeof(report_buf);
		const GesDeadlockProbePayload *probe;
		uint64 accepted_epoch;

		if (env->payload_length != sizeof(GesDeadlockProbePayload)) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		probe = (const GesDeadlockProbePayload *)payload;
		accepted_epoch = cluster_epoch_get_current();
		if (probe->coordinator_node_id != env->source_node_id || env->epoch != accepted_epoch
			|| cluster_conf_lookup_node((int32)env->source_node_id) == NULL
			|| !cluster_qvotec_in_quorum() || (int)env->source_node_id == cluster_node_id) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		if (cluster_lmd_is_ready()) {
			(void)cluster_ges_deadlock_probe_handler(probe, report_buf, &report_len);
			return;
		}
		cluster_grd_inc_deadlock_probe_drop();
		return;
	}
	if (opcode == GES_REQ_OPCODE_DEADLOCK_REPORT) {
		const GesDeadlockReportHeader *report;
		uint64 accepted_epoch;

		if (env->payload_length < sizeof(GesDeadlockReportHeader)) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		report = (const GesDeadlockReportHeader *)payload;
		accepted_epoch = cluster_epoch_get_current();
		if (report->responding_node_id != env->source_node_id || env->epoch != accepted_epoch
			|| cluster_conf_lookup_node((int32)env->source_node_id) == NULL
			|| !cluster_qvotec_in_quorum() || (int)env->source_node_id == cluster_node_id) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		/* Collection/union Tarjan is deferred to the cross-node activation spec. */
		return;
	}
	if (env->payload_length != sizeof(GesRequestPayload)) {
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
	case GES_REQ_OPCODE_CANCEL_PENDING:
		return;
	case GES_REQ_OPCODE_REQUEST:
	case GES_REQ_OPCODE_CONVERT:
	case GES_REQ_OPCODE_RELEASE:
		break;
	case GES_REQ_OPCODE_DEADLOCK_PROBE:
	case GES_REQ_OPCODE_DEADLOCK_REPORT:
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	/* Phase 1 (handler):  enqueue into work_queue.  Grant decision runs
	 * Phase 2 in LMON tick body (Step 4 D9 wires).  work_queue full →
	 * REJECT_BUSY reply via reserved pool (I46 nofail). */
	if (!cluster_grd_work_queue_enqueue(env->source_node_id, payload, sizeof(*req))) {
		GesReplyPayload reject;

		cluster_grd_inc_ges_work_queue_full();
		memset(&reject, 0, sizeof(reject));
		reject.opcode = GES_REPLY_OPCODE_REJECT;
		/* PGRAC: spec-2.23 D1 / HC17 — echo original request opcode so the
		 * sender's reply wait table 5-tuple key matches.  Without this echo
		 * REQUEST and RELEASE replies sharing the same request_id slot would
		 * collide in the wait table. */
		reject.reply_for_opcode = req->opcode;
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

	/*
	 * PGRAC: spec-2.23 D2 / HC17 — 5-tuple reply correlation.
	 *
	 *	The reply identifies the original caller via the holder tuple
	 *	(holder_node_id == this node post-validation) plus the echoed
	 *	request_id and reply_for_opcode.  Build the same 5-tuple key
	 *	the caller used when inserting its wait entry, then look up.
	 *
	 *	Lookup miss → late reply (caller already timed out / canceled).
	 *	Drop silently (handler context cannot ereport) and bump the
	 *	late-drop counter so dump_ges surfaces it.
	 */
	{
		GesReplyWaitKey key;
		GesReplyWaitEntry *entry;

		memset(&key, 0, sizeof(key));
		key.request_id = ((uint64) rep->holder_request_id_lo)
						 | (((uint64) rep->holder_request_id_hi) << 32);
		key.source_node_id = cluster_node_id;	   /* this node was the sender */
		key.dest_node_id = (int32) env->source_node_id; /* replying master */
		key.request_opcode = rep->reply_for_opcode;
		key.cluster_epoch = holder_epoch;

		entry = cluster_ges_reply_wait_lookup(&key);
		if (entry == NULL) {
			cluster_ges_inc_reply_late_drop();
			ereport(DEBUG2, (errmsg_internal("cluster_ges_reply: late reply (no waiter) "
											 "request_id=" UINT64_FORMAT
											 " opcode=%u from peer %u",
											 key.request_id, rep->reply_for_opcode,
											 env->source_node_id)));
			return;
		}

		cluster_ges_reply_wait_wake(entry, rep->opcode, rep->reject_reason);
	}
}

/*
 * Build a GES_REPLY GRANT envelope addressed to a popped waiter and
 * enqueue it via the LMON reply ring.  Helper for the drain path; keeps
 * the envelope construction local to the file.
 */
static void
ges_send_grant_reply(int32 dest_node_id, const ClusterGrdHolderId *holder,
					 const ClusterResId *resid, uint32 request_opcode)
{
	GesReplyPayload reply;

	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REPLY_OPCODE_GRANT;
	reply.reply_for_opcode = request_opcode;
	reply.reject_reason = GES_REJECT_REASON_NONE;
	reply.holder_node_id = (uint32) holder->node_id;
	reply.holder_procno = holder->procno;
	reply.holder_cluster_epoch_lo = (uint32) (holder->cluster_epoch & 0xffffffffu);
	reply.holder_cluster_epoch_hi = (uint32) (holder->cluster_epoch >> 32);
	reply.holder_request_id_lo = (uint32) (holder->request_id & 0xffffffffu);
	reply.holder_request_id_hi = (uint32) (holder->request_id >> 32);
	memcpy(reply.resid, resid, sizeof(reply.resid));
	cluster_grd_outbound_enqueue_lmon_reply((uint32) dest_node_id, &reply, sizeof(reply));
}

int
cluster_ges_lmon_drain_work_queue(void)
{
	ClusterGrdWorkItem item;
	int drained = 0;

	while (drained < 64 && cluster_grd_work_queue_dequeue(&item)) {
		const GesRequestPayload *req;
		ClusterGrdHolderId holder;
		ClusterResId resid;
		uint64 holder_epoch;
		uint64 holder_request_id;

		drained++;

		if (item.payload_len < sizeof(GesRequestPayload)) {
			cluster_grd_inc_ges_inbound_validation_fail();
			continue;
		}

		req = (const GesRequestPayload *) item.payload;

		holder_epoch = ((uint64) req->holder_cluster_epoch_lo)
					   | (((uint64) req->holder_cluster_epoch_hi) << 32);
		holder_request_id = ((uint64) req->holder_request_id_lo)
							| (((uint64) req->holder_request_id_hi) << 32);

		holder.node_id = req->holder_node_id;
		holder.procno = req->holder_procno;
		holder.cluster_epoch = holder_epoch;
		holder.request_id = holder_request_id;
		memcpy(&resid, req->resid, sizeof(resid));

		switch ((GesRequestOpcode) req->opcode) {
			case GES_REQ_OPCODE_REQUEST:
			case GES_REQ_OPCODE_CONVERT: {
				/*
				 * spec-2.23 D6 — GRD-owned conflict matrix + waiter queue.
				 *
				 *	enqueue_or_grant handles the entry lock, conflict scan
				 *	via DoLockModesConflict, and either grants immediately
				 *	or enqueues a waiter carrying the full reply identity
				 *	(HC17/HC19).  conflict_holders[] surface is consumed by
				 *	Step 5 D4 cluster_ges_send_bast_targeted (HC18 BAST
				 *	target filter).
				 */
				ClusterGrdConflictHolder conflict_holders[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
				int n_conflict = 0;
				ClusterGrdGrantAction action;

				action = cluster_grd_entry_enqueue_or_grant(
					&resid, &holder, (int32) item.source_node_id, holder_request_id,
					req->opcode, (int) req->lockmode, conflict_holders, &n_conflict);

				if (action == CLUSTER_GRD_GRANT_NOW) {
					ges_send_grant_reply((int32) item.source_node_id, &holder, &resid,
										 req->opcode);
				} else if (action == CLUSTER_GRD_ENQUEUED_WAITER) {
					/*
					 * Step 5 D4 wires the actual targeted BAST send here.
					 * For Step 4 we just count the conflict surface so
					 * dump_ges observability reflects how often the LMS
					 * had to enqueue a waiter behind incompatible holders.
					 */
					(void) n_conflict;
					(void) conflict_holders;
				} else if (action == CLUSTER_GRD_WAIT_QUEUE_FULL) {
					GesReplyPayload reject;

					memset(&reject, 0, sizeof(reject));
					reject.opcode = GES_REPLY_OPCODE_REJECT;
					reject.reply_for_opcode = req->opcode;
					reject.reject_reason = GES_REJECT_REASON_WORK_QUEUE_FULL;
					reject.holder_node_id = req->holder_node_id;
					reject.holder_procno = req->holder_procno;
					reject.holder_cluster_epoch_lo = req->holder_cluster_epoch_lo;
					reject.holder_cluster_epoch_hi = req->holder_cluster_epoch_hi;
					reject.holder_request_id_lo = req->holder_request_id_lo;
					reject.holder_request_id_hi = req->holder_request_id_hi;
					memcpy(reject.resid, req->resid, sizeof(reject.resid));
					cluster_grd_outbound_enqueue_lmon_reply(item.source_node_id, &reject,
															sizeof(reject));
				}
				/* CLUSTER_GRD_NOT_READY → silently retry on next drain tick. */
				break;
			}
			case GES_REQ_OPCODE_RELEASE: {
				/*
				 * spec-2.23 D6 — release_and_pop_compatible_waiter wakes
				 * the first FIFO-compatible waiter (if any) and returns
				 * its identity so we can send a GES_REPLY GRANT back.
				 */
				ClusterGrdWaiterIdentity granted[1];
				int n_granted;

				n_granted = cluster_grd_entry_release_and_pop_compatible_waiter(
					&resid, &holder, granted, lengthof(granted));

				/* Reply GRANT to the original releaser (acks the RELEASE). */
				ges_send_grant_reply((int32) item.source_node_id, &holder, &resid,
									 req->opcode);

				for (int i = 0; i < n_granted; i++) {
					ges_send_grant_reply(granted[i].source_node_id, &granted[i].holder, &resid,
										 granted[i].request_opcode);
				}
				break;
			}
			default:
				/*
				 * BAST / BAST_ACK / DEADLOCK_* opcodes are dispatched in
				 * cluster_ges_request_handler; should never reach the
				 * drain path.  Count as inbound validation fail to surface
				 * any future routing regression.
				 */
				cluster_grd_inc_ges_inbound_validation_fail();
				break;
		}
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


/* ============================================================
 * spec-2.21 D8 — GES request/release send-and-wait stubs.
 *
 *	Minimal real implementation for the ADVISORY MVP single-node case.
 *	Local-master requests may continue through the local S5 promote path.
 *	Remote-master requests fail closed until the real GES_REQUEST wire
 *	send/reply pipeline ships.  When spec-2.23 BAST 配套 ships that
 *	pipeline, these helpers will:
 *	  - cluster_ges_send_request_and_wait:enqueue GesRequestPayload + wait
 *	    on cluster_ges_reply cv;return reject_reason from GesReplyPayload.
 *	  - cluster_ges_send_release_and_wait:enqueue GES_RELEASE + bounded
 *	    ACK wait.
 *
 *	For now both call the local LMS handler stub directly and inc deferred
 *	counters so dump_ges remains observable.
 * ============================================================ */

uint32
cluster_ges_send_request_and_wait(const struct ClusterResId *resid, uint32 lockmode,
								  const struct ClusterGrdHolderId *holder, uint64 request_id,
								  int timeout_ms)
{
	int32 master;
	GesReplyWaitKey key;
	GesReplyWaitEntry *entry;
	GesRequestPayload req;
	TimestampTz deadline;
	uint64 epoch;
	int effective_timeout_ms;
	uint32 reject_reason;

	if (resid == NULL || holder == NULL)
		return GES_REJECT_REASON_TIMEOUT;

	master = cluster_grd_lookup_master(resid);

	/*
	 * Local-master fast path:  no cross-node IPC needed.  The LMS local
	 * handle path runs entirely in-process (Step 4 D6 真激活 conflict
	 * matrix + waiter queue).  Skeleton phase returns immediate grant so
	 * the spec-2.21 caller can continue into the PG-native lock path.
	 */
	if (master < 0 || master == cluster_node_id) {
		if (cluster_ges_state != NULL)
			pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);
		return 0; /* GES_REJECT_NONE = grant OK (local-master MVP) */
	}

	/*
	 * Remote-master path:  build reply wait entry (HC17 5-tuple) BEFORE
	 * sending the request so the reply handler cannot race past the
	 * waiter.  Then send via outbound queue; sleep on CV up to the
	 * effective timeout; delete entry on wake or timeout (HC17:
	 * unconditional delete prevents late reply matching a recycled slot).
	 */
	epoch = cluster_epoch_get_current();
	effective_timeout_ms = timeout_ms > 0 ? timeout_ms : cluster_ges_request_timeout_ms;
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), effective_timeout_ms);

	memset(&key, 0, sizeof(key));
	key.request_id = request_id;
	key.source_node_id = cluster_node_id;
	key.dest_node_id = master;
	key.request_opcode = GES_REQ_OPCODE_REQUEST;
	key.cluster_epoch = epoch;

	entry = cluster_ges_reply_wait_insert(&key, deadline);
	if (entry == NULL) {
		/* 53R71 fail-closed at caller (spec-2.16 errcode ship). */
		ereport(WARNING,
				(errmsg_internal("cluster_ges_send_request_and_wait: reply wait table full "
								 "(request_id=" UINT64_FORMAT " dest=%d) — fail closed",
								 request_id, master)));
		return GES_REJECT_REASON_TIMEOUT;
	}

	/* Build wire payload. */
	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.lockmode = lockmode;
	req.holder_node_id = (uint32) holder->node_id;
	req.holder_procno = (uint32) holder->procno;
	req.holder_cluster_epoch_lo = (uint32) (holder->cluster_epoch & 0xffffffffu);
	req.holder_cluster_epoch_hi = (uint32) (holder->cluster_epoch >> 32);
	req.holder_request_id_lo = (uint32) (request_id & 0xffffffffu);
	req.holder_request_id_hi = (uint32) (request_id >> 32);
	memcpy(req.resid, resid, sizeof(req.resid));

	if (!cluster_grd_outbound_enqueue_backend_request((uint32) master, &req, sizeof(req))) {
		/* Outbound ring full — fail closed.  Caller may retry. */
		cluster_ges_reply_wait_delete(&key);
		return GES_REJECT_REASON_WORK_QUEUE_FULL;
	}

	if (cluster_ges_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);

	/*
	 * Wait for reply via CV.  Reuse WAIT_EVENT_CLUSTER_GES_S4_WAIT
	 * (spec-2.20 ship);  spec-2.23 D12 (Step 9) introduces a dedicated
	 * WAIT_EVENT_CLUSTER_GES_REPLY_WAIT and the wait-event name swap is
	 * a 1-line edit at that step.
	 */
	ConditionVariablePrepareToSleep(&entry->cv);
	while (!entry->ready) {
		if (!ConditionVariableTimedSleep(&entry->cv, effective_timeout_ms,
										 WAIT_EVENT_CLUSTER_GES_S4_WAIT)) {
			/* HC17:  timeout MUST unconditionally delete entry. */
			cluster_ges_reply_wait_delete(&key);
			ConditionVariableCancelSleep();
			return GES_REJECT_REASON_TIMEOUT;
		}
	}
	ConditionVariableCancelSleep();

	/* Capture verdict, then delete entry (HC17 pairing invariant). */
	reject_reason = entry->reject_reason;
	cluster_ges_reply_wait_delete(&key);

	return reject_reason;
}

uint32
cluster_ges_send_release_and_wait(const struct ClusterResId *resid,
								  const struct ClusterGrdHolderId *holder, uint64 request_id)
{
	int32 master;
	GesReplyWaitKey key;
	GesReplyWaitEntry *entry;
	GesRequestPayload req;
	TimestampTz deadline;
	uint64 epoch;
	int timeout_ms;
	uint32 reject_reason;

	if (resid == NULL || holder == NULL)
		return GES_REJECT_REASON_TIMEOUT;

	master = cluster_grd_lookup_master(resid);

	/*
	 * Local-master path:  release runs entirely in-process (Step 4 D6
	 * release_and_pop_compatible_waiter真激活).  No CV round-trip needed.
	 */
	if (master < 0 || master == cluster_node_id) {
		if (cluster_ges_state != NULL)
			pg_atomic_fetch_add_u64(&cluster_ges_state->reply_defer_count, 1);
		return 0;
	}

	/*
	 * Remote-master path:  send GES_RELEASE + bounded ACK wait.  Reply
	 * wait key uses request_opcode = GES_REQ_OPCODE_RELEASE so REQUEST
	 * and RELEASE replies sharing the same request_id slot do not
	 * collide in the 5-tuple HTAB (HC17).
	 *
	 *	If the holder backend had cluster_grd_bast_pending set, the
	 *	RELEASE doubles as a logical BAST_ACK (HC19) — Step 5 D5 wires
	 *	the bast_ack_flag carry on the payload + increment.  Step 3
	 *	skeleton: just send the RELEASE and bump release_ack_count on
	 *	GRANT reply.
	 */
	epoch = cluster_epoch_get_current();
	timeout_ms = cluster_ges_request_timeout_ms;
	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout_ms);

	memset(&key, 0, sizeof(key));
	key.request_id = request_id;
	key.source_node_id = cluster_node_id;
	key.dest_node_id = master;
	key.request_opcode = GES_REQ_OPCODE_RELEASE;
	key.cluster_epoch = epoch;

	entry = cluster_ges_reply_wait_insert(&key, deadline);
	if (entry == NULL)
		return GES_REJECT_REASON_TIMEOUT;

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_RELEASE;
	req.lockmode = 0;
	req.holder_node_id = (uint32) holder->node_id;
	req.holder_procno = (uint32) holder->procno;
	req.holder_cluster_epoch_lo = (uint32) (holder->cluster_epoch & 0xffffffffu);
	req.holder_cluster_epoch_hi = (uint32) (holder->cluster_epoch >> 32);
	req.holder_request_id_lo = (uint32) (request_id & 0xffffffffu);
	req.holder_request_id_hi = (uint32) (request_id >> 32);
	memcpy(req.resid, resid, sizeof(req.resid));

	if (!cluster_grd_outbound_enqueue_backend_request((uint32) master, &req, sizeof(req))) {
		cluster_ges_reply_wait_delete(&key);
		return GES_REJECT_REASON_WORK_QUEUE_FULL;
	}

	if (cluster_ges_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_ges_state->reply_defer_count, 1);

	ConditionVariablePrepareToSleep(&entry->cv);
	while (!entry->ready) {
		if (!ConditionVariableTimedSleep(&entry->cv, timeout_ms,
										 WAIT_EVENT_CLUSTER_GES_S4_WAIT)) {
			cluster_ges_reply_wait_delete(&key);
			ConditionVariableCancelSleep();
			return GES_REJECT_REASON_TIMEOUT;
		}
	}
	ConditionVariableCancelSleep();

	reject_reason = entry->reject_reason;
	cluster_ges_reply_wait_delete(&key);

	if (reject_reason == 0)
		cluster_ges_inc_release_ack();

	return reject_reason;
}


/* ============================================================
 * spec-2.22 D6 — DEADLOCK_PROBE handler scaffold.
 *
 *	Coordinator (lowest active node_id LMD) broadcasts a PROBE;each
 *	probed node's LMD handler runs this body to snapshot its own graph
 *	and prepare a REPORT.  Production send (cluster_ges_send) wired in
 *	spec-2.23 BAST 配套;本 spec ships handler + payload format only.
 *
 *	HC15 read-only:  this handler MUST NOT mutate remote state.  Only
 *	snapshot the local graph via cluster_lmd_graph_snapshot_copy().
 * ============================================================ */

int
cluster_ges_deadlock_probe_handler(const GesDeadlockProbePayload *probe, void *out_buf,
								   Size *inout_buflen)
{
	GesDeadlockReportHeader *hdr;
	Size header_size = sizeof(GesDeadlockReportHeader);
	Size edges_size;
	int max_edges;
	int n_copied;
	uint64 gen_at_snapshot;
	ClusterLmdWaitEdge *edges_dst;

	if (probe == NULL || out_buf == NULL || inout_buflen == NULL)
		return -1;
	if (probe->opcode != GES_REQ_OPCODE_DEADLOCK_PROBE)
		return -2;
	if (*inout_buflen < header_size)
		return -3; /* not enough room for even a zero-edge REPORT */

	pgstat_report_wait_start(PG_WAIT_EXTENSION | WAIT_EVENT_CLUSTER_LMD_PROBE);

	max_edges = (int)((*inout_buflen - header_size) / sizeof(ClusterLmdWaitEdge));
	if (max_edges < 0)
		max_edges = 0;

	hdr = (GesDeadlockReportHeader *)out_buf;
	memset(hdr, 0, sizeof(*hdr));
	hdr->opcode = GES_REQ_OPCODE_DEADLOCK_REPORT;
	hdr->responding_node_id = (uint32)cluster_node_id;
	hdr->probe_id = probe->probe_id;
	hdr->lmd_ready_state = (uint32)cluster_lmd_is_ready();

	edges_dst = (ClusterLmdWaitEdge *)((char *)out_buf + header_size);
	n_copied = cluster_lmd_graph_snapshot_copy(edges_dst, max_edges, &gen_at_snapshot);
	hdr->nedges = (uint32)n_copied;
	hdr->graph_generation = gen_at_snapshot;

	edges_size = (Size)n_copied * sizeof(ClusterLmdWaitEdge);
	*inout_buflen = header_size + edges_size;

	pgstat_report_wait_end();
	return 0;
}
