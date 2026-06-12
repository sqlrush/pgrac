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
#include "cluster/cluster_lmd.h"	   /* + spec-2.23 D8 probe collector receive */
#include "cluster/cluster_ges_dedup.h" /* spec-2.27 D2 / D3 — retransmit dedup */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_native_lock_probe.h" /* spec-2.25 D5 — probe protocol handlers */
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_guc.h" /* cluster_node_id + cluster_ges_request_timeout_ms */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_qvotec.h" /* cluster_qvotec_in_quorum */
#include "cluster/cluster_conf.h"	/* cluster_conf_lookup_node */
#include "cluster/cluster_shmem.h"
#include "storage/condition_variable.h"
#include "storage/proc.h"		/* MyProc->cluster_grd_bast_pending (D5) */
#include "storage/procarray.h"	/* ProcSignalReason dispatch helper */
#include "storage/procsignal.h" /* SendProcSignal + PROCSIG_CLUSTER_GES_BAST */
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

static inline uint64
ges_request_holder_epoch(const GesRequestPayload *req)
{
	return ((uint64)req->holder_cluster_epoch_lo) | (((uint64)req->holder_cluster_epoch_hi) << 32);
}

static inline uint64
ges_request_holder_request_id(const GesRequestPayload *req)
{
	return ((uint64)req->holder_request_id_lo) | (((uint64)req->holder_request_id_hi) << 32);
}

static inline uint64
ges_request_shard_master_generation(const GesRequestPayload *req)
{
	return ((uint64)req->shard_master_generation_lo)
		   | (((uint64)req->shard_master_generation_hi) << 32);
}

static inline bool
ges_request_uses_dedup(uint32 opcode)
{
	return opcode == GES_REQ_OPCODE_REQUEST || opcode == GES_REQ_OPCODE_CONVERT
		   || opcode == GES_REQ_OPCODE_RELEASE;
}

static void
ges_record_dedup_reply(uint32 source_node_id, uint32 opcode, uint64 request_id,
					   uint64 cluster_epoch, uint64 shard_master_generation,
					   const GesReplyPayload *reply)
{
	ClusterGesDedupKey key;

	if (!ges_request_uses_dedup(opcode) || reply == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = source_node_id;
	key.opcode = opcode;
	key.request_id = request_id;
	key.cluster_epoch = cluster_epoch;
	key.shard_master_generation = shard_master_generation;
	cluster_ges_dedup_record_reply(&key, (const uint8 *)reply, sizeof(*reply));
}

static void
ges_record_dedup_reply_for_request(uint32 source_node_id, const GesRequestPayload *req,
								   const GesReplyPayload *reply)
{
	if (req == NULL)
		return;
	ges_record_dedup_reply(source_node_id, req->opcode, ges_request_holder_request_id(req),
						   ges_request_holder_epoch(req), ges_request_shard_master_generation(req),
						   reply);
}

static void
ges_record_dedup_reply_for_waiter(const ClusterGrdWaiterIdentity *waiter,
								  const GesReplyPayload *reply)
{
	if (waiter == NULL)
		return;
	ges_record_dedup_reply((uint32)waiter->source_node_id, waiter->request_opcode,
						   waiter->request_id, waiter->holder.cluster_epoch,
						   waiter->shard_master_generation, reply);
}


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
	 * GesRequestPayload.  Handle them before the generic request cast
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
		/*
		 * spec-2.23 D8 — feed the coordinator's REPORT collector.  If the
		 * probe_id doesn't match the current in-flight scan, the receive
		 * helper returns false and we silently drop the late REPORT (HC8
		 * partial OK contract — a future scan tick will retry).
		 */
		(void)cluster_lmd_probe_collect_receive(report, env->payload_length);
		return;
	}
	/*
	 * PGRAC: spec-2.25 D6 / HC33 — NATIVE_LOCK_PROBE (opcode 9) & REPLY (opcode
	 * 10) use dedicated 32B payload structs.  Like DEADLOCK_PROBE/REPORT, they
	 * bypass the generic GesRequestPayload validator.  Both opcodes carry
	 * bespoke source/target validation per HC33:
	 *
	 *	NATIVE_LOCK_PROBE request:
	 *	  - envelope source must be the LMS owning the (resid-implied) shard
	 *	    (cross-checked via cluster_conf_lookup_node + quorum)
	 *	  - target (this node) processes the probe;  no explicit target field
	 *	    needed since envelope already routed to us
	 *	  - epoch must match current cluster epoch
	 *
	 *	NATIVE_LOCK_PROBE_REPLY:
	 *	  - payload.sender_node_id must equal env->source_node_id (HC33 dual
	 *	    check — prevents spoof where peer claims node A but envelope
	 *	    came from B)
	 *	  - collector slot lookup deferred to cluster_lms_native_probe_recv_reply
	 *	    (which applies HC36 stale-reply drop via probe_id epoch + expected
	 *	    bitmap match)
	 *
	 *	Skeleton:  handler body wired Step 6 (D5);  Step 1 only routes opcode
	 *	to the dispatcher + applies validation + counter accounting.  Production
	 *	dispatch lands when cluster_lms_* probe collector is online (Step 5).
	 */
	if (opcode == GES_REQ_OPCODE_NATIVE_LOCK_PROBE) {
		const GesNativeLockProbePayload *probe;
		LOCKTAG probe_locktag;
		ClusterResId probe_resid;
		int32 probe_master;
		uint64 accepted_epoch;

		if (env->payload_length != sizeof(GesNativeLockProbePayload)) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		probe = (const GesNativeLockProbePayload *)payload;
		memcpy(&probe_locktag, probe->locktag_bytes, sizeof(probe_locktag));
		cluster_grd_resid_encode(&probe_locktag, &probe_resid);
		probe_master = cluster_grd_lookup_master(&probe_resid);
		accepted_epoch = cluster_epoch_get_current();
		if (env->epoch != accepted_epoch
			|| cluster_conf_lookup_node((int32)env->source_node_id) == NULL
			|| !cluster_qvotec_in_quorum() || (int)env->source_node_id == cluster_node_id
			|| probe_master != (int32)env->source_node_id) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		cluster_ges_handle_native_lock_probe_request(env, probe);
		return;
	}
	if (opcode == GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY) {
		const GesNativeLockProbeReplyPayload *reply;
		uint64 accepted_epoch;

		if (env->payload_length != sizeof(GesNativeLockProbeReplyPayload)) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		reply = (const GesNativeLockProbeReplyPayload *)payload;
		accepted_epoch = cluster_epoch_get_current();
		/* HC33 dual-source check: payload.sender ≡ envelope source */
		if (reply->sender_node_id != env->source_node_id || env->epoch != accepted_epoch
			|| cluster_conf_lookup_node((int32)env->source_node_id) == NULL
			|| !cluster_qvotec_in_quorum() || (int)env->source_node_id == cluster_node_id) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		cluster_ges_handle_native_lock_probe_reply(env, reply);
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
	 * spec-2.17 adds BAST as master->holder advisory, and spec-2.24
	 * activates CANCEL_PENDING as coordinator->victim advisory.  For both
	 * opcodes the payload holder node is the local target, not the envelope
	 * source.  The remaining request-family opcodes still identify the
	 * remote sender and must match env->source_node_id. */
	payload_node_must_be_source
		= (req->opcode != GES_REQ_OPCODE_BAST && req->opcode != GES_REQ_OPCODE_CANCEL_PENDING);
	/* spec-4.6 D3:  opcode_max extended to REDECLARE_DONE (13).  Opcodes
	 * 8-11 passing the coarse range check still fail closed in the
	 * explicit switch below (dedicated-payload opcodes never reach this
	 * handler legitimately — early dispatch above). */
	if (!ges_validate_inbound(env, req->holder_node_id, holder_epoch, req->opcode,
							  GES_REQ_OPCODE_REQUEST, GES_REQ_OPCODE_REDECLARE_DONE,
							  payload_node_must_be_source)) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	/* spec-4.6 P0#3 cluster gate — fire-and-forget barrier announcement:
	 * record and return (no work queue, no reply, no dedup;  the sender
	 * re-announces each tick, the receiver write is a monotonic max). */
	if (req->opcode == GES_REQ_OPCODE_REDECLARE_DONE) {
		cluster_grd_recovery_mark_peer_done((int32)env->source_node_id, holder_epoch);
		return;
	}

	/* spec-2.17 checkpoint dispatch.  These opcodes are accepted and
	 * accounted explicitly so they are not misclassified as validation
	 * failures.  Full BAST/CANCEL/deadlock state machines land with the
	 * caller-side activation path; until then, keep behavior fail-closed. */
	switch ((GesRequestOpcode)req->opcode) {
	case GES_REQ_OPCODE_BAST: {
		/*
		 * spec-2.23 D5 — when the holder backend lives on this node,
		 * signal it via PROCSIG_CLUSTER_GES_BAST so the existing
		 * spec-2.17 handler path sets MyProc->cluster_grd_bast_pending.
		 * The natural LockRelease at transaction commit then carries
		 * the logical BAST_ACK on its GES_RELEASE envelope (HC19).
		 *
		 * Locating the target backend by procno alone requires the
		 * ProcArray; for now we increment the receive counter (spec-
		 * 2.17 ship behaviour) and rely on the spec-2.17 ProcSignal
		 * path being driven by the LMS-side helper.  Real cross-node
		 * signal forwarding lands with spec-2.24 retransmit + compound
		 * reliability (HC22 BAST_ACK lifecycle).
		 */
		cluster_grd_inc_bast_received();
		return;
	}
	case GES_REQ_OPCODE_BAST_ACK:
		cluster_grd_inc_bast_ack();
		return;
	case GES_REQ_OPCODE_CANCEL_PENDING: {
		/*
		 * spec-2.24 D1 / HC23 — cross-node victim cancel forwarding.
		 *
		 *	5-项 inbound validation already done above (line 260).
		 *	HC23 additional gate:  target_node_id == cluster_node_id
		 *	(envelope source is the coordinator;  payload holder_id
		 *	identifies the local victim target).  Then enqueue to
		 *	LMD-owned cancel queue;  LMD daemon tick body (D5)
		 *	performs HC24 4-tuple stale procno match and dispatches
		 *	to cluster_lmd_signal_local_victim().
		 */
		if (req->holder_node_id != (uint32)cluster_node_id) {
			cluster_grd_inc_ges_inbound_validation_fail();
			return;
		}
		if (cluster_lmd_cancel_queue_enqueue(env->source_node_id, req, sizeof(*req)))
			cluster_lmd_cross_node_cancel_received_count_inc(1);
		else
			cluster_lmd_cross_node_cancel_queue_full_count_inc(1);
		return;
	}
	case GES_REQ_OPCODE_REQUEST:
	case GES_REQ_OPCODE_CONVERT:
	case GES_REQ_OPCODE_RELEASE:
	case GES_REQ_OPCODE_REDECLARE: /* spec-4.6 D3 — same work_queue path */
		break;
	case GES_REQ_OPCODE_DEADLOCK_PROBE:
	case GES_REQ_OPCODE_DEADLOCK_REPORT:
	case GES_REQ_OPCODE_NATIVE_LOCK_PROBE:
	case GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY:
	case GES_REQ_OPCODE_PRIORITY_BOOST:
	case GES_REQ_OPCODE_REDECLARE_DONE: /* handled + returned above */
		/*
			 * spec-2.25 D6:  these opcodes use dedicated payload structs +
		 * early dispatch path above (line 205-/256-).  Reaching the
		 * GesRequestPayload switch means a request-sized envelope carried
		 * a non-request opcode — fail closed.
		 */
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	/*
	 * spec-2.27 D3 / HC51 / HC52 — receiver-side dedup pre-lookup.
	 *
	 *	Only REQUEST / CONVERT / RELEASE share the work_queue path and are
	 *	candidates for retransmit on the caller side (BAST advisory stays
	 *	best-effort per HC85;  CANCEL_PENDING / DEADLOCK_* use dedicated
	 *	dispatch).  CONVERT is included defensively even though MVP retry
	 *	loop only retransmits REQUEST + RELEASE; future spec-2.28+ may
	 *	extend retransmit to CONVERT, and dedup is idempotent for any
	 *	registered key.
	 *
	 *	5-tuple key constructed from holder + opcode + payload-carried
	 *	shard_master_generation.  IN_FLIGHT_DUPLICATE -> drop silently
	 *	(caller will keep waiting; the original work_queue entry will
	 *	complete and cache the reply).  CACHED_REPLY -> resend cached
	 *	GesReplyPayload via lmon_reply outbound, skip work_queue.  FULL ->
	 *	REJECT_BUSY fail-closed.  MISS_REGISTERED -> proceed to enqueue.
	 */
	if (req->opcode == GES_REQ_OPCODE_REQUEST || req->opcode == GES_REQ_OPCODE_CONVERT
		|| req->opcode == GES_REQ_OPCODE_RELEASE || req->opcode == GES_REQ_OPCODE_REDECLARE) {
		ClusterGesDedupKey dkey;
		uint8 dreply_buf[sizeof(GesReplyPayload)];
		uint16 dreply_len = 0;
		ClusterGesDedupLookupStatus dstatus;
		uint64 holder_request_id;

		holder_request_id = ges_request_holder_request_id(req);

		memset(&dkey, 0, sizeof(dkey));
		dkey.origin_node_id = (uint32)env->source_node_id;
		dkey.opcode = req->opcode;
		dkey.request_id = holder_request_id;
		dkey.cluster_epoch = holder_epoch;
		dkey.shard_master_generation = ges_request_shard_master_generation(req);

		dstatus = cluster_ges_dedup_lookup_or_register(&dkey, dreply_buf, sizeof(dreply_buf),
													   &dreply_len);
		switch (dstatus) {
		case CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE:
			/* Original request still being processed — drop the retransmit. */
			return;

		case CLUSTER_GES_DEDUP_CACHED_REPLY:
			/* Resend cached reply via lmon_reply outbound; no work_queue. */
			if (dreply_len > 0)
				cluster_grd_outbound_enqueue_lmon_reply(env->source_node_id, dreply_buf,
														(uint16)dreply_len);
			return;

		case CLUSTER_GES_DEDUP_STALE_REPROCESS:
			/* UNREACHABLE — STALE_REPROCESS is sweep-only bookkeeping
			 * (cluster_ges_dedup_drop_stale_entries counter); never
			 * returned by cluster_ges_dedup_lookup_or_register.  HC51 in
			 * cluster_ges_dedup.c §invalidation-model explicitly rejects
			 * inline stale detection (drop-and-reregister loop).  Stale
			 * entries are removed by the LMS restart sweep before the
			 * caller's retransmit arrives, after which the retransmit
			 * registers a fresh entry (MISS_REGISTERED).  Assert in debug
			 * to catch enum-misuse regressions. */
			Assert(false);
			return;

		case CLUSTER_GES_DEDUP_FULL: {
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
			cluster_grd_outbound_enqueue_lmon_reply(env->source_node_id, &reject, sizeof(reject));
			return;
		}

		case CLUSTER_GES_DEDUP_MISS_REGISTERED:
		default:
			/* Fresh entry registered.  Every drain path that emits a reply
			 * records the exact GesReplyPayload before enqueueing it, so later
			 * retransmits hit CACHED_REPLY instead of IN_FLIGHT_DUPLICATE. */
			break;
		}
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
		ges_record_dedup_reply_for_request(env->source_node_id, req, &reject);
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
		key.request_id
			= ((uint64)rep->holder_request_id_lo) | (((uint64)rep->holder_request_id_hi) << 32);
		key.source_node_id = cluster_node_id;		   /* this node was the sender */
		key.dest_node_id = (int32)env->source_node_id; /* replying master */
		key.request_opcode = rep->reply_for_opcode;
		key.cluster_epoch = holder_epoch;

		entry = cluster_ges_reply_wait_lookup(&key);
		if (entry == NULL) {
			cluster_ges_inc_reply_late_drop();
			ereport(DEBUG2,
					(errmsg_internal("cluster_ges_reply: late reply (no waiter) "
									 "request_id=" UINT64_FORMAT " opcode=%u from peer %u",
									 key.request_id, rep->reply_for_opcode, env->source_node_id)));
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
					 const ClusterResId *resid, uint32 request_opcode, GesReplyPayload *reply_out)
{
	GesReplyPayload reply;

	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REPLY_OPCODE_GRANT;
	reply.reply_for_opcode = request_opcode;
	reply.reject_reason = GES_REJECT_REASON_NONE;
	reply.holder_node_id = (uint32)holder->node_id;
	reply.holder_procno = holder->procno;
	reply.holder_cluster_epoch_lo = (uint32)(holder->cluster_epoch & 0xffffffffu);
	reply.holder_cluster_epoch_hi = (uint32)(holder->cluster_epoch >> 32);
	reply.holder_request_id_lo = (uint32)(holder->request_id & 0xffffffffu);
	reply.holder_request_id_hi = (uint32)(holder->request_id >> 32);
	memcpy(reply.resid, resid, sizeof(reply.resid));
	if (reply_out != NULL)
		*reply_out = reply;
	cluster_grd_outbound_enqueue_lmon_reply((uint32)dest_node_id, &reply, sizeof(reply));
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

		req = (const GesRequestPayload *)item.payload;

		holder_epoch = ges_request_holder_epoch(req);
		holder_request_id = ges_request_holder_request_id(req);

		holder.node_id = req->holder_node_id;
		holder.procno = req->holder_procno;
		holder.cluster_epoch = holder_epoch;
		holder.request_id = holder_request_id;
		memcpy(&resid, req->resid, sizeof(resid));

		switch ((GesRequestOpcode)req->opcode) {
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

			/*
				 * spec-4.6 D4 — master-side shard recovery gate.  A shard
				 * in FROZEN/REBUILDING must not serve new grants:  the
				 * holder rebuild (D3) is still filling holders[], so a
				 * grant decided against the partial set could double
				 * grant.  REJECT(SHARD_FROZEN);  the requester short-waits
				 * and retries (or raises 53R9I).  REDECLARE/RELEASE stay
				 * allowed (they are the rebuild traffic itself).
				 */
			if (cluster_grd_shard_phase(cluster_grd_shard_for_resource(&resid))
				!= GRD_SHARD_NORMAL) {
				GesReplyPayload reject;

				memset(&reject, 0, sizeof(reject));
				reject.opcode = GES_REPLY_OPCODE_REJECT;
				reject.reply_for_opcode = req->opcode;
				reject.reject_reason = GES_REJECT_REASON_SHARD_FROZEN;
				reject.holder_node_id = req->holder_node_id;
				reject.holder_procno = req->holder_procno;
				reject.holder_cluster_epoch_lo = req->holder_cluster_epoch_lo;
				reject.holder_cluster_epoch_hi = req->holder_cluster_epoch_hi;
				reject.holder_request_id_lo = req->holder_request_id_lo;
				reject.holder_request_id_hi = req->holder_request_id_hi;
				memcpy(reject.resid, req->resid, sizeof(reject.resid));
				ges_record_dedup_reply_for_request((uint32)item.source_node_id, req, &reject);
				cluster_grd_outbound_enqueue_lmon_reply(item.source_node_id, &reject,
														sizeof(reject));
				break;
			}

			action = cluster_grd_entry_enqueue_or_grant(
				&resid, &holder, (int32)item.source_node_id, holder_request_id,
				ges_request_shard_master_generation(req), req->opcode, (int)req->lockmode,
				conflict_holders, &n_conflict);

			if (action == CLUSTER_GRD_GRANT_NOW) {
				if (cluster_lms_native_probe_required(&resid, (LOCKMODE)req->lockmode)) {
					if (!cluster_lms_native_probe_schedule_grant(
							&resid, (LOCKMODE)req->lockmode, &holder, (int32)item.source_node_id,
							req->opcode, ges_request_shard_master_generation(req))) {
						GesReplyPayload reject;

						(void)cluster_grd_release_holder_by_id(&resid, &holder);
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
						ges_record_dedup_reply_for_request((uint32)item.source_node_id, req,
														   &reject);
						cluster_grd_outbound_enqueue_lmon_reply(item.source_node_id, &reject,
																sizeof(reject));
					}
				} else {
					GesReplyPayload reply;
					ges_send_grant_reply((int32)item.source_node_id, &holder, &resid, req->opcode,
										 &reply);
					ges_record_dedup_reply_for_request((uint32)item.source_node_id, req, &reply);
				}
			} else if (action == CLUSTER_GRD_ENQUEUED_WAITER) {
				/*
					 * spec-2.23 D4 / HC18 — targeted BAST.  conflict_holders
					 * was captured under entry->lock in enqueue_or_grant;
					 * send_bast_targeted re-verifies DoLockModesConflict at
					 * send time and skips entries that the concurrent
					 * release path may have already cleared.
					 */
				if (n_conflict > 0)
					cluster_ges_send_bast_targeted(&resid, (int)req->lockmode, conflict_holders,
												   n_conflict);
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
				ges_record_dedup_reply_for_request((uint32)item.source_node_id, req, &reject);
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
			{
				GesReplyPayload reply;
				ges_send_grant_reply((int32)item.source_node_id, &holder, &resid, req->opcode,
									 &reply);
				ges_record_dedup_reply_for_request((uint32)item.source_node_id, req, &reply);
			}

			for (int i = 0; i < n_granted; i++) {
				GesReplyPayload reply;
				ges_send_grant_reply(granted[i].source_node_id, &granted[i].holder, &resid,
									 granted[i].request_opcode, &reply);
				ges_record_dedup_reply_for_waiter(&granted[i], &reply);
			}
			break;
		}
		case GES_REQ_OPCODE_REDECLARE: {
			/*
				 * spec-4.6 D3 — cooperative holder rebind (insert-or-rebind).
				 *
				 *	The payload carries the NEW current-epoch holder
				 *	(validated above:  payload epoch == accepted epoch).
				 *	Match key for an existing holder = (node_id, procno,
				 *	lockmode) + resid;  match → overwrite the holder
				 *	identity in place (unaffected-shard rebind, idempotent
				 *	for retransmits);  no match → insert (remastered-shard
				 *	rebuild:  the new master fills holders[] from these
				 *	re-declarations ONLY).  Allowed in every shard phase —
				 *	this IS the rebuild traffic.
				 */
			ClusterGrdEntryResult rr;

			rr = cluster_grd_entry_rebind_or_insert_holder(
				&resid, &holder, (int32)item.source_node_id, (int)req->lockmode);
			if (rr == CLUSTER_GRD_ENTRY_OK) {
				GesReplyPayload reply;

				ges_send_grant_reply((int32)item.source_node_id, &holder, &resid, req->opcode,
									 &reply);
				ges_record_dedup_reply_for_request((uint32)item.source_node_id, req, &reply);
			} else {
				GesReplyPayload reject;

				memset(&reject, 0, sizeof(reject));
				reject.opcode = GES_REPLY_OPCODE_REJECT;
				reject.reply_for_opcode = req->opcode;
				reject.reject_reason = (rr == CLUSTER_GRD_ENTRY_FULL)
										   ? GES_REJECT_REASON_WORK_QUEUE_FULL
										   : GES_REJECT_REASON_LOCK_CONFLICT;
				reject.holder_node_id = req->holder_node_id;
				reject.holder_procno = req->holder_procno;
				reject.holder_cluster_epoch_lo = req->holder_cluster_epoch_lo;
				reject.holder_cluster_epoch_hi = req->holder_cluster_epoch_hi;
				reject.holder_request_id_lo = req->holder_request_id_lo;
				reject.holder_request_id_hi = req->holder_request_id_hi;
				memcpy(reject.resid, req->resid, sizeof(reject.resid));
				ges_record_dedup_reply_for_request((uint32)item.source_node_id, req, &reject);
				cluster_grd_outbound_enqueue_lmon_reply(item.source_node_id, &reject,
														sizeof(reject));
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

/*
 * spec-4.6 D3 — shared body for REQUEST and REDECLARE send-and-wait.
 *	Both opcodes ride the identical GesRequestPayload + retransmit +
 *	5-tuple reply-wait machinery;  only the opcode byte differs (and
 *	REDECLARE is idempotent on the master, so retransmit stays safe).
 */
static uint32
ges_send_request_opcode_and_wait(const struct ClusterResId *resid, uint32 lockmode,
								 const struct ClusterGrdHolderId *holder, uint64 request_id,
								 int timeout_ms, uint32 send_opcode)
{
	int32 master;
	GesReplyWaitKey key;
	GesReplyWaitEntry *entry;
	GesRequestPayload req;
	TimestampTz deadline;
	uint64 epoch;
	uint64 master_gen;
	int effective_timeout_ms;
	uint32 reject_reason;
	int attempt;
	int backoff_ms;
	int max_attempts;
	bool perpetual;
	bool warned_starvation;
	bool debug1_starvation_fired;

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
	 * spec-2.27 D3 — Remote-master path with retransmit + dedup.
	 *
	 *	Sample shard_master_generation ONCE up front so all retransmits in
	 *	this acquire cycle carry the same value (the LMS receiver dedup
	 *	HTAB keys are stable across the retransmit loop).
	 *
	 *	timeout_ms semantics (HC53):
	 *	  -1  → perpetual wait;  no absolute deadline;  retransmit forever.
	 *	   0  → use cluster.ges_request_timeout_ms.
	 *	  >0  → caller-supplied finite timeout.
	 *
	 *	cluster.ges_retransmit_max_attempts (HC52):
	 *	  finite mode:  abort with 53R70 after attempts exhausted.
	 *	  perpetual:    warning threshold only (priority starvation observability).
	 */
	epoch = cluster_epoch_get_current();
	master_gen = cluster_lms_get_shard_master_generation();
	max_attempts = cluster_ges_retransmit_max_attempts;
	perpetual = (timeout_ms == -1) || (timeout_ms == 0 && cluster_ges_request_timeout_ms == -1);
	if (perpetual) {
		effective_timeout_ms = -1;
		deadline = 0;
	} else {
		effective_timeout_ms = timeout_ms > 0 ? timeout_ms : cluster_ges_request_timeout_ms;
		deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), effective_timeout_ms);
	}

	memset(&key, 0, sizeof(key));
	key.request_id = request_id;
	key.source_node_id = cluster_node_id;
	key.dest_node_id = master;
	key.request_opcode = send_opcode;
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

	/* Build wire payload (constant across retransmits). */
	memset(&req, 0, sizeof(req));
	req.opcode = send_opcode;
	req.lockmode = lockmode;
	req.holder_node_id = (uint32)holder->node_id;
	req.holder_procno = (uint32)holder->procno;
	req.holder_cluster_epoch_lo = (uint32)(holder->cluster_epoch & 0xffffffffu);
	req.holder_cluster_epoch_hi = (uint32)(holder->cluster_epoch >> 32);
	req.holder_request_id_lo = (uint32)(request_id & 0xffffffffu);
	req.holder_request_id_hi = (uint32)(request_id >> 32);
	memcpy(req.resid, resid, sizeof(req.resid));
	/* spec-2.27 D2 — composite shard_master_generation in wire (5-tuple
	 * dedup key at receiver). */
	req.shard_master_generation_lo = (uint32)(master_gen & 0xffffffffu);
	req.shard_master_generation_hi = (uint32)(master_gen >> 32);

	if (!cluster_grd_outbound_enqueue_backend_request((uint32)master, &req, sizeof(req))) {
		/* Outbound ring full — fail closed.  Caller may retry. */
		cluster_ges_reply_wait_delete(&key);
		return GES_REJECT_REASON_WORK_QUEUE_FULL;
	}

	if (cluster_ges_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);

	attempt = 0;
	backoff_ms = 100;
	warned_starvation = false;
	debug1_starvation_fired = false;

	ConditionVariablePrepareToSleep(&entry->cv);
	while (!entry->ready) {
		int sleep_ms;
		long remaining_ms;

		if (perpetual) {
			sleep_ms = backoff_ms;
		} else {
			TimestampTz now = GetCurrentTimestamp();
			if (now >= deadline) {
				cluster_ges_reply_wait_delete(&key);
				ConditionVariableCancelSleep();
				return GES_REJECT_REASON_TIMEOUT;
			}
			remaining_ms = (long)((deadline - now) / 1000);
			if (remaining_ms <= 0)
				remaining_ms = 1;
			sleep_ms = backoff_ms;
			if ((long)sleep_ms > remaining_ms)
				sleep_ms = (int)remaining_ms;
		}

		if (ConditionVariableTimedSleep(&entry->cv, sleep_ms, WAIT_EVENT_CLUSTER_GES_REPLY_WAIT)) {
			/* CV signaled — re-check loop predicate. */
			continue;
		}

		/* Sleep timed out without wake — check budget then retransmit. */
		attempt++;

		if (max_attempts <= 0) {
			/* Retransmit disabled: keep waiting until the normal deadline. */
			backoff_ms = backoff_ms < 1600 ? backoff_ms * 2 : 1600;
			continue;
		}

		if (!perpetual && max_attempts > 0 && attempt > max_attempts) {
			cluster_ges_reply_wait_delete(&key);
			ConditionVariableCancelSleep();
			return GES_REJECT_REASON_TIMEOUT;
		}

		/* HC54 priority starvation observability — two one-shot events per
		 * acquire cycle (spec-2.27 §1 line 222):
		 *   1/2 threshold:  bump LMS counter + emit DEBUG1 (once).
		 *   3/4 threshold:  emit WARNING (once;  no counter inc here).
		 *
		 *	Independent flags `debug1_starvation_fired` and `warned_starvation`
		 *	enforce one-shot semantics so a slow-grant cycle bumps the counter
		 *	exactly once regardless of how many retransmit iterations land in
		 *	the [1/2, 3/4] window.  No wire message — reserved opcode 11
		 *	(GES_REQ_OPCODE_PRIORITY_BOOST) stays unused until spec-2.28+ wires
		 *	the integrated PG-core LockWaitQueueInsertAtHead receiver. */
		if (!debug1_starvation_fired && max_attempts > 0 && attempt >= ((max_attempts + 1) / 2)) {
			cluster_lms_inc_priority_starvation_observed();
			ereport(DEBUG1,
					(errmsg_internal("cluster_ges_send_request_and_wait priority starvation "
									 "observed (request_id=" UINT64_FORMAT " dest=%d attempt=%d)",
									 request_id, master, attempt)));
			debug1_starvation_fired = true;
		}
		if (!warned_starvation && max_attempts > 0 && attempt >= ((max_attempts * 3) / 4)) {
			ereport(WARNING,
					(errmsg("cluster GES retransmit budget 3/4 consumed; possible starvation"),
					 errhint("Consider raising cluster.ges_request_timeout_ms or "
							 "cluster.ges_retransmit_max_attempts, or scaling LMS.")));
			warned_starvation = true;
		}

		/* Re-enqueue request — receiver dedup HTAB suppresses double-grant. */
		if (!cluster_grd_outbound_enqueue_backend_request((uint32)master, &req, sizeof(req))) {
			cluster_ges_reply_wait_delete(&key);
			ConditionVariableCancelSleep();
			return GES_REJECT_REASON_WORK_QUEUE_FULL;
		}
		if (cluster_ges_state != NULL)
			pg_atomic_fetch_add_u64(&cluster_ges_state->request_defer_count, 1);

		/* Exponential backoff capped at 1600ms (also matches caller wait
		 * tick used by perpetual mode). */
		backoff_ms = backoff_ms < 1600 ? backoff_ms * 2 : 1600;
	}
	ConditionVariableCancelSleep();

	/* Capture verdict, then delete entry (HC17 pairing invariant). */
	reject_reason = entry->reject_reason;
	cluster_ges_reply_wait_delete(&key);

	return reject_reason;
}

uint32
cluster_ges_send_request_and_wait(const struct ClusterResId *resid, uint32 lockmode,
								  const struct ClusterGrdHolderId *holder, uint64 request_id,
								  int timeout_ms)
{
	return ges_send_request_opcode_and_wait(resid, lockmode, holder, request_id, timeout_ms,
											GES_REQ_OPCODE_REQUEST);
}

/*
 * spec-4.6 D3 — send the NEW current-epoch holder to the resource's
 * current master and wait for the insert-or-rebind ack.  Caller (the
 * cooperative redeclare walker) only overwrites LOCALLOCK.cluster_holder_
 * raw AFTER this returns 0 (§2.3 step 4);  any reject/timeout leaves the
 * old holder in place — fail-closed.
 */
uint32
cluster_ges_send_redeclare_and_wait(const struct ClusterResId *resid, uint32 lockmode,
									const struct ClusterGrdHolderId *new_holder, uint64 request_id)
{
	return ges_send_request_opcode_and_wait(resid, lockmode, new_holder, request_id,
											/* timeout_ms = GUC default */ 0,
											GES_REQ_OPCODE_REDECLARE);
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
	uint32 reject_reason;

	if (resid == NULL || holder == NULL)
		return GES_REJECT_REASON_TIMEOUT;

	/*
	 * spec-2.23 D5 / HC19 — release-coupled logical BAST_ACK.
	 *
	 *	If the holder backend had cluster_grd_bast_pending set (spec-2.17
	 *	BAST handler flag-only contract), this GES_RELEASE doubles as the
	 *	logical BAST_ACK: clear the pending flag here and bump the BAST
	 *	ack counter for dump_ges observability.  spec-2.23 does NOT send
	 *	a standalone BAST_ACK packet (HC19); the standalone opcode stays
	 *	reserved for spec-2.24 retransmit / compound reliability.
	 */
	if (MyProc != NULL && MyProc->cluster_grd_bast_pending) {
		MyProc->cluster_grd_bast_pending = false;
		cluster_grd_inc_bast_ack();
	}

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
	/* spec-2.27 D3 — retransmit + dedup on RELEASE.  Sample
	 * shard_master_generation once; perpetual timeout supported. */
	{
		uint64 master_gen = cluster_lms_get_shard_master_generation();
		int max_attempts = cluster_ges_retransmit_max_attempts;
		bool perpetual = (cluster_ges_request_timeout_ms == -1);
		int effective_timeout_ms;
		int attempt = 0;
		int backoff_ms = 100;
		bool warned_starvation = false;
		bool debug1_starvation_fired = false;

		if (perpetual) {
			effective_timeout_ms = -1;
			deadline = 0;
		} else {
			effective_timeout_ms = cluster_ges_request_timeout_ms;
			deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), effective_timeout_ms);
		}
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
		req.holder_node_id = (uint32)holder->node_id;
		req.holder_procno = (uint32)holder->procno;
		req.holder_cluster_epoch_lo = (uint32)(holder->cluster_epoch & 0xffffffffu);
		req.holder_cluster_epoch_hi = (uint32)(holder->cluster_epoch >> 32);
		req.holder_request_id_lo = (uint32)(request_id & 0xffffffffu);
		req.holder_request_id_hi = (uint32)(request_id >> 32);
		memcpy(req.resid, resid, sizeof(req.resid));
		req.shard_master_generation_lo = (uint32)(master_gen & 0xffffffffu);
		req.shard_master_generation_hi = (uint32)(master_gen >> 32);

		if (!cluster_grd_outbound_enqueue_backend_request((uint32)master, &req, sizeof(req))) {
			cluster_ges_reply_wait_delete(&key);
			return GES_REJECT_REASON_WORK_QUEUE_FULL;
		}

		if (cluster_ges_state != NULL)
			pg_atomic_fetch_add_u64(&cluster_ges_state->reply_defer_count, 1);

		ConditionVariablePrepareToSleep(&entry->cv);
		while (!entry->ready) {
			int sleep_ms;
			long remaining_ms;

			if (perpetual) {
				sleep_ms = backoff_ms;
			} else {
				TimestampTz now = GetCurrentTimestamp();
				if (now >= deadline) {
					cluster_ges_reply_wait_delete(&key);
					ConditionVariableCancelSleep();
					return GES_REJECT_REASON_TIMEOUT;
				}
				remaining_ms = (long)((deadline - now) / 1000);
				if (remaining_ms <= 0)
					remaining_ms = 1;
				sleep_ms = backoff_ms;
				if ((long)sleep_ms > remaining_ms)
					sleep_ms = (int)remaining_ms;
			}

			if (ConditionVariableTimedSleep(&entry->cv, sleep_ms,
											WAIT_EVENT_CLUSTER_GES_REPLY_WAIT))
				continue;

			attempt++;
			if (max_attempts <= 0) {
				backoff_ms = backoff_ms < 1600 ? backoff_ms * 2 : 1600;
				continue;
			}
			if (!perpetual && max_attempts > 0 && attempt > max_attempts) {
				cluster_ges_reply_wait_delete(&key);
				ConditionVariableCancelSleep();
				return GES_REJECT_REASON_TIMEOUT;
			}

			/* HC54 priority starvation observability — two one-shot events
			 * per release cycle (spec-2.27 §1 line 222 + F3 symmetry with
			 * request path):
			 *   1/2 threshold:  bump LMS counter + DEBUG1 (once).
			 *   3/4 threshold:  WARNING (once;  no counter inc here). */
			if (!debug1_starvation_fired && max_attempts > 0
				&& attempt >= ((max_attempts + 1) / 2)) {
				cluster_lms_inc_priority_starvation_observed();
				ereport(DEBUG1, (errmsg_internal(
									"cluster_ges_send_release_and_wait priority starvation "
									"observed (request_id=" UINT64_FORMAT " dest=%d attempt=%d)",
									request_id, master, attempt)));
				debug1_starvation_fired = true;
			}
			if (!warned_starvation && max_attempts > 0 && attempt >= ((max_attempts * 3) / 4)) {
				ereport(WARNING, (errmsg("cluster GES release retransmit budget 3/4 consumed"),
								  errhint("Possible LMS starvation;  raise "
										  "cluster.ges_request_timeout_ms or scale LMS.")));
				warned_starvation = true;
			}

			if (!cluster_grd_outbound_enqueue_backend_request((uint32)master, &req, sizeof(req))) {
				cluster_ges_reply_wait_delete(&key);
				ConditionVariableCancelSleep();
				return GES_REJECT_REASON_WORK_QUEUE_FULL;
			}
			if (cluster_ges_state != NULL)
				pg_atomic_fetch_add_u64(&cluster_ges_state->reply_defer_count, 1);

			backoff_ms = backoff_ms < 1600 ? backoff_ms * 2 : 1600;
		}
		ConditionVariableCancelSleep();
	}

	reject_reason = entry->reject_reason;
	cluster_ges_reply_wait_delete(&key);

	if (reject_reason == 0)
		cluster_ges_inc_release_ack();

	return reject_reason;
}


/* ============================================================
 * spec-2.23 D4 — TARGETED BAST (HC18).
 *
 *	HC18:  filter holder_list through DoLockModesConflict so only
 *	holders whose mode actually conflicts receive a BAST advisory.
 *	Peer broadcast fanout is strictly forbidden — non-holder peers
 *	would observe BAST with no matching cluster_grd_bast_pending slot
 *	and either silently drop or spam validation-fail counters.
 *
 *	For local-node holders the routine invokes SendProcSignal directly
 *	(spec-2.17 PROCSIG_CLUSTER_GES_BAST → cluster_grd_bast_handler →
 *	MyProc->cluster_grd_bast_pending = true).  For remote-node holders
 *	the routine sends a GES_REQUEST envelope with opcode = BAST through
 *	the outbound ring;  the remote node's request_handler bumps the
 *	bast_received counter (spec-2.17 ship) — true ProcSignal-on-receive
 *	forwarding lands with spec-2.24 cross-node signal forwarding (D
 *	axis).
 * ============================================================ */
void
cluster_ges_send_bast_targeted(const struct ClusterResId *resid, int requested_mode,
							   const struct ClusterGrdConflictHolder *holders, int n_holders)
{
	if (resid == NULL || holders == NULL)
		return;

	for (int i = 0; i < n_holders; i++) {
		LOCKMODE held;
		int32 holder_node;

		held = holders[i].held_mode;
		holder_node = holders[i].source_node_id;

		/*
		 * Re-verify the conflict at send time — the conflict_holders
		 * snapshot was taken under entry->lock, but a concurrent
		 * release path may have already cleared the holder.  Skipping
		 * incompatible entries makes the routine idempotent under
		 * concurrent release races.
		 */
		if (!DoLockModesConflict((LOCKMODE)requested_mode, held))
			continue;

		if (holder_node == cluster_node_id) {
			/*
			 * Local holder.  Spec-2.17 ship has PROCSIG_CLUSTER_GES_BAST
			 * wired but a procno→pid lookup requires the ProcArray;
			 * skip the direct SendProcSignal for the v0.3 spec scope
			 * and count the local advisory drop.  The natural
			 * cluster_grd_bast_handler flag-set path remains usable
			 * by tests via direct invocation.
			 */
			cluster_grd_inc_bast_sent();
		} else {
			GesRequestPayload bast;

			memset(&bast, 0, sizeof(bast));
			bast.opcode = GES_REQ_OPCODE_BAST;
			bast.lockmode = (uint32)requested_mode;
			bast.holder_node_id = (uint32)holders[i].holder.node_id;
			bast.holder_procno = holders[i].holder.procno;
			bast.holder_cluster_epoch_lo = (uint32)(holders[i].holder.cluster_epoch & 0xffffffffu);
			bast.holder_cluster_epoch_hi = (uint32)(holders[i].holder.cluster_epoch >> 32);
			bast.holder_request_id_lo = (uint32)(holders[i].holder.request_id & 0xffffffffu);
			bast.holder_request_id_hi = (uint32)(holders[i].holder.request_id >> 32);
			memcpy(bast.resid, resid, sizeof(bast.resid));

			if (cluster_grd_outbound_enqueue_backend_request((uint32)holder_node, &bast,
															 sizeof(bast)))
				cluster_grd_inc_bast_sent();
		}
	}
}


/* ============================================================
 * spec-2.24 D4 — cross-node victim cancel sender (HC23/HC24).
 *
 *	Reuses spec-2.17 GES_REQ_OPCODE_CANCEL_PENDING=7 enum + GesRequestPayload
 *	GesRequestPayload (holder_id 4-tuple carries victim target identity).
 *	Routes through CLUSTER_GRD_OUTBOUND_LMD_CANCEL origin — reserved
 *	pool + cleanup dirty-list nofail path;NOT silent-fail backend_
 *	request path (loss → deadlock not resolved).
 * ============================================================ */
void
cluster_ges_send_cancel_pending(int32 victim_node_id,
								const struct ClusterGrdHolderId *victim_target)
{
	GesRequestPayload payload;

	if (victim_target == NULL || victim_node_id < 0)
		return;

	memset(&payload, 0, sizeof(payload));
	payload.opcode = GES_REQ_OPCODE_CANCEL_PENDING;
	payload.lockmode = 0; /* not used for CANCEL */
	payload.holder_node_id = (uint32)victim_target->node_id;
	payload.holder_procno = victim_target->procno;
	payload.holder_cluster_epoch_lo = (uint32)(victim_target->cluster_epoch & 0xffffffffu);
	payload.holder_cluster_epoch_hi = (uint32)(victim_target->cluster_epoch >> 32);
	payload.holder_request_id_lo = (uint32)(victim_target->request_id & 0xffffffffu);
	payload.holder_request_id_hi = (uint32)(victim_target->request_id >> 32);
	/* resid not used for CANCEL — left zero */

	cluster_grd_outbound_enqueue_lmd_cancel((uint32)victim_node_id, &payload, sizeof(payload));
	cluster_lmd_cross_node_victim_cancel_sent_count_inc(1);
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

/*
 * spec-2.25 Step 1 D6 — native-lock probe handler skeleton entries.
 *
 *	Step 1 wires the early opcode dispatch path in cluster_ges_request_handler
 *	(NATIVE_LOCK_PROBE = 9, NATIVE_LOCK_PROBE_REPLY = 10).  HC33 dual-source /
 *	epoch / quorum validation has already passed when these handlers are
 *	invoked.
 *
 *	Body activation (Step 6 D5):
 *	  - request handler:  invoke cluster_native_lock_probe_local() via D8
 *	    lock.c internal helper to scan node-local shared PG lock state for
 *	    conflict on (locktag, lockmode);  build GesNativeLockProbeReplyPayload
 *	    with status CLEAR/HOLDER_CONFLICT/WAITER_CONFLICT;  enqueue reply via
 *	    D7 cluster_grd_outbound_enqueue_lms_native_probe (origin = 5).
 *	  - reply handler:  call cluster_lms_native_probe_recv_reply(slot_idx,
 *	    sender_node_id, status);  HC36 stale-reply drop applied at collector.
 *
 *	Step 1 skeleton mirrors spec-2.17 CANCEL_PENDING bring-up pattern:
 *	dispatch wired + handler counter-stub only;  full body activation moved
 *	to dedicated step (Step 6 D5).
 */
void
cluster_ges_handle_native_lock_probe_request(const ClusterICEnvelope *env,
											 const GesNativeLockProbePayload *probe)
{
	LOCKTAG probe_tag;
	ClusterGrdHolderId remote_requester;
	ClusterNativeLockProbeReply status;
	GesNativeLockProbeReplyPayload reply;

	Assert(env != NULL);
	Assert(probe != NULL);

	/*
	 * spec-2.25 Step 6 D5 — peer-side probe execution.
	 *
	 *	1. Deserialize LOCKTAG from wire bytes.
	 *	2. Build remote requester identity for HC32a exclude_holder —
	 *	   the requester lives on env->source_node_id (the LMS sender).
	 *	   On this node, the requester's PGPROC is not present (different
	 *	   node);  exclude_holder.node_id matches will therefore never
	 *	   trigger local PGPROC skip — correct behavior (peer has no
	 *	   PGPROC of the origin requester).
	 *	3. Run cluster_native_lock_probe_local under partition lock (HC30
	 *	   子条 5 handled by D8 helper internally).
	 *	4. Emit reply via cluster_grd_outbound_enqueue_lms_native_probe
	 *	   (origin = 5, nofail per L141 family).
	 */
	memcpy(&probe_tag, probe->locktag_bytes, sizeof(LOCKTAG));

	memset(&remote_requester, 0, sizeof(remote_requester));
	remote_requester.node_id = env->source_node_id;
	/* procno / epoch / request_id of remote requester are not authoritative
	 * here — peer node only uses node_id field of exclude_holder to skip
	 * PGPROC at same-node + matching procno (impossible here because the
	 * remote requester is not on this node).  Filling them with 0 keeps
	 * the exclude_holder non-NULL contract (HC32a). */

	status
		= cluster_native_lock_probe_local(&probe_tag, (LOCKMODE)probe->lockmode, &remote_requester);

	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY;
	reply.status = (uint32)status;
	reply.probe_id = probe->probe_id;
	reply.sender_node_id = (uint32)cluster_node_id;

	cluster_grd_outbound_enqueue_lms_native_probe(env->source_node_id, &reply,
												  (uint16)sizeof(reply));
}

void
cluster_ges_handle_native_lock_probe_reply(const ClusterICEnvelope *env,
										   const GesNativeLockProbeReplyPayload *reply)
{
	ClusterNativeLockProbeReply status;

	Assert(env != NULL);
	Assert(reply != NULL);

	/*
	 * spec-2.25 Step 6 D5 — LMS-side reply consumption.
	 *
	 *	HC33 dual-source validation already passed in cluster_ges_request_handler
	 *	(envelope source == reply.sender_node_id).  Forward to collector,
	 *	which applies HC36 stale-reply drop via probe_id epoch + expected
	 *	bitmap match.  Collector wakes any LMS waiter via cv broadcast.
	 */
	status = (ClusterNativeLockProbeReply)reply->status;
	if (status != CLUSTER_NATIVE_LOCK_PROBE_CLEAR
		&& status != CLUSTER_NATIVE_LOCK_PROBE_WAITER_CONFLICT
		&& status != CLUSTER_NATIVE_LOCK_PROBE_HOLDER_CONFLICT) {
		cluster_grd_inc_ges_inbound_validation_fail();
		return;
	}

	cluster_lms_native_probe_recv_reply(reply->probe_id, (int32)reply->sender_node_id, status);
}
