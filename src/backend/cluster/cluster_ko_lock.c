/*-------------------------------------------------------------------------
 *
 * cluster_ko_lock.c
 *	  KO (object-reuse flush) backend: shmem counters + the SPSC peer inbound
 *	  ring + KO(X) GES serialise lock + the apply-after-drop flush fanout/ACK
 *	  barrier + the SI-Broadcaster-aux peer drain (spec-5.7 §3.5/§3.6 / D6/D7).
 *	  The pure resid encoder lives in cluster_ko.c (standalone-linkable for the
 *	  unit test).
 *
 *	  Two sides of the barrier:
 *
 *	    Dropping node (cluster_ko_flush_and_wait_ack): hooked PRE-COMMIT at the
 *	    DDL path (RelationDropStorage / RelationTruncate) while the relation's
 *	    cross-node AccessExclusiveLock is held.  Acquires KO(X) on the
 *	    relfilenode, fanouts PGRAC_IC_MSG_KO_FLUSH to every alive peer (one msg
 *	    per peer via the GRD outbound ring; LMON sends), and waits on the reused
 *	    2.39 ack_wait correlation infra (KO-B2) until every peer has ACK'd DONE.
 *	    Any peer not ACKing in time -> ereport(ERROR 53RAA) (8.A fail-closed).
 *
 *	    Peer node: the IC inbound handler (LMON) nonblocking-enqueues the request
 *	    into a lock-free SPSC ring (HC133-style) and wakes the SI Broadcaster aux;
 *	    the aux (cluster_ko_drain_inbound_and_apply) does the heavy work off the
 *	    heartbeat path -- FlushRelationsAllBuffers (dirty -> shared storage, so a
 *	    rolled-back DROP loses no data) then DropRelationsAllBuffers (invalidate,
 *	    so no stale writeback) -- and ONLY THEN enqueues a KO_FLUSH_ACK (DONE).
 *	    This apply-after-drop ordering is the KO correctness gate (KO-M6): the
 *	    enqueuer never proceeds to the physical op until every peer's buffers are
 *	    gone.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ko_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6/D7, §3.5/§3.6)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h" /* RecoveryInProgress */
#include "catalog/pg_class.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sinval.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/backendid.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* PG core doesn't define USECS_PER_MSEC;  define locally (mirror cluster_sinval.c). */
#ifndef USECS_PER_MSEC
#define USECS_PER_MSEC INT64CONST(1000)
#endif

/* ============================================================
 * Shmem region: six observability counters + the SPSC peer inbound ring.
 *
 *   The inbound ring is single-producer (the LMON IC handler, which owns the
 *   tier1 fds, is the only KO_FLUSH dispatcher) / single-consumer (the SI
 *   Broadcaster aux is the only drainer), so it is lock-free: the producer
 *   publishes a slot then advances the tail with a write barrier, and the
 *   consumer reads the slot after loading the tail with a read barrier.
 * ============================================================ */

#define CLUSTER_KO_INBOUND_CAPACITY 64

typedef struct ClusterKoInboundSlot {
	uint32 db_oid;
	uint32 rel_number;
	uint32 spc_oid;
	int32 source_node;
	uint64 batch_id;
} ClusterKoInboundSlot;

typedef struct ClusterKoShared {
	pg_atomic_uint64 flush_count;		 /* barriers initiated (enqueuer) */
	pg_atomic_uint64 ack_received_count; /* peer DONE ACKs recorded (enqueuer) */
	pg_atomic_uint64 failclosed_count;	 /* 53RAA fail-closed (enqueuer) */
	pg_atomic_uint64 native_count;		 /* no-op: single-node / no peer / private */
	pg_atomic_uint64 lockfail_count;	 /* KO(X) acquire failed (best-effort; barrier ran) */
	pg_atomic_uint64 peer_apply_count;	 /* flush+drop applied + ACK'd (peer) */
	pg_atomic_uint64 inbound_full_count; /* KO inbound ring full (peer -> no ACK) */
	pg_atomic_uint32 inbound_head;		 /* consumer (SI Broadcaster aux) */
	pg_atomic_uint32 inbound_tail;		 /* producer (LMON IC handler) */
	ClusterKoInboundSlot inbound[CLUSTER_KO_INBOUND_CAPACITY];
} ClusterKoShared;

static ClusterKoShared *ko_state = NULL;

Size
cluster_ko_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterKoShared));
}

void
cluster_ko_shmem_init(void)
{
	bool found;

	ko_state = (ClusterKoShared *)ShmemInitStruct("pgrac cluster ko",
												  MAXALIGN(sizeof(ClusterKoShared)), &found);
	if (!IsUnderPostmaster) {
		pg_atomic_init_u64(&ko_state->flush_count, 0);
		pg_atomic_init_u64(&ko_state->ack_received_count, 0);
		pg_atomic_init_u64(&ko_state->failclosed_count, 0);
		pg_atomic_init_u64(&ko_state->native_count, 0);
		pg_atomic_init_u64(&ko_state->lockfail_count, 0);
		pg_atomic_init_u64(&ko_state->peer_apply_count, 0);
		pg_atomic_init_u64(&ko_state->inbound_full_count, 0);
		pg_atomic_init_u32(&ko_state->inbound_head, 0);
		pg_atomic_init_u32(&ko_state->inbound_tail, 0);
	}
}

static const ClusterShmemRegion cluster_ko_region = {
	.name = "pgrac cluster ko",
	.size_fn = cluster_ko_shmem_size,
	.init_fn = cluster_ko_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-5.7 KO object-reuse flush",
	.reserved_flags = 0,
};

void
cluster_ko_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ko_region);
}

#define KO_BUMP(field)                                                                             \
	do {                                                                                           \
		if (ko_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&ko_state->field, 1);                                          \
	} while (0)

uint64
cluster_ko_flush_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->flush_count) : 0;
}
uint64
cluster_ko_ack_received_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->ack_received_count) : 0;
}
uint64
cluster_ko_failclosed_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->failclosed_count) : 0;
}
uint64
cluster_ko_native_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->native_count) : 0;
}
uint64
cluster_ko_lockfail_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->lockfail_count) : 0;
}
uint64
cluster_ko_peer_apply_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->peer_apply_count) : 0;
}
uint64
cluster_ko_inbound_full_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->inbound_full_count) : 0;
}


/* ============================================================
 * KO(X) GES serialise lock over the spec-5.3 substrate (mirror dl_lock).
 *
 *   KO(X) serialises concurrent DROP/TRUNCATE of the SAME relfilenode and makes
 *   holder-crash recovery reconfig-driven (KO-M3).  The buffer-safety guarantee
 *   comes from the flush-ACK barrier, not from this lock; KO(X) is the coarse
 *   serialiser the spec lists alongside it (§3.5 KO-M1).
 * ============================================================ */

typedef struct KoLock {
	bool held;
	bool coordinated;
	ClusterLockAcquireRequest req;
} KoLock;

typedef enum KoAcquireOutcome {
	KO_ACQUIRE_GRANTED = 0,
	KO_ACQUIRE_NATIVE,
	KO_ACQUIRE_FAILED,
} KoAcquireOutcome;

static KoAcquireOutcome
ko_lock(const ClusterResId *resid, KoLock *lk)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(resid != NULL && lk != NULL);
	memset(lk, 0, sizeof(*lk));

	memset(&req, 0, sizeof(req));
	req.resid = *resid;
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST; /* KO never converts */
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false;
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	/* A blocked KO(X) waiter is "awaiting a GES grant" -- reuse the GES reply wait
	 * event (per spec-5.7 Q10, only HW/KO get dedicated events, and KO's dedicated
	 * ClusterObjectFlushWait covers the peer-ACK wait, not the KO(X) acquire). */
	req.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
		/* Cluster/LMS layer inactive for this resid (e.g. a new-in-txn private
		 * relfilenode no peer can see) -- no peer holds its buffers, so the flush
		 * barrier is vacuous.  Proceed without coordination (held=false). */
		return KO_ACQUIRE_NATIVE;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		/* Modern wire success: promote to register the GRD holder for cross-node
		 * conflict.  KO has no PG-native heavyweight lock to take in between. */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return KO_ACQUIRE_FAILED;
		lk->held = true;
		lk->coordinated = true;
		lk->req = req;
		return KO_ACQUIRE_GRANTED;

	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/* Legacy / stub S4 path: seven_step already ran S5 and returned a promoted
		 * holder.  Do NOT promote a second time; just record it. */
		lk->held = true;
		lk->coordinated = true;
		lk->req = req;
		return KO_ACQUIRE_GRANTED;

	default:
		/* NOT_AVAIL / timeout / LMS-unavailable / deadlock / internal. */
		return KO_ACQUIRE_FAILED;
	}
}

static void
ko_unlock(KoLock *lk)
{
	if (lk == NULL || !lk->held)
		return;
	if (lk->coordinated)
		(void)cluster_lock_acquire_s6_release(&lk->req);
	lk->held = false;
	lk->coordinated = false;
}


/* ============================================================
 * Enqueuer side: the apply-after-drop flush fanout + ACK barrier.
 * ============================================================ */

/*
 * ko_run_barrier -- fanout KO_FLUSH to every alive peer and wait (reusing the
 * 2.39 ack_wait correlation infra, KO-B2) until each has ACK'd DONE.  Returns
 * true iff every alive peer ACK'd in time; false on ack_wait-table-full,
 * outbound-queue-full, or timeout (all map to 53RAA in the caller).  Never
 * consults cluster.sinval_ack_mode (KO-M6: ack_mode=none must not turn the
 * barrier into fire-and-forget).
 */
static bool
ko_run_barrier(RelFileLocator rloc, uint32 alive_mask)
{
	uint64 batch_id;
	TimestampTz deadline_us;
	KoFlushHeader hdr;
	int peer;
	bool ok = false;

	batch_id = cluster_sinval_ack_wait_alloc_batch_id();
	deadline_us = GetCurrentTimestamp() + (int64)cluster_sinval_ack_timeout_ms * USECS_PER_MSEC;
	if (!cluster_sinval_ack_wait_begin(batch_id, alive_mask, deadline_us))
		return false; /* ack_wait table full */

	/* Reset the latch before publishing so we never miss an early ACK. */
	ResetLatch(MyLatch);

	memset(&hdr, 0, sizeof(hdr));
	hdr.batch_id = batch_id;
	hdr.epoch = cluster_epoch_get_current();
	hdr.db_oid = (uint32)rloc.dbOid;
	hdr.rel_number = (uint32)rloc.relNumber;
	hdr.spc_oid = (uint32)rloc.spcOid;
	hdr.source_node = cluster_node_id;

	KO_BUMP(flush_count);
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		if ((alive_mask & (1u << peer)) == 0)
			continue;
		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_KO_FLUSH, (uint32)peer, &hdr,
													  (uint16)sizeof(hdr))) {
			/* Could not enqueue -> this peer would never get the flush.  Fail closed
			 * rather than proceed with an un-flushed peer (8.A). */
			cluster_sinval_ack_wait_remove(batch_id);
			return false;
		}
	}
	cluster_lmon_wakeup(); /* wake LMON to drain the outbound ring */

	for (;;) {
		TimestampTz now_us;
		long timeout_ms;
		int rc;

		if (cluster_sinval_ack_wait_is_complete(batch_id)) {
			ok = true;
			break;
		}
		now_us = GetCurrentTimestamp();
		if (now_us >= deadline_us)
			break; /* timeout -> fail closed */
		timeout_ms = (deadline_us - now_us + USECS_PER_MSEC - 1) / USECS_PER_MSEC;
		if (timeout_ms <= 0)
			timeout_ms = 1;
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
					   WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
		if (rc & WL_POSTMASTER_DEATH)
			break;
	}

	cluster_sinval_ack_wait_remove(batch_id);
	return ok;
}

void
cluster_ko_flush_and_wait_ack(RelFileLocator rloc, char relpersistence)
{
	ClusterResId resid;
	KoLock lk;
	KoAcquireOutcome out;
	bool ok = false;

	/*
	 * Gates.  KO only matters for a cross-node-visible relation in a live,
	 * multi-node cluster.  A temp relation is private (no peer caches it); a
	 * single node / recovery / disabled flush has no remote buffers to drop.
	 */
	if (!cluster_object_reuse_flush_enabled)
		return;
	if (relpersistence == RELPERSISTENCE_TEMP)
		return;
	if (cluster_node_id < 0 || RecoveryInProgress())
		return;
	/*
	 * Engage from runtime liveness, not the static configured node count
	 * (spec-5.7 §3.1d).  wait_for_lms = false: the flush barrier rides on sinval
	 * (not LMS) and has its own alive-peer gate below, so KO never blocks a
	 * DROP/TRUNCATE on LMS warming up.  With no alive peer (NATIVE) there are no
	 * remote buffers to flush -> skip.  COORDINATE / FAIL_CLOSED both fall through
	 * to the barrier, which self-gates on the peer-ACK requirement.
	 */
	if (cluster_extend_liveness_engage(false) == CLUSTER_EXTEND_ENGAGE_NATIVE)
		return;

	/*
	 * KO(X) is BEST-EFFORT serialisation, not the correctness gate.  The
	 * flush-ACK barrier below is the 8.A gate; KO(X) only serialises concurrent
	 * DROP/TRUNCATE of the SAME relfilenode, which the relation's cross-node
	 * AccessExclusiveLock (spec-5.3 TM) already does.  So a KO(X) acquire that
	 * cannot be granted (e.g. the GES is not provisioned with enough entries)
	 * must NOT abort a DROP the barrier itself would permit -- we record it and
	 * proceed to the barrier, which self-gates via the peer-ACK requirement
	 * (a genuinely unhealthy cluster fails closed there instead).
	 */
	cluster_ko_resid_encode(rloc, &resid);
	out = ko_lock(&resid, &lk);
	if (out == KO_ACQUIRE_FAILED)
		KO_BUMP(lockfail_count);

	/* Run the barrier; release KO(X) (if held) on every exit path. */
	PG_TRY();
	{
		uint32 alive_mask = cluster_sinval_compute_alive_peer_mask();

		if (alive_mask == 0) {
			/* No alive peers -> nobody else can hold these buffers. */
			KO_BUMP(native_count);
			ok = true;
		} else
			ok = ko_run_barrier(rloc, alive_mask);
	}
	PG_FINALLY();
	{
		if (out == KO_ACQUIRE_GRANTED)
			ko_unlock(&lk);
	}
	PG_END_TRY();

	if (!ok) {
		KO_BUMP(failclosed_count);
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
				 errmsg("could not confirm every peer dropped buffers for relfilenode %u/%u/%u "
						"before reuse",
						(unsigned)rloc.spcOid, (unsigned)rloc.dbOid, (unsigned)rloc.relNumber),
				 errhint("A peer did not acknowledge the cross-node buffer flush within "
						 "cluster.sinval_ack_timeout_ms; check cluster health and retry.")));
	}
}


/* ============================================================
 * IC msg type registration + inbound handlers.
 * ============================================================ */

void
cluster_ko_register_ic_msg_types(void)
{
	ClusterICMsgTypeInfo info;

	memset(&info, 0, sizeof(info));
	info.msg_type = PGRAC_IC_MSG_KO_FLUSH;
	info.name = "ko_flush";
	info.allowed_producer_mask = CLUSTER_IC_PRODUCER_KO_FLUSH;
	info.broadcast_ok = false;
	info.handler = cluster_ko_flush_request_handler;
	cluster_ic_register_msg_type(&info);

	memset(&info, 0, sizeof(info));
	info.msg_type = PGRAC_IC_MSG_KO_FLUSH_ACK;
	info.name = "ko_flush_ack";
	info.allowed_producer_mask = CLUSTER_IC_PRODUCER_KO_FLUSH_ACK;
	info.broadcast_ok = false;
	info.handler = cluster_ko_flush_ack_handler;
	cluster_ic_register_msg_type(&info);
}

/*
 * cluster_ko_flush_request_handler -- peer-side IC inbound handler (runs in
 * LMON).  Validates the request, nonblocking-enqueues it into the SPSC inbound
 * ring, and wakes the SI Broadcaster aux to do the heavy flush+drop.  HC133-style
 * constraint: never blocks, never touches buffers here (that is the aux's job).
 * If the ring is full it drops the request (no ACK), so the enqueuer times out
 * and fails closed -- the request is never silently treated as fulfilled.
 */
void
cluster_ko_flush_request_handler(const ClusterICEnvelope *env, const void *payload)
{
	const KoFlushHeader *hdr = (const KoFlushHeader *)payload;
	uint32 head, tail, next;

	if (ko_state == NULL)
		return;
	if (env->payload_length != (uint32)sizeof(KoFlushHeader))
		return;
	if (hdr->epoch < cluster_epoch_get_current())
		return; /* stale (a reconfig has bumped the epoch) */
	if (hdr->source_node < 0 || hdr->source_node >= CLUSTER_MAX_NODES
		|| env->source_node_id != (uint32)hdr->source_node
		|| cluster_conf_lookup_node(hdr->source_node) == NULL)
		return;

	/* SPSC enqueue (LMON is the only producer). */
	tail = pg_atomic_read_u32(&ko_state->inbound_tail);
	head = pg_atomic_read_u32(&ko_state->inbound_head);
	next = (tail + 1) % CLUSTER_KO_INBOUND_CAPACITY;
	if (next == head) {
		/* Ring full -> drop (no ACK -> enqueuer fails closed 53RAA). */
		KO_BUMP(inbound_full_count);
		return;
	}
	ko_state->inbound[tail].db_oid = hdr->db_oid;
	ko_state->inbound[tail].rel_number = hdr->rel_number;
	ko_state->inbound[tail].spc_oid = hdr->spc_oid;
	ko_state->inbound[tail].source_node = hdr->source_node;
	ko_state->inbound[tail].batch_id = hdr->batch_id;
	pg_write_barrier(); /* publish the slot before advancing the tail */
	pg_atomic_write_u32(&ko_state->inbound_tail, next);

	cluster_sinval_set_proc_latch(); /* wake the SI Broadcaster aux to drain */
}

/*
 * cluster_ko_flush_ack_handler -- enqueuer-side IC inbound handler (runs in
 * LMON).  Records a peer's apply-after-drop ACK against the ack_wait entry.
 * KO-M6: ONLY status DONE counts as fulfilled -- a FAILED ACK (or no ACK at all)
 * leaves the enqueuer to time out and fail closed; there is no RESET_PENDING
 * shortcut (SIResetAll does not drop buffer-pool dirty pages).
 */
void
cluster_ko_flush_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const KoFlushAckHeader *hdr = (const KoFlushAckHeader *)payload;

	if (ko_state == NULL)
		return;
	if (env->payload_length != (uint32)sizeof(KoFlushAckHeader))
		return;
	if (hdr->flags != 0)
		return;
	if (hdr->acker_node < 0 || hdr->acker_node >= CLUSTER_MAX_NODES
		|| env->source_node_id != (uint32)hdr->acker_node
		|| cluster_conf_lookup_node(hdr->acker_node) == NULL)
		return;
	if (hdr->epoch < cluster_epoch_get_current())
		return; /* stale */
	if (hdr->status != (uint16)KO_FLUSH_ACK_DONE)
		return; /* KO-M6: only an apply-after-drop DONE fulfils the barrier */

	cluster_sinval_ack_wait_record(hdr->batch_id, hdr->acker_node);
	KO_BUMP(ack_received_count);
}


/* ============================================================
 * Peer side: SI Broadcaster aux drain -- flush + drop, THEN ACK.
 * ============================================================ */

void
cluster_ko_drain_inbound_and_apply(void)
{
	uint32 head, tail;

	if (ko_state == NULL)
		return;

	head = pg_atomic_read_u32(&ko_state->inbound_head);
	tail = pg_atomic_read_u32(&ko_state->inbound_tail);

	while (head != tail) {
		ClusterKoInboundSlot slot = ko_state->inbound[head]; /* copy out */
		RelFileLocator rloc;
		SMgrRelation smgr;
		KoFlushAckHeader ack;

		pg_read_barrier(); /* read the slot before advancing the head */
		/*
		 * Advance the head BEFORE applying: a failed flush (longjmp to the aux
		 * error handler) must not re-process the same request forever -- the
		 * dropped request leaves the enqueuer to time out (53RAA), which is the
		 * correct fail-closed behavior rather than an infinite retry loop.
		 */
		head = (head + 1) % CLUSTER_KO_INBOUND_CAPACITY;
		pg_atomic_write_u32(&ko_state->inbound_head, head);

		rloc.spcOid = (Oid)slot.spc_oid;
		rloc.dbOid = (Oid)slot.db_oid;
		rloc.relNumber = (RelFileNumber)slot.rel_number;
		smgr = smgropen(rloc, InvalidBackendId);

		/*
		 * Flush THEN invalidate (the abort-safe order): write any dirty buffers to
		 * shared storage so a rolled-back DROP loses no data, then drop ALL of the
		 * relfilenode's buffers so no later writeback can scribble into the file or
		 * a reused relfilenode.
		 */
		FlushRelationsAllBuffers(&smgr, 1);
		DropRelationsAllBuffers(&smgr, 1);
		KO_BUMP(peer_apply_count);

		/*
		 * Fault injection (t/297 L2): when armed, the peer has applied the drop
		 * but deliberately does NOT ACK, so the dropping node's barrier times out
		 * and fails closed (53RAA).  Proves the apply-after-drop fail-closed path
		 * deterministically without having to kill a node.
		 */
		CLUSTER_INJECTION_POINT("cluster-ko-peer-skip-ack");
		if (cluster_injection_should_skip("cluster-ko-peer-skip-ack")) {
			head = pg_atomic_read_u32(&ko_state->inbound_head);
			tail = pg_atomic_read_u32(&ko_state->inbound_tail);
			continue;
		}

		/* apply-after-drop: ACK only now that the buffers are really gone. */
		memset(&ack, 0, sizeof(ack));
		ack.batch_id = slot.batch_id;
		ack.epoch = cluster_epoch_get_current();
		ack.acker_node = cluster_node_id;
		ack.status = (uint16)KO_FLUSH_ACK_DONE;
		ack.flags = 0;
		(void)cluster_grd_outbound_enqueue_backend_msg(
			PGRAC_IC_MSG_KO_FLUSH_ACK, (uint32)slot.source_node, &ack, (uint16)sizeof(ack));
		cluster_lmon_wakeup(); /* wake LMON to send the ACK */

		head = pg_atomic_read_u32(&ko_state->inbound_head);
		tail = pg_atomic_read_u32(&ko_state->inbound_tail);
	}
}
