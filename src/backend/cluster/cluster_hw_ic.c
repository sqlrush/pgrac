/*-------------------------------------------------------------------------
 *
 * cluster_hw_ic.c
 *	  HW (relation extend) authority interconnect: the HW_ALLOC request/reply
 *	  round trip + the HW(X) acquire/release wrappers (spec-5.7a D1).
 *
 *	  A backend extending a GLOBALIZE relation holds HW(X) (cluster_hw_lock) and
 *	  asks the resid's GES master for a block range (cluster_hw_allocate).  The
 *	  master is a pure per-(rel,fork) block-number counter (amend #3: the
 *	  authority owns the relation from block 0; no seed): it advances the HWM,
 *	  durably reserves the advance in WAL (HW_RESERVE, XLogFlush BEFORE the
 *	  reply -- §3.1a M1c), and replies the granted range.
 *
 *	  Topology:
 *	    - local master (or LMS layer inactive / single node): the requester
 *	      processes inline (cluster_ic_send_envelope short-circuits self-sends,
 *	      so a remote round trip to self would never be answered).
 *	    - remote master: the request is enqueued to the LMON outbound ring
 *	      (the tier-1 sockets are LMON-owned) and the requester waits on its
 *	      reply mailbox; the master's LMON inbound handler processes it and
 *	      replies directly.
 *
 *	  NOTE (perf, forward spec-5.19a): the master's XLogFlush runs in the LMON
 *	  inbound handler for remote requests, mirroring how GES grants are LMON-
 *	  processed today.  Offloading to the LMS daemon + batching the flush are
 *	  the HW perf path (this spec is correctness-only, report-only).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hw_ic.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D1, §2.2 / §3.1 / §3.1a)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* per-backend monotonic HW_ALLOC request id (0 is a reserved sentinel). */
static uint64 hw_request_seq = 0;


/*
 * cluster_hw_master_process -- master-side: advance the (rel,fork) authority,
 *	durably reserve the advance in WAL, and fill *reply.  Shared by the local
 *	master fast path (runs in the requesting backend) and the remote master
 *	inbound handler (runs in LMON).
 */
static void
cluster_hw_master_process(const HwAllocRequest *req, HwAllocReply *reply)
{
	ClusterResId resid;
	RelFileLocator rloc;
	ForkNumber fork = (ForkNumber)req->fork;
	BlockNumber first = InvalidBlockNumber;
	uint32 granted = 0;
	BlockNumber new_hwm = 0;
	ClusterHwStatus st;

	memset(reply, 0, sizeof(*reply));
	reply->request_id = req->request_id;
	reply->source_procno = req->source_procno;

	rloc.spcOid = req->spcOid;
	rloc.dbOid = req->dbOid;
	rloc.relNumber = req->relNumber;
	cluster_hw_resid_encode(rloc, fork, &resid);

	st = cluster_hw_try_advance(&resid, req->want, (BlockNumber)req->seed_nblocks, &first, &granted,
								&new_hwm);

	if (st == CLUSTER_HW_OK) {
		XLogRecPtr lsn = cluster_hw_emit_reserve(rloc, fork, new_hwm, granted);

		/*
		 * §3.1a M1c: the HWM advance must be durable BEFORE the range is
		 * replied, so no block is ever used without a durable reservation.
		 *
		 * §3.1b R13 (snapshot no-loss): the shmem advance (cluster_hw_try_advance
		 * above) happens BEFORE this HW_RESERVE WAL insert.  So a reservation
		 * whose WAL lsn < a checkpoint's snapshot_lsn already shows in that
		 * checkpoint's hw_htab capture (the single-writer copy reflects all
		 * earlier advances, R10), while one with lsn >= snapshot_lsn is in the
		 * replayed tail -- every reservation lands in snapshot OR tail, never
		 * lost (the L2k crash interleave proves it end to end).
		 */
		XLogFlush(lsn);
		cluster_hw_bump_reserve_wal();

		reply->status = HW_ALLOC_REPLY_OK;
		reply->first_block = (uint32)first;
		reply->granted = granted;
	} else if (st == CLUSTER_HW_NOT_READY) {
		/*
		 * §3.1b R4/R6 serve gate: the resid's shard was just adopted and its HWM
		 * is not yet rebuilt from the dead master's snapshot+tail.  Fail closed so
		 * the requester raises 53RA6 -- never an uncoordinated extend / block-0
		 * re-hand (R9).  Counted at the gate (cluster_hw_try_advance), so no extra
		 * failclosed bump here.
		 */
		reply->status = HW_ALLOC_REPLY_FAIL_NOT_READY;
	} else if (st == CLUSTER_HW_EXHAUSTED) {
		reply->status = HW_ALLOC_REPLY_FAIL_EXHAUSTED;
		cluster_hw_bump_failclosed();
	} else { /* CLUSTER_HW_FULL */
		reply->status = HW_ALLOC_REPLY_FAIL_FULL;
		cluster_hw_bump_failclosed();
	}
}


/*
 * cluster_hw_allocate -- requester-side entry (see cluster_hw.h).
 */
BlockNumber
cluster_hw_allocate(RelFileLocator rloc, ForkNumber fork, uint32 want, BlockNumber seed_nblocks,
					uint32 *granted)
{
	ClusterResId resid;
	int32 master;
	HwAllocRequest req;
	HwAllocReply reply;

	Assert(granted != NULL);
	Assert(want > 0);
	*granted = 0;

	cluster_hw_resid_encode(rloc, fork, &resid);

	memset(&req, 0, sizeof(req));
	req.request_id = ++hw_request_seq;
	if (req.request_id == 0)
		req.request_id = ++hw_request_seq; /* skip the 0 sentinel */
	req.source_node = (uint32)cluster_node_id;
	req.source_procno = (uint32)MyProc->pgprocno;
	req.spcOid = (uint32)rloc.spcOid;
	req.dbOid = (uint32)rloc.dbOid;
	req.relNumber = (uint32)rloc.relNumber;
	req.fork = (uint32)fork;
	req.want = want;
	req.seed_nblocks = (uint32)seed_nblocks; /* §3.1c: used by master only at establish */

	master = cluster_grd_lookup_master(&resid);

	if (master < 0 || master == cluster_node_id) {
		/* local master / LMS inactive / single node: process inline. */
		cluster_hw_master_process(&req, &reply);
	} else {
		/* remote master: enqueue to the LMON outbound ring (tier-1 sockets are
		 * LMON-owned) and wait on the reply mailbox. */
		cluster_hw_reply_slot_arm(req.request_id);
		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_HW_ALLOC, (uint32)master, &req,
													  (uint16)sizeof(req))) {
			/* ring full: drop the armed slot and fail closed. */
			(void)cluster_hw_reply_slot_wait(req.request_id, 0, &reply);
			cluster_hw_bump_failclosed();
			return InvalidBlockNumber;
		}
		if (!cluster_hw_reply_slot_wait(req.request_id, cluster_ges_request_timeout_ms, &reply)) {
			cluster_hw_bump_failclosed();
			return InvalidBlockNumber; /* timeout: caller fails closed (53RA6) */
		}
	}

	if (reply.status != HW_ALLOC_REPLY_OK || reply.granted == 0) {
		cluster_hw_bump_failclosed();
		return InvalidBlockNumber;
	}
	*granted = reply.granted;
	return (BlockNumber)reply.first_block;
}


/*
 * cluster_hw_alloc_request_handler -- master-side IC inbound handler (LMON).
 *	Processes the HW_ALLOC and replies directly to the originating node (LMON
 *	is the allowed producer for HW_ALLOC_REPLY).
 */
void
cluster_hw_alloc_request_handler(const struct ClusterICEnvelope *env, const void *payload)
{
	const HwAllocRequest *req = (const HwAllocRequest *)payload;
	HwAllocReply reply;

	/* The envelope (magic/version/crc/payload_len) is validated by the IC
	 * dispatch layer before the handler runs. */
	cluster_hw_master_process(req, &reply);

	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_HW_ALLOC_REPLY, (int32)env->source_node_id, &reply,
								   (uint32)sizeof(reply));
	/* A dropped reply (would-block / dead peer) leaves the requester to time
	 * out and fail closed -- never a silent grant. */
}

/*
 * cluster_hw_alloc_reply_handler -- requester-side IC inbound handler (LMON).
 *	Delivers the reply to the waiting backend's mailbox.
 */
void
cluster_hw_alloc_reply_handler(const struct ClusterICEnvelope *env, const void *payload)
{
	const HwAllocReply *reply = (const HwAllocReply *)payload;

	(void)env;
	cluster_hw_reply_slot_deliver(reply);
}

/*
 * cluster_hw_register_ic_msg_types -- register the HW_ALLOC + HW_ALLOC_REPLY
 *	message types.  Called from the central LMON IC registration (postmaster
 *	phase 1).
 */
void
cluster_hw_register_ic_msg_types(void)
{
	ClusterICMsgTypeInfo info;

	memset(&info, 0, sizeof(info));
	info.msg_type = PGRAC_IC_MSG_HW_ALLOC;
	info.name = "hw_alloc";
	/*
	 * BACKEND | LMON: a backend produces the request on the requester side
	 * (the cluster_grd_outbound_enqueue_backend_msg producer check), and the
	 * master's LMON dispatches it on the receive side (the router producer check
	 * runs against the dispatching process's MyBackendType = B_LMON).  Without
	 * LMON the remote-master request FATALs the master's LMON on arrival -- the
	 * same BACKEND | LMON pattern the GCS request messages use (cluster_gcs.c).
	 */
	info.allowed_producer_mask = CLUSTER_IC_PRODUCER_BACKEND | CLUSTER_IC_PRODUCER_LMON;
	info.broadcast_ok = false;
	info.handler = cluster_hw_alloc_request_handler;
	cluster_ic_register_msg_type(&info);

	memset(&info, 0, sizeof(info));
	info.msg_type = PGRAC_IC_MSG_HW_ALLOC_REPLY;
	info.name = "hw_alloc_reply";
	info.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON;
	info.broadcast_ok = false;
	info.handler = cluster_hw_alloc_reply_handler;
	cluster_ic_register_msg_type(&info);
}


/*
 * cluster_hw_lock / cluster_hw_unlock -- HW(X) acquire/release over the
 *	spec-5.3 GES substrate (mirrors cluster_cf_lock; the HW resid is not a PG
 *	LOCKTAG, so we drive cluster_lock_acquire_seven_step + S5 promote / S6
 *	release directly).  HW is always ExclusiveLock and never converts.
 */
bool
cluster_hw_lock(const ClusterResId *resid, HwLock *lk)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(resid != NULL && lk != NULL);
	memset(lk, 0, sizeof(*lk));

	memset(&req, 0, sizeof(req));
	req.resid = *resid;
	/* locktag left zeroed: not a PG LOCKTAG, so normal blocking semantics. */
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST; /* HW never converts */
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false;
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	req.wait_event = WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:

		/*
		 * cluster/LMS layer inactive (lms_enabled=off, or LMS not ready).  HW is
		 * only ever acquired for a GLOBALIZE extend in a multi-node cluster (the
		 * bufmgr gate requires cluster_conf_node_count() > 1), where an
		 * uncoordinated "native" hold would let each node advance its own block-0
		 * authority -> duplicate block ranges -> silent corruption.  Unlike CF
		 * (opt-in, default-off authority), HW is default-on, so OK_NATIVE here is
		 * a degraded multi-node state and MUST fail closed: the caller raises
		 * 53RA6 rather than extend without cross-node exclusion (review P1, 8.A).
		 */
		return false;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		/*
		 * S3 local-fast-path or modern S4 wire success: seven_step did NOT run S5
		 * (HW has no PG-native heavyweight lock to acquire in between).  Promote
		 * now to register the GRD holder for cross-node conflict.  S5 failure
		 * fails closed (caller raises 53RA6).
		 */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return false;
		lk->held = true;
		lk->coordinated = true;
		lk->req = req;
		return true;

	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/*
		 * Legacy / stub S4 path only: seven_step already ran S5 internally and
		 * returned an already-promoted holder.  Do NOT promote a second time
		 * (double-promote would re-revalidate an entry that is no longer a
		 * reservation).  The lock is held -- just record it.  The modern real
		 * wire returns NEED_PG_NATIVE_LOCK, so this arm is the legacy path; kept
		 * for forward-compat rather than failing closed (which would strand the
		 * holder seven_step already registered).
		 */
		lk->held = true;
		lk->coordinated = true;
		lk->req = req;
		return true;

	default:
		/* NOT_AVAIL / timeout / LMS-unavailable / deadlock / internal: the lock
		 * could not be proven held -- fail closed (caller raises 53RA6). */
		return false;
	}
}

void
cluster_hw_unlock(HwLock *lk)
{
	if (lk == NULL || !lk->held)
		return;

	if (lk->coordinated)
		(void)cluster_lock_acquire_s6_release(&lk->req);

	lk->held = false;
	lk->coordinated = false;
}
