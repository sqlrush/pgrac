/*-------------------------------------------------------------------------
 *
 * cluster_ko.h
 *	  KO (object-reuse flush) cross-node barrier -- spec-5.7 §3.5 / D6.
 *
 *	  When a relation's storage is physically removed or shrunk (DROP TABLE,
 *	  TRUNCATE, VACUUM trailing-page truncation) the relfilenode's file is
 *	  unlinked / truncated on shared storage.  A peer node that still holds
 *	  dirty buffers for that relfilenode would, on a later writeback, recreate
 *	  or corrupt the file -- or worse, scribble into a DIFFERENT relation that
 *	  has since reused the relfilenode (ABA).  KO is the cross-node barrier that
 *	  closes that window: before the physical op, every alive peer must drop the
 *	  relfilenode's buffers and acknowledge.
 *
 *	  KO differs from the other spec-5.7 enqueue classes (HW/DL/TT/IR), which
 *	  serialise via a GES lock.  KO's correctness gate is an APPLY-AFTER-DROP
 *	  flush barrier, not a lock: a peer ACKs only AFTER it has really dropped the
 *	  buffers (§3.6 KO-B1, KO-M6).  KO(X) (a GES lock on the relfilenode) is
 *	  taken alongside the barrier to serialise concurrent DROP/TRUNCATE of the
 *	  same relfilenode and to make holder-crash recovery (KO-M3) reconfig-driven,
 *	  but the buffer safety comes from the barrier.
 *
 *	  Timing (8.A, spec-5.7 KO Hardening): the barrier runs PRE-COMMIT, at the
 *	  DDL path (RelationDropStorage / RelationTruncate), while the dropping node
 *	  already holds the cross-node AccessExclusiveLock (spec-5.3 TM) on the
 *	  relation -- so no peer can re-dirty the relfilenode after the flush.  The
 *	  peer FLUSHES dirty buffers to shared storage and THEN invalidates them
 *	  (flush-then-invalidate, not pure discard): if the dropping transaction
 *	  rolls back the relation survives intact and peers re-read from storage with
 *	  no data loss, while ereport(ERROR 53RAA) is still able to abort the
 *	  statement on a barrier failure (the physical mdunlink for DROP is itself
 *	  post-commit and could not fail-closed there).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ko.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5/§3.6)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_KO_H
#define CLUSTER_KO_H

#include "cluster/cluster_grd.h"	/* ClusterResId */
#include "cluster/cluster_dl.h"		/* CLUSTER_DL_RESID_TYPE (collision check) */
#include "storage/relfilelocator.h" /* RelFileLocator */
#include "storage/lock.h"			/* LOCKTAG_LAST_TYPE */

/*
 * CLUSTER_KO_RESID_TYPE -- KO resource-id namespace marker.  Above every PG
 * LockTagType, distinct from SQ (0xF0) / CF (0xF1) / HW (0xF2) / DL (0xF3) /
 * IR (0xF5).
 */
#define CLUSTER_KO_RESID_TYPE 0xF6

StaticAssertDecl(CLUSTER_KO_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "KO resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_KO_RESID_TYPE != CLUSTER_DL_RESID_TYPE,
				 "KO and DL resid namespaces must be distinct");

/* ---- pure layer (standalone-linkable; never ereports) --------------- */

/*
 * cluster_ko_resid_encode -- build the KO resource id for a relfilenode.  KO is
 * per-RELFILENODE (field4 = 0, no fork): a DROP/TRUNCATE removes the whole
 * relfilenode (all forks), so the barrier and KO(X) are keyed on the relfilenode
 * triple.  field2 = relNumber is the relfilenode -- ABA defence, so a stale
 * flush for a dropped incarnation never aliases a relfilenode that has since
 * been reused by a new relation.
 */
extern void cluster_ko_resid_encode(RelFileLocator rloc, ClusterResId *dst);

#ifndef FRONTEND

#include "cluster/cluster_ic_envelope.h" /* ClusterICEnvelope */

/* ---- wire ABI (KO_FLUSH fanout + apply-after-drop ACK; §3.6) -------- */

/*
 * KoFlushHeader -- PGRAC_IC_MSG_KO_FLUSH payload (dropping node -> each alive
 * peer).  The peer must flush + drop ALL of the relfilenode's buffers and reply
 * KO_FLUSH_ACK only AFTER the drop completes (apply-after-drop, KO-M6).
 *
 *   batch_id     -- ack_wait correlation id (shared 2.39 allocator, KO-B2)
 *   epoch        -- HC100 stale-reply guard (reconfig fences old barriers)
 *   db/rel/spc   -- the relfilenode triple to drop
 *   source_node  -- the dropping node (where to send KO_FLUSH_ACK)
 */
typedef struct KoFlushHeader {
	uint64 batch_id;   /*  8B [ 0,  8) */
	uint64 epoch;	   /*  8B [ 8, 16) HC100 */
	uint32 db_oid;	   /*  4B [16, 20) relfilenode triple */
	uint32 rel_number; /*  4B [20, 24) */
	uint32 spc_oid;	   /*  4B [24, 28) */
	int32 source_node; /*  4B [28, 32) where to ACK */
} KoFlushHeader;

StaticAssertDecl(sizeof(KoFlushHeader) == 32, "spec-5.7 D6 KoFlushHeader wire ABI 32B fixed");

/*
 * KoFlushAckStatus -- only DONE counts as fulfilled (KO-M6).  A peer that could
 * not drop the buffers either sends FAILED or sends nothing; both leave the
 * enqueuer to time out and fail closed (53RAA).  There is deliberately NO
 * "reset-pending" equivalent: SIResetAll does not drop buffer-pool dirty pages,
 * so it would not satisfy the KO barrier.
 */
typedef enum KoFlushAckStatus {
	KO_FLUSH_ACK_DONE = 0,	 /* peer really dropped the buffers (apply-after-drop) */
	KO_FLUSH_ACK_FAILED = 1, /* peer could not drop -> NOT fulfilled (enqueuer 53RAA) */
} KoFlushAckStatus;

/*
 * KoFlushAckHeader -- PGRAC_IC_MSG_KO_FLUSH_ACK payload (peer -> dropping node).
 * Sent ONLY after the peer has really dropped the relfilenode's buffers.
 */
typedef struct KoFlushAckHeader {
	uint64 batch_id;  /*  8B [ 0,  8) echo of the KO_FLUSH batch_id */
	uint64 epoch;	  /*  8B [ 8, 16) HC100 */
	int32 acker_node; /*  4B [16, 20) the peer that dropped + ACK'd */
	uint16 status;	  /*  2B [20, 22) KoFlushAckStatus */
	uint16 flags;	  /*  2B [22, 24) reserved (must be 0) */
} KoFlushAckHeader;

StaticAssertDecl(sizeof(KoFlushAckHeader) == 24, "spec-5.7 D6 KoFlushAckHeader wire ABI 24B fixed");

/* HC139-style producer masks: both KO wire envelopes are sent by LMON draining
 * the GRD outbound ring (tier1 fds are LMON-owned).  KO_FLUSH is enqueued by the
 * dropping backend; KO_FLUSH_ACK is enqueued by the SI Broadcaster aux after the
 * drop -- but the SEND (cluster_ic_send_envelope) always runs in LMON. */
#define CLUSTER_IC_PRODUCER_KO_FLUSH ((uint32)((1u << B_BACKEND) | (1u << B_LMON)))
#define CLUSTER_IC_PRODUCER_KO_FLUSH_ACK ((uint32)(1u << B_LMON))

/* ---- enqueuer wrapper (the §3.5/§3.6 barrier) ---------------------- */

/*
 * cluster_ko_flush_and_wait_ack -- §3.5 KO-M1: before a relation's storage is
 * physically removed/truncated, make every alive peer flush + drop the
 * relfilenode's buffers and ACK.  Acquires KO(X) on the relfilenode (serialise
 * concurrent drops), fanouts PGRAC_IC_MSG_KO_FLUSH to each alive peer, and waits
 * (apply-after-drop) until every peer has ACK'd DONE.  Returns normally when the
 * barrier is satisfied (including the no-op cases: KO disabled, single node, temp
 * relation, or no alive peers).  ereport(ERROR 53RAA) if any alive peer does not
 * ACK in time / is unreachable / the KO(X) lease cannot be guaranteed -- never
 * proceeds to the physical op with a peer still holding dirty buffers (8.A).
 *
 * Pre-commit, under the relation's cross-node AccessExclusiveLock (spec-5.3 TM),
 * so no peer can re-dirty the relfilenode after the flush; the peer FLUSHES dirty
 * buffers to shared storage before invalidating them, so a rolled-back DROP loses
 * no data.
 */
extern void cluster_ko_flush_and_wait_ack(RelFileLocator rlocator, char relpersistence);

/* ---- shmem region (counters + the SPSC peer inbound ring) ---------- */
extern Size cluster_ko_shmem_size(void);
extern void cluster_ko_shmem_init(void);
extern void cluster_ko_shmem_register(void);

/* ---- IC msg type registration + inbound handlers ------------------- */
extern void cluster_ko_register_ic_msg_types(void);
extern void cluster_ko_flush_request_handler(const ClusterICEnvelope *env, const void *payload);
extern void cluster_ko_flush_ack_handler(const ClusterICEnvelope *env, const void *payload);

/*
 * cluster_ko_drain_inbound_and_apply -- SI Broadcaster aux drain hook (KO-B1
 * peer side).  Drains the KO inbound ring and, for each request, flushes + drops
 * the relfilenode's buffers locally and THEN enqueues a KO_FLUSH_ACK (DONE).
 * Runs in the aux process (off the LMON heartbeat path) because the drop is a
 * full buffer-pool scan.
 */
extern void cluster_ko_drain_inbound_and_apply(void);

/* ---- observability counters (dump_ko; surfaced by pg_cluster_state) - */
extern uint64 cluster_ko_flush_count(void);		   /* barriers initiated (enqueuer) */
extern uint64 cluster_ko_ack_received_count(void); /* peer DONE ACKs recorded (enqueuer) */
extern uint64 cluster_ko_failclosed_count(void);   /* 53RAA fail-closed (enqueuer) */
extern uint64 cluster_ko_native_count(void);	   /* no-op: single-node / no peer / private */
extern uint64 cluster_ko_lockfail_count(void); /* KO(X) acquire failed (best-effort; barrier ran) */
extern uint64 cluster_ko_peer_apply_count(void);   /* flush+drop applied + ACK'd (peer) */
extern uint64 cluster_ko_inbound_full_count(void); /* KO inbound ring full (peer -> no ACK) */

#endif /* !FRONTEND */

#endif /* CLUSTER_KO_H */
