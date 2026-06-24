/*-------------------------------------------------------------------------
 *
 * cluster_hw.h
 *	  HW (high-water mark / relation extend) cluster block-number authority
 *	  -- spec-5.7a.
 *
 *	  PostgreSQL extends a relation by reading its current size from the
 *	  local kernel file (smgrnblocks -> FileSize) under a per-instance
 *	  LockRelationForExtension.  In a shared-storage cluster that is unsafe:
 *	  two nodes can each read the same stale-low size (non-coherent storage,
 *	  L368) and allocate the same block range -> silent corruption.  spec-5.7
 *	  HW makes the extend block number come from a cross-node CLUSTER
 *	  AUTHORITY instead of the file size.
 *
 *	  Authority model (spec-5.7 §3.1a, v1.1):
 *	    - HW(X) over the spec-5.3 GES substrate is pure cross-node mutual
 *	      exclusion + master location; it carries no value.
 *	    - The authoritative HWM for each (rel,fork) lives in shmem at the HW
 *	      enqueue master.  A holder asks the master for a block range via a
 *	      dedicated IC message (PGRAC_IC_MSG_HW_ALLOC); the master runs the
 *	      batch allocator below, durably reserves the advance in WAL
 *	      (XLogFlush before reply), and replies [first_block, first+granted).
 *	    - Never reads FileSize as the authority; remaster rebuilds the HWM
 *	      from the HW reservation WAL only.  Any ambiguous / unreachable /
 *	      WAL-missing state fails closed (ERRCODE_CLUSTER_RELATION_EXTEND_
 *	      UNAVAILABLE, 53RA6) -- it never guesses the next block.
 *
 *	  This header declares the PURE layer (backend-pure: no elog / shmem /
 *	  lock; standalone-linkable so the cluster_unit test links it directly):
 *	    - cluster_hw_resid_encode  (HW resource-id encoding)
 *	    - cluster_hw_classify_persistence (three-state extend gate, HW-M7)
 *	    - cluster_hw_alloc_segment (master-side batch allocator math)
 *	  The backend layer (master shmem HWM table, IC HW_ALLOC handler,
 *	  reservation WAL, and the HW(X) acquire/release wrappers) lands with the
 *	  D1 backend / D2 buffer-manager integration.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_hw.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D1, §2.1 / §3.1 / §3.1a)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HW_H
#define CLUSTER_HW_H

#include "cluster/cluster_cf_enqueue.h" /* CLUSTER_CF_RESID_TYPE (collision check) */
#include "cluster/cluster_grd.h"		/* ClusterResId */
#include "cluster/cluster_sequence.h"	/* CLUSTER_SQ_RESID_TYPE (collision check) */
#include "storage/block.h"				/* BlockNumber */
#include "storage/relfilelocator.h"		/* RelFileLocator */
#include "common/relpath.h"				/* ForkNumber */
#include "storage/lock.h"				/* LOCKTAG_LAST_TYPE, DEFAULT_LOCKMETHOD */

/*
 * CLUSTER_HW_RESID_TYPE -- HW resource-id namespace marker.  Must be above
 * every PG LockTagType and distinct from the SQ (0xF0) and CF (0xF1) resid
 * types.  The GES request/grant path only hashes the 16-byte resid for
 * shard/master routing and never decodes it back to a real LOCKTAG, so a
 * synthetic high type value is safe.
 */
#define CLUSTER_HW_RESID_TYPE 0xF2

StaticAssertDecl(CLUSTER_HW_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "HW resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_HW_RESID_TYPE != CLUSTER_SQ_RESID_TYPE,
				 "HW and SQ resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_HW_RESID_TYPE != CLUSTER_CF_RESID_TYPE,
				 "HW and CF resid namespaces must be distinct");

/*
 * ClusterHwClass -- three-state relation-extend classifier outcome
 * (spec-5.7 HW-M7).  Deliberately NOT a boolean: an implementer who maps an
 * unlogged relation to "false" would silently fall through to a PG-native
 * local extend, which is exactly the unsafe path HW-M7 forbids.
 *
 *	GLOBALIZE     route the extend through the cluster authority (HW(X) +
 *	              IC HW_ALLOC).  WAL-logged shared permanent relations.
 *	NATIVE_LOCAL  PG-native local extend is safe: temp relations, single-node
 *	              unlogged, and new-in-txn minimal-WAL permanent relations
 *	              (uncommitted -> invisible to peers -> single-node).
 *	FAIL_CLOSED   the extend can be neither safely globalized nor safely done
 *	              locally (unlogged in a multi-node cluster: no WAL authority
 *	              to rebuild the HWM).  The caller MUST ereport 53RA6 and never
 *	              fall back to a local extend.
 */
typedef enum ClusterHwClass {
	CLUSTER_HW_GLOBALIZE = 0,
	CLUSTER_HW_NATIVE_LOCAL = 1,
	CLUSTER_HW_FAIL_CLOSED = 2,
} ClusterHwClass;

/* ---- pure layer (standalone-linkable; never ereports) --------------- */

/*
 * cluster_hw_resid_encode -- build the HW resource id for a relation fork.
 *
 *	dst is filled deterministically from the RelFileLocator + fork:
 *	  field1 = dbOid, field2 = relNumber (the relfilenode -- ABA defence so a
 *	  DROP+CREATE of the same relation yields a distinct resource), field3 =
 *	  spcOid, field4 = fork.  One HW resource per (db,rel,spc,fork): the main,
 *	  fsm and vm forks of a relation extend independently.
 */
extern void cluster_hw_resid_encode(RelFileLocator rloc, ForkNumber fork, ClusterResId *dst);

/*
 * cluster_hw_classify_persistence -- the three-state extend gate (HW-M7,
 * spec-5.7 amend #3).
 *
 *	Pure helper over the two persistence facts the caller extracts from the
 *	Relation:
 *	  relpersistence  RELPERSISTENCE_PERMANENT / _UNLOGGED / _TEMP
 *	  multi_node      true if the cluster currently has more than one node
 *	                  (so cross-node coherence actually matters)
 *	All permanent shared relations -- including new-in-txn / minimal-WAL -- are
 *	GLOBALIZE (the authority owns them from block 0; no seed, so RelationNeedsWAL
 *	/ new-in-txn no longer affect classification).  Returns the ClusterHwClass;
 *	never ereports.
 */
extern ClusterHwClass cluster_hw_classify_persistence(char relpersistence, bool multi_node);

/*
 * cluster_hw_alloc_segment -- master-side batch allocator (authority math).
 *
 *	Given the current authoritative HWM (the next free block number) and the
 *	number of blocks wanted, grant a contiguous run [first .. first+granted):
 *	  return value   first_block of the granted run (InvalidBlockNumber if the
 *	                 relation is already at the block-number ceiling)
 *	  *granted       blocks actually granted (<= want; fewer when the run hits
 *	                 MaxBlockNumber, zero when exhausted)
 *	  *new_hwm       the advanced HWM (cur_hwm + *granted)
 *	The HWM is strictly monotone along block numbers, so two consecutive
 *	allocations never overlap -- this is the cross-node no-duplicate-block
 *	invariant.  Mirrors cluster_sq_alloc_segment (SQ value authority).
 */
extern BlockNumber cluster_hw_alloc_segment(BlockNumber cur_hwm, uint32 want, uint32 *granted,
											BlockNumber *new_hwm);

/*
 * cluster_hw_serve_allowed -- §3.1b R4/R6 serve gate (8.A, pure).  True iff a
 * resid's GRD shard is NORMAL and the HW authority has rebuilt it for its
 * current remaster generation (hw_rebuilt_generation == master_generation).
 * False -> the survivor just adopted the shard and has not yet rebuilt the HWM:
 * the caller fails closed (53RA6) and never auto-creates an entry at block 0.
 */
extern bool cluster_hw_serve_allowed(ClusterGrdShardPhase phase, uint32 master_generation,
									 uint32 hw_rebuilt_generation);

#ifndef FRONTEND

#include "access/xlogdefs.h"			  /* XLogRecPtr */
#include "cluster/cluster_lock_acquire.h" /* ClusterLockAcquireRequest (HwLock) */
#include "port/atomics.h"

/*
 * HwAllocReplyStatus -- the master's verdict for an HW_ALLOC request.  Any
 * non-OK verdict makes the requester fail closed (53RA6) -- never a silent
 * local extend.
 */
typedef enum HwAllocReplyStatus {
	HW_ALLOC_REPLY_OK = 0,			   /* first_block / granted are valid */
	HW_ALLOC_REPLY_FAIL_FULL = 1,	   /* authority HTAB full */
	HW_ALLOC_REPLY_FAIL_EXHAUSTED = 2, /* relation at the block-number ceiling */
	HW_ALLOC_REPLY_FAIL_INTERNAL = 3,  /* unexpected master error */
	HW_ALLOC_REPLY_FAIL_NOT_READY = 4, /* shard adopted but HWM not yet rebuilt (§3.1b R4) */
} HwAllocReplyStatus;

/*
 * HwAllocRequest -- the IC HW_ALLOC wire payload (PGRAC_IC_MSG_HW_ALLOC).
 *
 *	A backend holding HW(X) sends this to the resid's GES master to obtain a
 *	block range.  request_id (per-backend monotonic) correlates the reply;
 *	source_node / source_procno route + wake it.
 *
 *	seed_nblocks (v1.4 amend, §3.1c Q14/Q17): the requester's FORCED re-stat of
 *	the relation fork's real size (smgrnblocks, cache bypassed), carried on every
 *	request.  The master uses it ONLY at first establishment -- when the authority
 *	has no entry for the resid -- to seed the HWM at the relation's true committed
 *	size, instead of auto-creating at block 0.  This closes the gap where a
 *	relation's first blocks were created by a non-authority path (private build,
 *	sequence create) before the authority first saw it.  For an already-
 *	established resid the master ignores seed_nblocks (the running counter +
 *	HW_RESERVE WAL is the sole authority thereafter -- never FileSize again).  The
 *	requester holds HW(X) across the allocate, so establishment is serialized
 *	cross-node and no concurrent authority extend races the seed.
 */
typedef struct HwAllocRequest {
	uint64 request_id;	  /* offset 0 */
	uint32 source_node;	  /* offset 8 */
	uint32 source_procno; /* offset 12 */
	uint32 dbOid;		  /* offset 16 */
	uint32 relNumber;	  /* offset 20 */
	uint32 spcOid;		  /* offset 24 */
	uint32 fork;		  /* offset 28 */
	uint32 want;		  /* offset 32 */
	uint32 seed_nblocks;  /* offset 36; forced re-stat size, used only at establish */
} HwAllocRequest;

StaticAssertDecl(sizeof(HwAllocRequest) == 40, "HwAllocRequest IC wire ABI 40-byte lock");

/*
 * HwAllocReply -- the IC HW_ALLOC_REPLY wire payload (PGRAC_IC_MSG_HW_ALLOC_REPLY).
 *	Master -> originating backend, correlated by request_id.
 */
typedef struct HwAllocReply {
	uint64 request_id;	  /* offset 0; echo of the request */
	uint32 first_block;	  /* offset 8; granted range start (BlockNumber) */
	uint32 granted;		  /* offset 12; blocks granted (0 on any failure) */
	uint32 status;		  /* offset 16; HwAllocReplyStatus */
	uint32 source_procno; /* offset 20; echo: routes the reply to the waiter's slot */
} HwAllocReply;

StaticAssertDecl(sizeof(HwAllocReply) == 24, "HwAllocReply IC wire ABI 24-byte lock");

/*
 * cluster_hw_allocate -- requester-side: obtain a cluster-authoritative block
 *	range for extending (rloc, fork).  MUST be called while holding HW(X) for the
 *	(rel,fork) (caller's responsibility; §3.1a M1a + lock order
 *	DL->HW(X)->LockRelationForExtension).  Sends an HW_ALLOC to the resid's GES
 *	master, waits for the reply (bounded by cluster.ges_request_timeout_ms), and
 *	returns the first granted block with *granted set.  Returns
 *	InvalidBlockNumber on ANY failure (timeout / master fail-closed / exhausted);
 *	the caller MUST then fail closed with 53RA6 -- never a silent local extend.
 *
 *	seed_nblocks (§3.1c Q14/Q17): the caller's FORCED re-stat of the fork's real
 *	size (cache bypassed), used by the master only at first establishment.  The
 *	caller MUST hold HW(X) for (rloc, fork) before calling (so establishment is
 *	cross-node serialized and the seed reflects a quiescent, coherent size).
 */
extern BlockNumber cluster_hw_allocate(RelFileLocator rloc, ForkNumber fork, uint32 want,
									   BlockNumber seed_nblocks, uint32 *granted);

/*
 * ClusterHwStatus -- outcome of a master-side authority advance attempt
 * (spec-5.7 amend #3: the authority owns each (rel,fork) from block 0, so a
 * first-sight entry is auto-created at 0 -- there is no seed path -- but ONLY
 * once the §3.1b R4/R6 serve gate proves the resid's shard is rebuilt for its
 * current remaster generation).
 *
 *	OK         a block range was granted.
 *	EXHAUSTED  the relation is at the block-number ceiling; zero granted.
 *	FULL       the authority HTAB is full; the caller fails closed (53RA6),
 *	           never a silent local extend.
 *	NOT_READY  the resid's shard was just adopted (reconfig freeze / GES rebuild
 *	           in flight, or the per-(rel,fork) HWM not yet rebuilt from the dead
 *	           master's snapshot+tail).  The caller fails closed (53RA6); serving
 *	           -- or auto-creating at block 0 -- could re-hand an allocated range
 *	           (§3.1b R4/R6/R9, 8.A).
 */
typedef enum ClusterHwStatus {
	CLUSTER_HW_OK = 0,
	CLUSTER_HW_EXHAUSTED = 1,
	CLUSTER_HW_FULL = 2,
	CLUSTER_HW_NOT_READY = 3,
} ClusterHwStatus;

/*
 * Maximum number of distinct (rel,fork) authority entries a master holds, and
 * therefore the most entries any HW snapshot can carry.  Fixed for now (a GUC is
 * a forward refinement); overflow fails closed (53RA6) for the surplus relations
 * -- never an incorrect HWM.  Shared by the authority HTAB sizing, the snapshot
 * capture/read buffers, and the online-remaster rebuild.
 */
#define CLUSTER_HW_AUTHORITY_MAX 4096

/* Master HWM authority shmem region lifecycle (mirror cluster_sequence_shmem_*). */
extern Size cluster_hw_shmem_size(void);
extern void cluster_hw_shmem_init(void);
extern void cluster_hw_shmem_register(void);

/*
 * cluster_hw_try_advance -- master-side: grant `want` blocks for a (rel,fork).
 *	First applies the §3.1b R4/R6 serve gate (cluster_hw_serve_allowed over the
 *	resid's GRD shard phase + master/HW-rebuilt generations): a shard the master
 *	just adopted but has not rebuilt returns CLUSTER_HW_NOT_READY -- the caller
 *	fails closed (53RA6), never establishing over an allocated range (R9).
 *	Otherwise, at FIRST SIGHT (no entry) the authority entry is established at
 *	`seed_nblocks` -- the requester's forced re-stat of the relation's true
 *	committed size (§3.1c Q14/Q17) -- NOT at block 0, so a relation whose first
 *	blocks were created by a non-authority path (private build / sequence create)
 *	is owned from its real EOF.  An already-existing entry ignores seed_nblocks
 *	(the running counter is the sole authority thereafter).  Returns
 *	CLUSTER_HW_FULL if the authority HTAB is full, CLUSTER_HW_EXHAUSTED at the
 *	block ceiling, else CLUSTER_HW_OK with [*first, *first+*granted) and *new_hwm.
 *	The advance is applied to the volatile shmem HWM under the region lock; the
 *	caller MUST emit + XLogFlush an HW_RESERVE for *new_hwm BEFORE it replies the
 *	range, so the established seed + granted range is durable (§3.1a M1c / Q17).
 */
extern ClusterHwStatus cluster_hw_try_advance(const ClusterResId *resid, uint32 want,
											  BlockNumber seed_nblocks, BlockNumber *first,
											  uint32 *granted, BlockNumber *new_hwm);

/*
 * Per-GRD-shard "HW rebuilt for this remaster generation" tracking (§3.1b
 * R4/R9).  cluster_hw_shard_rebuilt_generation reads the generation the HW
 * authority last rebuilt the shard's HWMs for (0 when never rebuilt / out of
 * range); the serve gate compares it to cluster_grd_shard_master_generation.
 * cluster_hw_mark_shard_rebuilt records `generation` as rebuilt -- called by the
 * online-remaster rebuild AFTER the dead master's snapshot+tail is applied and
 * the adoption snapshot is durable (R9 order step ③), before the shard unfreezes.
 */
extern uint32 cluster_hw_shard_rebuilt_generation(uint32 shard_id);
extern void cluster_hw_mark_shard_rebuilt(uint32 shard_id, uint32 generation);

/*
 * Per-dead-origin online-remaster launch idempotency (S5d).  The GRD FSM records
 * the episode it last launched a rebuild worker for a dead origin under, and
 * skips relaunching while the value equals the current episode.
 */
extern uint64 cluster_hw_remaster_launched_episode(int node_id);
extern void cluster_hw_remaster_set_launched(int node_id, uint64 episode);

/*
 * cluster_hw_apply_hwm -- redo / remaster rebuild: create-if-absent and raise
 *	the authority HWM for a (rel,fork) to at least `hwm` (monotone).  Called from
 *	the cluster undo rmgr redo handler for HW_RESERVE (hwm = new_hwm), so a crash
 *	/ remaster rebuilds the authority from WAL alone (§3.1a M1e; amend #3: the
 *	only HWM source is HW_RESERVE -- new relations are owned from 0).
 */
extern void cluster_hw_apply_hwm(const ClusterResId *resid, BlockNumber hwm);

/*
 * HwLock -- a held HW(X) acquire, returned by cluster_hw_lock and passed to
 * cluster_hw_unlock.  Stack-allocated by the extend caller; HW is acquired and
 * released within one ExtendBufferedRelShared, so it is not reentrant.
 */
typedef struct HwLock {
	bool held;
	bool coordinated;			   /* false on OK_NATIVE: release is a no-op */
	ClusterLockAcquireRequest req; /* resid + holder + request_id for the release */
} HwLock;

/*
 * cluster_hw_lock / cluster_hw_unlock -- acquire/release HW(X) on a (rel,fork)
 * over the spec-5.3 GES substrate (mirrors cluster_cf_lock).  cluster_hw_lock
 * fills *lk and returns true if HW(X) is held (whether GES-coordinated or, when
 * the cluster/LMS layer is inactive, locally), false when it could not be proven
 * held so the caller fails closed (53RA6).  cluster_hw_unlock releases it
 * (draining/waking cross-node waiters via S6); a no-op if not held.
 */
extern bool cluster_hw_lock(const ClusterResId *resid, HwLock *lk);
extern void cluster_hw_unlock(HwLock *lk);

/*
 * Per-procno HW_ALLOC reply mailbox (requester side).  arm before sending,
 * wait until the reply handler delivers (request_id-disambiguated) or the
 * timeout expires.  One in-flight HW_ALLOC per backend (synchronous under HW(X)),
 * so a single slot per ProcNumber suffices -- no HTAB.
 */
extern void cluster_hw_reply_slot_arm(uint64 request_id);
extern bool cluster_hw_reply_slot_wait(uint64 request_id, long timeout_ms, HwAllocReply *out);
extern void cluster_hw_reply_slot_deliver(const HwAllocReply *reply);

/*
 * IC HW_ALLOC handlers + registration (cluster_hw_ic.c).  The request handler
 * runs on the master (advances the authority + flushes the reservation WAL +
 * replies); the reply handler runs on the requester (delivers to the reply
 * slot).  cluster_hw_register_ic_msg_types is called from the central LMON
 * IC registration (postmaster phase 1).
 */
struct ClusterICEnvelope;
extern void cluster_hw_alloc_request_handler(const struct ClusterICEnvelope *env,
											 const void *payload);
extern void cluster_hw_alloc_reply_handler(const struct ClusterICEnvelope *env,
										   const void *payload);
extern void cluster_hw_register_ic_msg_types(void);

/* Observability counters (dump_grd-style; surfaced by D12). */
extern uint64 cluster_hw_alloc_count(void);
extern uint64 cluster_hw_authority_create_count(void);
extern uint64 cluster_hw_reserve_wal_count(void);
extern uint64 cluster_hw_rebuild_count(void);
extern uint64 cluster_hw_failclosed_count(void);
extern uint64 cluster_hw_not_ready_count(void);
extern uint64 cluster_hw_remaster_done_count(void);
extern uint64 cluster_hw_remaster_blocked_count(void);
extern void cluster_hw_bump_alloc(void);
extern void cluster_hw_bump_authority_create(void);
extern void cluster_hw_bump_reserve_wal(void);
extern void cluster_hw_bump_rebuild(void);
extern void cluster_hw_bump_failclosed(void);
extern void cluster_hw_bump_not_ready(void);
extern void cluster_hw_bump_remaster_done(void);
extern void cluster_hw_bump_remaster_blocked(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_HW_H */
