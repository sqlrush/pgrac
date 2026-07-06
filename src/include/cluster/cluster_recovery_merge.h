/*-------------------------------------------------------------------------
 *
 * cluster_recovery_merge.h
 *	  pgrac k-way SCN merge replay engine (spec-4.5).
 *
 *	  Cold-crash merged recovery (engage only when no foreign node is
 *	  ALIVE) reads the WAL streams of the merge set (crash candidates
 *	  plus own) and replays their records in global SCN order.  This
 *	  header holds the PURE, unit-testable core:
 *
 *	    - cluster_recovery_merge_cmp(): the (xl_scn -> lsn -> node)
 *	      three-level ordering, delegated to scn_recovery_cmp so the
 *	      SCN-cmp gate stays satisfied.
 *	    - a small binary min-heap over per-stream cursors keyed by that
 *	      ordering (k <= 16 streams; insert/pop are O(log k)).
 *	    - cluster_recovery_record_class(): the §3.3e G/S/L/U apply
 *	      matrix as a pure rmid + has-block-ref + is-shared-block
 *	      decision (the caller supplies is_shared because the smgr
 *	      routing predicate is backend-only).
 *
 *	  The backend engine (cluster_recovery_merge.c) wraps real
 *	  XLogReaders around these and drives ApplyWalRecord; the §3.3b
 *	  freshness contract and §3.3c candidate self-recovery authority
 *	  live there.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_recovery_merge.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.5-kway-scn-merge-replay.md FROZEN v1.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_MERGE_H
#define CLUSTER_RECOVERY_MERGE_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_wal_state.h"
#include "port/pg_crc32c.h" /* spec-6.14 D9 amend: merge-claim CRC */

/*
 * §3.3e apply class.  Per record, decided from the rmid white-table +
 * whether it carries a block reference + (for block records) whether
 * the first block routes to shared storage.
 */
typedef enum ClusterRecoveryRecordClass {
	CLUSTER_RECMERGE_GLOBAL = 0,		/* G: XACT/CLOG/MULTIXACT/STANDBY/COMMIT_TS --
								  * per-node replicated logical state; applied on
								  * BOTH the foreign-merge and self-recovery
								  * paths (A needs the peer's commit bits) */
	CLUSTER_RECMERGE_SHARED,			/* S: block ref routed to shared storage --
								  * the merge body; A applies, B skips <=
								  * merge_recovered_lsn */
	CLUSTER_RECMERGE_LOCAL,				/* L: node-local (catalog pages, RELMAP, XLOG
								  * housekeeping) -- A skips a foreign stream's
								  * L record (it would clobber A's own files),
								  * B applies its own */
	CLUSTER_RECMERGE_UNCLASSIFIABLE,	/* U: cannot be safely routed (DBASE/
									  * TBLSPC, or a mixed S/L block set) --
									  * fail-closed 53RA3 on the foreign path */
	CLUSTER_RECMERGE_MATERIALIZE_LOCAL, /* spec-4.5a P0-3: RM_CLUSTER_UNDO --
										 * undo is path-based
										 * pg_undo/instance_<N>/seg.dat, outside
										 * the RelFileLocator namespace and NOT
										 * shared.  The redo handler resolves the
										 * path from the record's owner_instance
										 * (not cluster_node_id), so applying a
										 * PEER's record MATERIALIZES that peer's
										 * undo into the local pg_undo tree --
										 * which is exactly what the remote
										 * UBA/TT readers need.  Applied on BOTH
										 * the foreign and own paths. */
} ClusterRecoveryRecordClass;

/*
 * cluster_recovery_record_class -- the §3.3e matrix.
 *
 *	has_block_ref: the record registered at least one buffer.
 *	first_block_is_shared: for a block record, whether the FIRST block's
 *	    RelFileLocator routes to shared storage (caller's smgr predicate;
 *	    ignored when has_block_ref is false).
 *	all_blocks_same_routing: false when a single record mixes shared and
 *	    local blocks -> U (fail-closed; expected never to occur).
 */
static inline ClusterRecoveryRecordClass
cluster_recovery_record_class(RmgrId rmid, bool has_block_ref, bool first_block_is_shared,
							  bool all_blocks_same_routing)
{
	/*
	 * spec-4.5a P0-3: cluster undo materializes locally regardless of
	 * block refs (production undo records carry none -- the redo handler
	 * pwrites the per-instance segment file directly).
	 */
	if (rmid == RM_CLUSTER_UNDO_ID)
		return CLUSTER_RECMERGE_MATERIALIZE_LOCAL;

	/* G: global logical state carried by WAL, no block reference. */
	if (!has_block_ref) {
		switch (rmid) {
		case RM_XACT_ID:
		case RM_CLOG_ID:
		case RM_MULTIXACT_ID:
		case RM_STANDBY_ID:
		case RM_COMMIT_TS_ID:
			return CLUSTER_RECMERGE_GLOBAL;
		case RM_CLUSTER_XID_STRIPE_ID:
			/* spec-6.15 stripe JOIN/RETIRE notes: cluster-wide logical
			 * facts ("slot N joined at floor F / retired"), payload-only,
			 * idempotent by slot.  A merger replaying BOTH threads builds
			 * the complete stripe view in true SCN order -- exactly the G
			 * contract.  (First co-run of merged recovery x xid striping;
			 * pre-arm these fell through to U and 53RA3'd the merge.) */
			return CLUSTER_RECMERGE_GLOBAL;
		case RM_XLOG_ID:
		case RM_RELMAP_ID:
			/* XLOG housekeeping (checkpoint/fpw/...) + relmap are
			 * node-local on a foreign stream. */
			return CLUSTER_RECMERGE_LOCAL;
		case RM_SMGR_ID:
			/* smgr create/truncate name their relfile in the payload;
			 * the caller resolves its routing through the same smgr
			 * predicate and reports it via first_block_is_shared
			 * (spec-6.14 D9 amend -- pre-amend these fell through to U
			 * and 53RA3'd the merge on any foreign CREATE TABLE tail). */
			return first_block_is_shared ? CLUSTER_RECMERGE_SHARED : CLUSTER_RECMERGE_LOCAL;
		case RM_DBASE_ID:
		case RM_TBLSPC_ID:
			/* createdb / tablespace ops touch local dirs we cannot
			 * safely re-drive from a foreign stream. */
			return CLUSTER_RECMERGE_UNCLASSIFIABLE;
		default:
			/* An unlisted no-block-ref record is not safe to assume
			 * either way. */
			return CLUSTER_RECMERGE_UNCLASSIFIABLE;
		}
	}

	/* Block records: a mixed routing set is never expected -> U. */
	if (!all_blocks_same_routing)
		return CLUSTER_RECMERGE_UNCLASSIFIABLE;

	return first_block_is_shared ? CLUSTER_RECMERGE_SHARED : CLUSTER_RECMERGE_LOCAL;
}

/* ---- k-way merge min-heap (pure) ---- */

#define CLUSTER_RECMERGE_MAX_STREAMS 16

typedef struct ClusterRecmergeKey {
	uint64 scn;
	XLogRecPtr lsn;
	int32 node; /* tid - 1 */
} ClusterRecmergeKey;

typedef struct ClusterRecmergeHeapEntry {
	ClusterRecmergeKey key;
	int stream; /* index into the engine's per-stream cursor array */
} ClusterRecmergeHeapEntry;

typedef struct ClusterRecmergeHeap {
	ClusterRecmergeHeapEntry e[CLUSTER_RECMERGE_MAX_STREAMS];
	int n;
} ClusterRecmergeHeap;

typedef enum ClusterRecmergeStreamingDecision {
	CLUSTER_RECMERGE_STREAMING_NO_RECORD = 0,
	CLUSTER_RECMERGE_STREAMING_RECORD_READY = 1
} ClusterRecmergeStreamingDecision;

typedef struct ClusterRecmergeStreamingInput {
	bool record_available;
	bool heartbeat_seen;
	ClusterRecmergeKey key;
	ClusterRecmergeKey heartbeat_key;
} ClusterRecmergeStreamingInput;

/* Strict weak ordering: a < b ?  (scn -> lsn -> node, via the gated cmp). */
static inline int
cluster_recovery_merge_cmp(const ClusterRecmergeKey *a, const ClusterRecmergeKey *b)
{
	return scn_recovery_cmp((SCN)a->scn, a->lsn, (NodeId)a->node, (SCN)b->scn, b->lsn,
							(NodeId)b->node);
}

static inline void
cluster_recmerge_heap_init(ClusterRecmergeHeap *h)
{
	h->n = 0;
}

static inline void
cluster_recmerge_heap_push(ClusterRecmergeHeap *h, ClusterRecmergeKey key, int stream)
{
	int i;

	Assert(h->n < CLUSTER_RECMERGE_MAX_STREAMS);
	i = h->n++;
	h->e[i].key = key;
	h->e[i].stream = stream;
	while (i > 0) {
		int parent = (i - 1) / 2;

		if (cluster_recovery_merge_cmp(&h->e[i].key, &h->e[parent].key) >= 0)
			break;
		{
			ClusterRecmergeHeapEntry t = h->e[i];

			h->e[i] = h->e[parent];
			h->e[parent] = t;
		}
		i = parent;
	}
}

/* Pop the minimum (NULL when empty); returns the stream index + key. */
static inline bool
cluster_recmerge_heap_pop(ClusterRecmergeHeap *h, ClusterRecmergeHeapEntry *out)
{
	int i = 0;

	if (h->n == 0)
		return false;
	*out = h->e[0];
	h->e[0] = h->e[--h->n];
	while (true) {
		int l = 2 * i + 1;
		int r = 2 * i + 2;
		int sm = i;

		if (l < h->n && cluster_recovery_merge_cmp(&h->e[l].key, &h->e[sm].key) < 0)
			sm = l;
		if (r < h->n && cluster_recovery_merge_cmp(&h->e[r].key, &h->e[sm].key) < 0)
			sm = r;
		if (sm == i)
			break;
		{
			ClusterRecmergeHeapEntry t = h->e[i];

			h->e[i] = h->e[sm];
			h->e[sm] = t;
		}
		i = sm;
	}
	return true;
}

/*
 * Streaming mode selector for spec-6.4 ADG MRP.
 *
 * Heartbeats prove that a stream has no earlier concrete head through the
 * advertised heartbeat key.  A caller gets RECORD_READY only when every input
 * has either a concrete record head or a heartbeat at or beyond the selected
 * record, and at least one concrete record is available.
 */
static inline ClusterRecmergeStreamingDecision
cluster_recmerge_streaming_select(const ClusterRecmergeStreamingInput *inputs, int ninputs,
								  int *stream_out, ClusterRecmergeKey *key_out)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry out;
	int i;

	if (stream_out == NULL || key_out == NULL || inputs == NULL || ninputs <= 0
		|| ninputs > CLUSTER_RECMERGE_MAX_STREAMS)
		return CLUSTER_RECMERGE_STREAMING_NO_RECORD;

	cluster_recmerge_heap_init(&h);
	for (i = 0; i < ninputs; i++) {
		if (!inputs[i].record_available) {
			if (!inputs[i].heartbeat_seen)
				return CLUSTER_RECMERGE_STREAMING_NO_RECORD;
			continue;
		}
		cluster_recmerge_heap_push(&h, inputs[i].key, i);
	}

	if (!cluster_recmerge_heap_pop(&h, &out))
		return CLUSTER_RECMERGE_STREAMING_NO_RECORD;
	for (i = 0; i < ninputs; i++) {
		if (inputs[i].record_available)
			continue;
		if (cluster_recovery_merge_cmp(&inputs[i].heartbeat_key, &out.key) < 0)
			return CLUSTER_RECMERGE_STREAMING_NO_RECORD;
	}

	*stream_out = out.stream;
	*key_out = out.key;
	return CLUSTER_RECMERGE_STREAMING_RECORD_READY;
}

/* ============================================================
 * Sole-merger claim (spec-6.14 D9 amend, INV-D9-R).
 *
 *	spec-4.5a Q3 assumed cold merges are SERIALIZED (the stage-4
 *	harnesses restarted nodes one at a time).  Under the spec-5.6
 *	shared control-file authority the Phase-2 rendezvous forces
 *	concurrent boots, so two nodes can pass the all-cold engage gate
 *	simultaneously and run two merged replays over the SAME shared
 *	pages -- the second merger's raw read-modify-write can regress the
 *	first merger's post-recovery live writes (lost write).  A durable
 *	claim file on the shared root serializes the whole shared-regime
 *	crash-recovery critical section: EVERY node about to run local
 *	crash redo acquires it first (blocking), so a vanilla own-stream
 *	replay can never overlap a peer's merge of the same thread either
 *	(the registry-staleness SKIPPED verdict cannot see a mid-redo node
 *	-- redo refreshes no watermarks).  The winner-of-the-moment runs
 *	its recovery (k-way merge or plain own-stream redo per the
 *	unchanged engage decision) and releases only after its
 *	end-of-recovery checkpoint has made the recovered content durable
 *	in shared storage; a waiter then acquires and proceeds, its own
 *	stream protected by the §3.3c own-bound skip when a merger already
 *	covered it.  A claim held by a node that is provably dead is
 *	NEVER taken over -- there is no early-fencing oracle to prove the
 *	holder's in-flight writes ceased (same posture as the sole-survivor
 *	bootstrap fail-close in cluster_cf_storage.c) -- boot fails closed
 *	and the operator resolves it.
 *
 *	The pure build/classify core lives here (unit-testable, frontend-
 *	safe); the claim I/O + wait loop are backend-only below.
 * ============================================================
 */
#define CLUSTER_MERGE_CLAIM_MAGIC UINT64CONST(0x4d52474d434c4d31) /* "MRGMCLM1" */

typedef struct ClusterMergeClaimFile {
	uint64 magic;
	uint64 sysid;  /* cluster system identifier (foreign-root guard) */
	int32 node;	   /* 0-based claimant node id */
	int32 pad;	   /* zero; keeps the CRC field aligned + layout explicit */
	pg_crc32c crc; /* over the four fields above */
} ClusterMergeClaimFile;

typedef enum ClusterMergeClaimVerdict {
	CLUSTER_MERGE_CLAIM_VALID = 0,
	CLUSTER_MERGE_CLAIM_INVALID_SHORT,	  /* wrong byte count */
	CLUSTER_MERGE_CLAIM_INVALID_CRC,	  /* checksum mismatch */
	CLUSTER_MERGE_CLAIM_INVALID_MAGIC,	  /* not a claim file */
	CLUSTER_MERGE_CLAIM_INVALID_IDENTITY, /* another cluster's sysid */
	CLUSTER_MERGE_CLAIM_INVALID_NODE	  /* claimant id out of range */
} ClusterMergeClaimVerdict;

static inline void
cluster_merge_claim_build(ClusterMergeClaimFile *f, int32 node, uint64 sysid)
{
	f->magic = CLUSTER_MERGE_CLAIM_MAGIC;
	f->sysid = sysid;
	f->node = node;
	f->pad = 0;
	INIT_CRC32C(f->crc);
	COMP_CRC32C(f->crc, f, offsetof(ClusterMergeClaimFile, crc));
	FIN_CRC32C(f->crc);
}

/*
 * Classify a claim image read back from the shared root.  CRC first (a
 * torn/garbage file must not be interpreted at all), then magic, then
 * identity, then node range -- mirrors cluster_cf_classify_buffer's
 * ordering discipline.
 */
static inline ClusterMergeClaimVerdict
cluster_merge_claim_classify(const void *buf, size_t len, uint64 expected_sysid)
{
	ClusterMergeClaimFile f;
	pg_crc32c crc;

	if (len != sizeof(ClusterMergeClaimFile))
		return CLUSTER_MERGE_CLAIM_INVALID_SHORT;
	memcpy(&f, buf, sizeof(f));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &f, offsetof(ClusterMergeClaimFile, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, f.crc))
		return CLUSTER_MERGE_CLAIM_INVALID_CRC;

	if (f.magic != CLUSTER_MERGE_CLAIM_MAGIC)
		return CLUSTER_MERGE_CLAIM_INVALID_MAGIC;
	if (f.sysid != expected_sysid)
		return CLUSTER_MERGE_CLAIM_INVALID_IDENTITY;
	if (f.node < 0 || f.node >= CLUSTER_WAL_STATE_SLOT_COUNT)
		return CLUSTER_MERGE_CLAIM_INVALID_NODE;
	return CLUSTER_MERGE_CLAIM_VALID;
}

#ifndef FRONTEND

/* Backend engine (cluster_recovery_merge.c). */
typedef struct ClusterRecoveryMergeState ClusterRecoveryMergeState;

/* Engage decision (spec-4.5 §3.1).  Computed at the top of
 * PerformWalRecovery before the redo loop.  ENGAGE means: merged
 * recovery is on, this is a plain local crash with crash candidates
 * and no ALIVE foreign node, AND the 53RA3 gate passed.  Every NO_*
 * reason keeps today's single-stream recovery (never a failure). */
typedef enum ClusterMergeEngage {
	CLUSTER_MERGE_NO_DISABLED = 0,	 /* cluster.merged_recovery = off       */
	CLUSTER_MERGE_NO_NOT_CONFIGURED, /* no wal_threads_dir / legacy id     */
	CLUSTER_MERGE_NO_NO_PLAN,		 /* plan absent or failed               */
	CLUSTER_MERGE_NO_NO_CANDIDATES,	 /* nothing crashed to merge in         */
	CLUSTER_MERGE_NO_NOT_COLD,		 /* a foreign node is ALIVE (warm = 4.6) */
	CLUSTER_MERGE_ENGAGE,			 /* gate passed below; do merged replay */
} ClusterMergeEngage;

/* Decide whether to engage; out_bitmap/out_start receive the merge set
 * (candidates + own) and per-thread start LSNs when ENGAGE.  When the
 * decision is ENGAGE but the 53RA3 gate fails, this FATALs 53RA3 with
 * the blocking reasons -- it never silently downgrades to single
 * stream (spec-4.5 §3.2). */
extern ClusterMergeEngage cluster_recovery_merge_decide(uint16 own_thread, XLogRecPtr own_redo,
														uint64 out_bitmap[2],
														XLogRecPtr *out_start);

/* Sole-merger claim lifecycle (spec-6.14 D9 amend; see the pure core
 * above).  acquire_blocking runs in PerformWalRecovery BEFORE the engage
 * decision (crash recovery only -- never archive/standby/restore, whose
 * recovery is unbounded or off-regime); it no-ops unless the shared-WAL
 * regime is fully configured.  release runs in StartupXLOG AFTER the
 * end-of-recovery checkpoint (only then is the recovered content durable
 * in shared storage; releasing at merge end would let a late booter
 * re-merge from stale storage concurrently with this node's live
 * writes). */
extern void cluster_recovery_merge_claim_acquire_blocking(void);
extern void cluster_recovery_merge_claim_release_if_held(void);
extern bool cluster_recovery_merge_claim_is_held(void);

extern ClusterRecoveryMergeState *cluster_recovery_merge_begin(const uint64 merge_bitmap[2],
															   const XLogRecPtr *start_lsn,
															   uint16 own_thread, TimeLineID tli);
extern ClusterRecoveryMergeState *
cluster_recovery_merge_begin_restore(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
									 const XLogRecPtr *stop_lsn, const char *wal_root,
									 uint16 own_thread, TimeLineID tli);
extern struct XLogReaderState *cluster_recovery_merge_next(ClusterRecoveryMergeState *st,
														   uint16 *thread_out, char **errmsg_out);
extern ClusterRecoveryMergeState *
cluster_recovery_merge_streaming_begin(const uint64 merge_bitmap[2], const XLogRecPtr *start_lsn,
									   TimeLineID tli);
extern struct XLogReaderState *
cluster_recovery_merge_streaming_next(ClusterRecoveryMergeState *st, const XLogRecPtr *receive_lsn,
									  const XLogRecPtr *barrier_lsn, const SCN *barrier_scn,
									  uint16 *thread_out, char **errmsg_out);
extern void cluster_recovery_merge_end(ClusterRecoveryMergeState *st);

/* §3.3b/§3.3d window (startup-process scoped).  enter/leave bracket the
 * merged replay loop; set_scn publishes the current record's xl_scn so
 * the freshness skip + central pd_block_scn stamp can read it; the GCS
 * cold bypass reads window_is_active. */
extern void cluster_recovery_merge_window_enter(void);
extern void cluster_recovery_merge_window_leave(void);
extern void cluster_recovery_merge_set_scn(uint64 scn);
extern void cluster_recovery_merge_set_own_lsn(uint64 lsn);
extern void cluster_recovery_merge_set_apply_foreign(bool foreign);
extern bool cluster_recovery_merge_window_is_active(void);
extern uint64 cluster_recovery_merge_window_scn(void);

#endif /* !FRONTEND */

/*
 * spec-4.5a §3.3b: merge-window state exported as plain globals so the
 * PageSetLSN central hook (bufpage.h) and the XLogReadBufferForRedo
 * freshness gate (xlogutils.c) can read them without pulling cluster
 * headers into PG core paths.  Written ONLY by the startup process
 * (window enter/leave/set_scn below); every backend sees false/0.
 */
extern bool cluster_recmerge_window_active;
extern uint64 cluster_recmerge_window_scn;
extern uint64 cluster_recmerge_window_own_lsn;
extern bool cluster_recmerge_apply_foreign;

/*
 * spec-4.5a: has this node EVER merged-recovered `origin_node`'s stream?
 * Authority = the per-thread wal-state slot's merge_recovered_lsn (> 0
 * means a merged replay of that thread completed here) -- persistent and
 * backend-readable, so the remote UBA/TT read paths (D8/D9/D10) can gate
 * on it after recovery finishes.  Missing/unreadable slot -> false
 * (fail-closed).
 */
extern bool cluster_merged_instance_is_materialized(int origin_node);
/* spec-4.7 D5 (Q5) — actual persisted recovered_lsn (0 = not materialized /
 * fail-closed) for the redo-before-unfreeze LSN gate. */
extern uint64 cluster_merged_instance_recovered_through(int origin_node);
extern bool cluster_merged_any_remote_materialized(void);
extern void cluster_merged_authority_publish(int origin_node, uint64 recovered_lsn);
/* spec-4.11 3b-2 (R13): online variant -- a failure raises a CATCHABLE ERROR so
 * the thread-recovery orchestrator's harness demotes it to BLOCKED (no serving
 * authority published; survivor keeps running). */
extern void cluster_merged_authority_publish_online(int origin_node, uint64 recovered_lsn);

#endif /* CLUSTER_RECOVERY_MERGE_H */
