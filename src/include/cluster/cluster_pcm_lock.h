/*-------------------------------------------------------------------------
 *
 * cluster_pcm_lock.h
 *	  pgrac cluster PCM (Parallel Cache Management) lock state machine.
 *
 *	  spec-1.7 shipped the API typedefs, opaque GrdEntry forward
 *	  declaration, GUC, inject points, and shmem registration surface.
 *	  spec-2.30 activates the local PCM state-machine body:
 *	  acquire / release / upgrade / downgrade now mutate a shmem HTAB
 *	  of opaque GrdEntry records protected by per-entry LWLockPadded.
 *	  Buffer manager callers, GCS wire requests, and convert-queue
 *	  protocol are intentionally deferred to later Cache Fusion specs.
 *
 *	  GrdEntry is intentionally an opaque struct (Q3 user 修订
 *	  2026-05-02): the full struct definition lives in
 *	  src/backend/cluster/cluster_pcm_lock.c (private).  Header
 *	  exposes only the typedef + helper functions
 *	  (cluster_pcm_grd_count / cluster_pcm_grd_shmem_size /
 *	  cluster_pcm_grd_init / cluster_pcm_grd_get_summary plus counter
 *	  accessors).  The opaque boundary lets later specs evolve fields
 *	  such as convert queues without exposing the internal ABI.
 *
 *	  GUC cluster.pcm_grd_max_entries default -1: auto-resolve to
 *	  NBuffers at startup.  Explicit 0 disables the PCM state machine
 *	  and preserves the old scaffolding behavior.
 *
 *	  pg_cluster_state.pcm exposes the activation state, live HTAB
 *	  summary rows, and per-transition counters for DBA diagnostics.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_pcm_lock.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.7-pcm-state-placeholder.md (frozen 2026-05-02 v1.1)
 *	  Design: docs/pcm-lock-protocol-design.md v1.0 §3-§5
 *	  AD-002 (PCM lock state machine N/S/X + PI orthogonal flag)
 *	  + AD-005 (Cache Fusion full; cf_state stub via BufferDesc)
 *	  + AD-006 (CR construction; PCM_TRANS_S_TO_X_CLEANOUT placeholder)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_LOCK_H
#define CLUSTER_PCM_LOCK_H

#include "c.h"
#include "cluster/cluster_buffer_desc.h" /* PcmState (1.6), INVALID_NODE_ID */
#include "cluster/cluster_conf.h"		 /* cluster_conf_has_peers */
#include "storage/buf_internals.h"		 /* BufferTag */

#ifdef USE_PGRAC_CLUSTER

/*
 * PcmLockMode -- API-level alias of PcmState (cluster_buffer_desc.h).
 *
 *	BufferDesc field stays named pcm_state (1.6 introduced); PCM lock
 *	API parameters use PcmLockMode for namespace clarity.  Same enum,
 *	same values via typedef alias (avoids value drift).
 *
 *	Constant aliases PCM_LOCK_MODE_N/S/X let API callers write
 *	  cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S)
 *	rather than
 *	  cluster_pcm_lock_acquire(tag, PCM_STATE_S)
 *	while sharing the underlying 0/1/2 values exactly.
 *
 *	Spec: spec-1.7 §1.4 example #4 (Q2 user 修订 2026-05-02 verified).
 */
typedef PcmState PcmLockMode;
#define PCM_LOCK_MODE_N PCM_STATE_N
#define PCM_LOCK_MODE_S PCM_STATE_S
#define PCM_LOCK_MODE_X PCM_STATE_X


/*
 * PcmLockTransition -- 9 legal state-machine transitions.
 *
 *	Defined per docs/pcm-lock-protocol-design.md §4.1 + AD-002.
 *	spec-2.30 uses these values as the executable local PCM
 *	state-machine input and increments one counter per transition.
 *
 *	Transition #9 (S→X cleanout) is reader-triggered ITL cleanout per
 *	AD-006 第四轮; Stage 3 (AD-006 第五轮 ~27000 LOC) wires actual
 *	cleanout calls.
 */
typedef enum PcmLockTransition {
	PCM_TRANS_N_TO_S = 1,			 /* read-first */
	PCM_TRANS_N_TO_X = 2,			 /* write-first */
	PCM_TRANS_S_TO_X_UPGRADE = 3,	 /* self upgrade */
	PCM_TRANS_X_TO_S_DOWNGRADE = 4,	 /* downgrade with PI */
	PCM_TRANS_X_TO_N_DOWNGRADE = 5,	 /* full downgrade with PI */
	PCM_TRANS_X_TO_N_RELEASE = 6,	 /* release without PI */
	PCM_TRANS_S_TO_N_INVALIDATE = 7, /* invalidated by remote X request */
	PCM_TRANS_S_TO_N_RELEASE = 8,	 /* local release */
	PCM_TRANS_S_TO_X_CLEANOUT = 9	 /* AD-006 ITL cleanout */
} PcmLockTransition;
#define PCM_TRANSITION_COUNT 9


/*
 * GrdEntry -- opaque per-block global lock state (master node).
 *
 *	Full struct definition lives in src/backend/cluster/cluster_pcm_lock.c
 *	(private).  Header exposes only the typedef and accessor/mutation APIs.
 *	The opaque boundary lets future Cache Fusion specs evolve fields such as
 *	convert queues without leaking layout into callers.
 */
typedef struct GrdEntry GrdEntry;


/*
 * GUC cluster.pcm_grd_max_entries -- maximum number of GrdEntry slots
 *	in the cluster_pcm_grd shmem region.
 *
 *	Default -1: auto-resolve to NBuffers at startup.  Explicit 0 disables
 *	the PCM state machine.  Positive values must be >= NBuffers.  Range
 *	[-1, 1048576].  PGC_POSTMASTER (startup-fixed).
 */
extern int cluster_pcm_grd_max_entries;

/*
 * PGRAC: spec-2.31 D2 v0.5 — gated PCM activation predicate.
 *
 *	Forward-declare cluster_enabled / cluster_node_id (defined in
 *	cluster_guc.c) without including cluster_guc.h, to keep that
 *	dependency one-way.  cluster_conf_has_peers() is included because
 *	no-peer single-node mode must not enter PCM acquire/release: there
 *	is no remote holder to coordinate with, and partial acquire/release
 *	paths can otherwise turn a local buffer lock into spurious PCM work.
 *
 *	cluster_pcm_is_active() is the single gate entry point used by
 *	bufmgr LockBuffer / LockBufferForCleanup hot path (HC67).
 *
 *	Gate layers:
 *	  1. compile-time: USE_PGRAC_CLUSTER (this header is empty outside it)
 *	  2. cluster_enabled (Stage 1.11 GUC; PGC_POSTMASTER)
 *	  3. cluster_node_id >= 0 (single-node fallback uses -1 sentinel until
 *	     pgrac.conf loads;  during initdb / non-cluster bootstrap the
 *	     PCM API would FATAL on the -1 range check, so we must skip the
 *	     hook entirely until a node id is assigned)
 *	  4. cluster_conf_has_peers() (single-node fallback pays no PCM tax)
 *	  5. cluster_pcm_grd_max_entries != 0 (spec-1.7 disable path)
 *	  6. NOT inside the cold merged-recovery window (spec-4.5a G6): the
 *	     engage gate proved every peer DEAD (all-cold premise), so no
 *	     remote holder exists to coordinate with, the GRD this startup
 *	     just rebuilt is empty, and a dead peer can never answer a
 *	     master request; the startup process also has no backend
 *	     identity for the GCS data plane (MyBackendId = -1 FATALs).
 *	     WARM crash recovery on a peer-configured node stays on the
 *	     PCM path (and currently fail-closes -- roadmap 4.7 instance
 *	     recovery / CF freeze; see t/243 L4 scope note).
 *
 *	Inline expansion yields 5 global reads + predictable branch (default
 *	cluster_enabled=true + max_entries=-1 → NBuffers + node_id set during
 *	postmaster startup before any LockBuffer is reachable, and the merged
 *	window flag is false outside startup redo, so branch predictor 99%+
 *	taken in the steady state).
 */
extern bool cluster_enabled;
extern int cluster_node_id;
extern bool cluster_recmerge_window_active; /* cluster_recovery_merge.c */

static inline bool
cluster_pcm_is_active(void)
{
	return cluster_enabled && cluster_node_id >= 0 && cluster_conf_has_peers()
		   && cluster_pcm_grd_max_entries != 0 && !cluster_recmerge_window_active;
}


/*
 * PGRAC: spec-2.31 D5 v0.4 — apply PCM ownership fields to BufferDesc.
 *
 *	Defined here (not in cluster_buffer_desc.h) because we depend on
 *	PcmLockMode which is owned by this header;  cluster_buffer_desc.h
 *	already provides BUF_TYPE_SCUR / BUF_TYPE_XCUR / PCM_STATE_* via the
 *	#include above, and this avoids the circular header dependency that
 *	would result from cluster_buffer_desc.h declaring the helper itself.
 *
 *	Called by bufmgr LockBuffer hook on the success path only (HC66):
 *	  - cluster_pcm_lock_acquire succeeded, AND
 *	  - LWLockAcquire(content_lock) succeeded (no ereport)
 *	→ then this helper updates the caller-supplied buffer_type pointer
 *	  (monotone hint of last PCM ownership mode) and pcm_state pointer
 *	  (real-time mirror of PCM master state).
 */
static inline void
cluster_buffer_desc_apply_pcm_ownership_fields(uint8 *out_buffer_type, uint8 *out_pcm_state,
											   PcmLockMode mode)
{
	*out_buffer_type = (mode == PCM_LOCK_MODE_S) ? (uint8)BUF_TYPE_SCUR : (uint8)BUF_TYPE_XCUR;
	*out_pcm_state = (mode == PCM_LOCK_MODE_S) ? (uint8)PCM_STATE_S : (uint8)PCM_STATE_X;
}


/*
 * PCM lock mutation API.
 *
 *	spec-2.30 activates these as C-internal APIs.  They do not have
 *	SQL-callable bindings; pg_proc.dat and system_views.sql are untouched
 *	for this API surface.
 *
 *	Inject points wrap each function entry (Q6 user 修订 2026-05-02
 *	release-pre instead of release-post for naming honesty -- 1.7
 *	stub never reaches a 'post' point because it ereports immediately):
 *	  cluster_pcm_lock_acquire   -> "cluster-pcm-acquire-entry"
 *	  cluster_pcm_lock_release   -> "cluster-pcm-release-pre"
 *	  cluster_pcm_lock_upgrade   -> "cluster-pcm-convert-pre"
 *	  cluster_pcm_lock_downgrade -> "cluster-pcm-downgrade-pre"
 */
extern void cluster_pcm_lock_acquire(BufferTag tag, PcmLockMode mode);

/*
 * PGRAC: spec-2.33 D7 — BufferDesc-aware variant.  Used by bufmgr LockBuffer
 * so that the GCS data-plane sender can install received block bytes into
 * the caller's shared buffer (HC84) without requiring callers to thread
 * BufferDesc through the tag-only path.  Falls back to the same local path
 * as cluster_pcm_lock_acquire when master == self.
 *
 * For S/X with a remote master, this entry point invokes
 * cluster_gcs_send_block_request_and_wait (spec-2.33 D3).  Tag-only
 * cluster_pcm_lock_acquire fails closed in that case with an errhint
 * directing callers here.
 *
 * Returns true if a DURABLE PCM grant was recorded (caller mirrors ownership
 * into buf->pcm_state).  Returns false for a spec-5.2 D2 one-shot READ_IMAGE:
 * bytes were installed for this read only and the caller MUST leave
 * buf->pcm_state == N so the next access re-fetches.
 */
extern bool cluster_pcm_lock_acquire_buffer(BufferDesc *buf, PcmLockMode mode);

/*
 * PGRAC: spec-2.35 D5 (HC111 + HC112) — bufmgr release hook bifurcation.
 *
 *	spec-2.31 D7 had a single cluster_pcm_lock_release_buffer() invoked
 *	from bufmgr LockBuffer's UNLOCK path.  That conflated two semantically
 *	distinct events:
 *	  (a) "content lock released" — the in-process content_lock SHARED or
 *	      EXCLUSIVE LWLock just dropped, but the buffer is still resident
 *	      in shared_buffers and may still serve other backends as a SCUR
 *	      cache holder (relevant for CF 2-way read sharing per spec-2.35).
 *	  (b) "cache residency lost" — the buffer is being evicted from
 *	      shared_buffers (InvalidateBuffer / InvalidateVictimBuffer /
 *	      DropRelations*Buffers / DropDatabaseBuffers), so the master's
 *	      s_holders_bitmap bit and master_holder lifecycle must clear.
 *
 *	HC111 redefines s_holders_bitmap as "cache residency" semantics.  HC112
 *	requires bufmgr to call (a) on content-lock unlock and (b) only on
 *	actual eviction.
 *
 *	cluster_pcm_lock_unlock_content_buffer(buf, mode):
 *	  - SCUR: no-op (cache residency preserved; bit stays set so the
 *	    master can still forward GCS_BLOCK_REQUEST to this node)
 *	  - XCUR: delegates to cluster_pcm_lock_release_buffer_for_eviction
 *	    (X is single-holder; releasing the X content lock also drops the
 *	    cache claim, matching spec-2.31 D7 prior semantics for X)
 *	  - N: no-op
 *
 *	cluster_pcm_lock_release_buffer_for_eviction(buf, mode):
 *	  - Performs the bit-clearing + master_holder lifecycle update.
 *	  - mode is taken from BufferDesc.pcm_state at eviction time so the
 *	    caller does not need to remember which mode was last held.
 */
extern void cluster_pcm_lock_unlock_content_buffer(BufferDesc *buf, PcmLockMode mode);
extern void cluster_pcm_lock_release_buffer_for_eviction(BufferDesc *buf, PcmLockMode mode);

/*
 * PGRAC: spec-2.35 D3 (HC110) — master_holder lookup for forward routing.
 *
 *	master-side GCS_BLOCK_REQUEST handler invokes this after
 *	cluster_pcm_lock_query(tag) returns S to decide whether the request
 *	can be forwarded to an authorized holder.  Returns -1 if no GrdEntry
 *	exists for the tag, or if master_holder is in the cleared sentinel
 *	state.  Otherwise returns the holder's cluster.node_id (0..31).
 */
extern int32 cluster_pcm_master_holder_node_by_tag(BufferTag tag);

extern void cluster_pcm_lock_release(BufferTag tag);
extern void cluster_pcm_lock_upgrade(BufferTag tag);
extern void cluster_pcm_lock_downgrade(BufferTag tag, PcmLockMode target_mode, bool keep_pi);


/*
 * Diagnostic / introspection helpers (always-callable).
 *
 *	cluster_pcm_lock_query: returns the live local PCM state for the tag,
 *	  or PCM_LOCK_MODE_N if no GRD entry exists.
 *
 *	cluster_pcm_grd_count: returns the live HTAB entry count.
 *
 *	cluster_pcm_grd_shmem_size: returns 0 if GUC=0, else shmem for the
 *	  header plus the resolved entry capacity.
 *
 *	cluster_pcm_grd_init: shmem registry init_fn callback.  Explicit
 *	  disable path (GUC=0) returns before ShmemInitStruct; otherwise it
 *	  initializes the header, HTAB, HTAB lock, and per-entry locks lazily.
 */
extern PcmLockMode cluster_pcm_lock_query(BufferTag tag);
extern int cluster_pcm_grd_count(void);
extern void cluster_pcm_grd_get_summary(int *n_count, int *s_count, int *x_count,
										int *pi_holders_total, int *convert_queue_active);
extern Size cluster_pcm_grd_shmem_size(void);
extern void cluster_pcm_grd_init(void);


/* ============================================================
 * PGRAC: spec-2.30 D2 — transition validator + apply (file-private struct).
 *
 *	`struct GrdEntry` definition lives in cluster_pcm_lock.c (file-private
 *	per spec-2.30 §2.1 + spec-1.7 Q3 opaque ABI 严守);  callers MUST go
 *	through these accessors, never deref `GrdEntry *` directly.
 *
 *	cluster_pcm_transition_legal(from, to, trans):  returns true iff
 *	  (from, to, trans) combination matches AD-002 9-transition map.
 *	  Caller invokes before apply; illegal combination MUST
 *	  ereport(ERROR, ERRCODE_DATA_CORRUPTED) at caller side (HC56).
 *
 *	cluster_pcm_transition_apply(entry, trans, holder_node_id):  caller
 *	  MUST hold entry->entry_lock EXCLUSIVE (HC57 Asserted);  applies
 *	  master_state / holder bitmap mutation + bumps counters.  Trans-9
 *	  fail-closed ereport(ERRCODE_FEATURE_NOT_SUPPORTED) (HC60).
 * ============================================================ */
extern bool cluster_pcm_transition_legal(PcmState from, PcmState to, PcmLockTransition trans);
extern void cluster_pcm_transition_apply(struct GrdEntry *entry, PcmLockTransition trans,
										 int holder_node_id);
extern bool cluster_pcm_lock_apply_gcs_transition(BufferTag tag, PcmLockTransition trans,
												  int holder_node_id);


/* ============================================================
 * PGRAC: spec-2.30 D2 — 9 transition counter accessors.
 *
 *	Counters live in shmem header (ClusterPcmShared);  every backend sees
 *	cluster-wide values.  Trans-9 (S→X cleanout) counter永 0 in spec-2.30
 *	(HC60 apply-fail-closed) until Stage 3 AD-006 第五轮 wires ITL cleanout.
 *	When cluster.pcm_grd_max_entries=0 (disable path) accessors return 0.
 * ============================================================ */
extern uint64 cluster_pcm_get_trans_n_to_s_count(void);
extern uint64 cluster_pcm_get_trans_n_to_x_count(void);
extern uint64 cluster_pcm_get_trans_s_to_x_upgrade_count(void);
extern uint64 cluster_pcm_get_trans_x_to_s_downgrade_count(void);
extern uint64 cluster_pcm_get_trans_x_to_n_downgrade_count(void);
extern uint64 cluster_pcm_get_trans_x_to_n_release_count(void);
extern uint64 cluster_pcm_get_trans_s_to_n_invalidate_count(void);
extern uint64 cluster_pcm_get_trans_s_to_n_release_count(void);
extern uint64 cluster_pcm_get_trans_s_to_x_cleanout_count(void);


/*
 * Module-level shmem registration entry point.
 *
 *	Called from cluster_init_shmem_module() (cluster_shmem.c) to
 *	register the cluster_pcm_grd region with the spec-1.3 shmem
 *	registry.  Idempotent (registry checks for duplicate names).
 *
 *	spec-2.30 registers the region with size_fn = cluster_pcm_grd_shmem_size
 *	and init_fn = cluster_pcm_grd_init.  The HTAB lock and per-entry locks
 *	are initialized inside the region.
 */
extern void cluster_pcm_lock_module_init(void);

/* ============================================================
 * PGRAC: spec-2.36 D5 HC117 + HC124 — S barrier helpers.
 *
 *   cluster_pcm_lock_set_pending_x — record that an X request is
 *     in flight at this master for `tag`;  N→S handlers short-
 *     circuit with DENIED_PENDING_X while set.
 *   cluster_pcm_lock_clear_pending_x — clear the field after X
 *     grant install ack OR reconfig epoch advance.
 *   cluster_pcm_lock_query_pending_x_requester — read for N→S
 *     decision (returns -1 = none).
 *   cluster_pcm_lock_clear_pending_x_for_node (HC124) — LMON
 *     node-dead sweep;  scans all GrdEntry and clears any
 *     pending_x_requester_node matching the dead node.  Must
 *     be idempotent under concurrent X grant clear races.
 * ============================================================ */
extern void cluster_pcm_lock_set_pending_x(BufferTag tag, int32 requester_node, uint64 current_lsn);
extern void cluster_pcm_lock_clear_pending_x(BufferTag tag);
extern int32 cluster_pcm_lock_query_pending_x_requester(BufferTag tag);
extern uint64 cluster_pcm_lock_clear_pending_x_for_node(int32 dead_node);

/* PGRAC: spec-2.36 D2/D3 — master broadcast invalidate needs raw bitmap
 * read.  Returns 0 if entry not present (treated as "no holders"). */
extern uint32 cluster_pcm_lock_query_s_holders_bitmap(BufferTag tag);

/* PGRAC: spec-4.7a D3 — strict master-side check "is `node` already a
 * recorded holder of `tag` whose grant covers `trans`?"  Enables idempotent
 * re-acknowledge of a holder's re-request instead of DENIED_MASTER_NOT_HOLDER
 * (which loops to 53R90).  S→X never qualifies (real writer path).  Missing
 * entry / uncertainty → false → caller fails closed. */
extern bool cluster_pcm_master_requester_is_holder(BufferTag tag, int32 node,
												   PcmLockTransition trans);

/* PGRAC: spec-4.7a D4 — does a node OTHER than `sender` hold `tag` (X or S)
 * and is still LIVE?  The GCS block master bounded-fail-closes an X request
 * before any state mutation when this is true (writer transfer deferred).
 * DEAD holders excluded (warm-recovery path).  See cluster_pcm_lock.c. */
extern bool cluster_pcm_master_other_live_holder_exists(BufferTag tag, int32 sender);

/* PGRAC: spec-4.7 D2/D3 — master-side rebuild of the minimal block-resource
 * view from one survivor re-declare.  Primitive args (not the wire payload)
 * to avoid a cluster_gcs_block.h ↔ cluster_pcm_lock.h include cycle; the
 * GCS_BLOCK_REDECLARE handler extracts the fields and calls this.  D2 records
 * the holder + max PI watermark;  D3 adds conflict detection (two X
 * declarers = protocol anomaly → fail-closed) + the not-double-X invariant.
 * source_node must equal the declared holder_node_id (the sender). */
extern bool cluster_gcs_block_master_rebuild_from_redeclare(BufferTag tag, uint8 held_mode,
															XLogRecPtr page_lsn, int32 source_node,
															uint64 cluster_epoch);

/*
 * cluster_pcm_mode_covers — spec-4.7a D2.
 *
 *	Does a node already holding PCM mode `have` cover a new request for `want`?
 *	X ⊇ {S, X}; S ⊇ {S}; N covers nothing.  The bufmgr acquire gate uses this
 *	to skip a remote master round-trip when the node still holds a sufficient
 *	mode (hold-until-revoked).  Safe ONLY because buf->pcm_state is cleared by
 *	the INVALIDATE handler and the eviction/drop paths, so a non-N value here
 *	means this node still genuinely holds the lock (Rule 8.A — no stale grant).
 */
static inline bool
cluster_pcm_mode_covers(PcmLockMode have, PcmLockMode want)
{
	if (have == PCM_LOCK_MODE_X)
		return true; /* X holder can read (S) and write (X) */
	if (have == PCM_LOCK_MODE_S)
		return want == PCM_LOCK_MODE_S; /* S holder can read only */
	return false;						/* N (or anything else) covers nothing */
}

/* ============================================================
 * PGRAC: spec-2.37 D2/D7/D8/D9 HC125-HC130 — PI watermark API.
 *
 *   pi_watermark_advance: D7 caller-side advance — caller (GCS/invalidate
 *     handler) has already obtained the downgrading holder's page_lsn via
 *     cluster_bufmgr_invalidate_block_for_gcs(..., &page_lsn) and now
 *     records max-historical watermark on the master.  Monotone — never
 *     regress.  Single field max model.
 *   pi_watermark_query:   master direct ship + forward path use this to
 *     populate GcsBlockForwardPayload.expected_pi_watermark_lsn_bytes[8]
 *     and master-direct DENIED_LOST_WRITE check.
 *   pi_watermark_retire_for_tag:        single-tag retire (test fixture).
 *   pi_watermark_retire_for_relation_fork:  D8 — relation drop / relfilenode
 *     change sweep (db, relNumber, fork) range.
 *   pi_watermark_retire_for_truncate_range: D8 — relation truncate sweep
 *     all entries whose tag.blockNum >= new_nblocks within (db, relNumber,
 *     fork).
 *   pi_watermark_retire_if_durable:     D9 HC130 part 2 — checkpointer/smgr
 *     sync-complete path only.  Helper立 for unit test + future use;
 *     production callsite defer to spec-2.38/Stage3 (PG has no per-block
 *     durable-complete hook today).
 *
 *   HC130: retire is FORBIDDEN by epoch advance (reconfig is the scenario
 *   most likely to involve stale sources;  clearing watermark there would
 *   weaken detection).  Only tag lifecycle + durable-confirm retire.
 * ============================================================ */
#include "storage/relfilelocator.h" /* RelFileNumber */
#include "common/relpath.h"			/* ForkNumber */
extern void cluster_pcm_lock_pi_watermark_advance(BufferTag tag, XLogRecPtr page_lsn);
extern XLogRecPtr cluster_pcm_lock_pi_watermark_query(BufferTag tag);
extern void cluster_pcm_lock_pi_watermark_retire_for_tag(BufferTag tag);
extern uint64 cluster_pcm_lock_pi_watermark_retire_for_relation_fork(Oid db_oid,
																	 RelFileNumber rel_number,
																	 ForkNumber fork_num);
extern uint64 cluster_pcm_lock_pi_watermark_retire_for_truncate_range(Oid db_oid,
																	  RelFileNumber rel_number,
																	  ForkNumber fork_num,
																	  BlockNumber new_nblocks);
extern bool cluster_pcm_lock_pi_watermark_retire_if_durable(BufferTag tag,
															XLogRecPtr written_page_lsn);


#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PCM_LOCK_H */
