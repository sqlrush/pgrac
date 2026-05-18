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


#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PCM_LOCK_H */
