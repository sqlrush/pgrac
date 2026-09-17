/*-------------------------------------------------------------------------
 *
 * cluster_undo_alloc.h
 *	  pgrac undo segment allocator (runtime API).
 *
 *	  Stage 1.22 ships:
 *	    - cluster_undo_path_resolve(): build the segment file path
 *	      $PGDATA/pg_undo/instance_<N>/seg_<segment_id>.dat
 *	    - cluster_undo_segment_allocate(): create a segment file +
 *	      initialize block 0 + emit XLOG_UNDO_SEGMENT_INIT
 *
 *	  Stage 1.22 single-node restriction: owner_instance must equal 1
 *	  (cluster_node_id 0 + 1).  Cross-instance allocation rejected
 *	  with ERRCODE_FEATURE_NOT_SUPPORTED until Stage 2+ feature-117.
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §2.3 + §D5 + §D6.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_undo_alloc.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_ALLOC_H
#define CLUSTER_UNDO_ALLOC_H

#include "c.h"
#include "cluster/cluster_scn.h" /* SCN (cluster_undo_segment_try_mark_recyclable) */
#include "storage/block.h"


/*
 * CLUSTER_UNDO_OWNER_INVALID — owner_instance sentinel meaning "absent /
 * not yet allocated".  Renamed from CLUSTER_UNDO_DEFAULT_OWNER (which was
 * spec-1.22's single-node hard-coded owner = 1) when spec-3.4b D2 unlocked
 * the multi-instance allocator.  Real allocator callers MUST pass an
 * owner_instance in [1, UNDO_OWNER_INSTANCE_MAX].
 *
 *	Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 */
#define CLUSTER_UNDO_OWNER_INVALID ((uint8)0)


/*
 * CLUSTER_UNDO_SEGS_PER_INSTANCE — segment-id range reserved per owning
 * node, used by spec-3.4b cluster_uba_origin_node_id() to derive the
 * owner node from a UBA segment_id in O(1) without reading the on-disk
 * segment header.
 *
 *	Encoding:
 *	    segment_id = (owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE
 *	                + per_instance_slot + 1     (per_instance_slot in [0, N))
 *	    owner_instance(segment_id) = ((segment_id - 1)
 *	                                   / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1
 *
 *	With N = 256 and max owner_instance = UNDO_OWNER_INSTANCE_MAX = 128,
 *	max segment_id = 128 * 256 = 32768, which is comfortably inside
 *	UINT16_MAX -- the spec-3.4b F4 exact-key alias guard requires
 *	segment_id <= UINT16_MAX so the 16-bit fields in
 *	ClusterUndoTTSlotRef / ClusterTTStatusKey don't silently truncate.
 *
 *	Backward-compat with spec-1.22 single-node:
 *	    cluster_node_id = 0  →  owner_instance = 1  →  per-node range
 *	    [1, 256].  Today spec-1.22 only allocates segment_id = 1, which
 *	    maps back to owner_instance = 1 ✓.
 *
 *	Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 */
#define CLUSTER_UNDO_SEGS_PER_INSTANCE ((uint32)256)


/*
 * ClusterUndoPathIntent -- spec-5.22b D2-2 (P1-3 hard contract).
 *
 *	Declares, at every undo path-resolve call site, WHICH physical root a
 *	segment resolves to.  The intent is stated EXPLICITLY by the caller and
 *	is never inferred from owner==self, because D4 will introduce a
 *	"foreign + runtime-shared serve" case (a survivor reading a dead owner's
 *	durable block from shared storage) where owner!=self no longer implies
 *	"local materialized copy".
 *
 *	  CLUSTER_UNDO_PATH_RUNTIME_SHARED
 *	      The owner's own live runtime undo.  Migrates to the shared
 *	      cluster_fs root ONLY under peer-mode AND cluster.undo_gcs_coherence
 *	      (both off => local DataDir, inert).
 *
 *	  CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL
 *	      A dead-origin materialized copy that recovery rebuilt in the local
 *	      DataDir (cluster_tt_durable.c by-xid resolve of a foreign origin,
 *	      merged recovery, remote-xact outcome store).  ALWAYS resolves to
 *	      the local DataDir, in any mode -- D2 must never redirect these
 *	      reads, else dead-origin recovery / CR regress (spec-5.22b §3.6, R1).
 *
 *	  CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0  (spec-5.22d D4-3)
 *	      A survivor authority serving a DEAD owner's durable block0 (TTSlot
 *	      verdict) from shared storage.  Here owner != self, yet the read
 *	      DOES migrate to the shared cluster_fs root -- the ONLY intent that
 *	      resolves a FOREIGN owner's shared block0.  Armed by the same gate
 *	      as own RUNTIME_SHARED (peer-mode + cluster.undo_gcs_coherence).
 *	      Read-only and block0-only by call-site contract (§2.3/§3.6); it
 *	      never grants DATA-block access, and a plain RUNTIME_SHARED with
 *	      owner != self is still rejected by cluster_undo_path_resolve.
 */
typedef enum ClusterUndoPathIntent {
	CLUSTER_UNDO_PATH_RUNTIME_SHARED,	  /* own live undo; shared under peer-mode+coherence */
	CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, /* dead-origin materialized copy; always local */
	CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0 /* D4: authority reads a dead owner's
													   * shared block0 (read-only, foreign) */
} ClusterUndoPathIntent;

/* cluster_node_id owns the +1 sentinel offset: owner_instance == node_id + 1. */
extern int cluster_node_id; /* cluster_guc.c */

/*
 * cluster_undo_intent_for_owner -- per-call intent derivation (D2-2, B).
 *
 *	The single derivation every undo smgr / path call site consults: this
 *	node's own undo (owner_instance == cluster_node_id + 1) is
 *	RUNTIME_SHARED; a foreign owner -- in D2 always a dead-origin materialized
 *	copy that recovery rebuilt in the local DataDir -- is MATERIALIZED_LOCAL.
 *	Inline (mirrors cluster_mode.h's node-id gates) so the ~30 call sites take
 *	no link dependency on the GCS routing object.
 */
static inline ClusterUndoPathIntent
cluster_undo_intent_for_owner(uint8 owner_instance)
{
	return (owner_instance == (uint8)(cluster_node_id + 1)) ? CLUSTER_UNDO_PATH_RUNTIME_SHARED
															: CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL;
}


/*
 * cluster_undo_path_resolve
 *	  Build the segment file path:
 *	    $PGDATA/pg_undo/instance_<N>/seg_<segment_id>.dat
 *
 *	  Path components are bounded:
 *	    <N> in [0, 255] (uint8 max)
 *	    <segment_id> in [0, 4294967295] (uint32 max)
 *
 *	  spec-5.22b D2-2: takes an explicit ClusterUndoPathIntent.  A
 *	  RUNTIME_SHARED own-instance segment resolves under the shared cluster_fs
 *	  root when peer-mode + cluster.undo_gcs_coherence are on; MATERIALIZED_LOCAL
 *	  (and every non-coherent mode) resolves under the local DataDir.  Callers
 *	  pass cluster_undo_intent_for_owner(owner) unless they have a stronger
 *	  reason to name a literal intent.
 *
 *	  Returns 0 on success, -1 on buffer overflow (or an unset shared root on
 *	  the shared branch -- fail-closed).  Caller supplies a buf with capacity
 *	  >= MAXPGPATH.
 */
extern int cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 instance,
									 uint32 segment_id, char *buf, size_t buf_size);


/*
 * cluster_undo_segment_allocate
 *	  Allocate (create on disk + initialize block 0 + WAL-protect) an
 *	  undo segment file.
 *
 *	  Behavior:
 *	    1. Validate owner_instance + segment_id (spec-3.4b D2):
 *	         - owner_instance in [1, UNDO_OWNER_INSTANCE_MAX]
 *	         - owner_instance == cluster_node_id + 1 (node may only
 *	           allocate its own segments)
 *	         - segment_id != 0 (bootstrap-only sentinel)
 *	         - segment_id <= UINT16_MAX (F4 exact-key alias guard)
 *	         - segment_id within the node's reserved range, i.e.
 *	           ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1
 *	           must equal owner_instance.
 *	    2. Ensure the parent directory and acquire live-owner current X.
 *	    3. Load an existing exact header without writing, or reserve an
 *	       unpublished frame and private temporary file for an absent segment.
 *	    4. Initialize that frame and emit/flush XLOG_UNDO_SEGMENT_INIT.
 *	    5. Write/fsync the complete temp, publish without replacement, fsync
 *	       the parent, then make the resident image visible. Invalid existing
 *	       files fail closed; no online truncate/repair is performed.
 *
 *	  spec-3.4b D2 unlock: the previous Stage 1.22 single-node restriction
 *	  (owner_instance must equal 1) is removed.  The remaining restriction
 *	  is "owner_instance == cluster_node_id + 1" so a node never writes
 *	  another node's segment directory.
 *
 *	  Caller MUST NOT hold lifecycle_lock or be inside a critical section:
 *	  current acquisition can wait, WAL is emitted, and failures may ERROR.
 */
extern void cluster_undo_segment_allocate(uint32 segment_id, uint8 owner_instance);


/*
 * cluster_undo_active_segment_for_node_or_create
 *
 *	  Return the segment_id of this node's currently-active undo segment,
 *	  creating it on first call (idempotent thanks to the underlying
 *	  cluster_undo_segment_allocate).
 *
 *	  spec-3.4b MVP: each node uses a single active segment whose id is
 *	  fixed at `per_instance_slot = 0`, i.e.
 *
 *	      segment_id = node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1
 *
 *	  This gives node 0 → segment_id 1 (backward-compat with spec-1.22's
 *	  single-allocated segment), node 1 → segment_id 257, etc.  All
 *	  resulting segment_ids fall comfortably within the F4 alias guard
 *	  (max = 128 * 256 = 32768 < UINT16_MAX).
 *
 *	  Failure modes (all ereport(ERROR), no provisional fallback per F7):
 *	    - node_id outside [0, SCN_MAX_VALID_NODE_ID]
 *	    - derived owner_instance outside [1, UNDO_OWNER_INSTANCE_MAX]
 *	    - underlying cluster_undo_segment_allocate raises
 *
 *	  Caller MUST NOT be inside a critical section (transitively via
 *	  cluster_undo_segment_allocate).
 */
extern uint32 cluster_undo_active_segment_for_node_or_create(int node_id);

typedef struct ClusterUndoSegmentExtendPlan {
	uint32 segment_id;
	uint32 generation;
	bool needs_reuse;
	bool at_hard_cap;
	bool needs_provision;
	uint8 reserved[5];
} ClusterUndoSegmentExtendPlan;

StaticAssertDecl(sizeof(ClusterUndoSegmentExtendPlan) == 16,
				 "undo segment extend plan must remain stack-only and fixed-size");

/*
 * Choose one fresh/available segment or
 * freeze an exact RECYCLABLE candidate. The selector never creates a file or
 * performs the generation-changing reuse transition. Callers retain cursor
 * ownership but release lifecycle_lock before first provision or reuse
 * through current authority, then revalidate the selected image.
 */
extern bool cluster_undo_segment_extend_or_create(uint8 owner_instance,
												  ClusterUndoSegmentExtendPlan *plan);


/*
 * spec-3.8 Fix 4: restart scan helper.
 *
 *	  Scans owner_instance's segment pool to find the highest segment_id
 *	  whose file exists on disk.  Returns 0 if no segments exist
 *	  (fresh init).  Used by cluster_undo_record_shmem_init() to resume
 *	  active_segment_id to the most-recent segment rather than going
 *	  back to segment_id = 1 on restart.
 */
extern uint32 cluster_undo_segment_scan_max_existing(uint8 owner_instance);

/*
 * spec-3.22: lock-free probe for whether the (owner_instance, segment_id) undo
 * segment file exists on disk.  Used by the by-xid durable resolve to tell a
 * genuinely-absent segment (a sound scan skip) from an existing-but-unreadable
 * one (an incomplete scan -> SCAN_UNAVAILABLE, never a recycled 0-match).  A pure
 * access(F_OK); takes no lock (existence is monotone -- segments are never
 * deleted once allocated, only recycled in place).
 */
extern bool cluster_undo_segment_file_exists(uint8 owner_instance, uint32 segment_id);

/*
 * spec-3.18 D3.2 (review finding 2):  restart resume must pick the live active
 * segment (SEGMENT_ACTIVE, FULL flag clear), not the highest-numbered file --
 * reuse-first can make the active a low-numbered reborn slot.  Returns 0 when
 * none is resumable or the pool is ambiguous (>1 writable-active -> fail-closed).
 */
extern uint32 cluster_undo_segment_scan_resumable_active(uint8 owner_instance);

/*
 * spec-3.13 D3: COMMITTED -> RECYCLABLE advancement outcome.
 */
typedef enum ClusterUndoSegTryRecycle {
	CLUSTER_SEG_RECYCLE_ADVANCED = 0,	   /* transitioned + durable */
	CLUSTER_SEG_RECYCLE_ALREADY = 1,	   /* idempotent: already RECYCLABLE */
	CLUSTER_SEG_RECYCLE_RETAINED = 2,	   /* predicate says a reader may need it */
	CLUSTER_SEG_RECYCLE_NOT_COMMITTED = 3, /* ALLOCATED / ACTIVE: not a candidate */
	CLUSTER_SEG_RECYCLE_READ_FAIL = 4,	   /* absent / I/O / identity mismatch */
	CLUSTER_SEG_RECYCLE_WRITE_FAIL = 5,	   /* pwrite / fsync failed; retry next pass */
	CLUSTER_SEG_RECYCLE_EPOCH_CHANGED = 6  /* spec-5.22e F-D2 epoch fence: the
											* reconfig epoch moved after the
											* recycle floor was folded; the
											* mutation was NOT performed and the
											* caller must abort the whole pass
											* (the folded floor's member set no
											* longer covers the cluster) */
} ClusterUndoSegTryRecycle;

/* Caller holds no lifecycle/content lock; expected_epoch binds the folded floor. */
extern ClusterUndoSegTryRecycle cluster_undo_segment_try_mark_recyclable(uint32 segment_id,
																		 uint8 owner_instance,
																		 SCN horizon,
																		 uint64 expected_epoch);

/* spec-3.13 D4: exact in-place rebirth; caller holds no lifecycle/content lock. */
extern uint32 cluster_undo_segment_reuse_in_place(uint32 segment_id, uint8 owner_instance,
												  uint32 old_generation);

/* spec-3.13 D4: durable segment generation (== header wrap_count; 0 = unknown). */
extern uint32 cluster_undo_segment_generation(uint32 segment_id, uint8 owner_instance);

/* spec-3.13: identity check exported for redo + reuse peek (L212 surface). */
extern bool cluster_undo_segment_header_identity_ok(const char *blockbuf, uint32 segment_id,
													uint8 owner_instance);

typedef enum ClusterUndoPoolObservationResult {
	CLUSTER_UNDO_POOL_OBS_OK = 0,
	CLUSTER_UNDO_POOL_OBS_INVALID_OWNER,
	CLUSTER_UNDO_POOL_OBS_IO_ERROR,
	CLUSTER_UNDO_POOL_OBS_INVALID_HEADER
} ClusterUndoPoolObservationResult;

typedef struct ClusterUndoPoolObservation {
	uint32 allocated_count;
	uint32 configured_cap;
	uint32 effective_cap;
} ClusterUndoPoolObservation;

extern ClusterUndoPoolObservationResult
cluster_undo_segment_observe_pool(uint8 owner_instance, ClusterUndoPoolObservation *out);


#endif /* CLUSTER_UNDO_ALLOC_H */
