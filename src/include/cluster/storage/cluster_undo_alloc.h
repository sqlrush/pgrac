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
 * cluster_undo_path_resolve
 *	  Build the segment file path:
 *	    $PGDATA/pg_undo/instance_<N>/seg_<segment_id>.dat
 *
 *	  Path components are bounded:
 *	    <N> in [0, 255] (uint8 max)
 *	    <segment_id> in [0, 4294967295] (uint32 max)
 *
 *	  Returns 0 on success, -1 on buffer overflow.  Caller supplies a
 *	  buf with capacity >= MAXPGPATH.
 */
extern int cluster_undo_path_resolve(uint8 instance, uint32 segment_id, char *buf, size_t buf_size);


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
 *	    2. Resolve path, ensure parent dir, open-or-create file.
 *	    3. Generate the 8 KB header bytes via the shared helper.
 *	    4. ftruncate to UNDO_SEGMENT_SIZE_BYTES.
 *	    5. pwrite block 0 + fsync + close + fsync parent dir.
 *	    6. Emit XLOG_UNDO_SEGMENT_INIT (RM_CLUSTER_UNDO) so crash recovery
 *	       reapplies the same byte layout if the page is lost.
 *
 *	  spec-3.4b D2 unlock: the previous Stage 1.22 single-node restriction
 *	  (owner_instance must equal 1) is removed.  The remaining restriction
 *	  is "owner_instance == cluster_node_id + 1" so a node never writes
 *	  another node's segment directory.
 *
 *	  Caller MUST NOT be inside a critical section: function emits WAL
 *	  via XLogInsert + may ereport(ERROR) on validation / I/O failures.
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

/*
 * spec-3.8 D2:  autoextend lazy at exhaustion.
 *
 *	Allocates the next free segment slot for owner_instance.  Returns
 *	NEW segment_id on success;  0 on failure.  Sets *out_at_hard_cap
 *	when no free slot remains (pool exhausted within encoding limit).
 *
 *	Caller MUST hold lifecycle_lock (NOT cursor_lock) per spec §3.2.
 *	NOT critical-section safe (does file I/O + fsync).
 */
extern uint32 cluster_undo_segment_extend_or_create(uint8 owner_instance, bool *out_at_hard_cap);


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
 * spec-3.13 D3: COMMITTED -> RECYCLABLE advancement outcome.
 */
typedef enum ClusterUndoSegTryRecycle {
	CLUSTER_SEG_RECYCLE_ADVANCED = 0,	   /* transitioned + durable */
	CLUSTER_SEG_RECYCLE_ALREADY = 1,	   /* idempotent: already RECYCLABLE */
	CLUSTER_SEG_RECYCLE_RETAINED = 2,	   /* predicate says a reader may need it */
	CLUSTER_SEG_RECYCLE_NOT_COMMITTED = 3, /* ALLOCATED / ACTIVE: not a candidate */
	CLUSTER_SEG_RECYCLE_READ_FAIL = 4,	   /* absent / I/O / identity mismatch */
	CLUSTER_SEG_RECYCLE_WRITE_FAIL = 5	   /* pwrite / fsync failed; retry next pass */
} ClusterUndoSegTryRecycle;

/* Caller holds undo lifecycle_lock and excluded the active record segment. */
extern ClusterUndoSegTryRecycle
cluster_undo_segment_try_mark_recyclable(uint32 segment_id, uint8 owner_instance, SCN horizon);


#endif /* CLUSTER_UNDO_ALLOC_H */
