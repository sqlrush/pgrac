/*-------------------------------------------------------------------------
 *
 * cluster_undo_vtable.h
 *	  pgrac buffer-backed undo smgr adapter (spec-3.27 D2, §2.2 / Q3-A).
 *
 *	  Wires the per-instance undo segment files (pg_undo/instance_<N>/seg_<id>.dat,
 *	  I/O via cluster_undo_smgr.c) into PG's smgrsw[] as a THIRD storage manager
 *	  (index CLUSTER_UNDO_SMGR_SMGRSW_INDEX = 2) so that the shared buffer manager
 *	  can ReadBufferExtended / FlushBuffer / checkpoint undo blocks directly (the
 *	  B2-full buffer-backed model).
 *
 *	  Selection: smgropen() -> cluster_smgr_which_for() returns index 2 for any
 *	  RelFileLocator whose spcOid == CLUSTER_UNDO_REL_SPCOID (the reserved undo
 *	  tablespace, cluster_undo_buftag.h).  That routing is UNCONDITIONAL (not
 *	  GUC-gated) because an undo RelFileLocator only ever reaches smgropen from
 *	  the bufmgr write path -- in legacy_pool mode (cluster.undo_buffer_backend,
 *	  D7) the undo code never constructs an undo BufferTag, so no undo tag can
 *	  arrive here to be mis-routed.
 *
 *	  Each undo SEGMENT is one bufmgr "relation":  relNumber = segment_id, dbOid
 *	  carries owner_instance (cluster_undo_buftag.h), fork = MAIN_FORKNUM, block =
 *	  undo block_no.  Segments are fixed 64 MB / UNDO_BLOCKS_PER_SEGMENT (8192)
 *	  blocks, pre-extended full-size by the undo allocator (ftruncate,
 *	  cluster_undo_alloc.c), so smgr_nblocks is the constant and bufmgr never
 *	  drives the extend path on the hot read/write route.
 *
 *	  Block 0 (segment header + durable TT slot, spec-3.11) is NOT routed through
 *	  bufmgr (spec-3.27 Q2-A):  the D3 write path only ReadBuffers data blocks
 *	  [1, UNDO_BLOCKS_PER_SEGMENT).  The vtable itself serves whatever block bufmgr
 *	  requests;  the block-0 exclusion is a caller (D3) contract.
 *
 *	  All sixteen prototypes match PG's f_smgr typedef (storage/smgr/smgr.c)
 *	  byte-for-byte so smgr.c can populate smgrsw[2] with no glue layer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.27-undo-buffer-backed-model.md (FROZEN v1.0, D2 / §2.2 / Q3-A)
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_undo_vtable.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Backend-only.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_VTABLE_H
#define CLUSTER_UNDO_VTABLE_H

#ifndef FRONTEND

#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"

/*
 * smgrsw[] index of the buffer-backed undo smgr.  md.c = 0, cluster_smgr = 1
 * (CLUSTER_SMGR_SMGRSW_INDEX), undo = 2.  Kept in sync with the smgrsw[] array
 * in storage/smgr/smgr.c (PGRAC MODIFICATIONS).
 */
#define CLUSTER_UNDO_SMGR_SMGRSW_INDEX 2

/* ----------
 * Sixteen f_smgr callbacks (signatures match PG's f_smgr typedef exactly).
 * ---------- */
extern void cluster_undo_vtable_open(SMgrRelation reln);
extern void cluster_undo_vtable_close(SMgrRelation reln, ForkNumber forknum);
extern void cluster_undo_vtable_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool cluster_undo_vtable_exists(SMgrRelation reln, ForkNumber forknum);
extern void cluster_undo_vtable_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum,
									   bool isRedo);
extern void cluster_undo_vtable_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
									   const void *buffer, bool skipFsync);
extern void cluster_undo_vtable_zeroextend(SMgrRelation reln, ForkNumber forknum,
										   BlockNumber blocknum, int nblocks, bool skipFsync);
extern bool cluster_undo_vtable_prefetch(SMgrRelation reln, ForkNumber forknum,
										 BlockNumber blocknum);
extern void cluster_undo_vtable_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
									 void *buffer);
extern void cluster_undo_vtable_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
									  const void *buffer, bool skipFsync);
extern void cluster_undo_vtable_writeback(SMgrRelation reln, ForkNumber forknum,
										  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber cluster_undo_vtable_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void cluster_undo_vtable_truncate(SMgrRelation reln, ForkNumber forknum,
										 BlockNumber old_blocks, BlockNumber nblocks);
extern void cluster_undo_vtable_immedsync(SMgrRelation reln, ForkNumber forknum);

#endif /* !FRONTEND */

#endif /* CLUSTER_UNDO_VTABLE_H */
