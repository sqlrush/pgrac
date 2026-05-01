/*-------------------------------------------------------------------------
 *
 * cluster_smgr.h
 *	  pgrac cluster-aware storage manager (Stage 1.2 single-node passthrough).
 *
 *	  Bridges the spec-1.1 cluster_shared_fs vtable into PG's smgrsw[]
 *	  array.  All sixteen f_smgr callbacks are declared here so that
 *	  storage/smgr/smgr.c can populate its second smgrsw[] entry from
 *	  this file's symbols (PGRAC MODIFICATIONS to smgr.c).
 *
 *	  Stage 1.2 = single-node passthrough: on-disk file layout (path,
 *	  1GB segment splitting, fsync semantics) is byte-identical to PG's
 *	  md.c.  Only the I/O entry point changes: smgr -> cluster_smgr ->
 *	  cluster_shared_fs -> active backend (local for stage 1.2) ->
 *	  fd.c.  Stage 2 swaps in real cluster backends (block_device /
 *	  cluster_fs / rbd / multi_attach) without touching this file.
 *
 *	  Selection happens at smgropen() time via cluster_smgr_which_for():
 *	  permanent (non-temp) relations route to smgr_which=1 when both
 *	  cluster.shared_storage_backend != stub AND
 *	  cluster.smgr_user_relations = on.  Default off: nothing changes.
 *
 *	  See specs/spec-1.2-smgr-cluster.md and
 *	  docs/cluster-smgr-design.md for the full design.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_smgr.h
 *
 * NOTES
 *	  This is a pgrac-original file.  All sixteen callback prototypes
 *	  match PG's f_smgr typedef in storage/smgr/smgr.c exactly so that
 *	  smgr.c can wire them into smgrsw[1] without any glue layer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SMGR_H
#define CLUSTER_SMGR_H

#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"


/* ----------
 * Lifecycle
 *
 *	cluster_smgr_init -- called by smgrinit() once per backend.  Sets
 *	    up the per-process bypass HTAB used to track per-rlocator
 *	    segment state.  Invoked through smgrsw[1].smgr_init.
 *
 *	cluster_smgr_shutdown -- called at backend exit (smgrshutdown).
 *	    Closes every open ClusterSharedFsHandle and frees the HTAB.
 * ----------
 */
extern void cluster_smgr_init(void);
extern void cluster_smgr_shutdown(void);


/* ----------
 * smgrsw[] dispatch decision
 *
 *	cluster_smgr_which_for(rlocator, backend) -- returns 0 (= md.c) or
 *	    1 (= cluster_smgr).  Called by smgropen() to populate
 *	    SMgrRelationData.smgr_which for the lifetime of that
 *	    SMgrRelation entry.
 *
 *	Decision rules (see docs/cluster-smgr-design.md §5):
 *	  - backend != InvalidBackendId       -> 0 (temp relations)
 *	  - shared_storage_backend == STUB    -> 0 (cluster fs disabled)
 *	  - cluster.smgr_user_relations == off -> 0 (opt-in default off)
 *	  - otherwise                          -> 1
 * ----------
 */
extern int	cluster_smgr_which_for(RelFileLocator rlocator, BackendId backend);


/* ----------
 * Sixteen f_smgr callback implementations
 *
 *	Signatures match PG's f_smgr typedef in src/backend/storage/smgr/
 *	smgr.c byte-for-byte so that smgrsw[1] can be initialised directly
 *	from these symbols.  Stage 1.2 implementations dispatch to
 *	cluster_shared_fs (for the nine core I/O ops) or fall through to
 *	md.c counterparts (for the three advisory ops: zeroextend /
 *	prefetch / writeback).  See §2.2 / §10 of the design doc for the
 *	full mapping table.
 * ----------
 */
extern void cluster_smgr_open(SMgrRelation reln);
extern void cluster_smgr_close(SMgrRelation reln, ForkNumber forknum);
extern void cluster_smgr_create(SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
extern bool cluster_smgr_exists(SMgrRelation reln, ForkNumber forknum);
extern void cluster_smgr_unlink(RelFileLocatorBackend rlocator,
								ForkNumber forknum, bool isRedo);
extern void cluster_smgr_extend(SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, const void *buffer,
								bool skipFsync);
extern void cluster_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum, int nblocks,
									bool skipFsync);
extern bool cluster_smgr_prefetch(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum);
extern void cluster_smgr_read(SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, void *buffer);
extern void cluster_smgr_write(SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum, const void *buffer,
							   bool skipFsync);
extern void cluster_smgr_writeback(SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber cluster_smgr_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void cluster_smgr_truncate(SMgrRelation reln, ForkNumber forknum,
								  BlockNumber old_blocks, BlockNumber nblocks);
extern void cluster_smgr_immedsync(SMgrRelation reln, ForkNumber forknum);


/* ----------
 * Diagnostic accessor
 *
 *	cluster_smgr_active_relation_count -- number of SMgrRelations
 *	    currently registered in the bypass HTAB.  Read by
 *	    cluster_debug.c::dump_shared_fs to surface
 *	    "shared_fs.smgr_active_relations" in pg_cluster_state.
 *	    Returns 0 if the HTAB has not been initialised yet.
 * ----------
 */
extern int	cluster_smgr_active_relation_count(void);


#endif							/* CLUSTER_SMGR_H */
