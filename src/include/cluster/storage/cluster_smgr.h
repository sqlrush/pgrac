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
 *	  Stage 1.2 = single-node passthrough.  Stage 1.8 verified the
 *	  end-to-end opt-in workflow (initdb -> postgresql.conf opt-in ->
 *	  pg_ctl start -> SQL CRUD -> restart -> shutdown) on the local
 *	  backend; see t/050_shared_storage_initdb.pl L1-L10 matrix.
 *	  Path scheme is the same as md.c (per-relation file under
 *	  PGDATA/pg_tblspc).  But several contracts intentionally differ
 *	  from md.c and should not be treated as byte-identical (Sprint A
 *	  2026-05-02 hardening, spec-1.X-cluster-smgr-hardening §
 *	  correcting the original Stage 1.2 over-claim; spec-1.7.2
 *	  2026-05-03 round 2 hardening):
 *
 *	    - Single-file layout: NO 1GB segment splitting (decided in
 *	      spec-1.2 选 C).  md.c splits relations into 1GB segments
 *	      (relfilenode, relfilenode.1, .2 ...); cluster_smgr keeps
 *	      one file per relation per fork to simplify shared-storage
 *	      backend semantics in Stage 2.
 *	    - fsync registration: spec-6.0a wires cluster_smgr writes into
 *	      PG's RegisterSyncRequest path via SYNC_HANDLER_CLUSTER_SHARED;
 *	      queue-full fallback performs an immediate backend barrier_sync.
 *	      Pending-unlink remains backend-specific because raw layout frees
 *	      extents through WAL-logged metadata rather than md.c segments.
 *	    - GUC `cluster.smgr_user_relations` is EXPERIMENTAL in
 *	      Stage 1.X (default off; ON triggers postmaster startup
 *	      WARNING from cluster_shared_fs_init -- moved here from
 *	      cluster_smgr_init in spec-1.7.2 F2 fix because PG
 *	      smgr.c:162 explicitly states smgrinit() is "not called
 *	      during postmaster start").  Stage 1.8 verifies the opt-in
 *	      workflow end-to-end.  spec-6.0a adds production shared-storage
 *	      durability hooks, but merge/ship remains blocked on the Stage 5
 *	      beta close-out and final Stage 6 D0 re-ground.
 *
 *	  I/O dispatch chain: smgr -> cluster_smgr -> cluster_shared_fs
 *	  -> active backend (local for Stage 1.2) -> fd.c.  Stage 2 swaps
 *	  in real cluster backends (block_device / cluster_fs / rbd /
 *	  multi_attach) without touching this file.
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

#include "port/atomics.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/sinval.h" /* spec-5.2 D1: SharedInvalidationMessage */
#include "storage/smgr.h"
#include "storage/sync.h"


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
extern int cluster_smgr_which_for(RelFileLocator rlocator, BackendId backend);


/* ----------
 * Sixteen f_smgr callback implementations
 *
 *	Signatures match PG's f_smgr typedef in src/backend/storage/smgr/
 *	smgr.c byte-for-byte so that smgrsw[1] can be initialised directly
 *	from these symbols.  Stage 1.2 implementations dispatch to
 *	cluster_shared_fs (core storage, lifecycle, durability/fence, and
 *	advisory callbacks as of spec-6.0a).  See §2.2 / §10 of the design
 *	doc for the full mapping table.
 * ----------
 */
extern void cluster_smgr_open(SMgrRelation reln);
extern void cluster_smgr_close(SMgrRelation reln, ForkNumber forknum);
extern void cluster_smgr_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool cluster_smgr_exists(SMgrRelation reln, ForkNumber forknum);
extern void cluster_smgr_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
extern void cluster_smgr_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
								const void *buffer, bool skipFsync);
extern void cluster_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
									int nblocks, bool skipFsync);
extern bool cluster_smgr_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern void cluster_smgr_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							  void *buffer);
extern void cluster_smgr_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							   const void *buffer, bool skipFsync);
extern void cluster_smgr_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
								   BlockNumber nblocks);
extern BlockNumber cluster_smgr_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void cluster_smgr_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks,
								  BlockNumber nblocks);
extern void cluster_smgr_immedsync(SMgrRelation reln, ForkNumber forknum);

/* spec-6.0a: sync.c handler for cluster shared-storage relation tags. */
extern int cluster_smgr_syncfiletag(const FileTag *ftag, char *path);
extern int cluster_smgr_unlinkfiletag(const FileTag *ftag, char *path);
extern bool cluster_smgr_filetagmatches(const FileTag *ftag, const FileTag *candidate);


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
extern int cluster_smgr_active_relation_count(void);


/* ----------
 * spec-2.7 invalidation hooks (v0.2 frozen 2026-05-09).
 *
 *	Three entry points for cluster-aware cache invalidation that
 *	spec-2.27 will wire into the SI Broadcaster aux process (see
 *	specs/spec-2.7-smgr-cluster-2node-concurrent-open.md §8 Q1 v0.2
 *	for the full design rationale).
 *
 *	Per Q1 v0.2:
 *	  - cluster_smgr_invalidate_relation:  pure cross-instance
 *	    broadcast STUB.  No local action -- PG smgr.c invalidates
 *	    its own smgr_cached_nblocks via existing extend / truncate
 *	    internals.  cluster_smgr layer carries no relation-keyed
 *	    nblocks cache (see ClusterSmgrRelationState comment in
 *	    cluster_smgr.c).
 *	  - cluster_smgr_invalidate_relmap(bool shared):  pure cross-
 *	    instance STUB.  Signature matches PG's RelationMapInvalidate
 *	    (shared=true means the shared-catalog map; shared=false means
 *	    the current MyDatabaseId per-database map).  PG relmapper.c
 *	    reloads its local cache via load_relmap_file().
 *	  - cluster_smgr_invalidate_unlink_pending:  cross-instance STUB
 *	    PLUS a LOCAL REAL action -- close any open
 *	    ClusterSharedFsHandle for `rlocator` and remove the bypass
 *	    HTAB entry.  Without this, an unlink-then-recreate of the
 *	    same rlocator could reach the now-gone underlying file via
 *	    a stale fd.
 *
 *	Per Q3 v0.2:  hook bodies do not emit hot-path DEBUG2 ereport
 *	(errstart_cold short-circuit still costs ~100ns; per-block
 *	smgrextend in 1024-block batches would amount to ~100us of pure
 *	noise).  Coalescing across (rel, fork) is spec-2.27 SI Broadcaster
 *	queue's job, not the hook layer's.
 *
 *	Per Q5 v0.2:  the single counter
 *	`cluster_smgr_remote_invalidation_stub_call_count` records the
 *	cross-instance broadcast STUB portion only (atomic-add at the end
 *	of every hook body).  It does NOT count the local handle/HTAB
 *	cleanup inside invalidate_unlink_pending; that local action is
 *	already covered by PG SMgrRelation lifecycle observability.
 *	spec-2.27 will rename this to
 *	`cluster_smgr_remote_invalidation_count` (drop `_stub_`) and add
 *	per-type sub-counters + per-rlocator histograms.
 * ----------
 */
extern void cluster_smgr_invalidate_relation(RelFileLocator rlocator, ForkNumber forknum);
extern void cluster_smgr_invalidate_relmap(bool shared);
extern void cluster_smgr_invalidate_unlink_pending(RelFileLocator rlocator);

/*
 * The cross-instance broadcast STUB call counter is allocated in
 * shmem (registered via cluster_smgr_shmem_register) so that all
 * backends in this postmaster see one shared accumulator.  Hot-path
 * accessor below reads from the shmem atomic;disable-cluster builds
 * compile out the symbol entirely.
 */
extern Size cluster_smgr_shmem_size(void);
extern void cluster_smgr_shmem_init(void);
extern void cluster_smgr_shmem_register(void);
extern uint64 cluster_smgr_get_remote_invalidation_stub_call_count(void);

/* ----------
 * spec-5.2 D1 (relsize coherence, M2) — relation-extend invalidation
 * broadcast helpers.  cluster_smgr_invalidate_relation() now constructs
 * a PG-native SHAREDINVALSMGR_ID message (no new wire type, G2) and
 * broadcasts it via cluster_sinval_enqueue_batch() so peers drop their
 * stale SMgrRelation (incl. smgr_cached_nblocks) and re-stat the shared
 * file on next smgrnblocks().  The two helpers below are pure and exist
 * so the construction + enqueue-full policy are unit-testable in
 * isolation (U2).
 * ----------
 */

/*
 * cluster_smgr_build_smgr_inval_msg — build a PG-native SHAREDINVALSMGR_ID
 * invalidation message for a cluster relation.  Cluster relations live on
 * shared storage and are never temp, so backend == InvalidBackendId.
 * Mirrors PG's CacheInvalidateSmgr() message construction.
 */
extern void cluster_smgr_build_smgr_inval_msg(RelFileLocator rlocator,
											  SharedInvalidationMessage *out);

/*
 * cluster_smgr_inval_full_action — enqueue-full fail-closed policy (H2).
 * Never silently drop a relsize invalidation: outside a critical section
 * the caller aborts the extend (ERRCODE_CLUSTER_SINVAL_QUEUE_FULL 53R94);
 * inside a critical section it cannot ereport(ERROR), so it falls back to
 * a coarse RESET-all broadcast (peers re-stat everything).
 */
typedef enum ClusterSmgrInvalFullAction {
	CLUSTER_SMGR_INVAL_FULL_ABORT = 0, /* non-crit: ereport 53R94 */
	CLUSTER_SMGR_INVAL_FULL_RESET_ALL  /* crit: RESET-all fallback */
} ClusterSmgrInvalFullAction;

extern ClusterSmgrInvalFullAction cluster_smgr_inval_full_action(bool in_crit_section);

/*
 * spec-5.2 D1/D9 — count of relsize invalidations successfully broadcast
 * (source side).  Used by t/279 + dump_smgr observability.
 */
extern uint64 cluster_smgr_get_inval_bcast_sent_count(void);

/*
 * Public smgrsw[] dispatch index reserved for cluster_smgr.  Used by
 * PG-original hook call sites (smgr.c spec-2.7 hooks) to gate
 * cluster invalidation broadcasts to relations actually routed
 * through cluster_smgr;non-cluster_smgr (smgr_which == 0 -> md.c)
 * relations skip the hooks entirely.
 */
#define CLUSTER_SMGR_SMGRSW_INDEX 1


#endif /* CLUSTER_SMGR_H */
