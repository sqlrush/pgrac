/*-------------------------------------------------------------------------
 *
 * smgr.c
 *	  public interface routines to storage manager switch.
 *
 *	  All file system operations in POSTGRES dispatch through these
 *	  routines.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * PGRAC MODIFICATIONS
 *	  Modified by: SqlRush <sqlrush@gmail.com>
 *	  Stage:        1.2 + 2.7
 *
 *	  Stage 1.2 (initial):
 *	    Extended smgrsw[] from 1 to 2 entries (under USE_PGRAC_CLUSTER):
 *	    index 0 stays md.c, index 1 routes to cluster_smgr (cluster-aware
 *	    storage that bridges into the spec-1.1 cluster_shared_fs vtable).
 *	    smgropen() now consults cluster_smgr_which_for() to pick the
 *	    smgr_which value at relation-open time; default behaviour
 *	    (cluster.smgr_user_relations = off, the GUC default) returns 0
 *	    for every rlocator so the production path is byte-for-byte
 *	    identical to upstream.
 *
 *	    In --disable-cluster builds neither the include nor the array
 *	    extension fires, so smgrsw[] remains a single element and
 *	    smgr_which is forced to 0 -- spec-0.3 binary contract.
 *
 *	  Stage 2.7 (spec-2.7 v0.2 frozen 2026-05-09):
 *	    Added three cluster invalidation hook call sites alongside the
 *	    existing process-local invalidation paths:
 *	      smgrextend / smgrzeroextend -> cluster_smgr_invalidate_relation
 *	      smgrtruncate2               -> cluster_smgr_invalidate_relation
 *	      smgrdounlinkall             -> cluster_smgr_invalidate_unlink_pending
 *	    Hook bodies are pure cross-instance broadcast STUBs at this
 *	    stage (counter atomic add only;no DEBUG2 hot path log per
 *	    Q3 v0.2). spec-2.27 SI Broadcaster will activate the wire
 *	    send + ack path without further changes to this PG-original
 *	    file. All hook calls are gated `if (cluster_enabled)` and
 *	    wrapped in `#ifdef USE_PGRAC_CLUSTER`, so disable-cluster
 *	    and cluster.enabled=off paths stay byte-identical to
 *	    upstream PG.
 *
 *	  Related design:
 *	    docs/cluster-smgr-design.md v1.1 (方案 C 单文件)
 *	    specs/spec-1.2-smgr-cluster.md
 *	    specs/spec-2.7-smgr-cluster-2node-concurrent-open.md
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogutils.h"
#include "lib/ilist.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/md.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"		/* PGRAC: cluster_enabled */
#include "cluster/cluster_cr_pool.h"			/* PGRAC: spec-5.51 CR pool epoch bump */
#include "cluster/cluster_inject.h"			/* PGRAC: spec-5.53 D2b skip-bump fault */
#include "cluster/storage/cluster_smgr.h"		/* PGRAC: smgrsw[1] + spec-2.7 hooks */
#endif
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"


/*
 * This struct of function pointers defines the API between smgr.c and
 * any individual storage manager module.  Note that smgr subfunctions are
 * generally expected to report problems via elog(ERROR).  An exception is
 * that smgr_unlink should use elog(WARNING), rather than erroring out,
 * because we normally unlink relations during post-commit/abort cleanup,
 * and so it's too late to raise an error.  Also, various conditions that
 * would normally be errors should be allowed during bootstrap and/or WAL
 * recovery --- see comments in md.c for details.
 */
typedef struct f_smgr
{
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_open) (SMgrRelation reln);
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileLocatorBackend rlocator, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, const void *buffer, bool skipFsync);
	void		(*smgr_zeroextend) (SMgrRelation reln, ForkNumber forknum,
									BlockNumber blocknum, int nblocks, bool skipFsync);
	bool		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum);
	void		(*smgr_read) (SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, void *buffer);
	void		(*smgr_write) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum, const void *buffer, bool skipFsync);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber old_blocks, BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
} f_smgr;

static const f_smgr smgrsw[] = {
	/* magnetic disk */
	{
		.smgr_init = mdinit,
		.smgr_shutdown = NULL,
		.smgr_open = mdopen,
		.smgr_close = mdclose,
		.smgr_create = mdcreate,
		.smgr_exists = mdexists,
		.smgr_unlink = mdunlink,
		.smgr_extend = mdextend,
		.smgr_zeroextend = mdzeroextend,
		.smgr_prefetch = mdprefetch,
		.smgr_read = mdread,
		.smgr_write = mdwrite,
		.smgr_writeback = mdwriteback,
		.smgr_nblocks = mdnblocks,
		.smgr_truncate = mdtruncate,
		.smgr_immedsync = mdimmedsync,
	}
#ifdef USE_PGRAC_CLUSTER
	,
	/*
	 * PGRAC stage 1.2: cluster-aware smgr.  Selected by smgropen()
	 * via cluster_smgr_which_for() when both
	 * cluster.shared_storage_backend != stub AND
	 * cluster.smgr_user_relations = on AND backend == InvalidBackendId.
	 * See cluster_smgr.c (方案 C 单文件实装) for callback bodies.
	 */
	{
		.smgr_init = cluster_smgr_init,
		.smgr_shutdown = cluster_smgr_shutdown,
		.smgr_open = cluster_smgr_open,
		.smgr_close = cluster_smgr_close,
		.smgr_create = cluster_smgr_create,
		.smgr_exists = cluster_smgr_exists,
		.smgr_unlink = cluster_smgr_unlink,
		.smgr_extend = cluster_smgr_extend,
		.smgr_zeroextend = cluster_smgr_zeroextend,
		.smgr_prefetch = cluster_smgr_prefetch,
		.smgr_read = cluster_smgr_read,
		.smgr_write = cluster_smgr_write,
		.smgr_writeback = cluster_smgr_writeback,
		.smgr_nblocks = cluster_smgr_nblocks,
		.smgr_truncate = cluster_smgr_truncate,
		.smgr_immedsync = cluster_smgr_immedsync,
	}
#endif
};

static const int NSmgr = lengthof(smgrsw);

/*
 * Each backend has a hashtable that stores all extant SMgrRelation objects.
 * In addition, "unowned" SMgrRelation objects are chained together in a list.
 */
static HTAB *SMgrRelationHash = NULL;

static dlist_head unowned_relns;

/* local function prototypes */
static void smgrshutdown(int code, Datum arg);


/*
 * smgrinit(), smgrshutdown() -- Initialize or shut down storage
 *								 managers.
 *
 * Note: smgrinit is called during backend startup (normal or standalone
 * case), *not* during postmaster start.  Therefore, any resources created
 * here or destroyed in smgrshutdown are backend-local.
 */
void
smgrinit(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_init)
			smgrsw[i].smgr_init();
	}

	/* register the shutdown proc */
	on_proc_exit(smgrshutdown, 0);
}

/*
 * on_proc_exit hook for smgr cleanup during backend shutdown
 */
static void
smgrshutdown(int code, Datum arg)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_shutdown)
			smgrsw[i].smgr_shutdown();
	}
}

/*
 * smgropen() -- Return an SMgrRelation object, creating it if need be.
 *
 * This does not attempt to actually open the underlying file.
 */
SMgrRelation
smgropen(RelFileLocator rlocator, BackendId backend)
{
	RelFileLocatorBackend brlocator;
	SMgrRelation reln;
	bool		found;

	if (SMgrRelationHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		ctl.keysize = sizeof(RelFileLocatorBackend);
		ctl.entrysize = sizeof(SMgrRelationData);
		SMgrRelationHash = hash_create("smgr relation table", 400,
									   &ctl, HASH_ELEM | HASH_BLOBS);
		dlist_init(&unowned_relns);
	}

	/* Look up or create an entry */
	brlocator.locator = rlocator;
	brlocator.backend = backend;
	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  &brlocator,
									  HASH_ENTER, &found);

	/* Initialize it if not present before */
	if (!found)
	{
		/* hash_search already filled in the lookup key */
		reln->smgr_owner = NULL;
		reln->smgr_targblock = InvalidBlockNumber;
		for (int i = 0; i <= MAX_FORKNUM; ++i)
			reln->smgr_cached_nblocks[i] = InvalidBlockNumber;
#ifdef USE_PGRAC_CLUSTER
		/*
		 * PGRAC: pick md.c (smgr_which=0) or cluster_smgr (1) based on
		 * GUC + InvalidBackendId.  Default returns 0 so the production
		 * path is byte-for-byte identical to upstream (cluster.smgr_
		 * user_relations is opt-in, off by default).
		 */
		reln->smgr_which = cluster_smgr_which_for(rlocator, backend);
#else
		reln->smgr_which = 0;	/* we only have md.c at present */
#endif

		/* implementation-specific initialization */
		smgrsw[reln->smgr_which].smgr_open(reln);

		/* it has no owner yet */
		dlist_push_tail(&unowned_relns, &reln->node);
	}

	return reln;
}

/*
 * smgrsetowner() -- Establish a long-lived reference to an SMgrRelation object
 *
 * There can be only one owner at a time; this is sufficient since currently
 * the only such owners exist in the relcache.
 */
void
smgrsetowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* We don't support "disowning" an SMgrRelation here, use smgrclearowner */
	Assert(owner != NULL);

	/*
	 * First, unhook any old owner.  (Normally there shouldn't be any, but it
	 * seems possible that this can happen during swap_relation_files()
	 * depending on the order of processing.  It's ok to close the old
	 * relcache entry early in that case.)
	 *
	 * If there isn't an old owner, then the reln should be in the unowned
	 * list, and we need to remove it.
	 */
	if (reln->smgr_owner)
		*(reln->smgr_owner) = NULL;
	else
		dlist_delete(&reln->node);

	/* Now establish the ownership relationship. */
	reln->smgr_owner = owner;
	*owner = reln;
}

/*
 * smgrclearowner() -- Remove long-lived reference to an SMgrRelation object
 *					   if one exists
 */
void
smgrclearowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* Do nothing if the SMgrRelation object is not owned by the owner */
	if (reln->smgr_owner != owner)
		return;

	/* unset the owner's reference */
	*owner = NULL;

	/* unset our reference to the owner */
	reln->smgr_owner = NULL;

	/* add to list of unowned relations */
	dlist_push_tail(&unowned_relns, &reln->node);
}

/*
 * smgrexists() -- Does the underlying file for a fork exist?
 */
bool
smgrexists(SMgrRelation reln, ForkNumber forknum)
{
	return smgrsw[reln->smgr_which].smgr_exists(reln, forknum);
}

/*
 * smgrclose() -- Close and delete an SMgrRelation object.
 */
void
smgrclose(SMgrRelation reln)
{
	SMgrRelation *owner;
	ForkNumber	forknum;

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		smgrsw[reln->smgr_which].smgr_close(reln, forknum);

	owner = reln->smgr_owner;

	if (!owner)
		dlist_delete(&reln->node);

	if (hash_search(SMgrRelationHash,
					&(reln->smgr_rlocator),
					HASH_REMOVE, NULL) == NULL)
		elog(ERROR, "SMgrRelation hashtable corrupted");

	/*
	 * Unhook the owner pointer, if any.  We do this last since in the remote
	 * possibility of failure above, the SMgrRelation object will still exist.
	 */
	if (owner)
		*owner = NULL;
}

/*
 * smgrrelease() -- Release all resources used by this object.
 *
 * The object remains valid.
 */
void
smgrrelease(SMgrRelation reln)
{
	for (ForkNumber forknum = 0; forknum <= MAX_FORKNUM; forknum++)
	{
		smgrsw[reln->smgr_which].smgr_close(reln, forknum);
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
	}
	reln->smgr_targblock = InvalidBlockNumber;
}

/*
 * smgrreleaseall() -- Release resources used by all objects.
 *
 * This is called for PROCSIGNAL_BARRIER_SMGRRELEASE.
 */
void
smgrreleaseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	hash_seq_init(&status, SMgrRelationHash);

	while ((reln = (SMgrRelation) hash_seq_search(&status)) != NULL)
		smgrrelease(reln);
}

/*
 * smgrcloseall() -- Close all existing SMgrRelation objects.
 */
void
smgrcloseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	hash_seq_init(&status, SMgrRelationHash);

	while ((reln = (SMgrRelation) hash_seq_search(&status)) != NULL)
		smgrclose(reln);
}

/*
 * smgrcloserellocator() -- Close SMgrRelation object for given RelFileLocator,
 *							if one exists.
 *
 * This has the same effects as smgrclose(smgropen(rlocator)), but it avoids
 * uselessly creating a hashtable entry only to drop it again when no
 * such entry exists already.
 */
void
smgrcloserellocator(RelFileLocatorBackend rlocator)
{
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  &rlocator,
									  HASH_FIND, NULL);
	if (reln != NULL)
		smgrclose(reln);
}

/*
 * smgrcreate() -- Create a new relation.
 *
 * Given an already-created (but presumably unused) SMgrRelation,
 * cause the underlying disk file or other storage for the fork
 * to be created.
 */
void
smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	smgrsw[reln->smgr_which].smgr_create(reln, forknum, isRedo);
}

/*
 * smgrdosyncall() -- Immediately sync all forks of all given relations
 *
 * All forks of all given relations are synced out to the store.
 *
 * This is equivalent to FlushRelationBuffers() for each smgr relation,
 * then calling smgrimmedsync() for all forks of each relation, but it's
 * significantly quicker so should be preferred when possible.
 */
void
smgrdosyncall(SMgrRelation *rels, int nrels)
{
	int			i = 0;
	ForkNumber	forknum;

	if (nrels == 0)
		return;

	FlushRelationsAllBuffers(rels, nrels);

	/*
	 * Sync the physical file(s).
	 */
	for (i = 0; i < nrels; i++)
	{
		int			which = rels[i]->smgr_which;

		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			if (smgrsw[which].smgr_exists(rels[i], forknum))
				smgrsw[which].smgr_immedsync(rels[i], forknum);
		}
	}
}

/*
 * smgrdounlinkall() -- Immediately unlink all forks of all given relations
 *
 * All forks of all given relations are removed from the store.  This
 * should not be used during transactional operations, since it can't be
 * undone.
 *
 * If isRedo is true, it is okay for the underlying file(s) to be gone
 * already.
 */
void
smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo)
{
	int			i = 0;
	RelFileLocatorBackend *rlocators;
	ForkNumber	forknum;

	if (nrels == 0)
		return;

	/*
	 * Get rid of any remaining buffers for the relations.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelationsAllBuffers(rels, nrels);

	/*
	 * create an array which contains all relations to be dropped, and close
	 * each relation's forks at the smgr level while at it
	 */
	rlocators = palloc(sizeof(RelFileLocatorBackend) * nrels);
	for (i = 0; i < nrels; i++)
	{
		RelFileLocatorBackend rlocator = rels[i]->smgr_rlocator;
		int			which = rels[i]->smgr_which;

		rlocators[i] = rlocator;

		/* Close the forks at smgr level */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_close(rels[i], forknum);
	}

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for these rels.  We should do
	 * this before starting the actual unlinking, in case we fail partway
	 * through that step.  Note that the sinval messages will eventually come
	 * back to this backend, too, and thereby provide a backstop that we
	 * closed our own smgr rel.
	 */
	for (i = 0; i < nrels; i++)
		CacheInvalidateSmgr(rlocators[i]);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */

	/* PGRAC: spec-2.7 cross-instance unlink-pending hook (hardening F2
	 * 2026-05-09 — moved from post-unlink to pre-unlink, alongside
	 * CacheInvalidateSmgr above).  PG sinval is sent BEFORE the
	 * physical unlink so peers transitioning to invalidated state
	 * during the broadcast→unlink window stay safe;the reverse opens
	 * a stale-state read window once spec-2.27 flips this stub to a
	 * real wire send.  Per-rel smgr_which filter (hardening F4) skips
	 * md.c relations entirely so the hook only fires for relations
	 * actually routed through cluster_smgr. */
#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled)
	{
		for (i = 0; i < nrels; i++)
		{
			if (rels[i]->smgr_which == CLUSTER_SMGR_SMGRSW_INDEX)
				cluster_smgr_invalidate_unlink_pending(rlocators[i].locator);
		}

		/*
		 * PGRAC: spec-5.51 D5 / spec-5.53 D2b — bump the shared CR pool lifecycle
		 * epoch on ANY relfilenode unlink.  This is the PROVABLY-COMPLETE reuse
		 * floor: relNumber=Oid is reusable, so a stale CR image keyed by a freed
		 * relNumber must be fenced before that number can be re-allocated (rule
		 * 8.A; F0-10).  Completeness rests on a single chokepoint: every physical
		 * relfilenode unlink funnels through smgrdounlinkall, and the bump is here,
		 * UNCONDITIONAL (only cluster_enabled-gated; intentionally OUTSIDE the
		 * CLUSTER_SMGR_SMGRSW_INDEX filter above — the spec-5.51 H1 regression was
		 * exactly a narrow gate that missed md.c heaps).  smgrdounlinkall's callers
		 * cover every relfilenode-free path:
		 *   - catalog/storage.c smgrDoPendingDeletes   (commit: DROP, TRUNCATE old
		 *     relfilenode via RelationSetNewRelfilenumber, CLUSTER/VACUUM FULL old
		 *     via swap_relation_files — all register pendingDeletes)
		 *   - utils/cache/relcache.c                    (RelationSetNewRelfilenumber)
		 *   - storage/smgr/md.c                         (redo replay of unlink)
		 * A new relation can only re-use a relNumber after the old file is unlinked
		 * (GetNewRelFileNumber's access(F_OK) collision), i.e. strictly AFTER this
		 * bump — so reuse is always fenced.  O(1) atomic; a no-op when the pool is
		 * disabled.  spec-5.53 D2b fault-inject: when armed, skip the bump for ONE
		 * unlink to prove the bump is load-bearing (a "missed bump" leaves the
		 * epoch un-advanced — the regression is observable in cr_pool_epoch).
		 */
		CLUSTER_INJECTION_POINT("cluster-cr-skip-epoch-bump");
		if (!cluster_injection_should_skip("cluster-cr-skip-epoch-bump"))
			cluster_cr_pool_bump_epoch();
	}
#endif

	for (i = 0; i < nrels; i++)
	{
		int			which = rels[i]->smgr_which;

		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_unlink(rlocators[i], forknum, isRedo);
	}

	pfree(rlocators);
}


/*
 * smgrextend() -- Add a new block to a file.
 *
 * The semantics are nearly the same as smgrwrite(): write at the
 * specified position.  However, this is to be used for the case of
 * extending a relation (i.e., blocknum is at or beyond the current
 * EOF).  Note that we assume writing a block beyond current EOF
 * causes intervening file space to become filled with zeroes.
 */
void
smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   const void *buffer, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_extend(reln, forknum, blocknum,
										 buffer, skipFsync);

	/*
	 * Normally we expect this to increase nblocks by one, but if the cached
	 * value isn't as expected, just invalidate it so the next call asks the
	 * kernel.
	 */
	if (reln->smgr_cached_nblocks[forknum] == blocknum)
		reln->smgr_cached_nblocks[forknum] = blocknum + 1;
	else
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;

	/* PGRAC: spec-2.7 cross-instance invalidation hook (per-operation
	 * granularity per Q3 v0.2 — counter atomic-add only, no DEBUG2).
	 * Hardening F4 (2026-05-09):  smgr_which gate skips md.c relations
	 * so only cluster_smgr-routed extends fire the broadcast hook. */
#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled && reln->smgr_which == CLUSTER_SMGR_SMGRSW_INDEX)
		cluster_smgr_invalidate_relation(reln->smgr_rlocator.locator, forknum);
#endif
}

/*
 * smgrzeroextend() -- Add new zeroed out blocks to a file.
 *
 * Similar to smgrextend(), except the relation can be extended by
 * multiple blocks at once and the added blocks will be filled with
 * zeroes.
 */
void
smgrzeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   int nblocks, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_zeroextend(reln, forknum, blocknum,
											 nblocks, skipFsync);

	/*
	 * Normally we expect this to increase the fork size by nblocks, but if
	 * the cached value isn't as expected, just invalidate it so the next call
	 * asks the kernel.
	 */
	if (reln->smgr_cached_nblocks[forknum] == blocknum)
		reln->smgr_cached_nblocks[forknum] = blocknum + nblocks;
	else
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;

	/* PGRAC: spec-2.7 cross-instance invalidation hook (per-operation
	 * granularity per Q3 v0.2 — fires once per smgrzeroextend regardless
	 * of nblocks, not per-block — coalescing is spec-2.27 SI
	 * Broadcaster's responsibility).  Hardening F4 (2026-05-09):
	 * smgr_which gate skips md.c relations. */
#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled && reln->smgr_which == CLUSTER_SMGR_SMGRSW_INDEX)
		cluster_smgr_invalidate_relation(reln->smgr_rlocator.locator, forknum);
#endif
}

/*
 * smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
 *
 * In recovery only, this can return false to indicate that a file
 * doesn't exist (presumably it has been dropped by a later WAL
 * record).
 */
bool
smgrprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	return smgrsw[reln->smgr_which].smgr_prefetch(reln, forknum, blocknum);
}

/*
 * smgrread() -- read a particular block from a relation into the supplied
 *				 buffer.
 *
 * This routine is called from the buffer manager in order to
 * instantiate pages in the shared buffer cache.  All storage managers
 * return pages in the format that POSTGRES expects.
 */
void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 void *buffer)
{
	smgrsw[reln->smgr_which].smgr_read(reln, forknum, blocknum, buffer);
}

/*
 * smgrwrite() -- Write the supplied buffer out.
 *
 * This is to be used only for updating already-existing blocks of a
 * relation (ie, those before the current EOF).  To extend a relation,
 * use smgrextend().
 *
 * This is not a synchronous write -- the block is not necessarily
 * on disk at return, only dumped out to the kernel.  However,
 * provisions will be made to fsync the write before the next checkpoint.
 *
 * skipFsync indicates that the caller will make other provisions to
 * fsync the relation, so we needn't bother.  Temporary relations also
 * do not require fsync.
 */
void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  const void *buffer, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_write(reln, forknum, blocknum,
										buffer, skipFsync);
}


/*
 * smgrwriteback() -- Trigger kernel writeback for the supplied range of
 *					   blocks.
 */
void
smgrwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  BlockNumber nblocks)
{
	smgrsw[reln->smgr_which].smgr_writeback(reln, forknum, blocknum,
											nblocks);
}

/*
 * smgrnblocks() -- Calculate the number of blocks in the
 *					supplied relation.
 */
BlockNumber
smgrnblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber result;

	/* Check and return if we get the cached value for the number of blocks. */
	result = smgrnblocks_cached(reln, forknum);
	if (result != InvalidBlockNumber)
		return result;

	result = smgrsw[reln->smgr_which].smgr_nblocks(reln, forknum);

	reln->smgr_cached_nblocks[forknum] = result;

	return result;
}

/*
 * smgrnblocks_cached() -- Get the cached number of blocks in the supplied
 *						   relation.
 *
 * Returns an InvalidBlockNumber when not in recovery and when the relation
 * fork size is not cached.
 */
BlockNumber
smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * For now, this function uses cached values only in recovery due to lack
	 * of a shared invalidation mechanism for changes in file size.  Code
	 * elsewhere reads smgr_cached_nblocks and copes with stale data.
	 */
	if (InRecovery && reln->smgr_cached_nblocks[forknum] != InvalidBlockNumber)
		return reln->smgr_cached_nblocks[forknum];

	return InvalidBlockNumber;
}

/*
 * smgrtruncate() -- Truncate the given forks of supplied relation to
 *					 each specified numbers of blocks
 *
 * Backward-compatible version of smgrtruncate2() for the benefit of external
 * callers.  This version isn't used in PostgreSQL core code, and can't be
 * used in a critical section.
 */
void
smgrtruncate(SMgrRelation reln, ForkNumber *forknum, int nforks,
			 BlockNumber *nblocks)
{
	BlockNumber old_nblocks[MAX_FORKNUM + 1];

	for (int i = 0; i < nforks; ++i)
		old_nblocks[i] = smgrnblocks(reln, forknum[i]);

	smgrtruncate2(reln, forknum, nforks, old_nblocks, nblocks);
}

/*
 * smgrtruncate2() -- Truncate the given forks of supplied relation to
 *					  each specified numbers of blocks
 *
 * The truncation is done immediately, so this can't be rolled back.
 *
 * The caller must hold AccessExclusiveLock on the relation, to ensure that
 * other backends receive the smgr invalidation event that this function sends
 * before they access any forks of the relation again.  The current size of
 * the forks should be provided in old_nblocks.  This function should normally
 * be called in a critical section, but the current size must be checked
 * outside the critical section, and no interrupts or smgr functions relating
 * to this relation should be called in between.
 */
void
smgrtruncate2(SMgrRelation reln, ForkNumber *forknum, int nforks,
			  BlockNumber *old_nblocks, BlockNumber *nblocks)
{
	int			i;

	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	DropRelationBuffers(reln, forknum, nforks, nblocks);

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel.  This is useful because they
	 * might have open file pointers to segments that got removed, and/or
	 * smgr_targblock variables pointing past the new rel end.  (The inval
	 * message will come back to our backend, too, causing a
	 * probably-unnecessary local smgr flush.  But we don't expect that this
	 * is a performance-critical path.)  As in the unlink code, we want to be
	 * sure the message is sent before we start changing things on-disk.
	 */
	CacheInvalidateSmgr(reln->smgr_rlocator);

	/* PGRAC: spec-2.7 cross-instance invalidation hook (hardening F2
	 * 2026-05-09 — moved from post-truncate-loop to here, alongside
	 * CacheInvalidateSmgr, so spec-2.27 SI Broadcaster sees pre-modify
	 * timing identical to PG sinval — peers transitioning to
	 * invalidated state during the broadcast→truncate window stay
	 * safe).  Hardening F4: smgr_which gate skips md.c relations.
	 * Loop fires once per truncated fork so the stub counter sees
	 * deterministic input matching what spec-2.27 will wire-send. */
#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled && reln->smgr_which == CLUSTER_SMGR_SMGRSW_INDEX)
	{
		for (i = 0; i < nforks; i++)
			cluster_smgr_invalidate_relation(reln->smgr_rlocator.locator, forknum[i]);
	}
#endif

	/* Do the truncation */
	for (i = 0; i < nforks; i++)
	{
		/* Make the cached size is invalid if we encounter an error. */
		reln->smgr_cached_nblocks[forknum[i]] = InvalidBlockNumber;

		smgrsw[reln->smgr_which].smgr_truncate(reln, forknum[i],
											   old_nblocks[i], nblocks[i]);

		/*
		 * We might as well update the local smgr_cached_nblocks values. The
		 * smgr cache inval message that this function sent will cause other
		 * backends to invalidate their copies of smgr_cached_nblocks, and
		 * these ones too at the next command boundary. But ensure they aren't
		 * outright wrong until then.
		 *
		 * We can have nblocks > old_nblocks when a relation was truncated
		 * multiple times, a replica applied all the truncations, and later
		 * restarts from a restartpoint located before the truncations. The
		 * relation on disk will be the size of the last truncate. When
		 * replaying the first truncate, we will have nblocks > current size.
		 * In such cases, smgr_truncate does nothing, so set the cached size
		 * to the old size rather than the requested size.
		 */
		reln->smgr_cached_nblocks[forknum[i]] =
			nblocks[i] > old_nblocks[i] ? old_nblocks[i] : nblocks[i];
	}
}

/*
 * smgrimmedsync() -- Force the specified relation to stable storage.
 *
 * Synchronously force all previous writes to the specified relation
 * down to disk.
 *
 * This is useful for building completely new relations (eg, new
 * indexes).  Instead of incrementally WAL-logging the index build
 * steps, we can just write completed index pages to disk with smgrwrite
 * or smgrextend, and then fsync the completed index file before
 * committing the transaction.  (This is sufficient for purposes of
 * crash recovery, since it effectively duplicates forcing a checkpoint
 * for the completed index.  But it is *not* sufficient if one wishes
 * to use the WAL log for PITR or replication purposes: in that case
 * we have to make WAL entries as well.)
 *
 * The preceding writes should specify skipFsync = true to avoid
 * duplicative fsyncs.
 *
 * Note that you need to do FlushRelationBuffers() first if there is
 * any possibility that there are dirty buffers for the relation;
 * otherwise the sync is not very meaningful.
 */
void
smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	smgrsw[reln->smgr_which].smgr_immedsync(reln, forknum);
}

/*
 * AtEOXact_SMgr
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All transient SMgrRelation objects are closed.
 *
 * We do this as a compromise between wanting transient SMgrRelations to
 * live awhile (to amortize the costs of blind writes of multiple blocks)
 * and needing them to not live forever (since we're probably holding open
 * a kernel file descriptor for the underlying file, and we need to ensure
 * that gets closed reasonably soon if the file gets deleted).
 */
void
AtEOXact_SMgr(void)
{
	dlist_mutable_iter iter;

	/*
	 * Zap all unowned SMgrRelations.  We rely on smgrclose() to remove each
	 * one from the list.
	 */
	dlist_foreach_modify(iter, &unowned_relns)
	{
		SMgrRelation rel = dlist_container(SMgrRelationData, node,
										   iter.cur);

		Assert(rel->smgr_owner == NULL);

		smgrclose(rel);
	}
}

/*
 * This routine is called when we are ordered to release all open files by a
 * ProcSignalBarrier.
 */
bool
ProcessBarrierSmgrRelease(void)
{
	smgrreleaseall();
	return true;
}
