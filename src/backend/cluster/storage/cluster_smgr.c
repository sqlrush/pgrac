/*-------------------------------------------------------------------------
 *
 * cluster_smgr.c
 *	  pgrac cluster-aware storage manager: smgrsw[] entry that bridges
 *	  PG's f_smgr API into the cluster_shared_fs vtable.
 *
 *	  Stage 1.2 single-node single-file passthrough (方案 C).  Owns:
 *	    - the per-process bypass HTAB (RelFileLocatorBackend ->
 *	      ClusterSmgrRelationState) tracking per-fork handle and
 *	      nblocks cache;
 *	    - cluster_smgr_init / _shutdown lifecycle (called from PG's
 *	      smgrinit / smgrshutdown via smgrsw[1]);
 *	    - cluster_smgr_which_for() routing decision read by smgropen;
 *	    - sixteen f_smgr callbacks: nine core I/O ops dispatch to
 *	      cluster_shared_fs; three advisory ops (zeroextend, prefetch,
 *	      writeback) fall through to md.c; four lifecycle / structural
 *	      callbacks have local logic.
 *
 *	  Stage 1.2 deliberately does NOT split relations into 1GB
 *	  segments.  Each (rlocator, fork) maps to a single underlying
 *	  file; modern OSes allow multi-TB files.  This is方案 C of the
 *	  design iteration --- see
 *	  docs/spec-1.2-design-iteration-byte-identical.md for the full
 *	  rationale.  Single-file storage means cluster_smgr is NOT
 *	  byte-identical to md.c (no .1 .2 segment files), and switching
 *	  back to GUC=off (md.c) requires manual data migration.  Stage
 *	  1.4 PageHeader +8B SCN改造 makes byte-identical impossible
 *	  anyway, so 1.2 aligns with that reality early.
 *
 *	  See docs/cluster-smgr-design.md for the full design;
 *	  specs/spec-1.2-smgr-cluster.md for the stage scope.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_smgr.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Compiled only in --enable-cluster builds; see
 *	  src/backend/cluster/Makefile for the OBJS rules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "commands/tablespace.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/md.h"
#include "utils/hsearch.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_smgr.h"


#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * Bypass state structure (方案 C: 单文件版本)
 *
 *	One entry per RelFileLocatorBackend that smgropen has touched
 *	with smgr_which == 1.  Keyed by the full RelFileLocatorBackend
 *	struct (locator + backend) so temp / permanent collisions cannot
 *	happen.
 *
 *	fork_handles[forknum] is one cluster_shared_fs handle per fork;
 *	NULL means the lazy-open hasn't fired yet for this fork.
 *
 *	No local nblocks cache: PG's smgr.c already maintains
 *	SMgrRelationData.smgr_cached_nblocks and invalidates it on
 *	extend / truncate.  Caching here too creates double-cache
 *	staleness ("unexpected data beyond EOF" across backends).
 * ============================================================ */
typedef struct ClusterSmgrRelationState
{
	RelFileLocatorBackend	rlocator;	/* hash key */

	ClusterSharedFsHandle  *fork_handles[MAX_FORKNUM + 1];

	/*
	 * No nblocks cache: PG's smgr.c already maintains
	 * SMgrRelationData.smgr_cached_nblocks and invalidates it
	 * appropriately.  An additional cache here misses those
	 * invalidations and produces "unexpected data beyond EOF" across
	 * cross-backend writes.
	 */
} ClusterSmgrRelationState;


/* Process-local bypass HTAB.  NULL until cluster_smgr_init runs. */
static HTAB *cluster_smgr_relations = NULL;

#define CLUSTER_SMGR_INITIAL_HTAB_SIZE	1024


/* ============================================================
 * State helpers
 * ============================================================ */

/*
 * Look up or create the bypass state entry for an SMgrRelation.  Lazy:
 * the actual cluster_shared_fs handle is opened only when the relevant
 * fork is first read / written / extended.
 */
static ClusterSmgrRelationState *
cluster_smgr_state_lookup(SMgrRelation reln, bool create)
{
	ClusterSmgrRelationState *state;
	bool		found;

	Assert(cluster_smgr_relations != NULL);

	state = (ClusterSmgrRelationState *)
		hash_search(cluster_smgr_relations, &reln->smgr_rlocator,
					create ? HASH_ENTER : HASH_FIND, &found);

	if (state != NULL && !found)
	{
		/* Newly inserted entry; zero-init the per-fork arrays. */
		memset(state->fork_handles, 0, sizeof(state->fork_handles));
	}

	return state;
}


/*
 * Ensure the cluster_shared_fs handle for (state, fork) is open.
 * Lazy-opens on first access.  Caller must hold a valid state pointer.
 */
static ClusterSharedFsHandle *
cluster_smgr_ensure_handle(ClusterSmgrRelationState *state, ForkNumber forknum)
{
	if (state->fork_handles[forknum] == NULL)
		cluster_shared_fs_open(state->rlocator.locator, forknum,
							   &state->fork_handles[forknum]);
	return state->fork_handles[forknum];
}


/*
 * Drop and free all per-fork state for a given relation entry.  Used
 * by smgr_close (clean release) and smgr_unlink (file gone).  Does
 * not remove the HTAB entry itself; caller decides whether to keep
 * the entry for re-open (smgr_close) or remove it (smgr_unlink).
 */
static void
cluster_smgr_state_drop_handles(ClusterSmgrRelationState *state)
{
	int			f;

	for (f = 0; f <= MAX_FORKNUM; f++)
	{
		if (state->fork_handles[f] != NULL)
		{
			cluster_shared_fs_close(state->fork_handles[f]);
			state->fork_handles[f] = NULL;
		}
	}
}


/* ============================================================
 * Lifecycle
 * ============================================================ */

void
cluster_smgr_init(void)
{
	HASHCTL		info;

	/*
	 * smgrinit() is called once per backend (and once in postmaster).
	 * Idempotent: if cluster_smgr_init runs twice in the same process,
	 * the second call is a no-op.
	 */
	if (cluster_smgr_relations != NULL)
		return;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(RelFileLocatorBackend);
	info.entrysize = sizeof(ClusterSmgrRelationState);

	cluster_smgr_relations = hash_create("cluster_smgr_relations",
										 CLUSTER_SMGR_INITIAL_HTAB_SIZE,
										 &info,
										 HASH_ELEM | HASH_BLOBS);

	elog(DEBUG1, "cluster_smgr: bypass HTAB initialised "
		 "(initial size %d entries)", CLUSTER_SMGR_INITIAL_HTAB_SIZE);
}


void
cluster_smgr_shutdown(void)
{
	HASH_SEQ_STATUS				seq;
	ClusterSmgrRelationState   *state;

	if (cluster_smgr_relations == NULL)
		return;

	/* Walk every state entry and close every open fork handle. */
	hash_seq_init(&seq, cluster_smgr_relations);
	while ((state = (ClusterSmgrRelationState *) hash_seq_search(&seq)) != NULL)
		cluster_smgr_state_drop_handles(state);

	hash_destroy(cluster_smgr_relations);
	cluster_smgr_relations = NULL;
}


/* ============================================================
 * smgrsw[] dispatch decision
 *
 *	Four short-circuit return-0 (= md.c) cases:
 *	  - temp relations (backend != InvalidBackendId)
 *	  - cluster_shared_storage_backend == STUB
 *	  - cluster.smgr_user_relations == off (the opt-in default)
 *	  - cluster_smgr_relations not initialised yet (very early init)
 *
 *	See docs/cluster-smgr-design.md §5 for the rationale.
 * ============================================================ */

int
cluster_smgr_which_for(RelFileLocator rlocator, BackendId backend)
{
	CLUSTER_INJECTION_POINT("cluster-smgr-which-decision");

	(void) rlocator;

	if (backend != InvalidBackendId)
		return 0;	/* temp relation: always md.c */

	if (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_STUB)
		return 0;	/* cluster fs disabled: pure md.c path */

	if (!cluster_smgr_user_relations)
		return 0;	/* opt-in GUC off: keep default safe */

	return 1;		/* cluster_smgr */
}


/* ============================================================
 * Sixteen f_smgr callbacks (方案 C: 单文件直转)
 * ============================================================ */

void
cluster_smgr_open(SMgrRelation reln)
{
	CLUSTER_INJECTION_POINT("cluster-smgr-open-top");

	/* Ensure HTAB exists (lazy-init guard for very-early callers). */
	if (cluster_smgr_relations == NULL)
		cluster_smgr_init();

	/* Just create the bypass entry; lazy-open the handles on first
	 * I/O so smgropen of an unused relation is cheap. */
	(void) cluster_smgr_state_lookup(reln, true);
}


void
cluster_smgr_close(SMgrRelation reln, ForkNumber forknum)
{
	ClusterSmgrRelationState *state;

	state = cluster_smgr_state_lookup(reln, false);
	if (state == NULL)
		return;

	/*
	 * f_smgr.smgr_close per-fork variant: PG calls this once per fork
	 * and once with forknum = InvalidForkNumber to release everything.
	 */
	if (forknum == InvalidForkNumber)
	{
		cluster_smgr_state_drop_handles(state);
		hash_search(cluster_smgr_relations, &reln->smgr_rlocator,
					HASH_REMOVE, NULL);
	}
	else if (state->fork_handles[forknum] != NULL)
	{
		cluster_shared_fs_close(state->fork_handles[forknum]);
		state->fork_handles[forknum] = NULL;
	}
}


void
cluster_smgr_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	ClusterSmgrRelationState *state;

	CLUSTER_INJECTION_POINT("cluster-smgr-create-top");

	/*
	 * Ensure the tablespace's per-database directory exists before we
	 * try to create the relation file inside it.  Mirrors
	 * mdcreate's first action.  Without this, ALTER TABLE SET
	 * TABLESPACE fails when moving a relation to a fresh tablespace
	 * because PathNameOpenFile(O_CREAT) can't create the file under
	 * a nonexistent parent directory.
	 */
	TablespaceCreateDbspace(reln->smgr_rlocator.locator.spcOid,
							reln->smgr_rlocator.locator.dbOid, isRedo);

	state = cluster_smgr_state_lookup(reln, true);

	/* cluster_shared_fs_local opens with O_RDWR | O_CREAT, so the
	 * file is created on first open.  Force the open here. */
	(void) cluster_smgr_ensure_handle(state, forknum);
}


bool
cluster_smgr_exists(SMgrRelation reln, ForkNumber forknum)
{
	ClusterSmgrRelationState *state;
	char	   *path;
	struct stat st;
	bool		exists;

	/*
	 * Already opened in this backend?  Definitely exists.
	 */
	state = cluster_smgr_state_lookup(reln, false);
	if (state != NULL && state->fork_handles[forknum] != NULL)
		return true;

	/*
	 * Path-based existence check, mirroring md.c::mdexists.  We can't
	 * use cluster_shared_fs_open here because the local backend opens
	 * with O_CREAT (would always report "exists" by side-effect of
	 * creating the file).  The path scheme is a pgrac-stable contract
	 * (see docs/cluster-smgr-design.md §3.3): for stage 1.2's local
	 * backend the path is the same as md.c's main segment path, so
	 * stat() is the right check.  Stage 2 backends without paths will
	 * need an exists() callback added to cluster_shared_fs.
	 */
	path = relpathbackend(reln->smgr_rlocator.locator,
						  reln->smgr_rlocator.backend, forknum);
	exists = (stat(path, &st) == 0);
	pfree(path);

	return exists;
}


void
cluster_smgr_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum,
					bool isRedo)
{
	ClusterSmgrRelationState *state;

	(void) isRedo;

	if (cluster_smgr_relations != NULL)
	{
		state = (ClusterSmgrRelationState *)
			hash_search(cluster_smgr_relations, &rlocator,
						HASH_FIND, NULL);
		if (state != NULL)
		{
			/* Close any open handles before unlinking the underlying file. */
			if (forknum == InvalidForkNumber)
				cluster_smgr_state_drop_handles(state);
			else if (state->fork_handles[forknum] != NULL)
			{
				cluster_shared_fs_close(state->fork_handles[forknum]);
				state->fork_handles[forknum] = NULL;
			}
		}
	}

	/*
	 * Physical unlink.  forknum == InvalidForkNumber means "all forks";
	 * cluster_shared_fs_unlink takes a single fork, so iterate.
	 */
	if (forknum == InvalidForkNumber)
	{
		ForkNumber	f;

		for (f = 0; f <= MAX_FORKNUM; f++)
			cluster_shared_fs_unlink(rlocator.locator, f);

		/* Drop the bypass state entry now that disk is gone. */
		if (cluster_smgr_relations != NULL)
			hash_search(cluster_smgr_relations, &rlocator,
						HASH_REMOVE, NULL);
	}
	else
	{
		cluster_shared_fs_unlink(rlocator.locator, forknum);
	}
}


void
cluster_smgr_extend(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;

	(void) skipFsync;	/* PG handles fsync via the buffer manager */

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	/*
	 * Caller (PG bufmgr or hio.c) supplies a pre-filled buffer with
	 * either real tuples or all-zeros.  Writing at offset blocknum *
	 * BLCKSZ extends the underlying file; intermediate blocks (if any)
	 * appear as sparse zero-filled holes from the kernel's view, the
	 * same as md.c.
	 */
	cluster_shared_fs_write(handle, blocknum, (const char *) buffer);
}


void
cluster_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, int nblocks, bool skipFsync)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;
	char						zerobuf[BLCKSZ];
	int							i;

	(void) skipFsync;

	/*
	 * mdzeroextend cannot be used as a fallback: it operates on PG's
	 * SMgrRelationData.md_seg_fds[], which is uninitialised when
	 * smgr_which == 1 (cluster_smgr never calls mdcreate on this
	 * relation, so md_seg_fds[forknum] is NULL).  Calling mdzeroextend
	 * would double-open the underlying file via PG's md.c path layer
	 * and desynchronise our nblocks cache.  Implement zero-extend
	 * directly via cluster_shared_fs_write of zero blocks.  Stage 6+
	 * may add a bulk zero-extend callback to cluster_shared_fs for
	 * performance; for stage 1.2 simple iteration is sufficient.
	 */
	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	memset(zerobuf, 0, BLCKSZ);
	for (i = 0; i < nblocks; i++)
		cluster_shared_fs_write(handle, blocknum + i, zerobuf);

}


bool
cluster_smgr_prefetch(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum)
{
	/*
	 * Prefetch is purely advisory (no correctness consequence if it's
	 * a no-op).  Stage 6+ may wire posix_fadvise via a bulk
	 * cluster_shared_fs callback; stage 1.2 just returns true (= "I
	 * tried", per PG's smgr_prefetch contract).  We deliberately do
	 * NOT delegate to mdprefetch because that would touch md.c's
	 * SMgrRelationData state our smgr_which=1 path never initialises.
	 */
	(void) reln;
	(void) forknum;
	(void) blocknum;
	return true;
}


void
cluster_smgr_read(SMgrRelation reln, ForkNumber forknum,
				  BlockNumber blocknum, void *buffer)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_read(handle, blocknum, (char *) buffer);
}


void
cluster_smgr_write(SMgrRelation reln, ForkNumber forknum,
				   BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;

	(void) skipFsync;

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_write(handle, blocknum, (const char *) buffer);
}


void
cluster_smgr_writeback(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, BlockNumber nblocks)
{
	/*
	 * Writeback is purely advisory (posix_fadvise WILLNEED-style hint).
	 * Stage 6+ may wire it through cluster_shared_fs; stage 1.2 makes
	 * it a no-op.  Same reason as cluster_smgr_prefetch: cannot
	 * delegate to md.c (md_seg_fds uninitialised on smgr_which=1).
	 */
	(void) reln;
	(void) forknum;
	(void) blocknum;
	(void) nblocks;
}


BlockNumber
cluster_smgr_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;

	/*
	 * No double caching: PG's smgr.c already maintains
	 * SMgrRelationData.smgr_cached_nblocks and invalidates it on
	 * extend / truncate.  An additional cache layer here would not
	 * see those PG-side invalidations, which causes other backends
	 * to read stale nblocks across cross-backend writes (showed up
	 * as "unexpected data beyond EOF" during PG 219 with GUC=on).
	 * Always go straight to the kernel-level FileSize via
	 * cluster_shared_fs_nblocks.
	 */
	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);
	return cluster_shared_fs_nblocks(handle);
}


void
cluster_smgr_truncate(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber old_blocks, BlockNumber nblocks)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;

	(void) old_blocks;	/* not needed at stage 1.2; useful for sync metadata */

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_truncate(handle, nblocks);
}


void
cluster_smgr_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	ClusterSmgrRelationState   *state;
	ClusterSharedFsHandle	   *handle;

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_immedsync(handle);
}


/* ============================================================
 * Diagnostic accessor
 * ============================================================ */

int
cluster_smgr_active_relation_count(void)
{
	if (cluster_smgr_relations == NULL)
		return 0;

	return (int) hash_get_num_entries(cluster_smgr_relations);
}

#endif							/* USE_PGRAC_CLUSTER */
