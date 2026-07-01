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
 *	    - sixteen f_smgr callbacks: eleven core I/O ops dispatch to
 *	      cluster_shared_fs (which has eleven storage callbacks plus
 *	      two lifecycle callbacks, thirteen function pointers total
 *	      after spec-1.X Sprint A vtable split + spec-1.7.2 create
 *	      isRedo amend); three advisory ops (zeroextend, prefetch,
 *	      writeback) fall through to md.c; two lifecycle / structural
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
#include "miscadmin.h" /* spec-5.2 D1: CritSectionCount */
#include "storage/md.h"
#include "storage/shmem.h"
#include "utils/errcodes.h" /* spec-5.2 D1: ERRCODE_CLUSTER_SINVAL_QUEUE_FULL */
#include "utils/hsearch.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sinval.h"		 /* spec-5.2 D1: relsize inval broadcast */
#include "cluster/cluster_write_fence.h" /* spec-4.12 D5 — hot write-path fence gate */
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
typedef struct ClusterSmgrRelationState {
	RelFileLocatorBackend rlocator; /* hash key */

	ClusterSharedFsHandle *fork_handles[MAX_FORKNUM + 1];

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

#define CLUSTER_SMGR_INITIAL_HTAB_SIZE 1024

static void
cluster_smgr_init_filetag(FileTag *tag, RelFileLocator rlocator, ForkNumber forknum)
{
	memset(tag, 0, sizeof(*tag));
	tag->handler = SYNC_HANDLER_CLUSTER_SHARED;
	tag->forknum = forknum;
	tag->rlocator = rlocator;
	tag->segno = 0; /* cluster_shared_fs stores one logical file per fork. */
}

static void
cluster_smgr_register_dirty(SMgrRelation reln, ForkNumber forknum, ClusterSharedFsHandle *handle)
{
	FileTag tag;

	if (RelFileLocatorBackendIsTemp(reln->smgr_rlocator))
		return;

	cluster_smgr_init_filetag(&tag, reln->smgr_rlocator.locator, forknum);
	if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false)) {
		ereport(DEBUG1, (errmsg_internal("could not forward cluster shared-storage fsync request "
										 "because request queue is full")));
		cluster_shared_fs_barrier_sync(handle);
	}
}

static void
cluster_smgr_forget_fsync(RelFileLocator rlocator, ForkNumber forknum)
{
	FileTag tag;

	cluster_smgr_init_filetag(&tag, rlocator, forknum);
	RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);
}


/*
 * spec-2.7 D6 (v0.2 frozen 2026-05-09;hardening F1 2026-05-09):
 *
 *	Cross-instance broadcast STUB call counter, allocated in shmem
 *	so all backends in this postmaster share one accumulator.  Hot
 *	path adds bypass any LWLock (atomic fetch-add only).  Counts
 *	the cross-instance portion only — the local handle/HTAB
 *	cleanup inside invalidate_unlink_pending is NOT counted here
 *	(already covered by PG SMgrRelation lifecycle observability per
 *	Q5 v0.2).
 *
 *	Pre-hardening this counter was a process-local pg_atomic_uint64;
 *	user review 2026-05-09 caught that the per-backend semantics
 *	contradicted both the manual ("counter advances on every
 *	relation extend...") and the spec-1.X cluster_pgstat口径.
 *	Promoted to shmem so SQL queries against pg_stat_cluster_counters
 *	see the live cluster-wide value regardless of which backend
 *	answers the query.
 *
 *	spec-2.27 will rename this to
 *	cluster_smgr_remote_invalidation_count (drop `_stub_`) and add
 *	per-type sub-counters + per-rlocator histograms once the SI
 *	Broadcaster wire protocol lands.
 */
typedef struct ClusterSmgrShmem {
	pg_atomic_uint64 remote_invalidation_stub_call_count;
	/* spec-5.2 D1: count of relsize SMGR invalidations actually broadcast
	 * to peers (source side).  Distinct from the legacy hook-invocation
	 * counter above, which advances even when no broadcast is emitted. */
	pg_atomic_uint64 smgr_inval_bcast_sent_count;
} ClusterSmgrShmem;

static ClusterSmgrShmem *cluster_smgr_state = NULL;


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
	bool found;

	Assert(cluster_smgr_relations != NULL);

	state = (ClusterSmgrRelationState *)hash_search(cluster_smgr_relations, &reln->smgr_rlocator,
													create ? HASH_ENTER : HASH_FIND, &found);

	if (state != NULL && !found) {
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
	/*
	 * Sprint A 2026-05-02 (spec-1.X-cluster-smgr-hardening): vtable
	 * `open` was split into exists / open_existing / create.  This
	 * lazy-open path is reached from smgr_read / smgr_write / smgr_
	 * extend etc. -- by the time the caller hits these, smgrcreate
	 * has already been called for new relations, so the file must
	 * exist on disk.  Use open_existing (no O_CREAT side effect).
	 *
	 * If the file does not exist (e.g. after DROP TABLE while a
	 * stale SMgrRelation is still cached), open_existing ereports
	 * ERRCODE_UNDEFINED_FILE which propagates correctly.
	 */
	if (state->fork_handles[forknum] == NULL)
		cluster_shared_fs_open_existing(state->rlocator.locator, forknum,
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
	int f;

	for (f = 0; f <= MAX_FORKNUM; f++) {
		if (state->fork_handles[f] != NULL) {
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
	HASHCTL info;

	/*
	 * PG's smgrinit() is called by BaseInit() during backend startup
	 * (normal or standalone), but NOT during postmaster start
	 * (see PG smgr.c:162 and spec-1.7.2 F2 fix discussion).  Each
	 * backend therefore lazy-initialises its own bypass HTAB on first
	 * use.  Idempotent: if cluster_smgr_init runs twice in the same
	 * process, the second call is a no-op.
	 */
	if (cluster_smgr_relations != NULL)
		return;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(RelFileLocatorBackend);
	info.entrysize = sizeof(ClusterSmgrRelationState);

	cluster_smgr_relations = hash_create("cluster_smgr_relations", CLUSTER_SMGR_INITIAL_HTAB_SIZE,
										 &info, HASH_ELEM | HASH_BLOBS);

	elog(DEBUG1,
		 "cluster_smgr: bypass HTAB initialised "
		 "(initial size %d entries)",
		 CLUSTER_SMGR_INITIAL_HTAB_SIZE);
}


void
cluster_smgr_shutdown(void)
{
	HASH_SEQ_STATUS seq;
	ClusterSmgrRelationState *state;

	if (cluster_smgr_relations == NULL)
		return;

	/* Walk every state entry and close every open fork handle. */
	hash_seq_init(&seq, cluster_smgr_relations);
	while ((state = (ClusterSmgrRelationState *)hash_seq_search(&seq)) != NULL)
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

	if (backend != InvalidBackendId)
		return 0; /* temp relation: always md.c */

	if (cluster_shared_storage_backend == CLUSTER_SHARED_FS_BACKEND_STUB)
		return 0; /* cluster fs disabled: pure md.c path */

	if (!cluster_smgr_user_relations)
		return 0; /* opt-in GUC off: keep default safe */

	/*
	 * USER relations only (spec-4.5a G6).  Catalogs -- including the
	 * shared ones under global/ -- are per-node state: every node runs
	 * its own catalog copy (cross-node catalog coordination is feature
	 * #11), so routing them into a genuinely shared root would point a
	 * node at files initdb wrote into a DIFFERENT node's PGDATA.  The
	 * local/stub backends masked this (their paths resolve inside the
	 * node's own PGDATA either way); cluster_fs does not.  Same
	 * relfilenumber boundary as cluster_bufmgr_should_pcm_track; a
	 * rewritten catalog (VACUUM FULL pg_class) moving above
	 * FirstNormalObjectId is out of harness scope until feature #11.
	 */
	if (rlocator.relNumber < FirstNormalObjectId)
		return 0; /* catalog / system relation: per-node md.c */

	return 1; /* cluster_smgr */
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
	(void)cluster_smgr_state_lookup(reln, true);
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
	if (forknum == InvalidForkNumber) {
		cluster_smgr_state_drop_handles(state);
		hash_search(cluster_smgr_relations, &reln->smgr_rlocator, HASH_REMOVE, NULL);
	} else if (state->fork_handles[forknum] != NULL) {
		cluster_shared_fs_close(state->fork_handles[forknum]);
		state->fork_handles[forknum] = NULL;
	}
}


void
cluster_smgr_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	ClusterSmgrRelationState *state;

	CLUSTER_INJECTION_POINT("cluster-smgr-create-top");

	/* spec-4.12 D5 (L240): reject before any side effect if this node is fenced. */
	cluster_write_fence_reject_if_fenced("create");

	/*
	 * Ensure the tablespace's per-database directory exists before we
	 * try to create the relation file inside it.  Mirrors
	 * mdcreate's first action.  Without this, ALTER TABLE SET
	 * TABLESPACE fails when moving a relation to a fresh tablespace
	 * because PathNameOpenFile(O_CREAT) can't create the file under
	 * a nonexistent parent directory.
	 */
	TablespaceCreateDbspace(reln->smgr_rlocator.locator.spcOid, reln->smgr_rlocator.locator.dbOid,
							isRedo);

	state = cluster_smgr_state_lookup(reln, true);

	/*
	 * Sprint A 2026-05-02 (spec-1.X-cluster-smgr-hardening): use the
	 * dedicated create() callback (was: implicit O_CREAT side effect
	 * from open()).  The split makes the create-vs-open distinction
	 * explicit and lets Stage 2 共享存储后端 implement protocol-aware
	 * idempotency (e.g. CAS create) instead of inheriting POSIX
	 * O_CREAT semantics.
	 *
	 * Spec-1.7.2 round 2 2026-05-03: forward isRedo so the local
	 * backend can use O_CREAT|O_EXCL (!isRedo) vs idempotent open
	 * (isRedo) per md.c mdcreate semantics.  Without isRedo a stale
	 * relfilenode file from a crashed CREATE could be silently reused
	 * with stale block contents -- P1.
	 */
	if (state->fork_handles[forknum] == NULL)
		cluster_shared_fs_create(state->rlocator.locator, forknum, isRedo,
								 &state->fork_handles[forknum]);
}


bool
cluster_smgr_exists(SMgrRelation reln, ForkNumber forknum)
{
	const ClusterSmgrRelationState *state;

	/*
	 * Already opened in this backend?  Definitely exists.
	 */
	state = cluster_smgr_state_lookup(reln, false);
	if (state != NULL && state->fork_handles[forknum] != NULL)
		return true;

	/*
	 * Sprint A 2026-05-02 (spec-1.X-cluster-smgr-hardening): use the
	 * vtable exists() callback (newly added by Sprint A item #1).
	 * This replaces the previous local-stat() hack which:
	 *   - bypassed the vtable contract (only worked for backends with
	 *     a valid local path),
	 *   - prevented Stage 2 共享存储后端 (NFS / S3 / Multi-Attach)
	 *     from being usable since they may have no local-path
	 *     fallback.
	 *
	 * The exists() callback for the local backend uses POSIX stat();
	 * Stage 2 backends will use protocol-level existence queries.
	 */
	return cluster_shared_fs_exists(reln->smgr_rlocator.locator, forknum);
}


void
cluster_smgr_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	(void)isRedo;

	/* spec-4.12 D5 (L240): reject before any handle close / physical unlink. */
	cluster_write_fence_reject_if_fenced("unlink");

	if (cluster_smgr_relations != NULL) {
		ClusterSmgrRelationState *state;

		state = (ClusterSmgrRelationState *)hash_search(cluster_smgr_relations, &rlocator,
														HASH_FIND, NULL);
		if (state != NULL) {
			/* Close any open handles before unlinking the underlying file. */
			if (forknum == InvalidForkNumber)
				cluster_smgr_state_drop_handles(state);
			else if (state->fork_handles[forknum] != NULL) {
				cluster_shared_fs_close(state->fork_handles[forknum]);
				state->fork_handles[forknum] = NULL;
			}
		}
	}

	/*
	 * Physical unlink.  forknum == InvalidForkNumber means "all forks";
	 * cluster_shared_fs_unlink takes a single fork, so iterate.
	 */
	if (forknum == InvalidForkNumber) {
		ForkNumber f;

		for (f = 0; f <= MAX_FORKNUM; f++) {
			cluster_smgr_forget_fsync(rlocator.locator, f);
			cluster_shared_fs_unlink(rlocator.locator, f);
		}

		/* Drop the bypass state entry now that disk is gone. */
		if (cluster_smgr_relations != NULL)
			hash_search(cluster_smgr_relations, &rlocator, HASH_REMOVE, NULL);
	} else {
		cluster_smgr_forget_fsync(rlocator.locator, forknum);
		cluster_shared_fs_unlink(rlocator.locator, forknum);
	}
}


void
cluster_smgr_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void *buffer,
					bool skipFsync)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;

	/* spec-4.12 D5 (L240): reject before extending the underlying file. */
	cluster_write_fence_reject_if_fenced("extend");

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	/*
	 * Establish logical EOF first, then write the caller's real page.  POSIX
	 * backends tolerate this as a zero-write followed by the real write; raw
	 * block_device requires the explicit extend so writes past logical EOF fail
	 * closed instead of silently allocating.
	 */
	cluster_shared_fs_extend(handle, blocknum);
	cluster_shared_fs_write(handle, blocknum, (const char *)buffer);
	if (!skipFsync)
		cluster_smgr_register_dirty(reln, forknum, handle);
}


void
cluster_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks,
						bool skipFsync)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;
	char zerobuf[BLCKSZ];
	int i;

	/* spec-4.12 D5 (L240): reject before any zero-block write. */
	cluster_write_fence_reject_if_fenced("zero-extend");

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
	for (i = 0; i < nblocks; i++) {
		cluster_shared_fs_extend(handle, blocknum + i);
		cluster_shared_fs_write(handle, blocknum + i, zerobuf);
	}
	if (!skipFsync && nblocks > 0)
		cluster_smgr_register_dirty(reln, forknum, handle);
}


bool
cluster_smgr_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	/*
	 * Prefetch is purely advisory (no correctness consequence if it's
	 * a no-op).  Stage 6+ may wire posix_fadvise via a bulk
	 * cluster_shared_fs callback; stage 1.2 just returns true (= "I
	 * tried", per PG's smgr_prefetch contract).  We deliberately do
	 * NOT delegate to mdprefetch because that would touch md.c's
	 * SMgrRelationData state our smgr_which=1 path never initialises.
	 */
	(void)reln;
	(void)forknum;
	(void)blocknum;
	return true;
}


void
cluster_smgr_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void *buffer)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_read(handle, blocknum, (char *)buffer);
}


void
cluster_smgr_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void *buffer,
				   bool skipFsync)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;

	/* spec-4.12 D5 (L240): reject before the shared-storage block write. */
	cluster_write_fence_reject_if_fenced("write");

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_write(handle, blocknum, (const char *)buffer);
	if (!skipFsync)
		cluster_smgr_register_dirty(reln, forknum, handle);
}


void
cluster_smgr_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					   BlockNumber nblocks)
{
	/*
	 * Writeback is purely advisory (posix_fadvise WILLNEED-style hint).
	 * Stage 6+ may wire it through cluster_shared_fs; stage 1.2 makes
	 * it a no-op.  Same reason as cluster_smgr_prefetch: cannot
	 * delegate to md.c (md_seg_fds uninitialised on smgr_which=1).
	 */
	(void)reln;
	(void)forknum;
	(void)blocknum;
	(void)nblocks;
}


BlockNumber
cluster_smgr_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;

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
cluster_smgr_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks,
					  BlockNumber nblocks)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;

	(void)old_blocks; /* not needed at stage 1.2; useful for sync metadata */

	/* spec-4.12 D5 (L240): reject before truncating the underlying file. */
	cluster_write_fence_reject_if_fenced("truncate");

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_truncate(handle, nblocks);
}


void
cluster_smgr_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	ClusterSmgrRelationState *state;
	ClusterSharedFsHandle *handle;

	state = cluster_smgr_state_lookup(reln, true);
	handle = cluster_smgr_ensure_handle(state, forknum);

	cluster_shared_fs_immedsync(handle);
}

int
cluster_smgr_syncfiletag(const FileTag *ftag, char *path)
{
	ClusterSharedFsHandle *handle = NULL;

	snprintf(path, MAXPGPATH, "cluster_shared:%u/%u/%u fork %d", ftag->rlocator.spcOid,
			 ftag->rlocator.dbOid, ftag->rlocator.relNumber, ftag->forknum);

	if (!cluster_shared_fs_exists(ftag->rlocator, ftag->forknum)) {
		errno = ENOENT;
		return -1;
	}

	cluster_shared_fs_open_existing(ftag->rlocator, ftag->forknum, &handle);
	cluster_shared_fs_barrier_sync(handle);
	cluster_shared_fs_close(handle);
	return 0;
}

int
cluster_smgr_unlinkfiletag(const FileTag *ftag, char *path)
{
	snprintf(path, MAXPGPATH, "cluster_shared:%u/%u/%u fork %d", ftag->rlocator.spcOid,
			 ftag->rlocator.dbOid, ftag->rlocator.relNumber, ftag->forknum);
	cluster_shared_fs_unlink(ftag->rlocator, ftag->forknum);
	return 0;
}

bool
cluster_smgr_filetagmatches(const FileTag *ftag, const FileTag *candidate)
{
	return ftag->rlocator.dbOid == candidate->rlocator.dbOid;
}


/* ============================================================
 * Diagnostic accessor
 * ============================================================ */

int
cluster_smgr_active_relation_count(void)
{
	if (cluster_smgr_relations == NULL)
		return 0;

	return (int)hash_get_num_entries(cluster_smgr_relations);
}


/* ============================================================
 * spec-2.7 invalidation hooks (v0.2 frozen 2026-05-09)
 *
 *	Three entry points for cluster-aware cache invalidation that
 *	spec-2.27 will activate with cross-instance SI Broadcaster wire
 *	send + ack.  The current bodies are stubs except for one local
 *	real action inside invalidate_unlink_pending.
 *
 *	See cluster_smgr.h hook block for the full per-hook contract and
 *	specs/spec-2.7-smgr-cluster-2node-concurrent-open.md §3.2 for
 *	the v0.2 stub behaviour契约.
 * ============================================================ */

/*
 * Helper: close any open ClusterSharedFsHandle for `rlocator` and
 * remove the bypass HTAB entry.  Used only by
 * cluster_smgr_invalidate_unlink_pending to prevent a stale fd /
 * stale HTAB entry from outliving an unlink.
 *
 * Permanent relations live under InvalidBackendId in the HTAB key
 * (RelFileLocatorBackend); temp relations are routed to md.c by
 * cluster_smgr_which_for and never reach this path.  We therefore
 * look up with backend == InvalidBackendId.
 */
static void
cluster_smgr_close_handle_for_rlocator(RelFileLocator rlocator)
{
	RelFileLocatorBackend key;
	ClusterSmgrRelationState *state;

	if (cluster_smgr_relations == NULL)
		return;

	key.locator = rlocator;
	key.backend = InvalidBackendId;

	state = (ClusterSmgrRelationState *)hash_search(cluster_smgr_relations, &key, HASH_FIND, NULL);
	if (state == NULL)
		return;

	cluster_smgr_state_drop_handles(state);
	hash_search(cluster_smgr_relations, &key, HASH_REMOVE, NULL);

	elog(DEBUG3, "cluster_smgr: invalidate_unlink_pending closed handle for rlocator %u/%u/%u",
		 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber);
}


/*
 * Helper: bump the shmem cross-instance STUB counter.  Defensive
 * NULL-guard against cluster_smgr_state == NULL so that unit-test
 * harnesses (which don't run cluster_shmem_init) plus any pre-shmem
 * call site stay safe.  Real backend lifecycle attaches the shared
 * struct via cluster_smgr_shmem_init before any SQL runs.
 */
static inline void
cluster_smgr_remote_invalidation_inc(void)
{
	if (cluster_smgr_state == NULL)
		return;
	pg_atomic_fetch_add_u64(&cluster_smgr_state->remote_invalidation_stub_call_count, 1);
}


/*
 * spec-5.2 D1 (M2 relsize coherence) — pure helpers (unit-tested by U2).
 */
void
cluster_smgr_build_smgr_inval_msg(RelFileLocator rlocator, SharedInvalidationMessage *out)
{
	uint32 backend = (uint32)InvalidBackendId;

	/*
	 * Mirror PG's CacheInvalidateSmgr() construction (inval.c).  Cluster
	 * relations live on shared storage and are never temp, so the backend
	 * component is InvalidBackendId — peers store the three packed bytes
	 * and reconstruct it in inval.c's SHAREDINVALSMGR_ID apply path.
	 */
	out->sm.id = SHAREDINVALSMGR_ID;
	/* Shift through unsigned: InvalidBackendId is -1 and shifting a negative
	 * value is UB (cppcheck shiftNegativeLHS).  ((uint32) -1) >> 16 == 0xffff,
	 * which truncates into the int8 backend_hi as -1 — byte-identical to PG's
	 * CacheInvalidateSmgr() and round-trips back to InvalidBackendId in the
	 * SHAREDINVALSMGR_ID apply path. */
	out->sm.backend_hi = backend >> 16;
	out->sm.backend_lo = backend & 0xffff;
	out->sm.rlocator = rlocator;
}

ClusterSmgrInvalFullAction
cluster_smgr_inval_full_action(bool in_crit_section)
{
	/*
	 * H2 fail-closed:  a dropped relsize invalidation leaves the peer with
	 * a stale-low smgr_cached_nblocks forever (8.A — it would read a
	 * committed block as "does not exist").  Outside a critical section we
	 * abort the extend (53R94);  inside one we cannot ereport(ERROR), so we
	 * fall back to a coarse RESET-all broadcast.
	 */
	return in_crit_section ? CLUSTER_SMGR_INVAL_FULL_RESET_ALL : CLUSTER_SMGR_INVAL_FULL_ABORT;
}

static inline void
cluster_smgr_inval_bcast_sent_inc(void)
{
	if (cluster_smgr_state == NULL)
		return;
	pg_atomic_fetch_add_u64(&cluster_smgr_state->smgr_inval_bcast_sent_count, 1);
}

void
cluster_smgr_invalidate_relation(RelFileLocator rlocator, ForkNumber forknum)
{
	SharedInvalidationMessage msg;

	/*
	 * spec-5.2 D1 (was spec-2.7 pure STUB):  broadcast a PG-native
	 * SHAREDINVALSMGR_ID invalidation so peers drop their stale
	 * SMgrRelation (incl. smgr_cached_nblocks) and re-stat the shared file
	 * on next smgrnblocks().  G2:  no new wire type — reuse PG's
	 * SharedInvalSmgrMsg in the existing cluster sinval tail.  forknum is
	 * irrelevant to the message: smgrcloserellocator() on the peer drops
	 * every fork's cached size at once (matches CacheInvalidateSmgr).
	 */
	(void)forknum;

	/*
	 * Only the cross-instance broadcast path is in scope here.  If the
	 * outbound sinval queue is not attached (single node, bootstrap,
	 * cluster disabled), there are no peers to tell — skip silently (the
	 * legacy counter below still advances).  Distinguishing this from a
	 * genuinely full queue is essential: a NULL outbound is benign, a full
	 * one is the H2 fail-closed case.
	 */
	if (!cluster_sinval_is_active()) {
		cluster_smgr_remote_invalidation_inc();
		return;
	}

	cluster_smgr_build_smgr_inval_msg(rlocator, &msg);

	if (cluster_sinval_enqueue_batch(&msg, 1))
		cluster_smgr_inval_bcast_sent_inc();
	else {
		/*
		 * H2 enqueue-full fail-closed — never silently continue (the peer
		 * would stay stale-low and the relsize apply barrier would never
		 * fire).  Policy depends on whether we may throw here.
		 */
		if (cluster_smgr_inval_full_action(CritSectionCount > 0)
			== CLUSTER_SMGR_INVAL_FULL_RESET_ALL) {
			/* In a critical section (e.g. inside the extend's WAL insert):
			 * cannot ERROR.  Request a coarse RESET-all broadcast so peers
			 * re-stat everything — correctness over precision. */
			cluster_sinval_request_reset_all_broadcast();
		} else {
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_SINVAL_QUEUE_FULL),
							errmsg("cluster relsize invalidation queue full"),
							errhint("Peers could not be told the relation grew; "
									"raise cluster.sinval_broadcast_max_queue_size or "
									"retry the operation.")));
		}
	}

	/* Legacy hook-invocation counter (dump_smgr observability) — advances
	 * on every call regardless of broadcast outcome. */
	cluster_smgr_remote_invalidation_inc();
}


void
cluster_smgr_invalidate_relmap(bool shared)
{
	/*
	 * spec-2.7 Q1 v0.2 + Q2 v0.2:  pure cross-instance broadcast
	 * STUB.  PG relmapper.c reloads its local map cache via
	 * load_relmap_file().  `shared` matches PG's
	 * RelationMapInvalidate(bool shared) signature:
	 *   shared = true  -> shared catalogs (pg_database, pg_authid, ...)
	 *   shared = false -> current MyDatabaseId per-database catalogs
	 * spec-2.27 SI Broadcaster will read `shared` to dispatch the
	 * correct sinval message type.
	 */
	(void)shared;

	cluster_smgr_remote_invalidation_inc();
}


void
cluster_smgr_invalidate_unlink_pending(RelFileLocator rlocator)
{
	/*
	 * spec-2.7 Q1 v0.2 + hardening F2 (2026-05-09):
	 *
	 *	Cross-instance broadcast STUB + LOCAL REAL action.  The
	 *	caller (smgrdounlinkall in PG smgr.c) now invokes this hook
	 *	BEFORE the physical unlink loop, alongside CacheInvalidateSmgr,
	 *	so that spec-2.27 SI Broadcaster can broadcast the inval
	 *	pre-modify (matches PG's sinval timing — peers transitioning
	 *	to invalidated state during the broadcast→unlink gap is safe;
	 *	the reverse opens a window where peers read stale state).
	 *
	 *	Local real (handle/HTAB cleanup):  close any open
	 *	ClusterSharedFsHandle for `rlocator` and remove the bypass
	 *	HTAB entry.  PG's smgrdounlinkall already called
	 *	smgrsw[].smgr_close on each fork before this hook fires, so
	 *	the per-fork handles are typically NULL by now;the HTAB
	 *	entry itself is still around because smgr_close only removes
	 *	on InvalidForkNumber.  Removing it here prevents stale fds
	 *	on a future smgropen of the same rlocator (e.g. CREATE TABLE
	 *	reusing the relfilenumber after a recent DROP).
	 *
	 *	Cross-instance STUB:  spec-2.27 SI Broadcaster will replace
	 *	the counter add with SINVAL_SMGR_UNLINK_PENDING wire send +
	 *	peer-ack barrier here.
	 */
	cluster_smgr_close_handle_for_rlocator(rlocator);
	cluster_smgr_remote_invalidation_inc();
	/*
	 * spec-5.51 D5: the shared CR pool lifecycle epoch is bumped in
	 * smgrdounlinkall (smgr.c) for ANY relfilenode unlink — not here, because
	 * this hook only fires for cluster_smgr-routed relations, which is too
	 * narrow for the CR pool (it caches images for md.c relations too).
	 */
}


uint64
cluster_smgr_get_remote_invalidation_stub_call_count(void)
{
	if (cluster_smgr_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_smgr_state->remote_invalidation_stub_call_count);
}

uint64
cluster_smgr_get_inval_bcast_sent_count(void)
{
	if (cluster_smgr_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_smgr_state->smgr_inval_bcast_sent_count);
}


/* ============================================================
 * spec-2.7 D6 hardening F1 (2026-05-09):  shmem region for the
 * cross-instance broadcast STUB call counter.  Follows the
 * cluster_epoch / cluster_diag pattern (size_fn + init_fn +
 * register_fn invoked from cluster_shmem.c).
 * ============================================================ */

Size
cluster_smgr_shmem_size(void)
{
	return sizeof(ClusterSmgrShmem);
}

void
cluster_smgr_shmem_init(void)
{
	bool found;

	cluster_smgr_state = (ClusterSmgrShmem *)ShmemInitStruct("pgrac cluster smgr",
															 cluster_smgr_shmem_size(), &found);
	if (!found) {
		pg_atomic_init_u64(&cluster_smgr_state->remote_invalidation_stub_call_count, 0);
		pg_atomic_init_u64(&cluster_smgr_state->smgr_inval_bcast_sent_count, 0);
	}
}

static const ClusterShmemRegion cluster_smgr_region = {
	.name = "pgrac cluster smgr",
	.size_fn = cluster_smgr_shmem_size,
	.init_fn = cluster_smgr_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_smgr",
	.reserved_flags = 0,
};

void
cluster_smgr_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_smgr_region);
}


#endif /* USE_PGRAC_CLUSTER */
