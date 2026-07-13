/*-------------------------------------------------------------------------
 *
 * cluster_pcm_own.h
 *	  pgrac per-buffer node-local ownership generation + flags.
 *
 *	  The buffer manager's BufferDesc.pcm_state is this node's cached belief
 *	  of the PCM mode it holds for a block.  spec-2.31/4.7a mirror it across
 *	  grant / downgrade / invalidate, but pcm_state alone cannot distinguish
 *	  ownership ROUNDS: X->S->X and N->X->N leave pcm_state unchanged even
 *	  though the grant that produced it is different (and may carry different
 *	  page bytes).  The page LSN is a CONTENT generation, not an ownership
 *	  generation, so it cannot serve either.
 *
 *	  This module adds an INDEPENDENT monotonic per-buffer ownership
 *	  generation plus two transient flags, stored in a shmem array indexed by
 *	  buf_id (parallel to BufferDescriptors, so no BufferDesc ABI change).
 *	  The generation is bumped on every COMMITTED local ownership transition;
 *	  a consumer that captured the generation before a window and finds it
 *	  changed after knows an ownership round happened even if pcm_state looks
 *	  identical.  The flags mark a grant that is in flight to install
 *	  (GRANT_PENDING) or a revoke in progress (REVOKING) so an invalidate /
 *	  BAST handler does not mistake a not-yet-finalized N for an idle block,
 *	  and a cached writer re-verifies against them.
 *
 *	  The (pcm_state, generation, flags) triple MUST be read and written
 *	  under the SAME lock -- the buffer header spinlock -- to be free of a
 *	  read-check-act race (TOCTOU).  The transition/read helpers that enforce
 *	  that live in bufmgr.c (they need BufferDesc + LockBufHdr); this header
 *	  owns only the shmem array and the by-buf_id raw atomic accessors.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_pcm_own.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-4.7a-hold-until-revoked.md (ownership-generation wave).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_OWN_H
#define CLUSTER_PCM_OWN_H

#include "c.h"
#include "port/atomics.h"

/* Transient ownership flags (per buffer). */
#define PCM_OWN_FLAG_GRANT_PENDING ((uint32)0x1) /* a grant is in flight to install */
#define PCM_OWN_FLAG_REVOKING ((uint32)0x2)		 /* a revoke (downgrade/invalidate) started */

typedef struct ClusterPcmOwnEntry {
	pg_atomic_uint64 generation; /* monotone; bumped on every committed transition */
	pg_atomic_uint32 flags;		 /* PCM_OWN_FLAG_* */
	uint32 _pad;				 /* keep 16B aligned */
} ClusterPcmOwnEntry;

/* Shmem array, NBuffers entries, indexed by buf_id. */
extern PGDLLIMPORT ClusterPcmOwnEntry *ClusterPcmOwnArray;

extern Size cluster_pcm_own_shmem_size(void);
extern void cluster_pcm_own_shmem_init(void);
extern void cluster_pcm_own_shmem_register(void);

/*
 * Raw by-buf_id atomic accessors.  Callers that need the (pcm_state,gen,flags)
 * triple coherent must hold the buffer header spinlock around these AND the
 * BufferDesc.pcm_state access (see bufmgr.c cluster_pcm_own_* helpers); the
 * atomics here are individually safe but the triple's coherence is the lock's
 * job.  NULL-safe (returns 0) before shmem init, so the enable-cluster-off and
 * bootstrap paths are unaffected.
 */
static inline uint64
cluster_pcm_own_gen_get(int buf_id)
{
	if (ClusterPcmOwnArray == NULL || buf_id < 0)
		return 0;
	return pg_atomic_read_u64(&ClusterPcmOwnArray[buf_id].generation);
}

static inline uint32
cluster_pcm_own_flags_get(int buf_id)
{
	if (ClusterPcmOwnArray == NULL || buf_id < 0)
		return 0;
	return pg_atomic_read_u32(&ClusterPcmOwnArray[buf_id].flags);
}

static inline void
cluster_pcm_own_gen_bump(int buf_id)
{
	if (ClusterPcmOwnArray == NULL || buf_id < 0)
		return;
	(void)pg_atomic_fetch_add_u64(&ClusterPcmOwnArray[buf_id].generation, 1);
}

static inline void
cluster_pcm_own_flags_apply(int buf_id, uint32 set, uint32 clear)
{
	uint32 old;

	if (ClusterPcmOwnArray == NULL || buf_id < 0)
		return;
	/* Callers hold the buffer header spinlock, so a read-modify-write is race
	 * free; a plain atomic write keeps the store visible to lock-free readers. */
	old = pg_atomic_read_u32(&ClusterPcmOwnArray[buf_id].flags);
	pg_atomic_write_u32(&ClusterPcmOwnArray[buf_id].flags, (old | set) & ~clear);
}

#endif /* CLUSTER_PCM_OWN_H */
